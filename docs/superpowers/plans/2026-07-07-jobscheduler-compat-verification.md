# v1 검증: JobScheduler 호환 어댑터 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** chronos-go가 operator-review의 `JobScheduler` 인터페이스(문자열 taskName + `[]byte` 페이로드)를 **그대로 백킹**할 수 있음을 증명한다 — 어댑터 `examples/jobscheduler`를 만들어 6개 동작(interval/cron 스케줄 잡, `[]byte` Enqueue + dedup, 태스크 핸들러, Start/Shutdown)이 동일하게 동작함을 테스트로 검증한다. operator-review 운영 코드는 건드리지 않는다.

**Architecture:** chronos-go는 제네릭(`Task[T]`, `Kind()`) 기반이라 문자열-키 `[]byte` 핸들러와 직접 안 맞는다. 어댑터는 단일 브리지 타입 `rawTask{Name, Data}`(고정 `Kind`)를 쓰고, chronos 핸들러 하나가 `Name`으로 내부 맵을 라우팅해 원래 `SchedulerJobFunc`/`TaskHandlerFunc`를 호출한다. 스케줄 잡은 `chronos.RegisterInterval/RegisterCron`으로 `rawTask{Name}`를 주기 enqueue하고(리더-only + 결정적 dedup으로 분산 단일 실행), Enqueue는 taskName/QueueKey를 FNV 해시해 N개 큐에 분배한다(operator-review 동작 복제).

**Tech Stack:** Go 1.26, chronos-go 공개 API(`NewClient`/`Enqueue`/`NewMux`/`AddHandler`/`NewServer`/`NewScheduler`/`RegisterInterval`/`RegisterCron`/`WithQueue`/`WithMaxRetry`/`WithUnique`/`ErrDuplicateTask`), 실제 Redis 하니스.

**참조 인터페이스 (operator-review `internal/pkg/mono/job-scheduler/domain`, 검증 대상):**
```go
type SchedulerJobFunc func(ctx context.Context) error
type TaskHandlerFunc  func(ctx context.Context, payload []byte) error
type EnqueueConfig struct { QueueKey string; UniqueTTL time.Duration }
type EnqueueOption func(*EnqueueConfig)
func WithQueueKey(key string) EnqueueOption
func WithUniqueTTL(ttl time.Duration) EnqueueOption
type JobScheduler interface {
    Start() error
    Shutdown()
    RegisterScheduledJob(taskName string, interval time.Duration, fn SchedulerJobFunc) (string, error)
    RegisterCronJob(taskName, cronSpec string, fn SchedulerJobFunc) (string, error)
    RegisterTaskHandler(taskName string, fn TaskHandlerFunc) error
    Enqueue(ctx context.Context, taskName string, payload []byte, opts ...EnqueueOption) error
}
```
operator-review 동작 요점(복제 대상): interval < 1s → 에러; 잡은 MaxRetry(0)(주기 잡이라 실패 시 다음 주기); Enqueue는 QueueKey(없으면 taskName) FNV 해시 → N개 큐; UniqueTTL>0면 dedup, 중복이면 조용히 skip(nil); taskName 중복 등록 에러.

**범위 밖:** operator-review 실제 교체(사용자가 확신 후 별도), 메트릭.

---

## File Structure

| 파일 | 내용 |
|---|---|
| `examples/jobscheduler/jobscheduler.go` (신규) | `package jobscheduler`: 인터페이스·타입 미러 + `rawTask` 브리지 + `New` + 어댑터 구현 |
| `examples/jobscheduler/jobscheduler_test.go` (신규) | 6개 동작 동등성 테스트 (실 Redis) |

**테스트:** 실제 Redis. `make test-race`.

---

## Task 1: 어댑터 구현 (JobScheduler 백킹)

**Files:**
- Create: `examples/jobscheduler/jobscheduler.go`

- [ ] **Step 1: 어댑터 작성**

Create `examples/jobscheduler/jobscheduler.go`:
```go
// Package jobscheduler is a verification adapter proving chronos-go can back
// operator-review's string+[]byte JobScheduler interface. It bridges that
// interface onto chronos-go's generic API via a single rawTask type routed by
// name. It is an example/migration aid, not part of chronos-go's core API.
package jobscheduler

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
)

// SchedulerJobFunc runs a scheduled job (no payload).
type SchedulerJobFunc func(ctx context.Context) error

// TaskHandlerFunc handles an enqueued task's raw payload.
type TaskHandlerFunc func(ctx context.Context, payload []byte) error

// EnqueueConfig / EnqueueOption mirror operator-review's enqueue options.
type EnqueueConfig struct {
	QueueKey  string
	UniqueTTL time.Duration
}

type EnqueueOption func(*EnqueueConfig)

func WithQueueKey(key string) EnqueueOption   { return func(c *EnqueueConfig) { c.QueueKey = key } }
func WithUniqueTTL(ttl time.Duration) EnqueueOption { return func(c *EnqueueConfig) { c.UniqueTTL = ttl } }

// JobScheduler is the operator-review interface this adapter proves chronos-go can back.
type JobScheduler interface {
	Start() error
	Shutdown()
	RegisterScheduledJob(taskName string, interval time.Duration, fn SchedulerJobFunc) (string, error)
	RegisterCronJob(taskName, cronSpec string, fn SchedulerJobFunc) (string, error)
	RegisterTaskHandler(taskName string, fn TaskHandlerFunc) error
	Enqueue(ctx context.Context, taskName string, payload []byte, opts ...EnqueueOption) error
}

// rawTask is the single bridge type carrying a string task name + raw payload
// through chronos-go's generic pipeline.
type rawTask struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

func (rawTask) Kind() string { return "chronos:compat:raw" }

type adapter struct {
	client    *chronos.Client
	server    *chronos.Server
	scheduler *chronos.Scheduler
	mux       *chronos.Mux
	queues    []string

	mu           sync.Mutex
	schedulerJob map[string]SchedulerJobFunc
	taskHandler  map[string]TaskHandlerFunc

	ctx    context.Context
	cancel context.CancelFunc
}

// New returns a JobScheduler backed by chronos-go, distributing work across the
// given queue names (like operator-review's FNV hashing).
func New(rdb redis.UniversalClient, queueNames []string) JobScheduler {
	if len(queueNames) == 0 {
		queueNames = []string{chronos.DefaultQueue}
	}
	queues := make(map[string]int, len(queueNames))
	for _, q := range queueNames {
		queues[q] = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &adapter{
		client:       chronos.NewClient(rdb),
		server:       chronos.NewServer(rdb, chronos.ServerConfig{Queues: queues, Concurrency: 8}),
		scheduler:    chronos.NewScheduler(rdb, chronos.SchedulerConfig{Location: time.Local}),
		mux:          chronos.NewMux(),
		queues:       queueNames,
		schedulerJob: make(map[string]SchedulerJobFunc),
		taskHandler:  make(map[string]TaskHandlerFunc),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// queueFor hashes a key to one of the configured queues (FNV-1a), matching
// operator-review's load distribution.
func (a *adapter) queueFor(key string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return a.queues[int(h.Sum32()%uint32(len(a.queues)))]
}

func (a *adapter) checkDup(taskName string) error {
	if _, ok := a.schedulerJob[taskName]; ok {
		return fmt.Errorf("jobscheduler: duplicate task name %q", taskName)
	}
	if _, ok := a.taskHandler[taskName]; ok {
		return fmt.Errorf("jobscheduler: duplicate task name %q", taskName)
	}
	return nil
}

func (a *adapter) RegisterScheduledJob(taskName string, interval time.Duration, fn SchedulerJobFunc) (string, error) {
	if interval < time.Second {
		return "", errors.New("jobscheduler: interval must be >= 1s")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkDup(taskName); err != nil {
		return "", err
	}
	a.schedulerJob[taskName] = fn
	if err := chronos.RegisterInterval(a.scheduler, interval, rawTask{Name: taskName},
		chronos.WithQueue(a.queueFor(taskName)), chronos.WithMaxRetry(0)); err != nil {
		return "", err
	}
	return taskName, nil
}

func (a *adapter) RegisterCronJob(taskName, cronSpec string, fn SchedulerJobFunc) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkDup(taskName); err != nil {
		return "", err
	}
	if err := chronos.RegisterCron(a.scheduler, cronSpec, rawTask{Name: taskName},
		chronos.WithQueue(a.queueFor(taskName)), chronos.WithMaxRetry(0)); err != nil {
		return "", err
	}
	a.schedulerJob[taskName] = fn
	return taskName, nil
}

func (a *adapter) RegisterTaskHandler(taskName string, fn TaskHandlerFunc) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkDup(taskName); err != nil {
		return err
	}
	a.taskHandler[taskName] = fn
	return nil
}

func (a *adapter) Enqueue(ctx context.Context, taskName string, payload []byte, opts ...EnqueueOption) error {
	cfg := EnqueueConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	key := cfg.QueueKey
	if key == "" {
		key = taskName
	}
	chOpts := []chronos.Option{chronos.WithQueue(a.queueFor(key)), chronos.WithMaxRetry(0)}
	if cfg.UniqueTTL > 0 {
		chOpts = append(chOpts, chronos.WithUnique(cfg.UniqueTTL))
	}
	_, err := chronos.Enqueue(ctx, a.client, rawTask{Name: taskName, Data: payload}, chOpts...)
	if errors.Is(err, chronos.ErrDuplicateTask) {
		return nil // operator-review skips duplicates silently
	}
	return err
}

func (a *adapter) Start() error {
	// One bridge handler routes every rawTask to its registered func by name.
	chronos.AddHandler(a.mux, func(ctx context.Context, t *chronos.Task[rawTask]) error {
		a.mu.Lock()
		sjob, sok := a.schedulerJob[t.Args.Name]
		thandler, tok := a.taskHandler[t.Args.Name]
		a.mu.Unlock()
		switch {
		case sok:
			return sjob(ctx)
		case tok:
			return thandler(ctx, t.Args.Data)
		default:
			return fmt.Errorf("jobscheduler: no handler for task %q", t.Args.Name)
		}
	})
	if err := a.server.Start(a.ctx, a.mux); err != nil {
		return err
	}
	return a.scheduler.Start(a.ctx)
}

func (a *adapter) Shutdown() {
	a.cancel()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.scheduler.Shutdown(shutCtx)
	_ = a.server.Shutdown(shutCtx)
	_ = a.client.Close()
}
```

- [ ] **Step 2: 빌드 확인**

Run: `go build ./... && go vet ./...`
Expected: 성공, 클린.

- [ ] **Step 3: 커밋**

```bash
git add examples/jobscheduler/jobscheduler.go
git commit -m "feat: JobScheduler 호환 어댑터 (chronos-go로 operator-review 인터페이스 백킹)"
```

---

## Task 2: 동등성 검증 테스트

**Files:**
- Create: `examples/jobscheduler/jobscheduler_test.go`

- [ ] **Step 1: 테스트 작성**

Create `examples/jobscheduler/jobscheduler_test.go`:
```go
package jobscheduler

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	if err := c.Ping(context.Background()).Err(); err != nil {
		_ = c.Close()
		t.Skipf("redis not available: %v", err)
	}
	c.FlushDB(context.Background())
	t.Cleanup(func() { c.FlushDB(context.Background()); c.Close() })
	return c
}

func TestCompat_EnqueueAndTaskHandler(t *testing.T) {
	js := New(newRedis(t), []string{"q0", "q1"})
	got := make(chan []byte, 1)
	if err := js.RegisterTaskHandler("email", func(ctx context.Context, p []byte) error {
		got <- p
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := js.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer js.Shutdown()

	if err := js.Enqueue(context.Background(), "email", []byte(`{"to":"x"}`)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	select {
	case p := <-got:
		if string(p) != `{"to":"x"}` {
			t.Errorf("payload = %s", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestCompat_ScheduledJobRunsPeriodically(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	var runs atomic.Int32
	if _, err := js.RegisterScheduledJob("tick", time.Second, func(ctx context.Context) error {
		runs.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := js.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer js.Shutdown()

	time.Sleep(3500 * time.Millisecond)
	if n := runs.Load(); n < 2 {
		t.Errorf("scheduled runs = %d, want >= 2", n)
	}
}

func TestCompat_EnqueueUniqueDedup(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	var runs atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	var once sync.Once
	if err := js.RegisterTaskHandler("dedup", func(ctx context.Context, p []byte) error {
		runs.Add(1)
		once.Do(wg.Done)
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := js.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer js.Shutdown()

	ctx := context.Background()
	// Two enqueues with the same unique TTL before processing → only one runs.
	if err := js.Enqueue(ctx, "dedup", []byte("x"), WithUniqueTTL(time.Minute)); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	if err := js.Enqueue(ctx, "dedup", []byte("x"), WithUniqueTTL(time.Minute)); err != nil {
		t.Fatalf("enqueue2 (dup) should return nil: %v", err)
	}
	wg.Wait()
	time.Sleep(500 * time.Millisecond) // give any (wrongly) duplicated task time to run
	if n := runs.Load(); n != 1 {
		t.Errorf("runs = %d, want 1 (dedup)", n)
	}
}

func TestCompat_RegisterScheduledJob_RejectsSubSecond(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	if _, err := js.RegisterScheduledJob("x", 500*time.Millisecond, func(context.Context) error { return nil }); err == nil {
		t.Error("interval < 1s must be rejected")
	}
}

func TestCompat_DuplicateTaskNameRejected(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	if err := js.RegisterTaskHandler("dup", func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := js.RegisterTaskHandler("dup", func(context.Context, []byte) error { return nil }); err == nil {
		t.Error("duplicate task name must be rejected")
	}
}

func TestCompat_CronJob_AcceptsAndRejects(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	if _, err := js.RegisterCronJob("daily", "0 0 * * *", func(context.Context) error { return nil }); err != nil {
		t.Errorf("valid cron spec should be accepted: %v", err)
	}
	if _, err := js.RegisterCronJob("bad", "not a cron", func(context.Context) error { return nil }); err == nil {
		t.Error("invalid cron spec must be rejected")
	}
}
```

- [ ] **Step 2: 테스트 통과 확인**

Run: `go test ./examples/jobscheduler/ -race -v`
Expected: 6개 테스트 전부 PASS (SKIP 없음).

- [ ] **Step 3: 전체 검증 + 커밋**

Run: `make check`
Expected: gofmt/vet 클린, `go test ./... -race -p 1` 전 패키지 PASS.

```bash
git add examples/jobscheduler/jobscheduler_test.go
git commit -m "test: JobScheduler 어댑터 6개 동작 동등성 검증"
```

---

## 완료 기준 (v1 성공 기준 검증)

- [ ] `make check` 통과
- [ ] `[]byte` Enqueue → 등록된 TaskHandler가 payload 수신
- [ ] RegisterScheduledJob(interval) → 주기적으로 실행(분산 단일 실행)
- [ ] Enqueue + WithUniqueTTL → 중복 억제(정확히 1회)
- [ ] interval < 1s 거부, cron 스펙 검증, 중복 taskName 거부
- [ ] **결론:** chronos-go 공개 API만으로 operator-review의 `JobScheduler` 6개 동작을 모두 백킹 가능 → v1 성공 기준 충족

**다음 단계:** 이 어댑터가 통과하면 operator-review의 `job_scheduler.go`를 asynq에서 chronos-go 기반으로 실제 교체(별도 저장소 작업)할 수 있다. 이후 Prometheus 메트릭, heartbeat 마일스톤.
