# chronos-go heartbeat (lease + unique 락 갱신) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 그동안 누적된 마지막 신뢰성 한계 두 가지를 해결한다 — (1) `RecoverMinIdle`보다 오래 걸리는 in-flight 태스크가 recoverer에게 오회수되어 중복 실행되는 문제, (2) 처리 시간이 unique 락 TTL을 넘으면 uniqueness가 깨지는 문제. Server가 처리 중인 태스크를 추적하고, heartbeat 고루틴이 그 PEL 엔트리의 idle을 리셋하고 unique 락 TTL을 갱신한다.

**Architecture:** Server에 in-flight 레지스트리(taskID→{streamID, queue, uniqueKey})를 두고, 워커가 처리 시작 시 등록·종료 시 해제한다. `heartbeaterLoop`가 매 `HeartbeatInterval`마다: (a) 큐별로 in-flight streamID를 `XCLAIM group consumer 0 <ids> JUSTID`로 자기 자신에게 재클레임 → **idle 시간 리셋**(JUSTID라 delivery count 증가 없음), (b) in-flight unique 키를 `PEXPIRE`로 갱신. 처리 중인 태스크는 idle이 `RecoverMinIdle`을 넘지 않으므로 recoverer가 회수하지 않고, unique 락은 처리 내내 살아 있다. 워커 crash 시 heartbeat가 멈춰 idle이 자라고 락 TTL이 만료 → recoverer가 정상 회수(기존 동작).

**Tech Stack:** Go 1.26, `github.com/redis/go-redis/v9`(`XClaimJustID`, `PExpire`), 실제 Redis 하니스.

**설계 문서:** `docs/superpowers/specs/2026-07-05-multi-cluster-scheduler-design.md` (섹션 5 신뢰성; heartbeat는 M2에서 연기했던 부분).

**범위 밖:** retry/scheduled ZSET에서 **대기 중인**(비-in-flight) 태스크의 unique 락 갱신 — 이건 어떤 워커도 들고 있지 않으므로 heartbeat 대상이 아니다(그 구간은 여전히 TTL로 커버; unique 락 renewTTL을 넉넉히 잡아 완화). heartbeat는 "활성 처리 중" 태스크를 커버한다.

**확정된 실제 시그니처:**
- `server.go`: `ServerConfig{Queues,Concurrency,Logger,RetryDelayFunc,OnDeadLetter,Metrics,ForwardInterval,RecoverInterval,RecoverMinIdle,ArchivedRetention,MaxArchived,JanitorInterval}`. `Server{rdb,cfg,consumer,logger,mux,sem,wg,cancel}`. `Start`가 fetchLoop/forwarderLoop/recovererLoop/janitorLoop를 `wg.Add(1)`+go 기동. fetchLoop 워커 고루틴이 `s.process(ctx, qname, sid, m)` 호출(server.go:230-235). `NewServer`가 기본값 채움.
- `rdb.ConsumerGroup`, `base.StreamKey(qname)`. `base.TaskMessage.UniqueKey`.
- recoverer는 `XAUTOCLAIM min-idle=RecoverMinIdle`로 회수, 재시도 횟수는 msg.Retried(hash)로 추적(PEL delivery count 미사용) — 따라서 XCLAIM이 delivery count를 건드려도 무해(JUSTID는 어차피 증가 안 함).

---

## File Structure

| 파일 | 변경 |
|---|---|
| `internal/rdb/heartbeat.go` (신규) | `ExtendLease`(XCLAIM JUSTID), `RenewUnique`(PEXPIRE) |
| `server.go` (수정) | in-flight 레지스트리 + `heartbeaterLoop` + `HeartbeatInterval` config + fetchLoop 워커에 track/untrack + Start 기동 + 관련 doc 갱신 |
| 각 `*_test.go` | 신규 테스트 |

**의존 순서:** rdb → server → 통합/문서. 청크: A(rdb+server), B(통합+문서).

**테스트:** 실제 Redis. `make test-race`.

---

## Task 1: rdb — ExtendLease + RenewUnique

**Files:**
- Create: `internal/rdb/heartbeat.go`
- Test: `internal/rdb/heartbeat_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/heartbeat_test.go`:
```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

// pendingIdle returns the idle time (ms) of the single pending entry.
func pendingIdle(t *testing.T, client interface {
}, r *RDB, qname string) time.Duration {
	t.Helper()
	ext, err := r.Client().XPendingExt(context.Background(), &redisXPendingExtArgs{
		Stream: base.StreamKey(qname), Group: ConsumerGroup, Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("xpendingext: %v", err)
	}
	if len(ext) == 0 {
		t.Fatal("no pending entries")
	}
	return ext[0].Idle
}

func TestExtendLease_ResetsIdle(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.Enqueue(ctx, &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, streamID, err := r.Dequeue(ctx, "c1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	if err := r.ExtendLease(ctx, "default", "c1", []string{streamID}); err != nil {
		t.Fatalf("extend lease: %v", err)
	}
	// After extend, idle should be reset near 0 (well under the 300ms slept).
	if idle := pendingIdle(t, client, r, "default"); idle > 100*time.Millisecond {
		t.Errorf("idle after ExtendLease = %v, want < 100ms (reset)", idle)
	}
}

func TestRenewUnique_ExtendsTTL_MissingIsNoOp(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	key := base.UniqueKey("default", "k:abc")
	if err := client.Set(ctx, key, "t1", 1*time.Second).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := r.RenewUnique(ctx, []string{key, base.UniqueKey("default", "missing")}, time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}
	ttl, _ := client.PTTL(ctx, key).Result()
	if ttl < 30*time.Second {
		t.Errorf("ttl after renew = %v, want ~1m", ttl)
	}
	// Missing key must not be recreated.
	if ex, _ := client.Exists(ctx, base.UniqueKey("default", "missing")).Result(); ex != 0 {
		t.Error("RenewUnique must not create a missing key")
	}
}
```

주의: 위 테스트의 `pendingIdle` 헬퍼가 참조하는 `redisXPendingExtArgs`는 실제로는 `redis.XPendingExtArgs`다. 헬퍼 시그니처가 지저분하므로 **구현 단계에서 헬퍼를 아래처럼 단순화해서 작성하라**(테스트 파일 상단 import에 `"github.com/redis/go-redis/v9"` 추가):
```go
func pendingIdle(t *testing.T, r *RDB, qname string) time.Duration {
	t.Helper()
	ext, err := r.Client().XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: base.StreamKey(qname), Group: ConsumerGroup, Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("xpendingext: %v", err)
	}
	if len(ext) == 0 {
		t.Fatal("no pending entries")
	}
	return ext[0].Idle
}
```
그리고 호출부를 `pendingIdle(t, r, "default")`로 맞춰라. (계획 상단의 지저분한 헬퍼 시그니처는 무시하고 이 단순화 버전을 쓴다.)

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestExtendLease_ResetsIdle|TestRenewUnique_' -v`
Expected: FAIL — `undefined: (*RDB).ExtendLease`, `RenewUnique`.

- [ ] **Step 3: heartbeat.go 구현**

Create `internal/rdb/heartbeat.go`:
```go
package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// ExtendLease resets the idle time of in-flight PEL entries by re-claiming them
// to the same consumer with min-idle 0 (XCLAIM ... JUSTID). This keeps a task
// that is genuinely being processed from being reclaimed by the recoverer while
// it runs. JUSTID means the delivery count is NOT incremented.
func (r *RDB) ExtendLease(ctx context.Context, qname, consumer string, streamIDs []string) error {
	if len(streamIDs) == 0 {
		return nil
	}
	return r.client.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream:   base.StreamKey(qname),
		Group:    ConsumerGroup,
		Consumer: consumer,
		MinIdle:  0,
		Messages: streamIDs,
	}).Err()
}

// RenewUnique extends the TTL of the given unique-lock keys (PEXPIRE ttl). A key
// that no longer exists (e.g. released at terminal state) is a no-op — PEXPIRE
// returns 0 and does not recreate it — so renewing a just-released lock is safe.
func (r *RDB) RenewUnique(ctx context.Context, uniqueKeys []string, ttl time.Duration) error {
	if len(uniqueKeys) == 0 {
		return nil
	}
	pipe := r.client.Pipeline()
	for _, k := range uniqueKeys {
		pipe.PExpire(ctx, k, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}
```

- [ ] **Step 4: 통과 확인 + 커밋**

Run: `go test ./internal/rdb/ -run 'TestExtendLease_ResetsIdle|TestRenewUnique_' -race -v`
Expected: PASS

```bash
git add internal/rdb/heartbeat.go internal/rdb/heartbeat_test.go
git commit -m "feat: rdb ExtendLease(XCLAIM JUSTID) + RenewUnique(PEXPIRE)"
```

---

## Task 2: server — in-flight 레지스트리 + heartbeaterLoop

**Files:**
- Modify: `server.go`
- Test: `server_heartbeat_test.go` (신규)

- [ ] **Step 1: 실패하는 테스트 작성**

Create `server_heartbeat_test.go`:
```go
package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

// A task that runs longer than RecoverMinIdle must NOT be reclaimed and
// re-executed by the recoverer — the heartbeat keeps its lease fresh, so it runs
// exactly once.
func TestServer_HeartbeatPreventsRecovererDoubleRun(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var runs atomic.Int32
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		if runs.Add(1) == 1 {
			time.Sleep(1200 * time.Millisecond) // > RecoverMinIdle below
			close(done)
		}
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:            map[string]int{"default": 1},
		Concurrency:       4,
		RecoverMinIdle:    400 * time.Millisecond, // would reclaim a >400ms task...
		RecoverInterval:   200 * time.Millisecond,
		HeartbeatInterval: 150 * time.Millisecond, // ...but heartbeat keeps it fresh
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish")
	}
	// Give the recoverer a couple more cycles to (wrongly) double-run if it would.
	time.Sleep(600 * time.Millisecond)
	if n := runs.Load(); n != 1 {
		t.Errorf("runs = %d, want 1 (heartbeat must prevent recoverer double-run)", n)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run TestServer_HeartbeatPreventsRecovererDoubleRun -v`
Expected: FAIL — `unknown field HeartbeatInterval` (그리고 heartbeat 없으면 runs==2로 실패할 것).

- [ ] **Step 3: ServerConfig + Server 필드 + 기본값**

`server.go`의 `ServerConfig`에 필드 추가(`RecoverMinIdle` 아래):
```go
	// HeartbeatInterval is how often the server refreshes the lease and unique
	// lock of in-flight tasks. Defaults to RecoverMinIdle/3. Must be shorter than
	// RecoverMinIdle so an actively-processing task is never reclaimed.
	HeartbeatInterval time.Duration
```

`Server` 구조체에 in-flight 레지스트리 추가:
```go
type Server struct {
	rdb      *rdb.RDB
	cfg      ServerConfig
	consumer string
	logger   *slog.Logger

	mux    *Mux
	sem    chan struct{}
	wg     sync.WaitGroup
	cancel context.CancelFunc

	inflightMu sync.Mutex
	inflight   map[string]inflightEntry
}

// inflightEntry is a task currently being processed by this server, tracked so
// the heartbeat can refresh its lease and unique lock.
type inflightEntry struct {
	streamID  string
	queue     string
	uniqueKey string
}
```

`NewServer`의 기본값 블록에 추가(RecoverMinIdle 기본값 설정 뒤 — 순서 중요):
```go
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.RecoverMinIdle / 3
	}
```
그리고 `NewServer`의 `return &Server{...}`에 `inflight: make(map[string]inflightEntry),` 추가.

- [ ] **Step 4: track/untrack + heartbeaterLoop + Start 기동**

`server.go`에 헬퍼 추가(process 근처):
```go
func (s *Server) trackInflight(id string, e inflightEntry) {
	s.inflightMu.Lock()
	s.inflight[id] = e
	s.inflightMu.Unlock()
}

func (s *Server) untrackInflight(id string) {
	s.inflightMu.Lock()
	delete(s.inflight, id)
	s.inflightMu.Unlock()
}

// heartbeaterLoop periodically refreshes the lease (PEL idle) and unique lock
// TTL of every in-flight task, so a long-running task is not reclaimed by the
// recoverer and does not lose its unique lock mid-processing.
func (s *Server) heartbeaterLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	// Renew unique locks well past the recover window so a crash (heartbeat stops)
	// lets the recoverer take over before the lock lapses.
	renewTTL := 2 * s.cfg.RecoverMinIdle
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.beat(ctx, renewTTL)
		}
	}
}

// beat refreshes all currently in-flight tasks.
func (s *Server) beat(ctx context.Context, renewTTL time.Duration) {
	s.inflightMu.Lock()
	byQueue := make(map[string][]string)
	var uniqueKeys []string
	for _, e := range s.inflight {
		byQueue[e.queue] = append(byQueue[e.queue], e.streamID)
		if e.uniqueKey != "" {
			uniqueKeys = append(uniqueKeys, e.uniqueKey)
		}
	}
	s.inflightMu.Unlock()

	for q, ids := range byQueue {
		if err := s.rdb.ExtendLease(ctx, q, s.consumer, ids); err != nil && ctx.Err() == nil {
			s.logger.Error("chronos: extend lease failed", "queue", q, "error", err)
		}
	}
	if len(uniqueKeys) > 0 {
		if err := s.rdb.RenewUnique(ctx, uniqueKeys, renewTTL); err != nil && ctx.Err() == nil {
			s.logger.Error("chronos: renew unique failed", "error", err)
		}
	}
}
```

`Start`의 recovererLoop 기동 다음에 heartbeater 기동 추가:
```go
	s.wg.Add(1)
	go s.heartbeaterLoop(runCtx)
```

- [ ] **Step 5: fetchLoop 워커에 track/untrack 연결**

`server.go`의 fetchLoop 워커 고루틴(현재 `s.process(ctx, qname, sid, m)` 호출)을 다음으로 교체:
```go
		s.wg.Add(1)
		go func(qname, sid string, m *base.TaskMessage) {
			defer s.wg.Done()
			defer func() { <-s.sem }()
			s.trackInflight(m.ID, inflightEntry{streamID: sid, queue: qname, uniqueKey: m.UniqueKey})
			defer s.untrackInflight(m.ID)
			s.process(ctx, qname, sid, m)
		}(msg.Queue, streamID, msg)
```

- [ ] **Step 6: 통과 확인 + 회귀 + 커밋**

Run: `go test . -run TestServer_HeartbeatPreventsRecovererDoubleRun -race -count=2 -v && go test ./... -race -p 1`
Expected: PASS (전 패키지). 특히 heartbeat 테스트가 runs==1로 통과(without heartbeat면 recoverer가 재실행해 2가 됨).

```bash
git add server.go server_heartbeat_test.go
git commit -m "feat: server heartbeat — in-flight lease/unique 갱신 (recoverer 오회수·unique TTL 상한 해결)"
```

---

## Task 3: 통합(unique 처리>TTL) + 문서 갱신 + 검증

**Files:**
- Modify: `delayed_unique_integration_test.go`(테스트 추가), `server.go`(doc 갱신)
- Test 포함

- [ ] **Step 1: unique 락이 처리>TTL에도 유지되는 통합 테스트 (heartbeat 효과 실증)**

`delayed_unique_integration_test.go`에 추가(M3에서 넉넉한 TTL로 약화했던 것을, 이제 **짧은 TTL + 긴 처리**로 강화):
```go
// With the heartbeat renewing the lock, a unique task whose processing runs
// LONGER than its unique TTL still blocks a duplicate enqueue — the very gap M3
// could not close without renewal.
func TestIntegration_HeartbeatKeepsUniqueLockBeyondTTL(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	release := make(chan struct{})
	var releaseOnce sync.Once
	doRelease := func() { releaseOnce.Do(func() { close(release) }) }
	started := make(chan struct{}, 1)

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[slowArgs]) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:            map[string]int{"default": 1},
		Concurrency:       4,
		RecoverInterval:   time.Hour, // keep recoverer out of the way
		RecoverMinIdle:    2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { doRelease(); srv.Shutdown(context.Background()) }()

	// Unique TTL is SHORT (1s); the handler will be held far longer than that.
	if _, err := Enqueue(ctx, c, slowArgs{ID: 1}, WithUnique(1*time.Second)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	// Wait well past the 1s TTL while the handler is still processing.
	time.Sleep(2500 * time.Millisecond)

	// Without renewal the lock would have expired; with the heartbeat it is still
	// held, so the duplicate is rejected.
	if _, err := Enqueue(ctx, c, slowArgs{ID: 1}, WithUnique(1*time.Second)); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("duplicate during long processing err = %v, want ErrDuplicateTask (heartbeat should hold the lock)", err)
	}
	doRelease()
}
```
(`slowArgs`/`errors`/`sync`는 이 파일에 이미 있음.)

- [ ] **Step 2: 통과 확인**

Run: `go test . -run TestIntegration_HeartbeatKeepsUniqueLockBeyondTTL -race -count=2 -v`
Expected: PASS — 1s TTL인데 2.5s 처리 후에도 중복 거부(heartbeat가 락 유지).

- [ ] **Step 3: 문서 갱신 — 이제 heartbeat가 완화하는 부분 반영**

`server.go`에서:

(a) `ServerConfig.OnDeadLetter` 주석의 "may fire more than once ... handler runs longer than RecoverMinIdle" 부분을 갱신 — heartbeat가 in-flight 태스크를 갱신하므로 정상 동작 중에는 이 이중 발화가 발생하지 않음. 다음 취지로 교체:
```go
	// OnDeadLetter is invoked when a task exhausts its retries (or returns a
	// SkipRetry error). It fires whether the task is archived or discarded.
	//
	// The heartbeat keeps an actively-processing task's lease fresh, so the
	// recoverer does not normally reclaim it. It may still fire more than once in
	// pathological cases (the server/heartbeat unavailable long enough for the
	// lease to lapse), so make the hook idempotent (the archived ZSET entry is
	// deduplicated by task ID).
	OnDeadLetter func(ctx context.Context, info *TaskInfo, err error)
```

(b) `ServerConfig.RecoverMinIdle` 주석 갱신 — heartbeat가 있으면 긴 핸들러도 안전:
```go
	// RecoverMinIdle is how long a PEL entry must be idle before it is treated as
	// abandoned. Defaults to 30s when unset (<= 0). The heartbeat refreshes the
	// lease of in-flight tasks every HeartbeatInterval, so a task that runs longer
	// than RecoverMinIdle is safe as long as this server (its heartbeat) is alive;
	// RecoverMinIdle is the window after a worker actually dies before its tasks
	// are reclaimed.
	RecoverMinIdle time.Duration
```

`chronos.go`의 `WithUnique` 주석 갱신 — 처리 중 갱신됨을 반영:
```go
// WithUnique deduplicates tasks by (kind + payload): while a matching task is
// anywhere in the pipeline (pending, retrying, scheduled), enqueueing another
// returns ErrDuplicateTask. The lock is released when the task reaches a
// terminal state (completed / archived / discarded).
//
// While a task is actively being processed, the server's heartbeat renews the
// lock's TTL, so a single attempt that runs longer than ttl still holds the
// lock. ttl mainly bounds the lock for the time a task spends waiting (pending /
// scheduled / retry backoff) where no worker is renewing it; set it comfortably
// above the expected total waiting time. For a delayed task the lock TTL is
// automatically extended to cover the delay.
func WithUnique(ttl time.Duration) Option {
```

- [ ] **Step 4: 전체 검증 + 커밋**

Run: `make check`
Expected: gofmt/vet 클린, `go test ./... -race -p 1` 전 패키지 PASS, contrib 빌드/테스트 PASS.

```bash
git add delayed_unique_integration_test.go server.go chronos.go
git commit -m "test/docs: heartbeat가 처리>TTL에도 unique 락 유지 실증 + 한계 문서 갱신"
```

---

## 완료 기준

- [ ] `make check`(메인 + contrib) 통과
- [ ] `RecoverMinIdle`보다 오래 걸리는 in-flight 태스크가 recoverer에게 오회수되지 않음(정확히 1회 실행)
- [ ] 처리 시간 > unique TTL이어도 heartbeat가 락을 갱신해 중복 enqueue 거부(M3에서 못 닫았던 갭 해소)
- [ ] 워커 crash 시엔 heartbeat가 멈춰 recoverer가 정상 회수(기존 신뢰성 유지)
- [ ] 관련 문서(OnDeadLetter/RecoverMinIdle/WithUnique)에서 heartbeat 완화 반영

**의의:** unique 락 dimension에서 asynq를 실질적으로 넘어선다(asynq도 lease heartbeat는 있으나 chronos-go는 lease + unique 둘 다 갱신). 이로써 M2/M3에서 정직하게 남겨둔 마지막 한계가 해소된다.

**다음 단계:** 남은 건 주로 OSS 출시 준비(README/LICENSE/CI/v1.0.0) 및 operator-review 실제 교체, 선택 기능(우선순위 큐/completed retention/Web UI).
