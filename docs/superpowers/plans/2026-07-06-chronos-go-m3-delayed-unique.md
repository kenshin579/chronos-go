# chronos-go M3 (지연 실행 + unique 락) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** M2 신뢰성 계층 위에 (1) 미래 시각 지연 실행(`WithProcessIn`/`WithProcessAt` → scheduled ZSET)과 (2) 중복 억제 unique 락(`WithUnique` → 처리 완료까지 유지되는 dedup 락)을 추가한다.

**Architecture:** 지연 태스크는 stream이 아니라 scheduled ZSET(score = process_at)에 넣고, forwarder가 retry ZSET과 동일하게 due된 것을 stream으로 승격한다(M2 `forwardCmd` 재사용). unique 락은 enqueue/schedule 시 `SET NX`로 원자 획득하고(중복이면 `ErrDuplicateTask`), **태스크가 최종 상태(완료/보관/폐기)에 도달할 때 해제**한다 — 재시도 중에도 유지되므로 "처리 시간 > 락 TTL"에도 uniqueness가 유지된다(asynq의 결함 개선). TTL은 프로세스가 최종 상태 도달 전에 죽는 경우의 고아 안전망일 뿐이다.

**Tech Stack:** Go 1.26, `github.com/redis/go-redis/v9`, `crypto/sha256`(unique 키 해시), 표준 `testing` + 실제 Redis 하니스.

**설계 문서:** `docs/superpowers/specs/2026-07-05-multi-cluster-scheduler-design.md` (섹션 4 API, 섹션 5 Unique 락 시맨틱)

**M3 범위 밖:**
- 리더 선출, cron/interval 스케줄러, 결정적 TaskID, misfire → M4
- Inspector, CLI, 메트릭 → M5
- unique 락의 처리 중 자동 TTL 연장(heartbeat 연동) → M2에서 연기된 heartbeat 마일스톤. M3는 "최종 상태 도달 시 해제 + 넉넉한 TTL 안전망"으로 uniqueness를 보장하되, 프로세스가 최종 상태 전에 죽으면 TTL 만료까지 락이 남을 수 있음(문서화).

**M2에서 확정된 실제 시그니처 (이 계획이 의존/확장하는 것):**
- `rdb.RDB.Enqueue(ctx, msg *base.TaskMessage) error` — 즉시 실행(stream XADD). M3는 그대로 두고 unique 변형을 추가.
- `rdb.RDB.Done(ctx, qname, streamID, taskID string) error` — XACK+DEL. **M3에서 msg를 받도록 시그니처 변경(unique 락 해제 위해).**
- `rdb.RDB.Retry/Archive(ctx, qname, streamID string, msg *base.TaskMessage, at time.Time) error` — moveToZSet 기반. **Archive에 unique 락 해제 추가(Retry는 유지).**
- `rdb.RDB.ForwardRetry(ctx, qname string, now time.Time, max int) (int, error)` — `forwardCmd`(KEYS[1]=source zset, KEYS[2]=stream)를 RetryKey로 호출. M3는 동일 스크립트를 ScheduledKey로 호출하는 `ForwardScheduled` 추가.
- `base.TaskMessage{ID, Kind, Payload, Queue, State, Retried, MaxRetry, NoArchive}` — M3에서 `UniqueKey string` 추가.
- `base.StreamKey/TaskKey/TaskKeyPrefix/RetryKey/ArchivedKey/QueueKeyPrefix/QueuesKey`, 상태 상수 `StateScheduled` 이미 정의됨.
- `chronos.go` `enqueueOptions{queue, taskID, maxRetry, noArchive}`, `Option`/`optionFunc`, `Enqueue[T]`, `DefaultQueue`/`DefaultMaxRetry`.
- `server.go` `forwarderLoop`(현재 ForwardRetry만 호출), `process`/`deadLetter`(Done/Archive 호출), `ackTimeout`.

---

## File Structure

| 파일 | M3 변경 |
|---|---|
| `internal/base/keys.go` | `ScheduledKey`, `UniqueKey` 추가 |
| `internal/base/task.go` | `TaskMessage`에 `UniqueKey` 필드 추가 |
| `internal/base/unique.go` (신규) | `UniqueSuffix(kind string, payload []byte) string` (kind + sha256(payload)) |
| `internal/rdb/schedule.go` (신규) | `Schedule` (scheduled ZSET) |
| `internal/rdb/forward.go` | `ForwardScheduled` 추가 (forwardCmd 재사용) |
| `internal/rdb/unique.go` (신규) | `EnqueueUnique`, `ScheduleUnique` (SET NX 원자 획득), `ErrDuplicateTask` |
| `internal/rdb/rdb.go` | `Done` 시그니처 변경(msg 수신) + unique 락 해제 |
| `internal/rdb/retry.go` | `Archive`에 unique 락 해제 추가(Retry는 유지) |
| `chronos.go` | `WithProcessIn`/`WithProcessAt`/`WithUnique` 옵션, `enqueueOptions` 확장, `Enqueue` 라우팅, `ErrDuplicateTask` 재노출 |
| `server.go` | `forwarderLoop`에 ForwardScheduled 추가, `process`/`deadLetter`의 Done 호출을 msg 전달로 변경 |
| 각 `*_test.go` | 신규 테스트 |

**의존 순서:** base → rdb(schedule/forward/unique/done-archive) → chronos options → server. 청크: A(지연 실행 base+rdb+options+server), B(unique base+rdb+done/archive+options), C(통합).

**테스트:** 실제 Redis. `make test-race`(= `go test ./... -race -p 1`). SKIP 없이 통과.

---

## Task 1: base — scheduled/unique 키 + UniqueSuffix + TaskMessage.UniqueKey

**Files:**
- Modify: `internal/base/keys.go`, `internal/base/task.go`
- Create: `internal/base/unique.go`
- Test: `internal/base/keys_test.go`, `internal/base/task_test.go`, `internal/base/unique_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

`internal/base/keys_test.go`에 추가:
```go
func TestScheduledAndUniqueKeys(t *testing.T) {
	if got := ScheduledKey("default"); got != "chronos:{default}:scheduled" {
		t.Errorf("ScheduledKey = %q", got)
	}
	if got := UniqueKey("default", "email:send:abc"); got != "chronos:{default}:unique:email:send:abc" {
		t.Errorf("UniqueKey = %q", got)
	}
}
```

Create `internal/base/unique_test.go`:
```go
package base

import "testing"

func TestUniqueSuffix_StableAndPayloadSensitive(t *testing.T) {
	a := UniqueSuffix("email:send", []byte(`{"to":"x"}`))
	b := UniqueSuffix("email:send", []byte(`{"to":"x"}`))
	c := UniqueSuffix("email:send", []byte(`{"to":"y"}`))

	if a != b {
		t.Errorf("same kind+payload must hash equal: %q vs %q", a, b)
	}
	if a == c {
		t.Error("different payload must hash differently")
	}
	// Suffix starts with the kind for readability/debuggability.
	if len(a) <= len("email:send:") || a[:len("email:send:")] != "email:send:" {
		t.Errorf("suffix should start with %q, got %q", "email:send:", a)
	}
}
```

`internal/base/task_test.go`에 추가:
```go
func TestTaskMessage_UniqueKey_RoundTrip(t *testing.T) {
	msg := &TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default",
		UniqueKey: "chronos:{default}:unique:k:deadbeef"}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.UniqueKey != msg.UniqueKey {
		t.Errorf("UniqueKey = %q, want %q", got.UniqueKey, msg.UniqueKey)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/base/ -run 'TestScheduledAndUniqueKeys|TestUniqueSuffix_StableAndPayloadSensitive|TestTaskMessage_UniqueKey_RoundTrip' -v`
Expected: FAIL — `undefined: ScheduledKey/UniqueKey/UniqueSuffix`, `unknown field UniqueKey`.

- [ ] **Step 3: 키 + UniqueSuffix + 필드 구현**

`internal/base/keys.go` 끝에 추가:
```go
// ScheduledKey returns the ZSET key holding delayed tasks (score = process_at).
func ScheduledKey(qname string) string {
	return QueueKeyPrefix(qname) + "scheduled"
}

// UniqueKey returns the STRING key holding the deduplication lock for a task.
// suffix is produced by UniqueSuffix. The queue hash tag keeps it in the same
// slot as the task's other keys.
func UniqueKey(qname, suffix string) string {
	return QueueKeyPrefix(qname) + "unique:" + suffix
}
```

Create `internal/base/unique.go`:
```go
package base

import (
	"crypto/sha256"
	"encoding/hex"
)

// UniqueSuffix derives a stable deduplication suffix from a task's kind and
// payload: "<kind>:<sha256(payload) hex>". Two enqueues with the same kind and
// payload produce the same suffix (and thus compete for the same unique lock).
func UniqueSuffix(kind string, payload []byte) string {
	sum := sha256.Sum256(payload)
	return kind + ":" + hex.EncodeToString(sum[:])
}
```

`internal/base/task.go`의 `TaskMessage`에 필드 추가(`NoArchive` 아래):
```go
	// UniqueKey is the full Redis key of this task's deduplication lock, or ""
	// if the task is not unique. It is released when the task reaches a terminal
	// state (completed / archived / discarded).
	UniqueKey string `json:"unique_key"`
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/base/ -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/base/
git commit -m "feat: base scheduled/unique 키 + UniqueSuffix + TaskMessage.UniqueKey"
```

---

## Task 2: rdb — Schedule + ForwardScheduled

**Files:**
- Create: `internal/rdb/schedule.go`
- Modify: `internal/rdb/forward.go`
- Test: `internal/rdb/schedule_test.go`, `internal/rdb/forward_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/schedule_test.go`:
```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestSchedule_StoresTaskInScheduledZSet(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	processAt := time.Now().Add(1 * time.Hour)
	if err := r.Schedule(ctx, msg, processAt); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	// In scheduled ZSET with the right score; NOT in the stream yet.
	score, err := client.ZScore(ctx, base.ScheduledKey("default"), "t1").Result()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	if int64(score) != processAt.Unix() {
		t.Errorf("score = %d, want %d", int64(score), processAt.Unix())
	}
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 0 {
		t.Errorf("stream len = %d, want 0 (not due yet)", slen)
	}
	// Task hash exists with scheduled state and queue registered.
	raw, err := client.HGet(ctx, base.TaskKey("default", "t1"), "msg").Result()
	if err != nil {
		t.Fatalf("hget: %v", err)
	}
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.State != base.StateScheduled {
		t.Errorf("state = %v, want scheduled", stored.State)
	}
	if m, _ := client.SIsMember(ctx, base.QueuesKey(), "default").Result(); !m {
		t.Error("queue not registered")
	}
	_ = redis.Nil
}

func TestForwardScheduled_MovesDueTasksToStream(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	due := &base.TaskMessage{ID: "due", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	future := &base.TaskMessage{ID: "future", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Schedule(ctx, due, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("schedule due: %v", err)
	}
	if err := r.Schedule(ctx, future, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule future: %v", err)
	}

	n, err := r.ForwardScheduled(ctx, "default", time.Now(), 100)
	if err != nil {
		t.Fatalf("forward scheduled: %v", err)
	}
	if n != 1 {
		t.Errorf("forwarded = %d, want 1", n)
	}
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 1 {
		t.Errorf("stream len = %d, want 1", slen)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), "future").Result(); err != nil {
		t.Error("future task should remain scheduled")
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestSchedule_StoresTaskInScheduledZSet|TestForwardScheduled_MovesDueTasksToStream' -v`
Expected: FAIL — `undefined: (*RDB).Schedule`, `(*RDB).ForwardScheduled`.

- [ ] **Step 3: Schedule 구현**

Create `internal/rdb/schedule.go`:
```go
package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// scheduleCmd stores a task body and adds it to the scheduled ZSET (state
// scheduled), from which the forwarder promotes it when its time arrives.
// KEYS[1] task hash, KEYS[2] scheduled zset.
// ARGV[1] encoded msg, ARGV[2] state, ARGV[3] score (process_at), ARGV[4] task id.
var scheduleCmd = redis.NewScript(`
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
return 1
`)

// Schedule stores a task for delayed execution at processAt.
func (r *RDB) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	msg.State = base.StateScheduled
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.client.SAdd(ctx, base.QueuesKey(), msg.Queue).Err(); err != nil {
		return err
	}
	keys := []string{base.TaskKey(msg.Queue, msg.ID), base.ScheduledKey(msg.Queue)}
	argv := []interface{}{encoded, int(base.StateScheduled), processAt.Unix(), msg.ID}
	return scheduleCmd.Run(ctx, r.client, keys, argv...).Err()
}
```

- [ ] **Step 4: ForwardScheduled 구현**

`internal/rdb/forward.go`의 `ForwardRetry` 아래에 추가:
```go
// ForwardScheduled promotes due delayed tasks (score <= now) from the scheduled
// ZSET into the stream. It shares forwardCmd with ForwardRetry.
func (r *RDB) ForwardScheduled(ctx context.Context, qname string, now time.Time, max int) (int, error) {
	keys := []string{base.ScheduledKey(qname), base.StreamKey(qname)}
	argv := []interface{}{now.Unix(), max, base.TaskKeyPrefix(qname), int(base.StatePending)}
	n, err := forwardCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `go test ./internal/rdb/ -run 'TestSchedule_StoresTaskInScheduledZSet|TestForwardScheduled_MovesDueTasksToStream' -v`
Expected: PASS

- [ ] **Step 6: 커밋**

```bash
git add internal/rdb/schedule.go internal/rdb/schedule_test.go internal/rdb/forward.go
git commit -m "feat: rdb Schedule(지연) + ForwardScheduled(forwardCmd 재사용)"
```

---

## Task 3: chronos — WithProcessIn/WithProcessAt + Enqueue 라우팅

**Files:**
- Modify: `chronos.go`
- Test: `chronos_test.go` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`chronos_test.go`에 추가:
```go
func TestEnqueue_WithProcessIn_GoesToScheduledZSet(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(1*time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Lands in the scheduled ZSET, not the stream.
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), info.ID).Result(); err != nil {
		t.Errorf("task not in scheduled zset: %v", err)
	}
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 0 {
		t.Errorf("stream len = %d, want 0", slen)
	}
}

func TestEnqueue_WithProcessInPast_GoesToStreamImmediately(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(-1*time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// A non-future time is treated as immediate.
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 1 {
		t.Errorf("stream len = %d, want 1", slen)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), info.ID).Result(); err == nil {
		t.Error("immediate task should not be in scheduled zset")
	}
}
```

`chronos_test.go` import에 `time`과 `base`가 없으면 추가하라(기존 테스트에서 이미 쓰고 있으면 재사용).

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run 'TestEnqueue_WithProcessIn' -v`
Expected: FAIL — `undefined: WithProcessIn`.

- [ ] **Step 3: 옵션 + 라우팅 구현**

`chronos.go`의 `enqueueOptions`에 필드 추가:
```go
type enqueueOptions struct {
	queue     string
	taskID    string
	maxRetry  int
	noArchive bool
	processAt time.Time // zero = immediate
}
```

`WithDeadLetterDiscard` 아래에 옵션 추가:
```go
// WithProcessAt schedules the task to first become available at t. A non-future
// time enqueues immediately.
func WithProcessAt(t time.Time) Option {
	return optionFunc(func(o *enqueueOptions) { o.processAt = t })
}

// WithProcessIn schedules the task to first become available after d. A
// non-positive d enqueues immediately.
func WithProcessIn(d time.Duration) Option {
	return optionFunc(func(o *enqueueOptions) { o.processAt = time.Now().Add(d) })
}
```

`chronos.go` import 블록에 `"time"`을 추가하라(알파벳 순: context, time).

`Enqueue`의 `msg` 생성 이후 rdb 호출 부분을 교체:
```go
	msg := &base.TaskMessage{
		ID:        id,
		Kind:      args.Kind(),
		Payload:   payload,
		Queue:     options.queue,
		MaxRetry:  options.maxRetry,
		NoArchive: options.noArchive,
	}

	if !options.processAt.IsZero() && options.processAt.After(time.Now()) {
		if err := c.rdb.Schedule(ctx, msg, options.processAt); err != nil {
			return nil, err
		}
	} else if err := c.rdb.Enqueue(ctx, msg); err != nil {
		return nil, err
	}

	return &TaskInfo{ID: id, Kind: msg.Kind, Queue: msg.Queue}, nil
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test . -run 'TestEnqueue_WithProcessIn' -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add chronos.go chronos_test.go
git commit -m "feat: WithProcessIn/WithProcessAt + Enqueue 지연 라우팅"
```

---

## Task 4: server — forwarderLoop이 scheduled도 승격 (지연 실행 e2e)

**Files:**
- Modify: `server.go` (forwarderLoop)
- Test: `server_delayed_test.go` (신규)

- [ ] **Step 1: 실패하는 테스트 작성**

Create `server_delayed_test.go`:
```go
package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_DelayedTaskRunsAfterDelay(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var ranAt atomic.Int64
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		ranAt.Store(time.Now().UnixMilli())
		close(done)
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 50 * time.Millisecond,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	enqueuedAt := time.Now()
	const delay = 700 * time.Millisecond
	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(delay)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("delayed task did not run")
	}

	// It must not have run appreciably before its delay elapsed.
	elapsed := time.Duration(ranAt.Load()-enqueuedAt.UnixMilli()) * time.Millisecond
	if elapsed < delay-150*time.Millisecond {
		t.Errorf("task ran after %v, want >= ~%v", elapsed, delay)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run TestServer_DelayedTaskRunsAfterDelay -v`
Expected: FAIL — 태스크가 scheduled ZSET에만 있고 forwarder가 scheduled를 승격하지 않아 10s 타임아웃.

- [ ] **Step 3: forwarderLoop에 ForwardScheduled 추가**

`server.go`의 `forwarderLoop` 내부, `ForwardRetry` 호출이 있는 for 루프 본문을 다음으로 교체(retry와 scheduled 둘 다 승격):
```go
			for _, q := range queues {
				if _, err := s.rdb.ForwardRetry(ctx, q, time.Now(), forwardBatchSize); err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: forward retry failed", "queue", q, "error", err)
				}
				if _, err := s.rdb.ForwardScheduled(ctx, q, time.Now(), forwardBatchSize); err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: forward scheduled failed", "queue", q, "error", err)
				}
			}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test . -run TestServer_DelayedTaskRunsAfterDelay -race -v`
Expected: PASS

- [ ] **Step 5: 회귀 + 커밋**

Run: `go test ./... -race -p 1`
Expected: 전 패키지 PASS.

```bash
git add server.go server_delayed_test.go
git commit -m "feat: forwarderLoop이 scheduled ZSET도 승격 (지연 실행 완성)"
```

---

## Task 5: rdb — EnqueueUnique / ScheduleUnique (SET NX 원자 획득)

**Files:**
- Create: `internal/rdb/unique.go`
- Test: `internal/rdb/unique_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/unique_test.go`:
```go
package rdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func uniqueMsg(id, queue string) *base.TaskMessage {
	m := &base.TaskMessage{ID: id, Kind: "k", Payload: []byte(`{"a":1}`), Queue: queue}
	m.UniqueKey = base.UniqueKey(queue, base.UniqueSuffix(m.Kind, m.Payload))
	return m
}

func TestEnqueueUnique_SecondIsRejected(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	first := uniqueMsg("t1", "default")
	if err := r.EnqueueUnique(ctx, first, time.Minute); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	// Same kind+payload → same unique key → rejected while the lock is held.
	second := uniqueMsg("t2", "default")
	err := r.EnqueueUnique(ctx, second, time.Minute)
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second enqueue err = %v, want ErrDuplicateTask", err)
	}

	// Only the first task is in the stream; the lock stores the first task's ID.
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 1 {
		t.Errorf("stream len = %d, want 1", slen)
	}
	val, _ := client.Get(ctx, first.UniqueKey).Result()
	if val != "t1" {
		t.Errorf("unique lock value = %q, want t1", val)
	}
}

func TestScheduleUnique_SecondIsRejected(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	first := uniqueMsg("t1", "default")
	if err := r.ScheduleUnique(ctx, first, time.Now().Add(time.Hour), time.Minute); err != nil {
		t.Fatalf("first schedule: %v", err)
	}
	second := uniqueMsg("t2", "default")
	if err := r.ScheduleUnique(ctx, second, time.Now().Add(time.Hour), time.Minute); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second schedule err = %v, want ErrDuplicateTask", err)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), "t1").Result(); err != nil {
		t.Errorf("first task not scheduled: %v", err)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestEnqueueUnique_SecondIsRejected|TestScheduleUnique_SecondIsRejected' -v`
Expected: FAIL — `undefined: (*RDB).EnqueueUnique`, `ScheduleUnique`, `ErrDuplicateTask`.

- [ ] **Step 3: EnqueueUnique/ScheduleUnique 구현**

Create `internal/rdb/unique.go`:
```go
package rdb

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// ErrDuplicateTask is returned when a unique lock is already held for an
// identical (kind + payload) task.
var ErrDuplicateTask = errors.New("chronos: duplicate task")

// enqueueUniqueCmd acquires the unique lock (SET NX PX) and, only on success,
// stores the task and appends it to the stream — atomically. Returns -1 when
// the lock is already held. TTL is milliseconds (PX) so sub-second TTLs work.
// KEYS[1] unique lock, KEYS[2] task hash, KEYS[3] stream.
// ARGV[1] taskID, ARGV[2] ttl millis, ARGV[3] encoded msg, ARGV[4] state.
var enqueueUniqueCmd = redis.NewScript(`
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", tonumber(ARGV[2])) == false then
  return -1
end
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("XADD", KEYS[3], "*", "task_id", ARGV[1])
return 1
`)

// scheduleUniqueCmd is enqueueUniqueCmd's delayed counterpart: on lock success
// it stores the task and adds it to the scheduled ZSET.
// KEYS[1] unique lock, KEYS[2] task hash, KEYS[3] scheduled zset.
// ARGV[1] taskID, ARGV[2] ttl millis, ARGV[3] encoded msg, ARGV[4] state, ARGV[5] score.
var scheduleUniqueCmd = redis.NewScript(`
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", tonumber(ARGV[2])) == false then
  return -1
end
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[1])
return 1
`)

// uniqueTTLMillis converts a TTL duration to the millisecond value passed to the
// SET PX scripts, clamping to at least 1ms (SET PX 0 is an error).
func uniqueTTLMillis(ttl time.Duration) int {
	ms := ttl.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return int(ms)
}

// EnqueueUnique enqueues a task for immediate processing, acquiring its unique
// lock first. Returns ErrDuplicateTask if an identical task's lock is held. msg
// must have UniqueKey set (see base.UniqueKey/UniqueSuffix). uniqueTTL is the
// lock's orphan-safety expiry; the lock is released early when the task reaches
// a terminal state.
func (r *RDB) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, uniqueTTL time.Duration) error {
	msg.State = base.StatePending
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.client.SAdd(ctx, base.QueuesKey(), msg.Queue).Err(); err != nil {
		return err
	}
	keys := []string{msg.UniqueKey, base.TaskKey(msg.Queue, msg.ID), base.StreamKey(msg.Queue)}
	argv := []interface{}{msg.ID, uniqueTTLMillis(uniqueTTL), encoded, int(base.StatePending)}
	res, err := enqueueUniqueCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return err
	}
	if res == -1 {
		return ErrDuplicateTask
	}
	return nil
}

// ScheduleUnique is EnqueueUnique for delayed tasks (adds to the scheduled ZSET
// at processAt instead of the stream).
func (r *RDB) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, uniqueTTL time.Duration) error {
	msg.State = base.StateScheduled
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.client.SAdd(ctx, base.QueuesKey(), msg.Queue).Err(); err != nil {
		return err
	}
	keys := []string{msg.UniqueKey, base.TaskKey(msg.Queue, msg.ID), base.ScheduledKey(msg.Queue)}
	argv := []interface{}{msg.ID, uniqueTTLMillis(uniqueTTL), encoded, int(base.StateScheduled), processAt.Unix()}
	res, err := scheduleUniqueCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return err
	}
	if res == -1 {
		return ErrDuplicateTask
	}
	return nil
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/rdb/ -run 'TestEnqueueUnique_SecondIsRejected|TestScheduleUnique_SecondIsRejected' -v`
Expected: PASS

주의: go-redis에서 `SET ... NX`가 실패하면 Lua의 `redis.call("SET", ...)`가 `false`를 반환한다(Redis Lua 관례). 위 스크립트의 `== false` 비교가 이에 의존한다. 만약 실제 동작이 다르면(예: nil 반환) 테스트가 드러내며, 그 경우 `if not redis.call(...)` 형태로 수정하라.

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/unique.go internal/rdb/unique_test.go
git commit -m "feat: rdb EnqueueUnique/ScheduleUnique (SET NX 원자 dedup) + ErrDuplicateTask"
```

---

## Task 6: rdb — 최종 상태에서 unique 락 해제 (Done 시그니처 변경 + Archive)

**Files:**
- Modify: `internal/rdb/rdb.go` (Done)
- Modify: `internal/rdb/retry.go` (Archive는 해제, Retry는 유지)
- Test: `internal/rdb/unique_test.go` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`internal/rdb/unique_test.go`에 추가:
```go
func TestDone_ReleasesUniqueLock(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := uniqueMsg("t1", "default")
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.EnqueueUnique(ctx, msg, time.Minute); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, streamID, err := r.Dequeue(ctx, "c1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	if err := r.Done(ctx, "default", streamID, got); err != nil {
		t.Fatalf("done: %v", err)
	}
	// Lock released → a new identical task can be enqueued.
	if exists, _ := client.Exists(ctx, msg.UniqueKey).Result(); exists != 0 {
		t.Error("unique lock should be released after Done")
	}
}

func TestRetry_KeepsUniqueLock_ArchiveReleases(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// Retry keeps the lock (task still in flight).
	msg := uniqueMsg("t1", "default")
	msg.MaxRetry = 5
	if err := r.EnqueueUnique(ctx, msg, time.Minute); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, streamID, _ := r.Dequeue(ctx, "c1", 0, "default")
	got.Retried = 1
	if err := r.Retry(ctx, "default", streamID, got, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if exists, _ := client.Exists(ctx, msg.UniqueKey).Result(); exists != 1 {
		t.Error("unique lock must be kept across a retry")
	}

	// Archive releases the lock (terminal).
	// Bring it back to the stream and dequeue to get a fresh streamID.
	if _, err := r.ForwardRetry(ctx, "default", time.Now().Add(2*time.Hour), 10); err != nil {
		t.Fatalf("forward: %v", err)
	}
	got2, streamID2, err := r.Dequeue(ctx, "c1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if err := r.Archive(ctx, "default", streamID2, got2, time.Now()); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if exists, _ := client.Exists(ctx, msg.UniqueKey).Result(); exists != 0 {
		t.Error("unique lock should be released after Archive")
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestDone_ReleasesUniqueLock|TestRetry_KeepsUniqueLock_ArchiveReleases' -v`
Expected: FAIL — 컴파일 에러(`Done`이 아직 `taskID string`을 받음) 및 락 미해제.

- [ ] **Step 3: Done 시그니처 변경 + unique 해제**

`internal/rdb/rdb.go`의 `Done`을 다음으로 교체:
```go
// releaseUniqueCmd deletes the unique lock only if it still points at this task
// (so a lock re-acquired by a later task is not clobbered).
// KEYS[1] unique key. ARGV[1] taskID.
var releaseUniqueCmd = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("DEL", KEYS[1])
end
return 1
`)

// Done acknowledges a successfully processed task: it acks the stream entry,
// deletes the task body, and releases the task's unique lock (if any). It takes
// the full message so it can find the unique key.
func (r *RDB) Done(ctx context.Context, qname, streamID string, msg *base.TaskMessage) error {
	pipe := r.client.TxPipeline()
	pipe.XAck(ctx, base.StreamKey(qname), ConsumerGroup, streamID)
	pipe.Del(ctx, base.TaskKey(qname, msg.ID))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return r.releaseUnique(ctx, msg)
}

// releaseUnique releases the task's unique lock if it holds one.
func (r *RDB) releaseUnique(ctx context.Context, msg *base.TaskMessage) error {
	if msg.UniqueKey == "" {
		return nil
	}
	return releaseUniqueCmd.Run(ctx, r.client, []string{msg.UniqueKey}, msg.ID).Err()
}
```

- [ ] **Step 4: Archive에 unique 해제 추가**

`internal/rdb/retry.go`의 `Archive`를 다음으로 교체(Retry는 그대로 두어 락 유지):
```go
// Archive acks the active stream entry and moves the task to the archived ZSET
// (dead-letter) with score = diedAt. Archiving is terminal, so the task's
// unique lock (if any) is released.
func (r *RDB) Archive(ctx context.Context, qname, streamID string, msg *base.TaskMessage, diedAt time.Time) error {
	if err := r.moveToZSet(ctx, qname, streamID, msg, base.ArchivedKey(qname), base.StateArchived, diedAt.Unix()); err != nil {
		return err
	}
	return r.releaseUnique(ctx, msg)
}
```

- [ ] **Step 5: 테스트 통과 확인 + Done 호출부 수정**

`Done`의 시그니처가 바뀌었으므로 호출부(`server.go`)를 고쳐야 컴파일된다. `server.go`에서 `s.rdb.Done(...)` 호출 2곳을 `msg`를 넘기도록 수정:
- `process`의 성공 경로: `s.rdb.Done(opCtx, qname, streamID, msg)` (기존 `msg.ID` → `msg`)
- `deadLetter`의 `NoArchive` 경로: `s.rdb.Done(ctx, qname, streamID, msg)` (기존 `msg.ID` → `msg`)

Run: `go test ./internal/rdb/ -run 'TestDone_ReleasesUniqueLock|TestRetry_KeepsUniqueLock_ArchiveReleases' -v && go build ./...`
Expected: PASS, 빌드 성공.

- [ ] **Step 6: 회귀 + 커밋**

Run: `go test ./... -race -p 1`
Expected: 전 패키지 PASS(기존 Done 관련 테스트 포함 — Done 호출부가 msg를 넘기도록 갱신됨).

```bash
git add internal/rdb/rdb.go internal/rdb/retry.go internal/rdb/unique_test.go server.go
git commit -m "feat: 최종 상태에서 unique 락 해제 (Done msg 수신 + Archive 해제, Retry 유지)"
```

---

## Task 7: chronos — WithUnique + Enqueue unique 라우팅

**Files:**
- Modify: `chronos.go`
- Test: `chronos_test.go` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`chronos_test.go`에 추가:
```go
func TestEnqueue_WithUnique_RejectsDuplicate(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithUnique(time.Minute)); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	// Identical args → duplicate.
	_, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithUnique(time.Minute))
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second enqueue err = %v, want ErrDuplicateTask", err)
	}
	// Different args → allowed.
	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u2"}, WithUnique(time.Minute)); err != nil {
		t.Fatalf("different-args enqueue: %v", err)
	}
}

func TestEnqueue_WithUniqueAndProcessIn_Schedules(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithUnique(time.Minute), WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), info.ID).Result(); err != nil {
		t.Errorf("unique+delayed task should be scheduled: %v", err)
	}
}
```

`chronos_test.go` import에 `errors`가 없으면 추가하라.

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run 'TestEnqueue_WithUnique' -v`
Expected: FAIL — `undefined: WithUnique`, `ErrDuplicateTask`.

- [ ] **Step 3: WithUnique + ErrDuplicateTask 재노출 + 라우팅**

`chronos.go`의 import 블록에 아직 없다면 `"github.com/kenshin579/chronos-go/internal/rdb"`가 이미 있으니 재사용한다. 파일 상단(상수/타입 근처)에 재노출을 추가:
```go
// ErrDuplicateTask is returned by Enqueue when WithUnique is used and an
// identical task already holds the unique lock.
var ErrDuplicateTask = rdb.ErrDuplicateTask
```

`enqueueOptions`에 필드 추가:
```go
	uniqueTTL time.Duration // > 0 enables unique deduplication
```

옵션 추가(`WithProcessIn` 아래):
```go
// WithUnique deduplicates tasks by (kind + payload) for the lifetime of the
// task: while a matching task is anywhere in the pipeline (pending, retrying,
// scheduled), enqueueing another returns ErrDuplicateTask. ttl is an
// orphan-safety expiry used if the owning process dies before the task reaches
// a terminal state; it does not cap how long the lock is genuinely held.
func WithUnique(ttl time.Duration) Option {
	return optionFunc(func(o *enqueueOptions) {
		if ttl > 0 {
			o.uniqueTTL = ttl
		}
	})
}
```

`Enqueue`의 라우팅 블록(Task 3에서 만든 부분)을 unique까지 고려하도록 교체:
```go
	msg := &base.TaskMessage{
		ID:        id,
		Kind:      args.Kind(),
		Payload:   payload,
		Queue:     options.queue,
		MaxRetry:  options.maxRetry,
		NoArchive: options.noArchive,
	}

	scheduled := !options.processAt.IsZero() && options.processAt.After(time.Now())
	unique := options.uniqueTTL > 0
	if unique {
		msg.UniqueKey = base.UniqueKey(options.queue, base.UniqueSuffix(msg.Kind, payload))
	}

	var err2 error
	switch {
	case unique && scheduled:
		err2 = c.rdb.ScheduleUnique(ctx, msg, options.processAt, options.uniqueTTL)
	case unique:
		err2 = c.rdb.EnqueueUnique(ctx, msg, options.uniqueTTL)
	case scheduled:
		err2 = c.rdb.Schedule(ctx, msg, options.processAt)
	default:
		err2 = c.rdb.Enqueue(ctx, msg)
	}
	if err2 != nil {
		return nil, err2
	}

	return &TaskInfo{ID: id, Kind: msg.Kind, Queue: msg.Queue}, nil
```
(기존 `if !options.processAt.IsZero() ... else if ... Enqueue` 블록과 그 뒤 `return`을 위 블록으로 대체한다.)

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test . -run 'TestEnqueue_WithUnique' -race -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add chronos.go chronos_test.go
git commit -m "feat: WithUnique 옵션 + Enqueue unique/scheduled 라우팅 + ErrDuplicateTask 재노출"
```

---

## Task 8: 통합 — unique 락 생애 + 지연 실행 + 전체 검증

**Files:**
- Create: `delayed_unique_integration_test.go`

- [ ] **Step 1: unique 생애 통합 테스트 작성**

Create `delayed_unique_integration_test.go`:
```go
package chronos

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

type slowArgs struct {
	ID int `json:"id"`
}

func (slowArgs) Kind() string { return "test:slow" }

func mustPayload(t *testing.T, args slowArgs) []byte {
	t.Helper()
	b, err := encodeArgs(args)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

// While a unique task is being processed (longer than the lock TTL), a second
// identical enqueue is still rejected — uniqueness spans the whole lifetime,
// not just the TTL. After completion the lock frees.
func TestIntegration_UniqueLockSpansProcessingBeyondTTL(t *testing.T) {
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
		<-release // block until the test lets it finish
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     4,
		ForwardInterval: 50 * time.Millisecond,
		// Keep the recoverer away so it doesn't reclaim the deliberately-slow task.
		RecoverInterval: time.Hour,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { doRelease(); srv.Shutdown(context.Background()) }()

	// TTL deliberately short (1s); the handler is held longer than that.
	if _, err := Enqueue(ctx, c, slowArgs{ID: 1}, WithUnique(1*time.Second)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	// Wait past the TTL while the handler is still blocked.
	time.Sleep(1500 * time.Millisecond)

	// A second identical enqueue must still be rejected (lock held through processing).
	if _, err := Enqueue(ctx, c, slowArgs{ID: 1}, WithUnique(1*time.Second)); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second enqueue during processing err = %v, want ErrDuplicateTask", err)
	}

	// Let the handler finish; the lock is released on completion.
	doRelease()
	uniqueKey := base.UniqueKey("default", base.UniqueSuffix(slowArgs{}.Kind(), mustPayload(t, slowArgs{ID: 1})))
	eventually(t, 5*time.Second, func() bool {
		return client.Exists(ctx, uniqueKey).Val() == 0
	}, "unique lock should be released after the task completes")
}
```

Run: `go test . -run TestIntegration_UniqueLockSpansProcessingBeyondTTL -race -v`
Expected: PASS

- [ ] **Step 2: 지연 실행이 정확히 1회 실행됨을 확인 (추가 통합 테스트)**

`delayed_unique_integration_test.go`에 추가:
```go
func TestIntegration_DelayedTaskExecutesOnce(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var runs atomic.Int32
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[slowArgs]) error {
		if runs.Add(1) == 1 {
			close(done)
		}
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 50 * time.Millisecond,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, slowArgs{ID: 7}, WithProcessIn(300*time.Millisecond)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("delayed task did not run")
	}
	// Give it a moment to ensure it does not run twice (forwarder idempotency).
	time.Sleep(500 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Errorf("runs = %d, want 1", got)
	}
}
```

- [ ] **Step 3: 전체 스위트 + 정적 검증**

Run: `make check`
Expected: `gofmt` 클린, `go vet` 클린, `go test ./... -race -p 1` 전 패키지 PASS. 추가로 `go test . -run 'TestIntegration_' -race -count=2`로 안정성 확인.

- [ ] **Step 4: 커밋**

```bash
git add delayed_unique_integration_test.go
git commit -m "test: unique 락 생애(처리>TTL) + 지연 실행 통합 테스트"
```

---

## M3 완료 기준

- [ ] `make check` 전부 통과 (실 Redis 연결 시)
- [ ] `WithProcessIn`/`WithProcessAt` → scheduled ZSET → forwarder가 due 시 스트림 승격 → 지연 후 실행
- [ ] `WithUnique(ttl)` → 동일 (kind+payload) 중복 enqueue는 `ErrDuplicateTask`
- [ ] unique 락이 **처리 시간 > TTL**에도 유지됨(생애 전체), 최종 상태(완료/보관/폐기)에서 해제
- [ ] 재시도 중에는 락 유지, Archive/Done/discard에서 해제
- [ ] 지연 태스크가 정확히 1회 실행(forwarder 멱등)

**다음 단계:** M3 착지 후 M4(리더 선출 + cron/interval 스케줄러 + 결정적 TaskID + misfire) 계획을 실제 타입 기반으로 작성한다. M4의 스케줄러는 due 시 이 계획의 `Enqueue`/`EnqueueUnique`(결정적 TaskID로 dedup)를 호출하는 형태가 자연스럽다.
