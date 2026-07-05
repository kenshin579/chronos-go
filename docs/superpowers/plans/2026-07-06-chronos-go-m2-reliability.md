# chronos-go M2 (신뢰성: 재시도 + 크래시 복구 + dead-letter) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** M1 코어 큐 위에 신뢰성 계층을 얹는다 — 실패한 태스크는 지수 백오프로 재시도되고, 워커가 죽어 PEL에 갇힌 태스크는 recoverer가 회수하며, 재시도가 소진되면 dead-letter(archived)로 보관되고 `OnDeadLetter` 훅이 발화한다.

**Architecture:** 핸들러 에러/panic을 분류해 retry ZSET(지수 백오프+jitter)로 보내고, forwarder가 due된 재시도를 스트림으로 되돌린다. recoverer는 `XAUTOCLAIM`으로 idle 초과 PEL 엔트리를 회수해 재시도/보관한다. 재시도 예산(`Retried`/`MaxRetry`)이 소진되면 archived ZSET에 보관(또는 discard)하고 훅을 발화한다. crash 루프(poison pill)는 `Retried` 증가로 dead-letter에 수렴한다.

**Tech Stack:** Go 1.26, `github.com/redis/go-redis/v9`, 표준 `testing` + 실제 Redis 하니스(`internal/testutil`). Redis Streams `XAUTOCLAIM`(Redis 6.2+) 사용.

**설계 문서:** `docs/superpowers/specs/2026-07-05-multi-cluster-scheduler-design.md` (섹션 5: 에러 처리·신뢰성 시맨틱)

**M2 범위 밖 (후속 마일스톤):**
- 지연 실행(scheduled ZSET), `WithProcessIn`, unique 락 → M3
- 리더 선출, cron/interval 스케줄러, 결정적 TaskID, misfire → M4
- Inspector, CLI, 메트릭 → M5
- **heartbeat 기반 lease 연장은 M2에 포함하지 않는다.** 따라서 `RecoverMinIdle`보다 오래 걸리는 핸들러는 recoverer가 중복 실행할 수 있다(at-least-once). 이 한계는 문서화하고, `RecoverMinIdle` 기본값을 넉넉히(30s) 둔다. lease 연장은 후속 개선.

**M1에서 확정된 실제 시그니처 (이 계획이 의존하는 것):**
- `rdb.RDB.Dequeue(ctx, consumer string, block time.Duration, qname string) (*base.TaskMessage, string, error)` — 단일 큐, streamID 반환
- `rdb.RDB.Done(ctx, qname, streamID, taskID string) error` — XACK + DEL
- `rdb.RDB.EnsureGroup`, `rdb.ConsumerGroup = "chronos"`, `rdb.ErrNoTask`
- `base.TaskMessage{ID, Kind, Payload, Queue, State}` (M2에서 `Retried, MaxRetry, NoArchive` 추가)
- `base.StreamKey/TaskKey/QueueKeyPrefix/QueuesKey`, 상태 상수 `StateRetry`/`StateArchived` 이미 정의됨
- `server.go`의 `process(ctx, qname, streamID, msg)` — ack에 `context.WithoutCancel(ctx)` 사용, `Start`가 `fetchLoop` 하나만 기동
- `chronos.go`의 `enqueueOptions{queue, taskID}`, `Option`/`optionFunc`, `Enqueue[T]`
- `ServerConfig{Queues, Concurrency, Logger}`

---

## File Structure

| 파일 | M2 변경 |
|---|---|
| `internal/base/keys.go` | `RetryKey`, `ArchivedKey`, `TaskKeyPrefix` 추가 |
| `internal/base/task.go` | `TaskMessage`에 `Retried`, `MaxRetry`, `NoArchive` 필드 추가 |
| `internal/rdb/retry.go` (신규) | `Retry`, `Archive` (active→ZSET 이동, Lua) |
| `internal/rdb/forward.go` (신규) | `ForwardRetry` (retry ZSET→stream, Lua) |
| `internal/rdb/recover.go` (신규) | `Recover` (XAUTOCLAIM→retry/archive) |
| `chronos.go` | `WithMaxRetry`, `WithDeadLetterDiscard` 옵션; `enqueueOptions` 확장 |
| `retry.go` (신규, 루트) | `SkipRetry`/`asSkipRetry`, `DefaultRetryDelay`, `RetryDelayFunc` 타입 |
| `server.go` | `ServerConfig` 확장, `process` 재작성(분류/panic/retry/archive/훅), `forwarderLoop`/`recovererLoop` 기동 |
| 각 `*_test.go` | 신규 테스트 |

**의존 순서:** base → rdb(retry/forward/recover) → chronos options/retry.go → server. 마지막 두 태스크는 통합 시나리오.

**테스트:** 실제 Redis. 카노니컬 명령 `make test-race`(= `go test ./... -race -p 1`). 새 테스트도 SKIP 없이 통과해야 한다.

---

## Task 1: base — 키 추가 + TaskMessage 필드 확장

**Files:**
- Modify: `internal/base/keys.go`
- Modify: `internal/base/task.go`
- Test: `internal/base/keys_test.go`, `internal/base/task_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

`internal/base/keys_test.go`에 추가:
```go
func TestRetryAndArchivedAndTaskPrefixKeys(t *testing.T) {
	if got := RetryKey("default"); got != "chronos:{default}:retry" {
		t.Errorf("RetryKey = %q", got)
	}
	if got := ArchivedKey("default"); got != "chronos:{default}:archived" {
		t.Errorf("ArchivedKey = %q", got)
	}
	if got := TaskKeyPrefix("default"); got != "chronos:{default}:t:" {
		t.Errorf("TaskKeyPrefix = %q", got)
	}
	// TaskKey는 prefix + id와 일치해야 한다(forward Lua가 prefix로 키를 조립하므로).
	if TaskKeyPrefix("default")+"abc" != TaskKey("default", "abc") {
		t.Error("TaskKeyPrefix + id must equal TaskKey")
	}
}
```

`internal/base/task_test.go`에 추가:
```go
func TestTaskMessage_M2Fields_RoundTrip(t *testing.T) {
	msg := &TaskMessage{
		ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default",
		Retried: 3, MaxRetry: 25, NoArchive: true,
	}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Retried != 3 || got.MaxRetry != 25 || !got.NoArchive {
		t.Errorf("m2 fields round trip mismatch: %+v", got)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/base/ -run 'TestRetryAndArchivedAndTaskPrefixKeys|TestTaskMessage_M2Fields_RoundTrip' -v`
Expected: FAIL — `undefined: RetryKey` 등, 그리고 `unknown field Retried`.

- [ ] **Step 3: 키 추가**

`internal/base/keys.go` 끝에 추가:
```go
// TaskKeyPrefix returns the prefix shared by every task HASH key of a queue.
// Lua scripts build a task key by concatenating this prefix with a task ID read
// from a ZSET; the prefix keeps those keys in the same cluster slot.
func TaskKeyPrefix(qname string) string {
	return QueueKeyPrefix(qname) + "t:"
}

// RetryKey returns the ZSET key holding tasks awaiting retry (score = retry_at).
func RetryKey(qname string) string {
	return QueueKeyPrefix(qname) + "retry"
}

// ArchivedKey returns the ZSET key holding dead-lettered tasks (score = died_at).
func ArchivedKey(qname string) string {
	return QueueKeyPrefix(qname) + "archived"
}
```

- [ ] **Step 4: TaskMessage 필드 추가**

`internal/base/task.go`의 `TaskMessage`를 다음으로 교체:
```go
// TaskMessage is the canonical, serialized representation of a task stored in
// the task HASH.
type TaskMessage struct {
	ID      string    `json:"id"`
	Kind    string    `json:"kind"`
	Payload []byte    `json:"payload"`
	Queue   string    `json:"queue"`
	State   TaskState `json:"state"`

	// Retried is the number of retries already scheduled for this task.
	Retried int `json:"retried"`
	// MaxRetry is the maximum number of retries before the task is dead-lettered.
	MaxRetry int `json:"max_retry"`
	// NoArchive, when true, discards the task on retry exhaustion instead of
	// storing it in the archived ZSET (the OnDeadLetter hook still fires).
	NoArchive bool `json:"no_archive"`
}
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `go test ./internal/base/ -v`
Expected: PASS (기존 + 신규).

- [ ] **Step 6: 커밋**

```bash
git add internal/base/
git commit -m "feat: base에 retry/archived 키 + TaskMessage 재시도 필드 추가"
```

---

## Task 2: rdb — Retry / Archive (active → ZSET 이동)

**Files:**
- Create: `internal/rdb/retry.go`
- Test: `internal/rdb/retry_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/retry_test.go`:
```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

// enqueueAndDequeue is a helper: enqueue one task, dequeue it (so it is active
// and in the PEL), and return the message + its stream entry ID.
func enqueueAndDequeue(t *testing.T, r *RDB, qname string, msg *base.TaskMessage) (*base.TaskMessage, string) {
	t.Helper()
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, qname); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, streamID, err := r.Dequeue(ctx, "c1", 0, qname)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	return got, streamID
}

func TestRetry_MovesActiveToRetryZSet(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default", MaxRetry: 5}
	got, streamID := enqueueAndDequeue(t, r, "default", msg)
	got.Retried = 1

	retryAt := time.Now().Add(30 * time.Second)
	if err := r.Retry(ctx, "default", streamID, got, retryAt); err != nil {
		t.Fatalf("retry: %v", err)
	}

	// Stream entry acked (PEL empty).
	pending, err := client.XPending(ctx, base.StreamKey("default"), ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 0 {
		t.Errorf("pending = %d, want 0", pending.Count)
	}

	// Task is in the retry ZSET with the expected score.
	score, err := client.ZScore(ctx, base.RetryKey("default"), "t1").Result()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	if int64(score) != retryAt.Unix() {
		t.Errorf("retry score = %d, want %d", int64(score), retryAt.Unix())
	}

	// Task hash reflects retry state and incremented Retried.
	raw, err := client.HGet(ctx, base.TaskKey("default", "t1"), "msg").Result()
	if err != nil {
		t.Fatalf("hget: %v", err)
	}
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.State != base.StateRetry || stored.Retried != 1 {
		t.Errorf("stored = state:%v retried:%d, want retry/1", stored.State, stored.Retried)
	}
}

func TestArchive_MovesActiveToArchivedZSet(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default", MaxRetry: 0}
	got, streamID := enqueueAndDequeue(t, r, "default", msg)

	diedAt := time.Now()
	if err := r.Archive(ctx, "default", streamID, got, diedAt); err != nil {
		t.Fatalf("archive: %v", err)
	}

	pending, _ := client.XPending(ctx, base.StreamKey("default"), ConsumerGroup).Result()
	if pending.Count != 0 {
		t.Errorf("pending = %d, want 0", pending.Count)
	}
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "t1").Result(); err != nil {
		t.Fatalf("task not in archived zset: %v", err)
	}
	raw, _ := client.HGet(ctx, base.TaskKey("default", "t1"), "msg").Result()
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.State != base.StateArchived {
		t.Errorf("state = %v, want archived", stored.State)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestRetry_MovesActiveToRetryZSet|TestArchive_MovesActiveToArchivedZSet' -v`
Expected: FAIL — `undefined: (*RDB).Retry`, `(*RDB).Archive`.

- [ ] **Step 3: Retry/Archive 구현**

Create `internal/rdb/retry.go`:
```go
package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// moveToZSetCmd acks a stream entry and moves the task into a target ZSET
// (retry or archived), updating the stored message and state atomically.
// KEYS[1] stream, KEYS[2] task hash, KEYS[3] target zset.
// ARGV[1] group, ARGV[2] streamID, ARGV[3] encoded msg, ARGV[4] state,
// ARGV[5] score, ARGV[6] taskID.
var moveToZSetCmd = redis.NewScript(`
redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[6])
return 1
`)

// Retry acks the active stream entry and moves the task to the retry ZSET with
// score = retryAt. The caller is responsible for having set msg.Retried to the
// desired value; Retry persists msg as given and sets its state to retry.
func (r *RDB) Retry(ctx context.Context, qname, streamID string, msg *base.TaskMessage, retryAt time.Time) error {
	return r.moveToZSet(ctx, qname, streamID, msg, base.RetryKey(qname), base.StateRetry, retryAt.Unix())
}

// Archive acks the active stream entry and moves the task to the archived ZSET
// (dead-letter) with score = diedAt.
func (r *RDB) Archive(ctx context.Context, qname, streamID string, msg *base.TaskMessage, diedAt time.Time) error {
	return r.moveToZSet(ctx, qname, streamID, msg, base.ArchivedKey(qname), base.StateArchived, diedAt.Unix())
}

func (r *RDB) moveToZSet(ctx context.Context, qname, streamID string, msg *base.TaskMessage, zsetKey string, state base.TaskState, score int64) error {
	msg.State = state
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	keys := []string{base.StreamKey(qname), base.TaskKey(qname, msg.ID), zsetKey}
	argv := []interface{}{ConsumerGroup, streamID, encoded, int(state), score, msg.ID}
	return moveToZSetCmd.Run(ctx, r.client, keys, argv...).Err()
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/rdb/ -run 'TestRetry_MovesActiveToRetryZSet|TestArchive_MovesActiveToArchivedZSet' -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/retry.go internal/rdb/retry_test.go
git commit -m "feat: rdb Retry/Archive (active를 retry/archived ZSET으로 원자 이동)"
```

---

## Task 3: rdb — ForwardRetry (retry ZSET → stream 승격)

**Files:**
- Create: `internal/rdb/forward.go`
- Test: `internal/rdb/forward_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/forward_test.go`:
```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestForwardRetry_MovesDueTasksToStream(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// One due task (retryAt in the past), one not-yet-due (future).
	dueMsg := &base.TaskMessage{ID: "due", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	notMsg := &base.TaskMessage{ID: "future", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	for _, m := range []*base.TaskMessage{dueMsg, notMsg} {
		if err := r.Enqueue(ctx, m); err != nil { // stores task hash
			t.Fatalf("enqueue: %v", err)
		}
	}
	// Put them directly into the retry ZSET with controlled scores.
	client.ZAdd(ctx, base.RetryKey("default"), redis.Z{Score: float64(time.Now().Add(-1 * time.Minute).Unix()), Member: "due"})
	client.ZAdd(ctx, base.RetryKey("default"), redis.Z{Score: float64(time.Now().Add(1 * time.Hour).Unix()), Member: "future"})
	// Drain the stream entries created by Enqueue so we count only forwarded ones.
	client.Del(ctx, base.StreamKey("default"))

	n, err := r.ForwardRetry(ctx, "default", time.Now(), 100)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if n != 1 {
		t.Errorf("forwarded = %d, want 1", n)
	}

	// The due task is now in the stream; the future one remains in retry.
	slen, _ := client.XLen(ctx, base.StreamKey("default")).Result()
	if slen != 1 {
		t.Errorf("stream len = %d, want 1", slen)
	}
	if _, err := client.ZScore(ctx, base.RetryKey("default"), "due").Result(); err != redis.Nil {
		t.Errorf("due task should be removed from retry zset, err=%v", err)
	}
	if _, err := client.ZScore(ctx, base.RetryKey("default"), "future").Result(); err != nil {
		t.Errorf("future task should remain in retry zset: %v", err)
	}
}
```

Add the import for `redis` at the top of the file:
```go
	"github.com/redis/go-redis/v9"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run TestForwardRetry_MovesDueTasksToStream -v`
Expected: FAIL — `undefined: (*RDB).ForwardRetry`.

- [ ] **Step 3: ForwardRetry 구현**

Create `internal/rdb/forward.go`:
```go
package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// forwardCmd moves due tasks from a source ZSET back into the stream. It reads
// task IDs with score <= now, and for each: appends the ID to the stream, sets
// the task hash state to pending, and removes it from the ZSET.
// KEYS[1] source zset, KEYS[2] stream.
// ARGV[1] now (score cutoff), ARGV[2] max count, ARGV[3] task-key prefix,
// ARGV[4] pending state.
var forwardCmd = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
for _, id in ipairs(ids) do
  redis.call("XADD", KEYS[2], "*", "task_id", id)
  redis.call("HSET", ARGV[3] .. id, "state", ARGV[4])
  redis.call("ZREM", KEYS[1], id)
end
return #ids
`)

// ForwardRetry moves tasks whose retry time has arrived (score <= now) from the
// retry ZSET back into the stream for reprocessing. It processes at most max
// tasks and returns how many were forwarded. The computed task-hash keys share
// the queue's hash tag, so the multi-key script is cluster-safe.
func (r *RDB) ForwardRetry(ctx context.Context, qname string, now time.Time, max int) (int, error) {
	keys := []string{base.RetryKey(qname), base.StreamKey(qname)}
	argv := []interface{}{now.Unix(), max, base.TaskKeyPrefix(qname), int(base.StatePending)}
	n, err := forwardCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/rdb/ -run TestForwardRetry_MovesDueTasksToStream -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/forward.go internal/rdb/forward_test.go
git commit -m "feat: rdb ForwardRetry (retry ZSET에서 due 태스크를 stream으로 승격)"
```

---

## Task 4: rdb — Recover (XAUTOCLAIM으로 crash 태스크 회수)

**Files:**
- Create: `internal/rdb/recover.go`
- Test: `internal/rdb/recover_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/recover_test.go`:
```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestRecover_ReclaimsStuckTaskToRetry(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// Dequeue with consumer "dead" so the entry sits in dead's PEL, then never ack.
	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default", MaxRetry: 5}
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := r.Dequeue(ctx, "dead", 0, "default"); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	// minIdle=0 reclaims immediately (no need to wait).
	recovered, archived, err := r.Recover(ctx, "default", "recoverer", 0, 100)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered != 1 || len(archived) != 0 {
		t.Fatalf("recovered=%d archived=%d, want 1/0", recovered, len(archived))
	}

	// Task moved to retry ZSET, Retried incremented, PEL cleared.
	if _, err := client.ZScore(ctx, base.RetryKey("default"), "t1").Result(); err != nil {
		t.Errorf("recovered task not in retry zset: %v", err)
	}
	pending, _ := client.XPending(ctx, base.StreamKey("default"), ConsumerGroup).Result()
	if pending.Count != 0 {
		t.Errorf("pending = %d, want 0", pending.Count)
	}
	raw, _ := client.HGet(ctx, base.TaskKey("default", "t1"), "msg").Result()
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.Retried != 1 {
		t.Errorf("retried = %d, want 1", stored.Retried)
	}
}

func TestRecover_ArchivesWhenRetriesExhausted(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// MaxRetry=1 and already Retried=1 → the crash exhausts the budget → archive.
	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default", MaxRetry: 1, Retried: 1}
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := r.Dequeue(ctx, "dead", 0, "default"); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	recovered, archived, err := r.Recover(ctx, "default", "recoverer", 0, 100)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered != 0 || len(archived) != 1 {
		t.Fatalf("recovered=%d archived=%d, want 0/1", recovered, len(archived))
	}
	if archived[0].ID != "t1" {
		t.Errorf("archived id = %q, want t1", archived[0].ID)
	}
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "t1").Result(); err != nil {
		t.Errorf("task not in archived zset: %v", err)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestRecover_' -v`
Expected: FAIL — `undefined: (*RDB).Recover`.

- [ ] **Step 3: Recover 구현**

Create `internal/rdb/recover.go`:
```go
package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// Recover reclaims tasks stuck in the consumer group's PEL — entries whose
// owning consumer has been idle longer than minIdle (typically because the
// worker crashed). Each reclaimed task counts as one failed attempt: if that
// exhausts its retry budget it is archived, otherwise it is moved to the retry
// ZSET for immediate re-forwarding. It processes at most max entries per call.
//
// Returned: recovered = count moved to retry; archived = the messages that were
// dead-lettered (so the caller can fire OnDeadLetter for each).
//
// NOTE: Without heartbeat-based lease extension (a later milestone), a handler
// that runs longer than minIdle can be reclaimed and reprocessed concurrently.
// This is the at-least-once contract; set minIdle comfortably above expected
// handler duration.
func (r *RDB) Recover(ctx context.Context, qname, consumer string, minIdle time.Duration, max int) (recovered int, archived []*base.TaskMessage, err error) {
	streamKey := base.StreamKey(qname)

	msgs, _, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamKey,
		Group:    ConsumerGroup,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0",
		Count:    int64(max),
	}).Result()
	if err != nil {
		return 0, nil, err
	}

	now := time.Now()
	for _, m := range msgs {
		taskID, _ := m.Values["task_id"].(string)

		raw, err := r.client.HGet(ctx, base.TaskKey(qname, taskID), "msg").Result()
		if err == redis.Nil {
			// Body already gone: just drop the orphan PEL entry.
			_ = r.client.XAck(ctx, streamKey, ConsumerGroup, m.ID).Err()
			continue
		}
		if err != nil {
			return recovered, archived, err
		}
		msg, err := base.DecodeMessage([]byte(raw))
		if err != nil {
			return recovered, archived, err
		}

		// This reclaim counts as one failed attempt.
		if msg.Retried >= msg.MaxRetry {
			if aerr := r.Archive(ctx, qname, m.ID, msg, now); aerr != nil {
				return recovered, archived, aerr
			}
			archived = append(archived, msg)
			continue
		}
		msg.Retried++
		// Re-run promptly: schedule the retry for "now" so the forwarder
		// picks it up on its next tick.
		if rerr := r.Retry(ctx, qname, m.ID, msg, now); rerr != nil {
			return recovered, archived, rerr
		}
		recovered++
	}
	return recovered, archived, nil
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/rdb/ -run 'TestRecover_' -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/recover.go internal/rdb/recover_test.go
git commit -m "feat: rdb Recover (XAUTOCLAIM으로 crash 태스크 회수 → retry/archive)"
```

---

## Task 5: chronos — 재시도 옵션 + SkipRetry + 백오프

**Files:**
- Modify: `chronos.go` (enqueueOptions 확장, WithMaxRetry/WithDeadLetterDiscard, Enqueue에서 필드 전달)
- Create: `retry.go` (루트 패키지: SkipRetry, DefaultRetryDelay, RetryDelayFunc)
- Test: `retry_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `retry_test.go`:
```go
package chronos

import (
	"errors"
	"testing"
	"time"
)

func TestSkipRetry_Detectable(t *testing.T) {
	base := errors.New("bad input")
	err := SkipRetry(base)
	if !asSkipRetry(err) {
		t.Error("asSkipRetry should detect a SkipRetry-wrapped error")
	}
	if !errors.Is(err, base) {
		t.Error("SkipRetry should wrap and preserve the original error")
	}
	if asSkipRetry(errors.New("plain")) {
		t.Error("plain error must not be treated as SkipRetry")
	}
}

func TestDefaultRetryDelay_GrowsAndIsBounded(t *testing.T) {
	// Full-jitter delay is in [0, cap]; cap grows with retried but never exceeds max.
	const max = 15 * time.Minute
	for _, retried := range []int{0, 1, 5, 20, 100} {
		d := DefaultRetryDelay(retried, errors.New("x"))
		if d < 0 || d > max {
			t.Errorf("DefaultRetryDelay(%d) = %v, want within [0, %v]", retried, d, max)
		}
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run 'TestSkipRetry_Detectable|TestDefaultRetryDelay_GrowsAndIsBounded' -v`
Expected: FAIL — `undefined: SkipRetry` 등.

- [ ] **Step 3: retry.go 구현**

Create `retry.go`:
```go
package chronos

import (
	"errors"
	"math"
	"math/rand"
	"time"
)

// RetryDelayFunc computes how long to wait before the next retry, given the
// number of retries already performed and the error that caused the failure.
type RetryDelayFunc func(retried int, err error) time.Duration

// skipRetryError marks an error as non-retryable: the task is dead-lettered
// immediately instead of being retried.
type skipRetryError struct{ err error }

func (e *skipRetryError) Error() string { return e.err.Error() }
func (e *skipRetryError) Unwrap() error { return e.err }

// SkipRetry wraps err so that returning it from a handler dead-letters the task
// immediately, bypassing the remaining retry budget.
func SkipRetry(err error) error {
	return &skipRetryError{err: err}
}

// asSkipRetry reports whether err is (or wraps) a SkipRetry error.
func asSkipRetry(err error) bool {
	var se *skipRetryError
	return errors.As(err, &se)
}

// retryBaseDelay and retryMaxDelay bound the default exponential backoff.
const (
	retryBaseDelay = 5 * time.Second
	retryMaxDelay  = 15 * time.Minute
)

// DefaultRetryDelay is the default backoff: an exponential cap (base * 2^retried,
// clamped to retryMaxDelay) with full jitter — the actual delay is uniformly
// random in [0, cap]. Full jitter spreads retries to avoid thundering herds.
func DefaultRetryDelay(retried int, _ error) time.Duration {
	ceiling := float64(retryBaseDelay) * math.Pow(2, float64(retried))
	if ceiling > float64(retryMaxDelay) {
		ceiling = float64(retryMaxDelay)
	}
	return time.Duration(rand.Int63n(int64(ceiling) + 1))
}
```

- [ ] **Step 4: enqueueOptions 확장 및 옵션 추가**

`chronos.go`의 `enqueueOptions`를 교체:
```go
// enqueueOptions holds resolved enqueue-time settings.
type enqueueOptions struct {
	queue     string
	taskID    string
	maxRetry  int
	noArchive bool
}
```

`WithTaskID` 아래에 옵션 두 개 추가:
```go
// WithMaxRetry sets the maximum number of retries before the task is
// dead-lettered. Defaults to DefaultMaxRetry.
func WithMaxRetry(n int) Option {
	return optionFunc(func(o *enqueueOptions) {
		if n < 0 {
			n = 0
		}
		o.maxRetry = n
	})
}

// WithDeadLetterDiscard discards the task on retry exhaustion instead of storing
// it in the archived ZSET. The OnDeadLetter hook still fires.
func WithDeadLetterDiscard() Option {
	return optionFunc(func(o *enqueueOptions) { o.noArchive = true })
}
```

`DefaultQueue` 상수 아래에 추가:
```go
// DefaultMaxRetry is the retry budget used when WithMaxRetry is not given.
const DefaultMaxRetry = 25
```

`Enqueue`의 `options` 초기화와 `msg` 생성을 수정:
```go
	options := enqueueOptions{queue: DefaultQueue, maxRetry: DefaultMaxRetry}
```
그리고 `msg := &base.TaskMessage{...}` 를:
```go
	msg := &base.TaskMessage{
		ID:        id,
		Kind:      args.Kind(),
		Payload:   payload,
		Queue:     options.queue,
		MaxRetry:  options.maxRetry,
		NoArchive: options.noArchive,
	}
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `go test . -run 'TestSkipRetry_Detectable|TestDefaultRetryDelay_GrowsAndIsBounded' -v && go build ./...`
Expected: PASS, 빌드 성공.

- [ ] **Step 6: 커밋**

```bash
git add chronos.go retry.go retry_test.go
git commit -m "feat: 재시도 옵션(WithMaxRetry/WithDeadLetterDiscard) + SkipRetry + 백오프"
```

---

## Task 6: server — process 재작성 (분류/panic/retry/archive/훅)

**Files:**
- Modify: `server.go` (ServerConfig 확장, NewServer 기본값, process 재작성)
- Test: `server_reliability_test.go` (신규)

- [ ] **Step 1: 실패하는 테스트 작성**

Create `server_reliability_test.go`:
```go
package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestProcess_ErrorMovesTaskToRetry(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		return errors.New("boom")
	})

	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 2,
		// Keep the failed task parked in retry (don't let the forwarder move it
		// back) so we can observe the retry ZSET deterministically.
		RetryDelayFunc: func(retried int, err error) time.Duration { return time.Hour },
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	eventually(t, 5*time.Second, func() bool {
		score := client.ZScore(ctx, base.RetryKey("default"), info.ID)
		return score.Err() == nil
	}, "failed task should land in the retry ZSET")

	// PEL cleared, Retried incremented to 1.
	pending, _ := client.XPending(ctx, base.StreamKey("default"), rdb.ConsumerGroup).Result()
	if pending.Count != 0 {
		t.Errorf("pending = %d, want 0", pending.Count)
	}
	raw, _ := client.HGet(ctx, base.TaskKey("default", info.ID), "msg").Result()
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.Retried != 1 {
		t.Errorf("retried = %d, want 1", stored.Retried)
	}
}

func TestProcess_SkipRetryArchivesImmediatelyAndFiresHook(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var hookFired atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		return SkipRetry(errors.New("permanent"))
	})

	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 2,
		OnDeadLetter: func(ctx context.Context, info *TaskInfo, err error) {
			hookFired.Add(1)
		},
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(10))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	eventually(t, 5*time.Second, func() bool {
		archived := client.ZScore(ctx, base.ArchivedKey("default"), info.ID).Err() == nil
		return archived && hookFired.Load() == 1
	}, "SkipRetry should archive immediately (bypassing retry budget) and fire OnDeadLetter")
}

func TestProcess_PanicIsRecoveredAndRetried(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		panic("kaboom")
	})

	srv := NewServer(client, ServerConfig{
		Queues:         map[string]int{"default": 1},
		Concurrency:    2,
		RetryDelayFunc: func(retried int, err error) time.Duration { return time.Hour },
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// A panicking handler must not crash the server; the task is retried.
	eventually(t, 5*time.Second, func() bool {
		return client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil
	}, "panicking handler should be recovered and its task retried")
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run 'TestProcess_' -v`
Expected: FAIL — `unknown field RetryDelayFunc` / `OnDeadLetter` in ServerConfig.

- [ ] **Step 3: ServerConfig 확장 + NewServer 기본값**

`server.go`의 `ServerConfig`를 교체:
```go
// ServerConfig configures a Server.
type ServerConfig struct {
	// Queues maps queue name to weight. Only the keys are used today (all queues
	// are read equally); weighted priority is a later enhancement.
	Queues map[string]int
	// Concurrency is the max number of tasks processed simultaneously.
	Concurrency int
	// Logger receives operational logs. Defaults to slog.Default().
	Logger *slog.Logger

	// RetryDelayFunc computes the backoff before a retry. Defaults to
	// DefaultRetryDelay (exponential + full jitter).
	RetryDelayFunc RetryDelayFunc
	// OnDeadLetter is invoked when a task exhausts its retries (or returns a
	// SkipRetry error). It fires whether the task is archived or discarded.
	OnDeadLetter func(ctx context.Context, info *TaskInfo, err error)

	// ForwardInterval is how often the retry ZSET is scanned for due tasks.
	// Defaults to 1s.
	ForwardInterval time.Duration
	// RecoverInterval is how often stuck PEL entries are reclaimed. Defaults to 15s.
	RecoverInterval time.Duration
	// RecoverMinIdle is how long a PEL entry must be idle before it is treated as
	// abandoned. Defaults to 30s. Handlers that can run longer should raise this.
	RecoverMinIdle time.Duration
}
```

`NewServer`에서 기본값을 채우도록 수정. 기존 `NewServer` 본문의 `logger` 처리 다음, `return` 전에 추가:
```go
	if cfg.RetryDelayFunc == nil {
		cfg.RetryDelayFunc = DefaultRetryDelay
	}
	if cfg.ForwardInterval <= 0 {
		cfg.ForwardInterval = 1 * time.Second
	}
	if cfg.RecoverInterval <= 0 {
		cfg.RecoverInterval = 15 * time.Second
	}
	if cfg.RecoverMinIdle <= 0 {
		cfg.RecoverMinIdle = 30 * time.Second
	}
```

- [ ] **Step 4: process 재작성**

`server.go`의 `process` 메서드를 다음으로 교체:
```go
// process runs the handler for one task, recovering panics, and routes the
// outcome: success acks and deletes; a retryable error moves the task to the
// retry ZSET with backoff (until the retry budget is exhausted); a SkipRetry
// error or an exhausted budget dead-letters the task and fires OnDeadLetter.
func (s *Server) process(ctx context.Context, qname, streamID string, msg *base.TaskMessage) {
	err := s.dispatchSafely(ctx, msg)

	// Ack/move operations must outlive shutdown cancellation so a finished task
	// is never left dangling in the PEL.
	opCtx := context.WithoutCancel(ctx)

	if err == nil {
		if derr := s.rdb.Done(opCtx, qname, streamID, msg.ID); derr != nil {
			s.logger.Error("chronos: ack failed", "id", msg.ID, "error", derr)
		}
		return
	}

	s.logger.Error("chronos: task failed",
		"kind", msg.Kind, "id", msg.ID, "retried", msg.Retried, "error", err)

	// Dead-letter when the error is non-retryable or the budget is exhausted.
	if asSkipRetry(err) || msg.Retried >= msg.MaxRetry {
		s.deadLetter(opCtx, qname, streamID, msg, err)
		return
	}

	msg.Retried++
	retryAt := time.Now().Add(s.cfg.RetryDelayFunc(msg.Retried, err))
	if rerr := s.rdb.Retry(opCtx, qname, streamID, msg, retryAt); rerr != nil {
		s.logger.Error("chronos: retry scheduling failed", "id", msg.ID, "error", rerr)
	}
}

// dispatchSafely runs the handler and converts a panic into an error so a
// misbehaving handler cannot crash the worker.
func (s *Server) dispatchSafely(ctx context.Context, msg *base.TaskMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("chronos: handler panic: %v", r)
			s.logger.Error("chronos: handler panicked",
				"kind", msg.Kind, "id", msg.ID, "panic", r)
		}
	}()
	return s.mux.dispatch(ctx, msg)
}

// deadLetter archives the task (or discards it when NoArchive is set) and fires
// the OnDeadLetter hook.
func (s *Server) deadLetter(ctx context.Context, qname, streamID string, msg *base.TaskMessage, cause error) {
	if msg.NoArchive {
		if derr := s.rdb.Done(ctx, qname, streamID, msg.ID); derr != nil {
			s.logger.Error("chronos: discard failed", "id", msg.ID, "error", derr)
		}
	} else if aerr := s.rdb.Archive(ctx, qname, streamID, msg, time.Now()); aerr != nil {
		s.logger.Error("chronos: archive failed", "id", msg.ID, "error", aerr)
	}
	if s.cfg.OnDeadLetter != nil {
		s.cfg.OnDeadLetter(ctx, &TaskInfo{ID: msg.ID, Kind: msg.Kind, Queue: msg.Queue}, cause)
	}
}
```

`server.go` import 블록에 `"fmt"`를 추가하라(알파벳 순: context, errors, fmt, log/slog, sync, time).

- [ ] **Step 5: 테스트 통과 확인**

Run: `go test . -run 'TestProcess_' -race -v`
Expected: PASS (3개 테스트).

- [ ] **Step 6: 기존 테스트 회귀 확인 + 커밋**

Run: `go test . -race`
Expected: PASS (기존 서버/핸들러/통합 테스트 포함). 특히 `TestServer_ErrorHandlerAcksAndDeletes`는 M1에서 "에러 시 ack+삭제"를 검증했는데, M2에서 동작이 "에러 시 retry ZSET 이동"으로 바뀐다. **이 테스트는 M2 동작과 모순되므로 이 스텝에서 수정한다:** 해당 테스트가 기대하는 바를 "에러 태스크는 PEL에서 빠지고(XACK) retry ZSET으로 이동, task hash는 남아있음"으로 갱신하라. 구체적으로 `server_test.go`의 `TestServer_ErrorHandlerAcksAndDeletes`를 다음으로 교체:
```go
func TestServer_ErrorHandlerMovesToRetry(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	handled := make(chan struct{}, 1)
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		select {
		case handled <- struct{}{}:
		default:
		}
		return errors.New("boom")
	})

	srv := NewServer(client, ServerConfig{
		Queues:         map[string]int{"default": 1},
		Concurrency:    2,
		RetryDelayFunc: func(retried int, err error) time.Duration { return time.Hour },
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler not invoked")
	}

	eventually(t, 5*time.Second, func() bool {
		p, _ := client.XPending(ctx, base.StreamKey("default"), rdb.ConsumerGroup).Result()
		inRetry := client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil
		return p != nil && p.Count == 0 && inRetry
	}, "error task should be acked out of the PEL and moved to the retry ZSET")
}
```
`server_test.go`의 import에 `base`, `rdb`가 없으면 추가하라(`TestServer_UnregisteredKindAcksAndDeletes`가 이미 쓰고 있으면 재사용).

주의: `TestServer_UnregisteredKindAcksAndDeletes`는 그대로 유효하다 — 미등록 kind는 `dispatch`가 에러를 반환하고, 그 태스크는 `MaxRetry`가 기본 25라 retry로 갈 것 같지만, **미등록 kind 태스크도 재시도 대상이 된다**(핸들러가 나중에 등록될 수 있으므로 합리적). 따라서 이 테스트의 기대도 바뀐다. `TestServer_UnregisteredKindAcksAndDeletes`를 다음으로 교체하라:
```go
func TestServer_UnregisteredKindMovesToRetry(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	srv := NewServer(client, ServerConfig{
		Queues:         map[string]int{"default": 1},
		Concurrency:    2,
		RetryDelayFunc: func(retried int, err error) time.Duration { return time.Hour },
	})
	ctx := context.Background()
	if err := srv.Start(ctx, NewMux()); err != nil { // no handlers
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	eventually(t, 5*time.Second, func() bool {
		return client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil
	}, "unregistered-kind task should be retried (a handler may be registered later)")
}
```

Run again: `go test . -race`
Expected: PASS.

```bash
git add server.go server_reliability_test.go server_test.go
git commit -m "feat: server process 재작성 — 에러 분류/panic 복구/retry/archive/OnDeadLetter"
```

---

## Task 7: server — forwarder + recoverer 고루틴 기동

**Files:**
- Modify: `server.go` (Start에서 forwarder/recoverer 기동, 루프 구현)
- Test: `server_reliability_test.go` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`server_reliability_test.go`에 추가:
```go
func TestServer_RetriedTaskEventuallySucceeds(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var attempts atomic.Int32
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		if attempts.Add(1) == 1 {
			return errors.New("fail once")
		}
		close(done)
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 100 * time.Millisecond,
		RetryDelayFunc:  func(retried int, err error) time.Duration { return 50 * time.Millisecond },
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
	case <-time.After(10 * time.Second):
		t.Fatalf("task did not succeed on retry (attempts=%d)", attempts.Load())
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("attempts = %d, want 2 (1 fail + 1 success)", got)
	}
}

func TestServer_CrashedTaskIsRecovered(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	ctx := context.Background()
	// Simulate a crash: a foreign consumer reads the task and never acks, so it
	// sits idle in that consumer's PEL. A running server's recoverer must
	// reclaim it, and a real handler must then process it.
	if err := c.rdb.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := c.rdb.Dequeue(ctx, "dead-worker", 0, "default"); err != nil {
		t.Fatalf("simulate crash dequeue: %v", err)
	}

	processed := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(processed)
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 100 * time.Millisecond,
		RecoverInterval: 200 * time.Millisecond,
		RecoverMinIdle:  0, // reclaim immediately for the test
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	select {
	case <-processed:
	case <-time.After(10 * time.Second):
		t.Fatal("crashed task was not recovered and processed")
	}
	// After success, nothing lingers in retry/archived for this task.
	eventually(t, 3*time.Second, func() bool {
		inRetry := client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil
		inArch := client.ZScore(ctx, base.ArchivedKey("default"), info.ID).Err() == nil
		return !inRetry && !inArch
	}, "recovered-and-succeeded task should leave no retry/archived residue")
}
```

`c.rdb`는 `Client`의 비공개 필드다. 같은 패키지 테스트이므로 접근 가능하다.

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run 'TestServer_RetriedTaskEventuallySucceeds|TestServer_CrashedTaskIsRecovered' -v`
Expected: FAIL — 재시도 태스크가 forward되지 않고(포워더 없음), crash 태스크가 회수되지 않아(recoverer 없음) 타임아웃.

- [ ] **Step 3: forwarder/recoverer 루프 구현 + Start에서 기동**

`server.go`의 `Start`에서 fetchLoop 기동 부분을 다음으로 교체:
```go
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.wg.Add(1)
	go s.fetchLoop(runCtx)

	s.wg.Add(1)
	go s.forwarderLoop(runCtx)

	s.wg.Add(1)
	go s.recovererLoop(runCtx)
	return nil
```

`server.go`에 두 루프를 추가(예: `process` 관련 메서드들 뒤):
```go
// forwarderLoop periodically moves due tasks from each queue's retry ZSET back
// into its stream.
func (s *Server) forwarderLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.ForwardInterval)
	defer ticker.Stop()
	queues := s.queueNames()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range queues {
				if _, err := s.rdb.ForwardRetry(ctx, q, time.Now(), forwardBatchSize); err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: forward failed", "queue", q, "error", err)
				}
			}
		}
	}
}

// recovererLoop periodically reclaims tasks stuck in each queue's PEL (crashed
// workers) and fires OnDeadLetter for any that are dead-lettered as a result.
func (s *Server) recovererLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.RecoverInterval)
	defer ticker.Stop()
	queues := s.queueNames()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range queues {
				_, archived, err := s.rdb.Recover(ctx, q, s.consumer, s.cfg.RecoverMinIdle, recoverBatchSize)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: recover failed", "queue", q, "error", err)
					continue
				}
				if s.cfg.OnDeadLetter != nil {
					for _, msg := range archived {
						s.cfg.OnDeadLetter(ctx,
							&TaskInfo{ID: msg.ID, Kind: msg.Kind, Queue: msg.Queue},
							errRecoveredExhausted)
					}
				}
			}
		}
	}
}
```

`server.go`의 `pollBlock` 상수 근처에 배치 크기와 recoverer 훅 사유를 추가:
```go
// forwardBatchSize and recoverBatchSize bound how many tasks each maintenance
// tick moves, keeping individual Redis calls short.
const (
	forwardBatchSize = 100
	recoverBatchSize = 100
)

// errRecoveredExhausted is the cause passed to OnDeadLetter when a task is
// dead-lettered by the recoverer (its retry budget ran out across crashes).
var errRecoveredExhausted = errors.New("chronos: retries exhausted after recovery")
```
(`errors`는 이미 server.go에서 import됨.)

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test . -run 'TestServer_RetriedTaskEventuallySucceeds|TestServer_CrashedTaskIsRecovered' -race -v`
Expected: PASS

- [ ] **Step 5: 전체 회귀 + 커밋**

Run: `go test ./... -race -p 1`
Expected: 전 패키지 PASS.

```bash
git add server.go server_reliability_test.go
git commit -m "feat: server forwarder/recoverer 고루틴 (재시도 승격 + crash 회수)"
```

---

## Task 8: 통합 — poison pill 수렴 + dead-letter + 전체 검증

**Files:**
- Create: `reliability_integration_test.go`

- [ ] **Step 1: poison pill 수렴 통합 테스트 작성**

Create `reliability_integration_test.go`:
```go
package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

type poisonArgs struct {
	ID int `json:"id"`
}

func (poisonArgs) Kind() string { return "test:poison" }

func TestIntegration_PoisonPillConvergesToDeadLetter(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var calls atomic.Int32
	var deadLettered atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[poisonArgs]) error {
		calls.Add(1)
		return errors.New("always fails")
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     4,
		ForwardInterval: 50 * time.Millisecond,
		RetryDelayFunc:  func(retried int, err error) time.Duration { return 20 * time.Millisecond },
		OnDeadLetter: func(ctx context.Context, info *TaskInfo, err error) {
			deadLettered.Add(1)
		},
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	const maxRetry = 3
	info, err := Enqueue(ctx, c, poisonArgs{ID: 1}, WithMaxRetry(maxRetry))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Must land in archived and fire the hook exactly once.
	eventually(t, 10*time.Second, func() bool {
		return deadLettered.Load() == 1 &&
			client.ZScore(ctx, base.ArchivedKey("default"), info.ID).Err() == nil
	}, "poison pill should converge to the archived ZSET and fire OnDeadLetter once")

	// It should have executed exactly maxRetry+1 times (1 initial + N retries),
	// then stopped — no infinite reprocessing.
	// Give the system a moment to prove it does NOT keep running.
	time.Sleep(500 * time.Millisecond)
	if got := calls.Load(); got != maxRetry+1 {
		t.Errorf("handler calls = %d, want %d (1 initial + %d retries)", got, maxRetry+1, maxRetry)
	}
	if got := deadLettered.Load(); got != 1 {
		t.Errorf("OnDeadLetter fired %d times, want 1", got)
	}

	// Not left in the retry ZSET.
	if client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil {
		t.Error("task should not remain in retry ZSET after dead-lettering")
	}
}

func TestIntegration_DiscardModeSkipsArchiveButFiresHook(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var deadLettered atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[poisonArgs]) error {
		return errors.New("always fails")
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 50 * time.Millisecond,
		RetryDelayFunc:  func(retried int, err error) time.Duration { return 20 * time.Millisecond },
		OnDeadLetter: func(ctx context.Context, info *TaskInfo, err error) {
			deadLettered.Add(1)
		},
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, poisonArgs{ID: 2}, WithMaxRetry(1), WithDeadLetterDiscard())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	eventually(t, 10*time.Second, func() bool {
		return deadLettered.Load() == 1
	}, "discard-mode task should still fire OnDeadLetter")

	// Discard: not archived, and the task hash is deleted.
	time.Sleep(300 * time.Millisecond)
	if client.ZScore(ctx, base.ArchivedKey("default"), info.ID).Err() == nil {
		t.Error("discard-mode task must NOT be in the archived ZSET")
	}
	exists, _ := client.Exists(ctx, base.TaskKey("default", info.ID)).Result()
	if exists != 0 {
		t.Error("discard-mode task hash should be deleted")
	}
}
```

- [ ] **Step 2: 통합 테스트 통과 확인**

Run: `go test . -run 'TestIntegration_PoisonPillConvergesToDeadLetter|TestIntegration_DiscardModeSkipsArchiveButFiresHook' -race -v`
Expected: PASS

- [ ] **Step 3: 전체 스위트 + race + 정적 검증**

Run: `make check`
Expected: `gofmt` 클린, `go vet` 클린, `go test ./... -race -p 1` 전 패키지 PASS.

- [ ] **Step 4: 커밋**

```bash
git add reliability_integration_test.go
git commit -m "test: poison pill 수렴 + discard 모드 dead-letter 통합 테스트"
```

---

## M2 완료 기준

- [ ] `make check` 전부 통과 (실 Redis 연결 시)
- [ ] 핸들러 에러 → 지수 백오프로 retry ZSET → forwarder가 스트림으로 되돌려 재실행
- [ ] `SkipRetry` 에러 → 즉시 dead-letter + 훅
- [ ] panic → 복구되어 서버 중단 없이 재시도
- [ ] 워커 crash(미ACK PEL) → recoverer가 회수해 재실행
- [ ] poison pill(항상 실패) → 정확히 `MaxRetry+1`회 실행 후 archived + 훅 1회, 무한 재실행 없음
- [ ] `WithDeadLetterDiscard` → archived 저장 없이 훅만 발화 + task hash 삭제

**다음 단계:** M2 착지 후 M3(지연 실행 scheduled ZSET + `WithProcessIn` + unique 락) 계획을 실제 타입 기반으로 작성한다. M3의 forwarder는 이 계획의 `ForwardRetry`를 일반화(scheduled ZSET도 승격)하는 형태가 자연스럽다.
