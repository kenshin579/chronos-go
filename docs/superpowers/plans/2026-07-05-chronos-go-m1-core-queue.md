# chronos-go M1 (코어 큐) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redis Streams Consumer Group 기반의 즉시 실행 태스크 큐 코어를 구축한다 — 태스크를 제네릭 타입 안전 API로 enqueue하면 워커가 등록된 핸들러로 처리한다.

**Architecture:** Client가 태스크를 JSON으로 직렬화해 태스크 HASH에 본문을 저장하고 Stream에 task_id를 XADD한다. Server의 processor가 `XREADGROUP BLOCK`으로 소비해 kind→핸들러 라우팅으로 실행하고 `XACK`한다. 재시도·지연·리더선출은 이후 마일스톤(M2~M5)에서 이 토대 위에 얹는다.

**Tech Stack:** Go 1.26, `github.com/redis/go-redis/v9`, `github.com/google/uuid`, 표준 `testing` + 실제 Redis 테스트 하니스(`internal/testutil`).

**설계 문서:** `docs/superpowers/specs/2026-07-05-multi-cluster-scheduler-design.md` (섹션 2~4)

**M1 범위 경계 (이 계획에 포함되지 않는 것):**
- 재시도/백오프, retry ZSET → M2
- 크래시 복구(XAUTOCLAIM/recoverer), dead-letter → M2
- 지연 실행(scheduled ZSET), unique 락 → M3
- 리더 선출, 스케줄러(cron/interval) → M4
- Inspector, CLI, 메트릭 → M5
- 큐 가중치(weighted priority): M1은 등록된 모든 큐를 균등하게 읽음. strict 가중치는 이후.
- M1에서 핸들러가 에러를 반환하면 재시도 없이 XACK 후 태스크를 삭제하고 로깅만 한다(M2에서 retry 라우팅으로 교체).

---

## File Structure

| 파일 | 책임 |
|---|---|
| `go.mod` | 모듈 정의, 의존성 |
| `internal/base/keys.go` | Redis 키 빌더 (hash tag로 Cluster 호환) |
| `internal/base/task.go` | `TaskMessage` 구조체, 상태 상수, JSON 인코딩/디코딩 |
| `internal/rdb/rdb.go` | `RDB` 래퍼 + Enqueue/Dequeue/Done Redis 연산 |
| `internal/testutil/redis.go` | 테스트용 Redis 연결 하니스 (import 가능하도록 비-_test.go) |
| `chronos.go` | 공개 API: `TaskArgs`, `Task[T]`, `TaskInfo`, `Client`, `Enqueue[T]`, 옵션 |
| `handler.go` | `Mux`, `AddHandler[T]`, kind 라우팅 |
| `server.go` | `Server`, `ServerConfig`, processor 루프, `Start`/`Shutdown` |

**의존 순서:** base → rdb → (chronos, handler) → server. 각 태스크는 이 순서를 따른다.

**테스트 방침:** miniredis는 Streams Consumer Group / XAUTOCLAIM 지원이 불완전하므로, asynq와 동일하게 **실제 Redis**에 대해 테스트한다. `internal/testutil.NewRedis(t)`가 `REDIS_ADDR`(기본 `127.0.0.1:6379`)의 DB 15에 연결하고, Redis가 없으면 `t.Skip`, 있으면 각 테스트 전후로 `FlushDB`한다.

---

## Task 1: 모듈 초기화 + 테스트 Redis 하니스

**Files:**
- Create: `go.mod`
- Create: `internal/testutil/redis.go`
- Test: `internal/testutil/redis_test.go`

- [ ] **Step 1: 모듈 초기화 및 의존성 추가**

Run:
```bash
cd /Users/user/GolandProjects/chronos-go
go mod init github.com/kenshin579/chronos-go
go get github.com/redis/go-redis/v9@latest
go get github.com/google/uuid@latest
```
Expected: `go.mod`에 `module github.com/kenshin579/chronos-go`, `go 1.26`, require에 go-redis/v9와 uuid가 추가됨.

- [ ] **Step 2: 테스트 Redis 하니스 작성**

Create `internal/testutil/redis.go`:
```go
// Package testutil provides shared test helpers for chronos-go.
package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestDB is the Redis logical database dedicated to tests.
const TestDB = 15

// NewRedis connects to a test Redis instance and returns a client whose
// database is flushed before the test and cleaned up afterwards. If no Redis
// is reachable the test is skipped rather than failed.
func NewRedis(t *testing.T) redis.UniversalClient {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr, DB: TestDB})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("redis not available at %s: %v", addr, err)
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush test db: %v", err)
	}

	t.Cleanup(func() {
		_ = client.FlushDB(ctx).Err()
		_ = client.Close()
	})

	return client
}
```

- [ ] **Step 3: 하니스 동작 테스트 작성 (실패 확인용)**

Create `internal/testutil/redis_test.go`:
```go
package testutil

import (
	"context"
	"testing"
)

func TestNewRedis_PingSucceeds(t *testing.T) {
	client := NewRedis(t)
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `go test ./internal/testutil/ -run TestNewRedis_PingSucceeds -v`
Expected: PASS (Redis가 로컬에 없으면 SKIP — 이 경우 `docker run -d -p 6379:6379 redis:7`로 띄운 뒤 재실행).

- [ ] **Step 5: 커밋**

```bash
git add go.mod go.sum internal/testutil/
git commit -m "chore: 모듈 초기화 및 테스트 Redis 하니스"
```

---

## Task 2: base — Redis 키 빌더

**Files:**
- Create: `internal/base/keys.go`
- Test: `internal/base/keys_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/base/keys_test.go`:
```go
package base

import "testing"

func TestKeyBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"queue prefix", QueueKeyPrefix("default"), "chronos:{default}:"},
		{"stream", StreamKey("default"), "chronos:{default}:stream"},
		{"task", TaskKey("default", "abc"), "chronos:{default}:t:abc"},
		{"queues set", QueuesKey(), "chronos:queues"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/base/ -run TestKeyBuilders -v`
Expected: FAIL — `undefined: QueueKeyPrefix` 등 컴파일 에러.

- [ ] **Step 3: 키 빌더 구현**

Create `internal/base/keys.go`:
```go
// Package base defines the Redis key layout, task states, and message
// serialization shared across chronos-go internals.
package base

// QueueKeyPrefix returns the common prefix for all keys of a queue. The queue
// name is wrapped in a Redis Cluster hash tag ({...}) so that every key of a
// single queue hashes to the same slot, allowing multi-key Lua scripts to run
// on a cluster.
func QueueKeyPrefix(qname string) string {
	return "chronos:{" + qname + "}:"
}

// StreamKey returns the Stream key holding task IDs ready for immediate
// execution (consumed via a consumer group).
func StreamKey(qname string) string {
	return QueueKeyPrefix(qname) + "stream"
}

// TaskKey returns the HASH key holding a task's body and state.
func TaskKey(qname, id string) string {
	return QueueKeyPrefix(qname) + "t:" + id
}

// QueuesKey returns the SET key listing all known queue names. It has no hash
// tag on purpose: it is a global index touched by a standalone command, never
// inside a per-queue multi-key script.
func QueuesKey() string {
	return "chronos:queues"
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/base/ -run TestKeyBuilders -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/base/keys.go internal/base/keys_test.go
git commit -m "feat: base Redis 키 빌더 (Cluster hash tag)"
```

---

## Task 3: base — TaskMessage 및 상태, 직렬화

**Files:**
- Create: `internal/base/task.go`
- Test: `internal/base/task_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/base/task_test.go`:
```go
package base

import "testing"

func TestEncodeDecodeMessage_RoundTrip(t *testing.T) {
	msg := &TaskMessage{
		ID:      "task-1",
		Kind:    "email:send",
		Payload: []byte(`{"user_id":"u1"}`),
		Queue:   "default",
		State:   StatePending,
	}

	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.ID != msg.ID || got.Kind != msg.Kind || got.Queue != msg.Queue {
		t.Errorf("round trip mismatch: got %+v want %+v", got, msg)
	}
	if string(got.Payload) != string(msg.Payload) {
		t.Errorf("payload = %q, want %q", got.Payload, msg.Payload)
	}
	if got.State != StatePending {
		t.Errorf("state = %v, want StatePending", got.State)
	}
}

func TestTaskState_String(t *testing.T) {
	if StateActive.String() != "active" {
		t.Errorf("StateActive.String() = %q, want %q", StateActive.String(), "active")
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/base/ -run 'TestEncodeDecodeMessage_RoundTrip|TestTaskState_String' -v`
Expected: FAIL — `undefined: TaskMessage` 등.

- [ ] **Step 3: TaskMessage와 상태 구현**

Create `internal/base/task.go`:
```go
package base

import "encoding/json"

// TaskState is the lifecycle stage of a task.
type TaskState int

const (
	StatePending   TaskState = iota + 1 // in the stream, awaiting a worker
	StateActive                         // read by a worker (in the consumer group PEL)
	StateCompleted                      // finished successfully
	StateRetry                          // failed, awaiting retry (M2)
	StateArchived                       // dead-letter (M2)
	StateScheduled                      // delayed, awaiting its time (M3)
)

// String returns the lowercase name of the state.
func (s TaskState) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateActive:
		return "active"
	case StateCompleted:
		return "completed"
	case StateRetry:
		return "retry"
	case StateArchived:
		return "archived"
	case StateScheduled:
		return "scheduled"
	default:
		return "unknown"
	}
}

// TaskMessage is the canonical, serialized representation of a task stored in
// the task HASH. Later milestones extend this struct (Retried, MaxRetry, etc.)
// — only fields needed for immediate execution are present in M1.
type TaskMessage struct {
	ID      string    `json:"id"`
	Kind    string    `json:"kind"`
	Payload []byte    `json:"payload"`
	Queue   string    `json:"queue"`
	State   TaskState `json:"state"`
}

// EncodeMessage serializes a TaskMessage for storage in Redis.
func EncodeMessage(m *TaskMessage) ([]byte, error) {
	return json.Marshal(m)
}

// DecodeMessage deserializes a TaskMessage read from Redis.
func DecodeMessage(b []byte) (*TaskMessage, error) {
	var m TaskMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/base/ -v`
Expected: PASS (Task 2·3의 모든 테스트).

- [ ] **Step 5: 커밋**

```bash
git add internal/base/task.go internal/base/task_test.go
git commit -m "feat: base TaskMessage와 상태 정의, JSON 직렬화"
```

---

## Task 4: rdb — Enqueue

**Files:**
- Create: `internal/rdb/rdb.go`
- Test: `internal/rdb/rdb_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/rdb_test.go`:
```go
package rdb

import (
	"context"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestEnqueue_StoresBodyAndPushesToStream(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{
		ID:      "task-1",
		Kind:    "email:send",
		Payload: []byte(`{"user_id":"u1"}`),
		Queue:   "default",
	}

	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Task body is stored in the task HASH with state=pending.
	got, err := client.HGet(ctx, base.TaskKey("default", "task-1"), "msg").Result()
	if err != nil {
		t.Fatalf("hget msg: %v", err)
	}
	decoded, err := base.DecodeMessage([]byte(got))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Kind != "email:send" {
		t.Errorf("kind = %q, want %q", decoded.Kind, "email:send")
	}

	// Stream has exactly one entry.
	n, err := client.XLen(ctx, base.StreamKey("default")).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	if n != 1 {
		t.Errorf("stream length = %d, want 1", n)
	}

	// Queue name is registered.
	isMember, err := client.SIsMember(ctx, base.QueuesKey(), "default").Result()
	if err != nil {
		t.Fatalf("sismember: %v", err)
	}
	if !isMember {
		t.Error("queue 'default' not registered in queues set")
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run TestEnqueue_StoresBodyAndPushesToStream -v`
Expected: FAIL — `undefined: NewRDB`.

- [ ] **Step 3: RDB와 Enqueue 구현**

Create `internal/rdb/rdb.go`:
```go
// Package rdb implements the Redis operations backing chronos-go: enqueueing
// tasks, dequeueing via a consumer group, and acking completion.
package rdb

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// ConsumerGroup is the single consumer group name used on every queue stream.
const ConsumerGroup = "chronos"

// RDB wraps a Redis client with chronos-go's task operations.
type RDB struct {
	client redis.UniversalClient
}

// NewRDB returns an RDB backed by the given Redis client.
func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{client: client}
}

// Client exposes the underlying Redis client (used by higher layers for
// consumer-group setup and shutdown).
func (r *RDB) Client() redis.UniversalClient {
	return r.client
}

// enqueueCmd atomically stores the task body and appends its ID to the stream.
// KEYS[1] task hash, KEYS[2] stream. ARGV[1] encoded msg, ARGV[2] state, ARGV[3] task id.
var enqueueCmd = redis.NewScript(`
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("XADD", KEYS[2], "*", "task_id", ARGV[3])
return 1
`)

// Enqueue stores a task and makes it immediately available for processing.
func (r *RDB) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	msg.State = base.StatePending
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}

	// Register the queue name in the global index. Separate from the atomic
	// script because QueuesKey has no hash tag (different cluster slot).
	if err := r.client.SAdd(ctx, base.QueuesKey(), msg.Queue).Err(); err != nil {
		return err
	}

	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.StreamKey(msg.Queue),
	}
	argv := []interface{}{encoded, int(base.StatePending), msg.ID}
	return enqueueCmd.Run(ctx, r.client, keys, argv...).Err()
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/rdb/ -run TestEnqueue_StoresBodyAndPushesToStream -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/
git commit -m "feat: rdb Enqueue (원자적 HASH 저장 + Stream XADD)"
```

---

## Task 5: rdb — 컨슈머 그룹 보장 + Dequeue

**Files:**
- Modify: `internal/rdb/rdb.go` (Dequeue, EnsureGroup 추가)
- Test: `internal/rdb/rdb_test.go` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`internal/rdb/rdb_test.go`에 추가:
```go
func TestDequeue_ReturnsEnqueuedTask(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	msg := &base.TaskMessage{ID: "task-1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, streamID, err := r.Dequeue(ctx, "consumer-1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got.ID != "task-1" {
		t.Errorf("dequeued id = %q, want task-1", got.ID)
	}
	if streamID == "" {
		t.Error("expected non-empty stream id")
	}
	if got.State != base.StateActive {
		t.Errorf("state = %v, want active", got.State)
	}
}

func TestDequeue_EmptyReturnsErrNoTask(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	_, _, err := r.Dequeue(ctx, "consumer-1", 0, "default")
	if err != ErrNoTask {
		t.Errorf("err = %v, want ErrNoTask", err)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run TestDequeue -v`
Expected: FAIL — `undefined: EnsureGroup`, `ErrNoTask`, `Dequeue`.

- [ ] **Step 3: EnsureGroup, Dequeue, ErrNoTask 구현**

`internal/rdb/rdb.go`의 import에 `errors`와 `time`을 추가하고, 파일 끝에 추가:
```go
// ErrNoTask is returned by Dequeue when no task is available within the block
// duration.
var ErrNoTask = errors.New("chronos: no task available")

// EnsureGroup creates the consumer group on a queue's stream if it does not
// already exist. MKSTREAM creates the stream too, so this is safe to call
// before any task has been enqueued.
func (r *RDB) EnsureGroup(ctx context.Context, qname string) error {
	err := r.client.XGroupCreateMkStream(ctx, base.StreamKey(qname), ConsumerGroup, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Dequeue reads one task from the given queues using the consumer group. block
// is the max duration to wait for a task (0 means return immediately). It
// returns ErrNoTask when nothing is available. The task's state is set to
// active. streamID identifies the stream entry for later acking.
func (r *RDB) Dequeue(ctx context.Context, consumer string, block time.Duration, qnames ...string) (*base.TaskMessage, string, error) {
	// Build STREAMS argument: all stream keys, then one ">" per key.
	streams := make([]string, 0, len(qnames)*2)
	for _, q := range qnames {
		streams = append(streams, base.StreamKey(q))
	}
	for range qnames {
		streams = append(streams, ">")
	}

	res, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: consumer,
		Streams:  streams,
		Count:    1,
		Block:    block,
	}).Result()
	if err == redis.Nil {
		return nil, "", ErrNoTask
	}
	if err != nil {
		return nil, "", err
	}
	if len(res) == 0 || len(res[0].Messages) == 0 {
		return nil, "", ErrNoTask
	}

	stream := res[0]
	entry := stream.Messages[0]
	qname := qnameFromStreamKey(stream.Stream)
	taskID, _ := entry.Values["task_id"].(string)

	raw, err := r.client.HGet(ctx, base.TaskKey(qname, taskID), "msg").Result()
	if err == redis.Nil {
		// Orphan stream entry (body already gone): ack and report no task.
		_ = r.client.XAck(ctx, stream.Stream, ConsumerGroup, entry.ID).Err()
		return nil, "", ErrNoTask
	}
	if err != nil {
		return nil, "", err
	}

	msg, err := base.DecodeMessage([]byte(raw))
	if err != nil {
		return nil, "", err
	}
	msg.State = base.StateActive
	if err := r.client.HSet(ctx, base.TaskKey(qname, taskID), "state", int(base.StateActive)).Err(); err != nil {
		return nil, "", err
	}

	return msg, entry.ID, nil
}

// qnameFromStreamKey extracts the queue name from a stream key of the form
// "chronos:{<qname>}:stream".
func qnameFromStreamKey(streamKey string) string {
	start := len("chronos:{")
	end := len(streamKey) - len("}:stream")
	if start >= end {
		return ""
	}
	return streamKey[start:end]
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/rdb/ -v`
Expected: PASS (Task 4·5 전부).

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/
git commit -m "feat: rdb EnsureGroup + Dequeue (XREADGROUP)"
```

---

## Task 6: rdb — Done (XACK + 삭제)

**Files:**
- Modify: `internal/rdb/rdb.go` (Done 추가)
- Test: `internal/rdb/rdb_test.go` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`internal/rdb/rdb_test.go`에 추가:
```go
func TestDone_AcksAndDeletesTask(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	msg := &base.TaskMessage{ID: "task-1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, streamID, err := r.Dequeue(ctx, "consumer-1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	if err := r.Done(ctx, "default", streamID, "task-1"); err != nil {
		t.Fatalf("done: %v", err)
	}

	// Task hash is gone.
	exists, err := client.Exists(ctx, base.TaskKey("default", "task-1")).Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists != 0 {
		t.Error("task hash should be deleted after Done")
	}

	// PEL is empty (the entry was acked).
	pending, err := client.XPending(ctx, base.StreamKey("default"), ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 0 {
		t.Errorf("pending count = %d, want 0", pending.Count)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run TestDone_AcksAndDeletesTask -v`
Expected: FAIL — `undefined: Done`.

- [ ] **Step 3: Done 구현**

`internal/rdb/rdb.go` 끝에 추가:
```go
// Done acknowledges a successfully processed task: it acks the stream entry
// (removing it from the PEL) and deletes the task body. In M1 there is no
// completed-retention; later milestones may keep the body in a completed ZSET.
func (r *RDB) Done(ctx context.Context, qname, streamID, taskID string) error {
	pipe := r.client.TxPipeline()
	pipe.XAck(ctx, base.StreamKey(qname), ConsumerGroup, streamID)
	pipe.Del(ctx, base.TaskKey(qname, taskID))
	_, err := pipe.Exec(ctx)
	return err
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test ./internal/rdb/ -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/
git commit -m "feat: rdb Done (XACK + 태스크 삭제)"
```

---

## Task 7: 공개 API — TaskArgs, Task[T], Client, Enqueue[T]

**Files:**
- Create: `chronos.go`
- Test: `chronos_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `chronos_test.go`:
```go
package chronos

import (
	"context"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

type emailArgs struct {
	UserID string `json:"user_id"`
}

func (emailArgs) Kind() string { return "email:send" }

func TestEnqueue_DefaultQueue(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	info, err := Enqueue(context.Background(), c, emailArgs{UserID: "u1"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if info.Kind != "email:send" {
		t.Errorf("kind = %q, want email:send", info.Kind)
	}
	if info.Queue != "default" {
		t.Errorf("queue = %q, want default", info.Queue)
	}
	if info.ID == "" {
		t.Error("expected generated task id")
	}

	n, err := client.XLen(context.Background(), base.StreamKey("default")).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	if n != 1 {
		t.Errorf("stream length = %d, want 1", n)
	}
}

func TestEnqueue_WithQueueAndTaskID(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	info, err := Enqueue(context.Background(), c, emailArgs{UserID: "u2"},
		WithQueue("critical"), WithTaskID("fixed-id"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if info.Queue != "critical" {
		t.Errorf("queue = %q, want critical", info.Queue)
	}
	if info.ID != "fixed-id" {
		t.Errorf("id = %q, want fixed-id", info.ID)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run TestEnqueue -v`
Expected: FAIL — `undefined: NewClient` 등.

- [ ] **Step 3: 공개 API 구현**

Create `chronos.go`:
```go
// Package chronos is a Redis-backed distributed scheduler and task queue.
package chronos

import (
	"context"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// DefaultQueue is the queue used when none is specified.
const DefaultQueue = "default"

// TaskArgs is implemented by every task payload type. Kind returns a stable
// identifier used to route the task to its handler; it MUST be defined on a
// value receiver so it can be called on the zero value during registration.
type TaskArgs interface {
	Kind() string
}

// Task is a strongly-typed task delivered to a handler.
type Task[T TaskArgs] struct {
	Args T

	id    string
	queue string
}

// ID returns the task's unique identifier.
func (t *Task[T]) ID() string { return t.id }

// Queue returns the queue the task was enqueued to.
func (t *Task[T]) Queue() string { return t.queue }

// TaskInfo describes an enqueued task returned by Enqueue.
type TaskInfo struct {
	ID    string
	Kind  string
	Queue string
}

// Client enqueues tasks.
type Client struct {
	rdb *rdb.RDB
}

// NewClient returns a Client backed by the given Redis client.
func NewClient(r redis.UniversalClient) *Client {
	return &Client{rdb: rdb.NewRDB(r)}
}

// Close releases the client's resources. The underlying Redis client is owned
// by the caller and is not closed here.
func (c *Client) Close() error { return nil }

// enqueueOptions holds resolved enqueue-time settings.
type enqueueOptions struct {
	queue  string
	taskID string
}

// Option customizes a single Enqueue call.
type Option interface {
	apply(*enqueueOptions)
}

type optionFunc func(*enqueueOptions)

func (f optionFunc) apply(o *enqueueOptions) { f(o) }

// WithQueue routes the task to a specific queue.
func WithQueue(name string) Option {
	return optionFunc(func(o *enqueueOptions) { o.queue = name })
}

// WithTaskID sets an explicit task ID (used for deduplication). When omitted a
// random UUID is generated.
func WithTaskID(id string) Option {
	return optionFunc(func(o *enqueueOptions) { o.taskID = id })
}

// Enqueue serializes args and makes the task available for immediate
// processing. It is a package-level function rather than a method because Go
// methods cannot have type parameters.
func Enqueue[T TaskArgs](ctx context.Context, c *Client, args T, opts ...Option) (*TaskInfo, error) {
	options := enqueueOptions{queue: DefaultQueue}
	for _, opt := range opts {
		opt.apply(&options)
	}

	id := options.taskID
	if id == "" {
		id = uuid.NewString()
	}

	payload, err := encodeArgs(args)
	if err != nil {
		return nil, err
	}

	msg := &base.TaskMessage{
		ID:      id,
		Kind:    args.Kind(),
		Payload: payload,
		Queue:   options.queue,
	}
	if err := c.rdb.Enqueue(ctx, msg); err != nil {
		return nil, err
	}

	return &TaskInfo{ID: id, Kind: msg.Kind, Queue: msg.Queue}, nil
}
```

- [ ] **Step 4: encodeArgs 헬퍼 작성**

Create `codec.go`:
```go
package chronos

import "encoding/json"

// encodeArgs serializes task args to the payload stored in Redis. JSON is the
// default codec (chosen for redis-cli debuggability); a pluggable Marshaler is
// a later enhancement.
func encodeArgs[T TaskArgs](args T) ([]byte, error) {
	return json.Marshal(args)
}

// decodeArgs deserializes a payload into a task args value.
func decodeArgs[T TaskArgs](payload []byte) (T, error) {
	var args T
	err := json.Unmarshal(payload, &args)
	return args, err
}
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `go test . -run TestEnqueue -v`
Expected: PASS

- [ ] **Step 6: 커밋**

```bash
git add chronos.go codec.go chronos_test.go
git commit -m "feat: 공개 API — TaskArgs, Task[T], Client, 제네릭 Enqueue"
```

---

## Task 8: 핸들러 — Mux, AddHandler[T]

**Files:**
- Create: `handler.go`
- Test: `handler_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `handler_test.go`:
```go
package chronos

import (
	"context"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
)

func TestMux_RoutesToTypedHandler(t *testing.T) {
	mux := NewMux()

	var gotUser string
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		gotUser = task.Args.UserID
		return nil
	})

	payload, err := encodeArgs(emailArgs{UserID: "u42"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg := &base.TaskMessage{ID: "t1", Kind: "email:send", Payload: payload, Queue: "default"}

	if err := mux.dispatch(context.Background(), msg); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if gotUser != "u42" {
		t.Errorf("handler received user %q, want u42", gotUser)
	}
}

func TestMux_UnknownKindErrors(t *testing.T) {
	mux := NewMux()
	msg := &base.TaskMessage{ID: "t1", Kind: "nope", Payload: []byte("{}"), Queue: "default"}
	if err := mux.dispatch(context.Background(), msg); err == nil {
		t.Error("expected error for unregistered kind")
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run TestMux -v`
Expected: FAIL — `undefined: NewMux`.

- [ ] **Step 3: Mux와 AddHandler 구현**

Create `handler.go`:
```go
package chronos

import (
	"context"
	"fmt"

	"github.com/kenshin579/chronos-go/internal/base"
)

// internalHandler is the type-erased handler stored in the Mux. Each one
// decodes the payload into a concrete args type before calling the user's
// typed handler.
type internalHandler func(ctx context.Context, msg *base.TaskMessage) error

// Mux routes tasks to handlers by their Kind.
type Mux struct {
	handlers map[string]internalHandler
}

// NewMux returns an empty Mux.
func NewMux() *Mux {
	return &Mux{handlers: make(map[string]internalHandler)}
}

// AddHandler registers a strongly-typed handler for tasks of type T. The Kind
// is read from the zero value of T, so T's Kind method must use a value
// receiver. Registering two handlers for the same Kind panics.
func AddHandler[T TaskArgs](mux *Mux, fn func(ctx context.Context, task *Task[T]) error) {
	var zero T
	kind := zero.Kind()
	if _, exists := mux.handlers[kind]; exists {
		panic(fmt.Sprintf("chronos: handler already registered for kind %q", kind))
	}
	mux.handlers[kind] = func(ctx context.Context, msg *base.TaskMessage) error {
		args, err := decodeArgs[T](msg.Payload)
		if err != nil {
			return fmt.Errorf("chronos: decode payload for kind %q: %w", kind, err)
		}
		return fn(ctx, &Task[T]{Args: args, id: msg.ID, queue: msg.Queue})
	}
}

// dispatch routes a message to its registered handler.
func (mux *Mux) dispatch(ctx context.Context, msg *base.TaskMessage) error {
	h, ok := mux.handlers[msg.Kind]
	if !ok {
		return fmt.Errorf("chronos: no handler registered for kind %q", msg.Kind)
	}
	return h(ctx, msg)
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `go test . -run TestMux -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add handler.go handler_test.go
git commit -m "feat: Mux와 제네릭 AddHandler (kind 라우팅)"
```

---

## Task 9: 서버 — processor 루프, Start/Shutdown

**Files:**
- Create: `server.go`
- Test: `server_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `server_test.go`:
```go
package chronos

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_ProcessesEnqueuedTask(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var (
		mu      sync.Mutex
		gotUser string
		done    = make(chan struct{})
	)
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		mu.Lock()
		gotUser = task.Args.UserID
		mu.Unlock()
		close(done)
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 4,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u99"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not invoked within 5s")
	}

	mu.Lock()
	defer mu.Unlock()
	if gotUser != "u99" {
		t.Errorf("handler received user %q, want u99", gotUser)
	}
}

func TestServer_ShutdownIsClean(t *testing.T) {
	client := testutil.NewRedis(t)
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 2})
	if err := srv.Start(context.Background(), NewMux()); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Shutdown should return promptly without deadlock.
	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run TestServer -v`
Expected: FAIL — `undefined: NewServer`.

- [ ] **Step 3: Server 구현**

Create `server.go`:
```go
package chronos

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/rdb"
)

// pollBlock is how long a fetch blocks on XREADGROUP before looping. Kept short
// so Shutdown stays responsive.
const pollBlock = 1 * time.Second

// ServerConfig configures a Server.
type ServerConfig struct {
	// Queues maps queue name to weight. In M1 only the keys are used (all
	// queues are read equally); weighted priority is a later enhancement.
	Queues map[string]int
	// Concurrency is the max number of tasks processed simultaneously.
	Concurrency int
	// Logger receives operational logs. Defaults to slog.Default().
	Logger *slog.Logger
}

// Server fetches tasks from Redis and dispatches them to handlers.
type Server struct {
	rdb      *rdb.RDB
	cfg      ServerConfig
	consumer string
	logger   *slog.Logger

	mux    *Mux
	sem    chan struct{}
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewServer returns a Server backed by the given Redis client.
func NewServer(r redis.UniversalClient, cfg ServerConfig) *Server {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		rdb:      rdb.NewRDB(r),
		cfg:      cfg,
		consumer: uuid.NewString(),
		logger:   logger,
		sem:      make(chan struct{}, cfg.Concurrency),
	}
}

// queueNames returns the configured queue names.
func (s *Server) queueNames() []string {
	names := make([]string, 0, len(s.cfg.Queues))
	for q := range s.cfg.Queues {
		names = append(names, q)
	}
	return names
}

// Start ensures consumer groups exist and launches the fetch loop. It returns
// once startup is complete; processing continues in the background until
// Shutdown is called or ctx is cancelled.
func (s *Server) Start(ctx context.Context, mux *Mux) error {
	s.mux = mux
	for _, q := range s.queueNames() {
		if err := s.rdb.EnsureGroup(ctx, q); err != nil {
			return err
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.wg.Add(1)
	go s.fetchLoop(runCtx)
	return nil
}

// fetchLoop repeatedly dequeues tasks and hands them to worker goroutines,
// bounded by the concurrency semaphore.
func (s *Server) fetchLoop(ctx context.Context) {
	defer s.wg.Done()
	queues := s.queueNames()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Acquire a concurrency slot before fetching so we never hold a task
		// without a worker to run it.
		select {
		case s.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		msg, streamID, err := s.rdb.Dequeue(ctx, s.consumer, pollBlock, queues...)
		if err == rdb.ErrNoTask {
			<-s.sem
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				<-s.sem
				return
			}
			s.logger.Error("chronos: dequeue failed", "error", err)
			<-s.sem
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-s.sem }()
			s.process(ctx, msg.Queue, streamID, msg)
		}()
	}
}

// process runs the handler for one task and acks it. In M1 a handler error is
// logged and the task is acked+deleted (no retry). M2 replaces this with retry
// routing.
func (s *Server) process(ctx context.Context, qname, streamID string, msg *base.TaskMessage) {
	if err := s.mux.dispatch(ctx, msg); err != nil {
		s.logger.Error("chronos: task failed",
			"kind", msg.Kind, "id", msg.ID, "error", err)
	}
	if err := s.rdb.Done(ctx, qname, streamID, msg.ID); err != nil {
		s.logger.Error("chronos: ack failed", "id", msg.ID, "error", err)
	}
}

// Shutdown stops fetching and waits for in-flight tasks to finish, bounded by
// ctx. In-flight tasks that do not finish before ctx expires are left unacked
// and will be recovered by another instance in a later milestone.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

- [ ] **Step 4: base import 누락 수정**

`server.go`의 `process` 메서드가 `*base.TaskMessage`를 참조하므로 import 블록에 base를 추가한다:
```go
	"github.com/kenshin579/chronos-go/internal/base"
```
(import 블록의 rdb 줄 위에 알파벳 순으로 배치.)

- [ ] **Step 5: 테스트 통과 확인**

Run: `go test . -run TestServer -v`
Expected: PASS

- [ ] **Step 6: 커밋**

```bash
git add server.go server_test.go
git commit -m "feat: Server processor 루프 + Start/Shutdown"
```

---

## Task 10: 엔드투엔드 통합 테스트 + 전체 검증

**Files:**
- Create: `integration_test.go`

- [ ] **Step 1: 여러 태스크 처리 통합 테스트 작성**

Create `integration_test.go`:
```go
package chronos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type counterArgs struct {
	N int `json:"n"`
}

func (counterArgs) Kind() string { return "test:counter" }

func TestEndToEnd_ProcessesManyTasksAcrossQueues(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	const total = 50
	var processed int64
	var wg sync.WaitGroup
	wg.Add(total)

	mux := NewMux()
	seen := make(map[int]bool)
	var seenMu sync.Mutex
	AddHandler(mux, func(ctx context.Context, task *Task[counterArgs]) error {
		seenMu.Lock()
		seen[task.Args.N] = true
		seenMu.Unlock()
		atomic.AddInt64(&processed, 1)
		wg.Done()
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1, "critical": 1},
		Concurrency: 8,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	for i := 0; i < total; i++ {
		q := "default"
		if i%2 == 0 {
			q = "critical"
		}
		if _, err := Enqueue(ctx, c, counterArgs{N: i}, WithQueue(q)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	waitCh := make(chan struct{})
	go func() { wg.Wait(); close(waitCh) }()
	select {
	case <-waitCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("only %d/%d tasks processed within 15s", atomic.LoadInt64(&processed), total)
	}

	if got := atomic.LoadInt64(&processed); got != total {
		t.Errorf("processed = %d, want %d", got, total)
	}
	seenMu.Lock()
	if len(seen) != total {
		t.Errorf("distinct tasks seen = %d, want %d", len(seen), total)
	}
	seenMu.Unlock()
}
```

- [ ] **Step 2: 통합 테스트 실행하여 통과 확인**

Run: `go test . -run TestEndToEnd -v`
Expected: PASS — 50개 태스크가 두 큐에 걸쳐 모두, 중복 없이 처리됨.

- [ ] **Step 3: 전체 테스트 스위트 + race 검출 실행**

Run: `go test ./... -race`
Expected: 전 패키지 PASS (Redis 없으면 SKIP). race 경고 없음.

- [ ] **Step 4: 정적 검증**

Run:
```bash
go vet ./...
gofmt -l .
```
Expected: `go vet` 출력 없음, `gofmt -l` 출력 없음(포맷 이슈 파일 없음).

- [ ] **Step 5: 커밋**

```bash
git add integration_test.go
git commit -m "test: 다중 큐 엔드투엔드 통합 테스트"
```

---

## M1 완료 기준

- [ ] `go test ./... -race` 전부 통과 (실 Redis 연결 시)
- [ ] 태스크를 `Enqueue`하면 워커가 등록된 타입 안전 핸들러로 처리한다
- [ ] 여러 큐에 걸친 다수 태스크가 중복·유실 없이 처리된다
- [ ] `go vet`, `gofmt` 클린

**다음 단계:** M1 착지 후, 실제 구현된 타입·시그니처를 바탕으로 M2(재시도 + 크래시 복구) 계획을 별도로 작성한다. M2는 이 계획의 `process` 메서드(에러 시 삭제)를 retry ZSET 라우팅으로 교체하고, recoverer(XAUTOCLAIM)와 dead-letter를 추가한다.
