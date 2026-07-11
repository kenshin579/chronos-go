# Task Chain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `NewChain().Then(...).Enqueue()` 빌더로 태스크 연쇄(A 성공 → B → C)를 지원한다. 실패 시 중단, dead-letter `RunTask` 재실행 시 재개.

**Architecture:** 후속 링크를 `TaskMessage.Chain`에 내장(재개 공짜). 성공 경로에서 "후속 enqueue(결정적 ID `<chainID>:<i>` + create-if-absent Lua) → Done" 순서로 at-least-once 중복을 차단. 체인 링크의 `WithUnique`/`WithTaskID`는 명시적 거부(스펙 확정).

**Tech Stack:** Go, redis/go-redis v9, Lua(기존 패턴), 실제 Redis(DB 15, `-p 1`), docker cluster(스모크 1개 추가).

---

## File Structure

- Modify `internal/base/task.go` — `ChainLink` 타입 + `TaskMessage.Chain/ChainID/ChainIndex`. Test: `internal/base/task_test.go`
- Create `internal/rdb/chain.go` — `EnqueueChainLink` + `chainEnqueueCmd`/`chainScheduleCmd`. Test: `internal/rdb/chain_test.go`
- Create `chain.go` (루트) — `Chain` 빌더 + 검증 + msg 조립. `chronos.go`의 dispatch 부분 재사용을 위한 소규모 리팩터 포함.
- Modify `chronos.go` — `Enqueue`의 dispatch 스위치를 `dispatchMessage`로 추출(동작 무변화), `TaskInfo.ChainPending` 추가.
- Modify `inspector.go` — `taskInfoFromMsg`에 ChainPending 매핑.
- Modify `server.go` — 성공 경로에 `enqueueNext` 삽입(후속 enqueue → Done 순서).
- Create `chain_test.go` (루트) — 빌더 검증/순차 실행/중단·재개/ChainPending.
- Modify `cluster_test.go` — 14번째 스모크 + 체크리스트.
- Modify `examples/tour/main.go` — 섹션 12. Modify `README.md`.

**구현자 참고 (기존 코드 위치):**
- `encodeArgs[T TaskArgs]`는 `codec.go:8` — 인터페이스 값으로 호출 가능(`encodeArgs(args)` where args is TaskArgs).
- `enqueueOptions`는 `chronos.go` (queue/taskID/maxRetry/noArchive/processAt/uniqueTTL/misfire/retention).
- `Enqueue`의 msg 조립+dispatch 스위치는 `chronos.go:180-230` 부근.
- process 성공 경로는 `server.go`의 `if err == nil { Done... observe... return }` 블록.
- 테스트 헬퍼 `testutil.NewRedis(t)`, 루트 테스트의 `emailArgs`(chronos_test.go).

---

## Task 1: base — ChainLink + TaskMessage 체인 필드

**Files:**
- Modify: `internal/base/task.go`
- Test: `internal/base/task_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`internal/base/task_test.go`에 추가:

```go
func TestTaskMessage_ChainRoundTrips(t *testing.T) {
	msg := &TaskMessage{
		ID: "ch:0", Kind: "a", Queue: "default",
		ChainID: "ch", ChainIndex: 0,
		Chain: []ChainLink{
			{Kind: "b", Payload: []byte(`{"n":2}`), Queue: "low", MaxRetry: 5, Retention: 60, Delay: 3},
			{Kind: "c", Payload: []byte(`{"n":3}`), Queue: "default", MaxRetry: 25},
		},
	}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ChainID != "ch" || got.ChainIndex != 0 || len(got.Chain) != 2 {
		t.Fatalf("chain fields lost: %+v", got)
	}
	l := got.Chain[0]
	if l.Kind != "b" || l.Queue != "low" || l.MaxRetry != 5 || l.Retention != 60 || l.Delay != 3 {
		t.Errorf("link[0] = %+v", l)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/base/ -run TestTaskMessage_ChainRoundTrips`
Expected: FAIL — `undefined: ChainLink` 등 컴파일 에러.

- [ ] **Step 3: 구현**

`internal/base/task.go`에 `TaskMessage` 위에 타입 추가:

```go
// ChainLink is one pending successor task, carried inside its predecessor's
// message (so a dead-lettered link that is re-run naturally resumes the chain).
// It is a serializable snapshot of the enqueue parameters taken when the chain
// was built.
type ChainLink struct {
	Kind      string `json:"kind"`
	Payload   []byte `json:"payload"`
	Queue     string `json:"queue"`
	MaxRetry  int    `json:"max_retry"`
	NoArchive bool   `json:"no_archive,omitempty"`
	Retention int64  `json:"retention,omitempty"` // seconds
	Delay     int64  `json:"delay,omitempty"`     // seconds before the link runs
}
```

`TaskMessage`의 `CompletedAt` 필드 아래에 추가:

```go
	// Chain holds this task's pending successors: Chain[0] is enqueued when
	// this task succeeds, carrying Chain[1:] as its own tail. Empty for tasks
	// outside a chain and for the last link.
	Chain []ChainLink `json:"chain,omitempty"`
	// ChainID identifies the chain this task belongs to; successor task IDs are
	// deterministic ("<chain_id>:<index>") so a redelivered predecessor cannot
	// enqueue its successor twice.
	ChainID string `json:"chain_id,omitempty"`
	// ChainIndex is this task's position in its chain (0-based).
	ChainIndex int `json:"chain_index,omitempty"`
```

- [ ] **Step 4: 통과 확인**

Run: `go test ./internal/base/ -p 1 && go build ./...`
Expected: PASS, clean.

- [ ] **Step 5: 커밋**

```bash
git add internal/base/
git commit -m "feat: base ChainLink + TaskMessage 체인 필드"
```

---

## Task 2: rdb — EnqueueChainLink (create-if-absent Lua)

**Files:**
- Create: `internal/rdb/chain.go`
- Test: `internal/rdb/chain_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`internal/rdb/chain_test.go` 신규 (`package rdb`; 기존 rdb 테스트의 import 패턴 — context/testing/time/redis/base/testutil — 을 따름):

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

func TestEnqueueChainLink_CreatesAndNoOpsWhenExists(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "ch:1", Kind: "k", Queue: "default", ChainID: "ch", ChainIndex: 1}

	// 1) 최초 호출: 생성된다.
	enqueued, err := r.EnqueueChainLink(ctx, msg, 0)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if !enqueued {
		t.Fatal("first call: enqueued = false, want true")
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); xlen != 1 {
		t.Errorf("stream len = %d, want 1", xlen)
	}

	// 2) 재전달로 인한 두 번째 호출: no-op이어야 한다.
	enqueued, err = r.EnqueueChainLink(ctx, msg, 0)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if enqueued {
		t.Error("second call: enqueued = true, want false (no-op)")
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); xlen != 1 {
		t.Errorf("stream len after no-op = %d, want 1 (no duplicate)", xlen)
	}
}

func TestEnqueueChainLink_DelayGoesToScheduled(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "ch:2", Kind: "k", Queue: "default", ChainID: "ch", ChainIndex: 2}
	enqueued, err := r.EnqueueChainLink(ctx, msg, 2*time.Second)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !enqueued {
		t.Fatal("enqueued = false, want true")
	}
	score, err := client.ZScore(ctx, base.ScheduledKey("default"), "ch:2").Result()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	want := float64(time.Now().Add(2 * time.Second).Unix())
	if score < want-5 || score > want+5 {
		t.Errorf("score = %v, want ~%v", score, want)
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); xlen != 0 {
		t.Errorf("stream should be empty for delayed link, len=%d", xlen)
	}
	// 존재 가드: 두 번째 호출 no-op.
	if enq, _ := r.EnqueueChainLink(ctx, msg, 2*time.Second); enq {
		t.Error("second delayed call: want no-op")
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/rdb/ -run TestEnqueueChainLink -p 1`
Expected: FAIL — `undefined: r.EnqueueChainLink`.

- [ ] **Step 3: 구현**

`internal/rdb/chain.go` 신규:

```go
package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// chainEnqueueCmd enqueues a chain successor only if its task hash does not
// already exist. The guard makes successor enqueueing idempotent: a
// redelivered predecessor (at-least-once) re-runs its handler and re-attempts
// this call, but cannot create a duplicate. Deliberately does NOT clear stale
// completed/archived entries (unlike enqueueCmd): if the successor already ran
// and is retained, re-running it would be exactly the duplication this guard
// exists to prevent. Keys share the queue hash tag (cluster-safe).
// KEYS[1] task hash, KEYS[2] stream. ARGV[1] msg, ARGV[2] state, ARGV[3] id.
var chainEnqueueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("XADD", KEYS[2], "*", "task_id", ARGV[3])
return 1
`)

// chainScheduleCmd is chainEnqueueCmd's delayed variant: the successor lands in
// the scheduled ZSET instead of the stream.
// KEYS[1] task hash, KEYS[2] scheduled zset. ARGV[1] msg, ARGV[2] state,
// ARGV[3] score (unix), ARGV[4] id.
var chainScheduleCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
return 1
`)

// EnqueueChainLink makes a chain successor available (immediately, or in the
// scheduled set when delay > 0), unless a task with the same ID already exists
// — then it is a no-op and returns false. See chainEnqueueCmd for why.
func (r *RDB) EnqueueChainLink(ctx context.Context, msg *base.TaskMessage, delay time.Duration) (bool, error) {
	// Register the queue in the global index (same as other enqueue paths;
	// separate command because QueuesKey has no hash tag).
	if err := r.client.SAdd(ctx, base.QueuesKey(), msg.Queue).Err(); err != nil {
		return false, err
	}

	if delay > 0 {
		msg.State = base.StateScheduled
		encoded, err := base.EncodeMessage(msg)
		if err != nil {
			return false, err
		}
		keys := []string{base.TaskKey(msg.Queue, msg.ID), base.ScheduledKey(msg.Queue)}
		argv := []interface{}{encoded, int(base.StateScheduled), time.Now().Add(delay).Unix(), msg.ID}
		n, err := chainScheduleCmd.Run(ctx, r.client, keys, argv...).Int()
		if err != nil {
			return false, err
		}
		return n == 1, nil
	}

	msg.State = base.StatePending
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return false, err
	}
	keys := []string{base.TaskKey(msg.Queue, msg.ID), base.StreamKey(msg.Queue)}
	argv := []interface{}{encoded, int(base.StatePending), msg.ID}
	n, err := chainEnqueueCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
```

- [ ] **Step 4: 통과 확인**

Run: `go test ./internal/rdb/ -p 1` → 전체 PASS. `go vet ./internal/rdb/ && gofmt -l internal/rdb/` clean.

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/chain.go internal/rdb/chain_test.go
git commit -m "feat: rdb.EnqueueChainLink — create-if-absent 후속 enqueue (즉시/지연)"
```

---

## Task 3: Chain 빌더 + dispatchMessage 리팩터 + ChainPending

**Files:**
- Create: `chain.go` (루트)
- Modify: `chronos.go` (dispatch 추출 + TaskInfo.ChainPending)
- Modify: `inspector.go` (ChainPending 매핑)
- Test: `chain_test.go` (루트, 빌더 검증 + ChainPending)

- [ ] **Step 1: 실패 테스트 작성**

`chain_test.go` 신규 (`package chronos`):

```go
package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestChain_BuilderValidation(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// 빈 체인 → 에러.
	if _, err := NewChain().Enqueue(ctx, c); err == nil {
		t.Error("empty chain: want error")
	}
	// WithTaskID 병용 → 에러 (체인이 ID를 소유).
	if _, err := NewChain().Then(emailArgs{UserID: "a"}, WithTaskID("x")).Enqueue(ctx, c); err == nil {
		t.Error("WithTaskID in chain: want error")
	}
	// WithUnique 병용 → 에러 (스펙: 미지원).
	if _, err := NewChain().Then(emailArgs{UserID: "a"}, WithUnique(time.Minute)).Enqueue(ctx, c); err == nil {
		t.Error("WithUnique in chain: want error")
	}
	// 링크 1개 → 정상 (일반 enqueue와 동일).
	info, err := NewChain().Then(emailArgs{UserID: "solo"}).Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("single link: %v", err)
	}
	if info.ID == "" {
		t.Error("single link: empty task id")
	}
}

func TestChain_ChainPendingExposed(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// 1단계를 먼 미래로 예약해 scheduled 상태에서 조회한다.
	info, err := NewChain().
		Then(emailArgs{UserID: "s1"}, WithProcessIn(time.Hour)).
		Then(emailArgs{UserID: "s2"}).
		Then(emailArgs{UserID: "s3"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	got, err := insp.GetTask(ctx, "default", info.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ChainPending != 2 {
		t.Errorf("ChainPending = %d, want 2", got.ChainPending)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test . -run 'TestChain_Builder|TestChain_ChainPending' -p 1`
Expected: FAIL — `undefined: NewChain`.

- [ ] **Step 3: dispatchMessage 추출 (동작 무변화 리팩터)**

`chronos.go`의 `Enqueue`에서 msg 조립 이후의 스위치 부분(`scheduled := ...`부터 `if err2 != nil { return nil, err2 }`까지)을 아래 헬퍼로 추출하고 Enqueue가 호출하도록 변경:

```go
// dispatchMessage routes an assembled message to the right rdb enqueue path
// (immediate / scheduled / unique variants), shared by Enqueue and Chain.
func dispatchMessage(ctx context.Context, c *Client, msg *base.TaskMessage, options enqueueOptions) error {
	scheduled := !options.processAt.IsZero() && options.processAt.After(time.Now())
	unique := options.uniqueTTL > 0
	if unique {
		msg.UniqueKey = base.UniqueKey(msg.Queue, base.UniqueSuffix(msg.Kind, msg.Payload))
	}
	switch {
	case unique && scheduled:
		// The lock must outlive the delay, or it would expire before the task is
		// even promoted, silently breaking dedup. Extend it to cover the delay
		// plus the caller's ttl (the post-availability safety window).
		return c.rdb.ScheduleUnique(ctx, msg, options.processAt, options.uniqueTTL+time.Until(options.processAt))
	case unique:
		return c.rdb.EnqueueUnique(ctx, msg, options.uniqueTTL)
	case scheduled:
		return c.rdb.Schedule(ctx, msg, options.processAt)
	default:
		return c.rdb.Enqueue(ctx, msg)
	}
}
```

(주의: 기존 Enqueue의 `msg.UniqueKey` 세팅이 스위치 앞에 있으면 이 헬퍼로 함께 이동. Enqueue의 동작이 바이트 단위로 동일해야 함 — 기존 테스트가 회귀 보호.)

- [ ] **Step 4: chain.go 구현**

`chain.go` 신규 (루트, `package chronos`):

```go
package chronos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/kenshin579/chronos-go/internal/base"
)

// Chain builds a sequence of tasks in which each link is enqueued only after
// the previous one succeeds. A link that exhausts its retries stops the chain;
// re-running the dead-lettered link (Inspector/CLI RunTask) resumes it, because
// every link carries its remaining tail inside its own message.
//
// Handlers must be idempotent, as everywhere in chronos-go: a redelivered link
// may run its handler more than once, though its successor is enqueued at most
// once (deterministic successor IDs + create-if-absent).
type Chain struct {
	links []struct {
		args TaskArgs
		opts []Option
	}
}

// NewChain returns an empty chain builder.
func NewChain() *Chain { return &Chain{} }

// Then appends a link. opts accepts the usual per-task options (WithQueue,
// WithMaxRetry, WithRetention, WithProcessIn, ...); WithTaskID and WithUnique
// are rejected at Enqueue time (the chain owns task IDs, and unique dedup
// inside chains is not supported).
func (ch *Chain) Then(args TaskArgs, opts ...Option) *Chain {
	ch.links = append(ch.links, struct {
		args TaskArgs
		opts []Option
	}{args, opts})
	return ch
}

// Enqueue makes the first link available for processing and returns its
// TaskInfo. Later links run only as their predecessors succeed.
func (ch *Chain) Enqueue(ctx context.Context, c *Client) (*TaskInfo, error) {
	if len(ch.links) == 0 {
		return nil, errors.New("chronos: empty chain")
	}

	chainID := uuid.NewString()

	// Snapshot links 1..n-1 as the first link's tail.
	tail := make([]base.ChainLink, 0, len(ch.links)-1)
	for i := 1; i < len(ch.links); i++ {
		link, err := snapshotChainLink(ch.links[i].args, ch.links[i].opts)
		if err != nil {
			return nil, fmt.Errorf("chain link %d: %w", i, err)
		}
		tail = append(tail, link)
	}

	// First link: resolve options like Enqueue does, with chain-owned identity.
	first := ch.links[0]
	options, err := resolveChainOptions(first.opts)
	if err != nil {
		return nil, fmt.Errorf("chain link 0: %w", err)
	}
	payload, err := encodeArgs(first.args)
	if err != nil {
		return nil, fmt.Errorf("chain link 0: %w", err)
	}
	msg := &base.TaskMessage{
		ID:         chainID + ":0",
		Kind:       first.args.Kind(),
		Payload:    payload,
		Queue:      options.queue,
		MaxRetry:   options.maxRetry,
		NoArchive:  options.noArchive,
		Retention:  int64(options.retention / time.Second),
		Chain:      tail,
		ChainID:    chainID,
		ChainIndex: 0,
	}
	if err := dispatchMessage(ctx, c, msg, options); err != nil {
		return nil, err
	}
	return &TaskInfo{ID: msg.ID, Kind: msg.Kind, Queue: msg.Queue}, nil
}

// resolveChainOptions applies opts and rejects the ones a chain cannot honor.
func resolveChainOptions(opts []Option) (enqueueOptions, error) {
	o := enqueueOptions{queue: DefaultQueue, maxRetry: DefaultMaxRetry}
	for _, opt := range opts {
		opt.apply(&o)
	}
	if o.taskID != "" {
		return o, errors.New("chronos: WithTaskID cannot be used inside a chain (the chain owns task IDs)")
	}
	if o.uniqueTTL > 0 {
		return o, errors.New("chronos: WithUnique is not supported inside a chain")
	}
	return o, nil
}

// snapshotChainLink freezes a successor's enqueue parameters into a ChainLink.
func snapshotChainLink(args TaskArgs, opts []Option) (base.ChainLink, error) {
	o, err := resolveChainOptions(opts)
	if err != nil {
		return base.ChainLink{}, err
	}
	payload, err := encodeArgs(args)
	if err != nil {
		return base.ChainLink{}, err
	}
	var delay int64
	if !o.processAt.IsZero() {
		// WithProcessIn stored an absolute time; capture the intended relative
		// delay (we are still inside the builder call, so the drift is tiny).
		if d := time.Until(o.processAt); d > 0 {
			delay = int64(d / time.Second)
			if delay == 0 {
				delay = 1
			}
		}
	}
	return base.ChainLink{
		Kind:      args.Kind(),
		Payload:   payload,
		Queue:     o.queue,
		MaxRetry:  o.maxRetry,
		NoArchive: o.noArchive,
		Retention: int64(o.retention / time.Second),
		Delay:     delay,
	}, nil
}
```

- [ ] **Step 5: ChainPending 노출**

`chronos.go`의 `TaskInfo`에 `CompletedAt` 아래 추가:
```go
	ChainPending int // number of chain links still queued behind this task (0 = none/last)
```
`inspector.go`의 `taskInfoFromMsg`에서 `ti` 세팅에 추가:
```go
	ti.ChainPending = len(m.Chain)
```

- [ ] **Step 6: 통과 확인**

Run: `go test . -run 'TestChain_Builder|TestChain_ChainPending' -p 1 -race` → PASS.
전체 회귀: `go test . -p 1` → PASS (dispatchMessage 리팩터가 기존 Enqueue를 깨지 않았는지 — 기존 unique/scheduled 테스트가 보호).

- [ ] **Step 7: 커밋**

```bash
git add chain.go chain_test.go chronos.go inspector.go
git commit -m "feat: Chain 빌더 (NewChain/Then/Enqueue) + ChainPending 노출"
```

---

## Task 4: 서버 — 성공 시 후속 enqueue → Done

**Files:**
- Modify: `server.go`
- Test: `chain_test.go` (통합 테스트 2개 추가)

- [ ] **Step 1: 실패 테스트 작성**

`chain_test.go`에 추가 (import에 `"sync"`, `"sync/atomic"`, `"errors"` 필요 시 추가):

```go
// chainArgs is a dedicated kind so chain tests don't collide with other tests.
type chainArgs struct {
	Step int `json:"step"`
}

func (chainArgs) Kind() string { return "test:chainstep" }

func TestChain_SequentialExecution(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var mu sync.Mutex
	var order []int
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[chainArgs]) error {
		mu.Lock()
		order = append(order, task.Args.Step)
		n := len(order)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
		return nil
	})
	// 두 큐를 모두 소비 (2단계는 다른 큐로 보낸다 — 링크별 옵션 검증).
	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1, "chainq": 1},
		Concurrency: 2,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := NewChain().
		Then(chainArgs{Step: 1}).
		Then(chainArgs{Step: 2}, WithQueue("chainq")).
		Then(chainArgs{Step: 3}).
		Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		mu.Lock()
		t.Fatalf("chain did not complete; order=%v", order)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Errorf("order = %v, want [1 2 3]", order)
	}
}

func TestChain_StopsOnDeadLetterAndResumesViaRunTask(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var failStep2 atomic.Bool
	failStep2.Store(true)
	var step3Runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[chainArgs]) error {
		switch task.Args.Step {
		case 2:
			if failStep2.Load() {
				return errors.New("step2 boom")
			}
		case 3:
			step3Runs.Add(1)
		}
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := NewChain().
		Then(chainArgs{Step: 1}).
		Then(chainArgs{Step: 2}, WithMaxRetry(0)). // 즉시 dead-letter
		Then(chainArgs{Step: 3}).
		Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// 2단계가 archived로 갈 때까지 대기. 체인은 중단 — 3단계 미실행.
	insp := NewInspector(client)
	var deadID string
	deadline := time.Now().Add(10 * time.Second)
	for deadID == "" {
		tasks, _ := insp.ListTasks(ctx, "default", "archived", 10)
		for _, ti := range tasks {
			if ti.ChainPending == 1 { // 뒤에 3단계가 걸려 있는 dead-letter
				deadID = ti.ID
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("step2 never dead-lettered")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // 잘못 이어졌다면 3단계가 돌 시간
	if n := step3Runs.Load(); n != 0 {
		t.Fatalf("step3 ran %d times despite chain break", n)
	}

	// 원인 해소 후 재실행 → 체인 재개, 3단계 완주.
	failStep2.Store(false)
	if err := insp.RunTask(ctx, "default", deadID); err != nil {
		t.Fatalf("run: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for step3Runs.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("chain did not resume after RunTask")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test . -run 'TestChain_SequentialExecution|TestChain_StopsOnDeadLetter' -p 1`
Expected: FAIL — 체인이 이어지지 않음(서버가 후속을 enqueue하지 않으므로 1단계만 실행되고 타임아웃).

- [ ] **Step 3: 구현 — server.go**

`process`의 성공 블록을 다음으로 교체:

```go
	if err == nil {
		// A chained task must enqueue its successor BEFORE acking: if we acked
		// first and crashed, the chain would be lost. The reverse order is safe
		// because successor creation is idempotent (deterministic ID +
		// create-if-absent), so a redelivery cannot duplicate it.
		if len(msg.Chain) > 0 {
			if cerr := s.enqueueNext(opCtx, msg); cerr != nil {
				// Leave the task unacked: the recoverer will redeliver it, the
				// (idempotent) handler runs again, and the successor enqueue is
				// retried. Better a re-run than a silently broken chain.
				s.logger.Error("chronos: chain successor enqueue failed; leaving task for redelivery",
					"id", msg.ID, "error", cerr)
				return
			}
		}
		if derr := s.rdb.Done(opCtx, qname, streamID, msg); derr != nil {
			s.logger.Error("chronos: ack failed", "id", msg.ID, "error", derr)
		}
		s.observe(msg, OutcomeSuccess, dur)
		return
	}
```

그 아래(예: process 함수 뒤)에 추가:

```go
// enqueueNext makes msg's first pending chain link available, carrying the rest
// of the tail. Idempotent: the successor's deterministic ID plus the
// create-if-absent script mean a redelivered predecessor cannot duplicate it.
func (s *Server) enqueueNext(ctx context.Context, msg *base.TaskMessage) error {
	link := msg.Chain[0]
	next := &base.TaskMessage{
		ID:         fmt.Sprintf("%s:%d", msg.ChainID, msg.ChainIndex+1),
		Kind:       link.Kind,
		Payload:    link.Payload,
		Queue:      link.Queue,
		MaxRetry:   link.MaxRetry,
		NoArchive:  link.NoArchive,
		Retention:  link.Retention,
		Chain:      msg.Chain[1:],
		ChainID:    msg.ChainID,
		ChainIndex: msg.ChainIndex + 1,
	}
	enqueued, err := s.rdb.EnqueueChainLink(ctx, next, time.Duration(link.Delay)*time.Second)
	if err != nil {
		return err
	}
	if !enqueued {
		s.logger.Debug("chronos: chain successor already exists (redelivery)", "id", next.ID)
	}
	return nil
}
```

`server.go` import에 `"fmt"`가 있는지 확인(있음 — NewServer 에러 등에서 사용 여부 확인, 없으면 추가).

- [ ] **Step 4: 통과 확인**

Run: `go test . -run 'TestChain_' -p 1 -race` → 4개 전부 PASS (Task 3의 2개 포함).
전체 회귀: `make check` → PASS.

- [ ] **Step 5: 커밋**

```bash
git add server.go chain_test.go
git commit -m "feat: 서버 성공 경로 체인 후속 enqueue (후속→Done 순서, 멱등)"
```

---

## Task 5: cluster 스모크 14번째

**Files:**
- Modify: `cluster_test.go`

**전제:** docker 클러스터 필요 — `cd deploy/redis-cluster && docker compose up -d && sleep 12`, `redis-cli -p 7000 cluster info | head -1` = `cluster_state:ok`. Docker 데몬이 내려가 있으면 BLOCKED 보고.

- [ ] **Step 1: 테스트 + 체크리스트**

체크리스트에 추가:
```go
//  [x] chainEnqueueCmd/chainScheduleCmd (chain successor)   → TestCluster_ChainCompletes
```
파일 끝에 추가:

```go
func TestCluster_ChainCompletes(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var done atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		done.Add(1)
		return nil
	})
	// 서로 다른 슬롯의 큐 두 개에 걸친 체인 — 후속 enqueue가 슬롯을 넘나든다.
	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"alpha": 1, "bravo": 1},
		Concurrency:     2,
		ForwardInterval: 200 * time.Millisecond,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := NewChain().
		Then(clArgs{N: 21}, WithQueue("alpha")).
		Then(clArgs{N: 22}, WithQueue("bravo")).
		Then(clArgs{N: 23}, WithQueue("alpha")).
		Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 10*time.Second, "3-link chain completes across slots", func() bool {
		return done.Load() == 3
	})
}
```

- [ ] **Step 2: 실행 (14개, 2회)**

Run: `REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 go test -run 'TestCluster_' -p 1 -race -count=1 . 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: 14/14 PASS × 2회. CROSSSLOT → BLOCKED 보고.

- [ ] **Step 3: 커밋**

```bash
git add cluster_test.go
git commit -m "test: cluster 스모크에 chain 추가 (슬롯 교차 후속 enqueue)"
```

---

## Task 6: tour 섹션 12 + README + 최종 검증 + 리뷰 + PR

**Files:**
- Modify: `examples/tour/main.go`, `README.md`

- [ ] **Step 1: tour 섹션 12**

`examples/tour/main.go`에서 섹션 11 종료(`cancelR()`) 뒤, 마지막 구분선 앞에 추가. 상단 타입 정의부에 추가:

```go
// ChainStepArgs is one step of the chain demo.
type ChainStepArgs struct {
	Step int `json:"step"`
}

func (ChainStepArgs) Kind() string { return "demo:chainstep" }
```

섹션 본문 (기존 `insp`, `client`, `rdb`, `ctx` 재사용):

```go
	section("12) chain: A 성공 → B → C 연쇄 실행, 실패하면 중단 + 재실행으로 재개")
	var chainFail atomic.Bool
	chainFail.Store(true)
	cmux := chronos.NewMux()
	chronos.AddHandler(cmux, func(ctx context.Context, t *chronos.Task[ChainStepArgs]) error {
		if t.Args.Step == 2 && chainFail.Load() {
			fmt.Printf("   💥 [chain] %d단계 실패 — 체인 중단 (뒤 단계는 대기)\n", t.Args.Step)
			return errors.New("2단계 오류")
		}
		fmt.Printf("   🔗 [chain] %d단계 실행\n", t.Args.Step)
		return nil
	})
	csrv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"chain-demo": 1},
		Concurrency: 2,
	})
	if err := csrv.Start(ctx, cmux); err != nil {
		fmt.Printf("chain 서버 start 실패: %v\n", err)
	}
	if _, err := chronos.NewChain().
		Then(ChainStepArgs{Step: 1}, chronos.WithQueue("chain-demo")).
		Then(ChainStepArgs{Step: 2}, chronos.WithQueue("chain-demo"), chronos.WithMaxRetry(0)).
		Then(ChainStepArgs{Step: 3}, chronos.WithQueue("chain-demo")).
		Enqueue(ctx, client); err != nil {
		fmt.Printf("chain enqueue 실패: %v\n", err)
	}
	time.Sleep(1500 * time.Millisecond) // 1단계 성공, 2단계 dead-letter
	// dead-letter에서 ChainPending 확인 후 재실행으로 재개.
	tasks, _ := insp.ListTasks(ctx, "chain-demo", "archived", 10)
	for _, ti := range tasks {
		fmt.Printf("   📮 dead-letter: %s (뒤에 %d단계 대기 중)\n", ti.ID, ti.ChainPending)
		chainFail.Store(false)
		fmt.Println("   원인 해소 후 RunTask로 재실행 → 체인 재개:")
		if err := insp.RunTask(ctx, "chain-demo", ti.ID); err != nil {
			fmt.Printf("   run 실패: %v\n", err)
		}
	}
	time.Sleep(1500 * time.Millisecond) // 2단계 재실행 + 3단계 완주
	shutChainCtx, cancelC := context.WithTimeout(context.Background(), 3*time.Second)
	_ = csrv.Shutdown(shutChainCtx)
	cancelC()
```

상단 doc comment 기능 나열에 `, and task chains`를 추가. `errors`/`sync/atomic` import 확인(둘 다 이미 있음 — 확인 후 필요 시 추가).

Run: `gofmt -w examples/tour/main.go && go vet ./examples/tour/ && go run ./examples/tour 2>&1 | sed -n '/=== 12)/,$p'`
Expected: 1단계 실행 → 2단계 실패 → dead-letter(뒤에 1단계 대기) → 재실행 → 2단계·3단계 실행.

- [ ] **Step 2: README**

(a) Highlights에 항목 추가 (Weighted priority queues 아래):
```markdown
- **Chains** — run tasks in sequence (`NewChain().Then(...).Then(...)`); a link
  runs only after its predecessor succeeds, a failed link stops the chain, and
  re-running its dead-letter resumes it.
```
(b) "Queue priority" 섹션 뒤에 새 섹션 추가:
```markdown
## Chains

Run tasks strictly in sequence — each link is enqueued only when the previous
one succeeds:

```go
info, err := chronos.NewChain().
	Then(EncodeArgs{VideoID: "v1"}).
	Then(ThumbnailArgs{VideoID: "v1"}, chronos.WithQueue("low")).
	Then(NotifyArgs{UserID: "u1"}, chronos.WithRetention(time.Hour)).
	Enqueue(ctx, client)
```

- Per-link options: queue, retries, retention, delay (`WithProcessIn` on a link
  delays it relative to its predecessor's completion). `WithTaskID` and
  `WithUnique` are rejected inside chains.
- **Failure stops the chain.** When a link exhausts its retries and is
  dead-lettered, its successors wait inside the dead-letter (`ChainPending` in
  the Inspector shows how many). Re-run it (`chronos task run ...`) after fixing
  the cause and the chain resumes from that point.
- Handlers must stay idempotent (at-least-once); successors themselves are
  enqueued at most once (deterministic IDs + create-if-absent).
- Each link carries its remaining tail in its message, so very long chains grow
  the message size — keep chains reasonably short.
```
(c) "Known limitations / roadmap"의 `- Not yet built: a web UI, workflows (chains/groups).`를:
```markdown
- Not yet built: a web UI, task groups (parallel fan-out with a completion
  callback — chains are supported).
```

- [ ] **Step 3: 최종 검증**

```bash
make check          # 전체 무회귀
make test-cluster   # 14개
go run ./examples/tour  # 12 섹션 눈 확인
```

- [ ] **Step 4: 커밋**

```bash
git add examples/tour/main.go README.md
git commit -m "docs: tour 섹션 12(chain) + README Chains 섹션"
```

- [ ] **Step 5: 코드 리뷰 + PR**

k:code-reviewer로 브랜치 전체 리뷰 — 특히: 후속 enqueue→Done 순서의 크래시 창, chainEnqueueCmd 가드와 completed retention 상호작용(보관 중 후속 no-op이 올바른가), RunTask 재개 경로(archived에서 승격된 msg가 Chain 꼬리를 유지하는가 — runTaskCmd는 hash의 msg를 건드리지 않으므로 유지됨), dispatchMessage 리팩터의 동작 보존, Delay 스냅샷의 시간 드리프트. 반영 후:

```bash
gh pr create --assignee kenshin579 --title "feat: Task Chain — 연쇄 실행 (NewChain/Then/Enqueue)" --body "$(cat <<'EOF'
## 배경
태스크 의존 흐름(A 성공 → B → C)을 라이브러리가 지원한다. 지금은 핸들러 안에서 직접 enqueue해야 해 흐름이 흩어지고, at-least-once 재전달 시 후속 중복 방지를 사용자가 재발명해야 했다. Group(병렬+콜백)은 범위 제외.

## 변경
- `NewChain().Then(args, opts...).Enqueue(ctx, client)` 빌더. 링크별 큐/재시도/retention/지연. `WithTaskID`/`WithUnique`는 체인 안에서 거부.
- 후속 링크를 `TaskMessage.Chain`에 내장 → **dead-letter를 RunTask로 재실행하면 체인이 그 지점부터 재개** (별도 상태 저장소 없음). `TaskInfo.ChainPending`으로 노출.
- 정확성: 결정적 후속 ID(`<chainID>:<i>`) + create-if-absent Lua(`chainEnqueueCmd`/`chainScheduleCmd`), 성공 시 "후속 enqueue → Done" 순서(사이 크래시는 no-op 가드로 안전, enqueue 에러 시 Done 건너뛰어 재전달 유도 — 체인 유실 방지).
- cluster 스모크 14개(슬롯 교차 체인), tour 섹션 12(완주+중단→재개), README Chains 섹션.

## 테스트 계획
- [x] make check 무회귀
- [x] make test-cluster 14/14
- [x] go run ./examples/tour 섹션 12 눈 확인

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review (계획 작성자 확인 완료)

- **스펙 커버리지**: A 빌더(T3) / B 데이터모델+unique 제외(T1, T3 검증) / C 실행 흐름·순서(T4) / D chainEnqueueCmd(T2) / E ChainPending·tour·cluster·README(T3,T5,T6) / 테스트 1-6(T2-T5) — 전 항목 매핑.
- **placeholder 스캔**: 전 스텝 실제 코드·명령·기대출력 포함.
- **타입 일관성**: `base.ChainLink{Kind,Payload,Queue,MaxRetry,NoArchive,Retention,Delay}`(T1)를 T3 snapshot·T4 enqueueNext가 동일 필드로 사용. `EnqueueChainLink(ctx, msg, delay) (bool, error)`(T2)를 T4가 동일 시그니처로 호출. `dispatchMessage(ctx, c, msg, options)`(T3)를 chain.go가 사용. `ChainPending`(T3)을 T4·T6 테스트/tour가 사용.
- **주의**: T4의 성공 경로 교체 시 기존 `opCtx`(WithoutCancel+ackTimeout) 사용 유지 — enqueueNext도 opCtx로 호출(셧다운 중에도 체인 정합 유지).
