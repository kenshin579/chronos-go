# 결과 전달 (workflow results, PR 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 핸들러가 반환한 결과를 체인 후속 링크(`PrevResult`)와 그룹 콜백(`GroupResults`)에 전달한다 — breaking 없이(`AddHandlerR` 신설).

**Architecture:** 결과는 저장소 없이 릴레이로 흐른다. 핸들러 래퍼가 성공 반환값을 JSON으로 `TaskMessage.Result`에 기록 → (chain) 기존 "후속 enqueue→Done" 지점에서 후속 메시지의 `PrevResult`에 복사, (group) `groupCompleteCmd` Lua가 콜백 큐 슬롯의 결과 HASH에 모았다가 마지막 멤버 시점에 cjson으로 콜백 메시지에 내장 후 HASH 삭제. 1MiB 초과 결과는 SkipRetry 의미론으로 재시도 없이 dead-letter.

**Tech Stack:** Go 1.26 제네릭, Redis Lua(cjson), 기존 chronos 내부(base/rdb/server).

**스펙:** `docs/superpowers/specs/2026-07-13-workflow-results-design.md` (PR 1 절)

**전제:**
- 브랜치 `feat/workflow-results`. 로컬 Redis(127.0.0.1:6379) 필요. 테스트는 `-p 1`.
- **메인 작업 트리에서 git checkout/switch 금지** (파일 수정·커밋만).
- 커밋 author `kenshin579@hotmail.com`, 메시지 끝에 빈 줄 후
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`, HEREDOC 사용.
- 루트 모듈 검증 명령: `go build ./... && go vet ./...`, 대상 테스트만
  `go test -p 1 -count=1 -run '<패턴>' .` (패키지 `chronos`가 루트).

**확인된 기존 코드 (그대로 활용):**
- `handler.go`: `Mux.handlers map[string]internalHandler`, `AddHandler[T]`가
  래퍼에서 `decodeArgs[T](msg.Payload)` 후 `&Task[T]{Args, id: msg.ID, queue: msg.Queue}` 생성
- `retry.go`: `SkipRetry(err) error`(skipRetryError 래핑), `asSkipRetry(err) bool`
- `server.go:479` 성공 경로: `len(msg.Chain)>0`이면 `enqueueNextWithRetry` → 성공 시 `msg.Chain = nil` → `msg.GroupID != ""`이면 `completeGroupWithRetry` → `s.rdb.Done(...)`
- `server.go:575 enqueueNext`: `msg.Chain[0]`으로 next 메시지 구성(ID `<chainID>:<i+1>`), `EnqueueChainLink`
- `internal/rdb/group.go`: `groupCompleteCmd` Lua(KEYS[1] pending SET, KEYS[2] 콜백 hash, KEYS[3] 콜백 stream/zset; ARGV[1] member id, [2] 콜백 인코딩, [3] state, [4] 콜백 id, [5] mode, [6] score, [7] TTL초), `CompleteGroupMember(ctx, member)`가 `member.GroupCallback` 스냅샷으로 콜백 메시지 구성
- `group.go:86` 멤버 ID `<groupID>:m<i>`, `group.go:112` 멤버 TaskMessage 구성(GroupID/GroupQueue/GroupCallback)
- `internal/base/task.go`: `TaskMessage`(Chain/ChainID/ChainIndex/GroupID/GroupQueue/GroupCallback 존재), `EncodeMessage/DecodeMessage`
- `chronos.go`: `encodeArgs/decodeArgs`, `Task[T]{Args; id; queue}` + `ID()/Queue()`

---

### Task 1: base 메시지 필드 + 결과 상한 상수

**Files:**
- Modify: `internal/base/task.go` (TaskMessage 필드 추가)
- Test: `internal/base/task_test.go` (기존 파일에 추가)

- [ ] **Step 1: 실패하는 테스트 작성** — `internal/base/task_test.go`에 추가:

```go
func TestMessageResultFieldsRoundTrip(t *testing.T) {
	in := &TaskMessage{
		ID: "t1", Kind: "k", Queue: "q",
		Result:       []byte(`{"path":"s3://out"}`),
		PrevResult:   []byte(`{"n":1}`),
		GroupResults: [][]byte{[]byte(`{"a":1}`), nil, []byte(`{"b":2}`)},
		GroupIndex:   2,
		GroupSize:    3,
	}
	b, err := EncodeMessage(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeMessage(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(out.Result) != string(in.Result) || string(out.PrevResult) != string(in.PrevResult) {
		t.Errorf("result fields lost: %+v", out)
	}
	if len(out.GroupResults) != 3 || out.GroupResults[1] != nil ||
		string(out.GroupResults[2]) != `{"b":2}` {
		t.Errorf("group results wrong: %v", out.GroupResults)
	}
	if out.GroupIndex != 2 || out.GroupSize != 3 {
		t.Errorf("group index/size wrong: %+v", out)
	}
	// 빈 필드는 직렬화에서 생략(기존 메시지와 하위호환).
	empty, _ := EncodeMessage(&TaskMessage{ID: "t2", Kind: "k", Queue: "q"})
	for _, field := range []string{"result", "prev_result", "group_results", "group_index", "group_size"} {
		if strings.Contains(string(empty), `"`+field+`"`) {
			t.Errorf("empty message must omit %q", field)
		}
	}
}
```

(`strings` import가 없으면 추가.)

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestMessageResultFieldsRoundTrip ./internal/base/`
Expected: FAIL (`unknown field Result` 컴파일 에러)

- [ ] **Step 3: 최소 구현** — `internal/base/task.go`의 `TaskMessage`에서 `GroupCallback` 필드 바로 뒤에 추가:

```go
	// Result is the handler's returned value (JSON), set on success by
	// AddHandlerR handlers. It is relayed to the chain successor's PrevResult
	// and the group result HASH before ack, and persists with the task only
	// when the task itself is retained.
	Result []byte `json:"result,omitempty"`
	// PrevResult carries the previous chain link's Result (nil on the first
	// link or when the predecessor produced no result).
	PrevResult []byte `json:"prev_result,omitempty"`
	// GroupResults carries every member's Result in Add order (nil entry =
	// that member produced no result). Set on the callback message when the
	// group completes; nil when no member produced a result.
	GroupResults [][]byte `json:"group_results,omitempty"`
	// GroupIndex is this member's position in its group (Add order, 0-based).
	GroupIndex int `json:"group_index,omitempty"`
	// GroupSize is the group's member count, carried by every member so the
	// completion script can assemble GroupResults in order.
	GroupSize int `json:"group_size,omitempty"`
```

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run TestMessageResultFieldsRoundTrip ./internal/base/ && go build ./...`
Expected: PASS, 빌드 성공

- [ ] **Step 5: 커밋**

```bash
git add internal/base/task.go internal/base/task_test.go
git commit -m "feat: TaskMessage 결과 필드 (Result/PrevResult/GroupResults/GroupIndex/GroupSize)"
```

---

### Task 2: AddHandlerR + 수신 API

**Files:**
- Modify: `handler.go` (AddHandlerR, MaxResultSize, ErrResultTooLarge, newTask 헬퍼)
- Modify: `chronos.go` (Task 필드, ErrNoResult, PrevResult/GroupResults/RawGroupResults)
- Test: `handler_result_test.go` (신규)

- [ ] **Step 1: 실패하는 테스트 작성** — `handler_result_test.go` 신규:

```go
package chronos

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
)

type resArgs struct {
	N int `json:"n"`
}

func (resArgs) Kind() string { return "res:job" }

type resOut struct {
	Doubled int    `json:"doubled"`
	Big     string `json:"big,omitempty"`
}

func dispatchMsg(t *testing.T, mux *Mux, msg *base.TaskMessage) error {
	t.Helper()
	return mux.dispatch(context.Background(), msg)
}

func TestAddHandlerR_SetsResultOnSuccess(t *testing.T) {
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[resArgs]) (resOut, error) {
		return resOut{Doubled: task.Args.N * 2}, nil
	})
	msg := &base.TaskMessage{ID: "t1", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":21}`)}
	if err := dispatchMsg(t, mux, msg); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if string(msg.Result) != `{"doubled":42}` {
		t.Errorf("result = %s", msg.Result)
	}
}

func TestAddHandlerR_ErrorLeavesNoResult(t *testing.T) {
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[resArgs]) (resOut, error) {
		return resOut{Doubled: 99}, errors.New("boom")
	})
	msg := &base.TaskMessage{ID: "t1", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`)}
	if err := dispatchMsg(t, mux, msg); err == nil {
		t.Fatal("want error")
	}
	if msg.Result != nil {
		t.Errorf("failed handler must not set a result: %s", msg.Result)
	}
}

func TestAddHandlerR_OversizeResultSkipsRetry(t *testing.T) {
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[resArgs]) (resOut, error) {
		return resOut{Big: strings.Repeat("x", MaxResultSize)}, nil
	})
	msg := &base.TaskMessage{ID: "t1", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`)}
	err := dispatchMsg(t, mux, msg)
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("want ErrResultTooLarge, got %v", err)
	}
	if !asSkipRetry(err) {
		t.Error("oversize result must be non-retryable")
	}
	if msg.Result != nil {
		t.Error("oversize result must not be stored")
	}
}

func TestPrevResult(t *testing.T) {
	mux := NewMux()
	var got resOut
	var gotErr error
	AddHandler(mux, func(ctx context.Context, task *Task[resArgs]) error {
		got, gotErr = PrevResult[resOut](task)
		return nil
	})
	msg := &base.TaskMessage{ID: "t2", Kind: "res:job", Queue: "q",
		Payload: []byte(`{"n":1}`), PrevResult: []byte(`{"doubled":42}`)}
	if err := dispatchMsg(t, mux, msg); err != nil {
		t.Fatal(err)
	}
	if gotErr != nil || got.Doubled != 42 {
		t.Errorf("prev result = %+v err=%v", got, gotErr)
	}
	// 결과 없는 선행 → ErrNoResult.
	msg2 := &base.TaskMessage{ID: "t3", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`)}
	if err := dispatchMsg(t, mux, msg2); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(gotErr, ErrNoResult) {
		t.Errorf("want ErrNoResult, got %v", gotErr)
	}
}

func TestGroupResultsAccessors(t *testing.T) {
	mux := NewMux()
	var typed []resOut
	var typedErr error
	var raw [][]byte
	AddHandler(mux, func(ctx context.Context, task *Task[resArgs]) error {
		typed, typedErr = GroupResults[resOut](task)
		raw = task.RawGroupResults()
		return nil
	})
	// 동질 그룹: 전부 디코딩.
	msg := &base.TaskMessage{ID: "cb", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`),
		GroupResults: [][]byte{[]byte(`{"doubled":2}`), []byte(`{"doubled":4}`)}}
	if err := dispatchMsg(t, mux, msg); err != nil {
		t.Fatal(err)
	}
	if typedErr != nil || len(typed) != 2 || typed[0].Doubled != 2 || typed[1].Doubled != 4 {
		t.Errorf("typed = %+v err=%v", typed, typedErr)
	}
	if len(raw) != 2 {
		t.Errorf("raw = %v", raw)
	}
	// nil 멤버 포함 → 타입드는 ErrNoResult, raw는 위치 보존.
	msg2 := &base.TaskMessage{ID: "cb2", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`),
		GroupResults: [][]byte{[]byte(`{"doubled":2}`), nil}}
	if err := dispatchMsg(t, mux, msg2); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(typedErr, ErrNoResult) {
		t.Errorf("want ErrNoResult for nil member, got %v", typedErr)
	}
	if len(raw) != 2 || raw[1] != nil {
		t.Errorf("raw must keep positions: %v", raw)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run 'TestAddHandlerR|TestPrevResult|TestGroupResults' .`
Expected: FAIL (`undefined: AddHandlerR` 등)

- [ ] **Step 3: 구현**

`handler.go` — `AddHandler`의 래퍼 끝부분을 `newTask` 헬퍼로 교체하고 `AddHandlerR` 추가:

```go
// AddHandler의 기존 래퍼 마지막 줄을 다음으로 교체:
//   return fn(ctx, newTask[T](args, msg))

// MaxResultSize bounds a handler result's JSON encoding. Larger results
// dead-letter the task without retry (the same value would be produced
// again) — pass a reference (object-store path, row ID) instead.
const MaxResultSize = 1 << 20

// ErrResultTooLarge marks a handler result that exceeds MaxResultSize.
var ErrResultTooLarge = errors.New("chronos: result exceeds MaxResultSize")

// AddHandlerR registers a handler whose success return value becomes the
// task's result: it is relayed to the next chain link (read with PrevResult)
// and collected for the group callback (read with GroupResults). The result
// is marshalled as JSON. Kind rules and duplicate-registration panics match
// AddHandler.
func AddHandlerR[T TaskArgs, R any](mux *Mux, fn func(ctx context.Context, task *Task[T]) (R, error)) {
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
		res, err := fn(ctx, newTask[T](args, msg))
		if err != nil {
			return err
		}
		b, err := json.Marshal(res)
		if err != nil {
			return SkipRetry(fmt.Errorf("chronos: marshal result for kind %q: %w", kind, err))
		}
		if len(b) > MaxResultSize {
			return SkipRetry(fmt.Errorf("chronos: kind %q result is %d bytes: %w", kind, len(b), ErrResultTooLarge))
		}
		msg.Result = b
		return nil
	}
}

// newTask builds the typed task handed to handlers, carrying workflow inputs.
func newTask[T TaskArgs](args T, msg *base.TaskMessage) *Task[T] {
	return &Task[T]{
		Args:         args,
		id:           msg.ID,
		queue:        msg.Queue,
		prevResult:   msg.PrevResult,
		groupResults: msg.GroupResults,
	}
}
```

(`encoding/json`, `errors` import 추가.)

`chronos.go` — `Task[T]` 구조체에 필드 추가 + 접근자:

```go
// Task[T] 구조체에 추가:
	prevResult   []byte
	groupResults [][]byte

// ErrNoResult is returned when the previous step (or a group member) produced
// no result — the handler was registered with AddHandler, or this is the
// first chain link.
var ErrNoResult = errors.New("chronos: no result")

// PrevResult decodes the previous chain link's result:
//
//	out, err := chronos.PrevResult[EncodeResult](task)
func PrevResult[R any, T TaskArgs](t *Task[T]) (R, error) {
	var out R
	if len(t.prevResult) == 0 {
		return out, ErrNoResult
	}
	if err := json.Unmarshal(t.prevResult, &out); err != nil {
		return out, fmt.Errorf("chronos: decode prev result: %w", err)
	}
	return out, nil
}

// GroupResults decodes every member result in Add order. It assumes a
// homogeneous group (every member returned R); a member without a result
// fails with ErrNoResult. For heterogeneous or partial results use
// RawGroupResults.
func GroupResults[R any, T TaskArgs](t *Task[T]) ([]R, error) {
	if t.groupResults == nil {
		return nil, ErrNoResult
	}
	out := make([]R, len(t.groupResults))
	for i, raw := range t.groupResults {
		if len(raw) == 0 {
			return nil, fmt.Errorf("chronos: group member %d: %w", i, ErrNoResult)
		}
		if err := json.Unmarshal(raw, &out[i]); err != nil {
			return nil, fmt.Errorf("chronos: decode group member %d result: %w", i, err)
		}
	}
	return out, nil
}

// RawGroupResults returns raw member results in Add order (nil = no result).
// Nil when this task is not a group callback or no member produced a result.
func (t *Task[T]) RawGroupResults() [][]byte { return t.groupResults }
```

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run 'TestAddHandlerR|TestPrevResult|TestGroupResults' . && go vet ./...`
Expected: PASS 5건, vet 클린

- [ ] **Step 5: 커밋**

```bash
git add handler.go chronos.go handler_result_test.go
git commit -m "feat: AddHandlerR + PrevResult/GroupResults/RawGroupResults (1MiB 상한, no-retry)"
```

---

### Task 3: chain 릴레이

**Files:**
- Modify: `server.go:575` (enqueueNext)
- Test: `chain_result_test.go` (신규, 통합 — 로컬 Redis)

- [ ] **Step 1: 실패하는 테스트 작성** — `chain_result_test.go` 신규. 기존 통합 테스트 관례(`testutil.NewRedis(t)`, 서버 기동)는 `chain_test.go`를 참고하되 아래 코드를 기준으로:

```go
package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type relayArgs struct {
	Step int `json:"step"`
}

func (relayArgs) Kind() string { return "relay:step" }

type relayOut struct {
	Sum int `json:"sum"`
}

// 2링크 체인: 링크0의 결과가 링크1의 PrevResult로 도착하고, 링크1이 1회
// 실패(재시도)해도 보존되는지 확인.
func TestChain_RelaysResultToSuccessor(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var got atomic.Int64  // 링크1이 수신한 PrevResult.Sum
	var tries atomic.Int64
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[relayArgs]) (relayOut, error) {
		return relayOut{Sum: task.Args.Step + 41}, nil
	})
	// 두 번째 링크는 다른 Kind로 — 첫 시도 실패 후 재시도에서도 PrevResult 확인.
	AddHandler(mux, func(ctx context.Context, task *Task[relayCheckArgs]) error {
		out, err := PrevResult[relayOut](task)
		if err != nil {
			t.Errorf("prev result: %v", err)
			return nil
		}
		if tries.Add(1) == 1 {
			return errors.New("transient")
		}
		got.Store(int64(out.Sum))
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"relay": 1}, Concurrency: 2,
		RetryDelayFunc: func(n int, err error) time.Duration { return 100 * time.Millisecond }})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	c := NewClient(client)
	_, err := NewChain().
		Then(relayArgs{Step: 1}, WithQueue("relay")).
		Then(relayCheckArgs{}, WithQueue("relay")).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for got.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got.Load() != 42 {
		t.Fatalf("successor got %d, want 42 (tries=%d)", got.Load(), tries.Load())
	}
	if tries.Load() != 2 {
		t.Errorf("expected exactly one retry, tries=%d", tries.Load())
	}
}

type relayCheckArgs struct{}

func (relayCheckArgs) Kind() string { return "relay:check" }
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestChain_RelaysResultToSuccessor .`
Expected: FAIL — `successor got 0` (PrevResult 미전달, ErrNoResult 에러 로그)

- [ ] **Step 3: 구현** — `server.go` `enqueueNext`의 next 메시지 구성에 한 필드 추가:

```go
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
		PrevResult: msg.Result, // 이번 링크의 결과를 후속에 릴레이
	}
```

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run 'TestChain' .`
Expected: 신규 포함 기존 chain 테스트 전부 PASS

- [ ] **Step 5: 커밋**

```bash
git add server.go chain_result_test.go
git commit -m "feat: chain 결과 릴레이 — 후속 메시지에 PrevResult 내장 (재시도에도 보존)"
```

---

### Task 4: rdb group 결과 수집 (Lua 확장)

**Files:**
- Modify: `internal/base/keys.go` (GroupResultKey)
- Modify: `internal/rdb/group.go` (groupCompleteCmd + CompleteGroupMember)
- Test: `internal/rdb/group_test.go` (기존 파일에 추가)

- [ ] **Step 1: 실패하는 테스트 작성** — `internal/rdb/group_test.go`에 추가 (파일 상단의 기존 헬퍼/셋업 관례를 따라 `newTestRDB` 등 기존 이름 사용 — 파일을 먼저 읽고 동일 패턴으로):

```go
// 멤버 3(결과 2, 무결과 1) 그룹: 결과가 인덱스 순서로 콜백 메시지에 내장되고
// groupresult HASH가 삭제되는지 확인.
func TestCompleteGroupMember_CollectsResults(t *testing.T) {
	r, client := newTestRDB(t) // 기존 테스트 파일의 셋업 헬퍼 관례를 따를 것
	ctx := context.Background()

	cb := &base.ChainLink{Kind: "g:cb", Payload: []byte(`{}`), Queue: "gq"}
	mk := func(i int, result []byte) *base.TaskMessage {
		return &base.TaskMessage{
			ID: "g1:m" + strconv.Itoa(i), Kind: "g:m", Queue: "gq",
			GroupID: "g1", GroupQueue: "gq", GroupCallback: cb,
			GroupIndex: i, GroupSize: 3, Result: result,
		}
	}
	if err := r.CreateGroup(ctx, "gq", "g1", []string{"g1:m0", "g1:m1", "g1:m2"}); err != nil {
		t.Fatal(err)
	}

	if fired, err := r.CompleteGroupMember(ctx, mk(0, []byte(`{"v":0}`))); err != nil || fired {
		t.Fatalf("m0: fired=%v err=%v", fired, err)
	}
	if fired, err := r.CompleteGroupMember(ctx, mk(1, nil)); err != nil || fired {
		t.Fatalf("m1: fired=%v err=%v", fired, err)
	}
	// 결과 HASH에 인덱스 필드로 쌓이는지(마지막 멤버 전).
	if n, _ := client.HLen(ctx, base.GroupResultKey("gq", "g1")).Result(); n != 1 {
		t.Fatalf("result hash len = %d, want 1", n)
	}
	fired, err := r.CompleteGroupMember(ctx, mk(2, []byte(`{"v":2}`)))
	if err != nil || !fired {
		t.Fatalf("m2: fired=%v err=%v", fired, err)
	}

	// 콜백 메시지 검증: GroupResults가 [r0, nil, r2] 순서로 내장.
	raw, err := client.HGet(ctx, base.TaskKey("gq", "g1:cb"), "msg").Result()
	if err != nil {
		t.Fatal(err)
	}
	cbMsg, err := base.DecodeMessage([]byte(raw))
	if err != nil {
		t.Fatalf("callback message corrupted by lua roundtrip: %v", err)
	}
	if cbMsg.ID != "g1:cb" || cbMsg.Kind != "g:cb" || cbMsg.Queue != "gq" {
		t.Errorf("callback core fields lost: %+v", cbMsg)
	}
	if len(cbMsg.GroupResults) != 3 ||
		string(cbMsg.GroupResults[0]) != `{"v":0}` ||
		cbMsg.GroupResults[1] != nil ||
		string(cbMsg.GroupResults[2]) != `{"v":2}` {
		t.Errorf("group results = %v", cbMsg.GroupResults)
	}
	// 결과 HASH는 삭제됨.
	if n, _ := client.Exists(ctx, base.GroupResultKey("gq", "g1")).Result(); n != 0 {
		t.Error("groupresult hash must be deleted on completion")
	}
}

// 아무 멤버도 결과가 없으면 콜백 메시지에 group_results가 아예 없어야 한다
// (cjson 경로를 타지 않음 — 하위호환·빈 배열 함정 회피).
func TestCompleteGroupMember_NoResultsMeansNoField(t *testing.T) {
	r, client := newTestRDB(t)
	ctx := context.Background()
	cb := &base.ChainLink{Kind: "g:cb", Payload: []byte(`{}`), Queue: "gq"}
	mk := func(i int) *base.TaskMessage {
		return &base.TaskMessage{
			ID: "g2:m" + strconv.Itoa(i), Kind: "g:m", Queue: "gq",
			GroupID: "g2", GroupQueue: "gq", GroupCallback: cb,
			GroupIndex: i, GroupSize: 2,
		}
	}
	if err := r.CreateGroup(ctx, "gq", "g2", []string{"g2:m0", "g2:m1"}); err != nil {
		t.Fatal(err)
	}
	r.CompleteGroupMember(ctx, mk(0))
	if fired, err := r.CompleteGroupMember(ctx, mk(1)); err != nil || !fired {
		t.Fatalf("fired=%v err=%v", fired, err)
	}
	raw, _ := client.HGet(ctx, base.TaskKey("gq", "g2:cb"), "msg").Result()
	if strings.Contains(raw, "group_results") {
		t.Errorf("no-result group must omit group_results: %s", raw)
	}
	cbMsg, err := base.DecodeMessage([]byte(raw))
	if err != nil || cbMsg.GroupResults != nil {
		t.Errorf("decode: %v, results: %v", err, cbMsg.GroupResults)
	}
}
```

(`strconv`, `strings` import 추가. `newTestRDB`가 실제 파일의 헬퍼 이름과 다르면 기존 이름에 맞출 것 — 테스트 의도는 불변.)

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestCompleteGroupMember ./internal/rdb/`
Expected: FAIL (`undefined: base.GroupResultKey`)

- [ ] **Step 3: 구현**

`internal/base/keys.go` — `GroupKey` 아래에 추가:

```go
// GroupResultKey returns the HASH key collecting a group's member results
// (field = member index, value = base64 of the result JSON) while the group
// is in flight. Same hash tag as the pending SET — the completion script
// touches both atomically (cluster-safe).
func GroupResultKey(cbQueue, groupID string) string {
	return QueueKeyPrefix(cbQueue) + "groupresult:" + groupID
}
```

`internal/rdb/group.go` — Lua 교체 + CompleteGroupMember 확장:

```go
// groupCompleteCmd — 기존 주석에 이어 결과 수집을 설명 추가:
// The member's result (base64) is stored in the result HASH (KEYS[4]) under
// its member index; when the last member completes, the results are embedded
// into the callback message's group_results field (base64 strings — matching
// Go's []byte JSON encoding) via cjson before the callback is created, and
// both the pending SET and the result HASH are deleted. cjson round-trips the
// message JSON: all numeric fields are unix seconds / small ints (< 2^53),
// and every slice field is omitempty (no empty-array-to-{} hazard). The
// member-count guard (ARGV[10] > 0) keeps legacy in-flight members (encoded
// before GroupSize existed) on the old no-results path — and avoids cjson
// encoding an empty Lua table as {} into a slice field.
// KEYS[1] group set, KEYS[2] callback task hash, KEYS[3] callback stream or
// scheduled zset, KEYS[4] group result hash.
// ARGV[1] member id, ARGV[2] callback encoded msg, ARGV[3] callback state,
// ARGV[4] callback id, ARGV[5] mode ("stream"|"zset"), ARGV[6] score (zset),
// ARGV[7] group TTL in seconds, ARGV[8] member result base64 ("" = none),
// ARGV[9] member index, ARGV[10] member count.
var groupCompleteCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
  return 0
end
redis.call("SREM", KEYS[1], ARGV[1])
if ARGV[8] ~= "" then
  redis.call("HSET", KEYS[4], ARGV[9], ARGV[8])
  redis.call("EXPIRE", KEYS[4], ARGV[7])
end
if redis.call("SCARD", KEYS[1]) > 0 then
  redis.call("EXPIRE", KEYS[1], ARGV[7])
  return 0
end
local cb = ARGV[2]
if redis.call("EXISTS", KEYS[4]) == 1 and tonumber(ARGV[10]) > 0 then
  local msg = cjson.decode(cb)
  local results = {}
  local n = tonumber(ARGV[10])
  for i = 0, n - 1 do
    local v = redis.call("HGET", KEYS[4], tostring(i))
    if v then
      results[i + 1] = v
    else
      results[i + 1] = cjson.null
    end
  end
  msg["group_results"] = results
  cb = cjson.encode(msg)
end
redis.call("DEL", KEYS[1], KEYS[4])
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2], "msg", cb, "state", ARGV[3])
if ARGV[5] == "stream" then
  redis.call("XADD", KEYS[3], "*", "task_id", ARGV[4])
else
  redis.call("ZADD", KEYS[3], ARGV[6], ARGV[4])
end
return 1
`)
```

`CompleteGroupMember`의 keys/argv 구성 변경 (함수 끝부분):

```go
	keys := []string{
		base.GroupKey(member.GroupQueue, member.GroupID),
		base.TaskKey(cb.Queue, cb.ID),
		destKey,
		base.GroupResultKey(member.GroupQueue, member.GroupID),
	}
	resultB64 := ""
	if len(member.Result) > 0 {
		resultB64 = base64.StdEncoding.EncodeToString(member.Result)
	}
	argv := []interface{}{member.ID, encoded, int(state), cb.ID, mode, score,
		int(GroupTTL / time.Second), resultB64, member.GroupIndex, member.GroupSize}
	n, err := groupCompleteCmd.Run(ctx, r.client, keys, argv...).Int()
```

(`encoding/base64` import 추가.)

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run 'TestCompleteGroupMember|TestGroup' ./internal/rdb/`
Expected: 신규 2건 + 기존 group 테스트 전부 PASS (기존 테스트는 GroupSize 0이라 결과 경로를 안 탐 — 무결과 그룹으로 동작)

- [ ] **Step 5: 커밋**

```bash
git add internal/base/keys.go internal/rdb/group.go internal/rdb/group_test.go
git commit -m "feat: rdb group 결과 수집 — groupresult HASH + cjson 내장 (원자 Lua 확장)"
```

---

### Task 5: group 빌더 배선 + e2e

**Files:**
- Modify: `group.go:112` (멤버 메시지에 GroupIndex/GroupSize)
- Test: `group_result_test.go` (신규, 통합)

- [ ] **Step 1: 실패하는 테스트 작성** — `group_result_test.go` 신규:

```go
package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type gmArgs struct {
	I int `json:"i"`
}

func (gmArgs) Kind() string { return "gr:member" }

type gmSilentArgs struct{}

func (gmSilentArgs) Kind() string { return "gr:silent" }

type gmCbArgs struct{}

func (gmCbArgs) Kind() string { return "gr:cb" }

type gmOut struct {
	Sq int `json:"sq"`
}

// 멤버 3(결과 2 + 무결과 1) → 콜백이 Add 순서로 결과 수신.
func TestGroup_CallbackReceivesOrderedResults(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	type recv struct {
		raw [][]byte
	}
	var got atomic.Pointer[recv]
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[gmArgs]) (gmOut, error) {
		return gmOut{Sq: task.Args.I * task.Args.I}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[gmSilentArgs]) error { return nil })
	AddHandler(mux, func(ctx context.Context, task *Task[gmCbArgs]) error {
		got.Store(&recv{raw: task.RawGroupResults()})
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"gr": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	c := NewClient(client)
	_, err := NewGroup().
		Add(gmArgs{I: 2}, WithQueue("gr")).      // idx 0 → {"sq":4}
		Add(gmSilentArgs{}, WithQueue("gr")).    // idx 1 → nil
		Add(gmArgs{I: 3}, WithQueue("gr")).      // idx 2 → {"sq":9}
		OnComplete(gmCbArgs{}, WithQueue("gr")).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for got.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	r := got.Load()
	if r == nil {
		t.Fatal("callback never ran")
	}
	if len(r.raw) != 3 || string(r.raw[0]) != `{"sq":4}` || r.raw[1] != nil || string(r.raw[2]) != `{"sq":9}` {
		t.Fatalf("raw results = %v", r.raw)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestGroup_CallbackReceivesOrderedResults .`
Expected: FAIL — GroupIndex/GroupSize 미설정이라 콜백이 결과를 받지 못함(`raw results` 비어 있음, 또는 잘못된 내장으로 콜백이 아예 실행되지 않음 — 어느 쪽이든 FAIL)

- [ ] **Step 3: 구현** — `group.go` 멤버 TaskMessage 구성(112행 부근)에 두 필드 추가:

```go
			msg: &base.TaskMessage{
				ID:            memberIDs[i],
				Kind:          m.args.Kind(),
				Payload:       payload,
				Queue:         options.queue,
				MaxRetry:      options.maxRetry,
				Retention:     int64(options.retention / time.Second),
				GroupID:       groupID,
				GroupQueue:    cbLink.Queue,
				GroupCallback: &cbLink,
				GroupIndex:    i,
				GroupSize:     len(g.members),
			},
```

(`g.members`가 실제 필드명과 다르면 `memberIDs` 길이 사용: `GroupSize: len(memberIDs)`.)

- [ ] **Step 3b: 재개 경로 테스트 추가** — 같은 파일에 추가 (스펙 테스트 목록의 "dead-letter 멤버 RunTask 재개 후 콜백이 전체 결과 수신"; 기존 `TestGroup_StalledByDeadLetterResumesViaRunTask`(group_test.go:147)의 셋업 관례를 따르되 결과 수신을 단언):

```go
type gmFlakyArgs struct {
	I int `json:"i"`
}

func (gmFlakyArgs) Kind() string { return "gr:flaky" }

// dead-letter로 정지한 그룹을 RunTask로 재개하면, 재실행 멤버의 결과까지
// 포함해 콜백이 전체 결과를 받는다.
func TestGroup_ResumedMemberResultReachesCallback(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var fail atomic.Bool
	fail.Store(true)
	var got atomic.Pointer[[][]byte]
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[gmArgs]) (gmOut, error) {
		return gmOut{Sq: task.Args.I * task.Args.I}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[gmFlakyArgs]) (gmOut, error) {
		if fail.Load() {
			return gmOut{}, SkipRetry(errors.New("first pass fails"))
		}
		return gmOut{Sq: task.Args.I * 100}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[gmCbArgs]) error {
		raw := task.RawGroupResults()
		got.Store(&raw)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"gr2": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())
	c := NewClient(client)

	info, err := NewGroup().
		Add(gmArgs{I: 2}, WithQueue("gr2")).           // idx 0 → {"sq":4}
		Add(gmFlakyArgs{I: 3}, WithQueue("gr2")).      // idx 1 → dead-letter 후 재개 시 {"sq":300}
		OnComplete(gmCbArgs{}, WithQueue("gr2")).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	// flaky 멤버가 dead-letter로 갈 때까지 대기.
	insp := NewInspector(client)
	flakyID := info.MemberIDs[1]
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ti, err := insp.GetTask(ctx, "gr2", flakyID); err == nil && ti.State == "archived" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	fail.Store(false)
	if err := insp.RunTask(ctx, "gr2", flakyID); err != nil {
		t.Fatalf("run task: %v", err)
	}

	for got.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got.Load() == nil {
		t.Fatal("callback never ran after resume")
	}
	raw := *got.Load()
	if len(raw) != 2 || string(raw[0]) != `{"sq":4}` || string(raw[1]) != `{"sq":300}` {
		t.Fatalf("resumed results = %v", raw)
	}
}
```

(`errors`, `sync/atomic` import 확인. `TaskInfo.State`의 실제 타입/값 표기는 inspector.go를 확인해 archived 상태 비교를 기존 테스트 관례에 맞출 것.)

- [ ] **Step 4: 통과 확인 (기존 그룹 테스트 포함)**

Run: `go test -p 1 -count=1 -run 'TestGroup' .`
Expected: 전부 PASS — 특히 기존 dead-letter→RunTask 재개 테스트가 그대로 통과해야 함(재개 경로에서 msg의 GroupIndex/Result가 hash에 보존되므로 재실행 후 결과도 정상 수집)

- [ ] **Step 5: 커밋**

```bash
git add group.go group_result_test.go
git commit -m "feat: group 멤버에 GroupIndex/GroupSize — 콜백이 Add 순서 결과 수신"
```

---

### Task 6: Inspector 노출

**Files:**
- Modify: `inspector.go` (TaskInfo 필드 + 채움)
- Test: `inspector_test.go` (기존 파일에 추가)

- [ ] **Step 1: 실패하는 테스트 작성** — `inspector_test.go`에 추가 (기존 테스트의 셋업 관례를 따를 것):

```go
func TestGetTask_ExposesResultPresence(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()
	// 결과가 있는 완료(retained) 태스크를 직접 심는다.
	msg := &base.TaskMessage{ID: "r1", Kind: "k", Queue: "iq",
		State: base.StateCompleted, Result: []byte(`{"v":1}`)}
	encoded, _ := base.EncodeMessage(msg)
	client.HSet(ctx, base.TaskKey("iq", "r1"), "msg", encoded, "state", int(base.StateCompleted))
	client.ZAdd(ctx, base.CompletedKey("iq"), redis.Z{Score: float64(time.Now().Add(time.Hour).Unix()), Member: "r1"})

	insp := NewInspector(client)
	info, err := insp.GetTask(ctx, "iq", "r1")
	if err != nil {
		t.Fatal(err)
	}
	if !info.HasResult || info.ResultSize != len(`{"v":1}`) {
		t.Errorf("HasResult=%v ResultSize=%d", info.HasResult, info.ResultSize)
	}
}
```

(import에 `redis "github.com/redis/go-redis/v9"`가 이미 있는지 확인 — 기존 파일 관례에 맞춤.)

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestGetTask_ExposesResultPresence .`
Expected: FAIL (`unknown field HasResult`)

- [ ] **Step 3: 구현** — `inspector.go`의 `TaskInfo`에 추가:

```go
	// HasResult reports whether the handler produced a result (AddHandlerR).
	// The result itself travels the workflow (PrevResult/GroupResults); the
	// Inspector only reports its presence and size.
	HasResult bool
	// ResultSize is the result's JSON size in bytes (0 when HasResult=false).
	ResultSize int
```

`TaskInfo`를 msg에서 채우는 지점(GetTask/ListTasks가 공유하는 변환 함수 — `msgToTaskInfo` 류를 찾아 그곳에)에 추가:

```go
	info.HasResult = len(msg.Result) > 0
	info.ResultSize = len(msg.Result)
```

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run 'TestGetTask|TestInspector' . && go vet ./...`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add inspector.go inspector_test.go
git commit -m "feat: Inspector TaskInfo.HasResult/ResultSize"
```

---

### Task 7: cluster 스모크 18 + tour 15 + README + 최종 검증

**Files:**
- Modify: `cluster_test.go` (체크리스트 줄 + 18번째 시나리오)
- Modify: `examples/tour/main.go` (섹션 15 + 상단 doc)
- Modify: `README.md` (Workflow 서술)

- [ ] **Step 1: cluster 스모크 18번째** — `cluster_test.go`의 체크리스트 주석(TestCluster_PauseResume 줄 뒤)에 추가:

```go
// [x] 결과 릴레이 (chain PrevResult + group cjson 내장 Lua) → TestCluster_ResultRelay
```

파일 끝에 테스트 추가 (기존 cluster 테스트의 셋업 관례 — `testutil.NewClusterRedis(t)`, 큐 이름·대기 패턴 — 를 따를 것):

```go
// 결과 릴레이: 확장된 groupCompleteCmd(KEYS 4개 — 전부 콜백 큐 해시 슬롯)와
// chain PrevResult가 라이브 클러스터에서 CROSSSLOT 없이 동작하는지.
func TestCluster_ResultRelay(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	ctx := context.Background()

	var chainGot, groupGot atomic.Int64
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[clRelayArgs]) (clRelayOut, error) {
		return clRelayOut{V: task.Args.N * 2}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[clRelayCheckArgs]) error {
		if out, err := PrevResult[clRelayOut](task); err == nil {
			chainGot.Store(int64(out.V))
		}
		return nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[clRelayCbArgs]) error {
		if rs, err := GroupResults[clRelayOut](task); err == nil && len(rs) == 2 {
			groupGot.Store(int64(rs[0].V + rs[1].V))
		}
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"clres": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())
	c := NewClient(client)

	if _, err := NewChain().
		Then(clRelayArgs{N: 21}, WithQueue("clres")).
		Then(clRelayCheckArgs{}, WithQueue("clres")).
		Enqueue(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGroup().
		Add(clRelayArgs{N: 1}, WithQueue("clres")).
		Add(clRelayArgs{N: 2}, WithQueue("clres")).
		OnComplete(clRelayCbArgs{}, WithQueue("clres")).
		Enqueue(ctx, c); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for (chainGot.Load() == 0 || groupGot.Load() == 0) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if chainGot.Load() != 42 {
		t.Errorf("chain relay = %d, want 42", chainGot.Load())
	}
	if groupGot.Load() != 6 { // 1*2 + 2*2
		t.Errorf("group results sum = %d, want 6", groupGot.Load())
	}
}

type clRelayArgs struct {
	N int `json:"n"`
}

func (clRelayArgs) Kind() string { return "clres:m" }

type clRelayOut struct {
	V int `json:"v"`
}

type clRelayCheckArgs struct{}

func (clRelayCheckArgs) Kind() string { return "clres:chk" }

type clRelayCbArgs struct{}

func (clRelayCbArgs) Kind() string { return "clres:cb" }
```

(`sync/atomic` import 확인.)

- [ ] **Step 2: cluster 검증** (docker 필요 — 없으면 `deploy/redis-cluster`에서 `docker compose up -d` 후):

Run: `make test-cluster`
Expected: 18/18 PASS

- [ ] **Step 3: tour 섹션 15** — `examples/tour/main.go`의 섹션 14 종료 지점(`cancelP2()` 뒤, 마지막 구분선 출력 전)에 추가하고, 상단 doc comment의 기능 나열에 "task results (step-to-step data passing)"를 추가:

```go
	section("15) 결과 전달: 앞 스텝의 산출물이 다음 스텝으로 — OCR→번역 축소판")
	rmux := chronos.NewMux()
	chronos.AddHandlerR(rmux, func(ctx context.Context, t *chronos.Task[OcrArgs]) (OcrOut, error) {
		fmt.Printf("   ▶ [ocr] %s 인식\n", t.Args.Image)
		return OcrOut{Text: "hello chronos"}, nil
	})
	chronos.AddHandler(rmux, func(ctx context.Context, t *chronos.Task[TranslateArgs]) error {
		out, err := chronos.PrevResult[OcrOut](t)
		if err != nil {
			return err
		}
		fmt.Printf("   ▶ [translate] 이전 스텝 결과 수신: %q → 번역 완료\n", out.Text)
		return nil
	})
	rsrv := chronos.NewServer(rdb, chronos.ServerConfig{Queues: map[string]int{"results": 1}, Concurrency: 2})
	if err := rsrv.Start(ctx, rmux); err != nil {
		fmt.Printf("results 서버 start 실패: %v\n", err)
	}
	_, _ = chronos.NewChain().
		Then(OcrArgs{Image: "scan-001.png"}, chronos.WithQueue("results")).
		Then(TranslateArgs{}, chronos.WithQueue("results")).
		Enqueue(ctx, client)
	time.Sleep(2 * time.Second)
	shutR, cancelR := context.WithTimeout(context.Background(), 3*time.Second)
	_ = rsrv.Shutdown(shutR)
	cancelR()
```

파일의 다른 args 타입들 옆에 추가:

```go
type OcrArgs struct {
	Image string `json:"image"`
}

func (OcrArgs) Kind() string { return "tour:ocr" }

type OcrOut struct {
	Text string `json:"text"`
}

type TranslateArgs struct{}

func (TranslateArgs) Kind() string { return "tour:translate" }
```

Run: `gofmt -w examples/tour/main.go && go vet ./examples/tour/ && go run ./examples/tour 2>&1 | sed -n '/=== 15)/,$p' | head -6`
Expected: `[ocr] scan-001.png 인식` → `[translate] 이전 스텝 결과 수신: "hello chronos"`

- [ ] **Step 4: README** — Workflow(Chains/Groups) 섹션에 결과 전달 소절 추가 (주변 문체에 맞춤):

```markdown
### Passing results between steps

Register a handler with `AddHandlerR` and its success return value flows
through the workflow: the next chain link reads it with
`chronos.PrevResult[R](task)`, and a group's callback receives every member's
result in Add order via `chronos.GroupResults[R](task)` (or raw bytes with
`task.RawGroupResults()`). Results are carried inside task messages — no
extra keys, no TTL to manage — and survive retries, redeliveries and
dead-letter re-runs. A result's JSON form is capped at 1 MiB
(`MaxResultSize`); larger results dead-letter the task without retry, so pass
a reference (object-store path, row ID) for big artifacts.
```

- [ ] **Step 5: 최종 검증 + 커밋**

Run: `make check` (전체 그린 확인)

```bash
git add cluster_test.go examples/tour/main.go README.md
git commit -m "docs+test: cluster 스모크 18(결과 릴레이) + tour 섹션 15 + README 결과 전달"
```

---

## 완료 후

1. k:code-reviewer 최종 브랜치 리뷰 (집중: **cjson 왕복의 메시지 무결성** — int64 정밀도·빈 배열·필드 보존, groupresult HASH의 TTL·잔존(소크 sampler `group:*` 패턴이 groupresult도 세는지 부작용 확인), Result가 retention 없는 태스크에서 불필요하게 커지는 경로, AddHandlerR와 기존 AddHandler 혼용, 재전달 시 결과 멱등성 — 같은 멤버의 중복 보고가 HSET을 덮어써도 같은 값이라 무해한지).
2. 지적 반영 → `k:commit-pr`로 PR 생성 (base main, assignee kenshin579, HEREDOC):
   - 제목: `feat: 워크플로 결과 전달 — AddHandlerR·PrevResult·GroupResults`
3. CI 그린 → 사용자 머지 승인 요청. PR 2(ThenGroup)는 머지 후 별도 계획.
