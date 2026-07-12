# Task Groups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `NewGroup().Add(...).OnComplete(...).Enqueue()` — N개 멤버 병렬 실행 후 전부 성공하면 콜백 태스크 1회 발화. 실패 시 그룹 대기 + `RunTask` 재개.

**Architecture:** 그룹 상태 = pending 멤버 ID SET(콜백 큐 해시태그 슬롯 → "SREM+비면 콜백 생성"이 단일 원자 Lua `groupCompleteCmd`, SREM 멱등). 멤버 msg에 `GroupID/GroupQueue/GroupCallback(*ChainLink 재사용)` 내장. 서버는 Chain과 같은 "보고 → Done" 순서 + 백오프 재시도. SET TTL 7일 안전망 + EXISTS 가드.

**Tech Stack:** Go, redis/go-redis v9, Lua, 실제 Redis(DB 15, `-p 1`), docker cluster(스모크 15번째).

---

## File Structure

- Modify `internal/base/task.go` — `TaskMessage.GroupID/GroupQueue/GroupCallback`. `internal/base/keys.go` — `GroupKey`. Test: `internal/base/task_test.go`
- Create `internal/rdb/group.go` — `CreateGroup`/`CompleteGroupMember` + `groupCompleteCmd` Lua + `GroupTTL` 상수 + `GroupPending`. Test: `internal/rdb/group_test.go`
- Create `group.go` (루트) — `Group` 빌더 + `GroupInfo`. Test: `group_test.go` (루트)
- Modify `server.go` — 성공 경로에 그룹 보고(Chain 처리 뒤) + `completeGroupWithRetry`.
- Modify `chronos.go` — `TaskInfo.GroupID/GroupPending`. `inspector.go` — 매핑 + GetTask의 SCARD 조회.
- Modify `cluster_test.go` — 15번째 스모크. `examples/tour/main.go` — 섹션 13. `README.md` — Groups 섹션.

**구현자 참고 (기존 코드):**
- Chain 대칭 재료: `chain.go`(빌더·resolveChainOptions·snapshotChainLink), `internal/rdb/chain.go`(chainEnqueueCmd/chainScheduleCmd — 콜백 생성이 같은 패턴), `server.go:425 enqueueNextWithRetry`(백오프 패턴).
- `base.ChainLink`{Kind,Payload,Queue,MaxRetry,NoArchive,Retention,Delay} 재사용.
- `dispatchMessage(ctx, c, msg, options)` — 멤버 enqueue에 재사용.
- 옵션 가드 유틸: `resolveChainOptions`(TaskID/Unique 거부)와 `processAtAbsolute` 플래그, `snapshotChainLink(args, opts, isLast)` — 그룹용으로 재사용/변형.
- 테스트 헬퍼: `testutil.NewRedis`, 루트 `emailArgs`, `chainArgs`(chain_test.go).

---

## Task 1: base — Group 필드 + GroupKey

**Files:**
- Modify: `internal/base/task.go`, `internal/base/keys.go`
- Test: `internal/base/task_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`internal/base/task_test.go`에 추가:

```go
func TestTaskMessage_GroupRoundTrips(t *testing.T) {
	msg := &TaskMessage{
		ID: "g:m0", Kind: "a", Queue: "default",
		GroupID: "g", GroupQueue: "cbq",
		GroupCallback: &ChainLink{Kind: "cb", Payload: []byte(`{"b":1}`), Queue: "cbq", MaxRetry: 25, Delay: 2},
	}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.GroupID != "g" || got.GroupQueue != "cbq" || got.GroupCallback == nil {
		t.Fatalf("group fields lost: %+v", got)
	}
	if got.GroupCallback.Kind != "cb" || got.GroupCallback.Delay != 2 {
		t.Errorf("callback = %+v", got.GroupCallback)
	}
}

func TestGroupKey(t *testing.T) {
	if got, want := GroupKey("cbq", "g1"), "chronos:{cbq}:group:g1"; got != want {
		t.Errorf("GroupKey = %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/base/ -run 'TestTaskMessage_GroupRoundTrips|TestGroupKey'`
Expected: FAIL — unknown field GroupID / undefined GroupKey.

- [ ] **Step 3: 구현**

`internal/base/task.go`의 `TaskMessage`에서 `ChainIndex` 아래에 추가:

```go
	// GroupID identifies the group this task is a member of ("" = not grouped).
	// Member task IDs are deterministic ("<group_id>:m<i>"), the callback's is
	// "<group_id>:cb".
	GroupID string `json:"group_id,omitempty"`
	// GroupQueue is the queue whose hash slot holds the group's pending-member
	// SET (= the callback's queue), so a completing member knows where to report.
	GroupQueue string `json:"group_queue,omitempty"`
	// GroupCallback is the callback task snapshot, carried by every member; the
	// member that empties the pending SET creates it (create-if-absent).
	GroupCallback *ChainLink `json:"group_callback,omitempty"`
```

`internal/base/keys.go`의 `CompletedKey` 아래에 추가:

```go
// GroupKey returns the SET key holding a group's pending member IDs. It lives
// in the callback queue's hash slot so "remove member + fire callback when
// empty" runs as one atomic (cluster-safe) script.
func GroupKey(cbQueue, groupID string) string {
	return QueueKeyPrefix(cbQueue) + "group:" + groupID
}
```

- [ ] **Step 4: 통과 확인**

Run: `go test ./internal/base/ -p 1 && go build ./...` → PASS, clean.

- [ ] **Step 5: 커밋**

```bash
git add internal/base/
git commit -m "feat: base Group 필드(GroupID/GroupQueue/GroupCallback) + GroupKey"
```

---

## Task 2: rdb — CreateGroup / CompleteGroupMember / GroupPending

**Files:**
- Create: `internal/rdb/group.go`
- Test: `internal/rdb/group_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`internal/rdb/group_test.go` 신규 (`package rdb`; chain_test.go의 import 패턴):

```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

// mkGroupMember returns a member message reporting to group "g" whose set (and
// callback) lives on queue "cbq".
func mkGroupMember(id string) *base.TaskMessage {
	return &base.TaskMessage{
		ID: id, Kind: "m", Queue: "default",
		GroupID: "g", GroupQueue: "cbq",
		GroupCallback: &base.ChainLink{Kind: "cb", Payload: []byte(`{}`), Queue: "cbq", MaxRetry: 25},
	}
}

func TestGroup_CompleteMembersFiresCallbackOnce(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.CreateGroup(ctx, "cbq", "g", []string{"g:m0", "g:m1"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if n, _ := r.GroupPending(ctx, "cbq", "g"); n != 2 {
		t.Fatalf("pending = %d, want 2", n)
	}

	// 첫 멤버 완료: 콜백 미발화.
	fired, err := r.CompleteGroupMember(ctx, mkGroupMember("g:m0"))
	if err != nil {
		t.Fatalf("m0: %v", err)
	}
	if fired {
		t.Fatal("callback fired after first member")
	}
	// 같은 멤버 재보고(재전달): 멱등 no-op.
	if fired, _ = r.CompleteGroupMember(ctx, mkGroupMember("g:m0")); fired {
		t.Fatal("duplicate report fired callback")
	}
	if n, _ := r.GroupPending(ctx, "cbq", "g"); n != 1 {
		t.Fatalf("pending after dup = %d, want 1", n)
	}

	// 마지막 멤버: 콜백 발화 + SET 삭제.
	fired, err = r.CompleteGroupMember(ctx, mkGroupMember("g:m1"))
	if err != nil {
		t.Fatalf("m1: %v", err)
	}
	if !fired {
		t.Fatal("callback did not fire on last member")
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("cbq")).Result(); xlen != 1 {
		t.Errorf("callback stream len = %d, want 1", xlen)
	}
	if _, err := r.GetTask(ctx, "cbq", "g:cb"); err != nil {
		t.Errorf("callback task hash missing: %v", err)
	}
	if n, _ := r.GroupPending(ctx, "cbq", "g"); n != 0 {
		t.Errorf("pending after done = %d, want 0 (set deleted)", n)
	}

	// SET 소멸 후 뒤늦은 재보고: no-op, 콜백 불변.
	if fired, _ = r.CompleteGroupMember(ctx, mkGroupMember("g:m1")); fired {
		t.Error("late report after group done fired callback again")
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("cbq")).Result(); xlen != 1 {
		t.Errorf("callback duplicated: stream len = %d, want 1", xlen)
	}
}

func TestGroup_DelayedCallbackGoesToScheduled(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.CreateGroup(ctx, "cbq", "gd", []string{"gd:m0"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	m := &base.TaskMessage{
		ID: "gd:m0", Kind: "m", Queue: "default",
		GroupID: "gd", GroupQueue: "cbq",
		GroupCallback: &base.ChainLink{Kind: "cb", Payload: []byte(`{}`), Queue: "cbq", MaxRetry: 25, Delay: 2},
	}
	fired, err := r.CompleteGroupMember(ctx, m)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !fired {
		t.Fatal("callback not fired")
	}
	score, err := client.ZScore(ctx, base.ScheduledKey("cbq"), "gd:cb").Result()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	want := float64(time.Now().Add(2 * time.Second).Unix())
	if score < want-5 || score > want+5 {
		t.Errorf("score = %v, want ~%v", score, want)
	}
}

func TestGroup_SetHasTTL(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.CreateGroup(ctx, "cbq", "gt", []string{"gt:m0"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	ttl, err := client.TTL(ctx, base.GroupKey("cbq", "gt")).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= 0 || ttl > GroupTTL {
		t.Errorf("ttl = %v, want (0, %v]", ttl, GroupTTL)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/rdb/ -run TestGroup_ -p 1`
Expected: FAIL — undefined CreateGroup/CompleteGroupMember/GroupPending/GroupTTL.

- [ ] **Step 3: 구현**

`internal/rdb/group.go` 신규:

```go
package rdb

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// GroupTTL bounds how long a group's pending-member SET may linger. It is the
// safety net for abandoned groups (a member deleted or dead-lettered and never
// re-run): after it expires the callback can no longer fire and late member
// reports become no-ops. Mirrors the archived-retention default.
const GroupTTL = 7 * 24 * time.Hour

// groupCompleteCmd removes a finished member from the group's pending SET and,
// when the SET becomes empty, creates the callback task (create-if-absent) and
// deletes the SET. A missing SET means the group already completed or expired —
// the call is then a no-op, which makes redelivered member reports safe. All
// keys share the callback queue's hash tag (cluster-safe).
// KEYS[1] group set, KEYS[2] callback task hash, KEYS[3] callback stream or
// scheduled zset.
// ARGV[1] member id, ARGV[2] callback encoded msg, ARGV[3] callback state,
// ARGV[4] callback id, ARGV[5] mode ("stream"|"zset"), ARGV[6] score (zset).
var groupCompleteCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
  return 0
end
redis.call("SREM", KEYS[1], ARGV[1])
if redis.call("SCARD", KEYS[1]) > 0 then
  return 0
end
redis.call("DEL", KEYS[1])
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2], "msg", ARGV[2], "state", ARGV[3])
if ARGV[5] == "stream" then
  redis.call("XADD", KEYS[3], "*", "task_id", ARGV[4])
else
  redis.call("ZADD", KEYS[3], ARGV[6], ARGV[4])
end
return 1
`)

// CreateGroup registers a group's pending member IDs (with the safety TTL) in
// the callback queue's slot. Called once, before the members are enqueued, so
// a partially failed multi-member enqueue can never fire the callback early.
func (r *RDB) CreateGroup(ctx context.Context, cbQueue, groupID string, memberIDs []string) error {
	if len(memberIDs) == 0 {
		return errors.New("chronos: group needs at least one member")
	}
	key := base.GroupKey(cbQueue, groupID)
	pipe := r.client.TxPipeline()
	members := make([]interface{}, len(memberIDs))
	for i, id := range memberIDs {
		members[i] = id
	}
	pipe.SAdd(ctx, key, members...)
	pipe.Expire(ctx, key, GroupTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// CompleteGroupMember reports a member's success. When it was the last pending
// member it atomically creates the group's callback task and returns true.
// Idempotent under at-least-once redelivery (SREM of an absent member and a
// missing SET are both no-ops).
func (r *RDB) CompleteGroupMember(ctx context.Context, member *base.TaskMessage) (bool, error) {
	link := member.GroupCallback
	if link == nil {
		return false, errors.New("chronos: group member has no callback snapshot")
	}
	cb := &base.TaskMessage{
		ID:        member.GroupID + ":cb",
		Kind:      link.Kind,
		Payload:   link.Payload,
		Queue:     link.Queue,
		MaxRetry:  link.MaxRetry,
		NoArchive: link.NoArchive,
		Retention: link.Retention,
	}

	// Register the callback queue in the global index up front (separate
	// command: QueuesKey has no hash tag).
	if err := r.client.SAdd(ctx, base.QueuesKey(), cb.Queue).Err(); err != nil {
		return false, err
	}

	mode, state := "stream", base.StatePending
	var score int64
	var destKey string
	if link.Delay > 0 {
		mode, state = "zset", base.StateScheduled
		score = time.Now().Add(time.Duration(link.Delay) * time.Second).Unix()
		destKey = base.ScheduledKey(cb.Queue)
	} else {
		destKey = base.StreamKey(cb.Queue)
	}
	cb.State = state
	encoded, err := base.EncodeMessage(cb)
	if err != nil {
		return false, err
	}

	keys := []string{
		base.GroupKey(member.GroupQueue, member.GroupID),
		base.TaskKey(cb.Queue, cb.ID),
		destKey,
	}
	argv := []interface{}{member.ID, encoded, int(state), cb.ID, mode, score}
	n, err := groupCompleteCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// GroupPending returns how many members of the group have not yet succeeded
// (0 when the group finished or its state expired).
func (r *RDB) GroupPending(ctx context.Context, cbQueue, groupID string) (int64, error) {
	return r.client.SCard(ctx, base.GroupKey(cbQueue, groupID)).Result()
}
```

주의: `groupCompleteCmd`의 KEYS[2]/KEYS[3]는 콜백 큐(`link.Queue`) 키인데, KEYS[1]은 `member.GroupQueue` 키다. **둘이 같은 큐(=콜백 큐)여야 같은 슬롯**이다 — 빌더가 GroupQueue를 콜백 큐로 세팅하므로 항상 일치하지만, 방어적으로 `CompleteGroupMember` 시작부에 `if member.GroupQueue != link.Queue { return false, errors.New("chronos: group state and callback must live on the callback queue") }` 가드를 추가하라.

- [ ] **Step 4: 통과 확인**

Run: `go test ./internal/rdb/ -p 1` → 전체 PASS. `go vet ./internal/rdb/ && gofmt -l internal/rdb/` clean.

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/group.go internal/rdb/group_test.go
git commit -m "feat: rdb 그룹 상태 — CreateGroup/CompleteGroupMember(원자 Lua)/GroupPending"
```

---

## Task 3: Group 빌더 + GroupInfo + Inspector 노출

**Files:**
- Create: `group.go` (루트)
- Modify: `chronos.go` (TaskInfo.GroupID/GroupPending), `inspector.go` (매핑 + GetTask SCARD)
- Test: `group_test.go` (루트)

- [ ] **Step 1: 실패 테스트 작성**

`group_test.go` 신규 (`package chronos`):

```go
package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestGroup_BuilderValidation(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// 멤버 0개 → 에러.
	if _, err := NewGroup().OnComplete(emailArgs{UserID: "cb"}).Enqueue(ctx, c); err == nil {
		t.Error("no members: want error")
	}
	// OnComplete 누락 → 에러.
	if _, err := NewGroup().Add(emailArgs{UserID: "a"}).Enqueue(ctx, c); err == nil {
		t.Error("no callback: want error")
	}
	// 멤버의 WithTaskID/WithUnique → 에러.
	if _, err := NewGroup().Add(emailArgs{UserID: "a"}, WithTaskID("x")).
		OnComplete(emailArgs{UserID: "cb"}).Enqueue(ctx, c); err == nil {
		t.Error("WithTaskID member: want error")
	}
	if _, err := NewGroup().Add(emailArgs{UserID: "a"}, WithUnique(time.Minute)).
		OnComplete(emailArgs{UserID: "cb"}).Enqueue(ctx, c); err == nil {
		t.Error("WithUnique member: want error")
	}
	// 콜백의 WithProcessAt → 에러 (그룹 완료 기준 상대 지연만 허용).
	if _, err := NewGroup().Add(emailArgs{UserID: "a"}).
		OnComplete(emailArgs{UserID: "cb"}, WithProcessAt(time.Now().Add(time.Hour))).
		Enqueue(ctx, c); err == nil {
		t.Error("WithProcessAt callback: want error")
	}

	// 정상: GroupInfo 반환.
	info, err := NewGroup().
		Add(emailArgs{UserID: "m0"}, WithProcessIn(time.Hour)). // scheduled 멤버로 조회 가능하게
		Add(emailArgs{UserID: "m1"}, WithProcessIn(time.Hour)).
		OnComplete(emailArgs{UserID: "cb"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if info.GroupID == "" || len(info.MemberIDs) != 2 || info.CallbackID != info.GroupID+":cb" {
		t.Errorf("GroupInfo = %+v", info)
	}

	// Inspector 노출: 멤버의 GroupID + GroupPending(잔여 2).
	insp := NewInspector(client)
	got, err := insp.GetTask(ctx, "default", info.MemberIDs[0])
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GroupID != info.GroupID {
		t.Errorf("GroupID = %q, want %q", got.GroupID, info.GroupID)
	}
	if got.GroupPending != 2 {
		t.Errorf("GroupPending = %d, want 2", got.GroupPending)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test . -run TestGroup_BuilderValidation -p 1`
Expected: FAIL — undefined NewGroup.

- [ ] **Step 3: group.go 구현**

`group.go` 신규 (루트, `package chronos`; chain.go의 구조를 그대로 따름):

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

// Group builds a parallel fan-out: every member is enqueued at once, and when
// ALL members have succeeded, the callback task is enqueued exactly once while
// its record exists. A member that exhausts its retries parks the group:
// re-run its dead-letter (Inspector/CLI RunTask) and, once it succeeds, the
// group resumes — the same stop-and-resume rule chains follow. Abandoned
// groups (a member deleted and never re-run) expire after rdb.GroupTTL.
//
// Handlers must be idempotent, as everywhere in chronos-go.
type Group struct {
	members []struct {
		args TaskArgs
		opts []Option
	}
	callback     TaskArgs
	callbackOpts []Option
	hasCallback  bool
}

// GroupInfo describes an enqueued group.
type GroupInfo struct {
	GroupID    string   // the group's identity
	MemberIDs  []string // deterministic member task IDs ("<groupID>:m<i>")
	CallbackID string   // the callback's task ID ("<groupID>:cb")
}

// NewGroup returns an empty group builder.
func NewGroup() *Group { return &Group{} }

// Add appends a member task. Members run in parallel on their own queues with
// their own options; WithTaskID and WithUnique are rejected at Enqueue time.
func (g *Group) Add(args TaskArgs, opts ...Option) *Group {
	g.members = append(g.members, struct {
		args TaskArgs
		opts []Option
	}{args, opts})
	return g
}

// OnComplete sets the callback task, enqueued once every member has succeeded.
// WithProcessIn delays it relative to the group's completion; WithProcessAt,
// WithTaskID and WithUnique are rejected.
func (g *Group) OnComplete(args TaskArgs, opts ...Option) *Group {
	g.callback = args
	g.callbackOpts = opts
	g.hasCallback = true
	return g
}

// Enqueue creates the group's pending-member record first (so a partially
// failed enqueue can never fire the callback early), then enqueues every
// member. Members enqueue sequentially and non-atomically: on error, already
// enqueued members will still run, and the group simply never completes (its
// record expires after rdb.GroupTTL).
func (g *Group) Enqueue(ctx context.Context, c *Client) (*GroupInfo, error) {
	if len(g.members) == 0 {
		return nil, errors.New("chronos: group needs at least one member")
	}
	if !g.hasCallback {
		return nil, errors.New("chronos: group needs a callback (OnComplete)")
	}

	// Callback snapshot (same rules as a chain tail link: relative delay only).
	cbLink, err := snapshotChainLink(g.callback, g.callbackOpts, true)
	if err != nil {
		return nil, fmt.Errorf("group callback: %w", err)
	}

	groupID := uuid.NewString()
	memberIDs := make([]string, len(g.members))
	for i := range g.members {
		memberIDs[i] = fmt.Sprintf("%s:m%d", groupID, i)
	}

	// 1) Register the pending-member SET before any member can possibly finish.
	if err := c.rdb.CreateGroup(ctx, cbLink.Queue, groupID, memberIDs); err != nil {
		return nil, err
	}

	// 2) Enqueue the members.
	for i, m := range g.members {
		options, err := resolveChainOptions(m.opts)
		if err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
		payload, err := encodeArgs(m.args)
		if err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
		msg := &base.TaskMessage{
			ID:            memberIDs[i],
			Kind:          m.args.Kind(),
			Payload:       payload,
			Queue:         options.queue,
			MaxRetry:      options.maxRetry,
			NoArchive:     options.noArchive,
			Retention:     int64(options.retention / time.Second),
			GroupID:       groupID,
			GroupQueue:    cbLink.Queue,
			GroupCallback: &cbLink,
		}
		if err := dispatchMessage(ctx, c, msg, options); err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
	}

	return &GroupInfo{
		GroupID:    groupID,
		MemberIDs:  memberIDs,
		CallbackID: groupID + ":cb",
	}, nil
}
```

주의: `snapshotChainLink`의 시그니처는 `(args TaskArgs, opts []Option, isLast bool)` — isLast=true로 호출하면 NoArchive 콜백 허용(스펙 의도와 일치). `cbLink`가 값 타입이면 `&cbLink` 주소 사용에 문제 없는지 확인(값 복사라 안전).

- [ ] **Step 4: Inspector 노출**

`chronos.go` TaskInfo에 `ChainPending` 아래 추가:
```go
	GroupID      string // group this task belongs to ("" = none)
	GroupPending int    // members of that group not yet succeeded (GetTask only)
```
`inspector.go`:
- `taskInfoFromMsg`에 `ti.GroupID = m.GroupID` 추가.
- `GetTask`에서 반환 직전, GroupID가 있으면 잔여 수 조회:
```go
	if msg.GroupID != "" && msg.GroupQueue != "" {
		if n, perr := i.rdb.GroupPending(ctx, msg.GroupQueue, msg.GroupID); perr == nil {
			ti.GroupPending = int(n)
		}
	}
```
(`ListTasks`는 SCARD N회 비용을 피하려 GroupPending을 채우지 않음 — GroupID만. doc comment에 명시.)

- [ ] **Step 5: 통과 확인**

Run: `go test . -run TestGroup_BuilderValidation -p 1 -race` → PASS. 전체 `go test . -p 1` → PASS.

- [ ] **Step 6: 커밋**

```bash
git add group.go group_test.go chronos.go inspector.go
git commit -m "feat: Group 빌더 (NewGroup/Add/OnComplete) + GroupID/GroupPending 노출"
```

---

## Task 4: 서버 — 멤버 성공 시 그룹 보고

**Files:**
- Modify: `server.go`
- Test: `group_test.go` (통합 2개 추가)

- [ ] **Step 1: 실패 테스트 작성**

`group_test.go`에 추가 (import `"sync/atomic"`, `"errors"` 추가):

```go
// groupArgs is a dedicated kind for group integration tests.
type groupArgs struct {
	N int `json:"n"`
}

func (groupArgs) Kind() string { return "test:groupmember" }

// groupCbArgs is the callback kind.
type groupCbArgs struct {
	Batch string `json:"batch"`
}

func (groupCbArgs) Kind() string { return "test:groupcb" }

func TestGroup_FanOutFiresCallbackOnce(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var members, callbacks atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[groupArgs]) error {
		members.Add(1)
		return nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[groupCbArgs]) error {
		callbacks.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1, "gq": 1},
		Concurrency: 8, // 동시 완료 경합 유도
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	g := NewGroup()
	for i := 0; i < 6; i++ {
		q := "default"
		if i%2 == 1 {
			q = "gq"
		}
		g.Add(groupArgs{N: i}, WithQueue(q))
	}
	if _, err := g.OnComplete(groupCbArgs{Batch: "b1"}).Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for callbacks.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("callback never fired (members done=%d)", members.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // 중복 발화 시간 여유
	if n := callbacks.Load(); n != 1 {
		t.Errorf("callbacks = %d, want exactly 1", n)
	}
	if n := members.Load(); n != 6 {
		t.Errorf("members = %d, want 6", n)
	}
}

func TestGroup_StalledByDeadLetterResumesViaRunTask(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var failN2 atomic.Bool
	failN2.Store(true)
	var callbacks atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[groupArgs]) error {
		if task.Args.N == 2 && failN2.Load() {
			return errors.New("member 2 boom")
		}
		return nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[groupCbArgs]) error {
		callbacks.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 2})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := NewGroup().
		Add(groupArgs{N: 1}).
		Add(groupArgs{N: 2}, WithMaxRetry(0)). // 즉시 dead-letter
		OnComplete(groupCbArgs{Batch: "b2"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// 멤버 2가 archived로 갈 때까지 대기 → 그룹 대기(콜백 미발화).
	insp := NewInspector(client)
	deadIDIdx := info.MemberIDs[1]
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, gerr := insp.GetTask(ctx, "default", deadIDIdx)
		if gerr == nil && got.State == "archived" {
			if got.GroupID != info.GroupID || got.GroupPending != 1 {
				t.Fatalf("dead member group info wrong: %+v", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("member 2 never dead-lettered")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if callbacks.Load() != 0 {
		t.Fatal("callback fired despite stalled group")
	}

	// 원인 해소 → 재실행 → 그룹 재개, 콜백 발화.
	failN2.Store(false)
	if err := insp.RunTask(ctx, "default", deadIDIdx); err != nil {
		t.Fatalf("run: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for callbacks.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("group did not resume after RunTask")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test . -run 'TestGroup_FanOut|TestGroup_Stalled' -p 1`
Expected: FAIL — 콜백이 영원히 발화하지 않음(서버가 그룹 보고를 안 하므로 타임아웃).

- [ ] **Step 3: 구현 — server.go**

`process` 성공 블록에서 Chain 처리(`msg.Chain = nil` 뒤) 다음, `Done` 앞에 추가:

```go
		// A group member reports its completion BEFORE acking, mirroring the
		// chain rule and for the same reason: ack-then-crash must not lose the
		// group's progress. The report is idempotent (SREM + create-if-absent
		// callback), so redelivery cannot double-fire the callback.
		if msg.GroupID != "" {
			if gerr := s.completeGroupWithRetry(opCtx, msg); gerr != nil {
				s.logger.Error("chronos: group completion report failed; leaving task for redelivery",
					"id", msg.ID, "group", msg.GroupID, "error", gerr)
				return
			}
		}
```

그 아래(enqueueNextWithRetry 근처)에 추가:

```go
// completeGroupWithRetry reports a member's success with a short backoff, for
// the same reason enqueueNextWithRetry exists: one transient Redis hiccup must
// not park a succeeded task for the recoverer.
func (s *Server) completeGroupWithRetry(ctx context.Context, msg *base.TaskMessage) error {
	var err error
	for attempt, backoff := 0, 50*time.Millisecond; attempt < 3; attempt, backoff = attempt+1, backoff*4 {
		var fired bool
		if fired, err = s.rdb.CompleteGroupMember(ctx, msg); err == nil {
			if fired {
				s.logger.Debug("chronos: group callback fired", "group", msg.GroupID)
			}
			return nil
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return err
		}
	}
	return err
}
```

- [ ] **Step 4: 통과 확인**

Run: `go test . -run 'TestGroup_' -p 1 -race` → 전부 PASS (Task 3 포함 3개).
전체 회귀: `make check` → PASS.

- [ ] **Step 5: 커밋**

```bash
git add server.go group_test.go
git commit -m "feat: 서버 그룹 보고 (멤버 성공 시 SREM→마지막이면 콜백, 멱등)"
```

---

## Task 5: cluster 스모크 15번째

**Files:**
- Modify: `cluster_test.go`

**전제:** docker 클러스터 (`deploy/redis-cluster` up, `cluster_state:ok`). 다운이면 `docker compose up -d` 후 진행; docker 데몬 없으면 BLOCKED.

- [ ] **Step 1: 테스트 + 체크리스트**

체크리스트에 추가:
```go
//  [x] CreateGroup + groupCompleteCmd (group fan-out)       → TestCluster_GroupFanOut
```
파일 끝에 추가 (`clArgs` 재사용; 콜백도 clArgs로 — kind가 같아도 핸들러에서 N으로 구분):

```go
func TestCluster_GroupFanOut(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var members, callbacks atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		if task.Args.N == 99 {
			callbacks.Add(1)
		} else {
			members.Add(1)
		}
		return nil
	})
	// 멤버는 서로 다른 슬롯 큐 2개, 콜백은 제3의 큐 — 그룹 SET/콜백이 콜백 큐
	// 슬롯에서 원자 처리되고, 멤버 보고가 슬롯을 넘나드는 구성을 검증.
	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"alpha": 1, "bravo": 1, "cbq": 1},
		Concurrency:     4,
		ForwardInterval: 200 * time.Millisecond,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := NewGroup().
		Add(clArgs{N: 31}, WithQueue("alpha")).
		Add(clArgs{N: 32}, WithQueue("bravo")).
		Add(clArgs{N: 33}, WithQueue("alpha")).
		OnComplete(clArgs{N: 99}, WithQueue("cbq")).
		Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 10*time.Second, "3 members then callback", func() bool {
		return members.Load() == 3 && callbacks.Load() == 1
	})
}
```

- [ ] **Step 2: 실행 (15개, 2회)**

Run: `REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 go test -run 'TestCluster_' -p 1 -race -count=1 . 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: 15/15 PASS × 2회. CROSSSLOT → BLOCKED.

- [ ] **Step 3: 커밋**

```bash
git add cluster_test.go
git commit -m "test: cluster 스모크에 group fan-out 추가 (슬롯 교차 보고)"
```

---

## Task 6: tour 섹션 13 + README + 최종 검증 + 리뷰 + PR

**Files:**
- Modify: `examples/tour/main.go`, `README.md`

- [ ] **Step 1: tour 섹션 13**

타입 추가(기존 타입 정의부):

```go
// GroupMemberArgs is one member of the group demo.
type GroupMemberArgs struct {
	N int `json:"n"`
}

func (GroupMemberArgs) Kind() string { return "demo:groupmember" }

// GroupReportArgs is the group demo's callback.
type GroupReportArgs struct {
	Batch string `json:"batch"`
}

func (GroupReportArgs) Kind() string { return "demo:groupreport" }
```

섹션 12 종료(`cancelC()`) 뒤에 추가:

```go
	section("13) group: N개 병렬 실행 → 전부 성공하면 콜백 1회 (실패 시 대기, 재실행으로 재개)")
	var gFail atomic.Bool
	gFail.Store(true)
	gmux := chronos.NewMux()
	chronos.AddHandler(gmux, func(ctx context.Context, t *chronos.Task[GroupMemberArgs]) error {
		if t.Args.N == 2 && gFail.Load() {
			fmt.Printf("   💥 [group] 멤버 %d 실패 — 그룹 대기\n", t.Args.N)
			return errors.New("멤버 2 오류")
		}
		fmt.Printf("   🧩 [group] 멤버 %d 완료\n", t.Args.N)
		return nil
	})
	chronos.AddHandler(gmux, func(ctx context.Context, t *chronos.Task[GroupReportArgs]) error {
		fmt.Printf("   🎉 [group] 콜백 실행 — 배치 %s 전원 완료!\n", t.Args.Batch)
		return nil
	})
	gsrv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"group-demo": 1},
		Concurrency: 4,
	})
	if err := gsrv.Start(ctx, gmux); err != nil {
		fmt.Printf("group 서버 start 실패: %v\n", err)
	}
	ginfo, err := chronos.NewGroup().
		Add(GroupMemberArgs{N: 1}, chronos.WithQueue("group-demo")).
		Add(GroupMemberArgs{N: 2}, chronos.WithQueue("group-demo"), chronos.WithMaxRetry(0)).
		Add(GroupMemberArgs{N: 3}, chronos.WithQueue("group-demo")).
		OnComplete(GroupReportArgs{Batch: "demo"}, chronos.WithQueue("group-demo")).
		Enqueue(ctx, client)
	if err != nil {
		fmt.Printf("group enqueue 실패: %v\n", err)
	}
	time.Sleep(1500 * time.Millisecond) // 1·3 완료, 2 dead-letter → 그룹 대기
	if got, gerr := insp.GetTask(ctx, "group-demo", ginfo.MemberIDs[1]); gerr == nil {
		fmt.Printf("   📮 dead-letter 멤버: %s (그룹 잔여 %d명 — 콜백 대기 중)\n", got.ID, got.GroupPending)
		gFail.Store(false)
		fmt.Println("   원인 해소 후 RunTask로 재실행 → 그룹 재개:")
		if rerr := insp.RunTask(ctx, "group-demo", got.ID); rerr != nil {
			fmt.Printf("   run 실패: %v\n", rerr)
		}
	}
	time.Sleep(1500 * time.Millisecond) // 멤버 2 재실행 + 콜백
	shutGroupCtx, cancelG := context.WithTimeout(context.Background(), 3*time.Second)
	_ = gsrv.Shutdown(shutGroupCtx)
	cancelG()
```

상단 doc comment에 `, task chains, and task groups`로 갱신(기존 chains 표현에 groups 추가).

Run: `gofmt -w examples/tour/main.go && go vet ./examples/tour/ && go run ./examples/tour 2>&1 | sed -n '/=== 13)/,$p'`
Expected: 멤버 1·3 완료 → 멤버 2 실패 → dead-letter(잔여 1명) → 재실행 → 멤버 2 완료 → 🎉 콜백.

- [ ] **Step 2: README**

(a) Highlights의 Chains 항목을 다음으로 교체:
```markdown
- **Chains & groups** — run tasks in sequence (`NewChain`) or fan out in
  parallel and fire a callback when every member succeeds (`NewGroup`); a
  failure stops the flow, and re-running its dead-letter resumes it.
```
(b) "## Chains" 섹션 뒤에 추가:
```markdown
## Groups

Fan out members in parallel and run a callback once **all of them succeed**:

```go
info, err := chronos.NewGroup().
	Add(ResizeArgs{File: "a.jpg"}).
	Add(ResizeArgs{File: "b.jpg"}, chronos.WithQueue("low")).
	OnComplete(ReportArgs{Batch: "b1"}, chronos.WithRetention(time.Hour)).
	Enqueue(ctx, client)
```

- Members run on any queues with per-member options; the callback fires exactly
  once while its record exists (idempotent tracking — an at-least-once
  redelivery cannot double-fire it).
- **A failed member parks the group.** Its dead-letter shows the group
  (`GroupID`, remaining members via `GroupPending` in the Inspector); re-run it
  and, once it succeeds, the callback fires if it was the last one.
- Abandoned groups (a member deleted and never re-run) expire after 7 days —
  the callback then never fires.
- Enqueueing members is not atomic: if it fails midway, already-enqueued
  members still run, but the callback can never fire early.
- Not yet composable with chains (no group-as-chain-link); callback payloads
  are fixed at build time (no result passing).
```
(c) Known limitations의 groups 문구 갱신: `- Not yet built: a web UI, task groups (parallel fan-out with a completion callback — chains are supported).` →
```markdown
- Not yet built: a web UI, chain×group composition (groups and chains cannot
  nest yet), result passing between workflow steps.
```

- [ ] **Step 3: 최종 검증**

```bash
make check
make test-cluster   # 15개
go run ./examples/tour  # 13섹션
```

- [ ] **Step 4: 커밋**

```bash
git add examples/tour/main.go README.md
git commit -m "docs: tour 섹션 13(group) + README Groups 섹션"
```

- [ ] **Step 5: 코드 리뷰 + PR**

k:code-reviewer로 브랜치 전체 리뷰 — 특히: groupCompleteCmd의 원자성·경합(동시 마지막-멤버), EXISTS 가드의 재전달 안전성, GroupQueue≠콜백큐 방어 가드, 멤버 enqueue 비원자성의 문서 정합, 멤버가 chain 필드와 공존 불가(생성 경로상)한지, GetTask GroupPending의 SCARD 비용, 테스트 플레이키. 반영 후:

```bash
gh pr create --assignee kenshin579 --title "feat: Task Groups — 병렬 fan-out + 완료 콜백 (NewGroup/Add/OnComplete)" --body "$(cat <<'EOF'
## 배경
N개 태스크 병렬 실행 후 전부 성공하면 콜백 1회 — 사용자가 직접 만들기 가장 어려운 부분(at-least-once에서 N개 완료의 원자·멱등 추적)을 라이브러리가 담당. Chain과 합쳐 워크플로 기본기 완성. 그룹×체인 조합/중첩/부분성공 콜백은 범위 제외.

## 변경
- `NewGroup().Add(args, opts...).OnComplete(cb, opts...).Enqueue()` → `*GroupInfo{GroupID, MemberIDs, CallbackID}`.
- **그룹 상태 = pending 멤버 ID SET**(카운터 아님 — SREM 멱등으로 재전달 중복 감소 원천 차단). SET은 **콜백 큐 해시태그 슬롯**에 배치 → "SREM+비면 콜백 create-if-absent+SET 삭제"가 단일 원자 Lua(`groupCompleteCmd`), Cluster-safe. 멤버 큐는 자유.
- 부분 실패 = **그룹 대기 + RunTask 재개**(Chain과 동일 규칙). SET TTL 7일 안전망 + EXISTS 가드(만료·완료 후 뒤늦은 보고 no-op).
- 서버: 멤버 성공 시 "그룹 보고(백오프 3회) → Done" 순서(Chain과 동일 이유·패턴).
- 관찰: `TaskInfo.GroupID`/`GroupPending`(GetTask), tour 섹션 13(fan-out→대기→재개→콜백), cluster 스모크 15개(멤버 2슬롯+콜백 제3큐), README Groups.

## 테스트 계획
- [x] make check 무회귀 (그룹 테스트: fan-out 콜백 1회/멱등/지연 콜백/TTL/중단·재개/빌더 검증)
- [x] make test-cluster 15/15
- [x] go run ./examples/tour 섹션 13 눈 확인

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review (계획 작성자 확인 완료)

- **스펙 커버리지**: A 빌더+GroupInfo(T3) / B 데이터모델(T1) / C Enqueue 흐름·SET 선생성(T3) / D groupCompleteCmd·서버 보고(T2,T4) / E 실패·엣지(T2 멱등·TTL·no-op, T4 중단·재개) / F 노출·tour·cluster·README(T3,T5,T6) — 전 항목 매핑.
- **placeholder**: 전 스텝 실제 코드·명령·기대출력 포함.
- **타입 일관성**: `CreateGroup(ctx, cbQueue, groupID, memberIDs)`/`CompleteGroupMember(ctx, member) (bool, error)`/`GroupPending(ctx, cbQueue, groupID)`(T2)를 T3(빌더)·T4(서버)·inspector가 동일 시그니처로 사용. `snapshotChainLink(args, opts, isLast)`·`resolveChainOptions`·`dispatchMessage`는 chain.go의 실제 시그니처(직전 마일스톤 확정)와 일치. `GroupInfo` 필드가 테스트·tour에서 일관.
- **주의**: 멤버 msg는 Chain 필드와 공존하지 않음(생성 경로상 — NewGroup만 Group 필드를, NewChain만 Chain 필드를 세팅). 서버 성공 경로는 Chain 처리 후 Group 처리 순서로 독립 배치.
