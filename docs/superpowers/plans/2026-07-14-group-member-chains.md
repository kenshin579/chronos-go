# 그룹 멤버 체인 (1레벨 재귀 중첩) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Group.AddChain(chain)`으로 그룹 멤버가 체인일 수 있게 한다 — 각 멤버 체인의 마지막 링크가 부모 그룹에 보고한다.

**Architecture:** 그룹 보고 필드(GroupID/GroupQueue/GroupCallback/GroupIndex/GroupMemberID)를 멤버 체인의 첫 링크에 싣고 `enqueueNext`가 후속으로 전파하며, 서버 성공 경로는 "후속이 없는 링크(마지막)"에서만 부모 그룹에 SREM 보고한다. 새 `GroupMemberID`가 pending SET 슬롯(`<groupID>:m<j>`)과 태스크 자신의 ID(`<groupID>:m<j>:<i>`)를 분리한다. 새 Lua·cross-slot 연산 없음.

**Tech Stack:** Go 1.26, 기존 chronos 내부(base/rdb/server/chain/group).

**스펙:** `docs/superpowers/specs/2026-07-14-group-member-chains-design.md`

**전제:**
- 브랜치 `feat/group-member-chains`. 로컬 Redis(127.0.0.1:6379) 필요, 테스트 `-p 1`.
- **메인 작업 트리에서 git checkout/switch 금지** (파일 수정·커밋만).
- 커밋 author `-c user.email=kenshin579@hotmail.com -c user.name=kenshin579`로 커밋별 지정, 메시지 끝 빈 줄 후 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`, HEREDOC.
- 루트 모듈 테스트: `go test -p 1 -count=1 -run '<패턴>' .`

**확인된 기존 코드 (그대로 활용):**
- `group.go`: `Group{members []struct{args TaskArgs; opts []Option}; callback; callbackOpts; hasCallback}`, `Add`, `OnComplete`, `Enqueue`(cbLink=snapshotChainLink(callback,opts,true); groupID=uuid; memberIDs `<groupID>:m<i>`; 멤버 검증 resolveChainOptions+noArchive거부+GroupTTL초과거부; CreateGroup 후 dispatchMessage; 멤버 msg에 GroupID/GroupQueue=cbLink.Queue/GroupCallback=&cbLink/GroupIndex/GroupSize)
- `chain.go`: `chainStage{args; opts; group *Group; isGroup bool}`, `Chain{stages []chainStage}`, `Then`, `ThenGroup`, `Enqueue`(stages[0].isGroup 거부; chainID=uuid; msg ID `<chainID>:0`, Chain=tail; dispatchMessage), `snapshotTail`, `snapshotStage`, `snapshotGroupStage`(g.members 순회해 GroupMemberLink 구성 — **이 순회가 members 구조 변경 시 함께 바뀌어야 함**), `resolveChainOptions`, `snapshotChainLink`, `errNoArchiveMidChain`, `captureRelativeDelay`
- `server.go`: process 성공 경로 `if len(msg.Chain)>0 { enqueueNextWithRetry; msg.Chain=nil }; if msg.GroupID!="" { completeGroupWithRetry }`. `enqueueNext`(msg.Chain[0]로 next 구성, ID `<chainID>:<i+1>`, PrevResult=msg.Result; group 스테이지면 enqueueNextGroup). `enqueueNextGroup`, `drainCompletedStageMember`.
- `internal/rdb/group.go`: `CompleteGroupMember(ctx, member)` — SREM에 `member.ID` 사용(ARGV[1]), cb 구성, groupCompleteCmd 실행. `CreateGroup`, `GroupMembers`, `GroupTTL`.
- `internal/base/task.go`: `TaskMessage`(GroupID/GroupQueue/GroupCallback *ChainLink/GroupIndex/GroupSize/GroupCallbackChain/Result/PrevResult 등). `ChainLink{Kind,Payload,Queue,MaxRetry,NoArchive,Retention,Delay,Group []GroupMemberLink}`.
- `chronos.go`: `dispatchMessage(ctx,c,msg,options)`, `encodeArgs`, `resolveChainOptions`.

---

### Task 1: base — GroupMemberID 필드

**Files:**
- Modify: `internal/base/task.go`
- Test: `internal/base/task_test.go` (추가)

- [ ] **Step 1: 실패하는 테스트 작성** — `internal/base/task_test.go`에 추가:

```go
func TestGroupMemberIDRoundTrip(t *testing.T) {
	in := &TaskMessage{
		ID: "g1:m2:3", Kind: "k", Queue: "q",
		GroupID: "g1", GroupQueue: "cbq", GroupIndex: 2,
		GroupMemberID: "g1:m2", // pending SET 슬롯 (자기 ID와 다름)
	}
	b, err := EncodeMessage(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeMessage(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.GroupMemberID != "g1:m2" {
		t.Errorf("GroupMemberID lost: %q", out.GroupMemberID)
	}
	// 빈 값은 생략(하위호환 — flat 멤버는 이 필드 없이 SREM에 ID 폴백).
	empty, _ := EncodeMessage(&TaskMessage{ID: "t", Kind: "k", Queue: "q"})
	if strings.Contains(string(empty), "group_member_id") {
		t.Error("empty GroupMemberID must be omitted")
	}
}
```

(`strings` import 없으면 추가.)

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestGroupMemberIDRoundTrip ./internal/base/`
Expected: FAIL (`unknown field GroupMemberID`)

- [ ] **Step 3: 최소 구현** — `internal/base/task.go`의 `TaskMessage`에서 `GroupSize` 필드 바로 뒤(또는 `GroupCallbackChain` 근처 그룹 필드 묶음)에 추가:

```go
	// GroupMemberID is the pending-SET entry this task reports against when it
	// completes a group. For a flat member it equals the task's own ID and may
	// be empty (CompleteGroupMember falls back to ID). For a chain member it is
	// the member slot "<groupID>:m<j>", distinct from the terminal link's own
	// ID "<groupID>:m<j>:<i>".
	GroupMemberID string `json:"group_member_id,omitempty"`
```

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run TestGroupMemberIDRoundTrip ./internal/base/ && go build ./...`
Expected: PASS, 빌드 성공

- [ ] **Step 5: 커밋**

```bash
git add internal/base/task.go internal/base/task_test.go
git commit -m "feat: TaskMessage.GroupMemberID (pending SET 슬롯 ↔ 태스크 ID 분리)"
```

---

### Task 2: rdb — CompleteGroupMember가 GroupMemberID로 SREM

**Files:**
- Modify: `internal/rdb/group.go`
- Test: `internal/rdb/group_test.go` (추가)

- [ ] **Step 1: 실패하는 테스트 작성** — `internal/rdb/group_test.go`에 추가 (기존 관례 `client := testutil.NewRedis(t)` + `r := NewRDB(client)`):

```go
// 체인 멤버: 마지막 링크의 자기 ID는 "g:m0:2"지만 SREM 대상은 슬롯 "g:m0".
func TestCompleteGroupMember_SremsGroupMemberSlot(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	cb := &base.ChainLink{Kind: "cb", Payload: []byte(`{}`), Queue: "gq"}
	if err := r.CreateGroup(ctx, "gq", "g", []string{"g:m0", "g:m1"}); err != nil {
		t.Fatal(err)
	}
	// 멤버 0 = 체인, 마지막 링크가 보고: 자기 ID "g:m0:2", 슬롯 "g:m0".
	m0 := &base.TaskMessage{
		ID: "g:m0:2", Kind: "cb", Queue: "gq",
		GroupID: "g", GroupQueue: "gq", GroupCallback: cb,
		GroupIndex: 0, GroupSize: 2, GroupMemberID: "g:m0",
	}
	if fired, err := r.CompleteGroupMember(ctx, m0); err != nil || fired {
		t.Fatalf("m0: fired=%v err=%v", fired, err)
	}
	if ok, _ := client.SIsMember(ctx, base.GroupKey("gq", "g"), "g:m0").Result(); ok {
		t.Error("slot g:m0 should have been SREM'd")
	}
	// 멤버 1 = flat, GroupMemberID 없음 → ID 폴백.
	m1 := &base.TaskMessage{
		ID: "g:m1", Kind: "cb", Queue: "gq",
		GroupID: "g", GroupQueue: "gq", GroupCallback: cb,
		GroupIndex: 1, GroupSize: 2,
	}
	fired, err := r.CompleteGroupMember(ctx, m1)
	if err != nil || !fired {
		t.Fatalf("m1: fired=%v err=%v", fired, err)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestCompleteGroupMember_SremsGroupMemberSlot ./internal/rdb/`
Expected: FAIL — 슬롯 `g:m0`이 SREM되지 않음(현재 `member.ID`="g:m0:2"로 SREM해서 슬롯이 남음 → 콜백도 안 뜸)

- [ ] **Step 3: 구현** — `internal/rdb/group.go`의 `CompleteGroupMember`에서 groupCompleteCmd 호출 직전, ARGV 구성 부분을 수정. 현재 `argv := []interface{}{member.ID, ...}`에서 첫 인자를 슬롯 ID로:

```go
	memberSlot := member.GroupMemberID
	if memberSlot == "" {
		memberSlot = member.ID // flat member: slot == own ID
	}
	// ... (기존 keys 구성 그대로) ...
	// ARGV[1]을 member.ID → memberSlot으로 교체:
	argv := []interface{}{memberSlot, encoded, int(state), cb.ID, mode, score,
		int(GroupTTL / time.Second), resultB64, member.GroupIndex, member.GroupSize}
```

(정확한 argv 순서는 기존 코드 그대로 두되 **ARGV[1]만** `member.ID`→`memberSlot`으로 바꾼다. 나머지 인자는 손대지 않는다.)

- [ ] **Step 4: 통과 확인 (기존 group 테스트 포함)**

Run: `go test -p 1 -count=1 -run 'TestCompleteGroupMember|TestGroup|TestCreateGroup' ./internal/rdb/`
Expected: 신규 + 기존 전부 PASS (기존 flat 멤버는 GroupMemberID 빈 값 → ID 폴백이라 동작 불변)

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/group.go internal/rdb/group_test.go
git commit -m "feat: CompleteGroupMember가 GroupMemberID 슬롯으로 SREM (ID 폴백)"
```

---

### Task 3: 빌더 — Group.AddChain + 멤버 구조 확장 + 검증

**Files:**
- Modify: `group.go` (members 구조, Add/AddChain, Enqueue 체인 멤버 경로)
- Modify: `chain.go` (hasGroupStage, snapshotForMember, snapshotGroupStage의 members 순회 적응)
- Test: `group_member_chain_builder_test.go` (신규, 검증 에러는 Redis 불필요)

- [ ] **Step 1: 실패하는 테스트 작성** — `group_member_chain_builder_test.go` 신규:

```go
package chronos

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type mcDump struct{ T string `json:"t"` }

func (mcDump) Kind() string { return "mc:dump" }

type mcXform struct{ T string `json:"t"` }

func (mcXform) Kind() string { return "mc:xform" }

type mcLoad struct{ T string `json:"t"` }

func (mcLoad) Kind() string { return "mc:load" }

type mcVerify struct{}

func (mcVerify) Kind() string { return "mc:verify" }

type mcInner struct{}

func (mcInner) Kind() string { return "mc:inner" }

// 검증 에러는 Enqueue 시점에 결정적으로 발생 — Redis 도달 전.
func groupEnqErr(t *testing.T, g *Group) string {
	t.Helper()
	client := testutil.NewRedis(t)
	_, err := g.Enqueue(context.Background(), NewClient(client))
	if err == nil {
		t.Fatal("want validation error")
	}
	return err.Error()
}

func TestAddChain_Validation(t *testing.T) {
	// nil 체인 거부.
	if msg := groupEnqErr(t, NewGroup().AddChain(nil).OnComplete(mcVerify{})); !strings.Contains(msg, "nil") {
		t.Errorf("nil chain: %s", msg)
	}
	// 빈 체인 거부.
	if msg := groupEnqErr(t, NewGroup().AddChain(NewChain()).OnComplete(mcVerify{})); !strings.Contains(msg, "empty") {
		t.Errorf("empty chain: %s", msg)
	}
	// ThenGroup 포함 멤버 체인 거부(1레벨 경계).
	nested := NewChain().Then(mcDump{}).ThenGroup(NewGroup().Add(mcInner{}).OnComplete(mcVerify{}))
	if msg := groupEnqErr(t, NewGroup().AddChain(nested).OnComplete(mcVerify{})); !strings.Contains(msg, "parallel stage") {
		t.Errorf("nested ThenGroup: %s", msg)
	}
	// 멤버 체인 링크의 WithUnique 거부.
	if msg := groupEnqErr(t, NewGroup().AddChain(
		NewChain().Then(mcDump{}, WithUnique(time.Minute))).OnComplete(mcVerify{})); !strings.Contains(msg, "WithUnique") {
		t.Errorf("unique: %s", msg)
	}
	// 멤버 체인 어느 링크든 discard 거부.
	if msg := groupEnqErr(t, NewGroup().AddChain(
		NewChain().Then(mcDump{}).Then(mcLoad{}, WithDeadLetterDiscard())).OnComplete(mcVerify{})); !strings.Contains(msg, "discard") {
		t.Errorf("discard: %s", msg)
	}
}

// 그룹이 ThenGroup 스테이지로 쓰일 때는 체인 멤버 미지원(범위 밖) — 명확히 거부.
func TestThenGroupStage_RejectsChainMembers(t *testing.T) {
	client := testutil.NewRedis(t)
	ch := NewChain().Then(mcDump{}).ThenGroup(
		NewGroup().AddChain(NewChain().Then(mcXform{})).OnComplete(mcVerify{}))
	_, err := ch.Enqueue(context.Background(), NewClient(client))
	if err == nil || !strings.Contains(err.Error(), "chain member") {
		t.Fatalf("want chain-member rejection, got %v", err)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run 'TestAddChain|TestThenGroupStage_Rejects' .`
Expected: FAIL (`(*Group).AddChain undefined`)

- [ ] **Step 3: 구현**

`group.go` — members 구조 교체 + Add/AddChain:

```go
// groupMember is one member: a single task (chain nil) or a chain (chain set).
type groupMember struct {
	args  TaskArgs
	opts  []Option
	chain *Chain
}

// (Group 구조체의 members 필드를 교체)
//   members []groupMember

func (g *Group) Add(args TaskArgs, opts ...Option) *Group {
	g.members = append(g.members, groupMember{args: args, opts: opts})
	return g
}

// AddChain appends a chain member: its links run in sequence, and the chain's
// FINAL link reports the member's completion to this group (its result becomes
// this member's GroupResults entry). The chain may not contain a ThenGroup
// stage (one-level nesting only) and its links may not use WithUnique/
// WithTaskID or WithDeadLetterDiscard.
func (g *Group) AddChain(ch *Chain) *Group {
	g.members = append(g.members, groupMember{chain: ch})
	return g
}
```

`chain.go` — helpers 추가:

```go
// hasGroupStage reports whether any stage is a parallel (ThenGroup) stage.
func (ch *Chain) hasGroupStage() bool {
	for _, st := range ch.stages {
		if st.isGroup {
			return true
		}
	}
	return false
}

// snapshotForMember builds the first-link message of a chain used as a group
// member — same shape as Enqueue produces, but returned (not dispatched) so the
// caller can attach group-reporting fields. chainID is the member slot ID; the
// first link's own ID becomes "<chainID>:0". Rejects a ThenGroup stage anywhere
// and any discard link (a discarded member link would strand the group).
func (ch *Chain) snapshotForMember(chainID string) (*base.TaskMessage, enqueueOptions, error) {
	if ch == nil {
		return nil, enqueueOptions{}, errors.New("chronos: nil chain member")
	}
	if len(ch.stages) == 0 {
		return nil, enqueueOptions{}, errors.New("chronos: empty chain member")
	}
	if ch.hasGroupStage() {
		return nil, enqueueOptions{}, errors.New("chronos: a group member chain cannot contain a parallel stage (ThenGroup) — recursive nesting beyond one level is not supported")
	}
	tail, err := ch.snapshotTail()
	if err != nil {
		return nil, enqueueOptions{}, err
	}
	first := ch.stages[0]
	options, err := resolveChainOptions(first.opts)
	if err != nil {
		return nil, enqueueOptions{}, fmt.Errorf("chain member link 0: %w", err)
	}
	if options.noArchive {
		return nil, enqueueOptions{}, errors.New("chronos: a group member chain link cannot discard (WithDeadLetterDiscard) — it would strand the group")
	}
	for _, l := range tail {
		if l.NoArchive {
			return nil, enqueueOptions{}, errors.New("chronos: a group member chain link cannot discard (WithDeadLetterDiscard) — it would strand the group")
		}
	}
	payload, err := encodeArgs(first.args)
	if err != nil {
		return nil, enqueueOptions{}, fmt.Errorf("chain member link 0: %w", err)
	}
	msg := &base.TaskMessage{
		ID:         chainID + ":0",
		Kind:       first.args.Kind(),
		Payload:    payload,
		Queue:      options.queue,
		MaxRetry:   options.maxRetry,
		Retention:  int64(options.retention / time.Second),
		Chain:      tail,
		ChainID:    chainID,
		ChainIndex: 0,
	}
	return msg, options, nil
}
```

`chain.go` — `snapshotGroupStage`의 `g.members` 순회를 `groupMember`에 맞춰 적응하고 **체인 멤버를 거부**(ThenGroup 스테이지 그룹은 단일 태스크 멤버만 — 범위 밖). 기존 순회부(멤버별 `m.args`/`m.opts` 사용)를 다음으로:

```go
	for j, m := range g.members {
		if m.chain != nil {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: a group used as a chain stage cannot have chain members yet", i, j)
		}
		o, err := resolveChainOptions(m.opts)
		// ... (기존 GroupMemberLink 구성 그대로) ...
	}
```

`group.go` — `Enqueue`의 멤버 검증·스냅샷 루프를 체인 멤버 분기 포함으로 교체. 기존 단일 태스크 경로는 유지하고, 체인 멤버 경로 추가:

```go
	pending := make([]pendingMember, 0, len(g.members))
	for i, m := range g.members {
		memberSlot := memberIDs[i] // "<groupID>:m<i>"
		if m.chain != nil {
			msg, options, err := m.chain.snapshotForMember(memberSlot)
			if err != nil {
				return nil, fmt.Errorf("group member %d: %w", i, err)
			}
			// 그룹 보고 필드: 마지막 링크까지 enqueueNext가 전파, 마지막 링크가 보고.
			msg.GroupID = groupID
			msg.GroupQueue = cbLink.Queue
			msg.GroupCallback = &cbLink
			msg.GroupIndex = i
			msg.GroupSize = len(g.members)
			msg.GroupMemberID = memberSlot
			pending = append(pending, pendingMember{msg: msg, options: options})
			continue
		}
		// --- 기존 단일 태스크 경로 (m.args/m.opts) ---
		options, err := resolveChainOptions(m.opts)
		if err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
		if options.noArchive {
			return nil, fmt.Errorf("group member %d: WithDeadLetterDiscard would strand the group (no dead-letter left to re-run)", i)
		}
		if !options.processAt.IsZero() && time.Until(options.processAt) > rdb.GroupTTL {
			return nil, fmt.Errorf("group member %d: WithProcessIn/WithProcessAt exceeds the group TTL (%v)", i, rdb.GroupTTL)
		}
		payload, err := encodeArgs(m.args)
		if err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
		pending = append(pending, pendingMember{
			msg: &base.TaskMessage{
				ID:            memberSlot,
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
			options: options,
		})
	}
```

(`GroupInfo.MemberIDs`는 슬롯 ID `<groupID>:m<i>` 그대로 — 체인 멤버도 슬롯 단위로 노출.)

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run 'TestAddChain|TestThenGroupStage|TestGroup|TestChain|TestThenGroup' . && go vet ./...`
Expected: 신규 검증 테스트 PASS + 기존 Group/Chain/ThenGroup 회귀 없음

- [ ] **Step 5: 커밋**

```bash
git add group.go chain.go group_member_chain_builder_test.go
git commit -m "feat: Group.AddChain 빌더 — 체인 멤버 스냅샷·검증(ThenGroup·discard·unique 거부)"
```

---

### Task 4: 서버 — 보고 게이트 + enqueueNext 그룹필드 전파 + e2e

**Files:**
- Modify: `server.go` (process 성공 경로 게이트, enqueueNext 전파)
- Test: `group_member_chain_test.go` (신규, 통합)

- [ ] **Step 1: 실패하는 테스트 작성** — `group_member_chain_test.go` 신규 (Task 3의 mcDump/mcXform/mcLoad/mcVerify 재사용):

```go
package chronos

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type mcOut struct{ V string `json:"v"` }

// 그룹-of-체인: 각 멤버가 dump→xform→load 3링크 체인, 마지막(load)만 부모에
// 보고. 콜백이 멤버별 최종 결과를 Add 순서로 수신.
func TestGroupMemberChain_FanOutOfChains(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var loadRuns, dumpRuns atomic.Int64
	var cbResults atomic.Pointer[[]string]

	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcDump]) (mcOut, error) {
		dumpRuns.Add(1)
		return mcOut{V: "d:" + task.Args.T}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcXform]) (mcOut, error) {
		prev, _ := PrevResult[mcOut](task) // 체인 내부 릴레이 확인
		return mcOut{V: "x(" + prev.V + ")"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcLoad]) (mcOut, error) {
		loadRuns.Add(1)
		prev, _ := PrevResult[mcOut](task)
		return mcOut{V: "l(" + prev.V + ")"}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[mcVerify]) error {
		rs, err := GroupResults[mcOut](task)
		if err != nil {
			return err
		}
		vs := make([]string, len(rs))
		for i, r := range rs {
			vs[i] = r.V
		}
		cbResults.Store(&vs)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"mc": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	g := NewGroup()
	for _, tenant := range []string{"a", "b"} {
		g.AddChain(NewChain().
			Then(mcDump{T: tenant}, WithQueue("mc")).
			Then(mcXform{T: tenant}, WithQueue("mc")).
			Then(mcLoad{T: tenant}, WithQueue("mc")))
	}
	if _, err := g.OnComplete(mcVerify{}, WithQueue("mc")).Enqueue(ctx, NewClient(client)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for cbResults.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	rs := cbResults.Load()
	if rs == nil {
		t.Fatal("callback never ran (member chains did not report)")
	}
	// Add 순서: 멤버0(a)·멤버1(b), 각자 full chain 결과.
	if len(*rs) != 2 || (*rs)[0] != "l(x(d:a))" || (*rs)[1] != "l(x(d:b))" {
		t.Fatalf("group results = %v", *rs)
	}
	// 각 체인의 dump/load는 정확히 1회(중간 링크가 조기 보고하지 않음).
	if dumpRuns.Load() != 2 || loadRuns.Load() != 2 {
		t.Errorf("runs: dump=%d load=%d (want 2/2)", dumpRuns.Load(), loadRuns.Load())
	}
}

// 멤버 체인 중간 링크 dead-letter → RunTask 재개 → 그룹 완주.
func TestGroupMemberChain_MidLinkDeadLetterResumes(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var fail atomic.Bool
	fail.Store(true)
	var done atomic.Bool

	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcDump]) (mcOut, error) {
		return mcOut{V: "d"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcXform]) (mcOut, error) {
		if fail.Load() {
			return mcOut{}, SkipRetry(errors.New("xform down"))
		}
		return mcOut{V: "x"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcLoad]) (mcOut, error) {
		return mcOut{V: "l"}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[mcVerify]) error {
		done.Store(true)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"mc": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	_, err := NewGroup().
		AddChain(NewChain().Then(mcDump{}, WithQueue("mc")).Then(mcXform{}, WithQueue("mc")).Then(mcLoad{}, WithQueue("mc"))).
		OnComplete(mcVerify{}, WithQueue("mc")).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}

	// xform 링크(멤버 슬롯 :m0의 링크 1)가 dead-letter될 때까지 대기 후 재개.
	insp := NewInspector(client)
	deadline := time.Now().Add(15 * time.Second)
	var xformID string
	for time.Now().Before(deadline) && xformID == "" {
		tasks, _ := insp.ListTasks(ctx, "mc", "archived", 10)
		for _, ti := range tasks {
			if ti.Kind == "mc:xform" {
				xformID = ti.ID
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if xformID == "" {
		t.Fatal("xform never dead-lettered")
	}
	fail.Store(false)
	if err := insp.RunTask(ctx, "mc", xformID); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for !done.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !done.Load() {
		t.Fatal("group did not resume to callback after RunTask")
	}
}

var _ = sync.Once{} // (import 유지용 — 실제 미사용 시 제거)
```

주의: 마지막 `var _ = sync.Once{}` 줄과 `sync` import는 실제로 안 쓰면 제거하세요.

- [ ] **Step 2: 실패 확인**

Run: `redis-cli ping && go test -p 1 -count=1 -run TestGroupMemberChain_FanOutOfChains .`
Expected: FAIL — 중간 링크(dump)가 GroupID를 갖고 후속 enqueue 후 조기 보고 → 콜백이 잘못된/이른 결과로 발화하거나, GroupID가 전파 안 돼 마지막 링크가 보고 못 함(콜백 미실행). (구현 전이라 어느 쪽이든 실패)

- [ ] **Step 3: 구현** — `server.go`:

(a) process 성공 경로의 그룹 보고 게이트를 "후속 없음"으로:

```go
	if err == nil {
		hadSuccessor := len(msg.Chain) > 0
		if hadSuccessor {
			if cerr := s.enqueueNextWithRetry(opCtx, msg); cerr != nil {
				s.logger.Error("chronos: chain successor enqueue failed; leaving task for redelivery",
					"id", msg.ID, "error", cerr)
				return
			}
			msg.Chain = nil
		}
		// 그룹 보고는 후속이 없는(= 마지막) 링크에서만. 체인 멤버의 중간 링크는
		// 후속만 만들고 보고하지 않는다(조기 SREM 방지); flat 멤버·단일 링크 멤버·
		// 멤버 체인의 마지막 링크만 보고한다.
		if msg.GroupID != "" && !hadSuccessor {
			if gerr := s.completeGroupWithRetry(opCtx, msg); gerr != nil {
				s.logger.Error("chronos: group completion report failed; leaving task for redelivery",
					"id", msg.ID, "group", msg.GroupID, "error", gerr)
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

(현재 코드의 `if len(msg.Chain) > 0 {...}` + `if msg.GroupID != "" {...}` 두 블록을 위 형태로 교체. 기존 chain-only·group-only 동작은 불변: chain-only는 GroupID="", group-only(flat)는 Chain 비어 hadSuccessor=false.)

(b) `enqueueNext`가 후속에 그룹 보고 필드를 전파 (next 메시지 구성에 추가):

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
		PrevResult: msg.Result,
		// 멤버 체인이면 그룹 보고 필드를 마지막 링크까지 실어나른다(빈 값이면 무해).
		GroupID:       msg.GroupID,
		GroupQueue:    msg.GroupQueue,
		GroupCallback: msg.GroupCallback,
		GroupIndex:    msg.GroupIndex,
		GroupSize:     msg.GroupSize,
		GroupMemberID: msg.GroupMemberID,
	}
```

주의: `enqueueNext`는 그룹 스테이지(`len(link.Group)>0`)면 맨 앞에서 `enqueueNextGroup`으로 분기한다 — 그 분기는 그대로 두고, 단일 태스크 후속 경로의 next 구성에만 위 필드를 추가한다. 멤버 체인은 ThenGroup을 못 가지므로 항상 이 단일 경로만 탄다.

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run 'TestGroupMemberChain|TestGroup|TestChain|TestThenGroup' . && go vet ./...`
Expected: 신규 e2e 2건 + 기존 전부 PASS

- [ ] **Step 5: 커밋**

```bash
git add server.go group_member_chain_test.go
git commit -m "feat: 그룹 보고 게이트(마지막 링크만) + enqueueNext 그룹필드 전파 — 그룹-of-체인 실행"
```

---

### Task 5: cluster 스모크 20 + tour 16 + 소크 + README + 최종 검증

**Files:**
- Modify: `cluster_test.go`, `examples/tour/main.go`, `benchmarks/soak/workload.go`, `README.md`

- [ ] **Step 1: cluster 스모크 20** — `cluster_test.go` 체크리스트 주석에 추가:

```go
// [x] 그룹 멤버 체인 (마지막 링크만 부모 보고, 링크가 다른 슬롯) → TestCluster_GroupMemberChain
```

파일 끝에 (기존 관례 `testutil.NewClusterRedis(t)`):

```go
// 그룹 멤버 체인: 멤버 체인의 링크들이 서로 다른 큐(다른 슬롯)에 있어도 마지막
// 링크의 부모 보고가 CROSSSLOT 없이 동작하는지.
func TestCluster_GroupMemberChain(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	ctx := context.Background()

	var cbGot atomic.Int64
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[clMcA]) (clMcOut, error) {
		return clMcOut{N: task.Args.N}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[clMcB]) (clMcOut, error) {
		prev, _ := PrevResult[clMcOut](task)
		return clMcOut{N: prev.N + 1}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[clMcCb]) error {
		if rs, err := GroupResults[clMcOut](task); err == nil {
			var sum int
			for _, r := range rs {
				sum += r.N
			}
			cbGot.Store(int64(sum))
		}
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"clmc-a": 1, "clmc-b": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	// 멤버 2개, 각 체인은 A(clmc-a)→B(clmc-b) — 링크가 다른 슬롯.
	g := NewGroup()
	for _, n := range []int{10, 20} {
		g.AddChain(NewChain().
			Then(clMcA{N: n}, WithQueue("clmc-a")).
			Then(clMcB{}, WithQueue("clmc-b")))
	}
	if _, err := g.OnComplete(clMcCb{}, WithQueue("clmc-b")).Enqueue(ctx, NewClient(client)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for cbGot.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if cbGot.Load() != 32 { // (10+1) + (20+1)
		t.Errorf("callback sum = %d, want 32", cbGot.Load())
	}
}

type clMcA struct{ N int `json:"n"` }

func (clMcA) Kind() string { return "clmc:a" }

type clMcB struct{}

func (clMcB) Kind() string { return "clmc:b" }

type clMcOut struct{ N int `json:"n"` }

type clMcCb struct{}

func (clMcCb) Kind() string { return "clmc:cb" }
```

(`sync/atomic`·`time` import 확인.)

- [ ] **Step 2: cluster 검증** (docker 필요 — 없으면 `deploy/redis-cluster`에서 `docker compose up -d` 후 cluster_state:ok 대기):

Run: `make test-cluster`
Expected: 20/20 PASS

- [ ] **Step 3: tour 섹션 16** — `examples/tour/main.go`의 섹션 15 종료 지점(`cancelRes()` 뒤, 마지막 구분선 전)에 추가하고 상단 doc comment에 "group member chains (fan-out of pipelines)" 추가:

```go
	section("16) 그룹 멤버 체인: 테넌트별 '덤프→변환→적재' 파이프라인을 병렬로, 전부 끝나면 검증")
	migMux := chronos.NewMux()
	chronos.AddHandlerR(migMux, func(ctx context.Context, t *chronos.Task[MigDump]) (MigOut, error) {
		fmt.Printf("   ▶ [dump] %s\n", t.Args.Tenant)
		return MigOut{Rows: len(t.Args.Tenant) * 10}, nil
	})
	chronos.AddHandlerR(migMux, func(ctx context.Context, t *chronos.Task[MigLoad]) (MigOut, error) {
		prev, _ := chronos.PrevResult[MigOut](t)
		fmt.Printf("   ▶ [load] %s (%d rows)\n", t.Args.Tenant, prev.Rows)
		return MigOut{Rows: prev.Rows}, nil
	})
	chronos.AddHandler(migMux, func(ctx context.Context, t *chronos.Task[MigVerify]) error {
		rs, _ := chronos.GroupResults[MigOut](t)
		total := 0
		for _, r := range rs {
			total += r.Rows
		}
		fmt.Printf("   ▶ [verify] 테넌트 %d개 마이그레이션 완료, 총 %d rows\n", len(rs), total)
		return nil
	})
	migSrv := chronos.NewServer(rdb, chronos.ServerConfig{Queues: map[string]int{"mig": 1}, Concurrency: 4})
	if err := migSrv.Start(ctx, migMux); err != nil {
		fmt.Printf("mig 서버 start 실패: %v\n", err)
	}
	migG := chronos.NewGroup()
	for _, tenant := range []string{"acme", "globex", "initech"} {
		migG.AddChain(chronos.NewChain().
			Then(MigDump{Tenant: tenant}, chronos.WithQueue("mig")).
			Then(MigLoad{Tenant: tenant}, chronos.WithQueue("mig")))
	}
	_, _ = migG.OnComplete(MigVerify{}, chronos.WithQueue("mig")).Enqueue(ctx, client)
	time.Sleep(3 * time.Second)
	shutMig, cancelMig := context.WithTimeout(context.Background(), 3*time.Second)
	_ = migSrv.Shutdown(shutMig)
	cancelMig()
```

타입 추가 (다른 tour args 옆):

```go
type MigDump struct {
	Tenant string `json:"tenant"`
}

func (MigDump) Kind() string { return "tour:mig-dump" }

type MigLoad struct {
	Tenant string `json:"tenant"`
}

func (MigLoad) Kind() string { return "tour:mig-load" }

type MigOut struct {
	Rows int `json:"rows"`
}

type MigVerify struct{}

func (MigVerify) Kind() string { return "tour:mig-verify" }
```

Run: `gofmt -w examples/tour/main.go && go vet ./examples/tour/ && go run ./examples/tour 2>&1 | sed -n '/=== 16)/,$p' | head -10`
Expected: dump ×3 → load ×3 → `[verify] 테넌트 3개 마이그레이션 완료`

- [ ] **Step 4: 소크 경로 추가** — `benchmarks/soak/workload.go`의 10초 티커에, 기존 독립 group 티커 옆에 그룹-of-체인 1건 추가(chainArgs/groupArgs/cbArgs 기존 타입 재사용):

```go
			// 그룹 멤버 체인: 멤버 2개가 각각 2링크 체인.
			gmc := chronos.NewGroup()
			for k := 0; k < 2; k++ {
				gmc.AddChain(chronos.NewChain().
					Then(chainArgs{Seq: batch, Link: 0}, chronos.WithQueue("soak-a")).
					Then(chainArgs{Seq: batch, Link: 1}, chronos.WithQueue("soak-b")))
			}
			if _, err := gmc.OnComplete(cbArgs{Seq: batch}, chronos.WithQueue("soak-a")).Enqueue(ctx, w.client); err != nil && ctx.Err() == nil {
				log.Printf("soak: group-member-chain enqueue: %v", err)
			}
```

검증: `cd benchmarks && go build ./... && go vet ./... && go test ./soak/ -p 1 -count=1`.

- [ ] **Step 5: README + 최종 검증 + 커밋** — README의 "Parallel stages (fan-out → fan-in)" 절 뒤에 소절 추가:

```markdown
### Chains as group members

A group member can be a chain (`AddChain`): each member runs its links in
sequence, and the chain's final link reports the member's completion to the
group (its last result becomes that member's `GroupResults` entry). This
expresses fan-out-of-pipelines — e.g. migrate N tenants, each a
dump→transform→load chain, in parallel, then a verify callback:

```go
g := chronos.NewGroup()
for _, t := range tenants {
	g.AddChain(chronos.NewChain().Then(Dump{t}).Then(Transform{t}).Then(Load{t}))
}
g.OnComplete(Verify{}).Enqueue(ctx, client)
```

A dead-lettered member link stalls that member until you re-run it
(`RunTask`); the chain then resumes to its final link and reports. Nesting is
one level deep: a member chain may not contain a `ThenGroup` stage, and a
group used as a `ThenGroup` stage may not have chain members.
```

Known limitations의 재귀 중첩 항목을 "1레벨(그룹 멤버 체인) 지원; 2레벨(멤버 체인 안 병렬 스테이지, 그룹 안 그룹) 미지원"으로 갱신.

Run: `make check`
Expected: 전체 그린

```bash
git add cluster_test.go examples/tour/main.go benchmarks/soak/workload.go README.md
git commit -m "docs+test: cluster 스모크 20(그룹 멤버 체인) + tour 16 마이그레이션 + 소크 경로 + README"
```

---

## 완료 후

1. k:code-reviewer 최종 브랜치 리뷰 (집중: 보고 게이트 변경이 기존 chain-only/ThenGroup 경로에 무영향인지, enqueueNext 그룹필드 전파가 ThenGroup 콜백-꼬리 상속(GroupCallbackChain)과 충돌 없는지, 멤버 체인 첫 링크 create-if-absent 재전달 멱등, GroupMemberID 폴백, 멤버 체인 링크가 다른 큐일 때 cluster 보고, GroupResults 순서가 멤버 슬롯 인덱스와 일치하는지, 소크 sampler가 새 경로 키를 집계하는지).
2. 지적 반영 → `k:commit-pr`로 PR 생성 (base main, assignee kenshin579, HEREDOC):
   - 제목: `feat: 그룹 멤버 체인 — 파이프라인 팬아웃(AddChain)`
3. CI 그린 → 사용자 머지 승인 요청 → 이후 v0.12.0 태그 논의.
