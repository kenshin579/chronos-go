# ThenGroup (workflow 중첩, PR 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Chain.ThenGroup(group)`으로 체인 중간/끝에 병렬 스테이지를 넣는다 — 팬아웃→팬인→후속이 한 빌더 호출 사슬로 표현된다.

**Architecture:** 워크플로 = 스테이지 시퀀스(단일 태스크 | 그룹). 그룹 스테이지는 `ChainLink`의 변형(멤버 목록 `Group []GroupMemberLink` + 기존 단일 필드는 콜백 서술)으로 꼬리에 내장된다. 선행 스테이지 성공 시 서버가 단일 후속 대신 그룹을 create-if-absent로 enqueue하고(pending SET은 콜백 hash 존재를 펜스로), 앞 스테이지 결과(PrevResult)를 전 멤버에 복제한다. **콜백이 체인 꼬리를 상속**하므로 콜백 완료 시 기존 체인 메커니즘이 다음 스테이지로 진행한다. 실패·재개(RunTask)·결정적 ID는 기존 의미론 그대로.

**Tech Stack:** Go 1.26, Redis Lua(신규 1개 + groupCompleteCmd의 꼬리 상속), 기존 chronos 내부.

**스펙:** `docs/superpowers/specs/2026-07-13-workflow-results-design.md` (PR 2 절). **스펙과의 의도적 편차 2건**(구현 시 근거 주석): (1) 콜백 태스크 ID는 스펙의 `<chainID>:<i>` 대신 기존 그룹 관례 `<groupID>:cb` = `<chainID>:<i>:cb` — 결정성 동일, CompleteGroupMember의 ID 규칙 무변경. (2) 그룹이 **첫** 스테이지인 체인은 금지 — `Chain.Enqueue`의 `*TaskInfo` 반환 의미가 깨짐; 팬아웃만 필요하면 `NewGroup` 직접 사용(에러 메시지에 안내).

**전제:**
- 브랜치 `feat/then-group`. 로컬 Redis(127.0.0.1:6379) 필요, 테스트 `-p 1`.
- **메인 작업 트리에서 git checkout/switch 금지** (파일 수정·커밋만).
- 커밋 author는 `-c user.email=kenshin579@hotmail.com -c user.name=kenshin579`로 커밋별 지정, 메시지 끝에 빈 줄 후 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`, HEREDOC.
- 루트 모듈 테스트: `go test -p 1 -count=1 -run '<패턴>' .`

**확인된 기존 코드 (그대로 활용):**
- `chain.go`: `Chain{links []struct{args TaskArgs; opts []Option}}`, `Then`, `Enqueue`(chainID=uuid, 첫 링크 dispatch + 꼬리 snapshot), `resolveChainOptions`(TaskID/Unique 거부), `snapshotChainLink(args, opts, isLast)`(ProcessAt 거부·mid-chain noArchive 거부·상대 delay 캡처 → `base.ChainLink{Kind,Payload,Queue,MaxRetry,NoArchive,Retention,Delay}`), `errNoArchiveMidChain`
- `group.go`: `Group{members []struct{args;opts}; callback TaskArgs; callbackOpts []Option; hasCallback bool}`, `Add/OnComplete`, `Enqueue`(멤버 사전 검증: resolveChainOptions + noArchive(discard) 거부 + GroupTTL 초과 지연 거부; `CreateGroup` 후 멤버 dispatch; 멤버 msg에 GroupID/GroupQueue/GroupCallback/GroupIndex/GroupSize)
- `server.go:479` 성공 경로: `len(msg.Chain)>0` → `enqueueNextWithRetry`(3회 백오프) → `msg.Chain=nil` → group 보고 → Done. `server.go:575 enqueueNext`: `msg.Chain[0]`로 next 단일 태스크 구성(ID `fmt.Sprintf("%s:%d", msg.ChainID, msg.ChainIndex+1)`, `PrevResult: msg.Result`) → `rdb.EnqueueChainLink`
- `internal/rdb/chain.go`: `EnqueueChainLink(ctx, msg, delay) (bool, error)` — registerQueue 포함, task hash 존재 시 no-op(false)
- `internal/rdb/group.go`: `CreateGroup`(SAdd+EXPIRE — 무조건), `CompleteGroupMember(ctx, member)`(GroupCallback 스냅샷으로 cb 구성, ID `member.GroupID+":cb"`, groupCompleteCmd Lua — 결과 HASH 수집·cjson 내장·lockstep TTL), `GroupTTL`
- `internal/base/task.go`: `ChainLink{Kind,Payload,Queue,MaxRetry,NoArchive,Retention,Delay}`, `TaskMessage`(Chain/ChainID/ChainIndex/GroupID/GroupQueue/GroupCallback/GroupIndex/GroupSize/Result/PrevResult/GroupResults)
- `chronos.go`: `TaskInfo.ChainNext []string`(대기 중 링크 kind 목록), `inspector.go: taskInfoFromMsg`
- `benchmarks/soak/sampler.go`: SCAN 패턴 `chronos:*:unique:*` / `chronos:*:group:*`(패밀리당 1회). `benchmarks/soak/workload.go`: 10초 티커에서 chain 3링크·group 3멤버 enqueue
- `cluster_test.go`: 체크리스트 주석 + 18개 시나리오(마지막 `TestCluster_ResultRelay`), `examples/tour/main.go` 섹션 15(OCR→번역 2링크 체인)

---

### Task 1: base — 그룹 스테이지 직렬화

**Files:**
- Modify: `internal/base/task.go`
- Test: `internal/base/task_test.go` (추가)

- [ ] **Step 1: 실패하는 테스트 작성** — `internal/base/task_test.go`에 추가:

```go
func TestGroupStageLinkRoundTrip(t *testing.T) {
	in := &TaskMessage{
		ID: "c1:0", Kind: "k", Queue: "q", ChainID: "c1",
		Chain: []ChainLink{
			{ // 그룹 스테이지: 단일 필드는 콜백을 서술, Group은 멤버 목록
				Kind: "wf:merge", Payload: []byte(`{}`), Queue: "cbq", MaxRetry: 2,
				Group: []GroupMemberLink{
					{Kind: "wf:enc", Payload: []byte(`{"r":"720p"}`), Queue: "enc", MaxRetry: 1},
					{Kind: "wf:enc", Payload: []byte(`{"r":"4k"}`), Queue: "enc", Delay: 3},
				},
			},
			{Kind: "wf:deploy", Payload: []byte(`{}`), Queue: "q"}, // 단일 스테이지
		},
		GroupCallbackChain: []ChainLink{{Kind: "wf:deploy", Payload: []byte(`{}`), Queue: "q"}},
	}
	b, err := EncodeMessage(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeMessage(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	g := out.Chain[0].Group
	if len(g) != 2 || g[0].Kind != "wf:enc" || string(g[1].Payload) != `{"r":"4k"}` || g[1].Delay != 3 {
		t.Errorf("group members lost: %+v", g)
	}
	if out.Chain[0].Kind != "wf:merge" || len(out.Chain[1].Group) != 0 {
		t.Errorf("stage shape wrong: %+v", out.Chain)
	}
	if len(out.GroupCallbackChain) != 1 || out.GroupCallbackChain[0].Kind != "wf:deploy" {
		t.Errorf("callback chain lost: %+v", out.GroupCallbackChain)
	}
	// 단일 스테이지 링크·빈 메시지는 신규 필드를 생략(하위호환).
	single, _ := EncodeMessage(&TaskMessage{ID: "t", Kind: "k", Queue: "q",
		Chain: []ChainLink{{Kind: "a", Queue: "q"}}})
	for _, field := range []string{`"group"`, `"group_cb_chain"`} {
		if strings.Contains(string(single), field) {
			t.Errorf("single-stage message must omit %s", field)
		}
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestGroupStageLinkRoundTrip ./internal/base/`
Expected: FAIL (`undefined: GroupMemberLink` 컴파일 에러)

- [ ] **Step 3: 최소 구현** — `internal/base/task.go`:

`ChainLink`에 필드 추가(기존 필드 뒤):

```go
	// Group, when non-empty, turns this link into a parallel stage: the
	// members run concurrently and the link's own task fields (Kind, Payload,
	// Queue, ...) describe the stage's fan-in callback instead of a single
	// successor. The callback inherits the chain's remaining tail.
	Group []GroupMemberLink `json:"group,omitempty"`
```

`ChainLink` 정의 아래에 타입 추가:

```go
// GroupMemberLink freezes one parallel-stage member's enqueue parameters
// (the single-task subset of ChainLink; members are never NoArchive — a
// discarded member would strand the group).
type GroupMemberLink struct {
	Kind      string `json:"kind"`
	Payload   []byte `json:"payload"`
	Queue     string `json:"queue"`
	MaxRetry  int    `json:"max_retry"`
	Retention int64  `json:"retention,omitempty"` // seconds
	Delay     int64  `json:"delay,omitempty"`     // seconds before the member runs
}
```

`TaskMessage`에 필드 추가(`GroupSize` 뒤):

```go
	// GroupCallbackChain is the chain tail the group's callback must inherit,
	// carried by every member of a chain-embedded group stage. The completion
	// script copies it onto the callback message (with ChainID/ChainIndex) so
	// the chain continues after the fan-in.
	GroupCallbackChain []ChainLink `json:"group_cb_chain,omitempty"`
```

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run TestGroupStageLinkRoundTrip ./internal/base/ && go build ./...`
Expected: PASS, 빌드 성공

- [ ] **Step 5: 커밋**

```bash
git add internal/base/task.go internal/base/task_test.go
git commit -m "feat: ChainLink 그룹 스테이지 변형 (GroupMemberLink) + GroupCallbackChain"
```

---

### Task 2: rdb — 스테이지 그룹 생성·콜백 꼬리 상속

**Files:**
- Modify: `internal/rdb/group.go`
- Test: `internal/rdb/group_test.go` (추가)

- [ ] **Step 1: 실패하는 테스트 작성** — `internal/rdb/group_test.go`에 추가 (기존 관례: `client := testutil.NewRedis(t)`, `r := NewRDB(client)`):

```go
// CreateGroupIfAbsent의 3상태: 신규 생성(2) / 이미 존재(1) / 콜백 존재 = 스테이지 완료(0).
func TestCreateGroupIfAbsent_ThreeStates(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	members := []string{"c1:1:m0", "c1:1:m1"}
	st, err := r.CreateGroupIfAbsent(ctx, "gq", "c1:1", members, "c1:1:cb")
	if err != nil || st != GroupStageCreated {
		t.Fatalf("first create: st=%v err=%v", st, err)
	}
	// SET이 생겼고 TTL이 걸림.
	if n, _ := client.SCard(ctx, base.GroupKey("gq", "c1:1")).Result(); n != 2 {
		t.Fatalf("scard = %d", n)
	}
	if ttl, _ := client.TTL(ctx, base.GroupKey("gq", "c1:1")).Result(); ttl <= 0 {
		t.Fatal("pending set must have a TTL")
	}
	// 재호출(선행 재전달): 이미 존재.
	st, err = r.CreateGroupIfAbsent(ctx, "gq", "c1:1", members, "c1:1:cb")
	if err != nil || st != GroupStageExists {
		t.Fatalf("second create: st=%v err=%v", st, err)
	}
	// 콜백 hash가 생기면(스테이지 완료) 무엇도 만들지 않음.
	client.Del(ctx, base.GroupKey("gq", "c1:1"))
	client.HSet(ctx, base.TaskKey("gq", "c1:1:cb"), "state", 1)
	st, err = r.CreateGroupIfAbsent(ctx, "gq", "c1:1", members, "c1:1:cb")
	if err != nil || st != GroupStageDone {
		t.Fatalf("after callback exists: st=%v err=%v", st, err)
	}
	if n, _ := client.Exists(ctx, base.GroupKey("gq", "c1:1")).Result(); n != 0 {
		t.Fatal("completed stage must not recreate the pending set")
	}
}

// 멤버가 GroupCallbackChain을 실어 나르면 콜백이 꼬리(ChainID/ChainIndex 포함)를
// 상속하고, 결과 cjson 경로에서도 꼬리가 손상되지 않는다.
func TestCompleteGroupMember_CallbackInheritsChainTail(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	tail := []base.ChainLink{{Kind: "wf:deploy", Payload: []byte(`{"env":"prod"}`), Queue: "gq"}}
	cb := &base.ChainLink{Kind: "wf:merge", Payload: []byte(`{}`), Queue: "gq"}
	mk := func(i int, result []byte) *base.TaskMessage {
		return &base.TaskMessage{
			ID: "c2:1:m" + strconv.Itoa(i), Kind: "wf:enc", Queue: "gq",
			GroupID: "c2:1", GroupQueue: "gq", GroupCallback: cb,
			GroupIndex: i, GroupSize: 2, Result: result,
			ChainID: "c2", ChainIndex: 1, GroupCallbackChain: tail,
		}
	}
	if _, err := r.CreateGroupIfAbsent(ctx, "gq", "c2:1", []string{"c2:1:m0", "c2:1:m1"}, "c2:1:cb"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CompleteGroupMember(ctx, mk(0, []byte(`{"v":0}`))); err != nil {
		t.Fatal(err)
	}
	fired, err := r.CompleteGroupMember(ctx, mk(1, []byte(`{"v":1}`)))
	if err != nil || !fired {
		t.Fatalf("fired=%v err=%v", fired, err)
	}
	raw, err := client.HGet(ctx, base.TaskKey("gq", "c2:1:cb"), "msg").Result()
	if err != nil {
		t.Fatal(err)
	}
	cbMsg, err := base.DecodeMessage([]byte(raw))
	if err != nil {
		t.Fatalf("cjson roundtrip broke the callback: %v", err)
	}
	if cbMsg.ChainID != "c2" || cbMsg.ChainIndex != 1 {
		t.Errorf("chain identity lost: %+v", cbMsg)
	}
	if len(cbMsg.Chain) != 1 || cbMsg.Chain[0].Kind != "wf:deploy" ||
		string(cbMsg.Chain[0].Payload) != `{"env":"prod"}` {
		t.Errorf("tail lost: %+v", cbMsg.Chain)
	}
	if len(cbMsg.GroupResults) != 2 || string(cbMsg.GroupResults[1]) != `{"v":1}` {
		t.Errorf("results lost alongside tail: %v", cbMsg.GroupResults)
	}
	if cbMsg.GroupCallbackChain != nil {
		t.Error("callback itself must not carry GroupCallbackChain")
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run 'TestCreateGroupIfAbsent|TestCompleteGroupMember_CallbackInheritsChainTail' ./internal/rdb/`
Expected: FAIL (`undefined: ...CreateGroupIfAbsent`)

- [ ] **Step 3: 구현** — `internal/rdb/group.go`:

```go
// GroupStageState reports what CreateGroupIfAbsent found.
type GroupStageState int

const (
	// GroupStageDone: the stage's callback task already exists — the group
	// completed; nothing must be (re)created.
	GroupStageDone GroupStageState = iota
	// GroupStageExists: the pending SET already exists (redelivery while the
	// stage is in flight); members may still need create-if-absent enqueues.
	GroupStageExists
	// GroupStageCreated: the pending SET was created by this call.
	GroupStageCreated
)

// createGroupIfAbsentCmd guards a chain group stage against predecessor
// redelivery: an existing callback hash fences a completed stage, an existing
// SET means the stage is in flight. Both keys share the callback queue's hash
// slot. KEYS[1] pending SET, KEYS[2] callback task hash.
// ARGV[1] TTL seconds, ARGV[2..] member IDs.
var createGroupIfAbsentCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
if redis.call("EXISTS", KEYS[1]) == 1 then
  redis.call("EXPIRE", KEYS[1], ARGV[1])
  return 1
end
for i = 2, #ARGV do
  redis.call("SADD", KEYS[1], ARGV[i])
end
redis.call("EXPIRE", KEYS[1], ARGV[1])
return 2
`)

// CreateGroupIfAbsent registers a chain stage's pending members exactly once.
// Unlike CreateGroup (unconditional, for standalone groups with fresh UUIDs),
// chain stages have deterministic IDs and may be re-attempted by a redelivered
// predecessor — completed stages must not be resurrected.
func (r *RDB) CreateGroupIfAbsent(ctx context.Context, cbQueue, groupID string, memberIDs []string, cbTaskID string) (GroupStageState, error) {
	if len(memberIDs) == 0 {
		return GroupStageDone, errors.New("chronos: group needs at least one member")
	}
	keys := []string{base.GroupKey(cbQueue, groupID), base.TaskKey(cbQueue, cbTaskID)}
	argv := make([]interface{}, 0, len(memberIDs)+1)
	argv = append(argv, int(GroupTTL/time.Second))
	for _, id := range memberIDs {
		argv = append(argv, id)
	}
	n, err := createGroupIfAbsentCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return GroupStageDone, err
	}
	return GroupStageState(n), nil
}
```

`CompleteGroupMember`의 cb 구성(cb := &base.TaskMessage{...} 직후)에 꼬리 상속 추가:

```go
	// A chain-embedded stage's callback inherits the chain tail so the chain
	// continues after the fan-in.
	if len(member.GroupCallbackChain) > 0 {
		cb.Chain = member.GroupCallbackChain
		cb.ChainID = member.ChainID
		cb.ChainIndex = member.ChainIndex
	}
```

- [ ] **Step 4: 통과 확인 (기존 group 테스트 포함)**

Run: `go test -p 1 -count=1 -run 'TestCreateGroupIfAbsent|TestCompleteGroupMember|TestGroup' ./internal/rdb/ && go vet ./...`
Expected: 전건 PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/group.go internal/rdb/group_test.go
git commit -m "feat: rdb 스테이지 그룹 create-if-absent(콜백 펜스) + 콜백의 체인 꼬리 상속"
```

---

### Task 3: Chain 빌더 — ThenGroup + 검증

**Files:**
- Modify: `chain.go` (내부 표현을 스테이지로 재구성 + ThenGroup + 스냅샷)
- Test: `chain_group_builder_test.go` (신규, Redis 불필요)

- [ ] **Step 1: 실패하는 테스트 작성** — `chain_group_builder_test.go` 신규:

```go
package chronos

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type wfPrep struct{}

func (wfPrep) Kind() string { return "wf:prep" }

type wfEnc struct {
	Res string `json:"res"`
}

func (wfEnc) Kind() string { return "wf:enc" }

type wfMerge struct{}

func (wfMerge) Kind() string { return "wf:merge" }

type wfDeploy struct{}

func (wfDeploy) Kind() string { return "wf:deploy" }

// 빌더 검증은 Enqueue 시점에 일어난다 — Redis에 닿기 전의 결정적 에러만
// 테스트하고, 유효한 체인의 실제 실행은 Task 4의 통합 테스트가 담당한다.
func enqueueErr(t *testing.T, ch *Chain) string {
	t.Helper()
	client := testutil.NewRedis(t)
	_, err := ch.Enqueue(context.Background(), NewClient(client))
	if err == nil {
		t.Fatal("want validation error")
	}
	return err.Error()
}

func TestThenGroup_Validation(t *testing.T) {
	// 그룹이 첫 스테이지면 거부.
	if msg := enqueueErr(t, NewChain().
		ThenGroup(NewGroup().Add(wfEnc{}).OnComplete(wfMerge{}))); !strings.Contains(msg, "first") {
		t.Errorf("group-first: %s", msg)
	}
	// OnComplete 없는 그룹 거부.
	if msg := enqueueErr(t, NewChain().Then(wfPrep{}).
		ThenGroup(NewGroup().Add(wfEnc{}))); !strings.Contains(msg, "callback") {
		t.Errorf("no callback: %s", msg)
	}
	// 빈 그룹 거부.
	if msg := enqueueErr(t, NewChain().Then(wfPrep{}).
		ThenGroup(NewGroup().OnComplete(wfMerge{}))); !strings.Contains(msg, "member") {
		t.Errorf("empty group: %s", msg)
	}
	// nil 그룹 거부.
	if msg := enqueueErr(t, NewChain().Then(wfPrep{}).ThenGroup(nil)); !strings.Contains(msg, "nil") {
		t.Errorf("nil group: %s", msg)
	}
	// 멤버 discard(WithDeadLetterDiscard) 거부 — 그룹을 좌초시킴.
	if msg := enqueueErr(t, NewChain().Then(wfPrep{}).
		ThenGroup(NewGroup().Add(wfEnc{}, WithDeadLetterDiscard()).OnComplete(wfMerge{}))); !strings.Contains(msg, "strand") {
		t.Errorf("member discard: %s", msg)
	}
	// 멤버 WithUnique 거부(체인/그룹 규칙).
	if msg := enqueueErr(t, NewChain().Then(wfPrep{}).
		ThenGroup(NewGroup().Add(wfEnc{}, WithUnique(time.Minute)).OnComplete(wfMerge{}))); !strings.Contains(msg, "WithUnique") {
		t.Errorf("member unique: %s", msg)
	}
}

func TestThenGroup_SnapshotShape(t *testing.T) {
	// 그룹 스테이지가 마지막이어도 허용되고, 스냅샷이 올바른 ChainLink를 만든다.
	ch := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "720p"}, WithQueue("enc")).
			Add(wfEnc{Res: "4k"}, WithQueue("enc"), WithProcessIn(3*time.Second)).
			OnComplete(wfMerge{}, WithQueue("wf"), WithMaxRetry(2)))
	links, err := ch.snapshotTail()
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 1 {
		t.Fatalf("tail len = %d", len(links))
	}
	g := links[0]
	if g.Kind != "wf:merge" || g.Queue != "wf" || g.MaxRetry != 2 {
		t.Errorf("callback fields: %+v", g)
	}
	if len(g.Group) != 2 || g.Group[0].Kind != "wf:enc" || g.Group[0].Queue != "enc" ||
		g.Group[1].Delay != 3 {
		t.Errorf("members: %+v", g.Group)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestThenGroup .`
Expected: FAIL (`(*Chain).ThenGroup undefined`)

- [ ] **Step 3: 구현** — `chain.go` 재구성:

`Chain` 내부 표현을 스테이지로 변경(기존 `links` 익명 구조체를 이름 있는 타입으로):

```go
// chainStage is one step of a chain: a single task (group == nil) or a
// parallel group stage.
type chainStage struct {
	args  TaskArgs
	opts  []Option
	group *Group // non-nil = parallel stage (args/opts unused)
}

type Chain struct {
	stages []chainStage
}
```

`Then`/`ThenGroup`:

```go
// Then appends a single-task link. (기존 doc comment 유지)
func (ch *Chain) Then(args TaskArgs, opts ...Option) *Chain {
	ch.stages = append(ch.stages, chainStage{args: args, opts: opts})
	return ch
}

// ThenGroup appends a parallel stage: every member of g runs concurrently
// (each receiving the previous stage's result via PrevResult) and g's
// OnComplete callback fans the member results in (GroupResults) before the
// chain continues with the callback's own result. g must not be reused or
// mutated afterwards. A group cannot be the chain's first stage — use
// NewGroup directly when no preceding step exists.
func (ch *Chain) ThenGroup(g *Group) *Chain {
	ch.stages = append(ch.stages, chainStage{group: g})
	return ch
}
```

스냅샷: 기존 `Enqueue`의 꼬리 구성 루프를 `snapshotTail`로 추출하고 그룹 스테이지 처리 추가. 기존 `snapshotChainLink(args, opts, isLast)` 호출부와 첫 링크 처리(`resolveChainOptions`, `errNoArchiveMidChain`, payload 인코딩)는 필드명만 `ch.links[i].args`→`ch.stages[i].args`로 바꿔 유지:

```go
// snapshotTail freezes stages 1..n-1 into ChainLinks (test seam).
func (ch *Chain) snapshotTail() ([]base.ChainLink, error) {
	tail := make([]base.ChainLink, 0, len(ch.stages)-1)
	for i := 1; i < len(ch.stages); i++ {
		link, err := ch.snapshotStage(i)
		if err != nil {
			return nil, err
		}
		tail = append(tail, link)
	}
	return tail, nil
}

// snapshotStage freezes stage i (single task or group) into a ChainLink.
func (ch *Chain) snapshotStage(i int) (base.ChainLink, error) {
	st := ch.stages[i]
	isLast := i == len(ch.stages)-1
	if st.group == nil {
		link, err := snapshotChainLink(st.args, st.opts, isLast)
		if err != nil {
			return base.ChainLink{}, fmt.Errorf("chain link %d: %w", i, err)
		}
		return link, nil
	}
	return snapshotGroupStage(st.group, i, isLast)
}

// snapshotGroupStage freezes a parallel stage: the link's own task fields
// describe the fan-in callback; Group holds the members.
func snapshotGroupStage(g *Group, i int, isLast bool) (base.ChainLink, error) {
	if g == nil {
		return base.ChainLink{}, fmt.Errorf("chain stage %d: nil group", i)
	}
	if len(g.members) == 0 {
		return base.ChainLink{}, fmt.Errorf("chain stage %d: group needs at least one member", i)
	}
	if !g.hasCallback {
		return base.ChainLink{}, fmt.Errorf("chain stage %d: group needs a callback (OnComplete)", i)
	}
	// 콜백: 마지막 스테이지의 콜백만 noArchive 허용(단일 링크 규칙과 동일).
	link, err := snapshotChainLink(g.callback, g.callbackOpts, isLast)
	if err != nil {
		return base.ChainLink{}, fmt.Errorf("chain stage %d callback: %w", i, err)
	}
	link.Group = make([]base.GroupMemberLink, 0, len(g.members))
	for j, m := range g.members {
		o, err := resolveChainOptions(m.opts)
		if err != nil {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: %w", i, j, err)
		}
		if o.noArchive {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: WithDeadLetterDiscard would strand the group (no dead-letter left to re-run)", i, j)
		}
		if o.processAtAbsolute {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: WithProcessAt cannot be used on a group member (its delay is relative; use WithProcessIn)", i, j)
		}
		payload, err := encodeArgs(m.args)
		if err != nil {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: %w", i, j, err)
		}
		var delay int64
		if !o.processAt.IsZero() {
			if d := time.Until(o.processAt); d > 0 {
				delay = int64((d + time.Second/2) / time.Second)
				if delay == 0 {
					delay = 1
				}
			}
		}
		link.Group = append(link.Group, base.GroupMemberLink{
			Kind:      m.args.Kind(),
			Payload:   payload,
			Queue:     o.queue,
			MaxRetry:  o.maxRetry,
			Retention: int64(o.retention / time.Second),
			Delay:     delay,
		})
	}
	return link, nil
}
```

`Enqueue`: 첫 스테이지가 그룹이면 거부(맨 앞에서), 꼬리 구성은 `snapshotTail()` 호출로 교체:

```go
	if len(ch.stages) == 0 {
		return nil, errors.New("chronos: empty chain")
	}
	if ch.stages[0].group != nil {
		return nil, errors.New("chronos: a group cannot be the chain's first stage — start with Then, or use NewGroup directly")
	}
```

(첫 링크의 기존 처리 — resolveChainOptions/noArchive 가드/dispatch — 는 `ch.stages[0].args`/`.opts` 참조로만 변경.)

- [ ] **Step 4: 통과 확인 (기존 chain 테스트 포함)**

Run: `go test -p 1 -count=1 -run 'TestThenGroup|TestChain' . && go vet ./...`
Expected: 전건 PASS (기존 chain 빌더·릴레이 테스트 회귀 없음)

- [ ] **Step 5: 커밋**

```bash
git add chain.go chain_group_builder_test.go
git commit -m "feat: Chain.ThenGroup 빌더 — 스테이지 표현·스냅샷·사전 검증 (그룹 첫 스테이지 금지)"
```

---

### Task 4: 서버 실행 — 그룹 스테이지 enqueue + e2e

**Files:**
- Modify: `server.go` (enqueueNext 분기 + enqueueNextGroup)
- Modify: `group.go` (멤버 메시지 구성 참조용 — 변경 없음 확인)
- Test: `chain_thengroup_test.go` (신규, 통합)

- [ ] **Step 1: 실패하는 테스트 작성** — `chain_thengroup_test.go` 신규 (Task 3의 wfPrep/wfEnc/wfMerge/wfDeploy 타입 재사용):

```go
package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type wfOut struct {
	V string `json:"v"`
}

// 팬아웃→팬인→후속: prep 결과가 전 멤버에 복제되고, 콜백이 멤버 결과를 Add
// 순서로 받고, 콜백 결과가 마지막 스테이지로 릴레이된다.
func TestThenGroup_FanOutFanInContinue(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var memberPrev [2]atomic.Pointer[string] // 각 멤버가 받은 PrevResult.V
	var cbGot atomic.Pointer[[]wfOut]
	var finalGot atomic.Pointer[string]

	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "prepared"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		prev, err := PrevResult[wfOut](task)
		if err == nil {
			idx := 0
			if task.Args.Res == "4k" {
				idx = 1
			}
			v := prev.V
			memberPrev[idx].Store(&v)
		}
		return wfOut{V: task.Args.Res}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfMerge]) (wfOut, error) {
		rs, err := GroupResults[wfOut](task)
		if err != nil {
			return wfOut{}, err
		}
		cbGot.Store(&rs)
		return wfOut{V: rs[0].V + "+" + rs[1].V}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[wfDeploy]) error {
		prev, err := PrevResult[wfOut](task)
		if err != nil {
			return err
		}
		finalGot.Store(&prev.V)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1, "enc": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	info, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "720p"}, WithQueue("enc")).
			Add(wfEnc{Res: "4k"}, WithQueue("enc")).
			OnComplete(wfMerge{}, WithQueue("wf"))).
		Then(wfDeploy{}, WithQueue("wf")).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	if info.ID == "" {
		t.Fatal("first link TaskInfo missing")
	}

	deadline := time.Now().Add(15 * time.Second)
	for finalGot.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if finalGot.Load() == nil {
		t.Fatal("workflow never reached the final stage")
	}
	if got := *finalGot.Load(); got != "720p+4k" {
		t.Fatalf("final PrevResult = %q, want 720p+4k", got)
	}
	for i := 0; i < 2; i++ {
		if p := memberPrev[i].Load(); p == nil || *p != "prepared" {
			t.Errorf("member %d PrevResult = %v, want prepared", i, p)
		}
	}
	if rs := cbGot.Load(); rs == nil || (*rs)[0].V != "720p" || (*rs)[1].V != "4k" {
		t.Errorf("callback results = %v", cbGot.Load())
	}
}

// 그룹 스테이지가 마지막이어도 동작한다(콜백이 마지막 링크).
func TestThenGroup_AsLastStage(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var done atomic.Bool
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "p"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		return wfOut{V: task.Args.Res}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[wfMerge]) error {
		if raw := task.RawGroupResults(); len(raw) == 2 {
			done.Store(true)
		}
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	_, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "a"}, WithQueue("wf")).
			Add(wfEnc{Res: "b"}, WithQueue("wf")).
			OnComplete(wfMerge{}, WithQueue("wf"))).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !done.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !done.Load() {
		t.Fatal("last-stage group callback never ran")
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestThenGroup_FanOut .`
Expected: FAIL — 그룹 스테이지 링크가 단일 태스크로 enqueue돼(`wf:merge` 핸들러가 GroupResults 없이 실행 → ErrNoResult) 최종 스테이지 미도달

- [ ] **Step 3: 구현** — `server.go`의 `enqueueNext` 맨 앞에 분기 추가 + 메서드 신설:

```go
func (s *Server) enqueueNext(ctx context.Context, msg *base.TaskMessage) error {
	link := msg.Chain[0]
	if len(link.Group) > 0 {
		return s.enqueueNextGroup(ctx, msg, link)
	}
	// ... 기존 단일 태스크 경로 그대로 ...
}

// enqueueNextGroup materializes a parallel stage: the pending-member SET
// (guarded by the callback-hash fence against redelivery of a completed
// stage), then every member via the usual create-if-absent link enqueue.
// Members that already left the pending SET (completed while a partially
// failed attempt is being retried) are skipped so their work is not redone;
// the SISMEMBER check and the enqueue are not atomic, so a member finishing
// in between can still be recreated — the standard at-least-once caveat.
func (s *Server) enqueueNextGroup(ctx context.Context, msg *base.TaskMessage, link base.ChainLink) error {
	stageIdx := msg.ChainIndex + 1
	groupID := fmt.Sprintf("%s:%d", msg.ChainID, stageIdx)
	cbTaskID := groupID + ":cb"
	memberIDs := make([]string, len(link.Group))
	for j := range link.Group {
		memberIDs[j] = fmt.Sprintf("%s:m%d", groupID, j)
	}

	state, err := s.rdb.CreateGroupIfAbsent(ctx, link.Queue, groupID, memberIDs, cbTaskID)
	if err != nil {
		return err
	}
	if state == rdb.GroupStageDone {
		return nil // 재전달 — 스테이지는 이미 완료됨
	}
	var pendingSet map[string]bool
	if state == rdb.GroupStageExists {
		// 부분 실패 재시도: 이미 완료돼 SET을 떠난 멤버는 재생성하지 않는다.
		ids, err := s.rdb.GroupMembers(ctx, link.Queue, groupID)
		if err != nil {
			return err
		}
		pendingSet = make(map[string]bool, len(ids))
		for _, id := range ids {
			pendingSet[id] = true
		}
	}

	// 콜백 스냅샷(멤버들이 실어 나름): link의 단일 태스크 필드가 콜백을 서술.
	cbLink := base.ChainLink{
		Kind: link.Kind, Payload: link.Payload, Queue: link.Queue,
		MaxRetry: link.MaxRetry, NoArchive: link.NoArchive,
		Retention: link.Retention, Delay: link.Delay,
	}
	for j, m := range link.Group {
		if pendingSet != nil && !pendingSet[memberIDs[j]] {
			continue
		}
		member := &base.TaskMessage{
			ID:                 memberIDs[j],
			Kind:               m.Kind,
			Payload:            m.Payload,
			Queue:              m.Queue,
			MaxRetry:           m.MaxRetry,
			Retention:          m.Retention,
			GroupID:            groupID,
			GroupQueue:         link.Queue,
			GroupCallback:      &cbLink,
			GroupIndex:         j,
			GroupSize:          len(link.Group),
			GroupCallbackChain: msg.Chain[1:],
			ChainID:            msg.ChainID,
			ChainIndex:         stageIdx,
			PrevResult:         msg.Result, // 앞 스테이지 결과를 전 멤버에 복제
		}
		created, err := s.rdb.EnqueueChainLink(ctx, member, time.Duration(m.Delay)*time.Second)
		if err != nil {
			return fmt.Errorf("stage member %s: %w", member.ID, err)
		}
		if !created {
			// 멤버 hash가 이미 존재. in-flight면 스스로 보고하지만, 잔존
			// completed(멤버 retention > 콜백 retention로 펜스가 먼저 사라진
			// 재생성 경로)면 다시는 보고하지 않아 SET이 영원히 안 빈다 —
			// CreateGroupIfAbsent의 드레인 계약대로 저장된 메시지(원래 결과
			// 포함)로 완료를 재보고한다. 멱등: SREM no-op·create-if-absent.
			if derr := s.drainCompletedStageMember(ctx, member.Queue, member.ID); derr != nil {
				return fmt.Errorf("stage member %s drain: %w", member.ID, derr)
			}
		}
	}
	return nil
}

// drainCompletedStageMember re-reports a lingering completed member so the
// stage's pending SET can empty. Skips members in any other state (they will
// report themselves, or need a manual RunTask).
func (s *Server) drainCompletedStageMember(ctx context.Context, qname, taskID string) error {
	state, err := s.rdb.TaskState(ctx, qname, taskID)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil // 방금 삭제됨 — 재보고 대상 아님(다음 재전달이 처리)
		}
		return err
	}
	if state != base.StateCompleted {
		return nil
	}
	stored, err := s.rdb.GetTask(ctx, qname, taskID)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return err
	}
	_, err = s.rdb.CompleteGroupMember(ctx, stored)
	return err
}
```

(`errors`, `redis "github.com/redis/go-redis/v9"` import가 server.go에 없으면 추가. `rdb.TaskState`는 Task 2 리뷰 반영에서 추가된 헬퍼. `base.StateCompleted` 상수명은 internal/base 실제 정의에 맞출 것.)

`GroupMembers`가 rdb에 없으면 `internal/rdb/group.go`에 추가:

```go
// GroupMembers lists the group's still-pending member IDs.
func (r *RDB) GroupMembers(ctx context.Context, cbQueue, groupID string) ([]string, error) {
	return r.client.SMembers(ctx, base.GroupKey(cbQueue, groupID)).Result()
}
```

주의: 멤버는 `Chain` 필드를 갖지 않으므로(꼬리는 `GroupCallbackChain`에) 멤버 완료가 `enqueueNext`를 타지 않는다 — 콜백만 체인을 잇는다. `server.go`에 `internal/rdb` import가 이미 있는지 확인(`rdb.GroupStageDone` 사용).

- [ ] **Step 4: 통과 확인**

Run: `go test -p 1 -count=1 -run 'TestThenGroup|TestChain|TestGroup' . && go vet ./...`
Expected: 전건 PASS

- [ ] **Step 5: 커밋**

```bash
git add server.go internal/rdb/group.go chain_thengroup_test.go
git commit -m "feat: 그룹 스테이지 실행 — enqueueNextGroup(펜스·결과 복제·꼬리 상속) + 팬아웃→팬인→후속 e2e"
```

---

### Task 5: 실패·재개·멱등 e2e

**Files:**
- Test: `chain_thengroup_test.go` (추가)

- [ ] **Step 1: 실패하는(또는 즉시 통과를 검증하는) 테스트 작성** — 같은 파일에 추가:

```go
type wfFlaky struct{}

func (wfFlaky) Kind() string { return "wf:flaky" }

// 멤버 dead-letter → 그룹 대기 → RunTask 재개 → 콜백·후속 스테이지까지 완주.
func TestThenGroup_MemberDeadLetterResumesChain(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var fail atomic.Bool
	fail.Store(true)
	var finalGot atomic.Pointer[string]

	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "p"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		return wfOut{V: task.Args.Res}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfFlaky]) (wfOut, error) {
		if fail.Load() {
			return wfOut{}, SkipRetry(errNoResultTestSentinel)
		}
		return wfOut{V: "recovered"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfMerge]) (wfOut, error) {
		rs, err := GroupResults[wfOut](task)
		if err != nil {
			return wfOut{}, err
		}
		return wfOut{V: rs[0].V + "+" + rs[1].V}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[wfDeploy]) error {
		prev, err := PrevResult[wfOut](task)
		if err != nil {
			return err
		}
		finalGot.Store(&prev.V)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	_, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "ok"}, WithQueue("wf")).
			Add(wfFlaky{}, WithQueue("wf")).
			OnComplete(wfMerge{}, WithQueue("wf"))).
		Then(wfDeploy{}, WithQueue("wf")).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}

	// flaky 멤버(스테이지 1, 멤버 1)가 dead-letter로 갈 때까지 대기 후 재개.
	insp := NewInspector(client)
	deadline := time.Now().Add(15 * time.Second)
	var flakyID string
	for time.Now().Before(deadline) && flakyID == "" {
		tasks, _ := insp.ListTasks(ctx, "wf", "archived", 10)
		for _, ti := range tasks {
			if ti.Kind == "wf:flaky" {
				flakyID = ti.ID
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if flakyID == "" {
		t.Fatal("flaky member never dead-lettered")
	}
	fail.Store(false)
	if err := insp.RunTask(ctx, "wf", flakyID); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(15 * time.Second)
	for finalGot.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if finalGot.Load() == nil {
		t.Fatal("chain did not resume to the final stage")
	}
	if got := *finalGot.Load(); got != "ok+recovered" {
		t.Fatalf("final = %q, want ok+recovered", got)
	}
}

// 선행 재전달 멱등성: 완료된 그룹 스테이지는 재생성되지 않는다(콜백 hash 펜스).
func TestThenGroup_RedeliveryDoesNotResurrectStage(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var encRuns atomic.Int64
	var cbRuns atomic.Int64
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "p"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		encRuns.Add(1)
		return wfOut{V: "e"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfMerge]) (wfOut, error) {
		cbRuns.Add(1)
		return wfOut{V: "m"}, nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	// 콜백에 retention을 줘 완료 후에도 hash가 남게 → 펜스가 살아 있는 창에서 검증.
	_, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "x"}, WithQueue("wf")).
			OnComplete(wfMerge{}, WithQueue("wf"), WithRetention(time.Minute))).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for cbRuns.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if cbRuns.Load() == 0 {
		t.Fatal("stage never completed")
	}

	// 선행(prep, 스테이지 0) 재전달을 시뮬레이션: 서버 내부 enqueueNext를 직접
	// 재호출하는 대신 rdb.CreateGroupIfAbsent가 Done을 반환하는지 확인한다
	// (server 경로는 이 상태에서 멤버를 만들지 않음 — Task 4의 state 분기).
	st, err := rdbCreateIfAbsentForTest(ctx, client, "wf")
	if err != nil {
		t.Fatal(err)
	}
	if st != 0 { // rdb.GroupStageDone
		t.Fatalf("completed stage must be fenced, state=%d", st)
	}
	time.Sleep(500 * time.Millisecond)
	if encRuns.Load() != 1 || cbRuns.Load() != 1 {
		t.Fatalf("stage re-ran: enc=%d cb=%d", encRuns.Load(), cbRuns.Load())
	}
}
```

파일 상단에 헬퍼·센티넬 추가:

```go
var errNoResultTestSentinel = errors.New("wf: deliberate failure")

// rdbCreateIfAbsentForTest replays the stage-creation call a redelivered
// predecessor would make (chain ID를 몰라도 되도록 archived/completed에서
// 콜백 ID를 찾는 대신, 콜백 retention 덕에 남아 있는 hash를 스캔해 group ID를
// 복원한다).
func rdbCreateIfAbsentForTest(ctx context.Context, client redis.UniversalClient, q string) (int, error) {
	r := rdb.NewRDB(client)
	var cbKey string
	iter := client.Scan(ctx, 0, "chronos:{"+q+"}:t:*:cb", 100).Iterator()
	for iter.Next(ctx) {
		cbKey = iter.Val()
	}
	if cbKey == "" {
		return -1, errors.New("callback hash not found (retention?)")
	}
	// "chronos:{q}:t:<chainID>:<i>:cb" → groupID "<chainID>:<i>", cbID 그대로
	id := strings.TrimPrefix(cbKey, "chronos:{"+q+"}:t:")
	groupID := strings.TrimSuffix(id, ":cb")
	st, err := r.CreateGroupIfAbsent(ctx, q, groupID, []string{groupID + ":m0"}, id)
	return int(st), err
}
```

(import에 `errors`, `strings`, `redis "github.com/redis/go-redis/v9"`, `"github.com/kenshin579/chronos-go/internal/rdb"` 추가. `internal/rdb`는 루트 패키지 테스트에서 import 가능.)

- [ ] **Step 1b: 드레인 시나리오 테스트 추가** — 같은 파일에 추가. Task 4의 `drainCompletedStageMember` 검증: 멤버 retention > 콜백 retention로 펜스가 먼저 사라진 재생성 경로에서 잔존 completed 멤버가 드레인되어 스테이지가 정지하지 않는지. 같은 패키지라 서버 비공개 메서드를 직접 호출한다:

```go
// 펜스 소멸 후 재생성: 잔존 completed 멤버(재투입 no-op)를 드레인하면 SET이
// 비고 콜백이 재점화된다. (멤버 retention 1분, 콜백 무retention)
func TestThenGroup_DrainLingeringCompletedMember(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var cbRuns atomic.Int64
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "p"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		return wfOut{V: "e"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfMerge]) (wfOut, error) {
		cbRuns.Add(1)
		return wfOut{V: "m"}, nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	info, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "x"}, WithQueue("wf"), WithRetention(time.Minute)).
			OnComplete(wfMerge{}, WithQueue("wf"))).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for cbRuns.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if cbRuns.Load() == 0 {
		t.Fatal("stage never completed")
	}
	time.Sleep(500 * time.Millisecond) // 콜백(무retention) hash 삭제 대기

	// 선행 재전달의 스테이지 재생성을 재현: 펜스·SET 모두 없음 → Created.
	chainID := strings.TrimSuffix(info.ID, ":0")
	groupID := chainID + ":1"
	memberID := groupID + ":m0"
	r := rdb.NewRDB(client)
	st, err := r.CreateGroupIfAbsent(ctx, "wf", groupID, []string{memberID}, groupID+":cb")
	if err != nil || st != rdb.GroupStageCreated {
		t.Fatalf("recreate: st=%v err=%v", st, err)
	}
	// 잔존 completed 멤버는 create-if-absent에 걸린다 — 드레인 경로 직접 호출.
	if err := srv.drainCompletedStageMember(ctx, "wf", memberID); err != nil {
		t.Fatalf("drain: %v", err)
	}
	// SET이 비어 삭제되고 콜백 hash가 재생성됐는지.
	if n, _ := client.Exists(ctx, base.GroupKey("wf", groupID)).Result(); n != 0 {
		t.Error("pending set must be drained away")
	}
	if n, _ := client.Exists(ctx, base.TaskKey("wf", groupID+":cb")).Result(); n != 1 {
		t.Error("callback must be re-fired by the drained report")
	}
}
```

(import에 `strings`, `"github.com/kenshin579/chronos-go/internal/base"`, `"github.com/kenshin579/chronos-go/internal/rdb"` 확인 — Task 5의 다른 테스트와 공유. 콜백 2회 실행은 기존 at-least-once caveat로 허용.)

- [ ] **Step 2: 실행·통과 확인**

Run: `go test -p 1 -count=1 -run 'TestThenGroup_Member|TestThenGroup_Redelivery|TestThenGroup_Recreated' .`
Expected: PASS 2건 (Task 4 구현이 이미 있으므로 이 태스크는 시나리오 검증 — 실패하면 구현 결함이니 수정)

- [ ] **Step 3: 커밋**

```bash
git add chain_thengroup_test.go
git commit -m "test: ThenGroup 실패·재개(RunTask)·재전달 펜스 e2e"
```

---

### Task 6: Inspector — 그룹 스테이지 표기

**Files:**
- Modify: `inspector.go` (taskInfoFromMsg의 ChainNext 구성)
- Test: `inspector_test.go` (추가)

- [ ] **Step 1: 실패하는 테스트 작성** — `inspector_test.go`에 추가:

```go
func TestTaskInfo_ChainNextShowsGroupStage(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()
	msg := &base.TaskMessage{ID: "c9:0", Kind: "wf:prep", Queue: "iq2",
		State: base.StatePending, ChainID: "c9",
		Chain: []base.ChainLink{
			{Kind: "wf:merge", Queue: "iq2", Group: []base.GroupMemberLink{
				{Kind: "wf:enc", Queue: "iq2"}, {Kind: "wf:enc", Queue: "iq2"},
			}},
			{Kind: "wf:deploy", Queue: "iq2"},
		}}
	encoded, _ := base.EncodeMessage(msg)
	client.HSet(ctx, base.TaskKey("iq2", "c9:0"), "msg", encoded, "state", int(base.StatePending))
	client.XAdd(ctx, &redis.XAddArgs{Stream: base.StreamKey("iq2"), Values: map[string]any{"task_id": "c9:0"}})

	insp := NewInspector(client)
	info, err := insp.GetTask(ctx, "iq2", "c9:0")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.ChainNext) != 2 || info.ChainNext[0] != "group[2]→wf:merge" || info.ChainNext[1] != "wf:deploy" {
		t.Errorf("ChainNext = %v", info.ChainNext)
	}
	if info.ChainPending != 2 {
		t.Errorf("ChainPending = %d", info.ChainPending)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test -p 1 -count=1 -run TestTaskInfo_ChainNextShowsGroupStage .`
Expected: FAIL — `ChainNext = [wf:merge wf:deploy]` (그룹 표기 없음)

- [ ] **Step 3: 구현** — `inspector.go`의 `taskInfoFromMsg`에서 `ChainNext`를 채우는 지점을 찾아(현재 `link.Kind`를 그대로 append) 다음으로 교체:

```go
	for _, link := range m.Chain {
		if len(link.Group) > 0 {
			ti.ChainNext = append(ti.ChainNext, fmt.Sprintf("group[%d]→%s", len(link.Group), link.Kind))
			continue
		}
		ti.ChainNext = append(ti.ChainNext, link.Kind)
	}
```

(기존 루프 구조가 다르면 동일 의미로 맞춤. `fmt` import 확인. webui 체인 스테퍼는 `ChainNext` 문자열을 그대로 렌더하므로 추가 변경 없음 — Step 4에서 확인.)

- [ ] **Step 4: 통과 확인 + webui 렌더 확인**

Run: `go test -p 1 -count=1 -run 'TestTaskInfo_ChainNext|TestGetTask' . && grep -rn 'ChainNext' contrib/webui/ | head -5`
Expected: 테스트 PASS. grep 결과에서 webui가 ChainNext를 문자열 목록으로 렌더함을 확인(별도 파싱 없음 → 무변경으로 그룹 표기가 그대로 노출). 만약 webui가 kind를 파싱/링크화한다면 그룹 표기 문자열이 깨지지 않는지 `cd contrib/webui && go test ./...`로 확인.

- [ ] **Step 5: 커밋**

```bash
git add inspector.go inspector_test.go
git commit -m "feat: TaskInfo.ChainNext에 그룹 스테이지 표기 (group[N]→cbKind)"
```

---

### Task 7: 소크·cluster 19·tour·README + 최종 검증

**Files:**
- Modify: `benchmarks/soak/sampler.go` (SCAN 패턴), `benchmarks/soak/workload.go` (ThenGroup 경로)
- Modify: `cluster_test.go` (체크리스트 + 19번째)
- Modify: `examples/tour/main.go` (섹션 15 확장)
- Modify: `README.md`

- [ ] **Step 1: 소크 반영** —
`benchmarks/soak/sampler.go`: group SCAN 패턴 `chronos:*:group:*` → `chronos:*:group*` (groupresult HASH도 집계 — PR 1의 미해결 항목). 주석 한 줄: `// group:*(pending SET)과 groupresult:*(수집 HASH)를 함께 센다.`

`benchmarks/soak/workload.go`: 10초 티커의 chain 3링크를 ThenGroup 워크플로로 교체:

```go
			// chain 3링크 → 팬아웃→팬인→후속 워크플로(단일+그룹+단일 스테이지).
			wf := chronos.NewChain().
				Then(chainArgs{Seq: batch, Link: 0}, chronos.WithQueue("soak-a")).
				ThenGroup(chronos.NewGroup().
					Add(groupArgs{Seq: batch, Member: 0}, chronos.WithQueue("soak-a")).
					Add(groupArgs{Seq: batch, Member: 1}, chronos.WithQueue("soak-b")).
					OnComplete(cbArgs{Seq: batch}, chronos.WithQueue("soak-a"))).
				Then(chainArgs{Seq: batch, Link: 2}, chronos.WithQueue("soak-a"))
			if _, err := wf.Enqueue(ctx, w.client); err != nil && ctx.Err() == nil {
				log.Printf("soak: workflow enqueue: %v", err)
			}
```

(기존 독립 group 3멤버 티커는 그대로 유지 — 두 경로 모두 순환.) 검증: `cd benchmarks && go build ./... && go vet ./... && go test ./soak/ -p 1 -count=1`.

- [ ] **Step 2: cluster 스모크 19번째** — `cluster_test.go` 체크리스트에 추가:

```go
// [x] ThenGroup (그룹 스테이지 — create-if-absent 펜스·결과 복제·꼬리 상속) → TestCluster_ThenGroup
```

파일 끝에 (기존 관례 — `testutil.NewClusterRedis(t)`):

```go
// ThenGroup 전 구간: 멤버가 다른 큐(다른 슬롯)에 있어도 스테이지 생성·완료·
// 체인 계속이 CROSSSLOT 없이 동작하는지.
func TestCluster_ThenGroup(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	ctx := context.Background()

	var finalGot atomic.Pointer[string]
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[clWfPrep]) (clWfOut, error) {
		return clWfOut{V: "p"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[clWfEnc]) (clWfOut, error) {
		prev, _ := PrevResult[clWfOut](task)
		return clWfOut{V: prev.V + task.Args.R}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[clWfMerge]) (clWfOut, error) {
		rs, err := GroupResults[clWfOut](task)
		if err != nil {
			return clWfOut{}, err
		}
		return clWfOut{V: rs[0].V + "|" + rs[1].V}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[clWfFinal]) error {
		if prev, err := PrevResult[clWfOut](task); err == nil {
			finalGot.Store(&prev.V)
		}
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"clwf-a": 1, "clwf-b": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	_, err := NewChain().
		Then(clWfPrep{}, WithQueue("clwf-a")).
		ThenGroup(NewGroup().
			Add(clWfEnc{R: "x"}, WithQueue("clwf-a")).
			Add(clWfEnc{R: "y"}, WithQueue("clwf-b")). // 다른 슬롯의 멤버
			OnComplete(clWfMerge{}, WithQueue("clwf-b"))).
		Then(clWfFinal{}, WithQueue("clwf-a")).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for finalGot.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if finalGot.Load() == nil {
		t.Fatal("workflow never finished on cluster")
	}
	if got := *finalGot.Load(); got != "px|py" {
		t.Errorf("final = %q, want px|py", got)
	}
}

type clWfPrep struct{}

func (clWfPrep) Kind() string { return "clwf:prep" }

type clWfEnc struct {
	R string `json:"r"`
}

func (clWfEnc) Kind() string { return "clwf:enc" }

type clWfMerge struct{}

func (clWfMerge) Kind() string { return "clwf:merge" }

type clWfFinal struct{}

func (clWfFinal) Kind() string { return "clwf:final" }
```

검증(docker 필요 — 없으면 `deploy/redis-cluster`에서 `docker compose up -d`): `make test-cluster` → 19/19 PASS.

- [ ] **Step 3: tour 섹션 15 확장** — `examples/tour/main.go`의 섹션 15를 팬아웃→팬인→후속으로 확장: 기존 OCR→번역 2링크 체인을 다음으로 교체(기존 OcrArgs/OcrOut/TranslateArgs 유지, MergeArgs 추가):

```go
	section("15) 워크플로: OCR → [번역 2개 병렬] → 병합 — 결과가 스테이지를 타고 흐른다")
	resMux := chronos.NewMux()
	chronos.AddHandlerR(resMux, func(ctx context.Context, t *chronos.Task[OcrArgs]) (OcrOut, error) {
		fmt.Printf("   ▶ [ocr] %s 인식\n", t.Args.Image)
		return OcrOut{Text: "hello chronos"}, nil
	})
	chronos.AddHandlerR(resMux, func(ctx context.Context, t *chronos.Task[TranslateArgs]) (OcrOut, error) {
		src, err := chronos.PrevResult[OcrOut](t)
		if err != nil {
			return OcrOut{}, err
		}
		fmt.Printf("   ▶ [translate:%s] %q 번역\n", t.Args.Lang, src.Text)
		return OcrOut{Text: t.Args.Lang + "(" + src.Text + ")"}, nil
	})
	chronos.AddHandler(resMux, func(ctx context.Context, t *chronos.Task[MergeArgs]) error {
		rs, err := chronos.GroupResults[OcrOut](t)
		if err != nil {
			return err
		}
		fmt.Printf("   ▶ [merge] 병렬 결과 수신: %q + %q\n", rs[0].Text, rs[1].Text)
		return nil
	})
	resSrv := chronos.NewServer(rdb, chronos.ServerConfig{Queues: map[string]int{"results": 1}, Concurrency: 4})
	if err := resSrv.Start(ctx, resMux); err != nil {
		fmt.Printf("results 서버 start 실패: %v\n", err)
	}
	_, _ = chronos.NewChain().
		Then(OcrArgs{Image: "scan-001.png"}, chronos.WithQueue("results")).
		ThenGroup(chronos.NewGroup().
			Add(TranslateArgs{Lang: "ko"}, chronos.WithQueue("results")).
			Add(TranslateArgs{Lang: "ja"}, chronos.WithQueue("results")).
			OnComplete(MergeArgs{}, chronos.WithQueue("results"))).
		Enqueue(ctx, client)
	time.Sleep(3 * time.Second)
	shutRes, cancelRes := context.WithTimeout(context.Background(), 3*time.Second)
	_ = resSrv.Shutdown(shutRes)
	cancelRes()
```

타입 변경: `TranslateArgs`에 `Lang string \`json:"lang"\`` 필드 추가, `MergeArgs struct{}`(`Kind() "tour:merge"`) 신규. 상단 doc comment의 "task results" 문구를 "workflows (fan-out/fan-in with results)"로 갱신.

검증: `gofmt -w examples/tour/main.go && go vet ./examples/tour/ && go run ./examples/tour 2>&1 | sed -n '/=== 15)/,$p' | head -8` — ocr → translate:ko/ja(각각 "hello chronos" 수신) → merge가 두 결과 수신 출력.

- [ ] **Step 4: README** — "Passing results between steps" 다음에 소절 추가:

```markdown
### Parallel stages (fan-out → fan-in)

`ThenGroup` puts a group in the middle (or at the end) of a chain:

```go
chronos.NewChain().
	Then(Validate{}).
	ThenGroup(chronos.NewGroup().
		Add(Encode{Res: "720p"}).
		Add(Encode{Res: "4k"}).
		OnComplete(BuildManifest{})).   // fan-in: receives GroupResults
	Then(Deploy{}).                     // receives the callback's result
	Enqueue(ctx, client)
```

Every member receives the previous stage's result (`PrevResult`), the
callback fans the member results in, and its own result flows to the next
stage. Failure semantics are unchanged: a dead-lettered member stalls the
stage until you re-run it (`RunTask`), and completed stages are fenced
against predecessor redeliveries. A group cannot be the first stage — start
with `Then`, or use `NewGroup` directly.
```

그리고 기존 "Not yet composable with chains (no group-as-chain-link)" 류의 낡은 문장을 검색(`grep -n "composable" README.md`)해 제거/갱신. Known limitations의 chain×group 항목도 "그룹 멤버가 체인/그룹인 재귀 중첩은 미지원" 서술로 교체.

- [ ] **Step 5: 최종 검증 + 커밋**

Run: `make check` (전체 그린)

```bash
git add benchmarks/soak/sampler.go benchmarks/soak/workload.go cluster_test.go examples/tour/main.go README.md
git commit -m "docs+test: cluster 스모크 19(ThenGroup) + tour 15 팬아웃 확장 + 소크 워크플로 경로·groupresult 집계 + README"
```

---

## 완료 후

1. k:code-reviewer 최종 브랜치 리뷰 (집중: **enqueueNextGroup의 재전달 멱등성**(3상태 분기·SISMEMBER 창), **GroupCallbackChain의 cjson 왕복**(꼬리에 그룹 스테이지가 또 있을 때 — 연속 ThenGroup — Group 배열의 base64 payload 보존), 멤버 메시지 크기(N × (PrevResult+꼬리) 복제), ChainNext 그룹 표기가 CLI/webui에서 깨지지 않는지, 스펙 편차 2건(cb ID·그룹 첫 스테이지 금지)의 문서화).
2. 지적 반영 → PR 생성 (base main, assignee kenshin579, HEREDOC): 제목 `feat: Chain.ThenGroup — 팬아웃→팬인→후속 병렬 스테이지`.
3. CI 그린 → 사용자 머지 승인 요청 → 이후 v0.11.0 태그 논의(결과 전달 + ThenGroup 묶음).
