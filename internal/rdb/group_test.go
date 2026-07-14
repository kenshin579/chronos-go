package rdb

import (
	"context"
	"strconv"
	"strings"
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

func TestGroup_ReportRefreshesTTL(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.CreateGroup(ctx, "cbq", "gr", []string{"gr:m0", "gr:m1"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// TTL을 인위적으로 짧게 줄인 뒤, 멤버 보고가 다시 GroupTTL로 늘리는지 확인.
	if err := client.Expire(ctx, base.GroupKey("cbq", "gr"), time.Minute).Err(); err != nil {
		t.Fatalf("expire: %v", err)
	}
	if _, err := r.CompleteGroupMember(ctx, &base.TaskMessage{
		ID: "gr:m0", Kind: "m", Queue: "default", GroupID: "gr", GroupQueue: "cbq",
		GroupCallback: &base.ChainLink{Kind: "cb", Payload: []byte(`{}`), Queue: "cbq", MaxRetry: 25},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	ttl, err := client.TTL(ctx, base.GroupKey("cbq", "gr")).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= time.Minute {
		t.Errorf("ttl = %v, want refreshed to ~%v", ttl, GroupTTL)
	}
}

// 멤버 3(결과 2, 무결과 1) 그룹: 결과가 인덱스 순서로 콜백 메시지에 내장되고
// groupresult HASH가 삭제되는지 확인.
func TestCompleteGroupMember_CollectsResults(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
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
	client := testutil.NewRedis(t)
	r := NewRDB(client)
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

// 결과가 있는 멤버가 결과 HASH를 만든 뒤, 무결과 멤버가 뒤늦게(트리클) 보고할 때
// 결과 HASH의 TTL도 pending SET과 lockstep으로 GroupTTL 근처까지 갱신되는지 확인.
// 이 갱신이 없으면 무결과 멤버가 GroupTTL보다 긴 간격으로 보고하는 동안 HASH가
// SET보다 먼저 만료되어 이미 기록된 결과가 소실될 수 있다.
func TestCompleteGroupMember_NoResultReportRefreshesResultHashTTL(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	cb := &base.ChainLink{Kind: "g:cb", Payload: []byte(`{}`), Queue: "gq"}
	mk := func(i int, result []byte) *base.TaskMessage {
		return &base.TaskMessage{
			ID: "g3:m" + strconv.Itoa(i), Kind: "g:m", Queue: "gq",
			GroupID: "g3", GroupQueue: "gq", GroupCallback: cb,
			GroupIndex: i, GroupSize: 3, Result: result,
		}
	}
	if err := r.CreateGroup(ctx, "gq", "g3", []string{"g3:m0", "g3:m1", "g3:m2"}); err != nil {
		t.Fatal(err)
	}
	// 결과 있는 멤버가 HASH를 만든다(TTL = GroupTTL).
	if fired, err := r.CompleteGroupMember(ctx, mk(0, []byte(`{"v":0}`))); err != nil || fired {
		t.Fatalf("m0: fired=%v err=%v", fired, err)
	}
	// HASH TTL을 인위적으로 짧게 줄여, 무결과 보고가 lockstep으로 다시 늘리는지 본다.
	resultKey := base.GroupResultKey("gq", "g3")
	if err := client.Expire(ctx, resultKey, time.Minute).Err(); err != nil {
		t.Fatalf("expire: %v", err)
	}
	// 무결과 멤버 보고(ARGV[8] == "").
	if fired, err := r.CompleteGroupMember(ctx, mk(1, nil)); err != nil || fired {
		t.Fatalf("m1: fired=%v err=%v", fired, err)
	}
	ttl, err := client.TTL(ctx, resultKey).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= time.Minute {
		t.Errorf("result hash ttl = %v, want refreshed to ~%v", ttl, GroupTTL)
	}
}

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

	// 펜스(콜백 hash) 소멸 후 재생성(return 2): 만료 직전 잔존 결과 HASH가
	// 새 라운드에 섞이지 않도록 삭제되어야 한다.
	client.Del(ctx, base.TaskKey("gq", "c1:1:cb"))
	client.HSet(ctx, base.GroupResultKey("gq", "c1:1"), "0", "stale")
	st, err = r.CreateGroupIfAbsent(ctx, "gq", "c1:1", members, "c1:1:cb")
	if err != nil || st != GroupStageCreated {
		t.Fatalf("recreate after fence expiry: st=%v err=%v", st, err)
	}
	if n, _ := client.Exists(ctx, base.GroupResultKey("gq", "c1:1")).Result(); n != 0 {
		t.Fatal("recreated stage must delete the leftover result hash")
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
