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
	// 멤버 지연이 GroupTTL(7일)을 넘으면 거부 — pending SET이 멤버 실행 전에
	// 만료되어 콜백이 영영 뜨지 않는 조용한 좌초를 막는다(독립 Group.Enqueue와
	// 동일 규칙).
	if msg := enqueueErr(t, NewChain().Then(wfPrep{}).
		ThenGroup(NewGroup().Add(wfEnc{}, WithProcessIn(8*24*time.Hour)).OnComplete(wfMerge{}))); !strings.Contains(msg, "TTL") {
		t.Errorf("member delay over TTL: %s", msg)
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
