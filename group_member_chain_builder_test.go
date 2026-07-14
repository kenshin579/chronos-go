package chronos

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type mcDump struct {
	T string `json:"t"`
}

func (mcDump) Kind() string { return "mc:dump" }

type mcXform struct {
	T string `json:"t"`
}

func (mcXform) Kind() string { return "mc:xform" }

type mcLoad struct {
	T string `json:"t"`
}

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
	// 첫 링크 지연이 GroupTTL(7일)을 넘으면 거부 — pending SET이 멤버 실행 전
	// 만료되어 콜백이 조용히 미발화하는 좌초를 막는다(flat·ThenGroup 경로와 일관).
	if msg := groupEnqErr(t, NewGroup().AddChain(
		NewChain().Then(mcDump{}, WithProcessIn(8*24*time.Hour))).OnComplete(mcVerify{})); !strings.Contains(msg, "TTL") {
		t.Errorf("first-link delay exceeds TTL: %s", msg)
	}
	// 링크별 지연은 개별로는 GroupTTL 이내여도 합산이 넘으면 거부 — pending SET
	// TTL은 체인 진행 중 갱신되지 않으므로 링크 지연의 총합이 창을 넘으면 좌초.
	// 첫 링크 4일 + tail 링크 4일 = 8일 > 7일.
	if msg := groupEnqErr(t, NewGroup().AddChain(
		NewChain().
			Then(mcDump{}, WithProcessIn(4*24*time.Hour)).
			Then(mcLoad{}, WithProcessIn(4*24*time.Hour))).OnComplete(mcVerify{})); !strings.Contains(msg, "TTL") {
		t.Errorf("summed link delays exceed TTL: %s", msg)
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
