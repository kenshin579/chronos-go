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
