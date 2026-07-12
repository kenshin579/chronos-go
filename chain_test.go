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
	// WithUnique 병용 → 에러 (미지원).
	if _, err := NewChain().Then(emailArgs{UserID: "a"}, WithUnique(time.Minute)).Enqueue(ctx, c); err == nil {
		t.Error("WithUnique in chain: want error")
	}
	// 링크 1개 → 정상.
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
