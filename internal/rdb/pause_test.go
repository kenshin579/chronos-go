package rdb

import (
	"context"
	"testing"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestPauseResumeQueues(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if paused, _ := r.PausedQueues(ctx); len(paused) != 0 {
		t.Fatalf("initial paused = %v, want empty", paused)
	}
	if err := r.PauseQueue(ctx, "critical"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if err := r.PauseQueue(ctx, "critical"); err != nil { // 멱등
		t.Fatalf("pause twice: %v", err)
	}
	paused, err := r.PausedQueues(ctx)
	if err != nil || len(paused) != 1 || paused[0] != "critical" {
		t.Fatalf("paused = %v err=%v", paused, err)
	}
	if err := r.ResumeQueue(ctx, "critical"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if paused, _ := r.PausedQueues(ctx); len(paused) != 0 {
		t.Fatalf("after resume = %v, want empty", paused)
	}
}
