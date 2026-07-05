package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestSchedule_StoresTaskInScheduledZSet(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	processAt := time.Now().Add(1 * time.Hour)
	if err := r.Schedule(ctx, msg, processAt); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	// In scheduled ZSET with the right score; NOT in the stream yet.
	score, err := client.ZScore(ctx, base.ScheduledKey("default"), "t1").Result()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	if int64(score) != processAt.Unix() {
		t.Errorf("score = %d, want %d", int64(score), processAt.Unix())
	}
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 0 {
		t.Errorf("stream len = %d, want 0 (not due yet)", slen)
	}
	// Task hash exists with scheduled state and queue registered.
	raw, err := client.HGet(ctx, base.TaskKey("default", "t1"), "msg").Result()
	if err != nil {
		t.Fatalf("hget: %v", err)
	}
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.State != base.StateScheduled {
		t.Errorf("state = %v, want scheduled", stored.State)
	}
	if m, _ := client.SIsMember(ctx, base.QueuesKey(), "default").Result(); !m {
		t.Error("queue not registered")
	}
	_ = redis.Nil
}

func TestForwardScheduled_MovesDueTasksToStream(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	due := &base.TaskMessage{ID: "due", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	future := &base.TaskMessage{ID: "future", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Schedule(ctx, due, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("schedule due: %v", err)
	}
	if err := r.Schedule(ctx, future, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule future: %v", err)
	}

	n, err := r.ForwardScheduled(ctx, "default", time.Now(), 100)
	if err != nil {
		t.Fatalf("forward scheduled: %v", err)
	}
	if n != 1 {
		t.Errorf("forwarded = %d, want 1", n)
	}
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 1 {
		t.Errorf("stream len = %d, want 1", slen)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), "future").Result(); err != nil {
		t.Error("future task should remain scheduled")
	}
}
