package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

// enqueueAndDequeue is a helper: enqueue one task, dequeue it (so it is active
// and in the PEL), and return the message + its stream entry ID.
func enqueueAndDequeue(t *testing.T, r *RDB, qname string, msg *base.TaskMessage) (*base.TaskMessage, string) {
	t.Helper()
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, qname); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, streamID, err := r.Dequeue(ctx, "c1", 0, qname)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	return got, streamID
}

func TestRetry_MovesActiveToRetryZSet(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default", MaxRetry: 5}
	got, streamID := enqueueAndDequeue(t, r, "default", msg)
	got.Retried = 1

	retryAt := time.Now().Add(30 * time.Second)
	if err := r.Retry(ctx, "default", streamID, got, retryAt); err != nil {
		t.Fatalf("retry: %v", err)
	}

	// Stream entry acked (PEL empty).
	pending, err := client.XPending(ctx, base.StreamKey("default"), ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 0 {
		t.Errorf("pending = %d, want 0", pending.Count)
	}

	// Task is in the retry ZSET with the expected score.
	score, err := client.ZScore(ctx, base.RetryKey("default"), "t1").Result()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	if int64(score) != retryAt.Unix() {
		t.Errorf("retry score = %d, want %d", int64(score), retryAt.Unix())
	}

	// Task hash reflects retry state and incremented Retried.
	raw, err := client.HGet(ctx, base.TaskKey("default", "t1"), "msg").Result()
	if err != nil {
		t.Fatalf("hget: %v", err)
	}
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.State != base.StateRetry || stored.Retried != 1 {
		t.Errorf("stored = state:%v retried:%d, want retry/1", stored.State, stored.Retried)
	}
}

func TestArchive_MovesActiveToArchivedZSet(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default", MaxRetry: 0}
	got, streamID := enqueueAndDequeue(t, r, "default", msg)

	diedAt := time.Now()
	if err := r.Archive(ctx, "default", streamID, got, diedAt); err != nil {
		t.Fatalf("archive: %v", err)
	}

	pending, _ := client.XPending(ctx, base.StreamKey("default"), ConsumerGroup).Result()
	if pending.Count != 0 {
		t.Errorf("pending = %d, want 0", pending.Count)
	}
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "t1").Result(); err != nil {
		t.Fatalf("task not in archived zset: %v", err)
	}
	raw, _ := client.HGet(ctx, base.TaskKey("default", "t1"), "msg").Result()
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.State != base.StateArchived {
		t.Errorf("state = %v, want archived", stored.State)
	}
}
