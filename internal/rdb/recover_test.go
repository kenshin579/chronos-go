package rdb

import (
	"context"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestRecover_ReclaimsStuckTaskToRetry(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// Dequeue with consumer "dead" so the entry sits in dead's PEL, then never ack.
	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default", MaxRetry: 5}
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := r.Dequeue(ctx, "dead", 0, "default"); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	// minIdle=0 reclaims immediately (no need to wait).
	recovered, archived, err := r.Recover(ctx, "default", "recoverer", 0, 100)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered != 1 || len(archived) != 0 {
		t.Fatalf("recovered=%d archived=%d, want 1/0", recovered, len(archived))
	}

	// Task moved to retry ZSET, Retried incremented, PEL cleared.
	if _, err := client.ZScore(ctx, base.RetryKey("default"), "t1").Result(); err != nil {
		t.Errorf("recovered task not in retry zset: %v", err)
	}
	pending, _ := client.XPending(ctx, base.StreamKey("default"), ConsumerGroup).Result()
	if pending.Count != 0 {
		t.Errorf("pending = %d, want 0", pending.Count)
	}
	raw, _ := client.HGet(ctx, base.TaskKey("default", "t1"), "msg").Result()
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.Retried != 1 {
		t.Errorf("retried = %d, want 1", stored.Retried)
	}
}

func TestRecover_ArchivesWhenRetriesExhausted(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// MaxRetry=1 and already Retried=1 → the crash exhausts the budget → archive.
	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default", MaxRetry: 1, Retried: 1}
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := r.Dequeue(ctx, "dead", 0, "default"); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	recovered, archived, err := r.Recover(ctx, "default", "recoverer", 0, 100)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered != 0 || len(archived) != 1 {
		t.Fatalf("recovered=%d archived=%d, want 0/1", recovered, len(archived))
	}
	if archived[0].ID != "t1" {
		t.Errorf("archived id = %q, want t1", archived[0].ID)
	}
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "t1").Result(); err != nil {
		t.Errorf("task not in archived zset: %v", err)
	}
}

// An orphan PEL entry (body deleted while in-flight) must be trimmed from the
// stream by Recover, not merely acked — otherwise it leaks and inflates counts.
func TestRecover_OrphanEntryIsTrimmed(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.Enqueue(ctx, &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Dequeue with a "dead" consumer so the entry sits in the PEL, then delete the
	// body to make it an orphan.
	if _, _, err := r.Dequeue(ctx, "dead", 0, "default"); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if err := client.Del(ctx, base.TaskKey("default", "t1")).Err(); err != nil {
		t.Fatalf("del body: %v", err)
	}

	if _, _, err := r.Recover(ctx, "default", "recoverer", 0, 100); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n, _ := client.XLen(ctx, base.StreamKey("default")).Result(); n != 0 {
		t.Errorf("stream len after recovering an orphan = %d, want 0 (must be XDEL'd)", n)
	}
}
