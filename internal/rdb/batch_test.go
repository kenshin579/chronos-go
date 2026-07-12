package rdb

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestDequeueBatch_ClaimsUpToCount(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	for i := 0; i < 5; i++ {
		msg := &base.TaskMessage{ID: fmt.Sprintf("task-%d", i), Kind: "k", Payload: []byte("{}"), Queue: "default"}
		if err := r.Enqueue(ctx, msg); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	tasks, err := r.DequeueBatch(ctx, "c1", 0, "default", 3)
	if err != nil {
		t.Fatalf("dequeue batch: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("len(tasks) = %d, want 3", len(tasks))
	}

	seenIDs := make(map[string]bool)
	seenStreamIDs := make(map[string]bool)
	for _, d := range tasks {
		if d.Msg == nil {
			t.Fatal("nil msg in batch")
		}
		if d.Msg.State != base.StateActive {
			t.Errorf("task %s state = %v, want active", d.Msg.ID, d.Msg.State)
		}
		if d.StreamID == "" {
			t.Errorf("task %s has empty stream id", d.Msg.ID)
		}
		if seenIDs[d.Msg.ID] {
			t.Errorf("duplicate task id %q in batch", d.Msg.ID)
		}
		seenIDs[d.Msg.ID] = true
		if seenStreamIDs[d.StreamID] {
			t.Errorf("duplicate stream id %q in batch", d.StreamID)
		}
		seenStreamIDs[d.StreamID] = true

		// State is persisted in Redis, not just on the struct.
		raw, err := client.HGet(ctx, base.TaskKey("default", d.Msg.ID), "state").Result()
		if err != nil {
			t.Fatalf("hget state: %v", err)
		}
		if raw != strconv.Itoa(int(base.StateActive)) {
			t.Errorf("task %s persisted state = %q, want %d", d.Msg.ID, raw, int(base.StateActive))
		}
	}

	// The remaining 2 tasks are still fetchable.
	rest, err := r.DequeueBatch(ctx, "c1", 0, "default", 10)
	if err != nil {
		t.Fatalf("dequeue rest: %v", err)
	}
	if len(rest) != 2 {
		t.Fatalf("len(rest) = %d, want 2", len(rest))
	}
	for _, d := range rest {
		if seenIDs[d.Msg.ID] {
			t.Errorf("task %q delivered twice", d.Msg.ID)
		}
	}
}

func TestDequeueBatch_SkipsOrphans(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	for i := 0; i < 3; i++ {
		msg := &base.TaskMessage{ID: fmt.Sprintf("task-%d", i), Kind: "k", Payload: []byte("{}"), Queue: "default"}
		if err := r.Enqueue(ctx, msg); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	// Delete one task's body directly — its stream entry becomes an orphan.
	if err := client.Del(ctx, base.TaskKey("default", "task-1")).Err(); err != nil {
		t.Fatalf("del: %v", err)
	}

	tasks, err := r.DequeueBatch(ctx, "c1", 0, "default", 3)
	if err != nil {
		t.Fatalf("dequeue batch: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("len(tasks) = %d, want 2", len(tasks))
	}
	for _, d := range tasks {
		if d.Msg.ID == "task-1" {
			t.Errorf("orphan task-1 was returned")
		}
	}

	// The orphan's stream entry is acked and deleted.
	n, err := client.XLen(ctx, base.StreamKey("default")).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	if n != 2 {
		t.Errorf("stream length = %d, want 2 (orphan entry deleted)", n)
	}
}

func TestDequeueBatch_EmptyReturnsErrNoTask(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	_, err := r.DequeueBatch(ctx, "c1", 0, "default", 5)
	if err != ErrNoTask {
		t.Errorf("err = %v, want ErrNoTask", err)
	}
}
