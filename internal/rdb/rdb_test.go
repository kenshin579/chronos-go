package rdb

import (
	"context"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestEnqueue_StoresBodyAndPushesToStream(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{
		ID:      "task-1",
		Kind:    "email:send",
		Payload: []byte(`{"user_id":"u1"}`),
		Queue:   "default",
	}

	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Task body is stored in the task HASH with state=pending.
	got, err := client.HGet(ctx, base.TaskKey("default", "task-1"), "msg").Result()
	if err != nil {
		t.Fatalf("hget msg: %v", err)
	}
	decoded, err := base.DecodeMessage([]byte(got))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Kind != "email:send" {
		t.Errorf("kind = %q, want %q", decoded.Kind, "email:send")
	}

	// Stream has exactly one entry.
	n, err := client.XLen(ctx, base.StreamKey("default")).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	if n != 1 {
		t.Errorf("stream length = %d, want 1", n)
	}

	// Queue name is registered.
	isMember, err := client.SIsMember(ctx, base.QueuesKey(), "default").Result()
	if err != nil {
		t.Fatalf("sismember: %v", err)
	}
	if !isMember {
		t.Error("queue 'default' not registered in queues set")
	}
}

func TestDequeue_ReturnsEnqueuedTask(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	msg := &base.TaskMessage{ID: "task-1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, streamID, err := r.Dequeue(ctx, "consumer-1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got.ID != "task-1" {
		t.Errorf("dequeued id = %q, want task-1", got.ID)
	}
	if streamID == "" {
		t.Error("expected non-empty stream id")
	}
	if got.State != base.StateActive {
		t.Errorf("state = %v, want active", got.State)
	}
}

func TestDequeue_EmptyReturnsErrNoTask(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	_, _, err := r.Dequeue(ctx, "consumer-1", 0, "default")
	if err != ErrNoTask {
		t.Errorf("err = %v, want ErrNoTask", err)
	}
}

func TestDone_AcksAndDeletesTask(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	msg := &base.TaskMessage{ID: "task-1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, streamID, err := r.Dequeue(ctx, "consumer-1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	if err := r.Done(ctx, "default", streamID, got); err != nil {
		t.Fatalf("done: %v", err)
	}

	// Task hash is gone.
	exists, err := client.Exists(ctx, base.TaskKey("default", "task-1")).Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists != 0 {
		t.Error("task hash should be deleted after Done")
	}

	// PEL is empty (the entry was acked).
	pending, err := client.XPending(ctx, base.StreamKey("default"), ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 0 {
		t.Errorf("pending count = %d, want 0", pending.Count)
	}
}
