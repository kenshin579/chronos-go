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
