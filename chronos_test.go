package chronos

import (
	"context"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

type emailArgs struct {
	UserID string `json:"user_id"`
}

func (emailArgs) Kind() string { return "email:send" }

func TestEnqueue_DefaultQueue(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	info, err := Enqueue(context.Background(), c, emailArgs{UserID: "u1"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if info.Kind != "email:send" {
		t.Errorf("kind = %q, want email:send", info.Kind)
	}
	if info.Queue != "default" {
		t.Errorf("queue = %q, want default", info.Queue)
	}
	if info.ID == "" {
		t.Error("expected generated task id")
	}

	n, err := client.XLen(context.Background(), base.StreamKey("default")).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	if n != 1 {
		t.Errorf("stream length = %d, want 1", n)
	}
}

func TestEnqueue_WithQueueAndTaskID(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	info, err := Enqueue(context.Background(), c, emailArgs{UserID: "u2"},
		WithQueue("critical"), WithTaskID("fixed-id"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if info.Queue != "critical" {
		t.Errorf("queue = %q, want critical", info.Queue)
	}
	if info.ID != "fixed-id" {
		t.Errorf("id = %q, want fixed-id", info.ID)
	}
}
