package chronos

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestEnqueue_WithProcessIn_GoesToScheduledZSet(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(1*time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Lands in the scheduled ZSET, not the stream.
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), info.ID).Result(); err != nil {
		t.Errorf("task not in scheduled zset: %v", err)
	}
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 0 {
		t.Errorf("stream len = %d, want 0", slen)
	}
}

func TestEnqueue_WithProcessInPast_GoesToStreamImmediately(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(-1*time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// A non-future time is treated as immediate.
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 1 {
		t.Errorf("stream len = %d, want 1", slen)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), info.ID).Result(); err == nil {
		t.Error("immediate task should not be in scheduled zset")
	}
}

func TestEnqueue_WithUnique_RejectsDuplicate(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithUnique(time.Minute)); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	// Identical args → duplicate.
	_, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithUnique(time.Minute))
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second enqueue err = %v, want ErrDuplicateTask", err)
	}
	// Different args → allowed.
	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u2"}, WithUnique(time.Minute)); err != nil {
		t.Fatalf("different-args enqueue: %v", err)
	}
}

func TestEnqueue_WithUniqueAndProcessIn_Schedules(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithUnique(time.Minute), WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), info.ID).Result(); err != nil {
		t.Errorf("unique+delayed task should be scheduled: %v", err)
	}
}
