package rdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestEnqueuePeriodic_DedupsSameTrigger(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "job:1:1700000000", Kind: "job", Payload: []byte("{}"), Queue: "default"}
	dedupKey := base.PeriodicDedupKey("default", "job:1:1700000000")

	// First enqueue for this trigger succeeds.
	if err := r.EnqueuePeriodic(ctx, msg, dedupKey, time.Minute); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second enqueue for the SAME trigger (e.g. a split-brain second leader) is rejected.
	if err := r.EnqueuePeriodic(ctx, msg, dedupKey, time.Minute); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second: err=%v, want ErrDuplicateTask", err)
	}
	// Exactly one stream entry.
	if n, _ := client.XLen(ctx, base.StreamKey("default")).Result(); n != 1 {
		t.Errorf("stream len = %d, want 1", n)
	}
	// The dedup key is NOT this task's UniqueKey, so Done must not release it
	// (it expires by TTL only).
	if msg.UniqueKey != "" {
		t.Errorf("periodic dedup must not set UniqueKey, got %q", msg.UniqueKey)
	}
}

func TestLastFired_RoundTrip(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// Absent → zero time, ok=false.
	_, ok, err := r.GetLastFired(ctx, "job:1")
	if err != nil {
		t.Fatalf("get absent: %v", err)
	}
	if ok {
		t.Error("absent lastFired should report ok=false")
	}

	when := time.Unix(1700000000, 0)
	if err := r.SetLastFired(ctx, "job:1", when); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, ok, err := r.GetLastFired(ctx, "job:1")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if !got.Equal(when) {
		t.Errorf("lastFired = %v, want %v", got, when)
	}
}
