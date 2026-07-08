package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

// pendingIdle returns the idle time of the single pending entry.
func pendingIdle(t *testing.T, r *RDB, qname string) time.Duration {
	t.Helper()
	ext, err := r.Client().XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: base.StreamKey(qname), Group: ConsumerGroup, Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("xpendingext: %v", err)
	}
	if len(ext) == 0 {
		t.Fatal("no pending entries")
	}
	return ext[0].Idle
}

func TestExtendLease_ResetsIdle(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.Enqueue(ctx, &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, streamID, err := r.Dequeue(ctx, "c1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	if err := r.ExtendLease(ctx, "default", "c1", []string{streamID}); err != nil {
		t.Fatalf("extend lease: %v", err)
	}
	// After extend, idle should be reset near 0 (well under the 300ms slept).
	if idle := pendingIdle(t, r, "default"); idle > 100*time.Millisecond {
		t.Errorf("idle after ExtendLease = %v, want < 100ms (reset)", idle)
	}
}

func TestRenewUnique_ExtendsTTL_MissingIsNoOp(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	key := base.UniqueKey("default", "k:abc")
	if err := client.Set(ctx, key, "t1", 1*time.Second).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := r.RenewUnique(ctx, []string{key, base.UniqueKey("default", "missing")}, time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}
	ttl, _ := client.PTTL(ctx, key).Result()
	if ttl < 30*time.Second {
		t.Errorf("ttl after renew = %v, want ~1m", ttl)
	}
	// Missing key must not be recreated.
	if ex, _ := client.Exists(ctx, base.UniqueKey("default", "missing")).Result(); ex != 0 {
		t.Error("RenewUnique must not create a missing key")
	}
}
