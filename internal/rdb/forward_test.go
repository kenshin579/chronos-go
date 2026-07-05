package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestForwardRetry_MovesDueTasksToStream(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// One due task (retryAt in the past), one not-yet-due (future).
	dueMsg := &base.TaskMessage{ID: "due", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	notMsg := &base.TaskMessage{ID: "future", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	for _, m := range []*base.TaskMessage{dueMsg, notMsg} {
		if err := r.Enqueue(ctx, m); err != nil { // stores task hash
			t.Fatalf("enqueue: %v", err)
		}
	}
	// Put them directly into the retry ZSET with controlled scores.
	client.ZAdd(ctx, base.RetryKey("default"), redis.Z{Score: float64(time.Now().Add(-1 * time.Minute).Unix()), Member: "due"})
	client.ZAdd(ctx, base.RetryKey("default"), redis.Z{Score: float64(time.Now().Add(1 * time.Hour).Unix()), Member: "future"})
	// Drain the stream entries created by Enqueue so we count only forwarded ones.
	client.Del(ctx, base.StreamKey("default"))

	n, err := r.ForwardRetry(ctx, "default", time.Now(), 100)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if n != 1 {
		t.Errorf("forwarded = %d, want 1", n)
	}

	// The due task is now in the stream; the future one remains in retry.
	slen, _ := client.XLen(ctx, base.StreamKey("default")).Result()
	if slen != 1 {
		t.Errorf("stream len = %d, want 1", slen)
	}
	if _, err := client.ZScore(ctx, base.RetryKey("default"), "due").Result(); err != redis.Nil {
		t.Errorf("due task should be removed from retry zset, err=%v", err)
	}
	if _, err := client.ZScore(ctx, base.RetryKey("default"), "future").Result(); err != nil {
		t.Errorf("future task should remain in retry zset: %v", err)
	}
}
