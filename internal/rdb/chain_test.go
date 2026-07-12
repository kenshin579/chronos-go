package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestEnqueueChainLink_CreatesAndNoOpsWhenExists(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "ch:1", Kind: "k", Queue: "default", ChainID: "ch", ChainIndex: 1}

	// 1) 최초 호출: 생성된다.
	enqueued, err := r.EnqueueChainLink(ctx, msg, 0)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if !enqueued {
		t.Fatal("first call: enqueued = false, want true")
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); xlen != 1 {
		t.Errorf("stream len = %d, want 1", xlen)
	}

	// 2) 재전달로 인한 두 번째 호출: no-op이어야 한다.
	enqueued, err = r.EnqueueChainLink(ctx, msg, 0)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if enqueued {
		t.Error("second call: enqueued = true, want false (no-op)")
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); xlen != 1 {
		t.Errorf("stream len after no-op = %d, want 1 (no duplicate)", xlen)
	}
}

func TestEnqueueChainLink_DelayGoesToScheduled(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "ch:2", Kind: "k", Queue: "default", ChainID: "ch", ChainIndex: 2}
	enqueued, err := r.EnqueueChainLink(ctx, msg, 2*time.Second)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !enqueued {
		t.Fatal("enqueued = false, want true")
	}
	score, err := client.ZScore(ctx, base.ScheduledKey("default"), "ch:2").Result()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	want := float64(time.Now().Add(2 * time.Second).Unix())
	if score < want-5 || score > want+5 {
		t.Errorf("score = %v, want ~%v", score, want)
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); xlen != 0 {
		t.Errorf("stream should be empty for delayed link, len=%d", xlen)
	}
	if enq, _ := r.EnqueueChainLink(ctx, msg, 2*time.Second); enq {
		t.Error("second delayed call: want no-op")
	}
}
