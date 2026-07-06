package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestQueueStats_CountsByState(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// 1 pending (enqueued, not dequeued)
	if err := r.Enqueue(ctx, &base.TaskMessage{ID: "p1", Kind: "k", Payload: []byte("{}"), Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// 1 active (dequeued, not acked)
	if err := r.Enqueue(ctx, &base.TaskMessage{ID: "a1", Kind: "k", Payload: []byte("{}"), Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := r.Dequeue(ctx, "c1", 0, "default"); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	// 1 scheduled
	if err := r.Schedule(ctx, &base.TaskMessage{ID: "s1", Kind: "k", Payload: []byte("{}"), Queue: "default"}, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	st, err := r.QueueStats(ctx, "default")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Pending != 1 || st.Active != 1 || st.Scheduled != 1 {
		t.Errorf("stats = %+v, want pending1/active1/scheduled1", st)
	}
}
