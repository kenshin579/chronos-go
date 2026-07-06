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

func TestListZSetTasks_ReturnsMessages(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "s1", Kind: "k", Payload: []byte(`{"x":1}`), Queue: "default"}
	if err := r.Schedule(ctx, msg, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	tasks, err := r.ListZSetTasks(ctx, "default", base.ScheduledKey("default"), 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ID != "s1" {
		t.Fatalf("tasks = %+v, want 1 with id s1", tasks)
	}
}

func TestRunTask_MovesToStream(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// An archived task, run it now.
	msg := &base.TaskMessage{ID: "a1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, sid, _ := r.Dequeue(ctx, "c1", 0, "default")
	if err := r.Archive(ctx, "default", sid, msg, time.Now()); err != nil {
		t.Fatalf("archive: %v", err)
	}

	if err := r.RunTask(ctx, "default", "a1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	// Removed from archived, now in the stream.
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "a1").Result(); err == nil {
		t.Error("task should be removed from archived zset")
	}
	if n, _ := client.XLen(ctx, base.StreamKey("default")).Result(); n != 1 {
		t.Errorf("stream len = %d, want 1", n)
	}
}

func TestDeleteTask_RemovesEverywhere(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "s1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Schedule(ctx, msg, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if err := r.DeleteTask(ctx, "default", "s1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), "s1").Result(); err == nil {
		t.Error("task should be removed from scheduled zset")
	}
	if n, _ := client.Exists(ctx, base.TaskKey("default", "s1")).Result(); n != 0 {
		t.Error("task hash should be deleted")
	}
}
