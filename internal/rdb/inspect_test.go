package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

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

// TaskState는 hash top-level "state" 필드(권위 상태)를 읽고, hash가 없으면
// redis.Nil을 그대로 돌려 caller가 "태스크 없음"을 구분하게 한다.
func TestTaskState_ReadsHashStateField(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.Enqueue(ctx, &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	st, err := r.TaskState(ctx, "default", "t1")
	if err != nil {
		t.Fatalf("task state: %v", err)
	}
	if st != base.StatePending {
		t.Errorf("state = %v, want %v", st, base.StatePending)
	}

	if _, err := r.TaskState(ctx, "default", "no-such-task"); err != redis.Nil {
		t.Errorf("missing task err = %v, want redis.Nil", err)
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
	if len(tasks) != 1 || tasks[0].Msg.ID != "s1" {
		t.Fatalf("tasks = %+v, want 1 with id s1", tasks)
	}
}

func TestListZSetTasks_ReturnsScores(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "s1", Kind: "k", Queue: "default", State: base.StateScheduled}
	encoded, _ := base.EncodeMessage(msg)
	if err := client.HSet(ctx, base.TaskKey("default", "s1"), "msg", encoded, "state", int(base.StateScheduled)).Err(); err != nil {
		t.Fatalf("hset: %v", err)
	}
	if err := client.ZAdd(ctx, base.ScheduledKey("default"), redis.Z{Score: 12345, Member: "s1"}).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}

	got, err := r.ListZSetTasks(ctx, "default", base.ScheduledKey("default"), 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Msg.ID != "s1" {
		t.Errorf("ID = %q, want s1", got[0].Msg.ID)
	}
	if got[0].Score != 12345 {
		t.Errorf("Score = %v, want 12345", got[0].Score)
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

// RunTask on a task that is not in any state ZSET (e.g. in-flight/active) must be
// a no-op: it must not add a duplicate stream entry (which would double-execute).
func TestRunTask_ActiveTaskIsNoOp(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// Enqueue + dequeue → task is active (in PEL), not in any ZSET. Its hash exists.
	if err := r.Enqueue(ctx, &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := r.Dequeue(ctx, "c1", 0, "default"); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	// Stream currently holds exactly the one delivered (active) entry.
	before, _ := client.XLen(ctx, base.StreamKey("default")).Result()

	if err := r.RunTask(ctx, "default", "t1"); err != nil {
		t.Fatalf("run: %v", err)
	}

	after, _ := client.XLen(ctx, base.StreamKey("default")).Result()
	if after != before {
		t.Errorf("stream len changed %d→%d; RunTask on an active task must not add a duplicate", before, after)
	}
}

func TestGroupMemberIDs_AndLeaderAndSchedules(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.CreateGroup(ctx, "cbq", "gm", []string{"gm:m0", "gm:m1"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	ids, err := r.GroupMemberIDs(ctx, "cbq", "gm")
	if err != nil || len(ids) != 2 {
		t.Fatalf("members = %v err=%v, want 2", ids, err)
	}

	leader, err := r.LeaderID(ctx)
	if err != nil || leader != "" {
		t.Fatalf("leader = %q err=%v, want empty", leader, err)
	}
	client.Set(ctx, base.LeaderKey(), "inst-1", 0)
	if leader, _ = r.LeaderID(ctx); leader != "inst-1" {
		t.Errorf("leader = %q, want inst-1", leader)
	}

	client.Set(ctx, base.ScheduleLastFiredKey("job-a"), 1700000000, 0)
	client.Set(ctx, base.ScheduleLastFiredKey("job-b"), 1700000100, 0)
	scheds, err := r.ScanSchedules(ctx)
	if err != nil || len(scheds) != 2 {
		t.Fatalf("schedules = %v err=%v, want 2", scheds, err)
	}
	found := map[string]int64{}
	for _, s := range scheds {
		found[s.ID] = s.LastFired
	}
	if found["job-a"] != 1700000000 || found["job-b"] != 1700000100 {
		t.Errorf("schedules wrong: %v", found)
	}
}
