package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestInspector_QueuesAndListAndRun(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	insp := NewInspector(client)
	ctx := context.Background()

	// One archived task via a failing server run would be complex; enqueue a
	// scheduled task and inspect it directly.
	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	queues, err := insp.Queues(ctx)
	if err != nil {
		t.Fatalf("queues: %v", err)
	}
	if len(queues) != 1 || queues[0].Queue != "default" || queues[0].Scheduled != 1 {
		t.Fatalf("queues = %+v, want 1 default with scheduled=1", queues)
	}

	tasks, err := insp.ListTasks(ctx, "default", "scheduled", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ID != info.ID {
		t.Fatalf("tasks = %+v, want the scheduled task", tasks)
	}

	// Run it now → moves to stream.
	if err := insp.RunTask(ctx, "default", info.ID); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n, _ := client.XLen(ctx, "chronos:{default}:stream").Result(); n != 1 {
		t.Errorf("stream len = %d, want 1", n)
	}
}

func TestInspector_ListTasks_RejectsUnknownState(t *testing.T) {
	client := testutil.NewRedis(t)
	insp := NewInspector(client)
	if _, err := insp.ListTasks(context.Background(), "default", "bogus", 10); err == nil {
		t.Error("expected error for unknown state")
	}
}

func TestInspector_ListTasks_RichFields(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(time.Hour)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	got, err := insp.ListTasks(ctx, "default", "scheduled", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	ti := got[0]
	if ti.State != "scheduled" {
		t.Errorf("State = %q, want scheduled", ti.State)
	}
	if len(ti.Payload) == 0 {
		t.Error("Payload empty, want non-empty")
	}
	if ti.NextProcessAt.IsZero() {
		t.Error("NextProcessAt is zero, want the scheduled time")
	}
}

func TestInspector_GetTask_ReturnsDetailAndNotFound(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u9"}, WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)

	got, err := insp.GetTask(ctx, "default", info.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != info.ID {
		t.Errorf("ID = %q, want %q", got.ID, info.ID)
	}
	if got.State != "scheduled" || got.NextProcessAt.IsZero() {
		t.Errorf("state/time wrong: %+v", got)
	}

	if _, err := insp.GetTask(ctx, "default", "does-not-exist"); err == nil {
		t.Error("GetTask for missing id: want error, got nil")
	}
}

func TestInspector_GetTask_NotFoundIsSentinel(t *testing.T) {
	client := testutil.NewRedis(t)
	insp := NewInspector(client)
	_, err := insp.GetTask(context.Background(), "default", "nope")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("err = %v, want ErrTaskNotFound", err)
	}
}

func TestInspector_CompletedCountAndActions(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		runs.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "cc"}, WithRetention(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	waitCompleted := func(id string) {
		deadline := time.Now().Add(5 * time.Second)
		for {
			got, gerr := insp.GetTask(ctx, "default", id)
			if gerr == nil && got.State == "completed" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("task %s not completed in time", id)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitCompleted(info.ID)

	qs, err := insp.Queues(ctx)
	if err != nil {
		t.Fatalf("queues: %v", err)
	}
	var completed int64 = -1
	for _, q := range qs {
		if q.Queue == "default" {
			completed = q.Completed
		}
	}
	if completed != 1 {
		t.Errorf("Completed count = %d, want 1", completed)
	}

	tasks, err := insp.ListTasks(ctx, "default", "completed", 10)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("list completed: n=%d err=%v", len(tasks), err)
	}
	if tasks[0].CompletedAt.IsZero() {
		t.Error("ListTasks completed: CompletedAt zero")
	}

	// RunTask: completed 태스크 재실행 → 핸들러 2회.
	if err := insp.RunTask(ctx, "default", info.ID); err != nil {
		t.Fatalf("run: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runs.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("re-run did not execute (runs=%d)", runs.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	// 재완료 확인: RunTask가 completed ZSET에서 ZREM했으므로, 재등장이 곧 재완료다.
	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, zerr := client.ZScore(ctx, base.CompletedKey("default"), info.ID).Result(); zerr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task did not re-complete into the completed zset")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// DeleteTask: 보관분 조기 삭제.
	if err := insp.DeleteTask(ctx, "default", info.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := insp.GetTask(ctx, "default", info.ID); err == nil {
		t.Error("task still present after delete")
	}
}
