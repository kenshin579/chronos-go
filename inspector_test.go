package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

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

func TestInspector_ChainStepperFields(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := NewChain().
		Then(emailArgs{UserID: "s1"}, WithProcessIn(time.Hour)).
		Then(emailArgs{UserID: "s2"}).
		Then(emailArgs{UserID: "s3"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	got, err := insp.GetTask(ctx, "default", info.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ChainIndex != 0 {
		t.Errorf("ChainIndex = %d, want 0", got.ChainIndex)
	}
	if len(got.ChainNext) != 2 || got.ChainNext[0] != "email:send" {
		t.Errorf("ChainNext = %v, want 2 kinds", got.ChainNext)
	}
}

func TestInspector_GroupMembersAndSchedulerStatus(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()
	insp := NewInspector(client)

	st, err := insp.SchedulerStatus(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.LeaderID != "" || len(st.Schedules) != 0 {
		t.Errorf("empty status wrong: %+v", st)
	}

	c := NewClient(client)
	defer c.Close()
	ginfo, err := NewGroup().
		Add(emailArgs{UserID: "m0"}, WithProcessIn(time.Hour)).
		Add(emailArgs{UserID: "m1"}, WithProcessIn(time.Hour)).
		OnComplete(emailArgs{UserID: "cb"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("group: %v", err)
	}
	members, err := insp.GroupMembers(ctx, "default", ginfo.GroupID)
	if err != nil || len(members) != 2 {
		t.Fatalf("members = %v err=%v, want 2", members, err)
	}
	// 멤버 TaskInfo가 GroupQueue를 노출해 UI가 GroupMembers를 호출 가능해야 한다.
	ti, err := insp.GetTask(ctx, "default", ginfo.MemberIDs[0])
	if err != nil || ti.GroupQueue != "default" {
		t.Errorf("GroupQueue = %q err=%v, want default", ti.GroupQueue, err)
	}
}

func TestGetTask_ExposesResultPresence(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()
	// 결과가 있는 완료(retained) 태스크를 직접 심는다.
	msg := &base.TaskMessage{ID: "r1", Kind: "k", Queue: "iq",
		State: base.StateCompleted, Result: []byte(`{"v":1}`)}
	encoded, _ := base.EncodeMessage(msg)
	client.HSet(ctx, base.TaskKey("iq", "r1"), "msg", encoded, "state", int(base.StateCompleted))
	client.ZAdd(ctx, base.CompletedKey("iq"), redis.Z{Score: float64(time.Now().Add(time.Hour).Unix()), Member: "r1"})

	insp := NewInspector(client)
	info, err := insp.GetTask(ctx, "iq", "r1")
	if err != nil {
		t.Fatal(err)
	}
	if !info.HasResult || info.ResultSize != len(`{"v":1}`) {
		t.Errorf("HasResult=%v ResultSize=%d", info.HasResult, info.ResultSize)
	}
}

func TestInspector_PauseResumeAndPausedFlag(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	insp := NewInspector(client)

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "x"}, WithProcessIn(time.Hour)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := insp.PauseQueue(ctx, "default"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	qs, err := insp.Queues(ctx)
	if err != nil || len(qs) == 0 {
		t.Fatalf("queues: %v", err)
	}
	if !qs[0].Paused {
		t.Error("QueueInfo.Paused = false, want true")
	}
	paused, err := insp.PausedQueues(ctx)
	if err != nil || len(paused) != 1 {
		t.Fatalf("paused = %v err=%v", paused, err)
	}
	if err := insp.ResumeQueue(ctx, "default"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	qs, _ = insp.Queues(ctx)
	if qs[0].Paused {
		t.Error("still paused after resume")
	}
}

func TestTaskInfo_ChainNextShowsGroupStage(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()
	msg := &base.TaskMessage{ID: "c9:0", Kind: "wf:prep", Queue: "iq2",
		State: base.StatePending, ChainID: "c9",
		Chain: []base.ChainLink{
			{Kind: "wf:merge", Queue: "iq2", Group: []base.GroupMemberLink{
				{Kind: "wf:enc", Queue: "iq2"}, {Kind: "wf:enc", Queue: "iq2"},
			}},
			{Kind: "wf:deploy", Queue: "iq2"},
		}}
	encoded, _ := base.EncodeMessage(msg)
	client.HSet(ctx, base.TaskKey("iq2", "c9:0"), "msg", encoded, "state", int(base.StatePending))
	client.XAdd(ctx, &redis.XAddArgs{Stream: base.StreamKey("iq2"), Values: map[string]any{"task_id": "c9:0"}})

	insp := NewInspector(client)
	info, err := insp.GetTask(ctx, "iq2", "c9:0")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.ChainNext) != 2 || info.ChainNext[0] != "group[2]→wf:merge" || info.ChainNext[1] != "wf:deploy" {
		t.Errorf("ChainNext = %v", info.ChainNext)
	}
	if info.ChainPending != 2 {
		t.Errorf("ChainPending = %d", info.ChainPending)
	}
}
