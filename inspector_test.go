package chronos

import (
	"context"
	"testing"
	"time"

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
