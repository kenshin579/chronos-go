package chronos

// Cluster integration tests: run every Lua script and command pattern at least
// once against a real Redis Cluster (script-complete smoke). The cluster-
// specific failure modes these catch are CROSSSLOT (multi-key ops spanning
// slots), MOVED/ASK redirects, cross-node pub/sub, and per-node script caches.
//
// Requires REDIS_CLUSTER_ADDRS (see deploy/redis-cluster); skipped otherwise.
//
// Script/command checklist (each must be exercised by at least one test):
//  [x] enqueueCmd + Dequeue(XREADGROUP) + Done(XACK+XDEL)   → TestCluster_EnqueueProcessAck
//  [x] moveToZSetCmd(retry) + forwardCmd(retry)             → TestCluster_RetryThenSucceed
//  [x] moveToZSetCmd(archive) + OnDeadLetter                → TestCluster_DeadLetter
//  [x] scheduleCmd + forwardCmd(scheduled)                  → TestCluster_DelayedTask
//  [x] uniqueEnqueueCmd / uniqueScheduleCmd                 → TestCluster_UniqueDedup
//  [ ] periodicCmd + leader acquire/renew/resign + pub/sub  → TestCluster_SchedulerLeaderFailover (Task 5)
//  [ ] recover(XAUTOCLAIM)                                  → TestCluster_RecoverAbandonedTask (Task 5)
//  [ ] heartbeat(XCLAIM JUSTID + PEXPIRE)                   → TestCluster_HeartbeatLongTask (Task 5)
//  [ ] janitor(TrimArchived)                                → TestCluster_JanitorTrimsArchived (Task 5)
//  [ ] runTaskCmd / deleteTask (Inspector)                  → TestCluster_InspectorRunAndDelete (Task 6)
//  [ ] QueueStats/Queues/ListZSetTasks/GetTask              → TestCluster_InspectorQueries (Task 6)
//  [ ] two queues on different slots (MOVED redirects)      → TestCluster_TwoQueuesDifferentSlots (Task 6)

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

// clArgs is the task type for cluster tests (own kind to avoid clashing with
// other tests' handlers).
type clArgs struct {
	N int `json:"n"`
}

func (clArgs) Kind() string { return "cluster:demo" }

// clusterServerConfig returns a ServerConfig tuned for fast tests.
func clusterServerConfig(queue string) ServerConfig {
	return ServerConfig{
		Queues:          map[string]int{queue: 1},
		Concurrency:     4,
		ForwardInterval: 200 * time.Millisecond,
		RetryDelayFunc:  func(int, error) time.Duration { return 300 * time.Millisecond },
	}
}

// waitFor polls cond every 50ms until it returns true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestCluster_EnqueueProcessAck(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var done atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		done.Add(1)
		return nil
	})
	srv := NewServer(client, clusterServerConfig("default"))
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, clArgs{N: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 5*time.Second, "task processed", func() bool { return done.Load() == 1 })

	// Done = XACK+XDEL: the stream must be empty again.
	insp := NewInspector(client)
	waitFor(t, 5*time.Second, "stream drained", func() bool {
		qs, err := insp.Queues(ctx)
		if err != nil || len(qs) == 0 {
			return false
		}
		for _, q := range qs {
			if q.Queue == "default" {
				return q.Pending == 0 && q.Active == 0
			}
		}
		return false
	})
}

func TestCluster_RetryThenSucceed(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var attempts atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	srv := NewServer(client, clusterServerConfig("default"))
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, clArgs{N: 2}, WithMaxRetry(3)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 10*time.Second, "retry then success", func() bool { return attempts.Load() >= 2 })
}

func TestCluster_DeadLetter(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var hooked atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		return errors.New("permanent")
	})
	cfg := clusterServerConfig("default")
	cfg.OnDeadLetter = func(ctx context.Context, info *TaskInfo, err error) { hooked.Add(1) }
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, clArgs{N: 3}, WithMaxRetry(0))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	waitFor(t, 10*time.Second, "task archived", func() bool {
		got, gerr := insp.GetTask(ctx, "default", info.ID)
		return gerr == nil && got.State == "archived" && got.LastErr == "permanent"
	})
	if hooked.Load() == 0 {
		t.Error("OnDeadLetter hook did not fire")
	}
}

func TestCluster_DelayedTask(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var done atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		done.Add(1)
		return nil
	})
	srv := NewServer(client, clusterServerConfig("default"))
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, clArgs{N: 4}, WithProcessIn(800*time.Millisecond)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if done.Load() != 0 {
		t.Error("delayed task ran immediately")
	}
	waitFor(t, 10*time.Second, "delayed task promoted and run", func() bool { return done.Load() == 1 })
}

func TestCluster_UniqueDedup(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// uniqueEnqueueCmd: second identical enqueue is rejected.
	if _, err := Enqueue(ctx, c, clArgs{N: 5}, WithUnique(30*time.Second)); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	if _, err := Enqueue(ctx, c, clArgs{N: 5}, WithUnique(30*time.Second)); !errors.Is(err, ErrDuplicateTask) {
		t.Errorf("enqueue2: err = %v, want ErrDuplicateTask", err)
	}
	// uniqueScheduleCmd: same for a delayed unique task (different payload → own lock).
	if _, err := Enqueue(ctx, c, clArgs{N: 6}, WithUnique(30*time.Second), WithProcessIn(time.Hour)); err != nil {
		t.Fatalf("schedule1: %v", err)
	}
	if _, err := Enqueue(ctx, c, clArgs{N: 6}, WithUnique(30*time.Second), WithProcessIn(time.Hour)); !errors.Is(err, ErrDuplicateTask) {
		t.Errorf("schedule2: err = %v, want ErrDuplicateTask", err)
	}
}
