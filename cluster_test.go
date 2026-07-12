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
//  [x] periodicCmd + leader acquire/renew/resign + pub/sub  → TestCluster_SchedulerLeaderFailover
//  [x] recover(XAUTOCLAIM)                                  → TestCluster_RecoverAbandonedTask
//  [x] heartbeat(XCLAIM JUSTID + PEXPIRE)                   → TestCluster_HeartbeatLongTask
//  [x] janitor(TrimArchived)                                → TestCluster_JanitorTrimsArchived
//  [x] runTaskCmd / deleteTask (Inspector)                  → TestCluster_InspectorRunAndDelete
//  [x] QueueStats/Queues/ListZSetTasks/GetTask              → TestCluster_InspectorQueries
//  [x] two queues on different slots (MOVED redirects)      → TestCluster_TwoQueuesDifferentSlots
//  [x] Done retention (moveToZSetCmd) + TrimCompleted       → TestCluster_CompletedRetention
//  [x] chainEnqueueCmd/chainScheduleCmd (chain successor)   → TestCluster_ChainCompletes
//  [x] CreateGroup + groupCompleteCmd (group fan-out)       → TestCluster_GroupFanOut
//  [x] requeueCmd (shutdown batch return)                   → TestCluster_RequeueReturnsTask

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/rdb"
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

func TestCluster_SchedulerLeaderFailover(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	ctx := context.Background()

	// Worker that records processed trigger task IDs (deterministic dedup IDs).
	var (
		mu   sync.Mutex
		seen = map[string]int{}
	)
	record := func(id string) {
		mu.Lock()
		seen[id]++
		mu.Unlock()
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(seen)
	}
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		record(task.ID())
		return nil
	})
	srv := NewServer(client, clusterServerConfig("default"))
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	mkSched := func() *Scheduler {
		s := NewScheduler(client, SchedulerConfig{LeaderTTL: time.Second})
		if err := RegisterInterval(s, time.Second, clArgs{N: 7}); err != nil {
			t.Fatalf("register: %v", err)
		}
		if err := s.Start(ctx); err != nil {
			t.Fatalf("scheduler start: %v", err)
		}
		return s
	}
	schedA := mkSched()
	schedB := mkSched() // follower

	// Leader fires: distinct triggers accumulate.
	waitFor(t, 15*time.Second, "2+ distinct triggers", func() bool { return count() >= 2 })

	// Graceful shutdown of A publishes resign (cross-node pub/sub) → B takes over.
	before := count()
	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = schedA.Shutdown(shutCtx)
	cancel()
	waitFor(t, 15*time.Second, "progress after failover", func() bool { return count() > before })

	shutCtx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	_ = schedB.Shutdown(shutCtx2)
	cancel2()

	mu.Lock()
	defer mu.Unlock()
	for id, n := range seen {
		if n > 1 {
			t.Errorf("trigger %s ran %d times, want 1 (dedup)", id, n)
		}
	}
}

func TestCluster_RecoverAbandonedTask(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	r := rdb.NewRDB(client)

	// Enqueue, then dequeue with a consumer that "crashes" (never acks).
	info, err := Enqueue(ctx, c, clArgs{N: 8}, WithQueue("recq"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := r.EnsureGroup(ctx, "recq"); err != nil {
		t.Fatalf("group: %v", err)
	}
	msg, _, err := r.Dequeue(ctx, "dead-consumer", -1, "recq")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if msg.ID != info.ID {
		t.Fatalf("dequeued %s, want %s", msg.ID, info.ID)
	}

	// Recover with minIdle 0 reclaims it (XAUTOCLAIM) and requeues or archives.
	waitFor(t, 5*time.Second, "task recovered", func() bool {
		requeued, archived, rerr := r.Recover(ctx, "recq", "recoverer", 0, 100)
		return rerr == nil && (requeued > 0 || len(archived) > 0)
	})
}

func TestCluster_HeartbeatLongTask(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		runs.Add(1)
		time.Sleep(4 * time.Second) // longer than RecoverMinIdle
		return nil
	})
	cfg := clusterServerConfig("hbq")
	cfg.RecoverMinIdle = 1500 * time.Millisecond
	cfg.RecoverInterval = 300 * time.Millisecond
	cfg.HeartbeatInterval = 200 * time.Millisecond
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, clArgs{N: 9}, WithQueue("hbq")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	time.Sleep(5500 * time.Millisecond) // 4s handler + recoverer chances afterwards
	if n := runs.Load(); n != 1 {
		t.Errorf("runs = %d, want 1 (heartbeat must keep the lease fresh)", n)
	}
}

func TestCluster_JanitorTrimsArchived(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		return errors.New("always fails")
	})
	cfg := clusterServerConfig("janq")
	cfg.ArchivedRetention = 1 * time.Second
	cfg.JanitorInterval = 300 * time.Millisecond
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	for i := 0; i < 3; i++ {
		if _, err := Enqueue(ctx, c, clArgs{N: 100 + i}, WithQueue("janq"), WithMaxRetry(0)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	insp := NewInspector(client)
	archivedCount := func() int64 {
		qs, err := insp.Queues(ctx)
		if err != nil {
			return -1
		}
		for _, q := range qs {
			if q.Queue == "janq" {
				return q.Archived
			}
		}
		return 0
	}
	// >=1 proves the archive path ran; ==0 afterwards proves TrimArchived ran.
	// (With a 1s retention a fast janitor may trim early tasks before the last
	// one lands, so ==3 would be a momentary condition and flaky.)
	waitFor(t, 10*time.Second, "tasks archived", func() bool { return archivedCount() >= 1 })
	waitFor(t, 10*time.Second, "janitor trimmed archived", func() bool { return archivedCount() == 0 })
}

func TestCluster_InspectorRunAndDelete(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	insp := NewInspector(client)

	// runTaskCmd: promote a far-future scheduled task to the stream.
	runInfo, err := Enqueue(ctx, c, clArgs{N: 10}, WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue run-target: %v", err)
	}
	if err := insp.RunTask(ctx, "default", runInfo.ID); err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	tasks, err := insp.ListTasks(ctx, "default", "scheduled", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, ti := range tasks {
		if ti.ID == runInfo.ID {
			t.Error("task still in scheduled after RunTask")
		}
	}

	// deleteTask: remove a scheduled task entirely.
	delInfo, err := Enqueue(ctx, c, clArgs{N: 11}, WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue delete-target: %v", err)
	}
	if err := insp.DeleteTask(ctx, "default", delInfo.ID); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
	if _, err := insp.GetTask(ctx, "default", delInfo.ID); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("GetTask after delete: err = %v, want ErrTaskNotFound", err)
	}
}

func TestCluster_InspectorQueries(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	if _, err := Enqueue(ctx, c, clArgs{N: 12}, WithProcessIn(time.Hour)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)

	qs, err := insp.Queues(ctx) // Queues (SMembers) + QueueStats per queue
	if err != nil {
		t.Fatalf("queues: %v", err)
	}
	var found bool
	for _, q := range qs {
		if q.Queue == "default" && q.Scheduled == 1 {
			found = true
		}
	}
	if !found {
		t.Errorf("queue stats wrong: %+v", qs)
	}

	tasks, err := insp.ListTasks(ctx, "default", "scheduled", 10) // ListZSetTasks
	if err != nil || len(tasks) != 1 {
		t.Fatalf("list: n=%d err=%v", len(tasks), err)
	}
	ti := tasks[0]
	if ti.Kind != "cluster:demo" || ti.State != "scheduled" || ti.NextProcessAt.IsZero() {
		t.Errorf("task fields wrong: %+v", ti)
	}
	got, err := insp.GetTask(ctx, "default", ti.ID) // GetTask + ZScore
	if err != nil || got.ID != ti.ID {
		t.Errorf("GetTask: got=%+v err=%v", got, err)
	}
}

func TestCluster_TwoQueuesDifferentSlots(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// Prove the two queues really live on different slots — otherwise this
	// test would silently stop covering MOVED redirects if names change.
	const q1, q2 = "alpha", "bravo"
	slot1, err := client.ClusterKeySlot(ctx, "chronos:{"+q1+"}:stream").Result()
	if err != nil {
		t.Fatalf("keyslot: %v", err)
	}
	slot2, err := client.ClusterKeySlot(ctx, "chronos:{"+q2+"}:stream").Result()
	if err != nil {
		t.Fatalf("keyslot: %v", err)
	}
	if slot1 == slot2 {
		t.Fatalf("queues %q and %q hash to the same slot (%d); pick different names", q1, q2, slot1)
	}

	var n1, n2 atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		if task.Args.N == 1 {
			n1.Add(1)
		} else {
			n2.Add(1)
		}
		return nil
	})
	cfg := ServerConfig{
		Queues:          map[string]int{q1: 1, q2: 1},
		Concurrency:     4,
		ForwardInterval: 200 * time.Millisecond,
	}
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	for i := 0; i < 3; i++ {
		if _, err := Enqueue(ctx, c, clArgs{N: 1}, WithQueue(q1)); err != nil {
			t.Fatalf("enqueue q1: %v", err)
		}
		if _, err := Enqueue(ctx, c, clArgs{N: 2}, WithQueue(q2)); err != nil {
			t.Fatalf("enqueue q2: %v", err)
		}
	}
	waitFor(t, 10*time.Second, "both slots' queues fully processed", func() bool {
		return n1.Load() == 3 && n2.Load() == 3
	})
}

func TestCluster_CompletedRetention(t *testing.T) {
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
	cfg := clusterServerConfig("retq")
	cfg.JanitorInterval = 300 * time.Millisecond
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	// Done retention (moveToZSetCmd): 성공 후 completed에 보관된다.
	info, err := Enqueue(ctx, c, clArgs{N: 13}, WithQueue("retq"), WithRetention(time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	waitFor(t, 5*time.Second, "task retained as completed", func() bool {
		got, gerr := insp.GetTask(ctx, "retq", info.ID)
		return gerr == nil && got.State == "completed"
	})
	// TrimCompleted: retention(1s) 경과 후 janitor가 정리한다.
	waitFor(t, 10*time.Second, "completed task trimmed", func() bool {
		_, gerr := insp.GetTask(ctx, "retq", info.ID)
		return gerr != nil
	})
}

func TestCluster_ChainCompletes(t *testing.T) {
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
	// 서로 다른 슬롯의 큐 두 개에 걸친 체인 — 후속 enqueue가 슬롯을 넘나든다.
	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"alpha": 1, "bravo": 1},
		Concurrency:     2,
		ForwardInterval: 200 * time.Millisecond,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := NewChain().
		Then(clArgs{N: 21}, WithQueue("alpha")).
		Then(clArgs{N: 22}, WithQueue("bravo")).
		Then(clArgs{N: 23}, WithQueue("alpha")).
		Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 10*time.Second, "3-link chain completes across slots", func() bool {
		return done.Load() == 3
	})
}

func TestCluster_GroupFanOut(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var members, callbacks atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		if task.Args.N == 99 {
			callbacks.Add(1)
		} else {
			members.Add(1)
		}
		return nil
	})
	// 멤버는 서로 다른 슬롯 큐 2개, 콜백은 제3의 큐 — 그룹 SET/콜백이 콜백 큐
	// 슬롯에서 원자 처리되고, 멤버 보고가 슬롯을 넘나드는 구성을 검증.
	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"alpha": 1, "bravo": 1, "cbq": 1},
		Concurrency:     4,
		ForwardInterval: 200 * time.Millisecond,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := NewGroup().
		Add(clArgs{N: 31}, WithQueue("alpha")).
		Add(clArgs{N: 32}, WithQueue("bravo")).
		Add(clArgs{N: 33}, WithQueue("alpha")).
		OnComplete(clArgs{N: 99}, WithQueue("cbq")).
		Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 10*time.Second, "3 members then callback", func() bool {
		return members.Load() == 3 && callbacks.Load() == 1
	})
}

func TestCluster_RequeueReturnsTask(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	r := rdb.NewRDB(client)

	info, err := Enqueue(ctx, c, clArgs{N: 41}, WithQueue("alpha"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := r.EnsureGroup(ctx, "alpha"); err != nil {
		t.Fatalf("group: %v", err)
	}
	msg, sid, err := r.Dequeue(ctx, "w", -1, "alpha")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if msg.ID != info.ID {
		t.Fatalf("got %s", msg.ID)
	}
	if err := r.Requeue(ctx, "alpha", sid, msg); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	again, _, err := r.Dequeue(ctx, "w2", -1, "alpha")
	if err != nil || again.ID != info.ID {
		t.Fatalf("re-dequeue: %v %v", again, err)
	}
}
