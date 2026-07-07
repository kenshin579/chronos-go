package chronos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type tickArgs struct {
	N int `json:"n"`
}

func (tickArgs) Kind() string { return "sched:tick" }

func TestRegisterInterval_RejectsSubSecond(t *testing.T) {
	client := testutil.NewRedis(t)
	s := NewScheduler(client, SchedulerConfig{})
	if err := RegisterInterval(s, 500*time.Millisecond, tickArgs{}); err == nil {
		t.Error("interval < 1s must be rejected")
	}
	if err := RegisterInterval(s, 1*time.Second, tickArgs{}); err != nil {
		t.Errorf("1s interval should be accepted: %v", err)
	}
}

func TestRegisterCron_RejectsBadSpec(t *testing.T) {
	client := testutil.NewRedis(t)
	s := NewScheduler(client, SchedulerConfig{})
	if err := RegisterCron(s, "not a cron", tickArgs{}); err == nil {
		t.Error("bad cron spec must be rejected")
	}
}

// Two schedulers on the same Redis, both registering the same 1s interval job,
// must together enqueue each trigger only once (leader-only + deterministic dedup).
func TestScheduler_SingleExecutionAcrossInstances(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	// A consuming client + server counts how many tasks actually run.
	c := NewClient(client)
	defer c.Close()
	var runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[tickArgs]) error {
		runs.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	newSched := func() *Scheduler {
		s := NewScheduler(client, SchedulerConfig{LeaderTTL: 500 * time.Millisecond})
		if err := RegisterInterval(s, 1*time.Second, tickArgs{N: 1}); err != nil {
			t.Fatalf("register: %v", err)
		}
		return s
	}
	s1, s2 := newSched(), newSched()
	if err := s1.Start(ctx); err != nil {
		t.Fatalf("s1 start: %v", err)
	}
	if err := s2.Start(ctx); err != nil {
		t.Fatalf("s2 start: %v", err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { s1.Shutdown(context.Background()); s2.Shutdown(context.Background()) }) }
	defer stop()

	// Over ~3.5s a 1s job should fire ~3 times — and crucially the same number
	// whether one or two schedulers are running (no duplication).
	time.Sleep(3500 * time.Millisecond)
	stop()
	time.Sleep(300 * time.Millisecond) // let in-flight finish

	got := runs.Load()
	if got < 2 || got > 5 {
		t.Errorf("runs = %d, want ~3 (2 schedulers must not double-enqueue)", got)
	}
}
