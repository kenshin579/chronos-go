package chronos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
	"github.com/redis/go-redis/v9"
	cron "github.com/robfig/cron/v3"
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

type reportArgs struct {
	Team string `json:"team"`
}

func (reportArgs) Kind() string { return "sched:report" }

// Two jobs with the same kind and spec but different payloads must get distinct
// schedule IDs; otherwise their dedup/task/lastFired keys collide and one would
// be silently dropped.
func TestRegister_DistinctIDsForDifferentPayloads(t *testing.T) {
	client := testutil.NewRedis(t)
	s := NewScheduler(client, SchedulerConfig{})
	if err := RegisterInterval(s, time.Minute, reportArgs{Team: "A"}); err != nil {
		t.Fatalf("register A: %v", err)
	}
	if err := RegisterInterval(s, time.Minute, reportArgs{Team: "B"}); err != nil {
		t.Fatalf("register B: %v", err)
	}
	if len(s.entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(s.entries))
	}
	if s.entries[0].id == s.entries[1].id {
		t.Errorf("same-kind+spec jobs with different payloads share id %q (would collide)", s.entries[0].id)
	}
}

func newOfflineScheduler(loc *time.Location) *Scheduler {
	// NewScheduler never dials; RegisterCron does no Redis I/O. A bare client keeps
	// these schedule-parsing tests hermetic (no live Redis, no t.Skip).
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	return NewScheduler(client, SchedulerConfig{Location: loc})
}

func TestRegisterCron_HonorsSchedulerLocation(t *testing.T) {
	seoul, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatalf("load Asia/Seoul: %v", err)
	}
	s := newOfflineScheduler(seoul)
	if err := RegisterCron(s, "0 5 * * *", tickArgs{}); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}
	ss, ok := s.entries[0].schedule.(*cron.SpecSchedule)
	if !ok {
		t.Fatalf("schedule type = %T, want *cron.SpecSchedule", s.entries[0].schedule)
	}
	if ss.Location != seoul {
		t.Errorf("schedule Location = %v, want Asia/Seoul", ss.Location)
	}
	// Behavior: 0 5 * * * must fire at 05:00 KST (= 20:00 UTC), not 05:00 UTC.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next := s.entries[0].schedule.Next(base)
	if h := next.In(seoul).Hour(); h != 5 {
		t.Errorf("next fire hour in KST = %d, want 5", h)
	}
	if h := next.UTC().Hour(); h != 20 {
		t.Errorf("next fire hour in UTC = %d, want 20 (05:00 KST)", h)
	}
}

func TestRegisterCron_PreservesExplicitCronTZ(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load America/New_York: %v", err)
	}
	s := newOfflineScheduler(seoul)
	if err := RegisterCron(s, "CRON_TZ=America/New_York 0 5 * * *", tickArgs{}); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}
	ss := s.entries[0].schedule.(*cron.SpecSchedule)
	if ss.Location.String() != ny.String() {
		t.Errorf("explicit CRON_TZ overridden: Location = %v, want America/New_York", ss.Location)
	}
}

func TestRegisterCron_DefaultLocationUnchanged(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	s := NewScheduler(client, SchedulerConfig{})
	if err := RegisterCron(s, "0 5 * * *", tickArgs{}); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}
	ss := s.entries[0].schedule.(*cron.SpecSchedule)
	if ss.Location != time.Local {
		t.Errorf("default Location = %v, want time.Local", ss.Location)
	}
}
