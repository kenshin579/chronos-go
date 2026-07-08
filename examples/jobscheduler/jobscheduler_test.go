package jobscheduler

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	if err := c.Ping(context.Background()).Err(); err != nil {
		_ = c.Close()
		t.Skipf("redis not available: %v", err)
	}
	c.FlushDB(context.Background())
	t.Cleanup(func() { c.FlushDB(context.Background()); c.Close() })
	return c
}

func TestCompat_EnqueueAndTaskHandler(t *testing.T) {
	js := New(newRedis(t), []string{"q0", "q1"})
	got := make(chan []byte, 1)
	if err := js.RegisterTaskHandler("email", func(ctx context.Context, p []byte) error {
		got <- p
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := js.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer js.Shutdown()

	if err := js.Enqueue(context.Background(), "email", []byte(`{"to":"x"}`)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	select {
	case p := <-got:
		if string(p) != `{"to":"x"}` {
			t.Errorf("payload = %s", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestCompat_ScheduledJobRunsPeriodically(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	var runs atomic.Int32
	if _, err := js.RegisterScheduledJob("tick", time.Second, func(ctx context.Context) error {
		runs.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := js.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer js.Shutdown()

	// Generous window (leader election + first-tick baseline eat into the start),
	// so >= 2 fires is comfortable rather than borderline.
	time.Sleep(4500 * time.Millisecond)
	if n := runs.Load(); n < 2 {
		t.Errorf("scheduled runs = %d, want >= 2", n)
	}
}

func TestCompat_EnqueueUniqueDedup(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	var runs atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	if err := js.RegisterTaskHandler("dedup", func(ctx context.Context, p []byte) error {
		runs.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release // stay in-flight (unique lock held) until the test releases
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := js.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer js.Shutdown()

	ctx := context.Background()
	if err := js.Enqueue(ctx, "dedup", []byte("x"), WithUniqueTTL(time.Minute)); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	// Wait until the first task is actively processing (its unique lock is held)
	// before enqueueing the duplicate — otherwise the first could complete and
	// release the lock first, making the second a legitimate (non-duplicate)
	// enqueue and the test flaky.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first task did not start")
	}
	if err := js.Enqueue(ctx, "dedup", []byte("x"), WithUniqueTTL(time.Minute)); err != nil {
		t.Fatalf("enqueue2 (dup) should return nil: %v", err)
	}
	close(release)
	time.Sleep(500 * time.Millisecond) // let any wrongly-enqueued duplicate run
	if n := runs.Load(); n != 1 {
		t.Errorf("runs = %d, want 1 (dedup)", n)
	}
}

func TestCompat_RegisterScheduledJob_RejectsSubSecond(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	if _, err := js.RegisterScheduledJob("x", 500*time.Millisecond, func(context.Context) error { return nil }); err == nil {
		t.Error("interval < 1s must be rejected")
	}
}

func TestCompat_DuplicateTaskNameRejected(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	if err := js.RegisterTaskHandler("dup", func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := js.RegisterTaskHandler("dup", func(context.Context, []byte) error { return nil }); err == nil {
		t.Error("duplicate task name must be rejected")
	}
}

func TestCompat_CronJob_AcceptsAndRejects(t *testing.T) {
	js := New(newRedis(t), []string{"q0"})
	if _, err := js.RegisterCronJob("daily", "0 0 * * *", func(context.Context) error { return nil }); err != nil {
		t.Errorf("valid cron spec should be accepted: %v", err)
	}
	if _, err := js.RegisterCronJob("bad", "not a cron", func(context.Context) error { return nil }); err == nil {
		t.Error("invalid cron spec must be rejected")
	}
}
