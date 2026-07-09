package chronos

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_ProcessesEnqueuedTask(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var (
		mu      sync.Mutex
		gotUser string
		done    = make(chan struct{})
	)
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		mu.Lock()
		gotUser = task.Args.UserID
		mu.Unlock()
		close(done)
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 4,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u99"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not invoked within 5s")
	}

	mu.Lock()
	defer mu.Unlock()
	if gotUser != "u99" {
		t.Errorf("handler received user %q, want u99", gotUser)
	}
}

// Regression: a task enqueued BEFORE the first server ever starts (so before
// the consumer group exists) must still be delivered. With the group created
// at "$" instead of "0", such tasks were invisible to XREADGROUP forever.
func TestServer_ProcessesTaskEnqueuedBeforeStart(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// Enqueue first — no server has run, so the stream exists but no group.
	if _, err := Enqueue(ctx, c, emailArgs{UserID: "early"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(done)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("task enqueued before server start was never processed")
	}
}

func TestServer_ShutdownIsClean(t *testing.T) {
	client := testutil.NewRedis(t)
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 2})
	if err := srv.Start(context.Background(), NewMux()); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Shutdown should return promptly without deadlock.
	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// eventually는 cond가 참이 될 때까지 최대 timeout 동안 폴링한다.
func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", timeout, msg)
}

func TestServer_ErrorHandlerMovesToRetry(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	handled := make(chan struct{}, 1)
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		select {
		case handled <- struct{}{}:
		default:
		}
		return errors.New("boom")
	})

	srv := NewServer(client, ServerConfig{
		Queues:         map[string]int{"default": 1},
		Concurrency:    2,
		RetryDelayFunc: func(retried int, err error) time.Duration { return time.Hour },
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler not invoked")
	}

	eventually(t, 5*time.Second, func() bool {
		p, _ := client.XPending(ctx, base.StreamKey("default"), rdb.ConsumerGroup).Result()
		inRetry := client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil
		return p != nil && p.Count == 0 && inRetry
	}, "error task should be acked out of the PEL and moved to the retry ZSET")
}

func TestServer_UnregisteredKindMovesToRetry(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	srv := NewServer(client, ServerConfig{
		Queues:         map[string]int{"default": 1},
		Concurrency:    2,
		RetryDelayFunc: func(retried int, err error) time.Duration { return time.Hour },
	})
	ctx := context.Background()
	if err := srv.Start(ctx, NewMux()); err != nil { // no handlers
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	eventually(t, 5*time.Second, func() bool {
		return client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil
	}, "unregistered-kind task should be retried (a handler may be registered later)")
}
