package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestProcess_ErrorMovesTaskToRetry(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		return errors.New("boom")
	})

	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 2,
		// Keep the failed task parked in retry (don't let the forwarder move it
		// back) so we can observe the retry ZSET deterministically.
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

	eventually(t, 5*time.Second, func() bool {
		score := client.ZScore(ctx, base.RetryKey("default"), info.ID)
		return score.Err() == nil
	}, "failed task should land in the retry ZSET")

	// PEL cleared, Retried incremented to 1.
	pending, _ := client.XPending(ctx, base.StreamKey("default"), rdb.ConsumerGroup).Result()
	if pending.Count != 0 {
		t.Errorf("pending = %d, want 0", pending.Count)
	}
	raw, _ := client.HGet(ctx, base.TaskKey("default", info.ID), "msg").Result()
	stored, _ := base.DecodeMessage([]byte(raw))
	if stored.Retried != 1 {
		t.Errorf("retried = %d, want 1", stored.Retried)
	}
}

func TestProcess_SkipRetryArchivesImmediatelyAndFiresHook(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var hookFired atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		return SkipRetry(errors.New("permanent"))
	})

	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 2,
		OnDeadLetter: func(ctx context.Context, info *TaskInfo, err error) {
			hookFired.Add(1)
		},
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(10))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	eventually(t, 5*time.Second, func() bool {
		archived := client.ZScore(ctx, base.ArchivedKey("default"), info.ID).Err() == nil
		return archived && hookFired.Load() == 1
	}, "SkipRetry should archive immediately (bypassing retry budget) and fire OnDeadLetter")
}

func TestProcess_PanicIsRecoveredAndRetried(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		panic("kaboom")
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

	// A panicking handler must not crash the server; the task is retried.
	eventually(t, 5*time.Second, func() bool {
		return client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil
	}, "panicking handler should be recovered and its task retried")
}

func TestServer_RetriedTaskEventuallySucceeds(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var attempts atomic.Int32
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		if attempts.Add(1) == 1 {
			return errors.New("fail once")
		}
		close(done)
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 100 * time.Millisecond,
		RetryDelayFunc:  func(retried int, err error) time.Duration { return 50 * time.Millisecond },
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("task did not succeed on retry (attempts=%d)", attempts.Load())
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("attempts = %d, want 2 (1 fail + 1 success)", got)
	}
}

func TestServer_CrashedTaskIsRecovered(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	ctx := context.Background()
	// Simulate a crash: a foreign consumer reads the task and never acks, so it
	// sits idle in that consumer's PEL. A running server's recoverer must
	// reclaim it, and a real handler must then process it.
	if err := c.rdb.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(3))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := c.rdb.Dequeue(ctx, "dead-worker", 0, "default"); err != nil {
		t.Fatalf("simulate crash dequeue: %v", err)
	}

	processed := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(processed)
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 100 * time.Millisecond,
		RecoverInterval: 200 * time.Millisecond,
		RecoverMinIdle:  0, // reclaim immediately for the test
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	select {
	case <-processed:
	case <-time.After(10 * time.Second):
		t.Fatal("crashed task was not recovered and processed")
	}
	// After success, nothing lingers in retry/archived for this task.
	eventually(t, 3*time.Second, func() bool {
		inRetry := client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil
		inArch := client.ZScore(ctx, base.ArchivedKey("default"), info.ID).Err() == nil
		return !inRetry && !inArch
	}, "recovered-and-succeeded task should leave no retry/archived residue")
}
