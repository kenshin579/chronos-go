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
