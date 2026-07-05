package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

type poisonArgs struct {
	ID int `json:"id"`
}

func (poisonArgs) Kind() string { return "test:poison" }

func TestIntegration_PoisonPillConvergesToDeadLetter(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var calls atomic.Int32
	var deadLettered atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[poisonArgs]) error {
		calls.Add(1)
		return errors.New("always fails")
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     4,
		ForwardInterval: 50 * time.Millisecond,
		RetryDelayFunc:  func(retried int, err error) time.Duration { return 20 * time.Millisecond },
		OnDeadLetter: func(ctx context.Context, info *TaskInfo, err error) {
			deadLettered.Add(1)
		},
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	const maxRetry = 3
	info, err := Enqueue(ctx, c, poisonArgs{ID: 1}, WithMaxRetry(maxRetry))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Must land in archived and fire the hook exactly once.
	eventually(t, 10*time.Second, func() bool {
		return deadLettered.Load() == 1 &&
			client.ZScore(ctx, base.ArchivedKey("default"), info.ID).Err() == nil
	}, "poison pill should converge to the archived ZSET and fire OnDeadLetter once")

	// It should have executed exactly maxRetry+1 times (1 initial + N retries),
	// then stopped — no infinite reprocessing.
	// Give the system a moment to prove it does NOT keep running.
	time.Sleep(500 * time.Millisecond)
	if got := calls.Load(); got != maxRetry+1 {
		t.Errorf("handler calls = %d, want %d (1 initial + %d retries)", got, maxRetry+1, maxRetry)
	}
	if got := deadLettered.Load(); got != 1 {
		t.Errorf("OnDeadLetter fired %d times, want 1", got)
	}

	// Not left in the retry ZSET.
	if client.ZScore(ctx, base.RetryKey("default"), info.ID).Err() == nil {
		t.Error("task should not remain in retry ZSET after dead-lettering")
	}
}

func TestIntegration_DiscardModeSkipsArchiveButFiresHook(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var deadLettered atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[poisonArgs]) error {
		return errors.New("always fails")
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 50 * time.Millisecond,
		RetryDelayFunc:  func(retried int, err error) time.Duration { return 20 * time.Millisecond },
		OnDeadLetter: func(ctx context.Context, info *TaskInfo, err error) {
			deadLettered.Add(1)
		},
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, poisonArgs{ID: 2}, WithMaxRetry(1), WithDeadLetterDiscard())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	eventually(t, 10*time.Second, func() bool {
		return deadLettered.Load() == 1
	}, "discard-mode task should still fire OnDeadLetter")

	// Discard: not archived, and the task hash is deleted.
	time.Sleep(300 * time.Millisecond)
	if client.ZScore(ctx, base.ArchivedKey("default"), info.ID).Err() == nil {
		t.Error("discard-mode task must NOT be in the archived ZSET")
	}
	exists, _ := client.Exists(ctx, base.TaskKey("default", info.ID)).Result()
	if exists != 0 {
		t.Error("discard-mode task hash should be deleted")
	}
}
