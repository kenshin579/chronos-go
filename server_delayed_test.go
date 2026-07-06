package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_DelayedTaskRunsAfterDelay(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var ranAt atomic.Int64
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		ranAt.Store(time.Now().UnixMilli())
		close(done)
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 50 * time.Millisecond,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	enqueuedAt := time.Now()
	const delay = 700 * time.Millisecond
	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(delay)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("delayed task did not run")
	}

	// It must not have run appreciably before its delay elapsed.
	elapsed := time.Duration(ranAt.Load()-enqueuedAt.UnixMilli()) * time.Millisecond
	if elapsed < delay-150*time.Millisecond {
		t.Errorf("task ran after %v, want >= ~%v", elapsed, delay)
	}
}
