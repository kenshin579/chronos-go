package chronos

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type counterArgs struct {
	N int `json:"n"`
}

func (counterArgs) Kind() string { return "test:counter" }

func TestEndToEnd_ProcessesManyTasksAcrossQueues(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	const total = 50
	var processed int64
	var wg sync.WaitGroup
	wg.Add(total)

	mux := NewMux()
	seen := make(map[int]bool)
	var seenMu sync.Mutex
	AddHandler(mux, func(ctx context.Context, task *Task[counterArgs]) error {
		seenMu.Lock()
		seen[task.Args.N] = true
		seenMu.Unlock()
		atomic.AddInt64(&processed, 1)
		wg.Done()
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1, "critical": 1},
		Concurrency: 8,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	for i := 0; i < total; i++ {
		q := "default"
		if i%2 == 0 {
			q = "critical"
		}
		if _, err := Enqueue(ctx, c, counterArgs{N: i}, WithQueue(q)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	waitCh := make(chan struct{})
	go func() { wg.Wait(); close(waitCh) }()
	select {
	case <-waitCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("only %d/%d tasks processed within 15s", atomic.LoadInt64(&processed), total)
	}

	if got := atomic.LoadInt64(&processed); got != total {
		t.Errorf("processed = %d, want %d", got, total)
	}
	seenMu.Lock()
	if len(seen) != total {
		t.Errorf("distinct tasks seen = %d, want %d", len(seen), total)
	}
	seenMu.Unlock()
}

func TestIntegration_ArchivedStabilizesUnderRetention(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	// Handler always fails → every task dead-letters (fast archived growth).
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[poisonArgs]) error {
		return errors.New("always fails")
	})
	srv := NewServer(client, ServerConfig{
		Queues:            map[string]int{"default": 1},
		Concurrency:       4,
		ForwardInterval:   50 * time.Millisecond,
		RetryDelayFunc:    func(retried int, err error) time.Duration { return 20 * time.Millisecond },
		ArchivedRetention: 1 * time.Second,        // very short so archived drains quickly
		JanitorInterval:   100 * time.Millisecond, // clean often
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	insp := NewInspector(client)
	// Continuously enqueue failing tasks for ~2s.
	stop := time.Now().Add(2 * time.Second)
	for i := 0; time.Now().Before(stop); i++ {
		_, _ = Enqueue(ctx, c, poisonArgs{ID: i}, WithMaxRetry(0)) // MaxRetry 0 → immediate dead-letter
		time.Sleep(20 * time.Millisecond)
	}

	// With a 1s retention + frequent janitor, archived must not grow unbounded:
	// after letting the janitor run past the retention window, it should be small.
	eventually(t, 6*time.Second, func() bool {
		qs, err := insp.Queues(ctx)
		if err != nil || len(qs) == 0 {
			return false
		}
		return qs[0].Archived <= 5 // drained close to zero (retention 1s ≫ nothing lingers)
	}, "archived should stabilize (not grow unbounded) under short retention")
}
