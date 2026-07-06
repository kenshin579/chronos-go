package chronos

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

type slowArgs struct {
	ID int `json:"id"`
}

func (slowArgs) Kind() string { return "test:slow" }

func mustPayload(t *testing.T, args slowArgs) []byte {
	t.Helper()
	b, err := encodeArgs(args)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

// While a unique task is still being processed, a second identical enqueue is
// still rejected — uniqueness spans the whole in-flight lifetime and the lock is
// released on terminal completion (not by TTL expiry). The orphan-safety TTL is
// set comfortably larger than the processing window: M3 has no heartbeat-based
// TTL renewal (deferred to a later milestone), so a task that outran its TTL
// could otherwise let a duplicate slip in.
func TestIntegration_UniqueLockHeldThroughProcessingUntilCompletion(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	release := make(chan struct{})
	var releaseOnce sync.Once
	doRelease := func() { releaseOnce.Do(func() { close(release) }) }

	started := make(chan struct{}, 1)
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[slowArgs]) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release // block until the test lets it finish
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     4,
		ForwardInterval: 50 * time.Millisecond,
		// Keep the recoverer away so it doesn't reclaim the deliberately-slow task.
		RecoverInterval: time.Hour,
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { doRelease(); srv.Shutdown(context.Background()) }()

	// Orphan-safety TTL kept larger than the processing window so the lock does
	// not expire mid-flight (M3 has no TTL renewal during processing).
	if _, err := Enqueue(ctx, c, slowArgs{ID: 1}, WithUnique(30*time.Second)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	// Let a non-trivial slice of processing elapse while the handler is blocked.
	time.Sleep(1500 * time.Millisecond)

	// A second identical enqueue must still be rejected (lock held through processing).
	if _, err := Enqueue(ctx, c, slowArgs{ID: 1}, WithUnique(30*time.Second)); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second enqueue during processing err = %v, want ErrDuplicateTask", err)
	}

	// Let the handler finish; the lock is released on completion.
	doRelease()
	uniqueKey := base.UniqueKey("default", base.UniqueSuffix(slowArgs{}.Kind(), mustPayload(t, slowArgs{ID: 1})))
	eventually(t, 5*time.Second, func() bool {
		return client.Exists(ctx, uniqueKey).Val() == 0
	}, "unique lock should be released after the task completes")
}

func TestIntegration_DelayedTaskExecutesOnce(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var runs atomic.Int32
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[slowArgs]) error {
		if runs.Add(1) == 1 {
			close(done)
		}
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

	if _, err := Enqueue(ctx, c, slowArgs{ID: 7}, WithProcessIn(300*time.Millisecond)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("delayed task did not run")
	}
	// Give it a moment to ensure it does not run twice (forwarder idempotency).
	time.Sleep(500 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Errorf("runs = %d, want 1", got)
	}
}

// A delayed unique task must stay deduplicated until it is promoted and run,
// even when the caller's unique TTL is shorter than the delay: the lock TTL is
// extended to cover the delay. Regression test for the "lock expires before
// promotion" gap.
func TestIntegration_DelayedUniqueLockSurvivesUntilPromotion(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// Delay (1s) is longer than the caller's unique TTL (200ms). Without the
	// TTL extension the lock would expire at 200ms and the duplicate would slip in.
	if _, err := Enqueue(ctx, c, slowArgs{ID: 42},
		WithProcessIn(1*time.Second), WithUnique(200*time.Millisecond)); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}

	// Wait past the naive TTL but before the delay elapses.
	time.Sleep(500 * time.Millisecond)

	// The duplicate must still be rejected.
	if _, err := Enqueue(ctx, c, slowArgs{ID: 42},
		WithProcessIn(1*time.Second), WithUnique(200*time.Millisecond)); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("duplicate during delay err = %v, want ErrDuplicateTask", err)
	}
}
