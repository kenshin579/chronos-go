package chronos

import (
	"context"
	"sync"
	"testing"
	"time"

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
