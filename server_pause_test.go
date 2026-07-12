package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/rdb"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_PauseStopsConsumptionResumeRestarts(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	r := rdb.NewRDB(client)

	var done atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		done.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 2})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	// pause 후 캐시 반영 여유(1s+)를 두고 enqueue → 소비되지 않아야 한다.
	if err := r.PauseQueue(ctx, "default"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	time.Sleep(1500 * time.Millisecond) // pause cache refresh
	for i := 0; i < 3; i++ {
		if _, err := Enqueue(ctx, c, emailArgs{UserID: "p"}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	time.Sleep(2 * time.Second)
	if n := done.Load(); n != 0 {
		t.Fatalf("consumed %d tasks while paused, want 0", n)
	}

	// resume → 쌓인 3개 소비.
	if err := r.ResumeQueue(ctx, "default"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for done.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("resume did not restart consumption (done=%d)", done.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestServer_PauseOneQueueOthersContinue(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	r := rdb.NewRDB(client)

	var a, b atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[chainArgs]) error {
		if task.Args.Step == 1 {
			a.Add(1)
		} else {
			b.Add(1)
		}
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"qa": 1, "qb": 1}, Concurrency: 2})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if err := r.PauseQueue(ctx, "qa"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, err := Enqueue(ctx, c, chainArgs{Step: 1}, WithQueue("qa")); err != nil {
		t.Fatalf("enqueue qa: %v", err)
	}
	if _, err := Enqueue(ctx, c, chainArgs{Step: 2}, WithQueue("qb")); err != nil {
		t.Fatalf("enqueue qb: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for b.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("unpaused queue qb was not consumed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if a.Load() != 0 {
		t.Errorf("paused queue qa consumed %d", a.Load())
	}
}
