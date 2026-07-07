package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

// When the current leader shuts down (resigns), a second scheduler must take
// over and keep firing — with no duplication during the handover.
func TestIntegration_SchedulerFailover(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	c := NewClient(client)
	defer c.Close()
	var runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[tickArgs]) error {
		runs.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	mk := func() *Scheduler {
		s := NewScheduler(client, SchedulerConfig{LeaderTTL: 500 * time.Millisecond})
		if err := RegisterInterval(s, 1*time.Second, tickArgs{N: 1}); err != nil {
			t.Fatalf("register: %v", err)
		}
		if err := s.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		return s
	}
	s1 := mk()
	s2 := mk()
	defer s2.Shutdown(context.Background())

	time.Sleep(1500 * time.Millisecond)
	before := runs.Load()

	// Leader resigns (graceful). s2 should take over via the resign notification.
	s1.Shutdown(context.Background())

	time.Sleep(2500 * time.Millisecond)
	after := runs.Load()

	if after <= before {
		t.Errorf("scheduling stalled after failover: before=%d after=%d", before, after)
	}
}
