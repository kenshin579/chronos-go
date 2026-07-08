package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

// A task that runs longer than RecoverMinIdle must NOT be reclaimed and
// re-executed by the recoverer — the heartbeat keeps its lease fresh, so it runs
// exactly once.
func TestServer_HeartbeatPreventsRecovererDoubleRun(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	var runs atomic.Int32
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		if runs.Add(1) == 1 {
			time.Sleep(1200 * time.Millisecond) // > RecoverMinIdle below
			close(done)
		}
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:            map[string]int{"default": 1},
		Concurrency:       4,
		RecoverMinIdle:    400 * time.Millisecond, // would reclaim a >400ms task...
		RecoverInterval:   200 * time.Millisecond,
		HeartbeatInterval: 150 * time.Millisecond, // ...but heartbeat keeps it fresh
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
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish")
	}
	// Give the recoverer a couple more cycles to (wrongly) double-run if it would.
	time.Sleep(600 * time.Millisecond)
	if n := runs.Load(); n != 1 {
		t.Errorf("runs = %d, want 1 (heartbeat must prevent recoverer double-run)", n)
	}
}

// A HeartbeatInterval >= RecoverMinIdle (or unset) must be clamped below the
// recover window, so a misconfiguration cannot let the recoverer reclaim an
// actively-processing task. Also guards against a zero interval (NewTicker panic).
func TestNewServer_ClampsHeartbeatInterval(t *testing.T) {
	client := testutil.NewRedis(t)

	// Too large (>= RecoverMinIdle) → clamped to RecoverMinIdle/3.
	s := NewServer(client, ServerConfig{
		Queues:            map[string]int{"default": 1},
		RecoverMinIdle:    3 * time.Second,
		HeartbeatInterval: 5 * time.Second,
	})
	if s.cfg.HeartbeatInterval >= s.cfg.RecoverMinIdle {
		t.Errorf("HeartbeatInterval = %v, want < RecoverMinIdle (%v)", s.cfg.HeartbeatInterval, s.cfg.RecoverMinIdle)
	}
	if s.cfg.HeartbeatInterval != time.Second { // 3s/3
		t.Errorf("HeartbeatInterval = %v, want 1s", s.cfg.HeartbeatInterval)
	}

	// Explicit small-but-valid value is respected.
	s2 := NewServer(client, ServerConfig{
		Queues:            map[string]int{"default": 1},
		RecoverMinIdle:    2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
	})
	if s2.cfg.HeartbeatInterval != 200*time.Millisecond {
		t.Errorf("valid HeartbeatInterval overwritten: %v", s2.cfg.HeartbeatInterval)
	}
}
