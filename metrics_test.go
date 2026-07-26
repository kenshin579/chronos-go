package chronos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

type capturedObs struct {
	queue, kind string
	outcome     TaskOutcome
}

type fakeMetrics struct {
	mu  sync.Mutex
	obs []capturedObs
}

func (m *fakeMetrics) ObserveTask(queue, kind string, outcome TaskOutcome, dur time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.obs = append(m.obs, capturedObs{queue, kind, outcome})
}

func (m *fakeMetrics) outcomes() []TaskOutcome {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]TaskOutcome, len(m.obs))
	for i, o := range m.obs {
		out[i] = o.outcome
	}
	return out
}

type capturedSweep struct {
	queue                   string
	recovered, deadLettered int
}

// fakeRecoveryMetrics implements Metrics and the optional RecoveryMetrics, so a
// test can assert on recoverer sweeps and on the absence of ObserveTask calls.
type fakeRecoveryMetrics struct {
	fakeMetrics
	sweepMu sync.Mutex
	swept   []capturedSweep
}

func (m *fakeRecoveryMetrics) ObserveRecovered(queue string, recovered, deadLettered int) {
	m.sweepMu.Lock()
	defer m.sweepMu.Unlock()
	m.swept = append(m.swept, capturedSweep{queue, recovered, deadLettered})
}

func (m *fakeRecoveryMetrics) sweeps() []capturedSweep {
	m.sweepMu.Lock()
	defer m.sweepMu.Unlock()
	return append([]capturedSweep(nil), m.swept...)
}

// crashInFlight enqueues a task and leaves it in a foreign consumer's PEL,
// simulating a worker that died mid-flight. Call it before starting the server
// so no real worker can claim the task first.
func crashInFlight(t *testing.T, c *Client, opts ...Option) *TaskInfo {
	t.Helper()
	ctx := context.Background()
	if err := c.rdb.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, opts...)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := c.rdb.Dequeue(ctx, "dead-worker", 0, "default"); err != nil {
		t.Fatalf("simulate crash dequeue: %v", err)
	}
	return info
}

// recovererConfig is a server config whose recoverer reclaims crashed tasks
// almost immediately, so tests do not have to wait out the defaults.
func recovererConfig(m Metrics) ServerConfig {
	return ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     2,
		ForwardInterval: 100 * time.Millisecond,
		RecoverInterval: 100 * time.Millisecond,
		RecoverMinIdle:  1 * time.Millisecond,
		Metrics:         m,
	}
}

// A Metrics implementation that does NOT implement RecoveryMetrics must keep
// working unchanged: the type assertion fails, nothing panics, and recovery
// still processes the task.
func TestRecoveryMetrics_PlainMetricsIsUnaffected(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	m := &fakeMetrics{}
	crashInFlight(t, c, WithMaxRetry(3))

	processed := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(processed)
		return nil
	})
	srv := NewServer(client, recovererConfig(m))
	if err := srv.Start(context.Background(), mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	select {
	case <-processed:
	case <-time.After(10 * time.Second):
		t.Fatal("crashed task was not recovered and processed")
	}
	eventually(t, 5*time.Second, func() bool {
		for _, o := range m.outcomes() {
			if o == OutcomeSuccess {
				return true
			}
		}
		return false
	}, "plain Metrics should still observe the recovered task's success")
}

// A nil Metrics must not panic: the type assertion on a nil interface value
// returns ok == false.
func TestRecoveryMetrics_NilMetricsDoesNotPanic(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	crashInFlight(t, c, WithMaxRetry(3))

	processed := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(processed)
		return nil
	})
	cfg := recovererConfig(nil)
	srv := NewServer(client, cfg)
	// Direct call too: a panic in the recoverer goroutine would take the whole
	// test binary down, so pin the assertion here as well.
	srv.observeRecovered("default", 1, 1)
	if err := srv.Start(context.Background(), mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	select {
	case <-processed:
	case <-time.After(10 * time.Second):
		t.Fatal("crashed task was not recovered and processed with a nil Metrics")
	}
}

func TestRecoveryMetrics_ReportsRecoveredCount(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	m := &fakeRecoveryMetrics{}
	crashInFlight(t, c, WithMaxRetry(3))

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error { return nil })
	srv := NewServer(client, recovererConfig(m))
	if err := srv.Start(context.Background(), mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	eventually(t, 10*time.Second, func() bool {
		for _, s := range m.sweeps() {
			if s == (capturedSweep{queue: "default", recovered: 1, deadLettered: 0}) {
				return true
			}
		}
		return false
	}, "a reclaimed task should be reported as recovered on queue default")
}

// A sweep that reclaims nothing still reports (0, 0) — that is how an
// implementation tells an idle recoverer from one that is not running.
func TestRecoveryMetrics_IdleSweepReportsZero(t *testing.T) {
	client := testutil.NewRedis(t)

	m := &fakeRecoveryMetrics{}
	srv := NewServer(client, recovererConfig(m))
	if err := srv.Start(context.Background(), NewMux()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	eventually(t, 5*time.Second, func() bool {
		for _, s := range m.sweeps() {
			if s == (capturedSweep{queue: "default"}) {
				return true
			}
		}
		return false
	}, "an empty sweep should report (0, 0) rather than be skipped")
}

// The bug this hook exists for: a task whose retry budget is already exhausted
// is archived by the recoverer without ever reaching a handler. It must be
// reported as deadLettered — and ObserveTask must NOT be called for it, since a
// zero duration would poison the duration histogram.
func TestRecoveryMetrics_ExhaustedTaskIsDeadLetteredNotObserved(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	m := &fakeRecoveryMetrics{}
	var hookFired atomic.Int32
	// MaxRetry(0): the reclaim counts as the failed attempt that exhausts the
	// budget, so the recoverer archives instead of re-queueing.
	info := crashInFlight(t, c, WithMaxRetry(0))

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		t.Error("handler ran: the task should have been archived by the recoverer")
		return nil
	})
	cfg := recovererConfig(m)
	cfg.OnDeadLetter = func(ctx context.Context, info *TaskInfo, err error) { hookFired.Add(1) }
	srv := NewServer(client, cfg)
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	eventually(t, 10*time.Second, func() bool {
		for _, s := range m.sweeps() {
			if s == (capturedSweep{queue: "default", recovered: 0, deadLettered: 1}) {
				return true
			}
		}
		return false
	}, "a recovered task with an exhausted retry budget should be reported as deadLettered")

	if err := client.ZScore(ctx, base.ArchivedKey("default"), info.ID).Err(); err != nil {
		t.Errorf("task not in archived zset: %v", err)
	}
	if got := hookFired.Load(); got != 1 {
		t.Errorf("OnDeadLetter fired %d times, want 1", got)
	}
	// The regression guard: no ObserveTask call, at any outcome, for a task no
	// handler ever ran.
	if got := m.outcomes(); len(got) != 0 {
		t.Errorf("ObserveTask called %d times (%v), want 0 for a recoverer dead-letter", len(got), got)
	}
	// And exactly one dead-letter across all sweeps: later sweeps report (0, 0).
	total := 0
	for _, s := range m.sweeps() {
		total += s.deadLettered
	}
	if total != 1 {
		t.Errorf("total deadLettered across sweeps = %d, want 1", total)
	}
}

func TestMetrics_SuccessOutcomeObserved(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	m := &fakeMetrics{}
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(done)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 2, Metrics: m})
	if err := srv.Start(context.Background(), mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(context.Background(), c, emailArgs{UserID: "u1"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-done
	eventually(t, 3*time.Second, func() bool {
		for _, o := range m.outcomes() {
			if o == OutcomeSuccess {
				return true
			}
		}
		return false
	}, "success outcome should be observed")
}
