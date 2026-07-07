package chronos

import (
	"context"
	"sync"
	"testing"
	"time"

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
