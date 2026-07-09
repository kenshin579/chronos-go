package chronos

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

// prioArgs carries its queue name in the payload so the handler can record
// which queue each processed task came from.
type prioArgs struct {
	Q string `json:"q"`
	N int    `json:"n"`
}

func (prioArgs) Kind() string { return "test:prio" }

// runPriorityServer preloads counts[queue] tasks per queue, then processes them
// all with Concurrency 1 (so processing order == dequeue order) and returns the
// queue names in processing order.
func runPriorityServer(t *testing.T, queues map[string]int, strict bool, counts map[string]int) []string {
	t.Helper()
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	total := 0
	for q, n := range counts {
		for i := 0; i < n; i++ {
			if _, err := Enqueue(ctx, c, prioArgs{Q: q, N: i}, WithQueue(q)); err != nil {
				t.Fatalf("enqueue %s#%d: %v", q, i, err)
			}
			total++
		}
	}

	var (
		mu    sync.Mutex
		order []string
	)
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[prioArgs]) error {
		mu.Lock()
		order = append(order, task.Args.Q)
		if len(order) == total {
			close(done)
		}
		mu.Unlock()
		return nil
	})

	srv := NewServer(client, ServerConfig{
		Queues:         queues,
		StrictPriority: strict,
		Concurrency:    1, // sequential: processing order mirrors dequeue order
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		mu.Lock()
		n := len(order)
		mu.Unlock()
		t.Fatalf("processed %d/%d tasks within 30s", n, total)
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), order...)
}

func countQueue(seq []string, q string) int {
	n := 0
	for _, s := range seq {
		if s == q {
			n++
		}
	}
	return n
}

func TestServer_WeightedPriorityDistribution(t *testing.T) {
	// critical:3 vs low:1, both preloaded — under load the dequeue ratio must
	// follow the weights (SWRR: exactly 12 critical in the first 16; allow
	// slack for scheduling noise), and low must not starve.
	order := runPriorityServer(t,
		map[string]int{"critical": 3, "low": 1}, false,
		map[string]int{"critical": 20, "low": 20})

	head := order[:16]
	if got := countQueue(head, "critical"); got < 10 || got > 14 {
		t.Errorf("critical in first 16 = %d, want in [10,14] (order=%v)", got, head)
	}
	if got := countQueue(head, "low"); got < 2 {
		t.Errorf("low starved: %d in first 16, want >= 2 (order=%v)", got, head)
	}
}

func TestServer_StrictPriorityDrainsHighFirst(t *testing.T) {
	// StrictPriority: every critical task runs before any low task.
	order := runPriorityServer(t,
		map[string]int{"critical": 2, "low": 1}, true,
		map[string]int{"critical": 5, "low": 5})

	for i, q := range order[:5] {
		if q != "critical" {
			t.Fatalf("order[%d] = %q, want critical (strict must drain critical first; order=%v)", i, q, order)
		}
	}
}

func TestServer_ZeroWeightQueueStillServed(t *testing.T) {
	// Weight <= 0 is treated as 1, not an error and not starvation.
	order := runPriorityServer(t,
		map[string]int{"zeroq": 0}, false,
		map[string]int{"zeroq": 3})
	if len(order) != 3 {
		t.Fatalf("processed %d tasks, want 3", len(order))
	}
}
