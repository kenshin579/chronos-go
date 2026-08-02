package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type nsTaskA struct {
	V int `json:"v"`
}

func (nsTaskA) Kind() string { return "ns:a" }

type nsTaskB struct {
	V int `json:"v"`
}

func (nsTaskB) Kind() string { return "ns:b" }

// TestTwoNamespacesDoNotCollide is the acceptance test for key prefixes.
//
// Before prefixes, two applications on one Redis database contended for the
// single global leader lock, and the loser's schedules stopped firing with no
// error. This asserts both namespaces elect their own leader and both fire.
func TestTwoNamespacesDoNotCollide(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	nsA := NewNamespace(client, "appa")
	nsB := NewNamespace(client, "appb")

	firedA := make(chan struct{}, 10)
	firedB := make(chan struct{}, 10)

	muxA := NewMux()
	AddHandler(muxA, func(ctx context.Context, task *Task[nsTaskA]) error {
		firedA <- struct{}{}
		return nil
	})
	muxB := NewMux()
	AddHandler(muxB, func(ctx context.Context, task *Task[nsTaskB]) error {
		firedB <- struct{}{}
		return nil
	})

	srvA := nsA.NewServer(ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	srvB := nsB.NewServer(ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srvA.Start(ctx, muxA); err != nil {
		t.Fatalf("server A start: %v", err)
	}
	defer srvA.Shutdown(context.Background())
	if err := srvB.Start(ctx, muxB); err != nil {
		t.Fatalf("server B start: %v", err)
	}
	defer srvB.Shutdown(context.Background())

	schedA := nsA.NewScheduler(SchedulerConfig{})
	schedB := nsB.NewScheduler(SchedulerConfig{})
	if err := RegisterInterval(schedA, time.Second, nsTaskA{V: 1}); err != nil {
		t.Fatalf("register A: %v", err)
	}
	if err := RegisterInterval(schedB, time.Second, nsTaskB{V: 2}); err != nil {
		t.Fatalf("register B: %v", err)
	}
	if err := schedA.Start(ctx); err != nil {
		t.Fatalf("scheduler A start: %v", err)
	}
	defer schedA.Shutdown(context.Background())
	if err := schedB.Start(ctx); err != nil {
		t.Fatalf("scheduler B start: %v", err)
	}
	defer schedB.Shutdown(context.Background())

	// Both must fire. Before prefixes, exactly one would.
	deadline := time.After(20 * time.Second)
	gotA, gotB := false, false
	for !gotA || !gotB {
		select {
		case <-firedA:
			gotA = true
		case <-firedB:
			gotB = true
		case <-deadline:
			t.Fatalf("timed out: namespace A fired=%v, namespace B fired=%v "+
				"(both must fire; one-only means they share a leader lock)", gotA, gotB)
		}
	}

	// Each namespace's Inspector must see only its own queues.
	insA := nsA.NewInspector()
	qs, err := insA.Queues(ctx)
	if err != nil {
		t.Fatalf("inspector A queues: %v", err)
	}
	if len(qs) == 0 {
		t.Fatal("inspector A saw no queues")
	}

	// The leader locks must be distinct keys, both present.
	for _, key := range []string{"appa:leader", "appb:leader"} {
		n, err := client.Exists(ctx, key).Result()
		if err != nil {
			t.Fatalf("exists %s: %v", key, err)
		}
		if n != 1 {
			t.Errorf("leader key %s missing — namespaces are sharing a lock", key)
		}
	}
}

// TestNamespaceIsolatesEnqueue asserts data written in one namespace is
// invisible in another.
func TestNamespaceIsolatesEnqueue(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	nsA := NewNamespace(client, "isoa")
	nsB := NewNamespace(client, "isob")

	if _, err := Enqueue(ctx, nsA.NewClient(), nsTaskA{V: 1}, WithQueue("shared")); err != nil {
		t.Fatalf("enqueue into A: %v", err)
	}

	// Assert the positive direction first. Without it the negative assertion
	// below is vacuous: if Queues ever returned nothing at all, "B sees no
	// pending tasks" would pass while proving nothing.
	pendingIn := func(qs []*QueueInfo, name string) (int64, bool) {
		for _, q := range qs {
			if q.Queue == name {
				return q.Pending, true
			}
		}
		return 0, false
	}

	qsA, err := nsA.NewInspector().Queues(ctx)
	if err != nil {
		t.Fatalf("inspector A queues: %v", err)
	}
	pending, found := pendingIn(qsA, "shared")
	if !found {
		t.Fatalf("namespace A does not see queue %q it just enqueued into", "shared")
	}
	if pending != 1 {
		t.Fatalf("namespace A sees %d pending task(s) in %q, want 1", pending, "shared")
	}

	qsB, err := nsB.NewInspector().Queues(ctx)
	if err != nil {
		t.Fatalf("inspector B queues: %v", err)
	}
	if _, found := pendingIn(qsB, "shared"); found {
		t.Errorf("namespace B sees queue %q, which only namespace A wrote to", "shared")
	}
	for _, q := range qsB {
		if q.Pending > 0 {
			t.Errorf("namespace B sees %d pending task(s) in queue %q enqueued by namespace A",
				q.Pending, q.Queue)
		}
	}
}
