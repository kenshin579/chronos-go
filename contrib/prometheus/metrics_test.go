package prometheus

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/kenshin579/chronos-go"
)

func TestMetrics_ObserveTask_IncrementsCounter(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics()
	if err := reg.Register(m); err != nil {
		t.Fatalf("register: %v", err)
	}

	m.ObserveTask("default", "email:send", chronos.OutcomeSuccess, 5*time.Millisecond)
	m.ObserveTask("default", "email:send", chronos.OutcomeSuccess, 7*time.Millisecond)
	m.ObserveTask("default", "email:send", chronos.OutcomeRetry, 1*time.Millisecond)

	const want = `
# HELP chronos_tasks_processed_total Total tasks processed, by queue, kind and outcome.
# TYPE chronos_tasks_processed_total counter
chronos_tasks_processed_total{kind="email:send",outcome="retry",queue="default"} 1
chronos_tasks_processed_total{kind="email:send",outcome="success",queue="default"} 2
`
	if err := testutil.CollectAndCompare(m.processed, strings.NewReader(want)); err != nil {
		t.Fatalf("counter mismatch: %v", err)
	}
}

func TestMetrics_ObserveRecovered_CountsByOutcome(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics()
	if err := reg.Register(m); err != nil {
		t.Fatalf("register: %v", err)
	}

	m.ObserveRecovered("default", 2, 1)
	m.ObserveRecovered("default", 1, 0)
	m.ObserveRecovered("low", 0, 3)

	const want = `
# HELP chronos_tasks_recovered_total Total tasks reclaimed from crashed workers, by queue and outcome.
# TYPE chronos_tasks_recovered_total counter
chronos_tasks_recovered_total{outcome="dead_letter",queue="default"} 1
chronos_tasks_recovered_total{outcome="dead_letter",queue="low"} 3
chronos_tasks_recovered_total{outcome="requeued",queue="default"} 3
chronos_tasks_recovered_total{outcome="requeued",queue="low"} 0
`
	if err := testutil.CollectAndCompare(m.recovered, strings.NewReader(want)); err != nil {
		t.Fatalf("counter mismatch: %v", err)
	}
}

// An idle sweep reports (0, 0) and must still emit both child series — a metric
// that only appears once something breaks is unusable for alerting on a
// recoverer that stopped running.
func TestMetrics_ObserveRecovered_ZeroSweepEmitsSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics()
	if err := reg.Register(m); err != nil {
		t.Fatalf("register: %v", err)
	}

	m.ObserveRecovered("default", 0, 0)

	const want = `
# HELP chronos_tasks_recovered_total Total tasks reclaimed from crashed workers, by queue and outcome.
# TYPE chronos_tasks_recovered_total counter
chronos_tasks_recovered_total{outcome="dead_letter",queue="default"} 0
chronos_tasks_recovered_total{outcome="requeued",queue="default"} 0
`
	if err := testutil.CollectAndCompare(m.recovered, strings.NewReader(want)); err != nil {
		t.Fatalf("zero sweep should still emit both series: %v", err)
	}
	if got := testutil.CollectAndCount(m.recovered); got != 2 {
		t.Errorf("child series after a zero sweep = %d, want 2", got)
	}
}

// NewMetrics must not register itself: an adopter needs to decide what happens
// when registration fails. Killing the app because a metric collided trades
// availability for observability.
func TestNewMetrics_DoesNotSelfRegister(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics()

	if err := reg.Register(m); err != nil {
		t.Fatalf("first register: %v", err)
	}
	// The second registration must return an error, not panic.
	err := reg.Register(m)
	if err == nil {
		t.Fatal("second Register returned nil, want AlreadyRegisteredError")
	}
	var are prometheus.AlreadyRegisteredError
	if !errors.As(err, &are) {
		t.Errorf("got %T, want prometheus.AlreadyRegisteredError", err)
	}
}

// A task queue's natural duration range is workload-specific. DefBuckets tops
// out at 10s, which suits a web request but not a batch job that sweeps an
// external API for minutes: every observation lands in +Inf, and
// histogram_quantile then reports the highest finite bound instead of failing,
// so a p95 panel silently flatlines at 10s. WithDurationBuckets lets the adopter
// pick a range that matches the work.
func TestWithDurationBuckets_OverridesDefaultBuckets(t *testing.T) {
	m := NewMetrics(WithDurationBuckets([]float64{1, 5, 15, 30, 60, 120, 300, 600, 1800}))

	m.ObserveTask("batch", "batch:refresh", chronos.OutcomeSuccess, 90*time.Second)

	const want = `
# HELP chronos_task_duration_seconds Task handler duration in seconds, by queue and kind.
# TYPE chronos_task_duration_seconds histogram
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="1"} 0
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="5"} 0
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="15"} 0
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="30"} 0
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="60"} 0
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="120"} 1
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="300"} 1
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="600"} 1
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="1800"} 1
chronos_task_duration_seconds_bucket{kind="batch:refresh",queue="batch",le="+Inf"} 1
chronos_task_duration_seconds_sum{kind="batch:refresh",queue="batch"} 90
chronos_task_duration_seconds_count{kind="batch:refresh",queue="batch"} 1
`
	if err := testutil.CollectAndCompare(m.duration, strings.NewReader(want)); err != nil {
		t.Fatalf("custom buckets: %v", err)
	}
}

// The option is additive: an adopter who does not pass it keeps DefBuckets.
func TestNewMetrics_DefaultsToDefBuckets(t *testing.T) {
	m := NewMetrics()

	m.ObserveTask("default", "email:send", chronos.OutcomeSuccess, 3*time.Millisecond)

	got := bucketBounds(t, m)
	if len(got) != len(prometheus.DefBuckets) {
		t.Fatalf("bucket count = %d, want %d (DefBuckets)", len(got), len(prometheus.DefBuckets))
	}
	for i, want := range prometheus.DefBuckets {
		if got[i] != want {
			t.Errorf("bucket[%d] = %v, want %v", i, got[i], want)
		}
	}
}

// An empty or nil bucket slice must fall back to DefBuckets rather than produce
// a histogram with no finite buckets, which would make every quantile +Inf.
func TestWithDurationBuckets_EmptyFallsBackToDefault(t *testing.T) {
	for _, tc := range []struct {
		name    string
		buckets []float64
	}{
		{"nil", nil},
		{"empty", []float64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMetrics(WithDurationBuckets(tc.buckets))
			m.ObserveTask("default", "email:send", chronos.OutcomeSuccess, time.Millisecond)

			if got := len(bucketBounds(t, m)); got != len(prometheus.DefBuckets) {
				t.Errorf("bucket count = %d, want %d (DefBuckets)", got, len(prometheus.DefBuckets))
			}
		})
	}
}

// bucketBounds returns the finite upper bounds of the duration histogram's
// first child series, in the order Prometheus reports them.
func bucketBounds(t *testing.T, m *Metrics) []float64 {
	t.Helper()

	reg := prometheus.NewRegistry()
	if err := reg.Register(m); err != nil {
		t.Fatalf("register: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "chronos_task_duration_seconds" {
			continue
		}
		if len(mf.GetMetric()) == 0 {
			t.Fatal("duration histogram has no child series")
		}
		var out []float64
		for _, b := range mf.GetMetric()[0].GetHistogram().GetBucket() {
			out = append(out, b.GetUpperBound())
		}
		return out
	}
	t.Fatal("chronos_task_duration_seconds not gathered")
	return nil
}
