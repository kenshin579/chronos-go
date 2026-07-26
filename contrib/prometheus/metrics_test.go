package prometheus

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/kenshin579/chronos-go"
)

func TestMetrics_ObserveTask_IncrementsCounter(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

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
	m := NewMetrics(reg)

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
	m := NewMetrics(reg)

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
