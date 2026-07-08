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
