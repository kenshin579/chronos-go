// Package bench is a small harness for measuring task-queue throughput and
// end-to-end latency: scenarios produce a Result, the runner repeats them and
// picks the median, and the reporter renders tables / JSONL.
package bench

import (
	"sort"
	"time"
)

// Result is one scenario execution's outcome.
type Result struct {
	Scenario    string             `json:"scenario"`
	Target      string             `json:"target"` // "chronos" | "asynq"
	Tasks       int                `json:"tasks"`
	Concurrency int                `json:"concurrency"`
	Producers   int                `json:"producers"`
	PayloadSize int                `json:"payload_size"`
	Elapsed     time.Duration      `json:"elapsed_ns"`
	Throughput  float64            `json:"tasks_per_sec"`
	P50         time.Duration      `json:"p50_ns"`
	P95         time.Duration      `json:"p95_ns"`
	P99         time.Duration      `json:"p99_ns"`
	Max         time.Duration      `json:"max_ns"`
	Extra       map[string]float64 `json:"extra,omitempty"` // scenario-specific metrics
}

// Percentiles returns p50/p95/p99/max using nearest-rank on a sorted copy.
// Zero values for empty input.
func Percentiles(latencies []time.Duration) (p50, p95, p99, max time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0, 0
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := func(p float64) time.Duration {
		idx := int(p*float64(len(sorted))+0.5) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	return rank(0.50), rank(0.95), rank(0.99), sorted[len(sorted)-1]
}

// MedianByThroughput returns the run whose throughput is the median — reporting
// a real run (not an average of runs) keeps latency and throughput consistent.
func MedianByThroughput(rs []Result) Result {
	sorted := make([]Result, len(rs))
	copy(sorted, rs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Throughput < sorted[j].Throughput })
	return sorted[len(sorted)/2]
}
