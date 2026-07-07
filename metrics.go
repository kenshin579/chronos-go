package chronos

import "time"

// TaskOutcome is the terminal result of processing one task, reported to Metrics.
type TaskOutcome string

const (
	// OutcomeSuccess: the handler returned nil; the task was acked and removed.
	OutcomeSuccess TaskOutcome = "success"
	// OutcomeRetry: the handler failed and the task was scheduled for retry.
	OutcomeRetry TaskOutcome = "retry"
	// OutcomeDeadLetter: the task exhausted retries (or returned SkipRetry) and
	// was archived or discarded.
	OutcomeDeadLetter TaskOutcome = "dead_letter"
)

// Metrics receives one observation per processed task. Implementations MUST be
// safe for concurrent use (the server calls it from worker goroutines). The
// zero/nil Metrics disables observation. The Prometheus implementation lives in
// the contrib/prometheus module so the core stays dependency-free.
type Metrics interface {
	ObserveTask(queue, kind string, outcome TaskOutcome, dur time.Duration)
}
