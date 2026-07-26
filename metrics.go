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
//
// OutcomeDeadLetter covers only tasks that failed through a handler. A task
// dead-lettered by the recoverer — reclaimed from a crashed worker's PEL with
// its retry budget already exhausted — never reached a handler and is reported
// through RecoveryMetrics instead.
type Metrics interface {
	ObserveTask(queue, kind string, outcome TaskOutcome, dur time.Duration)
}

// RecoveryMetrics is an optional companion to Metrics. A Metrics implementation
// that also implements it receives recoverer activity, which ObserveTask cannot
// express because a reclaimed task has no handler duration.
//
// Counts are per-sweep deltas, not cumulative. A sweep that reclaims nothing
// reports (0, 0), which lets an implementation distinguish an idle recoverer
// from one that is not running at all.
type RecoveryMetrics interface {
	ObserveRecovered(queue string, recovered, deadLettered int)
}
