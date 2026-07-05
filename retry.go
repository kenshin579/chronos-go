package chronos

import (
	"errors"
	"math"
	"math/rand"
	"time"
)

// RetryDelayFunc computes how long to wait before the next retry, given the
// number of retries already performed and the error that caused the failure.
type RetryDelayFunc func(retried int, err error) time.Duration

// skipRetryError marks an error as non-retryable: the task is dead-lettered
// immediately instead of being retried.
type skipRetryError struct{ err error }

func (e *skipRetryError) Error() string { return e.err.Error() }
func (e *skipRetryError) Unwrap() error { return e.err }

// SkipRetry wraps err so that returning it from a handler dead-letters the task
// immediately, bypassing the remaining retry budget.
func SkipRetry(err error) error {
	return &skipRetryError{err: err}
}

// asSkipRetry reports whether err is (or wraps) a SkipRetry error.
func asSkipRetry(err error) bool {
	var se *skipRetryError
	return errors.As(err, &se)
}

// retryBaseDelay and retryMaxDelay bound the default exponential backoff.
const (
	retryBaseDelay = 5 * time.Second
	retryMaxDelay  = 15 * time.Minute
)

// DefaultRetryDelay is the default backoff: an exponential cap (base * 2^retried,
// clamped to retryMaxDelay) with full jitter — the actual delay is uniformly
// random in [0, cap]. Full jitter spreads retries to avoid thundering herds.
func DefaultRetryDelay(retried int, _ error) time.Duration {
	ceiling := float64(retryBaseDelay) * math.Pow(2, float64(retried))
	if ceiling > float64(retryMaxDelay) {
		ceiling = float64(retryMaxDelay)
	}
	return time.Duration(rand.Int63n(int64(ceiling) + 1))
}
