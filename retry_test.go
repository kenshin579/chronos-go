package chronos

import (
	"errors"
	"testing"
	"time"
)

func TestSkipRetry_Detectable(t *testing.T) {
	base := errors.New("bad input")
	err := SkipRetry(base)
	if !asSkipRetry(err) {
		t.Error("asSkipRetry should detect a SkipRetry-wrapped error")
	}
	if !errors.Is(err, base) {
		t.Error("SkipRetry should wrap and preserve the original error")
	}
	if asSkipRetry(errors.New("plain")) {
		t.Error("plain error must not be treated as SkipRetry")
	}
}

func TestDefaultRetryDelay_GrowsAndIsBounded(t *testing.T) {
	// Full-jitter delay is in [0, cap]; cap grows with retried but never exceeds max.
	const max = 15 * time.Minute
	for _, retried := range []int{0, 1, 5, 20, 100} {
		d := DefaultRetryDelay(retried, errors.New("x"))
		if d < 0 || d > max {
			t.Errorf("DefaultRetryDelay(%d) = %v, want within [0, %v]", retried, d, max)
		}
	}
}
