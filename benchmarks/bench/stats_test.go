package bench

import (
	"testing"
	"time"
)

func TestPercentiles(t *testing.T) {
	// 1..100ms — p50=50ms(중앙), p95=95ms, p99=99ms, max=100ms (최근접 순위법).
	lat := make([]time.Duration, 100)
	for i := range lat {
		lat[i] = time.Duration(i+1) * time.Millisecond
	}
	p50, p95, p99, max := Percentiles(lat)
	if p50 != 50*time.Millisecond || p95 != 95*time.Millisecond ||
		p99 != 99*time.Millisecond || max != 100*time.Millisecond {
		t.Errorf("got p50=%v p95=%v p99=%v max=%v", p50, p95, p99, max)
	}
}

func TestPercentiles_Empty(t *testing.T) {
	p50, _, _, max := Percentiles(nil)
	if p50 != 0 || max != 0 {
		t.Errorf("empty input should yield zeros, got p50=%v max=%v", p50, max)
	}
}

func TestMedianByThroughput(t *testing.T) {
	rs := []Result{{Throughput: 100}, {Throughput: 300}, {Throughput: 200}}
	if got := MedianByThroughput(rs); got.Throughput != 200 {
		t.Errorf("median = %v, want 200", got.Throughput)
	}
}
