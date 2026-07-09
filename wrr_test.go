package chronos

import "testing"

// pickN returns the next n picks from p.
func pickN(p *wrrPicker, n int) []string {
	seq := make([]string, n)
	for i := range seq {
		seq[i] = p.pick()
	}
	return seq
}

func TestWRRPicker_WeightedRatioIsSmooth(t *testing.T) {
	// {a:3, b:1}: every window of 4 consecutive picks must contain a exactly 3
	// times — the weighted ratio holds not just on average but smoothly.
	p := newWRRPicker(map[string]int{"a": 3, "b": 1})
	seq := pickN(p, 40)
	for i := 0; i+4 <= len(seq); i++ {
		na := 0
		for _, q := range seq[i : i+4] {
			if q == "a" {
				na++
			}
		}
		if na != 3 {
			t.Fatalf("window %d %v: a appears %d times, want 3", i, seq[i:i+4], na)
		}
	}
}

func TestWRRPicker_ThreeQueuesInterleave(t *testing.T) {
	// {a:2, b:1, c:1}: every window of 4 has a twice and b, c once each.
	p := newWRRPicker(map[string]int{"a": 2, "b": 1, "c": 1})
	seq := pickN(p, 40)
	for i := 0; i+4 <= len(seq); i++ {
		counts := map[string]int{}
		for _, q := range seq[i : i+4] {
			counts[q]++
		}
		if counts["a"] != 2 || counts["b"] != 1 || counts["c"] != 1 {
			t.Fatalf("window %d %v: counts=%v, want a:2 b:1 c:1", i, seq[i:i+4], counts)
		}
	}
}

func TestWRRPicker_EqualWeightsAlternate(t *testing.T) {
	p := newWRRPicker(map[string]int{"a": 1, "b": 1})
	seq := pickN(p, 10)
	for i := 1; i < len(seq); i++ {
		if seq[i] == seq[i-1] {
			t.Fatalf("equal weights must alternate, got %v", seq)
		}
	}
}

func TestWRRPicker_SingleQueue(t *testing.T) {
	p := newWRRPicker(map[string]int{"only": 7})
	for i := 0; i < 5; i++ {
		if got := p.pick(); got != "only" {
			t.Fatalf("pick %d = %q, want only", i, got)
		}
	}
}

func TestWRRPicker_NonPositiveWeightsTreatedAsOne(t *testing.T) {
	// Weights <= 0 normalize to 1, so {a:0, b:-5} behaves like {a:1, b:1}.
	p := newWRRPicker(map[string]int{"a": 0, "b": -5})
	seq := pickN(p, 10)
	for i := 1; i < len(seq); i++ {
		if seq[i] == seq[i-1] {
			t.Fatalf("normalized equal weights must alternate, got %v", seq)
		}
	}
}

func TestWRRPicker_Deterministic(t *testing.T) {
	// Two pickers over the same map produce the same sequence (sorted names
	// break ties, so map iteration order does not leak in).
	q := map[string]int{"low": 1, "critical": 6, "default": 3}
	a, b := newWRRPicker(q), newWRRPicker(q)
	for i := 0; i < 30; i++ {
		if x, y := a.pick(), b.pick(); x != y {
			t.Fatalf("pick %d diverged: %q vs %q", i, x, y)
		}
	}
}

func TestWRRPicker_Empty(t *testing.T) {
	p := newWRRPicker(nil)
	if got := p.pick(); got != "" {
		t.Fatalf("empty picker pick = %q, want \"\"", got)
	}
}

func TestWRRPicker_ExtremeWeightsDoNotOverflow(t *testing.T) {
	// Weights near MaxInt are clamped, so total stays positive and the picker
	// keeps working (no negative-total blow-up). The clamped huge queue still
	// dominates, and the small one is not permanently starved.
	p := newWRRPicker(map[string]int{"huge": int(^uint(0) >> 1), "tiny": 1})
	seq := pickN(p, 100)
	nHuge, nTiny := countStr(seq, "huge"), countStr(seq, "tiny")
	if nHuge == 0 || nHuge <= nTiny {
		t.Fatalf("huge should dominate: huge=%d tiny=%d", nHuge, nTiny)
	}
	if nHuge+nTiny != 100 {
		t.Fatalf("picker returned unexpected names: huge=%d tiny=%d", nHuge, nTiny)
	}
}

func countStr(seq []string, s string) int {
	n := 0
	for _, x := range seq {
		if x == s {
			n++
		}
	}
	return n
}
