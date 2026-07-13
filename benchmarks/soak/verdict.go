package soak

// Check is one pass/fail judgement over the trimmed sample windows.
type Check struct {
	Name   string  // "heap" | "goroutines" | "dbsize"
	First  float64 // first-half mean
	Second float64 // second-half mean
	Rule   string  // human-readable pass rule
	Pass   bool
}

const warmupFrac = 0.1

// 베이스라인이 작을 때 비율 임계(x1.2, x1.1)만으로는 노이즈에 휘둘리므로
// 절대 여유를 더한 하이브리드 임계를 쓴다.
const (
	heapSlackBytes = 32 << 20 // 베이스라인이 작을 때 ±20%는 노이즈 — 절대 여유
	dbSlackKeys    = 50
)

// Evaluate trims the first warmupFrac of samples, splits the rest in half and
// compares means. usable is false when fewer than 4 samples remain — callers
// must then treat the run as informational only.
func Evaluate(samples []Sample) (checks []Check, usable bool) {
	heap := make([]float64, len(samples))
	gor := make([]float64, len(samples))
	db := make([]float64, len(samples))
	for i, s := range samples {
		heap[i] = float64(s.HeapBytes)
		gor[i] = float64(s.Goroutines)
		db[i] = float64(s.DBSize)
	}
	h1, h2 := splitWindows(heap, warmupFrac)
	if len(h1) < 2 || len(h2) < 2 {
		return nil, false
	}
	g1, g2 := splitWindows(gor, warmupFrac)
	d1, d2 := splitWindows(db, warmupFrac)

	hf, hs := mean(h1), mean(h2)
	gf, gs := mean(g1), mean(g2)
	df, ds := mean(d1), mean(d2)
	return []Check{
		{Name: "heap", First: hf, Second: hs, Rule: "second <= first x1.2 + 32MB", Pass: hs <= hf*1.2+heapSlackBytes},
		{Name: "goroutines", First: gf, Second: gs, Rule: "second - first <= 10", Pass: gs-gf <= 10},
		{Name: "dbsize", First: df, Second: ds, Rule: "second <= first x1.1 + 50", Pass: ds <= df*1.1+dbSlackKeys},
	}, true
}

// splitWindows drops the first warmup fraction (rounded down, but always at
// least one sample when the input is non-empty — otherwise small n would keep
// the warmup spike and mask leaks) and splits the remainder into two equal
// halves; a leftover middle element when the remainder is odd goes to neither
// half.
func splitWindows(xs []float64, warmup float64) (first, second []float64) {
	skip := int(float64(len(xs)) * warmup)
	if skip == 0 && len(xs) > 0 {
		skip = 1
	}
	rest := xs[skip:]
	half := len(rest) / 2
	return rest[:half], rest[len(rest)-half:]
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}
