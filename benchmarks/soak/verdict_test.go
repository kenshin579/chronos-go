package soak

import (
	"math"
	"testing"
)

// mk는 지표별 시계열로 샘플 슬라이스를 만든다(다른 필드는 안정값).
func mk(heap []uint64, gor []int, db []int64) []Sample {
	n := len(heap)
	out := make([]Sample, n)
	for i := 0; i < n; i++ {
		out[i] = Sample{HeapBytes: heap[i], Goroutines: gor[i], DBSize: db[i]}
	}
	return out
}

func flatU(n int, v uint64) []uint64 {
	s := make([]uint64, n)
	for i := range s {
		s[i] = v
	}
	return s
}
func flatI(n int, v int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = v
	}
	return s
}
func flatL(n int, v int64) []int64 {
	s := make([]int64, n)
	for i := range s {
		s[i] = v
	}
	return s
}

func checkByName(t *testing.T, cs []Check, name string) Check {
	t.Helper()
	for _, c := range cs {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("check %q not found in %+v", name, cs)
	return Check{}
}

func TestEvaluate_StableSeriesPasses(t *testing.T) {
	cs, usable := Evaluate(mk(flatU(20, 50<<20), flatI(20, 60), flatL(20, 1000)))
	if !usable {
		t.Fatal("20 samples should be usable")
	}
	for _, c := range cs {
		if !c.Pass {
			t.Errorf("stable series: check %s failed (first=%.1f second=%.1f)", c.Name, c.First, c.Second)
		}
	}
}

func TestEvaluate_LinearHeapGrowthFails(t *testing.T) {
	heap := make([]uint64, 20)
	for i := range heap { // 50MB → 240MB 선형 증가(절대 슬랙 32MB를 넘어서는 기울기)
		heap[i] = uint64(50+10*i) << 20
	}
	cs, _ := Evaluate(mk(heap, flatI(20, 60), flatL(20, 1000)))
	if checkByName(t, cs, "heap").Pass {
		t.Error("~5x heap growth must fail")
	}
}

func TestEvaluate_GoroutineStepFails(t *testing.T) {
	gor := flatI(20, 60)
	for i := 10; i < 20; i++ { // 후반 계단형 +20
		gor[i] = 80
	}
	cs, _ := Evaluate(mk(flatU(20, 50<<20), gor, flatL(20, 1000)))
	if checkByName(t, cs, "goroutines").Pass {
		t.Error("+20 goroutine step must fail")
	}
}

func TestEvaluate_GoroutineSmallDriftPasses(t *testing.T) {
	gor := flatI(20, 60)
	for i := 10; i < 20; i++ { // +5는 허용(임계 +10)
		gor[i] = 65
	}
	cs, _ := Evaluate(mk(flatU(20, 50<<20), gor, flatL(20, 1000)))
	if !checkByName(t, cs, "goroutines").Pass {
		t.Error("+5 goroutine drift must pass")
	}
}

func TestEvaluate_DBSizeGrowthFails(t *testing.T) {
	db := make([]int64, 20)
	for i := range db { // 1000 → 2900
		db[i] = int64(1000 + 100*i)
	}
	cs, _ := Evaluate(mk(flatU(20, 50<<20), flatI(20, 60), db))
	if checkByName(t, cs, "dbsize").Pass {
		t.Error("~2x dbsize growth must fail")
	}
}

func TestEvaluate_WarmupTrimmed(t *testing.T) {
	// 첫 샘플(워밍업 10% = 20개 중 2개)만 비정상적으로 높음 — 절삭돼야 통과.
	heap := flatU(20, 50<<20)
	heap[0], heap[1] = 500<<20, 400<<20
	cs, _ := Evaluate(mk(heap, flatI(20, 60), flatL(20, 1000)))
	if !checkByName(t, cs, "heap").Pass {
		t.Error("warmup spike must be trimmed")
	}
}

func TestEvaluate_TooFewSamplesUnusable(t *testing.T) {
	if _, usable := Evaluate(mk(flatU(3, 1), flatI(3, 1), flatL(3, 1))); usable {
		t.Error("3 samples must be unusable")
	}
}

func TestSplitWindows_OddRemainder(t *testing.T) {
	// warmup 0이어도 최소 1개 절삭 → [2,3,4,5] → 2/2.
	first, second := splitWindows([]float64{1, 2, 3, 4, 5}, 0)
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("split 2/2, got %d/%d", len(first), len(second))
	}
	if first[0] != 2 || first[1] != 3 || second[0] != 4 || second[1] != 5 {
		t.Errorf("windows: %v %v", first, second)
	}

	// skip=max(1, int(6*0.1))=1 → 나머지 [1..5] 5개 → first [1,2], second [4,5],
	// 가운데 3은 어느 창에도 속하지 않는다.
	first, second = splitWindows([]float64{9, 1, 2, 3, 4, 5}, 0.1)
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("split 2/2, got %d/%d", len(first), len(second))
	}
	if first[0] != 1 || first[1] != 2 || second[0] != 4 || second[1] != 5 {
		t.Errorf("windows: %v %v", first, second)
	}
}

func TestEvaluate_MinimumUsableBoundary(t *testing.T) {
	// 5개: skip 1 → 4개 → 2/2 → usable.
	if _, usable := Evaluate(mk(flatU(5, 50<<20), flatI(5, 60), flatL(5, 1000))); !usable {
		t.Error("5 samples must be usable")
	}
	// 4개: skip 1 → 3개 → half 1 → unusable.
	if _, usable := Evaluate(mk(flatU(4, 50<<20), flatI(4, 60), flatL(4, 1000))); usable {
		t.Error("4 samples must be unusable")
	}
}

func TestEvaluate_EmptyInput(t *testing.T) {
	cs, usable := Evaluate(nil)
	if usable {
		t.Error("empty input must be unusable")
	}
	if cs != nil {
		t.Errorf("expected nil checks, got %+v", cs)
	}
}

func TestEvaluate_ZeroBaselineDBSize(t *testing.T) {
	// 베이스라인 0 근처: 전반 0, 후반 1 — 비율 임계만이면 실패하지만
	// 절대 슬랙(+50) 덕에 통과해야 한다.
	db := make([]int64, 20)
	for i := 10; i < 20; i++ {
		db[i] = 1
	}
	cs, usable := Evaluate(mk(flatU(20, 50<<20), flatI(20, 60), db))
	if !usable {
		t.Fatal("20 samples should be usable")
	}
	if !checkByName(t, cs, "dbsize").Pass {
		t.Error("dbsize 0 -> 1 must pass thanks to absolute slack")
	}
}

func TestSplitWindows_OddCount(t *testing.T) {
	// 11개, 워밍업 10%(=1개) 절삭 → 10개 → 5/5.
	xs := []float64{99, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	first, second := splitWindows(xs, 0.1)
	if len(first) != 5 || len(second) != 5 {
		t.Fatalf("split 5/5, got %d/%d", len(first), len(second))
	}
	if math.Abs(mean(first)-3) > 1e-9 || math.Abs(mean(second)-8) > 1e-9 {
		t.Errorf("means: %v %v", mean(first), mean(second))
	}
}
