package soak

import "testing"

func TestPickVariantDistribution(t *testing.T) {
	counts := map[variant]int{}
	for seq := 0; seq < 1000; seq++ {
		counts[pickVariant(seq)]++
	}
	want := map[variant]int{
		varFailOnce:       100, // 10%
		varDeadLetter:     50,  // 5%
		varDiscard:        50,  // 5%
		varNormalDelayed:  100, // 10%p (성공분에서 차출)
		varNormalRetained: 160, // 성공분의 ~20%
		varNormal:         540,
	}
	for v, n := range want {
		if counts[v] != n {
			t.Errorf("variant %d: got %d want %d", v, counts[v], n)
		}
	}
}

func TestVariantQueueSplit(t *testing.T) {
	a, b := 0, 0
	for seq := 0; seq < 1000; seq++ {
		if pickQueue(seq) == "soak-a" {
			a++
		} else {
			b++
		}
	}
	if a != 750 || b != 250 { // 가중치 3:1과 동일 비율
		t.Errorf("queue split a=%d b=%d, want 750/250", a, b)
	}
}
