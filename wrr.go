package chronos

import "sort"

// maxWeight caps a queue weight. Weights are summed into wrrPicker.total and
// added to running counters each pick; without a cap a caller could pass values
// whose sum overflows int, flipping total negative and breaking the algorithm.
// 1<<20 is far above any sane weight while leaving huge headroom before overflow.
const maxWeight = 1 << 20

// normalizeWeight maps any configured weight into [1, maxWeight]. Weights <= 0
// become 1 (lenient, like the Inspector's NOGROUP handling); absurdly large
// weights are clamped so the smooth-weighted-round-robin arithmetic cannot
// overflow. Used by both the picker and the Server's weight ordering so the two
// always agree.
func normalizeWeight(w int) int {
	if w <= 0 {
		return 1
	}
	if w > maxWeight {
		return maxWeight
	}
	return w
}

// wrrPicker implements smooth weighted round-robin (the nginx algorithm): each
// pick raises every queue's current score by its weight, selects the highest,
// and deducts the weight total from the winner. The resulting sequence is
// deterministic and smoothly interleaved — over each cycle of `total` picks a
// queue is selected exactly `weight` times, and the picks are spread out rather
// than bursting, so no queue starves. The running counters stay bounded by
// `total` in magnitude, so the picker never overflows however long it runs.
type wrrPicker struct {
	names   []string // sorted, so tie-breaks (and the sequence) are deterministic
	weights []int
	current []int
	total   int
}

// newWRRPicker builds a picker over a queue→weight map. Weights <= 0 are
// treated as 1 (matching the Server's lenient handling of ServerConfig.Queues).
func newWRRPicker(queues map[string]int) *wrrPicker {
	names := make([]string, 0, len(queues))
	for q := range queues {
		names = append(names, q)
	}
	sort.Strings(names)
	p := &wrrPicker{
		names:   names,
		weights: make([]int, len(names)),
		current: make([]int, len(names)),
	}
	for i, q := range names {
		w := normalizeWeight(queues[q])
		p.weights[i] = w
		p.total += w
	}
	return p
}

// pick returns the next queue in the sequence, or "" if the picker is empty.
func (p *wrrPicker) pick() string {
	if len(p.names) == 0 {
		return ""
	}
	best := 0
	for i := range p.names {
		p.current[i] += p.weights[i]
		if p.current[i] > p.current[best] {
			best = i
		}
	}
	p.current[best] -= p.total
	return p.names[best]
}
