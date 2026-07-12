package webui

import (
	"fmt"
	"strings"
	"sync"
)

// ring is a fixed-size ring buffer of samples (oldest evicted first).
type ring struct {
	buf  []float64
	next int
	full bool
}

func newRing(size int) *ring { return &ring{buf: make([]float64, size)} }

func (r *ring) push(v float64) {
	r.buf[r.next] = v
	r.next = (r.next + 1) % len(r.buf)
	if r.next == 0 {
		r.full = true
	}
}

// values returns samples oldest→newest.
func (r *ring) values() []float64 {
	if !r.full {
		return append([]float64(nil), r.buf[:r.next]...)
	}
	out := make([]float64, 0, len(r.buf))
	out = append(out, r.buf[r.next:]...)
	out = append(out, r.buf[:r.next]...)
	return out
}

// sparkStore keeps one ring per queue. Samples live only as long as the UI
// process — history belongs to Grafana, this is just a pulse.
type sparkStore struct {
	mu    sync.Mutex
	rings map[string]*ring
	size  int
}

func newSparkStore(size int) *sparkStore {
	return &sparkStore{rings: map[string]*ring{}, size: size}
}

func (s *sparkStore) push(queue string, v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rings[queue]
	if !ok {
		r = newRing(s.size)
		s.rings[queue] = r
	}
	r.push(v)
}

func (s *sparkStore) svg(queue string, w, h int) string {
	s.mu.Lock()
	r, ok := s.rings[queue]
	s.mu.Unlock()
	if !ok {
		return ""
	}
	return sparklineSVG(r.values(), w, h)
}

// sparklineSVG renders values as a min-max normalized polyline. Empty input
// renders "" ; a single point renders a flat line.
func sparklineSVG(vals []float64, w, h int) string {
	if len(vals) == 0 {
		return ""
	}
	if len(vals) == 1 {
		vals = []float64{vals[0], vals[0]}
	}
	min, max := vals[0], vals[0]
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	span := max - min
	if span == 0 {
		span = 1
	}
	pad := 2.0
	var pts strings.Builder
	for i, v := range vals {
		x := pad + (float64(w)-2*pad)*float64(i)/float64(len(vals)-1)
		y := pad + (float64(h)-2*pad)*(1-(v-min)/span)
		fmt.Fprintf(&pts, "%.1f,%.1f ", x, y)
	}
	return fmt.Sprintf(
		`<svg viewBox="0 0 %d %d" width="%d" height="%d" preserveAspectRatio="none"><polyline points="%s" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>`,
		w, h, w, h, strings.TrimSpace(pts.String()))
}
