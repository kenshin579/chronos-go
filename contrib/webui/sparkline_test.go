package webui

import (
	"strings"
	"testing"
)

func TestRing_PushAndSVG(t *testing.T) {
	r := newRing(5)
	for i := 1; i <= 7; i++ {
		r.push(float64(i))
	}
	vals := r.values() // 최근 5개: 3..7
	if len(vals) != 5 || vals[0] != 3 || vals[4] != 7 {
		t.Errorf("values = %v", vals)
	}
	svg := sparklineSVG(vals, 80, 20)
	if !strings.Contains(svg, "<svg") || !strings.Contains(svg, "polyline") {
		t.Errorf("svg = %s", svg)
	}
	if sparklineSVG(nil, 80, 20) != "" {
		t.Error("empty input should render empty string")
	}
	if sparklineSVG([]float64{5}, 80, 20) == "" {
		t.Error("single point should still render (flat line)")
	}
}
