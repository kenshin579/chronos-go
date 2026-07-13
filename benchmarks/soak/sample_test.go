package soak

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestSampleJSONLRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := Sample{
		ElapsedSec: 90, HeapBytes: 48 << 20, Goroutines: 52, DBSize: 1204,
		Throughput: 198.4, Stream: 3, Retry: 2, Scheduled: 20, Archived: 5,
		Completed: 310, Unique: 1, Groups: 2, Schedules: 1,
	}
	if err := WriteJSONL(&buf, in); err != nil {
		t.Fatalf("write: %v", err)
	}
	// 한 줄에 하나의 완전한 JSON 객체.
	sc := bufio.NewScanner(&buf)
	if !sc.Scan() {
		t.Fatal("no line written")
	}
	var out Sample
	if err := json.Unmarshal(sc.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != in {
		t.Errorf("round trip mismatch: got %+v want %+v", out, in)
	}
	if sc.Scan() {
		t.Error("expected exactly one line")
	}
}

func TestSampleLine(t *testing.T) {
	s := Sample{ElapsedSec: 750, HeapBytes: 48 << 20, Goroutines: 52,
		DBSize: 1204, Throughput: 198.4}
	line := s.Line()
	for _, want := range []string{"[00:12:30]", "heap=48MB", "gor=52", "dbsize=1204", "tput=198/s"} {
		if !strings.Contains(line, want) {
			t.Errorf("line %q missing %q", line, want)
		}
	}
}
