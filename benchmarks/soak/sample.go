// Package soak generates a long-running mixed workload against chronos-go and
// judges resource trends (heap, goroutines, Redis keyspace) for leaks.
package soak

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// Sample is one periodic measurement. Family counts (Stream..Schedules) are
// diagnostic only; the verdict uses HeapBytes, Goroutines and DBSize.
type Sample struct {
	ElapsedSec float64 `json:"elapsed_sec"`
	HeapBytes  uint64  `json:"heap_bytes"` // HeapAlloc after a forced GC
	Goroutines int     `json:"goroutines"`
	DBSize     int64   `json:"dbsize"`
	Throughput float64 `json:"throughput"` // completions/sec since the previous sample

	Stream    int64 `json:"stream"` // XLEN, all queues summed
	Retry     int64 `json:"retry"`  // ZCARD sums
	Scheduled int64 `json:"scheduled"`
	Archived  int64 `json:"archived"`
	Completed int64 `json:"completed_zset"`
	Unique    int64 `json:"unique_locks"` // SCAN counts
	Groups    int64 `json:"group_sets"`
	Schedules int64 `json:"schedules"` // HLEN chronos:schedules
}

// WriteJSONL appends s as one JSON line.
func WriteJSONL(w io.Writer, s Sample) error {
	return json.NewEncoder(w).Encode(s)
}

// Line renders the one-line human-readable form printed every sample.
func (s Sample) Line() string {
	d := time.Duration(s.ElapsedSec) * time.Second
	return fmt.Sprintf("[%02d:%02d:%02d] heap=%dMB gor=%d dbsize=%d tput=%.0f/s stream=%d retry=%d sched=%d arch=%d comp=%d uniq=%d grp=%d reg=%d",
		int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60,
		s.HeapBytes>>20, s.Goroutines, s.DBSize, s.Throughput,
		s.Stream, s.Retry, s.Scheduled, s.Archived, s.Completed,
		s.Unique, s.Groups, s.Schedules)
}
