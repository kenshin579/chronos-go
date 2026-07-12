package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// PrintTable renders results as an aligned human-readable table.
func PrintTable(w io.Writer, rs []Result) {
	fmt.Fprintf(w, "%-10s %-8s %8s %6s %12s %10s %10s %10s %10s\n",
		"SCENARIO", "TARGET", "TASKS", "CONC", "TASKS/S", "P50", "P95", "P99", "MAX")
	for _, r := range rs {
		fmt.Fprintf(w, "%-10s %-8s %8d %6d %12.0f %10s %10s %10s %10s\n",
			r.Scenario, r.Target, r.Tasks, r.Concurrency, r.Throughput,
			fmtDur(r.P50), fmtDur(r.P95), fmtDur(r.P99), fmtDur(r.Max))
		for k, v := range r.Extra {
			fmt.Fprintf(w, "    %s = %.2f\n", k, v)
		}
	}
}

// PrintJSONL writes one JSON object per result (machine-readable).
func PrintJSONL(w io.Writer, rs []Result) error {
	enc := json.NewEncoder(w)
	for _, r := range rs {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	return d.Round(10 * time.Microsecond).String()
}
