// Command bench measures chronos-go (and asynq, for comparison) against a
// local Redis. See docs/BENCHMARKS.md for methodology.
//
//	go run ./cmd/bench -target chronos -scenario e2e -tasks 20000 -concurrency 16
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/kenshin579/chronos-go/benchmarks/bench"
	"github.com/kenshin579/chronos-go/benchmarks/chronosbench"
)

func main() {
	target := flag.String("target", "chronos", "chronos | asynq")
	scenario := flag.String("scenario", "e2e", "enqueue | e2e | chain | group")
	tasks := flag.Int("tasks", 20000, "total tasks per run")
	concurrency := flag.Int("concurrency", 16, "server worker concurrency")
	producers := flag.Int("producers", 4, "concurrent producers")
	payload := flag.Int("payload", 100, "payload size (bytes)")
	repeats := flag.Int("repeats", 3, "runs per scenario (median reported)")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	db := flag.Int("db", 15, "Redis DB (FLUSHED between runs — use a dedicated DB)")
	jsonOut := flag.Bool("json", false, "emit JSONL instead of a table")
	flag.Parse()

	cfg := bench.Config{
		RedisAddr: *redisAddr, RedisDB: *db,
		Tasks: *tasks, Concurrency: *concurrency,
		Producers: *producers, PayloadSize: *payload,
	}
	s, err := pick(*target, *scenario)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(2)
	}
	r, err := bench.Run(context.Background(), s, cfg, *repeats)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
	if *jsonOut {
		_ = bench.PrintJSONL(os.Stdout, []bench.Result{r})
	} else {
		bench.PrintTable(os.Stdout, []bench.Result{r})
	}
}

// pick maps target×scenario to an implementation. asynq scenarios are wired in
// a later task; until then they return an error.
func pick(target, scenario string) (bench.Scenario, error) {
	switch target {
	case "chronos":
		switch scenario {
		case "enqueue":
			return chronosbench.Enqueue(), nil
		case "e2e":
			return chronosbench.E2E(), nil
		}
	case "asynq":
		return nil, fmt.Errorf("asynq scenarios not wired yet")
	}
	return nil, fmt.Errorf("unknown target/scenario: %s/%s", target, scenario)
}
