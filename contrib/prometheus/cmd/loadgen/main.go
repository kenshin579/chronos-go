// Command loadgen runs a small chronos-go workload and exposes Prometheus
// metrics on :2112/metrics, so a Prometheus+Grafana stack has live data to graph.
//
//	REDIS_ADDR=127.0.0.1:6379 go run ./cmd/loadgen
package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
	chronosprom "github.com/kenshin579/chronos-go/contrib/prometheus"
)

type workArgs struct {
	N int `json:"n"`
}

func (workArgs) Kind() string { return "demo:work" }

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()

	reg := prometheus.NewRegistry()
	metrics := chronosprom.NewMetrics(reg)
	reg.MustRegister(chronosprom.NewQueueCollector(chronos.NewInspector(rdb)))

	mux := chronos.NewMux()
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[workArgs]) error {
		time.Sleep(time.Duration(rand.Intn(40)+5) * time.Millisecond) // simulate work
		if rand.Intn(5) == 0 {                                        // ~20% fail → retries/dead-letters
			return errors.New("simulated failure")
		}
		return nil
	})

	srv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"default": 1, "critical": 1},
		Concurrency: 8,
		Metrics:     metrics,
	})
	if err := srv.Start(ctx, mux); err != nil {
		log.Fatalf("server start: %v", err)
	}

	// A scheduled job so the "scheduled/leader" path is exercised too.
	sched := chronos.NewScheduler(rdb, chronos.SchedulerConfig{})
	_ = chronos.RegisterInterval(sched, 2*time.Second, workArgs{N: -1}, chronos.WithMaxRetry(2))
	if err := sched.Start(ctx); err != nil {
		log.Fatalf("scheduler start: %v", err)
	}

	// Continuous enqueue load.
	client := chronos.NewClient(rdb)
	go func() {
		for i := 0; ; i++ {
			q := "default"
			if i%3 == 0 {
				q = "critical"
			}
			_, _ = chronos.Enqueue(ctx, client, workArgs{N: i}, chronos.WithQueue(q), chronos.WithMaxRetry(2))
			time.Sleep(150 * time.Millisecond)
		}
	}()

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Println("chronos loadgen: metrics on :2112/metrics")
	log.Fatal(http.ListenAndServe(":2112", nil))
}
