package chronos_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
)

type EmailArgs struct {
	To string `json:"to"`
}

func (EmailArgs) Kind() string { return "email:send" }

// Example shows the enqueue → handler round trip: define a typed args, register
// a handler on a Mux, start a Server, and enqueue.
func Example() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	ctx := context.Background()

	mux := chronos.NewMux()
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[EmailArgs]) error {
		fmt.Println("sending to", t.Args.To)
		return nil
	})

	srv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 10,
	})
	_ = srv.Start(ctx, mux)
	defer srv.Shutdown(context.Background())

	client := chronos.NewClient(rdb)
	_, _ = chronos.Enqueue(ctx, client, EmailArgs{To: "a@b.c"}, chronos.WithQueue("default"))
	// Output omitted: a real Redis round trip with async processing is not
	// deterministic, so go test compiles this example without running it.
}

type ResizeArgs struct {
	Path string `json:"path"`
}

func (ResizeArgs) Kind() string { return "img:resize" }

type ResizeResult struct {
	Bytes int `json:"bytes"`
}

// ExampleAddHandlerR shows a handler that returns a result, which the next
// workflow step reads with PrevResult / GroupResults.
func ExampleAddHandlerR() {
	mux := chronos.NewMux()
	chronos.AddHandlerR(mux, func(ctx context.Context, t *chronos.Task[ResizeArgs]) (ResizeResult, error) {
		return ResizeResult{Bytes: 1024}, nil
	})
	_ = mux
}

// ExampleNewChain runs tasks in sequence; each link starts after the previous
// one succeeds.
func ExampleNewChain() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	client := chronos.NewClient(rdb)
	_, _ = chronos.NewChain().
		Then(ResizeArgs{Path: "a.png"}).
		Then(ResizeArgs{Path: "b.png"}).
		Enqueue(context.Background(), client)
}

// ExampleNewGroup fans out members in parallel, then fires OnComplete once all
// succeed. ThenGroup embeds this as a chain stage; AddChain makes a member a
// chain.
func ExampleNewGroup() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	client := chronos.NewClient(rdb)
	_, _ = chronos.NewGroup().
		Add(ResizeArgs{Path: "a.png"}).
		AddChain(chronos.NewChain().Then(ResizeArgs{Path: "b.png"}).Then(ResizeArgs{Path: "b2.png"})).
		OnComplete(ResizeArgs{Path: "manifest"}).
		Enqueue(context.Background(), client)
}

// ExampleNewScheduler registers a periodic job. Every instance may call Start;
// leader election ensures only one fires each trigger.
func ExampleNewScheduler() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	s := chronos.NewScheduler(rdb, chronos.SchedulerConfig{})
	_ = chronos.RegisterInterval(s, time.Hour, EmailArgs{To: "digest@b.c"})
	_ = s.Start(context.Background())
	defer s.Shutdown(context.Background())
}

// ExampleNewInspector reads operational state: queue stats, task listing,
// pause/resume, scheduler status.
func ExampleNewInspector() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	insp := chronos.NewInspector(rdb)
	queues, _ := insp.Queues(context.Background())
	for _, q := range queues {
		fmt.Printf("%s: pending=%d paused=%v\n", q.Queue, q.Pending, q.Paused)
	}
}
