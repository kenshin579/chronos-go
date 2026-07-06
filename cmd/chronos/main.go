// Command chronos is a CLI for inspecting and administering chronos-go queues.
//
//	chronos [--redis addr] [--db n] queue ls
//	chronos [--redis addr] [--db n] task ls   <queue> <scheduled|retry|archived>
//	chronos [--redis addr] [--db n] task run  <queue> <task-id>
//	chronos [--redis addr] [--db n] task rm   <queue> <task-id>
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
)

func main() {
	addr := flag.String("redis", envOr("REDIS_ADDR", "127.0.0.1:6379"), "Redis address")
	db := flag.Int("db", 0, "Redis database number")
	flag.Parse()

	client := redis.NewClient(&redis.Options{Addr: *addr, DB: *db})
	defer client.Close()

	os.Exit(run(flag.Args(), client, os.Stdout))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// run executes a CLI command against the given client, writing to out. It
// returns a process exit code. Split out from main for testability.
func run(args []string, client redis.UniversalClient, out io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(out, "usage: chronos <queue|task> ...")
		return 2
	}
	insp := chronos.NewInspector(client)
	ctx := context.Background()

	switch args[0] {
	case "queue":
		if len(args) >= 2 && args[1] == "ls" {
			return queueLs(ctx, insp, out)
		}
	case "task":
		if len(args) >= 2 {
			switch args[1] {
			case "ls":
				if len(args) == 4 {
					return taskLs(ctx, insp, out, args[2], args[3])
				}
			case "run":
				if len(args) == 4 {
					return taskAction(ctx, out, "run", func() error { return insp.RunTask(ctx, args[2], args[3]) })
				}
			case "rm":
				if len(args) == 4 {
					return taskAction(ctx, out, "rm", func() error { return insp.DeleteTask(ctx, args[2], args[3]) })
				}
			}
		}
	}
	fmt.Fprintf(out, "unknown or malformed command: %v\n", args)
	return 2
}

func queueLs(ctx context.Context, insp *chronos.Inspector, out io.Writer) int {
	queues, err := insp.Queues(ctx)
	if err != nil {
		fmt.Fprintf(out, "error: %v\n", err)
		return 1
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "QUEUE\tPENDING\tACTIVE\tSCHEDULED\tRETRY\tARCHIVED")
	for _, q := range queues {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\n", q.Queue, q.Pending, q.Active, q.Scheduled, q.Retry, q.Archived)
	}
	tw.Flush()
	return 0
}

func taskLs(ctx context.Context, insp *chronos.Inspector, out io.Writer, queue, state string) int {
	tasks, err := insp.ListTasks(ctx, queue, state, 100)
	if err != nil {
		fmt.Fprintf(out, "error: %v\n", err)
		return 1
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tKIND\tQUEUE")
	for _, t := range tasks {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", t.ID, t.Kind, t.Queue)
	}
	tw.Flush()
	return 0
}

func taskAction(ctx context.Context, out io.Writer, verb string, fn func() error) int {
	if err := fn(); err != nil {
		fmt.Fprintf(out, "error: %v\n", err)
		return 1
	}
	fmt.Fprintf(out, "%s: ok\n", verb)
	return 0
}
