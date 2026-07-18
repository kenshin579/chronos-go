// Package chronos is a Redis-backed distributed task queue and scheduler.
//
// A [Client] enqueues tasks; a [Server] consumes them by routing each to a
// handler registered on a [Mux]. Tasks are strongly typed: define an args type
// implementing [TaskArgs], register a handler with [AddHandler] (or
// [AddHandlerR] to return a result), and enqueue with [Enqueue].
//
//	type EmailArgs struct{ To string }
//	func (EmailArgs) Kind() string { return "email:send" }
//
//	mux := chronos.NewMux()
//	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[EmailArgs]) error {
//		return send(t.Args.To)
//	})
//	// At least one queue is required: Start errors if Queues is empty.
//	srv := chronos.NewServer(rdb, chronos.ServerConfig{
//		Queues:      map[string]int{"default": 1},
//		Concurrency: 10,
//	})
//	srv.Start(ctx, mux)
//	chronos.Enqueue(ctx, client, EmailArgs{To: "a@b.c"})
//
// # Workflows
//
// [NewChain] runs tasks in sequence (each link starts when its predecessor
// succeeds); [NewGroup] fans out tasks in parallel and fires an OnComplete
// callback when all finish. Results flow between steps: a handler registered
// with [AddHandlerR] exposes its result to the next chain link via
// [PrevResult] and to a group callback via [GroupResults]. [Chain.ThenGroup]
// embeds a parallel stage inside a chain, and [Group.AddChain] makes a group
// member a chain — together they express fan-out/fan-in pipelines.
//
// # Scheduling
//
// [NewScheduler] runs periodic jobs (interval or cron) with leader election, so
// many instances may run a scheduler and only one fires each trigger.
//
// # Inspection
//
// [NewInspector] reads queue stats, lists and re-runs tasks, pauses queues, and
// reports scheduler state — the read/operate surface behind the CLI and web
// console.
//
// # Delivery semantics
//
// Delivery is at-least-once: a handler may run more than once (redelivery after
// a crash, or a recoverer reclaiming a stalled task), so handlers must be
// idempotent. See the module README for the full list of guarantees and
// caveats.
//
// # Stability
//
// From v1.0.0 the core package follows semantic versioning. The contrib
// modules (contrib/webui, contrib/prometheus) and internal/ are experimental
// and not covered by that guarantee.
package chronos
