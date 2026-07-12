package chronos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// pollBlock is how long a fetch blocks on XREADGROUP before looping. Kept short
// so Shutdown stays responsive.
const pollBlock = 1 * time.Second

// maxFetchBatch caps how many tasks one DequeueBatch claims, keeping a single
// XREADGROUP (and the follow-up pipelines) short even at high Concurrency.
const maxFetchBatch = 128

// forwardBatchSize and recoverBatchSize bound how many tasks each maintenance
// tick moves, keeping individual Redis calls short.
const (
	forwardBatchSize = 100
	recoverBatchSize = 100
)

// janitorBatchSize bounds how many age-expired archived tasks one janitor tick
// removes per queue.
const janitorBatchSize = 100

// ackTimeout bounds the post-handler ack/retry/archive operations. These run on
// a cancel-immune context (so they survive Shutdown), so a deadline is required
// to keep a stalled Redis from blocking the worker — and thus Shutdown — forever.
const ackTimeout = 30 * time.Second

// errRecoveredExhausted is the cause passed to OnDeadLetter when a task is
// dead-lettered by the recoverer (its retry budget ran out across crashes).
var errRecoveredExhausted = errors.New("chronos: retries exhausted after recovery")

// ServerConfig configures a Server.
type ServerConfig struct {
	// Queues maps queue name to weight. While every queue has work, a queue with
	// weight 6 is dequeued about 6x as often as a queue with weight 1 (smooth
	// weighted round-robin — no queue starves). When the queue picked for a round
	// is empty, that round falls through to the highest-weight queue that does
	// have work, so an idle high-priority queue never blocks lower ones. Weights
	// <= 0 are treated as 1; very large weights are capped.
	Queues map[string]int
	// StrictPriority, if true, always drains higher-weight queues first: a
	// lower-weight queue is served only while every higher-weight queue is
	// empty. Ties are broken by queue name for determinism.
	StrictPriority bool
	// Concurrency is the max number of tasks processed simultaneously.
	Concurrency int
	// Logger receives operational logs. Defaults to slog.Default().
	Logger *slog.Logger

	// RetryDelayFunc computes the backoff before a retry. Defaults to
	// DefaultRetryDelay (exponential + full jitter).
	RetryDelayFunc RetryDelayFunc
	// OnDeadLetter is invoked when a task exhausts its retries (or returns a
	// SkipRetry error). It fires whether the task is archived or discarded.
	//
	// The heartbeat keeps an actively-processing task's lease fresh, so the
	// recoverer does not normally reclaim it. It may still fire more than once in
	// pathological cases (the server/heartbeat unavailable long enough for the
	// lease to lapse), so make the hook idempotent (the archived ZSET entry is
	// deduplicated by task ID).
	OnDeadLetter func(ctx context.Context, info *TaskInfo, err error)

	// Metrics, if set, receives one observation per processed task. Use the
	// contrib/prometheus implementation, or your own. Defaults to nil (disabled).
	Metrics Metrics

	// ForwardInterval is how often the retry ZSET is scanned for due tasks.
	// Defaults to 1s.
	ForwardInterval time.Duration
	// RecoverInterval is how often stuck PEL entries are reclaimed. Defaults to 15s.
	RecoverInterval time.Duration
	// RecoverMinIdle is how long a PEL entry must be idle before it is treated as
	// abandoned. Defaults to 30s when unset (<= 0). The heartbeat refreshes the
	// lease of in-flight tasks every HeartbeatInterval, so a task that runs longer
	// than RecoverMinIdle is safe as long as this server (its heartbeat) is alive;
	// RecoverMinIdle is the window after a worker actually dies before its tasks
	// are reclaimed.
	RecoverMinIdle time.Duration

	// HeartbeatInterval is how often the server refreshes the lease and unique
	// lock of in-flight tasks. Defaults to RecoverMinIdle/3. Must be shorter than
	// RecoverMinIdle so an actively-processing task is never reclaimed.
	HeartbeatInterval time.Duration

	// ArchivedRetention is how long a dead-lettered (archived) task is kept
	// before the janitor deletes it. Defaults to 7 days (168h).
	ArchivedRetention time.Duration
	// MaxArchived caps the number of archived tasks per queue; the janitor
	// deletes the oldest beyond this even within the retention window. Defaults
	// to 10000. Set negative to disable the size cap.
	MaxArchived int
	// MaxCompleted caps the number of retained completed tasks per queue; the
	// janitor deletes the oldest beyond this even before their retention
	// expires. Defaults to 10000. Set negative to disable the size cap.
	MaxCompleted int
	// JanitorInterval is how often the janitor runs. Defaults to 1 minute.
	JanitorInterval time.Duration
}

// Server fetches tasks from Redis and dispatches them to handlers.
type Server struct {
	rdb      *rdb.RDB
	cfg      ServerConfig
	consumer string
	logger   *slog.Logger

	mux    *Mux
	sem    chan struct{}
	wg     sync.WaitGroup
	cancel context.CancelFunc

	inflightMu sync.Mutex
	inflight   map[string]inflightEntry
}

// inflightEntry is a task currently being processed by this server, tracked so
// the heartbeat can refresh its lease and unique lock.
type inflightEntry struct {
	streamID  string
	queue     string
	uniqueKey string
}

// NewServer returns a Server backed by the given Redis client.
func NewServer(r redis.UniversalClient, cfg ServerConfig) *Server {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.RetryDelayFunc == nil {
		cfg.RetryDelayFunc = DefaultRetryDelay
	}
	if cfg.ForwardInterval <= 0 {
		cfg.ForwardInterval = 1 * time.Second
	}
	if cfg.RecoverInterval <= 0 {
		cfg.RecoverInterval = 15 * time.Second
	}
	if cfg.RecoverMinIdle <= 0 {
		cfg.RecoverMinIdle = 30 * time.Second
	}
	// The heartbeat must run several times within the recover window, or an
	// actively-processing task could be reclaimed before its lease is refreshed.
	// Clamp any unset OR too-large value (>= RecoverMinIdle) to RecoverMinIdle/3,
	// with a small floor so a tiny RecoverMinIdle can't yield a zero interval
	// (time.NewTicker panics on <= 0).
	if cfg.HeartbeatInterval <= 0 || cfg.HeartbeatInterval >= cfg.RecoverMinIdle {
		cfg.HeartbeatInterval = cfg.RecoverMinIdle / 3
	}
	if cfg.HeartbeatInterval < time.Millisecond {
		cfg.HeartbeatInterval = time.Millisecond
	}
	if cfg.ArchivedRetention <= 0 {
		cfg.ArchivedRetention = 7 * 24 * time.Hour
	}
	if cfg.MaxArchived == 0 {
		cfg.MaxArchived = 10000
	}
	if cfg.MaxCompleted == 0 {
		cfg.MaxCompleted = 10000
	}
	if cfg.JanitorInterval <= 0 {
		cfg.JanitorInterval = 1 * time.Minute
	}
	return &Server{
		rdb:      rdb.NewRDB(r),
		cfg:      cfg,
		consumer: uuid.NewString(),
		logger:   logger,
		sem:      make(chan struct{}, cfg.Concurrency),
		inflight: make(map[string]inflightEntry),
	}
}

// queueNames returns the configured queue names.
func (s *Server) queueNames() []string {
	names := make([]string, 0, len(s.cfg.Queues))
	for q := range s.cfg.Queues {
		names = append(names, q)
	}
	return names
}

// queuesByWeight returns the queue names ordered by weight descending (ties by
// name ascending, so the order is deterministic). Weights <= 0 count as 1.
func (s *Server) queuesByWeight() []string {
	names := s.queueNames()
	weight := func(q string) int {
		return normalizeWeight(s.cfg.Queues[q])
	}
	sort.Slice(names, func(i, j int) bool {
		wi, wj := weight(names[i]), weight(names[j])
		if wi != wj {
			return wi > wj
		}
		return names[i] < names[j]
	})
	return names
}

// Start ensures consumer groups exist and launches the fetch loop. It returns
// once startup is complete; processing continues in the background until
// Shutdown is called or ctx is cancelled.
func (s *Server) Start(ctx context.Context, mux *Mux) error {
	s.mux = mux
	if len(s.cfg.Queues) == 0 {
		return errors.New("chronos: server requires at least one queue")
	}
	for _, q := range s.queueNames() {
		if err := s.rdb.EnsureGroup(ctx, q); err != nil {
			return err
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.wg.Add(1)
	go s.fetchLoop(runCtx)

	s.wg.Add(1)
	go s.forwarderLoop(runCtx)

	s.wg.Add(1)
	go s.recovererLoop(runCtx)

	s.wg.Add(1)
	go s.heartbeaterLoop(runCtx)

	s.wg.Add(1)
	go s.janitorLoop(runCtx)
	return nil
}

// fetchLoop repeatedly dequeues tasks and hands them to worker goroutines,
// bounded by the concurrency semaphore.
//
// Queue selection: StrictPriority always sweeps queues by weight descending, so
// a lower-weight queue is served only while every higher one is empty. The
// default (weighted) mode picks each round's first queue via smooth weighted
// round-robin — under load every queue is dequeued in proportion to its weight
// and none starves — then falls back to the remaining queues by weight so a
// worker never idles while any queue has work.
func (s *Server) fetchLoop(ctx context.Context) {
	defer s.wg.Done()
	byWeight := s.queuesByWeight()
	picker := newWRRPicker(s.cfg.Queues)
	order := make([]string, 0, len(byWeight))
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		select {
		case s.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		// 이번 라운드의 조회 순서를 결정한다.
		order = order[:0]
		if s.cfg.StrictPriority {
			order = append(order, byWeight...)
		} else {
			primary := picker.pick()
			order = append(order, primary)
			for _, q := range byWeight {
				if q != primary {
					order = append(order, q)
				}
			}
		}

		// 배치 크기: 지금 놀고 있는 워커 수만큼 한 번에 가져온다(우리가 잡은
		// 슬롯 1 + 남은 빈 슬롯). 근사치라도 안전하다 — 초과분은 아래 디스패치
		// 단계에서 세마포어 획득으로 백프레셔가 걸린다.
		batch := 1 + (cap(s.sem) - len(s.sem))
		if batch < 1 {
			batch = 1
		}
		if batch > maxFetchBatch {
			batch = maxFetchBatch
		}

		var tasks []rdb.Dequeued

		// 1) 우선순위 순서대로 논블로킹 조회. 배치는 한 라운드에 한 큐에서만
		// 가져온다(우선순위 의미 단순 유지).
		for _, q := range order {
			ts, err := s.rdb.DequeueBatch(ctx, s.consumer, -1, q, batch)
			if err == rdb.ErrNoTask {
				continue
			}
			if err != nil {
				if ctx.Err() != nil {
					<-s.sem
					return
				}
				s.logger.Error("chronos: dequeue failed", "queue", q, "error", err)
				continue
			}
			tasks = ts
			break
		}

		// 2) 모두 비었으면 이번 라운드의 첫 큐에 블로킹(응답성 유지). weighted에선
		// SWRR가 라운드마다 다른 큐를 뽑으므로 블로킹 감시도 가중치대로 분배된다.
		if len(tasks) == 0 {
			// order[0] should never be empty (Start rejects an empty Queues, so
			// the picker and byWeight are non-empty), but guard so a stray ""
			// can't turn into a busy-loop blocking on a nonexistent stream.
			if len(order) == 0 || order[0] == "" {
				<-s.sem
				return
			}
			q := order[0]
			ts, err := s.rdb.DequeueBatch(ctx, s.consumer, pollBlock, q, batch)
			if err == rdb.ErrNoTask {
				<-s.sem
				continue
			}
			if err != nil {
				if ctx.Err() != nil {
					<-s.sem
					return
				}
				s.logger.Error("chronos: dequeue failed", "queue", q, "error", err)
				<-s.sem
				time.Sleep(100 * time.Millisecond)
				continue
			}
			tasks = ts
		}

		if len(tasks) == 0 {
			<-s.sem
			continue
		}

		// 디스패치: 첫 태스크는 이미 잡아둔 세마포어 슬롯을 쓰고, 나머지는
		// 각각 슬롯을 새로 획득한 뒤 워커를 띄운다(백프레셔). 셧다운으로 슬롯을
		// 얻지 못하면 배치의 남은 태스크를 스트림으로 되돌린다(Requeue) — PEL에
		// 방치하면 recoverer가 Retried를 올리며 재전달해, 실행된 적 없는 태스크가
		// 재시도 예산을 잃는다(MaxRetry=0이면 즉시 데드레터).
		for i, dt := range tasks {
			if i > 0 {
				// Prefer taking a slot when one is free (batch was sized to the
				// free slots, so this normally never blocks); fall through to
				// the cancellation path only when we would actually block.
				select {
				case s.sem <- struct{}{}:
				default:
					select {
					case s.sem <- struct{}{}:
					case <-ctx.Done():
						// Shutdown mid-batch: return the claimed remainder to the
						// stream so no task loses retry budget without running.
						// Cancel-immune like the ack path — a stalled Redis must
						// not hang Shutdown forever.
						reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
						for _, rest := range tasks[i:] {
							if rerr := s.rdb.Requeue(reqCtx, rest.Msg.Queue, rest.StreamID, rest.Msg); rerr != nil {
								s.logger.Error("chronos: requeue on shutdown failed; recoverer will reclaim",
									"id", rest.Msg.ID, "error", rerr)
							}
						}
						cancel()
						return
					}
				}
			}
			s.wg.Add(1)
			go func(qname, sid string, m *base.TaskMessage) {
				defer s.wg.Done()
				defer func() { <-s.sem }()
				s.trackInflight(m.ID, inflightEntry{streamID: sid, queue: qname, uniqueKey: m.UniqueKey})
				defer s.untrackInflight(m.ID)
				s.process(ctx, qname, sid, m)
			}(dt.Msg.Queue, dt.StreamID, dt.Msg)
		}
	}
}

// process runs the handler for one task, recovering panics, and routes the
// outcome: success acks and deletes; a retryable error moves the task to the
// retry ZSET with backoff (until the retry budget is exhausted); a SkipRetry
// error or an exhausted budget dead-letters the task and fires OnDeadLetter.
func (s *Server) process(ctx context.Context, qname, streamID string, msg *base.TaskMessage) {
	start := time.Now()
	err := s.dispatchSafely(ctx, msg)
	dur := time.Since(start)

	// Ack/move operations must outlive shutdown cancellation so a finished task
	// is never left dangling in the PEL — but they still need a deadline, or a
	// stalled Redis would block this worker forever and hang Shutdown's wg.Wait.
	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()

	if err == nil {
		// A chained task must enqueue its successor BEFORE acking: if we acked
		// first and crashed, the chain would be lost. The reverse order is safe
		// because successor creation is idempotent (deterministic ID +
		// create-if-absent), so a redelivery cannot duplicate it.
		if len(msg.Chain) > 0 {
			if cerr := s.enqueueNextWithRetry(opCtx, msg); cerr != nil {
				// Leave the task unacked: the recoverer will redeliver it, the
				// (idempotent) handler runs again, and the successor enqueue is
				// retried. Note the reclaim consumes retry budget, so automatic
				// resumption needs budget left; the chain tail survives in the
				// archived message either way (manual RunTask resumes it).
				s.logger.Error("chronos: chain successor enqueue failed; leaving task for redelivery",
					"id", msg.ID, "error", cerr)
				return
			}
			// The successor now exists; drop the tail from this task's message so
			// a retained (WithRetention) copy doesn't advertise — or re-run —
			// links that were already handed off.
			msg.Chain = nil
		}
		// A group member reports its completion BEFORE acking, mirroring the
		// chain rule and for the same reason: ack-then-crash must not lose the
		// group's progress. The report is idempotent (SREM + create-if-absent
		// callback), so redelivery cannot double-fire the callback.
		if msg.GroupID != "" {
			if gerr := s.completeGroupWithRetry(opCtx, msg); gerr != nil {
				s.logger.Error("chronos: group completion report failed; leaving task for redelivery",
					"id", msg.ID, "group", msg.GroupID, "error", gerr)
				return
			}
		}
		if derr := s.rdb.Done(opCtx, qname, streamID, msg); derr != nil {
			s.logger.Error("chronos: ack failed", "id", msg.ID, "error", derr)
		}
		s.observe(msg, OutcomeSuccess, dur)
		return
	}

	s.logger.Error("chronos: task failed",
		"kind", msg.Kind, "id", msg.ID, "retried", msg.Retried, "error", err)

	// Dead-letter when the error is non-retryable or the budget is exhausted.
	if asSkipRetry(err) || msg.Retried >= msg.MaxRetry {
		s.deadLetter(opCtx, qname, streamID, msg, err)
		s.observe(msg, OutcomeDeadLetter, dur)
		return
	}

	msg.Retried++
	msg.LastErr = err.Error()
	msg.CompletedAt = 0 // a re-run task that fails must not show a stale completion time
	retryAt := time.Now().Add(s.cfg.RetryDelayFunc(msg.Retried, err))
	if rerr := s.rdb.Retry(opCtx, qname, streamID, msg, retryAt); rerr != nil {
		s.logger.Error("chronos: retry scheduling failed", "id", msg.ID, "error", rerr)
	}
	s.observe(msg, OutcomeRetry, dur)
}

// enqueueNextWithRetry attempts the successor enqueue a few times with a short
// backoff: one transient Redis hiccup must not park a *succeeded* task for the
// recoverer (whose reclaim consumes retry budget).
func (s *Server) enqueueNextWithRetry(ctx context.Context, msg *base.TaskMessage) error {
	var err error
	for attempt, backoff := 0, 50*time.Millisecond; attempt < 3; attempt, backoff = attempt+1, backoff*4 {
		if err = s.enqueueNext(ctx, msg); err == nil {
			return nil
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return err
		}
	}
	return err
}

// completeGroupWithRetry reports a member's success with a short backoff, for
// the same reason enqueueNextWithRetry exists: one transient Redis hiccup must
// not park a succeeded task for the recoverer.
func (s *Server) completeGroupWithRetry(ctx context.Context, msg *base.TaskMessage) error {
	var err error
	for attempt, backoff := 0, 50*time.Millisecond; attempt < 3; attempt, backoff = attempt+1, backoff*4 {
		var fired bool
		if fired, err = s.rdb.CompleteGroupMember(ctx, msg); err == nil {
			if fired {
				s.logger.Debug("chronos: group callback fired", "group", msg.GroupID)
			}
			return nil
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return err
		}
	}
	return err
}

// enqueueNext makes msg's first pending chain link available, carrying the rest
// of the tail. Idempotent: the successor's deterministic ID plus the
// create-if-absent script mean a redelivered predecessor cannot duplicate it.
func (s *Server) enqueueNext(ctx context.Context, msg *base.TaskMessage) error {
	link := msg.Chain[0]
	next := &base.TaskMessage{
		ID:         fmt.Sprintf("%s:%d", msg.ChainID, msg.ChainIndex+1),
		Kind:       link.Kind,
		Payload:    link.Payload,
		Queue:      link.Queue,
		MaxRetry:   link.MaxRetry,
		NoArchive:  link.NoArchive,
		Retention:  link.Retention,
		Chain:      msg.Chain[1:],
		ChainID:    msg.ChainID,
		ChainIndex: msg.ChainIndex + 1,
	}
	enqueued, err := s.rdb.EnqueueChainLink(ctx, next, time.Duration(link.Delay)*time.Second)
	if err != nil {
		return err
	}
	if !enqueued {
		s.logger.Debug("chronos: chain successor already exists (redelivery)", "id", next.ID)
	}
	return nil
}

func (s *Server) trackInflight(id string, e inflightEntry) {
	s.inflightMu.Lock()
	s.inflight[id] = e
	s.inflightMu.Unlock()
}

func (s *Server) untrackInflight(id string) {
	s.inflightMu.Lock()
	delete(s.inflight, id)
	s.inflightMu.Unlock()
}

// heartbeaterLoop periodically refreshes the lease (PEL idle) and unique lock
// TTL of every in-flight task, so a long-running task is not reclaimed by the
// recoverer and does not lose its unique lock mid-processing.
func (s *Server) heartbeaterLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	// Renew unique locks well past the recover window so a crash (heartbeat stops)
	// lets the recoverer take over before the lock lapses.
	renewTTL := 2 * s.cfg.RecoverMinIdle
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.beat(ctx, renewTTL)
		}
	}
}

// beat refreshes all currently in-flight tasks.
func (s *Server) beat(ctx context.Context, renewTTL time.Duration) {
	s.inflightMu.Lock()
	byQueue := make(map[string][]string)
	var uniqueKeys []string
	for _, e := range s.inflight {
		byQueue[e.queue] = append(byQueue[e.queue], e.streamID)
		if e.uniqueKey != "" {
			uniqueKeys = append(uniqueKeys, e.uniqueKey)
		}
	}
	s.inflightMu.Unlock()

	for q, ids := range byQueue {
		if err := s.rdb.ExtendLease(ctx, q, s.consumer, ids); err != nil && ctx.Err() == nil {
			s.logger.Error("chronos: extend lease failed", "queue", q, "error", err)
		}
	}
	if len(uniqueKeys) > 0 {
		if err := s.rdb.RenewUnique(ctx, uniqueKeys, renewTTL); err != nil && ctx.Err() == nil {
			s.logger.Error("chronos: renew unique failed", "error", err)
		}
	}
}

// observe reports a task outcome to the configured Metrics (no-op if unset).
func (s *Server) observe(msg *base.TaskMessage, outcome TaskOutcome, dur time.Duration) {
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.ObserveTask(msg.Queue, msg.Kind, outcome, dur)
	}
}

// dispatchSafely runs the handler and converts a panic into an error so a
// misbehaving handler cannot crash the worker.
func (s *Server) dispatchSafely(ctx context.Context, msg *base.TaskMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("chronos: handler panic: %v", r)
			s.logger.Error("chronos: handler panicked",
				"kind", msg.Kind, "id", msg.ID, "panic", r)
		}
	}()
	return s.mux.dispatch(ctx, msg)
}

// deadLetter archives the task (or discards it when NoArchive is set) and fires
// the OnDeadLetter hook.
func (s *Server) deadLetter(ctx context.Context, qname, streamID string, msg *base.TaskMessage, cause error) {
	msg.LastErr = cause.Error()
	msg.CompletedAt = 0 // a re-run task that fails must not show a stale completion time
	if msg.NoArchive {
		msg.Retention = 0 // a discarded failure is not a success — never retain as completed
		if derr := s.rdb.Done(ctx, qname, streamID, msg); derr != nil {
			s.logger.Error("chronos: discard failed", "id", msg.ID, "error", derr)
		}
	} else if aerr := s.rdb.Archive(ctx, qname, streamID, msg, time.Now()); aerr != nil {
		s.logger.Error("chronos: archive failed", "id", msg.ID, "error", aerr)
	}
	if s.cfg.OnDeadLetter != nil {
		s.cfg.OnDeadLetter(ctx, &TaskInfo{ID: msg.ID, Kind: msg.Kind, Queue: msg.Queue}, cause)
	}
}

// forwarderLoop periodically moves due tasks from each queue's retry ZSET back
// into its stream.
func (s *Server) forwarderLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.ForwardInterval)
	defer ticker.Stop()
	queues := s.queueNames()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range queues {
				if _, err := s.rdb.ForwardRetry(ctx, q, time.Now(), forwardBatchSize); err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: forward retry failed", "queue", q, "error", err)
				}
				if _, err := s.rdb.ForwardScheduled(ctx, q, time.Now(), forwardBatchSize); err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: forward scheduled failed", "queue", q, "error", err)
				}
			}
		}
	}
}

// recovererLoop periodically reclaims tasks stuck in each queue's PEL (crashed
// workers) and fires OnDeadLetter for any that are dead-lettered as a result.
func (s *Server) recovererLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.RecoverInterval)
	defer ticker.Stop()
	queues := s.queueNames()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range queues {
				_, archived, err := s.rdb.Recover(ctx, q, s.consumer, s.cfg.RecoverMinIdle, recoverBatchSize)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: recover failed", "queue", q, "error", err)
					continue
				}
				if s.cfg.OnDeadLetter != nil {
					for _, msg := range archived {
						s.cfg.OnDeadLetter(ctx,
							&TaskInfo{ID: msg.ID, Kind: msg.Kind, Queue: msg.Queue},
							errRecoveredExhausted)
					}
				}
			}
		}
	}
}

// janitorLoop periodically deletes expired / over-capacity archived tasks and
// retained-completed tasks from each queue. Removals are atomic, batched, and
// idempotent, so running it on every server instance is safe. A negative
// MaxArchived / MaxCompleted disables the corresponding size cap (handled by
// TrimArchived / TrimCompleted).
func (s *Server) janitorLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.JanitorInterval)
	defer ticker.Stop()
	queues := s.queueNames()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-s.cfg.ArchivedRetention)
			for _, q := range queues {
				n, err := s.rdb.TrimArchived(ctx, q, cutoff, s.cfg.MaxArchived, janitorBatchSize)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: janitor trim failed", "queue", q, "error", err)
					continue
				}
				if n > 0 {
					s.logger.Debug("chronos: janitor trimmed archived", "queue", q, "removed", n)
				}
				nc, err := s.rdb.TrimCompleted(ctx, q, time.Now(), s.cfg.MaxCompleted, janitorBatchSize)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: janitor trim completed failed", "queue", q, "error", err)
					continue
				}
				if nc > 0 {
					s.logger.Debug("chronos: janitor trimmed completed", "queue", q, "removed", nc)
				}
			}
		}
	}
}

// Shutdown stops fetching and waits for in-flight tasks to finish, bounded by
// ctx. In-flight tasks that do not finish before ctx expires are left unacked
// and will be recovered by another instance in a later milestone.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
