package chronos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// forwardBatchSize and recoverBatchSize bound how many tasks each maintenance
// tick moves, keeping individual Redis calls short.
const (
	forwardBatchSize = 100
	recoverBatchSize = 100
)

// ackTimeout bounds the post-handler ack/retry/archive operations. These run on
// a cancel-immune context (so they survive Shutdown), so a deadline is required
// to keep a stalled Redis from blocking the worker — and thus Shutdown — forever.
const ackTimeout = 30 * time.Second

// errRecoveredExhausted is the cause passed to OnDeadLetter when a task is
// dead-lettered by the recoverer (its retry budget ran out across crashes).
var errRecoveredExhausted = errors.New("chronos: retries exhausted after recovery")

// ServerConfig configures a Server.
type ServerConfig struct {
	// Queues maps queue name to weight. Only the keys are used today (all queues
	// are read equally); weighted priority is a later enhancement.
	Queues map[string]int
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
	// It may fire more than once for the same task: if a handler runs longer than
	// RecoverMinIdle, the recoverer can reclaim and dead-letter the task while the
	// original worker is still running, then dead-letter it again. Make the hook
	// idempotent (the archived ZSET entry itself is deduplicated by task ID).
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
	// abandoned. Defaults to 30s when unset (<= 0). Handlers that can run longer
	// than this may be reclaimed and reprocessed concurrently (at-least-once), so
	// raise it comfortably above the expected handler duration.
	RecoverMinIdle time.Duration

	// HeartbeatInterval is how often the server refreshes the lease and unique
	// lock of in-flight tasks. Defaults to RecoverMinIdle/3. Must be shorter than
	// RecoverMinIdle so an actively-processing task is never reclaimed.
	HeartbeatInterval time.Duration
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
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.RecoverMinIdle / 3
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
	return nil
}

// fetchLoop repeatedly dequeues tasks and hands them to worker goroutines,
// bounded by the concurrency semaphore.
func (s *Server) fetchLoop(ctx context.Context) {
	defer s.wg.Done()
	queues := s.queueNames()
	idx := 0
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

		var (
			msg      *base.TaskMessage
			streamID string
			found    bool
		)

		// 1) 구성된 큐를 순회하며 논블로킹 조회 (우선순위 가중치는 M2).
		for _, q := range queues {
			m, sid, err := s.rdb.Dequeue(ctx, s.consumer, -1, q)
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
			msg, streamID, found = m, sid, true
			break
		}

		// 2) 아무것도 없으면 라운드로빈으로 한 큐에 블로킹(응답성 유지, 기아 방지).
		if !found {
			if len(queues) == 0 {
				<-s.sem
				return
			}
			q := queues[idx%len(queues)]
			idx++
			m, sid, err := s.rdb.Dequeue(ctx, s.consumer, pollBlock, q)
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
			msg, streamID, found = m, sid, true
		}

		if !found {
			<-s.sem
			continue
		}

		s.wg.Add(1)
		go func(qname, sid string, m *base.TaskMessage) {
			defer s.wg.Done()
			defer func() { <-s.sem }()
			s.trackInflight(m.ID, inflightEntry{streamID: sid, queue: qname, uniqueKey: m.UniqueKey})
			defer s.untrackInflight(m.ID)
			s.process(ctx, qname, sid, m)
		}(msg.Queue, streamID, msg)
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
	retryAt := time.Now().Add(s.cfg.RetryDelayFunc(msg.Retried, err))
	if rerr := s.rdb.Retry(opCtx, qname, streamID, msg, retryAt); rerr != nil {
		s.logger.Error("chronos: retry scheduling failed", "id", msg.ID, "error", rerr)
	}
	s.observe(msg, OutcomeRetry, dur)
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
	if msg.NoArchive {
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
