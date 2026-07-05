package chronos

import (
	"context"
	"errors"
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

// ServerConfig configures a Server.
type ServerConfig struct {
	// Queues maps queue name to weight. In M1 only the keys are used (all
	// queues are read equally); weighted priority is a later enhancement.
	Queues map[string]int
	// Concurrency is the max number of tasks processed simultaneously.
	Concurrency int
	// Logger receives operational logs. Defaults to slog.Default().
	Logger *slog.Logger
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
	return &Server{
		rdb:      rdb.NewRDB(r),
		cfg:      cfg,
		consumer: uuid.NewString(),
		logger:   logger,
		sem:      make(chan struct{}, cfg.Concurrency),
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
			s.process(ctx, qname, sid, m)
		}(msg.Queue, streamID, msg)
	}
}

// process runs the handler for one task and acks it. In M1 a handler error is
// logged and the task is acked+deleted (no retry). M2 replaces this with retry
// routing.
func (s *Server) process(ctx context.Context, qname, streamID string, msg *base.TaskMessage) {
	if err := s.mux.dispatch(ctx, msg); err != nil {
		s.logger.Error("chronos: task failed",
			"kind", msg.Kind, "id", msg.ID, "error", err)
	}
	// Ack는 shutdown 취소보다 오래 살아야 한다: 이미 끝난 태스크를 unacked로 남기면
	// M1에서 PEL/hash 누수, M2 recoverer 도입 시 중복 실행이 된다.
	ackCtx := context.WithoutCancel(ctx)
	if err := s.rdb.Done(ackCtx, qname, streamID, msg.ID); err != nil {
		s.logger.Error("chronos: ack failed", "id", msg.ID, "error", err)
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
