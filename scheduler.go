package chronos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// SchedulerConfig configures a Scheduler.
type SchedulerConfig struct {
	// Location is the timezone for cron schedules. Defaults to time.Local.
	Location *time.Location
	// Logger receives operational logs. Defaults to slog.Default().
	Logger *slog.Logger
	// LeaderTTL is how long a leadership term lasts before it must be renewed.
	// Defaults to 5s. Failover happens within ~LeaderTTL of a leader dying.
	LeaderTTL time.Duration
}

// scheduleEntry is one registered cron/interval job.
type scheduleEntry struct {
	id       string // stable ID: "<kind>:<spec>"
	kind     string
	payload  []byte
	queue    string
	maxRetry int
	noArch   bool
	misfire  MisfirePolicy
	schedule cronSchedule
	next     time.Time // in-memory next trigger (leader only)
}

// Scheduler registers periodic jobs and, on whichever instance is elected
// leader, enqueues their due triggers. Every instance may call Start; only the
// leader enqueues, and deterministic dedup keys prevent double-enqueue during
// leader handover.
type Scheduler struct {
	rdb      *rdb.RDB
	cfg      SchedulerConfig
	instance string
	logger   *slog.Logger

	entries  []*scheduleEntry
	isLeader atomic.Bool
	wg       sync.WaitGroup
	cancel   context.CancelFunc
}

// NewScheduler returns a Scheduler backed by the given Redis client.
func NewScheduler(r redis.UniversalClient, cfg SchedulerConfig) *Scheduler {
	if cfg.Location == nil {
		cfg.Location = time.Local
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.LeaderTTL <= 0 {
		cfg.LeaderTTL = 5 * time.Second
	}
	return &Scheduler{
		rdb:      rdb.NewRDB(r),
		cfg:      cfg,
		instance: newInstanceID(),
		logger:   cfg.Logger,
	}
}

// register adds an entry after resolving options.
func (s *Scheduler) register(spec string, sched cronSchedule, kind string, payload []byte, opts []Option) {
	o := enqueueOptions{queue: DefaultQueue, maxRetry: DefaultMaxRetry}
	for _, opt := range opts {
		opt.apply(&o)
	}
	s.entries = append(s.entries, &scheduleEntry{
		id: kind + ":" + spec, kind: kind, payload: payload, queue: o.queue,
		maxRetry: o.maxRetry, noArch: o.noArchive, misfire: o.misfire, schedule: sched,
	})
}

// RegisterInterval registers args to be enqueued every interval (>= 1s).
func RegisterInterval[T TaskArgs](s *Scheduler, interval time.Duration, args T, opts ...Option) error {
	if interval < time.Second {
		return errors.New("chronos: interval must be >= 1s (sub-second schedules cannot survive leader failover)")
	}
	payload, err := encodeArgs(args)
	if err != nil {
		return err
	}
	var zero T
	s.register("@every "+interval.String(), cron.Every(interval), zero.Kind(), payload, opts)
	return nil
}

// RegisterCron registers args to be enqueued on a standard 5-field cron spec.
func RegisterCron[T TaskArgs](s *Scheduler, spec string, args T, opts ...Option) error {
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return fmt.Errorf("chronos: invalid cron spec %q: %w", spec, err)
	}
	payload, err := encodeArgs(args)
	if err != nil {
		return err
	}
	var zero T
	s.register(spec, sched, zero.Kind(), payload, opts)
	return nil
}

// Start launches leader election and the tick loop. Safe to call on every
// instance; only the leader enqueues.
func (s *Scheduler) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go s.run(runCtx)
	return nil
}

// run drives leadership renewal and, while leader, the tick loop.
func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()

	renew := time.NewTicker(s.cfg.LeaderTTL / 2)
	defer renew.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	sub := s.rdb.SubscribeResign(ctx)
	defer sub.Close()
	resign := sub.Channel()

	s.tryElect(ctx) // attempt leadership immediately

	for {
		select {
		case <-ctx.Done():
			return
		case <-renew.C:
			s.tryElect(ctx)
		case <-resign:
			// A leader resigned; try to take over right away.
			s.tryElect(ctx)
		case <-tick.C:
			if s.isLeader.Load() {
				s.fireDue(ctx)
			}
		}
	}
}

// tryElect acquires or renews leadership and, on a transition to leader,
// initializes each entry's next-trigger baseline from persisted lastFired.
func (s *Scheduler) tryElect(ctx context.Context) {
	ok, err := s.rdb.AcquireOrRenewLeadership(ctx, s.instance, s.cfg.LeaderTTL)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Error("chronos: leadership renew failed", "error", err)
		}
		return
	}
	if ok && !s.isLeader.Swap(true) {
		s.logger.Info("chronos: became scheduler leader", "instance", s.instance)
	} else if !ok {
		s.isLeader.Store(false)
	}
}

// fireDue enqueues all due triggers for every entry, applying the misfire policy.
func (s *Scheduler) fireDue(ctx context.Context) {
	now := time.Now().In(s.cfg.Location)
	for _, e := range s.entries {
		last := s.lastFiredOrInit(ctx, e, now)
		fires, newLast := computeFires(e.schedule, last, now, e.misfire)
		for _, trigger := range fires {
			if err := s.enqueueTrigger(ctx, e, trigger); err != nil {
				if !errors.Is(err, rdb.ErrDuplicateTask) && ctx.Err() == nil {
					s.logger.Error("chronos: schedule enqueue failed", "schedule", e.id, "error", err)
				}
			}
		}
		if !newLast.Equal(last) {
			if err := s.rdb.SetLastFired(ctx, e.id, newLast); err != nil && ctx.Err() == nil {
				s.logger.Error("chronos: set lastFired failed", "schedule", e.id, "error", err)
			}
		}
	}
}

// lastFiredOrInit returns the persisted lastFired for an entry. The first time a
// schedule is ever seen it baselines at now (so there is no immediate catch-up
// fire) AND persists that baseline — otherwise every tick would re-initialize to
// now, making the next trigger perpetually in the future so the job never fires.
func (s *Scheduler) lastFiredOrInit(ctx context.Context, e *scheduleEntry, now time.Time) time.Time {
	last, ok, err := s.rdb.GetLastFired(ctx, e.id)
	if err == nil && ok {
		return last
	}
	if err := s.rdb.SetLastFired(ctx, e.id, now); err != nil && ctx.Err() == nil {
		s.logger.Error("chronos: init lastFired failed", "schedule", e.id, "error", err)
	}
	return now
}

// enqueueTrigger enqueues one trigger with a deterministic dedup key so the same
// (schedule, trigger-time) is enqueued at most once cluster-wide.
func (s *Scheduler) enqueueTrigger(ctx context.Context, e *scheduleEntry, trigger time.Time) error {
	triggerID := fmt.Sprintf("%s:%d", e.id, trigger.Unix())
	msg := &base.TaskMessage{
		ID: triggerID, Kind: e.kind, Payload: e.payload, Queue: e.queue,
		MaxRetry: e.maxRetry, NoArchive: e.noArch,
	}
	dedupKey := base.PeriodicDedupKey(e.queue, triggerID)
	// Dedup key lives well beyond a leader-handover window but not forever.
	return s.rdb.EnqueuePeriodic(ctx, msg, dedupKey, 10*s.cfg.LeaderTTL)
}

// Shutdown stops the scheduler; if this instance is the leader it resigns so a
// follower can take over immediately.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.isLeader.Load() {
		_ = s.rdb.ResignLeadership(context.WithoutCancel(ctx), s.instance)
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newInstanceID() string { return uuid.NewString() }
