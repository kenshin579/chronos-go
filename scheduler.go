package chronos

import (
	"context"
	"crypto/sha256"
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
	id        string // stable ID: "<kind>:<spec>"
	kind      string
	spec      string // human-readable schedule spec ("@every 30s" / cron 5-field)
	payload   []byte
	queue     string
	maxRetry  int
	noArch    bool
	misfire   MisfirePolicy
	retention time.Duration
	schedule  cronSchedule
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
	// The schedule ID must disambiguate jobs that share a kind and spec but differ
	// by queue or payload — otherwise their dedup keys, task keys, and lastFired
	// keys collide and all but one job would be silently dropped.
	sum := sha256.Sum256(append([]byte(o.queue+"\x00"), payload...))
	id := fmt.Sprintf("%s:%s#%x", kind, spec, sum[:8])
	s.entries = append(s.entries, &scheduleEntry{
		id: id, kind: kind, spec: spec, payload: payload, queue: o.queue,
		maxRetry: o.maxRetry, noArch: o.noArchive, misfire: o.misfire,
		retention: o.retention, schedule: sched,
	})
}

// RegisterInterval registers args to be enqueued every interval (>= 1s). Any
// sub-second component is truncated to whole seconds (e.g. 1500ms behaves as 1s).
// Register all schedules before calling Start (registration is not concurrency-
// safe with the running tick loop).
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
	// robfig/cron defaults SpecSchedule.Location to time.Local for a spec without a
	// CRON_TZ=/TZ= prefix, and SpecSchedule.Next evaluates in that Location — so
	// SchedulerConfig.Location would otherwise be ignored. Apply cfg.Location when the
	// spec did not set its own zone (an explicit CRON_TZ leaves Location != time.Local
	// and is preserved).
	if ss, ok := sched.(*cron.SpecSchedule); ok && ss.Location == time.Local {
		ss.Location = s.cfg.Location
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
// instance; only the leader enqueues. Register all schedules before Start.
//
// For correct cross-instance behavior, every instance must register the same
// schedules with the same SchedulerConfig.Location and have reasonably
// synchronized clocks: the deterministic dedup key is derived from the computed
// trigger instant, so divergent timezones or badly skewed clocks could let two
// instances treat the same logical trigger as different ones.
func (s *Scheduler) Start(ctx context.Context) error {
	// Publish this instance's schedules to the global registry so inspectors
	// can list registered-but-never-fired schedules. Deterministic IDs make
	// concurrent registration from many instances an idempotent overwrite.
	// Failure is non-fatal: scheduling works without the registry.
	if err := s.rdb.RegisterSchedules(ctx, s.scheduleMetas()); err != nil {
		s.logger.Error("chronos: schedule registry write failed", "error", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go s.run(runCtx)
	return nil
}

// scheduleMetas snapshots the registered schedules as registry entries.
func (s *Scheduler) scheduleMetas() []rdb.ScheduleMeta {
	metas := make([]rdb.ScheduleMeta, 0, len(s.entries))
	for _, e := range s.entries {
		metas = append(metas, rdb.ScheduleMeta{ID: e.id, Kind: e.kind, Queue: e.queue, Spec: e.spec})
	}
	return metas
}

// scheduleIDs lists the registered schedule IDs (for registry heartbeats).
func (s *Scheduler) scheduleIDs() []string {
	ids := make([]string, 0, len(s.entries))
	for _, e := range s.entries {
		ids = append(ids, e.id)
	}
	return ids
}

// run drives leadership renewal and, while leader, the tick loop.
func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()

	// Renew at LeaderTTL/2, clamped to 20s so the registry heartbeat below
	// always beats the Inspector's staleAfter (1 minute) even with a large TTL.
	renewEvery := s.cfg.LeaderTTL / 2
	if renewEvery > 20*time.Second {
		renewEvery = 20 * time.Second
	}
	renew := time.NewTicker(renewEvery)
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
			// Registry heartbeat: refresh last_seen so inspectors can tell
			// live schedules from stale leftovers. Best-effort.
			if err := s.rdb.TouchSchedules(ctx, s.scheduleIDs()); err != nil && ctx.Err() == nil {
				s.logger.Debug("chronos: schedule registry touch failed", "error", err)
			}
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

// tryElect acquires or renews leadership. It is a no-op once the scheduler is
// shutting down, so it cannot re-acquire the lock this instance just resigned
// (which would strand leadership on a dying instance until TTL expiry).
func (s *Scheduler) tryElect(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
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

		// If a real (non-duplicate) enqueue error occurs, do NOT advance
		// lastFired: leaving it put lets the next tick retry the trigger instead
		// of silently losing it. A duplicate means it was already enqueued
		// (by this or another leader), so advancing past it is correct.
		failed := false
		for _, trigger := range fires {
			err := s.enqueueTrigger(ctx, e, trigger)
			if err == nil || errors.Is(err, rdb.ErrDuplicateTask) {
				continue
			}
			failed = true
			if ctx.Err() == nil {
				s.logger.Error("chronos: schedule enqueue failed", "schedule", e.id, "error", err)
			}
		}
		if failed {
			continue
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
		Retention: int64(e.retention / time.Second),
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
