// Package jobscheduler is a verification adapter proving chronos-go can back
// operator-review's string+[]byte JobScheduler interface. It bridges that
// interface onto chronos-go's generic API via a single rawTask type routed by
// name. It is an example/migration aid, not part of chronos-go's core API.
package jobscheduler

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
)

// SchedulerJobFunc runs a scheduled job (no payload).
type SchedulerJobFunc func(ctx context.Context) error

// TaskHandlerFunc handles an enqueued task's raw payload.
type TaskHandlerFunc func(ctx context.Context, payload []byte) error

// EnqueueConfig / EnqueueOption mirror operator-review's enqueue options.
type EnqueueConfig struct {
	QueueKey  string
	UniqueTTL time.Duration
}

type EnqueueOption func(*EnqueueConfig)

func WithQueueKey(key string) EnqueueOption { return func(c *EnqueueConfig) { c.QueueKey = key } }
func WithUniqueTTL(ttl time.Duration) EnqueueOption {
	return func(c *EnqueueConfig) { c.UniqueTTL = ttl }
}

// JobScheduler is the operator-review interface this adapter proves chronos-go can back.
type JobScheduler interface {
	Start() error
	Shutdown()
	RegisterScheduledJob(taskName string, interval time.Duration, fn SchedulerJobFunc) (string, error)
	RegisterCronJob(taskName, cronSpec string, fn SchedulerJobFunc) (string, error)
	RegisterTaskHandler(taskName string, fn TaskHandlerFunc) error
	Enqueue(ctx context.Context, taskName string, payload []byte, opts ...EnqueueOption) error
}

// rawTask is the single bridge type carrying a string task name + raw payload
// through chronos-go's generic pipeline.
type rawTask struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

func (rawTask) Kind() string { return "chronos:compat:raw" }

type adapter struct {
	client    *chronos.Client
	server    *chronos.Server
	scheduler *chronos.Scheduler
	mux       *chronos.Mux
	queues    []string

	mu           sync.Mutex
	schedulerJob map[string]SchedulerJobFunc
	taskHandler  map[string]TaskHandlerFunc

	ctx    context.Context
	cancel context.CancelFunc
}

// New returns a JobScheduler backed by chronos-go, distributing work across the
// given queue names (like operator-review's FNV hashing).
func New(rdb redis.UniversalClient, queueNames []string) JobScheduler {
	if len(queueNames) == 0 {
		queueNames = []string{chronos.DefaultQueue}
	}
	queues := make(map[string]int, len(queueNames))
	for _, q := range queueNames {
		queues[q] = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &adapter{
		client:       chronos.NewClient(rdb),
		server:       chronos.NewServer(rdb, chronos.ServerConfig{Queues: queues, Concurrency: 8}),
		scheduler:    chronos.NewScheduler(rdb, chronos.SchedulerConfig{Location: time.Local}),
		mux:          chronos.NewMux(),
		queues:       queueNames,
		schedulerJob: make(map[string]SchedulerJobFunc),
		taskHandler:  make(map[string]TaskHandlerFunc),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// queueFor hashes a key to one of the configured queues (FNV-1a), matching
// operator-review's load distribution.
func (a *adapter) queueFor(key string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return a.queues[int(h.Sum32()%uint32(len(a.queues)))]
}

func (a *adapter) checkDup(taskName string) error {
	if _, ok := a.schedulerJob[taskName]; ok {
		return fmt.Errorf("jobscheduler: duplicate task name %q", taskName)
	}
	if _, ok := a.taskHandler[taskName]; ok {
		return fmt.Errorf("jobscheduler: duplicate task name %q", taskName)
	}
	return nil
}

func (a *adapter) RegisterScheduledJob(taskName string, interval time.Duration, fn SchedulerJobFunc) (string, error) {
	if interval < time.Second {
		return "", errors.New("jobscheduler: interval must be >= 1s")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkDup(taskName); err != nil {
		return "", err
	}
	a.schedulerJob[taskName] = fn
	if err := chronos.RegisterInterval(a.scheduler, interval, rawTask{Name: taskName},
		chronos.WithQueue(a.queueFor(taskName)), chronos.WithMaxRetry(0)); err != nil {
		return "", err
	}
	return taskName, nil
}

func (a *adapter) RegisterCronJob(taskName, cronSpec string, fn SchedulerJobFunc) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkDup(taskName); err != nil {
		return "", err
	}
	if err := chronos.RegisterCron(a.scheduler, cronSpec, rawTask{Name: taskName},
		chronos.WithQueue(a.queueFor(taskName)), chronos.WithMaxRetry(0)); err != nil {
		return "", err
	}
	a.schedulerJob[taskName] = fn
	return taskName, nil
}

func (a *adapter) RegisterTaskHandler(taskName string, fn TaskHandlerFunc) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkDup(taskName); err != nil {
		return err
	}
	a.taskHandler[taskName] = fn
	return nil
}

func (a *adapter) Enqueue(ctx context.Context, taskName string, payload []byte, opts ...EnqueueOption) error {
	cfg := EnqueueConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	key := cfg.QueueKey
	if key == "" {
		key = taskName
	}
	chOpts := []chronos.Option{chronos.WithQueue(a.queueFor(key)), chronos.WithMaxRetry(0)}
	if cfg.UniqueTTL > 0 {
		chOpts = append(chOpts, chronos.WithUnique(cfg.UniqueTTL))
	}
	_, err := chronos.Enqueue(ctx, a.client, rawTask{Name: taskName, Data: payload}, chOpts...)
	if errors.Is(err, chronos.ErrDuplicateTask) {
		return nil // operator-review skips duplicates silently
	}
	return err
}

func (a *adapter) Start() error {
	// One bridge handler routes every rawTask to its registered func by name.
	chronos.AddHandler(a.mux, func(ctx context.Context, t *chronos.Task[rawTask]) error {
		a.mu.Lock()
		sjob, sok := a.schedulerJob[t.Args.Name]
		thandler, tok := a.taskHandler[t.Args.Name]
		a.mu.Unlock()
		switch {
		case sok:
			return sjob(ctx)
		case tok:
			return thandler(ctx, t.Args.Data)
		default:
			return fmt.Errorf("jobscheduler: no handler for task %q", t.Args.Name)
		}
	})
	if err := a.server.Start(a.ctx, a.mux); err != nil {
		return err
	}
	return a.scheduler.Start(a.ctx)
}

func (a *adapter) Shutdown() {
	a.cancel()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.scheduler.Shutdown(shutCtx)
	_ = a.server.Shutdown(shutCtx)
	_ = a.client.Close()
}
