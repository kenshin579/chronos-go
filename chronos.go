package chronos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// DefaultQueue is the queue used when none is specified.
const DefaultQueue = "default"

// DefaultMaxRetry is the retry budget used when WithMaxRetry is not given.
const DefaultMaxRetry = 25

// ErrDuplicateTask is returned by Enqueue when WithUnique is used and an
// identical task already holds the unique lock.
var ErrDuplicateTask = rdb.ErrDuplicateTask

// TaskArgs is implemented by every task payload type. Kind returns a stable
// identifier used to route the task to its handler; it MUST be defined on a
// value receiver so it can be called on the zero value during registration.
type TaskArgs interface {
	Kind() string
}

// Task is a strongly-typed task delivered to a handler.
type Task[T TaskArgs] struct {
	Args T

	id           string
	queue        string
	prevResult   []byte
	groupResults [][]byte
}

// ID returns the task's unique identifier.
func (t *Task[T]) ID() string { return t.id }

// Queue returns the queue the task was enqueued to.
func (t *Task[T]) Queue() string { return t.queue }

// ErrNoResult is returned when the previous step (or a group member) produced
// no result — the handler was registered with AddHandler, or this is the
// first chain link.
var ErrNoResult = errors.New("chronos: no result")

// PrevResult decodes the previous chain link's result:
//
//	out, err := chronos.PrevResult[EncodeResult](task)
func PrevResult[R any, T TaskArgs](t *Task[T]) (R, error) {
	var out R
	if len(t.prevResult) == 0 {
		return out, ErrNoResult
	}
	if err := json.Unmarshal(t.prevResult, &out); err != nil {
		return out, fmt.Errorf("chronos: decode prev result: %w", err)
	}
	return out, nil
}

// GroupResults decodes every member result in Add order. It assumes a
// homogeneous group (every member returned R); a member without a result
// fails with ErrNoResult. For heterogeneous or partial results use
// RawGroupResults.
func GroupResults[R any, T TaskArgs](t *Task[T]) ([]R, error) {
	if t.groupResults == nil {
		return nil, ErrNoResult
	}
	out := make([]R, len(t.groupResults))
	for i, raw := range t.groupResults {
		if len(raw) == 0 {
			return nil, fmt.Errorf("chronos: group member %d: %w", i, ErrNoResult)
		}
		if err := json.Unmarshal(raw, &out[i]); err != nil {
			return nil, fmt.Errorf("chronos: decode group member %d result: %w", i, err)
		}
	}
	return out, nil
}

// RawGroupResults returns raw member results in Add order (nil = no result).
// Nil when this task is not a group callback or no member produced a result.
func (t *Task[T]) RawGroupResults() [][]byte { return t.groupResults }

// TaskInfo describes an enqueued or stored task. Enqueue returns one with only
// ID/Kind/Queue set; the Inspector fills the rest for stored tasks.
type TaskInfo struct {
	ID    string
	Kind  string
	Queue string

	// The following are populated by Inspector.ListTasks / GetTask for tasks
	// stored in a state ZSET (scheduled / retry / archived).
	State         string    // "scheduled" | "retry" | "archived" | ...
	Payload       []byte    // raw task payload
	Retried       int       // retries already attempted
	MaxRetry      int       // retry budget
	LastErr       string    // most recent failure message ("" if none)
	NextProcessAt time.Time // ZSET score as a time: scheduled-for / retry-at / died-at / expires-at (completed)
	CompletedAt   time.Time // when the task finished successfully (zero unless retained)

	ChainPending int      // number of chain links still queued behind this task (0 = none/last)
	ChainIndex   int      // this task's 0-based position in its chain
	ChainNext    []string // kinds of the links waiting behind this task, in order

	GroupID      string // group this task belongs to ("" = none)
	GroupPending int    // members of that group not yet succeeded; populated by GetTask only. 0 also when the group finished or its record expired (see rdb.GroupTTL) or the lookup failed — it is a hint, not an authority.
	GroupQueue   string // queue holding the group's state (= callback queue)

	// HasResult reports whether the handler produced a result (AddHandlerR).
	// The result itself travels the workflow (PrevResult/GroupResults); the
	// Inspector only reports its presence and size.
	HasResult bool
	// ResultSize is the result's JSON size in bytes (0 when HasResult=false).
	ResultSize int
}

// Client enqueues tasks.
type Client struct {
	rdb *rdb.RDB
}

// NewClient returns a Client backed by the given Redis client.
func NewClient(r redis.UniversalClient) *Client {
	return &Client{rdb: rdb.NewRDB(r)}
}

// Close releases the client's resources. The underlying Redis client is owned
// by the caller and is not closed here.
func (c *Client) Close() error { return nil }

// enqueueOptions holds resolved enqueue-time settings.
type enqueueOptions struct {
	queue             string
	taskID            string
	maxRetry          int
	noArchive         bool
	processAt         time.Time     // zero = immediate
	processAtAbsolute bool          // set by WithProcessAt (not WithProcessIn)
	uniqueTTL         time.Duration // > 0 enables unique deduplication
	misfire           MisfirePolicy // used by scheduler registrations only
	retention         time.Duration // > 0 keeps the completed task for inspection
}

// Option customizes a single Enqueue call.
type Option interface {
	apply(*enqueueOptions)
}

type optionFunc func(*enqueueOptions)

func (f optionFunc) apply(o *enqueueOptions) { f(o) }

// WithQueue routes the task to a specific queue.
func WithQueue(name string) Option {
	return optionFunc(func(o *enqueueOptions) { o.queue = name })
}

// WithTaskID sets an explicit task ID (used for lookup and correlation). When
// omitted a random UUID is generated.
//
// This is not deduplication: enqueueing twice with the same ID does not reject
// the second call — it may overwrite the existing task's body and enqueue it
// again. For content-based duplicate suppression use WithUnique.
func WithTaskID(id string) Option {
	return optionFunc(func(o *enqueueOptions) { o.taskID = id })
}

// WithMaxRetry sets the maximum number of retries before the task is
// dead-lettered. Defaults to DefaultMaxRetry.
func WithMaxRetry(n int) Option {
	return optionFunc(func(o *enqueueOptions) {
		if n < 0 {
			n = 0
		}
		o.maxRetry = n
	})
}

// WithDeadLetterDiscard discards the task on retry exhaustion instead of storing
// it in the archived ZSET. The OnDeadLetter hook still fires.
func WithDeadLetterDiscard() Option {
	return optionFunc(func(o *enqueueOptions) { o.noArchive = true })
}

// WithProcessAt schedules the task to first become available at t. A non-future
// time enqueues immediately.
func WithProcessAt(t time.Time) Option {
	return optionFunc(func(o *enqueueOptions) {
		o.processAt = t
		o.processAtAbsolute = true
	})
}

// WithProcessIn schedules the task to first become available after d. A
// non-positive d enqueues immediately.
func WithProcessIn(d time.Duration) Option {
	return optionFunc(func(o *enqueueOptions) { o.processAt = time.Now().Add(d) })
}

// WithUnique deduplicates tasks by (kind + payload): while a matching task is
// anywhere in the pipeline (pending, retrying, scheduled), enqueueing another
// returns ErrDuplicateTask. The lock is released when the task reaches a
// terminal state (completed / archived / discarded).
//
// While a task is actively being processed, the server's heartbeat renews the
// lock's TTL, so a single attempt that runs longer than ttl still holds the
// lock. ttl mainly bounds the lock for the time a task spends waiting (pending /
// scheduled / retry backoff) where no worker is renewing it; set it comfortably
// above the expected total waiting time. For a delayed task the lock TTL is
// automatically extended to cover the delay.
func WithUnique(ttl time.Duration) Option {
	return optionFunc(func(o *enqueueOptions) {
		if ttl > 0 {
			o.uniqueTTL = ttl
		}
	})
}

// WithRetention keeps the task in the "completed" set for d after it succeeds,
// so it can be inspected (Inspector/CLI state "completed") before the janitor
// removes it. The default (no option) deletes the task immediately on success.
// Durations under one second are rounded up to one second; d <= 0 is ignored.
// Note: on high-throughput queues a long retention grows Redis memory — the
// per-queue MaxCompleted cap (ServerConfig) bounds the worst case.
func WithRetention(d time.Duration) Option {
	return optionFunc(func(o *enqueueOptions) {
		if d <= 0 {
			o.retention = 0
			return
		}
		if d < time.Second {
			d = time.Second
		}
		o.retention = d
	})
}

// WithMisfirePolicy sets how a scheduled job handles missed triggers (after a
// leader-election gap or downtime). Only meaningful for RegisterInterval /
// RegisterCron; ignored by a plain Enqueue. Defaults to MisfireSkip.
func WithMisfirePolicy(p MisfirePolicy) Option {
	return optionFunc(func(o *enqueueOptions) { o.misfire = p })
}

// Enqueue serializes args and makes the task available for immediate
// processing. It is a package-level function rather than a method because Go
// methods cannot have type parameters.
func Enqueue[T TaskArgs](ctx context.Context, c *Client, args T, opts ...Option) (*TaskInfo, error) {
	options := enqueueOptions{queue: DefaultQueue, maxRetry: DefaultMaxRetry}
	for _, opt := range opts {
		opt.apply(&options)
	}

	id := options.taskID
	if id == "" {
		id = uuid.NewString()
	}

	payload, err := encodeArgs(args)
	if err != nil {
		return nil, err
	}

	msg := &base.TaskMessage{
		ID:        id,
		Kind:      args.Kind(),
		Payload:   payload,
		Queue:     options.queue,
		MaxRetry:  options.maxRetry,
		NoArchive: options.noArchive,
		Retention: int64(options.retention / time.Second),
	}
	if err := dispatchMessage(ctx, c, msg, options); err != nil {
		return nil, err
	}

	return &TaskInfo{ID: id, Kind: msg.Kind, Queue: msg.Queue}, nil
}

// dispatchMessage routes an assembled message to the right rdb enqueue path
// (immediate / scheduled / unique variants), shared by Enqueue and Chain.
func dispatchMessage(ctx context.Context, c *Client, msg *base.TaskMessage, options enqueueOptions) error {
	scheduled := !options.processAt.IsZero() && options.processAt.After(time.Now())
	unique := options.uniqueTTL > 0
	if unique {
		msg.UniqueKey = base.UniqueKey(msg.Queue, base.UniqueSuffix(msg.Kind, msg.Payload))
	}
	switch {
	case unique && scheduled:
		// The lock must outlive the delay, or it would expire before the task is
		// even promoted, silently breaking dedup. Extend it to cover the delay
		// plus the caller's ttl (the post-availability safety window).
		return c.rdb.ScheduleUnique(ctx, msg, options.processAt, options.uniqueTTL+time.Until(options.processAt))
	case unique:
		return c.rdb.EnqueueUnique(ctx, msg, options.uniqueTTL)
	case scheduled:
		return c.rdb.Schedule(ctx, msg, options.processAt)
	default:
		return c.rdb.Enqueue(ctx, msg)
	}
}
