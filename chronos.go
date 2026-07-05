// Package chronos is a Redis-backed distributed scheduler and task queue.
package chronos

import (
	"context"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// DefaultQueue is the queue used when none is specified.
const DefaultQueue = "default"

// DefaultMaxRetry is the retry budget used when WithMaxRetry is not given.
const DefaultMaxRetry = 25

// TaskArgs is implemented by every task payload type. Kind returns a stable
// identifier used to route the task to its handler; it MUST be defined on a
// value receiver so it can be called on the zero value during registration.
type TaskArgs interface {
	Kind() string
}

// Task is a strongly-typed task delivered to a handler.
type Task[T TaskArgs] struct {
	Args T

	id    string
	queue string
}

// ID returns the task's unique identifier.
func (t *Task[T]) ID() string { return t.id }

// Queue returns the queue the task was enqueued to.
func (t *Task[T]) Queue() string { return t.queue }

// TaskInfo describes an enqueued task returned by Enqueue.
type TaskInfo struct {
	ID    string
	Kind  string
	Queue string
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
	queue     string
	taskID    string
	maxRetry  int
	noArchive bool
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

// WithTaskID sets an explicit task ID. Enforced deduplication is provided by
// the unique lock introduced in a later milestone; in M1, re-enqueueing with
// the same ID is not guaranteed to prevent duplicates. When omitted a random
// UUID is generated.
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
	}
	if err := c.rdb.Enqueue(ctx, msg); err != nil {
		return nil, err
	}

	return &TaskInfo{ID: id, Kind: msg.Kind, Queue: msg.Queue}, nil
}
