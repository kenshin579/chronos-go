// Package rdb implements the Redis operations backing chronos-go: enqueueing
// tasks, dequeueing via a consumer group, and acking completion.
package rdb

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// ConsumerGroup is the single consumer group name used on every queue stream.
const ConsumerGroup = "chronos"

// RDB wraps a Redis client with chronos-go's task operations.
type RDB struct {
	client redis.UniversalClient
}

// NewRDB returns an RDB backed by the given Redis client.
func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{client: client}
}

// Client exposes the underlying Redis client (used by higher layers for
// consumer-group setup and shutdown).
func (r *RDB) Client() redis.UniversalClient {
	return r.client
}

// enqueueCmd atomically stores the task body and appends its ID to the stream.
// KEYS[1] task hash, KEYS[2] stream. ARGV[1] encoded msg, ARGV[2] state, ARGV[3] task id.
var enqueueCmd = redis.NewScript(`
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("XADD", KEYS[2], "*", "task_id", ARGV[3])
return 1
`)

// Enqueue stores a task and makes it immediately available for processing.
func (r *RDB) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	msg.State = base.StatePending
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}

	// Register the queue name in the global index. Separate from the atomic
	// script because QueuesKey has no hash tag (different cluster slot).
	if err := r.client.SAdd(ctx, base.QueuesKey(), msg.Queue).Err(); err != nil {
		return err
	}

	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.StreamKey(msg.Queue),
	}
	argv := []interface{}{encoded, int(base.StatePending), msg.ID}
	return enqueueCmd.Run(ctx, r.client, keys, argv...).Err()
}

// ErrNoTask is returned by Dequeue when no task is available within the block
// duration.
var ErrNoTask = errors.New("chronos: no task available")

// EnsureGroup creates the consumer group on a queue's stream if it does not
// already exist. MKSTREAM creates the stream too, so this is safe to call
// before any task has been enqueued.
func (r *RDB) EnsureGroup(ctx context.Context, qname string) error {
	err := r.client.XGroupCreateMkStream(ctx, base.StreamKey(qname), ConsumerGroup, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Dequeue reads one task from the given queues using the consumer group. block
// is the max duration to wait for a task (0 means return immediately). It
// returns ErrNoTask when nothing is available. The task's state is set to
// active. streamID identifies the stream entry for later acking.
func (r *RDB) Dequeue(ctx context.Context, consumer string, block time.Duration, qnames ...string) (*base.TaskMessage, string, error) {
	// Build STREAMS argument: all stream keys, then one ">" per key.
	streams := make([]string, 0, len(qnames)*2)
	for _, q := range qnames {
		streams = append(streams, base.StreamKey(q))
	}
	for range qnames {
		streams = append(streams, ">")
	}

	// go-redis maps XReadGroupArgs.Block >= 0 to a Redis "BLOCK <ms>" option,
	// where "BLOCK 0" means block forever (not "don't block"). To honor this
	// method's documented contract (0 == return immediately), translate a
	// non-positive block into -1, which makes go-redis omit the BLOCK option
	// entirely (an immediate, non-blocking XREADGROUP).
	blockArg := block
	if blockArg <= 0 {
		blockArg = -1
	}

	res, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: consumer,
		Streams:  streams,
		Count:    1,
		Block:    blockArg,
	}).Result()
	if err == redis.Nil {
		return nil, "", ErrNoTask
	}
	if err != nil {
		return nil, "", err
	}
	if len(res) == 0 || len(res[0].Messages) == 0 {
		return nil, "", ErrNoTask
	}

	stream := res[0]
	entry := stream.Messages[0]
	qname := qnameFromStreamKey(stream.Stream)
	taskID, _ := entry.Values["task_id"].(string)

	raw, err := r.client.HGet(ctx, base.TaskKey(qname, taskID), "msg").Result()
	if err == redis.Nil {
		// Orphan stream entry (body already gone): ack and report no task.
		_ = r.client.XAck(ctx, stream.Stream, ConsumerGroup, entry.ID).Err()
		return nil, "", ErrNoTask
	}
	if err != nil {
		return nil, "", err
	}

	msg, err := base.DecodeMessage([]byte(raw))
	if err != nil {
		return nil, "", err
	}
	msg.State = base.StateActive
	if err := r.client.HSet(ctx, base.TaskKey(qname, taskID), "state", int(base.StateActive)).Err(); err != nil {
		return nil, "", err
	}

	return msg, entry.ID, nil
}

// qnameFromStreamKey extracts the queue name from a stream key of the form
// "chronos:{<qname>}:stream".
func qnameFromStreamKey(streamKey string) string {
	start := len("chronos:{")
	end := len(streamKey) - len("}:stream")
	if start >= end {
		return ""
	}
	return streamKey[start:end]
}
