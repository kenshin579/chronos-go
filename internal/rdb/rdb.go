// Package rdb implements the Redis operations backing chronos-go: enqueueing
// tasks, dequeueing via a consumer group, and acking completion.
package rdb

import (
	"context"

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
