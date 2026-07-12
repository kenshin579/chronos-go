// Package rdb implements the Redis operations backing chronos-go: enqueueing
// tasks, dequeueing via a consumer group, and acking completion.
package rdb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// ConsumerGroup is the single consumer group name used on every queue stream.
const ConsumerGroup = "chronos"

// RDB wraps a Redis client with chronos-go's task operations.
type RDB struct {
	client redis.UniversalClient

	// knownQueues caches queue names this instance has already registered in
	// the global queue index (SADD chronos:queues), so the hot enqueue path
	// pays that extra round trip only once per queue per process. Registration
	// is idempotent, so a lost cache (new process) just re-registers.
	knownQueues sync.Map // queue name -> struct{}
}

// NewRDB returns an RDB backed by the given Redis client.
func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{client: client}
}

// registerQueue adds qname to the global queue index, skipping the round trip
// when this instance has already done so. See knownQueues.
func (r *RDB) registerQueue(ctx context.Context, qname string) error {
	if _, ok := r.knownQueues.Load(qname); ok {
		return nil
	}
	if err := r.client.SAdd(ctx, base.QueuesKey(), qname).Err(); err != nil {
		return err
	}
	r.knownQueues.Store(qname, struct{}{})
	return nil
}

// Client exposes the underlying Redis client (used by higher layers for
// consumer-group setup and shutdown).
func (r *RDB) Client() redis.UniversalClient {
	return r.client
}

// enqueueCmd atomically stores the task body and appends its ID to the stream.
// It also clears any stale completed/archived ZSET entry left by a previous task
// with the same ID — otherwise the janitor would later delete the new task's hash
// when the stale entry expires.
// KEYS[1] task hash, KEYS[2] stream, KEYS[3] completed zset, KEYS[4] archived zset.
// ARGV[1] encoded msg, ARGV[2] state, ARGV[3] task id.
var enqueueCmd = redis.NewScript(`
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("XADD", KEYS[2], "*", "task_id", ARGV[3])
redis.call("ZREM", KEYS[3], ARGV[3])
redis.call("ZREM", KEYS[4], ARGV[3])
return 1
`)

// Enqueue stores a task and makes it immediately available for processing.
func (r *RDB) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	msg.State = base.StatePending
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}

	// Register the queue name in the global index (cached — one round trip per
	// queue per process). Separate from the atomic script because QueuesKey has
	// no hash tag (different cluster slot).
	if err := r.registerQueue(ctx, msg.Queue); err != nil {
		return err
	}

	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.StreamKey(msg.Queue),
		base.CompletedKey(msg.Queue),
		base.ArchivedKey(msg.Queue),
	}
	argv := []interface{}{encoded, int(base.StatePending), msg.ID}
	return enqueueCmd.Run(ctx, r.client, keys, argv...).Err()
}

// ErrNoTask is returned by Dequeue when no task is available within the block
// duration.
var ErrNoTask = errors.New("chronos: no task available")

// EnsureGroup creates the consumer group on a queue's stream if it does not
// already exist. MKSTREAM creates the stream too, so this is safe to call
// before any task has been enqueued. The group starts at "0" (the beginning of
// the stream), not "$": tasks enqueued before the first server starts must
// still be delivered — with "$" they would be invisible to XREADGROUP forever.
// Already-acked entries are XDEL'd, so starting at "0" never replays old work.
func (r *RDB) EnsureGroup(ctx context.Context, qname string) error {
	err := r.client.XGroupCreateMkStream(ctx, base.StreamKey(qname), ConsumerGroup, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Dequeue reads one task from the given queue using the consumer group. block
// is the max duration to wait for a task (0 means return immediately). It
// returns ErrNoTask when nothing is available. The task's state is set to
// active. streamID identifies the stream entry for later acking.
//
// Dequeue reads a single stream so that a message delivered by XREADGROUP is
// never dropped: with multiple STREAMS Redis may return entries for several
// streams in one call, but reading only the first would leave the rest in the
// consumer's PEL (already delivered via ">", so unrecoverable in M1). Callers
// scan queues one at a time to honor priority.
func (r *RDB) Dequeue(ctx context.Context, consumer string, block time.Duration, qname string) (*base.TaskMessage, string, error) {
	streamKey := base.StreamKey(qname)

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
		Streams:  []string{streamKey, ">"},
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

	entry := res[0].Messages[0]
	taskID, _ := entry.Values["task_id"].(string)

	raw, err := r.client.HGet(ctx, base.TaskKey(qname, taskID), "msg").Result()
	if err == redis.Nil {
		// Orphan stream entry (body already gone): ack, delete it, report no task.
		pipe := r.client.TxPipeline()
		pipe.XAck(ctx, streamKey, ConsumerGroup, entry.ID)
		pipe.XDel(ctx, streamKey, entry.ID)
		_, _ = pipe.Exec(ctx)
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

// releaseUniqueCmd deletes the unique lock only if it still points at this task
// (so a lock re-acquired by a later task is not clobbered).
// KEYS[1] unique key. ARGV[1] taskID.
var releaseUniqueCmd = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("DEL", KEYS[1])
end
return 1
`)

// Done acknowledges a successfully processed task. With no retention it acks
// the stream entry, deletes the task body, and releases the unique lock (if
// any). With msg.Retention > 0 it keeps the task in the completed ZSET until
// completed-at + retention (the janitor removes it later); the unique lock is
// still released immediately — a retained completed task must not block a new
// identical enqueue.
func (r *RDB) Done(ctx context.Context, qname, streamID string, msg *base.TaskMessage) error {
	if msg.Retention > 0 {
		now := time.Now()
		msg.CompletedAt = now.Unix()
		expireAt := now.Unix() + msg.Retention
		if err := r.moveToZSet(ctx, qname, streamID, msg, base.CompletedKey(qname), base.StateCompleted, expireAt); err != nil {
			return err
		}
		return r.releaseUnique(ctx, msg)
	}

	pipe := r.client.TxPipeline()
	pipe.XAck(ctx, base.StreamKey(qname), ConsumerGroup, streamID)
	pipe.XDel(ctx, base.StreamKey(qname), streamID)
	pipe.Del(ctx, base.TaskKey(qname, msg.ID))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return r.releaseUnique(ctx, msg)
}

// releaseUnique releases the task's unique lock if it holds one.
func (r *RDB) releaseUnique(ctx context.Context, msg *base.TaskMessage) error {
	if msg.UniqueKey == "" {
		return nil
	}
	return releaseUniqueCmd.Run(ctx, r.client, []string{msg.UniqueKey}, msg.ID).Err()
}
