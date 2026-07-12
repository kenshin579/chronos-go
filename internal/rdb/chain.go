package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// chainEnqueueCmd enqueues a chain successor only if its task hash does not
// already exist. The guard makes successor enqueueing idempotent: a
// redelivered predecessor (at-least-once) re-runs its handler and re-attempts
// this call, but cannot create a duplicate. (The guard holds only while the
// successor's task hash exists — see the Chain doc comment for the
// redelivery-after-completion caveat.) Deliberately does NOT clear stale
// completed/archived entries (unlike enqueueCmd): if the successor already ran
// and is retained, re-running it would be exactly the duplication this guard
// exists to prevent. Keys share the queue hash tag (cluster-safe).
// KEYS[1] task hash, KEYS[2] stream. ARGV[1] msg, ARGV[2] state, ARGV[3] id.
var chainEnqueueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("XADD", KEYS[2], "*", "task_id", ARGV[3])
return 1
`)

// chainScheduleCmd is chainEnqueueCmd's delayed variant: the successor lands in
// the scheduled ZSET instead of the stream.
// KEYS[1] task hash, KEYS[2] scheduled zset. ARGV[1] msg, ARGV[2] state,
// ARGV[3] score (unix), ARGV[4] id.
var chainScheduleCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
return 1
`)

// EnqueueChainLink makes a chain successor available (immediately, or in the
// scheduled set when delay > 0), unless a task with the same ID already exists
// — then it is a no-op and returns false. See chainEnqueueCmd for why.
func (r *RDB) EnqueueChainLink(ctx context.Context, msg *base.TaskMessage, delay time.Duration) (bool, error) {
	// Register the queue in the global index (same as other enqueue paths;
	// separate command because QueuesKey has no hash tag).
	if err := r.client.SAdd(ctx, base.QueuesKey(), msg.Queue).Err(); err != nil {
		return false, err
	}

	if delay > 0 {
		msg.State = base.StateScheduled
		encoded, err := base.EncodeMessage(msg)
		if err != nil {
			return false, err
		}
		keys := []string{base.TaskKey(msg.Queue, msg.ID), base.ScheduledKey(msg.Queue)}
		argv := []interface{}{encoded, int(base.StateScheduled), time.Now().Add(delay).Unix(), msg.ID}
		n, err := chainScheduleCmd.Run(ctx, r.client, keys, argv...).Int()
		if err != nil {
			return false, err
		}
		return n == 1, nil
	}

	msg.State = base.StatePending
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return false, err
	}
	keys := []string{base.TaskKey(msg.Queue, msg.ID), base.StreamKey(msg.Queue)}
	argv := []interface{}{encoded, int(base.StatePending), msg.ID}
	n, err := chainEnqueueCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
