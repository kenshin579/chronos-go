package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// forwardCmd moves due tasks from a source ZSET back into the stream. It reads
// task IDs with score <= now, and for each: appends the ID to the stream, sets
// the task hash state to pending, and removes it from the ZSET.
//
// Only the top-level "state" hash field is updated (not the State inside the
// serialized "msg"); the top-level field is the authoritative state. Readers
// (e.g. a future Inspector) must read "state", not msg.State.
// KEYS[1] source zset, KEYS[2] stream.
// ARGV[1] now (score cutoff), ARGV[2] max count, ARGV[3] task-key prefix,
// ARGV[4] pending state.
var forwardCmd = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
for _, id in ipairs(ids) do
  redis.call("XADD", KEYS[2], "*", "task_id", id)
  redis.call("HSET", ARGV[3] .. id, "state", ARGV[4])
  redis.call("ZREM", KEYS[1], id)
end
return #ids
`)

// ForwardRetry moves tasks whose retry time has arrived (score <= now) from the
// retry ZSET back into the stream for reprocessing. It processes at most max
// tasks and returns how many were forwarded. The computed task-hash keys share
// the queue's hash tag, so the multi-key script is cluster-safe.
func (r *RDB) ForwardRetry(ctx context.Context, qname string, now time.Time, max int) (int, error) {
	keys := []string{base.RetryKey(qname), base.StreamKey(qname)}
	argv := []interface{}{now.Unix(), max, base.TaskKeyPrefix(qname), int(base.StatePending)}
	n, err := forwardCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ForwardScheduled promotes due delayed tasks (score <= now) from the scheduled
// ZSET into the stream. It shares forwardCmd with ForwardRetry.
func (r *RDB) ForwardScheduled(ctx context.Context, qname string, now time.Time, max int) (int, error) {
	keys := []string{base.ScheduledKey(qname), base.StreamKey(qname)}
	argv := []interface{}{scheduleScore(now), max, base.TaskKeyPrefix(qname), int(base.StatePending)}
	n, err := forwardCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}
