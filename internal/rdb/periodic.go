package rdb

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// enqueuePeriodicCmd acquires a per-trigger dedup key (SET NX PX) and, only on
// success, stores the task and appends it to the stream. Returns -1 if the
// trigger was already enqueued. Unlike the unique lock, the dedup key is NOT
// recorded on the task, so it is never released early — it expires by TTL only,
// which is what fences a split-brain / late leader from re-enqueueing the same
// trigger.
// On success it also clears any stale completed/archived ZSET entry left by a
// previous task with the same ID — otherwise the janitor would later delete the
// new task's hash when the stale entry expires.
// KEYS[1] dedup key, KEYS[2] task hash, KEYS[3] stream,
// KEYS[4] completed zset, KEYS[5] archived zset.
// ARGV[1] taskID, ARGV[2] dedup ttl millis, ARGV[3] encoded msg, ARGV[4] state.
var enqueuePeriodicCmd = redis.NewScript(`
if redis.call("SET", KEYS[1], "1", "NX", "PX", tonumber(ARGV[2])) == false then
  return -1
end
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("XADD", KEYS[3], "*", "task_id", ARGV[1])
redis.call("ZREM", KEYS[4], ARGV[1])
redis.call("ZREM", KEYS[5], ARGV[1])
return 1
`)

// EnqueuePeriodic enqueues a scheduled trigger's task exactly once per dedupKey.
// Returns ErrDuplicateTask if the trigger was already enqueued (by this or
// another instance).
func (r *RDB) EnqueuePeriodic(ctx context.Context, msg *base.TaskMessage, dedupKey string, dedupTTL time.Duration) error {
	msg.State = base.StatePending
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.registerQueue(ctx, msg.Queue); err != nil {
		return err
	}
	ms := dedupTTL.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	keys := []string{
		dedupKey, base.TaskKey(msg.Queue, msg.ID), base.StreamKey(msg.Queue),
		base.CompletedKey(msg.Queue), base.ArchivedKey(msg.Queue),
	}
	res, err := enqueuePeriodicCmd.Run(ctx, r.client, keys, msg.ID, ms, encoded, int(base.StatePending)).Int()
	if err != nil {
		return err
	}
	if res == -1 {
		return ErrDuplicateTask
	}
	return nil
}

// SetLastFired records the unix time a schedule last fired.
func (r *RDB) SetLastFired(ctx context.Context, scheduleID string, when time.Time) error {
	return r.client.Set(ctx, base.ScheduleLastFiredKey(scheduleID), when.Unix(), 0).Err()
}

// GetLastFired returns the time a schedule last fired. ok is false if unset.
func (r *RDB) GetLastFired(ctx context.Context, scheduleID string) (time.Time, bool, error) {
	raw, err := r.client.Get(ctx, base.ScheduleLastFiredKey(scheduleID)).Result()
	if err == redis.Nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	sec, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, false, err
	}
	return time.Unix(sec, 0), true, nil
}
