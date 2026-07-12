package rdb

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// ErrDuplicateTask is returned when a unique lock is already held for an
// identical (kind + payload) task.
var ErrDuplicateTask = errors.New("chronos: duplicate task")

// enqueueUniqueCmd acquires the unique lock (SET NX PX) and, only on success,
// stores the task and appends it to the stream — atomically. Returns -1 when
// the lock is already held. TTL is milliseconds (PX) so sub-second TTLs work.
// On success it also clears any stale completed/archived ZSET entry left by a
// previous task with the same ID — otherwise the janitor would later delete the
// new task's hash when the stale entry expires.
// KEYS[1] unique lock, KEYS[2] task hash, KEYS[3] stream,
// KEYS[4] completed zset, KEYS[5] archived zset.
// ARGV[1] taskID, ARGV[2] ttl millis, ARGV[3] encoded msg, ARGV[4] state.
var enqueueUniqueCmd = redis.NewScript(`
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", tonumber(ARGV[2])) == false then
  return -1
end
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("XADD", KEYS[3], "*", "task_id", ARGV[1])
redis.call("ZREM", KEYS[4], ARGV[1])
redis.call("ZREM", KEYS[5], ARGV[1])
return 1
`)

// scheduleUniqueCmd is enqueueUniqueCmd's delayed counterpart: on lock success
// it stores the task, adds it to the scheduled ZSET, and clears any stale
// completed/archived ZSET entry for the same ID.
// KEYS[1] unique lock, KEYS[2] task hash, KEYS[3] scheduled zset,
// KEYS[4] completed zset, KEYS[5] archived zset.
// ARGV[1] taskID, ARGV[2] ttl millis, ARGV[3] encoded msg, ARGV[4] state, ARGV[5] score.
var scheduleUniqueCmd = redis.NewScript(`
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", tonumber(ARGV[2])) == false then
  return -1
end
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[1])
redis.call("ZREM", KEYS[4], ARGV[1])
redis.call("ZREM", KEYS[5], ARGV[1])
return 1
`)

// uniqueTTLMillis converts a TTL duration to the millisecond value passed to the
// SET PX scripts, clamping to at least 1ms (SET PX 0 is an error).
func uniqueTTLMillis(ttl time.Duration) int {
	ms := ttl.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return int(ms)
}

// EnqueueUnique enqueues a task for immediate processing, acquiring its unique
// lock first. Returns ErrDuplicateTask if an identical task's lock is held. msg
// must have UniqueKey set (see base.UniqueKey/UniqueSuffix). uniqueTTL is the
// lock's orphan-safety expiry; the lock is released early when the task reaches
// a terminal state.
func (r *RDB) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, uniqueTTL time.Duration) error {
	msg.State = base.StatePending
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.registerQueue(ctx, msg.Queue); err != nil {
		return err
	}
	keys := []string{
		msg.UniqueKey, base.TaskKey(msg.Queue, msg.ID), base.StreamKey(msg.Queue),
		base.CompletedKey(msg.Queue), base.ArchivedKey(msg.Queue),
	}
	argv := []interface{}{msg.ID, uniqueTTLMillis(uniqueTTL), encoded, int(base.StatePending)}
	res, err := enqueueUniqueCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return err
	}
	if res == -1 {
		return ErrDuplicateTask
	}
	return nil
}

// ScheduleUnique is EnqueueUnique for delayed tasks (adds to the scheduled ZSET
// at processAt instead of the stream).
func (r *RDB) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, uniqueTTL time.Duration) error {
	msg.State = base.StateScheduled
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.registerQueue(ctx, msg.Queue); err != nil {
		return err
	}
	keys := []string{
		msg.UniqueKey, base.TaskKey(msg.Queue, msg.ID), base.ScheduledKey(msg.Queue),
		base.CompletedKey(msg.Queue), base.ArchivedKey(msg.Queue),
	}
	argv := []interface{}{msg.ID, uniqueTTLMillis(uniqueTTL), encoded, int(base.StateScheduled), scheduleScore(processAt)}
	res, err := scheduleUniqueCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return err
	}
	if res == -1 {
		return ErrDuplicateTask
	}
	return nil
}
