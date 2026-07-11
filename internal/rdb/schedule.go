package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// scheduleCmd stores a task body and adds it to the scheduled ZSET (state
// scheduled), from which the forwarder promotes it when its time arrives.
// It also clears any stale completed/archived ZSET entry left by a previous task
// with the same ID — otherwise the janitor would later delete the new task's hash
// when the stale entry expires.
// KEYS[1] task hash, KEYS[2] scheduled zset, KEYS[3] completed zset, KEYS[4] archived zset.
// ARGV[1] encoded msg, ARGV[2] state, ARGV[3] score (process_at), ARGV[4] task id.
var scheduleCmd = redis.NewScript(`
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
redis.call("ZREM", KEYS[3], ARGV[4])
redis.call("ZREM", KEYS[4], ARGV[4])
return 1
`)

// scheduleScore renders a time as fractional Unix seconds for a ZSET score. Its
// int64 truncation equals t.Unix(), but the fractional part preserves
// sub-second precision so a delay shorter than a second is not rounded down to a
// second boundary (which would let the forwarder promote it early).
func scheduleScore(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

// Schedule stores a task for delayed execution at processAt.
func (r *RDB) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	msg.State = base.StateScheduled
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.client.SAdd(ctx, base.QueuesKey(), msg.Queue).Err(); err != nil {
		return err
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ScheduledKey(msg.Queue),
		base.CompletedKey(msg.Queue),
		base.ArchivedKey(msg.Queue),
	}
	argv := []interface{}{encoded, int(base.StateScheduled), scheduleScore(processAt), msg.ID}
	return scheduleCmd.Run(ctx, r.client, keys, argv...).Err()
}
