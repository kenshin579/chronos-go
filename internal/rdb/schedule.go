package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// scheduleCmd stores a task body and adds it to the scheduled ZSET (state
// scheduled), from which the forwarder promotes it when its time arrives.
// KEYS[1] task hash, KEYS[2] scheduled zset.
// ARGV[1] encoded msg, ARGV[2] state, ARGV[3] score (process_at), ARGV[4] task id.
var scheduleCmd = redis.NewScript(`
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
return 1
`)

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
	keys := []string{base.TaskKey(msg.Queue, msg.ID), base.ScheduledKey(msg.Queue)}
	argv := []interface{}{encoded, int(base.StateScheduled), processAt.Unix(), msg.ID}
	return scheduleCmd.Run(ctx, r.client, keys, argv...).Err()
}
