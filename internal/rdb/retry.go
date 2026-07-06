package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// moveToZSetCmd acks a stream entry and moves the task into a target ZSET
// (retry or archived), updating the stored message and state atomically.
// KEYS[1] stream, KEYS[2] task hash, KEYS[3] target zset.
// ARGV[1] group, ARGV[2] streamID, ARGV[3] encoded msg, ARGV[4] state,
// ARGV[5] score, ARGV[6] taskID.
var moveToZSetCmd = redis.NewScript(`
redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[6])
return 1
`)

// Retry acks the active stream entry and moves the task to the retry ZSET with
// score = retryAt. The caller is responsible for having set msg.Retried to the
// desired value; Retry persists msg as given and sets its state to retry.
func (r *RDB) Retry(ctx context.Context, qname, streamID string, msg *base.TaskMessage, retryAt time.Time) error {
	return r.moveToZSet(ctx, qname, streamID, msg, base.RetryKey(qname), base.StateRetry, retryAt.Unix())
}

// Archive acks the active stream entry and moves the task to the archived ZSET
// (dead-letter) with score = diedAt. Archiving is terminal, so the task's
// unique lock (if any) is released.
func (r *RDB) Archive(ctx context.Context, qname, streamID string, msg *base.TaskMessage, diedAt time.Time) error {
	if err := r.moveToZSet(ctx, qname, streamID, msg, base.ArchivedKey(qname), base.StateArchived, diedAt.Unix()); err != nil {
		return err
	}
	return r.releaseUnique(ctx, msg)
}

func (r *RDB) moveToZSet(ctx context.Context, qname, streamID string, msg *base.TaskMessage, zsetKey string, state base.TaskState, score int64) error {
	msg.State = state
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	keys := []string{base.StreamKey(qname), base.TaskKey(qname, msg.ID), zsetKey}
	argv := []interface{}{ConsumerGroup, streamID, encoded, int(state), score, msg.ID}
	return moveToZSetCmd.Run(ctx, r.client, keys, argv...).Err()
}
