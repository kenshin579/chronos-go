package rdb

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// QueueStats holds per-state task counts for a queue.
type QueueStats struct {
	Queue     string
	Pending   int64 // in the stream, not yet delivered to a worker
	Active    int64 // delivered, not yet acked (consumer group PEL)
	Scheduled int64
	Retry     int64
	Archived  int64
}

// QueueStats returns the per-state task counts for a queue.
func (r *RDB) QueueStats(ctx context.Context, qname string) (*QueueStats, error) {
	streamKey := base.StreamKey(qname)

	xlen, err := r.client.XLen(ctx, streamKey).Result()
	if err != nil {
		return nil, err
	}
	// A queue that has only ever held scheduled tasks has no consumer group yet
	// (the group is created lazily on the first Dequeue/Server start). Treat that
	// as zero active so read-only inspection never has to create the group.
	var active int64
	pending, err := r.client.XPending(ctx, streamKey, ConsumerGroup).Result()
	switch {
	case err == nil:
		active = pending.Count // entries currently in the PEL
	case strings.HasPrefix(err.Error(), "NOGROUP"):
		active = 0
	default:
		return nil, err
	}

	scheduled, err := r.client.ZCard(ctx, base.ScheduledKey(qname)).Result()
	if err != nil {
		return nil, err
	}
	retry, err := r.client.ZCard(ctx, base.RetryKey(qname)).Result()
	if err != nil {
		return nil, err
	}
	archived, err := r.client.ZCard(ctx, base.ArchivedKey(qname)).Result()
	if err != nil {
		return nil, err
	}

	streamPending := xlen - active // stream total minus in-flight = not-yet-delivered
	if streamPending < 0 {
		streamPending = 0
	}
	return &QueueStats{
		Queue:     qname,
		Pending:   streamPending,
		Active:    active,
		Scheduled: scheduled,
		Retry:     retry,
		Archived:  archived,
	}, nil
}

// Queues returns the names of all known queues.
func (r *RDB) Queues(ctx context.Context) ([]string, error) {
	return r.client.SMembers(ctx, base.QueuesKey()).Result()
}

// ListZSetTasks returns up to limit task messages referenced by a state ZSET
// (scheduled / retry / archived), ordered by score (soonest / oldest first).
func (r *RDB) ListZSetTasks(ctx context.Context, qname, zsetKey string, limit int) ([]*base.TaskMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	ids, err := r.client.ZRange(ctx, zsetKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	tasks := make([]*base.TaskMessage, 0, len(ids))
	for _, id := range ids {
		msg, err := r.GetTask(ctx, qname, id)
		if err == redis.Nil {
			continue // body gone; skip
		}
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, msg)
	}
	return tasks, nil
}

// GetTask reads a single task's message by ID. Returns redis.Nil if absent.
func (r *RDB) GetTask(ctx context.Context, qname, taskID string) (*base.TaskMessage, error) {
	raw, err := r.client.HGet(ctx, base.TaskKey(qname, taskID), "msg").Result()
	if err != nil {
		return nil, err
	}
	return base.DecodeMessage([]byte(raw))
}

// runTaskCmd moves a task from whichever state ZSET holds it into the stream for
// immediate processing. It only promotes a task that was actually removed from a
// state ZSET (scheduled/retry/archived): if the task is not in any of them (e.g.
// it is already pending or in-flight/active), it does nothing. This prevents a
// duplicate stream entry (double execution) and makes concurrent RunTask calls
// safe — only the one that wins the ZREM promotes.
// KEYS[1] scheduled, KEYS[2] retry, KEYS[3] archived, KEYS[4] stream, KEYS[5] task hash.
// ARGV[1] taskID, ARGV[2] pending state.
var runTaskCmd = redis.NewScript(`
local removed = redis.call("ZREM", KEYS[1], ARGV[1]) + redis.call("ZREM", KEYS[2], ARGV[1]) + redis.call("ZREM", KEYS[3], ARGV[1])
if removed == 0 then
  return 0
end
if redis.call("EXISTS", KEYS[5]) == 0 then
  return 0
end
redis.call("XADD", KEYS[4], "*", "task_id", ARGV[1])
redis.call("HSET", KEYS[5], "state", ARGV[2])
return 1
`)

// RunTask promotes a scheduled/retry/archived task to the stream so it runs now.
func (r *RDB) RunTask(ctx context.Context, qname, taskID string) error {
	keys := []string{
		base.ScheduledKey(qname), base.RetryKey(qname), base.ArchivedKey(qname),
		base.StreamKey(qname), base.TaskKey(qname, taskID),
	}
	return runTaskCmd.Run(ctx, r.client, keys, taskID, int(base.StatePending)).Err()
}

// DeleteTask removes a task from all state ZSETs and deletes its body, releasing
// any unique lock it holds. It is intended for scheduled/retry/archived tasks.
// If called on an in-flight (pending/active) task, deleting the body leaves an
// orphan stream/PEL entry; that orphan is harmless and is trimmed (XACK+XDEL) by
// the next dequeue or recover pass.
func (r *RDB) DeleteTask(ctx context.Context, qname, taskID string) error {
	msg, err := r.GetTask(ctx, qname, taskID)
	if err != nil && err != redis.Nil {
		return err
	}
	pipe := r.client.TxPipeline()
	pipe.ZRem(ctx, base.ScheduledKey(qname), taskID)
	pipe.ZRem(ctx, base.RetryKey(qname), taskID)
	pipe.ZRem(ctx, base.ArchivedKey(qname), taskID)
	pipe.Del(ctx, base.TaskKey(qname, taskID))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	if msg != nil {
		return r.releaseUnique(ctx, msg)
	}
	return nil
}
