package rdb

import (
	"context"
	"strconv"
	"strings"
	"sync"

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
	Completed int64
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
	completed, err := r.client.ZCard(ctx, base.CompletedKey(qname)).Result()
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
		Completed: completed,
	}, nil
}

// Queues returns the names of all known queues.
func (r *RDB) Queues(ctx context.Context) ([]string, error) {
	return r.client.SMembers(ctx, base.QueuesKey()).Result()
}

// GroupMemberIDs returns the IDs of a group's not-yet-succeeded members
// (empty when the group finished or its record expired).
func (r *RDB) GroupMemberIDs(ctx context.Context, cbQueue, groupID string) ([]string, error) {
	return r.client.SMembers(ctx, base.GroupKey(cbQueue, groupID)).Result()
}

// LeaderID returns the current scheduler leader's instance ID ("" when no
// leader holds the lock).
func (r *RDB) LeaderID(ctx context.Context) (string, error) {
	id, err := r.client.Get(ctx, base.LeaderKey()).Result()
	if err == redis.Nil {
		return "", nil
	}
	return id, err
}

// ScheduleFireInfo is one schedule that has fired at least once.
type ScheduleFireInfo struct {
	ID        string
	LastFired int64 // unix seconds
}

// ScanSchedules lists schedules that have ever fired, with their last-fired
// times, by scanning the per-schedule last-fired keys. Registered-but-never-
// fired schedules are invisible (they live only in scheduler process memory).
// The keys carry no hash tag, so on a cluster they spread across slots — scan
// every master.
func (r *RDB) ScanSchedules(ctx context.Context) ([]ScheduleFireInfo, error) {
	// Derive the scan pattern (and trim bounds) from the canonical key builder
	// so a key-shape change cannot silently break this scan.
	pattern := base.ScheduleLastFiredKey("*")
	star := strings.Index(pattern, "*")
	prefix, suffix := pattern[:star], pattern[star+1:]

	collect := func(ctx context.Context, c redis.UniversalClient, out *[]ScheduleFireInfo, mu *sync.Mutex) error {
		iter := c.Scan(ctx, 0, prefix+"*"+suffix, 100).Iterator()
		for iter.Next(ctx) {
			key := iter.Val()
			raw, err := r.client.Get(ctx, key).Result() // GET via the routed client
			if err == redis.Nil {
				continue
			}
			if err != nil {
				return err
			}
			ts, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				continue // foreign key shape; skip
			}
			id := strings.TrimSuffix(strings.TrimPrefix(key, prefix), suffix)
			mu.Lock()
			*out = append(*out, ScheduleFireInfo{ID: id, LastFired: ts})
			mu.Unlock()
		}
		return iter.Err()
	}

	var out []ScheduleFireInfo
	var mu sync.Mutex
	if cc, ok := r.client.(*redis.ClusterClient); ok {
		if err := cc.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
			return collect(ctx, m, &out, &mu)
		}); err != nil {
			return nil, err
		}
		return out, nil
	}
	if err := collect(ctx, r.client, &out, &mu); err != nil {
		return nil, err
	}
	return out, nil
}

// ZSetTask is a task referenced by a state ZSET together with its score
// (a Unix timestamp: scheduled-for / retry-at / died-at).
type ZSetTask struct {
	Msg   *base.TaskMessage
	Score float64
}

// ListZSetTasks returns up to limit tasks referenced by a state ZSET, each with
// its score. Entries whose task body has been deleted are skipped.
func (r *RDB) ListZSetTasks(ctx context.Context, qname, zsetKey string, limit int) ([]*ZSetTask, error) {
	if limit <= 0 {
		return nil, nil
	}
	zs, err := r.client.ZRangeWithScores(ctx, zsetKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	tasks := make([]*ZSetTask, 0, len(zs))
	for _, z := range zs {
		id, _ := z.Member.(string)
		msg, err := r.GetTask(ctx, qname, id)
		if err == redis.Nil {
			continue // body gone; skip
		}
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, &ZSetTask{Msg: msg, Score: z.Score})
	}
	return tasks, nil
}

// ZScore returns the score of taskID in zsetKey, or (0, false) if absent.
func (r *RDB) ZScore(ctx context.Context, zsetKey, taskID string) (float64, bool, error) {
	score, err := r.client.ZScore(ctx, zsetKey, taskID).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return score, true, nil
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
// state ZSET (scheduled/retry/archived/completed): if the task is not in any of
// them (e.g. it is already pending or in-flight/active), it does nothing. This
// prevents a duplicate stream entry (double execution) and makes concurrent
// RunTask calls safe — only the one that wins the ZREM promotes.
// KEYS[1] scheduled, KEYS[2] retry, KEYS[3] archived, KEYS[4] completed,
// KEYS[5] stream, KEYS[6] task hash.
// ARGV[1] taskID, ARGV[2] pending state.
var runTaskCmd = redis.NewScript(`
local removed = redis.call("ZREM", KEYS[1], ARGV[1]) + redis.call("ZREM", KEYS[2], ARGV[1]) + redis.call("ZREM", KEYS[3], ARGV[1]) + redis.call("ZREM", KEYS[4], ARGV[1])
if removed == 0 then
  return 0
end
if redis.call("EXISTS", KEYS[6]) == 0 then
  return 0
end
redis.call("XADD", KEYS[5], "*", "task_id", ARGV[1])
redis.call("HSET", KEYS[6], "state", ARGV[2])
return 1
`)

// RunTask promotes a scheduled/retry/archived/completed task to the stream so it runs now.
func (r *RDB) RunTask(ctx context.Context, qname, taskID string) error {
	keys := []string{
		base.ScheduledKey(qname), base.RetryKey(qname), base.ArchivedKey(qname),
		base.CompletedKey(qname),
		base.StreamKey(qname), base.TaskKey(qname, taskID),
	}
	return runTaskCmd.Run(ctx, r.client, keys, taskID, int(base.StatePending)).Err()
}

// DeleteTask removes a task from all state ZSETs and deletes its body, releasing
// any unique lock it holds. It is intended for scheduled/retry/archived/completed
// tasks.
// If called on an in-flight (pending/active) task, deleting the body leaves an
// orphan stream/PEL entry; that orphan is harmless and is trimmed (XACK+XDEL) by
// the next dequeue or recover pass. For an in-flight task with retention, a
// racing Done may resurrect the hash into the completed set; the janitor removes
// it at its expiry.
func (r *RDB) DeleteTask(ctx context.Context, qname, taskID string) error {
	msg, err := r.GetTask(ctx, qname, taskID)
	if err != nil && err != redis.Nil {
		return err
	}
	pipe := r.client.TxPipeline()
	pipe.ZRem(ctx, base.ScheduledKey(qname), taskID)
	pipe.ZRem(ctx, base.RetryKey(qname), taskID)
	pipe.ZRem(ctx, base.ArchivedKey(qname), taskID)
	pipe.ZRem(ctx, base.CompletedKey(qname), taskID)
	pipe.Del(ctx, base.TaskKey(qname, taskID))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	if msg != nil {
		return r.releaseUnique(ctx, msg)
	}
	return nil
}
