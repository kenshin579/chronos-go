package rdb

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// GroupTTL bounds how long a group's pending-member SET may linger. It is the
// safety net for abandoned groups (a member deleted or dead-lettered and never
// re-run): after it expires the callback can no longer fire and late member
// reports become no-ops. Mirrors the archived-retention default.
const GroupTTL = 7 * 24 * time.Hour

// groupCompleteCmd removes a finished member from the group's pending SET and,
// when the SET becomes empty, creates the callback task (create-if-absent) and
// deletes the SET. A missing SET means the group already completed or expired —
// the call is then a no-op, which makes redelivered member reports safe. All
// keys share the callback queue's hash tag (cluster-safe).
// KEYS[1] group set, KEYS[2] callback task hash, KEYS[3] callback stream or
// scheduled zset.
// ARGV[1] member id, ARGV[2] callback encoded msg, ARGV[3] callback state,
// ARGV[4] callback id, ARGV[5] mode ("stream"|"zset"), ARGV[6] score (zset).
var groupCompleteCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
  return 0
end
redis.call("SREM", KEYS[1], ARGV[1])
if redis.call("SCARD", KEYS[1]) > 0 then
  return 0
end
redis.call("DEL", KEYS[1])
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2], "msg", ARGV[2], "state", ARGV[3])
if ARGV[5] == "stream" then
  redis.call("XADD", KEYS[3], "*", "task_id", ARGV[4])
else
  redis.call("ZADD", KEYS[3], ARGV[6], ARGV[4])
end
return 1
`)

// CreateGroup registers a group's pending member IDs (with the safety TTL) in
// the callback queue's slot. Called once, before the members are enqueued, so
// a partially failed multi-member enqueue can never fire the callback early.
func (r *RDB) CreateGroup(ctx context.Context, cbQueue, groupID string, memberIDs []string) error {
	if len(memberIDs) == 0 {
		return errors.New("chronos: group needs at least one member")
	}
	key := base.GroupKey(cbQueue, groupID)
	pipe := r.client.TxPipeline()
	members := make([]interface{}, len(memberIDs))
	for i, id := range memberIDs {
		members[i] = id
	}
	pipe.SAdd(ctx, key, members...)
	pipe.Expire(ctx, key, GroupTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// CompleteGroupMember reports a member's success. When it was the last pending
// member it atomically creates the group's callback task and returns true.
// Idempotent under at-least-once redelivery (SREM of an absent member and a
// missing SET are both no-ops).
func (r *RDB) CompleteGroupMember(ctx context.Context, member *base.TaskMessage) (bool, error) {
	link := member.GroupCallback
	if link == nil {
		return false, errors.New("chronos: group member has no callback snapshot")
	}
	// The group SET and the callback's keys must share one hash slot, or the
	// atomic script would be cross-slot on a cluster. The builder guarantees
	// this (GroupQueue == callback queue); guard against corrupted messages.
	if member.GroupQueue != link.Queue {
		return false, errors.New("chronos: group state and callback must live on the callback queue")
	}
	cb := &base.TaskMessage{
		ID:        member.GroupID + ":cb",
		Kind:      link.Kind,
		Payload:   link.Payload,
		Queue:     link.Queue,
		MaxRetry:  link.MaxRetry,
		NoArchive: link.NoArchive,
		Retention: link.Retention,
	}

	// Register the callback queue in the global index up front (separate
	// command: QueuesKey has no hash tag).
	if err := r.client.SAdd(ctx, base.QueuesKey(), cb.Queue).Err(); err != nil {
		return false, err
	}

	mode, state := "stream", base.StatePending
	var score int64
	var destKey string
	if link.Delay > 0 {
		mode, state = "zset", base.StateScheduled
		score = time.Now().Add(time.Duration(link.Delay) * time.Second).Unix()
		destKey = base.ScheduledKey(cb.Queue)
	} else {
		destKey = base.StreamKey(cb.Queue)
	}
	cb.State = state
	encoded, err := base.EncodeMessage(cb)
	if err != nil {
		return false, err
	}

	keys := []string{
		base.GroupKey(member.GroupQueue, member.GroupID),
		base.TaskKey(cb.Queue, cb.ID),
		destKey,
	}
	argv := []interface{}{member.ID, encoded, int(state), cb.ID, mode, score}
	n, err := groupCompleteCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// GroupPending returns how many members of the group have not yet succeeded
// (0 when the group finished or its state expired).
func (r *RDB) GroupPending(ctx context.Context, cbQueue, groupID string) (int64, error) {
	return r.client.SCard(ctx, base.GroupKey(cbQueue, groupID)).Result()
}
