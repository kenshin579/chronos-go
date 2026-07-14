package rdb

import (
	"context"
	"encoding/base64"
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
// deletes the SET. While members remain, the SET's TTL is refreshed to the full
// GroupTTL — and the result HASH's TTL is refreshed in lockstep (when it exists)
// so a result-less member trickling reports slower than GroupTTL can never let
// the HASH expire ahead of the SET and drop already-recorded results. A group
// that keeps making progress never expires mid-flight; only a truly abandoned
// one does. A missing SET means the group already completed
// or expired — the call is then a no-op, which makes redelivered member reports
// safe. All keys share the callback queue's hash tag (cluster-safe).
// The member's result (base64) is stored in the result HASH (KEYS[4]) under
// its member index; when the last member completes, the results are embedded
// into the callback message's group_results field (base64 strings — matching
// Go's []byte JSON encoding) via cjson before the callback is created, and
// both the pending SET and the result HASH are deleted. cjson round-trips the
// message JSON: all numeric fields are unix seconds / small ints (< 2^53),
// and every slice field is omitempty (no empty-array-to-{} hazard). The
// member-count guard (ARGV[10] > 0) keeps legacy in-flight members (encoded
// before GroupSize existed) on the old no-results path — and avoids cjson
// encoding an empty Lua table as {} into a slice field.
// KEYS[1] group set, KEYS[2] callback task hash, KEYS[3] callback stream or
// scheduled zset, KEYS[4] group result hash.
// ARGV[1] member id, ARGV[2] callback encoded msg, ARGV[3] callback state,
// ARGV[4] callback id, ARGV[5] mode ("stream"|"zset"), ARGV[6] score (zset),
// ARGV[7] group TTL in seconds, ARGV[8] member result base64 ("" = none),
// ARGV[9] member index, ARGV[10] member count.
var groupCompleteCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
  return 0
end
redis.call("SREM", KEYS[1], ARGV[1])
if ARGV[8] ~= "" then
  redis.call("HSET", KEYS[4], ARGV[9], ARGV[8])
  redis.call("EXPIRE", KEYS[4], ARGV[7])
end
if redis.call("SCARD", KEYS[1]) > 0 then
  redis.call("EXPIRE", KEYS[1], ARGV[7])
  if redis.call("EXISTS", KEYS[4]) == 1 then
    redis.call("EXPIRE", KEYS[4], ARGV[7])
  end
  return 0
end
local cb = ARGV[2]
if redis.call("EXISTS", KEYS[4]) == 1 and tonumber(ARGV[10]) > 0 then
  local msg = cjson.decode(cb)
  local results = {}
  local n = tonumber(ARGV[10])
  for i = 0, n - 1 do
    local v = redis.call("HGET", KEYS[4], tostring(i))
    if v then
      results[i + 1] = v
    else
      results[i + 1] = cjson.null
    end
  end
  msg["group_results"] = results
  cb = cjson.encode(msg)
end
redis.call("DEL", KEYS[1], KEYS[4])
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2], "msg", cb, "state", ARGV[3])
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

// GroupStageState reports what CreateGroupIfAbsent found.
type GroupStageState int

const (
	// GroupStageDone: the stage's callback task already exists — the group
	// completed; nothing must be (re)created.
	GroupStageDone GroupStageState = iota
	// GroupStageExists: the pending SET already exists (redelivery while the
	// stage is in flight); members may still need create-if-absent enqueues.
	GroupStageExists
	// GroupStageCreated: the pending SET was created by this call.
	GroupStageCreated
)

// createGroupIfAbsentCmd guards a chain group stage against predecessor
// redelivery: an existing callback hash fences a completed stage, an existing
// SET means the stage is in flight. When the SET is (re)created, any leftover
// result HASH from a previous round is deleted so near-expiry stale results
// can never leak into the new round. All keys share the callback queue's hash
// slot. KEYS[1] pending SET, KEYS[2] callback task hash, KEYS[3] result HASH.
// ARGV[1] TTL seconds, ARGV[2..] member IDs.
var createGroupIfAbsentCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
if redis.call("EXISTS", KEYS[1]) == 1 then
  redis.call("EXPIRE", KEYS[1], ARGV[1])
  return 1
end
for i = 2, #ARGV do
  redis.call("SADD", KEYS[1], ARGV[i])
end
redis.call("EXPIRE", KEYS[1], ARGV[1])
redis.call("DEL", KEYS[3])
return 2
`)

// CreateGroupIfAbsent registers a chain stage's pending members exactly once.
// Unlike CreateGroup (unconditional, for standalone groups with fresh UUIDs),
// chain stages have deterministic IDs and may be re-attempted by a redelivered
// predecessor — completed stages must not be resurrected.
//
// Drain contract: a caller that gets GroupStageCreated or GroupStageExists and
// re-enqueues the members create-if-absent must, whenever a member enqueue is
// a no-op (the task hash already exists), check that member's hash state (see
// TaskState) — if it is a leftover StateCompleted (member retention outlived
// the callback fence), the caller must drain it via CompleteGroupMember.
// Otherwise that member's SET entry is never SREM'd and the stage stalls
// forever (until GroupTTL expires it).
func (r *RDB) CreateGroupIfAbsent(ctx context.Context, cbQueue, groupID string, memberIDs []string, cbTaskID string) (GroupStageState, error) {
	if len(memberIDs) == 0 {
		return GroupStageDone, errors.New("chronos: group needs at least one member")
	}
	keys := []string{
		base.GroupKey(cbQueue, groupID),
		base.TaskKey(cbQueue, cbTaskID),
		base.GroupResultKey(cbQueue, groupID),
	}
	argv := make([]interface{}, 0, len(memberIDs)+1)
	argv = append(argv, int(GroupTTL/time.Second))
	for _, id := range memberIDs {
		argv = append(argv, id)
	}
	n, err := createGroupIfAbsentCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return GroupStageDone, err
	}
	return GroupStageState(n), nil
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
	// A chain-embedded stage's callback inherits the chain tail so the chain
	// continues after the fan-in.
	if len(member.GroupCallbackChain) > 0 {
		cb.Chain = member.GroupCallbackChain
		cb.ChainID = member.ChainID
		cb.ChainIndex = member.ChainIndex
	}

	// Register the callback queue in the global index up front (separate
	// command: QueuesKey has no hash tag). Doing it on every report is slightly
	// wasteful for non-final members, but the reverse order would open a
	// "callback created but never indexed" window if SAdd failed afterwards.
	if err := r.registerQueue(ctx, cb.Queue); err != nil {
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
		base.GroupResultKey(member.GroupQueue, member.GroupID),
	}
	resultB64 := ""
	if len(member.Result) > 0 {
		resultB64 = base64.StdEncoding.EncodeToString(member.Result)
	}
	argv := []interface{}{member.ID, encoded, int(state), cb.ID, mode, score,
		int(GroupTTL / time.Second), resultB64, member.GroupIndex, member.GroupSize}
	n, err := groupCompleteCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// GroupMembers lists the group's still-pending member IDs.
func (r *RDB) GroupMembers(ctx context.Context, cbQueue, groupID string) ([]string, error) {
	return r.client.SMembers(ctx, base.GroupKey(cbQueue, groupID)).Result()
}

// GroupPending returns how many members of the group have not yet succeeded
// (0 when the group finished or its state expired).
func (r *RDB) GroupPending(ctx context.Context, cbQueue, groupID string) (int64, error) {
	return r.client.SCard(ctx, base.GroupKey(cbQueue, groupID)).Result()
}
