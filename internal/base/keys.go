// Package base defines the Redis key layout, task states, and message
// serialization shared across chronos-go internals.
package base

// Keys builds every Redis key chronos-go writes, under a configurable prefix.
//
// Two deployments sharing one Redis database must use different prefixes or
// they collide on the global keys — most damagingly LeaderKey, a single lock
// that decides which process fires schedules. See the design doc at
// docs/superpowers/specs/2026-08-02-key-prefix-design.md.
type Keys struct {
	prefix string
}

// DefaultKeys is the key layout used when no namespace is configured. It is
// byte-identical to the layout chronos-go wrote before prefixes existed.
var DefaultKeys = NewKeys(DefaultPrefix)

// NewKeys returns a Keys for the given prefix. It panics on an invalid prefix —
// see NormalizePrefix.
func NewKeys(prefix string) Keys {
	return Keys{prefix: NormalizePrefix(prefix)}
}

// Prefix returns the normalized prefix.
func (k Keys) Prefix() string { return k.prefix }

// QueueKeyPrefix returns the common prefix for all keys of a queue. The queue
// name is wrapped in a Redis Cluster hash tag ({...}) so that every key of a
// single queue hashes to the same slot, allowing multi-key Lua scripts to run
// on a cluster.
//
// The configurable prefix sits before the tag, which is slot-neutral: Redis
// hashes only the first {...} pair and ignores everything before it, so a
// queue's keys stay co-located whatever the prefix.
func (k Keys) QueueKeyPrefix(qname string) string {
	return k.prefix + ":{" + qname + "}:"
}

// StreamKey returns the Stream key holding task IDs ready for immediate
// execution (consumed via a consumer group).
func (k Keys) StreamKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "stream"
}

// TaskKey returns the HASH key holding a task's body and state.
func (k Keys) TaskKey(qname, id string) string {
	return k.QueueKeyPrefix(qname) + "t:" + id
}

// QueuesKey returns the SET key listing all known queue names. It has no hash
// tag on purpose: it is a global index touched by a standalone command, never
// inside a per-queue multi-key script.
func (k Keys) QueuesKey() string { return k.prefix + ":queues" }

// PausedKey is the SET key listing paused queue names. Global (no hash tag):
// only single-key commands touch it, so it is cluster-safe.
func (k Keys) PausedKey() string { return k.prefix + ":paused" }

// SchedulesKey is the HASH key holding the registry of known schedules
// (field = deterministic schedule ID, value = JSON metadata). Global single
// key, single-key commands only — cluster-safe.
func (k Keys) SchedulesKey() string { return k.prefix + ":schedules" }

// TaskKeyPrefix returns the prefix shared by every task HASH key of a queue.
// Lua scripts build a task key by concatenating this prefix with a task ID read
// from a ZSET; the prefix keeps those keys in the same cluster slot.
func (k Keys) TaskKeyPrefix(qname string) string {
	return k.QueueKeyPrefix(qname) + "t:"
}

// RetryKey returns the ZSET key holding tasks awaiting retry (score = retry_at).
func (k Keys) RetryKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "retry"
}

// ArchivedKey returns the ZSET key holding dead-lettered tasks (score = died_at).
func (k Keys) ArchivedKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "archived"
}

// CompletedKey returns the ZSET key holding successfully completed tasks that
// are retained for inspection (score = expire-at, i.e. completed-at + retention).
func (k Keys) CompletedKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "completed"
}

// GroupKey returns the SET key holding a group's pending member IDs. It lives
// in the callback queue's hash slot so "remove member + fire callback when
// empty" runs as one atomic (cluster-safe) script.
func (k Keys) GroupKey(cbQueue, groupID string) string {
	return k.QueueKeyPrefix(cbQueue) + "group:" + groupID
}

// GroupResultKey returns the HASH key collecting a group's member results
// (field = member index, value = base64 of the result JSON) while the group
// is in flight. Same hash tag as the pending SET — the completion script
// touches both atomically (cluster-safe).
func (k Keys) GroupResultKey(cbQueue, groupID string) string {
	return k.QueueKeyPrefix(cbQueue) + "groupresult:" + groupID
}

// ScheduledKey returns the ZSET key holding delayed tasks (score = process_at).
func (k Keys) ScheduledKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "scheduled"
}

// UniqueKey returns the STRING key holding the deduplication lock for a task.
// suffix is produced by UniqueSuffix. The queue hash tag keeps it in the same
// slot as the task's other keys.
func (k Keys) UniqueKey(qname, suffix string) string {
	return k.QueueKeyPrefix(qname) + "unique:" + suffix
}

// LeaderKey is the STRING key holding the current scheduler leader's instance ID.
func (k Keys) LeaderKey() string { return k.prefix + ":leader" }

// LeaderResignChannel is the pub/sub channel a leader publishes to on graceful
// resignation so followers can re-elect immediately instead of waiting for TTL.
func (k Keys) LeaderResignChannel() string { return k.prefix + ":leader:resign" }

// PeriodicDedupKey is the STRING key used to deduplicate a single scheduled
// trigger. id is "<scheduleID>:<trigger_unix>". Wrapped in the queue hash tag so
// it shares the queue's slot.
func (k Keys) PeriodicDedupKey(qname, id string) string {
	return k.QueueKeyPrefix(qname) + "pdedup:" + id
}

// ScheduleLastFiredKey is the STRING key holding the unix time a schedule last
// fired, used to compute missed triggers across leader changes. It is global
// (no queue hash tag) because a schedule is not tied to one queue's slot.
func (k Keys) ScheduleLastFiredKey(scheduleID string) string {
	return k.prefix + ":sched:" + scheduleID + ":last"
}

// The functions below delegate to DefaultKeys. Production code builds keys
// through RDB's Keys so a namespace is honoured; these exist so the many
// default-prefix call sites (chiefly tests) keep reading naturally.

func QueueKeyPrefix(qname string) string { return DefaultKeys.QueueKeyPrefix(qname) }
func StreamKey(qname string) string      { return DefaultKeys.StreamKey(qname) }
func TaskKey(qname, id string) string    { return DefaultKeys.TaskKey(qname, id) }
func QueuesKey() string                  { return DefaultKeys.QueuesKey() }
func PausedKey() string                  { return DefaultKeys.PausedKey() }
func SchedulesKey() string               { return DefaultKeys.SchedulesKey() }
func TaskKeyPrefix(qname string) string  { return DefaultKeys.TaskKeyPrefix(qname) }
func RetryKey(qname string) string       { return DefaultKeys.RetryKey(qname) }
func ArchivedKey(qname string) string    { return DefaultKeys.ArchivedKey(qname) }
func CompletedKey(qname string) string   { return DefaultKeys.CompletedKey(qname) }
func ScheduledKey(qname string) string   { return DefaultKeys.ScheduledKey(qname) }
func LeaderKey() string                  { return DefaultKeys.LeaderKey() }
func LeaderResignChannel() string        { return DefaultKeys.LeaderResignChannel() }

func GroupKey(cbQueue, groupID string) string { return DefaultKeys.GroupKey(cbQueue, groupID) }
func GroupResultKey(cbQueue, groupID string) string {
	return DefaultKeys.GroupResultKey(cbQueue, groupID)
}
func UniqueKey(qname, suffix string) string    { return DefaultKeys.UniqueKey(qname, suffix) }
func PeriodicDedupKey(qname, id string) string { return DefaultKeys.PeriodicDedupKey(qname, id) }
func ScheduleLastFiredKey(scheduleID string) string {
	return DefaultKeys.ScheduleLastFiredKey(scheduleID)
}
