// Package base defines the Redis key layout, task states, and message
// serialization shared across chronos-go internals.
package base

// QueueKeyPrefix returns the common prefix for all keys of a queue. The queue
// name is wrapped in a Redis Cluster hash tag ({...}) so that every key of a
// single queue hashes to the same slot, allowing multi-key Lua scripts to run
// on a cluster.
func QueueKeyPrefix(qname string) string {
	return "chronos:{" + qname + "}:"
}

// StreamKey returns the Stream key holding task IDs ready for immediate
// execution (consumed via a consumer group).
func StreamKey(qname string) string {
	return QueueKeyPrefix(qname) + "stream"
}

// TaskKey returns the HASH key holding a task's body and state.
func TaskKey(qname, id string) string {
	return QueueKeyPrefix(qname) + "t:" + id
}

// QueuesKey returns the SET key listing all known queue names. It has no hash
// tag on purpose: it is a global index touched by a standalone command, never
// inside a per-queue multi-key script.
func QueuesKey() string {
	return "chronos:queues"
}

// TaskKeyPrefix returns the prefix shared by every task HASH key of a queue.
// Lua scripts build a task key by concatenating this prefix with a task ID read
// from a ZSET; the prefix keeps those keys in the same cluster slot.
func TaskKeyPrefix(qname string) string {
	return QueueKeyPrefix(qname) + "t:"
}

// RetryKey returns the ZSET key holding tasks awaiting retry (score = retry_at).
func RetryKey(qname string) string {
	return QueueKeyPrefix(qname) + "retry"
}

// ArchivedKey returns the ZSET key holding dead-lettered tasks (score = died_at).
func ArchivedKey(qname string) string {
	return QueueKeyPrefix(qname) + "archived"
}

// CompletedKey returns the ZSET key holding successfully completed tasks that
// are retained for inspection (score = expire-at, i.e. completed-at + retention).
func CompletedKey(qname string) string {
	return QueueKeyPrefix(qname) + "completed"
}

// GroupKey returns the SET key holding a group's pending member IDs. It lives
// in the callback queue's hash slot so "remove member + fire callback when
// empty" runs as one atomic (cluster-safe) script.
func GroupKey(cbQueue, groupID string) string {
	return QueueKeyPrefix(cbQueue) + "group:" + groupID
}

// ScheduledKey returns the ZSET key holding delayed tasks (score = process_at).
func ScheduledKey(qname string) string {
	return QueueKeyPrefix(qname) + "scheduled"
}

// UniqueKey returns the STRING key holding the deduplication lock for a task.
// suffix is produced by UniqueSuffix. The queue hash tag keeps it in the same
// slot as the task's other keys.
func UniqueKey(qname, suffix string) string {
	return QueueKeyPrefix(qname) + "unique:" + suffix
}

// LeaderKey is the STRING key holding the current scheduler leader's instance ID.
func LeaderKey() string { return "chronos:leader" }

// LeaderResignChannel is the pub/sub channel a leader publishes to on graceful
// resignation so followers can re-elect immediately instead of waiting for TTL.
func LeaderResignChannel() string { return "chronos:leader:resign" }

// PeriodicDedupKey is the STRING key used to deduplicate a single scheduled
// trigger. id is "<scheduleID>:<trigger_unix>". Wrapped in the queue hash tag so
// it shares the queue's slot.
func PeriodicDedupKey(qname, id string) string {
	return QueueKeyPrefix(qname) + "pdedup:" + id
}

// ScheduleLastFiredKey is the STRING key holding the unix time a schedule last
// fired, used to compute missed triggers across leader changes. It is global
// (no queue hash tag) because a schedule is not tied to one queue's slot.
func ScheduleLastFiredKey(scheduleID string) string {
	return "chronos:sched:" + scheduleID + ":last"
}
