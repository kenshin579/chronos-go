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
