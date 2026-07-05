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
