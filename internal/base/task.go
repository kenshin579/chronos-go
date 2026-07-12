package base

import "encoding/json"

// TaskState is the lifecycle stage of a task.
type TaskState int

const (
	StatePending   TaskState = iota + 1 // in the stream, awaiting a worker
	StateActive                         // read by a worker (in the consumer group PEL)
	StateCompleted                      // finished successfully
	StateRetry                          // failed, awaiting retry (M2)
	StateArchived                       // dead-letter (M2)
	StateScheduled                      // delayed, awaiting its time (M3)
)

// String returns the lowercase name of the state.
func (s TaskState) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateActive:
		return "active"
	case StateCompleted:
		return "completed"
	case StateRetry:
		return "retry"
	case StateArchived:
		return "archived"
	case StateScheduled:
		return "scheduled"
	default:
		return "unknown"
	}
}

// ChainLink is one pending successor task, carried inside its predecessor's
// message (so a dead-lettered link that is re-run naturally resumes the chain).
// It is a serializable snapshot of the enqueue parameters taken when the chain
// was built.
type ChainLink struct {
	Kind      string `json:"kind"`
	Payload   []byte `json:"payload"`
	Queue     string `json:"queue"`
	MaxRetry  int    `json:"max_retry"`
	NoArchive bool   `json:"no_archive,omitempty"`
	Retention int64  `json:"retention,omitempty"` // seconds
	Delay     int64  `json:"delay,omitempty"`     // seconds before the link runs
}

// TaskMessage is the canonical, serialized representation of a task stored in
// the task HASH.
type TaskMessage struct {
	ID      string    `json:"id"`
	Kind    string    `json:"kind"`
	Payload []byte    `json:"payload"`
	Queue   string    `json:"queue"`
	State   TaskState `json:"state"`

	// Retried is the number of retries already scheduled for this task.
	Retried int `json:"retried"`
	// MaxRetry is the maximum number of retries before the task is dead-lettered.
	MaxRetry int `json:"max_retry"`
	// NoArchive, when true, discards the task on retry exhaustion instead of
	// storing it in the archived ZSET (the OnDeadLetter hook still fires).
	NoArchive bool `json:"no_archive"`
	// UniqueKey is the full Redis key of this task's deduplication lock, or ""
	// if the task is not unique. It is released when the task reaches a terminal
	// state (completed / archived / discarded).
	UniqueKey string `json:"unique_key"`
	// LastErr is the error message from the most recent failed attempt. It is
	// persisted so the Inspector and Web UI can show why a task was retried or
	// dead-lettered. Empty until the first failure.
	LastErr string `json:"last_err,omitempty"`
	// Retention is how long (in seconds) a successfully completed task is kept
	// in the completed ZSET for inspection. 0 (default) deletes it immediately.
	Retention int64 `json:"retention,omitempty"`
	// CompletedAt is the unix time the task finished successfully. Set only
	// when the task is kept (Retention > 0).
	CompletedAt int64 `json:"completed_at,omitempty"`

	// Chain holds this task's pending successors: Chain[0] is enqueued when
	// this task succeeds, carrying Chain[1:] as its own tail. Empty for tasks
	// outside a chain and for the last link.
	Chain []ChainLink `json:"chain,omitempty"`
	// ChainID identifies the chain this task belongs to; successor task IDs are
	// deterministic ("<chain_id>:<index>") so a redelivered predecessor cannot
	// enqueue its successor twice.
	ChainID string `json:"chain_id,omitempty"`
	// ChainIndex is this task's position in its chain (0-based).
	ChainIndex int `json:"chain_index,omitempty"`

	// GroupID identifies the group this task is a member of ("" = not grouped).
	// Member task IDs are deterministic ("<group_id>:m<i>"), the callback's is
	// "<group_id>:cb".
	GroupID string `json:"group_id,omitempty"`
	// GroupQueue is the queue whose hash slot holds the group's pending-member
	// SET (= the callback's queue), so a completing member knows where to report.
	GroupQueue string `json:"group_queue,omitempty"`
	// GroupCallback is the callback task snapshot, carried by every member; the
	// member that empties the pending SET creates it (create-if-absent).
	GroupCallback *ChainLink `json:"group_callback,omitempty"`
}

// EncodeMessage serializes a TaskMessage for storage in Redis.
func EncodeMessage(m *TaskMessage) ([]byte, error) {
	return json.Marshal(m)
}

// DecodeMessage deserializes a TaskMessage read from Redis.
func DecodeMessage(b []byte) (*TaskMessage, error) {
	var m TaskMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
