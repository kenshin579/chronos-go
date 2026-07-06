package base

import (
	"crypto/sha256"
	"encoding/hex"
)

// UniqueSuffix derives a stable deduplication suffix from a task's kind and
// payload: "<kind>:<sha256(payload) hex>". Two enqueues with the same kind and
// payload produce the same suffix (and thus compete for the same unique lock).
func UniqueSuffix(kind string, payload []byte) string {
	sum := sha256.Sum256(payload)
	return kind + ":" + hex.EncodeToString(sum[:])
}
