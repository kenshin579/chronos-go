package rdb

import (
	"context"

	"github.com/kenshin579/chronos-go/internal/base"
)

// PauseQueue marks a queue paused: servers stop consuming it (enqueueing,
// forwarding and recovery continue, so work accumulates as pending). Idempotent.
func (r *RDB) PauseQueue(ctx context.Context, qname string) error {
	return r.client.SAdd(ctx, base.PausedKey(), qname).Err()
}

// ResumeQueue lifts a pause. Idempotent.
func (r *RDB) ResumeQueue(ctx context.Context, qname string) error {
	return r.client.SRem(ctx, base.PausedKey(), qname).Err()
}

// PausedQueues lists currently paused queue names.
func (r *RDB) PausedQueues(ctx context.Context) ([]string, error) {
	return r.client.SMembers(ctx, base.PausedKey()).Result()
}
