package rdb

import (
	"context"

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
}

// QueueStats returns the per-state task counts for a queue.
func (r *RDB) QueueStats(ctx context.Context, qname string) (*QueueStats, error) {
	streamKey := base.StreamKey(qname)

	xlen, err := r.client.XLen(ctx, streamKey).Result()
	if err != nil {
		return nil, err
	}
	pending, err := r.client.XPending(ctx, streamKey, ConsumerGroup).Result()
	if err != nil {
		return nil, err
	}
	active := pending.Count // entries currently in the PEL

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
	}, nil
}

// Queues returns the names of all known queues.
func (r *RDB) Queues(ctx context.Context) ([]string, error) {
	return r.client.SMembers(ctx, base.QueuesKey()).Result()
}
