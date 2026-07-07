package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// Recover reclaims tasks stuck in the consumer group's PEL — entries whose
// owning consumer has been idle longer than minIdle (typically because the
// worker crashed). Each reclaimed task counts as one failed attempt: if that
// exhausts its retry budget it is archived, otherwise it is moved to the retry
// ZSET for immediate re-forwarding. It processes at most max entries per call.
//
// Returned: recovered = count moved to retry; archived = the messages that were
// dead-lettered (so the caller can fire OnDeadLetter for each).
//
// NOTE: Without heartbeat-based lease extension (a later milestone), a handler
// that runs longer than minIdle can be reclaimed and reprocessed concurrently.
// This is the at-least-once contract; set minIdle comfortably above expected
// handler duration.
func (r *RDB) Recover(ctx context.Context, qname, consumer string, minIdle time.Duration, max int) (recovered int, archived []*base.TaskMessage, err error) {
	streamKey := base.StreamKey(qname)

	msgs, _, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamKey,
		Group:    ConsumerGroup,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0",
		Count:    int64(max),
	}).Result()
	if err != nil {
		return 0, nil, err
	}

	// A per-entry failure skips that entry and continues the batch, so one bad
	// task cannot stall recovery of the rest. The last such error is returned so
	// the caller can log it; the skipped entry stays in the PEL and is retried on
	// the next tick.
	var lastErr error
	now := time.Now()
	for _, m := range msgs {
		taskID, _ := m.Values["task_id"].(string)

		raw, err := r.client.HGet(ctx, base.TaskKey(qname, taskID), "msg").Result()
		if err == redis.Nil {
			// Body already gone: drop the orphan entry (ack + delete) so it does
			// not linger in the stream and inflate the pending count.
			pipe := r.client.TxPipeline()
			pipe.XAck(ctx, streamKey, ConsumerGroup, m.ID)
			pipe.XDel(ctx, streamKey, m.ID)
			_, _ = pipe.Exec(ctx)
			continue
		}
		if err != nil {
			lastErr = err
			continue
		}
		msg, err := base.DecodeMessage([]byte(raw))
		if err != nil {
			lastErr = err
			continue
		}

		// This reclaim counts as one failed attempt.
		if msg.Retried >= msg.MaxRetry {
			if aerr := r.Archive(ctx, qname, m.ID, msg, now); aerr != nil {
				lastErr = aerr
				continue
			}
			archived = append(archived, msg)
			continue
		}
		msg.Retried++
		// Re-run promptly: schedule the retry for "now" so the forwarder
		// picks it up on its next tick.
		if rerr := r.Retry(ctx, qname, m.ID, msg, now); rerr != nil {
			lastErr = rerr
			continue
		}
		recovered++
	}
	return recovered, archived, lastErr
}
