package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// ExtendLease resets the idle time of in-flight PEL entries by re-claiming them
// to the same consumer with min-idle 0 (XCLAIM ... JUSTID). This keeps a task
// that is genuinely being processed from being reclaimed by the recoverer while
// it runs. JUSTID means the delivery count is NOT incremented.
func (r *RDB) ExtendLease(ctx context.Context, qname, consumer string, streamIDs []string) error {
	if len(streamIDs) == 0 {
		return nil
	}
	return r.client.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream:   base.StreamKey(qname),
		Group:    ConsumerGroup,
		Consumer: consumer,
		MinIdle:  0,
		Messages: streamIDs,
	}).Err()
}

// RenewUnique extends the TTL of the given unique-lock keys (PEXPIRE ttl). A key
// that no longer exists (e.g. released at terminal state) is a no-op — PEXPIRE
// returns 0 and does not recreate it — so renewing a just-released lock is safe.
func (r *RDB) RenewUnique(ctx context.Context, uniqueKeys []string, ttl time.Duration) error {
	if len(uniqueKeys) == 0 {
		return nil
	}
	pipe := r.client.Pipeline()
	for _, k := range uniqueKeys {
		pipe.PExpire(ctx, k, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}
