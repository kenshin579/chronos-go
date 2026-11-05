package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
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
		Stream:   r.keys.StreamKey(qname),
		Group:    ConsumerGroup,
		Consumer: consumer,
		MinIdle:  0,
		Messages: streamIDs,
	}).Err()
}

// renewUniqueCmd raises a unique lock's TTL to ARGV[1] milliseconds only when
// its remaining TTL is shorter — it never shortens one. This is PEXPIRE ... GT,
// which needs Redis 7.0; the script keeps Redis 6.2 supported. A missing key
// (PTTL -2) is left alone, so a lock released at terminal state is never
// recreated, and so is a key without an expiry (PTTL -1).
// KEYS[1] unique key. ARGV[1] ttl millis.
var renewUniqueCmd = redis.NewScript(`
local ttl = redis.call("PTTL", KEYS[1])
if ttl >= 0 and ttl < tonumber(ARGV[1]) then
  redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return 1
`)

// RenewUnique extends the given unique-lock keys so each has at least ttl left.
// A lock that already has more than ttl left keeps it: WithUnique's ttl is meant
// to cover the time a task waits (pending / retry backoff) with no heartbeat
// renewing it, so the heartbeat must not cut it short. A key that no longer
// exists (e.g. released at terminal state) is a no-op, so renewing a
// just-released lock is safe.
//
// Each key gets its own script call: keys from different queues hash to
// different cluster slots, so they cannot share one multi-key script. The calls
// are pipelined with EVAL rather than EVALSHA because a pipelined EVALSHA cannot
// fall back to EVAL on NOSCRIPT.
func (r *RDB) RenewUnique(ctx context.Context, uniqueKeys []string, ttl time.Duration) error {
	if len(uniqueKeys) == 0 {
		return nil
	}
	ms := uniqueTTLMillis(ttl)
	pipe := r.client.Pipeline()
	for _, k := range uniqueKeys {
		renewUniqueCmd.Eval(ctx, pipe, []string{k}, ms)
	}
	_, err := pipe.Exec(ctx)
	return err
}
