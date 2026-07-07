package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// acquireOrRenewCmd acquires leadership if vacant, or renews it if already held
// by this instance. Returns 1 if the caller is the leader after the call, 0 if
// another instance holds it.
// KEYS[1] leader key. ARGV[1] instanceID, ARGV[2] ttl millis.
var acquireOrRenewCmd = redis.NewScript(`
local cur = redis.call("GET", KEYS[1])
if cur == false then
  redis.call("SET", KEYS[1], ARGV[1], "PX", tonumber(ARGV[2]))
  return 1
elseif cur == ARGV[1] then
  redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))
  return 1
else
  return 0
end
`)

// resignCmd releases leadership only if this instance still holds it.
// KEYS[1] leader key. ARGV[1] instanceID.
var resignCmd = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("DEL", KEYS[1])
  return 1
end
return 0
`)

// AcquireOrRenewLeadership makes instanceID the leader (or renews its term) with
// the given TTL. Returns true if instanceID is the leader after the call.
func (r *RDB) AcquireOrRenewLeadership(ctx context.Context, instanceID string, ttl time.Duration) (bool, error) {
	ms := ttl.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	res, err := acquireOrRenewCmd.Run(ctx, r.client, []string{base.LeaderKey()}, instanceID, ms).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// ResignLeadership releases leadership if instanceID holds it, then notifies
// followers via pub/sub so they can re-elect immediately. Releasing when not the
// owner is a no-op.
func (r *RDB) ResignLeadership(ctx context.Context, instanceID string) error {
	if err := resignCmd.Run(ctx, r.client, []string{base.LeaderKey()}, instanceID).Err(); err != nil {
		return err
	}
	return r.client.Publish(ctx, base.LeaderResignChannel(), instanceID).Err()
}

// SubscribeResign returns a pub/sub subscription to the leader-resign channel.
func (r *RDB) SubscribeResign(ctx context.Context) *redis.PubSub {
	return r.client.Subscribe(ctx, base.LeaderResignChannel())
}
