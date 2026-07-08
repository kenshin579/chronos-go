package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// trimArchivedCmd deletes archived tasks in two passes: (1) by age — entries
// with score (died-at) <= cutoff, and (2) by size — if the ZSET still exceeds
// maxSize, the oldest excess entries. For each removed ID it deletes the task
// hash and removes the ZSET member. All keys share the queue hash tag, so the
// multi-key script is cluster-safe.
// KEYS[1] archived zset.
// ARGV[1] cutoff (unix), ARGV[2] max age-batch, ARGV[3] task-key prefix, ARGV[4] maxSize.
var trimArchivedCmd = redis.NewScript(`
local removed = 0

-- (1) age-based: score <= cutoff
local expired = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
for _, id in ipairs(expired) do
  redis.call("DEL", ARGV[3] .. id)
  redis.call("ZREM", KEYS[1], id)
  removed = removed + 1
end

-- (2) size cap: delete oldest entries beyond maxSize
local over = redis.call("ZCARD", KEYS[1]) - tonumber(ARGV[4])
if over > 0 then
  local excess = redis.call("ZRANGE", KEYS[1], 0, over - 1)
  for _, id in ipairs(excess) do
    redis.call("DEL", ARGV[3] .. id)
    redis.call("ZREM", KEYS[1], id)
    removed = removed + 1
  end
end

return removed
`)

// TrimArchived removes dead-lettered tasks that are older than the cutoff, and
// (after that) any that still exceed maxSize (oldest first). ageBatch bounds how
// many age-expired entries a single call removes (keeping each script short);
// the size cap always fully enforces maxSize. Returns the number removed.
// It is safe to call concurrently from multiple instances (removals are atomic
// and idempotent). Archived tasks hold no unique lock (released at archive time),
// so no lock cleanup is needed here.
func (r *RDB) TrimArchived(ctx context.Context, qname string, cutoff time.Time, maxSize, ageBatch int) (int, error) {
	keys := []string{base.ArchivedKey(qname)}
	argv := []interface{}{cutoff.Unix(), ageBatch, base.TaskKeyPrefix(qname), maxSize}
	n, err := trimArchivedCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}
