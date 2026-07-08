package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// trimArchivedCmd deletes archived tasks in two passes: (1) by age — entries
// with score (died-at) <= cutoff, and (2) by size — if the ZSET still exceeds
// maxSize, the oldest excess entries. Both passes are bounded by the same batch
// (ARGV[2]) so a single script stays short even with a huge backlog (it converges
// across ticks). A negative maxSize disables the size pass. For each removed ID
// it deletes the task hash and removes the ZSET member. All keys share the queue
// hash tag, so the multi-key script is cluster-safe.
// KEYS[1] archived zset.
// ARGV[1] cutoff (unix), ARGV[2] batch, ARGV[3] task-key prefix, ARGV[4] maxSize.
var trimArchivedCmd = redis.NewScript(`
local removed = 0
local batch = tonumber(ARGV[2])

-- (1) age-based: score <= cutoff (bounded by batch)
local expired = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, batch)
for _, id in ipairs(expired) do
  redis.call("DEL", ARGV[3] .. id)
  redis.call("ZREM", KEYS[1], id)
  removed = removed + 1
end

-- (2) size cap: delete oldest beyond maxSize, bounded by batch (converges over
-- ticks). Negative maxSize disables the cap.
local maxSize = tonumber(ARGV[4])
if maxSize >= 0 then
  local over = redis.call("ZCARD", KEYS[1]) - maxSize
  if over > 0 then
    if over > batch then over = batch end
    local excess = redis.call("ZRANGE", KEYS[1], 0, over - 1)
    for _, id in ipairs(excess) do
      redis.call("DEL", ARGV[3] .. id)
      redis.call("ZREM", KEYS[1], id)
      removed = removed + 1
    end
  end
end

return removed
`)

// TrimArchived removes dead-lettered tasks that are older than the cutoff, and
// (after that) any that still exceed maxSize (oldest first). batch bounds how
// many entries EACH pass removes per call, keeping the atomic script short; a
// large backlog therefore converges over successive calls rather than blocking
// Redis in one giant script. A negative maxSize disables the size cap. Returns
// the number removed. It is safe to call concurrently from multiple instances
// (removals are atomic and idempotent). Archived tasks hold no unique lock
// (released at archive time), so no lock cleanup is needed here.
func (r *RDB) TrimArchived(ctx context.Context, qname string, cutoff time.Time, maxSize, batch int) (int, error) {
	keys := []string{base.ArchivedKey(qname)}
	argv := []interface{}{cutoff.Unix(), batch, base.TaskKeyPrefix(qname), maxSize}
	n, err := trimArchivedCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}
