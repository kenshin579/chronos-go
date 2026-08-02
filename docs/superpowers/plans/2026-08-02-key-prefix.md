# Configurable key prefix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let two applications share one Redis DB by making the key prefix configurable, so a second app's scheduler can no longer silently stop the first app's cron jobs.

**Architecture:** `internal/base` gains a `Keys` value carrying the prefix; its 18 builders become methods, with package-level wrappers over a default-prefix `Keys` so existing call sites keep compiling. `rdb.RDB` carries a `Keys` and every key is built through it. A public `Namespace` handle derives `Client`/`Server`/`Scheduler`/`Inspector` from one prefix so three-of-four misconfiguration is unrepresentable. Default prefix `chronos` — keys byte-identical, no migration.

**Tech Stack:** Go 1.26, redis/go-redis v9, Redis 6.2+

**Design doc:** `docs/superpowers/specs/2026-08-02-key-prefix-design.md`. Read it for the *why* — especially why a separate Redis DB was rejected (Cluster has only DB 0) and why the handle exists (silent three-of-four misconfiguration).

---

## Prerequisites

Tests need a real Redis on `127.0.0.1:6379` (they `t.Skip` otherwise, which would make this plan's verification meaningless).

```bash
docker run -d --name chronos-test-redis -p 6379:6379 redis:7-alpine
redis-cli ping   # PONG
```

`-p 1` is mandatory for multi-package `go test` — packages share DB 15 and flush it.

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `internal/base/keys.go` | Rewrite | `Keys` type + 18 methods + package-level wrappers |
| `internal/base/keys_test.go` | Rewrite | Golden keys for default *and* custom prefix; Lua ARGV invariant |
| `internal/base/prefix.go` | Create | Prefix validation and normalization |
| `internal/base/prefix_test.go` | Create | Validation cases |
| `internal/rdb/rdb.go` | Modify | `RDB.keys` field; `NewRDB(client, keys)` |
| `internal/rdb/*.go` (15 files) | Modify | 82 call sites `base.X(...)` → `r.keys.X(...)` |
| `internal/rdb/inspect.go` | Modify | Absorb `zsetKeyForState` from the root package |
| `internal/rdb/unique.go` | Modify | Build the unique key internally |
| `internal/rdb/periodic.go` | Modify | Build the dedup key internally |
| `namespace.go` | Create | Public `Namespace` handle |
| `namespace_test.go` | Create | Handle behaviour + panic cases |
| `chronos.go`, `server.go`, `scheduler.go`, `inspector.go` | Modify | Pass `base.DefaultKeys`; drop local key building |
| `namespace_integration_test.go` | Create | **Acceptance test**: two namespaces, one DB |
| `benchmarks/soak/sampler.go` | Modify | Stop hand-building keys |
| `README.md`, `doc.go`, `example_test.go` | Modify | Document the feature and its one limitation |

---

### Task 1: Branch and environment

- [ ] **Step 1: Verify branch, spec, and Redis**

```bash
cd /Users/frankoh/src/workspace_inspireme/chronos-go
git branch --show-current
ls docs/superpowers/specs/2026-08-02-key-prefix-design.md
redis-cli -h 127.0.0.1 -p 6379 ping
```

Expected: `feat/key-prefix`, the spec exists, `PONG`. If Redis is refused, run the docker command from Prerequisites.

- [ ] **Step 2: Record the baseline**

```bash
make test 2>&1 | tail -5
```

Expected: all packages `ok` (or `no test files`). **If anything fails before you change a line, stop and report** — you need a green baseline to attribute later failures.

---

### Task 2: Prefix validation

**Files:** Create `internal/base/prefix.go`, `internal/base/prefix_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/base/prefix_test.go`:

```go
package base

import "testing"

func TestNormalizePrefix(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"chronos", "chronos"},
		{"inspireme", "inspireme"},
		{"myapp:", "myapp"},           // trailing colon trimmed
		{"myapp:::", "myapp"},         // repeated trailing colons trimmed
		{"chronos:inspireme", "chronos:inspireme"}, // interior colon kept
	}
	for _, tt := range tests {
		if got := NormalizePrefix(tt.in); got != tt.want {
			t.Errorf("NormalizePrefix(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNormalizePrefixPanics(t *testing.T) {
	bad := []struct {
		in     string
		reason string
	}{
		{"", "empty"},
		{":", "empty after trimming"},
		{"my{app", "opening brace corrupts the hash tag"},
		{"my}app", "closing brace corrupts the hash tag"},
		{"my*app", "glob metacharacter corrupts SCAN patterns"},
		{"my?app", "glob metacharacter corrupts SCAN patterns"},
		{"my[app", "glob metacharacter corrupts SCAN patterns"},
		{"my]app", "glob metacharacter corrupts SCAN patterns"},
		{"my app", "whitespace"},
		{"my\tapp", "whitespace"},
		{"my\napp", "whitespace"},
		{"my\x00app", "control character"},
	}
	for _, tt := range bad {
		t.Run(tt.in, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("NormalizePrefix(%q) did not panic (%s)", tt.in, tt.reason)
				}
			}()
			NormalizePrefix(tt.in)
		})
	}
}
```

- [ ] **Step 2: Run it — expect a compile failure**

```bash
go test ./internal/base/ -run TestNormalizePrefix -p 1
```

Expected: `undefined: NormalizePrefix`.

- [ ] **Step 3: Implement**

Create `internal/base/prefix.go`:

```go
package base

import (
	"fmt"
	"strings"
	"unicode"
)

// DefaultPrefix is the key prefix used when none is configured. It preserves
// the key layout chronos-go has always written, so an existing deployment that
// does not opt into a namespace sees byte-identical keys.
const DefaultPrefix = "chronos"

// NormalizePrefix validates a key prefix and returns it with trailing colons
// removed, so "myapp" and "myapp:" behave identically.
//
// It panics on an invalid prefix rather than returning an error. A prefix is
// almost always a compile-time constant, and every rejected character causes a
// silent failure rather than a loud one — braces collapse every queue onto a
// single cluster slot, and glob metacharacters corrupt the SCAN pattern
// ScanSchedules derives from ScheduleLastFiredKey. Failing at startup is
// strictly safer than misrouting every key. This matches AddHandler, which
// panics on a duplicate Kind for the same reason.
func NormalizePrefix(prefix string) string {
	p := strings.TrimRight(prefix, ":")
	if p == "" {
		panic(fmt.Sprintf("chronos: key prefix %q is empty", prefix))
	}
	for _, r := range p {
		switch {
		case r == '{' || r == '}':
			// The hash tag is what keeps a queue's keys in one cluster slot.
			// A brace in the prefix moves or fixes the tag: "my{app}:{q}:stream"
			// tags on "app" for every queue, collapsing the whole deployment
			// onto one slot with no error.
			panic(fmt.Sprintf("chronos: key prefix %q must not contain braces", prefix))
		case r == '*' || r == '?' || r == '[' || r == ']':
			// ScanSchedules builds a SCAN pattern from the key builder; a glob
			// metacharacter here silently matches the wrong keys.
			panic(fmt.Sprintf("chronos: key prefix %q must not contain glob metacharacters", prefix))
		case unicode.IsSpace(r) || unicode.IsControl(r):
			panic(fmt.Sprintf("chronos: key prefix %q must not contain whitespace or control characters", prefix))
		}
	}
	return p
}
```

- [ ] **Step 4: Run the tests**

```bash
go test ./internal/base/ -run TestNormalizePrefix -v -p 1
```

Expected: PASS, all subtests.

- [ ] **Step 5: Commit**

```bash
git add internal/base/prefix.go internal/base/prefix_test.go
git commit -m "feat(base): validate and normalize key prefixes

Panic on braces (they corrupt the cluster hash tag and collapse every
queue onto one slot), glob metacharacters (they corrupt the SCAN pattern
ScanSchedules derives), whitespace, and empty. Trim trailing colons so
\"myapp\" and \"myapp:\" behave the same.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 3: `base.Keys` type

**Files:** Modify `internal/base/keys.go`, rewrite `internal/base/keys_test.go`

- [ ] **Step 1: Write the failing test**

Replace `internal/base/keys_test.go` entirely:

```go
package base

import "testing"

// TestDefaultKeysUnchanged is the golden test: an existing deployment that does
// not opt into a namespace must see byte-identical keys. Every literal here is
// the layout chronos-go shipped before prefixes existed.
func TestDefaultKeysUnchanged(t *testing.T) {
	tests := []struct{ got, want string }{
		{QueueKeyPrefix("default"), "chronos:{default}:"},
		{StreamKey("default"), "chronos:{default}:stream"},
		{TaskKey("default", "abc"), "chronos:{default}:t:abc"},
		{TaskKeyPrefix("default"), "chronos:{default}:t:"},
		{RetryKey("default"), "chronos:{default}:retry"},
		{ArchivedKey("default"), "chronos:{default}:archived"},
		{CompletedKey("default"), "chronos:{default}:completed"},
		{ScheduledKey("default"), "chronos:{default}:scheduled"},
		{UniqueKey("default", "k:deadbeef"), "chronos:{default}:unique:k:deadbeef"},
		{GroupKey("cbq", "g1"), "chronos:{cbq}:group:g1"},
		{GroupResultKey("cbq", "g1"), "chronos:{cbq}:groupresult:g1"},
		{PeriodicDedupKey("default", "s1:100"), "chronos:{default}:pdedup:s1:100"},
		{QueuesKey(), "chronos:queues"},
		{PausedKey(), "chronos:paused"},
		{SchedulesKey(), "chronos:schedules"},
		{LeaderKey(), "chronos:leader"},
		{LeaderResignChannel(), "chronos:leader:resign"},
		{ScheduleLastFiredKey("s1"), "chronos:sched:s1:last"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("got %q, want %q", tt.got, tt.want)
		}
	}
}

// TestCustomPrefix asserts the prefix reaches every builder, including the
// global ones — those are the keys that make two applications collide.
func TestCustomPrefix(t *testing.T) {
	k := NewKeys("myapp")
	tests := []struct{ got, want string }{
		{k.QueueKeyPrefix("default"), "myapp:{default}:"},
		{k.StreamKey("default"), "myapp:{default}:stream"},
		{k.TaskKey("default", "abc"), "myapp:{default}:t:abc"},
		{k.TaskKeyPrefix("default"), "myapp:{default}:t:"},
		{k.RetryKey("default"), "myapp:{default}:retry"},
		{k.ArchivedKey("default"), "myapp:{default}:archived"},
		{k.CompletedKey("default"), "myapp:{default}:completed"},
		{k.ScheduledKey("default"), "myapp:{default}:scheduled"},
		{k.UniqueKey("default", "k:deadbeef"), "myapp:{default}:unique:k:deadbeef"},
		{k.GroupKey("cbq", "g1"), "myapp:{cbq}:group:g1"},
		{k.GroupResultKey("cbq", "g1"), "myapp:{cbq}:groupresult:g1"},
		{k.PeriodicDedupKey("default", "s1:100"), "myapp:{default}:pdedup:s1:100"},
		{k.QueuesKey(), "myapp:queues"},
		{k.PausedKey(), "myapp:paused"},
		{k.SchedulesKey(), "myapp:schedules"},
		{k.LeaderKey(), "myapp:leader"},
		{k.LeaderResignChannel(), "myapp:leader:resign"},
		{k.ScheduleLastFiredKey("s1"), "myapp:sched:s1:last"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("got %q, want %q", tt.got, tt.want)
		}
	}
}

// TestTaskKeyPrefixInvariant guards the contract forwardCmd and trimArchivedCmd
// depend on: both receive TaskKeyPrefix as ARGV and concatenate a task ID onto
// it inside Lua. If this ever diverges from TaskKey, those scripts write to keys
// nothing else reads.
func TestTaskKeyPrefixInvariant(t *testing.T) {
	for _, k := range []Keys{DefaultKeys, NewKeys("myapp"), NewKeys("chronos:inspireme")} {
		if k.TaskKeyPrefix("default")+"abc" != k.TaskKey("default", "abc") {
			t.Errorf("prefix %q: TaskKeyPrefix + id must equal TaskKey", k.Prefix())
		}
	}
}

// TestKeysNormalizePrefix asserts the constructor normalizes rather than
// storing the raw string.
func TestKeysNormalizePrefix(t *testing.T) {
	if got := NewKeys("myapp:").StreamKey("q"); got != "myapp:{q}:stream" {
		t.Errorf("trailing colon not normalized: %q", got)
	}
}
```

- [ ] **Step 2: Run it — expect a compile failure**

```bash
go test ./internal/base/ -run 'TestDefaultKeysUnchanged|TestCustomPrefix|TestTaskKeyPrefixInvariant|TestKeysNormalizePrefix' -p 1
```

Expected: `undefined: NewKeys`, `undefined: Keys`, `undefined: DefaultKeys`.

- [ ] **Step 3: Rewrite `internal/base/keys.go`**

Replace the file entirely:

```go
// Package base defines the Redis key layout, task states, and message
// serialization shared across chronos-go internals.
package base

// Keys builds every Redis key chronos-go writes, under a configurable prefix.
//
// Two deployments sharing one Redis database must use different prefixes or
// they collide on the global keys — most damagingly LeaderKey, a single lock
// that decides which process fires schedules. See the design doc at
// docs/superpowers/specs/2026-08-02-key-prefix-design.md.
type Keys struct {
	prefix string
}

// DefaultKeys is the key layout used when no namespace is configured. It is
// byte-identical to the layout chronos-go wrote before prefixes existed.
var DefaultKeys = NewKeys(DefaultPrefix)

// NewKeys returns a Keys for the given prefix. It panics on an invalid prefix —
// see NormalizePrefix.
func NewKeys(prefix string) Keys {
	return Keys{prefix: NormalizePrefix(prefix)}
}

// Prefix returns the normalized prefix.
func (k Keys) Prefix() string { return k.prefix }

// QueueKeyPrefix returns the common prefix for all keys of a queue. The queue
// name is wrapped in a Redis Cluster hash tag ({...}) so that every key of a
// single queue hashes to the same slot, allowing multi-key Lua scripts to run
// on a cluster.
//
// The configurable prefix sits before the tag, which is slot-neutral: Redis
// hashes only the first {...} pair and ignores everything before it, so a
// queue's keys stay co-located whatever the prefix.
func (k Keys) QueueKeyPrefix(qname string) string {
	return k.prefix + ":{" + qname + "}:"
}

// StreamKey returns the Stream key holding task IDs ready for immediate
// execution (consumed via a consumer group).
func (k Keys) StreamKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "stream"
}

// TaskKey returns the HASH key holding a task's body and state.
func (k Keys) TaskKey(qname, id string) string {
	return k.QueueKeyPrefix(qname) + "t:" + id
}

// QueuesKey returns the SET key listing all known queue names. It has no hash
// tag on purpose: it is a global index touched by a standalone command, never
// inside a per-queue multi-key script.
func (k Keys) QueuesKey() string { return k.prefix + ":queues" }

// PausedKey is the SET key listing paused queue names. Global (no hash tag):
// only single-key commands touch it, so it is cluster-safe.
func (k Keys) PausedKey() string { return k.prefix + ":paused" }

// SchedulesKey is the HASH key holding the registry of known schedules
// (field = deterministic schedule ID, value = JSON metadata). Global single
// key, single-key commands only — cluster-safe.
func (k Keys) SchedulesKey() string { return k.prefix + ":schedules" }

// TaskKeyPrefix returns the prefix shared by every task HASH key of a queue.
// Lua scripts build a task key by concatenating this prefix with a task ID read
// from a ZSET; the prefix keeps those keys in the same cluster slot.
func (k Keys) TaskKeyPrefix(qname string) string {
	return k.QueueKeyPrefix(qname) + "t:"
}

// RetryKey returns the ZSET key holding tasks awaiting retry (score = retry_at).
func (k Keys) RetryKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "retry"
}

// ArchivedKey returns the ZSET key holding dead-lettered tasks (score = died_at).
func (k Keys) ArchivedKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "archived"
}

// CompletedKey returns the ZSET key holding successfully completed tasks that
// are retained for inspection (score = expire-at, i.e. completed-at + retention).
func (k Keys) CompletedKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "completed"
}

// GroupKey returns the SET key holding a group's pending member IDs. It lives
// in the callback queue's hash slot so "remove member + fire callback when
// empty" runs as one atomic (cluster-safe) script.
func (k Keys) GroupKey(cbQueue, groupID string) string {
	return k.QueueKeyPrefix(cbQueue) + "group:" + groupID
}

// GroupResultKey returns the HASH key collecting a group's member results
// (field = member index, value = base64 of the result JSON) while the group
// is in flight. Same hash tag as the pending SET — the completion script
// touches both atomically (cluster-safe).
func (k Keys) GroupResultKey(cbQueue, groupID string) string {
	return k.QueueKeyPrefix(cbQueue) + "groupresult:" + groupID
}

// ScheduledKey returns the ZSET key holding delayed tasks (score = process_at).
func (k Keys) ScheduledKey(qname string) string {
	return k.QueueKeyPrefix(qname) + "scheduled"
}

// UniqueKey returns the STRING key holding the deduplication lock for a task.
// suffix is produced by UniqueSuffix. The queue hash tag keeps it in the same
// slot as the task's other keys.
func (k Keys) UniqueKey(qname, suffix string) string {
	return k.QueueKeyPrefix(qname) + "unique:" + suffix
}

// LeaderKey is the STRING key holding the current scheduler leader's instance ID.
func (k Keys) LeaderKey() string { return k.prefix + ":leader" }

// LeaderResignChannel is the pub/sub channel a leader publishes to on graceful
// resignation so followers can re-elect immediately instead of waiting for TTL.
func (k Keys) LeaderResignChannel() string { return k.prefix + ":leader:resign" }

// PeriodicDedupKey is the STRING key used to deduplicate a single scheduled
// trigger. id is "<scheduleID>:<trigger_unix>". Wrapped in the queue hash tag so
// it shares the queue's slot.
func (k Keys) PeriodicDedupKey(qname, id string) string {
	return k.QueueKeyPrefix(qname) + "pdedup:" + id
}

// ScheduleLastFiredKey is the STRING key holding the unix time a schedule last
// fired, used to compute missed triggers across leader changes. It is global
// (no queue hash tag) because a schedule is not tied to one queue's slot.
func (k Keys) ScheduleLastFiredKey(scheduleID string) string {
	return k.prefix + ":sched:" + scheduleID + ":last"
}

// The functions below delegate to DefaultKeys. Production code builds keys
// through RDB's Keys so a namespace is honoured; these exist so the many
// default-prefix call sites (chiefly tests) keep reading naturally.

func QueueKeyPrefix(qname string) string  { return DefaultKeys.QueueKeyPrefix(qname) }
func StreamKey(qname string) string       { return DefaultKeys.StreamKey(qname) }
func TaskKey(qname, id string) string     { return DefaultKeys.TaskKey(qname, id) }
func QueuesKey() string                   { return DefaultKeys.QueuesKey() }
func PausedKey() string                   { return DefaultKeys.PausedKey() }
func SchedulesKey() string                { return DefaultKeys.SchedulesKey() }
func TaskKeyPrefix(qname string) string   { return DefaultKeys.TaskKeyPrefix(qname) }
func RetryKey(qname string) string        { return DefaultKeys.RetryKey(qname) }
func ArchivedKey(qname string) string     { return DefaultKeys.ArchivedKey(qname) }
func CompletedKey(qname string) string    { return DefaultKeys.CompletedKey(qname) }
func ScheduledKey(qname string) string    { return DefaultKeys.ScheduledKey(qname) }
func LeaderKey() string                   { return DefaultKeys.LeaderKey() }
func LeaderResignChannel() string         { return DefaultKeys.LeaderResignChannel() }

func GroupKey(cbQueue, groupID string) string       { return DefaultKeys.GroupKey(cbQueue, groupID) }
func GroupResultKey(cbQueue, groupID string) string { return DefaultKeys.GroupResultKey(cbQueue, groupID) }
func UniqueKey(qname, suffix string) string         { return DefaultKeys.UniqueKey(qname, suffix) }
func PeriodicDedupKey(qname, id string) string      { return DefaultKeys.PeriodicDedupKey(qname, id) }
func ScheduleLastFiredKey(scheduleID string) string { return DefaultKeys.ScheduleLastFiredKey(scheduleID) }
```

- [ ] **Step 4: Run the base tests**

```bash
go test ./internal/base/ -v -p 1 2>&1 | tail -20
```

Expected: PASS. `TestDefaultKeysUnchanged` passing is the proof that nothing moved for existing deployments.

- [ ] **Step 5: Run the whole suite — nothing should have changed yet**

```bash
make test 2>&1 | tail -10
```

Expected: same green as the Task 1 baseline. Every call site still goes through the wrappers.

- [ ] **Step 6: Commit**

```bash
git add internal/base/keys.go internal/base/keys_test.go
git commit -m "feat(base): make the key layout carry a configurable prefix

Turn the 18 key builders into methods on a Keys value holding the prefix,
and keep package-level wrappers over a default-prefix Keys so existing
call sites compile unchanged.

The golden test asserts the default layout is byte-identical to what
shipped before, so an existing deployment sees no change.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 4: Give `RDB` a `Keys`

**Files:** Modify `internal/rdb/rdb.go`, `chronos.go`, `server.go`, `scheduler.go`, `inspector.go`

This task only threads the value through. Call sites still use the wrappers, so behaviour is unchanged and the suite must stay green.

- [ ] **Step 1: Add the field and change the constructor**

In `internal/rdb/rdb.go`, change the struct and `NewRDB` (currently at lines 20-34):

```go
// RDB wraps a Redis client with chronos-go's task operations.
type RDB struct {
	client redis.UniversalClient

	// keys builds every Redis key this RDB touches. It carries the namespace
	// prefix, so two RDBs with different Keys cannot see each other's data —
	// including the scheduler leader lock.
	keys base.Keys

	// knownQueues caches queue names this instance has already registered in
	// the global queue index (SADD <prefix>:queues), so the hot enqueue path
	// pays that extra round trip only once per queue per process. Registration
	// is idempotent, so a lost cache (new process) just re-registers.
	knownQueues sync.Map // queue name -> struct{}
}

// NewRDB returns an RDB backed by the given Redis client, building keys under
// the given layout.
func NewRDB(client redis.UniversalClient, keys base.Keys) *RDB {
	return &RDB{client: client, keys: keys}
}

// Keys returns the key layout this RDB builds with.
func (r *RDB) Keys() base.Keys { return r.keys }
```

- [ ] **Step 2: Update the four public constructors**

`chronos.go` (~line 132):

```go
func NewClient(r redis.UniversalClient) *Client {
	return &Client{rdb: rdb.NewRDB(r, base.DefaultKeys)}
}
```

`inspector.go` (~line 29):

```go
func NewInspector(r redis.UniversalClient) *Inspector {
	return &Inspector{rdb: rdb.NewRDB(r, base.DefaultKeys)}
}
```

`server.go` (~line 190), inside the returned struct literal:

```go
		rdb:      rdb.NewRDB(r, base.DefaultKeys),
```

`scheduler.go` (~line 75), inside the returned struct literal:

```go
		rdb:      rdb.NewRDB(r, base.DefaultKeys),
```

Add the `base` import to any of these files that lacks it.

- [ ] **Step 3: Find every other `NewRDB` caller**

```bash
grep -rn "rdb.NewRDB(\|NewRDB(" --include=*.go . | grep -v "func NewRDB"
```

Update each to pass `base.DefaultKeys` (test files in `internal/rdb` will call `NewRDB(client, base.DefaultKeys)`).

- [ ] **Step 4: Build and test**

```bash
go build ./... && make test 2>&1 | tail -10
```

Expected: green, unchanged from baseline.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(rdb): carry a Keys value on RDB

Thread the key layout through RDB's constructor. All callers pass the
default layout, so behaviour is unchanged; the next commit routes key
construction through it.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 5: Route `internal/rdb` through `r.keys`

**Files:** 15 files in `internal/rdb/`

82 production call sites. Every one already has an `r *RDB` receiver in scope. This is mechanical: `base.StreamKey(q)` → `r.keys.StreamKey(q)`.

- [ ] **Step 1: Convert, file by file**

Work through these in order, building after each so a mistake is attributable:

`rdb.go` (14), `inspect.go` (19), `group.go` (11), `unique.go` (4), `schedule.go` (4), `registry.go` (4), `periodic.go` (4), `leader.go` (4), `janitor.go` (4), `forward.go` (4), `retry.go` (3), `pause.go` (3), `recover.go` (2), `chain.go` (2), `heartbeat.go` (1).

```bash
go build ./internal/rdb/    # after each file
```

**Watch for methods without an `r *RDB` receiver.** If you find a package-level helper in `internal/rdb` that builds a key, pass `keys base.Keys` to it explicitly rather than reaching for a global.

- [ ] **Step 2: Verify no production call site was missed**

This grep is the completeness check — it must return nothing:

```bash
grep -rnE 'base\.(QueueKeyPrefix|StreamKey|TaskKeyPrefix|TaskKey|QueuesKey|PausedKey|SchedulesKey|RetryKey|ArchivedKey|CompletedKey|ScheduledKey|GroupKey|GroupResultKey|UniqueKey|LeaderKey|LeaderResignChannel|PeriodicDedupKey|ScheduleLastFiredKey)\(' internal/rdb/*.go | grep -v '_test.go'
```

Expected: **no output**. Any hit is a key that would ignore the namespace.

- [ ] **Step 3: Test**

```bash
make test 2>&1 | tail -10
```

Expected: green. Behaviour is still identical — every RDB is built with `DefaultKeys`.

- [ ] **Step 4: Commit**

```bash
git add internal/rdb/
git commit -m "refactor(rdb): build every key through RDB's Keys

Mechanical conversion of all 82 production call sites from the
package-level builders to the RDB's own layout, so a namespace is
honoured everywhere inside internal/rdb.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 6: Close the three root-package leaks

**Files:** `inspector.go`, `chronos.go`, `scheduler.go`, `internal/rdb/inspect.go`, `internal/rdb/unique.go`, `internal/rdb/periodic.go`

These three sites build keys *outside* `internal/rdb` and would bypass any prefix. Each moves into `RDB`.

- [ ] **Step 1: Move `zsetKeyForState` into `RDB`**

`ErrInvalidState` is defined at `inspector.go:19` and consumed publicly by
`contrib/webui/handlers.go:116` as `chronos.ErrInvalidState`. **Do not move it.**
Have `RDB` return only the key, and let the root package keep producing the
error — the key builder has no business owning a public sentinel.

Add to `internal/rdb/inspect.go`:

```go
// ZSetKeyForState maps a user-facing state name to its ZSET key, reporting
// whether the state is known. It lives here rather than in the root package so
// the key is always built under this RDB's namespace.
//
// It deliberately returns a bool rather than an error: chronos.ErrInvalidState
// is public API owned by the root package, and moving it here would drag a
// public sentinel into internal/.
func (r *RDB) ZSetKeyForState(qname, state string) (string, bool) {
	switch state {
	case "scheduled":
		return r.keys.ScheduledKey(qname), true
	case "retry":
		return r.keys.RetryKey(qname), true
	case "archived":
		return r.keys.ArchivedKey(qname), true
	case "completed":
		return r.keys.CompletedKey(qname), true
	default:
		return "", false
	}
}
```

Delete `zsetKeyForState` from `inspector.go` (lines 94-108) and update its two
call sites. Line 112 becomes:

```go
	zsetKey, ok := i.rdb.ZSetKeyForState(qname, state)
	if !ok {
		return nil, fmt.Errorf("%w %q (want scheduled|retry|archived|completed)", ErrInvalidState, state)
	}
```

Line 139 uses the bool form of the same call — read it before editing, it is
inside an `if` that currently discards the error (`if zsetKey, kerr := ...; kerr == nil`).

- [ ] **Step 2: Move unique-key construction into `RDB`**

In `chronos.go` `dispatchMessage` (~line 295), delete:

```go
		msg.UniqueKey = base.UniqueKey(msg.Queue, base.UniqueSuffix(msg.Kind, msg.Payload))
```

Instead have `RDB.EnqueueUnique` and `RDB.ScheduleUnique` set `msg.UniqueKey` themselves at the top of each method:

```go
	msg.UniqueKey = r.keys.UniqueKey(msg.Queue, base.UniqueSuffix(msg.Kind, msg.Payload))
```

**Read both methods fully before editing** — confirm neither already relies on `msg.UniqueKey` being set by the caller, and that no other caller sets it. Check with `grep -rn "UniqueKey" --include=*.go . | grep -v _test`.

- [ ] **Step 3: Move dedup-key construction into `RDB`**

In `scheduler.go` `enqueueTrigger` (~line 306), delete the `dedupKey` line and change the call:

```go
	// Dedup key lives well beyond a leader-handover window but not forever.
	return s.rdb.EnqueuePeriodic(ctx, msg, triggerID, 10*s.cfg.LeaderTTL)
```

Change `RDB.EnqueuePeriodic`'s signature in `internal/rdb/periodic.go` from `dedupKey string` to `triggerID string` and build the key inside:

```go
func (r *RDB) EnqueuePeriodic(ctx context.Context, msg *base.TaskMessage, triggerID string, dedupTTL time.Duration) error {
	dedupKey := r.keys.PeriodicDedupKey(msg.Queue, triggerID)
	...
```

- [ ] **Step 4: Verify no root-package key building remains**

```bash
grep -rnE 'base\.(QueueKeyPrefix|StreamKey|TaskKeyPrefix|TaskKey|QueuesKey|PausedKey|SchedulesKey|RetryKey|ArchivedKey|CompletedKey|ScheduledKey|GroupKey|GroupResultKey|UniqueKey|LeaderKey|LeaderResignChannel|PeriodicDedupKey|ScheduleLastFiredKey)\(' *.go | grep -v '_test.go'
```

Expected: **no output**.

- [ ] **Step 5: Test**

```bash
make test 2>&1 | tail -10
```

Expected: green.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: build keys only inside internal/rdb

Three sites built keys in the root package and would bypass a namespace:
Inspector.zsetKeyForState (a package-level func with no receiver), the
Client's unique key, and the Scheduler's periodic dedup key. Move all
three behind RDB so the key builder never escapes internal/rdb.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 7: The `Namespace` handle

**Files:** Create `namespace.go`, `namespace_test.go`

- [ ] **Step 1: Write the failing test**

Create `namespace_test.go`:

```go
package chronos

import (
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func testClient() redis.UniversalClient {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
}

func TestNamespacePanicsOnBadPrefix(t *testing.T) {
	for _, bad := range []string{"", "my{app", "my*app", "my app"} {
		t.Run(bad, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("NewNamespace(%q) did not panic", bad)
				}
			}()
			NewNamespace(testClient(), bad)
		})
	}
}

func TestNamespacePrefix(t *testing.T) {
	ns := NewNamespace(testClient(), "myapp:")
	if ns.Prefix() != "myapp" {
		t.Errorf("Prefix() = %q, want %q", ns.Prefix(), "myapp")
	}
}

// TestNamespaceDerivedTypesShareThePrefix is the point of the handle: you
// cannot configure three of the four and forget the fourth.
func TestNamespaceDerivedTypesShareThePrefix(t *testing.T) {
	ns := NewNamespace(testClient(), "myapp")
	want := "myapp"
	if got := ns.NewClient().rdb.Keys().Prefix(); got != want {
		t.Errorf("Client prefix = %q, want %q", got, want)
	}
	if got := ns.NewInspector().rdb.Keys().Prefix(); got != want {
		t.Errorf("Inspector prefix = %q, want %q", got, want)
	}
	if got := ns.NewServer(ServerConfig{Queues: map[string]int{"default": 1}}).rdb.Keys().Prefix(); got != want {
		t.Errorf("Server prefix = %q, want %q", got, want)
	}
	if got := ns.NewScheduler(SchedulerConfig{}).rdb.Keys().Prefix(); got != want {
		t.Errorf("Scheduler prefix = %q, want %q", got, want)
	}
}

// TestDefaultConstructorsUnchanged asserts the four existing constructors still
// write where they always did.
func TestDefaultConstructorsUnchanged(t *testing.T) {
	if got := NewClient(testClient()).rdb.Keys().StreamKey("default"); !strings.HasPrefix(got, "chronos:") {
		t.Errorf("NewClient key = %q, want the chronos: prefix", got)
	}
}
```

- [ ] **Step 2: Run it — expect a compile failure**

```bash
go test . -run TestNamespace -p 1
```

Expected: `undefined: NewNamespace`.

- [ ] **Step 3: Implement**

Create `namespace.go`:

```go
package chronos

import (
	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// Namespace scopes every Redis key chronos-go writes under a prefix, so two
// applications can share one Redis database without colliding.
//
// Sharing matters more than it looks: the scheduler leader lock is a single
// global key. Two deployments on one database contend for it, and the loser
// stops firing its schedules with no error logged anywhere. A namespace gives
// each deployment its own lock.
//
// Derive all four types from one Namespace. Configuring a prefix on some of
// them and not others is the failure this type exists to prevent — a Client
// writing under one prefix while a Server reads another produces tasks that are
// never processed and never reported.
//
//	ns := chronos.NewNamespace(rdb, "inspireme")
//	client := ns.NewClient()
//	srv := ns.NewServer(chronos.ServerConfig{Queues: map[string]int{"default": 1}})
//
// The four package-level constructors (NewClient, NewServer, NewScheduler,
// NewInspector) are equivalent to a namespace with the default prefix
// "chronos", which is the layout chronos-go has always written.
//
// Changing the prefix of a running deployment requires draining in-flight tasks
// first: a task's unique-lock key is stored in its body, so a task enqueued
// under one prefix cannot have its lock released under another.
type Namespace struct {
	client redis.UniversalClient
	keys   base.Keys
}

// NewNamespace returns a Namespace scoping all keys under prefix.
//
// It panics if the prefix is invalid — braces break the cluster hash tag and
// glob metacharacters break schedule scanning, both silently. See the package
// docs for the accepted form.
func NewNamespace(r redis.UniversalClient, prefix string) *Namespace {
	return &Namespace{client: r, keys: base.NewKeys(prefix)}
}

// Prefix returns the normalized prefix.
func (ns *Namespace) Prefix() string { return ns.keys.Prefix() }

// NewClient returns a Client scoped to this namespace.
func (ns *Namespace) NewClient() *Client {
	return &Client{rdb: rdb.NewRDB(ns.client, ns.keys)}
}

// NewInspector returns an Inspector scoped to this namespace.
func (ns *Namespace) NewInspector() *Inspector {
	return &Inspector{rdb: rdb.NewRDB(ns.client, ns.keys)}
}

// NewServer returns a Server scoped to this namespace.
func (ns *Namespace) NewServer(cfg ServerConfig) *Server {
	s := NewServer(ns.client, cfg)
	s.rdb = rdb.NewRDB(ns.client, ns.keys)
	return s
}

// NewScheduler returns a Scheduler scoped to this namespace.
func (ns *Namespace) NewScheduler(cfg SchedulerConfig) *Scheduler {
	s := NewScheduler(ns.client, cfg)
	s.rdb = rdb.NewRDB(ns.client, ns.keys)
	return s
}
```

**Note on `NewServer`/`NewScheduler`:** they reuse the existing constructors for their config normalization (~10 defaults each) and then swap the `rdb`. That avoids duplicating the defaulting logic, at the cost of building an RDB twice. If you prefer, extract the normalization into an unexported `newServer(r, cfg, keys)` and have both call it — **that is the cleaner shape; do it if it is not disruptive.** Report which you chose.

- [ ] **Step 4: Run the tests**

```bash
go test . -run 'TestNamespace|TestDefaultConstructors' -v -p 1 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add namespace.go namespace_test.go
git commit -m "feat: add Namespace to scope keys under a prefix

Derive Client, Server, Scheduler, and Inspector from one handle so a
prefix cannot be set on three of them and forgotten on the fourth — a
mistake that produces tasks nobody processes, silently.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 8: Acceptance test — two namespaces, one Redis DB

**Files:** Create `namespace_integration_test.go`

This is the test that proves the feature does the thing it exists for.

- [ ] **Step 1: Write the test**

```go
package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type nsTaskA struct{ V int `json:"v"` }

func (nsTaskA) Kind() string { return "ns:a" }

type nsTaskB struct{ V int `json:"v"` }

func (nsTaskB) Kind() string { return "ns:b" }

// TestTwoNamespacesDoNotCollide is the acceptance test for key prefixes.
//
// Before prefixes, two applications on one Redis database contended for the
// single global leader lock, and the loser's schedules stopped firing with no
// error. This asserts both namespaces elect their own leader and both fire.
func TestTwoNamespacesDoNotCollide(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	nsA := NewNamespace(client, "appa")
	nsB := NewNamespace(client, "appb")

	firedA := make(chan struct{}, 10)
	firedB := make(chan struct{}, 10)

	muxA := NewMux()
	AddHandler(muxA, func(ctx context.Context, task *Task[nsTaskA]) error {
		firedA <- struct{}{}
		return nil
	})
	muxB := NewMux()
	AddHandler(muxB, func(ctx context.Context, task *Task[nsTaskB]) error {
		firedB <- struct{}{}
		return nil
	})

	srvA := nsA.NewServer(ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	srvB := nsB.NewServer(ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srvA.Start(ctx, muxA); err != nil {
		t.Fatalf("server A start: %v", err)
	}
	defer srvA.Shutdown(context.Background())
	if err := srvB.Start(ctx, muxB); err != nil {
		t.Fatalf("server B start: %v", err)
	}
	defer srvB.Shutdown(context.Background())

	schedA := nsA.NewScheduler(SchedulerConfig{})
	schedB := nsB.NewScheduler(SchedulerConfig{})
	if err := RegisterInterval(schedA, time.Second, nsTaskA{V: 1}); err != nil {
		t.Fatalf("register A: %v", err)
	}
	if err := RegisterInterval(schedB, time.Second, nsTaskB{V: 2}); err != nil {
		t.Fatalf("register B: %v", err)
	}
	if err := schedA.Start(ctx); err != nil {
		t.Fatalf("scheduler A start: %v", err)
	}
	defer schedA.Shutdown(context.Background())
	if err := schedB.Start(ctx); err != nil {
		t.Fatalf("scheduler B start: %v", err)
	}
	defer schedB.Shutdown(context.Background())

	// Both must fire. Before prefixes, exactly one would.
	deadline := time.After(20 * time.Second)
	gotA, gotB := false, false
	for !gotA || !gotB {
		select {
		case <-firedA:
			gotA = true
		case <-firedB:
			gotB = true
		case <-deadline:
			t.Fatalf("timed out: namespace A fired=%v, namespace B fired=%v "+
				"(both must fire; one-only means they share a leader lock)", gotA, gotB)
		}
	}

	// Each namespace's Inspector must see only its own queues.
	insA := nsA.NewInspector()
	qs, err := insA.Queues(ctx)
	if err != nil {
		t.Fatalf("inspector A queues: %v", err)
	}
	if len(qs) == 0 {
		t.Fatal("inspector A saw no queues")
	}

	// The leader locks must be distinct keys, both present.
	for _, key := range []string{"appa:leader", "appb:leader"} {
		n, err := client.Exists(ctx, key).Result()
		if err != nil {
			t.Fatalf("exists %s: %v", key, err)
		}
		if n != 1 {
			t.Errorf("leader key %s missing — namespaces are sharing a lock", key)
		}
	}
}

// TestNamespaceIsolatesEnqueue asserts data written in one namespace is
// invisible in another.
func TestNamespaceIsolatesEnqueue(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	nsA := NewNamespace(client, "isoa")
	nsB := NewNamespace(client, "isob")

	if _, err := Enqueue(ctx, nsA.NewClient(), nsTaskA{V: 1}, WithQueue("shared")); err != nil {
		t.Fatalf("enqueue into A: %v", err)
	}

	qsB, err := nsB.NewInspector().Queues(ctx)
	if err != nil {
		t.Fatalf("inspector B queues: %v", err)
	}
	for _, q := range qsB {
		if q.Pending > 0 {
			t.Errorf("namespace B sees %d pending task(s) in queue %q enqueued by namespace A",
				q.Pending, q.Queue)
		}
	}
}
```

- [ ] **Step 2: Run it**

```bash
go test . -run 'TestTwoNamespaces|TestNamespaceIsolates' -v -p 1 2>&1 | tail -30
```

Expected: PASS. If `TestTwoNamespacesDoNotCollide` times out with one side false, a global key is still shared — go back to the Task 5/6 greps.

- [ ] **Step 3: Commit**

```bash
git add namespace_integration_test.go
git commit -m "test: assert two namespaces on one Redis DB do not collide

Both schedulers must elect their own leader and fire. This is the
behaviour the feature exists for: before prefixes, the loser of the
single global leader lock stopped firing silently.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 9: Cluster slot-neutrality test

**Files:** Modify `cluster_test.go`

The whole Cluster-safety argument rests on the claim that a prefix does not move the slot. Assert it.

- [ ] **Step 1: Read the existing assertions**

```bash
sed -n '470,495p' cluster_test.go
```

Lines 480 and 484 hardcode `"chronos:{"+q1+"}:stream"`. Replace those literals with the builders, and add the new assertion below.

- [ ] **Step 2: Add the test**

```go
// TestPrefixIsSlotNeutral asserts a key prefix does not change the cluster
// slot: Redis hashes only the first {...} pair and ignores bytes before it.
// Every multi-key Lua script depends on a queue's keys sharing one slot, so if
// this were false the prefix feature would break clustered deployments.
func TestPrefixIsSlotNeutral(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	ctx := context.Background()

	def := base.DefaultKeys
	custom := base.NewKeys("someotherapp")

	for _, q := range []string{"default", "critical"} {
		want, err := client.ClusterKeySlot(ctx, def.StreamKey(q)).Result()
		if err != nil {
			t.Fatalf("slot for default prefix: %v", err)
		}
		got, err := client.ClusterKeySlot(ctx, custom.StreamKey(q)).Result()
		if err != nil {
			t.Fatalf("slot for custom prefix: %v", err)
		}
		if got != want {
			t.Errorf("queue %q: prefixed key hashes to slot %d, unprefixed to %d — "+
				"a prefix must not move the slot", q, got, want)
		}
	}
}
```

Add the `base` import if missing.

- [ ] **Step 3: Run it**

```bash
go test . -run TestPrefixIsSlotNeutral -v -p 1
```

Expected: SKIP if `REDIS_CLUSTER_ADDRS` is unset (that is fine and normal), PASS if a cluster is available. To actually run it:

```bash
cd deploy/redis-cluster && docker compose up -d && cd ../..
REDIS_CLUSTER_ADDRS=127.0.0.1:7001 go test . -run TestPrefixIsSlotNeutral -v -p 1
```

**Run it at least once against a real cluster and report the result.** A skipped test proves nothing here.

- [ ] **Step 4: Commit**

```bash
git add cluster_test.go
git commit -m "test: assert a key prefix does not move the cluster slot

Redis hashes only the first {...} pair, so bytes before it are ignored.
Every multi-key script depends on a queue's keys sharing one slot.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 10: Fix the soak sampler

**Files:** `benchmarks/soak/sampler.go`, `benchmarks/soak/sampler_test.go`

`benchmarks/` is a separate module and builds keys by hand, so it will **not** fail to compile — it will silently report zeros.

- [ ] **Step 1: Read the current code**

```bash
sed -n '50,90p' benchmarks/soak/sampler.go
```

Four hardcoded sites: line 57 `"chronos:{" + q + "}:"`, line 77 `"chronos:*:unique:*"`, line 81 `"chronos:*:group*"`, line 84 `"chronos:schedules"`.

- [ ] **Step 2: Parameterize the prefix**

`benchmarks/` is a separate module, so `internal/base` is **not importable**. A
local constant is the right answer; keep a comment pointing at the real source
of truth.

Add to the `Sampler` struct (`sampler.go:16-24`) and its constructor
(`sampler.go:26-29`):

```go
// DefaultPrefix mirrors internal/base.DefaultPrefix. The soak deliberately
// observes Redis from the outside (see docs/OBSERVING.md), so it cannot import
// the internal key builders — keep this in sync with internal/base/keys.go.
const DefaultPrefix = "chronos"

type Sampler struct {
	rdb       redis.UniversalClient
	queues    []string
	completed *atomic.Int64
	prefix    string

	start    time.Time
	prevDone int64
	prevAt   time.Time
}

func NewSampler(rdb redis.UniversalClient, queues []string, completed *atomic.Int64) *Sampler {
	return NewSamplerWithPrefix(rdb, queues, completed, DefaultPrefix)
}

func NewSamplerWithPrefix(rdb redis.UniversalClient, queues []string, completed *atomic.Int64, prefix string) *Sampler {
	now := time.Now()
	return &Sampler{rdb: rdb, queues: queues, completed: completed, prefix: prefix, start: now, prevAt: now}
}
```

Then replace the four hardcoded sites in `Collect`:

```go
		p := s.prefix + ":{" + q + "}:"                                     // was "chronos:{" + q + "}:"
	if out.Unique, err = scanCount(ctx, s.rdb, s.prefix+":*:unique:*"); err != nil {
	if out.Groups, err = scanCount(ctx, s.rdb, s.prefix+":*:group*"); err != nil {
	if out.Schedules, err = s.rdb.HLen(ctx, s.prefix+":schedules").Result(); err != nil {
```

Keeping `NewSampler`'s signature intact avoids touching every caller; only
tests that want a custom prefix use the `WithPrefix` form.

- [ ] **Step 3: Update `sampler_test.go`**

Lines 27-34 seed 8 hardcoded keys (`chronos:{soak-a}:stream`, `:retry`,
`:scheduled`, `:archived`, `:completed`, `:unique:…`, `:group:g1`, and
`chronos:schedules`). Build them from the same constant the sampler uses, and
add one case constructing the sampler with a non-default prefix to prove the
parameterization actually flows through — seeding under `chronos:` and sampling
under `other:` must report zeros.

- [ ] **Step 4: Test**

```bash
cd benchmarks && go test ./soak/... && cd ..
make bench-build
```

Expected: pass, and `bench-build` compiles.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/
git commit -m "fix(soak): derive sampled keys from a configurable prefix

The sampler built keys by hand and, being outside the internals module,
would not fail to compile under a namespace — it would report zeros,
the worst possible failure for a leak detector.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 11: Documentation

**Files:** `README.md`, `doc.go`, `example_test.go`

- [ ] **Step 1: Add a runnable example**

In `example_test.go`, following the existing example style:

```go
// ExampleNewNamespace shows two applications sharing one Redis database.
func ExampleNewNamespace() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	// Each application scopes its keys, so their schedulers elect separate
	// leaders instead of contending for one global lock.
	ns := chronos.NewNamespace(rdb, "inspireme")

	client := ns.NewClient()
	_, _ = chronos.Enqueue(context.Background(), client, EmailArgs{To: "a@b.c"},
		chronos.WithQueue("default"))
}
```

Confirm the example compiles: `go test . -run Example -p 1`.

- [ ] **Step 2: README section**

Add a "Sharing a Redis database" subsection near the configuration docs. Draft
(adapt heading level and tone to the surrounding sections):

````markdown
### Sharing a Redis database

By default every key is prefixed `chronos:`. Two chronos-go deployments on the
same Redis database therefore collide — most damagingly on `chronos:leader`, the
single lock that decides which process fires schedules. The loser stops firing
and nothing is logged. Give each deployment its own namespace:

```go
ns := chronos.NewNamespace(rdb, "inspireme")

client := ns.NewClient()
srv    := ns.NewServer(chronos.ServerConfig{Queues: map[string]int{"default": 1}})
sched  := ns.NewScheduler(chronos.SchedulerConfig{})
insp   := ns.NewInspector()
```

Derive all four from one `Namespace`. Setting a prefix on some and not others
produces tasks that are enqueued but never processed, with no error anywhere.

The prefix must not contain braces (they break the Redis Cluster hash tag),
glob metacharacters, or whitespace; `NewNamespace` panics on those. A prefix
works identically on standalone and Cluster, which a separate logical database
does not — Cluster has only DB 0.

The package-level `NewClient`/`NewServer`/`NewScheduler`/`NewInspector` are
equivalent to the default `chronos` prefix, so existing deployments are
unaffected and need no migration.

**Changing the prefix of a running deployment requires draining in-flight tasks
first.** A task's unique-lock key is stored in its body, so a task enqueued
under one prefix cannot have its lock released under another.
````

- [ ] **Step 3: `doc.go`**

Add a short paragraph pointing at `Namespace` where the package overview describes configuration.

- [ ] **Step 4: Verify docs build**

```bash
go doc . Namespace | head -20
go test . -run Example -p 1
```

- [ ] **Step 5: Commit**

```bash
git add README.md doc.go example_test.go
git commit -m "docs: document namespaced key prefixes

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 12: Full gate and PR

- [ ] **Step 1: Run the full check**

```bash
make check 2>&1 | tail -20
```

`make check` = `fmt-check` + `vet` + `test-race` + `test-contrib` + `bench-build`. It must be green for a PR — this repo's CONTRIBUTING requires it.

- [ ] **Step 2: Re-run the completeness greps**

Both must return nothing:

```bash
grep -rnE 'base\.(QueueKeyPrefix|StreamKey|TaskKeyPrefix|TaskKey|QueuesKey|PausedKey|SchedulesKey|RetryKey|ArchivedKey|CompletedKey|ScheduledKey|GroupKey|GroupResultKey|UniqueKey|LeaderKey|LeaderResignChannel|PeriodicDedupKey|ScheduleLastFiredKey)\(' internal/rdb/*.go *.go | grep -v '_test.go'
grep -rn '"chronos:' --include=*.go . | grep -v '_test.go' | grep -v 'internal/base/prefix.go'
```

The second may legitimately hit `benchmarks/soak/sampler.go`'s default constant — confirm that is the only hit and that it is a documented default, not a hardcoded key.

- [ ] **Step 3: Push and open the PR — then stop**

```bash
git push -u origin feat/key-prefix
gh pr create --base main --title "feat: configurable key prefix for sharing a Redis database" --body "$(cat <<'EOF'
## Problem

Two applications sharing one Redis database cannot both run chronos-go.

`chronos:leader` is a single global key with no namespace, and `fireDue` runs
only on the instance holding it — firing the schedules registered in *that*
process. So if two applications run a `Scheduler` against the same database,
they contend for one lock and the loser stops firing entirely. Nothing errors,
nothing logs; its cron jobs simply never run again. `chronos:queues`,
`chronos:paused`, and `chronos:schedules` are shared the same way.

A separate logical database fixes this without a library change, and was the
first option considered. It was rejected because **Redis Cluster has only
DB 0** — any deployment that later moves to Cluster silently loses the
isolation.

## Change

A `Namespace` handle scopes every key under a prefix:

```go
ns := chronos.NewNamespace(rdb, "inspireme")
client := ns.NewClient()
srv    := ns.NewServer(cfg)
sched  := ns.NewScheduler(cfg)
insp   := ns.NewInspector()
```

All four derive from one handle on purpose. A `KeyPrefix` field on each config
would let a caller set three and forget the fourth — a `Client` writing under
one prefix while a `Server` reads another produces tasks nobody processes, with
no error. The handle makes that unrepresentable.

**The default prefix is `chronos`, so existing keys are byte-identical and no
migration is needed.** `TestDefaultKeysUnchanged` asserts every one of the 18
builders against its pre-existing literal.

Internally the 18 key builders became methods on a `base.Keys` value carried by
`rdb.RDB`; package-level wrappers over a default-prefix `Keys` keep existing
call sites compiling. Three sites that built keys in the root package — and
would have bypassed any prefix — moved behind `RDB`.

No Lua script changed. None contained the `chronos:` literal; the two that
concatenate a task-key prefix receive it via ARGV, and an extended test asserts
that invariant under a custom prefix.

## Verification

- `TestTwoNamespacesDoNotCollide` — two namespaces on one database each elect
  their own leader and both fire. This is the behaviour the feature exists for.
- `TestPrefixIsSlotNeutral` — run against a real cluster: a prefix does not
  move the hash slot, since Redis hashes only the first `{...}` pair.
- `make check` green.

## Limitation

Changing the prefix of a **running** deployment requires draining in-flight
tasks first: a task's unique-lock key is stored in its body, so it cannot be
released under a different prefix. Adopting a prefix on a fresh deployment, or
keeping the default, is unaffected. Documented in the README.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

**Stop after opening the PR and report.** Merging is the user's decision.

Releasing is out of scope for this plan. The design targets **v1.1.0** (additive,
no breaking change); tagging happens after merge. There is no CHANGELOG file in
this repo, so nothing to update.

---

## Completion criteria

- [ ] `TestDefaultKeysUnchanged` passes — existing deployments see byte-identical keys
- [ ] `TestTwoNamespacesDoNotCollide` passes — both schedulers elect their own leader and fire
- [ ] `TestNamespaceIsolatesEnqueue` passes
- [ ] `TestPrefixIsSlotNeutral` run against a **real cluster** at least once, not skipped
- [ ] Both completeness greps return nothing
- [ ] `make check` green
- [ ] Soak sampler no longer hardcodes `chronos:` keys

## Out of scope

- Using per-package prefixes to drop the `-p 1` test constraint (a real follow-up win, separate change)
- Migrating an existing deployment to a new prefix (documented as requiring a drain)
- Any `contrib/` change — both modules take a `*chronos.Inspector` and inherit the prefix
