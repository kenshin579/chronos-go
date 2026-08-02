# Configurable key prefix design

Date: 2026-08-02

## Background

Every Redis key chronos-go writes is built by one of 18 pure functions in
`internal/base/keys.go`, and all of them hardcode the literal `chronos:`:

```go
func QueueKeyPrefix(qname string) string {
	return "chronos:{" + qname + "}:"
}

func LeaderKey() string { return "chronos:leader" }
```

Per-queue keys wrap the queue name in a Redis Cluster hash tag
(`chronos:{queue}:stream`) so that all of a queue's keys share one slot. Five
keys are deliberately global with no hash tag: `chronos:queues`,
`chronos:paused`, `chronos:schedules`, `chronos:leader`, and
`chronos:sched:<id>:last` (plus the `chronos:leader:resign` pub/sub channel).

There is no prefix, namespace, or key-prefix option anywhere in the library —
not in any config struct, not in the README, not in the docs. It was never
considered, so there is no prior decision to overturn.

## Problem

**Two applications sharing one Redis database cannot both run chronos-go.**

The global keys have no hash tag *and* no namespace. The worst of them is
`chronos:leader`: a single global lock. `Scheduler.fireDue` runs only on the
instance holding that lock, and it fires **the schedules registered in its own
process memory**.

So if app A and app B both run a `Scheduler` against the same Redis DB, they
contend for one lock, and whichever loses stops firing entirely. Nothing errors.
Nothing logs. App B's cron jobs simply never run again.

This is not hypothetical. In the deployment that motivated this design,
`moneyflow-be` runs 11 chronos schedules on `redis-master` DB 0, and a second
application was about to be pointed at the same DB. Bringing it up would have
silently stopped all 11 of moneyflow's jobs.

`chronos:queues`, `chronos:paused`, and `chronos:schedules` are shared the same
way, so `Inspector` and the CLI would also report a merged view across
applications.

### Why a separate logical DB is not the answer

Selecting a different Redis DB per application does isolate everything, and it
needs no library change. It was the first option considered and rejected for one
reason: **Redis Cluster has only DB 0.** Any deployment that later moves to
Cluster loses the isolation, and the failure mode on that day is the silent one
described above. A key prefix works identically on standalone and Cluster.

## Goals

- Let two or more applications share one Redis database (or one Cluster) without
  their chronos-go deployments seeing or blocking each other.
- Change nothing for existing deployments. Byte-identical keys by default.
- Make the *misconfiguration* failure mode impossible by construction, not just
  documented.

## Non-goals

- Using the prefix for test isolation to drop the `-p 1` constraint in `make
  test`. This is a real secondary benefit (each test package could take its own
  prefix instead of sharing and flushing DB 15) but it is a separate change.
- Per-queue or per-task namespacing. The unit is the whole deployment.
- Migrating an existing deployment to a new prefix. See "Known limitation".

---

## Design

### Public API

The four existing constructors are unchanged and keep producing exactly today's
keys:

```go
chronos.NewClient(rdb)
chronos.NewServer(rdb, cfg)
chronos.NewScheduler(rdb, cfg)
chronos.NewInspector(rdb)
```

Callers that need a namespace go through a handle:

```go
ns := chronos.NewNamespace(rdb, "inspireme")

client := ns.NewClient()
srv    := ns.NewServer(chronos.ServerConfig{Queues: map[string]int{"default": 1}})
sched  := ns.NewScheduler(chronos.SchedulerConfig{})
insp   := ns.NewInspector()
```

**The handle is the whole point of the design.** The alternative — a
`KeyPrefix` field on `ServerConfig`/`SchedulerConfig` plus a variadic option on
`NewClient`/`NewInspector` — was rejected because it lets a caller set the
prefix in three places and forget the fourth. A `Client` writing under prefix A
while a `Server` reads under prefix B produces tasks that are never processed,
with no error anywhere. That is the same class of silent failure this feature
exists to eliminate, and it would be worse here: the cost of the mistake is not
"my app is broken" but "someone else's app stopped".

Deriving all four from one handle makes that mistake unrepresentable.

`contrib/prometheus` and `contrib/webui` need no changes — both take a
`*chronos.Inspector`, so they inherit the prefix.

### Key layout

The prefix **replaces** the `chronos` segment rather than being prepended:

| | default | `NewNamespace(rdb, "inspireme")` |
|---|---|---|
| stream | `chronos:{q}:stream` | `inspireme:{q}:stream` |
| leader | `chronos:leader` | `inspireme:leader` |

The default prefix is the string `chronos`, so existing keys are byte-identical
and no migration is needed.

Replacement is preferred over prepending (`inspireme:chronos:{q}:stream`)
because it is a superset: a caller who wants both segments passes
`"chronos:inspireme"` and gets `chronos:inspireme:{q}:stream`. Prepending offers
no equivalent way to get the shorter form.

### Cluster safety

Prepending anything before the hash tag is slot-neutral. Redis computes the slot
from the first `{`…`}` pair only; bytes before `{` are ignored. So
`inspireme:{q}:stream` and `chronos:{q}:stream` hash to the same slot, every
queue's keys stay co-located, and all 17 Lua scripts remain valid unchanged.

This holds only because the codebase already keeps global keys out of multi-key
scripts. Verified across all 17 scripts: each one's `KEYS[]` set is either
entirely within a single queue's tag, or a single global key. The discipline is
explicit in the source, e.g. `internal/rdb/rdb.go:80-85` notes that the
`QueuesKey` write is separated from the atomic script because it has no hash tag
and therefore a different slot. `internal/rdb/group.go:192-194` guards the one
script that spans two queues by rejecting mismatched queues at runtime.

### Prefix validation

`NewNamespace` **panics** on an invalid prefix.

This follows the existing precedent for programmer error in this library:
`AddHandler` panics on a duplicate `Kind`. A prefix is almost always a constant,
not runtime data, and the consequence of proceeding with a bad one is silent
misrouting of every key — so failing at startup is strictly safer than
returning an error a caller may ignore. None of the four existing constructors
return an error, so adding the library's only error-returning constructor here
would also be inconsistent.

Rejected characters and why:

| Rejected | Reason |
|---|---|
| `{`, `}` | Corrupts the hash tag. `my{app}:{q}:stream` has tag `app` — identical for every queue, so **every queue collapses onto one slot**. No CROSSSLOT error; just a silent loss of distribution. |
| `*`, `?`, `[`, `]` | `internal/rdb/inspect.go:114-133` derives a `SCAN` pattern from `ScheduleLastFiredKey("*")`. A glob metacharacter in the prefix corrupts the pattern and the scan matches the wrong keys. |
| whitespace, control chars | Valid in Redis keys but a reliable source of copy-paste and shell-quoting bugs. |
| empty | Would produce keys starting with `:`. |

A trailing `:` is trimmed rather than rejected, so `"myapp"` and `"myapp:"`
behave identically.

### Internal structure

`internal/base` gains a `Keys` value that carries the prefix, and the 18 key
builders become methods on it. The existing package-level functions stay as thin
wrappers over a default-prefix `Keys`:

```go
type Keys struct{ prefix string }

func (k Keys) QueueKeyPrefix(qname string) string {
	return k.prefix + ":{" + qname + "}:"
}

// Package-level wrappers keep the default-prefix call sites (mostly tests)
// compiling unchanged.
func QueueKeyPrefix(qname string) string { return DefaultKeys.QueueKeyPrefix(qname) }
```

Keeping the wrappers is deliberate. There are 133 `base.X(...)` call sites in
tests; without wrappers every one of them would need a receiver threaded in.
With wrappers, the production change is confined to the ~88 sites that actually
need to honour a prefix, and the golden-key tests keep working as written.

`rdb.RDB` gains a `keys base.Keys` field supplied by `NewRDB`. Since
`internal/rdb` is private, changing that signature costs nothing externally.
All four public types already construct their own `*rdb.RDB`, so the handle
simply passes its prefix down.

### Fixing the root-package leaks

Six call sites build keys **outside** `internal/rdb` today, and each one would
bypass the prefix:

| Site | What it does |
|---|---|
| `inspector.go:98-104` (4 calls) | `zsetKeyForState` is a package-level function with no receiver, so it cannot reach `i.rdb` |
| `chronos.go:295` | `Client` builds the unique key itself and passes the string down |
| `scheduler.go:306` | `Scheduler` builds the periodic dedup key itself and passes the string down |

All three move into `rdb.RDB` methods. This is not optional polish — it is what
makes "the key builder never escapes `internal/rdb`" true, and therefore what
makes the prefix impossible to bypass.

### Lua scripts

**No script changes.** None of the 17 scripts contains the literal `chronos`;
all receive keys through `KEYS[]`.

Two scripts concatenate a task-key prefix received via `ARGV` —
`forwardCmd` (`internal/rdb/forward.go:22-30`) and `trimArchivedCmd`
(`internal/rdb/janitor.go:21-50`):

```lua
redis.call("HSET", ARGV[3] .. id, "state", ARGV[4])
```

`ARGV[3]` is `base.TaskKeyPrefix(qname)`, so both keep working as long as that
builder is prefix-aware. The invariant is already guarded by a test at
`internal/base/keys_test.go:33-36`; that test must be extended to cover a
custom prefix.

### Also in scope: the soak sampler

`benchmarks/soak/sampler.go` builds keys by hand and bypasses `internal/base`
entirely (it is outside the module path for internals):

```go
p := "chronos:{" + q + "}:"                  // :57
scanCount(ctx, s.rdb, "chronos:*:unique:*")  // :77
scanCount(ctx, s.rdb, "chronos:*:group*")    // :81
s.rdb.HLen(ctx, "chronos:schedules")         // :84
```

These must be updated in the same change. They will not fail to compile, and if
missed the soak test silently reports zeros instead of erroring — the worst
possible failure for a leak detector.

---

## Testing

| Test | What it proves |
|---|---|
| Golden keys, default prefix | Existing deployments are byte-identical. Every one of the 18 builders asserted against the current literal. |
| Golden keys, custom prefix | The prefix lands where intended for all 18. |
| `TaskKeyPrefix(q) + id == TaskKey(q, id)` under a custom prefix | The `forwardCmd`/`trimArchivedCmd` ARGV invariant survives. |
| `ClusterKeySlot` equality across prefixes | Slot-neutrality — the claim the whole Cluster-safety argument rests on. |
| Panic cases | Each rejected character class, plus empty; and that a trailing `:` is trimmed rather than rejected. |
| **Two namespaces, one Redis DB** | The acceptance test. See below. |

The acceptance test is the one that matters:

1. Start a `Scheduler` in namespace A and another in namespace B against the
   same Redis DB, each with its own registered schedule.
2. Assert **both** acquire leadership — they take `A:leader` and `B:leader`,
   not one shared lock.
3. Assert both schedules fire.
4. Enqueue into namespace A; assert namespace B's `Inspector` reports nothing.

Step 2 is the direct proof that adding a second application no longer stops the
first one's cron jobs.

## Known limitation

`base.Task.UniqueKey` (`internal/base/task.go:88`) persists the **full** unique
key inside the task JSON. A task enqueued under one prefix therefore carries
that prefix in its body, and `releaseUniqueCmd` under a different prefix would
try to release a key it never wrote.

Consequence: **changing the prefix of a running deployment requires draining
in-flight tasks first.** Adopting a prefix on a fresh deployment is unaffected,
and existing deployments that keep the default are unaffected. This will be
documented in the README next to the feature.

`ConsumerGroup = "chronos"` (`internal/rdb/rdb.go:18`) is left alone. It names a
consumer group *inside* a stream, and stream keys are already namespaced, so two
namespaces cannot collide on it.

## Versioning

**v1.2.0** — additive, no breaking change. The v1 prep design
(`docs/superpowers/specs/2026-07-15-v1-prep-design.md`) classifies additive
improvements as v1.x backlog, and the default prefix keeps the public behaviour
identical: all four public constructors are unchanged byte-for-byte, and
`TestDefaultKeysUnchanged` asserts the key layout is too.

(This section originally said v1.1.0, written without checking the tag list.
v1.1.0 was already released on `f2c3eaa`, so this work lands as v1.2.0.)
