# Honor SchedulerConfig.Location for cron schedules

- Date: 2026-07-19
- Status: design approved (pending implementation plan)
- Repo: `github.com/kenshin579/chronos-go`

## Problem

`SchedulerConfig.Location` is documented as "the timezone for cron schedules"
(`scheduler.go:23-24`), but cron schedules registered via `RegisterCron` do **not**
actually evaluate in that timezone. They evaluate in the process-local timezone
(`time.Local`), so in a UTC container all cron schedules fire at UTC wall-clock
times regardless of the configured `Location`.

### Root cause

`RegisterCron` (`scheduler.go:118`) parses the spec with `cron.ParseStandard(spec)`.
For a plain spec (no `CRON_TZ=`/`TZ=` prefix), robfig/cron sets the resulting
`*cron.SpecSchedule.Location` to `time.Local`. robfig's `SpecSchedule.Next(t)`
converts `t` into the schedule's own `Location` before computing the next fire, so
the schedule's `Location` — not the caller's `t` — governs the wall-clock
interpretation.

`fireDue` does compute `now := time.Now().In(s.cfg.Location)` (`scheduler.go:239`),
but that only affects which instant is compared; `Next()` re-localizes to the
schedule's `time.Local`. As a result `cfg.Location` is effectively ignored for the
actual cron evaluation.

### Observed impact (downstream: moneyflow)

moneyflow configures `NewScheduler(..., SchedulerConfig{Location: Asia/Seoul})` and
registers KST-intended crons (e.g. `0 5 * * *` = 05:00 KST). In production (UTC
container) the recorded last-fire instants (`chronos:sched:<id>:last`, stored as
`when.Unix()`) land on UTC wall-clock times:

| spec | intended (KST) | observed `:last` |
|------|----------------|------------------|
| `0 5 * * *`  | 20:00:00Z | **05:00:00Z** |
| `0 1 * * *`  | 16:00:00Z | **01:00:00Z** |
| `0 20 * * *` | 11:00:00Z | **20:00:00Z** |

i.e. every schedule fires ~9h off from intent.

## Scope

- Fix `RegisterCron` so parsed cron schedules evaluate in `SchedulerConfig.Location`.
- `RegisterInterval` is unaffected (interval schedules via `cron.Every` are
  timezone-independent).
- Preserve a spec's own `CRON_TZ=`/`TZ=` prefix (per-schedule timezone still wins).
- Out of scope (YAGNI): per-schedule Location option API, changes to `fireDue`/
  `computeFires`, changes to interval scheduling.

## Design

In `RegisterCron`, after `cron.ParseStandard(spec)`, inject `s.cfg.Location` into the
parsed schedule **only when the spec did not specify its own timezone**:

```go
sched, err := cron.ParseStandard(spec)
if err != nil {
    return fmt.Errorf("chronos: invalid cron spec %q: %w", spec, err)
}
// robfig/cron defaults SpecSchedule.Location to time.Local for a spec without a
// CRON_TZ=/TZ= prefix, and Next() evaluates in that Location — so cfg.Location is
// otherwise ignored. Apply cfg.Location when the spec did not set its own zone.
if ss, ok := sched.(*cron.SpecSchedule); ok && ss.Location == time.Local {
    ss.Location = s.cfg.Location
}
```

Rationale:
- `cfg.Location` is always non-nil at this point (`NewScheduler` defaults it to
  `time.Local`, `scheduler.go:64-65`), so the assignment is safe; when it is the
  default `time.Local` the write is a harmless no-op.
- The `ss.Location == time.Local` guard distinguishes "no zone in spec" (robfig set
  `time.Local`, same package pointer) from "explicit `CRON_TZ=`" (robfig set a
  different `*Location`), so an explicit per-schedule timezone is preserved.
- Type assertion to `*cron.SpecSchedule` is safe: `ParseStandard` returns either a
  `*SpecSchedule` (standard/`@`-descriptor specs) or a `ConstantDelaySchedule`
  (`@every`); the latter is interval-based and needs no timezone, so skipping it is
  correct.

Alternative considered — injecting a `CRON_TZ=<loc>` prefix into the spec string —
was rejected: it relies on `Location.String()` round-tripping (`time.Local` renders
as `"Local"`) and is more fragile than setting the documented field directly.

## Testing (white-box, `scheduler_test.go`, no Redis)

`register` stores the parsed schedule in `s.entries[i].schedule`, so tests can
inspect it directly:

1. **Location applied**: `NewScheduler(cfg{Location: Asia/Seoul})` +
   `RegisterCron("0 5 * * *")` → the registered schedule's `Next(<a base time>)`
   returns an instant whose UTC value is `20:00:00Z` (05:00 KST), not `05:00:00Z`.
2. **Explicit CRON_TZ preserved**: `RegisterCron("CRON_TZ=America/New_York 0 5 * * *")`
   under a Seoul scheduler → `Next(...)` evaluates in New York, not Seoul.
3. **Default unchanged (no regression)**: `NewScheduler(cfg{})` (Location defaults to
   `time.Local`) + `RegisterCron("0 5 * * *")` → schedule evaluates in `time.Local`.
4. **Interval unaffected**: `RegisterInterval` still produces a `cron.Every` schedule
   (sanity; no timezone semantics).

## Release & downstream

- Bugfix on `main` → release **v1.0.1** (patch on the v1.x line).
- Downstream `moneyflow.advenoh.pe.kr/backend`: bump `go.mod`
  `github.com/kenshin579/chronos-go v0.12.0 → v1.0.1` + `go mod tidy`.
  - Compatibility verified: moneyflow already builds cleanly against v1.0.0
    (`go build ./...` exit 0) with no source changes, so the bump is drop-in; run
    the full test suite after bumping to confirm.
  - moneyflow already wires `SchedulerConfig{Location: Asia/Seoul}`, so no moneyflow
    code change is needed — the fix takes effect purely via the version bump.
- Deploy of moneyflow-be is a separate, gated step (handled in another session).

## Conventions

This repo requires **English** for commits, PRs, and release notes (#32); this
design doc and all related git artifacts follow that.
