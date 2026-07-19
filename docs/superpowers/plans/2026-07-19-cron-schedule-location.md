# Honor SchedulerConfig.Location for cron schedules — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `RegisterCron` evaluate cron schedules in `SchedulerConfig.Location` instead of the process-local timezone, so a Seoul-configured scheduler fires KST crons at KST (not UTC).

**Architecture:** In `RegisterCron`, after `cron.ParseStandard`, set the parsed `*cron.SpecSchedule.Location` to `cfg.Location` when the spec carried no `CRON_TZ=` of its own (robfig leaves it as `time.Local`). This honors the documented `SchedulerConfig.Location` contract while preserving per-spec `CRON_TZ`.

**Tech Stack:** Go, `github.com/robfig/cron/v3` v3.0.1, `github.com/redis/go-redis/v9` v9.21.0, standard `testing`.

**Repos/branches:**
- chronos-go: `github.com/kenshin579/chronos-go`, branch `fix/cron-schedule-location` (design doc already committed).
- moneyflow: `moneyflow.advenoh.pe.kr`, new branch `fix/chronos-schedule-tz-bump`.

**Convention:** chronos-go requires **English** for commits, PRs, and release notes (#32). All chronos-go git artifacts in English. moneyflow keeps its Korean commit convention.

**Gates (approval required):** Task 3 (chronos release v1.0.1), Task 5 (moneyflow deploy — another session).

---

## File Structure

**chronos-go:**
- Modify `scheduler.go` — `RegisterCron` (~line 118): inject `cfg.Location` post-parse.
- Modify `scheduler_test.go` — add 3 white-box tests (package `chronos`).

**moneyflow:**
- Modify `backend/go.mod` / `go.sum` — chronos `v0.12.0 → v1.0.1`.

---

## Task 1: (chronos-go) Failing tests for Location handling

**Files:**
- Modify: `chronos-go/scheduler_test.go`

- [ ] **Step 1: confirm branch**

```bash
cd /Users/frankoh/src/workspace_moneyflow/chronos-go
git checkout fix/cron-schedule-location
```

- [ ] **Step 2: add tests.** Append to `scheduler_test.go`. It is `package chronos` and already imports `time` and `testing`. Add imports `"github.com/redis/go-redis/v9"` and `cron "github.com/robfig/cron/v3"` to the file's import block. Use a bare redis client (no server needed — `NewScheduler` does not connect, and `RegisterCron` does no I/O).

```go
func newOfflineScheduler(loc *time.Location) *Scheduler {
	// NewScheduler never dials; RegisterCron does no Redis I/O. A bare client keeps
	// these schedule-parsing tests hermetic (no live Redis, no t.Skip).
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	return NewScheduler(client, SchedulerConfig{Location: loc})
}

func TestRegisterCron_HonorsSchedulerLocation(t *testing.T) {
	seoul, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatalf("load Asia/Seoul: %v", err)
	}
	s := newOfflineScheduler(seoul)
	if err := RegisterCron(s, "0 5 * * *", tickArgs{}); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}
	ss, ok := s.entries[0].schedule.(*cron.SpecSchedule)
	if !ok {
		t.Fatalf("schedule type = %T, want *cron.SpecSchedule", s.entries[0].schedule)
	}
	if ss.Location != seoul {
		t.Errorf("schedule Location = %v, want Asia/Seoul", ss.Location)
	}
	// Behavior: 0 5 * * * must fire at 05:00 KST (= 20:00 UTC), not 05:00 UTC.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next := s.entries[0].schedule.Next(base)
	if h := next.In(seoul).Hour(); h != 5 {
		t.Errorf("next fire hour in KST = %d, want 5", h)
	}
	if h := next.UTC().Hour(); h != 20 {
		t.Errorf("next fire hour in UTC = %d, want 20 (05:00 KST)", h)
	}
}

func TestRegisterCron_PreservesExplicitCronTZ(t *testing.T) {
	seoul, _ := time.LoadLocation("Asia/Seoul")
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load America/New_York: %v", err)
	}
	s := newOfflineScheduler(seoul)
	if err := RegisterCron(s, "CRON_TZ=America/New_York 0 5 * * *", tickArgs{}); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}
	ss := s.entries[0].schedule.(*cron.SpecSchedule)
	if ss.Location.String() != ny.String() {
		t.Errorf("explicit CRON_TZ overridden: Location = %v, want America/New_York", ss.Location)
	}
}

func TestRegisterCron_DefaultLocationUnchanged(t *testing.T) {
	// No Location configured → NewScheduler defaults cfg.Location to time.Local,
	// so the schedule must remain time.Local (no regression).
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	s := NewScheduler(client, SchedulerConfig{})
	if err := RegisterCron(s, "0 5 * * *", tickArgs{}); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}
	ss := s.entries[0].schedule.(*cron.SpecSchedule)
	if ss.Location != time.Local {
		t.Errorf("default Location = %v, want time.Local", ss.Location)
	}
}
```

- [ ] **Step 3: run → the Location test must fail (others pass).**

Run: `go test . -run 'TestRegisterCron_HonorsSchedulerLocation|PreservesExplicitCronTZ|DefaultLocationUnchanged' -v`
Expected: `TestRegisterCron_HonorsSchedulerLocation` FAILS (`Location = Local, want Asia/Seoul`; KST hour = 14 not 5). `PreservesExplicitCronTZ` and `DefaultLocationUnchanged` PASS (they assert current behavior).

---

## Task 2: (chronos-go) Implement the fix

**Files:**
- Modify: `chronos-go/scheduler.go` (`RegisterCron`, ~line 118)

- [ ] **Step 1: inject cfg.Location post-parse.** In `RegisterCron`, replace:

```go
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return fmt.Errorf("chronos: invalid cron spec %q: %w", spec, err)
	}
```

with:

```go
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return fmt.Errorf("chronos: invalid cron spec %q: %w", spec, err)
	}
	// robfig/cron defaults SpecSchedule.Location to time.Local for a spec without a
	// CRON_TZ=/TZ= prefix, and SpecSchedule.Next evaluates in that Location — so
	// SchedulerConfig.Location would otherwise be ignored. Apply cfg.Location when the
	// spec did not set its own zone (an explicit CRON_TZ leaves Location != time.Local
	// and is preserved).
	if ss, ok := sched.(*cron.SpecSchedule); ok && ss.Location == time.Local {
		ss.Location = s.cfg.Location
	}
```

Ensure `scheduler.go` imports `"time"` (it already does — `cfg.Location` is `*time.Location`).

- [ ] **Step 2: run the new tests → all pass.**

Run: `go test . -run 'TestRegisterCron_HonorsSchedulerLocation|PreservesExplicitCronTZ|DefaultLocationUnchanged' -v`
Expected: all 3 PASS.

- [ ] **Step 3: vet + fmt + full package tests.**

Run: `go vet ./... && gofmt -l scheduler.go scheduler_test.go && go test . -p 1`
Expected: vet clean, `gofmt -l` prints nothing, package tests pass. (Scheduler tests that need Redis will `t.Skip` if none is running; that is pre-existing and fine. If Redis is available locally, run `make test` for the full suite.)

- [ ] **Step 4: commit, push, open PR (English).**

```bash
cd /Users/frankoh/src/workspace_moneyflow/chronos-go
git add scheduler.go scheduler_test.go
git commit -m "fix: honor SchedulerConfig.Location for cron schedules

RegisterCron parsed specs with cron.ParseStandard, which pins SpecSchedule.Location
to time.Local; SpecSchedule.Next then evaluates in time.Local, so cron schedules
fired in the process timezone (UTC in containers) instead of the configured
Location. Inject cfg.Location into the parsed schedule when the spec has no explicit
CRON_TZ, so schedules evaluate in the configured zone while per-spec CRON_TZ is
preserved."
git push -u origin fix/cron-schedule-location
gh pr create --repo kenshin579/chronos-go --base main --title "fix: honor SchedulerConfig.Location for cron schedules" --fill
```

---

## Task 3: (chronos-go) Release v1.0.1 — **GATE: after approval + PR merge**

**Files:** none (tagging)

- [ ] **Step 1: confirm approval.** Confirm the PR is merged and release is approved (outward-facing, hard to reverse).

- [ ] **Step 2: update main, run the release gate.**

```bash
cd /Users/frankoh/src/workspace_moneyflow/chronos-go
git checkout main && git pull origin main
go vet ./... && go test . -p 1   # or `make check` if Redis + tooling available
```
Expected: clean.

- [ ] **Step 3: tag + GitHub release (English notes).**

```bash
git tag v1.0.1
git push origin v1.0.1
gh release create v1.0.1 --repo kenshin579/chronos-go --generate-notes --title "v1.0.1"
```
Expected: release created at `v1.0.1`.

- [ ] **Step 4: verify tag.**

Run: `git tag --sort=-v:refname | head -1`
Expected: `v1.0.1`.

---

## Task 4: (moneyflow) Bump chronos to v1.0.1 — after Task 3

**Files:**
- Modify: `moneyflow.advenoh.pe.kr/backend/go.mod`, `go.sum`

- [ ] **Step 1: branch + bump.**

```bash
cd /Users/frankoh/src/workspace_moneyflow/moneyflow.advenoh.pe.kr
git checkout main && git pull origin main
git checkout -b fix/chronos-schedule-tz-bump
cd backend
go get github.com/kenshin579/chronos-go@v1.0.1
go mod tidy
```
Expected: `go.mod` shows `github.com/kenshin579/chronos-go v1.0.1`.

- [ ] **Step 2: build + full tests.**

Run: `make build && go test ./...`
Expected: build OK; tests pass. (Pre-existing unrelated failure `internal/chart TestService_Resamples_Weekly` — timezone/date-dependent, exists on main independent of this change — may still fail; confirm nothing else regresses. v1.0.0 was already verified to build clean; v1.0.1 = v1.0.0 + the schedule-location fix, no API change.)

- [ ] **Step 3: commit + push + PR.**

```bash
cd /Users/frankoh/src/workspace_moneyflow/moneyflow.advenoh.pe.kr
git add backend/go.mod backend/go.sum
git commit -m "fix: chronos-go v0.12.0 -> v1.0.1 (cron 스케줄 KST 타임존 반영)

chronos-go v1.0.1 은 SchedulerConfig.Location 을 cron 스케줄 평가에 반영한다.
moneyflow 는 이미 Location: Asia/Seoul 로 배선돼 있어 go.mod bump 만으로
일/월간 워밍 sweep 이 의도된 KST 시각에 발화한다(기존엔 UTC 로 9시간 어긋남)."
git push -u origin fix/chronos-schedule-tz-bump
gh pr create --repo kenshin579/moneyflow.advenoh.pe.kr --base main --fill
```

---

## Task 5: (moneyflow) Deploy — **GATE: another session**

**Files:** none

- [ ] Merge the moneyflow PR and redeploy moneyflow-be via the standard pipeline (handled in a separate session). Confirm rollout: `kubectl -n app rollout status deploy/moneyflow-be-deployment`.

---

## Task 6: Verify KST scheduling in production (after deploy)

**Files:** none

- [ ] **Step 1: watch the next scheduled fire.** After deploy, the scheduler evaluates crons in KST. Confirm via the stored last-fire instant (`chronos:sched:<id>:last`, an epoch of the true fire instant). Read it against the same Redis the worker uses (port-forward `redis-master-0`, password from the pod's `REDIS_URL`):

```bash
# for a schedule whose slot has passed post-deploy, e.g. warm-kr-meta "0 5 * * *"
# EXPECT the epoch to decode to the KST instant (05:00 KST = 20:00 UTC), NOT 05:00 UTC.
```
Expected: e.g. `warm-kr-meta` `:last` decodes to `...T20:00:00Z` (05:00 KST), confirming KST evaluation. (Before the fix it was `...T05:00:00Z`.)

- [ ] **Step 2: sanity-check timing.** Confirm the daily warming (investor/program `30 16 * * *`) now fires at 16:30 KST (07:30 UTC) rather than 16:30 UTC.

---

## Self-Review

- **Spec coverage:** fix in RegisterCron (Task 2), CRON_TZ preserved + default regression + Location applied tests (Task 1), release v1.0.1 (Task 3), moneyflow bump (Task 4), deploy (Task 5), KST verification (Task 6) — all spec sections mapped.
- **Non-breaking:** field types/signatures unchanged; moneyflow needs only a go.mod bump (v1.0.0 build verified). ✔
- **Type/name consistency:** `newOfflineScheduler`, `SpecSchedule`, `s.entries[0].schedule`, `cfg.Location`, `tickArgs` (existing helper in scheduler_test.go) used consistently. ✔
- **Placeholder scan:** none. ✔
