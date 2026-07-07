# chronos-go 메트릭 + Prometheus/Grafana 관찰 스택 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** chronos-go에 운영 메트릭을 붙이고 Prometheus + Grafana로 **실제 그래프**로 볼 수 있게 한다. 코어는 의존성 없는 `Metrics` 훅 인터페이스만 갖고, Prometheus 구현체·수집기·데모·배포 스택은 자체 go.mod를 가진 `contrib/prometheus`에 둔다(코어를 prometheus로 오염시키지 않음).

**Architecture:** 코어 `Server.process`가 태스크 처리 결과(success/retry/dead_letter)와 소요시간을 `Metrics.ObserveTask`로 방출(nil이면 no-op). `contrib/prometheus`가 이를 Counter/Histogram으로 구현하고, 큐 적재량(pending/active/scheduled/retry/archived)은 `chronos.Inspector`를 스크레이프 시점에 읽는 `prometheus.Collector`로 노출. 데모 앱이 `/metrics`를 띄우고 부하를 생성하며, docker-compose가 redis+데모+prometheus+grafana를 묶고 Grafana에 대시보드를 프로비저닝한다.

**Tech Stack:** Go 1.26. 코어: 의존성 추가 0. `contrib/prometheus`: `github.com/prometheus/client_golang`. 배포: docker-compose, Prometheus, Grafana.

**중요 제약(의존성 격리):** prometheus를 import하는 모든 것(어댑터·수집기·데모 앱)은 **`contrib/prometheus` 모듈 안**에 둔다. 메인 모듈(`github.com/kenshin579/chronos-go`)의 go.mod는 prometheus를 절대 포함하지 않는다. `contrib/prometheus/go.mod`는 `replace github.com/kenshin579/chronos-go => ../../`로 로컬 코어를 참조한다.

**환경 주의:** 이 환경엔 Docker 데몬이 꺼져 있을 수 있다. Go 코드(코어 훅, contrib 어댑터/수집기)와 데모의 `/metrics` 출력은 실제 Redis로 검증 가능하지만, Grafana 렌더링은 `docker compose up`이 필요하다(Docker 필요). Docker 불가 시 스택 파일은 완성하되 "compose로 실행 시 동작"으로 남기고 그 사실을 보고한다.

**M1~M4에서 확정된 실제 시그니처:**
- `server.go`: `ServerConfig{Queues, Concurrency, Logger, RetryDelayFunc, OnDeadLetter, ForwardInterval, RecoverInterval, RecoverMinIdle}`, `process(ctx, qname, streamID, msg)` (dispatchSafely → nil이면 Done, SkipRetry/exhausted면 deadLetter, else Retry).
- `chronos.Inspector`: `NewInspector(r)`, `Queues(ctx) ([]*QueueInfo, error)`, `QueueInfo{Queue, Pending, Active, Scheduled, Retry, Archived int64}`.
- 공개: `NewClient`/`Enqueue`/`NewMux`/`AddHandler`/`NewServer`/`NewScheduler`/`RegisterInterval`/`WithMaxRetry`/`WithQueue`.

---

## File Structure

| 파일 | 내용 |
|---|---|
| `metrics.go` (신규, 코어) | `Metrics` 인터페이스 + `TaskOutcome` 상수 (의존성 0) |
| `server.go` (수정) | `ServerConfig.Metrics` + `process`에 observe 훅 |
| `contrib/prometheus/go.mod` (신규) | 자체 모듈 + replace |
| `contrib/prometheus/metrics.go` (신규) | `chronos.Metrics`의 Prometheus 구현(Counter/Histogram) |
| `contrib/prometheus/collector.go` (신규) | Inspector 기반 큐 적재량 `prometheus.Collector` |
| `contrib/prometheus/*_test.go` (신규) | 어댑터/수집기 테스트 |
| `contrib/prometheus/cmd/loadgen/main.go` (신규) | `/metrics` 노출 + 부하 생성 데모 |
| `contrib/prometheus/deploy/` (신규) | docker-compose, prometheus.yml, grafana 프로비저닝, 대시보드 JSON, Dockerfile, README |
| `Makefile` (수정) | contrib 모듈도 test |

---

## Task 1: 코어 Metrics 훅 (의존성 0)

**Files:**
- Create: `metrics.go`
- Modify: `server.go`
- Test: `metrics_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `metrics_test.go`:
```go
package chronos

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type capturedObs struct {
	queue, kind string
	outcome     TaskOutcome
}

type fakeMetrics struct {
	mu   sync.Mutex
	obs  []capturedObs
}

func (m *fakeMetrics) ObserveTask(queue, kind string, outcome TaskOutcome, dur time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.obs = append(m.obs, capturedObs{queue, kind, outcome})
}

func (m *fakeMetrics) outcomes() []TaskOutcome {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]TaskOutcome, len(m.obs))
	for i, o := range m.obs {
		out[i] = o.outcome
	}
	return out
}

func TestMetrics_SuccessOutcomeObserved(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	m := &fakeMetrics{}
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(done)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 2, Metrics: m})
	if err := srv.Start(context.Background(), mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(context.Background(), c, emailArgs{UserID: "u1"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-done
	eventually(t, 3*time.Second, func() bool {
		for _, o := range m.outcomes() {
			if o == OutcomeSuccess {
				return true
			}
		}
		return false
	}, "success outcome should be observed")
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run TestMetrics_SuccessOutcomeObserved -v`
Expected: FAIL — `unknown field Metrics`, `undefined: TaskOutcome/OutcomeSuccess`.

- [ ] **Step 3: metrics.go 작성**

Create `metrics.go`:
```go
package chronos

import "time"

// TaskOutcome is the terminal result of processing one task, reported to Metrics.
type TaskOutcome string

const (
	// OutcomeSuccess: the handler returned nil; the task was acked and removed.
	OutcomeSuccess TaskOutcome = "success"
	// OutcomeRetry: the handler failed and the task was scheduled for retry.
	OutcomeRetry TaskOutcome = "retry"
	// OutcomeDeadLetter: the task exhausted retries (or returned SkipRetry) and
	// was archived or discarded.
	OutcomeDeadLetter TaskOutcome = "dead_letter"
)

// Metrics receives one observation per processed task. Implementations MUST be
// safe for concurrent use (the server calls it from worker goroutines). The
// zero/nil Metrics disables observation. The Prometheus implementation lives in
// the contrib/prometheus module so the core stays dependency-free.
type Metrics interface {
	ObserveTask(queue, kind string, outcome TaskOutcome, dur time.Duration)
}
```

- [ ] **Step 4: ServerConfig에 Metrics + process 훅 추가**

`server.go`의 `ServerConfig`에 필드 추가(`OnDeadLetter` 아래):
```go
	// Metrics, if set, receives one observation per processed task. Use the
	// contrib/prometheus implementation, or your own. Defaults to nil (disabled).
	Metrics Metrics
```

`server.go`의 `process`를 다음으로 교체(소요시간 측정 + 분기별 observe):
```go
func (s *Server) process(ctx context.Context, qname, streamID string, msg *base.TaskMessage) {
	start := time.Now()
	err := s.dispatchSafely(ctx, msg)
	dur := time.Since(start)

	// Ack/move operations must outlive shutdown cancellation so a finished task
	// is never left dangling in the PEL — but they still need a deadline, or a
	// stalled Redis would block this worker forever and hang Shutdown's wg.Wait.
	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()

	if err == nil {
		if derr := s.rdb.Done(opCtx, qname, streamID, msg); derr != nil {
			s.logger.Error("chronos: ack failed", "id", msg.ID, "error", derr)
		}
		s.observe(msg, OutcomeSuccess, dur)
		return
	}

	s.logger.Error("chronos: task failed",
		"kind", msg.Kind, "id", msg.ID, "retried", msg.Retried, "error", err)

	// Dead-letter when the error is non-retryable or the budget is exhausted.
	if asSkipRetry(err) || msg.Retried >= msg.MaxRetry {
		s.deadLetter(opCtx, qname, streamID, msg, err)
		s.observe(msg, OutcomeDeadLetter, dur)
		return
	}

	msg.Retried++
	retryAt := time.Now().Add(s.cfg.RetryDelayFunc(msg.Retried, err))
	if rerr := s.rdb.Retry(opCtx, qname, streamID, msg, retryAt); rerr != nil {
		s.logger.Error("chronos: retry scheduling failed", "id", msg.ID, "error", rerr)
	}
	s.observe(msg, OutcomeRetry, dur)
}

// observe reports a task outcome to the configured Metrics (no-op if unset).
func (s *Server) observe(msg *base.TaskMessage, outcome TaskOutcome, dur time.Duration) {
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.ObserveTask(msg.Queue, msg.Kind, outcome, dur)
	}
}
```

- [ ] **Step 5: 통과 확인 + 회귀 + 커밋**

Run: `go test . -run TestMetrics_SuccessOutcomeObserved -race -v && go test ./... -race -p 1`
Expected: PASS (전 패키지).

```bash
git add metrics.go server.go metrics_test.go
git commit -m "feat: 코어 Metrics 훅 인터페이스 (의존성 0, process에서 outcome 방출)"
```

---

## Task 2: contrib/prometheus 모듈 (어댑터 + 수집기)

**Files:**
- Create: `contrib/prometheus/go.mod`, `contrib/prometheus/metrics.go`, `contrib/prometheus/collector.go`
- Test: `contrib/prometheus/metrics_test.go`

- [ ] **Step 1: 모듈 초기화**

Run:
```bash
cd /Users/user/GolandProjects/chronos-go/contrib/prometheus
go mod init github.com/kenshin579/chronos-go/contrib/prometheus
go mod edit -replace github.com/kenshin579/chronos-go=../../
go get github.com/kenshin579/chronos-go@v0.0.0
go get github.com/prometheus/client_golang@latest
```
(위 `go get chronos-go`는 replace가 로컬로 잡아주므로 버전은 형식상. 실패 시 `go mod tidy`가 replace를 사용해 해결한다.)

- [ ] **Step 2: 실패하는 테스트 작성**

Create `contrib/prometheus/metrics_test.go`:
```go
package prometheus

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/kenshin579/chronos-go"
)

func TestMetrics_ObserveTask_IncrementsCounter(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.ObserveTask("default", "email:send", chronos.OutcomeSuccess, 5*time.Millisecond)
	m.ObserveTask("default", "email:send", chronos.OutcomeSuccess, 7*time.Millisecond)
	m.ObserveTask("default", "email:send", chronos.OutcomeRetry, 1*time.Millisecond)

	const want = `
# HELP chronos_tasks_processed_total Total tasks processed, by queue, kind and outcome.
# TYPE chronos_tasks_processed_total counter
chronos_tasks_processed_total{kind="email:send",outcome="retry",queue="default"} 1
chronos_tasks_processed_total{kind="email:send",outcome="success",queue="default"} 2
`
	if err := testutil.CollectAndCompare(m.processed, strings.NewReader(want)); err != nil {
		t.Fatalf("counter mismatch: %v", err)
	}
}
```

- [ ] **Step 3: metrics.go 구현**

Create `contrib/prometheus/metrics.go`:
```go
// Package prometheus provides a Prometheus implementation of chronos-go's
// Metrics hook plus a Collector for live queue-depth gauges. It lives in a
// separate module so the chronos-go core stays free of the prometheus dependency.
package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/kenshin579/chronos-go"
)

// Metrics implements chronos.Metrics using Prometheus counters and a histogram.
type Metrics struct {
	processed *prometheus.CounterVec
	duration  *prometheus.HistogramVec
}

// NewMetrics creates the task metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronos_tasks_processed_total",
			Help: "Total tasks processed, by queue, kind and outcome.",
		}, []string{"queue", "kind", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chronos_task_duration_seconds",
			Help:    "Task handler duration in seconds, by queue and kind.",
			Buckets: prometheus.DefBuckets,
		}, []string{"queue", "kind"}),
	}
	reg.MustRegister(m.processed, m.duration)
	return m
}

// ObserveTask implements chronos.Metrics.
func (m *Metrics) ObserveTask(queue, kind string, outcome chronos.TaskOutcome, dur time.Duration) {
	m.processed.WithLabelValues(queue, kind, string(outcome)).Inc()
	m.duration.WithLabelValues(queue, kind).Observe(dur.Seconds())
}
```

- [ ] **Step 4: collector.go 구현 (큐 적재량 게이지)**

Create `contrib/prometheus/collector.go`:
```go
package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/kenshin579/chronos-go"
)

// QueueCollector reports per-queue task counts (pending/active/scheduled/retry/
// archived) as gauges, read from a chronos.Inspector at scrape time.
type QueueCollector struct {
	insp    *chronos.Inspector
	timeout time.Duration
	desc    *prometheus.Desc
}

// NewQueueCollector returns a collector over the given inspector. Register it
// with a prometheus registry.
func NewQueueCollector(insp *chronos.Inspector) *QueueCollector {
	return &QueueCollector{
		insp:    insp,
		timeout: 5 * time.Second,
		desc: prometheus.NewDesc(
			"chronos_queue_tasks",
			"Number of tasks in a queue by state.",
			[]string{"queue", "state"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *QueueCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector: it reads live queue stats per scrape.
func (c *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	queues, err := c.insp.Queues(ctx)
	if err != nil {
		return // skip this scrape; a transient Redis error should not crash /metrics
	}
	for _, q := range queues {
		g := func(state string, v int64) {
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(v), q.Queue, state)
		}
		g("pending", q.Pending)
		g("active", q.Active)
		g("scheduled", q.Scheduled)
		g("retry", q.Retry)
		g("archived", q.Archived)
	}
}
```

- [ ] **Step 5: 통과 확인 + 커밋**

Run: `cd contrib/prometheus && go test ./... -race && go vet ./... && cd ../..`
Expected: PASS, vet 클린. (실제 Redis 불필요한 counter 테스트; collector는 다음 태스크 데모에서 실동작 확인.)

```bash
git add contrib/prometheus/go.mod contrib/prometheus/go.sum contrib/prometheus/metrics.go contrib/prometheus/collector.go contrib/prometheus/metrics_test.go
git commit -m "feat: contrib/prometheus — Metrics 구현 + 큐 적재량 Collector (자체 모듈)"
```

---

## Task 3: 데모 앱 (/metrics 노출 + 부하 생성)

**Files:**
- Create: `contrib/prometheus/cmd/loadgen/main.go`

- [ ] **Step 1: 데모 앱 작성**

Create `contrib/prometheus/cmd/loadgen/main.go`:
```go
// Command loadgen runs a small chronos-go workload and exposes Prometheus
// metrics on :2112/metrics, so a Prometheus+Grafana stack has live data to graph.
//
//	REDIS_ADDR=127.0.0.1:6379 go run ./cmd/loadgen
package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
	chronosprom "github.com/kenshin579/chronos-go/contrib/prometheus"
)

type workArgs struct {
	N int `json:"n"`
}

func (workArgs) Kind() string { return "demo:work" }

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()

	reg := prometheus.NewRegistry()
	metrics := chronosprom.NewMetrics(reg)
	reg.MustRegister(chronosprom.NewQueueCollector(chronos.NewInspector(rdb)))

	mux := chronos.NewMux()
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[workArgs]) error {
		time.Sleep(time.Duration(rand.Intn(40)+5) * time.Millisecond) // simulate work
		if rand.Intn(5) == 0 {                                        // ~20% fail → retries/dead-letters
			return errors.New("simulated failure")
		}
		return nil
	})

	srv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"default": 1, "critical": 1},
		Concurrency: 8,
		Metrics:     metrics,
	})
	if err := srv.Start(ctx, mux); err != nil {
		log.Fatalf("server start: %v", err)
	}

	// A scheduled job so the "scheduled/leader" path is exercised too.
	sched := chronos.NewScheduler(rdb, chronos.SchedulerConfig{})
	_ = chronos.RegisterInterval(sched, 2*time.Second, workArgs{N: -1}, chronos.WithMaxRetry(2))
	if err := sched.Start(ctx); err != nil {
		log.Fatalf("scheduler start: %v", err)
	}

	// Continuous enqueue load.
	client := chronos.NewClient(rdb)
	go func() {
		for i := 0; ; i++ {
			q := "default"
			if i%3 == 0 {
				q = "critical"
			}
			_, _ = chronos.Enqueue(ctx, client, workArgs{N: i}, chronos.WithQueue(q), chronos.WithMaxRetry(2))
			time.Sleep(150 * time.Millisecond)
		}
	}()

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Println("chronos loadgen: metrics on :2112/metrics")
	log.Fatal(http.ListenAndServe(":2112", nil))
}
```

- [ ] **Step 2: 빌드 확인**

Run: `cd contrib/prometheus && go build ./... && cd ../..`
Expected: 빌드 성공.

- [ ] **Step 3: /metrics 실동작 검증 (실 Redis, Docker 불필요)**

로컬 Redis가 떠 있어야 한다(`redis-cli -p 6379 ping` → PONG). 데모를 백그라운드로 잠깐 띄워 `/metrics`가 chronos 지표를 내보내는지 확인:
```bash
cd contrib/prometheus
( REDIS_ADDR=127.0.0.1:6379 go run ./cmd/loadgen & echo $! > /tmp/loadgen.pid ) 
sleep 6
curl -s localhost:2112/metrics | grep -E '^chronos_(tasks_processed_total|task_duration_seconds_count|queue_tasks)' | head -20
kill "$(cat /tmp/loadgen.pid)" 2>/dev/null; pkill -f 'go run ./cmd/loadgen' 2>/dev/null; pkill -f loadgen 2>/dev/null
cd ../..
```
Expected: `chronos_tasks_processed_total{...outcome="success"...}`, `..._duration_seconds_count`, `chronos_queue_tasks{...state="pending"...}` 등의 라인이 0이 아닌 값으로 출력. (부하가 돌아 지표가 증가함을 확인.)

- [ ] **Step 4: 커밋**

```bash
git add contrib/prometheus/cmd/loadgen/main.go contrib/prometheus/go.mod contrib/prometheus/go.sum
git commit -m "feat: contrib/prometheus loadgen 데모 (/metrics 노출 + 부하 생성)"
```

---

## Task 4: 배포 스택 (docker-compose + Prometheus + Grafana 대시보드)

**Files:**
- Create: `contrib/prometheus/deploy/docker-compose.yml`, `Dockerfile`, `prometheus.yml`, `grafana/provisioning/datasources/datasource.yml`, `grafana/provisioning/dashboards/dashboards.yml`, `grafana/dashboards/chronos.json`, `README.md`
- Modify: `Makefile`

- [ ] **Step 1: Dockerfile (데모 앱 빌드)**

Create `contrib/prometheus/deploy/Dockerfile`:
```dockerfile
# Build the loadgen demo from the repo root context (nested module uses a
# replace directive pointing at the parent, so the whole repo must be present).
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
WORKDIR /src/contrib/prometheus
RUN CGO_ENABLED=0 go build -o /out/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/loadgen /loadgen
EXPOSE 2112
ENTRYPOINT ["/loadgen"]
```

- [ ] **Step 2: docker-compose.yml**

Create `contrib/prometheus/deploy/docker-compose.yml`:
```yaml
services:
  redis:
    image: redis:7
    ports: ["6379:6379"]

  loadgen:
    build:
      context: ../../..          # repo root (nested module replace needs parent)
      dockerfile: contrib/prometheus/deploy/Dockerfile
    environment:
      REDIS_ADDR: redis:6379
    depends_on: [redis]
    ports: ["2112:2112"]

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    depends_on: [loadgen]
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:latest
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    depends_on: [prometheus]
    ports: ["3000:3000"]
```

- [ ] **Step 3: prometheus.yml**

Create `contrib/prometheus/deploy/prometheus.yml`:
```yaml
global:
  scrape_interval: 2s
scrape_configs:
  - job_name: chronos
    static_configs:
      - targets: ["loadgen:2112"]
```

- [ ] **Step 4: Grafana 데이터소스 프로비저닝**

Create `contrib/prometheus/deploy/grafana/provisioning/datasources/datasource.yml`:
```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 5: Grafana 대시보드 프로비저닝**

Create `contrib/prometheus/deploy/grafana/provisioning/dashboards/dashboards.yml`:
```yaml
apiVersion: 1
providers:
  - name: chronos
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 6: 대시보드 JSON**

Create `contrib/prometheus/deploy/grafana/dashboards/chronos.json`:
```json
{
  "title": "chronos-go",
  "uid": "chronos-go",
  "timezone": "browser",
  "schemaVersion": 39,
  "time": { "from": "now-15m", "to": "now" },
  "refresh": "5s",
  "panels": [
    {
      "type": "timeseries",
      "title": "Task throughput by outcome (per s)",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [
        { "expr": "sum by (outcome) (rate(chronos_tasks_processed_total[1m]))", "legendFormat": "{{outcome}}" }
      ]
    },
    {
      "type": "timeseries",
      "title": "Task duration (p50 / p95, seconds)",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "targets": [
        { "expr": "histogram_quantile(0.5, sum by (le) (rate(chronos_task_duration_seconds_bucket[1m])))", "legendFormat": "p50" },
        { "expr": "histogram_quantile(0.95, sum by (le) (rate(chronos_task_duration_seconds_bucket[1m])))", "legendFormat": "p95" }
      ]
    },
    {
      "type": "timeseries",
      "title": "Queue depth by state",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "targets": [
        { "expr": "sum by (state) (chronos_queue_tasks)", "legendFormat": "{{state}}" }
      ]
    },
    {
      "type": "stat",
      "title": "Dead-letters (5m)",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "targets": [
        { "expr": "sum(increase(chronos_tasks_processed_total{outcome=\"dead_letter\"}[5m]))" }
      ]
    }
  ]
}
```

- [ ] **Step 7: README + Makefile**

Create `contrib/prometheus/deploy/README.md`:
```markdown
# chronos-go 관찰 스택 (Prometheus + Grafana)

로컬에서 chronos-go 메트릭을 실제 그래프로 본다.

## 실행

```bash
cd contrib/prometheus/deploy
docker compose up --build
```

- Grafana: http://localhost:3000 (익명 Admin 로그인) → 대시보드 "chronos-go"
- Prometheus: http://localhost:9090
- 데모 metrics: http://localhost:2112/metrics

`loadgen` 컨테이너가 태스크를 계속 enqueue/처리(약 20% 실패 → 재시도·dead-letter)하고 2초 주기 스케줄 잡도 돌아, 처리량·지연·큐 적재량·dead-letter 패널이 실시간으로 움직인다.

## 메트릭

- `chronos_tasks_processed_total{queue,kind,outcome}` — 처리 카운터(outcome: success/retry/dead_letter)
- `chronos_task_duration_seconds` — 핸들러 처리시간 히스토그램
- `chronos_queue_tasks{queue,state}` — 큐 적재량 게이지(pending/active/scheduled/retry/archived)

## 코드에서 쓰는 법

```go
reg := prometheus.NewRegistry()
metrics := chronosprom.NewMetrics(reg)
reg.MustRegister(chronosprom.NewQueueCollector(chronos.NewInspector(rdb)))
srv := chronos.NewServer(rdb, chronos.ServerConfig{Metrics: metrics, ...})
http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```
```

`Makefile`에 contrib 테스트를 추가한다. 기존 `test-race` 타깃 아래에 새 타깃 추가하고 `check`가 이를 포함하도록 수정:
```makefile
test-contrib:
	cd contrib/prometheus && go test ./... -race

check: fmt-check vet test-race test-contrib
```
(기존 `check` 정의를 위와 같이 교체. `test-contrib`를 `.PHONY`에 추가.)

- [ ] **Step 8: 검증**

Run:
```bash
go build ./... && (cd contrib/prometheus && go build ./... && go vet ./...)
make check
```
Expected: 메인/‑contrib 빌드·vet·테스트 전부 통과. (docker compose는 Docker 데몬이 있으면 `cd contrib/prometheus/deploy && docker compose up --build`로 실행 — 없으면 스킵하고 파일만 완성.)

- [ ] **Step 9: 커밋**

```bash
git add contrib/prometheus/deploy/ Makefile
git commit -m "feat: Prometheus+Grafana 배포 스택 (compose + 대시보드 프로비저닝)"
```

---

## 완료 기준

- [ ] 메인 모듈 go.mod에 prometheus 의존성 없음(코어 격리 유지)
- [ ] `make check`(메인 + contrib) 통과
- [ ] 데모 `/metrics`가 `chronos_tasks_processed_total` / `chronos_task_duration_seconds` / `chronos_queue_tasks`를 실제 값으로 노출(로컬 Redis로 검증)
- [ ] docker-compose + prometheus.yml + Grafana 데이터소스/대시보드 프로비저닝 완비 → `docker compose up`으로 Grafana에서 4개 패널(처리량/지연/큐적재량/dead-letter) 관찰
- [ ] README에 실행법·메트릭·코드 사용법

**Docker 미가용 시:** Go 코드와 `/metrics` 출력까지 검증하고, Grafana 렌더링은 "compose로 실행 시 동작"으로 남긴다(스택 파일은 완성). 사용자가 Docker를 켜면 `docker compose up --build`로 즉시 확인 가능.

**다음 단계:** 관찰 스택 완성 후 남은 후보 — operator-review 실제 교체, heartbeat 마일스톤(lease/unique TTL 연장).
