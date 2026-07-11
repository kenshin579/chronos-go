# Redis Cluster 지원 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** chronos-go에 Redis Cluster 접속 지원(CLI 플래그) + 스크립트-완전 통합 검증(로컬 opt-in) + 문서를 추가한다.

**Architecture:** 코어 라이브러리는 무변경(이미 cluster-safe: 해시태그 키 + 같은 슬롯 Lua). CLI에 `--standalone`/`--cluster` 모드 플래그를 추가하고, docker compose 6노드 클러스터(단일 컨테이너, announce 127.0.0.1) + `testutil.NewClusterRedis`(env 없으면 skip) + `cluster_test.go` 12개 스크립트-완전 스모크로 검증한다.

**Tech Stack:** Go stdlib(flag), redis/go-redis v9 (`NewClusterClient`, `ForEachMaster`, `ClusterKeySlot`), docker compose(공식 redis:7-alpine), Makefile.

---

## File Structure

- Modify `cmd/chronos/main.go` — `buildClient(standalone, cluster bool, addr string, db int)` 분리 + 플래그.
- Modify `cmd/chronos/main_test.go` — buildClient 단위 테스트(연결 안 함).
- Create `deploy/redis-cluster/docker-compose.yml` — 단일 컨테이너 6노드 클러스터.
- Create `deploy/redis-cluster/README.md` — 사용법.
- Modify `internal/testutil/redis.go` — `NewClusterRedis(t)` 추가.
- Create `cluster_test.go` (루트 패키지) — `TestCluster_*` 12개 시나리오 + 체크리스트 주석.
- Modify `Makefile` — `test-cluster` 타깃.
- Modify `README.md` — "Redis Cluster" 섹션.

**참고 (구현자 필독):**
- 테스트 헬퍼 패턴: `internal/testutil/redis.go`의 `NewRedis`(skip-if-unreachable, flush, cleanup).
- 코어 공개 API: `chronos.NewClient/NewServer/NewScheduler/NewInspector`는 전부 `redis.UniversalClient`를 받는다. `cluster_test.go`는 같은 모듈이므로 `internal/rdb`도 직접 쓸 수 있다(시나리오 7 recover에 필요).
- 기존 유사 테스트: `server_test.go`(기본 처리), `server_reliability_test.go`(retry/recover), `server_heartbeat_test.go`, `server_janitor_test.go`, `scheduler_integration_test.go`(리더/페일오버), `priority_test.go`(러너 패턴). 시나리오 코드는 이들을 참고하되 클러스터 클라이언트로 실행한다.
- 테스트용 태스크 타입: 루트 패키지 테스트엔 `emailArgs`(chronos_test.go, Kind `"email:send"`)가 이미 있다. cluster_test.go는 자체 타입 `clArgs`를 정의해 다른 테스트와 kind 충돌을 피한다.

---

## Task 1: CLI `--standalone`/`--cluster` 플래그 + `buildClient`

**Files:**
- Modify: `cmd/chronos/main.go`
- Test: `cmd/chronos/main_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`cmd/chronos/main_test.go`에 추가 (클라이언트 생성은 dial하지 않으므로 Redis 불필요):

```go
func TestBuildClient_ModesAndErrors(t *testing.T) {
	// 기본(둘 다 false) = standalone
	c, err := buildClient(false, false, "127.0.0.1:6379", 3)
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	if _, ok := c.(*redis.Client); !ok {
		t.Errorf("default: got %T, want *redis.Client", c)
	}
	_ = c.Close()

	// 명시적 standalone
	c, err = buildClient(true, false, "127.0.0.1:6379", 0)
	if err != nil {
		t.Fatalf("standalone: %v", err)
	}
	if _, ok := c.(*redis.Client); !ok {
		t.Errorf("standalone: got %T, want *redis.Client", c)
	}
	_ = c.Close()

	// cluster: 콤마 분리 다중 주소
	c, err = buildClient(false, true, "n1:7000,n2:7001", 0)
	if err != nil {
		t.Fatalf("cluster: %v", err)
	}
	cc, ok := c.(*redis.ClusterClient)
	if !ok {
		t.Fatalf("cluster: got %T, want *redis.ClusterClient", c)
	}
	_ = cc.Close()

	// 상호 배타
	if _, err := buildClient(true, true, "x:1", 0); err == nil {
		t.Error("standalone+cluster: want error, got nil")
	}
	// cluster + db != 0
	if _, err := buildClient(false, true, "x:1", 15); err == nil {
		t.Error("cluster with db!=0: want error, got nil")
	}
}
```

`main_test.go` import에 `"github.com/redis/go-redis/v9"` 추가 (없으면).

- [ ] **Step 2: 실패 확인**

Run: `go test ./cmd/chronos/ -run TestBuildClient_ModesAndErrors -p 1`
Expected: FAIL — `undefined: buildClient`

- [ ] **Step 3: 구현**

`cmd/chronos/main.go`에서 `main`과 상단 doc 주석을 아래로 교체하고 `buildClient`를 추가. import에 `"errors"`, `"strings"` 추가:

```go
// Command chronos is a CLI for inspecting and administering chronos-go queues.
//
//	chronos [--redis addr] [--db n] queue ls                       # standalone (default)
//	chronos --cluster --redis n1:7000,n2:7001 queue ls             # Redis Cluster
//	chronos [flags] task ls   <queue> <scheduled|retry|archived>
//	chronos [flags] task run  <queue> <task-id>
//	chronos [flags] task rm   <queue> <task-id>
package main
```

```go
func main() {
	standalone := flag.Bool("standalone", false, "connect to a standalone Redis (default)")
	cluster := flag.Bool("cluster", false, "connect to a Redis Cluster (--redis takes comma-separated seed nodes)")
	addr := flag.String("redis", envOr("REDIS_ADDR", "127.0.0.1:6379"), "Redis address (comma-separated for --cluster)")
	db := flag.Int("db", 0, "Redis database number (standalone only; cluster has only DB 0)")
	flag.Parse()

	client, err := buildClient(*standalone, *cluster, *addr, *db)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chronos:", err)
		os.Exit(2)
	}

	// Not deferred: os.Exit skips deferred calls, so close explicitly first.
	code := run(flag.Args(), client, os.Stdout)
	_ = client.Close()
	os.Exit(code)
}

// buildClient creates the Redis client for the chosen mode. The default (no
// mode flag) is standalone, matching the CLI's historical behavior.
func buildClient(standalone, cluster bool, addr string, db int) (redis.UniversalClient, error) {
	if standalone && cluster {
		return nil, errors.New("--standalone and --cluster are mutually exclusive")
	}
	if cluster {
		if db != 0 {
			return nil, errors.New("--db is not supported with --cluster: Redis Cluster has only database 0")
		}
		return redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addr, ",")}), nil
	}
	return redis.NewClient(&redis.Options{Addr: addr, DB: db}), nil
}
```

- [ ] **Step 4: 통과 확인**

Run: `go test ./cmd/chronos/ -p 1` (패키지 전체 — 기존 CLI 테스트 회귀 포함)
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add cmd/chronos/main.go cmd/chronos/main_test.go
git commit -m "feat: chronos CLI --standalone/--cluster 모드 플래그"
```

---

## Task 2: `deploy/redis-cluster` docker compose

**Files:**
- Create: `deploy/redis-cluster/docker-compose.yml`
- Create: `deploy/redis-cluster/README.md`

**설계 메모 (중요):** 노드들이 서로 다른 컨테이너면 announce IP 문제(호스트→MOVED 리다이렉트 불능 또는 노드 간 gossip 불능)가 생긴다. **단일 컨테이너에 6개 redis-server 프로세스**를 띄우고 `--cluster-announce-ip 127.0.0.1`을 주면, 컨테이너 안(노드 간)에서도 밖(호스트 클라이언트, 포트 매핑)에서도 127.0.0.1:700x가 일관되게 동작한다. 볼륨 없음 → 재시작마다 깨끗한 클러스터.

- [ ] **Step 1: docker-compose.yml 작성**

```yaml
# A disposable 6-node Redis Cluster (3 masters + 3 replicas) for local
# integration testing. All nodes run in ONE container and announce 127.0.0.1,
# so both node-to-node gossip (inside) and host clients following MOVED
# redirects (outside, via the 1:1 port mapping) see consistent addresses.
# No volume: every `up` starts a fresh cluster.
services:
  redis-cluster:
    image: redis:7-alpine
    container_name: chronos-redis-cluster
    ports:
      - "7000-7005:7000-7005"
    command:
      - sh
      - -c
      - |
        for port in 7000 7001 7002 7003 7004 7005; do
          redis-server --port $$port --cluster-enabled yes \
            --cluster-config-file /tmp/nodes-$$port.conf \
            --cluster-node-timeout 5000 \
            --cluster-announce-ip 127.0.0.1 \
            --appendonly no --save '' --daemonize yes
        done
        sleep 2
        redis-cli --cluster create \
          127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
          127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 \
          --cluster-replicas 1 --cluster-yes
        echo "cluster ready"
        tail -f /dev/null
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "7000", "cluster", "info"]
      interval: 3s
      timeout: 2s
      retries: 10
```

- [ ] **Step 2: README.md 작성**

`deploy/redis-cluster/README.md`:

```markdown
# Local Redis Cluster (integration testing)

A disposable 6-node Redis Cluster (3 masters + 3 replicas, ports 7000-7005)
for chronos-go's cluster integration tests. Data is NOT persisted — every
`up` starts a fresh cluster.

## Up / down

```bash
docker compose up -d      # wait a few seconds for "cluster ready"
docker compose down
```

## Run the tests against it

```bash
# from the repo root
make test-cluster
```

## Poke at it manually

```bash
redis-cli -c -p 7000 cluster info      # cluster_state:ok
go run ./cmd/chronos --cluster --redis 127.0.0.1:7000 queue ls
```
```

- [ ] **Step 3: 클러스터 기동 확인 (docker 필요)**

```bash
cd deploy/redis-cluster && docker compose up -d
sleep 8
redis-cli -p 7000 cluster info | head -2
```
Expected: `cluster_state:ok`. (redis-cli가 호스트에 없으면 `docker exec chronos-redis-cluster redis-cli -p 7000 cluster info`.)
확인 후 클러스터는 **띄워둔 채로 둔다** (다음 태스크들이 사용).

- [ ] **Step 4: 커밋**

```bash
git add deploy/redis-cluster
git commit -m "feat: deploy/redis-cluster 로컬 6노드 클러스터 (통합 테스트용)"
```

---

## Task 3: `testutil.NewClusterRedis`

**Files:**
- Modify: `internal/testutil/redis.go`
- Test: `internal/testutil/redis_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`internal/testutil/redis_test.go`에 추가 (기존 파일 패턴에 맞춰):

```go
func TestNewClusterRedis_SkipsWithoutEnv(t *testing.T) {
	t.Setenv("REDIS_CLUSTER_ADDRS", "")
	inner := &testing.T{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// NewClusterRedis must call t.Skip (runtime.Goexit) when env is unset.
		NewClusterRedis(inner)
		t.Error("NewClusterRedis returned instead of skipping")
	}()
	<-done
	if !inner.Skipped() {
		t.Error("expected inner test to be skipped")
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/testutil/ -run TestNewClusterRedis -p 1`
Expected: FAIL — `undefined: NewClusterRedis`

- [ ] **Step 3: 구현**

`internal/testutil/redis.go`에 추가 (import에 `"strings"` 추가):

```go
// NewClusterRedis connects to a disposable test Redis Cluster listed in
// REDIS_CLUSTER_ADDRS (comma-separated seed addresses, e.g. the cluster from
// deploy/redis-cluster). It skips the test when the variable is unset or the
// cluster is unreachable, flushes every master before the test, and cleans up
// afterwards. Unlike NewRedis there is no DB selection: Redis Cluster has only
// logical database 0, so the cluster must be dedicated to tests.
func NewClusterRedis(t *testing.T) redis.UniversalClient {
	t.Helper()

	addrs := os.Getenv("REDIS_CLUSTER_ADDRS")
	if addrs == "" {
		t.Skip("REDIS_CLUSTER_ADDRS not set; skipping cluster integration test")
	}

	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addrs, ",")})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("redis cluster not reachable at %s: %v", addrs, err)
	}

	flush := func() error {
		return client.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
			return m.FlushAll(ctx).Err()
		})
	}
	if err := flush(); err != nil {
		t.Fatalf("flush cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = flush()
		_ = client.Close()
	})
	return client
}
```

- [ ] **Step 4: 통과 확인 (env 없이 skip + env 있으면 연결)**

Run: `go test ./internal/testutil/ -p 1`
Expected: PASS.
클러스터가 떠 있으므로 실제 연결도 확인:
Run: `REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 go test ./internal/testutil/ -run TestNewClusterRedis -p 1 -v`
Expected: PASS (skip 테스트는 env를 비우므로 여전히 통과).

- [ ] **Step 5: 커밋**

```bash
git add internal/testutil/redis.go internal/testutil/redis_test.go
git commit -m "feat: testutil.NewClusterRedis (REDIS_CLUSTER_ADDRS opt-in, 없으면 skip)"
```

---

## Task 4: cluster_test.go — 코어 플로우 (시나리오 1-5)

**Files:**
- Create: `cluster_test.go` (루트 패키지 `chronos`)

**실행 방법 (이 태스크부터 공통):** 클러스터가 떠 있어야 한다(Task 2). 테스트 실행은
`REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 go test -run 'TestCluster_' -p 1 -race .`

- [ ] **Step 1: 파일 생성 — 체크리스트 헤더 + 헬퍼 + 시나리오 1-5**

`cluster_test.go`:

```go
package chronos

// Cluster integration tests: run every Lua script and command pattern at least
// once against a real Redis Cluster (script-complete smoke). The cluster-
// specific failure modes these catch are CROSSSLOT (multi-key ops spanning
// slots), MOVED/ASK redirects, cross-node pub/sub, and per-node script caches.
//
// Requires REDIS_CLUSTER_ADDRS (see deploy/redis-cluster); skipped otherwise.
//
// Script/command checklist (each must be exercised by at least one test):
//  [x] enqueueCmd + Dequeue(XREADGROUP) + Done(XACK+XDEL)   → TestCluster_EnqueueProcessAck
//  [x] moveToZSetCmd(retry) + forwardCmd(retry)             → TestCluster_RetryThenSucceed
//  [x] moveToZSetCmd(archive) + OnDeadLetter                → TestCluster_DeadLetter
//  [x] scheduleCmd + forwardCmd(scheduled)                  → TestCluster_DelayedTask
//  [x] uniqueEnqueueCmd / uniqueScheduleCmd                 → TestCluster_UniqueDedup
//  [x] periodicCmd + leader acquire/renew/resign + pub/sub  → TestCluster_SchedulerLeaderFailover
//  [x] recover(XAUTOCLAIM)                                  → TestCluster_RecoverAbandonedTask
//  [x] heartbeat(XCLAIM JUSTID + PEXPIRE)                   → TestCluster_HeartbeatLongTask
//  [x] janitor(TrimArchived)                                → TestCluster_JanitorTrimsArchived
//  [x] runTaskCmd / deleteTask (Inspector)                  → TestCluster_InspectorRunAndDelete
//  [x] QueueStats/Queues/ListZSetTasks/GetTask              → TestCluster_InspectorQueries
//  [x] two queues on different slots (MOVED redirects)      → TestCluster_TwoQueuesDifferentSlots

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/rdb"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

// clArgs is the task type for cluster tests (own kind to avoid clashing with
// other tests' handlers).
type clArgs struct {
	N int `json:"n"`
}

func (clArgs) Kind() string { return "cluster:demo" }

// clusterServerConfig returns a ServerConfig tuned for fast tests.
func clusterServerConfig(queue string) ServerConfig {
	return ServerConfig{
		Queues:          map[string]int{queue: 1},
		Concurrency:     4,
		ForwardInterval: 200 * time.Millisecond,
		RetryDelayFunc:  func(int, error) time.Duration { return 300 * time.Millisecond },
	}
}

// waitFor polls cond every 50ms until it returns true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestCluster_EnqueueProcessAck(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var done atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		done.Add(1)
		return nil
	})
	srv := NewServer(client, clusterServerConfig("default"))
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, clArgs{N: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 5*time.Second, "task processed", func() bool { return done.Load() == 1 })

	// Done = XACK+XDEL: the stream must be empty again.
	insp := NewInspector(client)
	waitFor(t, 5*time.Second, "stream drained", func() bool {
		qs, err := insp.Queues(ctx)
		if err != nil || len(qs) == 0 {
			return false
		}
		for _, q := range qs {
			if q.Queue == "default" {
				return q.Pending == 0 && q.Active == 0
			}
		}
		return false
	})
}

func TestCluster_RetryThenSucceed(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var attempts atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	srv := NewServer(client, clusterServerConfig("default"))
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, clArgs{N: 2}, WithMaxRetry(3)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 10*time.Second, "retry then success", func() bool { return attempts.Load() >= 2 })
}

func TestCluster_DeadLetter(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var hooked atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		return errors.New("permanent")
	})
	cfg := clusterServerConfig("default")
	cfg.OnDeadLetter = func(ctx context.Context, info *TaskInfo, err error) { hooked.Add(1) }
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, clArgs{N: 3}, WithMaxRetry(0))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	waitFor(t, 10*time.Second, "task archived", func() bool {
		got, gerr := insp.GetTask(ctx, "default", info.ID)
		return gerr == nil && got.State == "archived" && got.LastErr == "permanent"
	})
	if hooked.Load() == 0 {
		t.Error("OnDeadLetter hook did not fire")
	}
}

func TestCluster_DelayedTask(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var done atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		done.Add(1)
		return nil
	})
	srv := NewServer(client, clusterServerConfig("default"))
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, clArgs{N: 4}, WithProcessIn(800*time.Millisecond)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if done.Load() != 0 {
		t.Error("delayed task ran immediately")
	}
	waitFor(t, 10*time.Second, "delayed task promoted and run", func() bool { return done.Load() == 1 })
}

func TestCluster_UniqueDedup(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// uniqueEnqueueCmd: second identical enqueue is rejected.
	if _, err := Enqueue(ctx, c, clArgs{N: 5}, WithUnique(30*time.Second)); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	if _, err := Enqueue(ctx, c, clArgs{N: 5}, WithUnique(30*time.Second)); !errors.Is(err, ErrDuplicateTask) {
		t.Errorf("enqueue2: err = %v, want ErrDuplicateTask", err)
	}
	// uniqueScheduleCmd: same for a delayed unique task (different payload → own lock).
	if _, err := Enqueue(ctx, c, clArgs{N: 6}, WithUnique(30*time.Second), WithProcessIn(time.Hour)); err != nil {
		t.Fatalf("schedule1: %v", err)
	}
	if _, err := Enqueue(ctx, c, clArgs{N: 6}, WithUnique(30*time.Second), WithProcessIn(time.Hour)); !errors.Is(err, ErrDuplicateTask) {
		t.Errorf("schedule2: err = %v, want ErrDuplicateTask", err)
	}
}
```

(시나리오 6-12는 Task 5·6에서 이 파일에 이어 붙인다. `rdb` import는 Task 5의 recover 테스트가 사용한다 — Task 4 시점에 unused import 에러가 나면 Task 5에서 쓸 때까지 import를 잠시 빼두거나 `_ = rdb.NewRDB` 없이 Task 5에서 추가하라. 권장: Task 4에서는 `rdb` import를 넣지 말고 Task 5에서 추가.)

- [ ] **Step 2: 클러스터 없이 skip 확인**

Run: `go test -run 'TestCluster_' -p 1 .`
Expected: 전부 SKIP (`REDIS_CLUSTER_ADDRS not set`).

- [ ] **Step 3: 클러스터에서 실행**

Run: `REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 go test -run 'TestCluster_' -p 1 -race -v . 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: 5개 전부 PASS. 실패하면 원인을 조사해 고친다(CROSSSLOT 에러가 나오면 라이브러리 버그 — 보고).

- [ ] **Step 4: 커밋**

```bash
git add cluster_test.go
git commit -m "test: cluster 통합 스모크 1-5 (enqueue/retry/dead-letter/delayed/unique)"
```

---

## Task 5: cluster_test.go — 스케줄러/신뢰성 (시나리오 6-9)

**Files:**
- Modify: `cluster_test.go` (이어 붙이기; import에 `"github.com/kenshin579/chronos-go/internal/rdb"` 추가)

- [ ] **Step 1: 시나리오 6-9 추가**

`cluster_test.go` 끝에 추가:

```go
func TestCluster_SchedulerLeaderFailover(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	ctx := context.Background()

	// Worker that records processed trigger task IDs (deterministic dedup IDs).
	var (
		mu   = make(chan struct{}, 1)
		seen = map[string]int{}
	)
	record := func(id string) {
		mu <- struct{}{}
		seen[id]++
		<-mu
	}
	count := func() int {
		mu <- struct{}{}
		n := len(seen)
		<-mu
		return n
	}
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		record(task.ID())
		return nil
	})
	srv := NewServer(client, clusterServerConfig("default"))
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	mkSched := func() *Scheduler {
		s := NewScheduler(client, SchedulerConfig{LeaderTTL: time.Second})
		if err := RegisterInterval(s, time.Second, clArgs{N: 7}); err != nil {
			t.Fatalf("register: %v", err)
		}
		if err := s.Start(ctx); err != nil {
			t.Fatalf("scheduler start: %v", err)
		}
		return s
	}
	schedA := mkSched()
	schedB := mkSched() // follower

	// Leader fires: distinct triggers accumulate, and none runs twice.
	waitFor(t, 15*time.Second, "2+ distinct triggers", func() bool { return count() >= 2 })

	// Graceful shutdown of A publishes resign (cross-node pub/sub) → B takes over.
	before := count()
	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = schedA.Shutdown(shutCtx)
	cancel()
	waitFor(t, 15*time.Second, "progress after failover", func() bool { return count() > before })

	shutCtx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	_ = schedB.Shutdown(shutCtx2)
	cancel2()

	mu <- struct{}{}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("trigger %s ran %d times, want 1 (dedup)", id, n)
		}
	}
	<-mu
}

func TestCluster_RecoverAbandonedTask(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	r := rdb.NewRDB(client)

	// Enqueue, then dequeue with a consumer that "crashes" (never acks).
	info, err := Enqueue(ctx, c, clArgs{N: 8}, WithQueue("recq"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := r.EnsureGroup(ctx, "recq"); err != nil {
		t.Fatalf("group: %v", err)
	}
	msg, _, err := r.Dequeue(ctx, "dead-consumer", -1, "recq")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if msg.ID != info.ID {
		t.Fatalf("dequeued %s, want %s", msg.ID, info.ID)
	}

	// Recover with minIdle 0 reclaims it (XAUTOCLAIM) and requeues.
	waitFor(t, 5*time.Second, "task recovered", func() bool {
		requeued, archived, rerr := r.Recover(ctx, "recq", "recoverer", 0, 100)
		return rerr == nil && (requeued > 0 || len(archived) > 0)
	})
}

func TestCluster_HeartbeatLongTask(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		runs.Add(1)
		time.Sleep(2 * time.Second) // longer than RecoverMinIdle
		return nil
	})
	cfg := clusterServerConfig("hbq")
	cfg.RecoverMinIdle = 700 * time.Millisecond
	cfg.RecoverInterval = 300 * time.Millisecond
	cfg.HeartbeatInterval = 200 * time.Millisecond
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, clArgs{N: 9}, WithQueue("hbq")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	time.Sleep(3 * time.Second) // processing window + recoverer chances
	if n := runs.Load(); n != 1 {
		t.Errorf("runs = %d, want 1 (heartbeat must keep the lease fresh)", n)
	}
}

func TestCluster_JanitorTrimsArchived(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		return errors.New("always fails")
	})
	cfg := clusterServerConfig("janq")
	cfg.ArchivedRetention = 1 * time.Second
	cfg.JanitorInterval = 300 * time.Millisecond
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	for i := 0; i < 3; i++ {
		if _, err := Enqueue(ctx, c, clArgs{N: 100 + i}, WithQueue("janq"), WithMaxRetry(0)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	insp := NewInspector(client)
	archivedCount := func() int64 {
		qs, err := insp.Queues(ctx)
		if err != nil {
			return -1
		}
		for _, q := range qs {
			if q.Queue == "janq" {
				return q.Archived
			}
		}
		return 0
	}
	waitFor(t, 10*time.Second, "tasks archived", func() bool { return archivedCount() == 3 })
	waitFor(t, 10*time.Second, "janitor trimmed archived", func() bool { return archivedCount() == 0 })
}
```

- [ ] **Step 2: 클러스터에서 실행**

Run: `REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 go test -run 'TestCluster_' -p 1 -race -v . 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: 9개 전부 PASS.

- [ ] **Step 3: 커밋**

```bash
git add cluster_test.go
git commit -m "test: cluster 통합 스모크 6-9 (리더 페일오버/recover/heartbeat/janitor)"
```

---

## Task 6: cluster_test.go — Inspector/슬롯 분산 (시나리오 10-12)

**Files:**
- Modify: `cluster_test.go` (이어 붙이기)

- [ ] **Step 1: 시나리오 10-12 추가**

`cluster_test.go` 끝에 추가:

```go
func TestCluster_InspectorRunAndDelete(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	insp := NewInspector(client)

	// runTaskCmd: promote a far-future scheduled task to the stream.
	runInfo, err := Enqueue(ctx, c, clArgs{N: 10}, WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue run-target: %v", err)
	}
	if err := insp.RunTask(ctx, "default", runInfo.ID); err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	tasks, err := insp.ListTasks(ctx, "default", "scheduled", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, ti := range tasks {
		if ti.ID == runInfo.ID {
			t.Error("task still in scheduled after RunTask")
		}
	}

	// deleteTask: remove a scheduled task entirely.
	delInfo, err := Enqueue(ctx, c, clArgs{N: 11}, WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue delete-target: %v", err)
	}
	if err := insp.DeleteTask(ctx, "default", delInfo.ID); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
	if _, err := insp.GetTask(ctx, "default", delInfo.ID); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("GetTask after delete: err = %v, want ErrTaskNotFound", err)
	}
}

func TestCluster_InspectorQueries(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	if _, err := Enqueue(ctx, c, clArgs{N: 12}, WithProcessIn(time.Hour)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)

	qs, err := insp.Queues(ctx) // Queues (SMembers) + QueueStats per queue
	if err != nil {
		t.Fatalf("queues: %v", err)
	}
	var found bool
	for _, q := range qs {
		if q.Queue == "default" && q.Scheduled == 1 {
			found = true
		}
	}
	if !found {
		t.Errorf("queue stats wrong: %+v", qs)
	}

	tasks, err := insp.ListTasks(ctx, "default", "scheduled", 10) // ListZSetTasks
	if err != nil || len(tasks) != 1 {
		t.Fatalf("list: n=%d err=%v", len(tasks), err)
	}
	ti := tasks[0]
	if ti.Kind != "cluster:demo" || ti.State != "scheduled" || ti.NextProcessAt.IsZero() {
		t.Errorf("task fields wrong: %+v", ti)
	}
	got, err := insp.GetTask(ctx, "default", ti.ID) // GetTask + ZScore
	if err != nil || got.ID != ti.ID {
		t.Errorf("GetTask: got=%+v err=%v", got, err)
	}
}

func TestCluster_TwoQueuesDifferentSlots(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// Prove the two queues really live on different slots — otherwise this
	// test would silently stop covering MOVED redirects if names change.
	const q1, q2 = "alpha", "bravo"
	slot1, err := client.ClusterKeySlot(ctx, "chronos:{"+q1+"}:stream").Result()
	if err != nil {
		t.Fatalf("keyslot: %v", err)
	}
	slot2, err := client.ClusterKeySlot(ctx, "chronos:{"+q2+"}:stream").Result()
	if err != nil {
		t.Fatalf("keyslot: %v", err)
	}
	if slot1 == slot2 {
		t.Fatalf("queues %q and %q hash to the same slot (%d); pick different names", q1, q2, slot1)
	}

	var n1, n2 atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		if task.Args.N == 1 {
			n1.Add(1)
		} else {
			n2.Add(1)
		}
		return nil
	})
	cfg := ServerConfig{
		Queues:          map[string]int{q1: 1, q2: 1},
		Concurrency:     4,
		ForwardInterval: 200 * time.Millisecond,
	}
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	for i := 0; i < 3; i++ {
		if _, err := Enqueue(ctx, c, clArgs{N: 1}, WithQueue(q1)); err != nil {
			t.Fatalf("enqueue q1: %v", err)
		}
		if _, err := Enqueue(ctx, c, clArgs{N: 2}, WithQueue(q2)); err != nil {
			t.Fatalf("enqueue q2: %v", err)
		}
	}
	waitFor(t, 10*time.Second, "both slots' queues fully processed", func() bool {
		return n1.Load() == 3 && n2.Load() == 3
	})
}
```

- [ ] **Step 2: 클러스터에서 전체 12개 실행**

Run: `REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 go test -run 'TestCluster_' -p 1 -race -v . 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: 12개 전부 PASS, 전체 1분 이내.

- [ ] **Step 3: 헤더 체크리스트 대조**

파일 상단 체크리스트의 12항목이 실제 테스트 함수와 1:1 대응하는지 눈으로 대조. 누락 시 추가.

- [ ] **Step 4: 커밋**

```bash
git add cluster_test.go
git commit -m "test: cluster 통합 스모크 10-12 (Inspector 액션·조회, 슬롯 분산)"
```

---

## Task 7: Makefile + 회귀 확인

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: test-cluster 타깃 추가**

`Makefile`의 `.PHONY` 줄과 타깃에 추가:

```make
.PHONY: test test-race vet fmt fmt-check check test-contrib test-cluster
```

`test-contrib` 타깃 아래에 추가:

```make
# Cluster integration tests. Requires the disposable cluster from
# deploy/redis-cluster (docker compose up -d). Skipped when the env is unset.
test-cluster:
	REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
		go test -run 'TestCluster_' -p 1 -race .
```

- [ ] **Step 2: 실행 확인**

Run: `make test-cluster`
Expected: `ok github.com/kenshin579/chronos-go` (12개 PASS).
Run: `make check`
Expected: 기존 스위트 전부 PASS — cluster 테스트는 env 없이 skip되므로 시간 증가 미미. (`go test ./... -p 1`은 cluster_test도 컴파일하므로 빌드 에러가 없어야 한다.)

- [ ] **Step 3: CLI 눈 확인 (관찰 습관)**

```bash
go run ./cmd/chronos --cluster --redis 127.0.0.1:7000 queue ls
```
Expected: 마지막 테스트가 flush한 뒤라 빈 목록 또는 잔여 큐 출력 — 에러 없이 동작하면 OK. `--cluster --db 15`로는 에러가 나는지도 확인:
```bash
go run ./cmd/chronos --cluster --db 15 --redis 127.0.0.1:7000 queue ls; echo "exit=$?"
```
Expected: `chronos: --db is not supported with --cluster...` + exit=2.

- [ ] **Step 4: 커밋**

```bash
git add Makefile
git commit -m "build: make test-cluster 타깃"
```

---

## Task 8: README 문서 + 최종 검증 + 리뷰 + PR

**Files:**
- Modify: `README.md`

- [ ] **Step 1: README에 "Redis Cluster" 섹션 추가**

`README.md`의 `## Delivery semantics` 섹션 **앞**에 삽입:

```markdown
## Redis Cluster

chronos-go works on Redis Cluster out of the box. Every key of a queue is
wrapped in a `{queue}` hash tag, so a queue's keys share one slot (multi-key
Lua stays atomic) while different queues spread across the cluster.

```go
rdb := redis.NewClusterClient(&redis.ClusterOptions{
	Addrs: []string{"node1:6379", "node2:6379", "node3:6379"},
})
srv := chronos.NewServer(rdb, chronos.ServerConfig{ /* ... */ })
```

The CLI connects with `--cluster` (seed nodes, comma-separated — one is enough):

```bash
chronos --cluster --redis node1:6379,node2:6379 queue ls
```

Notes:
- Redis Cluster has only logical database 0 (`--db` is standalone-only).
- The global keys (`chronos:queues`, the scheduler leader lock) are accessed
  with single-key commands only, so they are cluster-safe without a hash tag.
- Sentinel: inject a `redis.NewFailoverClient` — it satisfies the same
  `redis.UniversalClient` interface — but Sentinel is not part of our tested
  matrix yet.

### Verifying against a real cluster

The repo ships a disposable 6-node cluster and a script-complete integration
suite (every Lua script and command pattern runs on cluster at least once):

```bash
cd deploy/redis-cluster && docker compose up -d && cd ../..
make test-cluster
```
```

또한 `## Development` 섹션의 테스트 안내 뒤에 한 줄 추가:

```markdown
Cluster integration tests are opt-in: `make test-cluster` (see
[`deploy/redis-cluster`](deploy/redis-cluster)).
```

- [ ] **Step 2: 최종 검증**

```bash
make check          # 기존 무회귀 (cluster는 skip)
make test-cluster   # 12개 그린 (클러스터 떠 있는 상태)
```

- [ ] **Step 3: 커밋**

```bash
git add README.md
git commit -m "docs: README Redis Cluster 섹션 (키 설계·CLI·로컬 검증법)"
```

- [ ] **Step 4: 코드 리뷰**

k:code-reviewer 서브에이전트로 브랜치 전체(`git diff main...HEAD`) 리뷰. 특히: buildClient 에러 처리, compose의 announce 구성, 테스트 플레이키 가능성(waitFor 타임아웃), 체크리스트-테스트 1:1 대응. 지적 반영 후 재검증.

- [ ] **Step 5: PR 생성 후 클러스터 정리**

```bash
gh pr create --assignee kenshin579 --title "feat: Redis Cluster 지원 (CLI --cluster + 스크립트-완전 통합 검증)" --body "$(cat <<'EOF'
## 배경
코어는 설계상 cluster-safe(해시태그 키, 같은 슬롯 Lua)지만 실제 Cluster에서 검증된 적이 없고, CLI는 standalone 전용이었다. 접속 지원 + 통합 검증 + 문서로 신뢰 갭을 닫는다. 코어 라이브러리 코드는 무변경. Sentinel은 범위 제외(UniversalClient 주입 가능, 공식 검증 밖).

## 변경
- CLI: `--standalone`/`--cluster` 상호 배타 모드 플래그(기본 standalone, 완전 하위호환). `--cluster`는 `--redis` 콤마 시드 노드, `--db≠0` 거부. `buildClient` 분리 + 단위 테스트.
- `deploy/redis-cluster`: 일회용 6노드 클러스터(단일 컨테이너, announce 127.0.0.1 — 호스트에서 MOVED 리다이렉트 동작).
- `testutil.NewClusterRedis`: `REDIS_CLUSTER_ADDRS` opt-in, 없으면 skip.
- `cluster_test.go`: 스크립트-완전 스모크 12개 — 모든 Lua/명령 패턴이 cluster에서 최소 1회 실행(체크리스트 주석으로 리뷰 가능). 리더 resign pub/sub, XAUTOCLAIM, XCLAIM heartbeat, 슬롯 상이 큐 2개(CLUSTER KEYSLOT 단언) 포함.
- `make test-cluster` + README "Redis Cluster" 섹션.

## 테스트 계획
- [x] `make check` 무회귀 (cluster 테스트는 env 없으면 skip — CI 무변경)
- [x] `make test-cluster` 12개 그린 (deploy/redis-cluster 대상)
- [x] CLI 눈 확인: `chronos --cluster --redis 127.0.0.1:7000 queue ls`, `--cluster --db 15` 에러

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
cd deploy/redis-cluster && docker compose down
```

---

## Self-Review (계획 작성자 확인 완료)

- **스펙 커버리지**: CLI 플래그(T1) / compose(T2) / testutil(T3) / 12개 시나리오(T4-6, 스펙 매트릭스와 1:1) / Makefile(T7) / README+Sentinel 한 줄(T8) / 코어 무변경 유지 — 전 항목 매핑.
- **placeholder 스캔**: 전 스텝 실제 코드·명령·기대출력 포함. Task 4의 rdb import 시점 주의사항 명시.
- **타입 일관성**: `NewClusterRedis(t) redis.UniversalClient`(T3)를 T4-6이 사용, `buildClient(standalone, cluster bool, addr string, db int)`(T1) 시그니처가 테스트와 일치, `clArgs`/`clusterServerConfig`/`waitFor`가 T4에서 정의되고 T5-6에서 사용, `Recover(ctx, qname, consumer, minIdle, batch)` 시그니처는 기존 rdb와 일치(M4 메모리 노트 기준 — 구현자는 실제 시그니처 확인 후 필요 시 조정).
- **주의**: compose의 `$$port`(compose 변수 이스케이프), 시나리오 6의 채널 기반 뮤텍스는 sync.Mutex로 바꿔도 무방(구현자 재량).
