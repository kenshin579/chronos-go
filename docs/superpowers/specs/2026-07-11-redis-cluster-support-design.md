# chronos-go Redis Cluster 지원 설계

- 상태: 승인됨 (2026-07-11)
- 관련: `internal/base/keys.go`(해시태그 키 설계), `cmd/chronos`, `internal/testutil`

## 배경 / 목적

코어 라이브러리는 설계상 이미 Cluster-ready다:
- 모든 API(`NewClient`/`NewServer`/`NewInspector`/`NewScheduler`)가 `redis.UniversalClient`를
  받으므로 `redis.NewClusterClient`를 그대로 주입할 수 있다.
- 큐의 모든 키가 `chronos:{queue}:...` 해시태그로 같은 슬롯에 모여, 멀티키 Lua가
  Cluster에서 안전하다. 전수 검증 결과 **모든 Lua 스크립트의 KEYS가 같은 태그이거나
  단일 전역 키(leader)뿐**이고, 전역 키(`chronos:queues`, `chronos:sched:*`)는 의도적으로
  스크립트 밖 단독 명령으로 분리돼 있다.

그러나 두 가지 갭이 있다:
1. **한 번도 실제 Cluster에서 실행·검증된 적이 없다** — 모든 테스트가 standalone(DB 15) 전용.
2. `cmd/chronos` CLI가 `redis.NewClient` standalone 전용이라 Cluster에 붙을 수 없다.

"multi cluster scheduler"로 출발한 라이브러리가 Cluster에서 검증된 적 없다는 것이 가장 큰
신뢰 갭이므로, 이번 작업으로 **접속 지원 + 스크립트-완전 통합 검증 + 문서화**를 완성한다.

## 확정 결정 (브레인스토밍)

1. **범위**: Redis Cluster만. **Sentinel 제외** — 코어가 `UniversalClient`라 주입은 이미
   가능하므로 문서에 "주입 가능하나 공식 검증 범위 밖" 한 줄만 남긴다.
2. **성공 기준**: (c) CLI 접속 지원 + Cluster 통합 검증 + README 문서화.
3. **테스트 실행 환경**: (b) **로컬 opt-in만**. CI 변경 없음. `REDIS_CLUSTER_ADDRS` 환경변수가
   없으면 skip(기존 "Redis 없으면 skip" 패턴). docker compose로 로컬 클러스터 제공.
4. **CLI 플래그**: `--standalone` / `--cluster` 명시적 모드 플래그(상호 배타, 기본
   standalone → 기존 사용법 완전 하위호환).
5. **테스트 커버리지**: (a+) **스크립트-완전 스모크** — "모든 Lua 스크립트와 모든 명령
   패턴이 Cluster에서 최소 1회 실행되고 결과가 검증된다"를 명시적 충분성 기준으로 삼는다.

### 충분성 논거 (a+가 충분한 이유)

Cluster가 standalone과 다르게 동작할 수 있는 지점은 4가지뿐이다:
(1) CROSSSLOT(멀티키 키들이 다른 슬롯) — **결정적 실패**라 해당 스크립트 1회 실행으로 잡힘,
(2) MOVED/ASK 리다이렉트 — 다른 슬롯 큐 2개 병행으로 실증,
(3) pub/sub 노드 간 전파 — leader resign 테스트로 실증,
(4) 노드별 스크립트 캐시(NOSCRIPT) — go-redis가 재시도, 명령이 노드에 분산되면 자연 검증.
나머지(재시도 로직·misfire·janitor 배치 등)는 순수 로직이라 standalone 스위트가 이미 커버.
잔여 리스크는 노드 장애/지연 하의 타이밍 동작(카오스 영역)으로, 전체 스위트 재실행(b안)으로도
잡히지 않으며 at-least-once 설계가 감내하는 부분이다.

## 설계

코어 라이브러리 코드는 **변경 없음**. 바뀌는 것은 CLI, 테스트 인프라, 문서뿐이다.

### 1. CLI (`cmd/chronos/main.go`)

```bash
chronos --redis 127.0.0.1:6379 --db 15 queue ls                # 기본 = standalone (하위호환)
chronos --standalone --redis 127.0.0.1:6379 queue ls           # 명시적 standalone
chronos --cluster --redis node1:7000,node2:7001 queue ls       # cluster (시드 노드 1개도 OK)
```

- `--standalone`/`--cluster` bool 플래그 추가. 둘 다 주면 에러. 둘 다 없으면 standalone.
- `--cluster`일 때:
  - `--redis` 값을 콤마 분리해 `redis.NewClusterClient(&redis.ClusterOptions{Addrs: ...})`.
  - `--db`가 0이 아니면 에러: "Redis Cluster has only database 0".
- `run(args, client, out)`은 이미 `redis.UniversalClient`를 받으므로 연결 생성부만 변경.
  기존 CLI 테스트 무변경, 새 플래그 파싱은 단위 테스트 추가(연결 생성 함수를 분리해 검증).

### 2. 로컬 Cluster 환경 (`deploy/redis-cluster/`)

- `docker-compose.yml`: 공식 `redis:7-alpine` 6노드(포트 7000-7005, 3마스터+3레플리카),
  각 노드 `--cluster-enabled yes`, 마지막에 일회성 init 컨테이너가
  `redis-cli --cluster create ... --cluster-replicas 1 --cluster-yes` 실행.
  공식 이미지 사용으로 arm64(Mac) 포함 플랫폼 안전.
- `README.md`(deploy/redis-cluster): 띄우는 법, 내리는 법, 데이터가 일회용임을 명시.

### 3. 테스트 헬퍼 (`internal/testutil`)

```go
// NewClusterRedis connects to a disposable test Redis Cluster listed in
// REDIS_CLUSTER_ADDRS (comma-separated). Skips the test when unset. Flushes
// every master before the test and on cleanup.
func NewClusterRedis(t *testing.T) redis.UniversalClient
```

- `REDIS_CLUSTER_ADDRS` 없으면 `t.Skip` → 일반 `make check`는 지금과 완전히 동일.
- 연결 후 `ForEachMaster`로 `FlushAll`(전용 일회용 클러스터 전제) + `t.Cleanup` 동일 처리.
- Cluster는 논리 DB 0뿐이므로 DB 선택 없음.

### 4. 통합 테스트 (`cluster_test.go`, 루트 패키지, `TestCluster_*`)

파일 상단에 **스크립트 체크리스트 주석**을 박아 "빠진 스크립트 없음"을 리뷰 가능하게 한다.
커버 매트릭스(모든 Lua/명령 패턴 ≥ 1회):

| # | 검증 대상 | 시나리오 |
|---|---|---|
| 1 | enqueueCmd + Dequeue(XREADGROUP) + Done(XACK+XDEL) | enqueue→처리→ack |
| 2 | moveToZSetCmd(retry) + forwardCmd(retry) | 실패→retry→재승격→성공 |
| 3 | moveToZSetCmd(archive) + OnDeadLetter | MaxRetry 소진→archived |
| 4 | scheduleCmd + forwardCmd(scheduled) | 지연 태스크 승격·실행 |
| 5 | uniqueEnqueueCmd / uniqueScheduleCmd | 중복 거부(ErrDuplicateTask) |
| 6 | periodicCmd + leader acquire/renew/resign + resign pub/sub | 스케줄러 2대, 리더만 발화, resign 즉시 인계 |
| 7 | recover(XAUTOCLAIM) | 죽은 컨슈머 태스크 회수 |
| 8 | heartbeat(XCLAIM JUSTID + PEXPIRE) | RecoverMinIdle 초과 긴 태스크 1회 실행 |
| 9 | janitor(TrimArchived) | archived 나이/개수 정리 |
| 10 | runTaskCmd / deleteTask (Inspector) | dead-letter 재실행·삭제 |
| 11 | QueueStats/Queues/ListZSetTasks/GetTask | Inspector 조회 정합 |
| 12 | 다른 슬롯 큐 2개 병행 | 해시태그 분산 + MOVED 리다이렉트 실증 |

- #12의 큐 이름은 CRC16 슬롯이 실제로 다르도록 선정하고, 슬롯이 다름을 테스트 안에서
  `CLUSTER KEYSLOT`으로 단언한다(이름 하드코딩의 침묵 무효화 방지).
- 각 테스트는 짧은 인터벌(기존 테스트 패턴)로 수 초 내 완료. 전체 목표 < 1분.

### 5. Makefile

```make
test-cluster:  ## Run cluster integration tests (requires deploy/redis-cluster up)
	REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
		go test -run 'TestCluster_' -p 1 -race .
```

### 6. 문서 (루트 README)

- **"Redis Cluster" 섹션 신규**:
  - 키 설계 설명: 한 큐 = 한 슬롯(해시태그) → 멀티키 Lua 안전, 큐 여러 개 = 슬롯 분산.
  - 코드 예시: `redis.NewClusterClient` 주입.
  - CLI 사용법(`--cluster --redis ...`).
  - 로컬 검증법: `cd deploy/redis-cluster && docker compose up -d` → `make test-cluster`.
  - 제약: Cluster는 논리 DB 0뿐. `chronos:queues`/leader/`chronos:sched:*`는 전역 키지만
    단일 명령으로만 접근하므로 안전.
  - Sentinel: `UniversalClient` 주입으로 사용 가능하나 공식 검증 범위 밖.
- Known limitations의 관련 항목 갱신(cluster 검증됨을 반영).

## 테스트 (TDD)

- CLI: 연결 생성 로직을 `buildClient(mode, addrs, db) (redis.UniversalClient, error)`류의
  순수 함수로 분리해 단위 테스트(상호 배타 에러, cluster+db 에러, 콤마 분리, 기본값).
- 통합: 위 매트릭스 12개(각각 실패 확인이 어려운 통합 특성상, 클러스터 없이 skip됨을
  확인하고 클러스터에서 green을 확인하는 방식).

## 검증 / 마무리

- `make check` — 기존 스위트 무회귀(cluster 테스트는 skip).
- `docker compose up -d`(deploy/redis-cluster) → `make test-cluster` 전체 그린.
- CLI 눈 확인: cluster에 `chronos --cluster --redis 127.0.0.1:7000 queue ls`.
- k:code-reviewer 리뷰 → PR(assignee kenshin579) → 머지.

## 알려진 한계 / 후속

- 노드 장애·failover 중 타이밍 동작(카오스)은 범위 밖 — at-least-once 설계가 감내.
- Sentinel 공식 지원은 후속(필요 시 같은 패턴으로 `--sentinel` + FailoverClient).
- CI에서의 cluster job은 후속 선택지(현재는 로컬 opt-in).
