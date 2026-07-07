# chronos-go 관찰 가이드 (UI 없이 동작 확인하기)

chronos-go는 UI가 없는 라이브러리다. "정말 동작하는가"를 눈으로 확인하는 두 가지 방법:

1. **투어 데모 실행** — 여태 만든 기능이 실제로 도는 모습을 로그로 본다.
2. **Redis 직접 들여다보기** — 내부 자료구조(stream/ZSET/락)가 설계대로 움직이는지 확인한다.

테스트가 "통과했다"고 말한다면, 이 문서는 "내가 돌려서 봤다"를 준다.

---

## 0. 준비: 로컬 Redis

테스트·데모 모두 `$REDIS_ADDR`(기본 `127.0.0.1:6379`)의 **DB 15**를 쓴다.

```bash
redis-server --daemonize yes --port 6379 --save "" --appendonly no
redis-cli -p 6379 ping    # → PONG
```

---

## 1. 투어 데모

M1~M3의 핵심 동작(기본 처리 · 재시도 · dead-letter · 지연 실행 · 중복 억제)을 한 번에 보여준다.

```bash
go run ./examples/tour
```

시작 시 DB 15를 flush하므로 매번 깨끗한 출력이 나온다. 각 섹션에서 태스크가 enqueue되고
워커가 처리·재시도·보관·지연 실행·중복 거부하는 로그를 순서대로 볼 수 있다.

> 앞으로 마일스톤마다 이 투어에 시나리오를 하나씩 추가해, "완료 = 테스트 통과 + 투어에서 눈으로 확인"으로 삼는다.

---

## 2. Redis 실시간 관찰

투어를 돌리는 동안(또는 데모의 `time.Sleep` 구간에) **다른 터미널**에서 상태를 들여다본다.

### 2-1. 모든 명령 실시간 스트림

```bash
redis-cli -n 15 MONITOR
```

`XADD`(큐 투입), `XREADGROUP`(워커 소비), `XACK`(완료), `ZADD`(재시도/지연/보관 예약),
`SET ... NX PX`(unique 락)가 흐르는 것을 그대로 볼 수 있다.

### 2-2. 키 레이아웃 (설계 문서 §3과 대조)

큐 이름은 Redis Cluster hash tag `{}`로 감싸 같은 슬롯에 배치된다.

```bash
redis-cli -n 15 KEYS 'chronos:*'
```

| 키 | 자료구조 | 의미 |
|---|---|---|
| `chronos:{default}:stream` | Stream | 즉시 실행 대기 (Consumer Group `chronos`) |
| `chronos:{default}:t:<id>` | HASH | 태스크 본문(`msg`) + 권위 상태(`state`) |
| `chronos:{default}:scheduled` | ZSET | 지연/예약 (score = process_at, 소수 초) |
| `chronos:{default}:retry` | ZSET | 재시도 대기 (score = retry_at) |
| `chronos:{default}:archived` | ZSET | dead-letter (score = died_at) |
| `chronos:{default}:unique:<kind>:<sha256>` | STRING | 중복 억제 락 (값 = taskID) |
| `chronos:queues` | SET | 등록된 큐 목록 |

### 2-3. 상태별 조회

```bash
# 스트림(대기 중) 길이와 컨슈머 그룹 상태
redis-cli -n 15 XLEN chronos:{default}:stream
redis-cli -n 15 XINFO GROUPS chronos:{default}:stream

# 처리 중(PEL, 워커가 읽었으나 미ACK)
redis-cli -n 15 XPENDING chronos:{default}:stream chronos

# 지연/재시도/보관 예약 (점수 = unix 시각)
redis-cli -n 15 ZRANGE chronos:{default}:scheduled 0 -1 WITHSCORES
redis-cli -n 15 ZRANGE chronos:{default}:retry     0 -1 WITHSCORES
redis-cli -n 15 ZRANGE chronos:{default}:archived  0 -1 WITHSCORES

# 특정 태스크 본문/상태
redis-cli -n 15 HGETALL chronos:{default}:t:<task-id>

# unique 락 (값이 어떤 taskID를 가리키는지)
redis-cli -n 15 KEYS 'chronos:{default}:unique:*'
redis-cli -n 15 GET  chronos:{default}:unique:<kind>:<sha256>
```

### 2-4. 관찰 포인트 (투어 섹션과 매칭)

- **재시도**: 실패 직후 `chronos:{default}:retry` ZSET에 태스크가 잠깐 나타났다가, forwarder가
  스트림으로 되돌리면 사라진다.
- **dead-letter**: 재시도 소진 후 `chronos:{default}:archived` ZSET에 남는다(수동 조회/재시도 대상).
- **지연 실행**: enqueue 직후 `scheduled` ZSET에 있다가 예약 시각이 지나면 스트림으로 이동.
- **중복 억제**: 1차 enqueue 후 `unique:*` 키가 생기고, 처리 완료 시 사라진다. 이 키가 있는 동안
  동일 payload의 2차 enqueue는 `ErrDuplicateTask`.

---

## 2b. CLI로 조회·관리

redis-cli로 원시 키를 보는 대신, `chronos` CLI로 상태를 조회하고 태스크를 재실행/삭제할 수 있다.

```bash
go run ./cmd/chronos --db 15 queue ls                      # 큐별 상태 카운트
go run ./cmd/chronos --db 15 task ls default archived      # dead-letter 목록
go run ./cmd/chronos --db 15 task run default <task-id>    # 지금 재실행
go run ./cmd/chronos --db 15 task rm  default <task-id>    # 삭제
```

`--redis <addr>`(기본 `$REDIS_ADDR` 또는 `127.0.0.1:6379`), `--db <n>`(기본 0)로 대상 지정.
실사용에서는 앱이 쓰는 DB에 맞춰 `--db`를 지정한다(테스트/데모는 15).

---

## 3. 자동화된 검증 (테스트)

관찰과 별개로, 회귀 방지는 테스트가 담당한다. 카노니컬 명령:

```bash
make test-race     # = go test ./... -race -p 1  (패키지 공유 DB 때문에 -p 1 필수)
make check         # gofmt-check + vet + test-race
```

Redis가 없으면 테스트는 실패가 아니라 SKIP된다.
