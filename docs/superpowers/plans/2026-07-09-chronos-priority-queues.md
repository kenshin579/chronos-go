# chronos-go 우선순위 큐 (weighted + strict)

## 목표

`ServerConfig.Queues map[string]int`의 가중치는 현재 무시된다(키만 사용, 모든 큐 동등).
이 가중치를 실제로 사용하고, asynq 패리티인 `StrictPriority` 옵션을 추가한다.

- **weighted(기본)**: 모든 큐에 일감이 있을 때, weight가 6인 큐는 weight 1인 큐보다
  약 6배 자주 dequeue된다. 어떤 큐도 굶지 않는다(기아 없음).
- **strict(`StrictPriority: true`)**: weight가 높은 큐를 항상 먼저 비운다. 낮은 큐는
  높은 큐가 빌 때만 처리된다(의도된 기아 — asynq와 동일 의미).
- weight `<= 0`은 1로 정규화한다(footgun 방지, asynq는 에러지만 우리는 관대 처리 —
  Inspector NOGROUP 관대 처리와 같은 철학).

## 비-목표

- rdb 계층 변경 없음 — `Dequeue(ctx, consumer, block, qname)`는 이미 단일 큐.
- 큐별 동시성 상한, 동적 가중치 변경(재시작 필요), pause/resume은 범위 밖.

## 설계

### 선택 알고리즘: Smooth Weighted Round-Robin (SWRR, nginx 방식)

슬롯 리스트 확장(weight 6+3+1 → 10슬롯) 대신 SWRR을 쓴다:
- 각 pick마다 `current[i] += weight[i]`, 최댓값 큐를 선택, `current[선택] -= total`.
- 결정적이고, 매끄럽게 섞이며(A A A B가 아니라 A B A A식 인터리브), 상태가 O(큐 수).
- 순수 Go 자료구조(`wrr.go`)로 두고 Redis 없이 단위 테스트한다.

### fetchLoop 변경 (server.go)

현재 구조(비블로킹 스윕 → 비면 라운드로빈 블로킹)를 유지하되 순서만 우선순위화:

1. **비블로킹 스윕 순서**
   - weighted: SWRR로 primary 큐를 뽑고, primary부터 시도. 비면 나머지 큐를
     가중치 내림차순(동률은 이름순, 결정성)으로 시도. → 모든 큐에 일감이 있으면
     분포가 정확히 가중치 비율, 일부만 있으면 노는 워커 없이 즉시 처리.
   - strict: 항상 가중치 내림차순으로 시도. 높은 큐가 비어야만 낮은 큐 차례.
2. **블로킹 단계(모두 비었을 때)**
   - weighted: SWRR가 뽑은 primary에 `pollBlock`(1s) 블로킹. 시간이 지나면 SWRR가
     다른 큐도 차례로 뽑으므로 어떤 큐도 블로킹 감시에서 굶지 않는다.
   - strict: 최고 가중치 큐에 블로킹. 낮은 큐의 최악 응답 지연은 `pollBlock`(1s) —
     매 루프 비블로킹 스윕이 다시 돌기 때문. (현재 라운드로빈도 동일한 상한.)

### 공개 API 변화

```go
type ServerConfig struct {
    // Queues maps queue name to weight. Under load, a queue with weight 6 is
    // dequeued ~6x as often as a queue with weight 1 (no starvation).
    // Weights <= 0 are treated as 1.
    Queues map[string]int
    // StrictPriority, if true, always drains higher-weight queues first;
    // lower-weight queues run only when every higher one is empty.
    StrictPriority bool
    ...
}
```

`server.go:44`의 "Only the keys are used today" 주석 삭제.

## TDD 순서

1. `wrr_test.go` (Redis 불필요): SWRR 순수 로직 —
   - {A:3, B:1} 연속 4픽마다 A 3회·B 1회, 인터리브(A 3연속 아님).
   - {A:1, B:1} 교대. 단일 큐. weight<=0 → 1 정규화.
2. `priority_test.go` (실제 Redis, DB 15):
   - **가중치 분포**: critical(3):low(1)에 각 20개 선적재, Concurrency 1 →
     처리 순서 기록, 앞 16개 중 critical이 [10,14] 범위(이론값 12).
   - **기아 없음**: weight 5:1 각 12개 선적재 → 앞 12개 안에 low ≥ 1.
   - **strict**: critical 5 + low 5 선적재, `StrictPriority: true`, Concurrency 1 →
     앞 5개 전부 critical.
   - **가중치 0 관대 처리**: {q: 0} 서버가 정상 동작(=1 취급).
3. 구현: `wrr.go` → `fetchLoop` 교체 → 테스트 그린.

## 관찰 습관 (완료 조건)

- `examples/tour`에 **섹션 10**: critical(가중치 4)·low(가중치 1)에 태스크 선적재 →
  처리 순서 출력으로 critical이 더 자주 나오는 것을 눈으로 확인. 마지막에
  `StrictPriority`면 어떻게 달라지는지 한 줄 설명.
- README: `vs. asynq` 표에 우선순위 행 추가, Known limitations의
  "priority is not implemented" 제거, ServerConfig 예시에 가중치 설명.

## 검증

- `make check` (gofmt + vet + core -race -p 1 + contrib)
- `go run ./examples/tour` 10섹션 눈 확인
- 코드 리뷰(k:code-reviewer) 후 PR
