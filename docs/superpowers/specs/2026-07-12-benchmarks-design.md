# chronos-go 성능 벤치마크 설계

- 상태: 승인됨 (2026-07-12)
- 관련: `contrib/prometheus`(별도 모듈 선례, loadgen), `Makefile`, `docs/BENCHMARKS.md`(신규)
- 배경: 큐 라이브러리인데 성능 수치가 없다 — "asynq 대비 뭐가 나은가"에 기능으로만
  답하는 상태. 벤치마크로 (1) 병목·버그를 v0.x인 지금 발견, (2) README에 채택
  근거 수치 확보, (3) v1.0.0 동결의 근거 마련.

## 확정 결정 (브레인스토밍)

1. **asynq 비교 포함** — 결과가 어느 쪽이든 그대로 공개(느리면 병목을 찾아
   고치는 것이 이 작업의 원래 목적). 공정성 원칙 명문화(아래).
2. **전용 바이너리** (`go test -bench` 아님) — producer/consumer 파이프라인의
   정상 상태 처리량·지연 백분위 측정은 반복 단위 모델과 맞지 않음.
3. **`benchmarks/` 별도 Go 모듈** — asynq 의존성을 코어에서 격리(contrib 패턴).
4. **로컬 standalone Redis 기준** (docker 아님 — macOS docker 네트워크 변동성
   회피, 문서 명시). CI 미포함(로컬 opt-in, cluster 테스트와 동일 철학).
5. **진행 방식**: 하네스+chronos 단독 측정 → 결과 보고 → asynq 비교 → 결과
   보고(병목 시 개선 논의) → 문서화·PR. 측정·해석이 본체인 작업.

## 설계

### A. 모듈 구조

```
benchmarks/
  go.mod            module .../benchmarks, replace ../ => 코어, require asynq
  bench/            측정 라이브러리 (러너·통계·리포터)
    scenario.go     시나리오 인터페이스 + 공통 러너(워밍업/타이머/수집)
    stats.go        지연 수집(slice) → p50/p95/p99/max, tasks/s 계산
    report.go       사람용 표 + JSONL 출력
  chronosbench/     chronos-go 시나리오 구현 (enqueue/e2e/chain/group)
  asynqbench/       asynq 시나리오 구현 (enqueue/e2e)
  cmd/bench/        CLI: -target chronos|asynq -scenario enqueue|e2e|chain|group
                    -concurrency N -tasks N -payload bytes -producers P
                    -redis addr -db N -json
  README.md         사용법
```

### B. 시나리오 정의

공통: payload = 지정 크기(기본 100바이트)의 JSON(`{"ts": <enqueue unix nano>, "pad": "..."}`),
실행 전 FlushDB, 워밍업(전체의 10% 태스크는 통계 제외), 완료 판정은 처리 수 카운트.

| # | 시나리오 | 방법 | 산출 | asynq |
|---|---|---|---|---|
| 1 | enqueue | P개 goroutine이 총 M개 enqueue(서버 없음) | enqueue tasks/s | ✅ |
| 2 | e2e | 서버(Concurrency C) 기동 후 P개 producer가 M개 enqueue, 핸들러가 payload의 ts로 지연 기록 | 처리 tasks/s, 지연 p50/p95/p99/max | ✅ |
| 3 | scaling | 시나리오 2를 C=1/4/16/64로 반복 | C별 표 | ✅ |
| 4 | chain | 길이 L(기본 10) 체인 K개 완주 vs 같은 수의 독립 태스크 | 링크당 오버헤드(ms) | ❌ |
| 5 | group | N(기본 10)멤버 그룹 K개(콜백 완료까지) vs 독립 태스크 | 멤버당 오버헤드 + 콜백 지연 | ❌ |

- 지연 측정: 핸들러에서 `time.Now() - payload.ts`를 채널로 수집(락 경합 회피),
  종료 후 정렬해 백분위 계산. HDR 히스토그램은 YAGNI(태스크 수 ≤ 수십만이라
  slice 정렬로 충분).
- 각 시나리오 3회 실행, **중앙값 보고**(러너가 반복·집계 지원).

### C. 공정성 원칙 (BENCHMARKS.md에 명문화)

- 양쪽 모두 **기본 설정**(chronos `ServerConfig`/asynq `Config` 기본값), 공통
  파라미터(동시성·payload·태스크 수)만 동일 지정.
- 동일 Redis(로컬 standalone, 버전 명시), 동일 Go 버전, 같은 머신에서 교대 실행.
- 결과 그대로 공개 + "환경 따라 다름, `make bench`로 직접 재현 권장, asynq 설정
  개선 제안 환영" 명시.
- asynq 시나리오도 동일 payload 스키마·동일 측정 방식(핸들러에서 ts delta).

### D. 문서화 / 통합

- `docs/BENCHMARKS.md`: 방법론·머신 사양(측정 시점 기입)·전체 표·재현 방법.
- README "Performance" 섹션: 핵심 수치 3~4줄 + BENCHMARKS.md 링크.
  (수치는 2단계 측정 후 확정 — 하네스 PR에는 자리 잡고, 수치 기입은 측정 후.)
- `Makefile`: `make bench`(전 시나리오 순차 실행, chronos+asynq, 표 출력).
- CI 무변경. `make check`가 benchmarks 모듈의 **빌드**는 검증(테스트는 스모크
  1개 — 러너가 소량 태스크로 동작하는지, Redis 필요 시 skip 패턴).

## 검증 / 마무리

- 하네스 자체의 정확성: 스모크 테스트(소량으로 tasks/s·백분위가 0이 아니고
  단조 정렬됨), 지연 계산 단위 테스트(고정 입력 → 기대 백분위).
- 1단계: chronos 단독 전 시나리오 실행 → 결과 보고(사용자와 해석).
- 2단계: asynq 비교(시나리오 1-3) → 결과 보고 → 병목 시 개선 여부 논의.
- 3단계: BENCHMARKS.md·README 수치 기입 → 리뷰 → PR(assignee kenshin579).

## 알려진 한계 / 후속

- 단일 머신·로컬 Redis 수치 — 네트워크 왕복이 실제 배포보다 짧아 절대치는
  낙관적. 상대 비교(chronos vs asynq, 시나리오 간)가 주 정보.
- Cluster 벤치·소크 테스트(A-2)는 범위 외(후속).
- 벤치 하네스는 코어 공개 API만 사용(내부 튜닝 금지 — 공정성).
