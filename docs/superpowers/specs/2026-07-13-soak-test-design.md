# chronos-go 소크 테스트 설계 (A-2)

- 상태: 승인됨 (2026-07-13)
- 관련: `benchmarks/`(모듈 구조·JSONL 관례), `contrib/prometheus/cmd/loadgen`
  (유사 워크로드 — 재사용하지 않음, 데모와 검증의 목적 분리),
  `cluster_test.go`(로컬 opt-in 검증 철학)
- 범위: 장기 부하 실행 커맨드 1개 + 판정 라이브러리. CI 잡·Prometheus
  노출·Grafana 대시보드는 범위 외.

## 확정 결정 (브레인스토밍)

1. **실행 형태 = `benchmarks/cmd/soak` 독립 커맨드** (a안): 서버+스케줄러+
   클라이언트를 한 프로세스에서 돌리며 주기 샘플링·최종 판정(exit code).
   Go 테스트(-timeout 제약, 출력 버퍼링)나 loadgen 확장(데모/검증 목적 혼합)
   대신 선택.
2. **부하 프로파일 = 전 기능 혼합** (a안): 누수는 대부분 "특정 경로가 키를
   안 지우는" 형태 — 커버리지가 곧 검출력. 모든 키 패밀리(스트림, retry/
   scheduled/archived/completed ZSET, unique 락, group SET, queues/paused/
   schedules/리더 키)의 생애주기를 반복시킨다.
3. **판정 = 전·후반 창 비교** (a안): 워밍업 첫 10% 절삭 후 전반/후반 평균
   비교. GC 주기·배치 타이밍 노이즈에 강건하고 판정 근거를 표로 제시 가능.
4. **로컬 전용, 기본 1시간** (a안): `make soak`. CI 미변경 — cluster 스모크·
   벤치마크와 동일한 "로컬 opt-in + 문서화" 패턴. 큰 릴리스 전 4h 수동 실행을
   README 체크리스트로 명시.
5. **관찰 = stdout 한 줄 + JSONL** (a안): Prometheus 노출은 하지 않음(YAGNI —
   그래프 필요 시 기존 loadgen+Grafana 스택이 존재).

## 구조

```
benchmarks/
  soak/            # 라이브러리 (판정 로직 단위 테스트 가능)
    workload.go    # 혼합 부하 생성기
    sampler.go     # 30초 주기 지표 수집 → Sample + JSONL 기록
    verdict.go     # 창 비교 판정 (순수 함수 — TDD 대상)
  cmd/soak/
    main.go        # 플래그 파싱, 조립, 신호 처리
```

- benchmarks 모듈 내부 → 루트 모듈 의존성 오염 없음, `bench/`와 대칭.
- 플래그: `-duration 1h -rate 200 -redis 127.0.0.1:6379 -db 15 -out soak.jsonl`.
- 시작 시 대상 DB를 FLUSH하고 경고 출력(벤치마크와 동일 관례).
- Ctrl-C(SIGINT): 워크로드를 멈추고 **그 시점까지의 샘플로 판정** 후 종료.

## 워크로드 (전 기능 혼합)

한 프로세스에서: 서버(큐 `soak-a`/`soak-b` 가중치 3:1, Concurrency 16) +
interval 스케줄러 + 클라이언트 부하 goroutine들.

| 경로 | 비율/주기 | 목적 |
|---|---|---|
| 일반 성공 | rate의 ~80% | 스트림·hash 생애주기 |
| 실패→재시도→성공 | 10% (2번째 시도 성공) | retry ZSET 순환 |
| dead-letter행 | 5% (MaxRetry=1, 항상 실패) | archived 증가 → janitor 정리 |
| discard | 5% (항상 실패 + WithDeadLetterDiscard) | 즉시 삭제 경로 |
| 지연 태스크 | 일반 성공분 중 10%p를 5~15s 지연으로 | scheduled ZSET 순환 (rate 총량은 불변) |
| unique | 10초마다 동일 payload 5개(4개 dedup) | 락 획득·해제·잔존 |
| retention | 성공분의 20%에 WithRetention(30s) | completed ZSET 순환 |
| chain 3링크 | 10초마다 1개 | chain 경로 |
| group 3멤버+콜백 | 10초마다 1개 | group SET 생성·삭제 |
| interval 스케줄 1s | 상시 | 레지스트리·dedup 키·리더 갱신 |
| pause/resume | 2분마다 soak-b를 30초 pause | pause 경로 + 적체 후 배수 |

- 핸들러: 1~5ms 가짜 작업. 실패 여부는 **payload 플래그로 결정적** 제어.
- 비율은 태스크 종류를 라운드로빈/카운터로 섞어 구현(난수 시드 불요).

## 샘플러 (30초 주기)

매 샘플:
1. `runtime.GC()` 후 `runtime.ReadMemStats` → `HeapAlloc` (GC 노이즈 제거).
2. `runtime.NumGoroutine()`.
3. Redis `DBSIZE` + 패밀리별 카운트: 큐별 XLEN, retry/scheduled/archived/
   completed ZCARD, unique 락 수(SCAN `chronos:*:unique:*`), group SET 수
   (SCAN `chronos:*:group:*`), 레지스트리 HLEN.
4. 누적 완료 카운트에서 처리량(t/s) 계산 — 성능 급락도 이상 신호.

stdout 한 줄(`[00:12:30] heap=48MB gor=52 dbsize=1204 tput=198/s ...`) +
JSONL 한 레코드(전 필드).

## 판정 (창 비교)

워밍업 첫 10% 샘플 절삭 → 나머지를 전반/후반으로 이등분, 평균 비교:

| 지표 | 실패 조건 |
|---|---|
| HeapAlloc | 후반 > 전반 × 1.2 |
| goroutine | 후반 − 전반 > 10 (절대치) |
| DBSIZE | 후반 > 전반 × 1.1 |

- pause 적체(30초 주기)는 30분+ 창에서 평균화되므로 오탐 없음.
- 패밀리별 카운트는 판정에 쓰지 않고 실패 시 원인 진단용으로 표에 포함.
- 종료 시 지표별 전반/후반/증감/판정 표 출력. 실패 → exit 1.
- `-duration < 30m`: "참고용 — 판정 신뢰 불가" 경고와 함께 **항상 exit 0**
  (quick 실행이 CI적 습관으로 오용되는 것 방지).

## 실행·문서

- Makefile: `soak`(1h), `soak-quick`(3m — 배선 확인용).
- `benchmarks/README.md`에 소크 섹션, 루트 README에 릴리스 전 체크리스트
  ("큰 릴리스 전 `make soak`, v1.0.0급은 `-duration 4h`").

## 테스트 (TDD)

1. `verdict.go` 순수 함수 단위 테스트: 워밍업 절삭, 창 분할(홀수 개수),
   안정/선형 증가/계단형 시계열, 각 임계 경계.
2. `sampler` JSONL 직렬화 왕복 테스트.
3. 워크로드: `make soak-quick`(3m) 수동 실행으로 배선 검증 — 전 패밀리
   카운트가 0이 아니게 찍히는지 확인(커버리지 증거).

## 알려진 한계

- 아주 느린 누수(시간당 수 KB)는 1h 창 비교로 못 잡을 수 있음 — 실행 시간
  연장(-duration)으로 완화, 자동화는 하지 않음.
- 단일 프로세스라 다중 서버 인스턴스 간 상호작용(리더 경합 등)은 미커버 —
  cluster 스모크와 기존 통합 테스트의 영역.
