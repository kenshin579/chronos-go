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
- `chronos_task_duration_seconds{queue,kind}` — 핸들러 처리시간 히스토그램
- `chronos_tasks_recovered_total{queue,outcome}` — 크래시 워커에서 회수한 태스크 카운터
- `chronos_queue_tasks{queue,state}` — 큐 적재량 게이지(pending/active/scheduled/retry/archived/completed)
- `chronos_queue_paused{queue}` — 큐 일시정지 여부
- `chronos_collector_up` — 마지막 스크레이프가 큐 상태를 읽는 데 성공했는지
- `chronos_schedule_last_fired_timestamp_seconds{id,kind,queue}` — 스케줄이 마지막으로 발화한 unix 시각
- `chronos_schedule_stale` — 스케줄 레지스트리 항목 중 하나라도 stale인지
- `chronos_scheduler_leader{leader_id}` — 현재 스케줄러 리더

전체 설명은 [상위 README](../README.md) 참고.

## 코드에서 쓰는 법

```go
reg := prometheus.NewRegistry()
insp := chronos.NewInspector(rdb)

// NewMetrics는 레지스트리를 받지 않고 스스로 등록하지도 않는다. 등록은 호출자 몫.
metrics := chronosprom.NewMetrics()
reg.MustRegister(metrics)
reg.MustRegister(chronosprom.NewQueueCollector(insp))
reg.MustRegister(chronosprom.NewSchedulerCollector(insp))

srv := chronos.NewServer(rdb, chronos.ServerConfig{Metrics: metrics, ...})
http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

`MustRegister`는 중복 등록 시 패닉한다. 데모라서 그대로 쓰지만, 라이브러리·서비스
코드에서는 `reg.Register` 오류를 로그로 남기고 계속 진행하는 편이 낫다.
