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
