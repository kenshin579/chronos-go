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
