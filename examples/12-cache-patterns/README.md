# 12-cache-patterns — production recipes

Runnable Python + nexrade-cache examples for the most common cache
workloads. All scripts are self-contained; point `NEXRADE_HOST` /
`NEXRADE_PORT` (or just the defaults `127.0.0.1:6379`) at a running
server and run.

## Scripts

| File                         | Pattern                                                                                    |
|------------------------------|---------------------------------------------------------------------------------------------|
| `connection_pool.py`         | `ConnectionPool` + retry / backoff / health check, `CLIENT SETINFO` smoke                   |
| `cache_aside.py`             | Get-or-load with TTL, stampede-safe refresh, invalidation on write                         |
| `maxmemory_eviction.py`      | Live proof of `allkeys-lru` keeping `used_memory` under cap                                  |
| `multi_az_replicas.py`       | Async + sync `redis-py` against a primary + N replicas with `WAIT` for write durability (0.8.3) |

## Run

```bash
# Default config (the production cache profile is at /etc/nexrade/nexrade.cache.toml,
# the standalone equivalent is at the repo root as nexrade.example.toml):
nexrade-cache --config nexrade.cache.toml --metrics-port 0 &

# Pick a script:
pip install redis
python3 connection_pool.py
python3 cache_aside.py
python3 maxmemory_eviction.py

# Multi-AZ example (needs primary + ≥2 replicas running):
NEXRADE_PRIMARY=10.0.0.10:6380 python3 multi_az_replicas.py
```

## See also

- `docs/backup-restore.md` — snapshot / AOF / corruption recovery
- `docs/multi-az-deploy.md` — primary + replicas across AZs
- `docs/redis-sentinel-compat.md` — Sentinel is explicitly unsupported
- `docs/dump-restore.md` — `DUMP` / `RESTORE` (`NEXD` payload)
