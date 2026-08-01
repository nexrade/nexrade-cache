#!/usr/bin/env python3
"""
12-cache-patterns/connection_pool.py
Production connection-pool setup for nexrade-cache as a cache (redis-py).

Covers:
  ConnectionPool with health checks + retry
  CLIENT SETINFO (redis-py sends this automatically on connect)
  A quick round-trip to confirm the pool is live

Run against the cache profile:
  nexrade-cache --config nexrade.cache.toml --metrics-port 0 &
  pip install redis
  python3 examples/12-cache-patterns/connection_pool.py
"""

import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.exceptions import ConnectionError, TimeoutError

HOST = "127.0.0.1"
PORT = 6379

# A pool, not a bare Redis() per request — the pool caps concurrent
# connections and reuses them, which matters once QPS is high enough that
# TCP handshake overhead would otherwise show up in p99 latency.
pool = redis.ConnectionPool(
    host=HOST,
    port=PORT,
    decode_responses=True,
    max_connections=50,
    socket_connect_timeout=2.0,
    socket_timeout=2.0,
    # Detect a half-dead connection before handing it back out.
    health_check_interval=30,
    retry=Retry(ExponentialBackoff(cap=1.0, base=0.05), retries=3),
    retry_on_error=[ConnectionError, TimeoutError],
)

r = redis.Redis(connection_pool=pool)

assert r.ping(), "server did not respond to PING"
print(f"connected via pool (max_connections={pool.max_connections})")

# redis-py sends CLIENT SETINFO LIB-NAME/LIB-VER automatically on connect
# (0.5.3+ on the server side stores + surfaces these in CLIENT LIST).
info = r.client_info()
blob = info if isinstance(info, str) else " ".join(f"{k}={v}" for k, v in info.items())
print(f"client info: {blob[:160]}...")

r.set("pool:smoke", "ok", ex=30)
assert r.get("pool:smoke") == "ok"
print("round-trip ok")

pool.disconnect()
