#!/usr/bin/env python3
"""
12-cache-patterns/multi_az_replicas.py
Multi-AZ nexrade-cache topology with redis-py (sync + asyncio).

Topology assumed by this example:
   ┌─────────────────────┐
   │ primary             │   (zone A)
   │ nexrade-cache:6380  │
   └──────────┬──────────┘
              │  async PSYNC
   ┌──────────┴────────────────────┐
   ▼                              ▼
┌────────────────┐         ┌────────────────┐
│ replica 1      │         │ replica 2      │   (zones B + C)
│ nexrade-cache  │         │ nexrade-cache  │
│ :6380 + REPLICA│         │ :6380 + REPLICA│
└────────────────┘         └────────────────┘

The application is **always pointed at the primary**. Use the
`--write-then-read` block to verify a write + WAIT against all
connected replicas before returning to the caller.

Run:
  nexrade-cache --port 6380 --replicaof-zone B :6380  # on zone B
  nexrade-cache --port 6380 --replicaof-zone C :6380  # on zone C
  python3 examples/12-cache-patterns/multi_az_replicas.py
"""

import asyncio
import os
import sys

import redis
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError, TimeoutError
from redis.retry import Retry

PRIMARY = os.environ.get("NEXRADE_PRIMARY", "127.0.0.1:6380")
HOST, PORT = PRIMARY.split(":")

# Sync client (sync app). Pool + retry/backoff/HEALTH CHECK matches the
# production cache profile (see `connection_pool.py`).
def build_sync_client() -> redis.Redis:
    pool = redis.ConnectionPool(
        host=HOST,
        port=int(PORT),
        decode_responses=True,
        max_connections=64,
        socket_connect_timeout=2.0,
        socket_timeout=2.0,
        # Health check: redis-py sends PING on a connection that's been
        # idle for `health_check_interval` seconds — surfaces half-dead
        # sockets that the TCP keepalive might miss.
        health_check_interval=30,
        retry=Retry(ExponentialBackoff(cap=1.0, base=0.05), retries=3),
        retry_on_error=[ConnectionError, TimeoutError],
    )
    return redis.Redis(connection_pool=pool)


# Async client (asyncio app). Same retry shape; `redis.asyncio.retry.Retry`
# accepts the same `ExponentialBackoff`.
def build_async_client() -> AsyncRedis:
    return AsyncRedis(
        host=HOST,
        port=int(PORT),
        decode_responses=True,
        max_connections=64,
        socket_connect_timeout=2.0,
        socket_timeout=2.0,
        health_check_interval=30,
        retry=AsyncRetry(ExponentialBackoff(cap=1.0, base=0.05), retries=3),
        retry_on_error=[ConnectionError, TimeoutError],
    )


# Sync: write + WAIT 2 (both replicas). The app blocks until the write
# has been acknowledged by 2 replicas (zone B + zone C) or the timeout
# elapses — see `docs/backup-restore.md` for the WAIT semantics.
def sync_write_then_wait(r: redis.Redis, key: str, value: str) -> int:
    pipe = r.pipeline(transaction=False)
    pipe.set(key, value)
    pipe.execute_command("WAIT", 2, 1000)  # 2 replicas, 1 s timeout
    _, acked = pipe.execute()
    return int(acked)


# Async equivalent — same contract.
async def async_write_then_wait(r: AsyncRedis, key: str, value: str) -> int:
    pipe = r.pipeline(transaction=False)
    pipe.set(key, value)
    await pipe.execute_command("WAIT", 2, 1000)
    _, acked = await pipe.execute()
    return int(acked)


def main_sync() -> int:
    r = build_sync_client()
    assert r.ping(), "primary did not respond to PING"
    n_acked = sync_write_then_wait(r, "multi-az:hello", "sync")
    print(f"sync write acknowledged by {n_acked}/2 replicas")
    assert n_acked == 2, "WAIT must be 2 when both zones are healthy"
    r.delete("multi-az:hello")
    return 0


async def main_async() -> int:
    r = build_async_client()
    assert await r.ping(), "primary did not respond to PING"
    n_acked = await async_write_then_wait(r, "multi-az:hello", "async")
    print(f"async write acknowledged by {n_acked}/2 replicas")
    assert n_acked == 2, "WAIT must be 2 when both zones are healthy"
    await r.delete("multi-az:hello")
    await r.aclose()
    return 0


if __name__ == "__main__":
    rc = main_sync()
    rc |= asyncio.run(main_async())
    sys.exit(rc)
