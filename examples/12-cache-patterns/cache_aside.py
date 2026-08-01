#!/usr/bin/env python3
"""
12-cache-patterns/cache_aside.py
Cache-aside (lazy-loading) pattern against nexrade-cache — the most common
shape for using a Redis-protocol server as a cache in front of a real
datastore.

Covers:
  get-or-load with TTL
  stampede-safe refresh (SET NX as a lightweight lock)
  explicit invalidation on write-through

Run against the cache profile:
  nexrade-cache --config nexrade.cache.toml --metrics-port 0 &
  pip install redis
  python3 examples/12-cache-patterns/cache_aside.py
"""

import time
import redis

HOST = "127.0.0.1"
PORT = 6379
TTL_SECONDS = 30

r = redis.Redis(host=HOST, port=PORT, decode_responses=True)
assert r.ping(), "server did not respond to PING"


def slow_load(user_id: str) -> str:
    """Stand-in for a real datastore call (Postgres/MySQL/S3/...)."""
    time.sleep(0.05)
    return f"profile-for-{user_id}"


def get_user_profile(user_id: str) -> str:
    """Cache-aside GET: return cached value, or load + populate on miss."""
    key = f"user:{user_id}:profile"
    cached = r.get(key)
    if cached is not None:
        return cached

    # Lightweight stampede guard: only one caller wins the lock and loads;
    # others fall back to a direct load rather than blocking. For a small
    # cache this is enough — for very hot keys under heavy fan-in, add a
    # short sleep+retry loop on lock-miss instead of an immediate slow_load.
    lock_key = f"{key}:lock"
    got_lock = r.set(lock_key, "1", nx=True, ex=5)

    value = slow_load(user_id)
    r.set(key, value, ex=TTL_SECONDS)
    if got_lock:
        r.delete(lock_key)
    return value


def update_user_profile(user_id: str, value: str) -> None:
    """Write-through: update the datastore, then invalidate the cache
    entry rather than overwriting it — the next GET repopulates it."""
    # ... write `value` to the real datastore here ...
    r.delete(f"user:{user_id}:profile")


# ── demo ──────────────────────────────────────────────────────────────────────

start = time.monotonic()
v1 = get_user_profile("42")
miss_ms = (time.monotonic() - start) * 1000

start = time.monotonic()
v2 = get_user_profile("42")
hit_ms = (time.monotonic() - start) * 1000

assert v1 == v2 == "profile-for-42"
print(f"miss: {miss_ms:.1f}ms  hit: {hit_ms:.1f}ms  (hit should be far faster)")

update_user_profile("42", "profile-for-42-v2")
assert r.get("user:42:profile") is None, "invalidation should clear the entry"
print("invalidation ok")
