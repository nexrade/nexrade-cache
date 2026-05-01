#!/usr/bin/env python3
"""
04-transactions/atomic-ops.py — MULTI/EXEC and optimistic locking with WATCH.

Demonstrates:
  1. Atomic balance transfer using MULTI/EXEC
  2. Optimistic locking with WATCH to detect concurrent modification
  3. Retry loop pattern for watch-based CAS (compare-and-swap)

Requires: pip install redis
"""

import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)


# ── 1. Atomic transfer ────────────────────────────────────────────────────────
def transfer(from_account: str, to_account: str, amount: int) -> bool:
    """Atomically move `amount` from one account to another."""
    pipe = r.pipeline(transaction=True)
    pipe.multi()
    pipe.decrby(from_account, amount)
    pipe.incrby(to_account, amount)
    pipe.execute()
    return True


r.set("account:alice", 1000)
r.set("account:bob", 500)

print("=== Atomic transfer ===")
print(f"Before — Alice: {r.get('account:alice')}, Bob: {r.get('account:bob')}")
transfer("account:alice", "account:bob", 200)
print(f"After  — Alice: {r.get('account:alice')}, Bob: {r.get('account:bob')}")


# ── 2. Optimistic locking with WATCH + retry ──────────────────────────────────
def safe_increment(key: str, max_retries: int = 5) -> int:
    """
    Increment a counter using WATCH for optimistic concurrency.
    Retries if another client modifies the key before EXEC.
    """
    for attempt in range(max_retries):
        with r.pipeline() as pipe:
            try:
                pipe.watch(key)
                current = int(pipe.get(key) or 0)
                pipe.multi()
                pipe.set(key, current + 1)
                pipe.execute()
                return current + 1
            except redis.WatchError:
                print(f"  [watch conflict on attempt {attempt + 1}, retrying...]")
                continue
    raise RuntimeError(f"Failed after {max_retries} retries")


print("\n=== Optimistic locking (WATCH + retry) ===")
r.set("safe_counter", 0)
new_val = safe_increment("safe_counter")
print(f"Counter incremented to: {new_val}")


# ── 3. Batch pipeline (not transactional, but fast) ───────────────────────────
print("\n=== Non-transactional pipeline (maximum throughput) ===")
pipe = r.pipeline(transaction=False)
for i in range(10):
    pipe.set(f"bulk:{i}", i * i)
pipe.execute()
print(f"Set 10 keys in one round-trip: {[r.get(f'bulk:{i}') for i in range(5)]} ...")
