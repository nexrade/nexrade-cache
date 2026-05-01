#!/usr/bin/env python3
"""
07-pipeline/benchmark.py — Pipelining vs. sequential commands.

Pipelining batches multiple commands into one TCP round-trip,
dramatically reducing latency when you need to issue many operations.

Requires: pip install redis
"""

import time
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
N = 1000


# ── Without pipeline ──────────────────────────────────────────────────────────
def sequential_set(n: int) -> float:
    start = time.perf_counter()
    for i in range(n):
        r.set(f"seq:{i}", i)
    return time.perf_counter() - start


# ── With pipeline ─────────────────────────────────────────────────────────────
def pipelined_set(n: int) -> float:
    start = time.perf_counter()
    pipe = r.pipeline(transaction=False)
    for i in range(n):
        pipe.set(f"pipe:{i}", i)
    pipe.execute()
    return time.perf_counter() - start


# ── Chunked pipeline (large datasets) ────────────────────────────────────────
def chunked_pipeline(n: int, chunk_size: int = 200) -> float:
    """Split large batches into smaller chunks to avoid memory spikes."""
    start = time.perf_counter()
    for offset in range(0, n, chunk_size):
        pipe = r.pipeline(transaction=False)
        for i in range(offset, min(offset + chunk_size, n)):
            pipe.set(f"chunk:{i}", i)
        pipe.execute()
    return time.perf_counter() - start


print(f"Benchmarking {N} SET commands...\n")

t_seq  = sequential_set(N)
t_pipe = pipelined_set(N)
t_chk  = chunked_pipeline(N)

print(f"Sequential:        {t_seq:.3f}s  ({N/t_seq:,.0f} ops/s)")
print(f"Pipeline (single): {t_pipe:.3f}s  ({N/t_pipe:,.0f} ops/s)  {t_seq/t_pipe:.1f}x faster")
print(f"Pipeline (chunks): {t_chk:.3f}s  ({N/t_chk:,.0f} ops/s)  {t_seq/t_chk:.1f}x faster")

# ── Mixed pipeline (GET + SET) ────────────────────────────────────────────────
print("\n=== Mixed pipeline: SET 10 keys then GET them back ===")
pipe = r.pipeline()
for i in range(10):
    pipe.set(f"mixed:{i}", f"value:{i}")
for i in range(10):
    pipe.get(f"mixed:{i}")
results = pipe.execute()
sets = results[:10]    # first 10 = True/True/...
gets = results[10:]    # next 10 = the values
print(f"SET results: {sets}")
print(f"GET results: {gets}")
