#!/usr/bin/env python3
"""
11-python-patterns/patterns.py
Common redis-py patterns tested against nexrade-cache.

Covers:
  scan()        cursor-based key iteration
  scan_iter()   Pythonic iterator wrapper over SCAN
  keys()        glob pattern matching
  pipeline()    batched commands + delete
  ttl()         time-to-live
  hset()        write a hash field
  hgetall()     read all hash fields
  hdel()        delete hash fields
  delete()      remove a key

Run:
  pip install redis orjson
  python3 examples/11-python-patterns/patterns.py
"""

import time
import json
import redis

HOST = "127.0.0.1"
PORT = 6379

r = redis.Redis(host=HOST, port=PORT, decode_responses=True)

# ── sanity check ──────────────────────────────────────────────────────────────
assert r.ping(), "server did not respond to PING"
print("PING → PONG\n")

DIVIDER = lambda title: print(f"\n── {title} " + "─" * (50 - len(title)))


# ── seed helper ───────────────────────────────────────────────────────────────
def seed(prefix: str, n: int = 15) -> list[str]:
    pipe = r.pipeline(transaction=False)
    keys = [f"{prefix}:{i}" for i in range(n)]
    for k in keys:
        pipe.set(k, f"val:{k}")
    pipe.execute()
    return keys


# ═══════════════════════════════════════════════════════════════════
# 1. SCAN  — cursor-based iteration
# ═══════════════════════════════════════════════════════════════════
DIVIDER("1. scan()")

seed("demo:scan")
pattern = "demo:scan:*"
cursor = 0
found = []

while True:
    cursor, batch = r.scan(cursor, match=pattern, count=100)
    found.extend(batch)
    if cursor == 0:
        break

print(f"  scan matched {len(found)} keys  (first 3: {sorted(found)[:3]})")
assert len(found) == 15


# ═══════════════════════════════════════════════════════════════════
# 2. SCAN_ITER  — Pythonic iterator
# ═══════════════════════════════════════════════════════════════════
DIVIDER("2. scan_iter()")

iter_keys = sorted(r.scan_iter("demo:scan:*"))
print(f"  scan_iter matched {len(iter_keys)} keys  (first 3: {iter_keys[:3]})")
assert len(iter_keys) == 15


# ═══════════════════════════════════════════════════════════════════
# 3. KEYS  — glob matching
# ═══════════════════════════════════════════════════════════════════
DIVIDER("3. keys()")

r.set("product:laptop", "1299")
r.set("product:phone",  "799")
r.set("product:tablet", "599")

all_keys     = r.keys("*")
product_keys = sorted(r.keys("product:*"))

print(f"  keys('*')         → {len(all_keys)} total keys")
print(f"  keys('product:*') → {product_keys}")
assert len(product_keys) == 3


# ═══════════════════════════════════════════════════════════════════
# 4. PIPELINE + DELETE  — batched removal
# ═══════════════════════════════════════════════════════════════════
DIVIDER("4. pipeline() + delete()")

keys_to_del = r.keys("demo:scan:*")
print(f"  before: {len(keys_to_del)} demo:scan keys")

pipeline = r.pipeline()
for key in keys_to_del:
    pipeline.delete(key)
results = pipeline.execute()

deleted   = sum(results)
remaining = len(r.keys("demo:scan:*"))
print(f"  deleted {deleted} keys via pipeline, {remaining} remaining")
assert deleted == 15
assert remaining == 0


# ═══════════════════════════════════════════════════════════════════
# 5. TTL  — time-to-live
# ═══════════════════════════════════════════════════════════════════
DIVIDER("5. ttl()")

r.set("session:tok", "user42", ex=60)
r.set("cache:res",   "data",   ex=300)
r.set("perm:key",    "forever")

cases = {
    "session:tok": ("~60",  lambda t: 55 <= t <= 60),
    "cache:res":   ("~300", lambda t: 295 <= t <= 300),
    "perm:key":    ("-1",   lambda t: t == -1),
    "no:such:key": ("-2",   lambda t: t == -2),
}
for key, (label, check) in cases.items():
    ttl = r.ttl(key)
    status = "✓" if check(ttl) else "✗"
    print(f"  {status} ttl({key!r:18}) = {ttl:>4}  (expected {label})")
    assert check(ttl), f"TTL check failed for {key}: got {ttl}"


# ═══════════════════════════════════════════════════════════════════
# 6. HSET  — write hash fields (time-series pattern)
# ═══════════════════════════════════════════════════════════════════
DIVIDER("6. hset()")

hash_key = "sensor:readings"
base_ts  = int(time.time() * 1000)

readings = [
    {"temp": 21.3, "humidity": 55},
    {"temp": 21.7, "humidity": 54},
    {"temp": 22.1, "humidity": 53},
    {"temp": 21.9, "humidity": 56},
    {"temp": 22.5, "humidity": 52},
]

for i, data in enumerate(readings):
    timestamp = str(base_ts + i * 1000)
    r.hset(hash_key, timestamp, json.dumps(data))

print(f"  wrote {len(readings)} readings to '{hash_key}'")


# ═══════════════════════════════════════════════════════════════════
# 7. HGETALL  — read all hash fields
# ═══════════════════════════════════════════════════════════════════
DIVIDER("7. hgetall()")

raw = r.hgetall(hash_key)
print(f"  hgetall returned {len(raw)} fields")
for ts, payload in sorted(raw.items())[:3]:
    decoded = json.loads(payload)
    print(f"    ts={ts}  →  {decoded}")

assert len(raw) == len(readings)


# ═══════════════════════════════════════════════════════════════════
# 8. HDEL  — prune oldest hash entries
# ═══════════════════════════════════════════════════════════════════
DIVIDER("8. hdel()")

MAX_KEEP = 3
timestamps = sorted(r.hgetall(hash_key).keys())
old = timestamps[: len(timestamps) - MAX_KEEP]

print(f"  total={len(timestamps)}, keeping newest {MAX_KEEP}, pruning {len(old)}")
if old:
    r.hdel(hash_key, *old)

after = r.hgetall(hash_key)
print(f"  after prune: {len(after)} entries remain")
assert len(after) == MAX_KEEP


# ═══════════════════════════════════════════════════════════════════
# 9. DELETE  — remove a whole key
# ═══════════════════════════════════════════════════════════════════
DIVIDER("9. delete()")

assert r.exists(hash_key) == 1
r.delete(hash_key)
assert r.exists(hash_key) == 0
print(f"  '{hash_key}' deleted — exists() → 0  ✓")


# ═══════════════════════════════════════════════════════════════════
# 10. Combined: scan_iter + pipeline bulk-delete (production pattern)
# ═══════════════════════════════════════════════════════════════════
DIVIDER("10. scan_iter() + pipeline bulk-delete")

seed("tmp:gc", n=40)
print(f"  seeded 40 tmp:gc keys")

BATCH  = 10
total  = 0
batch: list[str] = []

for key in r.scan_iter("tmp:gc:*", count=20):
    batch.append(key)
    if len(batch) >= BATCH:
        pipe = r.pipeline()
        for k in batch:
            pipe.delete(k)
        total += sum(pipe.execute())
        batch.clear()

if batch:                          # flush remainder
    pipe = r.pipeline()
    for k in batch:
        pipe.delete(k)
    total += sum(pipe.execute())

left = len(r.keys("tmp:gc:*"))
print(f"  deleted {total} keys in batches of {BATCH}, {left} remaining")
assert total == 40
assert left  == 0


# ── cleanup ───────────────────────────────────────────────────────────────────
for pat in ("product:*", "session:*", "cache:*", "perm:*"):
    for k in r.scan_iter(pat):
        r.delete(k)

print("\n✓  All 10 pattern tests passed.\n")
