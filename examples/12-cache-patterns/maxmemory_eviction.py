#!/usr/bin/env python3
"""
12-cache-patterns/maxmemory_eviction.py
Demonstrates `maxmemory` + `allkeys-lru` eviction working end-to-end —
the core promise of the cache profile (nexrade.cache.toml).

This script sets a small maxmemory cap at runtime (CONFIG SET, so it does
not require restarting with a special config), fills the keyspace past
the cap, and confirms the server evicted keys to stay under it instead of
growing unbounded or refusing writes.

Run:
  nexrade-cache --port 6399 --metrics-port 0 &
  pip install redis
  python3 examples/12-cache-patterns/maxmemory_eviction.py --port 6399

Note: this script mutates maxmemory/maxmemory-policy on the target server
via CONFIG SET — point it at a throwaway/dev instance, not production.
"""

import argparse
import redis


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    args = ap.parse_args()

    r = redis.Redis(host=args.host, port=args.port, decode_responses=True)
    assert r.ping(), "server did not respond to PING"

    r.flushall()
    prev_maxmemory = r.config_get("maxmemory").get("maxmemory", "0")
    prev_policy = r.config_get("maxmemory-policy").get("maxmemory-policy", "noeviction")

    try:
        cap_bytes = 256 * 1024  # 256 KiB — deliberately tiny for a fast demo
        r.config_set("maxmemory", cap_bytes)
        r.config_set("maxmemory-policy", "allkeys-lru")

        value = "x" * 512  # ~512 B payload per key
        n = 2000  # enough to blow well past the 256 KiB cap if unevicted
        for i in range(n):
            r.set(f"evict:demo:{i}", value)

        info = r.info("memory")
        used = info.get("used_memory", 0)
        dbsize = r.dbsize()

        print(f"wrote {n} keys (~{n * (len(value) + 24)} B raw)")
        print(f"used_memory={used} B (cap={cap_bytes} B)")
        print(f"dbsize={dbsize} (would be {n} without eviction)")

        assert used <= cap_bytes * 1.1, (
            f"used_memory {used} exceeds cap {cap_bytes} by more than 10% — "
            "eviction did not keep up"
        )
        assert dbsize < n, "expected eviction to have removed some keys"
        print("eviction kept live_bytes under the cap — cache profile confirmed")
    finally:
        # Restore whatever the server had before this demo touched it.
        r.config_set("maxmemory", prev_maxmemory)
        r.config_set("maxmemory-policy", prev_policy)
        r.flushall()


if __name__ == "__main__":
    main()
