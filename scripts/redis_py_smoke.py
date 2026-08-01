#!/usr/bin/env python3
"""redis-py smoke suite against a running nexrade-cache (0.5.2+; expanded 0.6.4).

Covers the cache + application path redis-py / redis.asyncio users hit:
  PING, INFO, CLIENT SETINFO, CONFIG GET, MEMORY, strings, pipeline,
  hashes/lists/sets/zsets, SCAN family, TTL, MULTI/WATCH, pub/sub +
  pattern subscribe, streams + consumer groups, Lua, DUMP/RESTORE (NEXD),
  TIME/ROLE, SLOWLOG, keyspace-notify config, UNLINK/TYPE/RENAME.

Known-unsupported surfaces are recorded as SKIP with a reason (not FAIL):
  CLUSTER routing, Sentinel, WAIT replica ack, Redis-RDB DUMP interchange.

Exits non-zero only when a PASS-expected check fails.

Usage:
  nexrade-cache --port 6399 --metrics false &
  python3 scripts/redis_py_smoke.py --port 6399

Env:
  NEXRADE_HOST (default 127.0.0.1)
  NEXRADE_PORT (default 6379)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
import uuid

import redis

try:
    import redis.asyncio as aioredis
except ImportError:  # pragma: no cover
    aioredis = None


PASS = 0
FAIL = 0
SKIP = 0
RESULTS: list[tuple[str, str, str]] = []


def record(status: str, label: str, note: str = "") -> None:
    global PASS, FAIL, SKIP
    if status == "PASS":
        PASS += 1
    elif status == "SKIP":
        SKIP += 1
    else:
        FAIL += 1
    RESULTS.append((status, label, note))


def run(label: str, fn) -> None:
    try:
        note = fn() or ""
        record("PASS", label, str(note))
    except Exception as e:  # noqa: BLE001 — surface any client/server error
        record("FAIL", label, f"{type(e).__name__}: {e}")


def skip(label: str, reason: str) -> None:
    record("SKIP", label, reason)


def prefix() -> str:
    return f"smoke:{uuid.uuid4().hex[:10]}"


def make_client(host: str, port: int, decode: bool = True) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        decode_responses=decode,
        socket_connect_timeout=2.0,
        socket_timeout=5.0,
    )


def wait_ready(host: str, port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            c = make_client(host, port)
            if c.ping():
                c.close()
                return
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(0.1)
    raise SystemExit(f"server not ready at {host}:{port}: {last}")


# ── Sync checks ───────────────────────────────────────────────────────────────


def check_ping(r: redis.Redis) -> str:
    assert r.ping() is True
    return "PONG"


def check_info(r: redis.Redis) -> str:
    info = r.info("server")
    for required in (
        "redis_version",
        "tcp_port",
        "uptime_in_seconds",
        "process_id",
        "redis_mode",
    ):
        assert required in info, f"missing INFO server field {required}: {sorted(info)}"
    keys = sorted(info.keys())[:10]
    return f"redis_mode={info.get('redis_mode')} process_id={info.get('process_id')} keys={keys}"


def check_client_setinfo(r: redis.Redis) -> str:
    assert r.client_setinfo("LIB-NAME", "redis-py-smoke") is True
    assert r.client_setinfo("LIB-VER", "8.0.1") is True
    info_line = r.client_info()
    if isinstance(info_line, dict):
        blob = " ".join(f"{k}={v}" for k, v in info_line.items())
    else:
        blob = str(info_line)
    assert "redis-py-smoke" in blob, blob
    assert "8.0.1" in blob, blob
    return "setinfo ok"


def check_client_list(r: redis.Redis) -> str:
    listing = r.client_list()
    assert isinstance(listing, list) and len(listing) >= 1, listing
    return f"clients={len(listing)}"


def check_config_get(r: redis.Redis) -> str:
    mm = r.config_get("maxmemory")
    assert "maxmemory" in mm, mm
    hz = r.config_get("hz")
    assert "hz" in hz, hz
    star = r.config_get("*")
    assert isinstance(star, dict) and len(star) >= 5, star
    return f"maxmemory={mm.get('maxmemory')} keys={len(star)}"


def check_notify_config(r: redis.Redis) -> str:
    # Default empty; set a subset and read back.
    before = r.config_get("notify-keyspace-events")
    assert "notify-keyspace-events" in before, before
    assert r.config_set("notify-keyspace-events", "KE$") is True
    after = r.config_get("notify-keyspace-events")
    val = after.get("notify-keyspace-events", "")
    assert "K" in val and "E" in val and "$" in val, after
    # Restore empty so later tests aren't noisy.
    assert r.config_set("notify-keyspace-events", "") is True
    return f"flags={val}"


def check_memory(r: redis.Redis) -> str:
    p = prefix()
    k = f"{p}:mem"
    r.set(k, "hello-memory")
    usage = r.memory_usage(k)
    assert usage is not None and usage > 0, usage
    stats = r.memory_stats()
    assert isinstance(stats, dict) and len(stats) >= 1, stats
    return f"usage={usage} stats_keys={len(stats)}"


def check_echo_select(r: redis.Redis) -> str:
    assert r.echo("hello") == "hello"
    r.execute_command("SELECT", 0)
    return "echo+select"


def check_time_role(r: redis.Redis) -> str:
    t = r.time()
    # redis-py returns (seconds, microseconds) as ints or a list
    if isinstance(t, (list, tuple)):
        assert len(t) == 2, t
        secs = int(t[0])
    else:
        secs = int(t)
    assert secs > 1_600_000_000, secs  # after 2020
    role = r.role()
    # ROLE is a nested array; redis-py may return list starting with b'master'/'master'
    assert role is not None
    head = role[0] if isinstance(role, (list, tuple)) else role
    if isinstance(head, bytes):
        head = head.decode()
    assert str(head).lower() in ("master", "slave", "replica"), role
    return f"time={secs} role={head}"


def check_strings(r: redis.Redis) -> str:
    p = prefix()
    k = f"{p}:str"
    assert r.set(k, "v1") is True
    assert r.get(k) == "v1"
    assert r.set(k, "v2", nx=True) is None or r.set(k, "v2", nx=True) is False
    assert r.set(k, "v2", xx=True) is True
    assert r.getset(k, "v3") == "v2"
    assert r.append(k, "!") == 3
    assert r.strlen(k) == 3
    assert r.mset({f"{p}:a": "1", f"{p}:b": "2"}) is True
    assert r.mget(f"{p}:a", f"{p}:b") == ["1", "2"]
    assert r.incr(f"{p}:n") == 1
    assert r.incrby(f"{p}:n", 4) == 5
    return "strings ok"


def check_pipeline(r: redis.Redis) -> str:
    p = prefix()
    pipe = r.pipeline()
    pipe.set(f"{p}:p1", "x")
    pipe.get(f"{p}:p1")
    pipe.incr(f"{p}:p2")
    out = pipe.execute()
    assert out[0] is True
    assert out[1] == "x"
    assert out[2] == 1
    return f"pipeline={out}"


def check_hash_list_set_zset(r: redis.Redis) -> str:
    p = prefix()
    assert r.hset(f"{p}:h", mapping={"f1": "a", "f2": "b"}) == 2
    assert r.hget(f"{p}:h", "f1") == "a"
    assert r.hgetall(f"{p}:h") == {"f1": "a", "f2": "b"}
    assert r.rpush(f"{p}:l", "x", "y", "z") == 3
    assert r.lrange(f"{p}:l", 0, -1) == ["x", "y", "z"]
    assert r.lpop(f"{p}:l") == "x"
    assert r.sadd(f"{p}:s", "a", "b", "a") == 2
    assert r.smembers(f"{p}:s") == {"a", "b"}
    assert r.zadd(f"{p}:z", {"m1": 1.0, "m2": 2.0}) == 2
    assert r.zrange(f"{p}:z", 0, -1, withscores=True) == [("m1", 1.0), ("m2", 2.0)]
    return "collections ok"


def check_scan_family(r: redis.Redis) -> str:
    p = prefix()
    # Seed keys for SCAN.
    for i in range(5):
        r.set(f"{p}:scan:{i}", str(i))
    r.hset(f"{p}:hscan", mapping={"a": "1", "b": "2", "c": "3"})
    r.sadd(f"{p}:sscan", "x", "y", "z")
    r.zadd(f"{p}:zscan", {"m1": 1, "m2": 2, "m3": 3})

    found = list(r.scan_iter(match=f"{p}:scan:*", count=10))
    assert len(found) == 5, found

    h = dict(r.hscan_iter(f"{p}:hscan", count=10))
    assert set(h.keys()) == {"a", "b", "c"}, h

    s = set(r.sscan_iter(f"{p}:sscan", count=10))
    assert s == {"x", "y", "z"}, s

    z = list(r.zscan_iter(f"{p}:zscan", count=10))
    members = {m for m, _ in z}
    assert members == {"m1", "m2", "m3"}, z
    return f"scan={len(found)} hscan={len(h)} sscan={len(s)} zscan={len(z)}"


def check_ttl(r: redis.Redis) -> str:
    p = prefix()
    k = f"{p}:ttl"
    r.set(k, "v", ex=60)
    t = r.ttl(k)
    assert 0 < t <= 60, t
    r.persist(k)
    assert r.ttl(k) == -1
    return f"ttl_before_persist_was={t}"


def check_type_rename_unlink(r: redis.Redis) -> str:
    p = prefix()
    k = f"{p}:ty"
    r.set(k, "v")
    assert r.type(k) == "string"
    r.rename(k, f"{p}:ty2")
    assert r.exists(f"{p}:ty2") == 1
    assert r.exists(k) == 0
    assert r.unlink(f"{p}:ty2") == 1
    return "type+rename+unlink ok"


def check_multi_watch(r: redis.Redis) -> str:
    p = prefix()
    k = f"{p}:w"
    r.set(k, "0")
    pipe = r.pipeline()
    pipe.multi()
    pipe.incr(k)
    pipe.incr(k)
    out = pipe.execute()
    assert out == [1, 2], out
    r.watch(k)
    pipe = r.pipeline()
    pipe.multi()
    pipe.set(k, "10")
    out = pipe.execute()
    assert out == [True], out
    return "multi+watch ok"


def check_pubsub(r: redis.Redis) -> str:
    p = prefix()
    ch = f"{p}:ch"
    host = r.connection_pool.connection_kwargs.get("host", "127.0.0.1")
    port = int(r.connection_pool.connection_kwargs.get("port", 6379))
    sub_client = make_client(host, port)
    ps = sub_client.pubsub(ignore_subscribe_messages=True)
    try:
        ps.subscribe(ch)
        time.sleep(0.05)
        n = r.publish(ch, "hi")
        assert n >= 1, n
        msg = None
        for _ in range(20):
            msg = ps.get_message(timeout=0.2)
            if msg and msg.get("type") == "message":
                break
        assert msg is not None, "no pubsub message"
        assert msg.get("data") == "hi", msg
        ps.unsubscribe(ch)
        return f"subscribers={n}"
    finally:
        ps.close()
        sub_client.close()


def check_psubscribe(r: redis.Redis) -> str:
    p = prefix()
    ch = f"{p}:news.sports"
    pat = f"{p}:news.*"
    host = r.connection_pool.connection_kwargs.get("host", "127.0.0.1")
    port = int(r.connection_pool.connection_kwargs.get("port", 6379))
    sub_client = make_client(host, port)
    ps = sub_client.pubsub(ignore_subscribe_messages=True)
    try:
        ps.psubscribe(pat)
        time.sleep(0.05)
        n = r.publish(ch, "goal")
        # Pattern subscribers count toward publish receivers in Redis;
        # nexrade may report 0 for pattern-only (literal count). Either
        # way the pmessage must arrive.
        msg = None
        for _ in range(30):
            msg = ps.get_message(timeout=0.2)
            if msg and msg.get("type") == "pmessage":
                break
        assert msg is not None, "no pmessage"
        assert msg.get("data") == "goal", msg
        assert msg.get("channel") == ch or msg.get("channel") == ch.encode(), msg
        ps.punsubscribe(pat)
        return f"publish_n={n} pmessage ok"
    finally:
        ps.close()
        sub_client.close()


def check_streams(r: redis.Redis) -> str:
    p = prefix()
    s = f"{p}:stream"
    mid = r.xadd(s, {"f": "v1"})
    assert mid
    entries = r.xrange(s, min="-", max="+")
    assert len(entries) == 1
    assert entries[0][1]["f"] == "v1"
    assert r.xlen(s) == 1
    return f"id={mid}"


def check_stream_groups(r: redis.Redis) -> str:
    p = prefix()
    s = f"{p}:cg"
    r.xadd(s, {"job": "1"}, id="1-0")
    r.xadd(s, {"job": "2"}, id="2-0")
    assert r.xgroup_create(s, "workers", id="0", mkstream=True) is True
    # Deliver one entry.
    msgs = r.xreadgroup("workers", "c1", {s: ">"}, count=1)
    assert msgs, msgs
    # msgs shape: [[stream, [(id, fields), ...]]]
    stream_entries = msgs[0][1]
    assert len(stream_entries) == 1
    eid = stream_entries[0][0]
    acked = r.xack(s, "workers", eid)
    assert acked == 1, acked
    # Pending should be empty for c1 after ack (range form).
    pending = r.xpending_range(s, "workers", min="-", max="+", count=10, consumername="c1")
    assert pending == [] or len(pending) == 0, pending
    return f"acked={eid}"


def check_lua(r: redis.Redis) -> str:
    p = prefix()
    k = f"{p}:lua"
    out = r.eval("return redis.call('SET', KEYS[1], ARGV[1])", 1, k, "from-lua")
    assert out is True or out == "OK" or out == b"OK"
    assert r.get(k) == "from-lua"
    sha = r.script_load("return redis.call('GET', KEYS[1])")
    assert r.evalsha(sha, 1, k) == "from-lua"
    return "lua ok"


def check_dump_restore(r: redis.Redis) -> str:
    """nexrade-native NEXD DUMP/RESTORE (not Redis RDB interchange).

    Uses a binary (decode_responses=False) client: NEXD is opaque bytes and
    must not pass through UTF-8 decode/encode.
    """
    host = r.connection_pool.connection_kwargs.get("host", "127.0.0.1")
    port = int(r.connection_pool.connection_kwargs.get("port", 6379))
    rb = make_client(host, port, decode=False)
    try:
        p = prefix().encode()
        src = p + b":dump_src"
        dst = p + b":dump_dst"
        rb.set(src, b"payload")
        blob = rb.dump(src)
        assert blob is not None and isinstance(blob, (bytes, bytearray)), type(blob)
        assert blob[:4] == b"NEXD", blob[:8]
        # redis-py may return True or b'OK' depending on version/protocol.
        out = rb.restore(dst, 0, blob)
        assert out in (True, b"OK", "OK"), out
        assert rb.get(dst) == b"payload"
        rb.set(dst, b"old")
        out = rb.restore(dst, 0, blob, replace=True)
        assert out in (True, b"OK", "OK"), out
        assert rb.get(dst) == b"payload"
        return f"nexd_len={len(blob)}"
    finally:
        rb.close()


def check_slowlog(r: redis.Redis) -> str:
    # Empty or list — both fine; just must not error.
    entries = r.slowlog_get(10)
    assert isinstance(entries, list), entries
    length = r.slowlog_len()
    assert isinstance(length, int) and length >= 0, length
    return f"entries={len(entries)} len={length}"


def check_delete_exists(r: redis.Redis) -> str:
    p = prefix()
    k = f"{p}:del"
    r.set(k, "1")
    assert r.exists(k) == 1
    assert r.delete(k) == 1
    assert r.exists(k) == 0
    return "del ok"


def run_skips() -> None:
    """Document known gaps so CI reports them honestly without failing."""
    skip(
        "skip:CLUSTER",
        "standalone only — multi-node CLUSTER is explicitly unsupported "
        "(probe INFO/NODES/KEYSLOT work; see docs/cluster-compat.md)",
    )
    skip(
        "skip:Sentinel",
        "SENTINEL is explicitly unsupported — use process-manager failover "
        "(see docs/redis-sentinel-compat.md)",
    )
    skip(
        "skip:WAIT replica-ack",
        "WAIT is real (0.8.0) but returns 0 with no replicas attached",
    )
    skip(
        "skip:Redis-RDB DUMP",
        "DUMP/RESTORE use NEXD, not Redis RDB — use scripts/migrate_from_redis.py",
    )
    skip(
        "skip:RedisCluster client",
        "do not point RedisCluster at nexrade — use redis.Redis; "
        "see scripts/redis_py_cluster_smoke.py + docs/cluster-compat.md",
    )


def run_sync(host: str, port: int) -> None:
    r = make_client(host, port, decode=True)
    r.flushdb()
    run("sync:PING", lambda: check_ping(r))
    run("sync:INFO server", lambda: check_info(r))
    run("sync:CLIENT SETINFO", lambda: check_client_setinfo(r))
    run("sync:CLIENT LIST", lambda: check_client_list(r))
    run("sync:CONFIG GET", lambda: check_config_get(r))
    run("sync:notify-keyspace-events", lambda: check_notify_config(r))
    run("sync:MEMORY", lambda: check_memory(r))
    run("sync:ECHO/SELECT", lambda: check_echo_select(r))
    run("sync:TIME/ROLE", lambda: check_time_role(r))
    run("sync:strings", lambda: check_strings(r))
    run("sync:pipeline", lambda: check_pipeline(r))
    run("sync:hash/list/set/zset", lambda: check_hash_list_set_zset(r))
    run("sync:SCAN family", lambda: check_scan_family(r))
    run("sync:TTL", lambda: check_ttl(r))
    run("sync:TYPE/RENAME/UNLINK", lambda: check_type_rename_unlink(r))
    run("sync:MULTI/WATCH", lambda: check_multi_watch(r))
    run("sync:pubsub", lambda: check_pubsub(r))
    run("sync:psubscribe", lambda: check_psubscribe(r))
    run("sync:streams", lambda: check_streams(r))
    run("sync:stream groups", lambda: check_stream_groups(r))
    run("sync:Lua", lambda: check_lua(r))
    run("sync:DUMP/RESTORE NEXD", lambda: check_dump_restore(r))
    run("sync:SLOWLOG", lambda: check_slowlog(r))
    run("sync:DEL/EXISTS", lambda: check_delete_exists(r))

    rb = make_client(host, port, decode=False)
    try:
        k = f"{prefix()}:bin".encode()
        rb.set(k, b"\x00\x01")
        assert rb.get(k) == b"\x00\x01"
        record("PASS", "sync:binary values", "ok")
    except Exception as e:  # noqa: BLE001
        record("FAIL", "sync:binary values", f"{type(e).__name__}: {e}")
    finally:
        rb.close()
    r.close()


# ── Async checks ──────────────────────────────────────────────────────────────


async def run_async(host: str, port: int) -> None:
    if aioredis is None:
        record("FAIL", "async:import", "redis.asyncio not available")
        return

    r = aioredis.Redis(
        host=host,
        port=port,
        decode_responses=True,
        socket_connect_timeout=2.0,
        socket_timeout=5.0,
    )
    try:
        await r.flushdb()

        async def ping() -> str:
            assert await r.ping() is True
            return "PONG"

        async def strings() -> str:
            p = prefix()
            await r.set(f"{p}:a", "1")
            assert await r.get(f"{p}:a") == "1"
            pipe = r.pipeline()
            pipe.set(f"{p}:b", "2")
            pipe.get(f"{p}:b")
            out = await pipe.execute()
            assert out[1] == "2"
            return "async strings+pipeline"

        async def hash_list() -> str:
            p = prefix()
            await r.hset(f"{p}:h", mapping={"x": "y"})
            assert await r.hget(f"{p}:h", "x") == "y"
            await r.rpush(f"{p}:l", "1", "2")
            assert await r.lrange(f"{p}:l", 0, -1) == ["1", "2"]
            return "async hash/list"

        async def multi() -> str:
            p = prefix()
            k = f"{p}:m"
            await r.set(k, "0")
            async with r.pipeline(transaction=True) as pipe:
                pipe.incr(k)
                pipe.incr(k)
                out = await pipe.execute()
            assert out == [1, 2], out
            return "async multi"

        async def zset_stream() -> str:
            p = prefix()
            await r.zadd(f"{p}:z", {"a": 1, "b": 2})
            zr = await r.zrange(f"{p}:z", 0, -1, withscores=True)
            assert zr == [("a", 1.0), ("b", 2.0)], zr
            sid = await r.xadd(f"{p}:s", {"f": "1"})
            assert sid
            assert await r.xlen(f"{p}:s") == 1
            return f"zset+stream id={sid}"

        async def scan() -> str:
            p = prefix()
            for i in range(3):
                await r.set(f"{p}:as:{i}", str(i))
            keys = []
            async for k in r.scan_iter(match=f"{p}:as:*", count=10):
                keys.append(k)
            assert len(keys) == 3, keys
            return f"async_scan={len(keys)}"

        for label, coro in [
            ("async:PING", ping),
            ("async:strings/pipeline", strings),
            ("async:hash/list", hash_list),
            ("async:MULTI", multi),
            ("async:zset/stream", zset_stream),
            ("async:SCAN", scan),
        ]:
            try:
                note = await coro()
                record("PASS", label, str(note))
            except Exception as e:  # noqa: BLE001
                record("FAIL", label, f"{type(e).__name__}: {e}")
    finally:
        await r.aclose()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default=os.environ.get("NEXRADE_HOST", "127.0.0.1"))
    ap.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("NEXRADE_PORT", "6379")),
    )
    ap.add_argument("--skip-async", action="store_true")
    ap.add_argument(
        "--no-skips",
        action="store_true",
        help="Do not print known-unsupported SKIP rows",
    )
    args = ap.parse_args()

    print(f"redis-py smoke → {args.host}:{args.port} (redis {redis.__version__})")
    wait_ready(args.host, args.port)

    run_sync(args.host, args.port)
    if not args.skip_async:
        asyncio.run(run_async(args.host, args.port))
    if not args.no_skips:
        run_skips()

    print()
    for status, label, note in RESULTS:
        mark = {"PASS": "✓", "FAIL": "✗", "SKIP": "○"}.get(status, "?")
        extra = f"  ({note})" if note else ""
        print(f"  {mark} {status:4} {label}{extra}")
    print()
    print(f"PASS={PASS} FAIL={FAIL} SKIP={SKIP} total={PASS + FAIL + SKIP}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
