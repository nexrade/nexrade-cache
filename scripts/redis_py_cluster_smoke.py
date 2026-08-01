#!/usr/bin/env python3
"""redis-py RedisCluster smoke against a running nexrade-cache (0.9.2+).

Verifies that the standalone server does not break the cluster-client
introspection path. Uses `redis.Redis` over the same RESP port for
basic commands and CLUSTER probe subcommands — the cluster client
itself (RedisCluster) would auto-discover topology against a single
startup node and is a no-op routing layer for our standalone shape.
The smoke targets the **wire surface** that the cluster client
exercises.

  - Basic commands (PING / SET / GET / MSET / MGET) work
  - `CLUSTER INFO` reports the standalone shape (cluster_size:1)
  - `CLUSTER NODES` reports a single self-line covering slots 0..=16383
  - `CLUSTER KEYSLOT <key>` returns the canonical CRC16 slot
  - `CLUSTER LINKS` → empty array (no cluster gossip)
  - `CLUSTER COUNT-FAILURE-REPORTS <id>` → 0
  - Multi-node CLUSTER subcommands (`MEET` / `SETSLOT` / `FAILOVER`)
    return the explicit "not supported" error (cluster client falls
    back gracefully)

This is a **smoke** test, not a cluster-compatibility test — the
goal is to verify the standalone server does not break the redis-py
cluster client path. Real cluster functionality (gossip, slot
migration, MOVED/ASK) is explicitly out of scope for 0.x.

Usage:
  nexrade-cache --port 6399 --metrics false &
  python3 scripts/redis_py_cluster_smoke.py --port 6399

Exits non-zero only when a PASS-expected check fails.
"""

from __future__ import annotations

import argparse
import os
import sys

import redis

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


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default=os.environ.get("NEXRADE_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("NEXRADE_PORT", "6379")))
    args = p.parse_args()

    print(f"redis-py {redis.__version__} cluster smoke → {args.host}:{args.port}")

    # Plain client — the standalone server accepts the same RESP port
    # the cluster client would use.
    client: redis.Redis | None = None
    try:
        client = redis.Redis(host=args.host, port=args.port, decode_responses=False)
        record("PASS", "cluster:client:construct", "redis.Redis(host, port) accepted")
    except Exception as e:  # noqa: BLE001
        record("FAIL", "cluster:client:construct", f"client refused: {e}")
        print(f"FAIL: {e}")
        return 1

    def close():
        try:
            if client is not None:
                client.close()
        except Exception:
            pass

    # PING.
    def ping():
        ok = client.ping()
        if ok is not True and ok != b"PONG":
            raise AssertionError(f"unexpected ping reply {ok!r}")
        return "PONG"
    run("cluster:ping", ping)

    # SET / GET.
    def set_get():
        client.set("cluster:smoke", b"ok", ex=30)
        v = client.get("cluster:smoke")
        if v != b"ok":
            raise AssertionError(f"expected b'ok', got {v!r}")
        return "set+get round-trip"
    run("cluster:set+get", set_get)

    # MSET / MGET.
    def mset_mget():
        client.mset({"cluster:a": b"1", "cluster:b": b"2"})
        v = client.mget(["cluster:a", "cluster:b"])
        if v != [b"1", b"2"]:
            raise AssertionError(f"unexpected mget reply {v!r}")
        return "mset+mget"
    run("cluster:mset+mget", mset_mget)

    # CLUSTER INFO via raw socket — no client-side wrapping.
    def cluster_info_shape():
        pool = client.connection_pool
        conn = pool.get_connection("CLUSTER")
        try:
            conn.send_command("CLUSTER", "INFO")
            text = conn.read_response().decode()
        finally:
            pool.release(conn)
        if "cluster_state:ok" not in text:
            raise AssertionError(f"expected cluster_state:ok, got: {text}")
        if "cluster_size:1" not in text:
            raise AssertionError(f"expected cluster_size:1, got: {text}")
        return "cluster_state:ok, cluster_size:1"
    run("cluster:info", cluster_info_shape)

    # CLUSTER NODES — single self-line. redis-py 8 with
    # decode_responses=False returns the reply as a single bytes blob
    # with embedded CRLF separators; parse to a list of lines.
    def cluster_nodes_shape():
        pool = client.connection_pool
        conn = pool.get_connection()
        try:
            conn.send_command("CLUSTER", "NODES")
            reply = conn.read_response()
        finally:
            pool.release(conn)
        if isinstance(reply, (bytes, bytearray)):
            text = reply.decode() if isinstance(reply, bytes) else reply.decode()
            lines = [l for l in text.split("\r\n") if l]
        else:
            lines = reply
        if len(lines) != 1:
            raise AssertionError(f"expected 1 self-line, got: {reply!r}")
        return "1 self-line"
    run("cluster:nodes", cluster_nodes_shape)

    # CLUSTER KEYSLOT — Redis 7+ compatible value.
    def cluster_keyslot():
        s = client.execute_command("CLUSTER", "KEYSLOT", "foo")
        if s != 12182:
            raise AssertionError(f"expected 12182 for 'foo', got {s}")
        return "foo→12182"
    run("cluster:keyslot", cluster_keyslot)

    # CLUSTER LINKS — empty array (no cluster gossip).
    def cluster_links():
        v = client.execute_command("CLUSTER", "LINKS")
        if v != []:
            raise AssertionError(f"expected empty array, got {v!r}")
        return "[]"
    run("cluster:links", cluster_links)

    # CLUSTER COUNT-FAILURE-REPORTS — 0 (no failure tracking).
    def cluster_cfr():
        v = client.execute_command(
            "CLUSTER", "COUNT-FAILURE-REPORTS", "0" * 40,
        )
        if v != 0:
            raise AssertionError(f"expected 0, got {v}")
        return "0"
    run("cluster:count-failure-reports", cluster_cfr)

    # Multi-node CLUSTER subcommands → explicit hard-error.
    def meet_errors():
        try:
            client.execute_command("CLUSTER", "MEET", "10.0.0.1", "6380")
        except redis.exceptions.ResponseError as e:
            msg = str(e)
            if "CLUSTER MEET" in msg and "not supported" in msg:
                return "CLUSTER MEET not supported"
            raise AssertionError(f"unexpected error: {msg}")
        raise AssertionError("CLUSTER MEET did not error")
    run("cluster:meet-errors", meet_errors)

    def setslot_errors():
        try:
            client.execute_command("CLUSTER", "SETSLOT", "1234", "NODE", "abc")
        except redis.exceptions.ResponseError as e:
            if "SETSLOT" in str(e):
                return "SETSLOT not supported"
            raise AssertionError(f"unexpected: {e}")
        raise AssertionError("SETSLOT did not error")
    run("cluster:setslot-errors", setslot_errors)

    def failover_errors():
        try:
            client.execute_command("CLUSTER", "FAILOVER")
        except redis.exceptions.ResponseError as e:
            if "FAILOVER" in str(e):
                return "FAILOVER not supported"
            raise AssertionError(f"unexpected: {e}")
        raise AssertionError("FAILOVER did not error")
    run("cluster:failover-errors", failover_errors)

    # Pipeline shape (sends the same wire protocol the cluster client
    # would have used).
    def pipeline():
        pipe = client.pipeline(transaction=False)
        pipe.set("cluster:pipe:k", b"v")
        pipe.get("cluster:pipe:k")
        out = pipe.execute()
        if out != [True, b"v"]:
            raise AssertionError(f"unexpected pipeline reply {out!r}")
        return "pipeline round-trip"
    run("cluster:pipeline", pipeline)

    close()

    print()
    for status, label, note in RESULTS:
        mark = {"PASS": "✓", "FAIL": "✗", "SKIP": "○"}[status]
        print(f"  {mark} {status:4} {label}  ({note})")
    print(f"\nPASS={PASS} FAIL={FAIL} SKIP={SKIP} total={PASS + FAIL + SKIP}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
