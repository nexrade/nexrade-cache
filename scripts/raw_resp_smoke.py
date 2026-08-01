#!/usr/bin/env python3
"""Wire-surface smoke via raw RESP3 over a raw TCP socket.

Cross-validation smoke for nexrade-cache: builds RESP requests by
hand and parses the server's replies byte-by-byte. No third-party
Redis client is imported — the goal is to prove the wire format
**independently** of `redis-py`, `coredis`, or any other library.

Coverage:
  - Inline / PING via raw `*1\r\n$4\r\nPING\r\n`
  - Inline / SET / GET via raw bulk-array frames
  - MSET / MGET (multi-key batch)
  - HSET / HGET (hash command surface)
  - LPUSH / LRANGE (list surface)
  - Bulk-string framing: `$len\r\n<bytes>\r\n`
  - RESP3 simple-string framing: `+OK\r\n`
  - RESP3 integer framing: `:N\r\n`
  - Array framing: `*N\r\n...\r\n`
  - Error framing: `-ERR message\r\n`
  - CLUSTER KEYSLOT (CRC16 slot probe)

Why raw socket? `redis-py` and `coredis` both encode the same wire
format, so a wire bug that passes one passes the other. By using
the Python standard library `socket` and writing RESP frames
manually, we get a third, independent parser — any wire-shape bug has
a third chance to surface here.

Usage:
  nexrade-cache --port 6399 --metrics false &
  python3 scripts/raw_resp_smoke.py --port 6399

Exits non-zero only when a PASS-expected check fails.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys

PASS = 0
FAIL = 0
RESULTS: list[tuple[str, str, str]] = []


def record(status: str, label: str, note: str = "") -> None:
    global PASS, FAIL
    if status == "PASS":
        PASS += 1
    else:
        FAIL += 1
    RESULTS.append((status, label, note))


def run(label: str, fn) -> None:
    try:
        note = fn() or ""
        record("PASS", label, str(note))
    except Exception as e:  # noqa: BLE001 — surface any client/server error
        record("FAIL", label, f"{type(e).__name__}: {e}")


def encode_bulk_array(*args: bytes) -> bytes:
    """Build a RESP array of bulk strings."""
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out += f"${len(a)}\r\n".encode() + a + b"\r\n"
    return out


def encode_bulk_int_array(arr: list[bytes]) -> bytes:
    out = f"*{len(arr)}\r\n".encode()
    for a in arr:
        out += f"${len(a)}\r\n".encode() + a + b"\r\n"
    return out


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("server closed connection mid-frame")
        buf += chunk
    return buf


def recv_line(sock: socket.socket, max: int = 4096) -> bytes:
    """Read until \r\n (inclusive)."""
    buf = b""
    while True:
        b = sock.recv(1)
        if not b:
            raise ConnectionError("server closed connection mid-line")
        buf += b
        if buf.endswith(b"\r\n") or len(buf) >= max:
            return buf


def parse_one(sock: socket.socket):
    """Read and parse exactly one RESP value. Returns (value, None) or
    raises on protocol error / unexpected EOF."""
    header = recv_line(sock)
    if not header or not header.endswith(b"\r\n"):
        raise ConnectionError(f"malformed RESP header {header!r}")
    t = header[0:1]
    body = header[1:-2]  # strip type byte + trailing \r\n
    if t == b"+":
        return body.decode(), None
    if t == b"-":
        return ("ERR", body.decode()), None
    if t == b":":
        return int(body), None
    if t == b"$":
        n = int(body)
        if n < 0:
            return None, None
        data = recv_exact(sock, n)
        # Consume the trailing \r\n
        recv_exact(sock, 2)
        return data.decode("utf-8", "replace"), None
    if t == b"*":
        n = int(body)
        if n < 0:
            return None, None
        return [parse_one(sock)[0] for _ in range(n)], None
    raise ValueError(f"unknown RESP type {t!r}")


def request(host: str, port: int, *args: bytes) -> object:
    """Open a fresh socket, send a RESP bulk-array request, return the parsed reply."""
    s = socket.create_connection((host, port), timeout=5)
    try:
        s.sendall(encode_bulk_array(*args))
        return parse_one(s)[0]
    finally:
        s.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default=os.environ.get("NEXRADE_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("NEXRADE_PORT", "6379")))
    args = p.parse_args()

    print(f"raw-RESP smoke → {args.host}:{args.port}")

    def ping():
        v = request(args.host, args.port, b"PING")
        if v != "PONG":
            raise AssertionError(f"expected PONG, got {v!r}")
        return "PONG"
    run("raw:PING", ping)

    def set_get():
        request(args.host, args.port, b"SET", b"raw:smoke", b"hello", b"EX", b"60")
        v = request(args.host, args.port, b"GET", b"raw:smoke")
        if v != "hello":
            raise AssertionError(f"expected 'hello', got {v!r}")
        return "round-trip"
    run("raw:set+get", set_get)

    def set_with_ttl():
        request(args.host, args.port, b"SET", b"raw:ttl", b"v", b"PX", b"200")
        ttl = request(args.host, args.port, b"PTTL", b"raw:ttl")
        if not (0 < ttl <= 200):
            raise AssertionError(f"PTTL out of range, got {ttl}")
        return f"PTTL={ttl}ms"
    run("raw:set+pttl", set_with_ttl)

    def del_test():
        request(args.host, args.port, b"SET", b"raw:del", b"v")
        n = request(args.host, args.port, b"DEL", b"raw:del")
        if n != 1:
            raise AssertionError(f"expected DEL=1, got {n}")
        return "DEL=1"
    run("raw:del", del_test)

    def mset_mget():
        request(args.host, args.port, b"MSET", b"raw:a", b"1", b"raw:b", b"2")
        v = request(args.host, args.port, b"MGET", b"raw:a", b"raw:b")
        if v != ["1", "2"]:
            raise AssertionError(f"unexpected mget reply {v!r}")
        return "mset+mget"
    run("raw:mset+mget", mset_mget)

    def hset_hget():
        # Reset state — fields persist across smoke runs.
        request(args.host, args.port, b"DEL", b"raw:h")
        n = request(args.host, args.port, b"HSET", b"raw:h", b"f1", b"v1", b"f2", b"v2")
        if n != 2:
            raise AssertionError(f"expected HSET=2, got {n}")
        v = request(args.host, args.port, b"HGET", b"raw:h", b"f1")
        if v != "v1":
            raise AssertionError(f"expected v1, got {v!r}")
        return "HSET=2, HGET=v1"
    run("raw:hset+hget", hset_hget)

    def lpush_lrange():
        # Reset state — list persists across smoke runs.
        request(args.host, args.port, b"DEL", b"raw:l")
        n = request(args.host, args.port, b"LPUSH", b"raw:l", b"a", b"b", b"c")
        if n != 3:
            raise AssertionError(f"expected LPUSH=3, got {n}")
        items = request(args.host, args.port, b"LRANGE", b"raw:l", b"0", b"-1")
        if items != ["c", "b", "a"]:
            raise AssertionError(f"unexpected LRANGE reply {items!r}")
        return "LPUSH=3, LRANGE=3"
    run("raw:lpush+lrange", lpush_lrange)

    def cluster_keyslot():
        # foo → slot 12182 (Redis canonical)
        s = request(args.host, args.port, b"CLUSTER", b"KEYSLOT", b"foo")
        if s != 12182:
            raise AssertionError(f"expected 12182, got {s}")
        return "foo→12182"
    run("raw:cluster-keyslot", cluster_keyslot)

    def info_replication():
        text = request(args.host, args.port, b"INFO", b"replication")
        if "role:master" not in text:
            raise AssertionError(f"expected role:master, got:\n{text}")
        if "connected_slaves:0" not in text:
            raise AssertionError(f"expected connected_slaves:0, got:\n{text}")
        return "role=master, connected_slaves=0"
    run("raw:info-replication", info_replication)

    def err_frame_shape():
        # Garbage command → -ERR <message>\r\n
        s = socket.create_connection((args.host, args.port), timeout=5)
        try:
            s.sendall(b"*1\r\n$4\r\nNOPE\r\n")
            v, _ = parse_one(s)
            if not (isinstance(v, tuple) and v[0] == "ERR"):
                raise AssertionError(f"expected ERR tuple, got {v!r}")
            return f"-ERR frame: {v[1][:40]!r}"
        finally:
            s.close()
    run("raw:err-frame", err_frame_shape)

    print()
    for status, label, note in RESULTS:
        mark = {"PASS": "✓", "FAIL": "✗"}.get(status, "?")
        print(f"  {mark} {status:4} {label}  ({note})")
    print(f"\nPASS={PASS} FAIL={FAIL} total={PASS + FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
