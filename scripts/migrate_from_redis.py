#!/usr/bin/env python3
"""
migrate_from_redis.py — copy data from a Redis-protocol server into nexrade-cache.

Why not DUMP/RESTORE for Redis → nexrade?
  nexrade DUMP/RESTORE use a custom NEXD payload (bincode of Entry), not Redis
  RDB. See docs/dump-restore.md. This script re-reads each key with
  type-appropriate commands and replays writes on the destination so any
  RESP server can be a source.

Copies:
  - strings, lists, hashes, sets, sorted sets, streams (entries + IDs)
  - per-key TTL (PTTL → PEXPIRE)
  - optional stream consumer group name + last-delivered-id
    (--copy-stream-groups; PELs are NOT restored)

Does not copy:
  - ACL / CONFIG / SCRIPT LOAD cache
  - Stream pending entries (PELLOG / XPENDING state)
  - Module-specific types (skipped + counted)

Usage:
  pip install redis
  python3 scripts/migrate_from_redis.py \\
      --source-host 127.0.0.1 --source-port 6379 \\
      --dest-host   127.0.0.1 --dest-port   6380 \\
      --all-dbs --verify

  # Single db, dry run first:
  python3 scripts/migrate_from_redis.py --source-db 0 --dest-db 0 --dry-run
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from typing import Any

import redis


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--source-host", default="127.0.0.1")
    p.add_argument("--source-port", type=int, default=6379)
    p.add_argument("--source-password", default=None)
    p.add_argument(
        "--source-db",
        type=int,
        default=0,
        help="Ignored if --all-dbs is set.",
    )

    p.add_argument("--dest-host", default="127.0.0.1")
    p.add_argument(
        "--dest-port",
        type=int,
        default=6380,
        help="nexrade-cache default non-TLS port; use 6379 if unchanged.",
    )
    p.add_argument("--dest-password", default=None)
    p.add_argument(
        "--dest-db",
        type=int,
        default=0,
        help="Ignored if --all-dbs is set.",
    )

    p.add_argument(
        "--all-dbs",
        action="store_true",
        help="Migrate every db 0..N-1 from source CONFIG GET databases.",
    )
    p.add_argument(
        "--match",
        default="*",
        help="SCAN MATCH glob pattern (default: all keys).",
    )
    p.add_argument(
        "--scan-count",
        type=int,
        default=1000,
        help="SCAN COUNT hint per cursor step.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Keys buffered per destination pipeline flush.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Stop after N keys per db (0 = unlimited). Useful for smoke.",
    )
    p.add_argument(
        "--flush-dest",
        action="store_true",
        help="FLUSHDB destination db(s) before copy (needs --yes unless interactive).",
    )
    p.add_argument(
        "--yes",
        action="store_true",
        help="Skip the --flush-dest confirmation prompt.",
    )
    p.add_argument(
        "--copy-stream-groups",
        action="store_true",
        help="Best-effort: recreate stream group name + last-delivered-id (no PEL).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Read source and report counts without writing to destination.",
    )
    p.add_argument(
        "--verify",
        action="store_true",
        help="After copy, re-scan source and spot-check type/TTL/value on dest.",
    )
    p.add_argument(
        "--verify-sample",
        type=int,
        default=200,
        help="Max keys per db to verify when --verify is set (default 200).",
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=2000,
        help="Print a progress line every N keys.",
    )
    return p.parse_args()


def connect(host: str, port: int, password: str | None, db: int) -> redis.Redis:
    r = redis.Redis(
        host=host,
        port=port,
        password=password,
        db=db,
        decode_responses=False,
        socket_keepalive=True,
    )
    r.ping()
    return r


def discover_dbs(source: redis.Redis) -> int:
    try:
        cfg = source.config_get("databases")
        return int(cfg.get(b"databases", cfg.get("databases", 16)))
    except Exception:
        return 16


def copy_string(pipe: Any, key: bytes, src: redis.Redis) -> None:
    # GET works for plain strings and Redis bitmaps (string type).
    val = src.get(key)
    if val is not None:
        pipe.set(key, val)


def copy_list(pipe: Any, key: bytes, src: redis.Redis) -> None:
    items = src.lrange(key, 0, -1)
    if items:
        pipe.rpush(key, *items)


def copy_hash(pipe: Any, key: bytes, src: redis.Redis) -> None:
    mapping = src.hgetall(key)
    if mapping:
        pipe.hset(key, mapping=mapping)


def copy_set(pipe: Any, key: bytes, src: redis.Redis) -> None:
    members = src.smembers(key)
    if members:
        pipe.sadd(key, *members)


def copy_zset(pipe: Any, key: bytes, src: redis.Redis) -> None:
    pairs = src.zrange(key, 0, -1, withscores=True)
    if pairs:
        mapping = {member: score for member, score in pairs}
        pipe.zadd(key, mapping)


def copy_stream(
    pipe: Any,
    key: bytes,
    src: redis.Redis,
    dest: redis.Redis | None,
    copy_groups: bool,
    dry_run: bool,
) -> None:
    entries = src.xrange(key, min="-", max="+")
    for entry_id, fields in entries:
        flat: list[bytes] = []
        for k, v in fields.items():
            flat.append(k)
            flat.append(v)
        if not dry_run:
            pipe.execute_command("XADD", key, entry_id, *flat)

    if copy_groups and not dry_run and dest is not None:
        try:
            groups = src.xinfo_groups(key)
        except redis.ResponseError:
            groups = []
        for g in groups:
            name = g.get(b"name") or g.get("name")
            last_delivered = g.get(b"last-delivered-id") or g.get("last-delivered-id")
            if name is None or last_delivered is None:
                continue
            try:
                dest.execute_command(
                    "XGROUP", "CREATE", key, name, last_delivered, "MKSTREAM"
                )
            except redis.ResponseError:
                pass  # group probably already exists


COPY_FN = {
    b"string": copy_string,
    b"list": copy_list,
    b"hash": copy_hash,
    b"set": copy_set,
    b"zset": copy_zset,
}


def migrate_db(
    source: redis.Redis,
    dest: redis.Redis | None,
    args: argparse.Namespace,
    db_index: int,
) -> tuple[int, int, Counter]:
    if args.flush_dest and not args.dry_run and dest is not None:
        if not args.yes:
            resp = input(
                f"About to FLUSHDB destination db {db_index} at "
                f"{args.dest_host}:{args.dest_port}. Type 'yes' to continue: "
            )
            if resp.strip().lower() != "yes":
                print("Aborted.")
                sys.exit(1)
        dest.flushdb()

    copied = 0
    skipped = 0
    by_type: Counter = Counter()
    pipe = None if args.dry_run else dest.pipeline(transaction=False)  # type: ignore[union-attr]
    buffered = 0
    t0 = time.time()

    for key in source.scan_iter(match=args.match, count=args.scan_count):
        if args.limit and copied >= args.limit:
            break
        try:
            key_type = source.type(key)
        except redis.ResponseError:
            skipped += 1
            by_type["error"] += 1
            continue

        type_label = (
            key_type.decode("utf-8", "replace")
            if isinstance(key_type, (bytes, bytearray))
            else str(key_type)
        )
        by_type[type_label] += 1

        if key_type == b"stream":
            if not args.dry_run:
                copy_stream(
                    pipe,
                    key,
                    source,
                    dest,
                    args.copy_stream_groups,
                    dry_run=False,
                )
                buffered += 1
            else:
                # Still touch XRANGE so dry-run costs resemble a real scan.
                copy_stream(None, key, source, None, False, dry_run=True)
        else:
            fn = COPY_FN.get(key_type)
            if fn is None:
                skipped += 1
                by_type[f"skipped:{type_label}"] += 1
                continue
            if not args.dry_run:
                fn(pipe, key, source)
                buffered += 1

        if not args.dry_run:
            pttl = source.pttl(key)
            if pttl and pttl > 0:
                pipe.pexpire(key, pttl)

        copied += 1

        if not args.dry_run and buffered >= args.batch_size:
            pipe.execute()
            pipe = dest.pipeline(transaction=False)  # type: ignore[union-attr]
            buffered = 0

        if args.progress_every and copied % args.progress_every == 0:
            elapsed = time.time() - t0
            rate = copied / elapsed if elapsed > 0 else 0
            print(
                f"  db {db_index}: {copied} keys ({skipped} skipped), "
                f"{rate:.0f} keys/sec"
            )

    if not args.dry_run and buffered > 0 and pipe is not None:
        pipe.execute()

    return copied, skipped, by_type


def _as_bytes(v: Any) -> bytes:
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, str):
        return v.encode()
    return str(v).encode()


def verify_db(
    source: redis.Redis,
    dest: redis.Redis,
    args: argparse.Namespace,
    db_index: int,
) -> tuple[int, int]:
    """Spot-check type + a value sample + TTL sign on destination."""
    checked = 0
    mismatches = 0
    for key in source.scan_iter(match=args.match, count=args.scan_count):
        if checked >= args.verify_sample:
            break
        try:
            src_type = source.type(key)
            dst_type = dest.type(key)
        except redis.ResponseError:
            mismatches += 1
            continue
        checked += 1
        if src_type != dst_type:
            print(
                f"  VERIFY type mismatch db={db_index} key={key!r}: "
                f"src={src_type!r} dest={dst_type!r}"
            )
            mismatches += 1
            continue

        # Value sample by type (cheap equality for small keys).
        try:
            if src_type == b"string":
                if source.get(key) != dest.get(key):
                    print(f"  VERIFY value mismatch (string) key={key!r}")
                    mismatches += 1
            elif src_type == b"list":
                if source.llen(key) != dest.llen(key):
                    print(f"  VERIFY llen mismatch key={key!r}")
                    mismatches += 1
            elif src_type == b"hash":
                if source.hlen(key) != dest.hlen(key):
                    print(f"  VERIFY hlen mismatch key={key!r}")
                    mismatches += 1
            elif src_type == b"set":
                if source.scard(key) != dest.scard(key):
                    print(f"  VERIFY scard mismatch key={key!r}")
                    mismatches += 1
            elif src_type == b"zset":
                if source.zcard(key) != dest.zcard(key):
                    print(f"  VERIFY zcard mismatch key={key!r}")
                    mismatches += 1
            elif src_type == b"stream":
                if source.xlen(key) != dest.xlen(key):
                    print(f"  VERIFY xlen mismatch key={key!r}")
                    mismatches += 1
        except redis.ResponseError as e:
            print(f"  VERIFY error key={key!r}: {e}")
            mismatches += 1
            continue

        # TTL: both should be persistent, or both positive (values may drift).
        s_pttl = source.pttl(key)
        d_pttl = dest.pttl(key)
        src_has = s_pttl is not None and s_pttl > 0
        dst_has = d_pttl is not None and d_pttl > 0
        if src_has != dst_has:
            print(
                f"  VERIFY TTL presence mismatch key={key!r}: "
                f"src_pttl={s_pttl} dest_pttl={d_pttl}"
            )
            mismatches += 1

    return checked, mismatches


def main() -> None:
    args = parse_args()

    source = connect(
        args.source_host, args.source_port, args.source_password, 0
    )
    dest = (
        None
        if args.dry_run
        else connect(args.dest_host, args.dest_port, args.dest_password, 0)
    )

    if args.all_dbs:
        db_count = discover_dbs(source)
        db_indices = list(range(db_count))
    else:
        db_indices = [args.source_db]

    print(
        f"Source: {args.source_host}:{args.source_port}  "
        f"Dest: {args.dest_host}:{args.dest_port}  "
        f"dry_run={args.dry_run} verify={args.verify}"
    )
    print(f"Databases to migrate: {db_indices}")
    if args.limit:
        print(f"Limit: {args.limit} keys/db")

    total_copied = 0
    total_skipped = 0
    total_types: Counter = Counter()
    t0 = time.time()

    for db_index in db_indices:
        source.execute_command("SELECT", db_index)
        dest_db_index = db_index if args.all_dbs else args.dest_db
        if not args.dry_run and dest is not None:
            dest.execute_command("SELECT", dest_db_index)

        print(f"\n── db {db_index} → dest db {dest_db_index} ──")
        copied, skipped, by_type = migrate_db(source, dest, args, db_index)
        total_copied += copied
        total_skipped += skipped
        total_types.update(by_type)
        print(f"  done: {copied} keys copied, {skipped} skipped")
        if by_type:
            summary = ", ".join(f"{k}={v}" for k, v in sorted(by_type.items()))
            print(f"  types: {summary}")

        if args.verify and not args.dry_run and dest is not None:
            print(f"  verifying (sample ≤ {args.verify_sample})…")
            checked, mismatches = verify_db(source, dest, args, db_index)
            print(f"  verify: {checked} checked, {mismatches} mismatches")
            if mismatches:
                print("  WARNING: verification found differences", file=sys.stderr)

    elapsed = time.time() - t0
    rate = total_copied / elapsed if elapsed else 0
    print(
        f"\nTotal: {total_copied} keys copied, {total_skipped} skipped, "
        f"in {elapsed:.1f}s ({rate:.0f} keys/sec)"
    )
    if total_types:
        print(
            "Type totals: "
            + ", ".join(f"{k}={v}" for k, v in sorted(total_types.items()))
        )

    if args.dry_run:
        print("(dry run — nothing was written to the destination)")
        print("See docs/dump-restore.md for DUMP/RESTORE vs migrate guidance.")


if __name__ == "__main__":
    main()
