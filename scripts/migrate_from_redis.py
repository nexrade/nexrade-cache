#!/usr/bin/env python3
"""
migrate_from_redis.py — copy all data from a Redis server into nexrade-cache.

Why not DUMP/RESTORE: nexrade-cache's DUMP/RESTORE are currently stubs
(DUMP always returns nil, RESTORE always returns OK without storing
anything — crates/nexrade-core/src/command/generic.rs). This script
instead reads each key back with its type-appropriate command (GET,
LRANGE, HGETALL, SMEMBERS, ZRANGE WITHSCORES, XRANGE) on the source and
replays it with the matching write command on the destination, which
works against any Redis-protocol server regardless of DUMP/RESTORE
support.

What it does:
  - Iterates every key via SCAN (non-blocking, safe on a live server).
  - Copies strings, lists, hashes, sets, sorted sets, and streams.
  - Preserves per-key TTL (PTTL on source -> PEXPIRE on destination).
  - Batches writes with pipelining for throughput.
  - Supports a single db or --all-dbs (0..db_count-1).
  - --dry-run to preview what would be copied without writing anything.

What it does NOT do:
  - Stream consumer groups / pending entries are not recreated (stream
    *entries* are copied faithfully with their original IDs; groups are
    metadata that would need to be reconstructed separately if you rely
    on XREADGROUP/XACK checkpoints — pass --copy-stream-groups to attempt
    a best-effort recreation of just the group name + last-delivered-id).
  - Any Lua scripts cached with SCRIPT LOAD (SHA cache is server-local
    and has no data-plane content to copy).
  - ACL users / config — this is a data migration, not a full config
    migration.

Usage:
  pip install redis
  python3 scripts/migrate_from_redis.py \\
      --source-host 127.0.0.1 --source-port 6379 \\
      --dest-host   127.0.0.1 --dest-port   6380 \\
      --all-dbs

  # Single db, dry run first:
  python3 scripts/migrate_from_redis.py --source-db 0 --dest-db 0 --dry-run
"""

import argparse
import sys
import time

import redis


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source-host", default="127.0.0.1")
    p.add_argument("--source-port", type=int, default=6379)
    p.add_argument("--source-password", default=None)
    p.add_argument("--source-db", type=int, default=0, help="Ignored if --all-dbs is set.")

    p.add_argument("--dest-host", default="127.0.0.1")
    p.add_argument("--dest-port", type=int, default=6380, help="nexrade-cache default TLS-free port; use --dest-port 6379 if unchanged.")
    p.add_argument("--dest-password", default=None)
    p.add_argument("--dest-db", type=int, default=0, help="Ignored if --all-dbs is set.")

    p.add_argument("--all-dbs", action="store_true", help="Migrate every db 0..N-1 reported by the source's CONFIG GET databases.")
    p.add_argument("--match", default="*", help="SCAN MATCH glob pattern (default: all keys).")
    p.add_argument("--scan-count", type=int, default=1000, help="SCAN COUNT hint per cursor step.")
    p.add_argument("--batch-size", type=int, default=500, help="Keys buffered per destination pipeline flush.")
    p.add_argument("--flush-dest", action="store_true", help="FLUSHDB the destination db(s) before copying (asks for confirmation unless --yes).")
    p.add_argument("--yes", action="store_true", help="Skip the --flush-dest confirmation prompt.")
    p.add_argument("--copy-stream-groups", action="store_true", help="Best-effort: recreate stream consumer group names + last-delivered-id on the destination (pending entries are NOT replayed).")
    p.add_argument("--dry-run", action="store_true", help="Read from source and report counts/bytes without writing to destination.")
    p.add_argument("--progress-every", type=int, default=2000, help="Print a progress line every N keys.")
    return p.parse_args()


def connect(host: str, port: int, password: str | None, db: int) -> redis.Redis:
    r = redis.Redis(host=host, port=port, password=password, db=db, decode_responses=False, socket_keepalive=True)
    r.ping()
    return r


def discover_dbs(source: redis.Redis) -> int:
    try:
        cfg = source.config_get("databases")
        return int(cfg.get(b"databases", cfg.get("databases", 16)))
    except Exception:
        return 16


def copy_string(pipe, key: bytes, src: redis.Redis):
    val = src.get(key)
    if val is not None:
        pipe.set(key, val)


def copy_list(pipe, key: bytes, src: redis.Redis):
    items = src.lrange(key, 0, -1)
    if items:
        pipe.rpush(key, *items)


def copy_hash(pipe, key: bytes, src: redis.Redis):
    mapping = src.hgetall(key)
    if mapping:
        pipe.hset(key, mapping=mapping)


def copy_set(pipe, key: bytes, src: redis.Redis):
    members = src.smembers(key)
    if members:
        pipe.sadd(key, *members)


def copy_zset(pipe, key: bytes, src: redis.Redis):
    pairs = src.zrange(key, 0, -1, withscores=True)
    if pairs:
        mapping = {member: score for member, score in pairs}
        pipe.zadd(key, mapping)


def copy_stream(pipe, key: bytes, src: redis.Redis, dest: redis.Redis, copy_groups: bool):
    entries = src.xrange(key, min="-", max="+")
    for entry_id, fields in entries:
        # XADD with an explicit id replays the original id exactly, so
        # ordering and any external references to specific IDs survive.
        flat = []
        for k, v in fields.items():
            flat.append(k)
            flat.append(v)
        pipe.execute_command("XADD", key, entry_id, *flat)

    if copy_groups:
        try:
            groups = src.xinfo_groups(key)
        except redis.ResponseError:
            groups = []
        for g in groups:
            name = g.get(b"name") or g.get("name")
            last_delivered = g.get(b"last-delivered-id") or g.get("last-delivered-id")
            if name is None or last_delivered is None:
                continue
            # Best-effort: recreate the group pointer only. Pending/ack
            # state is not reconstructed — a fresh consumer group with the
            # same last-delivered-id resumes reads from the same point,
            # but in-flight (delivered, unacked) entries are lost.
            try:
                dest.execute_command("XGROUP", "CREATE", key, name, last_delivered, "MKSTREAM")
            except redis.ResponseError:
                pass  # group probably already exists


COPY_FN = {
    b"string": copy_string,
    b"list": copy_list,
    b"hash": copy_hash,
    b"set": copy_set,
    b"zset": copy_zset,
    # stream handled specially below (needs dest handle + flag)
}


def migrate_db(source: redis.Redis, dest: redis.Redis, args: argparse.Namespace, db_index: int) -> tuple[int, int]:
    if args.flush_dest and not args.dry_run:
        if not args.yes:
            resp = input(f"About to FLUSHDB destination db {db_index} at {args.dest_host}:{args.dest_port}. Type 'yes' to continue: ")
            if resp.strip().lower() != "yes":
                print("Aborted.")
                sys.exit(1)
        dest.flushdb()

    copied = 0
    skipped = 0
    pipe = None if args.dry_run else dest.pipeline(transaction=False)
    buffered = 0
    t0 = time.time()

    for key in source.scan_iter(match=args.match, count=args.scan_count):
        try:
            key_type = source.type(key)
        except redis.ResponseError:
            skipped += 1
            continue

        if key_type == b"stream":
            if not args.dry_run:
                copy_stream(pipe, key, source, dest, args.copy_stream_groups)
                buffered += 1
        else:
            fn = COPY_FN.get(key_type)
            if fn is None:
                skipped += 1
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
            pipe = dest.pipeline(transaction=False)
            buffered = 0

        if copied % args.progress_every == 0:
            elapsed = time.time() - t0
            rate = copied / elapsed if elapsed > 0 else 0
            print(f"  db {db_index}: {copied} keys copied ({skipped} skipped), {rate:.0f} keys/sec")

    if not args.dry_run and buffered > 0:
        pipe.execute()

    return copied, skipped


def main() -> None:
    args = parse_args()

    source = connect(args.source_host, args.source_port, args.source_password, 0)
    dest = None if args.dry_run else connect(args.dest_host, args.dest_port, args.dest_password, 0)

    if args.all_dbs:
        db_count = discover_dbs(source)
        db_indices = list(range(db_count))
    else:
        db_indices = [args.source_db]

    print(f"Source: {args.source_host}:{args.source_port}  Dest: {args.dest_host}:{args.dest_port}  dry_run={args.dry_run}")
    print(f"Databases to migrate: {db_indices}")

    total_copied = 0
    total_skipped = 0
    t0 = time.time()

    for db_index in db_indices:
        source.execute_command("SELECT", db_index)
        dest_db_index = db_index if args.all_dbs else args.dest_db
        if not args.dry_run:
            dest.execute_command("SELECT", dest_db_index)

        print(f"\n── db {db_index} → dest db {dest_db_index} ──")
        copied, skipped = migrate_db(source, dest, args, db_index)
        total_copied += copied
        total_skipped += skipped
        print(f"  done: {copied} keys copied, {skipped} skipped")

    elapsed = time.time() - t0
    print(f"\nTotal: {total_copied} keys copied, {total_skipped} skipped, in {elapsed:.1f}s ({total_copied / elapsed if elapsed else 0:.0f} keys/sec)")

    if args.dry_run:
        print("(dry run — nothing was written to the destination)")


if __name__ == "__main__":
    main()
