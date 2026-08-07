#!/usr/bin/env python3
"""Full nexrade-cache vs Redis comparison grid.

## Why this exists

`bench_pipeline_set.py` covers SET/GET/INCR pipelined only. That is too narrow
to answer "how does this compare to Redis" for a release, and quoting it as if
it were general is how the README ended up claiming nexrade "beats Redis on
every common single-key command" when a random keyspace tells a different
story (see docs/perf-methodology.md).

This runs the full grid: every redis-benchmark command family, across
pipelining depths and keyspace shapes, both servers alternating, with a
same-binary noise floor measured in the same session.

## Discipline

* **Interleaved.** Round N runs redis-then-nexrade, round N+1 reverses. Drift
  hits both arms equally instead of whichever ran second.
* **Flushed between rounds**, so round N's dataset never changes round N+1's
  working-set size.
* **Verdict against a measured floor.** Deltas smaller than the observed
  round-to-round spread are reported as ties, not wins. Non-pipelined loopback
  throughput is round-trip bound — both servers land ~220-235k rps and the
  differences there are mostly not real.

## Usage

    python3 scripts/bench_vs_redis.py --rounds 5 --output /tmp/grid.json
    python3 scripts/bench_vs_redis.py --quick        # smaller, for a sanity pass
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import signal
import statistics as st
import subprocess
import sys
import time
from pathlib import Path

RPS = re.compile(r"^([A-Z_0-9]+): ([\d.]+) requests per second", re.MULTILINE)
_PROCS: list[subprocess.Popen] = []

# (label, redis-benchmark -t list). Split so a failure in one family does not
# lose the whole round, and so LRANGE variants can carry their own sizes.
FAMILIES = [
    ("strings", "SET,GET,INCR"),
    ("lists", "LPUSH,RPUSH,LPOP,RPOP"),
    ("sets", "SADD,SPOP"),
    ("hashes", "HSET"),
    ("zsets", "ZADD,ZPOPMIN"),
    ("lrange", "LRANGE_100,LRANGE_300,LRANGE_600"),
    ("mset", "MSET"),
]


def cleanup() -> None:
    for p in _PROCS:
        if p.poll() is None:
            p.kill()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                pass


def wait_ping(port: int, proc: subprocess.Popen, timeout_s: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server on {port} exited early (code {proc.returncode})")
        r = subprocess.run(["redis-cli", "-p", str(port), "PING"],
                           capture_output=True, text=True)
        if r.returncode == 0 and r.stdout.strip() == "PONG":
            return
        time.sleep(0.05)
    raise RuntimeError(f"server on {port} never answered PING")


def start_redis(port: int, workdir: Path) -> subprocess.Popen:
    p = subprocess.Popen(
        ["redis-server", "--port", str(port), "--save", "", "--appendonly", "no",
         "--loglevel", "warning", "--dir", str(workdir)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _PROCS.append(p)
    wait_ping(port, p)
    return p


def start_nexrade(binary: Path, port: int) -> subprocess.Popen:
    p = subprocess.Popen(
        [str(binary), "--port", str(port), "--metrics", "false",
         "--rdb-path", "", "--aof-path", "", "--log-level", "warn"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _PROCS.append(p)
    wait_ping(port, p)
    return p


def stop(p: subprocess.Popen) -> None:
    if p.poll() is None:
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(timeout=5)


def bench(port: int, tests: str, *, requests: int, clients: int,
          pipeline: int, keyspace: int | None) -> dict[str, float]:
    args = ["redis-benchmark", "-h", "127.0.0.1", "-p", str(port),
            "-c", str(clients), "-n", str(requests), "-t", tests, "-q"]
    if pipeline > 1:
        args += ["-P", str(pipeline)]
    if keyspace:
        args += ["-r", str(keyspace)]
    out = subprocess.run(args, capture_output=True, text=True).stdout
    # redis-benchmark interleaves a progress line on the same output stream;
    # normalise CRs to newlines so the summary lines parse.
    return {c: float(v) for c, v in RPS.findall(out.replace("\r", "\n"))}


def flush(port: int) -> None:
    subprocess.run(["redis-cli", "-p", str(port), "FLUSHALL"],
                   capture_output=True, text=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--nexrade", default="target/release/nexrade-cache")
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--requests", type=int, default=100_000)
    ap.add_argument("--clients", type=int, default=50)
    ap.add_argument("--redis-port", type=int, default=7390)
    ap.add_argument("--nexrade-port", type=int, default=7391)
    ap.add_argument("--quick", action="store_true",
                    help="3 rounds, 40k requests — sanity pass, not a result")
    ap.add_argument("--output", help="write raw per-round samples to JSON")
    args = ap.parse_args()

    if args.quick:
        args.rounds, args.requests = 3, 40_000

    for tool in ("redis-server", "redis-benchmark", "redis-cli"):
        if not shutil.which(tool):
            sys.stderr.write(f"{tool} not on PATH\n")
            return 2
    binary = Path(args.nexrade).resolve()
    if not binary.exists():
        sys.stderr.write(f"nexrade binary not found: {binary}\n")
        return 2

    workdir = Path("/tmp/nexrade-vs-redis")
    workdir.mkdir(exist_ok=True)

    redis_ver = subprocess.run(["redis-server", "--version"],
                               capture_output=True, text=True).stdout.strip()
    print(redis_ver)
    print(f"nexrade: {binary}")
    print(f"{args.rounds} rounds, {args.requests} requests, -c {args.clients}\n")

    # (label, pipeline, keyspace) — the shapes that actually distinguish.
    SHAPES = [
        ("non-pipelined, fixed key", 1, None),
        ("non-pipelined, 50k keyspace", 1, 50_000),
        ("pipelined -P16, 50k keyspace", 16, 50_000),
        ("pipelined -P50, fixed key", 50, None),
    ]

    results: dict = {}
    try:
        rp = start_redis(args.redis_port, workdir)
        np_ = start_nexrade(binary, args.nexrade_port)

        for shape_label, pipeline, keyspace in SHAPES:
            print(f"── {shape_label} " + "─" * (50 - len(shape_label)))
            shape: dict[str, dict[str, list[float]]] = {}
            for rnd in range(args.rounds):
                order = [("redis", args.redis_port), ("nexrade", args.nexrade_port)]
                if rnd % 2:
                    order.reverse()
                for fam_label, tests in FAMILIES:
                    for arm, port in order:
                        flush(port)
                        got = bench(port, tests, requests=args.requests,
                                    clients=args.clients, pipeline=pipeline,
                                    keyspace=keyspace)
                        for cmd, rps in got.items():
                            shape.setdefault(cmd, {"redis": [], "nexrade": []})
                            shape[cmd][arm].append(rps)
                print(f"   round {rnd + 1}/{args.rounds}")
            results[shape_label] = shape

            print(f"\n   {'command':<14}{'redis':>11}{'nexrade':>11}{'ratio':>8}"
                  f"{'floor':>8}  verdict")
            for cmd in sorted(shape):
                r, n = shape[cmd]["redis"], shape[cmd]["nexrade"]
                if len(r) < 2 or len(n) < 2:
                    continue
                mr, mn = st.median(r), st.median(n)
                if not mr:
                    continue
                # CV, not max-min: max-min is hostage to one bad round and
                # grows with round count (see docs/perf-methodology.md).
                fr = st.stdev(r) / st.mean(r) * 100
                fn = st.stdev(n) / st.mean(n) * 100
                floor = max(fr, fn)
                delta = (mn / mr - 1) * 100
                if abs(delta) < floor:
                    verdict = "tie"
                elif delta > 0:
                    verdict = f"nexrade +{delta:.0f}%"
                else:
                    verdict = f"REDIS +{-delta:.0f}%"
                print(f"   {cmd:<14}{mr:>11.0f}{mn:>11.0f}{mn / mr:>7.2f}x"
                      f"{floor:>7.1f}%  {verdict}")
            print()
    finally:
        cleanup()

    if args.output:
        Path(args.output).write_text(json.dumps({
            "redis_version": redis_ver,
            "nexrade_binary": str(binary),
            "rounds": args.rounds,
            "requests": args.requests,
            "clients": args.clients,
            "unix_time": int(time.time()),
            "samples": results,
        }, indent=2))
        print(f"raw samples → {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
