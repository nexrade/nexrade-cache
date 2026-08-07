#!/usr/bin/env python3
"""End-to-end read benchmark with p99 latency and RSS sampling (1.5.0).

## Why this exists

`store_bench` drives `ShardedDatabase` directly. It has no client, no socket,
and no memory instrumentation, so it cannot answer two questions the 1.5.0
exit gate asks:

* **p99 latency** — throughput can improve while tail latency degrades. A
  change that adds a slow path taken 1% of the time is invisible to a mean.
* **Resident memory** — any design that trades memory for speed (RCU with
  epoch reclamation being the motivating example) must have that trade
  measured, not assumed. An experiment that cannot measure its own main cost
  cannot be evaluated.

This runner covers both, through the real RESP surface.

## What it does NOT do

It is not a Redis comparison — use `scripts/bench_pipeline_set.py` for that.
Here both arms are nexrade-cache; the point is A-vs-B on one codebase.

It does not build anything. Point it at two binaries you built yourself,
**at the same filesystem path if you care about small deltas** — see
`docs/perf-methodology.md`; a path-length difference alone once produced an
apparent significant −8.6%.

## Usage

    # Sanity: same binary twice. Any delta reported is measurement error and
    # sets the floor under which a real comparison means nothing.
    python3 scripts/bench_e2e_read.py --baseline ./nc --candidate ./nc --rounds 6

    # Real comparison
    python3 scripts/bench_e2e_read.py \\
        --baseline /tmp/base/nexrade-cache \\
        --candidate ./target/release/nexrade-cache \\
        --rounds 8 --keys 100000 --output /tmp/e2e.json

Rounds alternate baseline/candidate so drift hits both arms equally, the same
discipline `bench_ab.py` applies to the Criterion suite. Requires
`redis-benchmark` and `redis-cli` on PATH.
"""

from __future__ import annotations

import argparse
import json
import re
import signal
import statistics as st
import subprocess
import sys
import time
from pathlib import Path

# `redis-benchmark` summary block:
#           avg       min       p50       p95       p99       max
#         0.052     0.000     0.031     0.063     0.095     4.111
LATENCY_RE = re.compile(
    r"latency summary \(msec\):\s*\n\s*avg\s+min\s+p50\s+p95\s+p99\s+max\s*\n"
    r"\s*([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)"
)
THROUGHPUT_RE = re.compile(r"throughput summary:\s+([\d.]+) requests per second")

_PROCS: list[subprocess.Popen] = []


def cleanup() -> None:
    for p in _PROCS:
        if p.poll() is None:
            p.kill()
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass


def rss_kb(pid: int) -> int | None:
    """Resident set size in KiB from /proc, or None off Linux.

    Field 2 of /proc/<pid>/statm is resident pages. Read directly rather than
    shelling out to ps, so sampling is cheap enough to poll during a run.
    """
    try:
        with open(f"/proc/{pid}/statm") as f:
            pages = int(f.read().split()[1])
        return pages * 4  # 4 KiB pages
    except (OSError, IndexError, ValueError):
        return None


def wait_ready(port: int, proc: subprocess.Popen, timeout_s: float = 15.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out = (proc.stderr.read() or b"").decode(errors="replace")[-800:]
            raise RuntimeError(f"server exited early (code {proc.returncode}):\n{out}")
        r = subprocess.run(
            ["redis-cli", "-p", str(port), "PING"],
            capture_output=True, text=True,
        )
        if r.returncode == 0 and r.stdout.strip() == "PONG":
            return
        time.sleep(0.05)
    raise RuntimeError(f"server on port {port} never answered PING")


def start(binary: Path, port: int) -> subprocess.Popen:
    proc = subprocess.Popen(
        [
            str(binary),
            "--port", str(port),
            "--metrics", "false",
            "--log-level", "warn",
            # Persistence off: this measures the read path, and a background
            # BGSAVE mid-run would show up as a latency tail that has nothing
            # to do with the change under test.
            "--rdb-path", "",
            "--aof-path", "",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _PROCS.append(proc)
    wait_ready(port, proc)
    return proc


def stop(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def preload(port: int, keys: int) -> None:
    """Populate `keys` string keys via redis-benchmark's own SET workload.

    Using redis-benchmark rather than a pipe of SET commands keeps the key
    shape identical to what the GET phase will request (`key:__rand_int__`).
    """
    subprocess.run(
        [
            "redis-benchmark", "-h", "127.0.0.1", "-p", str(port),
            "-t", "SET", "-n", str(keys), "-r", str(keys),
            "-c", "50", "-P", "16", "-q",
        ],
        capture_output=True, text=True, check=True,
    )


def measure(port: int, pid: int, keys: int, requests: int, clients: int) -> dict:
    """One GET round. Returns throughput, latency percentiles, and peak RSS."""
    rss_before = rss_kb(pid)
    proc = subprocess.Popen(
        [
            "redis-benchmark", "-h", "127.0.0.1", "-p", str(port),
            "-t", "GET", "-n", str(requests), "-r", str(keys),
            "-c", str(clients),
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    # Sample RSS while the load runs — a peak that only appears under load is
    # exactly what a memory-for-speed trade would produce, and a before/after
    # reading would miss it.
    peak = rss_before or 0
    while proc.poll() is None:
        cur = rss_kb(pid)
        if cur and cur > peak:
            peak = cur
        time.sleep(0.02)
    out, err = proc.communicate()
    blob = out + err

    lat = LATENCY_RE.search(blob)
    thr = THROUGHPUT_RE.search(blob)
    if not lat or not thr:
        raise RuntimeError(f"could not parse redis-benchmark output:\n{blob[-1500:]}")
    return {
        "rps": float(thr.group(1)),
        "avg_ms": float(lat.group(1)),
        "p50_ms": float(lat.group(3)),
        "p95_ms": float(lat.group(4)),
        "p99_ms": float(lat.group(5)),
        "max_ms": float(lat.group(6)),
        "rss_before_kb": rss_before,
        "rss_peak_kb": peak,
    }


def summarise(name: str, rounds: list[dict], key: str) -> tuple[float, float]:
    """Return (median, floor%) for one metric.

    The floor is the **coefficient of variation**, not max−min spread.

    This was originally max−min, matching `bench_ab.py`'s column, and that
    was wrong here. Max−min is hostage to a single bad sample and grows with
    round count, so adding rounds — which should *increase* confidence — made
    the reported floor worse. Observed directly: an 8-round run where one
    round dipped to 163k rps against a ~232k median reported a 31.2% floor,
    while a noisier 3-round run reported 2.3%. Same binary both arms in both
    cases.

    CV is bounded by the distribution rather than the extremes, so it settles
    as rounds accumulate. (`bench_ab.py` keeps max−min deliberately: there it
    answers "how wrong can one `cargo bench` run be?", which is the right
    question for a warning about single-run numbers. Here the question is
    "can this run resolve the delta?", which is a CV question.)
    """
    vals = [r[key] for r in rounds if r.get(key) is not None]
    if not vals:
        return (0.0, 0.0)
    med = st.median(vals)
    if len(vals) < 2 or not med:
        return (med, 0.0)
    mean = st.mean(vals)
    floor = (st.stdev(vals) / mean * 100.0) if mean else 0.0
    return med, floor


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--baseline", required=True, help="path to the control binary")
    p.add_argument("--candidate", required=True, help="path to the test binary")
    p.add_argument("--rounds", type=int, default=6, help="interleaved rounds per arm")
    p.add_argument("--keys", type=int, default=100_000, help="keyspace size")
    p.add_argument("--requests", type=int, default=200_000, help="GETs per round")
    p.add_argument("--clients", type=int, default=50)
    p.add_argument("--base-port", type=int, default=16810)
    p.add_argument("--output", help="write raw per-round samples to this JSON path")
    args = p.parse_args()

    for tool in ("redis-benchmark", "redis-cli"):
        if subprocess.run(["which", tool], capture_output=True).returncode != 0:
            sys.stderr.write(f"{tool} not found on PATH\n")
            return 2

    base, cand = Path(args.baseline).resolve(), Path(args.candidate).resolve()
    for b in (base, cand):
        if not b.exists():
            sys.stderr.write(f"binary not found: {b}\n")
            return 2

    same = base == cand
    print(f"baseline  {base}")
    print(f"candidate {cand}")
    if same:
        print(
            "\nSAME BINARY BOTH ARMS — this is a noise-floor run. Any delta\n"
            "below is measurement error and is the threshold under which a\n"
            "real comparison means nothing.\n"
        )
    print(f"{args.keys} keys, {args.requests} GETs/round, {args.clients} clients, "
          f"{args.rounds} rounds/arm\n")

    results: dict[str, list[dict]] = {"baseline": [], "candidate": []}
    try:
        for i in range(args.rounds):
            order = [("baseline", base), ("candidate", cand)]
            if i % 2:
                order.reverse()
            for arm, binary in order:
                port = args.base_port + (0 if arm == "baseline" else 1)
                proc = start(binary, port)
                try:
                    preload(port, args.keys)
                    results[arm].append(
                        measure(port, proc.pid, args.keys, args.requests, args.clients)
                    )
                finally:
                    stop(proc)
            print(f"  round {i + 1}/{args.rounds} done")

        print()
        header = f"{'metric':<12} {'baseline':>12} {'candidate':>12} {'delta':>9} {'floor':>7}  verdict"
        print(header)
        # Lower is better for latency; higher is better for throughput.
        for key, label, lower_better in [
            ("rps", "throughput", False),
            ("p50_ms", "p50", True),
            ("p95_ms", "p95", True),
            ("p99_ms", "p99", True),
            ("rss_peak_kb", "peak RSS", True),
        ]:
            mb, sb = summarise("baseline", results["baseline"], key)
            mc, sc = summarise("candidate", results["candidate"], key)
            if not mb:
                continue
            delta = (mc - mb) / mb * 100.0
            floor = max(sb, sc)
            if mb == mc:
                # Identical medians. Happens routinely for latency
                # percentiles, which redis-benchmark reports to 2 decimal
                # places — at sub-millisecond latencies that quantisation is
                # coarser than the effect. Without this branch a 0.00% delta
                # against a 0.0% floor fell through to the comparison below
                # and was labelled "WORSE".
                verdict = "identical (resolution-limited)"
            elif abs(delta) < floor:
                verdict = f"inside noise floor (±{floor:.1f}%)"
            else:
                better = (delta < 0) if lower_better else (delta > 0)
                verdict = "REAL: " + ("better" if better else "WORSE")
            print(
                f"{label:<12} {mb:>12.2f} {mc:>12.2f} {delta:>+8.2f}% "
                f"{floor:>6.1f}%  {verdict}"
            )

        print(
            "\n`floor` is the within-arm coefficient of variation for this run.\n"
            "A delta smaller than it is not a result regardless of its size.\n"
            "Latency and RSS: lower is better. Throughput: higher is better."
        )
        print(
            "\nTail metrics are far noisier than throughput. A same-binary run\n"
            "on the reference host gave a ~1.5% floor on throughput and ~2% on\n"
            "RSS, but p95/p99 routinely exceed 100% — redis-benchmark reports\n"
            "them to 2 decimal places, so at sub-millisecond latencies the\n"
            "quantisation is coarser than the effect. Treat p99 as directional\n"
            "unless the delta is large; raise --rounds until the floor is\n"
            "comfortably below what you want to assert."
        )

        if args.output:
            Path(args.output).write_text(json.dumps({
                "baseline": str(base),
                "candidate": str(cand),
                "noise_floor_mode": same,
                "keys": args.keys,
                "requests": args.requests,
                "clients": args.clients,
                "rounds": args.rounds,
                "unix_time": int(time.time()),
                "samples": results,
            }, indent=2))
            print(f"\nraw samples → {args.output}")
    finally:
        cleanup()
    return 0


if __name__ == "__main__":
    sys.exit(main())
