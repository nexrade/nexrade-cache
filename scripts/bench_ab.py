#!/usr/bin/env python3
"""Paired A/B comparison for the Criterion store benchmarks (1.4.0).

## Why this exists

`cargo bench` compares against whatever criterion last stored in
`target/criterion`, which means a single run tells you how this build
differed from *some earlier build on a differently-loaded machine*. On a
shared or virtualised host that is not a code comparison.

Measured on an idle 24-core WSL2 host, seven consecutive runs of one
**unchanged** binary spanned 130.6–139.8 ns (6.8%), and a longer sequence
produced a run reporting `+21.9% (p = 0.00) Performance has regressed`.
Criterion's `p` value answers "did these two samples differ?", not "is the
code slower" — and on a noisy host the honest answer to the first question
is often yes for identical code.

This script removes the two largest confounders:

1. **Interleaving.** Baseline and candidate alternate A/B/A/B rather than
   running as two blocks, so slow drift (thermal, host contention, another
   tenant) hits both arms equally instead of whichever ran second.
   Interleaving alone took the observed spread from ~7% to ~4%.
2. **Identical binary paths.** Both builds are compiled at the *same*
   filesystem path, because the path string is embedded in the binary and
   changes code layout. A previous investigation traced an apparent
   statistically-significant −8.6% SET "regression" entirely to building
   the baseline in a different directory.

It deliberately does **not** pin CPUs. Pinning to 4 of 24 cores was tested
and made things worse — median 137.8 ns pinned vs 134.4 ns unpinned, with a
wider spread — because the benchmark's own threads then contend for a
smaller core set.

## Usage

    # Compare working tree against its merge-base with main
    python3 scripts/bench_ab.py --baseline main

    # Compare two explicit commits
    python3 scripts/bench_ab.py --baseline 649fd5e --candidate HEAD

    # Establish this session's noise floor (same commit both arms).
    # Run this FIRST. Any delta below the floor it reports is not a result.
    python3 scripts/bench_ab.py --noise-floor

    # Narrow to one benchmark and take more pairs
    python3 scripts/bench_ab.py --baseline main --filter get_single_thread --pairs 12

Reports median, spread, and a Mann-Whitney U p-value per benchmark, plus an
explicit verdict that accounts for the measured floor. Raw samples are
written to JSON so a claim can be re-checked without re-running.

Exits non-zero only on harness failure, never on a performance verdict —
deciding what an 8% delta means is a human's job, not this script's.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import statistics as st
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Criterion's own summary line, e.g.
#   get_single_thread/read_for+get_ro
#                           time:   [133.57 ns 135.51 ns 137.55 ns]
BENCH_NAME_RE = re.compile(r"^(\S+/\S+)\s*$")
TIME_RE = re.compile(
    r"time:\s+\[([\d.]+)\s+(\w+)\s+([\d.]+)\s+(\w+)\s+([\d.]+)\s+(\w+)\]"
)

UNIT_TO_NS = {"ps": 1e-3, "ns": 1.0, "µs": 1e3, "us": 1e3, "ms": 1e6, "s": 1e9}


def run(cmd: list[str], cwd: Path | None = None, timeout: int = 3600) -> str:
    res = subprocess.run(
        cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"command failed ({res.returncode}): {' '.join(cmd)}\n"
            f"stdout:\n{res.stdout[-2000:]}\nstderr:\n{res.stderr[-2000:]}"
        )
    return res.stdout + res.stderr


def parse_criterion(output: str) -> dict[str, float]:
    """Extract {benchmark_name: median_ns} from criterion's stdout.

    Only the point estimate (middle of the three values) is kept; the
    confidence interval criterion prints is *within* one run, whereas the
    variation this script is built to expose is *between* runs.
    """
    results: dict[str, float] = {}
    pending: str | None = None
    for line in output.splitlines():
        stripped = line.strip()
        m = BENCH_NAME_RE.match(stripped)
        if m and "time:" not in stripped and "/" in stripped:
            pending = m.group(1)
            continue
        t = TIME_RE.search(line)
        if t and pending:
            median, unit = float(t.group(3)), t.group(4)
            results[pending] = median * UNIT_TO_NS.get(unit, 1.0)
            pending = None
    return results


def mann_whitney_u(a: list[float], b: list[float]) -> float:
    """Two-sided Mann-Whitney U p-value via a normal approximation.

    Stdlib-only by design — this script must run wherever the release
    binary does, without a pip install. With n=10 per arm the normal
    approximation is adequate for the "is this even worth looking at"
    question it is used for here.
    """
    n1, n2 = len(a), len(b)
    if n1 == 0 or n2 == 0:
        return 1.0
    combined = sorted([(v, 0) for v in a] + [(v, 1) for v in b])
    # Average ranks for ties.
    ranks: list[float] = [0.0] * len(combined)
    i = 0
    while i < len(combined):
        j = i
        while j + 1 < len(combined) and combined[j + 1][0] == combined[i][0]:
            j += 1
        avg = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[k] = avg
        i = j + 1
    r1 = sum(r for r, (_, g) in zip(ranks, combined) if g == 0)
    u1 = r1 - n1 * (n1 + 1) / 2
    u2 = n1 * n2 - u1
    u = min(u1, u2)
    mu = n1 * n2 / 2
    sigma = (n1 * n2 * (n1 + n2 + 1) / 12) ** 0.5
    if sigma == 0:
        return 1.0
    z = (u - mu) / sigma
    # Two-sided normal tail via erf.
    import math

    return max(0.0, min(1.0, math.erfc(abs(z) / math.sqrt(2))))


def build_at(repo: Path, ref: str, build_dir: Path, filter_: str | None) -> Path:
    """Check `ref` out into `build_dir` and build the bench binary there.

    Uses `git worktree` so the caller's working tree is never touched. Both
    arms are built under the same parent path with the same directory name
    so the embedded path strings — and therefore code layout — match.
    """
    if build_dir.exists():
        shutil.rmtree(build_dir, ignore_errors=True)
    run(["git", "worktree", "add", "--detach", str(build_dir), ref], cwd=repo)
    out = run(
        ["cargo", "bench", "-p", "nexrade-core", "--bench", "store_bench", "--no-run"],
        cwd=build_dir,
    )
    m = re.findall(r"Executable benches/store_bench\.rs \(([^)]+)\)", out)
    if not m:
        # Fall back to locating the newest matching artifact.
        cands = sorted(
            (build_dir / "target" / "release" / "deps").glob("store_bench-*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        cands = [c for c in cands if c.suffix != ".d" and c.is_file()]
        if not cands:
            raise RuntimeError(f"could not locate bench binary in {build_dir}")
        return cands[0]
    return (build_dir / m[-1]).resolve()


def bench_once(binary: Path, filter_: str | None, extra: list[str]) -> dict[str, float]:
    cmd = [str(binary), "--bench"]
    if filter_:
        cmd.append(filter_)
    cmd += extra
    return parse_criterion(run(cmd, timeout=3600))


def bootstrap_false_positive(
    samples: list[float], k: int, threshold_pct: float, trials: int = 3000
) -> float:
    """Fraction of median-of-k-pair comparisons of IDENTICAL data that would
    report a delta of at least `threshold_pct`.

    This is the number that actually matters for sizing a comparison. Raw
    spread (max−min of single runs) describes individual observations and
    grows with sample count; averaging k of them shrinks the error on the
    *estimate*. A host where one run can be 24% off can still resolve a 10%
    effect reliably, given enough pairs — which is why an early 1.4.0 draft
    that reasoned from spread alone reached the wrong conclusion.

    **Needs a decent number of input samples to be meaningful.** The bootstrap
    resamples the observed values, so with only a handful of them it is
    drawing from a coarse discrete distribution whose tails are dominated by
    whichever extreme values happened to be observed. Measured directly: for
    the same benchmark at essentially the same CV (4.1% vs 3.8%), bootstrapping
    from 6 samples reported P(≥5%) = 6.8% at k=8, while 12 samples reported
    1.3% — a 5× overestimate that would have demanded 24 pairs instead of 12.
    `run_sizing` warns below `MIN_SAMPLES_FOR_BOOTSTRAP`.
    """
    import random

    rng = random.Random(1729)  # fixed so a rerun reproduces the table
    hits = 0
    for _ in range(trials):
        a = st.median([rng.choice(samples) for _ in range(k)])
        b = st.median([rng.choice(samples) for _ in range(k)])
        if a and abs(b - a) / a * 100.0 >= threshold_pct:
            hits += 1
    return hits / trials * 100.0


# Below this many repetitions the bootstrap's tail estimate is unreliable and
# biased towards demanding too many pairs. See bootstrap_false_positive.
MIN_SAMPLES_FOR_BOOTSTRAP = 10


def run_sizing(repo: Path, args) -> int:
    """Characterise the current build: how many pairs to resolve an effect."""
    import math

    print(
        f"Sizing run: {args.sizing} repetitions of the current build.\n"
        "No comparison is made — this measures how noisy THIS host is and\n"
        "how many pairs a future verdict would need.\n"
    )
    if args.sizing < MIN_SAMPLES_FOR_BOOTSTRAP:
        print(
            f"WARNING: {args.sizing} repetitions is below the recommended "
            f"{MIN_SAMPLES_FOR_BOOTSTRAP}.\n"
            "         The k@ columns will be biased HIGH (they demand more\n"
            "         pairs than actually needed) because the bootstrap is\n"
            "         resampling too few distinct values. Treat them as an\n"
            "         upper bound, and prefer --sizing 12 for a number you\n"
            "         intend to write down.\n"
        )
    out = run(
        ["cargo", "bench", "-p", "nexrade-core", "--bench", "store_bench", "--no-run"],
        cwd=repo,
    )
    m = re.findall(r"Executable benches/store_bench\.rs \(([^)]+)\)", out)
    if m:
        binary = (repo / m[-1]).resolve()
    else:
        cands = sorted(
            (repo / "target" / "release" / "deps").glob("store_bench-*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        cands = [c for c in cands if c.suffix != ".d" and c.is_file()]
        if not cands:
            print("could not locate the bench binary", file=sys.stderr)
            return 2
        binary = cands[0]

    acc: dict[str, list[float]] = {}
    for i in range(args.sizing):
        for name, ns in bench_once(binary, args.filter, args.bench_arg).items():
            acc.setdefault(name, []).append(ns)
        print(f"  run {i + 1}/{args.sizing} done")

    print()
    w = max((len(n) for n in acc), default=20)
    print(
        f"{'benchmark':<{w}}  {'n':>2}  {'median':>12}  {'cv':>5}  "
        f"{'spread':>6}  {'k@10%':>6}  {'k@5%':>5}"
    )
    for name in sorted(acc):
        v = acc[name]
        if len(v) < 3:
            continue
        med = st.median(v)
        cv = st.stdev(v) / st.mean(v) * 100.0
        spread = (max(v) - min(v)) / med * 100.0
        # Smallest k (from a practical ladder) whose bootstrapped
        # false-positive rate at that threshold is under 1%.
        def need(threshold: float) -> str:
            for k in (4, 8, 12, 16, 24, 32, 48):
                if bootstrap_false_positive(v, k, threshold, trials=1500) < 1.0:
                    return str(k)
            return ">48"

        print(
            f"{name:<{w}}  {len(v):>2}  {med:>10.2f}ns  {cv:>4.1f}%  "
            f"{spread:>5.1f}%  {need(10.0):>6}  {need(5.0):>5}"
        )

    print(
        "\n`k@10%` / `k@5%` = --pairs needed so that comparing identical code\n"
        "reports a delta that large less than 1% of the time. Spread is a\n"
        "single-run statistic; CV and k are what govern a verdict."
    )
    if args.output:
        Path(args.output).write_text(
            json.dumps(
                {
                    "mode": "sizing",
                    "reps": args.sizing,
                    "filter": args.filter,
                    "unix_time": int(time.time()),
                    "samples_ns": acc,
                },
                indent=2,
            )
        )
        print(f"\nraw samples → {args.output}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--baseline",
        help="git ref for the control arm (default: merge-base with main)",
    )
    p.add_argument(
        "--candidate",
        default="HEAD",
        help="git ref for the test arm (default: %(default)s)",
    )
    p.add_argument(
        "--noise-floor",
        action="store_true",
        help="build the SAME ref for both arms to measure this host's floor. "
        "Run this before trusting any comparison.",
    )
    p.add_argument(
        "--pairs",
        type=int,
        default=8,
        help="interleaved A/B pairs to collect (default: %(default)s)",
    )
    p.add_argument("--filter", help="criterion benchmark filter, e.g. get_single_thread")
    p.add_argument("--output", help="write raw samples to this JSON path")
    p.add_argument(
        "--bench-arg",
        action="append",
        default=[],
        help="extra arg passed to the bench binary (repeatable)",
    )
    p.add_argument(
        "--sizing",
        metavar="N",
        type=int,
        help="skip the A/B comparison; instead run the current build N times "
        "and report, per benchmark, the CV and the number of pairs needed to "
        "resolve a 10%% and a 5%% effect. Use this to size a future comparison "
        "or to characterise a new host.",
    )
    args = p.parse_args()

    repo = Path(run(["git", "rev-parse", "--show-toplevel"]).strip())

    if args.sizing:
        return run_sizing(repo, args)

    if args.noise_floor:
        base_ref = cand_ref = args.candidate
    else:
        base_ref = args.baseline
        if not base_ref:
            base_ref = run(["git", "merge-base", "HEAD", "main"], cwd=repo).strip()
        cand_ref = args.candidate

    base_sha = run(["git", "rev-parse", base_ref], cwd=repo).strip()
    cand_sha = run(["git", "rev-parse", cand_ref], cwd=repo).strip()

    print(f"baseline  {base_ref}  ({base_sha[:9]})")
    print(f"candidate {cand_ref}  ({cand_sha[:9]})")
    if args.noise_floor:
        print(
            "\nNOISE FLOOR MODE — both arms are the same commit. Any delta\n"
            "reported below is measurement error, and sets the threshold\n"
            "under which a real comparison means nothing.\n"
        )
    elif base_sha == cand_sha:
        print(
            "\nNote: both refs resolve to the same commit, so this is a\n"
            "noise-floor run whether or not --noise-floor was passed.\n"
        )

    # Both worktrees live under one parent with fixed names, so the embedded
    # path strings differ only in the final component and stay the same
    # length — keeping layout effects comparable between arms.
    parent = Path(tempfile.mkdtemp(prefix="nexrade-ab-"))
    a_dir, b_dir = parent / "arm_a", parent / "arm_b"
    try:
        print("building baseline …")
        a_bin = build_at(repo, base_sha, a_dir, args.filter)
        print("building candidate …")
        b_bin = build_at(repo, cand_sha, b_dir, args.filter)

        samples: dict[str, dict[str, list[float]]] = {}
        for i in range(args.pairs):
            # Alternate which arm goes first so neither systematically
            # inherits a warmer cache.
            order = [("baseline", a_bin), ("candidate", b_bin)]
            if i % 2:
                order.reverse()
            for arm, binary in order:
                res = bench_once(binary, args.filter, args.bench_arg)
                for name, ns in res.items():
                    samples.setdefault(name, {"baseline": [], "candidate": []})
                    samples[name][arm].append(ns)
            done = i + 1
            print(f"  pair {done}/{args.pairs} done")

        print()
        rows = []
        for name in sorted(samples):
            base = samples[name]["baseline"]
            cand = samples[name]["candidate"]
            if not base or not cand:
                continue
            mb, mc = st.median(base), st.median(cand)
            delta = (mc - mb) / mb * 100.0
            pval = mann_whitney_u(base, cand)
            spread_b = (max(base) - min(base)) / mb * 100.0
            spread_c = (max(cand) - min(cand)) / mc * 100.0
            floor = max(spread_b, spread_c)
            rows.append((name, mb, mc, delta, pval, floor))

        w = max((len(r[0]) for r in rows), default=20)
        print(f"{'benchmark':<{w}}  {'base':>10}  {'cand':>10}  {'delta':>8}  "
              f"{'p':>6}  {'floor':>6}  verdict")
        for name, mb, mc, delta, pval, floor in rows:
            if abs(delta) < floor:
                verdict = f"inside noise floor (±{floor:.1f}%)"
            elif pval >= 0.05:
                verdict = "not significant"
            else:
                verdict = "REAL: " + ("slower" if delta > 0 else "faster")
            print(
                f"{name:<{w}}  {mb:>8.2f}ns  {mc:>8.2f}ns  {delta:>+7.2f}%  "
                f"{pval:>6.3f}  {floor:>5.1f}%  {verdict}"
            )

        print(
            "\n`floor` is the within-arm spread this run actually observed. A "
            "delta\nsmaller than it is not a result no matter what p says."
        )

        if args.output:
            payload = {
                "baseline_ref": base_ref,
                "baseline_sha": base_sha,
                "candidate_ref": cand_ref,
                "candidate_sha": cand_sha,
                "noise_floor_mode": bool(args.noise_floor) or base_sha == cand_sha,
                "pairs": args.pairs,
                "filter": args.filter,
                "unix_time": int(time.time()),
                "samples_ns": samples,
            }
            Path(args.output).write_text(json.dumps(payload, indent=2))
            print(f"\nraw samples → {args.output}")
    finally:
        for d in (a_dir, b_dir):
            if d.exists():
                run(["git", "worktree", "remove", "--force", str(d)], cwd=repo)
        shutil.rmtree(parent, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
