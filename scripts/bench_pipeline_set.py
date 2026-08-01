#!/usr/bin/env python3
"""Paired Redis 7.0.15 / nexrade pipeline benchmark.

Runs alternating server rounds and keeps raw samples so a transient host change
cannot be reported as a performance win. The harness starts both servers with
persistence and metrics disabled, checks nexrade's INFO state, then compares
SET/GET/INCR under the canonical pipeline shape.

Example:
  python3 scripts/bench_pipeline_set.py --rounds 12 --output /tmp/nexrade-perf

Requires a built target/release/nexrade-cache plus redis-server and
redis-benchmark on PATH. Redis 7.0.15 is required by default because that is
the project comparator.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

RPS = re.compile(r"^(SET|GET|INCR):\s+([0-9.]+) requests per second", re.MULTILINE)


def run(*args: str, check: bool = True, **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, check=check, capture_output=True, **kwargs)


def wait_ping(port: int, proc: subprocess.Popen[str]) -> None:
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError(f"server on {port} exited early: {proc.stderr.read()}")
        ping = run("redis-cli", "-p", str(port), "PING", check=False)
        if ping.returncode == 0 and ping.stdout.strip() == "PONG":
            return
        time.sleep(0.05)
    raise RuntimeError(f"server on {port} did not become ready")


def benchmark(port: int, pipeline: int, clients: int, requests: int, random_keys: bool) -> dict[str, float]:
    args = [
        "redis-benchmark", "-h", "127.0.0.1", "-p", str(port),
        "-c", str(clients), "-n", str(requests), "-P", str(pipeline),
        "-t", "SET,GET,INCR", "-q",
    ]
    if random_keys:
        args.extend(["-r", "100000"])
    output = run(*args).stdout
    result = {command: float(rps) for command, rps in RPS.findall(output)}
    if set(result) != {"SET", "GET", "INCR"}:
        raise RuntimeError(f"unexpected redis-benchmark output:\n{output}")
    return result


def stop(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nexrade", default="target/release/nexrade-cache")
    parser.add_argument("--rounds", type=int, default=12)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--pipeline", type=int, default=50)
    parser.add_argument("--requests", type=int, default=500000)
    parser.add_argument("--random-keys", action="store_true")
    parser.add_argument("--allow-any-redis", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("perf-pipeline-set"))
    args = parser.parse_args()

    if args.rounds < 4:
        parser.error("--rounds must be at least 4")
    nexrade = Path(args.nexrade).resolve()
    if not nexrade.is_file():
        parser.error(f"nexrade binary not found: {nexrade}")
    for binary in ("redis-server", "redis-benchmark", "redis-cli"):
        if not shutil.which(binary):
            parser.error(f"required command missing: {binary}")

    redis_version = run("redis-server", "--version").stdout.strip()
    if "v=7.0.15" not in redis_version and not args.allow_any_redis:
        parser.error(f"requires Redis 7.0.15, found: {redis_version}; pass --allow-any-redis to override")

    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="nexrade-pipeline-set-") as temp:
        root = Path(temp)
        config = root / "nexrade.toml"
        config.write_text('[persistence]\nrdb_path = ""\naof_path = ""\n', encoding="utf-8")
        redis_port, nexrade_port = 16530, 16531
        redis = subprocess.Popen([
            "redis-server", "--port", str(redis_port), "--save", "", "--appendonly", "no",
            "--bind", "127.0.0.1", "--dir", str(root), "--dbfilename", "redis.rdb",
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        nex = subprocess.Popen([
            str(nexrade), "--config", str(config), "--port", str(nexrade_port),
            "--metrics", "false", "--log-level", "warn",
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            wait_ping(redis_port, redis)
            wait_ping(nexrade_port, nex)
            info = run("redis-cli", "-p", str(nexrade_port), "INFO", "persistence").stdout
            if "aof_enabled:0" not in info:
                raise RuntimeError(f"nexrade persistence unexpectedly enabled:\n{info}")
            warm_requests = min(5000, args.requests)
            for port in (redis_port, nexrade_port):
                benchmark(port, 1, min(10, args.clients), warm_requests, args.random_keys)

            servers = [("nexrade", nexrade_port), ("redis", redis_port), ("redis", redis_port), ("nexrade", nexrade_port)]
            samples: list[dict[str, object]] = []
            while len(samples) < args.rounds * 2:
                for name, port in servers:
                    if len(samples) >= args.rounds * 2:
                        break
                    run("redis-cli", "-p", str(port), "FLUSHALL")
                    rps = benchmark(port, args.pipeline, args.clients, args.requests, args.random_keys)
                    samples.append({"server": name, "rps": rps})
            grouped: dict[str, dict[str, list[float]]] = {name: {cmd: [] for cmd in ("SET", "GET", "INCR")} for name in ("redis", "nexrade")}
            for sample in samples:
                for cmd, value in sample["rps"].items():
                    grouped[sample["server"]][cmd].append(value)
            summary = {}
            for cmd in ("SET", "GET", "INCR"):
                redis_values = grouped["redis"][cmd]
                nex_values = grouped["nexrade"][cmd]
                summary[cmd] = {
                    "redis_median": statistics.median(redis_values),
                    "nexrade_median": statistics.median(nex_values),
                    "redis_mean": statistics.mean(redis_values),
                    "nexrade_mean": statistics.mean(nex_values),
                    "ratio_median": statistics.median(nex_values) / statistics.median(redis_values),
                    "nexrade_stdev": statistics.stdev(nex_values) if len(nex_values) > 1 else 0.0,
                }
            report = {
                "redis_version": redis_version,
                "redis_benchmark_version": run("redis-benchmark", "--version").stdout.strip(),
                "nexrade_binary": str(nexrade),
                "workload": vars(args),
                "samples": samples,
                "summary": summary,
            }
            (args.output / "pipeline-set.json").write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
            print(f"Redis: {redis_version}")
            print(f"Rounds per server: {args.rounds}; -c {args.clients}; -P {args.pipeline}; -n {args.requests}")
            for cmd, data in summary.items():
                print(
                    f"{cmd:4s} Redis median {data['redis_median']:10.1f}  "
                    f"nexrade median {data['nexrade_median']:10.1f}  "
                    f"ratio {data['ratio_median']:.3f}x  "
                    f"({(data['ratio_median'] - 1) * 100:+.1f}%)"
                )
            print(f"Raw samples: {args.output / 'pipeline-set.json'}")
        finally:
            stop(nex)
            stop(redis)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
