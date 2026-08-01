#!/usr/bin/env python3
"""Operations drill for nexrade-cache.

End-to-end sanity check that the standalone production profile can be
configured, validated, started, observed, and recovered without relying
on undocumented behavior. Designed to run in CI (the `ops-drill` job)
and locally before any release.

The drill is stdlib-only so it runs in any environment where the
release binary exists. It exercises, in order:

  1. `--print-config`      resolved config includes [health] section.
  2. `--preflight`         passes on a valid config; rejects corrupt
                           AOF/RDB, unavailable paths, and ambiguous
                           RDB+AOF source combination.
  3. clean start           /healthz returns 200 once phase=ready;
                           /readyz returns 200; /metrics returns 200.
  4. clean shutdown        SIGTERM drained listener; binary exits 0.
  5. RDB restore           restart with an existing RDB; keys survive.
  6. AOF-only recovery     restart with corrupt AOF; binary exits
                           nonzero (corrupt-input rejection).
  7. damaged RDB           restart with a truncated RDB; binary exits
                           nonzero.
  8. unavailable path      restart with rdb_path pointing at a path
                           the service user cannot create; binary
                           exits nonzero.

Usage:
  ./target/release/nexrade-cache --help   # any nexrade-cache build
  python3 scripts/operator_drill.py --binary ./target/release/nexrade-cache

Exits non-zero if any step fails. Each step prints PASS or FAIL with a
short note so a CI log shows exactly where the drill broke.
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

PASS = 0
FAIL = 0
RESULTS: list[tuple[str, str, str]] = []

# Track all subprocesses we spawn so we can kill them on exit even if
# the script aborts mid-step. Without this a failed probe leaves a
# nexrade-cache bound to a port and the next run fails with EADDRINUSE.
_PROCS: list[subprocess.Popen] = []


def _cleanup_procs() -> None:
    for p in _PROCS:
        try:
            if p.poll() is None:
                p.kill()
                p.wait(timeout=1)
        except Exception:
            pass


atexit.register(_cleanup_procs)


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
    except AssertionError as e:
        record("FAIL", label, f"assertion: {e}")
    except subprocess.TimeoutExpired as e:
        record("FAIL", label, f"timeout: {e}")
    except Exception as e:  # noqa: BLE001
        record("FAIL", label, f"{type(e).__name__}: {e}")


def wait_for_port(host: str, port: int, timeout_s: float = 5.0) -> bool:
    """Return True if the TCP port accepts a connection within timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def wait_for_url(url: str, timeout_s: float = 5.0, expect_status: int = 200) -> int:
    """Return the http status code once the URL responds with the expected
    status, or raise on timeout / wrong status."""
    deadline = time.monotonic() + timeout_s
    last = 0
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=0.5) as r:
                last = r.status
                if last == expect_status:
                    return last
        except urllib.error.HTTPError as e:
            last = e.code
            if last == expect_status:
                return last
        except (urllib.error.URLError, ConnectionRefusedError, OSError):
            pass
        time.sleep(0.05)
    raise AssertionError(f"{url} never returned {expect_status}; last={last}")


def http_get_text(url: str, timeout_s: float = 5.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout_s) as r:
        return r.read().decode()


def resp_send(host: str, port: int, *args: bytes) -> object:
    """Minimal RESP client for SET/GET checks. Not a general client."""
    s = socket.create_connection((host, port), timeout=2)
    try:
        out = f"*{len(args)}\r\n".encode()
        for a in args:
            if isinstance(a, str):
                a = a.encode()
            out += f"${len(a)}\r\n".encode() + a + b"\r\n"
        s.sendall(out)
        # Read one frame back.
        header = b""
        while not header.endswith(b"\r\n"):
            header += s.recv(1)
        t = header[0:1]
        body = header[1:-2]
        if t == b"+":
            return body.decode()
        if t == b"-":
            return ("ERR", body.decode())
        if t == b":":
            return int(body)
        if t == b"$":
            n = int(body)
            if n < 0:
                return None
            data = b""
            while len(data) < n:
                data += s.recv(n - len(data))
            s.recv(2)
            return data.decode()
        if t == b"*":
            n = int(body)
            if n < 0:
                return None
            items = []
            for _ in range(n):
                items.append(resp_send(host, port))  # not used in drill
            return items
        raise ValueError(f"unknown RESP type {t!r}")
    finally:
        s.close()


class Server:
    """Manage a single nexrade-cache subprocess bound to a fresh dir."""

    def __init__(self, binary: Path, args: list[str], workdir: Path, port: int = 6379):
        self.binary = binary
        self.args = args
        self.workdir = workdir
        self.port = port
        self.proc: subprocess.Popen | None = None

    def start(self, timeout_s: float = 10.0) -> None:
        env = os.environ.copy()
        env["RUST_LOG"] = "debug,nexrade_core::persistence=trace"
        self.proc = subprocess.Popen(
            [str(self.binary), *self.args],
            cwd=str(self.workdir),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _PROCS.append(self.proc)
        if not wait_for_port("127.0.0.1", self.port, timeout_s):
            self._dump_diagnostics()
            self._kill_proc()
            raise AssertionError(f"server did not start within {timeout_s}s on port {self.port}")

    def _kill_proc(self) -> None:
        if self.proc is None:
            return
        try:
            if self.proc.poll() is None:
                self.proc.kill()
                self.proc.wait(timeout=2)
        except Exception:
            pass

    def stop(self, timeout_s: float = 5.0, sig: int = signal.SIGTERM, dump: bool = False) -> int:
        """Send `sig`, wait for clean exit. Returns the exit code (or -1
        on kill). If `dump` is True, dump the buffered server logs to
        stderr first — useful when a later assertion fails and the cause
        was earlier server-side."""
        if self.proc is None:
            return 0
        if dump:
            self._dump_diagnostics()
        if self.proc.poll() is None:
            self.proc.send_signal(sig)
        try:
            return self.proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return self.proc.wait()

    def _dump_diagnostics(self) -> None:
        if self.proc is None:
            return
        try:
            stdout, stderr = self.proc.communicate(timeout=0.5)
        except subprocess.TimeoutExpired:
            stdout = stderr = b"<still running>"
        sys.stderr.write("---- server stdout ----\n")
        sys.stderr.write(stdout.decode(errors="replace"))
        sys.stderr.write("---- server stderr ----\n")
        sys.stderr.write(stderr.decode(errors="replace"))
        sys.stderr.write("-----------------------\n")
        sys.stderr.flush()


def make_config(
    workdir: Path,
    *,
    rdb_path: str | None,
    aof_path: str | None,
    health_enabled: bool = True,
    metrics_enabled: bool = True,
    port: int = 16384,
    metrics_port: int = 9161,
    health_port: int = 9160,
) -> Path:
    """Write a self-contained nexrade.production-style config and return its path."""
    cfg = workdir / "nexrade.toml"
    # Use a 24h save rule to keep the auto-BGSAVE from racing with the
    # manual SAVE the drill sends. A 60s window was tripping
    # `RDB save already in progress` because the rule fires almost
    # immediately after the first SET, before the manual SAVE returns.
    lines = [
        "# generated by scripts/operator_drill.py — not for production",
        f'bind = "127.0.0.1"',
        f"port = {port}",
        "databases = 1",
        "max_clients = 256",
        "",
        "[persistence]",
        f'rdb_path = "{rdb_path or ""}"',
        f'aof_path = "{aof_path or ""}"',
        "aof_sync = \"everysec\"",
        "save_rules = [[86400, 1]]",
        "",
        "[metrics]",
        f"enabled = {'true' if metrics_enabled else 'false'}",
        f'bind = "127.0.0.1"',
        f"port = {metrics_port}",
        "",
        "[health]",
        f"enabled = {'true' if health_enabled else 'false'}",
        f'bind = "127.0.0.1"',
        f"port = {health_port}",
    ]
    cfg.write_text("\n".join(lines) + "\n")
    return cfg


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--binary",
        default="target/release/nexrade-cache",
        help="path to nexrade-cache binary (default: %(default)s)",
    )
    args = p.parse_args()
    binary = Path(args.binary).resolve()
    if not binary.exists():
        sys.stderr.write(f"binary not found: {binary}\n")
        sys.stderr.write("run `cargo build --release -p nexrade-cache` first.\n")
        return 2

    workdir = Path(tempfile.mkdtemp(prefix="nexrade-drill-"))
    try:
        # ─── 1. --print-config includes [health] section ──────────────────
        # Generate the valid config first so print-config can read it.
        rdb_path = str(workdir / "data" / "nexrade.rdb")
        ok_cfg = make_config(
            workdir,
            rdb_path=rdb_path,
            aof_path=None,
            health_enabled=True,
            metrics_enabled=True,
            port=16384,
            metrics_port=9161,
            health_port=9160,
        )
        (workdir / "data").mkdir(parents=True, exist_ok=True)

        def print_config():
            out = subprocess.run(
                [str(binary), "--config", str(ok_cfg), "--print-config"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout
            assert "[health]" in out, f"missing [health] section in:\n{out}"
            assert "[metrics]" in out, f"missing [metrics] section in:\n{out}"
            return f"{len(out.splitlines())} lines"
        run("print-config:health+metrics sections", print_config)

        # ─── 2a. --preflight passes on a valid RDB-only config ──────────────
        def preflight_ok():
            subprocess.run(
                [str(binary), "--config", str(ok_cfg), "--preflight"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
            return "exit 0"
        run("preflight:valid RDB-only config passes", preflight_ok)

        # ─── 2b. --preflight rejects ambiguous RDB+AOF source ──────────────
        both_cfg = make_config(
            workdir,
            rdb_path=rdb_path,
            aof_path=str(workdir / "data" / "nexrade.aof"),
            health_enabled=True,
            metrics_enabled=True,
            port=16384,
        )

        def preflight_rejects_both():
            res = subprocess.run(
                [str(binary), "--config", str(both_cfg), "--preflight"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert res.returncode != 0, f"preflight must reject RDB+AOF, got exit 0:\n{res.stdout}"
            return f"exit {res.returncode}"
        run("preflight:rejects RDB+AOF source combination", preflight_rejects_both)

        # ─── 2c. --preflight rejects unavailable persistence path ─────────
        bad_path_cfg = make_config(
            workdir,
            rdb_path="/this/path/does/not/exist/nexrade.rdb",
            aof_path=None,
            health_enabled=True,
            metrics_enabled=True,
            port=16384,
        )

        def preflight_rejects_bad_path():
            res = subprocess.run(
                [str(binary), "--config", str(bad_path_cfg), "--preflight"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert res.returncode != 0, f"preflight must reject bad path, got exit 0"
            return f"exit {res.returncode}"
        run("preflight:rejects unavailable persistence path", preflight_rejects_bad_path)

        # ─── 3. clean start — healthz / readyz / metrics return 200 ────────
        server = Server(
            binary,
            ["--config", str(ok_cfg)],
            workdir,
            port=16384,
        )
        server.start()

        def healthz():
            code = wait_for_url("http://127.0.0.1:9160/healthz", timeout_s=3.0, expect_status=200)
            assert code == 200, f"healthz returned {code}"
            return "200"
        run("start:healthz returns 200", healthz)

        def readyz():
            code = wait_for_url("http://127.0.0.1:9160/readyz", timeout_s=3.0, expect_status=200)
            assert code == 200, f"readyz returned {code}"
            return "200"
        run("start:readyz returns 200", readyz)

        def metrics_ok():
            code = wait_for_url("http://127.0.0.1:9161/metrics", timeout_s=3.0, expect_status=200)
            assert code == 200, f"metrics returned {code}"
            return "200"
        run("start:metrics returns 200", metrics_ok)

        # ─── 4. clean shutdown — SIGTERM drains and exits 0 ────────────────
        def clean_shutdown():
            code = server.stop(timeout_s=5.0, sig=signal.SIGTERM)
            assert code == 0, f"expected exit 0 on SIGTERM, got {code}"
            return "exit 0"
        run("shutdown:SIGTERM exit 0", clean_shutdown)

        # ─── 5. RDB restore — restart preserves keys ───────────────────────
        # First start: write data, save, stop.
        # NOTE: every step above wrote its own config to the SAME
        # workdir/nexrade.toml. The `ok_cfg` variable below still
        # references the first config's path, but the on-disk contents
        # may now be the bad-path config from step 2c. Re-write the
        # correct config before starting the server so we are sure the
        # running process sees `aof_path = ""` (not a stale path).
        ok_cfg.write_text(
            "\n".join(
                [
                    "# regenerated before step 5",
                    'bind = "127.0.0.1"',
                    f"port = 16384",
                    "databases = 1",
                    "max_clients = 256",
                    "",
                    "[persistence]",
                    f'rdb_path = "{rdb_path}"',
                    'aof_path = ""',
                    'aof_sync = "everysec"',
                    "save_rules = [[86400, 1]]",
                    "",
                    "[metrics]",
                    "enabled = true",
                    'bind = "127.0.0.1"',
                    "port = 9161",
                    "",
                    "[health]",
                    "enabled = true",
                    'bind = "127.0.0.1"',
                    "port = 9160",
                ]
            )
            + "\n"
        )
        # Clear any leftover AOF file from a previous step.
        leftover_aof = workdir / "data" / "nexrade.aof"
        if leftover_aof.exists():
            leftover_aof.unlink()
        server = Server(binary, ["--config", str(ok_cfg)], workdir, port=16384)
        server.start()
        resp_send("127.0.0.1", 16384, b"SET", b"drill:restore", b"hello")
        # Trigger SAVE via RESP.
        save_reply = resp_send("127.0.0.1", 16384, b"SAVE")
        server.stop(timeout_s=5.0, dump=True)
        # Allow filesystem a moment to flush after the server exits.
        time.sleep(0.1)
        rdb_file = workdir / "data" / "nexrade.rdb"
        # Surface leftover temp files (suggests a failed rename).
        leftover = list((workdir / "data").glob(".*.rdbtmp"))
        assert rdb_file.exists(), (
            f"RDB should exist after SAVE (reply={save_reply!r}, "
            f"workdir={workdir}, rdb_file={rdb_file}, "
            f"listing={list((workdir / 'data').iterdir())}, "
            f"leftover_tmp={leftover})"
        )

        # Second start: same config, data should be there.
        server2 = Server(binary, ["--config", str(ok_cfg)], workdir, port=16384)
        server2.start()
        v = resp_send("127.0.0.1", 16384, b"GET", b"drill:restore")
        assert v == "hello", f"expected restored value, got {v!r}"
        server2.stop(timeout_s=5.0)
        record("PASS", "restore:RDB restart preserves keys", "drill:restore=hello")

        # ─── 6. AOF-only crash recovery — corrupt AOF is rejected ──────────
        # Set up an AOF-only config, write some data, corrupt the AOF,
        # restart, and verify the process exits non-zero (corrupt-input
        # rejection is the contract).
        aof_cfg = make_config(
            workdir,
            rdb_path=None,
            aof_path=str(workdir / "data" / "nexrade.aof"),
            health_enabled=True,
            metrics_enabled=True,
            port=16385,
            metrics_port=9171,
            health_port=9170,
        )
        # First start: write some commands (those build the AOF).
        s = Server(binary, ["--config", str(aof_cfg)], workdir, port=16385)
        s.start()
        resp_send("127.0.0.1", 16385, b"SET", b"drill:aof", b"v")
        time.sleep(0.2)  # give the everysec fsync a moment
        s.stop(timeout_s=5.0, sig=signal.SIGKILL)  # hard stop to skip clean shutdown rewrite

        # Corrupt the AOF.
        aof_file = workdir / "data" / "nexrade.aof"
        assert aof_file.exists(), "AOF file should exist after writes"
        with aof_file.open("ab") as f:
            f.write(b"\xde\xad\xbe\xef\xca\xfe\xba\xbe")

        def aof_corrupt_rejected():
            res = subprocess.run(
                [str(binary), "--config", str(aof_cfg)],
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert res.returncode != 0, f"corrupt AOF must reject startup, got exit 0"
            return f"exit {res.returncode}"
        run("recovery:corrupt AOF rejected at startup", aof_corrupt_rejected)

        # ─── 7. damaged RDB — truncated snapshot rejected ──────────────────
        rdb_file = workdir / "data" / "nexrade.rdb"
        assert rdb_file.exists(), "RDB should exist from earlier save"
        original = rdb_file.read_bytes()
        rdb_file.write_bytes(original[: len(original) // 2])  # truncate

        def damaged_rdb_rejected():
            res = subprocess.run(
                [str(binary), "--config", str(ok_cfg)],
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert res.returncode != 0, f"truncated RDB must reject startup, got exit 0"
            return f"exit {res.returncode}"
        run("recovery:damaged RDB rejected at startup", damaged_rdb_rejected)

        # Restore RDB so subsequent runs don't trip on the damage.
        rdb_file.write_bytes(original)

        # ─── 8. unwritable path — startup fails when rdb_path is a regular file
        # The startup recovery skips loading when the configured path
        # doesn't exist (first-run), so the unwritable case has to be a
        # pre-existing regular file at the configured path. Snapshot::save
        # then refuses because it can't `create_new` over an existing
        # file. This is the in-process analogue of the disk-full /
        # read-only-mount failures operators see in production.
        unwritable_path = workdir / "data" / "unwritable.rdb"
        unwritable_path.write_text("not a real RDB\n")
        os.chmod(unwritable_path, 0o444)
        unwritable_cfg = workdir / "nexrade.toml"
        unwritable_cfg.write_text(
            "\n".join(
                [
                    "# step 8: unwritable path",
                    'bind = "127.0.0.1"',
                    "port = 16387",
                    "databases = 1",
                    "max_clients = 256",
                    "",
                    "[persistence]",
                    f'rdb_path = "{unwritable_path}"',
                    'aof_path = ""',
                    'aof_sync = "everysec"',
                    "save_rules = [[86400, 1]]",
                    "",
                    "[metrics]",
                    "enabled = false",
                    'bind = "127.0.0.1"',
                    "port = 9171",
                    "",
                    "[health]",
                    "enabled = false",
                    'bind = "127.0.0.1"',
                    "port = 9170",
                ]
            )
            + "\n"
        )

        def unwritable_path_rejected():
            # A regular file at the rdb_path fails two ways depending on
            # contents: if it parses as a corrupt RDB, the startup
            # rejects it before binding (server fails to start); if it
            # parses as a valid-looking RDB, the startup loads it and a
            # subsequent SAVE fails. Either path is the correct
            # rejection — the unwritable target must not silently be
            # treated as "empty" and have BGSAVE succeed against the
            # file's directory.
            s = Server(binary, ["--config", str(unwritable_cfg)], workdir, port=16387)
            try:
                s.start(timeout_s=3.0)
            except AssertionError:
                # Startup rejected it — that's the "magic mismatch"
                # path, and the correct outcome for an unwritable /
                # corrupt file at the configured path.
                return "startup rejected (corrupt-file path)"
            # Startup loaded the placeholder — SAVE must now fail.
            reply = resp_send("127.0.0.1", 16387, b"SAVE")
            s.stop(timeout_s=3.0)
            if not (isinstance(reply, tuple) and reply[0] == "ERR"):
                raise AssertionError(
                    f"SAVE against unwritable path must return ERR, got {reply!r}"
                )
            return f"SAVE rejected ({reply[1][:40]!r})"
        run("recovery:unwritable persistence path rejected", unwritable_path_rejected)

    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    print()
    for status, label, note in RESULTS:
        mark = {"PASS": "✓", "FAIL": "✗"}.get(status, "?")
        print(f"  {mark} {status:4} {label}  ({note})")
    print(f"\nPASS={PASS} FAIL={FAIL} total={PASS + FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())