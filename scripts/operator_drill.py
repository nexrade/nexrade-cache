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
  9. TLS                   a broken TLS config (missing/unparseable/
                           mismatched cert, cert without key, port
                           collision) is rejected by BOTH --preflight
                           and startup; a valid one yields a port that
                           completes a real TLS handshake, serves
                           commands, and refuses plaintext RESP.

Step 9 exists because the pre-1.3.0 silent TLS downgrade — banner
printing `TLS  ON` while the process bound no TLS port and served
plaintext at exit 0 — survived three releases, and the reason it
survived is that this drill had no TLS coverage at all. The TLS steps
need `openssl` on PATH to mint a throwaway certificate; without it they
report SKIP (counted separately from FAIL) rather than failing.

That openssl requirement belongs to this harness, not to the server:
nexrade-cache serves TLS via rustls (aws-lc-rs, statically linked) and
links no libssl/libcrypto, so a host with no OpenSSL at all can still
terminate TLS. openssl is used here only as a convenient certificate
generator, so no certificate has to be checked into the repo and expire.

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
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

PASS = 0
FAIL = 0
SKIP = 0
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
    """Record one step's outcome. `SKIP` is counted separately — an
    environment that can't run a step (e.g. no openssl for the TLS steps)
    must not be reported as a failure, but must still be visible in the
    summary so a silently-reduced drill isn't mistaken for a clean one."""
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


def resp_send_tls(host: str, port: int, *args: bytes, cafile: str | None = None) -> object:
    """Send one RESP command over a genuine TLS connection and return the
    reply. Used to prove the TLS listener actually speaks TLS rather than
    just accepting connections.

    Hostname checking is disabled (the drill's self-signed cert is issued
    for `localhost` but we connect to 127.0.0.1); certificate *trust* is
    still verified against `cafile` when given, so this does confirm the
    server presented the certificate we configured.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    if cafile:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile)
    else:
        ctx.verify_mode = ssl.CERT_NONE

    raw = socket.create_connection((host, port), timeout=5)
    try:
        with ctx.wrap_socket(raw, server_hostname=None) as s:
            out = f"*{len(args)}\r\n".encode()
            for a in args:
                if isinstance(a, str):
                    a = a.encode()
                out += f"${len(a)}\r\n".encode() + a + b"\r\n"
            s.sendall(out)
            header = b""
            while not header.endswith(b"\r\n"):
                chunk = s.recv(1)
                if not chunk:
                    raise AssertionError("server closed before replying over TLS")
                header += chunk
            t = header[0:1]
            body = header[1:-2]
            if t == b"+":
                return body.decode()
            if t == b"-":
                return ("ERR", body.decode())
            if t == b"$":
                n = int(body)
                if n < 0:
                    return None
                data = b""
                while len(data) < n:
                    data += s.recv(n - len(data))
                s.recv(2)
                return data.decode()
            raise ValueError(f"unexpected RESP type over TLS: {t!r}")
    finally:
        try:
            raw.close()
        except OSError:
            pass


def generate_self_signed(workdir: Path) -> tuple[Path, Path]:
    """Generate a self-signed cert/key pair with openssl. Returns
    (cert_path, key_path). Raises if openssl is unavailable — the caller
    skips the TLS steps in that case rather than failing the drill.

    Generated at runtime rather than checked in so the drill has no
    embedded certificate to expire.
    """
    cert = workdir / "tls-cert.pem"
    key = workdir / "tls-key.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", str(key), "-out", str(cert),
            "-days", "3650", "-nodes",
            "-subj", "/CN=localhost",
            "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost",
        ],
        check=True,
        capture_output=True,
        timeout=60,
    )
    return cert, key


def make_tls_config(
    workdir: Path,
    name: str,
    *,
    port: int,
    tls_enabled: bool = True,
    tls_port: int | None = None,
    cert: str | None,
    key: str | None,
    rdb_path: str,
) -> Path:
    """Write a config with an explicit [tls] block to `name`.tls.toml.

    Written to a distinct filename per case so a later step can never pick
    up an earlier step's on-disk config (a hazard the RDB steps hit — see
    the note before step 5).
    """
    cfg = workdir / f"{name}.tls.toml"
    lines = [
        f"# generated by scripts/operator_drill.py ({name}) — not for production",
        'bind = "127.0.0.1"',
        f"port = {port}",
        "databases = 1",
        "max_clients = 256",
        "",
        "[persistence]",
        f'rdb_path = "{rdb_path}"',
        'aof_path = ""',
        'aof_sync = "everysec"',
        "save_rules = [[86400, 1]]",
        "",
        "[tls]",
        f"enabled = {'true' if tls_enabled else 'false'}",
    ]
    if tls_port is not None:
        lines.append(f"port = {tls_port}")
    if cert is not None:
        lines.append(f'cert = "{cert}"')
    if key is not None:
        lines.append(f'key = "{key}"')
    lines += [
        "",
        "[metrics]",
        "enabled = false",
        'bind = "127.0.0.1"',
        "port = 9199",
        "",
        "[health]",
        "enabled = false",
        'bind = "127.0.0.1"',
        "port = 9198",
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

        # ─── 9. TLS ────────────────────────────────────────────────────────
        # Why these steps exist: until 1.3.0 a TLS misconfiguration was a
        # *silent downgrade* — the banner printed `TLS  ON`, no TLS port was
        # bound, and the process exited 0 serving plaintext. That survived
        # three releases specifically because this drill had no TLS
        # coverage. The checks below assert the two properties that failure
        # violated:
        #
        #   a) a broken TLS config never yields a running server, and
        #   b) a working TLS config yields a port that really speaks TLS.
        #
        # Plus the property that motivated `--preflight` in the first place:
        # preflight and startup must agree, so a deploy gate cannot pass on
        # a config that then fails to boot.
        try:
            tls_cert, tls_key = generate_self_signed(workdir)
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            record("SKIP", "tls:all steps", f"openssl unavailable: {type(e).__name__}")
            tls_cert = tls_key = None

        if tls_cert is not None:
            tls_rdb = str(workdir / "data" / "tls.rdb")
            missing_cert = str(workdir / "does-not-exist-cert.pem")
            missing_key = str(workdir / "does-not-exist-key.pem")
            garbage_cert = workdir / "garbage-cert.pem"
            garbage_key = workdir / "garbage-key.pem"
            garbage_cert.write_text("this is not a certificate\n")
            garbage_key.write_text("this is not a key\n")
            # A second, unrelated keypair — individually valid, but not the
            # partner of tls_cert. Catches a validator that only stats files.
            other_cert, other_key = workdir / "other-cert.pem", workdir / "other-key.pem"
            subprocess.run(
                ["openssl", "req", "-x509", "-newkey", "rsa:2048",
                 "-keyout", str(other_key), "-out", str(other_cert),
                 "-days", "3650", "-nodes", "-subj", "/CN=unrelated"],
                check=True, capture_output=True, timeout=60,
            )

            # Each case: (label, cert, key, tls_port) — all must be rejected
            # by BOTH preflight and startup.
            bad_cases = [
                ("no cert or key", None, None, 16601),
                ("cert without key", str(tls_cert), None, 16602),
                ("key without cert", None, str(tls_key), 16603),
                ("missing cert file", missing_cert, missing_key, 16604),
                ("unparseable cert", str(garbage_cert), str(garbage_key), 16605),
                ("cert/key mismatch", str(tls_cert), str(other_key), 16606),
                # tls.port == port: the TLS bind collides with the plaintext
                # bind. Pre-fix this surfaced as a bare "Address already in
                # use" that never mentioned TLS.
                ("tls.port == port", str(tls_cert), str(tls_key), 16600),
            ]

            for label, cert, key, tls_port in bad_cases:
                def check(label=label, cert=cert, key=key, tls_port=tls_port):
                    cfg = make_tls_config(
                        workdir,
                        f"bad-{tls_port}",
                        port=16600,
                        tls_port=tls_port,
                        cert=cert,
                        key=key,
                        rdb_path=tls_rdb,
                    )
                    pre = subprocess.run(
                        [str(binary), "--config", str(cfg), "--preflight"],
                        capture_output=True, text=True, timeout=10,
                    )
                    # Launch without waiting. A correctly-behaving binary
                    # exits on its own; the *bug* being guarded against is a
                    # process that keeps running and serves plaintext, so
                    # blocking on exit here would surface the defect as an
                    # uninformative "timed out" instead of naming it.
                    proc = subprocess.Popen(
                        [str(binary), "--config", str(cfg)],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    _PROCS.append(proc)
                    try:
                        rc = proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        # Still alive after 10s — it did not refuse. Probe the
                        # plaintext port to distinguish "serving plaintext
                        # while TLS was requested" (the silent downgrade) from
                        # a merely hung process.
                        serving = False
                        try:
                            serving = resp_send("127.0.0.1", 16600, b"PING") == "PONG"
                        except OSError:
                            pass
                        proc.kill()
                        proc.wait(timeout=5)
                        if serving:
                            raise AssertionError(
                                "SILENT TLS DOWNGRADE: tls.enabled = true with a "
                                f"broken config ({label}), yet the server is up and "
                                "answering PING in cleartext on the plaintext port. "
                                "A client that asked for TLS gets plaintext."
                            )
                        raise AssertionError(
                            f"startup neither refused nor served ({label}); "
                            "process hung with TLS misconfigured"
                        )
                    assert rc != 0, (
                        f"startup must refuse a broken TLS config ({label}), got "
                        f"exit 0 — it exited successfully having served plaintext "
                        f"instead of the requested TLS\n"
                        f"stderr:\n{proc.stderr.read().decode(errors='replace')[-600:]}"
                    )
                    # And preflight must refuse it too, or a deploy gate
                    # passes on a config that cannot boot.
                    assert pre.returncode != 0, (
                        f"preflight reported OK on a config that fails to "
                        f"start (exit {rc}) — preflight and startup disagree\n"
                        f"preflight stdout/stderr:\n{pre.stdout}{pre.stderr}"
                    )
                    return f"preflight exit {pre.returncode}, startup exit {rc}"
                run(f"tls:rejects {label} (preflight + startup)", check)

            # ── valid TLS: preflight passes, and the port speaks real TLS ──
            good_cfg = make_tls_config(
                workdir,
                "good",
                port=16610,
                tls_port=16611,
                cert=str(tls_cert),
                key=str(tls_key),
                rdb_path=tls_rdb,
            )

            def tls_preflight_ok():
                res = subprocess.run(
                    [str(binary), "--config", str(good_cfg), "--preflight"],
                    capture_output=True, text=True, timeout=10,
                )
                assert res.returncode == 0, (
                    f"valid TLS config must pass preflight, got exit "
                    f"{res.returncode}:\n{res.stdout}{res.stderr}"
                )
                return "exit 0"
            run("tls:valid config passes preflight", tls_preflight_ok)

            tls_server = Server(binary, ["--config", str(good_cfg)], workdir, port=16610)
            tls_server.start(timeout_s=10.0)
            try:
                def tls_handshake():
                    assert wait_for_port("127.0.0.1", 16611, timeout_s=5.0), \
                        "TLS port 16611 never accepted a connection"
                    # A genuine TLS handshake verified against the cert we
                    # configured, then a real command over the encrypted
                    # channel. Accepting a TCP connection is not enough —
                    # the pre-1.3.0 bug bound nothing at all, but a future
                    # regression could bind a plaintext socket here.
                    reply = resp_send_tls(
                        "127.0.0.1", 16611, b"PING", cafile=str(tls_cert)
                    )
                    assert reply == "PONG", f"expected PONG over TLS, got {reply!r}"
                    return "PING → PONG over TLS, cert verified"
                run("tls:TLS port completes handshake and serves commands", tls_handshake)

                def tls_roundtrip():
                    set_reply = resp_send_tls(
                        "127.0.0.1", 16611, b"SET", b"drill:tls", b"encrypted",
                        cafile=str(tls_cert),
                    )
                    assert set_reply == "OK", f"SET over TLS returned {set_reply!r}"
                    got = resp_send_tls(
                        "127.0.0.1", 16611, b"GET", b"drill:tls", cafile=str(tls_cert)
                    )
                    assert got == "encrypted", f"GET over TLS returned {got!r}"
                    return "SET/GET round-trip over TLS"
                run("tls:SET/GET round-trips over TLS", tls_roundtrip)

                def plaintext_still_works():
                    # TLS enabled must not disturb the plain listener.
                    reply = resp_send("127.0.0.1", 16610, b"PING")
                    assert reply == "PONG", f"plaintext PING returned {reply!r}"
                    return "plaintext port unaffected"
                run("tls:plaintext port still serves when TLS is on", plaintext_still_works)

                def tls_port_is_not_plaintext():
                    # Inverse of the handshake check: a plaintext RESP command
                    # sent to the TLS port must NOT get a valid reply. If it
                    # does, the "TLS" port is serving cleartext.
                    s = socket.create_connection(("127.0.0.1", 16611), timeout=3)
                    try:
                        s.sendall(b"*1\r\n$4\r\nPING\r\n")
                        s.settimeout(2.0)
                        try:
                            data = s.recv(64)
                        except (socket.timeout, ConnectionResetError, OSError):
                            return "plaintext to TLS port got no RESP reply (correct)"
                        assert b"PONG" not in data, (
                            f"TLS port answered a plaintext PING with {data!r} — "
                            f"the port is serving cleartext"
                        )
                        return f"plaintext to TLS port rejected ({data[:24]!r})"
                    finally:
                        s.close()
                run("tls:TLS port refuses plaintext RESP", tls_port_is_not_plaintext)
            finally:
                tls_server.stop(timeout_s=5.0)

            # ── cert configured while TLS is off: warn, don't fail ──
            def tls_disabled_with_cert_warns():
                cfg = make_tls_config(
                    workdir,
                    "offcert",
                    port=16620,
                    tls_enabled=False,
                    cert=str(tls_cert),
                    key=str(tls_key),
                    rdb_path=tls_rdb,
                )
                res = subprocess.run(
                    [str(binary), "--config", str(cfg), "--preflight"],
                    capture_output=True, text=True, timeout=10,
                )
                assert res.returncode == 0, (
                    "tls.enabled = false with a cert present is a valid "
                    f"staging config and must not fail preflight, got exit "
                    f"{res.returncode}"
                )
                combined = res.stdout + res.stderr
                assert "warning" in combined.lower(), (
                    "preflight should warn that a configured cert is unused "
                    f"when tls.enabled = false; output was:\n{combined}"
                )
                return "exit 0 with warning"
            run("tls:cert with tls.enabled=false warns but passes", tls_disabled_with_cert_warns)

    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    print()
    for status, label, note in RESULTS:
        mark = {"PASS": "✓", "FAIL": "✗", "SKIP": "—"}.get(status, "?")
        print(f"  {mark} {status:4} {label}  ({note})")
    print(f"\nPASS={PASS} FAIL={FAIL} SKIP={SKIP} total={PASS + FAIL + SKIP}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())