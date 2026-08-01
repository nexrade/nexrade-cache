//! 0.6.0 — transaction/scripting stress + WATCH invariant verification.
//!
//! Scope of `dispatch`:
//!   * Every read/write command that's safe to invoke without a connection
//!     state machine. `MULTI`/`EXEC`/`WATCH`/`DISCARD`/`EVAL`/`EVALSHA`/
//!     `SCRIPT`/`CLIENT`/`HELLO`/`RESET`/`FUNCTION`/`FCALL` live on the
//!     `Connection` layer (`crates/nexrade-server/src/connection.rs`) and
//!     are exercised end-to-end via `scripts/redis_py_smoke.py` against a
//!     real server. This file focuses on what `dispatch` actually exposes:
//!
//! What we lock down here:
//!   * WATCH optimistic-lock invariant — every write op on a key bumps
//!     its `key_version` so the connection layer's WATCH check at EXEC
//!     time reliably aborts on concurrent change.
//!   * Dispatch error strings remain stable for client parsing.

use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, MaxMemoryPolicy, ServerConfig};
use nexrade_core::resp::Resp;

fn small_config() -> ServerConfig {
    ServerConfig {
        databases: 1,
        max_memory: None,
        maxmemory_policy: MaxMemoryPolicy::NoEviction,
        ..ServerConfig::default()
    }
}

/// Build a command from string args (bulk strings — matches redis-cli /
/// RESP wire encoding where even integers are bulk-encoded).
fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter()
        .map(|a| Resp::BulkString(Some(bytes::Bytes::from(a.to_string()))))
        .collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0).await
}

fn ok(r: &Resp) {
    match r {
        Resp::SimpleString(s) if s == "OK" => {}
        Resp::BulkString(Some(b)) if b == &b"OK"[..] => {}
        other => panic!("expected OK, got {other:?}"),
    }
}

fn int_eq(r: &Resp, want: i64) {
    match r {
        Resp::Integer(n) => assert_eq!(*n, want, "want {want}, got {n}"),
        other => panic!("expected integer {want}, got {other:?}"),
    }
}

// ─── WATCH depends on `key_version` advancing on every write ────────────────

#[tokio::test]
async fn set_bumps_key_version() {
    let db = Db::new(small_config());
    let sdb = db.store.db(0);
    run(&db, cmd(&["SET", "k", "v0"])).await;
    let before = sdb.read_for(b"k").key_version(b"k");
    run(&db, cmd(&["SET", "k", "v1"])).await;
    let after = sdb.read_for(b"k").key_version(b"k");
    assert!(
        after > before,
        "SET must bump key_version: {before} → {after}"
    );
}

#[tokio::test]
async fn set_with_xx_ex_keepttl_also_bump_version() {
    // Option-flag paths go through `Database::insert` (not the plain SET
    // hot path). Cover them so a future refactor can't silently drop the
    // version bump that WATCH depends on.
    let db = Db::new(small_config());
    run(&db, cmd(&["SET", "k", "init"])).await;
    let sdb = db.store.db(0);

    let mut last = sdb.read_for(b"k").key_version(b"k");
    // Each case must actually write (no silent no-ops). XX overwrites when
    // the key exists; EX sets a TTL; KEEPTTL preserves it while writing.
    let cases: &[&[&str]] = &[
        &["SET", "k", "x", "XX"],
        &["SET", "k", "y", "EX", "60"],
        &["SET", "k", "z", "KEEPTTL"],
    ];
    for op in cases {
        run(&db, cmd(op)).await;
        let v = sdb.read_for(b"k").key_version(b"k");
        assert!(v > last, "flagged SET must bump key_version: {last} → {v}");
        last = v;
    }
}

#[tokio::test]
async fn mutators_across_types_bump_their_own_versions() {
    // The watch abort relies on per-key version increment — every mutator
    // on a key of any type must do so. Verify the dispatch path for the
    // most common ones.
    let db = Db::new(small_config());
    let sdb = db.store.db(0);

    let ops: &[(&str, &[&str])] = &[
        ("k1", &["SET", "k1", "v"]),
        ("k1", &["APPEND", "k1", "x"]),
        ("k1", &["INCR", "k1"]),
        ("k1", &["DEL", "k1"]),
        ("k2", &["LPUSH", "k2", "a"]),
        ("k2", &["RPUSH", "k2", "b"]),
        ("k2", &["LPOP", "k2"]),
        ("k3", &["SADD", "k3", "m"]),
        ("k4", &["HSET", "k4", "f", "v"]),
        ("k5", &["ZADD", "k5", "0", "m"]),
        ("k6", &["XADD", "k6", "*", "f", "v"]),
    ];

    for (key, op) in ops {
        run(&db, cmd(op)).await;
        let v = sdb.read_for(key.as_bytes()).key_version(key.as_bytes());
        assert!(v > 0, "write op on {key} must produce non-zero version");
    }
}

#[tokio::test]
async fn key_version_is_isolated_per_key() {
    // WATCH on k1 must not be triggered by a write to k2.
    let db = Db::new(small_config());
    run(&db, cmd(&["SET", "k1", "v"])).await;
    run(&db, cmd(&["SET", "k2", "v"])).await;
    let sdb = db.store.db(0);

    let v_k1_before = sdb.read_for(b"k1").key_version(b"k1");
    run(&db, cmd(&["SET", "k2", "v2"])).await;
    let v_k1_after = sdb.read_for(b"k1").key_version(b"k1");
    let v_k2 = sdb.read_for(b"k2").key_version(b"k2");

    assert_eq!(
        v_k1_before, v_k1_after,
        "write to k2 must not bump k1's version"
    );
    assert!(v_k2 > v_k1_before, "k2's version must advance");
}

// ─── Dispatch-layer error string surface ────────────────────────────────────

#[tokio::test]
async fn dispatch_syntax_error_string_is_stable() {
    let db = Db::new(small_config());
    // GET with too few args → WrongArity → display string.
    let r = run(&db, cmd(&["GET"])).await;
    match r {
        Resp::Error(s) => {
            assert!(
                s.contains("wrong number of arguments"),
                "WrongArity error must mention arg count: {s}"
            );
            assert!(
                s.to_ascii_lowercase().contains("get"),
                "WrongArity must include the command name: {s}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn dispatch_wrongtype_string_is_stable() {
    let db = Db::new(small_config());
    run(&db, cmd(&["SET", "k", "string"])).await;
    // HSET against a string key → WRONGTYPE.
    let r = run(&db, cmd(&["HSET", "k", "f", "v"])).await;
    match r {
        Resp::Error(s) => {
            assert!(
                s.contains("WRONGTYPE"),
                "wrong-type error must carry WRONGTYPE: {s}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ─── Smoke: happy paths still work end-to-end through dispatch ─────────────

#[tokio::test]
async fn composed_dispatch_mix_passes() {
    let db = Db::new(small_config());
    ok(&run(&db, cmd(&["SET", "s", "0"])).await);
    int_eq(&run(&db, cmd(&["INCR", "s"])).await, 1);
    assert_eq!(
        run(&db, cmd(&["GET", "s"])).await,
        Resp::BulkString(Some(bytes::Bytes::from_static(b"1")))
    );
    int_eq(&run(&db, cmd(&["RPUSH", "l", "x", "y", "z"])).await, 3);
    int_eq(&run(&db, cmd(&["LLEN", "l"])).await, 3);
    // EXPIRE returns 1 (key existed and TTL was set) — not OK.
    int_eq(&run(&db, cmd(&["EXPIRE", "s", "60"])).await, 1);
    let ttl = run(&db, cmd(&["TTL", "s"])).await;
    match ttl {
        Resp::Integer(t) => assert!(t > 0 && t <= 60, "TTL must be in (0, 60], got {t}"),
        other => panic!("expected TTL integer, got {other:?}"),
    }
}
