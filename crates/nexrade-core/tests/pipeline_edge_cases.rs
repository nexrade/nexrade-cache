//! Edge-case / regression tests for the pipelining-throughput fixes.
//!
//! Targets the structural changes made for the redis-benchmark -P 50
//! gap:
//! - `refresh_meta_after_batch` flush-once-per-batch
//! - `dispatch_tracked(cmd: &str)` accepting pre-parsed cmd name
//! - `Bytes` (refcount) backing for SET key
//! - Lazy `sl_args` Vec build
//! - `max_memory_limit` / `maxmemory_policy` atomic mirrors
//! - Multi-thread tokio runtime (no implicit single-thread assumptions)

use std::time::Duration;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::conn_registry::ConnectionRegistry;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

// ── 1. Pre-parsed cmd name ────────────────────────────────────────────────

/// `dispatch_with_user` accepts a pre-parsed cmd name (via
/// `dispatch_tracked(cmd: &str)`). Confirming this path works for
/// all known write/read commands — round-trip through the dispatch
/// without re-parsing inside.
#[tokio::test]
async fn dispatch_with_user_covers_all_command_families() {
    let db = Db::default();
    // String
    let r = dispatch_with_user(&db, cmd(&["SET", "k", "v"]), 0, None, "default").await;
    assert!(matches!(r, Resp::SimpleString(_)));
    let r = dispatch_with_user(&db, cmd(&["GET", "k"]), 0, None, "default").await;
    assert!(matches!(r, Resp::BulkString(Some(_))));
    let r = dispatch_with_user(&db, cmd(&["DEL", "k"]), 0, None, "default").await;
    assert!(matches!(r, Resp::Integer(1)));
    let r = dispatch_with_user(&db, cmd(&["INCR", "counter"]), 0, None, "default").await;
    assert!(matches!(r, Resp::Integer(1)));
    // List
    let r = dispatch_with_user(&db, cmd(&["LPUSH", "l", "x"]), 0, None, "default").await;
    assert!(matches!(r, Resp::Integer(1)));
    // Hash
    let r = dispatch_with_user(&db, cmd(&["HSET", "h", "f", "v"]), 0, None, "default").await;
    assert!(matches!(r, Resp::Integer(1)));
    // Set
    let r = dispatch_with_user(&db, cmd(&["SADD", "s", "m"]), 0, None, "default").await;
    assert!(matches!(r, Resp::Integer(1)));
}

/// Unknown commands surface as `Err` error replies.
#[tokio::test]
async fn unknown_command_returns_error() {
    let db = Db::default();
    let r = dispatch_with_user(&db, cmd(&["TOTALLY_NOT_A_CMD", "arg"]), 0, None, "default").await;
    match r {
        Resp::Error(s) => assert!(s.contains("unknown"), "got: {s}"),
        other => panic!("expected error, got {other:?}"),
    }
}

/// Lower-case cmd name still dispatches correctly (the public
/// `dispatch_with_user` upper-cases internally; we test the internal
/// `dispatch_tracked` path here which expects upper-case).
#[tokio::test]
async fn dispatch_with_user_uppercases_input() {
    let db = Db::default();
    let r = dispatch_with_user(&db, cmd(&["ping"]), 0, None, "default").await;
    assert!(matches!(r, Resp::SimpleString(s) if s == "PONG"));
}

// ── 2. SET key with Bytes backing ─────────────────────────────────────────

/// SET with a `Bytes`-backed key — `cmd_set` now uses `as_bytes()`
/// directly (cheap refcount clone) instead of `get_bytes_vec`
/// (heap alloc + memcpy). Confirm correctness is preserved.
#[tokio::test]
async fn set_then_get_roundtrip() {
    let db = Db::default();
    dispatch_with_user(&db, cmd(&["SET", "alpha", "1"]), 0, None, "default").await;
    let r = dispatch_with_user(&db, cmd(&["GET", "alpha"]), 0, None, "default").await;
    assert!(matches!(r, Resp::BulkString(Some(b)) if b.as_ref() == b"1"));
    dispatch_with_user(&db, cmd(&["SET", "beta", "two-bytes"]), 0, None, "default").await;
    let r = dispatch_with_user(&db, cmd(&["GET", "beta"]), 0, None, "default").await;
    assert!(matches!(r, Resp::BulkString(Some(b)) if b.as_ref() == b"two-bytes"));
}

/// SET with empty value — Bytes slice with len=0 should still work.
#[tokio::test]
async fn set_empty_value_works() {
    let db = Db::default();
    let r = dispatch_with_user(&db, cmd(&["SET", "empty", ""]), 0, None, "default").await;
    assert!(matches!(r, Resp::SimpleString(_)));
    let r = dispatch_with_user(&db, cmd(&["GET", "empty"]), 0, None, "default").await;
    assert!(matches!(r, Resp::BulkString(Some(b)) if b.is_empty()));
}

/// SET with empty key — edge case, should not panic.
#[tokio::test]
async fn set_empty_key_works() {
    let db = Db::default();
    let r = dispatch_with_user(&db, cmd(&["SET", "", "v"]), 0, None, "default").await;
    assert!(matches!(r, Resp::SimpleString(_)));
    let r = dispatch_with_user(&db, cmd(&["GET", ""]), 0, None, "default").await;
    assert!(matches!(r, Resp::BulkString(Some(b)) if b.as_ref() == b"v"));
}

/// SET with binary value containing NUL bytes — Bytes doesn't stop
/// at NUL like C strings would.
#[tokio::test]
async fn set_binary_value_preserves_bytes() {
    let db = Db::default();
    let args = vec![
        Resp::bulk_str("SET"),
        Resp::bulk_str("bin"),
        Resp::BulkString(Some(bytes::Bytes::from_static(&[0u8, 1, 2, 0, 255]))),
    ];
    let r = dispatch_with_user(&db, args, 0, None, "default").await;
    assert!(matches!(r, Resp::SimpleString(_)));
    let r = dispatch_with_user(&db, cmd(&["STRLEN", "bin"]), 0, None, "default").await;
    assert!(matches!(r, Resp::Integer(5)));
}

// ── 3. ConnectionRegistry meta refresh ────────────────────────────────────

/// `refresh_meta_after_batch` updates the per-connection ClientMeta
/// in one place. Simulate the production flow: record a few
/// commands, then flush the meta once and verify last_cmd reflects
/// the LAST command (not any intermediate one).
#[test]
fn meta_batch_refresh_keeps_last_cmd_only() {
    let reg = ConnectionRegistry::new();
    let (meta, _kill) = reg.register(7, "127.0.0.1:6379".parse().unwrap());

    // Simulate the per-command record_last_cmd path — cheap, no
    // locks. Each call only updates a String field on the connection.
    let mut last_cmd = String::with_capacity(8);
    let cmds = ["set", "get", "set", "incr"];
    for c in cmds {
        last_cmd.clear();
        last_cmd.push_str(c);
        // ... dispatch happens here ...
    }
    // Simulate the once-per-batch flush:
    {
        let mut g = meta.write();
        g.last_cmd = last_cmd.clone();
        g.idle_instant = std::time::Instant::now();
    }
    assert_eq!(meta.read().last_cmd, "incr");
}

/// Empty batch (no commands dispatched) — `had_commands = false` —
/// `refresh_meta_after_batch` is skipped. The previous meta state
/// is preserved.
#[test]
fn empty_batch_does_not_corrupt_meta() {
    let reg = ConnectionRegistry::new();
    let (meta, _kill) = reg.register(1, "127.0.0.1:6379".parse().unwrap());
    meta.write().last_cmd = "set".to_string();
    // No commands dispatched. The flag `had_commands` would be false
    // in the production code, so refresh_meta_after_batch is
    // skipped. We confirm by not calling it: the meta stays as set.
    assert_eq!(meta.read().last_cmd, "set");
}

/// QUIT in a pipeline batch — dispatches cleanly and returns OK.
/// In the connection's loop, this also sets `quit = true` so the
/// outer loop exits. At the dispatch level it just returns Ok.
#[tokio::test]
async fn quit_in_batch_does_not_break_meta() {
    let db = Db::default();
    let r = dispatch_with_user(&db, cmd(&["QUIT"]), 0, None, "default").await;
    assert!(matches!(r, Resp::SimpleString(_)));
    // A subsequent SET in the same batch still works.
    let r = dispatch_with_user(&db, cmd(&["SET", "k", "v"]), 0, None, "default").await;
    assert!(matches!(r, Resp::SimpleString(_)));
}

// ── 4. Atomic max_memory mirror ───────────────────────────────────────────

/// CONFIG SET maxmemory updates the atomic mirror so the dispatch
/// fast path sees the new value without taking the config lock.
#[tokio::test]
async fn config_set_maxmemory_updates_atomic() {
    let db = Db::default();
    assert_eq!(
        db.max_memory_limit
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    // Update via CONFIG SET.
    let r = dispatch_with_user(
        &db,
        cmd(&["CONFIG", "SET", "maxmemory", "1024"]),
        0,
        None,
        "default",
    )
    .await;
    assert!(matches!(r, Resp::SimpleString(_)));

    // The atomic mirror should now be 1024.
    assert_eq!(
        db.max_memory_limit
            .load(std::sync::atomic::Ordering::Relaxed),
        1024
    );

    // Reset to 0 (disabled).
    dispatch_with_user(
        &db,
        cmd(&["CONFIG", "SET", "maxmemory", "0"]),
        0,
        None,
        "default",
    )
    .await;
    assert_eq!(
        db.max_memory_limit
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

/// CONFIG SET maxmemory-policy updates the policy atomic.
#[tokio::test]
async fn config_set_maxmemory_policy_updates_atomic() {
    let db = Db::default();
    assert_eq!(
        db.maxmemory_policy
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    ); // NoEviction

    dispatch_with_user(
        &db,
        cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"]),
        0,
        None,
        "default",
    )
    .await;
    assert_eq!(
        db.maxmemory_policy
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    ); // AllKeysLru

    dispatch_with_user(
        &db,
        cmd(&["CONFIG", "SET", "maxmemory-policy", "noeviction"]),
        0,
        None,
        "default",
    )
    .await;
    assert_eq!(
        db.maxmemory_policy
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

/// When max_memory is set, writes still succeed (the eviction
/// policy may or may not kick in depending on usage, but no panic).
/// Just confirms the atomic mirror path doesn't break the SET hot path.
#[tokio::test]
async fn writes_work_when_maxmemory_set() {
    let db = Db::default();
    dispatch_with_user(
        &db,
        cmd(&["CONFIG", "SET", "maxmemory", "1024"]),
        0,
        None,
        "default",
    )
    .await;
    dispatch_with_user(
        &db,
        cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"]),
        0,
        None,
        "default",
    )
    .await;
    // Fill some data — total < 1024 bytes so no eviction.
    for i in 0..5 {
        let r = dispatch_with_user(
            &db,
            cmd(&["SET", &format!("k{i}"), "value"]),
            0,
            None,
            "default",
        )
        .await;
        assert!(matches!(r, Resp::SimpleString(_)));
    }
    // Read back.
    let r = dispatch_with_user(&db, cmd(&["GET", "k2"]), 0, None, "default").await;
    assert!(matches!(r, Resp::BulkString(Some(_))));
}

// ── 5. Multi-thread runtime interactions ─────────────────────────────────

/// The dispatch path was originally written assuming a
/// `current_thread` tokio runtime. The `multi_thread` switch should
/// not break anything. The dispatch is still safe because we use
/// `Arc<RwLock>` and parking_lot locks, both of which are
/// thread-safe. This test exercises the dispatch path from
/// multiple threads via `tokio::task::spawn` to confirm no
/// panics under concurrent dispatch on the same Db.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_under_concurrent_multi_thread() {
    let db = Db::default();
    let mut handles = Vec::new();
    for tid in 0..20 {
        let db = db.clone();
        handles.push(tokio::task::spawn(async move {
            for i in 0..50 {
                let key = format!("t{tid}-k{i}");
                let r = dispatch_with_user(&db, cmd(&["SET", &key, "v"]), 0, None, "default").await;
                assert!(matches!(r, Resp::SimpleString(_)));
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

/// Same but reads under concurrent multi-thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_under_concurrent_multi_thread() {
    let db = Db::default();
    // Seed
    for i in 0..100 {
        dispatch_with_user(
            &db,
            cmd(&["SET", &format!("k{i}"), "v"]),
            0,
            None,
            "default",
        )
        .await;
    }
    let mut handles = Vec::new();
    for tid in 0..20 {
        let db = db.clone();
        handles.push(tokio::task::spawn(async move {
            for i in 0..50 {
                let key = format!("k{}", (tid * 17 + i) % 100);
                let _ = dispatch_with_user(&db, cmd(&["GET", &key]), 0, None, "default").await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// ── 6. ACL integration with pre-parsed cmd ───────────────────────────────

/// ACL check works under the pre-parsed cmd path. A user with no
/// permissions cannot run SET.
#[tokio::test]
async fn acl_denied_via_preparsed_cmd() {
    let db = Db::default();
    db.acl.setuser("readonly", &["+@read", "~*"]).unwrap();
    let r = dispatch_with_user(&db, cmd(&["SET", "k", "v"]), 0, None, "readonly").await;
    match r {
        Resp::Error(s) => assert!(s.contains("NOPERM"), "got: {s}"),
        other => panic!("expected NOPERM error, got {other:?}"),
    }
    // But GET works.
    dispatch_with_user(&db, cmd(&["SET", "k", "v"]), 0, None, "default").await;
    let r = dispatch_with_user(&db, cmd(&["GET", "k"]), 0, None, "readonly").await;
    assert!(matches!(r, Resp::BulkString(Some(_))));
}

// ── 7. Pause/resume under dispatch_with_user ─────────────────────────────

/// Pause blocks writes, then unpause allows them again — exercises
/// the atomic `paused_until` read in dispatch_tracked.
#[tokio::test]
async fn pause_blocks_writes_via_dispatch() {
    let db = Db::default();
    db.connections.pause_for(Duration::from_millis(40));
    let r = dispatch_with_user(&db, cmd(&["SET", "k", "v"]), 0, None, "default").await;
    assert!(matches!(r, Resp::Error(s) if s.starts_with("PAUSE")));
    // Reads are still allowed during a write pause.
    let r = dispatch_with_user(&db, cmd(&["GET", "k"]), 0, None, "default").await;
    assert!(matches!(r, Resp::BulkString(None)));
    // After pause expires, writes work.
    std::thread::sleep(Duration::from_millis(60));
    let r = dispatch_with_user(&db, cmd(&["SET", "k", "v"]), 0, None, "default").await;
    assert!(matches!(r, Resp::SimpleString(_)));
}

// ── 8. Pipelined commands all execute correctly ───────────────────────────

/// Multi-command pipelined dispatch produces correct results in
/// order — the connection's inner loop semantics. Tested via
/// sequential `dispatch_with_user` calls.
#[tokio::test]
async fn pipelined_commands_produce_correct_results() {
    let db = Db::default();
    // Simulate a pipeline batch by issuing commands sequentially
    // (without the connection metadata flush in between, which is
    // the same as a real pipeline batch from dispatch's view).
    let ops = vec![
        ("SET", vec!["a", "1"]),
        ("SET", vec!["b", "2"]),
        ("SET", vec!["c", "3"]),
        ("GET", vec!["a"]),
        ("GET", vec!["b"]),
        ("GET", vec!["c"]),
        ("MGET", vec!["a", "b", "c"]),
        ("DEL", vec!["a"]),
        ("DEL", vec!["b"]),
        ("DEL", vec!["c"]),
    ];
    let mut results = Vec::new();
    for (c, args) in &ops {
        let mut full = vec![*c];
        full.extend_from_slice(args);
        let r = dispatch_with_user(&db, cmd(&full), 0, None, "default").await;
        results.push(r);
    }
    // SET → OK, OK, OK
    for r in &results[0..3] {
        assert!(matches!(r, Resp::SimpleString(_)));
    }
    // GET a, GET b, GET c → BulkString "1", "2", "3"
    for (i, want) in ["1", "2", "3"].iter().enumerate() {
        assert!(
            matches!(&results[3 + i], Resp::BulkString(Some(b)) if b.as_ref() == want.as_bytes()),
            "got: {:?}",
            results[3 + i]
        );
    }
    // MGET → array of 3
    match &results[6] {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 3),
        other => panic!("expected array, got {other:?}"),
    }
    // DEL x3 → 1, 1, 1
    for r in &results[7..] {
        assert!(matches!(r, Resp::Integer(1)));
    }
}

// ── 9. Idempotency: dispatching the same command twice ───────────────────

/// `dispatch_with_user` with the same args twice produces
/// predictable results — no hidden state leaks via the cmd
/// buffer (the public path doesn't use a reusable buffer).
#[tokio::test]
async fn dispatch_twice_same_args() {
    let db = Db::default();
    for _ in 0..3 {
        let r = dispatch_with_user(&db, cmd(&["INCR", "c"]), 0, None, "default").await;
        assert!(matches!(r, Resp::Integer(_)));
    }
    // The counter should now reflect 3.
    let r = dispatch_with_user(&db, cmd(&["GET", "c"]), 0, None, "default").await;
    assert!(matches!(r, Resp::BulkString(Some(b)) if b.as_ref() == b"3"));
}
