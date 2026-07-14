//! Tests for the ConnectionRegistry — `CLIENT LIST`/`INFO`/`KILL`/`PAUSE`.
//!
//! These exercise the registry directly (not through a live TCP
//! connection) so they're fully deterministic. End-to-end smoke tests
//! against the running binary live in `examples/` and the verification
//! section of the plan file.

use std::sync::atomic::Ordering;
use std::time::Duration;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::conn_registry::{
    format_client_list_line, ConnectionRegistry, CLIENT_FLAG_MULTI, CLIENT_FLAG_PUBSUB,
};
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

#[test]
fn registry_register_unregister_roundtrip() {
    let reg = ConnectionRegistry::new();
    let (_m, k) = reg.register(1, "127.0.0.1:6379".parse().unwrap());
    assert!(!k.load(Ordering::Acquire));
    assert!(reg.meta(1).is_some());
    assert_eq!(reg.snapshot().len(), 1);
    reg.unregister(1);
    assert!(reg.meta(1).is_none());
    assert_eq!(reg.snapshot().len(), 0);
}

#[test]
fn registry_pause_blocks_writes_via_dispatch() {
    let db = Db::default();
    db.connections.pause_for(Duration::from_millis(50));
    // A write (SET) should be rejected with the PAUSE error.
    let resp = futures::executor::block_on(dispatch_with_user(
        &db,
        cmd(&["SET", "k", "v"]),
        0,
        None,
        "default",
    ));
    match resp {
        Resp::Error(s) => {
            assert!(s.starts_with("PAUSE"), "expected PAUSE error, got: {s}");
        }
        other => panic!("expected Resp::Error, got {other:?}"),
    }
}

#[test]
fn registry_reads_allowed_during_pause() {
    let db = Db::default();
    // Seed a value while not paused.
    futures::executor::block_on(dispatch_with_user(
        &db,
        cmd(&["SET", "k", "v"]),
        0,
        None,
        "default",
    ));
    db.connections.pause_for(Duration::from_millis(50));
    // GET (read) should still succeed.
    let resp = futures::executor::block_on(dispatch_with_user(
        &db,
        cmd(&["GET", "k"]),
        0,
        None,
        "default",
    ));
    assert!(matches!(resp, Resp::BulkString(Some(ref b)) if b.as_ref() == b"v"));
}

#[test]
fn registry_pause_lifts_after_window() {
    let db = Db::default();
    db.connections.pause_for(Duration::from_millis(20));
    assert!(db.connections.is_paused());
    std::thread::sleep(Duration::from_millis(30));
    assert!(!db.connections.is_paused());
    // Now writes should pass.
    let resp = futures::executor::block_on(dispatch_with_user(
        &db,
        cmd(&["SET", "k", "v"]),
        0,
        None,
        "default",
    ));
    assert!(matches!(resp, Resp::SimpleString(ref s) if s == "OK"));
}

#[test]
fn registry_unpause_clears_immediately() {
    let db = Db::default();
    db.connections.pause_for(Duration::from_secs(60));
    db.connections.unpause();
    assert!(!db.connections.is_paused());
}

#[test]
fn registry_request_kill_sets_flag() {
    let reg = ConnectionRegistry::new();
    let (_m, k) = reg.register(42, "127.0.0.1:1234".parse().unwrap());
    assert!(!k.load(Ordering::Acquire));
    assert!(reg.request_kill(42));
    assert!(k.load(Ordering::Acquire));
    // Killing an unknown id returns false.
    assert!(!reg.request_kill(99));
}

#[test]
fn snapshot_returns_all_meta() {
    let reg = ConnectionRegistry::new();
    reg.register(10, "127.0.0.1:6379".parse().unwrap());
    reg.register(20, "127.0.0.1:6380".parse().unwrap());
    reg.register(30, "127.0.0.1:6381".parse().unwrap());
    let snap = reg.snapshot();
    assert_eq!(snap.len(), 3);
}

#[test]
fn format_client_list_line_includes_required_fields() {
    let reg = ConnectionRegistry::new();
    let (m, _k) = reg.register(7, "127.0.0.1:6379".parse().unwrap());
    {
        let mut g = m.write();
        g.name = "worker-1".to_string();
        g.last_cmd = "set".to_string();
        g.db_index = 2;
        g.flags = CLIENT_FLAG_PUBSUB;
    }
    let line = format_client_list_line(&m.read());
    assert!(line.contains("id=7"), "got: {line}");
    assert!(line.contains("addr=127.0.0.1:6379"));
    assert!(line.contains("name=worker-1"));
    assert!(line.contains("db=2"));
    assert!(line.contains("cmd=set"));
    assert!(line.contains("user=default"));
    assert!(line.contains("flags=P"), "expected flags=P, got: {line}");
    assert!(line.contains("age="));
    assert!(line.contains("idle="));
}

#[test]
fn format_idle_age_grows_with_time() {
    let reg = ConnectionRegistry::new();
    let (m, _k) = reg.register(1, "127.0.0.1:6379".parse().unwrap());
    let line1 = format_client_list_line(&m.read());
    std::thread::sleep(Duration::from_millis(1100));
    let line2 = format_client_list_line(&m.read());
    // age is whole seconds; second format must have a strictly larger
    // age than the first.
    let age1: u64 = line1
        .split_whitespace()
        .find_map(|f| f.strip_prefix("age="))
        .unwrap()
        .parse()
        .unwrap();
    let age2: u64 = line2
        .split_whitespace()
        .find_map(|f| f.strip_prefix("age="))
        .unwrap()
        .parse()
        .unwrap();
    assert!(age2 > age1, "age did not grow: {age1} -> {age2}");
}

#[test]
fn multi_flag_set_then_cleared_on_discard() {
    let reg = ConnectionRegistry::new();
    let (m, _k) = reg.register(1, "127.0.0.1:6379".parse().unwrap());
    // Simulate entering MULTI then leaving it.
    {
        let mut g = m.write();
        g.flags |= CLIENT_FLAG_MULTI;
    }
    assert!(m.read().flags & CLIENT_FLAG_MULTI != 0);
    {
        let mut g = m.write();
        g.flags &= !CLIENT_FLAG_MULTI;
    }
    assert!(m.read().flags & CLIENT_FLAG_MULTI == 0);
}
