//! Tests for the atomic mirrors on `ReplicationState`:
//! `is_replica_fast()` (mirrors `role`) and
//! `propagate_subscriber_count()` (mirrors live broadcast subscribers).
//!
//! These exist to let the per-command hot path skip parking_lot
//! RwLock / tokio broadcast Mutex acquisitions — see
//! `crates/nexrade-core/src/replication.rs`.

use nexrade_core::replication::{ReplicationRole, ReplicationState};

#[test]
fn is_replica_fast_tracks_role_change() {
    let repl = ReplicationState::new_primary("a".repeat(40));
    assert!(!repl.is_replica_fast());
    assert!(!repl.is_replica());

    repl.set_role(ReplicationRole::Replica);
    assert!(repl.is_replica_fast());
    assert!(repl.is_replica());

    repl.set_role(ReplicationRole::Primary);
    assert!(!repl.is_replica_fast());
    assert!(!repl.is_replica());
}

#[test]
fn propagate_subscriber_count_starts_at_zero() {
    let repl = ReplicationState::new_primary("b".repeat(40));
    assert_eq!(repl.propagate_subscriber_count(), 0);
}

#[test]
fn propagate_subscriber_count_matches_registered_replicas() {
    let repl = ReplicationState::new_primary("c".repeat(40));
    let id1 = repl.register_replica("127.0.0.1:1".parse().unwrap(), 0);
    assert_eq!(repl.propagate_subscriber_count(), 1);
    let id2 = repl.register_replica("127.0.0.1:2".parse().unwrap(), 0);
    assert_eq!(repl.propagate_subscriber_count(), 2);

    repl.unregister_replica(id1);
    assert_eq!(repl.propagate_subscriber_count(), 1);
    repl.unregister_replica(id2);
    assert_eq!(repl.propagate_subscriber_count(), 0);
}

#[test]
fn unregister_unknown_replica_does_not_underflow_count() {
    let repl = ReplicationState::new_primary("d".repeat(40));
    repl.register_replica("127.0.0.1:1".parse().unwrap(), 0);
    // Unregistering an id that was never registered must not touch the
    // counter (guarded by `replicas.len() < before` in `unregister_replica`).
    repl.unregister_replica(999);
    assert_eq!(repl.propagate_subscriber_count(), 1);
}

#[test]
fn set_role_is_idempotent() {
    let repl = ReplicationState::new_primary("e".repeat(40));
    repl.set_role(ReplicationRole::Replica);
    repl.set_role(ReplicationRole::Replica);
    assert!(repl.is_replica_fast());
    assert_eq!(repl.current_role(), ReplicationRole::Replica);
}

// ── 0.8.0: WAIT + lag ─────────────────────────────────────────────────────────

#[test]
fn register_replica_seeds_offset() {
    let repl = ReplicationState::new_primary("f".repeat(40));
    let id = repl.register_replica("127.0.0.1:9".parse().unwrap(), 42);
    assert_eq!(repl.replicas_at_or_beyond(42), 1);
    assert_eq!(repl.replicas_at_or_beyond(43), 0);
    let snaps = repl.replica_snapshots();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].1, 42);
    assert!(snaps[0].2 > 0, "last_ack_ms must be stamped");
    repl.unregister_replica(id);
    assert_eq!(repl.replicas_at_or_beyond(0), 0);
}

#[test]
fn update_replica_offset_is_monotonic_and_wakes() {
    let repl = ReplicationState::new_primary("g".repeat(40));
    let addr: std::net::SocketAddr = "127.0.0.1:10".parse().unwrap();
    let _id = repl.register_replica(addr, 10);
    // Move forward.
    repl.update_replica_offset(addr, 20);
    assert_eq!(repl.replicas_at_or_beyond(20), 1);
    // Backward is ignored.
    repl.update_replica_offset(addr, 15);
    assert_eq!(repl.replicas_at_or_beyond(20), 1);
    assert_eq!(repl.replicas_at_or_beyond(15), 1);
}

#[test]
fn replicas_at_or_beyond_counts_caught_up() {
    let repl = ReplicationState::new_primary("h".repeat(40));
    repl.register_replica("127.0.0.1:1".parse().unwrap(), 100);
    repl.register_replica("127.0.0.1:2".parse().unwrap(), 50);
    repl.register_replica("127.0.0.1:3".parse().unwrap(), 100);
    assert_eq!(repl.replicas_at_or_beyond(100), 2);
    assert_eq!(repl.replicas_at_or_beyond(50), 3);
    assert_eq!(repl.replicas_at_or_beyond(101), 0);
}

#[tokio::test]
async fn wait_no_replicas_returns_zero() {
    use nexrade_core::command::dispatch;
    use nexrade_core::db::Db;
    use nexrade_core::resp::Resp;

    let db = Db::default();
    // Primary with no replicas.
    let args: Vec<Resp> = ["WAIT", "1", "100"]
        .iter()
        .map(|s| Resp::bulk_str(*s))
        .collect();
    let r = dispatch(&db, args, 0).await;
    match r {
        Resp::Integer(n) => assert_eq!(n, 0, "standalone WAIT must return 0"),
        other => panic!("expected integer, got {other:?}"),
    }
}

#[tokio::test]
async fn wait_with_caught_up_replica_returns_one() {
    use nexrade_core::command::dispatch;
    use nexrade_core::db::Db;
    use nexrade_core::resp::Resp;
    use std::sync::atomic::Ordering;

    let db = Db::default();
    // Simulate a write so the primary offset is non-zero.
    db.replication
        .replication_offset
        .store(100, Ordering::Relaxed);
    // Register a replica already at offset 100.
    let _id = db
        .replication
        .register_replica("127.0.0.1:55".parse().unwrap(), 100);

    let args: Vec<Resp> = ["WAIT", "1", "1000"]
        .iter()
        .map(|s| Resp::bulk_str(*s))
        .collect();
    let r = dispatch(&db, args, 0).await;
    match r {
        Resp::Integer(n) => assert_eq!(n, 1, "caught-up replica must satisfy WAIT 1"),
        other => panic!("expected integer, got {other:?}"),
    }
}

#[tokio::test]
async fn wait_timeout_returns_partial() {
    use nexrade_core::command::dispatch;
    use nexrade_core::db::Db;
    use nexrade_core::resp::Resp;
    use std::sync::atomic::Ordering;

    let db = Db::default();
    db.replication
        .replication_offset
        .store(200, Ordering::Relaxed);
    // Replica only at 50 — not caught up.
    let _id = db
        .replication
        .register_replica("127.0.0.1:66".parse().unwrap(), 50);

    let args: Vec<Resp> = ["WAIT", "1", "50"] // 50 ms timeout
        .iter()
        .map(|s| Resp::bulk_str(*s))
        .collect();
    let start = std::time::Instant::now();
    let r = dispatch(&db, args, 0).await;
    let elapsed = start.elapsed();
    match r {
        Resp::Integer(n) => assert_eq!(n, 0, "lagging replica must not satisfy WAIT"),
        other => panic!("expected integer, got {other:?}"),
    }
    // Should have waited roughly the timeout (allow wide slack for CI).
    assert!(
        elapsed.as_millis() >= 40,
        "WAIT must block for ~timeout, elapsed {:?}",
        elapsed
    );
    assert!(
        elapsed.as_millis() < 500,
        "WAIT must not hang, elapsed {:?}",
        elapsed
    );
}

#[tokio::test]
async fn wait_wakes_on_ack() {
    use nexrade_core::command::dispatch;
    use nexrade_core::db::Db;
    use nexrade_core::resp::Resp;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let db = Arc::new(Db::default());
    db.replication
        .replication_offset
        .store(300, Ordering::Relaxed);
    let addr: std::net::SocketAddr = "127.0.0.1:77".parse().unwrap();
    // Start lagging.
    let _id = db.replication.register_replica(addr, 0);

    let db2 = Arc::clone(&db);
    let waiter = tokio::spawn(async move {
        let args: Vec<Resp> = ["WAIT", "1", "2000"]
            .iter()
            .map(|s| Resp::bulk_str(*s))
            .collect();
        dispatch(&db2, args, 0).await
    });

    // Give WAIT a moment to park, then ACK.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    db.replication.update_replica_offset(addr, 300);

    let r = waiter.await.unwrap();
    match r {
        Resp::Integer(n) => assert_eq!(n, 1, "ACK must wake WAIT"),
        other => panic!("expected integer, got {other:?}"),
    }
}

#[tokio::test]
async fn info_replication_reports_lag() {
    use nexrade_core::command::dispatch;
    use nexrade_core::db::Db;
    use nexrade_core::resp::Resp;

    let db = Db::default();
    let addr: std::net::SocketAddr = "10.0.0.5:6380".parse().unwrap();
    let _id = db.replication.register_replica(addr, 42);

    let args: Vec<Resp> = ["INFO", "replication"]
        .iter()
        .map(|s| Resp::bulk_str(*s))
        .collect();
    let r = dispatch(&db, args, 0).await;
    let text = match r {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        Resp::SimpleString(s) => s,
        other => panic!("expected bulk, got {other:?}"),
    };
    assert!(text.contains("role:master"), "{text}");
    assert!(text.contains("connected_slaves:1"), "{text}");
    assert!(text.contains("offset=42"), "{text}");
    assert!(text.contains("lag="), "{text}");
    assert!(text.contains("10.0.0.5"), "{text}");
}
