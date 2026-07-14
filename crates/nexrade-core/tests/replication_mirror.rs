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
    let id1 = repl.register_replica("127.0.0.1:1".parse().unwrap());
    assert_eq!(repl.propagate_subscriber_count(), 1);
    let id2 = repl.register_replica("127.0.0.1:2".parse().unwrap());
    assert_eq!(repl.propagate_subscriber_count(), 2);

    repl.unregister_replica(id1);
    assert_eq!(repl.propagate_subscriber_count(), 1);
    repl.unregister_replica(id2);
    assert_eq!(repl.propagate_subscriber_count(), 0);
}

#[test]
fn unregister_unknown_replica_does_not_underflow_count() {
    let repl = ReplicationState::new_primary("d".repeat(40));
    repl.register_replica("127.0.0.1:1".parse().unwrap());
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
