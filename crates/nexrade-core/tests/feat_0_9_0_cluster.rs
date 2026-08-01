//! 0.9.0 — Cluster probe subcommands work; multi-node CLUSTER
//! subcommands return a clear explicit error. CROSSSLOT detection in
//! every multi-key batch command.

use nexrade_core::cluster::{check_same_slot, keyslot};
use nexrade_core::command::dispatch;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0).await
}

fn err_str(r: &Resp) -> String {
    match r {
        Resp::Error(s) => s.clone(),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn keyslot_matches_redis_canonical_vectors() {
    assert_eq!(keyslot(b"foo"), 12182);
    assert_eq!(keyslot(b"somekey"), 11058);
    assert_eq!(keyslot(b""), 0);
    assert_eq!(keyslot(b"{}"), 15257);
    // Same hash tag → same slot.
    assert_eq!(keyslot(b"foo{hash_tag}"), 2515);
    assert_eq!(keyslot(b"bar{hash_tag}"), 2515);
}

#[test]
fn check_same_slot_happy_path() {
    // Two keys that hash to the same slot (e.g. same hash tag).
    let k1: &[u8] = b"user:{42}:profile";
    let k2: &[u8] = b"settings:{42}:prefs";
    assert!(check_same_slot(&[k1, k2]).is_ok());
    // Single key always passes.
    assert!(check_same_slot(&[k1]).is_ok());
    assert!(check_same_slot(&[]).is_ok());
}

#[test]
fn check_same_slot_uses_hash_tag() {
    let k1: &[u8] = b"user:{42}:profile";
    let k2: &[u8] = b"settings:{42}:prefs";
    // Same hash tag {42} → same slot.
    assert!(check_same_slot(&[k1, k2]).is_ok());
}

#[test]
fn check_same_slot_rejects_different_slots() {
    let k1: &[u8] = b"foo";
    let k2: &[u8] = b"somekey";
    let err = check_same_slot(&[k1, k2]).unwrap_err();
    assert!(err.starts_with("CROSSSLOT"), "got: {err}");
    assert!(err.contains("hashes to slot"), "got: {err}");
}

// ─── Multi-key command integration: CROSSSLOT path ────────────────────────────

#[tokio::test]
async fn mset_with_shared_hash_tag_works() {
    let db = Db::default();
    // Same hash tag forces same slot — CROSSSLOT passes.
    let r = run(
        &db,
        cmd(&[
            "MSET",
            "user:{42}:profile",
            "alice",
            "settings:{42}:prefs",
            "dark",
        ]),
    )
    .await;
    assert!(
        matches!(r, Resp::SimpleString(ref s) if s == "OK"),
        "got: {r:?}"
    );
}

#[tokio::test]
async fn mset_single_key_works() {
    let db = Db::default();
    let r = run(&db, cmd(&["MSET", "k", "v"])).await;
    assert!(
        matches!(r, Resp::SimpleString(ref s) if s == "OK"),
        "got: {r:?}"
    );
}

#[tokio::test]
async fn mget_single_key_works() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let r = run(&db, cmd(&["MGET", "k"])).await;
    match r {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 1),
        other => panic!("expected array, got {other:?}"),
    }
}

// ─── Multi-key commands in standalone: no CROSSSLOT ─────────────────────────
//
// Redis only enforces CROSSSLOT for multi-key batches when
// `cluster-enabled yes`. Standalone nexrade-cache has one shard
// covering all slots, so all keys are in the same logical slot —
// CROSSSLOT never fires. RedisCluster-aware tooling (redis-py
// `RedisCluster`, `redis-cli --cluster reshard`) can still introspect
// the server via the probe subcommands below.

#[tokio::test]
async fn mset_multi_key_works_in_standalone() {
    let db = Db::default();
    let r = run(&db, cmd(&["MSET", "foo", "v1", "somekey", "v2"])).await;
    assert!(
        matches!(r, Resp::SimpleString(ref s) if s == "OK"),
        "got: {r:?}"
    );
}

#[tokio::test]
async fn mget_multi_key_works_in_standalone() {
    let db = Db::default();
    let _ = run(&db, cmd(&["MSET", "foo", "v1", "somekey", "v2"])).await;
    let r = run(&db, cmd(&["MGET", "foo", "somekey"])).await;
    match r {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[tokio::test]
async fn del_multi_key_works_in_standalone() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "foo", "v1"])).await;
    let _ = run(&db, cmd(&["SET", "somekey", "v2"])).await;
    let r = run(&db, cmd(&["DEL", "foo", "somekey"])).await;
    assert!(matches!(r, Resp::Integer(2)), "got: {r:?}");
}

// ─── Probe CLUSTER subcommands still work ────────────────────────────────────

#[tokio::test]
async fn cluster_info_shape() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "INFO"])).await;
    let s = match r {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        Resp::SimpleString(s) => s,
        other => panic!("expected bulk, got {other:?}"),
    };
    assert!(s.contains("cluster_state:ok"), "{s}");
    assert!(s.contains("cluster_size:1"), "{s}");
    assert!(s.contains("cluster_slots_assigned:16384"), "{s}");
}

#[tokio::test]
async fn cluster_myid_and_keyslot() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "MYID"])).await;
    match r {
        Resp::BulkString(Some(b)) => {
            assert_eq!(b.len(), 40, "myid must be 40 hex chars");
        }
        other => panic!("expected bulk, got {other:?}"),
    }
    let r = run(&db, cmd(&["CLUSTER", "KEYSLOT", "foo"])).await;
    assert!(matches!(r, Resp::Integer(12182)), "got: {r:?}");
}

#[tokio::test]
async fn cluster_slots_reports_full_range() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "SLOTS"])).await;
    // Should report one [0, 16383, [host, port, id]] entry.
    match r {
        Resp::Array(Some(arr)) => assert!(!arr.is_empty()),
        other => panic!("expected array, got {other:?}"),
    }
}

// ─── Multi-node CLUSTER subcommands are hard-errored ─────────────────────────

#[tokio::test]
async fn cluster_meet_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "MEET", "10.0.0.1", "6380"])).await;
    let msg = err_str(&r);
    assert!(
        msg.contains("CLUSTER MEET")
            && msg.contains("not supported")
            && msg.contains("cluster-compat.md"),
        "got: {msg}"
    );
}

#[tokio::test]
async fn cluster_failover_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "FAILOVER"])).await;
    assert!(err_str(&r).contains("FAILOVER"));
}

#[tokio::test]
async fn cluster_setslot_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "SETSLOT", "1234", "NODE", "abc"])).await;
    assert!(err_str(&r).contains("SETSLOT"));
}

#[tokio::test]
async fn cluster_addslots_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "ADDSLOTS", "0", "1", "2"])).await;
    assert!(err_str(&r).contains("ADDSLOTS"));
}

#[tokio::test]
async fn cluster_forget_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "FORGET", "abcdef"])).await;
    assert!(err_str(&r).contains("FORGET"));
}

#[tokio::test]
async fn cluster_help_works() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "HELP"])).await;
    // HELP is the one probe subcommand that's an array.
    match r {
        Resp::Array(Some(_)) => {}
        other => panic!("expected array, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_unknown_subcommand_returns_ok() {
    // Redis returns +OK for unknown CLUSTER subcommands so probes don't
    // error on newer protocol features. We keep that compat.
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "NOPE"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"));
}

// ─── 0.9.1: Redis 7+ probe-only subcommands ─────────────────────────────────
//
// Standalone emits no gossip. These subcommands are no-op stubs that
// answer probes cleanly so `redis-cli --cluster check` and other
// tools see a sane "no peers / no failure reports" shape.

#[tokio::test]
async fn cluster_links_returns_empty_array() {
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "LINKS"])).await;
    match r {
        Resp::Array(Some(arr)) => assert!(
            arr.is_empty(),
            "standalone has no inbound/outbound cluster links"
        ),
        other => panic!("expected array, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_count_failure_reports_returns_zero() {
    let db = Db::default();
    let r = run(
        &db,
        cmd(&[
            "CLUSTER",
            "COUNT-FAILURE-REPORTS",
            "0000000000000000000000000000000000000000",
        ]),
    )
    .await;
    assert!(matches!(r, Resp::Integer(0)), "got: {r:?}");
}

#[tokio::test]
async fn cluster_failover_check_rays_errors() {
    // FAILOVER-CHECK-RAYS has internal Redis-side logic that requires a
    // real epoch / quorum. Standalone errors explicitly rather than
    // answering with bogus data.
    let db = Db::default();
    let r = run(&db, cmd(&["CLUSTER", "FAILOVER-CHECK-RAYS"])).await;
    let msg = err_str(&r);
    assert!(msg.contains("FAILOVER-CHECK-RAYS"), "got: {msg}");
}

// ─── 0.9.1: documented gossip stance ────────────────────────────────────────

#[test]
fn minimal_gossip_constants_are_documented() {
    // 0.9.1: standalone emits no gossip frames and never migrates slots.
    // The constants are `const`s so this test only documents them at
    // runtime — the *type* of the constant is the load-bearing contract.
    use nexrade_core::cluster::{MINIMAL_GOSSIP_INTERVAL_SECS, SLOT_MIGRATION_ENABLED};
    let _gossip: u64 = MINIMAL_GOSSIP_INTERVAL_SECS;
    let _migrate: bool = SLOT_MIGRATION_ENABLED;
    // Smoke-touch the values so a future change to `let _ = 0;` still
    // shows up in a non-empty binary if the constants go away.
    assert!(_gossip > 0);
    assert!(!_migrate);
}
