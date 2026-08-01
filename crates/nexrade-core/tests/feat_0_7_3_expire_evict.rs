//! 0.7.3 — eviction + active expire under write storms.
//!
//! Locks down:
//!   * adaptive expire_cycle drains a large pile of expired keys in one call
//!   * expire_cycle is a cheap no-op when nothing is expired
//!   * evict_if_needed reclaims free space from expired keys before sampling
//!     live keys for LRU eviction
//!   * active_expire(20) still works (legacy fixed-budget path)

use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, MaxMemoryPolicy, ServerConfig};
use nexrade_core::resp::Resp;
use nexrade_core::store::Store;

fn small_config() -> ServerConfig {
    ServerConfig {
        databases: 1,
        max_memory: None,
        maxmemory_policy: MaxMemoryPolicy::NoEviction,
        ..ServerConfig::default()
    }
}

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter()
        .map(|a| Resp::BulkString(Some(bytes::Bytes::from(a.to_string()))))
        .collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0).await
}

#[tokio::test]
async fn expire_cycle_drains_large_pile() {
    let db = Db::new(small_config());
    // Seed 200 keys with a 1ms TTL.
    for i in 0..200 {
        let k = format!("k{i:03}");
        let _ = run(&db, cmd(&["SET", &k, "v", "PX", "1"])).await;
    }
    // Wait for them to expire.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    // Adaptive cycle must drain all of them in one call (budget grows
    // 20 → 40 → 80 → 160 → 256).
    let expired = db.store.active_expire_cycle();
    assert!(
        expired.len() >= 200,
        "adaptive cycle must drain all 200 keys, got {}",
        expired.len()
    );
    // Second call is a no-op.
    let again = db.store.active_expire_cycle();
    assert!(
        again.is_empty(),
        "second cycle must be empty, got {}",
        again.len()
    );
    assert_eq!(db.store.total_keys(), 0);
}

#[tokio::test]
async fn expire_cycle_noop_when_nothing_expired() {
    let db = Db::new(small_config());
    for i in 0..50 {
        let k = format!("k{i:02}");
        // 60s TTL — still live.
        let _ = run(&db, cmd(&["SET", &k, "v", "EX", "60"])).await;
    }
    let expired = db.store.active_expire_cycle();
    assert!(
        expired.is_empty(),
        "live keys must not be expired, got {}",
        expired.len()
    );
    assert_eq!(db.store.total_keys(), 50);
}

#[tokio::test]
async fn expire_cycle_legacy_fixed_budget_still_works() {
    let db = Db::new(small_config());
    for i in 0..50 {
        let k = format!("k{i:02}");
        let _ = run(&db, cmd(&["SET", &k, "v", "PX", "1"])).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    // Legacy path: fixed budget of 20 total, but per-shard distribution
    // (budget/num_shards.max(1)) can return more than 20 when many shards
    // each have ≥1 expired key. Just assert some progress and residual remain.
    let expired = db.store.active_expire(20);
    assert!(!expired.is_empty(), "fixed budget must drain some keys");
    assert!(expired.len() <= 50, "cannot drain more than seeded");
    let remaining = db.store.total_keys();
    assert!(remaining < 50, "some keys must have been drained");
    // Adaptive cycle drains the rest.
    let rest = db.store.active_expire_cycle();
    assert_eq!(rest.len() + remaining, remaining + rest.len()); // tautology; real check:
    assert_eq!(
        db.store.total_keys(),
        0,
        "adaptive cycle must drain residual"
    );
}

#[tokio::test]
async fn evict_if_needed_reclaims_expired_before_live() {
    // Seed WITHOUT a maxmemory cap so writes aren't interrupted by
    // mid-seed eviction. Then apply the cap via the store API.
    let db = Db::new(small_config());

    // Seed 50 live keys (~64 B each).
    for i in 0..50 {
        let k = format!("live{i:02}");
        let v = "x".repeat(64);
        let _ = run(&db, cmd(&["SET", &k, &v])).await;
    }
    // Seed 100 short-TTL keys (same size).
    for i in 0..100 {
        let k = format!("dead{i:03}");
        let v = "y".repeat(64);
        let _ = run(&db, cmd(&["SET", &k, &v, "PX", "1"])).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    // All 150 still present (no maxmemory during seed).
    assert_eq!(db.store.total_keys(), 150);

    // Cap is just above the live set (~50 * 128 ≈ 6 KiB) so reclaiming
    // the 100 expired keys is enough to get under the limit without
    // sampling live ones.
    let limit = 8 * 1024;
    let policy = MaxMemoryPolicy::AllKeysLru;
    let _evicted = db.store.evict_if_needed(&policy, limit);

    // After expire-before-evict, dead keys are gone.
    let dead_after = (0..100)
        .filter(|i| {
            let k = format!("dead{i:03}");
            db.store
                .db(0)
                .read_for(k.as_bytes())
                .get_ro(k.as_bytes())
                .is_some()
        })
        .count();
    assert_eq!(dead_after, 0, "all expired keys must be reclaimed");

    // Live keys mostly survive.
    let live_after = (0..50)
        .filter(|i| {
            let k = format!("live{i:02}");
            db.store
                .db(0)
                .read_for(k.as_bytes())
                .get_ro(k.as_bytes())
                .is_some()
        })
        .count();
    assert!(
        live_after >= 40,
        "most live keys must survive; got {live_after}/50"
    );

    // live_bytes under the cap (plus small slack).
    let after = db.store.estimated_memory_bytes();
    assert!(
        after <= limit + 4096,
        "after expire-before-evict, live_bytes ({after}) must be near the cap ({limit})"
    );
}

#[tokio::test]
async fn expire_cycle_under_write_storm() {
    // Simulate a write storm: many short-TTL keys arrive, then expire.
    // Adaptive cycle must keep up.
    let db = Db::new(small_config());
    for round in 0..5 {
        for i in 0..100 {
            let k = format!("s{round}_{i:03}");
            let _ = run(&db, cmd(&["SET", &k, "v", "PX", "1"])).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        let expired = db.store.active_expire_cycle();
        assert!(
            expired.len() >= 100,
            "round {round}: adaptive cycle must drain ≥100 keys, got {}",
            expired.len()
        );
    }
    // Final drain.
    let rest = db.store.active_expire_cycle();
    assert!(rest.is_empty() || rest.len() < 20);
}

// Keep Store import used.
#[allow(dead_code)]
fn _use_store(_: &Store) {}
