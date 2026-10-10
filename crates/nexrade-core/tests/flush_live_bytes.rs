//! Regression: `FLUSHALL` / `FLUSHDB` must reset the live-memory counter.
//!
//! `Database::flush` cleared `entries`, `expiry_index` and `key_versions` but
//! left `live_bytes` at its pre-flush value. On a server configured with
//! `maxmemory`, `evict_if_needed` then compared the *stale* counter against
//! the limit, decided it was over budget, and evicted the key it had just
//! inserted — so after a single FLUSHALL the cache accepted every write,
//! replied `OK`, and silently retained nothing until the process restarted.

use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, MaxMemoryPolicy, ServerConfig};
use nexrade_core::resp::Resp;

fn config_with_limit(limit: usize) -> ServerConfig {
    ServerConfig {
        databases: 1,
        max_memory: Some(limit),
        maxmemory_policy: MaxMemoryPolicy::AllKeysLru,
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
async fn flushall_resets_live_bytes_so_writes_are_retained() {
    // 64 KiB budget — small enough that a handful of 4 KiB keys fills it.
    let db = Db::new(config_with_limit(64 * 1024));

    let payload = "x".repeat(4096);
    for i in 0..40 {
        let _ = run(&db, cmd(&["SET", &format!("k{i}"), &payload])).await;
    }
    let before = db.store.estimated_memory_bytes();
    assert!(before > 0, "test did not populate any memory");

    let _ = run(&db, cmd(&["FLUSHALL"])).await;

    assert_eq!(
        db.store.estimated_memory_bytes(),
        0,
        "FLUSHALL left {before} bytes of stale live_bytes behind"
    );
    assert_eq!(db.store.db(0).len(), 0, "FLUSHALL did not empty the store");

    // The whole point: a write after FLUSHALL must survive. Before the fix
    // every one of these was evicted the instant it was inserted.
    let _ = run(&db, cmd(&["SET", "after:flush", "v"])).await;
    let got = run(&db, cmd(&["GET", "after:flush"])).await;
    assert!(
        matches!(&got, Resp::BulkString(Some(b)) if b.as_ref() == b"v"),
        "a write issued after FLUSHALL was not retained — got {got:?}"
    );
}

#[tokio::test]
async fn flushdb_resets_live_bytes_for_only_the_targeted_database() {
    let db = Db::new(ServerConfig {
        databases: 2,
        max_memory: Some(64 * 1024),
        maxmemory_policy: MaxMemoryPolicy::AllKeysLru,
        ..ServerConfig::default()
    });

    let payload = "x".repeat(4096);
    for i in 0..20 {
        let _ = run(&db, cmd(&["SET", &format!("db0:{i}"), &payload])).await;
        let _ = dispatch(&db, cmd(&["SET", &format!("db1:{i}"), &payload]), 1).await;
    }

    let db1_before = db.store.db(1).estimated_memory_bytes();
    assert!(db1_before > 0, "db 1 was not populated");

    let _ = run(&db, cmd(&["FLUSHDB"])).await;

    assert_eq!(
        db.store.db(0).estimated_memory_bytes(),
        0,
        "FLUSHDB did not reset db 0's live_bytes"
    );
    assert_eq!(
        db.store.db(1).estimated_memory_bytes(),
        db1_before,
        "FLUSHDB must not disturb other databases"
    );

    // db 0 is writable again after the flush.
    let _ = run(&db, cmd(&["SET", "post:flushdb", "v"])).await;
    let got = run(&db, cmd(&["GET", "post:flushdb"])).await;
    assert!(
        matches!(&got, Resp::BulkString(Some(b)) if b.as_ref() == b"v"),
        "write after FLUSHDB was not retained — got {got:?}"
    );
}

#[tokio::test]
async fn repeated_flushall_cycles_keep_the_cache_writable() {
    let db = Db::new(config_with_limit(64 * 1024));
    for round in 0..5 {
        let _ = run(&db, cmd(&["FLUSHALL"])).await;
        let key = format!("round:{round}");
        let _ = run(&db, cmd(&["SET", &key, "v"])).await;
        let got = run(&db, cmd(&["GET", &key])).await;
        assert!(
            matches!(&got, Resp::BulkString(Some(b)) if b.as_ref() == b"v"),
            "round {round}: write was not retained — got {got:?}"
        );
    }
}
