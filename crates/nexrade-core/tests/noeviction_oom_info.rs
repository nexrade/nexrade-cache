//! 1.4.7 — Redis-correct `noeviction` (OOM on writes above maxmemory)
//! and the INFO fields that were missing from the Redis shape.

use std::sync::atomic::Ordering;

use nexrade_core::command::{dispatch, dispatch_with_user};
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: &[&str]) -> Resp {
    dispatch_with_user(db, cmd(args), 0, None, "default").await
}

fn text(r: &Resp) -> String {
    match r {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        Resp::SimpleString(s) => s.clone(),
        other => panic!("expected bulk/simple reply, got {other:?}"),
    }
}

fn info_fields(info: &str) -> std::collections::HashMap<&str, &str> {
    info.lines()
        .filter_map(|line| line.split_once(':'))
        .collect()
}

#[tokio::test]
async fn noeviction_rejects_growing_writes_above_maxmemory() {
    let db = Db::default();
    assert!(matches!(
        run(&db, &["SET", "k", "original"]).await,
        Resp::SimpleString(_)
    ));

    // Cap below the live bytes of the single key. CONFIG SET itself is
    // never OOM-gated so operators can always raise the cap again.
    assert!(matches!(
        run(&db, &["CONFIG", "SET", "maxmemory", "1"]).await,
        Resp::SimpleString(_)
    ));
    assert!(
        db.store.estimated_memory_bytes() > 1,
        "test needs an over-cap store"
    );

    let r = run(&db, &["SET", "k", "new"]).await;
    match &r {
        Resp::Error(s) => assert!(s.contains("OOM command not allowed"), "got: {s}"),
        other => panic!("expected OOM error, got {other:?}"),
    }
    // Rejected writes must not have touched the data.
    assert_eq!(text(&run(&db, &["GET", "k"]).await), "original");

    // Reads and freeing commands still work while over the cap.
    assert_eq!(text(&run(&db, &["GET", "k"]).await), "original");
    assert!(matches!(run(&db, &["DEL", "k"]).await, Resp::Integer(1)));

    // Raising the limit immediately unblocks writes.
    assert!(matches!(
        run(&db, &["CONFIG", "SET", "maxmemory", "1048576"]).await,
        Resp::SimpleString(_)
    ));
    assert!(matches!(
        run(&db, &["SET", "k", "new"]).await,
        Resp::SimpleString(_)
    ));
    assert_eq!(text(&run(&db, &["GET", "k"]).await), "new");
}

#[tokio::test]
async fn noeviction_allows_trim_and_expire_but_blocks_push_above_cap() {
    let db = Db::default();
    for i in 0..10 {
        run(&db, &["RPUSH", "hist", &format!("point-{i}")]).await;
    }
    run(&db, &["CONFIG", "SET", "maxmemory", "1"]).await;

    assert!(matches!(
        run(&db, &["RPUSH", "hist", "new"]).await,
        Resp::Error(s) if s.contains("OOM")
    ));
    // LTRIM/EXPIRE only free data — the ingestion pipeline's trim leg
    // keeps working so an over-cap list can shrink itself.
    assert!(matches!(
        run(&db, &["LTRIM", "hist", "-5", "-1"]).await,
        Resp::SimpleString(_)
    ));
    assert!(matches!(
        run(&db, &["LLEN", "hist"]).await,
        Resp::Integer(5)
    ));
    assert!(matches!(
        run(&db, &["EXPIRE", "hist", "60"]).await,
        Resp::Integer(1)
    ));
    assert!(matches!(
        run(&db, &["PERSIST", "hist"]).await,
        Resp::Integer(1)
    ));
}

#[tokio::test]
async fn eviction_policies_are_not_oom_gated() {
    let db = Db::default();
    for i in 0..10 {
        run(&db, &["SET", &format!("k{i}"), "value"]).await;
    }
    run(&db, &["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"]).await;
    run(&db, &["CONFIG", "SET", "maxmemory", "200"]).await;

    // Pre-write eviction (not OOM rejection) makes room for the write.
    assert!(matches!(
        run(&db, &["SET", "fresh", "value"]).await,
        Resp::SimpleString(_)
    ));
    assert!(
        db.stats.evicted_keys.load(Ordering::Relaxed) > 0,
        "evictions must be counted for INFO evicted_keys"
    );
}

#[tokio::test]
async fn internal_replay_is_exempt_from_oom() {
    let db = Db::default();
    run(&db, &["CONFIG", "SET", "maxmemory", "1"]).await;
    // AOF/replication replay goes through `dispatch()` and must still be
    // able to reconstruct a dataset larger than maxmemory.
    let r = dispatch(&db, cmd(&["SET", "replayed", "value"]), 0).await;
    assert!(
        matches!(r, Resp::SimpleString(_)),
        "replay was OOM-gated: {r:?}"
    );
}

#[tokio::test]
async fn info_exposes_redis_memory_and_stats_fields() {
    let db = Db::default();
    let info = text(&run(&db, &["INFO", "all"]).await);
    let f = info_fields(&info);

    for field in [
        "maxmemory_policy",
        "total_system_memory",
        "allocator_frag_ratio",
        "allocator_frag_bytes",
        "mem_fragmentation_bytes",
        "lazyfree_pending_objects",
        "expired_keys",
        "evicted_keys",
    ] {
        assert!(f.contains_key(field), "INFO missing {field}:\n{info}");
    }
    assert_eq!(f["maxmemory_policy"], "noeviction");
    assert_eq!(f["lazyfree_pending_objects"], "0");
    assert!(f["total_system_memory"].parse::<u64>().unwrap() > 0);
    assert!(f["allocator_frag_ratio"].parse::<f64>().is_ok());
    assert!(f["allocator_frag_bytes"].parse::<u64>().is_ok());
    assert!(f["mem_fragmentation_bytes"].parse::<u64>().is_ok());

    db.stats.expired_keys.store(7, Ordering::Relaxed);
    db.stats.evicted_keys.store(3, Ordering::Relaxed);
    run(&db, &["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"]).await;

    let info = text(&run(&db, &["INFO", "all"]).await);
    let f = info_fields(&info);
    assert_eq!(f["expired_keys"], "7");
    assert_eq!(f["evicted_keys"], "3");
    assert_eq!(f["maxmemory_policy"], "volatile-ttl");
}
