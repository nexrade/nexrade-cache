//! 0.8.1 — SENTINEL is explicitly unsupported (standalone server).
//!
//! All `SENTINEL *` subcommands return a clear error pointing the operator
//! at `docs/redis-sentinel-compat.md` for failover guidance.

use nexrade_core::command::dispatch;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0).await
}

fn is_error_with(r: &Resp, needle: &str) -> bool {
    match r {
        Resp::Error(s) => s
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase()),
        _ => false,
    }
}

#[tokio::test]
async fn sentinel_master_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["SENTINEL", "MASTER", "mymaster"])).await;
    assert!(is_error_with(&r, "sentinel is not supported"), "got: {r:?}");
    assert!(is_error_with(&r, "standalone server"), "got: {r:?}");
}

#[tokio::test]
async fn sentinel_masters_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["SENTINEL", "MASTERS"])).await;
    assert!(is_error_with(&r, "sentinel"));
}

#[tokio::test]
async fn sentinel_monitor_errors() {
    let db = Db::default();
    let r = run(
        &db,
        cmd(&["SENTINEL", "MONITOR", "mymaster", "127.0.0.1", "6379", "1"]),
    )
    .await;
    assert!(is_error_with(&r, "sentinel"));
}

#[tokio::test]
async fn sentinel_failover_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["SENTINEL", "FAILOVER"])).await;
    assert!(is_error_with(&r, "sentinel"));
}

#[tokio::test]
async fn sentinel_get_master_addr_by_name_errors() {
    let db = Db::default();
    let r = run(
        &db,
        cmd(&["SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"]),
    )
    .await;
    assert!(is_error_with(&r, "sentinel"));
}

#[tokio::test]
async fn sentinel_replicas_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["SENTINEL", "REPLICAS", "mymaster"])).await;
    assert!(is_error_with(&r, "sentinel"));
}

#[tokio::test]
async fn sentinel_ckquorum_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["SENTINEL", "CKQUORUM", "mymaster"])).await;
    assert!(is_error_with(&r, "sentinel"));
}

#[tokio::test]
async fn sentinel_flushconfig_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["SENTINEL", "FLUSHCONFIG"])).await;
    assert!(is_error_with(&r, "sentinel"));
}

#[tokio::test]
async fn sentinel_info_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["SENTINEL", "INFO", "mymaster"])).await;
    assert!(is_error_with(&r, "sentinel"));
}

#[tokio::test]
async fn sentinel_ping_errors() {
    let db = Db::default();
    let r = run(&db, cmd(&["SENTINEL", "PING"])).await;
    assert!(is_error_with(&r, "sentinel"));
}

#[tokio::test]
async fn non_sentinel_commands_still_work() {
    // Sanity: adding the SENTINEL hard-error must not affect anything else.
    let db = Db::default();
    let r = run(&db, cmd(&["PING"])).await;
    match r {
        Resp::SimpleString(s) if s == "PONG" => {}
        other => panic!("PING should still reply PONG, got {other:?}"),
    }
    let r = run(&db, cmd(&["INFO", "replication"])).await;
    assert!(matches!(r, Resp::BulkString(_) | Resp::SimpleString(_)));
}
