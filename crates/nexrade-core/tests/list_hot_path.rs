//! Behavioral tests for the `get_or_create_list` single-lookup fix (backing
//! LPUSH/RPUSH/LPUSHX/RPUSHX), which ports `get_or_create_list` onto
//! `Database::get_or_insert_with` to match the already-converted
//! `get_or_create_hash`/`get_or_create_set`/`get_or_create_zset`. These
//! don't instrument lookup counts directly (that would require intrusive
//! counters in the hot path) — instead they verify that the observable
//! behavior is unchanged after the reorder.
//!
//! Note: LRANGE returns a pre-encoded `Resp::Raw` buffer (a fast-path
//! optimization), not a typed `Resp::Array`, so these tests use
//! LLEN/LINDEX to inspect list contents instead of matching on LRANGE's
//! return value.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

async fn lindex(db: &Db, key: &str, idx: i64) -> Option<Vec<u8>> {
    let r = run(db, cmd(&["LINDEX", key, &idx.to_string()])).await;
    match r {
        Resp::BulkString(Some(b)) => Some(b.to_vec()),
        _ => None,
    }
}

#[tokio::test]
async fn lpush_creates_list_on_absent_key() {
    let db = Db::default();
    let r = run(&db, cmd(&["LPUSH", "l", "a"])).await;
    assert!(matches!(r, Resp::Integer(1)));
    let r = run(&db, cmd(&["LLEN", "l"])).await;
    assert!(matches!(r, Resp::Integer(1)));
}

#[tokio::test]
async fn rpush_creates_list_on_absent_key() {
    let db = Db::default();
    let r = run(&db, cmd(&["RPUSH", "l", "a"])).await;
    assert!(matches!(r, Resp::Integer(1)));
    let r = run(&db, cmd(&["LLEN", "l"])).await;
    assert!(matches!(r, Resp::Integer(1)));
}

#[tokio::test]
async fn lpush_appends_on_existing_list() {
    let db = Db::default();
    let _ = run(&db, cmd(&["LPUSH", "l", "a"])).await;
    let r = run(&db, cmd(&["LPUSH", "l", "b"])).await;
    assert!(matches!(r, Resp::Integer(2)));
    // LPUSH prepends, so most-recent push is at the head.
    assert_eq!(lindex(&db, "l", 0).await, Some(b"b".to_vec()));
    assert_eq!(lindex(&db, "l", 1).await, Some(b"a".to_vec()));
}

#[tokio::test]
async fn rpush_appends_on_existing_list() {
    let db = Db::default();
    let _ = run(&db, cmd(&["RPUSH", "l", "a"])).await;
    let r = run(&db, cmd(&["RPUSH", "l", "b"])).await;
    assert!(matches!(r, Resp::Integer(2)));
    assert_eq!(lindex(&db, "l", 0).await, Some(b"a".to_vec()));
    assert_eq!(lindex(&db, "l", 1).await, Some(b"b".to_vec()));
}

#[tokio::test]
async fn lpush_wrong_type_still_errors() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let r = run(&db, cmd(&["LPUSH", "k", "x"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn rpush_wrong_type_still_errors() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let r = run(&db, cmd(&["RPUSH", "k", "x"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn lpushx_on_absent_key_is_noop_returns_zero() {
    let db = Db::default();
    let r = run(&db, cmd(&["LPUSHX", "missing", "x"])).await;
    assert!(matches!(r, Resp::Integer(0)));
    let r = run(&db, cmd(&["EXISTS", "missing"])).await;
    assert!(
        matches!(r, Resp::Integer(0)),
        "LPUSHX on an absent key must never create it"
    );
}

#[tokio::test]
async fn rpushx_on_absent_key_is_noop_returns_zero() {
    let db = Db::default();
    let r = run(&db, cmd(&["RPUSHX", "missing", "x"])).await;
    assert!(matches!(r, Resp::Integer(0)));
    let r = run(&db, cmd(&["EXISTS", "missing"])).await;
    assert!(
        matches!(r, Resp::Integer(0)),
        "RPUSHX on an absent key must never create it"
    );
}

#[tokio::test]
async fn lpushx_on_existing_key_appends() {
    let db = Db::default();
    let _ = run(&db, cmd(&["LPUSH", "l", "a"])).await;
    let r = run(&db, cmd(&["LPUSHX", "l", "b"])).await;
    assert!(matches!(r, Resp::Integer(2)));
}

#[tokio::test]
async fn rpushx_on_existing_key_appends() {
    let db = Db::default();
    let _ = run(&db, cmd(&["RPUSH", "l", "a"])).await;
    let r = run(&db, cmd(&["RPUSHX", "l", "b"])).await;
    assert!(matches!(r, Resp::Integer(2)));
}

#[tokio::test]
async fn lpush_on_lazily_expired_key_recreates_fresh() {
    let db = Db::default();
    let _ = run(&db, cmd(&["LPUSH", "l", "old"])).await;
    let _ = run(&db, cmd(&["PEXPIRE", "l", "1"])).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // LPUSH on the lazily-expired key should recreate it fresh via the
    // get_or_insert_with expired-entry path, not append to the stale list.
    let r = run(&db, cmd(&["LPUSH", "l", "new"])).await;
    assert!(matches!(r, Resp::Integer(1)));
    let r = run(&db, cmd(&["LLEN", "l"])).await;
    assert!(
        matches!(r, Resp::Integer(1)),
        "old elements should be gone after expiry+recreate"
    );
    assert_eq!(lindex(&db, "l", 0).await, Some(b"new".to_vec()));
}

#[tokio::test]
async fn rpush_on_lazily_expired_key_recreates_fresh() {
    let db = Db::default();
    let _ = run(&db, cmd(&["RPUSH", "l", "old"])).await;
    let _ = run(&db, cmd(&["PEXPIRE", "l", "1"])).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let r = run(&db, cmd(&["RPUSH", "l", "new"])).await;
    assert!(matches!(r, Resp::Integer(1)));
    let r = run(&db, cmd(&["LLEN", "l"])).await;
    assert!(
        matches!(r, Resp::Integer(1)),
        "old elements should be gone after expiry+recreate"
    );
    assert_eq!(lindex(&db, "l", 0).await, Some(b"new".to_vec()));
}
