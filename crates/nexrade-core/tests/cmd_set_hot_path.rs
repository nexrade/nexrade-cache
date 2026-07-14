//! Behavioral tests for the `cmd_set` single-lookup reorder (Fix B) and
//! the `Database::get_or_insert_with` entry API (Fix D) used by
//! HSET/SADD. These don't instrument lookup counts directly (that would
//! require intrusive counters in the hot path) — instead they verify
//! that the observable behavior is unchanged after the reorder.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

#[tokio::test]
async fn set_plain_no_nx_xx_still_works() {
    let db = Db::default();
    let r = run(&db, cmd(&["SET", "k", "v"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"));
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"v"));
}

#[tokio::test]
async fn set_nx_with_existing_key_returns_null() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v1"])).await;
    let r = run(&db, cmd(&["SET", "k", "v2", "NX"])).await;
    assert!(matches!(r, Resp::BulkString(None)));
    // Value unchanged.
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"v1"));
}

#[tokio::test]
async fn set_nx_with_missing_key_succeeds() {
    let db = Db::default();
    let r = run(&db, cmd(&["SET", "k", "v1", "NX"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"));
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"v1"));
}

#[tokio::test]
async fn set_xx_with_missing_key_returns_null() {
    let db = Db::default();
    let r = run(&db, cmd(&["SET", "missing", "v", "XX"])).await;
    assert!(matches!(r, Resp::BulkString(None)));
}

#[tokio::test]
async fn set_xx_with_existing_key_succeeds() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v1"])).await;
    let r = run(&db, cmd(&["SET", "k", "v2", "XX"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"));
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"v2"));
}

#[tokio::test]
async fn set_get_option_returns_old_value_on_plain_path() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "old"])).await;
    let r = run(&db, cmd(&["SET", "k", "new", "GET"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"old"));
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"new"));
}

#[tokio::test]
async fn set_get_option_on_missing_key_returns_null_but_still_sets() {
    let db = Db::default();
    let r = run(&db, cmd(&["SET", "missing", "v", "GET"])).await;
    assert!(matches!(r, Resp::BulkString(None)));
    let r = run(&db, cmd(&["GET", "missing"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"v"));
}

#[tokio::test]
async fn hset_single_lookup_path_creates_and_updates() {
    let db = Db::default();
    // Create with 2 fields (get_or_insert_with hot path: absent -> insert).
    let r = run(&db, cmd(&["HSET", "h", "f1", "v1", "f2", "v2"])).await;
    assert!(matches!(r, Resp::Integer(2)));
    // Update existing field + add new one (get_or_insert_with hot path:
    // present, not expired -> just return &mut Entry, no extra insert).
    let r = run(&db, cmd(&["HSET", "h", "f1", "v1-updated", "f3", "v3"])).await;
    assert!(matches!(r, Resp::Integer(1))); // only f3 is new
    let r = run(&db, cmd(&["HGET", "h", "f1"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"v1-updated"));
}

#[tokio::test]
async fn hset_wrong_type_still_errors() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let r = run(&db, cmd(&["HSET", "k", "f", "v"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn sadd_single_lookup_path_creates_and_updates() {
    let db = Db::default();
    let r = run(&db, cmd(&["SADD", "s", "a", "b"])).await;
    assert!(matches!(r, Resp::Integer(2)));
    // Re-add "a" (duplicate) + add "c" (get_or_insert_with hot path on
    // an already-existing set).
    let r = run(&db, cmd(&["SADD", "s", "a", "c"])).await;
    assert!(matches!(r, Resp::Integer(1))); // only "c" is new
    let r = run(&db, cmd(&["SCARD", "s"])).await;
    assert!(matches!(r, Resp::Integer(3)));
}

#[tokio::test]
async fn sadd_wrong_type_still_errors() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let r = run(&db, cmd(&["SADD", "k", "m"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn hset_and_sadd_on_expired_key_recreate_correctly() {
    let db = Db::default();
    // Set a hash with a 1ms TTL so it's expired by the time we HSET again.
    let _ = run(&db, cmd(&["HSET", "h", "f", "v"])).await;
    let _ = run(&db, cmd(&["PEXPIRE", "h", "1"])).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    // HSET on the lazily-expired key should recreate it fresh via the
    // get_or_insert_with expired-entry path.
    let r = run(&db, cmd(&["HSET", "h", "f2", "v2"])).await;
    assert!(matches!(r, Resp::Integer(1)));
    let r = run(&db, cmd(&["HGET", "h", "f"])).await;
    assert!(
        matches!(r, Resp::BulkString(None)),
        "old field should be gone after expiry+recreate"
    );
    let r = run(&db, cmd(&["HGET", "h", "f2"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"v2"));
}
