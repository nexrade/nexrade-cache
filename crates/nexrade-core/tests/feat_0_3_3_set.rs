//! 0.3.3 dual-encoded sets: compact start, promote, SMEMBERS, OBJECT ENCODING.
#![allow(clippy::await_holding_lock)]

use std::sync::Mutex;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;
use nexrade_core::set_data::{SetData, SET_COMPACT_MAX_VALUE};
use nexrade_core::types::DataType;

static SS_LOCK: Mutex<()> = Mutex::new(());

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

fn as_int(r: &Resp) -> i64 {
    match r {
        Resp::Integer(n) => *n,
        other => panic!("expected int, got {other:?}"),
    }
}

fn as_bulk(r: &Resp) -> &[u8] {
    match r {
        Resp::BulkString(Some(b)) => b.as_ref(),
        other => panic!("expected bulk, got {other:?}"),
    }
}

fn encoding(db: &Db, key: &str) -> String {
    let k = key.as_bytes();
    let sdb = db.store.db(0).read_for(k);
    match &sdb.get_ro(k).unwrap().value {
        DataType::Set(s) => s.encoding_name().to_string(),
        other => panic!("expected set, got {other:?}"),
    }
}

#[tokio::test]
async fn new_set_starts_compact() {
    let _g = SS_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["SADD", "s", "a", "b", "c"])).await;
    assert_eq!(encoding(&db, "s"), "listpack");
    assert_eq!(as_int(&run(&db, cmd(&["SCARD", "s"])).await), 3);
    assert_eq!(as_int(&run(&db, cmd(&["SISMEMBER", "s", "a"])).await), 1);
}

#[tokio::test]
async fn promote_on_large_member() {
    let _g = SS_LOCK.lock().unwrap();
    let db = Db::default();
    let big = "x".repeat(SET_COMPACT_MAX_VALUE + 1);
    let _ = run(&db, cmd(&["SADD", "s", &big])).await;
    assert_eq!(encoding(&db, "s"), "hashtable");
    assert_eq!(as_int(&run(&db, cmd(&["SCARD", "s"])).await), 1);
}

#[tokio::test]
async fn object_encoding_honest() {
    let _g = SS_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["SADD", "c", "a"])).await;
    let enc = run(&db, cmd(&["OBJECT", "ENCODING", "c"])).await;
    assert_eq!(as_bulk(&enc), b"listpack");

    let big = "y".repeat(SET_COMPACT_MAX_VALUE + 1);
    let _ = run(&db, cmd(&["SADD", "ht", &big])).await;
    let enc = run(&db, cmd(&["OBJECT", "ENCODING", "ht"])).await;
    assert_eq!(as_bulk(&enc), b"hashtable");
}

#[tokio::test]
async fn smembers_and_srem() {
    let _g = SS_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["SADD", "s", "a", "b"])).await;
    let r = run(&db, cmd(&["SMEMBERS", "s"])).await;
    match r {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 2),
        other => panic!("expected Array, got {other:?}"),
    }
    assert_eq!(as_int(&run(&db, cmd(&["SREM", "s", "a"])).await), 1);
    assert_eq!(as_int(&run(&db, cmd(&["SCARD", "s"])).await), 1);
}

#[tokio::test]
async fn sunionstore_picks_compact() {
    let _g = SS_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["SADD", "a", "1", "2"])).await;
    let _ = run(&db, cmd(&["SADD", "b", "2", "3"])).await;
    let n = run(&db, cmd(&["SUNIONSTORE", "u", "a", "b"])).await;
    assert_eq!(as_int(&n), 3);
    assert_eq!(encoding(&db, "u"), "listpack");
}

#[test]
fn set_data_unit_roundtrip() {
    let mut s = SetData::new();
    assert!(s.insert(b"m".to_vec()).0);
    assert!(matches!(s, SetData::Compact(_)));
    assert!(s.contains(b"m"));
    assert!(!s.insert(b"m".to_vec()).0);
    assert!(s.remove(b"m").0);
    assert!(s.is_empty());
}
