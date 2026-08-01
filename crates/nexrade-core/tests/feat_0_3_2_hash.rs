//! 0.3.2 dual-encoded hashes: compact start, promote, HGETALL, OBJECT ENCODING.
//!
//! Tests share process-wide hash thresholds — std Mutex across await is
//! intentional isolation (same pattern as feat_0_3_0_list).
#![allow(clippy::await_holding_lock)]

use std::sync::Mutex;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::hash_data::{HashData, HASH_COMPACT_MAX_VALUE};
use nexrade_core::resp::Resp;
use nexrade_core::types::DataType;

// Thresholds are process-wide — serialize tests that depend on defaults.
static HH_LOCK: Mutex<()> = Mutex::new(());

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
        DataType::Hash(h) => h.encoding_name().to_string(),
        other => panic!("expected hash, got {other:?}"),
    }
}

#[tokio::test]
async fn new_hash_starts_compact() {
    let _g = HH_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["HSET", "h", "a", "1", "b", "2"])).await;
    assert_eq!(encoding(&db, "h"), "listpack");
    assert_eq!(as_int(&run(&db, cmd(&["HLEN", "h"])).await), 2);
    assert_eq!(as_bulk(&run(&db, cmd(&["HGET", "h", "a"])).await), b"1");
}

#[tokio::test]
async fn promote_on_large_value() {
    let _g = HH_LOCK.lock().unwrap();
    let db = Db::default();
    let big = "x".repeat(HASH_COMPACT_MAX_VALUE + 1);
    let _ = run(&db, cmd(&["HSET", "h", "f", &big])).await;
    assert_eq!(encoding(&db, "h"), "hashtable");
    assert_eq!(as_int(&run(&db, cmd(&["HLEN", "h"])).await), 1);
}

#[tokio::test]
async fn object_encoding_honest() {
    let _g = HH_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["HSET", "c", "a", "1"])).await;
    let enc = run(&db, cmd(&["OBJECT", "ENCODING", "c"])).await;
    assert_eq!(as_bulk(&enc), b"listpack");

    let big = "y".repeat(HASH_COMPACT_MAX_VALUE + 1);
    let _ = run(&db, cmd(&["HSET", "ht", "f", &big])).await;
    let enc = run(&db, cmd(&["OBJECT", "ENCODING", "ht"])).await;
    assert_eq!(as_bulk(&enc), b"hashtable");
}

#[tokio::test]
async fn hgetall_even_array() {
    let _g = HH_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["HSET", "h", "a", "1", "b", "2"])).await;
    let r = run(&db, cmd(&["HGETALL", "h"])).await;
    match r {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
            assert_eq!(items.len() % 2, 0);
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn hdel_and_hexists() {
    let _g = HH_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["HSET", "h", "a", "1", "b", "2"])).await;
    assert_eq!(as_int(&run(&db, cmd(&["HEXISTS", "h", "a"])).await), 1);
    assert_eq!(as_int(&run(&db, cmd(&["HDEL", "h", "a"])).await), 1);
    assert_eq!(as_int(&run(&db, cmd(&["HEXISTS", "h", "a"])).await), 0);
    assert_eq!(as_int(&run(&db, cmd(&["HLEN", "h"])).await), 1);
}

#[test]
fn hash_data_unit_roundtrip() {
    let mut h = HashData::new();
    assert!(h.insert(b"k".to_vec(), b"v".to_vec()).0);
    assert!(matches!(h, HashData::Compact(_)));
    assert_eq!(h.get(b"k").unwrap(), b"v");
    assert!(!h.insert(b"k".to_vec(), b"v2".to_vec()).0);
    assert_eq!(h.get(b"k").unwrap(), b"v2");
}
