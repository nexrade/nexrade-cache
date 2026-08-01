//! 0.3.4 dual-encoded sorted sets: compact start, promote, ZRANGE, OBJECT ENCODING.
#![allow(clippy::await_holding_lock)]

use std::sync::Mutex;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;
use nexrade_core::types::DataType;
use nexrade_core::zset_data::{ZSetData, ZSET_COMPACT_MAX_VALUE};

static ZZ_LOCK: Mutex<()> = Mutex::new(());

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
        DataType::ZSet(z) => z.encoding_name().to_string(),
        other => panic!("expected zset, got {other:?}"),
    }
}

#[tokio::test]
async fn new_zset_starts_compact() {
    let _g = ZZ_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"])).await;
    assert_eq!(encoding(&db, "z"), "listpack");
    assert_eq!(as_int(&run(&db, cmd(&["ZCARD", "z"])).await), 3);
    assert_eq!(as_bulk(&run(&db, cmd(&["ZSCORE", "z", "a"])).await), b"1");
}

#[tokio::test]
async fn promote_on_large_member() {
    let _g = ZZ_LOCK.lock().unwrap();
    let db = Db::default();
    let big = "x".repeat(ZSET_COMPACT_MAX_VALUE + 1);
    let _ = run(&db, cmd(&["ZADD", "z", "1", &big])).await;
    assert_eq!(encoding(&db, "z"), "skiplist");
    assert_eq!(as_int(&run(&db, cmd(&["ZCARD", "z"])).await), 1);
}

#[tokio::test]
async fn object_encoding_honest() {
    let _g = ZZ_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "c", "1", "a"])).await;
    let enc = run(&db, cmd(&["OBJECT", "ENCODING", "c"])).await;
    assert_eq!(as_bulk(&enc), b"listpack");

    let big = "y".repeat(ZSET_COMPACT_MAX_VALUE + 1);
    let _ = run(&db, cmd(&["ZADD", "ht", "1", &big])).await;
    let enc = run(&db, cmd(&["OBJECT", "ENCODING", "ht"])).await;
    assert_eq!(as_bulk(&enc), b"skiplist");
}

#[tokio::test]
async fn zrange_and_zrem() {
    let _g = ZZ_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "2", "b", "1", "a"])).await;
    let r = run(&db, cmd(&["ZRANGE", "z", "0", "-1"])).await;
    match r {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            assert_eq!(as_bulk(&items[0]), b"a");
            assert_eq!(as_bulk(&items[1]), b"b");
        }
        other => panic!("expected Array, got {other:?}"),
    }
    assert_eq!(as_int(&run(&db, cmd(&["ZREM", "z", "a"])).await), 1);
    assert_eq!(as_int(&run(&db, cmd(&["ZCARD", "z"])).await), 1);
}

#[tokio::test]
async fn zunionstore_picks_compact() {
    let _g = ZZ_LOCK.lock().unwrap();
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "a", "1", "x", "2", "y"])).await;
    let _ = run(&db, cmd(&["ZADD", "b", "3", "y", "4", "z"])).await;
    let n = run(&db, cmd(&["ZUNIONSTORE", "u", "2", "a", "b"])).await;
    assert_eq!(as_int(&n), 3);
    assert_eq!(encoding(&db, "u"), "listpack");
}

#[test]
fn zset_data_unit_roundtrip() {
    let mut z = ZSetData::new();
    assert!(z.insert(b"m".to_vec(), 1.5));
    assert!(matches!(z, ZSetData::Compact(_)));
    assert_eq!(z.score(b"m"), Some(1.5));
    assert!(!z.insert(b"m".to_vec(), 2.0));
    assert_eq!(z.score(b"m"), Some(2.0));
    assert_eq!(z.remove(b"m"), Some(2.0));
    assert!(z.is_empty());
}
