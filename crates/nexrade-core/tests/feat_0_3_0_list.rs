//! 0.3.0 dual-encoded lists: compact start, promote, LRANGE, mid-list convert.
//!
//! Tests share process-wide listpack threshold atomics, so they take a
//! `std::sync::Mutex` across awaits purely for isolation (not production
//! code). Clippy's await_holding_lock is intentional here.
#![allow(clippy::await_holding_lock)]

use std::sync::Mutex;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::list_data::{ListData, LIST_COMPACT_MAX_ELEM};
use nexrade_core::resp::Resp;
use nexrade_core::types::DataType;

// CONFIG SET mutates process-wide listpack atomics — serialize every test
// that depends on default thresholds so parallel runs see a consistent state.
static LP_LOCK: Mutex<()> = Mutex::new(());

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

async fn reset_defaults(db: &Db) {
    let _ = run(
        db,
        cmd(&["CONFIG", "SET", "list-max-listpack-entries", "512"]),
    )
    .await;
    let _ = run(
        db,
        cmd(&["CONFIG", "SET", "list-max-listpack-size", "8192"]),
    )
    .await;
    let _ = run(
        db,
        cmd(&["CONFIG", "SET", "list-max-listpack-value", "256"]),
    )
    .await;
    let _ = run(db, cmd(&["CONFIG", "SET", "list-demote-entries", "128"])).await;
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

fn as_simple(r: &Resp) -> &str {
    match r {
        Resp::SimpleString(s) => s.as_str(),
        other => panic!("expected simple, got {other:?}"),
    }
}

fn encoding(db: &Db, key: &str) -> String {
    let k = key.as_bytes();
    let sdb = db.store.db(0).read_for(k);
    match &sdb.get_ro(k).unwrap().value {
        DataType::List(l) => l.encoding_name().to_string(),
        other => panic!("expected list, got {other:?}"),
    }
}

#[tokio::test]
async fn new_list_starts_compact() {
    let _g = LP_LOCK.lock().unwrap();
    let db = Db::default();
    reset_defaults(&db).await;
    let _ = run(&db, cmd(&["LPUSH", "l", "a", "b", "c"])).await;
    assert_eq!(encoding(&db, "l"), "listpack");
    let n = run(&db, cmd(&["LLEN", "l"])).await;
    assert_eq!(as_int(&n), 3);
    let head = run(&db, cmd(&["LINDEX", "l", "0"])).await;
    // LPUSH a b c → front is c
    assert_eq!(as_bulk(&head), b"c");
}

#[tokio::test]
async fn lrange_on_compact() {
    let _g = LP_LOCK.lock().unwrap();
    let db = Db::default();
    reset_defaults(&db).await;
    let _ = run(&db, cmd(&["RPUSH", "l", "0", "1", "2", "3", "4"])).await;
    assert_eq!(encoding(&db, "l"), "listpack");
    // LRANGE returns Raw on success path — use LINDEX to verify order.
    for i in 0..5 {
        let v = run(&db, cmd(&["LINDEX", "l", &i.to_string()])).await;
        assert_eq!(as_bulk(&v), i.to_string().as_bytes());
    }
    // Also exercise the short-lock LRANGE framing path.
    let r = run(&db, cmd(&["LRANGE", "l", "1", "3"])).await;
    match r {
        Resp::Raw(bytes) => {
            // *3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n
            assert_eq!(&bytes[..], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n");
        }
        other => panic!("expected Raw LRANGE reply, got {other:?}"),
    }
}

#[tokio::test]
async fn promote_on_large_element() {
    let _g = LP_LOCK.lock().unwrap();
    let db = Db::default();
    reset_defaults(&db).await;
    let big = "x".repeat(LIST_COMPACT_MAX_ELEM + 1);
    let _ = run(&db, cmd(&["RPUSH", "l", &big])).await;
    assert_eq!(encoding(&db, "l"), "linkedlist");
    let n = run(&db, cmd(&["LLEN", "l"])).await;
    assert_eq!(as_int(&n), 1);
}

#[tokio::test]
async fn mid_list_lset_converts_to_linked() {
    let _g = LP_LOCK.lock().unwrap();
    let db = Db::default();
    reset_defaults(&db).await;
    let _ = run(&db, cmd(&["RPUSH", "l", "a", "b", "c"])).await;
    assert_eq!(encoding(&db, "l"), "listpack");
    let r = run(&db, cmd(&["LSET", "l", "1", "B"])).await;
    assert_eq!(as_simple(&r), "OK");
    assert_eq!(encoding(&db, "l"), "linkedlist");
    let v = run(&db, cmd(&["LINDEX", "l", "1"])).await;
    assert_eq!(as_bulk(&v), b"B");
}

#[tokio::test]
async fn object_encoding_honest() {
    let _g = LP_LOCK.lock().unwrap();
    let db = Db::default();
    reset_defaults(&db).await;
    let _ = run(&db, cmd(&["RPUSH", "c", "a"])).await;
    let enc = run(&db, cmd(&["OBJECT", "ENCODING", "c"])).await;
    assert_eq!(as_bulk(&enc), b"listpack");

    let big = "y".repeat(LIST_COMPACT_MAX_ELEM + 1);
    let _ = run(&db, cmd(&["RPUSH", "lk", &big])).await;
    let enc = run(&db, cmd(&["OBJECT", "ENCODING", "lk"])).await;
    assert_eq!(as_bulk(&enc), b"linkedlist");
}

#[tokio::test]
async fn lmove_across_compact_lists() {
    let _g = LP_LOCK.lock().unwrap();
    let db = Db::default();
    reset_defaults(&db).await;
    let _ = run(&db, cmd(&["RPUSH", "src", "a", "b"])).await;
    let moved = run(&db, cmd(&["LMOVE", "src", "dst", "LEFT", "RIGHT"])).await;
    assert_eq!(as_bulk(&moved), b"a");
    assert_eq!(as_int(&run(&db, cmd(&["LLEN", "src"])).await), 1);
    assert_eq!(as_int(&run(&db, cmd(&["LLEN", "dst"])).await), 1);
}

#[test]
fn list_data_unit_promote_and_range() {
    use bytes::Bytes;
    let mut l = ListData::new();
    for s in [b"a" as &[u8], b"b", b"c", b"d"] {
        l.push_back(Bytes::copy_from_slice(s));
    }
    assert!(matches!(l, ListData::Compact(_)));
    let r = l.range_bytes(1, 2);
    assert_eq!(r[0].as_ref(), b"b");
    assert_eq!(r[1].as_ref(), b"c");
}

/// Phase 5: shrinking back from Linked to Compact after pop/trim.
#[tokio::test]
async fn demote_after_trim_back_to_listpack() {
    let _g = LP_LOCK.lock().unwrap();
    let db = Db::default();
    reset_defaults(&db).await;
    // Push past default 512 to promote.
    let mut args = vec![Resp::bulk_str("RPUSH"), Resp::bulk_str("k")];
    for i in 0..600 {
        args.push(Resp::bulk_str(format!("v{i}")));
    }
    let _ = run(&db, args).await;
    assert_eq!(encoding(&db, "k"), "linkedlist");

    // LTRIM keeps only the first 10 — under demote threshold → listpack.
    let r = run(&db, cmd(&["LTRIM", "k", "0", "9"])).await;
    assert_eq!(as_simple(&r), "OK");
    assert_eq!(encoding(&db, "k"), "listpack");
    let n = run(&db, cmd(&["LLEN", "k"])).await;
    assert_eq!(as_int(&n), 10);
}

/// Phase 5: CONFIG SET list-max-listpack-entries shrinks the promote threshold.
#[tokio::test]
async fn config_set_listpack_entries_retunes_promote() {
    let _g = LP_LOCK.lock().unwrap();
    let db = Db::default();
    // Set tiny threshold so 4 pushes promote.
    let _ = run(
        &db,
        cmd(&["CONFIG", "SET", "list-max-listpack-entries", "3"]),
    )
    .await;
    let _ = run(&db, cmd(&["DEL", "k"])).await;
    let _ = run(&db, cmd(&["RPUSH", "k", "a", "b", "c"])).await;
    assert_eq!(encoding(&db, "k"), "listpack");
    let _ = run(&db, cmd(&["RPUSH", "k", "d"])).await;
    assert_eq!(encoding(&db, "k"), "linkedlist");
    // Reset for other tests.
    reset_defaults(&db).await;
}

/// Phase 5: CONFIG GET exposes listpack knobs.
#[tokio::test]
async fn config_get_listpack_knobs() {
    let _g = LP_LOCK.lock().unwrap();
    let db = Db::default();
    reset_defaults(&db).await;
    let r = run(&db, cmd(&["CONFIG", "GET", "list-max-listpack-entries"])).await;
    let s = match r {
        Resp::Array(arr) => format!("{:?}", arr),
        other => panic!("expected array, got {other:?}"),
    };
    assert!(s.contains("512"), "expected 512 in {s}");
}

/// 0.3.1: ServerConfig list thresholds applied at Db::new.
#[tokio::test]
async fn db_new_applies_list_thresholds_from_config() {
    let _g = LP_LOCK.lock().unwrap();
    let cfg = nexrade_core::db::ServerConfig {
        list_max_listpack_entries: 5,
        list_max_listpack_size: 8192,
        list_max_listpack_value: 256,
        list_demote_entries: 128,
        ..Default::default()
    };
    let db = Db::new(cfg);
    // 5 small elems stay listpack; 6th promotes.
    let _ = run(&db, cmd(&["RPUSH", "k", "a", "b", "c", "d", "e"])).await;
    assert_eq!(encoding(&db, "k"), "listpack");
    let _ = run(&db, cmd(&["RPUSH", "k", "f"])).await;
    assert_eq!(encoding(&db, "k"), "linkedlist");
    // Restore defaults for other tests.
    reset_defaults(&db).await;
}
