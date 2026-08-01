//! 0.2.6: SCAN cursors, RESP size caps, CONFIG REWRITE honesty, AOF write status.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::{Resp, RespParser, MAX_ARRAY_LEN, MAX_BULK_LEN};
use std::sync::atomic::Ordering;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

fn as_array(r: Resp) -> Vec<Resp> {
    match r {
        Resp::Array(Some(a)) => a,
        other => panic!("expected array, got {other:?}"),
    }
}

fn as_bulk(r: &Resp) -> String {
    match r {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected bulk, got {other:?}"),
    }
}

// ── SCAN cursor ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn scan_paginates_with_count_and_returns_to_zero() {
    let db = Db::default();
    for i in 0..25 {
        let k = format!("k{i:02}");
        let _ = run(&db, cmd(&["SET", &k, "v"])).await;
    }

    let mut cursor = "0".to_string();
    let mut seen = std::collections::HashSet::new();
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds <= 50, "scan must terminate");
        let r = run(&db, cmd(&["SCAN", &cursor, "COUNT", "5", "MATCH", "k*"])).await;
        let parts = as_array(r);
        assert_eq!(parts.len(), 2);
        cursor = as_bulk(&parts[0]);
        let page = as_array(parts.into_iter().nth(1).unwrap());
        assert!(
            page.len() <= 5,
            "COUNT=5 must cap page size, got {}",
            page.len()
        );
        for item in page {
            seen.insert(as_bulk(&item));
        }
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(
        seen.len(),
        25,
        "must see all 25 keys across pages: {seen:?}"
    );
    assert!(rounds > 1, "COUNT=5 over 25 keys must take multiple rounds");
}

#[tokio::test]
async fn hscan_sscan_zscan_paginate() {
    let db = Db::default();
    // HSET 10 fields
    for i in 0..10 {
        let f = format!("f{i}");
        let _ = run(&db, cmd(&["HSET", "h", &f, "v"])).await;
    }
    // SADD 10 members
    for i in 0..10 {
        let m = format!("m{i}");
        let _ = run(&db, cmd(&["SADD", "s", &m])).await;
    }
    // ZADD 10 members
    for i in 0..10 {
        let m = format!("z{i}");
        let _ = run(&db, cmd(&["ZADD", "z", &(i as f64).to_string(), &m])).await;
    }

    // HSCAN COUNT 3 → multiple pages of field/value pairs.
    let mut cursor = "0".to_string();
    let mut fields = 0usize;
    loop {
        let r = run(&db, cmd(&["HSCAN", "h", &cursor, "COUNT", "3"])).await;
        let parts = as_array(r);
        cursor = as_bulk(&parts[0]);
        let page = as_array(parts.into_iter().nth(1).unwrap());
        assert_eq!(page.len() % 2, 0);
        fields += page.len() / 2;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(fields, 10);

    // SSCAN
    cursor = "0".to_string();
    let mut members = 0usize;
    loop {
        let r = run(&db, cmd(&["SSCAN", "s", &cursor, "COUNT", "4"])).await;
        let parts = as_array(r);
        cursor = as_bulk(&parts[0]);
        let page = as_array(parts.into_iter().nth(1).unwrap());
        members += page.len();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(members, 10);

    // ZSCAN
    cursor = "0".to_string();
    let mut zmembers = 0usize;
    loop {
        let r = run(&db, cmd(&["ZSCAN", "z", &cursor, "COUNT", "3"])).await;
        let parts = as_array(r);
        cursor = as_bulk(&parts[0]);
        let page = as_array(parts.into_iter().nth(1).unwrap());
        assert_eq!(page.len() % 2, 0);
        zmembers += page.len() / 2;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(zmembers, 10);
}

// ── RESP size caps ────────────────────────────────────────────────────────────

#[test]
fn resp_rejects_oversized_array_length() {
    let mut p = RespParser::new();
    // Array claiming MAX_ARRAY_LEN+1 elements — reject before allocating.
    let huge = format!("*{}\r\n", MAX_ARRAY_LEN + 1);
    p.feed(huge.as_bytes());
    match p.parse_one() {
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("multibulk") || msg.contains("array") || msg.contains("invalid"),
                "expected size-cap error, got: {msg}"
            );
        }
        Ok(v) => panic!("expected Err for oversized array, got Ok({v:?})"),
    }
}

#[test]
fn resp_rejects_oversized_bulk_length_header() {
    let mut p = RespParser::new();
    // Bulk claiming MAX_BULK_LEN+1 bytes — reject on the length header alone.
    let huge = format!("${}\r\n", MAX_BULK_LEN + 1);
    p.feed(huge.as_bytes());
    match p.parse_one() {
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("bulk") || msg.contains("invalid"),
                "expected bulk length error, got: {msg}"
            );
        }
        // Incomplete is wrong: we have the full header with no body needed
        // once the length itself is illegal.
        Ok(v) => panic!("expected Err for oversized bulk, got Ok({v:?})"),
    }
}

// ── CONFIG REWRITE honesty ────────────────────────────────────────────────────

#[tokio::test]
async fn config_rewrite_returns_clear_error() {
    let db = Db::default();
    let r = run(&db, cmd(&["CONFIG", "REWRITE"])).await;
    match r {
        Resp::Error(msg) => {
            assert!(
                msg.to_uppercase().contains("REWRITE") || msg.to_uppercase().contains("NOT"),
                "expected clear REWRITE error, got: {msg}"
            );
        }
        other => panic!("CONFIG REWRITE must not silently OK, got {other:?}"),
    }
}

#[tokio::test]
async fn config_get_exposes_dir_and_appendfsync() {
    let db = Db::default();
    let r = run(&db, cmd(&["CONFIG", "GET", "*"])).await;
    let parts = as_array(r);
    let keys: Vec<String> = parts.iter().step_by(2).map(as_bulk).collect();
    for want in [
        "dir",
        "dbfilename",
        "appendfsync",
        "appendfilename",
        "maxmemory",
    ] {
        assert!(
            keys.iter().any(|k| k == want),
            "CONFIG GET * missing '{want}', have {keys:?}"
        );
    }
}

// ── AOF last write status ─────────────────────────────────────────────────────

#[tokio::test]
async fn aof_last_write_status_ok_when_enabled_and_clean() {
    let db = Db::default();
    // Simulate AOF enabled + clean writes.
    db.stats.aof_enabled.store(true, Ordering::Relaxed);
    db.stats.aof_last_write_status.store(0, Ordering::Relaxed);

    let info = run(&db, cmd(&["INFO", "persistence"])).await;
    let bulk = match info {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("expected bulk INFO, got {other:?}"),
    };
    assert!(
        bulk.contains("aof_last_write_status:ok"),
        "expected ok status, got:\n{bulk}"
    );

    // Flip to error and re-check.
    db.stats.aof_last_write_status.store(1, Ordering::Relaxed);
    let info = run(&db, cmd(&["INFO", "persistence"])).await;
    let bulk = match info {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("expected bulk INFO, got {other:?}"),
    };
    assert!(
        bulk.contains("aof_last_write_status:err"),
        "expected err status after write failure flag, got:\n{bulk}"
    );
}
