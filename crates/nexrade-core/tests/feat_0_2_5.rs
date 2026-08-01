//! 0.2.5 feature coverage: COMMAND table, BLMOVE, GEOSEARCHSTORE, LCS.

use std::time::Duration;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

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

fn as_array(r: Resp) -> Vec<Resp> {
    match r {
        Resp::Array(Some(a)) => a,
        other => panic!("expected array, got {other:?}"),
    }
}

fn as_bulk(r: &Resp) -> &[u8] {
    match r {
        Resp::BulkString(Some(b)) => b.as_ref(),
        other => panic!("expected bulk, got {other:?}"),
    }
}

// ── COMMAND ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn command_count_is_nonzero_and_matches_table() {
    let db = Db::default();
    let count = run(&db, cmd(&["COMMAND", "COUNT"])).await;
    let n = as_int(&count);
    assert!(n > 100, "expected a real table, got {n}");

    let list = run(&db, cmd(&["COMMAND", "LIST"])).await;
    let names = as_array(list);
    assert_eq!(names.len() as i64, n);

    // Full COMMAND returns an array of command descriptors.
    let full = run(&db, cmd(&["COMMAND"])).await;
    let descs = as_array(full);
    assert_eq!(descs.len() as i64, n);

    // Each descriptor is itself an array starting with the name.
    let first = as_array(descs.into_iter().next().unwrap());
    assert!(matches!(&first[0], Resp::BulkString(Some(_))));
    assert!(matches!(&first[1], Resp::Integer(_)), "arity slot");
}

#[tokio::test]
async fn command_info_and_getkeys() {
    let db = Db::default();

    let info = run(&db, cmd(&["COMMAND", "INFO", "GET", "NOPE"])).await;
    let parts = as_array(info);
    assert_eq!(parts.len(), 2);
    // GET is known → array; NOPE is unknown → null array.
    assert!(matches!(&parts[0], Resp::Array(Some(_))));
    assert!(
        matches!(
            &parts[1],
            Resp::Array(None) | Resp::Null | Resp::BulkString(None)
        ),
        "unknown command should be null-ish, got {:?}",
        parts[1]
    );

    let keys = run(&db, cmd(&["COMMAND", "GETKEYS", "SET", "mykey", "myval"])).await;
    let ks = as_array(keys);
    assert_eq!(ks.len(), 1);
    assert_eq!(as_bulk(&ks[0]), b"mykey");

    let keys = run(
        &db,
        cmd(&["COMMAND", "GETKEYS", "LMOVE", "a", "b", "LEFT", "RIGHT"]),
    )
    .await;
    let ks = as_array(keys);
    assert_eq!(ks.len(), 2);
    assert_eq!(as_bulk(&ks[0]), b"a");
    assert_eq!(as_bulk(&ks[1]), b"b");
}

// ── BLMOVE ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn blmove_moves_immediately_when_source_nonempty() {
    let db = Db::default();
    let _ = run(&db, cmd(&["LPUSH", "src", "x", "y"])).await;
    let r = run(&db, cmd(&["BLMOVE", "src", "dst", "RIGHT", "LEFT", "1"])).await;
    // RIGHT pop of [y, x] → x; push LEFT onto dst.
    assert_eq!(as_bulk(&r), b"x");
    // LLEN/LINDEX avoid LRANGE's Resp::Raw fast path.
    let len = run(&db, cmd(&["LLEN", "dst"])).await;
    assert_eq!(as_int(&len), 1);
    let head = run(&db, cmd(&["LINDEX", "dst", "0"])).await;
    assert_eq!(as_bulk(&head), b"x");
}

#[tokio::test]
async fn blmove_wakes_on_lpush() {
    let db = Db::default();

    let waiter_db = db.clone();
    let waiter = tokio::spawn(async move {
        run(
            &waiter_db,
            cmd(&["BLMOVE", "empty", "dst2", "LEFT", "LEFT", "5"]),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(80)).await;
    let _ = run(&db, cmd(&["LPUSH", "empty", "hello"])).await;

    let resp = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("BLMOVE task must finish")
        .expect("BLMOVE task must not panic");
    assert_eq!(as_bulk(&resp), b"hello");
}

#[tokio::test]
async fn blmove_timeout_returns_null() {
    let db = Db::default();
    let resp = tokio::time::timeout(
        Duration::from_secs(3),
        run(&db, cmd(&["BLMOVE", "nope", "dst", "LEFT", "RIGHT", "0.2"])),
    )
    .await
    .expect("must finish");
    assert!(
        matches!(
            resp,
            Resp::Array(None) | Resp::Null | Resp::BulkString(None)
        ) || matches!(&resp, Resp::Array(Some(a)) if a.is_empty()),
        "empty BLMOVE on timeout must be null-ish, got {resp:?}"
    );
}

// ── GEOSEARCHSTORE ────────────────────────────────────────────────────────────

#[tokio::test]
async fn geosearchstore_writes_geo_and_storedist_zset() {
    let db = Db::default();
    // Palermo ~ (13.361389, 38.115556), Catania ~ (15.087269, 37.502669)
    let _ = run(
        &db,
        cmd(&[
            "GEOADD",
            "Sicily",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ]),
    )
    .await;

    let n = run(
        &db,
        cmd(&[
            "GEOSEARCHSTORE",
            "out",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
        ]),
    )
    .await;
    assert!(as_int(&n) >= 1, "expected ≥1 hit, got {n:?}");

    // Destination is a geo key — GEOPOS should work.
    let pos = run(&db, cmd(&["GEOPOS", "out", "Catania"])).await;
    assert!(matches!(pos, Resp::Array(Some(_))), "{pos:?}");

    // STOREDIST → zset of distances.
    let n = run(
        &db,
        cmd(&[
            "GEOSEARCHSTORE",
            "dists",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
            "STOREDIST",
        ]),
    )
    .await;
    assert!(as_int(&n) >= 1);
    let ztype = run(&db, cmd(&["TYPE", "dists"])).await;
    match ztype {
        Resp::SimpleString(s) => assert_eq!(s, "zset"),
        Resp::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"zset"),
        other => panic!("expected zset type, got {other:?}"),
    }
}

// ── LCS ───────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn lcs_basic_len_and_idx() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k1", "abcdefgh"])).await;
    let _ = run(&db, cmd(&["SET", "k2", "abxcdyef"])).await;

    // Default: LCS string. Common subsequence of "abcdefgh" / "abxcdyef"
    // is "abcdef" (a,b,c,d,e,f).
    let r = run(&db, cmd(&["LCS", "k1", "k2"])).await;
    let s = as_bulk(&r);
    assert!(!s.is_empty(), "LCS must be non-empty");
    // Every char of LCS must appear in both inputs in order.
    assert!(is_subsequence(s, b"abcdefgh"));
    assert!(is_subsequence(s, b"abxcdyef"));

    let len = run(&db, cmd(&["LCS", "k1", "k2", "LEN"])).await;
    assert_eq!(as_int(&len), s.len() as i64);

    let idx = run(&db, cmd(&["LCS", "k1", "k2", "IDX"])).await;
    let parts = as_array(idx);
    // Shape: ["matches", [...], "len", N]
    assert!(parts.len() >= 4, "{parts:?}");
    assert_eq!(as_bulk(&parts[0]), b"matches");
    assert_eq!(as_bulk(&parts[2]), b"len");
    assert_eq!(as_int(&parts[3]), s.len() as i64);

    // Missing keys → empty LCS.
    let empty = run(&db, cmd(&["LCS", "missing1", "missing2", "LEN"])).await;
    assert_eq!(as_int(&empty), 0);
}

fn is_subsequence(sub: &[u8], s: &[u8]) -> bool {
    let mut i = 0;
    for &c in s {
        if i < sub.len() && sub[i] == c {
            i += 1;
        }
    }
    i == sub.len()
}
