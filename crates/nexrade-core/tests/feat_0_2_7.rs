//! 0.2.7: LATENCY family, SORT BY/GET/LIMIT/STORE, waiter race smoke.

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

// ── LATENCY ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn latency_help_and_latest_shape() {
    let db = Db::default();
    // 0.7.4: seed the dedicated latency monitor (not only slowlog).
    db.latency.record("get", 5_000);

    let help = run(&db, cmd(&["LATENCY", "HELP"])).await;
    let parts = as_array(help);
    assert!(!parts.is_empty());
    // HELP must mention HISTOGRAM (0.7.4).
    let help_text: String = parts
        .iter()
        .filter_map(|r| match r {
            Resp::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        help_text.to_ascii_uppercase().contains("HISTOGRAM"),
        "HELP must list HISTOGRAM, got {help_text}"
    );

    let latest = run(&db, cmd(&["LATENCY", "LATEST"])).await;
    let rows = as_array(latest);
    assert!(!rows.is_empty(), "expected at least one event row");
    let row = as_array(rows.into_iter().next().unwrap());
    // [event, timestamp, latest_us, max_us]
    assert!(row.len() >= 4, "{row:?}");
    assert_eq!(as_bulk(&row[0]), b"get");

    let hist = run(&db, cmd(&["LATENCY", "HISTORY", "get"])).await;
    let h = as_array(hist);
    assert!(!h.is_empty());

    let n = run(&db, cmd(&["LATENCY", "RESET"])).await;
    assert!(as_int(&n) >= 1);
}

// ── SORT BY / GET / LIMIT / STORE ─────────────────────────────────────────────

#[tokio::test]
async fn sort_by_get_limit_store() {
    let db = Db::default();
    let _ = run(&db, cmd(&["LPUSH", "list", "a", "b", "c"])).await;
    // Weights: a→3, b→1, c→2
    let _ = run(&db, cmd(&["SET", "w_a", "3"])).await;
    let _ = run(&db, cmd(&["SET", "w_b", "1"])).await;
    let _ = run(&db, cmd(&["SET", "w_c", "2"])).await;
    // External values for GET
    let _ = run(&db, cmd(&["SET", "obj_a", "A"])).await;
    let _ = run(&db, cmd(&["SET", "obj_b", "B"])).await;
    let _ = run(&db, cmd(&["SET", "obj_c", "C"])).await;

    // SORT by weight ascending → b, c, a
    let r = run(&db, cmd(&["SORT", "list", "BY", "w_*"])).await;
    let items = as_array(r);
    assert_eq!(items.len(), 3);
    assert_eq!(as_bulk(&items[0]), b"b");
    assert_eq!(as_bulk(&items[1]), b"c");
    assert_eq!(as_bulk(&items[2]), b"a");

    // GET obj_* → external values in sorted order
    let r = run(&db, cmd(&["SORT", "list", "BY", "w_*", "GET", "obj_*"])).await;
    let items = as_array(r);
    assert_eq!(as_bulk(&items[0]), b"B");
    assert_eq!(as_bulk(&items[1]), b"C");
    assert_eq!(as_bulk(&items[2]), b"A");

    // LIMIT 1 1 → just "c"
    let r = run(&db, cmd(&["SORT", "list", "BY", "w_*", "LIMIT", "1", "1"])).await;
    let items = as_array(r);
    assert_eq!(items.len(), 1);
    assert_eq!(as_bulk(&items[0]), b"c");

    // STORE
    let n = run(&db, cmd(&["SORT", "list", "BY", "w_*", "STORE", "out"])).await;
    assert_eq!(as_int(&n), 3);
    let head = run(&db, cmd(&["LINDEX", "out", "0"])).await;
    assert_eq!(as_bulk(&head), b"b");

    // SORT_RO rejects STORE
    let err = run(&db, cmd(&["SORT_RO", "list", "STORE", "nope"])).await;
    assert!(matches!(err, Resp::Error(_)), "{err:?}");
}

// ── SPUBLISH (standalone alias) ───────────────────────────────────────────────

#[tokio::test]
async fn spublish_returns_integer() {
    let db = Db::default();
    // No subscribers → 0
    let r = run(&db, cmd(&["SPUBLISH", "ch", "hello"])).await;
    assert_eq!(as_int(&r), 0);
}

// ── Blocking waiter still wakes (smoke after race fix) ────────────────────────

#[tokio::test]
async fn blpop_still_wakes_on_lpush_after_race_fix() {
    let db = Db::default();
    let waiter_db = db.clone();
    let waiter = tokio::spawn(async move { run(&waiter_db, cmd(&["BLPOP", "q", "5"])).await });
    tokio::time::sleep(Duration::from_millis(80)).await;
    let _ = run(&db, cmd(&["LPUSH", "q", "x"])).await;
    let resp = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("BLPOP must finish")
        .expect("BLPOP must not panic");
    let parts = as_array(resp);
    assert_eq!(as_bulk(&parts[0]), b"q");
    assert_eq!(as_bulk(&parts[1]), b"x");
}
