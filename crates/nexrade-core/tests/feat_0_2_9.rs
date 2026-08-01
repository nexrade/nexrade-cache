//! 0.2.9: XSETID and durable stream last-id.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

fn as_bulk(r: &Resp) -> String {
    match r {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        Resp::SimpleString(s) => s.clone(),
        Resp::Integer(n) => n.to_string(),
        other => panic!("expected bulk/simple/int, got {other:?}"),
    }
}

fn as_array(r: Resp) -> Vec<Resp> {
    match r {
        Resp::Array(Some(a)) => a,
        other => panic!("expected array, got {other:?}"),
    }
}

fn info_field(parts: &[Resp], name: &str) -> Option<String> {
    let mut i = 0;
    while i + 1 < parts.len() {
        if as_bulk(&parts[i]) == name {
            return Some(as_bulk(&parts[i + 1]));
        }
        i += 2;
    }
    None
}

#[tokio::test]
async fn xsetid_advances_last_generated_id() {
    let db = Db::default();
    let id = run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await;
    assert_eq!(as_bulk(&id), "1-0");

    // Advance past the top without adding an entry.
    let r = run(&db, cmd(&["XSETID", "s", "5-0", "ENTRIESADDED", "10"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"), "{r:?}");

    let info = run(&db, cmd(&["XINFO", "STREAM", "s"])).await;
    let parts = as_array(info);
    assert_eq!(
        info_field(&parts, "last-generated-id").as_deref(),
        Some("5-0")
    );
    assert_eq!(info_field(&parts, "entries-added").as_deref(), Some("10"));
    // Length still 1 — no fake entry inserted.
    assert_eq!(info_field(&parts, "length").as_deref(), Some("1"));

    // Auto XADD must continue past 5-0.
    let id2 = run(&db, cmd(&["XADD", "s", "*", "f", "v2"])).await;
    let id2s = as_bulk(&id2);
    let sid = nexrade_core::command::stream::StreamId::parse(&id2s).expect("parse");
    assert!(
        sid > nexrade_core::command::stream::StreamId(5, 0),
        "auto id {id2s} must be > 5-0"
    );
}

#[tokio::test]
async fn xsetid_rejects_smaller_than_top() {
    let db = Db::default();
    let _ = run(&db, cmd(&["XADD", "s", "10-0", "f", "v"])).await;
    let err = run(&db, cmd(&["XSETID", "s", "5-0"])).await;
    assert!(matches!(err, Resp::Error(_)), "{err:?}");
}

#[tokio::test]
async fn xdel_does_not_lower_last_id() {
    let db = Db::default();
    let _ = run(&db, cmd(&["XADD", "s", "1-0", "f", "a"])).await;
    let _ = run(&db, cmd(&["XADD", "s", "2-0", "f", "b"])).await;
    let n = run(&db, cmd(&["XDEL", "s", "2-0"])).await;
    assert!(matches!(n, Resp::Integer(1)), "{n:?}");

    let info = run(&db, cmd(&["XINFO", "STREAM", "s"])).await;
    let parts = as_array(info);
    assert_eq!(
        info_field(&parts, "last-generated-id").as_deref(),
        Some("2-0"),
        "XDEL must not lower last-generated-id"
    );

    // Explicit id equal to deleted top must still be rejected.
    let err = run(&db, cmd(&["XADD", "s", "2-0", "f", "c"])).await;
    assert!(matches!(err, Resp::Error(_)), "{err:?}");
}
