//! Regression: BZMPOP must wake when a producer runs ZADD / ZINCRBY on the
//! watched key. Before the fix BZMPOP waited on `list_notify`, which only
//! fires on list/stream pushes — pure zset producers never unblocked the
//! waiter.

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

fn as_array(r: Resp) -> Vec<Resp> {
    match r {
        Resp::Array(Some(a)) => a,
        other => panic!("expected array, got {other:?}"),
    }
}

#[tokio::test]
async fn bzmpop_wakes_on_zadd() {
    let db = Db::default();

    // Spawn a waiter on an empty zset with a generous timeout so a hang is
    // distinguishable from a clean timeout.
    let waiter_db = db.clone();
    let waiter =
        tokio::spawn(async move { run(&waiter_db, cmd(&["BZMPOP", "5", "1", "zq", "MIN"])).await });

    // Give the waiter a moment to park on zset_notify.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Producer: ZADD should notify and unblock the waiter.
    let added = run(&db, cmd(&["ZADD", "zq", "1", "m1"])).await;
    assert!(
        matches!(added, Resp::Integer(1)),
        "ZADD must insert, got {added:?}"
    );

    let resp = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("BZMPOP task must finish")
        .expect("BZMPOP task must not panic");

    // Shape: [key, [[member, score], ...]]  (or flat under RESP2 — either way
    // a non-null array with the key present).
    match resp {
        Resp::Array(Some(parts)) => {
            assert!(!parts.is_empty(), "BZMPOP reply must not be empty");
            // First element is the key name.
            let key = match &parts[0] {
                Resp::BulkString(Some(b)) => std::str::from_utf8(b).unwrap_or(""),
                other => panic!("expected key bulk, got {other:?}"),
            };
            assert_eq!(key, "zq");
        }
        Resp::Array(None) | Resp::Null | Resp::BulkString(None) => {
            panic!("BZMPOP timed out / returned null — ZADD did not wake it");
        }
        other => panic!("unexpected BZMPOP reply: {other:?}"),
    }
}

#[tokio::test]
async fn bzmpop_wakes_on_zincrby() {
    let db = Db::default();

    let waiter_db = db.clone();
    let waiter = tokio::spawn(async move {
        run(
            &waiter_db,
            cmd(&["BZMPOP", "5", "1", "zq2", "MAX", "COUNT", "1"]),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let _ = run(&db, cmd(&["ZINCRBY", "zq2", "3.5", "m"])).await;

    let resp = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("BZMPOP task must finish")
        .expect("BZMPOP task must not panic");

    let parts = as_array(resp);
    assert!(!parts.is_empty(), "BZMPOP via ZINCRBY must return data");
}

#[tokio::test]
async fn bzmpop_timeout_still_returns_null_when_empty() {
    let db = Db::default();
    // Short timeout, no producer — must return null array, not hang.
    let resp = tokio::time::timeout(
        Duration::from_secs(3),
        run(&db, cmd(&["BZMPOP", "0.2", "1", "empty", "MIN"])),
    )
    .await
    .expect("BZMPOP must finish within timeout budget");

    assert!(
        matches!(
            resp,
            Resp::Array(None) | Resp::Null | Resp::BulkString(None)
        ) || matches!(&resp, Resp::Array(Some(a)) if a.is_empty()),
        "empty BZMPOP on timeout must be null-ish, got {resp:?}"
    );
}
