//! Regression tests for:
//!  1. EXPIRE GT/LT semantics on a key with no existing TTL (persistent =
//!     infinite TTL) and non-positive / past deadlines deleting the key.
//!  2. The newly-added stream commands XINFO / XCLAIM / XAUTOCLAIM.

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
        other => panic!("expected integer, got {other:?}"),
    }
}

// ── EXPIRE GT/LT on persistent keys ─────────────────────────────────────────

#[tokio::test]
async fn expire_gt_on_persistent_key_does_not_apply() {
    // Redis: "A non-volatile key is treated as an infinite TTL for GT." A
    // finite new expiry is never greater than infinity, so GT must be a no-op
    // (return 0, key stays persistent).
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let resp = run(&db, cmd(&["EXPIRE", "k", "100", "GT"])).await;
    assert_eq!(as_int(&resp), 0, "GT on persistent key must not set expiry");
    let ttl = run(&db, cmd(&["TTL", "k"])).await;
    assert_eq!(as_int(&ttl), -1, "key must stay persistent after GT no-op");
}

#[tokio::test]
async fn expire_lt_on_persistent_key_applies() {
    // Redis: "A non-volatile key is treated as an infinite TTL for LT." Any
    // finite new expiry is less than infinity, so LT must apply.
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let resp = run(&db, cmd(&["EXPIRE", "k", "100", "LT"])).await;
    assert_eq!(as_int(&resp), 1, "LT on persistent key must set expiry");
    let ttl = run(&db, cmd(&["TTL", "k"])).await;
    assert!(
        (1..=100).contains(&as_int(&ttl)),
        "TTL should be ~100 after LT applied, got {}",
        as_int(&ttl)
    );
}

#[tokio::test]
async fn expire_gt_lowers_never() {
    // GT with an existing, larger new value applies; with a smaller value, it
    // does not.
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let _ = run(&db, cmd(&["EXPIRE", "k", "100"])).await;
    // New value 50 < current 100 → GT must not apply.
    let r1 = run(&db, cmd(&["EXPIRE", "k", "50", "GT"])).await;
    assert_eq!(as_int(&r1), 0);
    // New value 200 > current → applies.
    let r2 = run(&db, cmd(&["EXPIRE", "k", "200", "GT"])).await;
    assert_eq!(as_int(&r2), 1);
}

#[tokio::test]
async fn expire_negative_deletes_key() {
    // Redis: EXPIRE with a non-positive timeout deletes the key and returns 1.
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let resp = run(&db, cmd(&["EXPIRE", "k", "-1"])).await;
    assert_eq!(as_int(&resp), 1, "non-positive EXPIRE returns 1");
    let exists = run(&db, cmd(&["EXISTS", "k"])).await;
    assert_eq!(as_int(&exists), 0, "key must be deleted, not error");
}

#[tokio::test]
async fn expireat_past_timestamp_deletes_key() {
    // Redis: EXPIREAT with a timestamp in the past deletes the key immediately.
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let resp = run(&db, cmd(&["EXPIREAT", "k", "1"])).await; // 1970
    assert_eq!(as_int(&resp), 1);
    let exists = run(&db, cmd(&["EXISTS", "k"])).await;
    assert_eq!(as_int(&exists), 0);
}

#[tokio::test]
async fn expire_negative_with_xx_on_persistent_key_is_noop() {
    // XX must fail first (no existing expiry) → returns 0, key NOT deleted.
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let resp = run(&db, cmd(&["EXPIRE", "k", "-1", "XX"])).await;
    assert_eq!(as_int(&resp), 0, "XX condition fails before deletion");
    let exists = run(&db, cmd(&["EXISTS", "k"])).await;
    assert_eq!(as_int(&exists), 1, "key must survive skipped condition");
}

// ── XINFO ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn xinfo_stream_reports_length_and_groups() {
    let db = Db::default();
    let _ = run(&db, cmd(&["XADD", "s", "1-1", "f", "v"])).await;
    let _ = run(&db, cmd(&["XADD", "s", "2-1", "f", "v"])).await;
    let _ = run(&db, cmd(&["XGROUP", "CREATE", "s", "g1", "0"])).await;
    let resp = run(&db, cmd(&["XINFO", "STREAM", "s"])).await;
    // Flatten the field/value reply into a map-ish check.
    let arr = match resp {
        Resp::Array(Some(a)) => a,
        other => panic!("expected array, got {other:?}"),
    };
    // "length" should be immediately followed by 2, "groups" by 1.
    let idx_len = arr
        .iter()
        .position(|r| matches!(r, Resp::BulkString(Some(b)) if b.as_ref() == b"length"))
        .expect("length field present");
    assert_eq!(as_int(&arr[idx_len + 1]), 2);
    let idx_groups = arr
        .iter()
        .position(|r| matches!(r, Resp::BulkString(Some(b)) if b.as_ref() == b"groups"))
        .expect("groups field present");
    assert_eq!(as_int(&arr[idx_groups + 1]), 1);
}

#[tokio::test]
async fn xinfo_groups_lists_created_group() {
    let db = Db::default();
    let _ = run(&db, cmd(&["XADD", "s", "1-1", "f", "v"])).await;
    let _ = run(&db, cmd(&["XGROUP", "CREATE", "s", "g1", "0"])).await;
    let resp = run(&db, cmd(&["XINFO", "GROUPS", "s"])).await;
    match resp {
        Resp::Array(Some(groups)) => assert_eq!(groups.len(), 1),
        other => panic!("expected array of groups, got {other:?}"),
    }
}

#[tokio::test]
async fn xinfo_stream_missing_key_errors() {
    let db = Db::default();
    let resp = run(&db, cmd(&["XINFO", "STREAM", "nope"])).await;
    assert!(matches!(resp, Resp::Error(_)), "missing key must error");
}

// ── XCLAIM / XAUTOCLAIM ─────────────────────────────────────────────────────

#[tokio::test]
async fn xclaim_transfers_pending_ownership() {
    let db = Db::default();
    let _ = run(&db, cmd(&["XADD", "s", "1-1", "f", "v"])).await;
    let _ = run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await;
    // c1 reads the entry, becoming its pending owner.
    let _ = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;
    // c2 claims it with min-idle-time 0.
    let resp = run(&db, cmd(&["XCLAIM", "s", "g", "c2", "0", "1-1"])).await;
    match resp {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 1, "one entry claimed"),
        other => panic!("expected claimed array, got {other:?}"),
    }
    // XPENDING for c2 should now show the entry.
    let pend = run(&db, cmd(&["XPENDING", "s", "g", "-", "+", "10", "c2"])).await;
    match pend {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 1, "c2 now owns the pending entry"),
        other => panic!("expected pending array, got {other:?}"),
    }
}

#[tokio::test]
async fn xclaim_respects_min_idle_time() {
    let db = Db::default();
    let _ = run(&db, cmd(&["XADD", "s", "1-1", "f", "v"])).await;
    let _ = run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await;
    let _ = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;
    // Huge min-idle-time: entry was just delivered, so nothing claims.
    let resp = run(&db, cmd(&["XCLAIM", "s", "g", "c2", "999999", "1-1"])).await;
    match resp {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 0, "min-idle-time blocks the claim"),
        other => panic!("expected empty array, got {other:?}"),
    }
}

#[tokio::test]
async fn xautoclaim_returns_cursor_and_claimed() {
    let db = Db::default();
    let _ = run(&db, cmd(&["XADD", "s", "1-1", "f", "v"])).await;
    let _ = run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await;
    let _ = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;
    let resp = run(&db, cmd(&["XAUTOCLAIM", "s", "g", "c2", "0", "0"])).await;
    match resp {
        Resp::Array(Some(parts)) => {
            assert_eq!(parts.len(), 3, "cursor + claimed + deleted");
            // Claimed set has the one entry.
            match &parts[1] {
                Resp::Array(Some(items)) => assert_eq!(items.len(), 1),
                other => panic!("expected claimed array, got {other:?}"),
            }
        }
        other => panic!("expected 3-element reply, got {other:?}"),
    }
}
