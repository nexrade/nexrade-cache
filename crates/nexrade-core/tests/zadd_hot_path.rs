//! Behavioral tests for the `ZSetData::insert` single-lookup fix (Fix C,
//! Level 1) backing ZADD/ZINCRBY. These don't instrument lookup counts
//! directly (that would require intrusive counters in the hot path) —
//! instead they verify that the observable behavior, including NX/XX/GT/LT/
//! CH/INCR semantics and the `by_score` index, is unchanged after the
//! reorder.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

fn score_of(r: &Resp) -> Option<f64> {
    match r {
        Resp::BulkString(Some(b)) => std::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

#[tokio::test]
async fn zadd_creates_zset_on_absent_key() {
    let db = Db::default();
    let r = run(&db, cmd(&["ZADD", "z", "1", "a"])).await;
    assert!(matches!(r, Resp::Integer(1)));
    let r = run(&db, cmd(&["ZSCORE", "z", "a"])).await;
    assert_eq!(score_of(&r), Some(1.0));
}

#[tokio::test]
async fn zadd_updates_score_on_existing_member() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "1", "a", "2", "b"])).await;
    let r = run(&db, cmd(&["ZADD", "z", "5", "a"])).await;
    assert!(matches!(r, Resp::Integer(0))); // not new
    let r = run(&db, cmd(&["ZSCORE", "z", "a"])).await;
    assert_eq!(score_of(&r), Some(5.0));

    // by_score ordering must reflect the new score, not a stale entry.
    let r = run(&db, cmd(&["ZRANGE", "z", "0", "-1"])).await;
    let Resp::Array(Some(items)) = r else {
        panic!("expected array, got {r:?}")
    };
    assert_eq!(items.len(), 2);
    assert!(matches!(&items[0], Resp::BulkString(Some(b)) if b.as_ref() == b"b"));
    assert!(matches!(&items[1], Resp::BulkString(Some(b)) if b.as_ref() == b"a"));
}

#[tokio::test]
async fn zadd_nx_skips_existing_member() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "1", "a"])).await;
    let r = run(&db, cmd(&["ZADD", "z", "NX", "99", "a"])).await;
    assert!(matches!(r, Resp::Integer(0)));
    let r = run(&db, cmd(&["ZSCORE", "z", "a"])).await;
    assert_eq!(
        score_of(&r),
        Some(1.0),
        "NX must not touch an existing member"
    );
}

#[tokio::test]
async fn zadd_nx_adds_new_member() {
    let db = Db::default();
    let r = run(&db, cmd(&["ZADD", "z", "NX", "1", "a"])).await;
    assert!(matches!(r, Resp::Integer(1)));
}

#[tokio::test]
async fn zadd_xx_skips_missing_member() {
    let db = Db::default();
    let r = run(&db, cmd(&["ZADD", "z", "XX", "1", "a"])).await;
    assert!(matches!(r, Resp::Integer(0)));
    let r = run(&db, cmd(&["ZSCORE", "z", "a"])).await;
    assert!(matches!(r, Resp::BulkString(None)));
}

#[tokio::test]
async fn zadd_xx_updates_existing_member() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "1", "a"])).await;
    let r = run(&db, cmd(&["ZADD", "z", "XX", "9", "a"])).await;
    assert!(matches!(r, Resp::Integer(0)));
    let r = run(&db, cmd(&["ZSCORE", "z", "a"])).await;
    assert_eq!(score_of(&r), Some(9.0));
}

#[tokio::test]
async fn zadd_gt_skips_lower_or_equal_score() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "5", "a"])).await;
    let _ = run(&db, cmd(&["ZADD", "z", "GT", "5", "a"])).await;
    let _ = run(&db, cmd(&["ZADD", "z", "GT", "3", "a"])).await;
    let r = run(&db, cmd(&["ZSCORE", "z", "a"])).await;
    assert_eq!(score_of(&r), Some(5.0), "GT must skip equal/lower scores");
}

#[tokio::test]
async fn zadd_gt_updates_higher_score() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "5", "a"])).await;
    let r = run(&db, cmd(&["ZADD", "z", "GT", "10", "a"])).await;
    assert!(matches!(r, Resp::Integer(0)));
    let r = run(&db, cmd(&["ZSCORE", "z", "a"])).await;
    assert_eq!(score_of(&r), Some(10.0));
}

#[tokio::test]
async fn zadd_lt_skips_higher_or_equal_score() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "5", "a"])).await;
    let _ = run(&db, cmd(&["ZADD", "z", "LT", "5", "a"])).await;
    let _ = run(&db, cmd(&["ZADD", "z", "LT", "9", "a"])).await;
    let r = run(&db, cmd(&["ZSCORE", "z", "a"])).await;
    assert_eq!(score_of(&r), Some(5.0), "LT must skip equal/higher scores");
}

#[tokio::test]
async fn zadd_lt_updates_lower_score() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "5", "a"])).await;
    let r = run(&db, cmd(&["ZADD", "z", "LT", "1", "a"])).await;
    assert!(matches!(r, Resp::Integer(0)));
    let r = run(&db, cmd(&["ZSCORE", "z", "a"])).await;
    assert_eq!(score_of(&r), Some(1.0));
}

#[tokio::test]
async fn zadd_ch_counts_changed_not_just_added() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "1", "a", "2", "b"])).await;
    // Change "a"'s score, leave "b" alone, add "c" new.
    let r = run(&db, cmd(&["ZADD", "z", "CH", "9", "a", "2", "b", "3", "c"])).await;
    assert!(
        matches!(r, Resp::Integer(2)),
        "CH should count 'a' (changed) + 'c' (added) = 2, not just the 1 new member"
    );
}

#[tokio::test]
async fn zadd_incr_returns_new_score_and_creates_if_absent() {
    let db = Db::default();
    let r = run(&db, cmd(&["ZADD", "z", "INCR", "5", "a"])).await;
    assert_eq!(score_of(&r), Some(5.0));
    let r = run(&db, cmd(&["ZADD", "z", "INCR", "3", "a"])).await;
    assert_eq!(score_of(&r), Some(8.0));
}

#[tokio::test]
async fn zadd_added_and_changed_counts_correct_multi_member() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "1", "a", "2", "b"])).await;
    // "a" changed, "b" unchanged (same score), "c" new.
    let r = run(&db, cmd(&["ZADD", "z", "9", "a", "2", "b", "3", "c"])).await;
    assert!(
        matches!(r, Resp::Integer(1)),
        "plain ZADD (no CH) should only count 'c' as newly added"
    );
    let r = run(&db, cmd(&["ZADD", "z", "CH", "9", "a", "2", "b", "3", "c"])).await;
    // Re-running: "a"/"c" already at these scores (unchanged), "b" unchanged too.
    assert!(matches!(r, Resp::Integer(0)));
}

#[tokio::test]
async fn zadd_wrong_type_still_errors() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;
    let r = run(&db, cmd(&["ZADD", "k", "1", "a"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn zadd_on_lazily_expired_key_recreates_fresh() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "1", "old"])).await;
    let _ = run(&db, cmd(&["PEXPIRE", "z", "1"])).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let r = run(&db, cmd(&["ZADD", "z", "1", "new"])).await;
    assert!(matches!(r, Resp::Integer(1)));
    let r = run(&db, cmd(&["ZSCORE", "z", "old"])).await;
    assert!(
        matches!(r, Resp::BulkString(None)),
        "old member should be gone after expiry+recreate"
    );
}

#[tokio::test]
async fn zadd_score_unchanged_reinsert_is_idempotent() {
    // Regression guard for the Level-1 optimization that skips the
    // by_score remove/insert churn when the score is unchanged — a bug
    // there could leave stale duplicate by_score entries.
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "5", "a", "10", "b"])).await;
    // Re-ZADD "a" at the same score, twice.
    let _ = run(&db, cmd(&["ZADD", "z", "5", "a"])).await;
    let _ = run(&db, cmd(&["ZADD", "z", "5", "a"])).await;

    let r = run(&db, cmd(&["ZCARD", "z"])).await;
    assert!(matches!(r, Resp::Integer(2)), "no duplicate members");

    let r = run(&db, cmd(&["ZRANGE", "z", "0", "-1"])).await;
    let Resp::Array(Some(items)) = r else {
        panic!("expected array, got {r:?}")
    };
    assert_eq!(
        items.len(),
        2,
        "by_score index must not have accumulated stale duplicate entries for 'a'"
    );
    assert!(matches!(&items[0], Resp::BulkString(Some(b)) if b.as_ref() == b"a"));
    assert!(matches!(&items[1], Resp::BulkString(Some(b)) if b.as_ref() == b"b"));
}
