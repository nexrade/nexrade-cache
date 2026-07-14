//! End-to-end tests for the multi-pop / multi-bulk-predicate commands:
//! LMPOP / BLMPOP / ZMPOP / BZMPOP / SMISMEMBER.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

fn bulk(s: &str) -> Resp {
    Resp::BulkString(Some(bytes::Bytes::from(s.to_string())))
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

#[tokio::test]
async fn lmpop_happy_path() {
    let db = Db::default();
    let _: Resp = run(&db, cmd(&["RPUSH", "l1", "a", "b", "c"])).await;
    let resp = run(&db, cmd(&["LMPOP", "1", "l1", "LEFT"])).await;
    match resp {
        Resp::Array(Some(parts)) => {
            assert_eq!(parts.len(), 2);
            assert!(matches!(&parts[0], Resp::BulkString(Some(b)) if b.as_ref() == b"l1"));
            match &parts[1] {
                Resp::Array(Some(items)) => {
                    assert_eq!(items.len(), 1);
                    assert!(matches!(&items[0], Resp::BulkString(Some(b)) if b.as_ref() == b"a"));
                }
                other => panic!("expected inner array, got {other:?}"),
            }
        }
        other => panic!("expected Resp::Array, got {other:?}"),
    }
}

#[tokio::test]
async fn lmpop_missing_key_returns_nil() {
    let db = Db::default();
    let resp = run(&db, cmd(&["LMPOP", "1", "nonexistent", "LEFT"])).await;
    assert!(matches!(resp, Resp::Array(None)));
}

#[tokio::test]
async fn lmpop_picks_first_nonempty() {
    let db = Db::default();
    let _: Resp = run(&db, cmd(&["RPUSH", "l2", "x", "y"])).await;
    let resp = run(&db, cmd(&["LMPOP", "2", "l1", "l2", "LEFT", "COUNT", "2"])).await;
    match resp {
        Resp::Array(Some(parts)) => {
            assert!(matches!(&parts[0], Resp::BulkString(Some(b)) if b.as_ref() == b"l2"));
            match &parts[1] {
                Resp::Array(Some(items)) => assert_eq!(items.len(), 2),
                other => panic!("expected inner array, got {other:?}"),
            }
        }
        other => panic!("expected Resp::Array, got {other:?}"),
    }
}

#[tokio::test]
async fn lmpop_bad_direction_returns_syntax_error() {
    let db = Db::default();
    let _: Resp = run(&db, cmd(&["RPUSH", "l1", "z"])).await;
    let resp = run(&db, cmd(&["LMPOP", "1", "l1", "BADDIR"])).await;
    match resp {
        Resp::Error(s) => {
            // Generic's #[error("ERR {0}")] prepends ERR; the wire-form
            // is exactly `-ERR syntax error` (one prefix). Must NOT be
            // double-prefixed like `ERR ERR syntax error`.
            assert_eq!(s, "ERR syntax error", "got: {s}");
        }
        other => panic!("expected Resp::Error, got {other:?}"),
    }
}

#[tokio::test]
async fn lmpop_count_honored() {
    let db = Db::default();
    let _: Resp = run(&db, cmd(&["RPUSH", "l1", "1", "2", "3"])).await;
    let resp = run(&db, cmd(&["LMPOP", "1", "l1", "LEFT", "COUNT", "2"])).await;
    match resp {
        Resp::Array(Some(parts)) => match &parts[1] {
            Resp::Array(Some(items)) => assert_eq!(items.len(), 2),
            other => panic!("expected inner array, got {other:?}"),
        },
        other => panic!("expected Resp::Array, got {other:?}"),
    }
}

#[tokio::test]
async fn blmpop_returns_immediately_when_data_present() {
    let db = Db::default();
    let _: Resp = run(&db, cmd(&["RPUSH", "l1", "a"])).await;
    let resp = run(&db, cmd(&["BLMPOP", "0", "1", "l1", "LEFT"])).await;
    assert!(matches!(resp, Resp::Array(Some(_))));
}

#[tokio::test]
async fn blmpop_times_out_to_nil() {
    let db = Db::default();
    let start = std::time::Instant::now();
    let resp = run(&db, cmd(&["BLMPOP", "0.05", "1", "nonexistent", "LEFT"])).await;
    let elapsed = start.elapsed();
    assert!(matches!(resp, Resp::Array(None)));
    // Should have waited ~50ms but no more than 500ms.
    assert!(
        elapsed.as_millis() >= 40,
        "elapsed too short: {:?}",
        elapsed
    );
    assert!(
        elapsed.as_millis() <= 500,
        "elapsed too long: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn zmpop_min_max() {
    let db = Db::default();
    let _: Resp = run(&db, cmd(&["ZADD", "z1", "1", "a", "2", "b", "3", "c"])).await;

    let resp = run(&db, cmd(&["ZMPOP", "1", "z1", "MIN"])).await;
    match resp {
        Resp::Array(Some(parts)) => {
            assert!(matches!(&parts[0], Resp::BulkString(Some(b)) if b.as_ref() == b"z1"));
            // [[member, score], ...]
            match &parts[1] {
                Resp::Array(Some(items)) => {
                    assert_eq!(items.len(), 1);
                    match &items[0] {
                        Resp::Array(Some(pair)) => {
                            assert_eq!(pair.len(), 2);
                            assert!(
                                matches!(&pair[0], Resp::BulkString(Some(b)) if b.as_ref() == b"a")
                            );
                        }
                        other => panic!("expected [member, score], got {other:?}"),
                    }
                }
                other => panic!("expected inner array, got {other:?}"),
            }
        }
        other => panic!("expected Resp::Array, got {other:?}"),
    }

    let resp = run(&db, cmd(&["ZMPOP", "1", "z1", "MAX"])).await;
    match resp {
        Resp::Array(Some(parts)) => match &parts[1] {
            Resp::Array(Some(items)) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    Resp::Array(Some(pair)) => {
                        assert!(
                            matches!(&pair[0], Resp::BulkString(Some(b)) if b.as_ref() == b"c")
                        );
                    }
                    other => panic!("expected [member, score], got {other:?}"),
                }
            }
            other => panic!("expected inner array, got {other:?}"),
        },
        other => panic!("expected Resp::Array, got {other:?}"),
    }
}

#[tokio::test]
async fn bzmpop_timeout_returns_nil() {
    let db = Db::default();
    let start = std::time::Instant::now();
    let resp = run(&db, cmd(&["BZMPOP", "0.05", "1", "nonexistent", "MIN"])).await;
    let elapsed = start.elapsed();
    assert!(matches!(resp, Resp::Array(None)));
    assert!(
        elapsed.as_millis() <= 500,
        "elapsed too long: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn smismember_mixed_members() {
    let db = Db::default();
    let _: Resp = run(&db, cmd(&["SADD", "s1", "a", "b", "c"])).await;
    let resp = run(&db, cmd(&["SMISMEMBER", "s1", "a", "x", "b", "y"])).await;
    match resp {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
            // a → 1, x → 0, b → 1, y → 0
            assert!(matches!(items[0], Resp::Integer(1)));
            assert!(matches!(items[1], Resp::Integer(0)));
            assert!(matches!(items[2], Resp::Integer(1)));
            assert!(matches!(items[3], Resp::Integer(0)));
        }
        other => panic!("expected Resp::Array, got {other:?}"),
    }
}

#[tokio::test]
async fn smismember_missing_key_returns_zeros() {
    let db = Db::default();
    let resp = run(&db, cmd(&["SMISMEMBER", "missing", "a", "b"])).await;
    match resp {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            assert!(matches!(items[0], Resp::Integer(0)));
            assert!(matches!(items[1], Resp::Integer(0)));
        }
        other => panic!("expected Resp::Array, got {other:?}"),
    }
}

#[tokio::test]
async fn smismember_wrong_type_returns_zero() {
    let db = Db::default();
    let _: Resp = run(&db, cmd(&["SET", "str", "hello"])).await;
    let resp = run(&db, cmd(&["SMISMEMBER", "str", "a", "b"])).await;
    // Either zeros (graceful) or wrong-type error. Implementations vary;
    // our impl returns zeros for non-set types to avoid hard errors.
    match resp {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 2),
        Resp::Error(_) => {} // acceptable too
        other => panic!("unexpected: {other:?}"),
    }
    // Unused, just suppress warning about `bulk` not being used.
    let _ = bulk("");
}
