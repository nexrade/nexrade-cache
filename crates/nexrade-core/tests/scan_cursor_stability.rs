//! SCAN cursor-stability guarantees.
//!
//! The contract SCAN must honour: **every key present for the entire
//! iteration is returned at least once**. Duplicates are explicitly
//! permitted; omissions are not.
//!
//! Regression origin: the cursor used to be a plain offset into a
//! freshly-materialised, sorted key list. Deleting keys mid-iteration —
//! the canonical `scan_iter()` + pipelined-delete cleanup pattern —
//! shrank the list under the cursor, so every surviving key shifted down
//! into slots already passed. The next round started beyond the new end,
//! returned nothing, and reported cursor 0 ("complete"). Seeding 40 keys
//! and deleting each page returned exactly 20 of 40, and
//! `examples/11-python-patterns/patterns.py` failed on `assert total == 40`.

use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter()
        .map(|a| Resp::BulkString(Some(bytes::Bytes::from(a.to_string()))))
        .collect()
}

async fn run(db: &Db, args: &[&str]) -> Resp {
    dispatch(db, cmd(args), 0).await
}

/// Parse a SCAN reply into (next_cursor, keys).
fn parse_scan(reply: &Resp) -> (String, Vec<String>) {
    let Resp::Array(Some(items)) = reply else {
        panic!("SCAN must reply with an array, got {reply:?}");
    };
    assert_eq!(items.len(), 2, "SCAN reply is [cursor, keys]");
    let cursor = match &items[0] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("SCAN cursor must be a bulk string, got {other:?}"),
    };
    let Resp::Array(Some(raw)) = &items[1] else {
        panic!("SCAN second element must be an array");
    };
    let keys = raw
        .iter()
        .map(|k| match k {
            Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("SCAN key must be a bulk string, got {other:?}"),
        })
        .collect();
    (cursor, keys)
}

/// The exact shape of the failing example: scan, delete each page, repeat.
/// Every seeded key must be returned before the cursor reports completion.
#[tokio::test]
async fn scan_returns_every_key_when_pages_are_deleted_mid_iteration() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });

    const TOTAL: usize = 40;
    for i in 0..TOTAL {
        run(&db, &["SET", &format!("tmp:gc:{i}"), "v"]).await;
    }

    let mut cursor = "0".to_string();
    let mut deleted = 0usize;
    // Generous ceiling: catches a non-terminating cursor without hanging CI.
    for _ in 0..100 {
        let reply = run(&db, &["SCAN", &cursor, "MATCH", "tmp:gc:*", "COUNT", "20"]).await;
        let (next, keys) = parse_scan(&reply);

        for k in &keys {
            let d = run(&db, &["DEL", k]).await;
            if let Resp::Integer(n) = d {
                deleted += n as usize;
            }
        }

        cursor = next;
        if cursor == "0" {
            break;
        }
    }

    assert_eq!(cursor, "0", "SCAN must terminate");
    assert_eq!(
        deleted, TOTAL,
        "SCAN skipped keys: deleted {deleted} of {TOTAL}. Every key present \
         for the whole iteration must be returned at least once."
    );

    let remaining = run(&db, &["KEYS", "tmp:gc:*"]).await;
    match remaining {
        Resp::Array(Some(items)) => assert!(
            items.is_empty(),
            "{} keys were never visited by SCAN",
            items.len()
        ),
        other => panic!("KEYS should return an array, got {other:?}"),
    }
}

/// Without mutation, a full pass must still cover the keyspace exactly.
#[tokio::test]
async fn scan_covers_all_keys_without_mutation() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });

    const TOTAL: usize = 40;
    for i in 0..TOTAL {
        run(&db, &["SET", &format!("plain:{i}"), "v"]).await;
    }

    let mut cursor = "0".to_string();
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..100 {
        let reply = run(&db, &["SCAN", &cursor, "MATCH", "plain:*", "COUNT", "7"]).await;
        let (next, keys) = parse_scan(&reply);
        seen.extend(keys);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }

    assert_eq!(cursor, "0", "SCAN must terminate");
    seen.sort();
    seen.dedup();
    assert_eq!(
        seen.len(),
        TOTAL,
        "a full SCAN pass must visit every key exactly once"
    );
}

/// A cursor whose boundary key has been deleted must not end iteration
/// early — the remaining keys still have to come back.
#[tokio::test]
async fn scan_resumes_after_boundary_key_is_deleted() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });

    for i in 0..30 {
        run(&db, &["SET", &format!("b:{i:02}"), "v"]).await;
    }

    // First page, then delete precisely the boundary key it ended on.
    let reply = run(&db, &["SCAN", "0", "MATCH", "b:*", "COUNT", "10"]).await;
    let (cursor, first_page) = parse_scan(&reply);
    assert_ne!(
        cursor, "0",
        "30 keys at COUNT 10 should not finish in one page"
    );
    let boundary = first_page.last().expect("first page is non-empty").clone();
    run(&db, &["DEL", &boundary]).await;

    // Continue; collect everything still reachable.
    let mut seen: Vec<String> = first_page;
    let mut cursor = cursor;
    for _ in 0..100 {
        let reply = run(&db, &["SCAN", &cursor, "MATCH", "b:*", "COUNT", "10"]).await;
        let (next, keys) = parse_scan(&reply);
        seen.extend(keys);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(cursor, "0", "SCAN must terminate");

    seen.sort();
    seen.dedup();
    // 30 seeded, 1 deleted mid-iteration. The deleted key may or may not
    // appear (it was returned before deletion); the other 29 must all be
    // present regardless.
    for i in 0..30 {
        let key = format!("b:{i:02}");
        if key == boundary {
            continue;
        }
        assert!(
            seen.contains(&key),
            "{key} was never returned after the boundary key was deleted"
        );
    }
}

// ── HSCAN / SSCAN / ZSCAN ────────────────────────────────────────────────
//
// Same offset-cursor flaw as SCAN, same 20-of-40 signature: removing
// elements mid-iteration shrank the list under the cursor and the scan
// reported "complete" with half the collection never returned. Real Redis
// returns all 40 for each of these.

/// Drive a sub-scan to completion, removing every page as it is returned.
/// Returns the number of distinct elements the scan surfaced.
async fn drive_removing(
    db: &Db,
    scan_cmd: &str,
    key: &str,
    remove_cmd: &str,
    count: usize,
) -> usize {
    let mut cursor = "0".to_string();
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..100 {
        let reply = run(db, &[scan_cmd, key, &cursor, "COUNT", &count.to_string()]).await;
        let (next, flat) = parse_scan(&reply);
        // HSCAN/ZSCAN interleave value/score; the element is every other entry.
        let step = if scan_cmd == "SSCAN" { 1 } else { 2 };
        let elems: Vec<String> = flat.iter().step_by(step).cloned().collect();

        for e in &elems {
            run(db, &[remove_cmd, key, e]).await;
        }
        seen.extend(elems);

        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(cursor, "0", "{scan_cmd} must terminate");
    seen.sort();
    seen.dedup();
    seen.len()
}

#[tokio::test]
async fn hscan_returns_every_field_when_pages_are_deleted_mid_iteration() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });
    for i in 0..40 {
        run(&db, &["HSET", "h", &format!("f{i:02}"), "v"]).await;
    }
    let seen = drive_removing(&db, "HSCAN", "h", "HDEL", 20).await;
    assert_eq!(
        seen, 40,
        "HSCAN skipped fields: saw {seen} of 40 while deleting each page"
    );
}

#[tokio::test]
async fn sscan_returns_every_member_when_pages_are_deleted_mid_iteration() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });
    for i in 0..40 {
        run(&db, &["SADD", "s", &format!("m{i:02}")]).await;
    }
    let seen = drive_removing(&db, "SSCAN", "s", "SREM", 20).await;
    assert_eq!(
        seen, 40,
        "SSCAN skipped members: saw {seen} of 40 while removing each page"
    );
}

#[tokio::test]
async fn zscan_returns_every_member_when_pages_are_deleted_mid_iteration() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });
    for i in 0..40 {
        run(&db, &["ZADD", "z", &i.to_string(), &format!("m{i:02}")]).await;
    }
    let seen = drive_removing(&db, "ZSCAN", "z", "ZREM", 20).await;
    assert_eq!(
        seen, 40,
        "ZSCAN skipped members: saw {seen} of 40 while removing each page"
    );
}

/// Deleting the boundary key must not rewind the cursor.
///
/// The page-deleting tests above cannot see this: they remove every key
/// they are handed, so a cursor that silently restarts at offset 0 still
/// finds only the not-yet-returned remainder and looks correct. The bug
/// shows up when the keyspace *survives* — a restart then re-serves the
/// same page forever. This pins forward progress directly.
#[tokio::test]
async fn scan_makes_forward_progress_when_boundary_key_is_deleted() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });

    const TOTAL: usize = 30;
    for i in 0..TOTAL {
        run(&db, &["SET", &format!("fp:{i:02}"), "v"]).await;
    }

    let mut cursor = "0".to_string();
    let mut returns = 0usize;
    let mut rounds = 0usize;
    for _ in 0..100 {
        rounds += 1;
        let reply = run(&db, &["SCAN", &cursor, "MATCH", "fp:*", "COUNT", "10"]).await;
        let (next, keys) = parse_scan(&reply);
        returns += keys.len();

        // Delete only the boundary key; every other key stays present.
        if let Some(boundary) = keys.last() {
            run(&db, &["DEL", boundary]).await;
        }

        cursor = next;
        if cursor == "0" {
            break;
        }
    }

    assert_eq!(cursor, "0", "SCAN must terminate");
    // Correct cursors walk the keyspace once: ~3 pages of 10. A cursor that
    // rewinds to offset 0 re-serves surviving keys and blows past this.
    assert!(
        returns <= TOTAL * 2,
        "SCAN re-served keys after the boundary was deleted: {returns} returns \
         over {rounds} rounds for {TOTAL} keys — the cursor is rewinding"
    );
}

/// The cursor must be a decimal integer on the wire.
///
/// Cursors are opaque to *application* code, but client libraries do parse
/// them: `redis-py` runs `int(cursor)` on every SCAN reply before handing
/// the value back. A cursor that is not a base-10 integer — hex, the raw
/// key bytes, an empty string — raises `ValueError` and breaks every
/// standard client, while the Rust tests above stay green because they
/// compare the cursor as an opaque string.
///
/// This pins the wire contract the other tests cannot see.
#[tokio::test]
async fn scan_cursor_is_a_decimal_integer_on_the_wire() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });

    // Keys chosen to include bytes that hex/binary encodings would mangle.
    for i in 0..40 {
        run(&db, &["SET", &format!("wire:{i:02}"), "v"]).await;
    }

    let check = |c: &str| {
        assert!(
            !c.is_empty(),
            "cursor must never be the empty string: int('') fails in clients"
        );
        assert!(
            c.parse::<u64>().is_ok(),
            "cursor {c:?} is not a base-10 u64 — redis-py's int(cursor) \
             would raise ValueError and break the client"
        );
    };

    let mut cursor = "0".to_string();
    for _ in 0..100 {
        let reply = run(&db, &["SCAN", &cursor, "MATCH", "wire:*", "COUNT", "7"]).await;
        let (next, _) = parse_scan(&reply);
        check(&next);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(cursor, "0", "SCAN must terminate");

    // Same contract for the sub-scans.
    for i in 0..40 {
        run(&db, &["HSET", "wh", &format!("f{i:02}"), "v"]).await;
        run(&db, &["SADD", "ws", &format!("m{i:02}")]).await;
        run(&db, &["ZADD", "wz", &i.to_string(), &format!("m{i:02}")]).await;
    }
    for (cmd, key) in [("HSCAN", "wh"), ("SSCAN", "ws"), ("ZSCAN", "wz")] {
        let mut cursor = "0".to_string();
        for _ in 0..100 {
            let reply = run(&db, &[cmd, key, &cursor, "COUNT", "7"]).await;
            let (next, _) = parse_scan(&reply);
            check(&next);
            cursor = next;
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(cursor, "0", "{cmd} must terminate");
    }
}

/// A cursor above `i64::MAX` must survive the round trip.
///
/// Tokens are `u64`, so ~35% of them exceed `i64::MAX`. Parsing the cursor
/// through a signed integer overflowed on those and silently restarted the
/// scan at page 1 — returning the same first page forever.
#[tokio::test]
async fn scan_accepts_cursors_above_i64_max() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });
    for i in 0..200 {
        run(&db, &["SET", &format!("big:{i}"), "v"]).await;
    }

    // Walk the whole keyspace and confirm at least one emitted cursor sits
    // above i64::MAX — otherwise this test would silently prove nothing.
    let mut cursor = "0".to_string();
    let mut saw_large = false;
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..500 {
        let reply = run(&db, &["SCAN", &cursor, "MATCH", "big:*", "COUNT", "5"]).await;
        let (next, keys) = parse_scan(&reply);
        seen.extend(keys);
        if next.parse::<u64>().expect("decimal cursor") > i64::MAX as u64 {
            saw_large = true;
        }
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(cursor, "0", "SCAN must terminate");
    assert!(
        saw_large,
        "no cursor above i64::MAX was produced; this test cannot detect \
         the signed-overflow regression it exists to catch"
    );
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 200, "every key must be returned exactly once");
}

// ── Shard-scoped paging (1.2.x) ──────────────────────────────────────────
//
// SCAN pages out of one shard at a time instead of materialising and sorting
// the whole keyspace per call. These pin the behaviour that rework must not
// change: TYPE/MATCH/COUNT still compose, expired keys stay invisible, and
// SCAN no longer reaps them.

/// TYPE, MATCH, and COUNT together must still page correctly. TYPE is now
/// evaluated inside the shard guard rather than by re-locking per key.
#[tokio::test]
async fn scan_type_match_and_count_compose() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });

    for i in 0..60 {
        run(&db, &["SET", &format!("t:str:{i}"), "v"]).await;
        run(&db, &["LPUSH", &format!("t:list:{i}"), "v"]).await;
        run(&db, &["SET", &format!("other:{i}"), "v"]).await;
    }

    let mut cursor = "0".to_string();
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..500 {
        let reply = run(
            &db,
            &[
                "SCAN", &cursor, "MATCH", "t:*", "COUNT", "7", "TYPE", "string",
            ],
        )
        .await;
        let (next, keys) = parse_scan(&reply);
        assert!(
            keys.len() <= 7,
            "COUNT must cap the page: got {}",
            keys.len()
        );
        seen.extend(keys);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(cursor, "0", "SCAN must terminate");

    seen.sort();
    seen.dedup();
    assert_eq!(
        seen.len(),
        60,
        "TYPE string + MATCH t:* must return exactly the 60 string keys"
    );
    assert!(
        seen.iter().all(|k| k.starts_with("t:str:")),
        "lists and non-matching keys must be filtered out"
    );
}

/// An expired key must never be returned, and — unlike the old
/// `keys_matching` path, which write-locked every shard to reap — SCAN must
/// not delete it either. Reclamation belongs to the active expiry cycle.
///
/// Note the TTL has to be short-but-future: `PEXPIREAT` with a deadline
/// already in the past *deletes* the key immediately (Redis semantics), so
/// seeding that way would leave nothing in the store and the test would
/// prove nothing about SCAN.
#[tokio::test]
async fn scan_hides_expired_keys_without_reaping_them() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });

    for i in 0..30 {
        run(&db, &["SET", &format!("live:{i}"), "v"]).await;
        run(&db, &["SET", &format!("gone:{i}"), "v"]).await;
        run(&db, &["PEXPIRE", &format!("gone:{i}"), "40"]).await;
    }

    // Let the 40ms TTLs lapse. The keys are now logically expired but still
    // physically present (nothing has touched them).
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    let before = match run(&db, &["DBSIZE"]).await {
        Resp::Integer(n) => n,
        other => panic!("DBSIZE should return an integer, got {other:?}"),
    };
    assert_eq!(
        before, 60,
        "expired keys should still be resident before SCAN runs"
    );

    let mut cursor = "0".to_string();
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..500 {
        let reply = run(&db, &["SCAN", &cursor, "COUNT", "10"]).await;
        let (next, keys) = parse_scan(&reply);
        seen.extend(keys);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(cursor, "0", "SCAN must terminate");

    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 30, "only the 30 live keys may be returned");
    assert!(
        seen.iter().all(|k| k.starts_with("live:")),
        "expired keys must be invisible to SCAN"
    );

    // The point of the test: SCAN is `readonly` and takes read locks, so it
    // must not have deleted the 30 expired keys it walked past.
    let after = match run(&db, &["DBSIZE"]).await {
        Resp::Integer(n) => n,
        other => panic!("DBSIZE should return an integer, got {other:?}"),
    };
    assert_eq!(
        after, before,
        "SCAN must not reap expired keys (DBSIZE {before} -> {after})"
    );
}

/// A large keyspace must be covered exactly once with no duplicates when
/// nothing mutates — this is what catches an off-by-one at a shard boundary,
/// where a page could re-serve or skip the first key of the next shard.
#[tokio::test]
async fn scan_covers_a_large_keyspace_across_shard_boundaries() {
    let db = Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    });

    const TOTAL: usize = 2000;
    for i in 0..TOTAL {
        run(&db, &["SET", &format!("wide:{i}"), "v"]).await;
    }

    for &count in &["1", "3", "50"] {
        let mut cursor = "0".to_string();
        let mut seen: Vec<String> = Vec::new();
        let mut rounds = 0;
        loop {
            rounds += 1;
            assert!(rounds < 20_000, "SCAN must terminate at COUNT={count}");
            let reply = run(&db, &["SCAN", &cursor, "MATCH", "wide:*", "COUNT", count]).await;
            let (next, keys) = parse_scan(&reply);
            seen.extend(keys);
            cursor = next;
            if cursor == "0" {
                break;
            }
        }
        let returned = seen.len();
        seen.sort();
        seen.dedup();
        assert_eq!(
            seen.len(),
            TOTAL,
            "COUNT={count}: {} of {TOTAL} keys returned",
            seen.len()
        );
        assert_eq!(
            returned, TOTAL,
            "COUNT={count}: static keyspace must not yield duplicates"
        );
    }
}
