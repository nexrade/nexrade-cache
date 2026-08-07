//! 1.3.0 — `HSCAN`/`SSCAN`/`ZSCAN` page selection.
//!
//! The sub-scans used to materialise the whole collection, sort it by
//! `(token, bytes)`, and slice one page out — O(n log n) per page and
//! O(n² log n) for a full walk. `scan_select_page` replaced that with a
//! bounded-heap selection.
//!
//! This is a pure optimisation, so the tests that matter are **equivalence**
//! tests: the page contents, the page order, and the `next_cursor` must match
//! what the sort-and-slice produced, and a full walk must still visit every
//! element exactly once. A reference implementation of the old algorithm is
//! included below and compared against the live command output.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::resp::Resp;

fn b(s: &str) -> Resp {
    Resp::bulk_str(s)
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

/// Unpack a SCAN-family reply into `(next_cursor, flat_items)`.
fn unpack(r: Resp) -> (String, Vec<String>) {
    match r {
        Resp::Array(Some(parts)) => {
            assert_eq!(parts.len(), 2, "scan reply is [cursor, items]");
            let cursor = match &parts[0] {
                Resp::BulkString(Some(c)) => String::from_utf8_lossy(c).into_owned(),
                other => panic!("cursor should be a bulk string, got {other:?}"),
            };
            let items = match &parts[1] {
                Resp::Array(Some(items)) => items
                    .iter()
                    .map(|i| match i {
                        Resp::BulkString(Some(v)) => String::from_utf8_lossy(v).into_owned(),
                        other => panic!("item should be a bulk string, got {other:?}"),
                    })
                    .collect(),
                other => panic!("items should be an array, got {other:?}"),
            };
            (cursor, items)
        }
        other => panic!("unexpected scan reply: {other:?}"),
    }
}

// ─── Reference implementation of the pre-1.3.0 algorithm ──────────────────────

/// `scan_cursor_token` — duplicated here deliberately. If the production hash
/// ever changes, this test should fail loudly rather than silently track it.
fn token(key: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = FNV_OFFSET;
    for &byte in key {
        h ^= byte as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    if h < (1u64 << 32) {
        h + (1u64 << 32)
    } else {
        h
    }
}

/// The old sort-and-slice: sort everything by `(token, bytes)`, find the first
/// element past the cursor, take `count`.
fn reference_page(all: &[String], cursor: Option<u64>, count: usize) -> (u64, Vec<String>) {
    let mut ordered: Vec<&String> = all.iter().collect();
    ordered.sort_by(|a, b| {
        token(a.as_bytes())
            .cmp(&token(b.as_bytes()))
            .then_with(|| a.as_bytes().cmp(b.as_bytes()))
    });
    let start = match cursor {
        None => 0,
        Some(c) => ordered.partition_point(|k| token(k.as_bytes()) <= c),
    };
    let end = (start + count).min(ordered.len());
    let next = if end >= ordered.len() {
        0
    } else {
        token(ordered[end - 1].as_bytes())
    };
    (
        next,
        ordered[start..end].iter().map(|s| (*s).clone()).collect(),
    )
}

// ─── HSCAN ────────────────────────────────────────────────────────────────────

async fn hash_with(n: usize) -> (Db, Vec<String>) {
    let db = Db::new(ServerConfig::default());
    let mut fields = Vec::with_capacity(n);
    for i in 0..n {
        let f = format!("field:{i}");
        run(&db, vec![b("HSET"), b("h"), b(&f), b(&format!("v{i}"))]).await;
        fields.push(f);
    }
    (db, fields)
}

#[tokio::test]
async fn hscan_first_page_matches_the_old_sort_and_slice() {
    let (db, fields) = hash_with(200).await;
    for count in [1usize, 7, 10, 64, 199, 200, 500] {
        let (cursor, items) = unpack(
            run(
                &db,
                vec![
                    b("HSCAN"),
                    b("h"),
                    b("0"),
                    b("COUNT"),
                    b(&count.to_string()),
                ],
            )
            .await,
        );
        let got_fields: Vec<String> = items.chunks(2).map(|c| c[0].clone()).collect();
        let (want_cursor, want_fields) = reference_page(&fields, None, count);
        assert_eq!(
            got_fields, want_fields,
            "COUNT={count}: page contents/order must match the sort-and-slice"
        );
        assert_eq!(
            cursor,
            want_cursor.to_string(),
            "COUNT={count}: next_cursor must match"
        );
    }
}

#[tokio::test]
async fn hscan_every_page_matches_the_old_sort_and_slice() {
    // Walk the whole hash, checking each page against the reference at the
    // cursor the server just handed back.
    let (db, fields) = hash_with(150).await;
    let count = 10usize;
    let mut cursor = "0".to_string();
    let mut pages = 0;

    loop {
        let parsed: Option<u64> = if cursor == "0" {
            None
        } else {
            Some(cursor.parse().expect("cursor is a decimal integer"))
        };
        let (next, items) = unpack(
            run(
                &db,
                vec![
                    b("HSCAN"),
                    b("h"),
                    b(&cursor),
                    b("COUNT"),
                    b(&count.to_string()),
                ],
            )
            .await,
        );
        let got: Vec<String> = items.chunks(2).map(|c| c[0].clone()).collect();
        let (want_next, want) = reference_page(&fields, parsed, count);
        assert_eq!(
            got, want,
            "page {pages} contents must match at cursor {cursor}"
        );
        assert_eq!(
            next,
            want_next.to_string(),
            "page {pages} cursor must match"
        );

        pages += 1;
        cursor = next;
        if cursor == "0" {
            break;
        }
        assert!(pages < 1000, "iteration should terminate");
    }
    assert_eq!(pages, 15, "150 fields at COUNT 10 is 15 pages");
}

#[tokio::test]
async fn hscan_full_walk_visits_every_field_exactly_once() {
    let (db, fields) = hash_with(500).await;
    let mut seen: Vec<String> = Vec::new();
    let mut cursor = "0".to_string();

    loop {
        let (next, items) = unpack(
            run(
                &db,
                vec![b("HSCAN"), b("h"), b(&cursor), b("COUNT"), b("13")],
            )
            .await,
        );
        seen.extend(items.chunks(2).map(|c| c[0].clone()));
        cursor = next;
        if cursor == "0" {
            break;
        }
    }

    let mut sorted_seen = seen.clone();
    sorted_seen.sort();
    sorted_seen.dedup();
    assert_eq!(
        sorted_seen.len(),
        seen.len(),
        "no field is returned twice across the walk"
    );
    let mut want = fields.clone();
    want.sort();
    assert_eq!(sorted_seen, want, "every field is visited exactly once");
}

#[tokio::test]
async fn hscan_match_filters_and_still_terminates() {
    let (db, _) = hash_with(300).await;
    let mut seen = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next, items) = unpack(
            run(
                &db,
                vec![
                    b("HSCAN"),
                    b("h"),
                    b(&cursor),
                    b("MATCH"),
                    b("field:1*"),
                    b("COUNT"),
                    b("5"),
                ],
            )
            .await,
        );
        seen.extend(items.chunks(2).map(|c| c[0].clone()));
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    // field:1, field:1x for x in 0..9, field:1xy for 100..199 → 1 + 10 + 100
    assert_eq!(seen.len(), 111, "MATCH field:1* over 0..299");
    assert!(seen.iter().all(|f| f.starts_with("field:1")));
}

// ─── SSCAN ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn sscan_matches_the_old_sort_and_slice_and_walks_fully() {
    let db = Db::new(ServerConfig::default());
    let mut members = Vec::new();
    for i in 0..250 {
        let m = format!("member-{i}");
        run(&db, vec![b("SADD"), b("s"), b(&m)]).await;
        members.push(m);
    }

    // First page equivalence across several COUNTs.
    for count in [1usize, 9, 32, 250] {
        let (cursor, items) = unpack(
            run(
                &db,
                vec![
                    b("SSCAN"),
                    b("s"),
                    b("0"),
                    b("COUNT"),
                    b(&count.to_string()),
                ],
            )
            .await,
        );
        let (want_cursor, want) = reference_page(&members, None, count);
        assert_eq!(items, want, "COUNT={count}: SSCAN page must match");
        assert_eq!(cursor, want_cursor.to_string());
    }

    // Full walk covers everything once.
    let mut seen = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next, items) = unpack(
            run(
                &db,
                vec![b("SSCAN"), b("s"), b(&cursor), b("COUNT"), b("7")],
            )
            .await,
        );
        seen.extend(items);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    seen.sort();
    seen.dedup();
    let mut want = members.clone();
    want.sort();
    assert_eq!(seen, want, "every member visited exactly once");
}

// ─── ZSCAN ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn zscan_matches_the_old_sort_and_slice_and_keeps_scores() {
    let db = Db::new(ServerConfig::default());
    let mut members = Vec::new();
    for i in 0..200 {
        let m = format!("m{i}");
        run(&db, vec![b("ZADD"), b("z"), b(&i.to_string()), b(&m)]).await;
        members.push(m);
    }

    for count in [1usize, 11, 200] {
        let (cursor, items) = unpack(
            run(
                &db,
                vec![
                    b("ZSCAN"),
                    b("z"),
                    b("0"),
                    b("COUNT"),
                    b(&count.to_string()),
                ],
            )
            .await,
        );
        let got: Vec<String> = items.chunks(2).map(|c| c[0].clone()).collect();
        let (want_cursor, want) = reference_page(&members, None, count);
        assert_eq!(got, want, "COUNT={count}: ZSCAN page must match");
        assert_eq!(cursor, want_cursor.to_string());
        // Score must still pair with its member.
        for c in items.chunks(2) {
            let idx: i64 = c[0].trim_start_matches('m').parse().unwrap();
            let score: f64 = c[1].parse().expect("score parses as a float");
            assert_eq!(score, idx as f64, "score stays paired with its member");
        }
    }

    let mut seen = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next, items) = unpack(
            run(
                &db,
                vec![b("ZSCAN"), b("z"), b(&cursor), b("COUNT"), b("6")],
            )
            .await,
        );
        seen.extend(items.chunks(2).map(|c| c[0].clone()));
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    seen.sort();
    seen.dedup();
    let mut want = members.clone();
    want.sort();
    assert_eq!(seen, want, "every member visited exactly once");
}

// ─── Edge cases the heap path could plausibly get wrong ───────────────────────

#[tokio::test]
async fn scan_on_an_empty_or_missing_collection_terminates() {
    let db = Db::new(ServerConfig::default());
    for cmd in ["HSCAN", "SSCAN", "ZSCAN"] {
        let (cursor, items) = unpack(run(&db, vec![b(cmd), b("nope"), b("0")]).await);
        assert_eq!(cursor, "0", "{cmd} on a missing key completes immediately");
        assert!(items.is_empty(), "{cmd} on a missing key returns nothing");
    }
}

#[tokio::test]
async fn single_element_collection_returns_cursor_zero() {
    let db = Db::new(ServerConfig::default());
    run(&db, vec![b("HSET"), b("h"), b("only"), b("v")]).await;
    let (cursor, items) =
        unpack(run(&db, vec![b("HSCAN"), b("h"), b("0"), b("COUNT"), b("10")]).await);
    assert_eq!(
        cursor, "0",
        "a page that exhausts the hash reports completion"
    );
    assert_eq!(items, vec!["only".to_string(), "v".to_string()]);
}

#[tokio::test]
async fn count_larger_than_the_collection_completes_in_one_page() {
    let db = Db::new(ServerConfig::default());
    for i in 0..5 {
        run(&db, vec![b("SADD"), b("s"), b(&format!("m{i}"))]).await;
    }
    let (cursor, items) =
        unpack(run(&db, vec![b("SSCAN"), b("s"), b("0"), b("COUNT"), b("1000")]).await);
    assert_eq!(cursor, "0");
    assert_eq!(items.len(), 5);
}

#[tokio::test]
async fn deleting_the_boundary_element_does_not_restart_iteration() {
    // The property the 1.2.2 cursor fix established, re-verified against the
    // heap selection: the resume point is the token, so removing the element
    // the cursor names must not re-serve the prefix already handed out.
    let db = Db::new(ServerConfig::default());
    for i in 0..100 {
        run(&db, vec![b("SADD"), b("s"), b(&format!("m{i}"))]).await;
    }

    let (cursor, first) =
        unpack(run(&db, vec![b("SSCAN"), b("s"), b("0"), b("COUNT"), b("10")]).await);
    assert_ne!(cursor, "0");
    let boundary = first.last().expect("page is non-empty").clone();
    run(&db, vec![b("SREM"), b("s"), b(&boundary)]).await;

    let (_next, second) = unpack(
        run(
            &db,
            vec![b("SSCAN"), b("s"), b(&cursor), b("COUNT"), b("10")],
        )
        .await,
    );
    for m in &second {
        assert!(
            !first.contains(m),
            "member {m} was already returned before the boundary was deleted"
        );
    }
}

#[tokio::test]
async fn a_cursor_past_every_element_returns_an_empty_final_page() {
    let db = Db::new(ServerConfig::default());
    for i in 0..20 {
        run(&db, vec![b("SADD"), b("s"), b(&format!("m{i}"))]).await;
    }
    // u64::MAX sorts after every real token.
    let (cursor, items) = unpack(
        run(
            &db,
            vec![
                b("SSCAN"),
                b("s"),
                b(&u64::MAX.to_string()),
                b("COUNT"),
                b("10"),
            ],
        )
        .await,
    );
    assert_eq!(cursor, "0", "nothing left to walk");
    assert!(items.is_empty());
}
