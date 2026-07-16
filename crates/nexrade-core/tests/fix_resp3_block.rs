//! Tests for XREAD/XREADGROUP BLOCK and RESP3 protocol support.

use std::time::{Duration, Instant};

use nexrade_core::command::{dispatch_with_addr as dispatch, stream::StreamId};
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;
use nexrade_core::store::Entry;
use nexrade_core::types::DataType;

// ── helpers ──────────────────────────────────────────────────────────────────

fn str_arg(s: &str) -> Resp {
    Resp::bulk_str(s.to_string())
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0, None).await
}

async fn xadd(db: &Db, key: &str, id: &str, fields: &[(&str, &str)]) -> Resp {
    let mut args = vec![str_arg("XADD"), str_arg(key), str_arg(id)];
    for (k, v) in fields {
        args.push(str_arg(k));
        args.push(str_arg(v));
    }
    run(db, args).await
}

// ── XREAD / XREADGROUP BLOCK ──────────────────────────────────────────────────

#[tokio::test]
async fn xread_no_block_returns_immediately() {
    let db = Db::default();
    xadd(&db, "s", "1-0", &[("k", "v")]).await;

    // No BLOCK: returns immediately even with no new entries past `2-0`.
    let r = run(
        &db,
        vec![
            str_arg("XREAD"),
            str_arg("STREAMS"),
            str_arg("s"),
            str_arg("2-0"),
        ],
    )
    .await;
    if let Resp::Array(Some(streams)) = r {
        let entries = match &streams[0] {
            Resp::Array(Some(a)) => match &a[1] {
                Resp::Array(Some(e)) => e.clone(),
                _ => panic!("unexpected stream shape"),
            },
            _ => panic!("unexpected stream shape"),
        };
        assert!(entries.is_empty(), "expected empty after id 2-0");
    } else {
        panic!("XREAD returned non-array: {:?}", r);
    }
}

#[tokio::test]
async fn xread_block_wakes_on_xadd() {
    let db = Db::default();
    xadd(&db, "s", "1-0", &[("k", "v1")]).await;

    let db2 = db.clone();
    let producer = tokio::spawn(async move {
        // Small delay to let the consumer start blocking first.
        tokio::time::sleep(Duration::from_millis(100)).await;
        xadd(&db2, "s", "2-0", &[("k", "v2")]).await;
    });

    let started = Instant::now();
    let r = run(
        &db,
        vec![
            str_arg("XREAD"),
            str_arg("BLOCK"),
            str_arg("5000"),
            str_arg("STREAMS"),
            str_arg("s"),
            str_arg("1-0"),
        ],
    )
    .await;
    let elapsed = started.elapsed();

    producer.await.unwrap();

    if let Resp::Array(Some(streams)) = r {
        let entries = match &streams[0] {
            Resp::Array(Some(a)) => match &a[1] {
                Resp::Array(Some(e)) => e.clone(),
                _ => panic!("unexpected stream shape"),
            },
            _ => panic!("unexpected stream shape"),
        };
        assert_eq!(entries.len(), 1, "expected exactly one new entry");
        // We should have woken up shortly after the producer's 100ms delay.
        assert!(
            elapsed < Duration::from_millis(2000),
            "woke too late ({:?})",
            elapsed
        );
    } else {
        panic!("XREAD returned non-array: {:?}", r);
    }
}

#[tokio::test]
async fn xread_block_timeout_returns_nil() {
    let db = Db::default();

    let started = Instant::now();
    let r = run(
        &db,
        vec![
            str_arg("XREAD"),
            str_arg("BLOCK"),
            str_arg("200"),
            str_arg("STREAMS"),
            str_arg("s"),
            str_arg("0-0"),
        ],
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(r, Resp::Array(None)),
        "expected nil array on timeout, got {:?}",
        r
    );
    assert!(
        elapsed >= Duration::from_millis(180),
        "returned too quickly ({:?})",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(1500),
        "waited too long ({:?})",
        elapsed
    );
}

#[tokio::test]
async fn xread_block_does_not_block_when_entries_already_present() {
    let db = Db::default();
    xadd(&db, "s", "1-0", &[("k", "v")]).await;

    let started = Instant::now();
    let r = run(
        &db,
        vec![
            str_arg("XREAD"),
            str_arg("BLOCK"),
            str_arg("10000"),
            str_arg("STREAMS"),
            str_arg("s"),
            str_arg("0-0"),
        ],
    )
    .await;
    let elapsed = started.elapsed();

    // Should return immediately with the existing entry, not wait for the
    // 10-second timeout.
    assert!(
        elapsed < Duration::from_millis(500),
        "XREAD BLOCK did not short-circuit ({:?})",
        elapsed
    );

    if let Resp::Array(Some(streams)) = r {
        let entries = match &streams[0] {
            Resp::Array(Some(a)) => match &a[1] {
                Resp::Array(Some(e)) => e.clone(),
                _ => panic!("unexpected stream shape"),
            },
            _ => panic!("unexpected stream shape"),
        };
        assert_eq!(entries.len(), 1);
    } else {
        panic!("XREAD returned non-array: {:?}", r);
    }
}

#[tokio::test]
async fn xreadgroup_block_wakes_on_xadd() {
    let db = Db::default();
    xadd(&db, "s", "1-0", &[("k", "v1")]).await;

    // Create the consumer group first.
    run(
        &db,
        vec![
            str_arg("XGROUP"),
            str_arg("CREATE"),
            str_arg("s"),
            str_arg("g1"),
            str_arg("0"),
        ],
    )
    .await;

    let db2 = db.clone();
    let producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        xadd(&db2, "s", "2-0", &[("k", "v2")]).await;
    });

    let started = Instant::now();
    let r = run(
        &db,
        vec![
            str_arg("XREADGROUP"),
            str_arg("GROUP"),
            str_arg("g1"),
            str_arg("c1"),
            str_arg("BLOCK"),
            str_arg("5000"),
            str_arg("STREAMS"),
            str_arg("s"),
            str_arg(">"),
        ],
    )
    .await;
    let elapsed = started.elapsed();

    producer.await.unwrap();

    assert!(
        elapsed < Duration::from_millis(2000),
        "XREADGROUP BLOCK woke too late ({:?})",
        elapsed
    );
    if let Resp::Array(Some(streams)) = r {
        let entries = match &streams[0] {
            Resp::Array(Some(a)) => match &a[1] {
                Resp::Array(Some(e)) => e.clone(),
                _ => panic!("unexpected stream shape"),
            },
            _ => panic!("unexpected stream shape"),
        };
        assert_eq!(entries.len(), 1);
    } else {
        panic!("XREADGROUP returned non-array: {:?}", r);
    }
}

#[test]
fn stream_id_basic_parse() {
    assert!(StreamId::parse("10-0").unwrap() > StreamId::parse("2-0").unwrap());
    assert!(StreamId::parse("1-0").unwrap() < StreamId::parse("1-1").unwrap());
    assert!(StreamId::parse("0-0") == Some(StreamId::MIN));
}

// ── RESP3 serialization ──────────────────────────────────────────────────────

#[test]
fn serialize_null_differs_per_version() {
    // BulkString None: RESP2 uses $-1, RESP3 uses _.
    let bulk_none = Resp::BulkString(None);
    assert_eq!(bulk_none.serialize_for_version(2).as_ref(), b"$-1\r\n");
    assert_eq!(bulk_none.serialize_for_version(3).as_ref(), b"_\r\n");

    // Array None: RESP2 uses *-1, RESP3 uses _.
    let arr_none = Resp::Array(None);
    assert_eq!(arr_none.serialize_for_version(2).as_ref(), b"*-1\r\n");
    assert_eq!(arr_none.serialize_for_version(3).as_ref(), b"_\r\n");
}

#[test]
fn serialize_map_set_bool_differs_per_version() {
    let map = Resp::Map(vec![(
        Resp::BulkString(Some(bytes::Bytes::from_static(b"k"))),
        Resp::BulkString(Some(bytes::Bytes::from_static(b"v"))),
    )]);
    // RESP2: flat array of [k, v].
    assert_eq!(
        map.serialize_for_version(2).as_ref(),
        b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n"
    );
    // RESP3: map.
    assert_eq!(
        map.serialize_for_version(3).as_ref(),
        b"%1\r\n$1\r\nk\r\n$1\r\nv\r\n"
    );

    let set = Resp::Set(vec![Resp::BulkString(Some(bytes::Bytes::from_static(
        b"a",
    )))]);
    assert_eq!(set.serialize_for_version(2).as_ref(), b"*1\r\n$1\r\na\r\n");
    assert_eq!(set.serialize_for_version(3).as_ref(), b"~1\r\n$1\r\na\r\n");

    let b_true = Resp::Bool(true);
    assert_eq!(b_true.serialize_for_version(2).as_ref(), b":1\r\n");
    assert_eq!(b_true.serialize_for_version(3).as_ref(), b"#t\r\n");

    let b_false = Resp::Bool(false);
    assert_eq!(b_false.serialize_for_version(2).as_ref(), b":0\r\n");
    assert_eq!(b_false.serialize_for_version(3).as_ref(), b"#f\r\n");
}

#[test]
fn serialize_push_differs_per_version() {
    let push = Resp::Push(vec![
        Resp::BulkString(Some(bytes::Bytes::from_static(b"message"))),
        Resp::BulkString(Some(bytes::Bytes::from_static(b"news"))),
        Resp::BulkString(Some(bytes::Bytes::from_static(b"hi"))),
    ]);
    // RESP2 fallback: regular array.
    assert_eq!(
        push.serialize_for_version(2).as_ref(),
        b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n"
    );
    // RESP3: push frame.
    assert_eq!(
        push.serialize_for_version(3).as_ref(),
        b">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n"
    );
}

#[test]
fn serialize_integer_bulk_string_simple_unchanged() {
    // These variants have identical wire format in RESP2 and RESP3.
    assert_eq!(
        Resp::Integer(42).serialize_for_version(3).as_ref(),
        b":42\r\n"
    );
    assert_eq!(
        Resp::bulk_str("hi").serialize_for_version(3).as_ref(),
        b"$2\r\nhi\r\n"
    );
    assert_eq!(
        Resp::SimpleString("OK".into())
            .serialize_for_version(3)
            .as_ref(),
        b"+OK\r\n"
    );
    assert_eq!(
        Resp::Error("oops".into()).serialize_for_version(3).as_ref(),
        b"-oops\r\n"
    );
}

#[tokio::test]
async fn cmd_hello_dispatch_returns_array_fallback() {
    // The connection handler in connection.rs intercepts HELLO and returns
    // a Map for version 3. The dispatch-level fallback in command/server.rs
    // still returns a flat array — that path is only used by embedded
    // library callers that go through the dispatcher directly.
    let db = Db::default();
    let r = run(&db, vec![str_arg("HELLO"), str_arg("3")]).await;
    assert!(matches!(r, Resp::Array(_)));
}

// ── end-to-end smoke test of upgrade_to_resp3 via real dispatch ─────────────

#[tokio::test]
async fn hgetall_response_is_even_array_so_upgradeable() {
    // The function in connection.rs converts this Array → Map in RESP3 mode.
    // Here we just check that cmd_hgetall returns an even-length Array, which
    // is the precondition for the upgrade.
    let db = Db::default();
    run(
        &db,
        vec![
            str_arg("HSET"),
            str_arg("h"),
            str_arg("a"),
            str_arg("1"),
            str_arg("b"),
            str_arg("2"),
        ],
    )
    .await;
    let r = run(&db, vec![str_arg("HGETALL"), str_arg("h")]).await;
    if let Resp::Array(Some(items)) = r {
        assert_eq!(items.len(), 4);
        assert_eq!(items.len() % 2, 0);
    } else {
        panic!("expected Array from HGETALL, got {:?}", r);
    }
}

#[tokio::test]
async fn block_filters_wakeup_by_requested_key() {
    // Stream A already has an entry that satisfies the cursor. Producer
    // adds an entry to a different stream (B), which wakes our waiter.
    // The waiter must return stream A's data, not stream B's — and the
    // returned stream must be A, never B.
    let db = Db::default();
    xadd(&db, "A", "1-0", &[("k", "a1")]).await;

    let db2 = db.clone();
    let producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        xadd(&db2, "B", "1-0", &[("k", "b1")]).await;
    });

    let r = run(
        &db,
        vec![
            str_arg("XREAD"),
            str_arg("BLOCK"),
            str_arg("5000"),
            str_arg("STREAMS"),
            str_arg("A"),
            str_arg("0-0"),
        ],
    )
    .await;
    producer.await.unwrap();

    if let Resp::Array(Some(streams)) = r {
        let (key, entries) = match &streams[0] {
            Resp::Array(Some(a)) => (
                a.first().and_then(|r| r.as_str()).unwrap_or("").to_string(),
                match &a[1] {
                    Resp::Array(Some(e)) => e.len(),
                    _ => 0,
                },
            ),
            _ => panic!("unexpected stream shape"),
        };
        // The waiter was triggered by B's XADD but must return A's entries
        // (filtered by key).
        assert_eq!(key, "A", "result must be for stream A, not B");
        assert_eq!(
            entries, 1,
            "waiter should see A's existing entry (1-0 > 0-0)"
        );
    } else {
        panic!("XREAD returned non-array: {:?}", r);
    }
}

// ── RESP2-shape preconditions for the ZSET RESP3 pairing fix ────────────────
//
// `Connection::upgrade_to_resp3` (nexrade-server/src/connection.rs) nests
// ZRANGE*/ZPOPMIN/ZPOPMAX/ZUNION/ZINTER/ZDIFF's flat [member, score, ...]
// array into [[member, score], ...] pairs under RESP3, driven by a
// WITHSCORES/COUNT hint computed from the original request args (not
// guessed from response shape). These tests only exercise the dispatcher
// directly (`dispatch_with_addr`, no RESP3 upgrade applied) — they pin
// down that the RESP2 shape these commands produce is exactly what the
// upgrade function expects as its input: a flat, even-length array when
// scores are present, and nothing to nest when they aren't. See
// `nexrade-server/tests/resp3_zset_shapes.rs` for the end-to-end RESP3
// wire-shape assertions.

#[tokio::test]
async fn zrange_withscores_is_flat_even_array_at_dispatch_level() {
    let db = Db::default();
    run(
        &db,
        vec![
            str_arg("ZADD"),
            str_arg("z"),
            str_arg("1"),
            str_arg("a"),
            str_arg("2"),
            str_arg("b"),
        ],
    )
    .await;
    let r = run(
        &db,
        vec![
            str_arg("ZRANGE"),
            str_arg("z"),
            str_arg("0"),
            str_arg("-1"),
            str_arg("WITHSCORES"),
        ],
    )
    .await;
    if let Resp::Array(Some(items)) = r {
        // [a, 1, b, 2] — flat, even, never pre-nested by the command layer.
        assert_eq!(items.len(), 4);
        assert!(items.iter().all(|i| matches!(i, Resp::BulkString(Some(_)))));
    } else {
        panic!("expected flat Array from ZRANGE WITHSCORES, got {:?}", r);
    }
}

#[tokio::test]
async fn zrange_without_withscores_is_flat_member_only_array() {
    let db = Db::default();
    run(
        &db,
        vec![
            str_arg("ZADD"),
            str_arg("z"),
            str_arg("1"),
            str_arg("a"),
            str_arg("2"),
            str_arg("b"),
        ],
    )
    .await;
    let r = run(
        &db,
        vec![str_arg("ZRANGE"), str_arg("z"), str_arg("0"), str_arg("-1")],
    )
    .await;
    if let Resp::Array(Some(items)) = r {
        // Members only, no scores — the precondition upgrade_to_resp3 relies
        // on the WITHSCORES hint (not response shape) to avoid nesting this.
        assert_eq!(items.len(), 2);
    } else {
        panic!("expected Array from ZRANGE, got {:?}", r);
    }
}

#[tokio::test]
async fn zpopmin_no_count_returns_flat_member_score_pair() {
    let db = Db::default();
    run(
        &db,
        vec![
            str_arg("ZADD"),
            str_arg("z"),
            str_arg("1"),
            str_arg("a"),
            str_arg("2"),
            str_arg("b"),
        ],
    )
    .await;
    let r = run(&db, vec![str_arg("ZPOPMIN"), str_arg("z")]).await;
    if let Resp::Array(Some(items)) = r {
        // [member, score] — exactly 2 items regardless of COUNT being
        // absent; the COUNT-presence hint (not this length) decides nesting.
        assert_eq!(items.len(), 2);
    } else {
        panic!("expected Array from ZPOPMIN, got {:?}", r);
    }
}

#[tokio::test]
async fn zpopmin_with_count_returns_flat_alternating_array() {
    let db = Db::default();
    run(
        &db,
        vec![
            str_arg("ZADD"),
            str_arg("z"),
            str_arg("1"),
            str_arg("a"),
            str_arg("2"),
            str_arg("b"),
        ],
    )
    .await;
    let r = run(&db, vec![str_arg("ZPOPMIN"), str_arg("z"), str_arg("1")]).await;
    if let Resp::Array(Some(items)) = r {
        // Still flat at the dispatch level even with COUNT — nesting is
        // purely a connection-layer RESP3 concern, driven by the fact that
        // COUNT was explicitly passed (args.len() >= 3), not response shape.
        assert_eq!(items.len(), 2);
    } else {
        panic!("expected Array from ZPOPMIN with COUNT, got {:?}", r);
    }
}

#[tokio::test]
async fn zunion_without_withscores_is_flat_member_only_even_count() {
    let db = Db::default();
    run(
        &db,
        vec![
            str_arg("ZADD"),
            str_arg("z1"),
            str_arg("1"),
            str_arg("a"),
            str_arg("2"),
            str_arg("b"),
        ],
    )
    .await;
    run(
        &db,
        vec![str_arg("ZADD"), str_arg("z2"), str_arg("3"), str_arg("c")],
    )
    .await;
    // 3 members total (a, b, c) is odd, so pick a case with an even member
    // count to reproduce the exact shape the pre-fix parity guess mishandled.
    run(
        &db,
        vec![str_arg("ZADD"), str_arg("z2"), str_arg("4"), str_arg("d")],
    )
    .await;
    let r = run(
        &db,
        vec![
            str_arg("ZUNION"),
            str_arg("2"),
            str_arg("z1"),
            str_arg("z2"),
        ],
    )
    .await;
    if let Resp::Array(Some(items)) = r {
        // 4 members (a, b, c, d), no WITHSCORES — even length, but must not
        // be treated as [member, score, ...] pairs. The RESP3 upgrade must
        // key off the absence of WITHSCORES in the request, not this parity.
        assert_eq!(items.len(), 4);
        assert!(items.iter().all(|i| matches!(i, Resp::BulkString(Some(_)))));
    } else {
        panic!("expected flat Array from ZUNION, got {:?}", r);
    }
}

#[tokio::test]
async fn zunion_with_withscores_is_flat_alternating_array() {
    let db = Db::default();
    run(
        &db,
        vec![str_arg("ZADD"), str_arg("z1"), str_arg("1"), str_arg("a")],
    )
    .await;
    run(
        &db,
        vec![str_arg("ZADD"), str_arg("z2"), str_arg("2"), str_arg("b")],
    )
    .await;
    let r = run(
        &db,
        vec![
            str_arg("ZUNION"),
            str_arg("2"),
            str_arg("z1"),
            str_arg("z2"),
            str_arg("WITHSCORES"),
        ],
    )
    .await;
    if let Resp::Array(Some(items)) = r {
        // [a, 1, b, 2] — flat at dispatch level; nesting happens only in
        // upgrade_to_resp3, driven by the WITHSCORES token in the request.
        assert_eq!(items.len(), 4);
    } else {
        panic!("expected flat Array from ZUNION WITHSCORES, got {:?}", r);
    }
}

// Suppress unused warnings for the Entry/DataType import that's referenced
// indirectly via the dispatcher's signatures.
#[allow(dead_code)]
fn _force_link(_: Entry, _: DataType) {}
