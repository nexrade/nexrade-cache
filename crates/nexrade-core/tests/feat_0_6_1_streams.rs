//! 0.6.1 — Streams parity pass: XGROUP / XREADGROUP / XCLAIM / XAUTOCLAIM edges.
//!
//! Existing coverage (fix_expire_and_streams, fix_resp3_block, fix_aof_hll_stream)
//! already hits the happy path for XINFO / XCLAIM transfer / XAUTOCLAIM 3-tuple /
//! XREADGROUP BLOCK wake. This file locks down the edges redis-py / go-redis
//! consumer-group apps hit next:
//!
//!   * XGROUP CREATE MKSTREAM / BUSYGROUP / missing-key
//!   * XGROUP SETID / DESTROY / CREATECONSUMER / DELCONSUMER (PEL cleanup)
//!   * XREADGROUP NOACK (no PEL) vs default PEL, redelivery by id, NOGROUP
//!   * XACK removes from PEL + consumer pending list
//!   * XCLAIM FORCE / JUSTID / RETRYCOUNT
//!   * XAUTOCLAIM COUNT pagination cursor + JUSTID
//!   * XPENDING summary form
//!   * XINFO CONSUMERS after delivery

use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, MaxMemoryPolicy, ServerConfig};
use nexrade_core::resp::Resp;

fn small_config() -> ServerConfig {
    ServerConfig {
        databases: 1,
        max_memory: None,
        maxmemory_policy: MaxMemoryPolicy::NoEviction,
        ..ServerConfig::default()
    }
}

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter()
        .map(|a| Resp::BulkString(Some(bytes::Bytes::from(a.to_string()))))
        .collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0).await
}

fn ok(r: &Resp) {
    match r {
        Resp::SimpleString(s) if s == "OK" => {}
        Resp::BulkString(Some(b)) if b.as_ref() == b"OK" => {}
        other => panic!("expected OK, got {other:?}"),
    }
}

/// XADD returns the generated/accepted entry id as a bulk string.
fn xadd_ok(r: &Resp) -> String {
    match r {
        Resp::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(b).into_owned();
            assert!(
                s.contains('-'),
                "XADD must return an id like ms-seq, got {s:?}"
            );
            s
        }
        other => panic!("expected XADD bulk id, got {other:?}"),
    }
}

fn int_eq(r: &Resp, want: i64) {
    match r {
        Resp::Integer(n) => assert_eq!(*n, want, "want {want}, got {n}"),
        other => panic!("expected integer {want}, got {other:?}"),
    }
}

fn as_array(r: Resp) -> Vec<Resp> {
    match r {
        Resp::Array(Some(a)) => a,
        other => panic!("expected array, got {other:?}"),
    }
}

fn is_error_containing(r: &Resp, needle: &str) -> bool {
    match r {
        Resp::Error(s) => s
            .to_ascii_uppercase()
            .contains(&needle.to_ascii_uppercase()),
        _ => false,
    }
}

fn bulk_eq(r: &Resp, want: &str) -> bool {
    match r {
        Resp::BulkString(Some(b)) => b.as_ref() == want.as_bytes(),
        Resp::SimpleString(s) => s == want,
        _ => false,
    }
}

// ─── XGROUP CREATE edges ────────────────────────────────────────────────────

#[tokio::test]
async fn xgroup_create_mkstream_on_missing_key() {
    let db = Db::new(small_config());
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"])).await);
    // Stream now exists with zero entries.
    int_eq(&run(&db, cmd(&["XLEN", "s"])).await, 0);
    // Group is listed.
    let groups = as_array(run(&db, cmd(&["XINFO", "GROUPS", "s"])).await);
    assert_eq!(groups.len(), 1);
}

#[tokio::test]
async fn xgroup_create_without_mkstream_on_missing_key_errors() {
    let db = Db::new(small_config());
    let r = run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await;
    assert!(
        is_error_containing(&r, "requires the key to exist")
            || is_error_containing(&r, "no such key")
            || is_error_containing(&r, "ERR"),
        "missing stream without MKSTREAM must error, got {r:?}"
    );
}

#[tokio::test]
async fn xgroup_create_busygroup_on_duplicate() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
    let r = run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await;
    assert!(
        is_error_containing(&r, "BUSYGROUP"),
        "duplicate group must be BUSYGROUP, got {r:?}"
    );
}

#[tokio::test]
async fn xgroup_create_dollar_starts_at_last_entry() {
    // `$` = "last entry in stream" — subsequent XREADGROUP `>` must not
    // redeliver pre-existing entries.
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "a"])).await);
    xadd_ok(&run(&db, cmd(&["XADD", "s", "2-0", "f", "b"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "$"])).await);

    let r = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;
    // No new entries past last_delivered_id → empty array of streams, or
    // streams with empty entry lists. Either way: zero entries delivered.
    match r {
        Resp::Array(Some(streams)) => {
            for stream in &streams {
                if let Resp::Array(Some(pair)) = stream {
                    if pair.len() >= 2 {
                        if let Resp::Array(Some(entries)) = &pair[1] {
                            assert!(
                                entries.is_empty(),
                                "CREATE $ must not redeliver pre-existing entries, got {entries:?}"
                            );
                        }
                    }
                }
            }
        }
        Resp::Array(None) | Resp::Null => {}
        other => panic!("unexpected XREADGROUP reply after CREATE $: {other:?}"),
    }

    // New entry is delivered.
    xadd_ok(&run(&db, cmd(&["XADD", "s", "3-0", "f", "c"])).await);
    let r2 = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;
    let streams = as_array(r2);
    assert!(!streams.is_empty(), "new entry after $ must deliver");
    match &streams[0] {
        Resp::Array(Some(pair)) if pair.len() >= 2 => match &pair[1] {
            Resp::Array(Some(entries)) => {
                assert_eq!(entries.len(), 1, "exactly the new entry");
            }
            other => panic!("expected entries array, got {other:?}"),
        },
        other => panic!("expected [key, entries], got {other:?}"),
    }
}

// ─── XGROUP SETID / DESTROY / CREATECONSUMER / DELCONSUMER ──────────────────

#[tokio::test]
async fn xgroup_setid_rewinds_and_redelivers() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "a"])).await);
    xadd_ok(&run(&db, cmd(&["XADD", "s", "2-0", "f", "b"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "$"])).await);
    // Rewind to the beginning.
    ok(&run(&db, cmd(&["XGROUP", "SETID", "s", "g", "0-0"])).await);
    let r = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;
    let streams = as_array(r);
    match &streams[0] {
        Resp::Array(Some(pair)) if pair.len() >= 2 => match &pair[1] {
            Resp::Array(Some(entries)) => {
                assert_eq!(entries.len(), 2, "SETID 0-0 must redeliver both entries");
            }
            other => panic!("expected entries, got {other:?}"),
        },
        other => panic!("expected stream pair, got {other:?}"),
    }
}

#[tokio::test]
async fn xgroup_destroy_returns_1_then_0() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
    int_eq(&run(&db, cmd(&["XGROUP", "DESTROY", "s", "g"])).await, 1);
    int_eq(&run(&db, cmd(&["XGROUP", "DESTROY", "s", "g"])).await, 0);
}

#[tokio::test]
async fn xgroup_createconsumer_is_idempotent() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
    int_eq(
        &run(&db, cmd(&["XGROUP", "CREATECONSUMER", "s", "g", "c1"])).await,
        1,
    );
    int_eq(
        &run(&db, cmd(&["XGROUP", "CREATECONSUMER", "s", "g", "c1"])).await,
        0,
    );
}

#[tokio::test]
async fn xgroup_delconsumer_returns_pending_count_and_clears_pel() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "a"])).await);
    xadd_ok(&run(&db, cmd(&["XADD", "s", "2-0", "f", "b"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
    // Deliver both to c1 → 2 pending.
    let _ = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;

    // DELCONSUMER returns the number of pending messages that consumer owned.
    int_eq(
        &run(&db, cmd(&["XGROUP", "DELCONSUMER", "s", "g", "c1"])).await,
        2,
    );

    // PEL is now empty.
    let pend = as_array(run(&db, cmd(&["XPENDING", "s", "g"])).await);
    // Summary form: [count, min, max, consumers]
    match &pend[0] {
        Resp::Integer(n) => assert_eq!(*n, 0, "PEL must be empty after DELCONSUMER"),
        other => panic!("expected pending count integer, got {other:?}"),
    }

    // Second delete is a no-op (0 pending).
    int_eq(
        &run(&db, cmd(&["XGROUP", "DELCONSUMER", "s", "g", "c1"])).await,
        0,
    );
}

// ─── XREADGROUP NOACK / redelivery / NOGROUP ────────────────────────────────

#[tokio::test]
async fn xreadgroup_noack_skips_pel() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);

    let r = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "1",
            "NOACK",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;
    let streams = as_array(r);
    match &streams[0] {
        Resp::Array(Some(pair)) if pair.len() >= 2 => match &pair[1] {
            Resp::Array(Some(entries)) => assert_eq!(entries.len(), 1),
            other => panic!("expected entries, got {other:?}"),
        },
        other => panic!("expected stream pair, got {other:?}"),
    }

    // NOACK → nothing pending.
    let pend = as_array(run(&db, cmd(&["XPENDING", "s", "g"])).await);
    match &pend[0] {
        Resp::Integer(n) => assert_eq!(*n, 0, "NOACK must not populate PEL"),
        other => panic!("expected pending count, got {other:?}"),
    }
}

#[tokio::test]
async fn xreadgroup_default_populates_pel_and_redelivers() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);

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

    // Summary XPENDING: 1 pending owned by c1.
    let pend = as_array(run(&db, cmd(&["XPENDING", "s", "g"])).await);
    match &pend[0] {
        Resp::Integer(n) => assert_eq!(*n, 1),
        other => panic!("expected pending count, got {other:?}"),
    }

    // Re-delivery of this consumer's pending via id cursor `0-0`.
    let r = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            "0-0",
        ]),
    )
    .await;
    let streams = as_array(r);
    match &streams[0] {
        Resp::Array(Some(pair)) if pair.len() >= 2 => match &pair[1] {
            Resp::Array(Some(entries)) => {
                assert_eq!(entries.len(), 1, "pending redelivery must return the entry");
            }
            other => panic!("expected entries, got {other:?}"),
        },
        other => panic!("expected stream pair, got {other:?}"),
    }
}

#[tokio::test]
async fn xreadgroup_nogroup_errors() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    let r = run(
        &db,
        cmd(&["XREADGROUP", "GROUP", "nope", "c1", "STREAMS", "s", ">"]),
    )
    .await;
    assert!(
        is_error_containing(&r, "NOGROUP"),
        "missing group must be NOGROUP, got {r:?}"
    );
}

// ─── XACK ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn xack_removes_from_pel() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
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

    int_eq(&run(&db, cmd(&["XACK", "s", "g", "1-0"])).await, 1);
    // Second ACK is a no-op.
    int_eq(&run(&db, cmd(&["XACK", "s", "g", "1-0"])).await, 0);

    let pend = as_array(run(&db, cmd(&["XPENDING", "s", "g"])).await);
    match &pend[0] {
        Resp::Integer(n) => assert_eq!(*n, 0, "XACK must empty the PEL"),
        other => panic!("expected pending count, got {other:?}"),
    }
}

// ─── XCLAIM FORCE / JUSTID / RETRYCOUNT ─────────────────────────────────────

#[tokio::test]
async fn xclaim_force_claims_non_pending_entry() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
    // Entry exists but is not pending (never delivered).
    let r = run(&db, cmd(&["XCLAIM", "s", "g", "c1", "0", "1-0", "FORCE"])).await;
    let claimed = as_array(r);
    assert_eq!(
        claimed.len(),
        1,
        "FORCE must claim a non-pending existing entry"
    );

    // Now it is pending for c1.
    let pend = as_array(run(&db, cmd(&["XPENDING", "s", "g", "-", "+", "10", "c1"])).await);
    assert_eq!(pend.len(), 1);
}

#[tokio::test]
async fn xclaim_without_force_skips_non_pending() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
    let r = run(&db, cmd(&["XCLAIM", "s", "g", "c1", "0", "1-0"])).await;
    let claimed = as_array(r);
    assert!(
        claimed.is_empty(),
        "without FORCE, non-pending entry must not be claimed"
    );
}

#[tokio::test]
async fn xclaim_justid_and_retrycount() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
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

    // JUSTID returns only the id (bulk string), not the full entry.
    let r = run(
        &db,
        cmd(&[
            "XCLAIM",
            "s",
            "g",
            "c2",
            "0",
            "1-0",
            "JUSTID",
            "RETRYCOUNT",
            "5",
        ]),
    )
    .await;
    let claimed = as_array(r);
    assert_eq!(claimed.len(), 1);
    assert!(
        bulk_eq(&claimed[0], "1-0"),
        "JUSTID must return bare id, got {:?}",
        claimed[0]
    );

    // Range XPENDING shows delivery_count = 5.
    let pend = as_array(run(&db, cmd(&["XPENDING", "s", "g", "-", "+", "10", "c2"])).await);
    assert_eq!(pend.len(), 1);
    match &pend[0] {
        Resp::Array(Some(fields)) if fields.len() >= 4 => match &fields[3] {
            Resp::Integer(n) => assert_eq!(*n, 5, "RETRYCOUNT must stick"),
            other => panic!("expected delivery_count int, got {other:?}"),
        },
        other => panic!("expected pending entry array, got {other:?}"),
    }
}

// ─── XAUTOCLAIM cursor / COUNT / JUSTID ─────────────────────────────────────

#[tokio::test]
async fn xautoclaim_count_paginates_with_cursor() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "a"])).await);
    xadd_ok(&run(&db, cmd(&["XADD", "s", "2-0", "f", "b"])).await);
    xadd_ok(&run(&db, cmd(&["XADD", "s", "3-0", "f", "c"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
    let _ = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;

    // First page: COUNT 2 → claim 2, cursor points at the remaining id.
    let page1 = as_array(
        run(
            &db,
            cmd(&["XAUTOCLAIM", "s", "g", "c2", "0", "0-0", "COUNT", "2"]),
        )
        .await,
    );
    assert_eq!(page1.len(), 3, "cursor + claimed + deleted");
    match &page1[1] {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 2, "first page claims 2"),
        other => panic!("expected claimed array, got {other:?}"),
    }
    let cursor = match &page1[0] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected cursor bulk, got {other:?}"),
    };
    assert_ne!(cursor, "0-0", "cursor must advance when more remain");

    // Second page from cursor: remaining entry (or cursor 0-0 if drained).
    let page2 = as_array(
        run(
            &db,
            cmd(&["XAUTOCLAIM", "s", "g", "c2", "0", &cursor, "COUNT", "10"]),
        )
        .await,
    );
    match &page2[1] {
        Resp::Array(Some(items)) => {
            // The remaining pending entry (owned by c1) should be claimable.
            // If cursor already pointed past claimed ones, we may get 0 or 1.
            assert!(
                items.len() <= 1,
                "second page should claim at most the remainder, got {}",
                items.len()
            );
        }
        other => panic!("expected claimed array, got {other:?}"),
    }
}

#[tokio::test]
async fn xautoclaim_justid_returns_ids_only() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
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

    let r = as_array(
        run(
            &db,
            cmd(&["XAUTOCLAIM", "s", "g", "c2", "0", "0-0", "JUSTID"]),
        )
        .await,
    );
    match &r[1] {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            assert!(
                bulk_eq(&items[0], "1-0"),
                "JUSTID must return bare id, got {:?}",
                items[0]
            );
        }
        other => panic!("expected claimed ids, got {other:?}"),
    }
}

// ─── XINFO CONSUMERS ────────────────────────────────────────────────────────

#[tokio::test]
async fn xinfo_consumers_lists_after_delivery() {
    let db = Db::new(small_config());
    xadd_ok(&run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await);
    ok(&run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0"])).await);
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

    let consumers = as_array(run(&db, cmd(&["XINFO", "CONSUMERS", "s", "g"])).await);
    assert_eq!(consumers.len(), 1, "c1 must appear after delivery");
    // Flatten name field check.
    match &consumers[0] {
        Resp::Array(Some(fields)) => {
            let has_c1 = fields.iter().any(|f| bulk_eq(f, "c1"));
            assert!(has_c1, "consumer name c1 must be present, got {fields:?}");
            // pending field should report 1.
            if let Some(idx) = fields.iter().position(|f| bulk_eq(f, "pending")) {
                match &fields[idx + 1] {
                    Resp::Integer(n) => assert_eq!(*n, 1),
                    other => panic!("pending value must be int, got {other:?}"),
                }
            }
        }
        other => panic!("expected consumer info array, got {other:?}"),
    }
}

// ─── Composed consumer-group flow ───────────────────────────────────────────

#[tokio::test]
async fn composed_consumer_group_flow() {
    // End-to-end: MKSTREAM → produce → read → claim → ack → destroy.
    let db = Db::new(small_config());
    ok(&run(
        &db,
        cmd(&["XGROUP", "CREATE", "q", "workers", "$", "MKSTREAM"]),
    )
    .await);
    xadd_ok(&run(&db, cmd(&["XADD", "q", "*", "job", "1"])).await);
    xadd_ok(&run(&db, cmd(&["XADD", "q", "*", "job", "2"])).await);

    let r = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "workers",
            "w1",
            "COUNT",
            "1",
            "STREAMS",
            "q",
            ">",
        ]),
    )
    .await;
    let streams = as_array(r);
    let entry_id = match &streams[0] {
        Resp::Array(Some(pair)) if pair.len() >= 2 => match &pair[1] {
            Resp::Array(Some(entries)) if !entries.is_empty() => match &entries[0] {
                Resp::Array(Some(entry)) if !entry.is_empty() => match &entry[0] {
                    Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                    other => panic!("entry id not bulk: {other:?}"),
                },
                other => panic!("expected entry, got {other:?}"),
            },
            other => panic!("expected entries, got {other:?}"),
        },
        other => panic!("expected stream pair, got {other:?}"),
    };

    // Transfer to w2 and ACK.
    let claimed = as_array(run(&db, cmd(&["XCLAIM", "q", "workers", "w2", "0", &entry_id])).await);
    assert_eq!(claimed.len(), 1);
    int_eq(
        &run(&db, cmd(&["XACK", "q", "workers", &entry_id])).await,
        1,
    );

    // Destroy the group.
    int_eq(
        &run(&db, cmd(&["XGROUP", "DESTROY", "q", "workers"])).await,
        1,
    );
}
