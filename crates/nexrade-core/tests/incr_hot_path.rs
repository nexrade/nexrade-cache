//! Behavioral tests for the `Database::incr_int` fast paths used by
//! INCR/INCRBY/DECR/DECRBY. Covers two independent optimizations:
//!
//! 1. The earlier `itoa`-based formatting fix (replaces the
//!    allocation-heavy `i64::to_string()` path). The main thing it guards
//!    against is the bug the old `get()` + `insert(Entry::new(..))` sequence
//!    had: silently clearing any TTL on the key, whereas Redis explicitly
//!    preserves the timeout across INCR (the value is conceptually altered,
//!    not replaced).
//!
//! 2. The `DataType::Int` representation: vacant / `String`-typed keys
//!    promote to an atomic-backed `Int` cell on the first INCR (and
//!    anything that writes a raw string — SET, APPEND, SETRANGE, GETSET,
//!    SETBIT, BITFIELD SET/INCRBY, AOF rewrite — demotes it back to
//!    `String`). These tests verify the promotion/demotion boundary
//!    conditions are correct. They also verify that the manual `Clone` impl
//!    on `DataType` snapshots Int cells by value (RDB round-trip / COPY /
//!    RENAME must not alias a live cell) and that `key_version()` correctly
//!    accounts for the per-cell counter (WATCH must abort on promotion /
//!    demotion just as it does on any other write).
//!
//! 3. The read-lock CAS fast path (`ShardedDatabase::incr_int`) that dispatches
//!    repeated INCRs on an already-promoted key through a read guard instead
//!    of the exclusive write lock. `store.rs`'s test module covers the
//!    concurrency/overflow/fallback correctness directly; the test here
//!    confirms the fast path is wired correctly end-to-end through the full
//!    RESP dispatcher, matching manual arithmetic exactly the same way
//!    `repeated_incr_matches_manual_get_set_sequence` does for the slow path.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

#[tokio::test]
async fn incr_preserves_existing_ttl() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "10", "EX", "100"])).await;
    let ttl_before = run(&db, cmd(&["TTL", "k"])).await;
    let Resp::Integer(before) = ttl_before else {
        panic!("expected integer TTL, got {ttl_before:?}")
    };
    assert!(before > 0, "TTL should be set before INCR");

    let r = run(&db, cmd(&["INCR", "k"])).await;
    assert!(matches!(r, Resp::Integer(11)));

    let ttl_after = run(&db, cmd(&["TTL", "k"])).await;
    let Resp::Integer(after) = ttl_after else {
        panic!("expected integer TTL, got {ttl_after:?}")
    };
    assert!(
        after > 0,
        "TTL should survive INCR (Redis preserves timeout across INCR), got {after}"
    );
}

#[tokio::test]
async fn incrby_decrby_preserve_ttl_too() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "10", "EX", "100"])).await;

    let _ = run(&db, cmd(&["INCRBY", "k", "5"])).await;
    let ttl = run(&db, cmd(&["TTL", "k"])).await;
    assert!(matches!(ttl, Resp::Integer(n) if n > 0));

    let _ = run(&db, cmd(&["DECRBY", "k", "3"])).await;
    let ttl = run(&db, cmd(&["TTL", "k"])).await;
    assert!(matches!(ttl, Resp::Integer(n) if n > 0));
}

#[tokio::test]
async fn incr_on_missing_key_starts_at_zero_no_ttl() {
    let db = Db::default();
    let r = run(&db, cmd(&["INCR", "counter"])).await;
    assert!(matches!(r, Resp::Integer(1)));
    let ttl = run(&db, cmd(&["TTL", "counter"])).await;
    assert!(matches!(ttl, Resp::Integer(-1)));
}

#[tokio::test]
async fn incr_wrong_type_still_errors() {
    let db = Db::default();
    let _ = run(&db, cmd(&["LPUSH", "l", "x"])).await;
    let r = run(&db, cmd(&["INCR", "l"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn incr_non_integer_value_errors() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "not-a-number"])).await;
    let r = run(&db, cmd(&["INCR", "k"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn incr_overflow_errors() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", &i64::MAX.to_string()])).await;
    let r = run(&db, cmd(&["INCR", "k"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn incr_on_lazily_expired_key_recreates_at_delta() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "100"])).await;
    let _ = run(&db, cmd(&["PEXPIRE", "k", "1"])).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Key is lazily expired; INCR should treat it as absent (start at 0)
    // rather than incrementing the stale value.
    let r = run(&db, cmd(&["INCR", "k"])).await;
    assert!(matches!(r, Resp::Integer(1)));
}

#[tokio::test]
async fn repeated_incr_matches_manual_get_set_sequence() {
    // Sanity cross-check: the fast path must produce identical results to
    // plain SET+GET arithmetic over many iterations.
    let db = Db::default();
    let mut expected: i64 = 0;
    for i in 1..=50 {
        let delta = if i % 2 == 0 { i } else { -i };
        expected += delta;
        let r = run(&db, cmd(&["INCRBY", "k", &delta.to_string()])).await;
        assert!(matches!(r, Resp::Integer(n) if n == expected));
    }
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(
        matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == expected.to_string().as_bytes())
    );
}

// ── DataType::Int promotion/demotion (command-level) ────────────────────────
//
// These exercise the full RESP dispatch path rather than `Database` directly
// (see `store.rs`'s `#[cfg(test)]` module for the lower-level Clone/RDB/COPY
// coverage) — the point here is that every other string command still
// behaves exactly as it did against a plain `DataType::String`, regardless
// of whether the key happens to be int-encoded underneath.

#[tokio::test]
async fn incr_on_vacant_key_promotes_directly_to_int() {
    let db = Db::default();
    let r = run(&db, cmd(&["INCR", "k"])).await;
    assert!(matches!(r, Resp::Integer(1)));
    let r = run(&db, cmd(&["OBJECT", "ENCODING", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"int"));
    // TYPE must still say "string" — Redis never reports "int" as a type.
    let r = run(&db, cmd(&["TYPE", "k"])).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "string"));
}

#[tokio::test]
async fn incr_on_existing_string_int_promotes() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "5"])).await;
    let r = run(&db, cmd(&["OBJECT", "ENCODING", "k"])).await;
    assert!(
        matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"embstr" || b.as_ref() == b"int")
    );
    let r = run(&db, cmd(&["INCR", "k"])).await;
    assert!(matches!(r, Resp::Integer(6)));
    let r = run(&db, cmd(&["OBJECT", "ENCODING", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"int"));
}

#[tokio::test]
async fn get_on_int_encoded_key_returns_decimal_bytes() {
    let db = Db::default();
    let _ = run(&db, cmd(&["INCR", "k"])).await;
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"1"));
}

#[tokio::test]
async fn set_on_int_encoded_key_demotes_to_string() {
    let db = Db::default();
    let _ = run(&db, cmd(&["INCR", "k"])).await;
    let _ = run(&db, cmd(&["SET", "k", "hello"])).await;
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"hello"));
    // Further INCR on the (now non-numeric) string errors, same as before
    // this change — confirms demotion actually replaced the representation.
    let r = run(&db, cmd(&["INCR", "k"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn append_on_int_encoded_key_demotes_and_appends() {
    let db = Db::default();
    let _ = run(&db, cmd(&["INCR", "k"])).await; // k = "1"
    let r = run(&db, cmd(&["APPEND", "k", "23"])).await;
    assert!(matches!(r, Resp::Integer(3)));
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"123"));
}

#[tokio::test]
async fn setrange_on_int_encoded_key_demotes_and_patches() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "100"])).await;
    let _ = run(&db, cmd(&["INCR", "k"])).await; // k = "101" (Int)
    let r = run(&db, cmd(&["SETRANGE", "k", "0", "9"])).await;
    assert!(matches!(r, Resp::Integer(3)));
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"901"));
}

#[tokio::test]
async fn getset_on_int_encoded_key_returns_old_decimal_value() {
    let db = Db::default();
    let _ = run(&db, cmd(&["INCR", "k"])).await; // k = 1
    let _ = run(&db, cmd(&["INCR", "k"])).await; // k = 2
    let r = run(&db, cmd(&["GETSET", "k", "new"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"2"));
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"new"));
}

#[tokio::test]
async fn strlen_and_getrange_work_on_int_encoded_key() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "12345"])).await;
    let _ = run(&db, cmd(&["INCR", "k"])).await; // k = "12346" (Int)
    let r = run(&db, cmd(&["STRLEN", "k"])).await;
    assert!(matches!(r, Resp::Integer(5)));
    let r = run(&db, cmd(&["GETRANGE", "k", "0", "2"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"123"));
}

#[tokio::test]
async fn mget_includes_int_encoded_keys() {
    let db = Db::default();
    let _ = run(&db, cmd(&["INCR", "a"])).await; // a = 1 (Int)
    let _ = run(&db, cmd(&["SET", "b", "hello"])).await;
    let r = run(&db, cmd(&["MGET", "a", "b", "missing"])).await;
    let Resp::Array(Some(items)) = r else {
        panic!("expected array, got {r:?}")
    };
    assert_eq!(items.len(), 3);
    assert!(matches!(&items[0], Resp::BulkString(Some(b)) if b.as_ref() == b"1"));
    assert!(matches!(&items[1], Resp::BulkString(Some(b)) if b.as_ref() == b"hello"));
    assert!(matches!(&items[2], Resp::BulkString(None)));
}

#[tokio::test]
async fn incrbyfloat_on_int_encoded_key_reads_correctly_and_demotes() {
    let db = Db::default();
    let _ = run(&db, cmd(&["INCR", "k"])).await; // k = 1 (Int)
    let r = run(&db, cmd(&["INCRBYFLOAT", "k", "1.5"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"2.5"));
    // INCRBYFLOAT always demotes to String regardless of prior representation.
    let r = run(&db, cmd(&["OBJECT", "ENCODING", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"embstr"));
}

#[tokio::test]
async fn setbit_on_int_encoded_key_demotes_and_sets() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "k", "1"])).await;
    let _ = run(&db, cmd(&["INCR", "k"])).await; // k = "2" (Int), ASCII 0x32
                                                 // Flip the lowest bit of the first byte: '2' (0x32) -> '3' (0x33).
    let r = run(&db, cmd(&["SETBIT", "k", "7", "1"])).await;
    assert!(matches!(r, Resp::Integer(0)));
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"3"));
}

#[tokio::test]
async fn incr_wrong_type_on_bitmap_still_errors() {
    // Sanity: Int's new match arms shouldn't have loosened WrongType checks
    // for genuinely incompatible types.
    let db = Db::default();
    let _ = run(&db, cmd(&["SETBIT", "k", "0", "1"])).await;
    let r = run(&db, cmd(&["INCR", "k"])).await;
    assert!(matches!(r, Resp::Error(_)));
}

// ── PR 2: read-lock CAS fast path (full RESP dispatch) ──────────────────────
//
// The store-level tests in `store.rs` cover the dispatcher's concurrency
// behavior directly (lock choice, lost-update regression, overflow). This
// one just confirms that once a key is promoted, repeated INCR through the
// *full* command dispatcher — not a direct `ShardedDatabase::incr_int` call
// — still produces results identical to manual arithmetic. It's the same
// cross-check as `repeated_incr_matches_manual_get_set_sequence`, but
// starting from an already-promoted key so every iteration after the first
// hits the fast path.

#[tokio::test]
async fn repeated_incr_on_promoted_key_matches_manual_arithmetic() {
    let db = Db::default();
    // First INCR promotes k to Int; every iteration after this hits the
    // fast path inside `ShardedDatabase::incr_int`.
    let mut expected: i64 = 0;
    for i in 1..=50 {
        let delta = if i % 2 == 0 { i } else { -i };
        expected += delta;
        let r = run(&db, cmd(&["INCRBY", "k", &delta.to_string()])).await;
        assert!(matches!(r, Resp::Integer(n) if n == expected));
    }
    let r = run(&db, cmd(&["GET", "k"])).await;
    assert!(
        matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == expected.to_string().as_bytes())
    );
    let r = run(&db, cmd(&["OBJECT", "ENCODING", "k"])).await;
    assert!(matches!(r, Resp::BulkString(Some(ref b)) if b.as_ref() == b"int"));
}
