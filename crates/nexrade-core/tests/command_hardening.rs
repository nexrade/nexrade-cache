//! Regression tests for the client-triggered crash, unbounded-allocation and
//! silent-data-loss bugs found in the command-layer audit.
//!
//! Two distinct failure modes are covered:
//!
//! * **Panics** (`BITPOS` on an empty value, `GEORADIUS ... COUNT` with no
//!   value, blocking timeouts of `-1`/`inf`/`NaN`) — these killed the
//!   connection task mid-dispatch.
//! * **Unbounded allocations** (`BITFIELD` with a huge bit offset,
//!   `LMPOP`/`HSCAN`/`SRANDMEMBER` with huge counts) — these reached the
//!   allocator, and a failed allocation calls `handle_alloc_error` →
//!   `abort()`, which no `catch_unwind` can intercept and which killed the
//!   *entire process*, not just the connection.
//!
//! Plus the correctness fixes: `SMOVE`/`PFMERGE` no longer destroy data,
//! `WATCH` actually observes in-place mutations, and in-place updates keep
//! their TTL.

use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::resp::Resp;

// ── helpers ──────────────────────────────────────────────────────────────────

fn db() -> Db {
    Db::new(ServerConfig {
        databases: 1,
        ..ServerConfig::default()
    })
}

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter()
        .map(|a| Resp::BulkString(Some(bytes::Bytes::from(a.to_string()))))
        .collect()
}

async fn run(d: &Db, args: &[&str]) -> Resp {
    dispatch(d, cmd(args), 0).await
}

fn is_err(r: &Resp) -> bool {
    matches!(r, Resp::Error(_))
}

fn err_text(r: &Resp) -> String {
    match r {
        Resp::Error(e) => e.clone(),
        other => panic!("expected an error, got {other:?}"),
    }
}

/// Read a key's current write version the same way `connection.rs` does when
/// validating `WATCH`.
fn ver(d: &Db, key: &[u8]) -> u64 {
    d.store.db(0).read_for(key).key_version(key)
}

fn as_int(r: &Resp) -> i64 {
    match r {
        Resp::Integer(i) => *i,
        other => panic!("expected an integer, got {other:?}"),
    }
}

fn as_simple(r: &Resp) -> String {
    match r {
        Resp::SimpleString(s) => s.clone(),
        other => panic!("expected a simple string, got {other:?}"),
    }
}

fn as_str(r: &Resp) -> String {
    match r {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected a bulk string, got {other:?}"),
    }
}

/// The bulk-string elements of an array reply, for order-sensitive assertions.
fn arr(r: &Resp) -> Vec<String> {
    match r {
        Resp::Array(Some(items)) => items
            .iter()
            .map(|i| match i {
                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                other => panic!("expected a bulk string element, got {other:?}"),
            })
            .collect(),
        other => panic!("expected an array, got {other:?}"),
    }
}

// ── unbounded allocation: BITFIELD ───────────────────────────────────────────

/// `BITFIELD k SET u8 17592186044416 1` computed a 2 TiB resize. The
/// allocation failed, `handle_alloc_error` called `abort()`, and the whole
/// server process died from one ~30-byte unauthenticated command.
#[tokio::test]
async fn bitfield_rejects_an_unbounded_bit_offset() {
    let d = db();
    let _ = run(&d, &["SET", "k", "x"]).await;

    for offset in [
        "17592186044416", // 2 TiB
        "68719476736",    // 8 GiB
        "18446744073709551615",
    ] {
        let r = run(&d, &["BITFIELD", "k", "SET", "u8", offset, "1"]).await;
        assert!(
            is_err(&r),
            "BITFIELD with offset {offset} was accepted ({r:?}); it must be \
             rejected before it reaches the allocator"
        );
    }

    // The key is untouched and still readable.
    assert_eq!(as_str(&run(&d, &["GET", "k"]).await), "x");
}

#[tokio::test]
async fn bitfield_within_bounds_still_works() {
    let d = db();
    let r = run(&d, &["BITFIELD", "bf", "SET", "u8", "8", "255"]).await;
    assert!(!is_err(&r), "valid BITFIELD failed: {r:?}");
    // u8 at bit offset 8 is the second byte.
    assert_eq!(as_int(&run(&d, &["BITCOUNT", "bf"]).await), 8);
}

/// The signed arm of `BITFIELD` overflow handling used `1i64 << 63` and
/// `-(1i64 << 63)`, which overflow — panicking under `overflow-checks`
/// (i.e. every `cargo test` / `cargo run` build) and computing garbage in
/// release, where the shift is masked away.
#[tokio::test]
async fn bitfield_signed_64_bit_overflow_does_not_panic() {
    let d = db();

    // INCRBY on a signed 64-bit field must wrap within i64::MIN..=i64::MAX.
    let r = run(
        &d,
        &[
            "BITFIELD", "s", "OVERFLOW", "WRAP", "INCRBY", "i64", "0", "1",
        ],
    )
    .await;
    assert!(!is_err(&r), "signed 64-bit INCRBY failed: {r:?}");

    // Saturation clamps instead of wrapping.
    let r = run(
        &d,
        &[
            "BITFIELD", "t", "OVERFLOW", "SAT", "INCRBY", "i64", "0", "1",
        ],
    )
    .await;
    assert!(!is_err(&r), "signed 64-bit SAT INCRBY failed: {r:?}");

    // FAIL mode reports null rather than panicking.
    let r = run(
        &d,
        &[
            "BITFIELD", "u", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "255",
        ],
    )
    .await;
    assert!(!is_err(&r), "unsigned FAIL INCRBY failed: {r:?}");
}

// ── panics ───────────────────────────────────────────────────────────────────

/// `SET k ""; BITPOS k 1` sliced `bytes[0..=0]` on a zero-length slice.
#[tokio::test]
async fn bitpos_on_an_empty_value_returns_minus_one() {
    let d = db();
    let _ = run(&d, &["SET", "e", ""]).await;

    assert_eq!(as_int(&run(&d, &["BITPOS", "e", "1"]).await), -1);
    assert_eq!(
        as_int(&run(&d, &["BITPOS", "e", "1", "0", "0", "BIT"]).await),
        -1,
        "BITPOS with the BIT unit on an empty value must not panic"
    );
    assert_eq!(
        as_int(&run(&d, &["BITPOS", "e", "1", "100", "0"]).await),
        -1
    );
}

#[tokio::test]
async fn bitpos_out_of_range_start_does_not_panic() {
    let d = db();
    let _ = run(&d, &["SET", "abc", "abc"]).await;
    // A start past the end used to produce an inverted range and panic.
    let r = run(&d, &["BITPOS", "abc", "1", "100"]).await;
    assert!(
        !is_err(&r) || err_text(&r).contains("WRONGTYPE"),
        "got {r:?}"
    );
}

/// `GEORADIUS g 15 37 200 km COUNT` indexed `args[i]` straight after
/// incrementing `i`, so the trailing keyword panicked with an out-of-bounds
/// index. `cmd_geosearch` in the same file already used `args.get(i)`.
#[tokio::test]
async fn georadius_count_without_a_value_is_an_error_not_a_panic() {
    let d = db();
    let _ = run(&d, &["GEOADD", "g", "15", "37", "m"]).await;

    for args in [
        vec!["GEORADIUS", "g", "15", "37", "200", "km", "COUNT"],
        vec!["GEORADIUSBYMEMBER", "g", "m", "200", "km", "COUNT"],
    ] {
        let r = run(&d, &args).await;
        assert!(!matches!(r, Resp::BulkString(_)), "unexpected reply {r:?}");
    }

    // The server is still usable afterwards.
    assert_eq!(as_int(&run(&d, &["EXISTS", "g"]).await), 1);
}

/// `Duration::from_secs_f64` panics on negative, infinite and NaN input, and
/// `parse::<f64>()` accepts all three. Every blocking command took its
/// timeout straight from the client.
#[tokio::test]
async fn blocking_commands_reject_non_finite_and_negative_timeouts() {
    let d = db();

    for timeout in ["-1", "inf", "-inf", "NaN", "-0.5"] {
        for cmdline in [
            vec!["BLPOP", "nosuchkey_a", timeout],
            vec!["BRPOP", "nosuchkey_b", timeout],
            // BZPOPMIN/MAX take the timeout *last*, after every key, so
            // `BZPOPMIN c -1 0` is two keys and a block-forever timeout —
            // which correctly blocks rather than erroring.
            vec!["BZPOPMIN", "nosuchkey_c", timeout],
            vec!["BZPOPMAX", "nosuchkey_d", timeout],
            vec!["BLMPOP", timeout, "1", "nosuchkey_e", "LEFT"],
        ] {
            let r = run(&d, &cmdline).await;
            assert!(
                is_err(&r),
                "{} with timeout {timeout} was accepted ({r:?}); \
                 from_secs_f64 would have panicked",
                cmdline[0]
            );
        }
    }

    // Still alive and serving.
    assert_eq!(as_simple(&run(&d, &["PING"]).await), "PONG");
}

#[tokio::test]
async fn blocking_commands_still_accept_a_zero_timeout() {
    let d = db();
    // `0` means "block forever"; the conversion must not reject it. A key
    // that exists returns immediately, so this must not hang.
    let _ = run(&d, &["RPUSH", "l", "a"]).await;
    let r = run(&d, &["BLPOP", "l", "0"]).await;
    assert!(!is_err(&r), "BLPOP with timeout 0 regressed: {r:?}");
}

// ── unbounded allocation: counts ─────────────────────────────────────────────

/// `(0..count).collect()` over a `Range<usize>` is `TrustedLen`, so it
/// pre-allocates: `SRANDMEMBER s -1000000000` requested ~32 GB.
#[tokio::test]
async fn random_member_negative_count_is_capped() {
    let d = db();
    let _ = run(&d, &["SADD", "s", "a"]).await;

    let r = run(&d, &["SRANDMEMBER", "s", "-1000000000"]).await;
    assert!(
        !is_err(&r),
        "SRANDMEMBER with a huge negative count failed: {r:?}"
    );
    match &r {
        Resp::Array(Some(items)) => assert!(items.len() <= 100_000, "got {}", items.len()),
        other => panic!("expected an array, got {other:?}"),
    }

    let _ = run(&d, &["HSET", "h", "f", "v"]).await;
    let r = run(&d, &["HRANDFIELD", "h", "-1000000000"]).await;
    assert!(
        !is_err(&r),
        "HRANDFIELD with a huge negative count failed: {r:?}"
    );

    let _ = run(&d, &["ZADD", "z", "1", "m"]).await;
    let r = run(&d, &["ZRANDMEMBER", "z", "-1000000000"]).await;
    assert!(
        !is_err(&r),
        "ZRANDMEMBER with a huge negative count failed: {r:?}"
    );
}

/// `HSCAN`/`SSCAN`/`ZSCAN` fed the client's COUNT straight into
/// `BinaryHeap::with_capacity(count + 1)`; `COUNT 9223372036854775807`
/// panicked with "capacity overflow".
#[tokio::test]
async fn container_scans_clamp_the_count_hint() {
    let d = db();
    let _ = run(&d, &["HSET", "h", "f", "v"]).await;
    let _ = run(&d, &["SADD", "s", "a"]).await;
    let _ = run(&d, &["ZADD", "z", "1", "m"]).await;

    for cmdline in [
        vec!["HSCAN", "h", "0", "COUNT", "9223372036854775807"],
        vec!["SSCAN", "s", "0", "COUNT", "9223372036854775807"],
        vec!["ZSCAN", "z", "0", "COUNT", "9223372036854775807"],
        vec!["HSCAN", "h", "0", "COUNT", "1000000000000000"],
    ] {
        let r = run(&d, &cmdline).await;
        assert!(
            !is_err(&r),
            "{} panicked on an absurd COUNT: {r:?}",
            cmdline[0]
        );
    }
}

/// `LMPOP 1 L LEFT COUNT 288230376151711744` panicked with "capacity
/// overflow"; `COUNT 10000000000` reached the allocator for a 320 GB
/// pre-allocation.
#[tokio::test]
async fn lmpop_and_zmpop_clamp_the_count() {
    let d = db();
    let _ = run(&d, &["RPUSH", "L", "a", "b", "c"]).await;
    let _ = run(&d, &["ZADD", "Z", "1", "m1", "2", "m2"]).await;

    for count in ["288230376151711744", "10000000000"] {
        let r = run(&d, &["LMPOP", "1", "L", "LEFT", "COUNT", count]).await;
        assert!(!is_err(&r), "LMPOP COUNT {count} failed: {r:?}");
    }
    for count in ["288230376151711744", "10000000000"] {
        let r = run(&d, &["ZMPOP", "1", "Z", "MIN", "COUNT", count]).await;
        assert!(!is_err(&r), "ZMPOP COUNT {count} failed: {r:?}");
    }
}

/// `SETRANGE k 536870911 ""` allocated 512 MB to store a zero-length value,
/// because only `offset` was bounded and never `offset + patch.len()`.
#[tokio::test]
async fn setrange_bounds_the_result_not_just_the_offset() {
    let d = db();

    // An empty patch must not extend the string or create the key.
    let r = run(&d, &["SETRANGE", "fresh", "536870911", ""]).await;
    assert_eq!(as_int(&r), 0);
    assert_eq!(
        as_int(&run(&d, &["EXISTS", "fresh"]).await),
        0,
        "SETRANGE with an empty patch created the key"
    );

    // offset + patch length must also respect the 512 MB ceiling.
    let r = run(&d, &["SETRANGE", "big", "536870910", "xx"]).await;
    assert!(is_err(&r), "oversized SETRANGE was accepted: {r:?}");
}

// ── correctness ──────────────────────────────────────────────────────────────

/// `SETBIT`/`BITOP`/`BITFIELD` store a dedicated `Bitmap` variant, but
/// `as_string_bytes` only accepted `String`/`Int`. So `TYPE k` reported
/// `string` while `GET`, `STRLEN`, `APPEND` and every other string command
/// answered `WRONGTYPE` on that same key.
#[tokio::test]
async fn a_key_created_by_setbit_is_a_usable_string() {
    let d = db();
    let _ = run(&d, &["SETBIT", "b", "7", "1"]).await;

    assert_eq!(as_simple(&run(&d, &["TYPE", "b"]).await), "string");
    assert_eq!(as_int(&run(&d, &["STRLEN", "b"]).await), 1);
    let got = run(&d, &["GET", "b"]).await;
    // Bit offsets count down from the MSB, so offset 7 is the *least*
    // significant bit of byte 0 — `SETBIT b 7 1` yields "\x01".
    assert!(
        matches!(&got, Resp::BulkString(Some(v)) if v.as_ref() == b"\x01"),
        "GET after SETBIT returned {got:?}, expected a 1-byte 0x01 string"
    );

    // Every mutating string path must accept it too.
    assert!(!is_err(&run(&d, &["APPEND", "b", "!"]).await));
    assert_eq!(as_int(&run(&d, &["STRLEN", "b"]).await), 2);
    assert!(!is_err(&run(&d, &["GETRANGE", "b", "0", "-1"]).await));
    assert!(!is_err(&run(&d, &["SETRANGE", "b", "0", "A"]).await));

    // BITOP destinations have the same problem.
    let _ = run(&d, &["BITOP", "AND", "d", "b", "b"]).await;
    assert_eq!(as_simple(&run(&d, &["TYPE", "d"]).await), "string");
    assert!(!is_err(&run(&d, &["GET", "d"]).await));
}

/// `SMOVE` removed the member from the source *before* checking the
/// destination, then swallowed the mismatch in an `if let ... else { 0 }`.
/// `SMOVE src dst a` with a string `dst` replied `1` and destroyed `a` from
/// both sets.
#[tokio::test]
async fn smove_into_a_wrong_typed_destination_destroys_nothing() {
    let d = db();
    let _ = run(&d, &["SADD", "src", "a", "b", "c"]).await;
    let _ = run(&d, &["SET", "dst", "hello"]).await;

    let r = run(&d, &["SMOVE", "src", "dst", "a"]).await;
    assert!(is_err(&r), "SMOVE into a string destination returned {r:?}");

    assert_eq!(
        as_int(&run(&d, &["SISMEMBER", "src", "a"]).await),
        1,
        "the member was removed from the source despite the error"
    );
    // `SISMEMBER` against a string key is itself a WRONGTYPE error; what
    // matters is that the destination value survived and `src` is intact.
    assert!(is_err(&run(&d, &["SISMEMBER", "dst", "a"]).await));
    assert_eq!(as_str(&run(&d, &["GET", "dst"]).await), "hello");
}

#[tokio::test]
async fn smove_still_works_between_two_sets() {
    let d = db();
    let _ = run(&d, &["SADD", "s1", "a"]).await;
    let _ = run(&d, &["SADD", "s2", "b"]).await;

    assert_eq!(as_int(&run(&d, &["SMOVE", "s1", "s2", "a"]).await), 1);
    assert_eq!(as_int(&run(&d, &["SISMEMBER", "s1", "a"]).await), 0);
    assert_eq!(as_int(&run(&d, &["SISMEMBER", "s2", "a"]).await), 1);
}

/// `PFMERGE` mapped a wrong-length string destination to `None`, which fell
/// through to `unwrap_or_else(accum)` — overwriting and destroying whatever
/// the destination held while replying `+OK`.
#[tokio::test]
async fn pfmerge_refuses_to_overwrite_a_non_hll_destination() {
    let d = db();
    let _ = run(&d, &["SET", "dest", "hello"]).await;
    let _ = run(&d, &["PFADD", "src", "a"]).await;

    let r = run(&d, &["PFMERGE", "dest", "src"]).await;
    assert!(
        is_err(&r),
        "PFMERGE into a string destination returned {r:?}"
    );
    assert_eq!(
        as_str(&run(&d, &["GET", "dest"]).await),
        "hello",
        "PFMERGE destroyed the destination value"
    );
}

#[tokio::test]
async fn pfmerge_still_creates_a_missing_destination() {
    let d = db();
    let _ = run(&d, &["PFADD", "src", "a"]).await;

    let r = run(&d, &["PFMERGE", "dest", "src"]).await;
    assert!(!is_err(&r), "PFMERGE into a missing key failed: {r:?}");
    assert_eq!(as_int(&run(&d, &["EXISTS", "dest"]).await), 1);
    assert!(as_int(&run(&d, &["PFCOUNT", "dest"]).await) >= 1);
}

/// `command_table.rs` declares `PFADD` with arity -2 and Redis accepts
/// `PFADD key`, but the handler required at least one element.
#[tokio::test]
async fn pfadd_with_no_elements_is_accepted() {
    let d = db();
    let r = run(&d, &["PFADD", "k"]).await;
    assert!(!is_err(&r), "PFADD with no elements returned {r:?}");
}

/// `WATCH` is the standard lost-update guard, but only `insert`/`set_plain`/
/// `get_or_insert_with`'s vacant path bumped the key version. Every in-place
/// mutator — APPEND, HSET on an existing field, SADD, ZADD, LPUSH — left it
/// untouched, so `WATCH k` + another client's `APPEND k x` + `EXEC` still
/// committed.
#[tokio::test]
async fn watch_sees_in_place_mutations() {
    use nexrade_core::command::dispatch;
    let d = db();

    let _ = run(&d, &["SET", "k", "v"]).await;
    let before = ver(&d, b"k");

    let _ = run(&d, &["APPEND", "k", "x"]).await;
    let after = ver(&d, b"k");
    assert_ne!(
        before, after,
        "APPEND did not advance the key version, so WATCH cannot see it"
    );

    // The same must hold for the container fast paths and for TTL changes.
    let _ = run(&d, &["HSET", "h", "f", "1"]).await;
    let v1 = ver(&d, b"h");
    let _ = run(&d, &["HSET", "h", "f", "2"]).await;
    assert_ne!(
        v1,
        ver(&d, b"h"),
        "HSET on an existing field is invisible to WATCH"
    );

    let v2 = ver(&d, b"k");
    let _ = run(&d, &["EXPIRE", "k", "100"]).await;
    assert_ne!(v2, ver(&d, b"k"), "EXPIRE is invisible to WATCH");

    let v3 = ver(&d, b"k");
    let _ = run(&d, &["PERSIST", "k"]).await;
    assert_ne!(v3, ver(&d, b"k"), "PERSIST is invisible to WATCH");

    // A write on a different key must NOT invalidate it.
    let v4 = ver(&d, b"k");
    let _ = dispatch(&d, cmd(&["SET", "unrelated", "1"]), 0).await;
    assert_eq!(v4, ver(&d, b"k"));
}

/// `Entry::new` carries no expiry and `insert` replaces the whole entry, so
/// these in-place updates silently turned volatile keys permanent.
#[tokio::test]
async fn in_place_updates_preserve_the_ttl() {
    let d = db();

    for (setup, mutate) in [
        (
            vec!["SET", "s", "10", "EX", "1000"],
            vec!["INCRBYFLOAT", "s", "5"],
        ),
        (
            vec!["SET", "t", "v", "EX", "1000"],
            vec!["SETRANGE", "t", "0", "Z"],
        ),
    ] {
        let _ = run(&d, &setup).await;
        let r = run(&d, &mutate).await;
        assert!(!is_err(&r), "{} failed: {r:?}", mutate[0]);

        let ttl = as_int(&run(&d, &["TTL", mutate[1]]).await);
        assert!(
            ttl > 0,
            "{} dropped the key's TTL (TTL is now {ttl})",
            mutate[0]
        );
    }

    // PFADD likewise.
    let _ = run(&d, &["SET", "p", "x"]).await;
    let _ = run(&d, &["DEL", "p"]).await;
    let _ = run(&d, &["PFADD", "p", "a"]).await;
    let _ = run(&d, &["EXPIRE", "p", "1000"]).await;
    let _ = run(&d, &["PFADD", "p", "b"]).await;
    assert!(
        as_int(&run(&d, &["TTL", "p"]).await) > 0,
        "PFADD dropped the key's TTL"
    );
}

// ── silently-ignored options and phantom keys ─────────────────────────────────

/// `XADD`'s field/value loop started at index 3, so the trimming modifiers
/// were parsed as payload: `XADD s * MAXLEN 2 f v` stored a field named
/// "MAXLEN" holding "2", and `XADD s * NOMKSTREAM f v` (odd count) dropped the
/// trailing pair while still replying `+<id>`.
#[tokio::test]
async fn xadd_parses_its_trimming_modifiers() {
    let d = db();
    let _ = run(&d, &["XADD", "s", "*", "f", "v"]).await;

    // MAXLEN 2 keeps the newest two entries. Redis puts the trimming
    // modifiers *before* the id: `XADD key [MAXLEN n] <id> field value ...`.
    let _ = run(&d, &["XADD", "s", "MAXLEN", "2", "*", "a", "1"]).await;
    let _ = run(&d, &["XADD", "s", "MAXLEN", "2", "*", "b", "2"]).await;
    assert_eq!(as_int(&run(&d, &["XLEN", "s"]).await), 2);

    // The payload survived intact — no field named "MAXLEN".
    let r = run(&d, &["XRANGE", "s", "-", "+"]).await;
    assert!(!is_err(&r), "XRANGE failed: {r:?}");
    let text = format!("{r:?}");
    assert!(
        !text.contains("MAXLEN"),
        "MAXLEN leaked into the entry fields: {text}"
    );

    // The `~` hint is accepted.
    let _ = run(&d, &["XADD", "t", "MAXLEN", "~", "1", "*", "c", "3"]).await;
    assert_eq!(as_int(&run(&d, &["XLEN", "t"]).await), 1);

    // MINID trims by id on the same path.
    for i in 1..=4 {
        let id = format!("{i}-0");
        let _ = run(
            &d,
            &["XADD", "m", "MINID", "~", "3-0", id.as_str(), "f", "v"],
        )
        .await;
    }
    let r = run(&d, &["XRANGE", "m", "-", "+"]).await;
    assert!(
        arr_ids(&r).first().map(String::as_str) == Some("3-0"),
        "MINID kept the wrong entries: {r:?}"
    );
}

/// The id element of each entry in an `XRANGE` reply.
fn arr_ids(r: &Resp) -> Vec<String> {
    match r {
        Resp::Array(Some(streams)) => streams
            .iter()
            .filter_map(|s| match s {
                Resp::Array(Some(parts)) => match &parts[0] {
                    Resp::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                    _ => None,
                },
                _ => None,
            })
            .collect(),
        other => panic!("expected an array, got {other:?}"),
    }
}

#[tokio::test]
async fn xadd_nomkststream_does_not_create_the_key() {
    let d = db();
    let r = run(&d, &["XADD", "absent", "NOMKSTREAM", "*", "f", "v"]).await;
    assert!(
        matches!(r, Resp::Null | Resp::BulkString(None)),
        "got {r:?}"
    );
    assert_eq!(
        as_int(&run(&d, &["EXISTS", "absent"]).await),
        0,
        "NOMKSTREAM created the stream anyway"
    );
}

#[tokio::test]
async fn xadd_rejects_a_dangling_field_name() {
    let d = db();
    // The old loop's `i + 1 < args.len()` guard silently dropped `v`.
    let r = run(&d, &["XADD", "s", "*", "field"]).await;
    assert!(is_err(&r), "a dangling field name was accepted: {r:?}");
    assert_eq!(as_int(&run(&d, &["EXISTS", "s"]).await), 0);
}

/// `XTRIM` only understood `MAXLEN` and returned 0 for everything else,
/// including `MINID` — a stream that could never be trimmed by id.
#[tokio::test]
async fn xtrim_supports_minid() {
    let d = db();
    for i in 1..=5 {
        let id = format!("{i}-0");
        let _ = run(&d, &["XADD", "s", id.as_str(), "f", "v"]).await;
    }
    let r = run(&d, &["XTRIM", "s", "MINID", "3-0"]).await;
    assert_eq!(as_int(&r), 2, "XTRIM MINID returned {r:?}");
    assert_eq!(as_int(&run(&d, &["XLEN", "s"]).await), 3);

    // An unknown strategy used to silently report 0 removals.
    assert!(is_err(&run(&d, &["XTRIM", "s", "BOGUS", "1"]).await));
}

/// Redis accepts a bare millisecond component as `ms-0`; this handler
/// required the `-seq` half, so `XADD s 5 f v` errored out.
#[tokio::test]
async fn a_bare_millisecond_id_is_accepted() {
    let d = db();
    let r = run(&d, &["XADD", "s", "5", "f", "v"]).await;
    assert_eq!(as_str(&r), "5-0", "bare-ms XADD returned {r:?}");

    let _ = run(&d, &["XADD", "s", "7-0", "g", "v"]).await;
    assert!(!is_err(&run(&d, &["XREAD", "STREAMS", "s", "5"]).await));
    assert!(!is_err(&run(&d, &["XRANGE", "s", "5", "+"]).await));
}

/// `ZRANGE` looked only at `args[4]` for `WITHSCORES` and always read the
/// bounds as ranks, so every modern option was silently dropped and the whole
/// sorted set came back.
#[tokio::test]
async fn zrange_honours_byscore_bylex_rev_and_limit() {
    let d = db();
    for (m, s) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")] {
        let _ = run(&d, &["ZADD", "z", s, m]).await;
    }

    // Plain rank range still works.
    let r = run(&d, &["ZRANGE", "z", "0", "2"]).await;
    assert_eq!(
        arr(&r),
        vec!["a", "b", "c"],
        "plain ZRANGE regressed: {r:?}"
    );

    // BYSCORE with LIMIT.
    let r = run(
        &d,
        &["ZRANGE", "z", "(2", "+inf", "BYSCORE", "LIMIT", "0", "2"],
    )
    .await;
    assert_eq!(arr(&r), vec!["c", "d"], "BYSCORE+LIMIT returned {r:?}");

    // REV reverses.
    let r = run(&d, &["ZRANGE", "z", "(2", "+inf", "BYSCORE", "REV"]).await;
    assert_eq!(arr(&r), vec!["e", "d", "c"], "BYSCORE+REV returned {r:?}");

    // BYLEX.
    let r = run(&d, &["ZRANGE", "z", "-", "+", "BYLEX"]).await;
    assert_eq!(
        arr(&r),
        vec!["a", "b", "c", "d", "e"],
        "BYLEX returned {r:?}"
    );
    let r = run(&d, &["ZRANGE", "z", "-", "(c", "BYLEX"]).await;
    assert_eq!(arr(&r), vec!["a", "b"], "exclusive BYLEX returned {r:?}");

    // WITHSCORES works in any position.
    let r = run(&d, &["ZRANGE", "z", "0", "0", "WITHSCORES"]).await;
    assert_eq!(arr(&r), vec!["a", "1"], "WITHSCORES returned {r:?}");

    // LIMIT is rank-range nonsense.
    assert!(is_err(
        &run(&d, &["ZRANGE", "z", "0", "-1", "LIMIT", "0", "1"]).await
    ));
    // Negative LIMIT, and typos, must not be silently swallowed.
    assert!(is_err(
        &run(
            &d,
            &["ZRANGE", "z", "-", "+", "BYSCORE", "LIMIT", "-1", "1"]
        )
        .await
    ));
    assert!(is_err(
        &run(&d, &["ZRANGE", "z", "-", "+", "BYSCORE", "LIMTI", "0", "1"]).await
    ));
}

/// A NaN bound makes every float comparison false, so it silently produced an
/// empty array instead of an error.
#[tokio::test]
async fn nan_score_bounds_are_rejected() {
    let d = db();
    let _ = run(&d, &["ZADD", "z", "1", "m"]).await;
    for args in [
        vec!["ZRANGEBYSCORE", "z", "nan", "5"],
        vec!["ZRANGEBYSCORE", "z", "1", "nan"],
        vec!["ZRANGEBYSCORE", "z", "(nan", "5"],
        vec!["ZRANGE", "z", "nan", "5", "BYSCORE"],
    ] {
        let r = run(&d, &args).await;
        assert!(is_err(&r), "{} with a NaN bound returned {r:?}", args[0]);
    }
}

/// `ZRANGEBYSCORE ... LIMIT` cast both values straight to `usize`, so a
/// negative offset became a multi-exabyte skip that returned nothing.
#[tokio::test]
async fn zrangebyscore_rejects_a_negative_limit() {
    let d = db();
    let _ = run(&d, &["ZADD", "z", "1", "m"]).await;
    assert!(is_err(
        &run(
            &d,
            &["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "-1", "1"]
        )
        .await
    ));
    assert!(is_err(
        &run(
            &d,
            &[
                "ZRANGEBYSCORE",
                "z",
                "-inf",
                "+inf",
                "LIMIT",
                "0",
                "1",
                "BOGUS"
            ]
        )
        .await
    ));
}

/// `get_or_create_zset` ran before the `XX` check, so `ZADD k XX ...` created
/// the key it was explicitly told not to touch.
#[tokio::test]
async fn zadd_xx_does_not_create_a_missing_key() {
    let d = db();
    let r = run(&d, &["ZADD", "k", "XX", "1", "m"]).await;
    assert_eq!(as_int(&r), 0, "ZADD XX returned {r:?}");
    assert_eq!(
        as_int(&run(&d, &["EXISTS", "k"]).await),
        0,
        "ZADD XX conjured a key into existence"
    );

    // Against an existing key XX still updates *existing* members. Note that XX
    // never adds a new member — `ZADD k XX 2 n` must stay 0.
    let _ = run(&d, &["ZADD", "k", "1", "m"]).await;
    assert_eq!(
        as_int(&run(&d, &["ZADD", "k", "XX", "2", "n"]).await),
        0,
        "XX added a new member"
    );
    assert_eq!(
        as_int(&run(&d, &["ZADD", "k", "XX", "CH", "2", "m"]).await),
        1,
        "XX failed to update an existing member"
    );
    assert_eq!(as_str(&run(&d, &["ZSCORE", "k", "m"]).await), "2");
}

/// Same trap in GEOADD.
#[tokio::test]
async fn geoadd_xx_does_not_create_a_missing_key() {
    let d = db();
    let r = run(&d, &["GEOADD", "g", "XX", "15", "37", "m"]).await;
    assert_eq!(as_int(&r), 0, "GEOADD XX returned {r:?}");
    assert_eq!(as_int(&run(&d, &["EXISTS", "g"]).await), 0);
}

/// `HINCRBYFLOAT` created the hash before the overflow check, so a rejected
/// `1e309` still left an empty hash behind.
#[tokio::test]
async fn hincrbyfloat_overflow_creates_no_phantom_key() {
    let d = db();
    for args in [
        vec!["HINCRBYFLOAT", "h", "f", "1e309"],
        vec!["HINCRBYFLOAT", "h2", "f", "-1e309"],
        vec!["HINCRBYFLOAT", "h3", "f", "inf"],
    ] {
        let r = run(&d, &args).await;
        assert!(is_err(&r), "{} was accepted: {r:?}", args[0]);
        let k = args[1];
        assert_eq!(
            as_int(&run(&d, &["EXISTS", k]).await),
            0,
            "{} left a phantom hash behind",
            args[0]
        );
    }
    // The normal path still creates the key.
    assert!(!is_err(&run(&d, &["HINCRBYFLOAT", "ok", "f", "1.5"]).await));
}

/// With a count, a missing key is an empty array, not a nil — RESP
/// distinguishes `*0` from `$-1` and clients branch on it.
#[tokio::test]
async fn counted_random_and_pop_on_a_missing_key_reply_empty_array() {
    let d = db();
    for cmdname in ["SRANDMEMBER", "SPOP"] {
        let r = run(&d, &[cmdname, "absent", "3"]).await;
        assert!(
            matches!(&r, Resp::Array(Some(v)) if v.is_empty()),
            "{cmdname} with a count on a missing key returned {r:?}"
        );
        // Without a count, nil is correct.
        let r = run(&d, &[cmdname, "absent"]).await;
        assert!(
            matches!(r, Resp::Null | Resp::BulkString(None)),
            "{cmdname} without a count on a missing key returned {r:?}"
        );
    }
}

/// `RANK -9223372036854775808` went through `unsigned_abs() as i64`, which
/// wrapped straight back to i64::MIN and matched the *first* hit from the tail.
#[tokio::test]
async fn lpos_negative_rank_at_i64_min_matches_nothing() {
    let d = db();
    let _ = run(&d, &["RPUSH", "l", "a", "x", "b", "x", "c", "x"]).await;
    let r = run(&d, &["LPOS", "l", "x", "RANK", "-9223372036854775808"]).await;
    assert!(
        matches!(r, Resp::Null | Resp::BulkString(None)),
        "LPOS with the most negative rank returned {r:?}"
    );
    // Sanity: rank -1 finds the last occurrence (index 5).
    assert_eq!(as_int(&run(&d, &["LPOS", "l", "x", "RANK", "-1"]).await), 5);
}

/// Redis: `LPOS` on a missing key with COUNT is `[]`, without COUNT is nil.
#[tokio::test]
async fn lpos_on_a_missing_key_matches_the_resp_contract() {
    let d = db();
    let r = run(&d, &["LPOS", "absent", "x"]).await;
    assert!(
        matches!(r, Resp::Null | Resp::BulkString(None)),
        "got {r:?}"
    );
    let r = run(&d, &["LPOS", "absent", "x", "COUNT", "2"]).await;
    assert!(
        matches!(&r, Resp::Array(Some(v)) if v.is_empty()),
        "got {r:?}"
    );
}
