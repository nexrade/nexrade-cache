//! 0.4.8 — live_bytes tracks APPEND / SETBIT growth / stream / GEOADD.

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

fn str_arg(s: &str) -> Resp {
    Resp::BulkString(Some(bytes::Bytes::from(s.to_string())))
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0).await
}

#[tokio::test]
async fn live_bytes_tracks_append() {
    let db = Db::new(small_config());
    let _ = run(&db, vec![str_arg("SET"), str_arg("k"), str_arg("hi")]).await;
    let after_set = db.store.estimated_memory_bytes();
    assert!(after_set >= 2, "SET undercount: {after_set}");

    let r = run(&db, vec![str_arg("APPEND"), str_arg("k"), str_arg("there")]).await;
    assert_eq!(r, Resp::int(7)); // "hithere"
    let after_append = db.store.estimated_memory_bytes();
    assert!(
        after_append >= after_set + 5,
        "APPEND did not grow live_bytes: set={after_set} append={after_append}"
    );
}

#[tokio::test]
async fn live_bytes_tracks_setbit_growth() {
    let db = Db::new(small_config());
    // Offset 80 → byte_idx 10 → 11-byte bitmap.
    let r = run(
        &db,
        vec![str_arg("SETBIT"), str_arg("b"), str_arg("80"), str_arg("1")],
    )
    .await;
    assert_eq!(r, Resp::int(0));
    let after = db.store.estimated_memory_bytes();
    assert!(after >= 11, "SETBIT create undercounts bitmap: {after}");

    // Same-size flip must not balloon (Δ0 payload).
    let before_flip = after;
    let _ = run(
        &db,
        vec![str_arg("SETBIT"), str_arg("b"), str_arg("80"), str_arg("0")],
    )
    .await;
    let after_flip = db.store.estimated_memory_bytes();
    assert_eq!(
        after_flip, before_flip,
        "same-length SETBIT must be Δ0: before={before_flip} after={after_flip}"
    );

    // Grow further: offset 800 → byte 100.
    let _ = run(
        &db,
        vec![
            str_arg("SETBIT"),
            str_arg("b"),
            str_arg("800"),
            str_arg("1"),
        ],
    )
    .await;
    let after_grow = db.store.estimated_memory_bytes();
    assert!(
        after_grow >= before_flip + 80,
        "SETBIT grow undercount: before={before_flip} after={after_grow}"
    );
}

#[tokio::test]
async fn live_bytes_tracks_xadd_xdel_xtrim() {
    let db = Db::new(small_config());
    let initial = db.store.estimated_memory_bytes();

    let r = run(
        &db,
        vec![
            str_arg("XADD"),
            str_arg("s"),
            str_arg("1-0"),
            str_arg("field"),
            str_arg("value-xxx"), // 9 bytes
        ],
    )
    .await;
    match r {
        Resp::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"1-0"),
        other => panic!("expected id bulk, got {other:?}"),
    }
    // id "1-0"(3) + field(5) + value(9) = 17 payload (+ shell)
    let after_add = db.store.estimated_memory_bytes();
    assert!(
        after_add >= initial + 17,
        "XADD undercount: initial={initial} after={after_add}"
    );

    let _ = run(
        &db,
        vec![
            str_arg("XADD"),
            str_arg("s"),
            str_arg("2-0"),
            str_arg("f"),
            str_arg("v"),
        ],
    )
    .await;
    let after_two = db.store.estimated_memory_bytes();
    assert!(after_two > after_add, "second XADD must grow live_bytes");

    let del = run(&db, vec![str_arg("XDEL"), str_arg("s"), str_arg("2-0")]).await;
    assert_eq!(del, Resp::int(1));
    let after_del = db.store.estimated_memory_bytes();
    // Dropped id "2-0"(3) + "f"(1) + "v"(1) = 5
    assert!(
        after_del + 5 <= after_two,
        "XDEL did not shrink: two={after_two} del={after_del}"
    );

    // Trim remaining entry away (MAXLEN 0).
    let trim = run(
        &db,
        vec![
            str_arg("XTRIM"),
            str_arg("s"),
            str_arg("MAXLEN"),
            str_arg("0"),
        ],
    )
    .await;
    assert_eq!(trim, Resp::int(1));
    let after_trim = db.store.estimated_memory_bytes();
    // Empty stream shell remains (Redis keeps the key); payload gone.
    assert!(
        after_trim < after_del,
        "XTRIM must drop entry payload: del={after_del} trim={after_trim}"
    );
    // Still above pure zero if shell overhead remains.
    assert!(
        after_trim >= initial,
        "unexpected underflow after XTRIM: {after_trim}"
    );
}

#[tokio::test]
async fn live_bytes_tracks_geoadd() {
    let db = Db::new(small_config());
    let initial = db.store.estimated_memory_bytes();
    let r = run(
        &db,
        vec![
            str_arg("GEOADD"),
            str_arg("g"),
            str_arg("13.361389"),
            str_arg("38.115556"),
            str_arg("Palermo"),
        ],
    )
    .await;
    assert_eq!(r, Resp::int(1));
    let after = db.store.estimated_memory_bytes();
    // Empty geo shell + 24 B slot for one member.
    assert!(
        after >= initial + 24,
        "GEOADD undercount: initial={initial} after={after}"
    );

    // Overwrite same member — Δ0 for coordinates.
    let before_ow = after;
    let r2 = run(
        &db,
        vec![
            str_arg("GEOADD"),
            str_arg("g"),
            str_arg("15.087269"),
            str_arg("37.502669"),
            str_arg("Palermo"),
        ],
    )
    .await;
    assert_eq!(r2, Resp::int(0)); // already existed, not CH
    let after_ow = db.store.estimated_memory_bytes();
    assert_eq!(
        after_ow, before_ow,
        "GEOADD overwrite must be Δ0: before={before_ow} after={after_ow}"
    );

    // Second member.
    let _ = run(
        &db,
        vec![
            str_arg("GEOADD"),
            str_arg("g"),
            str_arg("15.087269"),
            str_arg("37.502669"),
            str_arg("Catania"),
        ],
    )
    .await;
    let after_two = db.store.estimated_memory_bytes();
    assert!(
        after_two >= before_ow + 24,
        "second GEOADD undercount: one={before_ow} two={after_two}"
    );
}
