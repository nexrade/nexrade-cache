//! 0.4.9 — live_bytes tracks BITFIELD SET/INCRBY buffer growth.

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
async fn live_bytes_tracks_bitfield_set_growth() {
    let db = Db::new(small_config());
    let initial = db.store.estimated_memory_bytes();

    // SET u8 at bit offset 0 → 1-byte bitmap (create via insert).
    let r = run(
        &db,
        vec![
            str_arg("BITFIELD"),
            str_arg("bf"),
            str_arg("SET"),
            str_arg("u8"),
            str_arg("0"),
            str_arg("42"),
        ],
    )
    .await;
    match r {
        Resp::Array(Some(v)) => assert_eq!(v, vec![Resp::int(0)]),
        other => panic!("expected array [0], got {other:?}"),
    }
    let after_create = db.store.estimated_memory_bytes();
    assert!(
        after_create > initial,
        "BITFIELD create undercount: initial={initial} after={after_create}"
    );

    // Same-size overwrite must be Δ0 for payload.
    let before = after_create;
    let _ = run(
        &db,
        vec![
            str_arg("BITFIELD"),
            str_arg("bf"),
            str_arg("SET"),
            str_arg("u8"),
            str_arg("0"),
            str_arg("99"),
        ],
    )
    .await;
    let after_same = db.store.estimated_memory_bytes();
    assert_eq!(
        after_same, before,
        "same-length BITFIELD SET must be Δ0: before={before} after={after_same}"
    );

    // Grow: SET u8 at bit offset 800 → needs 101 bytes (ceil(808/8)).
    let _ = run(
        &db,
        vec![
            str_arg("BITFIELD"),
            str_arg("bf"),
            str_arg("SET"),
            str_arg("u8"),
            str_arg("800"),
            str_arg("7"),
        ],
    )
    .await;
    let after_grow = db.store.estimated_memory_bytes();
    assert!(
        after_grow >= before + 80,
        "BITFIELD SET grow undercount: before={before} after={after_grow}"
    );
}

#[tokio::test]
async fn live_bytes_tracks_bitfield_incrby_growth() {
    let db = Db::new(small_config());
    // Seed a 1-byte field.
    let _ = run(
        &db,
        vec![
            str_arg("BITFIELD"),
            str_arg("bf"),
            str_arg("SET"),
            str_arg("u8"),
            str_arg("0"),
            str_arg("1"),
        ],
    )
    .await;
    let before = db.store.estimated_memory_bytes();

    // INCRBY at a far offset forces growth.
    let r = run(
        &db,
        vec![
            str_arg("BITFIELD"),
            str_arg("bf"),
            str_arg("INCRBY"),
            str_arg("u16"),
            str_arg("640"),
            str_arg("3"),
        ],
    )
    .await;
    match r {
        Resp::Array(Some(v)) => assert_eq!(v, vec![Resp::int(3)]),
        other => panic!("expected array [3], got {other:?}"),
    }
    let after = db.store.estimated_memory_bytes();
    // bit 640+16 → ceil(656/8)=82 bytes; was 1 → +81
    assert!(
        after >= before + 70,
        "BITFIELD INCRBY grow undercount: before={before} after={after}"
    );
}
