//! The streamed `Snapshot::save` must stay byte-identical to the previous
//! `encode_to_vec` layout (MAGIC + payload + CRC32C(payload) LE) and must
//! still round-trip through `Snapshot::load`. Byte compatibility matters
//! because the restore path and any other process reading the file expect
//! the exact same format the buffer variant produced.

use nexrade_core::db::Db;
use nexrade_core::persistence::Snapshot;
use nexrade_core::resp::Resp;
use nexrade_core::types::DataType;

fn str_arg(s: &str) -> Resp {
    Resp::bulk_str(s.to_string())
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    nexrade_core::command::dispatch_with_addr(db, args, 0, None).await
}

#[tokio::test]
async fn save_is_byte_identical_to_encode_to_vec_layout_and_round_trips() {
    let db = Db::default();
    run(
        &db,
        vec![
            str_arg("SET"),
            str_arg("ticker:bybit:lyn"),
            str_arg("0.109589"),
        ],
    )
    .await;
    run(
        &db,
        vec![
            str_arg("RPUSH"),
            str_arg("spread:x"),
            str_arg("1.5"),
            str_arg("2.5"),
            str_arg("3.5"),
        ],
    )
    .await;
    run(
        &db,
        vec![
            str_arg("HSET"),
            str_arg("meta:conf"),
            str_arg("region"),
            str_arg("apac"),
        ],
    )
    .await;
    run(
        &db,
        vec![
            str_arg("SET"),
            str_arg("ephemeral"),
            str_arg("v"),
            str_arg("EX"),
            str_arg("60"),
        ],
    )
    .await;

    let snapshot = Snapshot::new(db.store.snapshot_dbs());

    // Reference layout: MAGIC + encode_to_vec payload + CRC32C(payload) LE.
    let payload = bincode::serde::encode_to_vec(&snapshot, bincode::config::standard()).unwrap();
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&payload);
    let crc = hasher.finalize();
    let mut expected = Vec::with_capacity(5 + payload.len() + 4);
    expected.extend_from_slice(&Snapshot::MAGIC);
    expected.extend_from_slice(&payload);
    expected.extend_from_slice(&crc.to_le_bytes());

    let path = std::env::temp_dir().join(format!("nexrade_stream_save_{}.rdb", std::process::id()));
    let _ = std::fs::remove_file(&path);
    snapshot.save(&path).expect("streamed save");
    let bytes = std::fs::read(&path).expect("read saved file");

    assert_eq!(
        bytes, expected,
        "streamed save must be byte-identical to the encode_to_vec layout"
    );

    // Round-trip through load and spot-check the values survived.
    let loaded = Snapshot::load(&path).expect("load");
    let _ = std::fs::remove_file(&path);

    assert_eq!(loaded.databases.len(), 1, "one non-empty db expected");
    let entries = &loaded.databases[0].1.entries;
    match &entries.get(&b"ticker:bybit:lyn".to_vec()).unwrap().value {
        DataType::String(v) => assert_eq!(v.as_ref(), b"0.109589"),
        other => panic!("expected string, got {other:?}"),
    }
    match &entries.get(&b"spread:x".to_vec()).unwrap().value {
        DataType::List(items) => assert_eq!(items.len(), 3),
        other => panic!("expected list, got {other:?}"),
    }
    match &entries.get(&b"meta:conf".to_vec()).unwrap().value {
        DataType::Hash(h) => assert_eq!(h.len(), 1),
        other => panic!("expected hash, got {other:?}"),
    }
    let ephemeral = entries.get(&b"ephemeral".to_vec()).unwrap();
    assert!(
        ephemeral.expiry.is_some(),
        "expiry must survive the streamed save"
    );
}
