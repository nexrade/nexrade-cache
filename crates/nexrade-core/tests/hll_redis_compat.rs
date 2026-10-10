//! Compatibility with HyperLogLog values copied straight out of Redis.
//!
//! Redis stores an HLL as a string: a 16-byte header (magic `HYLL`, a 1-byte
//! encoding, 3 bytes padding, 8 bytes cached cardinality) followed by either a
//! dense 6-bit-packed register array or a sparse run-length encoding. A
//! verbatim byte copy of such a key into nexrade looks correct — same `TYPE`,
//! same bytes — but `PFCOUNT` / `PFADD` / `PFMERGE` then reject it, which is
//! exactly what a migration produces if the copy step does not convert.

use bytes::Bytes;

use nexrade_core::command::hll;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;
use nexrade_core::store::Entry;
use nexrade_core::types::{hll_count, DataType, HLL_REGISTERS};

fn str_arg(s: &str) -> Resp {
    Resp::bulk_str(s.to_string())
}

fn resp_to_string(r: &Resp) -> String {
    match r {
        Resp::SimpleString(s) => s.clone(),
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        Resp::Integer(i) => i.to_string(),
        Resp::Error(s) => s.clone(),
        _ => format!("{r:?}"),
    }
}

fn err_of(r: &Resp) -> String {
    match r {
        Resp::Error(s) => s.clone(),
        other => panic!("expected an error, got {other:?}"),
    }
}

/// Pack 6-bit registers the way Redis does: one little-endian bit stream,
/// register `i` occupying stream bits `[6i, 6i+6)`. Written bit-by-bit so it
/// is an independent restatement of the format rather than a copy of the
/// byte-straddling shortcut the decoder uses.
fn pack_redis_dense(registers: &[u8; HLL_REGISTERS]) -> Vec<u8> {
    let mut out = vec![0u8; HLL_REGISTERS * 6 / 8];
    for (i, &v) in registers.iter().enumerate() {
        for bit in 0..6 {
            if v & (1 << bit) != 0 {
                let s = i * 6 + bit;
                out[s / 8] |= 1 << (s % 8);
            }
        }
    }
    out
}

fn redis_dense_blob(registers: &[u8; HLL_REGISTERS]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(16 + HLL_REGISTERS * 6 / 8);
    blob.extend_from_slice(b"HYLL");
    blob.push(0); // HLL_DENSE
    blob.extend_from_slice(&[0u8; 3]); // padding
    blob.extend_from_slice(&[0u8; 8]); // cached cardinality — nexrade recomputes
    blob.extend_from_slice(&pack_redis_dense(registers));
    blob
}

fn set_raw(db: &Db, key: &str, bytes: Vec<u8>) {
    let value = Bytes::from(bytes);
    let k = key.as_bytes().to_vec();
    db.store
        .db(0)
        .write_for(&k)
        .insert(k, Entry::new(DataType::String(value)));
}

/// A register pattern that exercises the three awkward cases of the packing:
/// a register straddling a byte boundary at offset 6, one at offset 4, and a
/// value using all 6 bits.
fn sample_registers() -> [u8; HLL_REGISTERS] {
    let mut r = [0u8; HLL_REGISTERS];
    for (i, v) in [
        (0usize, 1u8),
        (1, 63),
        (2, 32),
        (3, 5),
        (11, 17),
        (12, 63),
        (13, 1),
        (1318, 7),
        (15949, 21),
        (16383, 63),
    ] {
        r[i] = v;
    }
    r
}

#[tokio::test]
async fn pfcount_reads_a_redis_dense_blob() {
    let db = Db::default();
    let registers = sample_registers();
    let expected = hll_count(&registers);
    set_raw(&db, "mig", redis_dense_blob(&registers));

    let r = hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("mig")], 0)
        .await
        .unwrap();
    assert_eq!(resp_to_string(&r), expected.to_string());
}

#[tokio::test]
async fn pfadd_on_a_redis_dense_blob_normalises_the_key() {
    let db = Db::default();
    let mut registers = sample_registers();
    registers[0] = 9;
    set_raw(&db, "mig", redis_dense_blob(&registers));

    // Re-adding an element only touches registers; the key must survive and
    // still answer PFCOUNT with the same estimate.
    let before = hll_count(&registers);
    let r = hll::cmd_pfadd(&db, &[str_arg("PFADD"), str_arg("mig"), str_arg("x")], 0)
        .await
        .unwrap();
    assert_eq!(resp_to_string(&r), "1");

    let after = resp_to_string(
        &hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("mig")], 0)
            .await
            .unwrap(),
    );
    // Register 0 went 9 -> 9 (x may or may not land elsewhere), so the count is
    // unchanged unless x raised some register; either way it must still parse.
    assert!(
        after.parse::<u64>().is_ok_and(|c| c.abs_diff(before) <= 2),
        "PFCOUNT went from {before} to {after}"
    );

    // And the stored value is now nexrade's own dense form.
    let key = Bytes::from_static(b"mig");
    let shard = db.store.db(0).read_for(&key);
    let entry = shard.get_ro(&key);
    match &entry.expect("key still present").value {
        DataType::HyperLogLog(v) => assert_eq!(v.len(), HLL_REGISTERS),
        DataType::String(v) => assert_eq!(v.len(), HLL_REGISTERS),
        other => panic!("unexpected type {other:?}"),
    }
}

#[tokio::test]
async fn pfmerge_reads_a_redis_dense_blob() {
    let db = Db::default();
    let mut source = [0u8; HLL_REGISTERS];
    source[100] = 4;
    source[9000] = 9;
    set_raw(&db, "src", redis_dense_blob(&source));

    let r = hll::cmd_pfmerge(
        &db,
        &[str_arg("PFMERGE"), str_arg("dst"), str_arg("src")],
        0,
    )
    .await
    .unwrap();
    assert_eq!(resp_to_string(&r), "OK");

    let count: u64 = resp_to_string(
        &hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("dst")], 0)
            .await
            .unwrap(),
    )
    .parse()
    .unwrap();
    assert_eq!(count, hll_count(&source));
}

/// A real 27-byte sparse blob captured from redis:7 — the shape a migration
/// produces for every HLL below `hll_sparse_max_bytes`.
const REAL_SPARSE_BLOB: &[u8] = &[
    0x48, 0x59, 0x4c, 0x4c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
    0x60, 0xf3, 0x80, 0x50, 0xb1, 0x84, 0x4b, 0xfb, 0x80, 0x42, 0x5a,
];

#[tokio::test]
async fn sparse_redis_blob_is_rejected_with_an_actionable_message() {
    let db = Db::default();
    assert_eq!(REAL_SPARSE_BLOB.len(), 27);
    set_raw(&db, "sparse", REAL_SPARSE_BLOB.to_vec());

    for r in [
        hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("sparse")], 0)
            .await
            .unwrap_err(),
        hll::cmd_pfadd(&db, &[str_arg("PFADD"), str_arg("sparse"), str_arg("d")], 0)
            .await
            .unwrap_err(),
        hll::cmd_pfmerge(
            &db,
            &[str_arg("PFMERGE"), str_arg("out"), str_arg("sparse")],
            0,
        )
        .await
        .unwrap_err(),
    ] {
        let msg = err_of(&Resp::Error(r.to_string()));
        assert!(
            msg.contains("sparse"),
            "message should name the encoding, got: {msg}"
        );
        assert!(
            msg.contains("migrate_from_redis.py") || msg.contains("PFADD"),
            "message should say what to do, got: {msg}"
        );
    }
}

#[tokio::test]
async fn an_unknown_redis_encoding_is_rejected() {
    let db = Db::default();
    let mut blob = redis_dense_blob(&[0u8; HLL_REGISTERS]);
    blob[4] = 9;
    set_raw(&db, "weird", blob);

    let e = hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("weird")], 0)
        .await
        .unwrap_err();
    let msg = err_of(&Resp::Error(e.to_string()));
    assert!(msg.contains("unknown Redis encoding 9"), "got: {msg}");
}

/// A plain wrong-length string keeps its original wording — the migration
/// guidance must only appear for values that actually carry Redis' header.
#[tokio::test]
async fn a_plain_wrong_length_string_still_reports_its_length() {
    let db = Db::default();
    set_raw(&db, "plain", b"hello".to_vec());

    let e = hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("plain")], 0)
        .await
        .unwrap_err();
    let msg = err_of(&Resp::Error(e.to_string()));
    assert!(msg.contains("expected 16384 bytes, got 5"), "got: {msg}");
    assert!(!msg.contains("migrate_from_redis.py"), "got: {msg}");
}

/// A dense blob whose payload is the wrong size must not be silently zeroed.
#[tokio::test]
async fn a_truncated_dense_blob_is_rejected() {
    let db = Db::default();
    let mut blob = redis_dense_blob(&[0u8; HLL_REGISTERS]);
    blob.truncate(blob.len() - 3);
    set_raw(&db, "short", blob);

    let e = hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("short")], 0)
        .await
        .unwrap_err();
    let msg = err_of(&Resp::Error(e.to_string()));
    assert!(msg.contains("Redis dense header"), "got: {msg}");
}
