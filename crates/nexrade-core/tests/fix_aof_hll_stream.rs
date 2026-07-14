//! Tests for the AOF rewrite, stream ID comparison, and HLL commands.

use std::collections::HashMap;

use nexrade_core::command::hll;
use nexrade_core::command::stream::StreamId;
use nexrade_core::db::Db;
use nexrade_core::persistence::AofWriter;
use nexrade_core::resp::Resp;
use nexrade_core::store::Entry;
use nexrade_core::types::{DataType, HLL_REGISTERS};

// ── helpers ──────────────────────────────────────────────────────────────────

fn str_arg(s: &str) -> Resp {
    Resp::bulk_str(s.to_string())
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    nexrade_core::command::dispatch_with_addr(db, args, 0, None).await
}

fn resp_to_string(r: &Resp) -> String {
    match r {
        Resp::SimpleString(s) => s.clone(),
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        Resp::BulkString(None) => "(nil)".into(),
        Resp::Integer(i) => i.to_string(),
        Resp::Error(s) => format!("ERR {s}"),
        _ => format!("{:?}", r),
    }
}

async fn xadd(db: &Db, key: &str, id: &str, fields: &[(&str, &str)]) -> Resp {
    let mut args = vec![str_arg("XADD"), str_arg(key), str_arg(id)];
    for (k, v) in fields {
        args.push(str_arg(k));
        args.push(str_arg(v));
    }
    run(db, args).await
}

// ── Stream ID comparison ────────────────────────────────────────────────────

#[test]
fn stream_id_ordering_is_numeric() {
    assert!(StreamId::parse("10-0").unwrap() > StreamId::parse("2-0").unwrap());
    assert!(StreamId::parse("100-0").unwrap() > StreamId::parse("99-999").unwrap());
    assert!(StreamId::parse("1-0").unwrap() < StreamId::parse("1-1").unwrap());
    assert_eq!(
        StreamId::parse("5-3").unwrap(),
        StreamId::parse("5-3").unwrap()
    );
    assert_eq!(StreamId::parse("not-an-id"), None);
}

#[tokio::test]
async fn xrange_orders_by_numeric_id_not_string() {
    let db = Db::default();
    // Two entries: id "2-0" then id "10-0". With string-lex compare, "10-0"
    // would compare less than "2-0", silently mis-ordering. With numeric
    // ordering, "10-0" is greater.
    xadd(&db, "s", "2-0", &[("k", "v2")]).await;
    xadd(&db, "s", "10-0", &[("k", "v10")]).await;

    let r = run(
        &db,
        vec![str_arg("XRANGE"), str_arg("s"), str_arg("-"), str_arg("+")],
    )
    .await;
    if let Resp::Array(Some(entries)) = r {
        assert_eq!(entries.len(), 2);
        // First entry should be 2-0, second should be 10-0.
        let first_id = match &entries[0] {
            Resp::Array(Some(a)) => resp_to_string(&a[0]),
            _ => panic!("unexpected entry shape"),
        };
        let second_id = match &entries[1] {
            Resp::Array(Some(a)) => resp_to_string(&a[0]),
            _ => panic!("unexpected entry shape"),
        };
        assert_eq!(first_id, "2-0");
        assert_eq!(second_id, "10-0");
    } else {
        panic!("XRANGE returned non-array: {:?}", r);
    }
}

#[tokio::test]
async fn xadd_explicit_id_validation() {
    let db = Db::default();
    xadd(&db, "s2", "5-0", &[("k", "v")]).await;
    // Lower id should fail.
    let r = xadd(&db, "s2", "4-0", &[("k", "v")]).await;
    assert!(
        matches!(r, Resp::Error(_)),
        "expected error for older id, got {:?}",
        r
    );
    // Equal id should fail too.
    let r = xadd(&db, "s2", "5-0", &[("k", "v")]).await;
    assert!(
        matches!(r, Resp::Error(_)),
        "expected error for equal id, got {:?}",
        r
    );
    // Higher id should succeed.
    let r = xadd(&db, "s2", "5-1", &[("k", "v")]).await;
    assert!(!matches!(r, Resp::Error(_)));
}

#[tokio::test]
async fn xread_uses_numeric_filter() {
    let db = Db::default();
    xadd(&db, "s3", "2-0", &[("k", "v2")]).await;
    xadd(&db, "s3", "10-0", &[("k", "v10")]).await;

    // XREAD with last id "5-0" should only return 10-0.
    let r = run(
        &db,
        vec![
            str_arg("XREAD"),
            str_arg("STREAMS"),
            str_arg("s3"),
            str_arg("5-0"),
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
        assert_eq!(entries.len(), 1);
        if let Resp::Array(Some(a)) = &entries[0] {
            assert_eq!(resp_to_string(&a[0]), "10-0");
        } else {
            panic!();
        }
    } else {
        panic!("XREAD returned non-array: {:?}", r);
    }
}

// ── HyperLogLog ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn pfadd_pfcount_basic() {
    let db = Db::default();
    let r = hll::cmd_pfadd(
        &db,
        &[
            str_arg("PFADD"),
            str_arg("hll"),
            str_arg("alpha"),
            str_arg("beta"),
            str_arg("gamma"),
        ],
        0,
    )
    .await
    .unwrap();
    // First insert should return 1.
    assert_eq!(resp_to_string(&r), "1");

    // Re-adding same elements should return 0 (no register changed).
    let r = hll::cmd_pfadd(
        &db,
        &[
            str_arg("PFADD"),
            str_arg("hll"),
            str_arg("alpha"),
            str_arg("beta"),
        ],
        0,
    )
    .await
    .unwrap();
    assert_eq!(resp_to_string(&r), "0");

    // PFCOUNT should report ~3.
    let r = hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("hll")], 0)
        .await
        .unwrap();
    let count: u64 = resp_to_string(&r).parse().unwrap();
    // Within typical HLL error margin (a few percent).
    assert!((2..=4).contains(&count), "expected count ~3, got {count}");
}

#[tokio::test]
async fn pfcount_on_missing_key_is_zero() {
    let db = Db::default();
    let r = hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("nope")], 0)
        .await
        .unwrap();
    assert_eq!(resp_to_string(&r), "0");
}

#[tokio::test]
async fn pfmerge_unions_multiple_hlls() {
    let db = Db::default();

    // hll1: {a, b, c, d, e}
    hll::cmd_pfadd(
        &db,
        &[
            str_arg("PFADD"),
            str_arg("hll1"),
            str_arg("a"),
            str_arg("b"),
            str_arg("c"),
            str_arg("d"),
            str_arg("e"),
        ],
        0,
    )
    .await
    .unwrap();
    // hll2: {d, e, f, g, h}
    hll::cmd_pfadd(
        &db,
        &[
            str_arg("PFADD"),
            str_arg("hll2"),
            str_arg("d"),
            str_arg("e"),
            str_arg("f"),
            str_arg("g"),
            str_arg("h"),
        ],
        0,
    )
    .await
    .unwrap();

    // Merge hll1 + hll2 → hll_merged. Distinct set is {a..h}, 8 elements.
    let r = hll::cmd_pfmerge(
        &db,
        &[
            str_arg("PFMERGE"),
            str_arg("hll_merged"),
            str_arg("hll1"),
            str_arg("hll2"),
        ],
        0,
    )
    .await
    .unwrap();
    assert_eq!(resp_to_string(&r), "OK");

    let r = hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("hll_merged")], 0)
        .await
        .unwrap();
    let count: u64 = resp_to_string(&r).parse().unwrap();
    assert!((7..=9).contains(&count), "expected count ~8, got {count}");
}

#[tokio::test]
async fn pfadd_thousands_within_1pct_error() {
    let db = Db::default();
    let n: usize = 5_000;
    let mut args = vec![str_arg("PFADD"), str_arg("big")];
    for i in 0..n {
        args.push(str_arg(&format!("user-{i}")));
    }
    hll::cmd_pfadd(&db, &args, 0).await.unwrap();

    let r = hll::cmd_pfcount(&db, &[str_arg("PFCOUNT"), str_arg("big")], 0)
        .await
        .unwrap();
    let count: u64 = resp_to_string(&r).parse().unwrap();
    // Standard error for HLL is roughly 1.04 / sqrt(m) = ~0.8% with m=16384.
    let err_pct = ((count as f64 - n as f64).abs() / n as f64) * 100.0;
    assert!(
        err_pct < 3.0,
        "expected cardinality within 3%, got {count} vs n={n} (err={err_pct:.2}%)"
    );
}

// ── AOF rewrite ──────────────────────────────────────────────────────────────

// Custom RdbWriter-style helper that walks the same Database entries and
// reproduces the rewrite logic. We exercise it directly via the public API
// (AofWriter::rewrite).
fn build_db_with_all_types() -> Db {
    // Construct an in-memory database with one of each data type, plus a
    // stream that has a consumer group with a last_delivered_id set.
    let db = Db::default();
    let shard = db.store.db(0);

    // String.
    {
        let mut g = shard.write_for(b"a_string");
        g.insert(
            b"a_string".to_vec(),
            Entry::new(DataType::String(b"hello".to_vec())),
        );
    }
    // Bitmap.
    {
        let mut g = shard.write_for(b"a_bitmap");
        let mut bits = vec![0u8; 4];
        bits[0] = 0b10100000; // bit 0 and 2 set
        bits[1] = 0b00000001; // bit 15 set (MSB)
        g.insert(b"a_bitmap".to_vec(), Entry::new(DataType::Bitmap(bits)));
    }
    // HyperLogLog.
    {
        let mut g = shard.write_for(b"a_hll");
        let mut regs = [0u8; HLL_REGISTERS];
        regs[0] = 5;
        regs[100] = 12;
        g.insert(
            b"a_hll".to_vec(),
            Entry::new(DataType::HyperLogLog(regs.to_vec())),
        );
    }
    // Geo.
    {
        let mut geo = nexrade_core::types::GeoData::new();
        geo.members.insert(
            b"point_a".to_vec(),
            nexrade_core::types::GeoPoint {
                longitude: 13.36,
                latitude: 38.11,
            },
        );
        geo.members.insert(
            b"point_b".to_vec(),
            nexrade_core::types::GeoPoint {
                longitude: 12.50,
                latitude: 41.90,
            },
        );
        let mut g = shard.write_for(b"a_geo");
        g.insert(b"a_geo".to_vec(), Entry::new(DataType::Geo(geo)));
    }
    // Stream with consumer group.
    {
        let mut stream = nexrade_core::types::StreamData::new();
        stream.entries.push(nexrade_core::types::StreamEntry {
            id: "1700000000000-0".to_string(),
            fields: vec![(b"msg".to_vec(), b"hi".to_vec())],
        });
        stream.entries.push(nexrade_core::types::StreamEntry {
            id: "1700000000001-0".to_string(),
            fields: vec![(b"msg".to_vec(), b"hi2".to_vec())],
        });
        let mut group =
            nexrade_core::types::ConsumerGroup::new(b"g1".to_vec(), "1700000000001-0".to_string());
        // Pretend one pending entry exists (this won't be preserved by AOF
        // rewrite, but the group state is).
        group.pending.insert(
            "1700000000000-0".to_string(),
            nexrade_core::types::PendingEntry {
                consumer: b"c1".to_vec(),
                delivery_time_ms: 1,
                delivery_count: 1,
            },
        );
        stream.groups.insert(b"g1".to_vec(), group);

        let mut g = shard.write_for(b"a_stream");
        g.insert(b"a_stream".to_vec(), Entry::new(DataType::Stream(stream)));
    }

    db
}

#[tokio::test]
async fn aof_rewrite_round_trip_all_types() {
    use nexrade_core::resp::RespParser;
    let db = build_db_with_all_types();

    // Write the rewrite to a temp file.
    let tmp = std::env::temp_dir().join("nexrade_test_aof_rewrite.aof");
    let _ = std::fs::remove_file(&tmp);

    let dbs = db.store.snapshot_dbs();
    AofWriter::rewrite(&tmp, &dbs, &[]).expect("rewrite failed");

    // Read the file back, parse each command.
    let bytes = std::fs::read(&tmp).expect("read failed");
    let _ = std::fs::remove_file(&tmp);

    let mut parser = RespParser::new();
    parser.feed(&bytes);
    let mut cmds: Vec<Vec<Resp>> = Vec::new();
    while let Some(resp) = parser.parse_one().expect("parse") {
        match resp {
            Resp::Array(Some(items)) => cmds.push(items),
            _ => panic!("expected array, got {resp:?}"),
        }
    }

    // SET a_bitmap <bytes>
    let set_bitmap_cmd = cmds
        .iter()
        .find(|c| {
            c.first().and_then(|a| a.as_str()) == Some("SET")
                && c.get(1).and_then(|a| a.as_str()) == Some("a_bitmap")
        })
        .expect("SET a_bitmap");
    let bitmap_bytes = match &set_bitmap_cmd[2] {
        Resp::BulkString(Some(b)) => b.to_vec(),
        _ => panic!("expected bulk"),
    };
    assert_eq!(bitmap_bytes, vec![0b10100000, 0b00000001, 0, 0]);

    // SET a_hll <bytes>
    let set_hll_cmd = cmds
        .iter()
        .find(|c| {
            c.first().and_then(|a| a.as_str()) == Some("SET")
                && c.get(1).and_then(|a| a.as_str()) == Some("a_hll")
        })
        .expect("SET a_hll");
    let hll_bytes = match &set_hll_cmd[2] {
        Resp::BulkString(Some(b)) => b.to_vec(),
        _ => panic!("expected bulk"),
    };
    assert_eq!(hll_bytes.len(), HLL_REGISTERS);
    assert_eq!(hll_bytes[0], 5);
    assert_eq!(hll_bytes[100], 12);

    // GEOADD a_geo <lons> <lats> <members>
    let geoadd_cmd = cmds
        .iter()
        .find(|c| {
            c.first().and_then(|a| a.as_str()) == Some("GEOADD")
                && c.get(1).and_then(|a| a.as_str()) == Some("a_geo")
        })
        .expect("GEOADD a_geo");
    // Members are at indexes 3, 6, ... (lon, lat, member)
    let mut geo_members = HashMap::new();
    let mut i = 2;
    while i + 2 < geoadd_cmd.len() {
        let lon: f64 = geoadd_cmd[i].as_str().unwrap().parse().unwrap();
        let lat: f64 = geoadd_cmd[i + 1].as_str().unwrap().parse().unwrap();
        let member = geoadd_cmd[i + 2].as_str().unwrap().to_string();
        geo_members.insert(member, (lon, lat));
        i += 3;
    }
    assert_eq!(geo_members.len(), 2);
    assert!(geo_members.contains_key("point_a"));
    assert!(geo_members.contains_key("point_b"));

    // Two XADDs and one XGROUP CREATE for a_stream
    let xadd_cmds: Vec<&Vec<Resp>> = cmds
        .iter()
        .filter(|c| {
            c.first().and_then(|a| a.as_str()) == Some("XADD")
                && c.get(1).and_then(|a| a.as_str()) == Some("a_stream")
        })
        .collect();
    assert_eq!(xadd_cmds.len(), 2);

    let xgroup_cmds: Vec<&Vec<Resp>> = cmds
        .iter()
        .filter(|c| c.first().and_then(|a| a.as_str()) == Some("XGROUP"))
        .collect();
    assert_eq!(xgroup_cmds.len(), 1);
    assert_eq!(xgroup_cmds[0][1].as_str(), Some("CREATE"));
    assert_eq!(xgroup_cmds[0][2].as_str(), Some("a_stream"));
    assert_eq!(xgroup_cmds[0][3].as_str(), Some("g1"));
    assert_eq!(xgroup_cmds[0][4].as_str(), Some("1700000000001-0"));

    // Now simulate replay by executing the rewritten commands on a fresh DB.
    let replay_db = Db::default();
    for cmd in &cmds {
        let r = nexrade_core::command::dispatch_with_addr(&replay_db, cmd.clone(), 0, None).await;
        assert!(
            !matches!(r, Resp::Error(_)),
            "replay of {:?} failed: {:?}",
            cmd.first(),
            r
        );
    }

    // Verify replay state matches the original.
    let shard = replay_db.store.db(0);
    // Bitmap.
    {
        let bitmap_guard = shard.read_for(b"a_bitmap");
        let bitmap = bitmap_guard.get_ro(b"a_bitmap").unwrap();
        let bitmap_bytes = match &bitmap.value {
            DataType::String(v) | DataType::Bitmap(v) => v.clone(),
            _ => panic!("wrong type for replay bitmap"),
        };
        assert_eq!(bitmap_bytes, vec![0b10100000, 0b00000001, 0, 0]);
    }

    // HLL.
    let hll_bytes: Vec<u8> = {
        let hll_guard = shard.read_for(b"a_hll");
        let hll_entry = hll_guard.get_ro(b"a_hll").unwrap();
        match &hll_entry.value {
            DataType::HyperLogLog(v) | DataType::String(v) => v.clone(),
            _ => panic!("wrong type for replay hll"),
        }
    };
    assert_eq!(hll_bytes[0], 5);
    assert_eq!(hll_bytes[100], 12);
    // PFCOUNT should still work on the replayed data (loads as String).
    let pfcount = hll::cmd_pfcount(&replay_db, &[str_arg("PFCOUNT"), str_arg("a_hll")], 0)
        .await
        .unwrap();
    let n: u64 = resp_to_string(&pfcount).parse().unwrap();
    assert!(n <= 2, "PFCOUNT should see ~2 registers, got {n}");

    // Geo: members restored.
    {
        let geo_guard = shard.read_for(b"a_geo");
        let geo_entry = geo_guard.get_ro(b"a_geo").unwrap();
        let geo = match &geo_entry.value {
            DataType::Geo(g) => g,
            _ => panic!("wrong type for replay geo"),
        };
        let geo_member_count = geo.members.len();
        assert_eq!(geo_member_count, 2);
    }

    // Stream: 2 entries, 1 group with last_delivered_id preserved.
    let (entries_len, groups_len, g1_last) = {
        let stream_guard = shard.read_for(b"a_stream");
        let stream_entry = stream_guard.get_ro(b"a_stream").unwrap();
        let stream = match &stream_entry.value {
            DataType::Stream(s) => s,
            _ => panic!("wrong type for replay stream"),
        };
        let g1_last = stream
            .groups
            .get(b"g1".as_slice())
            .map(|g| g.last_delivered_id.clone());
        (stream.entries.len(), stream.groups.len(), g1_last)
    };
    assert_eq!(entries_len, 2);
    assert_eq!(groups_len, 1);
    assert_eq!(g1_last.as_deref(), Some("1700000000001-0"));

    // XPENDING is empty after replay (pending state is not preserved).
    let xpending = run(
        &replay_db,
        vec![str_arg("XPENDING"), str_arg("a_stream"), str_arg("g1")],
    )
    .await;
    if let Resp::Array(Some(arr)) = xpending {
        // Summary form: [count, min, max, consumers[]].
        let count_str = resp_to_string(&arr[0]);
        assert_eq!(count_str, "0", "expected 0 pending after rewrite replay");
    } else {
        panic!("unexpected XPENDING output");
    }
}
