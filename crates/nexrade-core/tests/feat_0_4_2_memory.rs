//! 0.4.2 — empty-key GC, post-write maxmemory, RDB version reject.

use nexrade_core::command::dispatch;
use nexrade_core::db::{Db, MaxMemoryPolicy, ServerConfig};
use nexrade_core::persistence::Snapshot;
use nexrade_core::resp::Resp;
use nexrade_core::store::Database;

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
async fn empty_key_deleted_after_lpop_all() {
    let db = Db::new(small_config());
    let _ = run(
        &db,
        vec![str_arg("LPUSH"), str_arg("mylist"), str_arg("only")],
    )
    .await;
    assert_eq!(
        run(&db, vec![str_arg("EXISTS"), str_arg("mylist")]).await,
        Resp::int(1)
    );
    let _ = run(&db, vec![str_arg("LPOP"), str_arg("mylist")]).await;
    assert_eq!(
        run(&db, vec![str_arg("EXISTS"), str_arg("mylist")]).await,
        Resp::int(0),
        "empty list key must be deleted (Redis-style)"
    );
    assert_eq!(
        db.store.estimated_memory_bytes(),
        0,
        "live_bytes must return to 0 after empty-key GC"
    );
}

#[tokio::test]
async fn empty_key_deleted_after_hdel_srem_zrem() {
    let db = Db::new(small_config());

    let _ = run(
        &db,
        vec![str_arg("HSET"), str_arg("h"), str_arg("f"), str_arg("v")],
    )
    .await;
    let _ = run(&db, vec![str_arg("HDEL"), str_arg("h"), str_arg("f")]).await;
    assert_eq!(
        run(&db, vec![str_arg("EXISTS"), str_arg("h")]).await,
        Resp::int(0)
    );

    let _ = run(&db, vec![str_arg("SADD"), str_arg("s"), str_arg("m")]).await;
    let _ = run(&db, vec![str_arg("SREM"), str_arg("s"), str_arg("m")]).await;
    assert_eq!(
        run(&db, vec![str_arg("EXISTS"), str_arg("s")]).await,
        Resp::int(0)
    );

    let _ = run(
        &db,
        vec![str_arg("ZADD"), str_arg("z"), str_arg("1"), str_arg("m")],
    )
    .await;
    let _ = run(&db, vec![str_arg("ZREM"), str_arg("z"), str_arg("m")]).await;
    assert_eq!(
        run(&db, vec![str_arg("EXISTS"), str_arg("z")]).await,
        Resp::int(0)
    );

    assert_eq!(db.store.estimated_memory_bytes(), 0);
}

#[tokio::test]
async fn empty_key_deleted_after_lmove_last() {
    let db = Db::new(small_config());
    let _ = run(&db, vec![str_arg("LPUSH"), str_arg("src"), str_arg("x")]).await;
    let resp = run(
        &db,
        vec![
            str_arg("LMOVE"),
            str_arg("src"),
            str_arg("dst"),
            str_arg("LEFT"),
            str_arg("LEFT"),
        ],
    )
    .await;
    match resp {
        Resp::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"x"),
        other => panic!("expected bulk, got {other:?}"),
    }
    assert_eq!(
        run(&db, vec![str_arg("EXISTS"), str_arg("src")]).await,
        Resp::int(0),
        "src emptied by LMOVE must be deleted"
    );
    assert_eq!(
        run(&db, vec![str_arg("EXISTS"), str_arg("dst")]).await,
        Resp::int(1)
    );
}

#[tokio::test]
async fn post_write_eviction_bounds_list_payload() {
    // Unlike the 0.4.0 test (manual evict loop), rely on dispatch's
    // post-write evict_if_needed so a fat write cannot sit above the cap.
    let mut c = small_config();
    c.max_memory = Some(64 * 1024);
    c.maxmemory_policy = MaxMemoryPolicy::AllKeysLru;
    let db = Db::new(c);

    for i in 0..2000 {
        let _ = run(
            &db,
            vec![
                str_arg("LPUSH"),
                str_arg(&format!("list{i}")),
                Resp::BulkString(Some(bytes::Bytes::from(vec![b'x'; 256]))),
            ],
        )
        .await;
    }

    let live = db.store.estimated_memory_bytes();
    assert!(
        live <= 64 * 1024 + 4096,
        "post-write eviction left live_bytes ({live}) far above 64KiB"
    );
}

#[test]
fn snapshot_rejects_wrong_version() {
    let snap = Snapshot::new(vec![(0, Database::new())]);
    assert_eq!(snap.version, Snapshot::VERSION);

    let mut bad = snap;
    bad.version = if Snapshot::VERSION > 1 {
        Snapshot::VERSION - 1
    } else {
        999
    };
    let path = std::env::temp_dir().join(format!("nexrade_bad_rdb_{}.rdb", std::process::id()));
    let encoded = bincode::serde::encode_to_vec(&bad, bincode::config::standard()).unwrap();
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&encoded);
    let crc = hasher.finalize();
    let mut file = Vec::with_capacity(Snapshot::MAGIC.len() + encoded.len() + 4);
    file.extend_from_slice(&Snapshot::MAGIC);
    file.extend_from_slice(&encoded);
    file.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, file).unwrap();
    let err = Snapshot::load(&path).expect_err("wrong version must fail");
    let _ = std::fs::remove_file(&path);
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported RDB snapshot version") || msg.contains("expects"),
        "unexpected error: {msg}"
    );
}

#[test]
fn snapshot_accepts_current_version() {
    let snap = Snapshot::new(vec![(0, Database::new())]);
    let path = std::env::temp_dir().join(format!("nexrade_ok_rdb_{}.rdb", std::process::id()));
    snap.save(&path).unwrap();
    let loaded = Snapshot::load(&path).expect("current version must load");
    let _ = std::fs::remove_file(&path);
    assert_eq!(loaded.version, Snapshot::VERSION);
}
