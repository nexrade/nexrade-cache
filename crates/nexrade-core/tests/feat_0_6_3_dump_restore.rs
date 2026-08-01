//! 0.6.3 — DUMP / RESTORE edges beyond the 0.2.4 string roundtrip.
//!
//! Locks down:
//!   * multi-type NEXD roundtrip (list / hash / set / zset / stream)
//!   * RESTORE relative TTL + ABSTTL
//!   * RESTORE rejects wrong version / corrupt body
//!   * REPLACE vs BUSYKEY already covered in feat_0_2_4; TTL + types here

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

fn int_eq(r: &Resp, want: i64) {
    match r {
        Resp::Integer(n) => assert_eq!(*n, want),
        other => panic!("expected integer {want}, got {other:?}"),
    }
}

async fn dump_async(db: &Db, key: &str) -> bytes::Bytes {
    match run(db, cmd(&["DUMP", key])).await {
        Resp::BulkString(Some(b)) => {
            assert!(b.starts_with(b"NEXD"), "DUMP must start with NEXD");
            assert_eq!(b[4], 1, "DUMP version must be 1");
            b
        }
        other => panic!("DUMP {key} expected bulk, got {other:?}"),
    }
}

async fn restore_ok(db: &Db, key: &str, ttl: &str, payload: bytes::Bytes, extra: &[&str]) {
    let mut args = vec![
        Resp::bulk_str("RESTORE"),
        Resp::bulk_str(key),
        Resp::bulk_str(ttl),
        Resp::BulkString(Some(payload)),
    ];
    for e in extra {
        args.push(Resp::bulk_str(*e));
    }
    ok(&run(db, args).await);
}

// ─── Multi-type roundtrips ──────────────────────────────────────────────────

#[tokio::test]
async fn dump_restore_list() {
    let db = Db::new(small_config());
    int_eq(&run(&db, cmd(&["RPUSH", "l", "a", "b", "c"])).await, 3);
    let p = dump_async(&db, "l").await;
    restore_ok(&db, "l2", "0", p, &[]).await;
    int_eq(&run(&db, cmd(&["LLEN", "l2"])).await, 3);
    match run(&db, cmd(&["LINDEX", "l2", "1"])).await {
        Resp::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"b"),
        other => panic!("expected b, got {other:?}"),
    }
}

#[tokio::test]
async fn dump_restore_hash() {
    let db = Db::new(small_config());
    int_eq(
        &run(&db, cmd(&["HSET", "h", "f1", "v1", "f2", "v2"])).await,
        2,
    );
    let p = dump_async(&db, "h").await;
    restore_ok(&db, "h2", "0", p, &[]).await;
    int_eq(&run(&db, cmd(&["HLEN", "h2"])).await, 2);
    match run(&db, cmd(&["HGET", "h2", "f1"])).await {
        Resp::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"v1"),
        other => panic!("expected v1, got {other:?}"),
    }
}

#[tokio::test]
async fn dump_restore_set() {
    let db = Db::new(small_config());
    int_eq(&run(&db, cmd(&["SADD", "s", "x", "y"])).await, 2);
    let p = dump_async(&db, "s").await;
    restore_ok(&db, "s2", "0", p, &[]).await;
    int_eq(&run(&db, cmd(&["SCARD", "s2"])).await, 2);
    int_eq(&run(&db, cmd(&["SISMEMBER", "s2", "x"])).await, 1);
}

#[tokio::test]
async fn dump_restore_zset() {
    let db = Db::new(small_config());
    int_eq(&run(&db, cmd(&["ZADD", "z", "1", "a", "2", "b"])).await, 2);
    let p = dump_async(&db, "z").await;
    restore_ok(&db, "z2", "0", p, &[]).await;
    int_eq(&run(&db, cmd(&["ZCARD", "z2"])).await, 2);
    match run(&db, cmd(&["ZSCORE", "z2", "b"])).await {
        Resp::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!((s.parse::<f64>().unwrap() - 2.0).abs() < 1e-9, "score={s}");
        }
        other => panic!("expected score bulk, got {other:?}"),
    }
}

#[tokio::test]
async fn dump_restore_stream() {
    let db = Db::new(small_config());
    match run(&db, cmd(&["XADD", "st", "1-0", "f", "v"])).await {
        Resp::BulkString(Some(_)) => {}
        other => panic!("XADD failed: {other:?}"),
    }
    let p = dump_async(&db, "st").await;
    restore_ok(&db, "st2", "0", p, &[]).await;
    int_eq(&run(&db, cmd(&["XLEN", "st2"])).await, 1);
}

// ─── TTL ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn restore_relative_ttl() {
    let db = Db::new(small_config());
    ok(&run(&db, cmd(&["SET", "k", "v"])).await);
    let p = dump_async(&db, "k").await;
    // 60_000 ms relative.
    restore_ok(&db, "k_ttl", "60000", p, &[]).await;
    match run(&db, cmd(&["PTTL", "k_ttl"])).await {
        Resp::Integer(t) => {
            assert!(t > 0 && t <= 60_000, "PTTL must be in (0, 60000], got {t}");
        }
        other => panic!("expected PTTL int, got {other:?}"),
    }
}

#[tokio::test]
async fn restore_absttl_future() {
    let db = Db::new(small_config());
    ok(&run(&db, cmd(&["SET", "k", "v"])).await);
    let p = dump_async(&db, "k").await;
    // Absolute deadline ~60s in the future (unix ms).
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let deadline = (now_ms + 60_000).to_string();
    restore_ok(&db, "k_abs", &deadline, p, &["ABSTTL"]).await;
    match run(&db, cmd(&["PTTL", "k_abs"])).await {
        Resp::Integer(t) => {
            assert!(t > 0 && t <= 60_000, "ABSTTL PTTL in (0, 60000], got {t}");
        }
        other => panic!("expected PTTL int, got {other:?}"),
    }
}

#[tokio::test]
async fn restore_absttl_past_errors() {
    let db = Db::new(small_config());
    ok(&run(&db, cmd(&["SET", "k", "v"])).await);
    let p = dump_async(&db, "k").await;
    let r = run(
        &db,
        vec![
            Resp::bulk_str("RESTORE"),
            Resp::bulk_str("past"),
            Resp::bulk_str("1"), // 1970
            Resp::BulkString(Some(p)),
            Resp::bulk_str("ABSTTL"),
        ],
    )
    .await;
    match r {
        Resp::Error(msg) => assert!(
            msg.to_ascii_lowercase().contains("ttl") || msg.contains("Invalid"),
            "past ABSTTL must error, got {msg}"
        ),
        other => panic!("expected error, got {other:?}"),
    }
}

// ─── Version / corrupt payload ──────────────────────────────────────────────

#[tokio::test]
async fn restore_rejects_wrong_version() {
    let db = Db::new(small_config());
    // NEXD + version 99 + empty body
    let mut payload = b"NEXD".to_vec();
    payload.push(99);
    payload.extend_from_slice(&[0u8; 8]);
    let r = run(
        &db,
        vec![
            Resp::bulk_str("RESTORE"),
            Resp::bulk_str("k"),
            Resp::bulk_str("0"),
            Resp::BulkString(Some(bytes::Bytes::from(payload))),
        ],
    )
    .await;
    match r {
        Resp::Error(msg) => assert!(
            msg.contains("unsupported DUMP version") || msg.contains("version"),
            "got {msg}"
        ),
        other => panic!("expected version error, got {other:?}"),
    }
}

#[tokio::test]
async fn restore_rejects_corrupt_body() {
    let db = Db::new(small_config());
    let mut payload = b"NEXD".to_vec();
    payload.push(1);
    payload.extend_from_slice(b"not-valid-bincode!!!!");
    let r = run(
        &db,
        vec![
            Resp::bulk_str("RESTORE"),
            Resp::bulk_str("k"),
            Resp::bulk_str("0"),
            Resp::BulkString(Some(bytes::Bytes::from(payload))),
        ],
    )
    .await;
    match r {
        Resp::Error(msg) => assert!(msg.contains("corrupt") || msg.contains("DUMP"), "got {msg}"),
        other => panic!("expected corrupt error, got {other:?}"),
    }
}

#[tokio::test]
async fn restore_ttl_zero_clears_expiry() {
    let db = Db::new(small_config());
    ok(&run(&db, cmd(&["SET", "k", "v", "EX", "30"])).await);
    let p = dump_async(&db, "k").await;
    // RESTORE with ttl=0 must create a persistent key (Redis semantics:
    // the command TTL overrides the payload expiry).
    restore_ok(&db, "k_persist", "0", p, &[]).await;
    match run(&db, cmd(&["TTL", "k_persist"])).await {
        Resp::Integer(t) => assert_eq!(t, -1, "ttl=0 must be persistent, got {t}"),
        other => panic!("expected TTL int, got {other:?}"),
    }
}
