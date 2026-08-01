//! 0.2.4 feature coverage: DUMP/RESTORE, ZRANDMEMBER WITHSCORES, BZPOPMIN/MAX,
//! SINTERCARD, BITFIELD_RO, and AOF PEL rewrite.

use std::time::Duration;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::persistence::AofWriter;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

fn as_bulk(r: &Resp) -> &[u8] {
    match r {
        Resp::BulkString(Some(b)) => b.as_ref(),
        other => panic!("expected bulk, got {other:?}"),
    }
}

fn as_array(r: Resp) -> Vec<Resp> {
    match r {
        Resp::Array(Some(a)) => a,
        other => panic!("expected array, got {other:?}"),
    }
}

fn as_int(r: &Resp) -> i64 {
    match r {
        Resp::Integer(n) => *n,
        other => panic!("expected int, got {other:?}"),
    }
}

// ── DUMP / RESTORE ────────────────────────────────────────────────────────────

#[tokio::test]
async fn dump_restore_roundtrip_string() {
    let db = Db::default();
    assert!(matches!(
        run(&db, cmd(&["SET", "k", "hello"])).await,
        Resp::SimpleString(_)
    ));

    let dumped = run(&db, cmd(&["DUMP", "k"])).await;
    let payload = match dumped {
        Resp::BulkString(Some(b)) => b,
        other => panic!("DUMP must return bulk, got {other:?}"),
    };
    assert!(
        payload.starts_with(b"NEXD"),
        "payload must start with NEXD magic"
    );

    // Missing key → null.
    assert!(matches!(
        run(&db, cmd(&["DUMP", "nope"])).await,
        Resp::BulkString(None) | Resp::Null
    ));

    // Restore under a new key.
    let mut restore_args = vec![
        Resp::bulk_str("RESTORE"),
        Resp::bulk_str("k2"),
        Resp::bulk_str("0"),
        Resp::BulkString(Some(payload.clone())),
    ];
    let r = run(&db, restore_args.clone()).await;
    assert!(matches!(r, Resp::SimpleString(ref s) if s == "OK"), "{r:?}");

    let got = run(&db, cmd(&["GET", "k2"])).await;
    assert_eq!(as_bulk(&got), b"hello");

    // Without REPLACE, restoring onto an existing key is BUSYKEY.
    restore_args[1] = Resp::bulk_str("k"); // overwrite attempt
    let busy = run(&db, restore_args).await;
    match busy {
        Resp::Error(msg) => assert!(msg.contains("BUSYKEY"), "{msg}"),
        other => panic!("expected BUSYKEY, got {other:?}"),
    }

    // With REPLACE it succeeds.
    let r = run(
        &db,
        vec![
            Resp::bulk_str("RESTORE"),
            Resp::bulk_str("k"),
            Resp::bulk_str("0"),
            Resp::BulkString(Some(payload)),
            Resp::bulk_str("REPLACE"),
        ],
    )
    .await;
    assert!(matches!(r, Resp::SimpleString(s) if s == "OK"));
}

#[tokio::test]
async fn restore_rejects_non_nexd_payload() {
    let db = Db::default();
    let r = run(
        &db,
        vec![
            Resp::bulk_str("RESTORE"),
            Resp::bulk_str("k"),
            Resp::bulk_str("0"),
            Resp::bulk_str("this-is-not-nexd"),
        ],
    )
    .await;
    match r {
        Resp::Error(msg) => {
            assert!(
                msg.contains("NEXD") || msg.contains("DUMP") || msg.contains("wrong"),
                "expected clear NEXD error, got: {msg}"
            );
        }
        other => panic!("expected error, got {other:?}"),
    }
}

// ── ZRANDMEMBER WITHSCORES ────────────────────────────────────────────────────

#[tokio::test]
async fn zrandmember_withscores_flat_pairs() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"])).await;

    let r = run(&db, cmd(&["ZRANDMEMBER", "z", "2", "WITHSCORES"])).await;
    let parts = as_array(r);
    // Flat [member, score, member, score] — 4 elements for count=2.
    assert_eq!(
        parts.len(),
        4,
        "expected flat WITHSCORES pairs, got {parts:?}"
    );
    // Scores are bulk strings.
    for i in (1..parts.len()).step_by(2) {
        assert!(matches!(parts[i], Resp::BulkString(Some(_))));
    }

    // WITHSCORES without count is a syntax error.
    let err = run(&db, cmd(&["ZRANDMEMBER", "z", "WITHSCORES"])).await;
    assert!(matches!(err, Resp::Error(_)), "{err:?}");
}

// ── BZPOPMIN / BZPOPMAX ───────────────────────────────────────────────────────

#[tokio::test]
async fn bzpopmin_returns_immediately_when_nonempty() {
    let db = Db::default();
    let _ = run(&db, cmd(&["ZADD", "z", "1", "lo", "9", "hi"])).await;
    let r = run(&db, cmd(&["BZPOPMIN", "z", "1"])).await;
    let parts = as_array(r);
    assert_eq!(parts.len(), 3);
    assert_eq!(as_bulk(&parts[0]), b"z");
    assert_eq!(as_bulk(&parts[1]), b"lo");
}

#[tokio::test]
async fn bzpopmax_wakes_on_zadd() {
    let db = Db::default();

    let waiter_db = db.clone();
    let waiter = tokio::spawn(async move { run(&waiter_db, cmd(&["BZPOPMAX", "zq", "5"])).await });

    tokio::time::sleep(Duration::from_millis(80)).await;
    let _ = run(&db, cmd(&["ZADD", "zq", "4.5", "m"])).await;

    let resp = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("BZPOPMAX task must finish")
        .expect("BZPOPMAX task must not panic");
    let parts = as_array(resp);
    assert_eq!(as_bulk(&parts[0]), b"zq");
    assert_eq!(as_bulk(&parts[1]), b"m");
}

#[tokio::test]
async fn bzpopmin_timeout_returns_null() {
    let db = Db::default();
    let resp = tokio::time::timeout(
        Duration::from_secs(3),
        run(&db, cmd(&["BZPOPMIN", "empty", "0.2"])),
    )
    .await
    .expect("must finish");
    assert!(
        matches!(
            resp,
            Resp::Array(None) | Resp::Null | Resp::BulkString(None)
        ) || matches!(&resp, Resp::Array(Some(a)) if a.is_empty()),
        "empty BZPOPMIN on timeout must be null-ish, got {resp:?}"
    );
}

// ── SINTERCARD ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn sintercard_basic_and_limit() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SADD", "a", "1", "2", "3", "4"])).await;
    let _ = run(&db, cmd(&["SADD", "b", "2", "3", "5"])).await;

    let r = run(&db, cmd(&["SINTERCARD", "2", "a", "b"])).await;
    assert_eq!(as_int(&r), 2);

    let r = run(&db, cmd(&["SINTERCARD", "2", "a", "b", "LIMIT", "1"])).await;
    assert_eq!(as_int(&r), 1);

    // Missing key → 0.
    let r = run(&db, cmd(&["SINTERCARD", "2", "a", "nope"])).await;
    assert_eq!(as_int(&r), 0);
}

// ── BITFIELD_RO ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn bitfield_ro_get_ok_set_rejected() {
    let db = Db::default();
    let _ = run(&db, cmd(&["SET", "bf", "\x00"])).await;
    // Seed a value via writable BITFIELD.
    let _ = run(&db, cmd(&["BITFIELD", "bf", "SET", "u8", "0", "42"])).await;

    let r = run(&db, cmd(&["BITFIELD_RO", "bf", "GET", "u8", "0"])).await;
    let parts = as_array(r);
    assert_eq!(as_int(&parts[0]), 42);

    let err = run(&db, cmd(&["BITFIELD_RO", "bf", "SET", "u8", "0", "1"])).await;
    match err {
        Resp::Error(msg) => assert!(
            msg.to_uppercase().contains("GET") || msg.to_uppercase().contains("BITFIELD_RO"),
            "{msg}"
        ),
        other => panic!("expected rejection, got {other:?}"),
    }
}

// ── AOF PEL rewrite ───────────────────────────────────────────────────────────

#[tokio::test]
async fn aof_rewrite_preserves_stream_pel() {
    use nexrade_core::resp::RespParser;

    let db = Db::default();

    // Build a stream with a group, a delivered-but-unacked message, and a consumer.
    let _ = run(&db, cmd(&["XADD", "s", "1-0", "f", "v"])).await;
    let _ = run(&db, cmd(&["XGROUP", "CREATE", "s", "g", "0-0"])).await;
    // XREADGROUP delivers 1-0 into the PEL for consumer c.
    let _ = run(
        &db,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ]),
    )
    .await;

    // Confirm PEL is non-empty before rewrite.
    let pending_before = run(&db, cmd(&["XPENDING", "s", "g"])).await;
    match &pending_before {
        Resp::Array(Some(a)) => {
            assert!(!a.is_empty(), "XPENDING summary must be non-empty");
            assert!(as_int(&a[0]) >= 1, "expected ≥1 pending, got {a:?}");
        }
        other => panic!("expected XPENDING array, got {other:?}"),
    }

    // Rewrite AOF to a temp path.
    let tmp = std::env::temp_dir().join(format!(
        "nexrade_test_pel_rewrite_{}.aof",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&tmp);
    let dbs = db.store.snapshot_dbs();
    AofWriter::rewrite(&tmp, &dbs, &[]).expect("rewrite");

    let bytes = std::fs::read(&tmp).expect("read aof");
    let _ = std::fs::remove_file(&tmp);

    // The rewrite must emit XCLAIM (and ideally CREATECONSUMER) for the PEL.
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        text.contains("XCLAIM") || bytes.windows(6).any(|w| w == b"XCLAIM"),
        "AOF rewrite must emit XCLAIM to restore PEL"
    );

    // Replay into a fresh Db.
    let mut parser = RespParser::new();
    parser.feed(&bytes);
    let mut cmds: Vec<Vec<Resp>> = Vec::new();
    while let Some(resp) = parser.parse_one().expect("parse") {
        match resp {
            Resp::Array(Some(items)) => cmds.push(items),
            _ => panic!("expected array, got {resp:?}"),
        }
    }

    let db2 = Db::default();
    for c in &cmds {
        let r = nexrade_core::command::dispatch_with_addr(&db2, c.clone(), 0, None).await;
        assert!(
            !matches!(r, Resp::Error(_)),
            "replay of {:?} failed: {:?}",
            c.first(),
            r
        );
    }

    let pending_after = run(&db2, cmd(&["XPENDING", "s", "g"])).await;
    match pending_after {
        Resp::Array(Some(a)) => {
            assert!(
                as_int(&a[0]) >= 1,
                "PEL must survive AOF rewrite, got {a:?}"
            );
        }
        other => panic!("expected XPENDING array after replay, got {other:?}"),
    }
}
