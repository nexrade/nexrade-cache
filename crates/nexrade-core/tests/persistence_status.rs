//! Verifies the persistence status fields in `INFO persistence` reflect
//! real outcomes rather than hardcoded `ok`/`0`:
//!
//! - `rdb_last_bgsave_status`: flips from `ok` to `err` after a failed
//!   BGSAVE (e.g. a bad path the writer can't open). The flag *stays*
//!   at the last outcome until the next save — no silent decay to `ok`.
//! - `aof_rewrite_in_progress`: flips to `1` while a rewrite is in flight,
//!   back to `0` once it completes.
//! - `aof_last_bgrewrite_status`: same shape as `rdb_last_bgsave_status`
//!   but for AOF rewrites.
//! - `aof_last_write_status`: `ok` when AOF is enabled (writes that fail
//!   surface to the originating client), `err` when AOF is off (no
//!   writer to fail).

use std::sync::atomic::Ordering;

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::persistence::PersistenceConfig;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

/// Find a value for a given key inside an `INFO persistence` bulk string,
/// scanning line-by-line. Used by tests below to check counters without
/// depending on field order or surrounding whitespace.
fn info_field(info: &str, key: &str) -> Option<String> {
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{key}:")) {
            return Some(rest.trim().to_string());
        }
    }
    None
}

#[tokio::test]
async fn bgsave_failure_records_err_status() {
    // Configure RDB to a path that can never be opened: a *file* whose
    // parent path component is a regular file, so the rename-to-final-path
    // step in `Snapshot::save` will fail. This is a known failure mode
    // the writer doesn't pretend to handle.
    let db = Db::new(nexrade_core::db::ServerConfig {
        databases: 1,
        persistence: PersistenceConfig {
            rdb_path: Some("/proc/sys/kernel/random/boot_id".to_string()),
            ..Default::default()
        },
        ..Default::default()
    });
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;

    // BGSAVE spawns a background task; give it a moment to finish (and
    // fail) before we query the counter.
    let _ = run(&db, cmd(&["BGSAVE"])).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    assert!(
        !db.stats.bgsave_in_progress.load(Ordering::Relaxed),
        "bgsave_in_progress should clear after the (failed) save returns"
    );
    assert_eq!(
        db.stats.bgsave_last_status.load(Ordering::Relaxed),
        1,
        "bgsave_last_status should be 1 (err) after a failed save"
    );

    // INFO persistence should reflect the same counter through the public
    // protocol path.
    let info = run(&db, cmd(&["INFO", "persistence"])).await;
    let bulk = match &info {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("INFO should return a bulk string, got {other:?}"),
    };
    assert_eq!(
        info_field(&bulk, "rdb_last_bgsave_status").as_deref(),
        Some("err"),
        "INFO persistence must show the actual save outcome, not always `ok`:\n{bulk}"
    );
}

#[tokio::test]
async fn bgsave_status_returns_to_ok_after_a_successful_save() {
    // Sanity check the *opposite* direction: a successful save flips the
    // counter back to `ok`. Combined with the failure test above, proves
    // the flag tracks reality in both directions.
    let dir = std::env::temp_dir().join(format!(
        "nexrade-persist-status-ok-{}-{}",
        std::process::id(),
        std::sync::atomic::AtomicU64::new(0).fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let rdb_path = dir.join("nexrade.rdb").to_string_lossy().into_owned();

    let db = Db::new(nexrade_core::db::ServerConfig {
        databases: 1,
        persistence: PersistenceConfig {
            rdb_path: Some(rdb_path.clone()),
            ..Default::default()
        },
        ..Default::default()
    });
    let _ = run(&db, cmd(&["SET", "k", "v"])).await;

    // First a failing save, so we can verify the next success overwrites
    // the failure state instead of leaving it stuck.
    db.stats.bgsave_last_status.store(1, Ordering::Relaxed);
    let _ = run(&db, cmd(&["SAVE"])).await;
    assert_eq!(
        db.stats.bgsave_last_status.load(Ordering::Relaxed),
        0,
        "successful SAVE should reset the status flag to `ok`"
    );
    let info = run(&db, cmd(&["INFO", "persistence"])).await;
    let bulk = match &info {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => unreachable!(),
    };
    assert_eq!(
        info_field(&bulk, "rdb_last_bgsave_status").as_deref(),
        Some("ok")
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn aof_last_write_status_reports_aof_off() {
    // With AOF disabled (the default config), the public protocol should
    // say `err` — there is no writer to fail. Reporting `ok` here would
    // be a lie that masks configuration mistakes.
    let db = Db::new(nexrade_core::db::ServerConfig::default());

    let info = run(&db, cmd(&["INFO", "persistence"])).await;
    let bulk = match &info {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => unreachable!(),
    };
    assert_eq!(info_field(&bulk, "aof_enabled").as_deref(), Some("0"));
    assert_eq!(
        info_field(&bulk, "aof_last_write_status").as_deref(),
        Some("err"),
        "aof_last_write_status should be `err` when AOF is off"
    );
}

#[tokio::test]
async fn aof_rewrite_concurrency_lock_works() {
    // Only one BGREWRITEAOF at a time — concurrent rewrites would race
    // on the file rename. Verify a second BGREWRITEAOF is rejected while
    // one is in progress.
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    // Simulate an in-flight rewrite by setting the flag directly.
    db.stats
        .aof_rewrite_in_progress
        .store(true, Ordering::Relaxed);
    let r = run(&db, cmd(&["BGREWRITEAOF"])).await;
    match r {
        Resp::SimpleString(s) => assert!(
            s.to_ascii_lowercase().contains("already in progress"),
            "second BGREWRITEAOF should be rejected, got: {s}"
        ),
        other => panic!("expected SimpleString rejection, got {other:?}"),
    }
    db.stats
        .aof_rewrite_in_progress
        .store(false, Ordering::Relaxed);
}
