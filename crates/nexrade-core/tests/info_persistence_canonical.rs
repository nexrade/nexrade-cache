//! 1.3.0 — C2 (`INFO persistence` consumes the canonical `HealthReport`) and
//! the `last_save_time` divergence that refactor exposed.
//!
//! Before this change `INFO persistence` did ten raw atomic loads while
//! `INFO health` two lines below called `health_report(db)`. The two could
//! disagree inside a single response. The clearest case:
//! `Stats::last_save_time` was written by `SAVE`/`BGSAVE`/auto-save/RDB-load,
//! while `HealthReport.persistence.last_save_time` read
//! `LifecycleState::last_save_unix` — a field **no save path ever wrote**. So
//! after a successful `SAVE`, `INFO persistence` showed a real timestamp and
//! `/healthz` showed `null`.

use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<&str>) -> Resp {
    dispatch_with_user(db, cmd(&args), 0, None, "default").await
}

fn info_field(info: &str, key: &str) -> Option<String> {
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{key}:")))
        .map(|v| v.trim().to_string())
}

async fn info(db: &Db, section: &str) -> String {
    match run(db, vec!["INFO", section]).await {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("INFO {section} returned {other:?}"),
    }
}

/// A temp dir removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p =
            std::env::temp_dir().join(format!("nexrade-c2-{}-{}-{}", tag, std::process::id(), n));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config_with_rdb(dir: &TempDir) -> ServerConfig {
    let mut c = ServerConfig::default();
    c.persistence.rdb_path = Some(dir.0.join("dump.rdb").to_string_lossy().into_owned());
    c.persistence.aof_path = None;
    c
}

// ─── The divergence itself ────────────────────────────────────────────────────

#[tokio::test]
async fn info_persistence_and_health_report_agree_on_last_save_time() {
    let dir = TempDir::new("save-agree");
    let db = Db::new(config_with_rdb(&dir));

    run(&db, vec!["SET", "k", "v"]).await;
    let saved = run(&db, vec!["SAVE"]).await;
    assert!(
        matches!(saved, Resp::SimpleString(ref s) if s == "OK"),
        "SAVE should succeed, got {saved:?}"
    );

    let info_ts: u64 = info_field(&info(&db, "persistence").await, "rdb_last_save_time")
        .expect("rdb_last_save_time present")
        .parse()
        .expect("numeric timestamp");
    let report_ts = nexrade_core::health::health_report(&db)
        .persistence
        .last_save_time;

    assert!(info_ts > 0, "a successful SAVE must record a timestamp");
    assert_eq!(
        report_ts,
        Some(info_ts),
        "INFO persistence and HealthReport must report the SAME save time; \
         pre-1.3.0 the report said None while INFO said {info_ts}"
    );
}

#[tokio::test]
async fn last_save_time_is_none_before_any_save() {
    let dir = TempDir::new("no-save");
    let db = Db::new(config_with_rdb(&dir));

    assert_eq!(
        nexrade_core::health::health_report(&db)
            .persistence
            .last_save_time,
        None,
        "no save yet, so no timestamp"
    );
    assert_eq!(
        info_field(&info(&db, "persistence").await, "rdb_last_save_time").as_deref(),
        Some("0"),
        "INFO renders 'never saved' as 0, unchanged from 1.2.x"
    );
}

// ─── Field-by-field: INFO persistence still matches the report ────────────────

#[tokio::test]
async fn info_persistence_fields_match_the_canonical_report() {
    let dir = TempDir::new("fields");
    let db = Db::new(config_with_rdb(&dir));
    run(&db, vec!["SET", "a", "1"]).await;
    run(&db, vec!["SAVE"]).await;
    run(&db, vec!["SET", "b", "2"]).await; // dirty again after the save

    let text = info(&db, "persistence").await;
    let p = nexrade_core::health::health_report(&db).persistence;

    assert_eq!(
        info_field(&text, "rdb_changes_since_last_save"),
        Some(p.dirty_keys.to_string())
    );
    assert_eq!(
        info_field(&text, "rdb_bgsave_in_progress"),
        Some((p.bgsave_in_progress as u8).to_string())
    );
    assert_eq!(
        info_field(&text, "rdb_last_save_time"),
        Some(p.last_save_time.unwrap_or(0).to_string())
    );
    assert_eq!(
        info_field(&text, "aof_enabled"),
        Some((p.aof_enabled as u8).to_string())
    );
    assert_eq!(
        info_field(&text, "aof_write_failed"),
        Some((p.aof_write_failed as u8).to_string())
    );
    assert_eq!(
        info_field(&text, "aof_last_write_error_time"),
        Some(p.aof_failure_time.unwrap_or(0).to_string())
    );
}

#[tokio::test]
async fn info_persistence_keeps_its_1_2_x_field_set() {
    // C2 is a source refactor, not an output change. Every key that shipped in
    // 1.2.x must still be present with the same name and order.
    let dir = TempDir::new("shape");
    let db = Db::new(config_with_rdb(&dir));
    let text = info(&db, "persistence").await;

    let expected = [
        "loading",
        "rdb_changes_since_last_save",
        "rdb_bgsave_in_progress",
        "rdb_last_save_time",
        "rdb_last_bgsave_status",
        "rdb_last_cow_size",
        "aof_enabled",
        "aof_rewrite_in_progress",
        "aof_last_bgrewrite_status",
        "aof_last_write_status",
        "aof_write_failed",
        "aof_last_write_error_time",
    ];
    assert!(text.starts_with("# Persistence\r\n"));
    let mut last = 0usize;
    for key in expected {
        let at = text
            .find(&format!("{key}:"))
            .unwrap_or_else(|| panic!("missing INFO persistence field {key}\n{text}"));
        assert!(at >= last, "field {key} is out of its historical order");
        last = at;
    }
}

#[tokio::test]
async fn aof_last_write_status_is_err_when_aof_is_disabled() {
    // Preserved quirk: with AOF off the field reads `err`, matching 1.2.x.
    let dir = TempDir::new("aof-off");
    let db = Db::new(config_with_rdb(&dir));
    let text = info(&db, "persistence").await;
    assert_eq!(
        info_field(&text, "aof_last_write_status").as_deref(),
        Some("err"),
        "AOF disabled reports err, unchanged"
    );
    assert_eq!(info_field(&text, "aof_enabled").as_deref(), Some("0"));
}

#[tokio::test]
async fn an_aof_failure_message_reaches_info_persistence() {
    let dir = TempDir::new("aof-fail");
    let db = Db::new(config_with_rdb(&dir));
    db.fail_aof("append", "no space left on device");

    let text = info(&db, "persistence").await;
    assert_eq!(info_field(&text, "aof_write_failed").as_deref(), Some("1"));
    let err = info_field(&text, "aof_last_write_error").expect("error line present");
    assert!(
        err.contains("no space left on device"),
        "INFO must carry the bounded cause, got: {err}"
    );
    // Same message the canonical report serves, not a second copy.
    assert_eq!(
        nexrade_core::health::health_report(&db)
            .persistence
            .aof_failure_message,
        Some(err)
    );
    assert_ne!(
        info_field(&text, "aof_last_write_error_time").as_deref(),
        Some("0"),
        "failure time is stamped alongside the message"
    );
}

#[tokio::test]
async fn info_all_is_internally_consistent() {
    // The original C2 complaint: `INFO` (all sections) could contradict itself
    // because `# Persistence` and `# Health` read different sources.
    let dir = TempDir::new("info-all");
    let db = Db::new(config_with_rdb(&dir));
    db.lifecycle()
        .set_phase(nexrade_core::health::HealthPhase::Ready);
    run(&db, vec!["SET", "k", "v"]).await;
    run(&db, vec!["SAVE"]).await;
    db.fail_aof("fsync", "input/output error");

    let text = info(&db, "all").await;
    // `aof_write_failed` (# Persistence) and the AofFailed reason (# Health)
    // must not disagree about whether AOF is broken.
    assert_eq!(info_field(&text, "aof_write_failed").as_deref(), Some("1"));
    assert_eq!(
        info_field(&text, "ready").as_deref(),
        Some("0"),
        "a latched AOF failure keeps readiness off in the same response"
    );
}
