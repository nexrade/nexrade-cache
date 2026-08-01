//! 0.7.4 — LATENCY monitor + HISTOGRAM + SLOWLOG HELP + metrics buckets.
//!
//! Locks down:
//!   * LatencyMonitor unit behaviour (via public API)
//!   * LATENCY LATEST / HISTORY / HISTOGRAM / RESET / DOCTOR / HELP shapes
//!   * SLOWLOG HELP
//!   * multiple events co-exist; RESET event-scoped

use nexrade_core::command::dispatch;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch(db, args, 0).await
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

fn as_bulk(r: &Resp) -> &[u8] {
    match r {
        Resp::BulkString(Some(b)) => b.as_ref(),
        other => panic!("expected bulk, got {other:?}"),
    }
}

fn bulk_contains(r: &Resp, needle: &str) -> bool {
    match r {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b)
            .to_ascii_uppercase()
            .contains(&needle.to_ascii_uppercase()),
        _ => false,
    }
}

// ─── LatencyMonitor unit (via Db) ───────────────────────────────────────────

#[tokio::test]
async fn latency_latest_from_monitor() {
    let db = Db::default();
    db.latency.record("get", 100);
    db.latency.record("get", 250);
    db.latency.record("set", 50);

    let latest = as_array(run(&db, cmd(&["LATENCY", "LATEST"])).await);
    assert_eq!(latest.len(), 2, "get + set");

    // Find get row: [name, ts, latest, max]
    let mut saw_get = false;
    for row in &latest {
        let fields = as_array(row.clone());
        assert!(fields.len() >= 4);
        if as_bulk(&fields[0]) == b"get" {
            saw_get = true;
            assert_eq!(as_int(&fields[2]), 250); // latest
            assert_eq!(as_int(&fields[3]), 250); // all-time max
        }
    }
    assert!(saw_get);
}

#[tokio::test]
async fn latency_history_and_histogram() {
    let db = Db::default();
    for us in [1u64, 2, 3, 100, 200] {
        db.latency.record("get", us);
    }

    let hist = as_array(run(&db, cmd(&["LATENCY", "HISTORY", "get"])).await);
    assert_eq!(hist.len(), 5);

    let h = as_array(run(&db, cmd(&["LATENCY", "HISTOGRAM", "get"])).await);
    // Flat array: event-name, {calls, histogram_usec, buckets...}
    assert!(h.len() >= 2, "expected event + map, got {h:?}");
    assert_eq!(as_bulk(&h[0]), b"get");
    let inner = as_array(h[1].clone());
    // Find "calls" field
    let mut calls = None;
    let mut i = 0;
    while i + 1 < inner.len() {
        if as_bulk(&inner[i]) == b"calls" {
            calls = Some(as_int(&inner[i + 1]));
        }
        i += 2;
    }
    assert_eq!(calls, Some(5), "histogram calls must be 5");
}

#[tokio::test]
async fn latency_reset_scoped() {
    let db = Db::default();
    db.latency.record("get", 10);
    db.latency.record("set", 20);

    let n = as_int(&run(&db, cmd(&["LATENCY", "RESET", "get"])).await);
    assert_eq!(n, 1);
    assert!(db.latency.history("get").is_empty());
    assert!(!db.latency.history("set").is_empty());

    let n = as_int(&run(&db, cmd(&["LATENCY", "RESET"])).await);
    assert_eq!(n, 1);
    assert_eq!(db.latency.event_count(), 0);
}

#[tokio::test]
async fn latency_doctor_reports_samples() {
    let db = Db::default();
    // Empty → "no samples" wording.
    let r = run(&db, cmd(&["LATENCY", "DOCTOR"])).await;
    assert!(bulk_contains(&r, "No samples") || bulk_contains(&r, "no samples"));

    db.latency.record("get", 50_000); // 50 ms
    let r = run(&db, cmd(&["LATENCY", "DOCTOR"])).await;
    assert!(
        bulk_contains(&r, "doctor") || bulk_contains(&r, "samples"),
        "doctor must mention samples, got {:?}",
        r
    );
}

#[tokio::test]
async fn latency_help_lists_histogram() {
    let db = Db::default();
    let help = as_array(run(&db, cmd(&["LATENCY", "HELP"])).await);
    let text: String = help
        .iter()
        .filter_map(|r| match r {
            Resp::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
        .to_ascii_uppercase();
    for needle in ["LATEST", "HISTORY", "HISTOGRAM", "RESET", "DOCTOR", "HELP"] {
        assert!(text.contains(needle), "HELP missing {needle}: {text}");
    }
}

// ─── SLOWLOG HELP ───────────────────────────────────────────────────────────

#[tokio::test]
async fn slowlog_help_and_unknown() {
    let db = Db::default();
    let help = as_array(run(&db, cmd(&["SLOWLOG", "HELP"])).await);
    assert!(!help.is_empty());
    let text: String = help
        .iter()
        .filter_map(|r| match r {
            Resp::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
        .to_ascii_uppercase();
    for needle in ["GET", "LEN", "RESET", "HELP"] {
        assert!(text.contains(needle), "SLOWLOG HELP missing {needle}");
    }

    // Unknown subcommand is an error (was empty array before 0.7.4).
    let r = run(&db, cmd(&["SLOWLOG", "NOPE"])).await;
    assert!(
        matches!(r, Resp::Error(_)),
        "unknown sub must error, got {r:?}"
    );
}

// ─── Monitor unit tests re-exported via lib ─────────────────────────────────

#[test]
fn latency_monitor_unit_tests_pass() {
    // The unit tests live in latency.rs and run under --lib; this is a
    // smoke that the module is reachable.
    let m = nexrade_core::latency::LatencyMonitor::new();
    m.record("ping", 1);
    assert_eq!(m.event_count(), 1);
    assert_eq!(m.total_samples(), 1);
}
