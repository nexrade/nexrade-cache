//! 1.3.0 — lifecycle phase DAG enforcement (F3/C3) and atomic AOF-failure
//! publication (F6).
//!
//! Both were latent in 1.2.x: reachable only through orderings the shipped
//! callers happened not to produce. These tests pin the invariants so a future
//! caller cannot reintroduce them silently.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use nexrade_core::db::Db;
use nexrade_core::health::{HealthPhase, LifecycleState};

// ─── F3/C3: phase transitions are one-way; Failed is terminal ─────────────────

#[test]
fn phase_starts_at_starting() {
    let l = LifecycleState::new();
    assert_eq!(l.phase(), HealthPhase::Starting);
}

#[test]
fn forward_transitions_are_allowed_in_order() {
    let l = LifecycleState::new();
    assert!(l.transition(HealthPhase::Recovering));
    assert!(l.transition(HealthPhase::Ready));
    assert!(l.transition(HealthPhase::Draining));
    assert_eq!(l.phase(), HealthPhase::Draining);
}

#[test]
fn forward_transitions_may_skip_phases() {
    // The listener sets Recovering then Ready, but a caller that jumps
    // straight to Ready is still moving forward and must be accepted.
    let l = LifecycleState::new();
    assert!(l.transition(HealthPhase::Ready));
    assert_eq!(l.phase(), HealthPhase::Ready);
}

#[test]
fn backward_transitions_are_rejected() {
    let l = LifecycleState::new();
    l.transition(HealthPhase::Ready);

    assert!(
        !l.transition(HealthPhase::Recovering),
        "Ready -> Recovering must be rejected"
    );
    assert_eq!(l.phase(), HealthPhase::Ready, "phase is unchanged");

    l.transition(HealthPhase::Draining);
    assert!(
        !l.transition(HealthPhase::Ready),
        "Draining -> Ready must be rejected: a draining instance must not \
         re-advertise readiness to a load balancer"
    );
    assert_eq!(l.phase(), HealthPhase::Draining);
}

#[test]
fn failed_is_reachable_from_every_non_terminal_phase() {
    for start in [
        HealthPhase::Starting,
        HealthPhase::Recovering,
        HealthPhase::Ready,
        HealthPhase::Draining,
    ] {
        let l = LifecycleState::new();
        if start != HealthPhase::Starting {
            l.transition(start);
        }
        assert_eq!(l.phase(), start);
        assert!(
            l.transition(HealthPhase::Failed),
            "{:?} -> Failed must be allowed",
            start
        );
        assert_eq!(l.phase(), HealthPhase::Failed);
    }
}

#[test]
fn failed_is_terminal() {
    // The whole point of F3: a permanent startup failure must never be
    // masked by a later transition. Pre-1.3.0 `set_phase` was an
    // unconditional store, so every one of these silently succeeded.
    for attempt in [
        HealthPhase::Starting,
        HealthPhase::Recovering,
        HealthPhase::Ready,
        HealthPhase::Draining,
    ] {
        let l = LifecycleState::new();
        l.transition(HealthPhase::Failed);
        assert!(
            !l.transition(attempt),
            "Failed -> {:?} must be rejected",
            attempt
        );
        assert_eq!(l.phase(), HealthPhase::Failed);
    }
}

#[test]
fn transition_to_current_phase_is_a_noop() {
    let l = LifecycleState::new();
    l.transition(HealthPhase::Ready);
    assert!(
        !l.transition(HealthPhase::Ready),
        "a repeated transition reports no change"
    );
    assert_eq!(l.phase(), HealthPhase::Ready);
}

#[test]
fn concurrent_failed_wins_over_racing_ready() {
    // Models the real race: a startup failure and the SCM stop path (or a
    // retried readiness flip) landing at the same time. Whatever the
    // interleaving, Failed must not be lost — a CAS loop guarantees this
    // where a plain load-decide-store would not.
    for _ in 0..200 {
        let l = Arc::new(LifecycleState::new());
        let a = Arc::clone(&l);
        let b = Arc::clone(&l);

        let h1 = std::thread::spawn(move || a.transition(HealthPhase::Failed));
        let h2 = std::thread::spawn(move || b.transition(HealthPhase::Ready));
        let _ = h1.join();
        let _ = h2.join();

        // Ready may or may not have landed first, but Failed is terminal, so
        // the end state must be Failed either way.
        assert_eq!(
            l.phase(),
            HealthPhase::Failed,
            "Failed must survive a concurrent Ready"
        );
    }
}

#[test]
fn set_phase_still_enforces_the_dag() {
    // `set_phase` is the listener-facing name and stays available; it must not
    // be an escape hatch around `transition`.
    let l = LifecycleState::new();
    l.set_phase(HealthPhase::Failed);
    l.set_phase(HealthPhase::Ready);
    assert_eq!(l.phase(), HealthPhase::Failed);
}

#[test]
fn phase_strings_are_stable() {
    // These strings are part of the `/healthz` JSON and `INFO health` output.
    assert_eq!(HealthPhase::Starting.as_str(), "starting");
    assert_eq!(HealthPhase::Recovering.as_str(), "recovering");
    assert_eq!(HealthPhase::Ready.as_str(), "ready");
    assert_eq!(HealthPhase::Draining.as_str(), "draining");
    assert_eq!(HealthPhase::Failed.as_str(), "failed");
}

// ─── F6: an AOF failure is published atomically ───────────────────────────────

#[test]
fn aof_failure_timestamp_and_message_are_published_together() {
    let l = LifecycleState::new();
    l.record_aof_failure("append: disk full".to_string());

    let f = l.aof_failure().expect("failure recorded");
    assert_eq!(f.message, "append: disk full");
    assert!(f.at_unix > 0, "timestamp stamped alongside the message");
    // The legacy accessors must agree with the combined record.
    assert_eq!(
        l.aof_failure_message().as_deref(),
        Some("append: disk full")
    );
    assert_eq!(l.aof_failure_unix(), f.at_unix);
}

#[test]
fn aof_failure_keeps_the_first_message() {
    let l = LifecycleState::new();
    l.record_aof_failure("append: disk full".to_string());
    l.record_aof_failure("fsync: io error".to_string());

    assert_eq!(
        l.aof_failure_message().as_deref(),
        Some("append: disk full"),
        "the first (root-cause) failure is retained"
    );
    assert_eq!(l.aof_failure_count(), 2, "but every failure is counted");
}

#[tokio::test]
async fn fail_aof_never_exposes_a_latch_without_a_message() {
    // F6's actual symptom: a reader landing between the latch and the message
    // store saw `aof_failed == true` with no message, and `/readyz` reported
    // the generic "AOF persistence failed" instead of the real cause.
    //
    // `fail_aof` now publishes the diagnostics before Release-storing the
    // latch, so this invariant holds for every reader that Acquire-loads it.
    for _ in 0..300 {
        let db = Db::new(nexrade_core::db::ServerConfig::default());
        db.lifecycle().set_phase(HealthPhase::Ready);

        let writer = {
            let db = db.clone();
            std::thread::spawn(move || db.fail_aof("append", "disk full"))
        };

        // Spin until the latch is visible, then immediately demand the reason.
        loop {
            if db.stats.aof_failed.load(Ordering::Acquire) {
                let report = nexrade_core::health::health_report(&db);
                let reason = report
                    .reasons
                    .iter()
                    .find(|r| matches!(r.code, nexrade_core::health::ReadinessReason::AofFailed))
                    .expect("AofFailed reason present once the latch is set");
                assert!(
                    reason.message.contains("disk full"),
                    "latch was visible before its message: got {:?}",
                    reason.message
                );
                assert!(
                    report.persistence.aof_failure_message.is_some(),
                    "persistence snapshot must carry the message too"
                );
                break;
            }
            std::hint::spin_loop();
        }

        writer.join().unwrap();
    }
}

#[tokio::test]
async fn fail_aof_reports_the_bounded_message_in_the_health_report() {
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    db.lifecycle().set_phase(HealthPhase::Ready);
    db.fail_aof("fsync", "input/output error");

    let report = nexrade_core::health::health_report(&db);
    assert!(!report.ready, "an AOF failure keeps readiness off");
    assert!(report.persistence.aof_write_failed);
    let msg = report
        .persistence
        .aof_failure_message
        .expect("bounded message present");
    assert!(msg.contains("fsync"), "operation is included: {msg}");
    assert!(msg.contains("input/output error"), "cause included: {msg}");
}

#[tokio::test]
async fn fail_aof_truncates_a_huge_message() {
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    db.fail_aof("append", "x".repeat(10_000));

    let msg = db
        .lifecycle()
        .aof_failure_message()
        .expect("message recorded");
    assert!(
        msg.len() <= 512,
        "message must stay bounded, got {} bytes",
        msg.len()
    );
}
