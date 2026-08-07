//! Health and readiness reporting for nexrade-cache.
//!
//! `health_report` is the single source of truth for `/healthz`,
//! `/readyz`, `INFO health`, and Prometheus operational gauges. Lifecycle
//! state is owned by `Db`, thresholds are owned by `HealthConfig`,
//! and active-operation state is derived from live persistence flags.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

use serde::{Deserialize, Serialize};

use crate::db::Db;
#[cfg(not(target_arch = "wasm32"))]
use crate::replication::ReplicationRole;

/// Lifecycle phase. Stored in `Db`; transitions are explicit and serialised
/// by the listener.
///
/// The forward order is `Starting → Recovering → Ready → Draining`, with
/// `Failed` reachable from any phase and terminal. See
/// [`LifecycleState::transition`] for the enforcement rules — the discriminants
/// below double as the rank used for that check, so their relative order is
/// load-bearing and must not be reshuffled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum HealthPhase {
    /// Process is up; persistence recovery has not been entered yet. This is
    /// the state a `Db` is born in, before the listener starts reading the
    /// RDB/AOF. Distinct from `Recovering` so an operator watching `/readyz`
    /// can tell "not started working yet" from "actively replaying".
    Starting = 0,
    /// Persistence recovery is in progress and has not finished.
    Recovering = 1,
    /// Persistence recovery finished; serving ready.
    Ready = 2,
    /// Shutdown initiated; still alive but not ready.
    Draining = 3,
    /// Startup failed; not alive. Terminal.
    Failed = 4,
}

impl HealthPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            HealthPhase::Starting => "starting",
            HealthPhase::Recovering => "recovering",
            HealthPhase::Ready => "ready",
            HealthPhase::Draining => "draining",
            HealthPhase::Failed => "failed",
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => HealthPhase::Starting,
            1 => HealthPhase::Recovering,
            2 => HealthPhase::Ready,
            3 => HealthPhase::Draining,
            // Any unexpected encoding is reported as `Failed` rather than
            // silently downgraded to a healthy-looking phase: an unreadable
            // lifecycle byte must never advertise readiness.
            _ => HealthPhase::Failed,
        }
    }
}

/// Readiness reason codes for machine-readable diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessReason {
    /// Persistence is currently quiescing for snapshot/rewrite.
    PersistenceQuiescing,
    /// AOF is in a failed state (append/fsync/reopen failure).
    AofFailed,
    /// Configured RDB path is invalid or last save failed.
    RdbUnavailable,
    /// Configured AOF path is invalid or writer unavailable.
    AofUnavailable,
    /// Snapshot age exceeds the configured maximum.
    SnapshotTooOld,
    /// Replica primary link is down.
    ReplicaLinkDown,
    /// Replication lag exceeds the configured maximum.
    ReplicationLagExceeded,
}

/// Configuration for the operations HTTP surface (health and, optionally,
/// metrics). Default is disabled so upgrading within 1.x never opens a new
/// port.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthConfig {
    /// Enable the health HTTP surface.
    pub enabled: bool,
    /// Bind address for the health HTTP listener.
    pub bind: String,
    /// Port for the health HTTP listener.
    pub port: u16,
    /// Maximum allowed snapshot age before instance becomes not ready.
    /// If `None`, snapshot age is not checked for readiness.
    pub max_snapshot_age_secs: Option<u64>,
    /// Maximum replication lag before instance becomes not ready.
    /// If `None`, replication lag is not checked for readiness.
    pub max_replication_lag_secs: Option<u64>,
    /// Serve the **full** `HealthReport` on `/healthz` and `/readyz` instead of
    /// the minimal probe shape (S3, 1.3.0).
    ///
    /// `false` (the default) restricts the unauthenticated response to
    /// liveness, readiness, phase, and machine-readable reason **codes**.
    /// `true` restores the pre-1.3.0 body, which includes
    /// `rdb_configured_path`, `aof_configured_path`, and the AOF failure
    /// message — filesystem layout and internal error text served to any
    /// client that can reach the port, with no authentication anywhere on this
    /// surface.
    ///
    /// Only enable this when the listener is genuinely reachable solely by
    /// trusted collectors. The full detail is always available unauthenticated
    /// via `INFO persistence` / `INFO health` on the RESP port, which at least
    /// honours `requirepass`.
    #[serde(default)]
    pub expose_details: bool,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1".to_string(),
            port: 9090,
            max_snapshot_age_secs: Some(3600),
            max_replication_lag_secs: Some(10),
            // Secure by default: an operator who moves `bind` off loopback
            // should not silently start publishing paths and error text.
            expose_details: false,
        }
    }
}

/// The unauthenticated probe body served on `/healthz` and `/readyz` when
/// `health.expose_details` is false (the default).
///
/// Carries exactly what an orchestrator needs to make a routing decision:
/// liveness, readiness, the lifecycle phase, and the machine-readable reason
/// codes. Deliberately omits every free-text and path-bearing field — the
/// reason *codes* are a closed enum, so they describe *why* an instance is
/// unready without disclosing a filesystem layout or an I/O error string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeReport {
    pub live: bool,
    pub ready: bool,
    pub phase: HealthPhase,
    /// Reason codes only — no messages.
    pub reasons: Vec<ReadinessReason>,
}

impl From<&HealthReport> for ProbeReport {
    fn from(r: &HealthReport) -> Self {
        Self {
            live: r.live,
            ready: r.ready,
            phase: r.phase,
            reasons: r.reasons.iter().map(|d| d.code).collect(),
        }
    }
}

/// Active background persistence job derived from live `Stats` flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveBackgroundJob {
    None,
    Bgsave,
    AofRewrite,
}

/// Machine-readable readiness reason entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessReasonDetail {
    pub code: ReadinessReason,
    pub message: String,
}

/// Complete health report. Single source for HTTP, `INFO`, and metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// Process liveness. True through Recovering, Ready, and Draining; only
    /// false in `Failed` or before `Db::new`.
    pub live: bool,
    /// Readiness for traffic. True only in `Ready` with no blocking reason.
    pub ready: bool,
    /// Current lifecycle phase.
    pub phase: HealthPhase,
    /// Ordered readiness reasons (empty when ready).
    pub reasons: Vec<ReadinessReasonDetail>,
    /// Persistence configuration and live status.
    pub persistence: PersistenceHealth,
    /// Replication configuration and live status.
    pub replication: ReplicationHealth,
    /// Active background persistence job.
    pub active_background_job: ActiveBackgroundJob,
}

impl Default for HealthReport {
    fn default() -> Self {
        Self {
            live: false,
            ready: false,
            phase: HealthPhase::Recovering,
            reasons: vec![ReadinessReasonDetail {
                code: ReadinessReason::PersistenceQuiescing,
                message: "startup recovery in progress".to_string(),
            }],
            persistence: PersistenceHealth::default(),
            replication: ReplicationHealth::default(),
            active_background_job: ActiveBackgroundJob::None,
        }
    }
}

/// Persistence health snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceHealth {
    /// Whether RDB is configured.
    pub rdb_configured: bool,
    /// Configured RDB path.
    pub rdb_configured_path: Option<String>,
    /// Whether AOF is configured.
    pub aof_configured: bool,
    /// Configured AOF path.
    pub aof_configured_path: Option<String>,
    /// Configured AOF sync policy.
    pub aof_sync_policy: String,
    /// Configured RDB save rules.
    pub rdb_save_rules: Vec<(u64, usize)>,
    /// Effective persistence mode.
    pub persistence_mode: String,
    /// Loaded snapshot creation time in unix seconds; `None` if no snapshot
    /// was loaded. The listener stores this once and never overwrites it.
    pub snapshot_created_at: Option<u64>,
    /// Seconds since the loaded snapshot was taken; `None` if no snapshot.
    pub snapshot_age_seconds: Option<u64>,
    /// Active background job derived from `Stats` flags.
    pub active_background_job: ActiveBackgroundJob,
    /// AOF failure latch state.
    pub aof_write_failed: bool,
    /// Timestamp of last AOF failure (unix seconds).
    pub aof_failure_time: Option<u64>,
    /// Bounded AOF failure message.
    pub aof_failure_message: Option<String>,
    /// Last successful RDB save timestamp (unix seconds).
    pub last_save_time: Option<u64>,
    /// Last RDB save status: 0 = ok, 1 = error.
    pub last_save_status: u8,
    /// Last AOF rewrite timestamp (unix seconds).
    pub last_rewrite_time: Option<u64>,
    /// Last AOF rewrite status: 0 = ok, 1 = error.
    pub last_rewrite_status: u8,
    /// Last AOF rewrite error message.
    pub last_rewrite_error: Option<String>,
    /// RDB save currently in progress.
    pub bgsave_in_progress: bool,
    /// AOF rewrite currently in progress.
    pub aof_rewrite_in_progress: bool,
    /// Whether the AOF writer is live (opened successfully and not disabled).
    /// Distinct from `aof_configured`, which only means a path is set: a
    /// configured-but-unopenable AOF is `configured && !enabled`.
    ///
    /// Added in 1.3.0 so `INFO persistence` can be driven from this report
    /// (`aof_enabled`, and the `aof_last_write_status` derivation that depends
    /// on it) instead of reading `Stats` directly.
    pub aof_enabled: bool,
    /// Key mutations since the last successful save (`rdb_changes_since_last_save`).
    pub dirty_keys: u64,
    /// Last AOF write status: 0 = ok, 1 = error.
    pub aof_last_write_status: u8,
}

impl Default for PersistenceHealth {
    fn default() -> Self {
        Self {
            rdb_configured: false,
            rdb_configured_path: None,
            aof_configured: false,
            aof_configured_path: None,
            aof_sync_policy: "everysec".to_string(),
            rdb_save_rules: Vec::new(),
            persistence_mode: "ephemeral".to_string(),
            snapshot_created_at: None,
            snapshot_age_seconds: None,
            active_background_job: ActiveBackgroundJob::None,
            aof_write_failed: false,
            aof_failure_time: None,
            aof_failure_message: None,
            last_save_time: None,
            last_save_status: 0,
            last_rewrite_time: None,
            last_rewrite_status: 0,
            last_rewrite_error: None,
            bgsave_in_progress: false,
            aof_rewrite_in_progress: false,
            aof_enabled: false,
            dirty_keys: 0,
            aof_last_write_status: 0,
        }
    }
}

/// Replication health snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationHealth {
    /// Current role (`primary` or `replica`).
    pub role: String,
    /// Primary link status (`up`/`down`/`unconfigured`).
    pub primary_link_status: String,
    /// Connected replica count (primary only).
    pub connected_replicas: u64,
    /// Maximum replica ACK age in seconds (primary only).
    pub max_replica_ack_age_seconds: Option<u64>,
    /// Maximum replica offset lag (primary only).
    pub max_replica_offset_lag: u64,
    /// Seconds since last primary stream batch (replica only).
    pub seconds_since_primary_stream: Option<u64>,
}

impl Default for ReplicationHealth {
    fn default() -> Self {
        Self {
            role: "primary".to_string(),
            primary_link_status: "unconfigured".to_string(),
            connected_replicas: 0,
            max_replica_ack_age_seconds: None,
            max_replica_offset_lag: 0,
            seconds_since_primary_stream: None,
        }
    }
}

/// A latched AOF failure: the timestamp and the bounded message are published
/// together so no reader can see one without the other.
///
/// Before 1.3.0 these were an `AtomicU64` and a separate `Mutex<Option<String>>`.
/// A reader landing between the two stores saw `aof_write_failed = true` with
/// `aof_failure_message = None` and reported the generic fallback
/// `"AOF persistence failed"` instead of the real cause.
#[derive(Debug, Clone)]
pub struct AofFailure {
    /// Unix seconds at which the failure was latched.
    pub at_unix: u64,
    /// Bounded (≤512 byte) failure message from the writer.
    pub message: String,
}

/// Lifecycle state storage on `Db`. Single atomic byte, one writer (the
/// listener) at a time.
pub struct LifecycleState {
    phase: AtomicU8,
    snapshot_loaded_at: AtomicU64,
    last_save_unix: AtomicU64,
    /// `Some` once an AOF failure has been latched. Timestamp and message live
    /// behind one lock so the pair is always observed consistently.
    aof_failure: parking_lot::Mutex<Option<AofFailure>>,
    aof_failure_count: AtomicU64,
}

impl LifecycleState {
    pub fn new() -> Self {
        Self {
            // `Starting`: the process is up but the listener has not begun
            // persistence recovery. `live` is already true (only `Failed`
            // clears it) and `ready` is false, so the HTTP status codes are
            // identical to the historical `Recovering` start — only the
            // reported phase string differs.
            phase: AtomicU8::new(HealthPhase::Starting as u8),
            snapshot_loaded_at: AtomicU64::new(0),
            last_save_unix: AtomicU64::new(0),
            aof_failure: parking_lot::Mutex::new(None),
            aof_failure_count: AtomicU64::new(0),
        }
    }

    /// Attempt a lifecycle phase transition, enforcing the documented DAG.
    ///
    /// Rules, in order:
    ///
    /// 1. `Failed` is **terminal** — once entered, nothing moves out of it. A
    ///    permanent startup failure must never be masked by a later `Ready`.
    /// 2. `Failed` is reachable from any non-terminal phase.
    /// 3. Otherwise the phase may only move **forward**
    ///    (`Starting → Recovering → Ready → Draining`), never backward.
    /// 4. A no-op transition to the current phase is allowed (idempotent
    ///    callers, e.g. a retried drain).
    ///
    /// Returns `true` if the phase changed, `false` if the transition was
    /// rejected or was a no-op. A rejected transition is logged at `warn`
    /// because it means a caller violated the lifecycle contract.
    ///
    /// Implemented as a CAS loop rather than a plain store so two threads
    /// racing (e.g. the SCM stop path and a startup failure) cannot interleave
    /// a read-decide-write and lose the terminal `Failed`.
    pub fn transition(&self, to: HealthPhase) -> bool {
        let target = to as u8;
        let mut current = self.phase.load(Ordering::Acquire);
        loop {
            let from = HealthPhase::from_u8(current);
            if from == to {
                return false;
            }
            let allowed = if from == HealthPhase::Failed {
                false
            } else {
                to == HealthPhase::Failed || target > current
            };
            if !allowed {
                tracing::warn!(
                    "rejected lifecycle transition {} -> {} (illegal); staying in {}",
                    from.as_str(),
                    to.as_str(),
                    from.as_str()
                );
                return false;
            }
            match self.phase.compare_exchange_weak(
                current,
                target,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                // Lost the race; re-evaluate against the phase that won so we
                // never overwrite a concurrently-set terminal Failed.
                Err(actual) => current = actual,
            }
        }
    }

    /// Set the lifecycle phase, enforcing the transition DAG.
    ///
    /// Retained as the listener-facing name; delegates to
    /// [`Self::transition`] and discards whether the move was applied. Prefer
    /// `transition` when the caller needs to know.
    pub fn set_phase(&self, p: HealthPhase) {
        let _ = self.transition(p);
    }

    pub fn phase(&self) -> HealthPhase {
        HealthPhase::from_u8(self.phase.load(Ordering::Acquire))
    }

    /// Record an AOF failure latch transition. First call stamps time and
    /// message; subsequent calls keep the original.
    ///
    /// The lock is taken *before* the latch counter is bumped and held across
    /// both, so a concurrent [`Self::aof_failure`] either sees no failure at
    /// all or sees the fully-populated record — never a latched flag with an
    /// absent message.
    pub fn record_aof_failure(&self, message: String) {
        let mut guard = self.aof_failure.lock();
        if self.aof_failure_count.fetch_add(1, Ordering::AcqRel) == 0 {
            *guard = Some(AofFailure {
                at_unix: crate::db::unix_secs(),
                message,
            });
        }
    }

    /// Snapshot the latched AOF failure, if any. Timestamp and message are
    /// consistent with each other by construction.
    pub fn aof_failure(&self) -> Option<AofFailure> {
        self.aof_failure.lock().clone()
    }

    pub fn aof_failure_message(&self) -> Option<String> {
        self.aof_failure.lock().as_ref().map(|f| f.message.clone())
    }

    pub fn aof_failure_unix(&self) -> u64 {
        self.aof_failure
            .lock()
            .as_ref()
            .map(|f| f.at_unix)
            .unwrap_or(0)
    }

    pub fn aof_failure_count(&self) -> u64 {
        self.aof_failure_count.load(Ordering::Acquire)
    }

    /// Record the actual loaded snapshot creation time, never overwriting
    /// with the current time. `0` means no snapshot was loaded.
    pub fn record_snapshot_loaded(&self, unix_seconds: u64) {
        if unix_seconds > 0 {
            self.snapshot_loaded_at
                .store(unix_seconds, Ordering::Release);
        }
    }

    pub fn snapshot_loaded_at(&self) -> u64 {
        self.snapshot_loaded_at.load(Ordering::Acquire)
    }

    /// Record a successful RDB save.
    ///
    /// **Deprecated in 1.3.0 and no longer consulted by [`health_report`].**
    /// It was never called by any save path — `SAVE`, `BGSAVE`, the auto-save
    /// tick, and RDB load all wrote `Stats::last_save_time` instead — so the
    /// health surfaces reported `last_save_time: null` while `INFO
    /// persistence` reported a real timestamp for the same instant.
    ///
    /// `Stats::last_save_time` is now the single source. This setter is
    /// retained so an out-of-tree caller keeps compiling, and it still updates
    /// the field, but nothing reads it.
    #[deprecated(
        since = "1.3.0",
        note = "save time is owned by `Stats::last_save_time`; this value is no longer read"
    )]
    pub fn record_save(&self, unix_seconds: u64) {
        self.last_save_unix.store(unix_seconds, Ordering::Release);
    }

    /// Previously the health surfaces' save timestamp. See [`Self::record_save`]
    /// — retained for source compatibility; always `0` in practice.
    #[deprecated(
        since = "1.3.0",
        note = "read `Stats::last_save_time` instead; this value is never written"
    )]
    pub fn last_save_unix(&self) -> u64 {
        self.last_save_unix.load(Ordering::Acquire)
    }
}

impl Default for LifecycleState {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the canonical health report from live `Db` state. Returns a fresh
/// snapshot on every call. The result is the single source of truth for
/// HTTP, `INFO health`, and Prometheus operational gauges.
#[cfg(not(target_arch = "wasm32"))]
pub fn health_report(db: &Db) -> HealthReport {
    let config = db.config.lock();
    let lifecycle = db.lifecycle();
    let stats = &db.stats;
    let now = crate::db::unix_secs();

    let phase = lifecycle.phase();
    let live = phase != HealthPhase::Failed;
    let mut ready = phase == HealthPhase::Ready;

    let mut reasons: Vec<ReadinessReasonDetail> = Vec::new();

    // Quiescing: persistence is currently waiting for active mutations.
    if db.persistence.is_quiescing() {
        ready = false;
        reasons.push(ReadinessReasonDetail {
            code: ReadinessReason::PersistenceQuiescing,
            message: "persistence is currently quiescing for snapshot/rewrite".to_string(),
        });
    }

    // AOF failure latch: report the bounded message and keep readiness off.
    //
    // Snapshot the failure record **once** so the message and timestamp in this
    // report come from the same observation. Reading them through the two
    // separate accessors would take the lock twice and could straddle a
    // concurrent latch, mixing a message from one failure with a timestamp
    // from another.
    let aof_failure = lifecycle.aof_failure();
    let aof_failure_msg = aof_failure.as_ref().map(|f| f.message.clone());
    if stats.aof_failed.load(Ordering::Acquire) || aof_failure_msg.is_some() {
        ready = false;
        reasons.push(ReadinessReasonDetail {
            code: ReadinessReason::AofFailed,
            message: aof_failure_msg
                .clone()
                .unwrap_or_else(|| "AOF persistence failed".to_string()),
        });
    }

    // Single source for the save timestamp: `Stats::last_save_time`, the same
    // atomic `INFO persistence` reports. Until 1.3.0 this read
    // `lifecycle.last_save_unix()`, which no save path ever wrote — so it was
    // permanently 0, `last_save_time` serialized as `null` while `INFO`
    // showed a real timestamp, and the `RdbUnavailable` gate below could
    // never fire because its `> 0` guard was never satisfied.
    let last_save_unix = stats.last_save_time.load(Ordering::Acquire);

    // RDB configured but most recent save failed with a recorded timestamp.
    if config.persistence.rdb_path.is_some()
        && stats.bgsave_last_status.load(Ordering::Acquire) != 0
        && last_save_unix > 0
    {
        ready = false;
        reasons.push(ReadinessReasonDetail {
            code: ReadinessReason::RdbUnavailable,
            message: "Configured RDB save failed".to_string(),
        });
    }

    // AOF configured but the writer never came up.
    if config.persistence.aof_path.is_some() && !stats.aof_enabled.load(Ordering::Acquire) {
        ready = false;
        reasons.push(ReadinessReasonDetail {
            code: ReadinessReason::AofUnavailable,
            message: "AOF configured but writer unavailable".to_string(),
        });
    }

    // Snapshot age threshold from the health config (single owner).
    let snap_age = {
        let loaded = lifecycle.snapshot_loaded_at();
        if loaded > 0 {
            Some(now.saturating_sub(loaded))
        } else {
            None
        }
    };
    if let (Some(age), Some(max)) = (snap_age, config.health.max_snapshot_age_secs) {
        if age > max {
            ready = false;
            reasons.push(ReadinessReasonDetail {
                code: ReadinessReason::SnapshotTooOld,
                message: format!("snapshot age {}s exceeds maximum {}", age, max),
            });
        }
    }

    // Active job derived from live flags.
    let active_job = if stats.bgsave_in_progress.load(Ordering::Acquire) {
        ActiveBackgroundJob::Bgsave
    } else if stats.aof_rewrite_in_progress.load(Ordering::Acquire) {
        ActiveBackgroundJob::AofRewrite
    } else {
        ActiveBackgroundJob::None
    };

    // Persistence snapshot.
    let last_save_time = Some(last_save_unix).filter(|t| *t > 0);
    let last_rewrite_time = stats.last_rewrite_time.load(Ordering::Acquire);
    let persistence_mode = match (
        config.persistence.rdb_path.is_some(),
        config.persistence.aof_path.is_some(),
    ) {
        (true, true) => "rdb+aof",
        (true, false) => "rdb-only",
        (false, true) => "aof-only",
        (false, false) => "ephemeral",
    }
    .to_string();
    let persistence = PersistenceHealth {
        rdb_configured: config.persistence.rdb_path.is_some(),
        rdb_configured_path: config.persistence.rdb_path.clone(),
        aof_configured: config.persistence.aof_path.is_some(),
        aof_configured_path: config.persistence.aof_path.clone(),
        aof_sync_policy: match config.persistence.aof_sync {
            crate::persistence::AofSync::Always => "always",
            crate::persistence::AofSync::EverySec => "everysec",
            crate::persistence::AofSync::No => "no",
        }
        .to_string(),
        rdb_save_rules: config.persistence.rdb_save_rules.clone(),
        persistence_mode,
        snapshot_created_at: {
            let v = lifecycle.snapshot_loaded_at();
            if v > 0 {
                Some(v)
            } else {
                None
            }
        },
        snapshot_age_seconds: snap_age,
        active_background_job: active_job,
        aof_write_failed: stats.aof_failed.load(Ordering::Acquire) || aof_failure_msg.is_some(),
        // From the same snapshot as the message above, so the pair is always
        // internally consistent.
        aof_failure_time: aof_failure.as_ref().map(|f| f.at_unix).filter(|t| *t > 0),
        aof_failure_message: aof_failure_msg,
        last_save_time,
        last_save_status: stats.bgsave_last_status.load(Ordering::Acquire),
        last_rewrite_time: {
            let t = last_rewrite_time;
            if t > 0 {
                Some(t)
            } else {
                None
            }
        },
        last_rewrite_status: stats.aof_rewrite_last_status.load(Ordering::Acquire),
        last_rewrite_error: if stats.aof_rewrite_last_status.load(Ordering::Acquire) != 0 {
            Some("AOF rewrite failed".to_string())
        } else {
            None
        },
        bgsave_in_progress: stats.bgsave_in_progress.load(Ordering::Acquire),
        aof_rewrite_in_progress: stats.aof_rewrite_in_progress.load(Ordering::Acquire),
        aof_enabled: stats.aof_enabled.load(Ordering::Acquire),
        dirty_keys: stats.dirty_keys.load(Ordering::Acquire),
        aof_last_write_status: stats.aof_last_write_status.load(Ordering::Acquire),
    };

    // Replication snapshot. Returns `true` if a blocking replication
    // condition was found so the caller can fold that into `ready`
    // — pushing a reason into the list is not enough; `/readyz`
    // must also flip to 503. Previously reasons were recorded but
    // `ready` stayed true, so a disconnected / laggy replica kept
    // returning HTTP 200 while reporting the reason in the body.
    let (replication, replication_blocks) = build_replication_health(&config, db, &mut reasons);
    if replication_blocks {
        ready = false;
    }

    HealthReport {
        live,
        ready,
        phase,
        reasons,
        persistence,
        replication,
        active_background_job: active_job,
    }
}

/// WASM builds have no persistence coordinator, replication state, or
/// lifecycle tracker on `Db` (all three are `#[cfg(not(wasm32))]`), so
/// there is nothing live to sample. Report the static default: not
/// ready, and `live` only in the sense that the module loaded.
#[cfg(target_arch = "wasm32")]
pub fn health_report(_db: &Db) -> HealthReport {
    HealthReport {
        live: true,
        ready: false,
        phase: HealthPhase::Recovering,
        reasons: vec![ReadinessReasonDetail {
            code: ReadinessReason::PersistenceQuiescing,
            message: "health reporting is not available in WASM mode".to_string(),
        }],
        persistence: PersistenceHealth::default(),
        replication: ReplicationHealth::default(),
        active_background_job: ActiveBackgroundJob::None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn build_replication_health(
    config: &crate::db::ServerConfig,
    db: &Db,
    reasons: &mut Vec<ReadinessReasonDetail>,
) -> (ReplicationHealth, bool) {
    let replication = &db.replication;
    let role = match replication.current_role() {
        ReplicationRole::Primary => "primary",
        ReplicationRole::Replica => "replica",
    }
    .to_string();

    let connected_replicas = replication.connected_replicas_count();
    let max_replica_ack_age_seconds =
        replication.max_replica_ack_age_seconds(config.health.max_replication_lag_secs);

    let mut blocks_ready = false;

    if matches!(replication.current_role(), ReplicationRole::Replica)
        && !replication.primary_link_up()
    {
        blocks_ready = true;
        reasons.push(ReadinessReasonDetail {
            code: ReadinessReason::ReplicaLinkDown,
            message: "replica primary link is down".to_string(),
        });
    }

    if let Some(max_age) = max_replica_ack_age_seconds {
        if matches!(replication.current_role(), ReplicationRole::Primary)
            && connected_replicas > 0
            && max_age >= config.health.max_replication_lag_secs.unwrap_or(u64::MAX)
        {
            blocks_ready = true;
            reasons.push(ReadinessReasonDetail {
                code: ReadinessReason::ReplicationLagExceeded,
                message: format!(
                    "max replica ACK age {}s exceeds maximum {}s",
                    max_age,
                    config.health.max_replication_lag_secs.unwrap_or(0)
                ),
            });
        }
    }

    let primary_link_status = if matches!(replication.current_role(), ReplicationRole::Replica) {
        if replication.primary_link_up() {
            "up".to_string()
        } else if replication.replica_of().is_some() {
            "down".to_string()
        } else {
            "unconfigured".to_string()
        }
    } else {
        "not_applicable".to_string()
    };

    let health = ReplicationHealth {
        role,
        primary_link_status,
        connected_replicas,
        max_replica_ack_age_seconds,
        max_replica_offset_lag: replication.max_replica_offset_lag(),
        seconds_since_primary_stream: None,
    };
    (health, blocks_ready)
}
