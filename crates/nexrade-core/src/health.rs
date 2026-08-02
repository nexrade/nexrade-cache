//! Health and readiness reporting for nexrade-cache.
//!
//! `health_report` is the single source of truth for `/healthz`,
//! `/readyz`, `INFO health`, and Prometheus operational gauges. Lifecycle
//! state is owned by `Db`, thresholds are owned by `HealthConfig`,
//! and active-operation state is derived from live persistence flags.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

use serde::{Deserialize, Serialize};

use crate::db::Db;
use crate::persistence::PersistenceConfig;
#[cfg(not(target_arch = "wasm32"))]
use crate::replication::ReplicationRole;

/// Lifecycle phase. Stored in `Db`; transitions are explicit and serialised
/// by the listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthPhase {
    /// Process is up but persistence recovery has not finished.
    Recovering,
    /// Persistence recovery finished; serving ready.
    Ready,
    /// Shutdown initiated; still alive but not ready.
    Draining,
    /// Startup failed; not alive.
    Failed,
}

impl HealthPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            HealthPhase::Recovering => "recovering",
            HealthPhase::Ready => "ready",
            HealthPhase::Draining => "draining",
            HealthPhase::Failed => "failed",
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
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1".to_string(),
            port: 9090,
            max_snapshot_age_secs: Some(3600),
            max_replication_lag_secs: Some(10),
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

/// Lifecycle state storage on `Db`. Single atomic byte, one writer (the
/// listener) at a time.
pub struct LifecycleState {
    phase: AtomicU8,
    snapshot_loaded_at: AtomicU64,
    last_save_unix: AtomicU64,
    aof_failure_unix: AtomicU64,
    aof_failure_msg: parking_lot::Mutex<Option<String>>,
    aof_failure_count: AtomicU64,
}

impl LifecycleState {
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(0),
            snapshot_loaded_at: AtomicU64::new(0),
            last_save_unix: AtomicU64::new(0),
            aof_failure_unix: AtomicU64::new(0),
            aof_failure_msg: parking_lot::Mutex::new(None),
            aof_failure_count: AtomicU64::new(0),
        }
    }

    /// Set the lifecycle phase. `false` is returned unchanged; `true`
    /// transitions the instance out of readiness in the report.
    pub fn set_phase(&self, p: HealthPhase) {
        let v = match p {
            HealthPhase::Recovering => 0,
            HealthPhase::Ready => 1,
            HealthPhase::Draining => 2,
            HealthPhase::Failed => 3,
        };
        self.phase.store(v, Ordering::Release);
    }

    pub fn phase(&self) -> HealthPhase {
        match self.phase.load(Ordering::Acquire) {
            0 => HealthPhase::Recovering,
            1 => HealthPhase::Ready,
            2 => HealthPhase::Draining,
            3 => HealthPhase::Failed,
            _ => HealthPhase::Failed,
        }
    }

    /// Record an AOF failure latch transition. First call stamps time and
    /// message; subsequent calls keep the original.
    pub fn record_aof_failure(&self, message: String) {
        if self.aof_failure_count.fetch_add(1, Ordering::AcqRel) == 0 {
            self.aof_failure_unix
                .store(crate::db::unix_secs(), Ordering::Release);
            let mut g = self.aof_failure_msg.lock();
            *g = Some(message);
        }
    }

    pub fn aof_failure_message(&self) -> Option<String> {
        self.aof_failure_msg.lock().clone()
    }

    pub fn aof_failure_unix(&self) -> u64 {
        self.aof_failure_unix.load(Ordering::Acquire)
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

    pub fn record_save(&self, unix_seconds: u64) {
        self.last_save_unix.store(unix_seconds, Ordering::Release);
    }

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
    let aof_failure_msg = lifecycle.aof_failure_message();
    if stats.aof_failed.load(Ordering::Acquire) || aof_failure_msg.is_some() {
        ready = false;
        reasons.push(ReadinessReasonDetail {
            code: ReadinessReason::AofFailed,
            message: aof_failure_msg
                .clone()
                .unwrap_or_else(|| "AOF persistence failed".to_string()),
        });
    }

    // RDB configured but most recent save failed with a recorded timestamp.
    if config.persistence.rdb_path.is_some()
        && stats.bgsave_last_status.load(Ordering::Acquire) != 0
        && lifecycle.last_save_unix() > 0
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
    let mut last_save_time = Some(lifecycle.last_save_unix());
    if last_save_time == Some(0) {
        last_save_time = None;
    }
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
        aof_failure_time: {
            let t = lifecycle.aof_failure_unix();
            if t > 0 {
                Some(t)
            } else {
                None
            }
        },
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

impl PersistenceConfig {
    /// Backwards-compatible accessors used by the old 0.x-style tests that
    /// still reference the duplicate threshold fields. The single source of
    /// truth is `HealthConfig`; these just forward to it.
    pub fn max_snapshot_age_secs(&self) -> Option<u64> {
        self.max_snapshot_age_secs
    }
    pub fn max_replication_lag_secs(&self) -> Option<u64> {
        self.max_replication_lag_secs
    }
}
