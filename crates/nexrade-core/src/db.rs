//! High-level database handle combining Store + PubSub + config.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(not(target_arch = "wasm32"))]
use std::time::SystemTime;

use parking_lot::Mutex;
use tokio::sync::Notify;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::acl::AclManager;
use crate::cluster::generate_node_id;
use crate::conn_registry::ConnectionRegistry;
use crate::health::HealthConfig;
use crate::latency::LatencyMonitor;
use crate::notify::NotifyFlagsAtomic;
#[cfg(not(target_arch = "wasm32"))]
use crate::persistence::AofWriter;
use crate::persistence::PersistenceConfig;
use crate::pubsub::PubSub;
#[cfg(not(target_arch = "wasm32"))]
use crate::replication::ReplicationState;
use crate::slowlog::SlowLog;
use crate::store::Store;
use crate::tracking::TrackingRegistry;

/// Low-cost mutation admission for persistence safety.
///
/// Ordinary writes only do two atomic state reads plus an active counter
/// increment/decrement. Rare persistence operations take `exclusive`, close
/// admission, and wait for active mutations to finish before capturing state.
#[cfg(not(target_arch = "wasm32"))]
pub struct PersistenceCoordinator {
    state: AtomicU8,
    active_mutations: AtomicUsize,
    drained: Notify,
    exclusive: Arc<AsyncMutex<()>>,
}

#[cfg(not(target_arch = "wasm32"))]
const PERSISTENCE_RUNNING: u8 = 0;
#[cfg(not(target_arch = "wasm32"))]
const PERSISTENCE_QUIESCING: u8 = 1;

#[cfg(not(target_arch = "wasm32"))]
impl PersistenceCoordinator {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(PERSISTENCE_RUNNING),
            active_mutations: AtomicUsize::new(0),
            drained: Notify::new(),
            exclusive: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Admit one write, waiting out a transient quiesce instead of failing.
    ///
    /// A background snapshot closes admission only for the duration of the
    /// in-memory `snapshot_dbs()` capture (file I/O happens after the guard
    /// is dropped), so the window is short and bounded. Rejecting writes
    /// during it surfaced to clients as
    /// `MISCONF persistence is quiescing` — which is wrong: `MISCONF` means
    /// persistence is *broken and needs an operator*, not that the server is
    /// mid-checkpoint. Redis blocks the writer for the equivalent window
    /// rather than erroring, and a routine `BGSAVE` must not fail user
    /// writes.
    ///
    /// Waits up to `timeout` for admission to reopen. Returns `None` only if
    /// the quiesce outlasts the timeout, in which case the caller still
    /// surfaces `MISCONF` — that residual case means persistence really is
    /// stuck, which is what the error is for.
    pub async fn enter_mutation_waiting(
        self: &Arc<Self>,
        timeout: std::time::Duration,
    ) -> Option<PersistenceMutation> {
        // Fast path: no quiesce in flight. This is the overwhelmingly
        // common case and costs the same as `enter_mutation`.
        if let Some(permit) = self.enter_mutation() {
            return Some(permit);
        }
        // Slow path: a capture is in flight. Wait for the guard to reopen
        // admission, re-arming the notification before each re-check so a
        // wake between attempts is never missed.
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(permit) = self.enter_mutation() {
                return Some(permit);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                // Timed out — fall through to one last attempt so a reopen
                // racing the deadline still succeeds.
                return self.enter_mutation();
            }
        }
    }

    /// Admit one write without an async lock on the hot path. The second
    /// state read closes the race with a concurrent quiesce transition.
    pub fn enter_mutation(self: &Arc<Self>) -> Option<PersistenceMutation> {
        if self.state.load(Ordering::Acquire) != PERSISTENCE_RUNNING {
            return None;
        }
        self.active_mutations.fetch_add(1, Ordering::AcqRel);
        if self.state.load(Ordering::Acquire) == PERSISTENCE_RUNNING {
            Some(PersistenceMutation {
                coordinator: Arc::clone(self),
            })
        } else {
            if self.active_mutations.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.drained.notify_waiters();
            }
            None
        }
    }

    /// Close admission and wait until every admitted writer has completed.
    pub async fn quiesce(self: &Arc<Self>) -> PersistenceQuiesce {
        let exclusive = Arc::clone(&self.exclusive).lock_owned().await;
        self.state.store(PERSISTENCE_QUIESCING, Ordering::Release);
        loop {
            if self.active_mutations.load(Ordering::Acquire) == 0 {
                break;
            }
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active_mutations.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
        PersistenceQuiesce {
            coordinator: Arc::clone(self),
            _exclusive: exclusive,
        }
    }

    /// True when the coordinator has been asked to quiesce but has not yet
    /// reopened admission (e.g. an in-flight snapshot, AOF rewrite, or
    /// shutdown). Exposed to the operations HTTP surface so readiness
    /// reports `PersistenceQuiescing` while the snapshot is in flight.
    pub fn is_quiescing(&self) -> bool {
        self.state.load(Ordering::Acquire) == PERSISTENCE_QUIESCING
    }

    /// Expose the raw atomic state byte for diagnostics and tests.
    pub fn state_byte(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }
}

/// Active mutation permit. Dropping it makes a quiescing persistence operation
/// eligible to capture a point-in-time view.
#[cfg(not(target_arch = "wasm32"))]
pub struct PersistenceMutation {
    coordinator: Arc<PersistenceCoordinator>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for PersistenceMutation {
    fn drop(&mut self) {
        if self
            .coordinator
            .active_mutations
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.coordinator.drained.notify_waiters();
        }
    }
}

/// Exclusive quiesce guard. Reopens write admission when dropped.
#[cfg(not(target_arch = "wasm32"))]
pub struct PersistenceQuiesce {
    coordinator: Arc<PersistenceCoordinator>,
    _exclusive: OwnedMutexGuard<()>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for PersistenceQuiesce {
    fn drop(&mut self) {
        self.coordinator
            .state
            .store(PERSISTENCE_RUNNING, Ordering::Release);
        self.coordinator.drained.notify_waiters();
    }
}

/// Shared server state — cloneable handle passed to every connection.
#[derive(Clone)]
pub struct Db {
    pub store: Store,
    pub pubsub: PubSub,
    pub config: Arc<Mutex<ServerConfig>>,
    pub stats: Arc<Stats>,
    /// Blocking wait channel for BLPOP/BRPOP/BLMPOP.
    pub list_chan: Arc<WaiterChannel>,
    /// Notify waiting BLMOVE callers (reserved; not yet wired).
    pub move_notify: Arc<Notify>,
    /// Blocking wait channel for XREAD/XREADGROUP BLOCK.
    pub stream_chan: Arc<WaiterChannel>,
    /// Blocking wait channel for BZMPOP (distinct so pure zset producers wake).
    pub zset_chan: Arc<WaiterChannel>,
    /// Monotonically increasing client ID counter.
    pub next_client_id: Arc<AtomicU64>,
    /// AOF writer — shared across all connections (None if AOF is disabled).
    #[cfg(not(target_arch = "wasm32"))]
    pub aof_writer: Arc<Mutex<Option<AofWriter>>>,
    /// Coordinates persistence with mutations. The fast path is atomic
    /// admission; rare snapshots, rewrites, and shutdown hold the exclusive
    /// coordinator while draining active mutations.
    #[cfg(not(target_arch = "wasm32"))]
    pub persistence: Arc<PersistenceCoordinator>,
    /// Serializes RDB save requests independently of snapshot admission.
    #[cfg(not(target_arch = "wasm32"))]
    pub rdb_save_job: Arc<AsyncMutex<()>>,
    /// Slow query log — shared across all connections.
    pub slowlog: Arc<SlowLog>,
    /// Per-event latency samples for the LATENCY command family (0.7.4).
    /// Distinct from slowlog: records every timed command into a fixed-size
    /// ring so LATEST/HISTORY/HISTOGRAM have data even when slowlog is quiet.
    pub latency: LatencyMonitor,
    /// Replication state — shared across all connections.
    #[cfg(not(target_arch = "wasm32"))]
    pub replication: Arc<ReplicationState>,
    /// Signalled by SHUTDOWN command to trigger graceful server exit.
    pub shutdown: Arc<Notify>,
    /// Single-source lifecycle state for the operations HTTP surface,
    /// `INFO health`, and operational Prometheus gauges.
    #[cfg(not(target_arch = "wasm32"))]
    pub lifecycle: Arc<crate::health::LifecycleState>,
    /// Cached LRU clock, updated by the background tick task. Reading this
    /// is a single atomic load — much cheaper than `SystemTime::now()` per
    /// access. Resolution is `1/hz` seconds.
    pub lru_clock: Arc<AtomicU32>,
    /// Server-wide ACL state — multi-user auth + per-command / per-key
    /// permissions. Cloning is cheap (Arc-internal).
    pub acl: AclManager,
    /// CLIENT TRACKING registry — per-client tracking state + key index
    /// for invalidation push delivery. Cloning is cheap (Arc-internal).
    pub tracking: TrackingRegistry,
    /// ACL permission checks (`AclManager::check_permission`) integrated into command dispatch.
    /// Server-wide registry of live TCP connections for `CLIENT LIST`,
    /// `CLIENT INFO`, `CLIENT KILL`, `CLIENT PAUSE`. Cloning is cheap
    /// (Arc-internal).
    pub connections: ConnectionRegistry,
    /// Stable 40-char hex node id for this server. Used by `CLUSTER MYID`
    /// and `CLUSTER NODES`. Generated at startup from a UUIDv4.
    pub cluster_node_id: String,
    /// Whether the cluster slot routing is active. When false, no
    /// MOVED/CROSSSLOT replies are emitted — the server behaves as a
    /// standalone. Set via `cluster_enabled` config; defaults false so
    /// the default user experience is unaffected.
    pub cluster_enabled: Arc<AtomicBool>,
    /// Lock-free mirror of `config.max_memory`. 0 means "no limit" so
    /// the dispatch path can skip the eviction check entirely without
    /// taking the config lock.
    pub max_memory_limit: Arc<AtomicUsize>,
    /// Lock-free mirror of `config.maxmemory_policy` encoded as a u8
    /// (matches the discriminant). Default `NoEviction` is 0.
    pub maxmemory_policy: Arc<AtomicU8>,
    /// Keyspace-notification flags (lock-free mirror of
    /// `config.notify_keyspace_events`). 0 ⇒ no events fire.
    pub notify_flags: NotifyFlagsAtomic,
    /// MONITOR feed — broadcast of per-command log lines to every active
    /// MONITOR client. Cheap no-op when nobody is monitoring (atomic load).
    pub monitor: MonitorBus,
}

impl Db {
    pub fn new(config: ServerConfig) -> Self {
        let db_count = config.databases;
        let slowlog = Arc::new(SlowLog::new(
            config.slowlog_log_slower_than.max(0) as u64,
            config.slowlog_max_len,
        ));
        #[cfg(not(target_arch = "wasm32"))]
        let replication = {
            let replica_of = config.replica_of.clone();
            let replication_id = ReplicationState::generate_replication_id();
            let repl = ReplicationState::new_primary(replication_id);
            if let Some(ref ro) = replica_of {
                repl.set_role(crate::replication::ReplicationRole::Replica);
                *repl.replica_of.write() = Some(ro.clone());
            }
            repl
        };
        let lru_clock_atomic = Arc::new(AtomicU32::new(current_lru_secs()));
        let lru_clock = crate::store::LruClock::new(lru_clock_atomic.clone());
        let mut store = Store::new(db_count);
        store.set_lru_clock(lru_clock);
        // Snapshot the initial config into lock-free atomics BEFORE
        // the config Arc is constructed.
        let initial_max_memory = config.max_memory.unwrap_or(0);
        let initial_maxmemory_policy = config.maxmemory_policy.clone() as u8;
        let initial_notify_flags = config.notify_keyspace_events;
        // Apply dual-encoding thresholds from config (file / defaults)
        // so the hot path sees the right atomics from process start.
        crate::list_data::set_list_thresholds(crate::list_data::ListThresholds {
            max_entries: config.list_max_listpack_entries,
            max_size: config.list_max_listpack_size,
            max_elem: config.list_max_listpack_value,
            demote_entries: config.list_demote_entries,
        });
        crate::hash_data::set_hash_thresholds(crate::hash_data::HashThresholds {
            max_entries: config.hash_max_listpack_entries,
            max_size: config.hash_max_listpack_size,
            max_value: config.hash_max_listpack_value,
            demote_entries: config.hash_demote_entries,
        });
        crate::set_data::set_set_thresholds(crate::set_data::SetThresholds {
            max_entries: config.set_max_listpack_entries,
            max_size: config.set_max_listpack_size,
            max_value: config.set_max_listpack_value,
            demote_entries: config.set_demote_entries,
        });
        crate::zset_data::set_zset_thresholds(crate::zset_data::ZSetThresholds {
            max_entries: config.zset_max_listpack_entries,
            max_size: config.zset_max_listpack_size,
            max_value: config.zset_max_listpack_value,
            demote_entries: config.zset_demote_entries,
        });
        Self {
            store,
            pubsub: PubSub::new(),
            config: Arc::new(Mutex::new(config)),
            stats: Arc::new(Stats::default()),
            list_chan: Arc::new(WaiterChannel::new()),
            move_notify: Arc::new(Notify::new()),
            stream_chan: Arc::new(WaiterChannel::new()),
            zset_chan: Arc::new(WaiterChannel::new()),
            next_client_id: Arc::new(AtomicU64::new(1)),
            #[cfg(not(target_arch = "wasm32"))]
            aof_writer: Arc::new(Mutex::new(None)),
            slowlog,
            latency: LatencyMonitor::new(),
            #[cfg(not(target_arch = "wasm32"))]
            replication,
            shutdown: Arc::new(Notify::new()),
            lru_clock: lru_clock_atomic,
            acl: AclManager::new(),
            tracking: TrackingRegistry::new(),
            connections: ConnectionRegistry::new(),
            cluster_node_id: generate_node_id(),
            cluster_enabled: Arc::new(AtomicBool::new(false)),
            // Mirror the initial config into the lock-free atomics so
            // the dispatch fast path is correct on startup.
            max_memory_limit: Arc::new(AtomicUsize::new(initial_max_memory)),
            maxmemory_policy: Arc::new(AtomicU8::new(initial_maxmemory_policy)),
            notify_flags: NotifyFlagsAtomic::new(initial_notify_flags),
            monitor: MonitorBus::new(),
            #[cfg(not(target_arch = "wasm32"))]
            persistence: Arc::new(PersistenceCoordinator::new()),
            #[cfg(not(target_arch = "wasm32"))]
            rdb_save_job: Arc::new(AsyncMutex::new(())),
            #[cfg(not(target_arch = "wasm32"))]
            lifecycle: Arc::new(crate::health::LifecycleState::new()),
        }
    }

    pub fn db_count(&self) -> usize {
        self.store.db_count
    }

    /// Lifecycle state for the operations HTTP surface and `INFO health`.
    /// One source of truth for phase transitions and stored timestamps.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lifecycle(&self) -> &Arc<crate::health::LifecycleState> {
        &self.lifecycle
    }

    /// Latch an AOF I/O failure exactly once. Subsequent writers are refused
    /// so the in-memory dataset cannot drift further from the durable log.
    ///
    /// Publication order is load-bearing. The diagnostic fields are written
    /// **before** the `aof_failed` latch is set, so any reader that observes
    /// `aof_failed == true` (via `Acquire`) is guaranteed to also see the
    /// bounded message and timestamp that explain it. The previous order set
    /// the latch first, leaving a window in which `/readyz` and `INFO
    /// persistence` reported the generic `"AOF persistence failed"` fallback
    /// instead of the real I/O error.
    ///
    /// `first_failure` (taken under the `aof_failed_msg` lock) is the
    /// exactly-once gate, replacing the latch's own `swap` return value —
    /// the latch can no longer serve that role now that it is set last.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn fail_aof(&self, operation: &str, err: impl std::fmt::Display) {
        self.stats.aof_last_write_status.store(1, Ordering::Relaxed);

        let mut message = format!("{operation}: {err}");
        message.truncate(512);

        let first_failure = {
            let mut slot = self.stats.aof_failed_msg.lock();
            if slot.is_none() {
                self.stats
                    .aof_failed_time
                    .store(unix_secs(), Ordering::Relaxed);
                *slot = Some(message.clone());
                true
            } else {
                false
            }
        };

        if first_failure {
            // Mirror into the canonical health snapshot so HTTP `/readyz`
            // and `INFO health` report the same bounded message the writer
            // produced, not a separate copy that could drift.
            self.lifecycle().record_aof_failure(message);
        }

        // Latch last, with Release, so the diagnostic stores above are visible
        // to every reader that Acquire-loads this flag.
        self.stats.aof_failed.store(true, Ordering::Release);
    }

    /// Wake list waiters (BLPOP/BRPOP/BLMPOP) — no-op if none parked.
    #[inline]
    pub fn notify_list_waiters(&self) {
        self.list_chan.notify_if_waiting();
    }

    /// Wake stream waiters (XREAD/XREADGROUP BLOCK) — no-op if none parked.
    #[inline]
    pub fn notify_stream_waiters(&self) {
        self.stream_chan.notify_if_waiting();
    }

    /// Wake zset waiters (BZMPOP) — no-op if none parked.
    #[inline]
    pub fn notify_zset_waiters(&self) {
        self.zset_chan.notify_if_waiting();
    }

    /// RAII: bumps the list waiter count for the life of a blocking wait.
    pub fn park_list_waiter(&self) -> WaiterGuard<'_> {
        self.list_chan.park()
    }

    pub fn park_stream_waiter(&self) -> WaiterGuard<'_> {
        self.stream_chan.park()
    }

    pub fn park_zset_waiter(&self) -> WaiterGuard<'_> {
        self.zset_chan.park()
    }

    /// Publish a keyspace / keyevent notification if the matching flags are
    /// enabled. Cheap no-op when the bitmask is 0 (the common case).
    ///
    /// `event` is the Redis event name (`"set"`, `"del"`, `"expired"`, …).
    /// Channel names follow Redis:
    ///   * keyspace: `__keyspace@<db>__:<key>`  payload = event
    ///   * keyevent: `__keyevent@<db>__:<event>` payload = key
    #[inline]
    pub fn notify_keyspace_event(&self, db_index: usize, event: &str, key: &[u8]) {
        let flags = self.notify_flags.load();
        if flags.0 == 0 {
            return;
        }
        use crate::notify::NotifyFlags;
        let emit_ks = flags.contains(NotifyFlags::PREFIX_KEYSPACE);
        let emit_ke = flags.contains(NotifyFlags::PREFIX_KEYEVENT);
        if !emit_ks && !emit_ke {
            return;
        }
        if emit_ks {
            // __keyspace@<db>__:<key>  → payload is the event name
            let mut channel = format!("__keyspace@{db_index}__:").into_bytes();
            channel.extend_from_slice(key);
            let _ = self.pubsub.publish(channel, event.as_bytes().to_vec());
        }
        if emit_ke {
            // __keyevent@<db>__:<event> → payload is the key
            let channel = format!("__keyevent@{db_index}__:{event}").into_bytes();
            let _ = self.pubsub.publish(channel, key.to_vec());
        }
    }

    /// Convenience: classify a write command and notify for each touched key
    /// if the corresponding class bit is set.
    #[inline]
    pub fn notify_command_keys(&self, db_index: usize, cmd: &str, keys: &[&[u8]]) {
        let flags = self.notify_flags.load();
        if flags.0 == 0 || keys.is_empty() {
            return;
        }
        let Some((bit, event)) = crate::notify::classify(cmd) else {
            return;
        };
        if !flags.contains(bit) {
            return;
        }
        for key in keys {
            self.notify_keyspace_event(db_index, event, key);
        }
    }
}

/// Server-wide MONITOR feed. Clients that issue `MONITOR` subscribe and
/// receive a Redis-compatible `+timestamp [db client] "cmd" "arg" ...\r\n`
/// line for every subsequent command on any connection.
#[derive(Clone)]
pub struct MonitorBus {
    tx: tokio::sync::broadcast::Sender<String>,
    subscribers: Arc<AtomicUsize>,
}

impl MonitorBus {
    pub fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(1024);
        Self {
            tx,
            subscribers: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub fn has_subscribers(&self) -> bool {
        self.subscribers.load(Ordering::Relaxed) > 0
    }

    /// Subscribe and bump the live-subscriber count. Drop the returned
    /// guard (or call `unsubscribe`) when the MONITOR client disconnects.
    pub fn subscribe(&self) -> (tokio::sync::broadcast::Receiver<String>, MonitorGuard) {
        self.subscribers.fetch_add(1, Ordering::Relaxed);
        (
            self.tx.subscribe(),
            MonitorGuard {
                counter: self.subscribers.clone(),
            },
        )
    }

    /// Publish a monitor line. No-op when nobody is listening.
    #[inline]
    pub fn publish(&self, line: String) {
        if self.has_subscribers() {
            let _ = self.tx.send(line);
        }
    }
}

impl Default for MonitorBus {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII: decrements MONITOR subscriber count on drop.
pub struct MonitorGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for MonitorGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A blocking-wait rendezvous: a `Notify` plus a waiter count so producers
/// can skip the notify call when nobody is parked. One type covers list /
/// stream / zset — only the channel instance differs.
pub struct WaiterChannel {
    notify: Notify,
    waiters: AtomicUsize,
}

impl WaiterChannel {
    pub fn new() -> Self {
        Self {
            notify: Notify::new(),
            waiters: AtomicUsize::new(0),
        }
    }

    /// Wake every parked waiter, or no-op if the count is zero.
    #[inline]
    pub fn notify_if_waiting(&self) {
        if self.waiters.load(Ordering::Relaxed) > 0 {
            self.notify.notify_waiters();
        }
    }

    /// Subscribe to the next wake. Callers typically hold a `WaiterGuard`
    /// for the duration of the wait so the producer-side count stays honest.
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    /// RAII: bumps `waiters` for the life of the returned guard.
    pub fn park(&self) -> WaiterGuard<'_> {
        WaiterGuard::new(&self.waiters)
    }
}

impl Default for WaiterChannel {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that bumps a waiter count for the life of a blocking wait.
/// Drop is the only way the count goes down, so early returns / timeouts
/// can't leak a permanently-inflated count. Borrows the atomic (no Arc clone).
pub struct WaiterGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> WaiterGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Read the current Unix timestamp in whole seconds.
fn current_lru_secs() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

impl Default for Db {
    fn default() -> Self {
        Self::new(ServerConfig::default())
    }
}

/// Global server statistics.
#[derive(Debug, Default)]
pub struct Stats {
    pub total_connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub total_commands: AtomicU64,
    pub total_reads: AtomicU64,
    pub total_writes: AtomicU64,
    pub keyspace_hits: AtomicU64,
    pub keyspace_misses: AtomicU64,
    #[cfg(not(target_arch = "wasm32"))]
    pub start_time: std::sync::OnceLock<Instant>,
    /// Number of write operations since last RDB save (like Redis's dirty counter).
    pub dirty_keys: AtomicU64,
    /// Unix timestamp of last successful RDB save.
    pub last_save_time: AtomicU64,
    /// True when the AOF writer is open and active.
    pub aof_enabled: AtomicBool,
    /// True when a background RDB save is in progress (prevents concurrent saves).
    pub bgsave_in_progress: AtomicBool,
    /// Outcome of the most recent RDB save — 0 = ok, 1 = error. Surfaced
    /// via `INFO persistence` `rdb_last_bgsave_status` so operators can
    /// tell failed saves apart from successful ones.
    pub bgsave_last_status: AtomicU8,
    /// True while a background AOF rewrite is in flight (only one
    /// concurrent rewrite is allowed; a second `BGREWRITEAOF` while this
    /// is set should be rejected). Surfaced via `INFO persistence`
    /// `aof_rewrite_in_progress`.
    pub aof_rewrite_in_progress: AtomicBool,
    /// Outcome of the most recent AOF rewrite — 0 = ok, 1 = error.
    /// Surfaced via `INFO persistence` `aof_last_bgrewrite_status`.
    pub aof_rewrite_last_status: AtomicU8,
    /// Timestamp of the last successful AOF rewrite.
    pub last_rewrite_time: AtomicU64,
    /// Outcome of the most recent AOF append/fsync — 0 = ok, 1 = error.
    /// Surfaced via `INFO persistence` `aof_last_write_status`. Stays at
    /// `ok` until a real write fails (or AOF is disabled, in which case
    /// INFO reports `err` regardless of this flag).
    pub aof_last_write_status: AtomicU8,
    /// AOF writer entered a failed state. When set, further writes are
    /// rejected with a MISCONF-style error and only reads/administrative
    /// commands remain available.
    pub aof_failed: AtomicBool,
    /// Timestamp of the AOF failure (unix seconds).
    pub aof_failed_time: AtomicU64,
    /// Bounded error message from the failing operation (append/fsync/reopen).
    /// Updated only when `aof_failed` transitions from false to true.
    pub aof_failed_msg: Mutex<Option<String>>,
    /// Set by `SHUTDOWN NOSAVE` so the listener can skip its final RDB save
    /// without corrupting the dirty counter used by telemetry.
    pub shutdown_nosave: AtomicBool,
    /// Approximate operations per second (updated by background task).
    pub ops_per_sec: AtomicU64,
    /// Snapshot of total_commands from the previous background tick.
    pub prev_commands: AtomicU64,
}

/// Returns the current Unix timestamp in whole seconds.
pub fn unix_secs() -> u64 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
    #[cfg(target_arch = "wasm32")]
    {
        0
    }
}

impl Stats {
    pub fn uptime_secs(&self) -> u64 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.start_time
                .get()
                .map(|t| t.elapsed().as_secs())
                .unwrap_or(0)
        }
        #[cfg(target_arch = "wasm32")]
        {
            0
        }
    }

    pub fn record_command(&self) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_hit(&self) {
        self.keyspace_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.keyspace_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connect(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn disconnect(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Eviction policy applied when `max_memory` is exceeded.
#[derive(Debug, Clone, PartialEq, Default)]
#[repr(u8)]
pub enum MaxMemoryPolicy {
    /// Return an error on writes when limit is reached (default).
    #[default]
    NoEviction = 0,
    /// Evict any random key across all databases.
    AllKeysRandom = 1,
    /// Evict the least-recently-used key across all databases.
    AllKeysLru = 2,
    /// Evict a random key that has a TTL set.
    VolatileRandom = 3,
    /// Evict the least-recently-used key that has a TTL set.
    VolatileLru = 4,
    /// Evict the key with the soonest expiry time.
    VolatileTtl = 5,
}

impl std::str::FromStr for MaxMemoryPolicy {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "allkeys-random" => Self::AllKeysRandom,
            "allkeys-lru" => Self::AllKeysLru,
            "volatile-random" => Self::VolatileRandom,
            "volatile-lru" => Self::VolatileLru,
            "volatile-ttl" => Self::VolatileTtl,
            _ => Self::NoEviction,
        })
    }
}

impl MaxMemoryPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysRandom => "allkeys-random",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileLru => "volatile-lru",
            Self::VolatileTtl => "volatile-ttl",
        }
    }
}

impl std::fmt::Display for MaxMemoryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub databases: usize,
    pub max_memory: Option<usize>,
    pub maxmemory_policy: MaxMemoryPolicy,
    pub max_clients: usize,
    pub tcp_backlog: u32,
    pub timeout: u64,
    pub loglevel: String,
    pub save_rules: Vec<(u64, usize)>,
    pub persistence: PersistenceConfig,
    /// Health endpoint configuration.
    pub health: HealthConfig,
    pub requirepass: Option<String>,
    pub tls_enabled: bool,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_port: Option<u16>,
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    /// Bind address for the Prometheus `/metrics` listener. Independent of
    /// `bind` (the Redis data-plane listener) so an operator can scope
    /// metrics to a sidecar address without touching the data plane.
    /// Default mirrors `bind` so a freshly constructed config behaves the
    /// same as 1.1.x — `config_from_cli` populates this from `bind` when
    /// no explicit `[metrics].bind` / `--metrics-bind` is supplied.
    pub metrics_bind: String,
    pub lua_time_limit: u64,
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: usize,
    pub hz: u32,
    pub active_expire: bool,
    /// When `Some((host, port))`, this server starts as a replica of the given primary.
    pub replica_of: Option<(String, u16)>,
    /// Path of the TOML config file this process was started with, if any.
    /// Used by `CONFIG REWRITE` to know where to write. Empty when started
    /// with pure CLI flags / defaults.
    pub config_file_path: Option<String>,
    /// Compact list promote/demote thresholds (mirrored into process-wide
    /// atomics at startup / CONFIG SET). Defaults match `list_data` constants.
    pub list_max_listpack_entries: usize,
    pub list_max_listpack_size: usize,
    pub list_max_listpack_value: usize,
    pub list_demote_entries: usize,
    /// Hash dual-encoding thresholds (see `hash_data`).
    pub hash_max_listpack_entries: usize,
    pub hash_max_listpack_size: usize,
    pub hash_max_listpack_value: usize,
    pub hash_demote_entries: usize,
    /// Set dual-encoding thresholds (see `set_data`).
    pub set_max_listpack_entries: usize,
    pub set_max_listpack_size: usize,
    pub set_max_listpack_value: usize,
    pub set_demote_entries: usize,
    /// Sorted-set dual-encoding thresholds (see `zset_data`).
    pub zset_max_listpack_entries: usize,
    pub zset_max_listpack_size: usize,
    pub zset_max_listpack_value: usize,
    pub zset_demote_entries: usize,
    /// Redis `notify-keyspace-events` flag string (parsed into a bitmask
    /// at startup / CONFIG SET). Empty = no notifications (default).
    pub notify_keyspace_events: crate::notify::NotifyFlags,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 6379,
            databases: 16,
            max_memory: None,
            maxmemory_policy: MaxMemoryPolicy::NoEviction,
            max_clients: 10000,
            tcp_backlog: 511,
            timeout: 0,
            loglevel: "notice".to_string(),
            save_rules: vec![(900, 1), (300, 10), (60, 10000)],
            persistence: PersistenceConfig::default(),
            health: HealthConfig::default(),
            requirepass: None,
            tls_enabled: false,
            tls_cert: None,
            tls_key: None,
            tls_port: Some(6380),
            metrics_enabled: true,
            metrics_port: 9091,
            // Mirrored from `bind` by `config_from_cli` after the file
            // and CLI overrides land. Default-empty keeps the field
            // type stable; `start_server` rejects empty here as a
            // defensive guard.
            metrics_bind: String::new(),
            lua_time_limit: 5000,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            hz: 10,
            active_expire: true,
            replica_of: None,
            config_file_path: None,
            list_max_listpack_entries: crate::list_data::LIST_COMPACT_MAX_ENTRIES,
            list_max_listpack_size: crate::list_data::LIST_COMPACT_MAX_SIZE,
            list_max_listpack_value: crate::list_data::LIST_COMPACT_MAX_ELEM,
            list_demote_entries: crate::list_data::LIST_DEMOTE_ENTRIES,
            hash_max_listpack_entries: crate::hash_data::HASH_COMPACT_MAX_ENTRIES,
            hash_max_listpack_size: crate::hash_data::HASH_COMPACT_MAX_SIZE,
            hash_max_listpack_value: crate::hash_data::HASH_COMPACT_MAX_VALUE,
            hash_demote_entries: crate::hash_data::HASH_DEMOTE_ENTRIES,
            set_max_listpack_entries: crate::set_data::SET_COMPACT_MAX_ENTRIES,
            set_max_listpack_size: crate::set_data::SET_COMPACT_MAX_SIZE,
            set_max_listpack_value: crate::set_data::SET_COMPACT_MAX_VALUE,
            set_demote_entries: crate::set_data::SET_DEMOTE_ENTRIES,
            zset_max_listpack_entries: crate::zset_data::ZSET_COMPACT_MAX_ENTRIES,
            zset_max_listpack_size: crate::zset_data::ZSET_COMPACT_MAX_SIZE,
            zset_max_listpack_value: crate::zset_data::ZSET_COMPACT_MAX_VALUE,
            zset_demote_entries: crate::zset_data::ZSET_DEMOTE_ENTRIES,
            notify_keyspace_events: crate::notify::NotifyFlags::empty(),
        }
    }
}
