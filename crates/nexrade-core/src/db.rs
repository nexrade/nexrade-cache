//! High-level database handle combining Store + PubSub + config.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
use std::time::SystemTime;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::acl::AclManager;
use crate::cluster::generate_node_id;
use crate::conn_registry::ConnectionRegistry;
#[cfg(not(target_arch = "wasm32"))]
use crate::persistence::AofWriter;
use crate::persistence::PersistenceConfig;
use crate::pubsub::PubSub;
#[cfg(not(target_arch = "wasm32"))]
use crate::replication::ReplicationState;
use crate::slowlog::SlowLog;
use crate::store::Store;
use crate::tracking::TrackingRegistry;

/// Shared server state — cloneable handle passed to every connection.
#[derive(Clone)]
pub struct Db {
    pub store: Store,
    pub pubsub: PubSub,
    pub config: Arc<Mutex<ServerConfig>>,
    pub stats: Arc<Stats>,
    /// Notify waiting BLPOP/BRPOP callers when a new item is pushed.
    pub list_notify: Arc<Notify>,
    /// Notify waiting BLMOVE callers.
    pub move_notify: Arc<Notify>,
    /// Notify waiting XREAD/XREADGROUP BLOCK callers when a new entry is
    /// appended to a stream. Single global notification is enough because
    /// stream keys are already sharded — the wakeup is filtered by key in the
    /// caller's re-check loop.
    pub stream_notify: Arc<Notify>,
    /// Monotonically increasing client ID counter.
    pub next_client_id: Arc<AtomicU64>,
    /// AOF writer — shared across all connections (None if AOF is disabled).
    #[cfg(not(target_arch = "wasm32"))]
    pub aof_writer: Arc<Mutex<Option<AofWriter>>>,
    /// Slow query log — shared across all connections.
    pub slowlog: Arc<SlowLog>,
    /// Replication state — shared across all connections.
    #[cfg(not(target_arch = "wasm32"))]
    pub replication: Arc<ReplicationState>,
    /// Signalled by SHUTDOWN command to trigger graceful server exit.
    pub shutdown: Arc<Notify>,
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
        Self {
            store,
            pubsub: PubSub::new(),
            config: Arc::new(Mutex::new(config)),
            stats: Arc::new(Stats::default()),
            list_notify: Arc::new(Notify::new()),
            move_notify: Arc::new(Notify::new()),
            stream_notify: Arc::new(Notify::new()),
            next_client_id: Arc::new(AtomicU64::new(1)),
            #[cfg(not(target_arch = "wasm32"))]
            aof_writer: Arc::new(Mutex::new(None)),
            slowlog,
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
        }
    }

    pub fn db_count(&self) -> usize {
        self.store.db_count
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
    pub requirepass: Option<String>,
    pub tls_enabled: bool,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_port: Option<u16>,
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    pub lua_time_limit: u64,
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: usize,
    pub hz: u32,
    pub active_expire: bool,
    /// When `Some((host, port))`, this server starts as a replica of the given primary.
    pub replica_of: Option<(String, u16)>,
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
            requirepass: None,
            tls_enabled: false,
            tls_cert: None,
            tls_key: None,
            tls_port: Some(6380),
            metrics_enabled: true,
            metrics_port: 9091,
            lua_time_limit: 5000,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            hz: 10,
            active_expire: true,
            replica_of: None,
        }
    }
}
