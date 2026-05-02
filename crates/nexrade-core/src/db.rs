//! High-level database handle combining Store + PubSub + config.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
use std::time::SystemTime;

use parking_lot::Mutex;
use tokio::sync::Notify;

#[cfg(not(target_arch = "wasm32"))]
use crate::persistence::AofWriter;
use crate::persistence::PersistenceConfig;
use crate::pubsub::PubSub;
#[cfg(not(target_arch = "wasm32"))]
use crate::replication::ReplicationState;
use crate::slowlog::SlowLog;
use crate::store::Store;

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
                *repl.role.write() = crate::replication::ReplicationRole::Replica;
                *repl.replica_of.write() = Some(ro.clone());
            }
            repl
        };
        Self {
            store: Store::new(db_count),
            pubsub: PubSub::new(),
            config: Arc::new(Mutex::new(config)),
            stats: Arc::new(Stats::default()),
            list_notify: Arc::new(Notify::new()),
            move_notify: Arc::new(Notify::new()),
            next_client_id: Arc::new(AtomicU64::new(1)),
            #[cfg(not(target_arch = "wasm32"))]
            aof_writer: Arc::new(Mutex::new(None)),
            slowlog,
            #[cfg(not(target_arch = "wasm32"))]
            replication,
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub fn db_count(&self) -> usize {
        self.store.db_count
    }
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
pub enum MaxMemoryPolicy {
    /// Return an error on writes when limit is reached (default).
    #[default]
    NoEviction,
    /// Evict any random key across all databases.
    AllKeysRandom,
    /// Evict the least-recently-used key across all databases.
    AllKeysLru,
    /// Evict a random key that has a TTL set.
    VolatileRandom,
    /// Evict the least-recently-used key that has a TTL set.
    VolatileLru,
    /// Evict the key with the soonest expiry time.
    VolatileTtl,
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
