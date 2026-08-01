//! Primary/replica replication state.
//!
//! This module holds the in-memory state used by both primaries and replicas
//! to perform Redis-compatible PSYNC-based replication.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tokio::sync::{broadcast, Notify};

/// Whether this server acts as a primary or a replica.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationRole {
    Primary,
    Replica,
}

/// Information about a connected replica.
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// Monotonically increasing internal identifier.
    pub id: u64,
    /// Remote address of the replica connection.
    pub addr: SocketAddr,
    /// The byte offset the replica has acknowledged.
    pub offset: u64,
    /// Wall-clock ms (UNIX epoch) of the last REPLCONF ACK from this replica.
    /// Used for `INFO replication` `lag=` and WAIT progress.
    pub last_ack_ms: u64,
}

/// Shared replication state — cloneable (all fields are Arc-backed).
pub struct ReplicationState {
    /// Current role of this server instance.
    pub role: RwLock<ReplicationRole>,
    /// 40-character hex replication ID (generated randomly at startup).
    pub replication_id: String,
    /// Total bytes of write commands propagated (primary) or received (replica).
    pub replication_offset: AtomicU64,
    /// When acting as a replica: the primary's host and port.
    pub replica_of: RwLock<Option<(String, u16)>>,
    /// Broadcast channel for propagating write commands to connected replicas.
    /// Only used on primaries; `None` on replicas.
    pub propagate_tx: Option<broadcast::Sender<bytes::Bytes>>,
    /// List of currently connected replica sessions.
    pub connected_replicas: RwLock<Vec<ReplicaInfo>>,
    /// Notify fired when `replica_of` is updated so the background replication
    /// task can (re-)connect to the new primary without delay.
    pub replica_notify: Arc<Notify>,
    /// 0.8.0: Notify fired whenever any replica ACKs a new offset so
    /// `WAIT` can wake without polling.
    pub ack_notify: Arc<Notify>,
    /// Whether the connection to the primary is currently active (replica only).
    pub primary_link_up: AtomicBool,
    /// Monotonically increasing replica ID counter.
    next_replica_id: AtomicU64,
    /// Atomic mirror of `*role.read() == ReplicationRole::Replica`.
    is_replica_flag: AtomicBool,
    /// Atomic mirror of the number of live replica subscribers.
    propagate_subscribers: AtomicUsize,
}

impl ReplicationState {
    /// Create a new `ReplicationState` for a primary server.
    pub fn new_primary(replication_id: String) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(1024);
        Arc::new(Self {
            role: RwLock::new(ReplicationRole::Primary),
            replication_id,
            replication_offset: AtomicU64::new(0),
            replica_of: RwLock::new(None),
            propagate_tx: Some(tx),
            connected_replicas: RwLock::new(Vec::new()),
            replica_notify: Arc::new(Notify::new()),
            ack_notify: Arc::new(Notify::new()),
            primary_link_up: AtomicBool::new(false),
            next_replica_id: AtomicU64::new(1),
            is_replica_flag: AtomicBool::new(false),
            propagate_subscribers: AtomicUsize::new(0),
        })
    }

    pub fn current_role(&self) -> ReplicationRole {
        self.role.read().clone()
    }

    pub fn is_replica(&self) -> bool {
        *self.role.read() == ReplicationRole::Replica
    }

    #[inline]
    pub fn is_replica_fast(&self) -> bool {
        self.is_replica_flag.load(Ordering::Acquire)
    }

    pub fn set_role(&self, role: ReplicationRole) {
        let is_replica = role == ReplicationRole::Replica;
        *self.role.write() = role;
        self.is_replica_flag.store(is_replica, Ordering::Release);
    }

    pub fn subscribe_propagation(&self) -> Option<broadcast::Receiver<bytes::Bytes>> {
        self.propagate_tx.as_ref().map(|tx| tx.subscribe())
    }

    #[inline]
    pub fn propagate_subscriber_count(&self) -> usize {
        self.propagate_subscribers.load(Ordering::Acquire)
    }

    /// Add a replica; `initial_offset` is the primary offset at FULLRESYNC time
    /// so WAIT sees the replica as caught up until further writes.
    pub fn register_replica(&self, addr: SocketAddr, initial_offset: u64) -> u64 {
        let id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        let now = now_ms();
        self.connected_replicas.write().push(ReplicaInfo {
            id,
            addr,
            offset: initial_offset,
            last_ack_ms: now,
        });
        self.propagate_subscribers.fetch_add(1, Ordering::AcqRel);
        self.ack_notify.notify_waiters();
        id
    }

    pub fn unregister_replica(&self, id: u64) {
        let mut replicas = self.connected_replicas.write();
        let before = replicas.len();
        replicas.retain(|r| r.id != id);
        if replicas.len() < before {
            self.propagate_subscribers.fetch_sub(1, Ordering::AcqRel);
            self.ack_notify.notify_waiters();
        }
    }

    /// Update the acknowledged offset for a replica (by address).
    pub fn update_replica_offset(&self, addr: SocketAddr, offset: u64) {
        let mut replicas = self.connected_replicas.write();
        let now = now_ms();
        for r in replicas.iter_mut() {
            if r.addr == addr {
                if offset > r.offset {
                    r.offset = offset;
                }
                r.last_ack_ms = now;
                drop(replicas);
                self.ack_notify.notify_waiters();
                return;
            }
        }
    }

    /// Count replicas whose acknowledged offset is ≥ `target_offset`.
    pub fn replicas_at_or_beyond(&self, target_offset: u64) -> usize {
        self.connected_replicas
            .read()
            .iter()
            .filter(|r| r.offset >= target_offset)
            .count()
    }

    /// Snapshot of (addr, offset, last_ack_ms) for INFO / ROLE.
    pub fn replica_snapshots(&self) -> Vec<(SocketAddr, u64, u64)> {
        self.connected_replicas
            .read()
            .iter()
            .map(|r| (r.addr, r.offset, r.last_ack_ms))
            .collect()
    }

    pub fn generate_replication_id() -> String {
        use uuid::Uuid;
        let a = Uuid::new_v4().simple().to_string();
        let b = Uuid::new_v4().simple().to_string();
        let combined = format!("{}{}", a, b);
        combined[..40].to_string()
    }

    /// Number of currently connected replicas (primary only).
    pub fn connected_replicas_count(&self) -> u64 {
        self.connected_replicas.read().len() as u64
    }

    /// Maximum offset lag across all connected replicas (primary only).
    /// 0 when no replicas are connected. The returned value is in bytes.
    pub fn max_replica_offset_lag(&self) -> u64 {
        let primary_offset = self.replication_offset.load(Ordering::Acquire);
        let mut max_lag = 0u64;
        for replica in self.connected_replicas.read().iter() {
            if primary_offset > replica.offset {
                let lag = primary_offset - replica.offset;
                if lag > max_lag {
                    max_lag = lag;
                }
            }
        }
        max_lag
    }

    /// Maximum age in seconds of the oldest unacked replica offset (primary
    /// only). Returns `None` if there are no connected replicas, since
    /// there is no replica to be slow.
    pub fn max_replica_ack_age_seconds(&self, threshold: Option<u64>) -> Option<u64> {
        if !matches!(self.current_role(), ReplicationRole::Primary) {
            return None;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut max_age: u64 = 0;
        let mut has_replica = false;
        for replica in self.connected_replicas.read().iter() {
            has_replica = true;
            let age_ms = now_ms.saturating_sub(replica.last_ack_ms);
            let age_secs = age_ms / 1000;
            if let Some(limit) = threshold {
                if age_secs >= limit {
                    return Some(age_secs);
                }
            }
            if age_secs > max_age {
                max_age = age_secs;
            }
        }
        if has_replica {
            Some(max_age)
        } else {
            None
        }
    }

    /// The current configured primary target if this instance is a replica.
    pub fn replica_of(&self) -> Option<(String, u16)> {
        self.replica_of.read().clone()
    }

    /// True when this replica is currently connected to its primary.
    pub fn primary_link_up(&self) -> bool {
        self.primary_link_up.load(Ordering::Acquire)
    }

    /// Broadcast a `REPLCONF GETACK *` so replicas report their offset.
    pub fn request_acks(&self) {
        const GETACK: &[u8] = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        if let Some(ref tx) = self.propagate_tx {
            let _ = tx.send(bytes::Bytes::from_static(GETACK));
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
