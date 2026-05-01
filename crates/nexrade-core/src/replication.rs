//! Primary/replica replication state.
//!
//! This module holds the in-memory state used by both primaries and replicas
//! to perform Redis-compatible PSYNC-based replication.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

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
    /// Whether the connection to the primary is currently active (replica only).
    pub primary_link_up: AtomicBool,
    /// Monotonically increasing replica ID counter.
    next_replica_id: AtomicU64,
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
            primary_link_up: AtomicBool::new(false),
            next_replica_id: AtomicU64::new(1),
        })
    }

    /// Return the current role (takes a read lock).
    pub fn current_role(&self) -> ReplicationRole {
        self.role.read().clone()
    }

    /// Returns `true` when this server is currently acting as a replica.
    pub fn is_replica(&self) -> bool {
        *self.role.read() == ReplicationRole::Replica
    }

    /// Subscribe a new receiver to the write-propagation broadcast channel.
    pub fn subscribe_propagation(&self) -> Option<broadcast::Receiver<bytes::Bytes>> {
        self.propagate_tx.as_ref().map(|tx| tx.subscribe())
    }

    /// Add a replica to the connected list; returns the assigned replica ID.
    pub fn register_replica(&self, addr: SocketAddr) -> u64 {
        let id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        self.connected_replicas.write().push(ReplicaInfo {
            id,
            addr,
            offset: 0,
        });
        id
    }

    /// Remove a replica from the connected list.
    pub fn unregister_replica(&self, id: u64) {
        self.connected_replicas.write().retain(|r| r.id != id);
    }

    /// Update the acknowledged offset for a replica.
    pub fn update_replica_offset(&self, addr: SocketAddr, offset: u64) {
        let mut replicas = self.connected_replicas.write();
        for r in replicas.iter_mut() {
            if r.addr == addr {
                r.offset = offset;
                return;
            }
        }
    }

    /// Generate a random 40-character hex replication ID.
    pub fn generate_replication_id() -> String {
        use uuid::Uuid;
        // Two UUIDs give 256 bits; we take the first 40 hex characters.
        let a = Uuid::new_v4().simple().to_string();
        let b = Uuid::new_v4().simple().to_string();
        let combined = format!("{}{}", a, b);
        combined[..40].to_string()
    }
}
