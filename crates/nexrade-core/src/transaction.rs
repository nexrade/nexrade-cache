//! MULTI/EXEC/WATCH transaction support.

use crate::resp::Resp;

/// State of a client's transaction.
#[derive(Debug, Default, Clone)]
pub struct Transaction {
    /// Are we inside a MULTI block?
    pub active: bool,
    /// Queued commands (raw RESP arrays)
    pub queue: Vec<Vec<Resp>>,
    /// Were there any command errors while queuing?
    pub error: bool,
    /// Keys registered with WATCH, stored as (key, version_at_watch_time).
    /// If any key's version has advanced by EXEC time, the transaction aborts.
    pub watch_keys: Vec<(Vec<u8>, u64)>,
}

impl Transaction {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(&mut self) {
        self.active = true;
        self.queue.clear();
        self.error = false;
        // Intentionally preserve watch_keys — WATCH persists across MULTI.
    }

    /// Full reset: clears queue, watch state, and transaction flag.
    pub fn discard(&mut self) {
        self.active = false;
        self.queue.clear();
        self.error = false;
        self.watch_keys.clear();
    }

    pub fn queue(&mut self, cmd: Vec<Resp>) {
        self.queue.push(cmd);
    }

    pub fn mark_error(&mut self) {
        self.error = true;
    }

    /// Maximum number of keys a single connection may WATCH at once.
    pub const MAX_WATCH_KEYS: usize = 4096;

    /// Record a key + its current version for optimistic locking.
    /// Returns false if the watch limit is exceeded.
    pub fn watch(&mut self, key: Vec<u8>, version: u64) -> bool {
        if self.watch_keys.len() >= Self::MAX_WATCH_KEYS {
            return false;
        }
        self.watch_keys.push((key, version));
        true
    }

    /// Drop all watched keys (UNWATCH or after EXEC/DISCARD).
    pub fn unwatch(&mut self) {
        self.watch_keys.clear();
    }
}
