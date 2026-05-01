//! The central in-memory key-value store.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

use crate::db::MaxMemoryPolicy;
use crate::expiry::Expiry;
use crate::types::DataType;

/// A single entry in the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub value: DataType,
    pub expiry: Option<Expiry>,
    /// Unix seconds of last access, used for LRU eviction.
    #[serde(default)]
    pub lru_clock: u32,
}

fn lru_now() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

impl Entry {
    pub fn new(value: DataType) -> Self {
        Self {
            value,
            expiry: None,
            lru_clock: lru_now(),
        }
    }

    pub fn with_expiry(value: DataType, expiry: Expiry) -> Self {
        Self {
            value,
            expiry: Some(expiry),
            lru_clock: lru_now(),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expiry.as_ref().is_some_and(|e| e.is_expired())
    }
}

/// The inner mutable state (one per logical database).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Database {
    pub entries: HashMap<Vec<u8>, Entry>,
    /// Monotonically increasing write counter per key — used by WATCH to detect
    /// concurrent modifications. Not persisted (watch state is per-connection).
    #[serde(skip, default)]
    key_versions: HashMap<Vec<u8>, u64>,
    /// Sorted expiry index: (expires_at_ms, key). Enables O(k log n) active
    /// expiry instead of random HashMap scanning. Not persisted — rebuilt
    /// on insert.
    #[serde(skip, default)]
    expiry_index: BTreeSet<(u128, Vec<u8>)>,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get an entry, performing lazy expiry deletion and updating LRU clock.
    pub fn get(&mut self, key: &[u8]) -> Option<&Entry> {
        if let Some(entry) = self.entries.get(key) {
            if entry.is_expired() {
                self.remove(key);
                trace!("lazy-expired key {:?}", String::from_utf8_lossy(key));
                return None;
            }
        }
        if let Some(entry) = self.entries.get_mut(key) {
            entry.lru_clock = lru_now();
        }
        self.entries.get(key)
    }

    /// Read-only get: checks expiry without removing the key.
    /// Use this with a read lock; expired keys are treated as absent.
    pub fn get_ro(&self, key: &[u8]) -> Option<&Entry> {
        match self.entries.get(key) {
            Some(entry) if !entry.is_expired() => Some(entry),
            _ => None,
        }
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        if let Some(entry) = self.entries.get(key) {
            if entry.is_expired() {
                self.remove(key);
                return None;
            }
        }
        self.entries.get_mut(key)
    }

    /// Returns the current write version for a key (0 if never written).
    pub fn key_version(&self, key: &[u8]) -> u64 {
        self.key_versions.get(key).copied().unwrap_or(0)
    }

    pub fn insert(&mut self, key: Vec<u8>, entry: Entry) {
        // Remove old expiry from index if the key already existed.
        if let Some(old) = self.entries.get(&key) {
            if let Some(ref e) = old.expiry {
                self.expiry_index.remove(&(e.expires_at_ms, key.clone()));
            }
        }
        // Add new expiry to index.
        if let Some(ref e) = entry.expiry {
            self.expiry_index.insert((e.expires_at_ms, key.clone()));
        }
        *self.key_versions.entry(key.clone()).or_insert(0) += 1;
        self.entries.insert(key, entry);
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        let removed = self.entries.remove(key);
        if let Some(ref e) = removed {
            if let Some(ref exp) = e.expiry {
                self.expiry_index.remove(&(exp.expires_at_ms, key.to_vec()));
            }
            *self.key_versions.entry(key.to_vec()).or_insert(0) += 1;
        }
        removed
    }

    pub fn contains_key(&mut self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Active expiry scan — removes up to `max` expired keys in O(k log n).
    /// Uses the sorted expiry index to find the earliest-expiring keys first,
    /// so old expired keys are never missed regardless of database size.
    pub fn expire_batch(&mut self, max: usize) -> usize {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        // Collect expired entries from the front of the sorted index.
        let to_delete: Vec<(u128, Vec<u8>)> = self
            .expiry_index
            .iter()
            .take_while(|(exp_ms, _)| *exp_ms <= now_ms)
            .take(max)
            .cloned()
            .collect();

        let count = to_delete.len();
        for (exp_ms, key) in to_delete {
            self.entries.remove(&key);
            self.expiry_index.remove(&(exp_ms, key));
        }
        count
    }

    /// Register an expiry for an existing entry (e.g. after EXPIRE command).
    /// Keeps the expiry index in sync when the expiry changes after insert.
    pub fn set_expiry(&mut self, key: &[u8], expiry: Option<Expiry>) {
        // Remove old entry from index.
        if let Some(old) = self.entries.get(key) {
            if let Some(ref e) = old.expiry {
                self.expiry_index.remove(&(e.expires_at_ms, key.to_vec()));
            }
        }
        // Add new entry to index and update entry.
        if let Some(entry) = self.entries.get_mut(key) {
            if let Some(ref e) = expiry {
                self.expiry_index.insert((e.expires_at_ms, key.to_vec()));
            }
            entry.expiry = expiry;
        }
    }

    /// Rough memory estimate for all live entries (bytes).
    pub fn estimated_memory_bytes(&self) -> usize {
        self.entries
            .iter()
            .map(|(k, e)| {
                let value_sz = match &e.value {
                    DataType::String(v) => v.len(),
                    DataType::List(l) => l.iter().map(|b| b.len()).sum(),
                    DataType::Set(s) => s.iter().map(|v| v.len()).sum(),
                    DataType::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len()).sum(),
                    DataType::Bitmap(v) => v.len(),
                    DataType::HyperLogLog(v) => v.len(),
                    DataType::ZSet(z) => z.members.keys().map(|m| m.len() + 8).sum(),
                    DataType::Stream(s) => s.estimated_size(),
                    DataType::Geo(g) => g.members.len() * 24,
                };
                64 + k.len() + value_sz
            })
            .sum()
    }

    /// Evict one entry according to `policy`. Returns true if an entry was removed.
    pub fn evict_one(&mut self, policy: &MaxMemoryPolicy) -> bool {
        match policy {
            MaxMemoryPolicy::NoEviction => false,

            MaxMemoryPolicy::AllKeysRandom => {
                let key = self.entries.keys().next().cloned();
                if let Some(k) = key {
                    self.remove(&k);
                    true
                } else {
                    false
                }
            }

            MaxMemoryPolicy::VolatileRandom => {
                let key = self
                    .entries
                    .iter()
                    .find(|(_, e)| e.expiry.is_some() && !e.is_expired())
                    .map(|(k, _)| k.clone());
                if let Some(k) = key {
                    self.remove(&k);
                    true
                } else {
                    false
                }
            }

            MaxMemoryPolicy::AllKeysLru => {
                // Pick the entry with the smallest (oldest) lru_clock.
                let key = self
                    .entries
                    .iter()
                    .min_by_key(|(_, e)| e.lru_clock)
                    .map(|(k, _)| k.clone());
                if let Some(k) = key {
                    self.remove(&k);
                    true
                } else {
                    false
                }
            }

            MaxMemoryPolicy::VolatileLru => {
                let key = self
                    .entries
                    .iter()
                    .filter(|(_, e)| e.expiry.is_some() && !e.is_expired())
                    .min_by_key(|(_, e)| e.lru_clock)
                    .map(|(k, _)| k.clone());
                if let Some(k) = key {
                    self.remove(&k);
                    true
                } else {
                    false
                }
            }

            MaxMemoryPolicy::VolatileTtl => {
                // Use the expiry_index — the first entry is the one expiring soonest.
                let key = self.expiry_index.iter().next().map(|(_, k)| k.clone());
                if let Some(k) = key {
                    self.remove(&k);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Keys matching a glob pattern.
    pub fn keys_matching(&mut self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(k, e)| {
                if e.is_expired() {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();
        for k in expired {
            self.remove(&k);
        }

        self.entries
            .keys()
            .filter(|k| glob_match(pattern, k))
            .cloned()
            .collect()
    }
}

// ── Multi-core sharding ───────────────────────────────────────────────────────

/// FNV-1a 64-bit hash for shard selection — fast, no external deps.
#[inline]
fn fnv1a(key: &[u8]) -> usize {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut h = OFFSET;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h as usize
}

/// Number of shards: next power-of-two ≥ CPU count, clamped to [16, 64].
fn compute_num_shards() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    cpus.next_power_of_two().clamp(16, 64)
}

/// Per-logical-database sharded store.
///
/// Each incoming key is routed to one of `num_shards` shards via FNV-1a
/// hash, so concurrent writes to different keys never contend.
pub struct ShardedDatabase {
    shards: Vec<RwLock<Database>>,
    num_shards: usize,
}

impl ShardedDatabase {
    pub fn new(num_shards: usize) -> Self {
        Self {
            shards: (0..num_shards)
                .map(|_| RwLock::new(Database::new()))
                .collect(),
            num_shards,
        }
    }

    #[inline]
    pub fn shard_idx(&self, key: &[u8]) -> usize {
        fnv1a(key) & (self.num_shards - 1)
    }

    pub fn shard_count(&self) -> usize {
        self.num_shards
    }

    /// Acquire an exclusive write guard scoped to the shard that owns `key`.
    #[inline]
    pub fn write_for(&self, key: &[u8]) -> parking_lot::RwLockWriteGuard<'_, Database> {
        self.shards[self.shard_idx(key)].write()
    }

    /// Acquire a shared read guard scoped to the shard that owns `key`.
    #[inline]
    pub fn read_for(&self, key: &[u8]) -> parking_lot::RwLockReadGuard<'_, Database> {
        self.shards[self.shard_idx(key)].read()
    }

    /// Direct shard access by index (for iteration-based commands).
    pub fn shard_write(&self, idx: usize) -> parking_lot::RwLockWriteGuard<'_, Database> {
        self.shards[idx].write()
    }

    pub fn shard_read(&self, idx: usize) -> parking_lot::RwLockReadGuard<'_, Database> {
        self.shards[idx].read()
    }

    // ── Whole-database helpers ────────────────────────────────────────────────

    pub fn flush(&self) {
        for shard in &self.shards {
            let mut s = shard.write();
            s.entries.clear();
            s.expiry_index.clear();
            s.key_versions.clear();
        }
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.read().is_empty())
    }

    pub fn expiry_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.read().expiry_index.len())
            .sum()
    }

    pub fn expire_batch(&self, max_total: usize) -> usize {
        let per_shard = (max_total / self.num_shards).max(1);
        self.shards
            .iter()
            .map(|s| s.write().expire_batch(per_shard))
            .sum()
    }

    pub fn keys_matching(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for shard in &self.shards {
            keys.extend(shard.write().keys_matching(pattern));
        }
        keys
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.read().estimated_memory_bytes())
            .sum()
    }

    pub fn evict_one(&self, policy: &MaxMemoryPolicy) -> bool {
        for shard in &self.shards {
            if shard.write().evict_one(policy) {
                return true;
            }
        }
        false
    }

    /// Clone all shard data into a single merged `Database` (for RDB snapshots).
    pub fn snapshot(&self) -> Database {
        let mut db = Database::new();
        for shard in &self.shards {
            let guard = shard.read();
            for (k, v) in &guard.entries {
                if let Some(ref e) = v.expiry {
                    db.expiry_index.insert((e.expires_at_ms, k.clone()));
                }
                db.entries.insert(k.clone(), v.clone());
            }
            for (k, ver) in &guard.key_versions {
                db.key_versions.insert(k.clone(), *ver);
            }
        }
        db
    }

    /// Return the first live key found across all shards (for RANDOMKEY).
    pub fn random_key(&self) -> Option<Vec<u8>> {
        for shard in &self.shards {
            let guard = shard.read();
            if let Some(k) = guard.entries.keys().next().cloned() {
                return Some(k);
            }
        }
        None
    }

    // ── Cross-shard atomic operations ─────────────────────────────────────────

    /// Lock two shards in a canonical order to prevent deadlocks.
    /// Returns `(src_guard, dst_guard)` or a single guard when both keys
    /// map to the same shard.
    fn lock_two(
        &self,
        si: usize,
        di: usize,
    ) -> (
        parking_lot::RwLockWriteGuard<'_, Database>,
        Option<parking_lot::RwLockWriteGuard<'_, Database>>,
        bool, // true when si > di (guards are swapped)
    ) {
        if si == di {
            (self.shards[si].write(), None, false)
        } else {
            let swapped = si > di;
            let (lo, hi) = if swapped { (di, si) } else { (si, di) };
            let lo_g = self.shards[lo].write();
            let hi_g = self.shards[hi].write();
            (lo_g, Some(hi_g), swapped)
        }
    }

    /// RENAME src → dst (atomic within the shard pair).
    pub fn rename(&self, src: &[u8], dst: Vec<u8>) -> Option<()> {
        let si = self.shard_idx(src);
        let di = self.shard_idx(&dst);
        let (mut lo_g, hi_opt, swapped) = self.lock_two(si, di);

        if let Some(mut hi_g) = hi_opt {
            let (src_s, dst_s) = if swapped {
                (&mut *hi_g, &mut *lo_g)
            } else {
                (&mut *lo_g, &mut *hi_g)
            };
            let entry = src_s.remove(src)?;
            dst_s.insert(dst, entry);
        } else {
            let entry = lo_g.remove(src)?;
            lo_g.insert(dst, entry);
        }
        Some(())
    }

    /// RENAMENX src → dst (atomic; returns 1 if renamed, 0 if dst exists).
    pub fn renamenx(&self, src: &[u8], dst: Vec<u8>) -> Option<i64> {
        let si = self.shard_idx(src);
        let di = self.shard_idx(&dst);
        let (mut lo_g, hi_opt, swapped) = self.lock_two(si, di);

        if let Some(mut hi_g) = hi_opt {
            let (src_s, dst_s) = if swapped {
                (&mut *hi_g, &mut *lo_g)
            } else {
                (&mut *lo_g, &mut *hi_g)
            };
            if dst_s.contains_key(&dst) {
                return Some(0);
            }
            let entry = src_s.remove(src)?;
            dst_s.insert(dst, entry);
        } else {
            if lo_g.contains_key(&dst) {
                return Some(0);
            }
            let entry = lo_g.remove(src)?;
            lo_g.insert(dst, entry);
        }
        Some(1)
    }

    /// COPY src → dst (atomic; returns true if copied).
    pub fn copy_entry(&self, src: &[u8], dst: Vec<u8>, replace: bool) -> bool {
        let si = self.shard_idx(src);
        let di = self.shard_idx(&dst);
        let (mut lo_g, hi_opt, swapped) = self.lock_two(si, di);

        if let Some(mut hi_g) = hi_opt {
            let (src_s, dst_s) = if swapped {
                (&mut *hi_g, &mut *lo_g)
            } else {
                (&mut *lo_g, &mut *hi_g)
            };
            if !replace && dst_s.contains_key(&dst) {
                return false;
            }
            let entry = match src_s.get(src) {
                Some(e) => e.clone(),
                None => return false,
            };
            dst_s.insert(dst, entry);
        } else {
            if !replace && lo_g.contains_key(&dst) {
                return false;
            }
            let entry = match lo_g.get(src) {
                Some(e) => e.clone(),
                None => return false,
            };
            lo_g.insert(dst, entry);
        }
        true
    }

    /// LMOVE / RPOPLPUSH: pop from one end of src list, push to one end of dst.
    pub fn lmove_atomic(
        &self,
        src: &[u8],
        dst: Vec<u8>,
        from_left: bool,
        to_left: bool,
    ) -> Option<bytes::Bytes> {
        use crate::types::DataType;
        let si = self.shard_idx(src);
        let di = self.shard_idx(&dst);
        let (mut lo_g, hi_opt, swapped) = self.lock_two(si, di);

        // Helper: pop from a list
        fn pop_list(s: &mut Database, key: &[u8], from_left: bool) -> Option<bytes::Bytes> {
            let entry = s.get_mut(key)?;
            match &mut entry.value {
                DataType::List(l) => {
                    let val = if from_left {
                        l.pop_front()?
                    } else {
                        l.pop_back()?
                    };
                    Some(val)
                }
                _ => None,
            }
        }

        // Helper: push to a list (creating it if absent)
        fn push_list(s: &mut Database, key: Vec<u8>, val: bytes::Bytes, to_left: bool) {
            let entry = s
                .entries
                .entry(key.clone())
                .or_insert_with(|| Entry::new(DataType::List(std::collections::VecDeque::new())));
            if let DataType::List(l) = &mut entry.value {
                if to_left {
                    l.push_front(val);
                } else {
                    l.push_back(val);
                }
            }
            // keep key_versions in sync
            *s.key_versions.entry(key).or_insert(0) += 1;
        }

        if let Some(mut hi_g) = hi_opt {
            let (src_s, dst_s) = if swapped {
                (&mut *hi_g, &mut *lo_g)
            } else {
                (&mut *lo_g, &mut *hi_g)
            };
            let val = pop_list(src_s, src, from_left)?;
            push_list(dst_s, dst, val.clone(), to_left);
            Some(val)
        } else {
            let val = pop_list(&mut lo_g, src, from_left)?;
            push_list(&mut lo_g, dst, val.clone(), to_left);
            Some(val)
        }
    }

    /// SMOVE src dst member — atomically move a set member between sets.
    pub fn smove_atomic(&self, src: &[u8], dst: Vec<u8>, member: Vec<u8>) -> i64 {
        use crate::types::DataType;
        let si = self.shard_idx(src);
        let di = self.shard_idx(&dst);
        let (mut lo_g, hi_opt, swapped) = self.lock_two(si, di);

        if let Some(mut hi_g) = hi_opt {
            let (src_s, dst_s) = if swapped {
                (&mut *hi_g, &mut *lo_g)
            } else {
                (&mut *lo_g, &mut *hi_g)
            };
            let removed = match src_s.get_mut(src) {
                Some(e) => match &mut e.value {
                    DataType::Set(s) => s.remove(&member),
                    _ => return 0,
                },
                None => return 0,
            };
            if !removed {
                return 0;
            }
            let dst_entry = dst_s
                .entries
                .entry(dst.clone())
                .or_insert_with(|| Entry::new(DataType::Set(std::collections::HashSet::new())));
            if let DataType::Set(s) = &mut dst_entry.value {
                s.insert(member);
            }
            *dst_s.key_versions.entry(dst).or_insert(0) += 1;
            1
        } else {
            let removed = match lo_g.get_mut(src) {
                Some(e) => match &mut e.value {
                    DataType::Set(s) => s.remove(&member),
                    _ => return 0,
                },
                None => return 0,
            };
            if !removed {
                return 0;
            }
            let dst_entry = lo_g
                .entries
                .entry(dst.clone())
                .or_insert_with(|| Entry::new(DataType::Set(std::collections::HashSet::new())));
            if let DataType::Set(s) = &mut dst_entry.value {
                s.insert(member);
            }
            *lo_g.key_versions.entry(dst).or_insert(0) += 1;
            1
        }
    }

    /// Acquire write guards for all distinct shards in `shard_indices` (must be
    /// sorted and deduped). Returns guards in the same order.
    fn lock_shards(
        &self,
        shard_indices: &[usize],
    ) -> Vec<parking_lot::RwLockWriteGuard<'_, Database>> {
        shard_indices
            .iter()
            .map(|&i| self.shards[i].write())
            .collect()
    }

    /// Collect, sort, and deduplicate shard indices for a set of keys.
    fn shard_indices_for(&self, pairs: &[(Vec<u8>, Entry)]) -> Vec<usize> {
        let mut indices: Vec<usize> = pairs.iter().map(|(k, _)| self.shard_idx(k)).collect();
        indices.sort_unstable();
        indices.dedup();
        indices
    }

    pub fn mset(&self, pairs: Vec<(Vec<u8>, Entry)>) {
        let si = self.shard_indices_for(&pairs);
        let mut guards = self.lock_shards(&si);
        for (key, entry) in pairs {
            let pos = si.binary_search(&self.shard_idx(&key)).unwrap();
            guards[pos].insert(key, entry);
        }
    }

    /// MSETNX: set all key-value pairs only when NONE of the keys exist.
    pub fn msetnx(&self, pairs: Vec<(Vec<u8>, Entry)>) -> bool {
        let si = self.shard_indices_for(&pairs);
        let mut guards = self.lock_shards(&si);

        for (key, _) in &pairs {
            let pos = si.binary_search(&self.shard_idx(key)).unwrap();
            if guards[pos].get_ro(key).is_some() {
                return false;
            }
        }

        for (key, entry) in pairs {
            let pos = si.binary_search(&self.shard_idx(&key)).unwrap();
            guards[pos].insert(key, entry);
        }
        true
    }
}

// ── Thread-safe multi-database store ─────────────────────────────────────────

/// Thread-safe multi-database store backed by per-key sharding.
#[derive(Clone)]
pub struct Store {
    dbs: Arc<Vec<ShardedDatabase>>,
    pub db_count: usize,
}

impl Store {
    pub fn new(db_count: usize) -> Self {
        let num_shards = compute_num_shards();
        let dbs = (0..db_count)
            .map(|_| ShardedDatabase::new(num_shards))
            .collect();
        Self {
            dbs: Arc::new(dbs),
            db_count,
        }
    }

    pub fn db(&self, index: usize) -> &ShardedDatabase {
        &self.dbs[index % self.db_count]
    }

    /// Flush a specific database.
    pub fn flush_db(&self, index: usize) {
        self.db(index).flush();
        debug!("flushed database {}", index);
    }

    /// Flush all databases.
    pub fn flush_all(&self) {
        for db in self.dbs.iter() {
            db.flush();
        }
        debug!("flushed all databases");
    }

    /// Total key count across all databases.
    pub fn total_keys(&self) -> usize {
        self.dbs.iter().map(|db| db.len()).sum()
    }

    /// Snapshot all non-empty databases (merges shards under read locks).
    pub fn snapshot_dbs(&self) -> Vec<(usize, Database)> {
        (0..self.db_count)
            .filter_map(|i| {
                let sdb = self.db(i);
                if sdb.is_empty() {
                    None
                } else {
                    Some((i, sdb.snapshot()))
                }
            })
            .collect()
    }

    /// Total estimated memory across all databases (rough approximation).
    pub fn estimated_memory_bytes(&self) -> usize {
        self.dbs.iter().map(|db| db.estimated_memory_bytes()).sum()
    }

    /// Evict entries until memory is below `limit_bytes`.
    pub fn evict_if_needed(&self, policy: &MaxMemoryPolicy, limit_bytes: usize) -> usize {
        if *policy == MaxMemoryPolicy::NoEviction || limit_bytes == 0 {
            return 0;
        }
        let mut evicted = 0;
        while self.estimated_memory_bytes() > limit_bytes {
            let mut any = false;
            for db in self.dbs.iter() {
                if db.evict_one(policy) {
                    evicted += 1;
                    any = true;
                    break;
                }
            }
            if !any {
                break;
            }
        }
        evicted
    }

    /// Run active expiry on all databases.
    pub fn active_expire(&self, max_per_db: usize) {
        for db in self.dbs.iter() {
            db.expire_batch(max_per_db);
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new(16)
    }
}

/// Simple glob pattern matcher (supports * and ?).
pub fn glob_match(pattern: &[u8], s: &[u8]) -> bool {
    glob_match_inner(pattern, s)
}

fn glob_match_inner(pat: &[u8], s: &[u8]) -> bool {
    let mut pi = 0usize;
    let mut si = 0usize;
    let mut star_pi = usize::MAX;
    let mut star_si = 0usize;

    while si < s.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == s[si]) {
            pi += 1;
            si += 1;
        } else if pi < pat.len() && pat[pi] == b'*' {
            star_pi = pi;
            star_si = si;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"foo*", b"foobar"));
        assert!(glob_match(b"*bar", b"foobar"));
        assert!(glob_match(b"f?o", b"foo"));
        assert!(!glob_match(b"f?o", b"fo"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(!glob_match(b"h?llo", b"hllo"));
    }

    #[test]
    fn test_expiry() {
        let mut db = Database::new();
        let entry = Entry::with_expiry(
            DataType::String(b"val".to_vec()),
            Expiry::from_duration(Duration::from_millis(1)),
        );
        db.insert(b"key".to_vec(), entry);
        std::thread::sleep(Duration::from_millis(5));
        assert!(db.get(b"key").is_none());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    fn str_entry(v: &[u8]) -> Entry {
        Entry::new(DataType::String(v.to_vec()))
    }

    fn set_entry(members: &[&[u8]]) -> Entry {
        Entry::new(DataType::Set(
            members.iter().map(|s| s.to_vec()).collect::<HashSet<_>>(),
        ))
    }

    /// Find two keys that land on different shards.
    fn two_different_shards(sdb: &ShardedDatabase) -> (Vec<u8>, Vec<u8>) {
        let a: Vec<u8> = b"__alpha__".to_vec();
        let sa = sdb.shard_idx(&a);
        for n in 0u64.. {
            let b = format!("__beta_{:016x}__", n).into_bytes();
            if sdb.shard_idx(&b) != sa {
                return (a, b);
            }
        }
        unreachable!()
    }

    fn set_member_count(sdb: &ShardedDatabase, key: &[u8], member: &[u8]) -> usize {
        match sdb.read_for(key).get_ro(key) {
            Some(e) => match &e.value {
                DataType::Set(s) => usize::from(s.contains(member)),
                _ => 0,
            },
            None => 0,
        }
    }

    fn list_len(sdb: &ShardedDatabase, key: &[u8]) -> usize {
        match sdb.read_for(key).get_ro(key) {
            Some(e) => match &e.value {
                DataType::List(l) => l.len(),
                _ => 0,
            },
            None => 0,
        }
    }

    // ── concurrent unique writes — no data loss ───────────────────────────────
    // N threads each write to disjoint keys. Every key must exist afterwards.
    #[test]
    fn concurrent_unique_writes_no_loss() {
        const THREADS: usize = 8;
        const KEYS_PER: usize = 5_000;
        let sdb = Arc::new(ShardedDatabase::new(16));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let sdb = Arc::clone(&sdb);
                thread::spawn(move || {
                    for i in 0..KEYS_PER {
                        let key = format!("t{}k{}", t, i).into_bytes();
                        sdb.write_for(&key).insert(key.clone(), str_entry(b"v"));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            sdb.len(),
            THREADS * KEYS_PER,
            "every written key must be present"
        );
    }

    // ── RENAME: no key loss ───────────────────────────────────────────────────
    // Two threads alternately rename A→B and B→A.
    // At all times exactly one of the two names must hold the value.
    #[test]
    fn rename_no_key_loss() {
        let sdb = Arc::new(ShardedDatabase::new(16));
        let (a, b) = two_different_shards(&sdb);
        sdb.write_for(&a).insert(a.clone(), str_entry(b"v"));

        const ITERS: usize = 40_000;

        let sdb2 = Arc::clone(&sdb);
        let (a2, b2) = (a.clone(), b.clone());
        let t1 = thread::spawn(move || {
            for _ in 0..ITERS {
                if sdb2.rename(&a2, b2.clone()).is_none() {
                    sdb2.rename(&b2, a2.clone());
                }
            }
        });

        let sdb3 = Arc::clone(&sdb);
        let (a3, b3) = (a.clone(), b.clone());
        let t2 = thread::spawn(move || {
            for _ in 0..ITERS {
                if sdb3.rename(&b3, a3.clone()).is_none() {
                    sdb3.rename(&a3, b3.clone());
                }
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let a_live = sdb.read_for(&a).get_ro(&a).is_some();
        let b_live = sdb.read_for(&b).get_ro(&b).is_some();
        assert!(
            a_live ^ b_live,
            "exactly one of the two keys must exist after concurrent rename: a={a_live} b={b_live}"
        );
    }

    // ── RENAME: no deadlock ───────────────────────────────────────────────────
    // Two threads rename in opposite directions simultaneously.
    // lock_two acquires shards in index order → no deadlock.
    #[test]
    fn rename_no_deadlock() {
        let sdb = Arc::new(ShardedDatabase::new(16));
        let (a, b) = two_different_shards(&sdb);
        sdb.write_for(&a).insert(a.clone(), str_entry(b"v1"));
        sdb.write_for(&b).insert(b.clone(), str_entry(b"v2"));

        const ITERS: usize = 100_000;

        let sdb2 = Arc::clone(&sdb);
        let (a2, b2) = (a.clone(), b.clone());
        let t1 = thread::spawn(move || {
            for _ in 0..ITERS {
                let _ = sdb2.rename(&a2, b2.clone());
            }
        });

        let sdb3 = Arc::clone(&sdb);
        let (a3, b3) = (a.clone(), b.clone());
        let t2 = thread::spawn(move || {
            for _ in 0..ITERS {
                let _ = sdb3.rename(&b3, a3.clone());
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
        // Reaching here means no deadlock occurred.
    }

    // ── SMOVE: member conservation ────────────────────────────────────────────
    // Thread 1 moves "x" from set_a → set_b; thread 2 moves it back.
    // The member must never appear in both sets simultaneously, and the total
    // count across both sets is always ≤ 1.
    #[test]
    fn smove_member_conservation() {
        let sdb = Arc::new(ShardedDatabase::new(16));
        let sa: Vec<u8> = b"set:alpha".to_vec();
        let sb: Vec<u8> = b"set:beta".to_vec();
        let m = b"x".to_vec();

        sdb.write_for(&sa).insert(sa.clone(), set_entry(&[b"x"]));

        const ITERS: usize = 50_000;

        let sdb2 = Arc::clone(&sdb);
        let (sa2, sb2, m2) = (sa.clone(), sb.clone(), m.clone());
        let t1 = thread::spawn(move || {
            for _ in 0..ITERS {
                sdb2.smove_atomic(&sa2, sb2.clone(), m2.clone());
            }
        });

        let sdb3 = Arc::clone(&sdb);
        let (sa3, sb3, m3) = (sa.clone(), sb.clone(), m.clone());
        let t2 = thread::spawn(move || {
            for _ in 0..ITERS {
                sdb3.smove_atomic(&sb3, sa3.clone(), m3.clone());
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let in_a = set_member_count(&sdb, &sa, &m);
        let in_b = set_member_count(&sdb, &sb, &m);
        assert!(
            in_a + in_b <= 1,
            "member must not be in both sets: in_a={in_a} in_b={in_b}"
        );
    }

    // ── LMOVE: element count conservation ────────────────────────────────────
    // Fill src with N items. N threads each pop one item and push to dst.
    // src_len + dst_len must equal N at the end.
    #[test]
    fn lmove_element_conservation() {
        const THREADS: usize = 4;
        const ITEMS: usize = 1_000; // each thread pops ITEMS elements
        let sdb = Arc::new(ShardedDatabase::new(16));
        let src: Vec<u8> = b"list:src".to_vec();
        let dst: Vec<u8> = b"list:dst".to_vec();

        {
            let items: VecDeque<Bytes> = (0..ITEMS * THREADS)
                .map(|i| Bytes::copy_from_slice(i.to_string().as_bytes()))
                .collect();
            sdb.write_for(&src)
                .insert(src.clone(), Entry::new(DataType::List(items)));
        }

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let sdb = Arc::clone(&sdb);
                let (src, dst) = (src.clone(), dst.clone());
                thread::spawn(move || {
                    for _ in 0..ITEMS {
                        sdb.lmove_atomic(&src, dst.clone(), true, false);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let total = list_len(&sdb, &src) + list_len(&sdb, &dst);
        assert_eq!(
            total,
            ITEMS * THREADS,
            "total list length must be conserved: got {total}"
        );
    }

    // ── MSETNX: exactly one winner ────────────────────────────────────────────
    // N threads all call MSETNX on the same key. Exactly one must succeed.
    #[test]
    fn msetnx_exactly_one_winner() {
        const THREADS: usize = 8;
        let sdb = Arc::new(ShardedDatabase::new(16));
        let key = b"contest".to_vec();
        let wins = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let sdb = Arc::clone(&sdb);
                let wins = Arc::clone(&wins);
                let key = key.clone();
                thread::spawn(move || {
                    let val = format!("thread-{t}").into_bytes();
                    if sdb.msetnx(vec![(key, str_entry(&val))]) {
                        wins.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            wins.load(Ordering::Relaxed),
            1,
            "exactly one MSETNX must succeed"
        );
    }

    // ── MSETNX: multi-key, all-or-nothing ─────────────────────────────────────
    // Two threads each try to claim a pair of keys where the pairs overlap.
    // The total unique keys written must equal the winning pair size (2).
    #[test]
    fn msetnx_multi_key_all_or_nothing() {
        let sdb = Arc::new(ShardedDatabase::new(16));
        let k1 = b"k1".to_vec();
        let k2 = b"k2".to_vec();
        let k3 = b"k3".to_vec();

        let wins = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..20)
            .map(|t| {
                let sdb = Arc::clone(&sdb);
                let wins = Arc::clone(&wins);
                let (k1, k2, k3) = (k1.clone(), k2.clone(), k3.clone());
                thread::spawn(move || {
                    // Even threads claim k1+k2, odd threads claim k2+k3.
                    // k2 is shared — only one group should win.
                    let pairs = if t % 2 == 0 {
                        vec![(k1, str_entry(b"a")), (k2, str_entry(b"a"))]
                    } else {
                        vec![(k2, str_entry(b"b")), (k3, str_entry(b"b"))]
                    };
                    if sdb.msetnx(pairs) {
                        wins.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Exactly one group wins; after that all further attempts fail (k2 exists)
        assert_eq!(
            wins.load(Ordering::Relaxed),
            1,
            "only one MSETNX group must win"
        );
    }
}
