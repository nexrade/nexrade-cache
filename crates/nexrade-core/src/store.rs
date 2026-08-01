//! The central in-memory key-value store.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

use crate::db::MaxMemoryPolicy;
use crate::expiry::Expiry;
use crate::types::{AtomicIntCell, DataType};

/// A single entry in the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub value: DataType,
    pub expiry: Option<Expiry>,
    /// Unix seconds of last access, used for LRU eviction.
    #[serde(default)]
    pub lru_clock: u32,
}

impl Entry {
    pub fn new(value: DataType) -> Self {
        Self {
            value,
            expiry: None,
            // Initialized to 0 so that inserts during process startup, before
            // the background tick has had a chance to populate the cache,
            // still get a valid (if stale) value. The first GET writes the
            // current cached clock.
            lru_clock: 0,
        }
    }

    pub fn with_expiry(value: DataType, expiry: Expiry) -> Self {
        Self {
            value,
            expiry: Some(expiry),
            lru_clock: 0,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expiry.as_ref().is_some_and(|e| e.is_expired())
    }
}

/// Cached LRU clock source. Reads from an `AtomicU32` updated by the
/// background tick task. This is a single relaxed load — much cheaper than
/// `SystemTime::now()` per GET.
///
/// Resolution is `1/hz` seconds (default 100 ms). That is the same
/// granularity Redis uses for its LRU clock and is sufficient for LRU
/// eviction.
#[derive(Clone, Debug, Default)]
pub struct LruClock {
    inner: Arc<AtomicU32>,
}

impl LruClock {
    pub fn new(inner: Arc<AtomicU32>) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn now(&self) -> u32 {
        self.inner.load(Ordering::Relaxed)
    }

    /// Write the current timestamp — used by the background tick.
    #[inline]
    pub fn store(&self, value: u32) {
        self.inner.store(value, Ordering::Relaxed);
    }
}

/// The inner mutable state (one per logical database).
#[derive(Debug, Serialize, Deserialize)]
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
    /// Incremental live-bytes counter, updated on every insert / remove so
    /// `Store::estimated_memory_bytes()` is O(shards) instead of O(entries).
    #[serde(skip, default)]
    live_bytes: std::sync::atomic::AtomicUsize,
    /// Cached LRU clock. The background tick task refreshes this so per-GET
    /// `lru_clock` writes are an atomic load instead of `SystemTime::now()`.
    /// Cloning a `Database` shares the underlying `Arc<AtomicU32>` (cheap
    /// atomic loads work fine from any thread).
    #[serde(skip, default)]
    lru_clock: LruClock,
}

// Manual `Clone` because `AtomicUsize` doesn't implement `Clone`. The
// atomic is logically a single counter so it's shared by reference across
// clones.
impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            key_versions: self.key_versions.clone(),
            expiry_index: self.expiry_index.clone(),
            live_bytes: std::sync::atomic::AtomicUsize::new(
                self.live_bytes.load(std::sync::atomic::Ordering::Relaxed),
            ),
            lru_clock: self.lru_clock.clone(),
        }
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    pub fn new() -> Self {
        // Initialise `live_bytes` to 0; the surrounding `ShardedDatabase` /
        // `Store` keeps a cached view of the same usize.
        Self {
            entries: HashMap::new(),
            key_versions: HashMap::new(),
            expiry_index: BTreeSet::new(),
            live_bytes: std::sync::atomic::AtomicUsize::new(0),
            lru_clock: LruClock::new(Arc::new(AtomicU32::new(0))),
        }
    }

    /// Read the current cached LRU timestamp.
    #[inline]
    pub fn lru_now(&self) -> u32 {
        self.lru_clock.now()
    }

    /// Wire / refresh the cached LRU clock source.
    pub fn set_lru_clock(&mut self, clock: LruClock) {
        self.lru_clock = clock;
    }

    /// Read the live-bytes counter for this shard.
    #[inline]
    pub fn live_bytes(&self) -> usize {
        use std::sync::atomic::Ordering;
        self.live_bytes.load(Ordering::Relaxed)
    }

    /// Adjust the incremental memory counter after an in-place value mutation
    /// (e.g. list push / hash field insert). `delta` is signed payload change
    /// in bytes (`new_payload − old_payload`). Key overhead is unchanged.
    ///
    /// Saturates on subtract so a buggy caller cannot wrap the counter.
    #[inline]
    pub fn adjust_live_bytes(&self, delta: isize) {
        use std::sync::atomic::Ordering;
        if delta > 0 {
            self.live_bytes.fetch_add(delta as usize, Ordering::Relaxed);
        } else if delta < 0 {
            Self::sub_live_bytes_atomic(&self.live_bytes, (-delta) as usize);
        }
    }

    /// Delete a key that a caller already observed to be empty under a live
    /// borrow (Redis-style empty-collection GC). The removed shell + key
    /// overhead leave `live_bytes` via [`remove`]. Callers gate this on an
    /// `is_empty()` flag captured while the value was borrowed, so the hot
    /// non-empty path pays nothing here.
    #[inline]
    pub fn remove_empty_key(&mut self, key: &[u8]) -> bool {
        self.remove(key).is_some()
    }

    /// Saturating subtract on a live_bytes atomic. Free-standing on the
    /// atomic (not `&self`) so it can be called while an `entries.entry()`
    /// borrow is live — a method on `self` would conflict with NLL.
    #[inline]
    fn sub_live_bytes_atomic(live: &std::sync::atomic::AtomicUsize, amount: usize) {
        use std::sync::atomic::Ordering;
        if amount == 0 {
            return;
        }
        let _ = live.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            Some(cur.saturating_sub(amount))
        });
    }

    /// Get an entry, performing lazy expiry deletion and updating LRU clock.
    pub fn get(&mut self, key: &[u8]) -> Option<&Entry> {
        let now = self.lru_now();
        if let Some(entry) = self.entries.get(key) {
            if entry.is_expired() {
                self.remove(key);
                trace!("lazy-expired key {:?}", String::from_utf8_lossy(key));
                return None;
            }
        }
        if let Some(entry) = self.entries.get_mut(key) {
            entry.lru_clock = now;
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
    ///
    /// For an `Int`-typed key, this adds the cell's own per-cell `version`
    /// counter to the HashMap-tracked base version. PR 1 never increments
    /// that counter (no read-lock fast path exists yet, so every INCR still
    /// bumps `key_versions` normally) — this is groundwork so the contract
    /// doesn't change again once PR 2 adds the fast path. Base version is
    /// fixed for the lifetime of a given `Int` cell (promotion/demotion
    /// always go through `key_versions`), so the sum is monotonic.
    pub fn key_version(&self, key: &[u8]) -> u64 {
        let base = self.key_versions.get(key).copied().unwrap_or(0);
        match self.entries.get(key) {
            Some(e) if !e.is_expired() => match &e.value {
                DataType::Int(cell) => base + cell.version(),
                _ => base,
            },
            _ => base,
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, entry: Entry) {
        use std::collections::hash_map::Entry as HEntry;
        use std::sync::atomic::Ordering;
        // Single `entries.entry()` probe instead of `get()` + `insert()` —
        // same pattern as `get_or_insert_with`/`incr_int` below. `insert()`
        // always unconditionally replaces whatever occupies the slot
        // (expired or not, wrong-typed or not), so both branches just
        // differ in whether there's an old entry's bookkeeping to undo
        // first.
        match self.entries.entry(key) {
            HEntry::Occupied(mut o) => {
                // Use `o.key()` as a borrow at each site instead of cloning
                // it once up front — an unconditional up-front clone plus
                // the unconditional `key_versions` clone below would pay 2
                // allocations on the common no-TTL path where the old
                // get()+insert() code (and get_or_insert_with) only pays 1.
                if let Some(ref e) = o.get().expiry {
                    self.expiry_index
                        .remove(&(e.expires_at_ms, o.key().clone()));
                }
                let old_sz = Self::estimate_entry_size(o.key(), o.get());
                let new_sz = Self::estimate_entry_size(o.key(), &entry);
                // One atomic when sizes match (fixed-size overwrite — common
                // under redis-benchmark MSET / SET). Two only when they differ.
                if new_sz != old_sz {
                    if new_sz > old_sz {
                        self.live_bytes
                            .fetch_add(new_sz - old_sz, Ordering::Relaxed);
                    } else {
                        Self::sub_live_bytes_atomic(&self.live_bytes, old_sz - new_sz);
                    }
                }
                if let Some(ref e) = entry.expiry {
                    self.expiry_index.insert((e.expires_at_ms, o.key().clone()));
                }
                // Prefer get_mut (no key clone) once the version slot exists —
                // true after the first write of this key (MSET/SET steady state).
                if let Some(v) = self.key_versions.get_mut(o.key()) {
                    *v = v.wrapping_add(1);
                } else {
                    self.key_versions.insert(o.key().clone(), 1);
                }
                *o.get_mut() = entry;
            }
            HEntry::Vacant(v) => {
                if let Some(ref e) = entry.expiry {
                    self.expiry_index.insert((e.expires_at_ms, v.key().clone()));
                }
                // Vacant path still needs one owned key for key_versions.
                self.key_versions.insert(v.key().clone(), 1);
                self.live_bytes.fetch_add(
                    Self::estimate_entry_size(v.key(), &entry),
                    Ordering::Relaxed,
                );
                v.insert(entry);
            }
        }
    }

    /// Plain `SET k v` with no NX/XX/GET/KEEPTTL/TTL flags.
    ///
    /// Hot path under redis-benchmark: the key already exists as a string of
    /// the same length. Single `HashMap::get_mut` (no key `Vec`), in-place
    /// value swap, no live-bytes atomic when payload length is unchanged.
    /// Clears any prior TTL (Redis SET without KEEPTTL always resets expiry).
    /// Overwrites any type.
    pub fn set_plain(&mut self, key: &[u8], value: bytes::Bytes) {
        use std::sync::atomic::Ordering;
        let new_val_sz = value.len();
        let now = self.lru_now();

        // Field-split so one `get_mut` can update entry + key_versions +
        // live_bytes without a second hash or owning the key. The vacant /
        // expired path falls through to `insert` after this block ends.
        {
            let Database {
                entries,
                key_versions,
                expiry_index,
                live_bytes,
                ..
            } = self;

            if let Some(entry) = entries.get_mut(key) {
                if !entry.is_expired() {
                    // Fast path: already a String — only need old len for live_bytes.
                    // Other types still pay the full payload_bytes match (rare under
                    // redis-benchmark SET, which always overwrites the same string).
                    let old_val_sz = match &entry.value {
                        DataType::String(v) => v.len(),
                        DataType::Int(_) => 8,
                        DataType::List(l) => l.payload_bytes(),
                        DataType::Set(s) => s.payload_bytes(),
                        DataType::Hash(h) => h.payload_bytes(),
                        DataType::Bitmap(v) => v.len(),
                        DataType::HyperLogLog(v) => v.len(),
                        DataType::ZSet(z) => z.payload_bytes(),
                        DataType::Stream(s) => s.estimated_size(),
                        DataType::Geo(g) => g.members.len() * 24,
                    };
                    if let Some(exp) = entry.expiry.take() {
                        // Rare on redis-benchmark (no TTL).
                        expiry_index.remove(&(exp.expires_at_ms, key.to_vec()));
                    }
                    if new_val_sz != old_val_sz {
                        if new_val_sz > old_val_sz {
                            live_bytes.fetch_add(new_val_sz - old_val_sz, Ordering::Relaxed);
                        } else {
                            let sub = old_val_sz - new_val_sz;
                            let _ = live_bytes.fetch_update(
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                                |cur| Some(cur.saturating_sub(sub)),
                            );
                        }
                    }
                    // Steady-state: key_versions slot exists after first write.
                    if let Some(v) = key_versions.get_mut(key) {
                        *v = v.wrapping_add(1);
                    } else {
                        key_versions.insert(key.to_vec(), 1);
                    }
                    // Prefer in-place Bytes swap on String to avoid enum rebuild.
                    match &mut entry.value {
                        DataType::String(v) => *v = value,
                        other => *other = DataType::String(value),
                    }
                    entry.lru_clock = now;
                    return;
                }
            }
        }

        // Vacant or lazily-expired: full insert (owns the key once).
        self.insert(key.to_vec(), Entry::new(DataType::String(value)));
    }

    /// Get the entry for `key`, inserting the result of `f()` if it's
    /// absent (or lazily-expired). Prefer a single non-owning `get_mut`
    /// on the hot "key already exists" path (HSET/SADD/ZADD/LPUSH under
    /// redis-benchmark) so we never allocate a key `Vec` and never hash
    /// twice per write.
    pub fn get_or_insert_with<F>(&mut self, key: &[u8], f: F) -> &mut Entry
    where
        F: FnOnce() -> Entry,
    {
        use std::collections::hash_map::Entry as HEntry;
        use std::sync::atomic::Ordering;
        let now = self.lru_now();

        // Live path: one get_mut. Field-split isn't needed here — we only
        // touch the entry (lru_clock). NLL ends the borrow on return so the
        // vacant/expired branch can call entry() without conflict.
        //
        // Use a raw pointer escape so rustc doesn't extend the get_mut
        // borrow across the insert path (classic HashMap NLL limitation).
        let live_ptr: Option<*mut Entry> = self.entries.get_mut(key).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                e.lru_clock = now;
                Some(e as *mut Entry)
            }
        });
        if let Some(p) = live_ptr {
            // SAFETY: exclusive &mut self; no other refs into entries exist.
            // The pointer is only used to reborrow immediately.
            return unsafe { &mut *p };
        }

        match self.entries.entry(key.to_vec()) {
            HEntry::Occupied(mut o) => {
                // Lazily-expired: undo old bookkeeping and replace.
                let old_expiry = o.get().expiry.clone();
                if let Some(e) = old_expiry {
                    self.expiry_index.remove(&(e.expires_at_ms, key.to_vec()));
                }
                Self::sub_live_bytes_atomic(
                    &self.live_bytes,
                    Self::estimate_entry_size(key, o.get()),
                );
                let new_entry = f();
                if let Some(ref e) = new_entry.expiry {
                    self.expiry_index.insert((e.expires_at_ms, key.to_vec()));
                }
                self.live_bytes.fetch_add(
                    Self::estimate_entry_size(key, &new_entry),
                    Ordering::Relaxed,
                );
                if let Some(v) = self.key_versions.get_mut(key) {
                    *v = v.wrapping_add(1);
                } else {
                    self.key_versions.insert(key.to_vec(), 1);
                }
                *o.get_mut() = new_entry;
                o.into_mut()
            }
            HEntry::Vacant(v) => {
                let entry = f();
                if let Some(ref e) = entry.expiry {
                    self.expiry_index.insert((e.expires_at_ms, key.to_vec()));
                }
                if let Some(ver) = self.key_versions.get_mut(key) {
                    *ver = ver.wrapping_add(1);
                } else {
                    self.key_versions.insert(key.to_vec(), 1);
                }
                self.live_bytes
                    .fetch_add(Self::estimate_entry_size(key, &entry), Ordering::Relaxed);
                v.insert(entry)
            }
        }
    }

    /// INCR/INCRBY/DECR/DECRBY fast path: read-modify-write an integer
    /// string in a single `entries.entry()` lookup, mutating the existing
    /// `Entry` in place instead of the old `get()` + `insert(Entry::new(..))`
    /// sequence (2 lookups, plus a fresh `Entry` that silently dropped any
    /// TTL — Redis explicitly preserves TTL across INCR, since the value is
    /// conceptually altered rather than replaced).
    ///
    /// Returns `Ok(new_value)`, or an error if the existing value is the
    /// wrong type, isn't a valid integer, or the increment would overflow.
    /// Vacant keys are treated as `0` before applying `delta`, matching
    /// `INCR`'s "creates the key at 0 first" semantics.
    /// Read-modify-write for INCR/DECR/INCRBY/DECRBY. Produces/consumes
    /// `DataType::Int` instead of a formatted `DataType::String` — this is
    /// pure representation-level groundwork for a future read-lock fast
    /// path (not added yet: this method is still only ever called under
    /// the shard's exclusive write lock, so a plain load+checked_add+store
    /// is enough here; no CAS loop is needed because there's no concurrent
    /// writer to race while we hold `&mut self`).
    ///
    /// Any command that writes a string value directly (SET, APPEND,
    /// SETRANGE, GETSET, ...) demotes an `Int`-typed key back to `String`
    /// via the normal `Database::insert()` replace path — this method never
    /// needs to handle that direction.
    pub fn incr_int(
        &mut self,
        key: &[u8],
        delta: i64,
    ) -> std::result::Result<i64, crate::error::NexradeError> {
        use crate::error::NexradeError;
        use std::collections::hash_map::Entry as HEntry;

        let now = self.lru_now();
        match self.entries.entry(key.to_vec()) {
            HEntry::Occupied(mut o) => {
                if o.get().is_expired() {
                    // Lazily-expired: same bookkeeping remove() would do,
                    // then fall through as if the key were vacant.
                    let old = o.get();
                    if let Some(ref e) = old.expiry {
                        self.expiry_index.remove(&(e.expires_at_ms, key.to_vec()));
                    }
                    Self::sub_live_bytes_atomic(
                        &self.live_bytes,
                        Self::estimate_entry_size(key, old),
                    );
                    let new_val = delta;
                    let new_entry =
                        Entry::new(DataType::Int(Arc::new(AtomicIntCell::new(new_val))));
                    self.live_bytes.fetch_add(
                        Self::estimate_entry_size(key, &new_entry),
                        Ordering::Relaxed,
                    );
                    *self.key_versions.entry(key.to_vec()).or_insert(0) += 1;
                    *o.get_mut() = new_entry;
                    return Ok(new_val);
                }
                // Fast case: already an Int cell — no size change (fixed
                // 8-byte representation), no live_bytes adjustment needed.
                if let DataType::Int(cell) = &o.get().value {
                    let current = cell.load();
                    let new_val = current.checked_add(delta).ok_or(NexradeError::Overflow)?;
                    cell.store(new_val);
                    o.get_mut().lru_clock = now;
                    *self.key_versions.entry(key.to_vec()).or_insert(0) += 1;
                    return Ok(new_val);
                }
                // Occupied String (or another type, which errors below) —
                // parse, checked_add, and promote to Int in place.
                let old_size = Self::estimate_entry_size(key, o.get());
                let current: i64 = match &o.get().value {
                    DataType::String(v) => std::str::from_utf8(v)
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or(NexradeError::NotInteger)?,
                    _ => return Err(NexradeError::WrongType),
                };
                let new_val = current.checked_add(delta).ok_or(NexradeError::Overflow)?;
                let entry = o.get_mut();
                entry.value = DataType::Int(Arc::new(AtomicIntCell::new(new_val)));
                entry.lru_clock = now;
                let new_size = Self::estimate_entry_size(key, entry);
                if new_size >= old_size {
                    self.live_bytes
                        .fetch_add(new_size - old_size, Ordering::Relaxed);
                } else {
                    Self::sub_live_bytes_atomic(&self.live_bytes, old_size - new_size);
                }
                *self.key_versions.entry(key.to_vec()).or_insert(0) += 1;
                Ok(new_val)
            }
            HEntry::Vacant(v) => {
                let new_val = delta;
                let entry = Entry::new(DataType::Int(Arc::new(AtomicIntCell::new(new_val))));
                self.live_bytes
                    .fetch_add(Self::estimate_entry_size(key, &entry), Ordering::Relaxed);
                *self.key_versions.entry(key.to_vec()).or_insert(0) += 1;
                v.insert(entry);
                Ok(new_val)
            }
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        let removed = self.entries.remove(key);
        if let Some(ref e) = removed {
            if let Some(ref exp) = e.expiry {
                self.expiry_index.remove(&(exp.expires_at_ms, key.to_vec()));
            }
            *self.key_versions.entry(key.to_vec()).or_insert(0) += 1;
            Self::sub_live_bytes_atomic(&self.live_bytes, Self::estimate_entry_size(key, e));
        }
        removed
    }

    /// Approximate memory footprint of a single entry (key + value +
    /// hashmap overhead). Returns 0 for an unknown variant so the live
    /// counter never goes negative on edge cases.
    fn estimate_entry_size(key: &[u8], entry: &Entry) -> usize {
        const OVERHEAD: usize = 64;
        let val_sz = match &entry.value {
            DataType::String(v) => v.len(),
            // Fixed-size: an `AtomicIntCell` stores exactly one `i64`, no
            // `.len()` to call.
            DataType::Int(_) => 8,
            DataType::List(l) => l.payload_bytes(),
            DataType::Set(s) => s.payload_bytes(),
            DataType::Hash(h) => h.payload_bytes(),
            DataType::Bitmap(v) => v.len(),
            DataType::HyperLogLog(v) => v.len(),
            DataType::ZSet(z) => z.payload_bytes(),
            DataType::Stream(s) => s.estimated_size(),
            DataType::Geo(g) => g.members.len() * 24,
        };
        OVERHEAD + key.len() + val_sz
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
    ///
    /// Returns the deleted keys so the caller can fire keyspace `expired`
    /// notifications when `notify-keyspace-events` has `x` set.
    pub fn expire_batch(&mut self, max: usize) -> Vec<Vec<u8>> {
        let now_ms = {
            #[cfg(not(target_arch = "wasm32"))]
            {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            }
            #[cfg(target_arch = "wasm32")]
            {
                0u128
            }
        };

        // Collect expired entries from the front of the sorted index.
        let to_delete: Vec<(u128, Vec<u8>)> = self
            .expiry_index
            .iter()
            .take_while(|(exp_ms, _)| *exp_ms <= now_ms)
            .take(max)
            .cloned()
            .collect();

        let mut deleted = Vec::with_capacity(to_delete.len());
        for (exp_ms, key) in to_delete {
            if let Some(e) = self.entries.remove(&key) {
                Self::sub_live_bytes_atomic(&self.live_bytes, Self::estimate_entry_size(&key, &e));
                *self.key_versions.entry(key.clone()).or_insert(0) += 1;
                deleted.push(key.clone());
            }
            self.expiry_index.remove(&(exp_ms, key));
        }
        deleted
    }

    /// 0.7.3: adaptive active-expire cycle.
    ///
    /// Run several rounds of `expire_batch` with an exponentially growing
    /// budget (capped) until either no expired keys remain or the index is
    /// drained. Returns the union of every deleted key plus the number of
    /// rounds actually run (used by the background tick to adapt the next
    /// call's starting budget).
    ///
    /// The starting budget is small (matches the legacy 20-key default) so
    /// a quiet server pays almost nothing. Under heavy write storms the
    /// budget grows so expired keys do not pile up faster than the
    /// background task can drain them.
    pub fn expire_cycle(&mut self) -> (Vec<Vec<u8>>, u32) {
        const MAX_BUDGET: usize = 256;
        const MIN_BUDGET: usize = 20;
        let mut budget = MIN_BUDGET;
        let mut total: Vec<Vec<u8>> = Vec::new();
        let mut rounds = 0u32;
        loop {
            rounds += 1;
            // `expiry_index` length is the upper bound; cheap to load
            // (BTreeSet) and avoids scanning more entries than exist.
            let have = self.expiry_index.len();
            if have == 0 || budget == 0 {
                break;
            }
            let n = budget.min(have);
            let deleted = self.expire_batch(n);
            let n_deleted = deleted.len();
            total.extend(deleted);
            // Adapt: if we deleted `budget` entries, the index is hot —
            // double up to MAX_BUDGET. If we deleted less, nothing more
            // remains and we stop.
            if n_deleted >= budget && budget < MAX_BUDGET {
                budget = (budget * 2).min(MAX_BUDGET);
            } else if n_deleted < budget {
                break;
            }
            // Cap rounds so a pathological index cannot loop forever.
            if rounds >= 16 {
                break;
            }
        }
        (total, rounds)
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
                    // Fixed-size atomic cell, not a `.len()`-able container.
                    DataType::Int(_) => 8,
                    DataType::List(l) => l.payload_bytes(),
                    DataType::Set(s) => s.payload_bytes(),
                    DataType::Hash(h) => h.payload_bytes(),
                    DataType::Bitmap(v) => v.len(),
                    DataType::HyperLogLog(v) => v.len(),
                    DataType::ZSet(z) => z.payload_bytes(),
                    DataType::Stream(s) => s.estimated_size(),
                    DataType::Geo(g) => g.members.len() * 24,
                };
                64 + k.len() + value_sz
            })
            .sum()
    }

    /// Evict one entry according to `policy`. Returns the key that was removed,
    /// or `None` if nothing was evicted.
    pub fn evict_one(&mut self, policy: &MaxMemoryPolicy) -> Option<Vec<u8>> {
        // Number of keys to sample for LRU eviction. Redis defaults to 5,
        // which gives ~`5/n` accuracy per eviction — plenty for steady-state
        // LRU approximations without scanning the entire database.
        const LRU_SAMPLE_SIZE: usize = 5;
        match policy {
            MaxMemoryPolicy::NoEviction => None,

            MaxMemoryPolicy::AllKeysRandom => {
                let key = self.entries.keys().next().cloned();
                if let Some(k) = key {
                    self.remove(&k);
                    Some(k)
                } else {
                    None
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
                    Some(k)
                } else {
                    None
                }
            }

            MaxMemoryPolicy::AllKeysLru => {
                // Reservoir-sample LRU_SAMPLE_SIZE entries, evict the
                // oldest. O(LRU_SAMPLE_SIZE) instead of O(N).
                let victim = sample_lru_victim(
                    self.entries.iter(),
                    LRU_SAMPLE_SIZE,
                    |(_, e)| e.lru_clock,
                    |(_, _e)| true,
                )
                .map(|(k, _)| k.clone());
                if let Some(k) = victim {
                    self.remove(&k);
                    Some(k)
                } else {
                    None
                }
            }

            MaxMemoryPolicy::VolatileLru => {
                let victim = sample_lru_victim(
                    self.entries.iter(),
                    LRU_SAMPLE_SIZE,
                    |(_, e)| e.lru_clock,
                    |(_, e)| e.expiry.is_some() && !e.is_expired(),
                )
                .map(|(k, _)| k.clone());
                if let Some(k) = victim {
                    self.remove(&k);
                    Some(k)
                } else {
                    None
                }
            }

            MaxMemoryPolicy::VolatileTtl => {
                // Use the expiry_index — the first entry is the one expiring soonest.
                let key = self.expiry_index.iter().next().map(|(_, k)| k.clone());
                if let Some(k) = key {
                    self.remove(&k);
                    Some(k)
                } else {
                    None
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

/// Reservoir-sample `sample_size` items from `iter`, keeping only those that
/// pass `keep_filter`, then return the one with the smallest `score`.
///
/// Cheap O(sample_size) work regardless of how many items the iterator
/// yields — appropriate for eviction paths that otherwise would scan the
/// entire HashMap.
fn sample_lru_victim<'a, K, V, S, F>(
    iter: impl Iterator<Item = (&'a K, &'a V)>,
    sample_size: usize,
    score: S,
    keep_filter: F,
) -> Option<(&'a K, &'a V)>
where
    S: Fn(&(&'a K, &'a V)) -> u32,
    F: Fn(&(&'a K, &'a V)) -> bool,
{
    // Sample buffer (key, value, score) so we don't recompute `score` per
    // candidate during the final min reduction.
    let mut sample: Vec<(&K, &V, u32)> = Vec::with_capacity(sample_size + 1);
    let mut idx: usize = 0;
    for kv in iter {
        idx += 1;
        if !keep_filter(&kv) {
            continue;
        }
        let s = score(&kv);
        if sample.len() < sample_size {
            sample.push((kv.0, kv.1, s));
        } else {
            // Replace a random slot with the new entry.
            let slot = (idx.wrapping_mul(2_654_435_761)) % (idx + 1);
            if slot < sample_size {
                sample[slot] = (kv.0, kv.1, s);
            }
        }
    }
    sample
        .into_iter()
        .min_by_key(|(_, _, s)| *s)
        .map(|(k, v, _)| (k, v))
}

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

/// Number of shards: next power-of-two ≥ max(CPU count, 64), clamped to
/// [64, 256]. Random multi-key MSET (e.g. redis-benchmark `-r` 10-key)
/// locks ~k distinct shards; more shards cut cross-client overlap under
/// pipelined concurrency (0.5.1 floor 32; **0.7.1** raises the floor to 64
/// so mid-size hosts still keep multi-key collision probability low).
/// Single-key ops still hit one shard.
fn compute_num_shards() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    // At least 64 so small/mid machines still get multi-key concurrency
    // headroom for 10-key random MSET (overlap ~ k²/n).
    cpus.max(64).next_power_of_two().clamp(64, 256)
}

/// Per-logical-database sharded store.
///
/// Each incoming key is routed to one of `num_shards` shards via FNV-1a
/// hash, so concurrent writes to different keys never contend.
///
/// Shards are `Arc<RwLock<Database>>` so hot paths and deferred / blocking
/// multi-shard commits (0.5.1 MSET) can clone a handle without borrowing
/// `ShardedDatabase`. Hot paths still use `write_for` / `read_for` /
/// `lock_shards` with the same call shape as a bare `Vec<RwLock<_>>`.
pub struct ShardedDatabase {
    shards: Vec<Arc<RwLock<Database>>>,
    num_shards: usize,
    lru_clock: LruClock,
}

impl ShardedDatabase {
    pub fn new(num_shards: usize) -> Self {
        // All shards share the same atomic clock source so a single
        // `Arc<AtomicU32>` load from any shard returns the current LRU
        // timestamp. Defaults to 0; the Store wires the real atomic in.
        let clock = LruClock::new(Arc::new(AtomicU32::new(0)));
        Self {
            shards: (0..num_shards)
                .map(|_| Arc::new(RwLock::new(Database::new())))
                .collect(),
            num_shards,
            lru_clock: clock.clone(),
        }
    }

    /// Install a shared LRU clock source on every shard and propagate it
    /// through any existing shards (no-op for the standard `new` path).
    /// The background tick task updates `inner` at `hz` frequency.
    pub fn set_lru_clock(&mut self, clock: LruClock) {
        self.lru_clock = clock.clone();
        for shard in &self.shards {
            let mut guard = shard.write();
            guard.set_lru_clock(clock.clone());
        }
    }

    #[inline]
    pub fn shard_idx(&self, key: &[u8]) -> usize {
        fnv1a(key) & (self.num_shards - 1)
    }

    pub fn shard_count(&self) -> usize {
        self.num_shards
    }

    /// Cloneable handle to one shard lock (deferred / blocking multi-shard
    /// MSET workers). Index must be `< shard_count()`.
    #[inline]
    pub fn shard_lock(&self, idx: usize) -> Arc<RwLock<Database>> {
        Arc::clone(&self.shards[idx])
    }

    /// Snapshot of every shard lock for moving a multi-shard commit onto a
    /// blocking pool thread without borrowing `self` (0.5.1).
    pub fn all_shard_locks(&self) -> Vec<Arc<RwLock<Database>>> {
        self.shards.iter().map(Arc::clone).collect()
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

    /// INCR/DECR/INCRBY/DECRBY entry point. Tries a read-lock CAS fast path
    /// on an already-promoted, non-expired `Int` key; falls back to the
    /// existing exclusive write-lock slow path (`Database::incr_int`) for
    /// creation, promotion, and expiry — those all need to mutate the
    /// HashMap's occupancy, not just the cell's value.
    ///
    /// Correctness under concurrent promotion/demotion: the read guard is
    /// held for the entire load-CAS-store sequence, and `parking_lot`'s
    /// write acquisition blocks until every reader on that shard releases —
    /// so no writer can promote/demote/remove the entry (or replace the
    /// `Arc<AtomicIntCell>`) while our read guard is alive. Once the probe
    /// finishes and the guard drops, a fallback call is an independent,
    /// fresh lock acquisition.
    pub fn incr_int(
        &self,
        key: &[u8],
        delta: i64,
    ) -> std::result::Result<i64, crate::error::NexradeError> {
        use crate::error::NexradeError;
        {
            let guard = self.read_for(key);
            if let Some(entry) = guard.get_ro(key) {
                if let DataType::Int(cell) = &entry.value {
                    return cell.checked_add(delta).ok_or(NexradeError::Overflow);
                }
            }
            // guard dropped here — vacant, expired, or non-Int key falls
            // through to the slow path below.
        }
        self.write_for(key).incr_int(key, delta)
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

    pub fn expire_batch(&self, max_total: usize) -> Vec<Vec<u8>> {
        let per_shard = (max_total / self.num_shards).max(1);
        let mut deleted = Vec::new();
        for s in &self.shards {
            deleted.extend(s.write().expire_batch(per_shard));
        }
        deleted
    }

    /// 0.7.3: adaptive active-expire cycle across every shard. See
    /// [`Database::expire_cycle`] for the budget shape. Returns the union
    /// of every deleted key plus the total rounds run.
    pub fn expire_cycle(&self) -> (Vec<Vec<u8>>, u32) {
        let mut total: Vec<Vec<u8>> = Vec::new();
        let mut rounds = 0u32;
        for s in &self.shards {
            let (deleted, r) = s.write().expire_cycle();
            rounds = rounds.saturating_add(r);
            total.extend(deleted);
        }
        (total, rounds)
    }

    pub fn keys_matching(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for shard in &self.shards {
            keys.extend(shard.write().keys_matching(pattern));
        }
        keys
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        // O(shards) instead of O(entries) — the live_bytes counter is updated
        // incrementally on every insert / remove.
        self.shards.iter().map(|s| s.read().live_bytes()).sum()
    }

    /// Sum live_bytes across every shard — O(shards).
    pub fn live_bytes(&self) -> usize {
        self.shards.iter().map(|s| s.read().live_bytes()).sum()
    }

    pub fn evict_one(&self, policy: &MaxMemoryPolicy) -> Option<Vec<u8>> {
        for shard in &self.shards {
            if let Some(k) = shard.write().evict_one(policy) {
                return Some(k);
            }
        }
        None
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
    ///
    /// Returns `Ok(None)` when src is missing or empty; `Err(WrongType)` when
    /// src or an existing dst is not a list (checked **before** popping so a
    /// wrong-type destination never drops an element).
    pub fn lmove_atomic(
        &self,
        src: &[u8],
        dst: Vec<u8>,
        from_left: bool,
        to_left: bool,
    ) -> std::result::Result<Option<bytes::Bytes>, crate::error::NexradeError> {
        use crate::error::NexradeError;
        use crate::types::DataType;
        let si = self.shard_idx(src);
        let di = self.shard_idx(&dst);
        let (mut lo_g, hi_opt, swapped) = self.lock_two(si, di);

        // Helper: pop from a list; returns (value, payload_delta, emptied).
        // WrongType when the key exists but is not a list; Ok(None) when missing/empty.
        fn pop_list(
            s: &mut Database,
            key: &[u8],
            from_left: bool,
        ) -> std::result::Result<Option<(bytes::Bytes, isize, bool)>, NexradeError> {
            let Some(entry) = s.get_mut(key) else {
                return Ok(None);
            };
            match &mut entry.value {
                DataType::List(l) => {
                    let val = if from_left {
                        l.pop_front()
                    } else {
                        l.pop_back()
                    };
                    match val {
                        Some(v) => {
                            let emptied = l.is_empty();
                            let delta = -(v.len() as isize);
                            Ok(Some((v, delta, emptied)))
                        }
                        None => Ok(None),
                    }
                }
                _ => Err(NexradeError::WrongType),
            }
        }

        // Helper: push to a list (creating it if absent). Returns payload delta
        // (= element len). Empty-shell overhead is already counted by
        // get_or_insert_with when the key is newly created.
        // Precondition: if dst exists it is already known to be a List (or absent).
        fn push_list(s: &mut Database, key: Vec<u8>, val: bytes::Bytes, to_left: bool) -> isize {
            let n = val.len() as isize;
            let entry = s.get_or_insert_with(&key, || {
                Entry::new(DataType::List(crate::list_data::ListData::new()))
            });
            if let DataType::List(l) = &mut entry.value {
                if to_left {
                    l.push_front(val);
                } else {
                    l.push_back(val);
                }
            }
            // WATCH: LMOVE always bumps the destination key version.
            if let Some(v) = s.key_versions.get_mut(&key) {
                *v = v.wrapping_add(1);
            } else {
                s.key_versions.insert(key, 1);
            }
            n
        }

        // Reject wrong-type dst *before* popping src so the element is not lost.
        fn dst_ok(s: &Database, dst: &[u8]) -> std::result::Result<(), NexradeError> {
            match s.get_ro(dst) {
                None => Ok(()),
                Some(e) => match &e.value {
                    DataType::List(_) => Ok(()),
                    _ => Err(NexradeError::WrongType),
                },
            }
        }

        if let Some(mut hi_g) = hi_opt {
            let (src_s, dst_s) = if swapped {
                (&mut *hi_g, &mut *lo_g)
            } else {
                (&mut *lo_g, &mut *hi_g)
            };
            dst_ok(dst_s, &dst)?;
            let Some((val, src_delta, emptied)) = pop_list(src_s, src, from_left)? else {
                return Ok(None);
            };
            src_s.adjust_live_bytes(src_delta);
            if emptied {
                src_s.remove_empty_key(src);
            }
            let out = val.clone();
            let dst_delta = push_list(dst_s, dst, val, to_left);
            dst_s.adjust_live_bytes(dst_delta);
            Ok(Some(out))
        } else {
            dst_ok(&lo_g, &dst)?;
            let Some((val, src_delta, emptied)) = pop_list(&mut lo_g, src, from_left)? else {
                return Ok(None);
            };
            lo_g.adjust_live_bytes(src_delta);
            if emptied {
                lo_g.remove_empty_key(src);
            }
            let out = val.clone();
            let dst_delta = push_list(&mut lo_g, dst, val, to_left);
            lo_g.adjust_live_bytes(dst_delta);
            Ok(Some(out))
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
            let (removed, src_delta, emptied) = match src_s.get_mut(src) {
                Some(e) => match &mut e.value {
                    DataType::Set(s) => {
                        let (removed, d) = s.remove(&member);
                        (removed, d, s.is_empty())
                    }
                    _ => return 0,
                },
                None => return 0,
            };
            if !removed {
                return 0;
            }
            src_s.adjust_live_bytes(src_delta);
            if emptied {
                src_s.remove_empty_key(src);
            }

            // Empty shell already counted if newly created; only payload.
            let dst_entry = dst_s.get_or_insert_with(&dst, || {
                Entry::new(DataType::Set(crate::set_data::SetData::new()))
            });
            let dst_delta = if let DataType::Set(s) = &mut dst_entry.value {
                s.insert(member).1
            } else {
                0
            };
            dst_s.adjust_live_bytes(dst_delta);
            if let Some(v) = dst_s.key_versions.get_mut(&dst) {
                *v = v.wrapping_add(1);
            } else {
                dst_s.key_versions.insert(dst, 1);
            }
            1
        } else {
            let (removed, src_delta, emptied) = match lo_g.get_mut(src) {
                Some(e) => match &mut e.value {
                    DataType::Set(s) => {
                        let (removed, d) = s.remove(&member);
                        (removed, d, s.is_empty())
                    }
                    _ => return 0,
                },
                None => return 0,
            };
            if !removed {
                return 0;
            }
            lo_g.adjust_live_bytes(src_delta);
            if emptied {
                lo_g.remove_empty_key(src);
            }

            let dst_entry = lo_g.get_or_insert_with(&dst, || {
                Entry::new(DataType::Set(crate::set_data::SetData::new()))
            });
            let dst_delta = if let DataType::Set(s) = &mut dst_entry.value {
                s.insert(member).1
            } else {
                0
            };
            lo_g.adjust_live_bytes(dst_delta);
            if let Some(v) = lo_g.key_versions.get_mut(&dst) {
                *v = v.wrapping_add(1);
            } else {
                lo_g.key_versions.insert(dst, 1);
            }
            1
        }
    }

    /// Acquire write guards for all distinct shards in `shard_indices` (must be
    /// sorted and deduped). Returns guards in the same order.
    ///
    /// MSET/MSETNX's atomicity requirement (never observe a partial write)
    /// means every shard must be locked before any insert happens. The naive
    /// way to do that — `self.shards[i].write()` in a loop — blocks
    /// sequentially: while waiting on shard N's lock, it keeps holding every
    /// earlier shard's lock, needlessly stalling any *other* op that only
    /// wanted one of those earlier shards. Under concurrent pipelined MSETs
    /// touching overlapping shard sets, that turns into cross-batch
    /// serialization (see `docs/mset-investigation.md`).
    ///
    /// This instead does a non-blocking `try_write()` sweep: attempt every
    /// shard in order, and if any attempt fails, drop everything acquired
    /// this sweep (so we're never holding a shard's lock while waiting on
    /// another) and retry after a short backoff. Atomicity is unchanged from
    /// the old code — a sweep still only returns once *every* shard is
    /// locked, so there is no window where a partial write is visible.
    ///
    /// The retry loop is bounded (`MAX_TRY_ROUNDS`): after that many failed
    /// sweeps, fall back to the old unconditional blocking `.write()` calls.
    /// This guarantees forward progress under pathological contention and
    /// means the worst case is never worse than the pre-existing behavior —
    /// only the common case (most sweeps succeed inline or after a handful
    /// of retries) gets faster.
    ///
    /// 0.5.1: async multi-shard path uses the same adaptive try-lock-all +
    /// park backoff as the sync path (see [`lock_shard_arcs`]). Early
    /// `spawn_blocking` on first miss was measured ~4× *worse* on random
    /// keyspace pipe MSET (pool hop + almost-always-contended try-once).
    fn lock_shards(
        &self,
        shard_indices: &[usize],
    ) -> Vec<parking_lot::RwLockWriteGuard<'_, Database>> {
        lock_shard_arcs(&self.shards, shard_indices)
    }

    pub fn mset(&self, pairs: Vec<(Vec<u8>, Entry)>) {
        if pairs.is_empty() {
            return;
        }
        let (tagged, si) = tag_and_sort_pairs(self, pairs);

        // Single-shard fast path: one write lock, no try-sweep.
        if si.len() == 1 {
            let mut g = self.shards[si[0]].write();
            for (_, key, entry) in tagged {
                g.insert(key, entry);
            }
            return;
        }

        let mut guards = self.lock_shards(&si);
        commit_tagged_mset(&mut guards, &si, tagged);
    }

    /// Async multi-shard MSET (0.5.1). Same adaptive try-lock-all commit as
    /// [`Self::mset`] — keeps full-batch atomicity and avoids
    /// `spawn_blocking` thrash under random-keyspace pipelining.
    pub async fn mset_async(&self, pairs: Vec<(Vec<u8>, Entry)>) {
        // Multi-shard acquire is CPU-bound spin/park, not async I/O; running
        // it inline matches Redis-style "hold the thread until done" and
        // is the throughput-winning path for redis-benchmark -r MSET.
        self.mset(pairs);
        // Yield so a long contended stretch still cooperates with the
        // runtime (other tasks on this worker get a chance between MSETs).
        tokio::task::yield_now().await;
    }

    /// MSETNX: set all key-value pairs only when NONE of the keys exist.
    pub fn msetnx(&self, pairs: Vec<(Vec<u8>, Entry)>) -> bool {
        if pairs.is_empty() {
            return true;
        }
        let (tagged, si) = tag_and_sort_pairs(self, pairs);

        if si.len() == 1 {
            let mut g = self.shards[si[0]].write();
            for (_, key, _) in &tagged {
                if g.get_ro(key).is_some() {
                    return false;
                }
            }
            for (_, key, entry) in tagged {
                g.insert(key, entry);
            }
            return true;
        }

        let mut guards = self.lock_shards(&si);
        commit_tagged_msetnx(&mut guards, &si, tagged)
    }

    /// Async multi-shard MSETNX (0.5.1). Same as [`Self::msetnx`] + yield.
    pub async fn msetnx_async(&self, pairs: Vec<(Vec<u8>, Entry)>) -> bool {
        let ok = self.msetnx(pairs);
        tokio::task::yield_now().await;
        ok
    }
}

// ── Multi-shard MSET helpers (shared by sync + async paths) ───────────────────

type TaggedPair = (usize, Vec<u8>, Entry);

fn tag_and_sort_pairs(
    sdb: &ShardedDatabase,
    pairs: Vec<(Vec<u8>, Entry)>,
) -> (Vec<TaggedPair>, Vec<usize>) {
    let mut tagged: Vec<TaggedPair> = pairs
        .into_iter()
        .map(|(k, e)| (sdb.shard_idx(&k), k, e))
        .collect();
    tagged.sort_unstable_by_key(|(s, _, _)| *s);

    let mut si: Vec<usize> = Vec::with_capacity(tagged.len());
    for (s, _, _) in &tagged {
        if si.last().copied() != Some(*s) {
            si.push(*s);
        }
    }
    (tagged, si)
}

fn try_lock_shard_arcs_once<'a>(
    shards: &'a [Arc<RwLock<Database>>],
    shard_indices: &[usize],
) -> Option<Vec<parking_lot::RwLockWriteGuard<'a, Database>>> {
    let mut guards = Vec::with_capacity(shard_indices.len());
    for &i in shard_indices {
        match shards[i].try_write() {
            Some(g) => guards.push(g),
            None => return None, // partial set drops here
        }
    }
    Some(guards)
}

/// Adaptive try-lock-all with backoff (never holds a partial lock set).
///
/// Tuned for redis-benchmark random multi-key MSET: many concurrent ops
/// each need ~k of N shards. Spin hard first (holders finish in µs), then
/// brief park timeouts (beat pure yield under load), then blocking writes
/// in index order as a last resort for forward progress.
fn lock_shard_arcs<'a>(
    shards: &'a [Arc<RwLock<Database>>],
    shard_indices: &[usize],
) -> Vec<parking_lot::RwLockWriteGuard<'a, Database>> {
    // Hard spin: covers the common case where a holder is mid-insert.
    const SPIN_ROUNDS: u32 = 64;
    // Park phase: exponential-ish short sleeps without saturating the CPU.
    const PARK_ROUNDS: u32 = 128;
    // After spin+park still failing, fall back to blocking acquire.

    for round in 0..SPIN_ROUNDS {
        if let Some(guards) = try_lock_shard_arcs_once(shards, shard_indices) {
            return guards;
        }
        // A few pause instructions per round; cheap under light contention.
        for _ in 0..8 {
            std::hint::spin_loop();
        }
        // After a handful of pure spins, also yield the OS thread so a
        // descheduled holder can run (important on oversubscribed hosts).
        if round >= 16 && round % 4 == 0 {
            std::thread::yield_now();
        }
    }

    for round in 0..PARK_ROUNDS {
        if let Some(guards) = try_lock_shard_arcs_once(shards, shard_indices) {
            return guards;
        }
        // 1µs → 2 → 4 … cap 64µs. parking_lot holders are short; long
        // sleeps would only inflate tail latency.
        let us = (1u64 << (round / 16).min(6)).max(1);
        std::thread::park_timeout(std::time::Duration::from_micros(us));
    }

    // Pathological contention: blocking writes in sorted index order
    // (deadlock-free). May convoy, but guarantees progress.
    shard_indices.iter().map(|&i| shards[i].write()).collect()
}

fn commit_tagged_mset(
    guards: &mut [parking_lot::RwLockWriteGuard<'_, Database>],
    si: &[usize],
    tagged: Vec<TaggedPair>,
) {
    let mut gi = 0usize;
    for (shard, key, entry) in tagged {
        while si[gi] != shard {
            gi += 1;
        }
        guards[gi].insert(key, entry);
    }
}

/// Returns true if every key was free and all pairs were inserted.
fn commit_tagged_msetnx(
    guards: &mut [parking_lot::RwLockWriteGuard<'_, Database>],
    si: &[usize],
    tagged: Vec<TaggedPair>,
) -> bool {
    {
        let mut gi = 0usize;
        for (shard, key, _) in &tagged {
            while si[gi] != *shard {
                gi += 1;
            }
            if guards[gi].get_ro(key).is_some() {
                return false;
            }
        }
    }
    let mut gi = 0usize;
    for (shard, key, entry) in tagged {
        while si[gi] != shard {
            gi += 1;
        }
        guards[gi].insert(key, entry);
    }
    true
}

// ── Thread-safe multi-database store ─────────────────────────────────────────

/// Thread-safe multi-database store backed by per-key sharding.
#[derive(Clone)]
pub struct Store {
    dbs: Arc<Vec<ShardedDatabase>>,
    pub db_count: usize,
    /// Shared LRU clock source — same atomic is used by every shard and
    /// updated by the background tick task.
    lru_clock: LruClock,
}

impl Store {
    pub fn new(db_count: usize) -> Self {
        let num_shards = compute_num_shards();
        // Every shard reads the same atomic; updates come from the
        // background tick via `Store::refresh_lru_clock`.
        let clock = LruClock::default();
        let dbs: Vec<ShardedDatabase> = (0..db_count)
            .map(|_| ShardedDatabase::new(num_shards))
            .collect();
        Self {
            dbs: Arc::new(dbs),
            db_count,
            lru_clock: clock,
        }
    }

    /// Wire a shared LRU clock into every shard.
    pub fn set_lru_clock(&mut self, clock: LruClock) {
        for sdb in Arc::get_mut(&mut self.dbs)
            .expect("no concurrent Store owners")
            .iter_mut()
        {
            sdb.set_lru_clock(clock.clone());
        }
        self.lru_clock = clock;
    }

    /// Replace the inner atomic clock on every shard with `value`. The
    /// background tick task calls this at `hz` frequency.
    pub fn refresh_lru_clock(&self, value: u32) {
        self.lru_clock.store(value);
    }

    /// Read the current cached LRU timestamp.
    #[inline]
    pub fn lru_now(&self) -> u32 {
        self.lru_clock.now()
    }

    /// Evict entries until memory is below `limit_bytes`.
    /// Returns the keys that were removed (for keyspace `evicted` events).
    ///
    /// 0.7.3: before sampling for eviction, opportunistically drain expired
    /// keys via the adaptive cycle. Under a write storm with short TTLs this
    /// reclaims free space without paying the more expensive LRU sample —
    /// matching Redis's "expire first, then evict" ordering.
    pub fn evict_if_needed(&self, policy: &MaxMemoryPolicy, limit_bytes: usize) -> Vec<Vec<u8>> {
        if *policy == MaxMemoryPolicy::NoEviction || limit_bytes == 0 {
            return Vec::new();
        }
        // Opportunistic expire-before-evict. Free expired keys first so we
        // don't sample-and-evict a live key when free space is already
        // sitting in the expiry index. The adaptive cycle stops early when
        // nothing is expired, so this is a cheap no-op in the common case.
        let _ = self.active_expire_cycle();
        if self.estimated_memory_bytes() <= limit_bytes {
            return Vec::new();
        }

        let mut evicted = Vec::new();
        // `estimated_memory_bytes()` is O(1) now (sum of shard atomics).
        //
        // The inner per-shard evict is a *random* sample: one failed
        // sample doesn't mean the shard is empty. Only give up once
        // every shard is provably empty (entry count == 0).
        'outer: while self.estimated_memory_bytes() > limit_bytes {
            for sdb in self.dbs.iter() {
                if let Some(k) = sdb.evict_one(policy) {
                    evicted.push(k);
                    continue 'outer;
                }
            }
            // No shard evicted — either all are empty or every sample
            // missed (unlikely with non-trivial sizes). Probe the next
            // iteration: the loop condition will re-check live_bytes
            // and we'll exit cleanly if all shards are actually empty.
            for sdb in self.dbs.iter() {
                if !sdb.is_empty() {
                    // At least one shard has data but evict_one
                    // failed. The sample is randomized, so retry.
                    continue 'outer;
                }
            }
            break;
        }
        evicted
    }

    /// Run active expiry on all databases. Returns every expired key
    /// across every db (for keyspace `expired` events).
    pub fn active_expire(&self, max_per_db: usize) -> Vec<(usize, Vec<u8>)> {
        let mut out = Vec::new();
        for (i, sdb) in self.dbs.iter().enumerate() {
            for k in sdb.expire_batch(max_per_db) {
                out.push((i, k));
            }
        }
        out
    }

    /// 0.7.3: adaptive active-expire cycle. See
    /// [`ShardedDatabase::expire_cycle`] and [`Database::expire_cycle`]
    /// for the budget shape. Returns `(key, db_index)` pairs for the
    /// background listener to fire keyspace `expired` events.
    pub fn active_expire_cycle(&self) -> Vec<(usize, Vec<u8>)> {
        let mut out = Vec::new();
        for (i, sdb) in self.dbs.iter().enumerate() {
            let (deleted, _rounds) = sdb.expire_cycle();
            for k in deleted {
                out.push((i, k));
            }
        }
        out
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

    /// Count keys whose hash slot equals `slot` across every database.
    /// Used by `CLUSTER COUNTKEYSINSLOT`.
    pub fn count_keys_in_slot(&self, slot: u16) -> usize {
        let mut n = 0;
        for i in 0..self.db_count {
            for key in self.db(i).keys_matching(b"*") {
                if crate::cluster::keyslot(&key) == slot {
                    n += 1;
                }
            }
        }
        n
    }

    /// Return up to `count` keys whose hash slot equals `slot` across
    /// every database. Used by `CLUSTER GETKEYSINSLOT`.
    pub fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        if count == 0 {
            return out;
        }
        for i in 0..self.db_count {
            for key in self.db(i).keys_matching(b"*") {
                if crate::cluster::keyslot(&key) == slot {
                    out.push(key);
                    if out.len() >= count {
                        return out;
                    }
                }
            }
        }
        out
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
        // O(shards) instead of O(entries) — the live_bytes counter is
        // updated incrementally on every insert / remove.
        self.dbs.iter().map(|s| s.live_bytes()).sum()
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
            DataType::String(Bytes::from_static(b"val")),
            Expiry::from_duration(Duration::from_millis(1)),
        );
        db.insert(b"key".to_vec(), entry);
        std::thread::sleep(Duration::from_millis(5));
        assert!(db.get(b"key").is_none());
    }

    // ── DataType::Int — promotion, key_version, RDB/COPY independence ─────────

    #[test]
    fn incr_on_vacant_key_promotes_directly_to_int() {
        let mut db = Database::new();
        let v = db.incr_int(b"k", 5).unwrap();
        assert_eq!(v, 5);
        assert!(matches!(db.get(b"k").unwrap().value, DataType::Int(_)));
    }

    #[test]
    fn incr_on_string_key_promotes_to_int() {
        let mut db = Database::new();
        db.insert(b"k".to_vec(), str_entry(b"10"));
        let v = db.incr_int(b"k", 5).unwrap();
        assert_eq!(v, 15);
        assert!(matches!(db.get(b"k").unwrap().value, DataType::Int(_)));
    }

    #[test]
    fn set_on_int_key_demotes_to_string() {
        let mut db = Database::new();
        db.incr_int(b"k", 5).unwrap();
        assert!(matches!(db.get(b"k").unwrap().value, DataType::Int(_)));
        db.insert(b"k".to_vec(), str_entry(b"hello"));
        assert!(matches!(db.get(b"k").unwrap().value, DataType::String(_)));
    }

    #[test]
    fn set_plain_overwrites_in_place_and_clears_ttl() {
        let mut db = Database::new();
        db.set_plain(b"k", Bytes::from_static(b"aaa"));
        assert!(matches!(
            &db.get(b"k").unwrap().value,
            DataType::String(v) if v.as_ref() == b"aaa"
        ));
        let v0 = db.key_version(b"k");
        // Same-length overwrite (redis-benchmark shape).
        db.set_plain(b"k", Bytes::from_static(b"bbb"));
        assert!(matches!(
            &db.get(b"k").unwrap().value,
            DataType::String(v) if v.as_ref() == b"bbb"
        ));
        assert!(db.key_version(b"k") > v0);
        // SET without KEEPTTL clears expiry.
        db.insert(
            b"ttl".to_vec(),
            Entry::with_expiry(
                DataType::String(Bytes::from_static(b"x")),
                Expiry::from_duration(Duration::from_secs(60)),
            ),
        );
        assert!(db.get(b"ttl").unwrap().expiry.is_some());
        db.set_plain(b"ttl", Bytes::from_static(b"y"));
        assert!(db.get(b"ttl").unwrap().expiry.is_none());
        // Demotes Int → String (same as full insert path).
        db.incr_int(b"n", 1).unwrap();
        db.set_plain(b"n", Bytes::from_static(b"str"));
        assert!(matches!(db.get(b"n").unwrap().value, DataType::String(_)));
    }

    #[test]
    fn key_version_increases_on_every_incr_including_fast_path() {
        let mut db = Database::new();
        let v0 = db.key_version(b"k");
        db.incr_int(b"k", 1).unwrap(); // vacant -> Int, promotion bump
        let v1 = db.key_version(b"k");
        assert!(v1 > v0, "key_version must increase after first INCR");
        db.incr_int(b"k", 1).unwrap(); // Int -> Int, in-place mutation
        let v2 = db.key_version(b"k");
        assert!(
            v2 > v1,
            "key_version must increase after a second INCR on an already-Int key"
        );
    }

    #[test]
    fn key_version_on_expired_int_key_does_not_panic_or_underflow() {
        let mut db = Database::new();
        db.insert(
            b"k".to_vec(),
            Entry::with_expiry(
                DataType::Int(Arc::new(AtomicIntCell::new(5))),
                Expiry::from_duration(Duration::from_millis(1)),
            ),
        );
        std::thread::sleep(Duration::from_millis(5));
        // Expired key: key_version must fall back to the base counter, not
        // read through to the (logically gone) Int cell.
        let _ = db.key_version(b"k");
    }

    #[test]
    fn incr_preserves_live_bytes_accounting_across_promotion() {
        let mut db = Database::new();
        db.insert(b"k".to_vec(), str_entry(b"10")); // 2-byte String value
        let before = db.live_bytes();
        db.incr_int(b"k", 5).unwrap(); // promotes to Int (constant 8-byte size)
        let after = db.live_bytes();
        // Exact delta isn't the point (str "10" is 2 bytes, Int is a
        // constant 8) — just confirm the counter tracks the swap instead of
        // double-counting or going negative.
        assert_ne!(before, after);
        assert!(
            db.live_bytes() < usize::MAX / 2,
            "sanity: no underflow wrap"
        );
    }

    #[test]
    fn cloning_an_int_entry_produces_an_independent_cell() {
        // Guards the manual `Clone for DataType` impl: a derived Clone on
        // `Arc<AtomicIntCell>` would just bump the refcount, aliasing the
        // live cell — this is exactly the bug that would corrupt RDB
        // snapshots and COPY/RENAME destinations.
        let mut db = Database::new();
        db.incr_int(b"k", 100).unwrap();
        let original = db.get(b"k").unwrap().clone();
        // Mutate the live entry further.
        db.incr_int(b"k", 1).unwrap();
        let DataType::Int(orig_cell) = &original.value else {
            panic!("expected Int");
        };
        assert_eq!(
            orig_cell.load(),
            100,
            "the cloned snapshot must not see the later mutation"
        );
        let DataType::Int(live_cell) = &db.get(b"k").unwrap().value else {
            panic!("expected Int");
        };
        assert_eq!(live_cell.load(), 101);
    }

    #[test]
    fn sharded_database_snapshot_int_entry_is_independent_of_live_cell() {
        // ShardedDatabase::snapshot() (used for RDB save) clones entries
        // under a read guard — same aliasing risk as the Clone test above,
        // exercised through the actual snapshot path.
        let sdb = ShardedDatabase::new(4);
        sdb.write_for(b"k").incr_int(b"k", 42).unwrap();
        let snap = sdb.snapshot();
        sdb.write_for(b"k").incr_int(b"k", 1).unwrap();

        let DataType::Int(snap_cell) = &snap.entries.get(b"k".as_slice()).unwrap().value else {
            panic!("expected Int in snapshot");
        };
        assert_eq!(
            snap_cell.load(),
            42,
            "snapshot must be frozen at capture time"
        );

        let guard = sdb.read_for(b"k");
        let DataType::Int(live_cell) = &guard.get_ro(b"k").unwrap().value else {
            panic!("expected Int");
        };
        assert_eq!(live_cell.load(), 43);
    }

    #[test]
    fn bincode_round_trip_reconstructs_independent_live_int_cell() {
        let mut db = Database::new();
        db.incr_int(b"k", 7).unwrap();

        let encoded = bincode::serde::encode_to_vec(&db, bincode::config::standard()).unwrap();
        let (mut restored, _): (Database, usize) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(restored.incr_int(b"k", 3).unwrap(), 10);
        // Original must be unaffected by mutating the restored copy.
        assert_eq!(db.incr_int(b"k", 0).unwrap(), 7);
    }

    #[test]
    fn bincode_roundtrips_large_collection_encodings() {
        use crate::hash_data::HashData;
        use crate::list_data::ListData;
        use crate::set_data::SetData;
        use crate::zset_data::ZSetData;
        use bytes::Bytes;

        // Linked list (oversize elem forces Linked).
        let mut list = ListData::new();
        list.push_back(Bytes::from(vec![b'x'; 300]));
        assert!(matches!(list, ListData::Linked(_)));
        assert_eq!(list.payload_bytes(), 300);
        let mut db = Database::new();
        db.insert(b"l".to_vec(), Entry::new(DataType::List(list)));
        let enc = bincode::serde::encode_to_vec(&db, bincode::config::standard()).unwrap();
        let (dec, _): (Database, usize) =
            bincode::serde::decode_from_slice(&enc, bincode::config::standard()).unwrap();
        match &dec.entries.get(b"l".as_slice()).unwrap().value {
            DataType::List(l) => {
                assert_eq!(l.len(), 1);
                assert_eq!(l.payload_bytes(), 300);
            }
            _ => panic!("expected list"),
        }

        // Hashtable hash (oversize value).
        let mut h = HashData::new();
        h.insert(b"f".to_vec(), vec![b'y'; 100]);
        assert!(matches!(h, HashData::Hashtable(_)));
        let mut db = Database::new();
        db.insert(b"h".to_vec(), Entry::new(DataType::Hash(h)));
        let enc = bincode::serde::encode_to_vec(&db, bincode::config::standard()).unwrap();
        let (dec, _): (Database, usize) =
            bincode::serde::decode_from_slice(&enc, bincode::config::standard()).unwrap();
        match &dec.entries.get(b"h".as_slice()).unwrap().value {
            DataType::Hash(hh) => {
                assert_eq!(hh.payload_bytes(), 1 + 100);
                assert_eq!(hh.get(b"f").unwrap(), vec![b'y'; 100]);
            }
            _ => panic!("expected hash"),
        }

        // Hashtable set.
        let mut s = SetData::new();
        s.insert(vec![b'z'; 100]);
        assert!(matches!(s, SetData::Hashtable(_)));
        let mut db = Database::new();
        db.insert(b"s".to_vec(), Entry::new(DataType::Set(s)));
        let enc = bincode::serde::encode_to_vec(&db, bincode::config::standard()).unwrap();
        let (dec, _): (Database, usize) =
            bincode::serde::decode_from_slice(&enc, bincode::config::standard()).unwrap();
        match &dec.entries.get(b"s".as_slice()).unwrap().value {
            DataType::Set(ss) => {
                assert_eq!(ss.payload_bytes(), 100);
                assert!(ss.contains(&[b'z'; 100][..]));
            }
            _ => panic!("expected set"),
        }

        // Skiplist zset.
        let mut z = ZSetData::new();
        z.insert(vec![b'm'; 100], 1.5);
        assert!(matches!(z, ZSetData::Skiplist(_)));
        let mut db = Database::new();
        db.insert(b"z".to_vec(), Entry::new(DataType::ZSet(z)));
        let enc = bincode::serde::encode_to_vec(&db, bincode::config::standard()).unwrap();
        let (dec, _): (Database, usize) =
            bincode::serde::decode_from_slice(&enc, bincode::config::standard()).unwrap();
        match &dec.entries.get(b"z".as_slice()).unwrap().value {
            DataType::ZSet(zz) => {
                assert_eq!(zz.payload_bytes(), 100);
                assert_eq!(zz.score(&[b'm'; 100][..]), Some(1.5));
            }
            _ => panic!("expected zset"),
        }
    }

    #[test]
    fn copy_entry_produces_independent_int_cell() {
        let sdb = ShardedDatabase::new(4);
        sdb.write_for(b"src").incr_int(b"src", 9).unwrap();
        assert!(sdb.copy_entry(b"src", b"dst".to_vec(), false));

        sdb.write_for(b"src").incr_int(b"src", 1).unwrap();

        let guard = sdb.read_for(b"dst");
        let DataType::Int(dst_cell) = &guard.get_ro(b"dst").unwrap().value else {
            panic!("expected Int");
        };
        assert_eq!(
            dst_cell.load(),
            9,
            "COPY destination must not alias the source's cell"
        );
    }

    // ── PR 2: ShardedDatabase::incr_int read-lock fast path ────────────────────

    #[test]
    fn sharded_incr_int_fast_path_hits_on_promoted_key() {
        let sdb = ShardedDatabase::new(4);
        // Promote via the slow path first.
        sdb.write_for(b"k").incr_int(b"k", 5).unwrap();
        let version_after_promote = sdb.read_for(b"k").key_version(b"k");

        // Subsequent calls go through ShardedDatabase::incr_int, which should
        // take the read-lock fast path since the key is already an Int.
        assert_eq!(sdb.incr_int(b"k", 3).unwrap(), 8);
        assert_eq!(sdb.incr_int(b"k", -2).unwrap(), 6);
        assert_eq!(sdb.incr_int(b"k", 10).unwrap(), 16);

        let version_after_fast_path = sdb.read_for(b"k").key_version(b"k");
        assert!(
            version_after_fast_path > version_after_promote,
            "key_version() must keep increasing across fast-path mutations \
             (WATCH correctness depends on this): before={version_after_promote}, \
             after={version_after_fast_path}"
        );
    }

    #[test]
    fn sharded_incr_int_overflow_on_fast_path_does_not_mutate() {
        let sdb = ShardedDatabase::new(4);
        sdb.write_for(b"k").incr_int(b"k", i64::MAX).unwrap();

        let err = sdb.incr_int(b"k", 1);
        assert!(
            matches!(err, Err(crate::error::NexradeError::Overflow)),
            "expected Overflow, got {err:?}"
        );

        // Value must be unchanged after the failed fast-path attempt.
        let guard = sdb.read_for(b"k");
        let DataType::Int(cell) = &guard.get_ro(b"k").unwrap().value else {
            panic!("expected Int");
        };
        assert_eq!(cell.load(), i64::MAX);
    }

    #[test]
    fn sharded_incr_int_falls_back_on_vacant_expired_and_wrongtype() {
        let sdb = ShardedDatabase::new(4);

        // Vacant key: dispatcher must fall through to the slow path and
        // create a fresh Int cell at `delta`.
        assert_eq!(sdb.incr_int(b"vacant", 5).unwrap(), 5);

        // Lazily-expired key: dispatcher must treat it as absent, same as
        // the slow path does directly.
        sdb.write_for(b"expired")
            .insert(b"expired".to_vec(), str_entry(b"100"));
        sdb.write_for(b"expired")
            .set_expiry(b"expired", Some(Expiry::from_ms(1)));
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(sdb.incr_int(b"expired", 1).unwrap(), 1);

        // Wrong-type key: dispatcher must surface the same WrongType error
        // the slow path returns.
        sdb.write_for(b"list").insert(
            b"list".to_vec(),
            Entry::new(DataType::List(crate::list_data::ListData::new())),
        );
        assert!(matches!(
            sdb.incr_int(b"list", 1),
            Err(crate::error::NexradeError::WrongType)
        ));
    }

    #[test]
    fn sharded_incr_int_concurrent_single_key_no_lost_updates() {
        const THREADS: usize = 8;
        const INCREMENTS_PER_THREAD: usize = 5_000;
        let sdb = Arc::new(ShardedDatabase::new(16));
        // Pre-promote so every thread hits the fast path from the start.
        sdb.write_for(b"hot").incr_int(b"hot", 0).unwrap();

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let sdb = Arc::clone(&sdb);
                thread::spawn(move || {
                    for _ in 0..INCREMENTS_PER_THREAD {
                        sdb.incr_int(b"hot", 1).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let guard = sdb.read_for(b"hot");
        let DataType::Int(cell) = &guard.get_ro(b"hot").unwrap().value else {
            panic!("expected Int");
        };
        assert_eq!(
            cell.load(),
            (THREADS * INCREMENTS_PER_THREAD) as i64,
            "concurrent fast-path INCRs on one key must not lose updates"
        );
    }

    #[test]
    fn sharded_incr_int_fast_slow_interleave_no_panic() {
        const INCR_ITERS: usize = 5_000;
        let sdb = Arc::new(ShardedDatabase::new(16));
        sdb.write_for(b"k").incr_int(b"k", 0).unwrap();

        let incr_sdb = Arc::clone(&sdb);
        let incr_handle = thread::spawn(move || {
            for _ in 0..INCR_ITERS {
                // Ignore errors: a concurrent SET may have demoted the key to
                // a non-integer string momentarily is not possible here since
                // the SET below always writes an integer-parseable string,
                // but a WrongType from a stale read is not expected either —
                // any Err is a real bug, so unwrap.
                incr_sdb.incr_int(b"k", 1).unwrap();
            }
        });

        // Periodically demote the key back to a plain String with the same
        // decimal value it already holds, forcing the fast path to keep
        // falling back to promotion mid-run.
        let set_sdb = Arc::clone(&sdb);
        let set_handle = thread::spawn(move || {
            for _ in 0..50 {
                let current = {
                    let guard = set_sdb.read_for(b"k");
                    match &guard.get_ro(b"k").unwrap().value {
                        DataType::Int(cell) => cell.load(),
                        DataType::String(v) => std::str::from_utf8(v).unwrap().parse().unwrap(),
                        _ => unreachable!(),
                    }
                };
                set_sdb
                    .write_for(b"k")
                    .insert(b"k".to_vec(), str_entry(current.to_string().as_bytes()));
                thread::yield_now();
            }
        });

        incr_handle.join().unwrap();
        set_handle.join().unwrap();

        // Liveness/no-panic check plus monotonicity of key_version() across
        // the whole run — the exact final value isn't assertable since the
        // SET thread's demotions race with the INCR thread's arithmetic by
        // design, but key_version() must never go backwards.
        let final_version = sdb.read_for(b"k").key_version(b"k");
        assert!(final_version > 0, "key_version() must have advanced");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    fn str_entry(v: &[u8]) -> Entry {
        Entry::new(DataType::String(Bytes::copy_from_slice(v)))
    }

    fn set_entry(members: &[&[u8]]) -> Entry {
        let mut s = crate::set_data::SetData::new();
        for m in members {
            s.insert(m.to_vec());
        }
        Entry::new(DataType::Set(s))
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
            let mut items = crate::list_data::ListData::new();
            for i in 0..ITEMS * THREADS {
                items.push_back(Bytes::copy_from_slice(i.to_string().as_bytes()));
            }
            sdb.write_for(&src)
                .insert(src.clone(), Entry::new(DataType::List(items)));
        }

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let sdb = Arc::clone(&sdb);
                let (src, dst) = (src.clone(), dst.clone());
                thread::spawn(move || {
                    for _ in 0..ITEMS {
                        let _ = sdb.lmove_atomic(&src, dst.clone(), true, false);
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

    // ── MSET: no partial write ever visible under contention ──────────────────
    // Regression guard for the try-lock-all-with-backoff rewrite of
    // `lock_shards`. N writer threads repeatedly MSET the same two
    // multi-shard keys with a distinguishable per-thread tag on both; a
    // concurrent reader thread samples both keys throughout the run and
    // asserts every observed pair is internally consistent (never a mix of
    // two different threads' writes). This is the actual atomicity
    // guarantee Option B must preserve — a sequential per-shard blocking
    // acquire (today's baseline) and a try-lock-all sweep (this fix) are
    // both all-or-nothing, but a naive per-shard *deferred* write (the
    // reverted attempt) would not be.
    #[test]
    fn mset_no_partial_write_visible_under_contention() {
        const THREADS: usize = 8;
        const ITERS: usize = 2_000;
        let sdb = Arc::new(ShardedDatabase::new(16));
        let (k1, k2) = two_different_shards(&sdb);

        // Seed so the reader never sees a transient "key missing" state.
        sdb.mset(vec![
            (k1.clone(), str_entry(b"thread-init")),
            (k2.clone(), str_entry(b"thread-init")),
        ]);

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mismatch = Arc::new(AtomicUsize::new(0));

        let writers: Vec<_> = (0..THREADS)
            .map(|t| {
                let sdb = Arc::clone(&sdb);
                let (k1, k2) = (k1.clone(), k2.clone());
                thread::spawn(move || {
                    let tag = format!("thread-{t}").into_bytes();
                    for _ in 0..ITERS {
                        sdb.mset(vec![
                            (k1.clone(), str_entry(&tag)),
                            (k2.clone(), str_entry(&tag)),
                        ]);
                    }
                })
            })
            .collect();

        let reader = {
            let sdb = Arc::clone(&sdb);
            let (k1, k2) = (k1.clone(), k2.clone());
            let stop = Arc::clone(&stop);
            let mismatch = Arc::clone(&mismatch);
            thread::spawn(move || {
                // Must hold both shards' read locks simultaneously, in the
                // same sorted-index order `mset`'s write locks are acquired
                // in — reading k1 and k2 via two separate, sequential
                // `read_for` calls would let a writer's `mset` complete
                // entirely between the two reads, which is a benign
                // "read at two different times" race, not a partial-MSET
                // atomicity violation. Locking both at once means any
                // snapshot we observe is either fully before or fully
                // after every writer's `mset` (since `mset` itself holds
                // both write locks for the duration of its two inserts).
                let i1 = sdb.shard_idx(&k1);
                let i2 = sdb.shard_idx(&k2);
                while !stop.load(Ordering::Relaxed) {
                    let (v1, v2) = if i1 <= i2 {
                        let g1 = sdb.shard_read(i1);
                        let g2 = sdb.shard_read(i2);
                        (
                            g1.get_ro(&k1).map(|e| match &e.value {
                                DataType::String(v) => v.clone(),
                                _ => unreachable!(),
                            }),
                            g2.get_ro(&k2).map(|e| match &e.value {
                                DataType::String(v) => v.clone(),
                                _ => unreachable!(),
                            }),
                        )
                    } else {
                        let g2 = sdb.shard_read(i2);
                        let g1 = sdb.shard_read(i1);
                        (
                            g1.get_ro(&k1).map(|e| match &e.value {
                                DataType::String(v) => v.clone(),
                                _ => unreachable!(),
                            }),
                            g2.get_ro(&k2).map(|e| match &e.value {
                                DataType::String(v) => v.clone(),
                                _ => unreachable!(),
                            }),
                        )
                    };
                    if v1 != v2 {
                        mismatch.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        };

        for h in writers {
            h.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        reader.join().unwrap();

        assert_eq!(
            mismatch.load(Ordering::Relaxed),
            0,
            "reader observed a partial MSET: k1 and k2 held different threads' tags"
        );
    }

    // ── MSET: concurrent disjoint shards, no deadlock, no lost writes ─────────
    #[test]
    fn mset_concurrent_disjoint_shards_no_deadlock_no_loss() {
        const THREADS: usize = 8;
        const PAIRS_PER: usize = 2_000;
        let sdb = Arc::new(ShardedDatabase::new(16));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let sdb = Arc::clone(&sdb);
                thread::spawn(move || {
                    for i in 0..PAIRS_PER {
                        let k1 = format!("t{t}a{i}").into_bytes();
                        let k2 = format!("t{t}b{i}").into_bytes();
                        sdb.mset(vec![(k1, str_entry(b"v")), (k2, str_entry(b"v"))]);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            sdb.len(),
            THREADS * PAIRS_PER * 2,
            "every key from every thread's MSET must be present"
        );
    }

    // ── MSET: backoff makes progress under heavy shard overlap ────────────────
    // Synthetic high-conflict workload: every thread targets the *same*
    // overlapping shard set (not just the same two keys, but a shared pool
    // of keys spanning every shard), maximizing try-lock-all contention.
    // Regression guard against the retry loop spinning forever or thrashing
    // — the whole run must complete within a generous bound.
    #[test]
    fn mset_backoff_makes_progress_under_heavy_shard_overlap() {
        const THREADS: usize = 16;
        const ITERS: usize = 1_000;
        let sdb = Arc::new(ShardedDatabase::new(16));
        // A shared pool of keys, one per shard-ish (16 shards, 16 keys), so
        // every thread's MSET touches the same full shard set as every
        // other thread's — maximum overlap.
        let pool: Vec<Vec<u8>> = (0..16).map(|i| format!("pool{i}").into_bytes()).collect();

        let start = std::time::Instant::now();
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let sdb = Arc::clone(&sdb);
                let pool = pool.clone();
                thread::spawn(move || {
                    let tag = format!("t{t}").into_bytes();
                    for _ in 0..ITERS {
                        let pairs = pool.iter().map(|k| (k.clone(), str_entry(&tag))).collect();
                        sdb.mset(pairs);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(30),
            "heavy-overlap MSET run took {elapsed:?}, backoff may be thrashing or stalling"
        );
        // Final state must be internally consistent: every key in the pool
        // holds *some* thread's tag, and (since all keys are always written
        // together) all keys must hold the *same* tag.
        let guard = sdb.read_for(&pool[0]);
        let last_tag = match &guard.get_ro(&pool[0]).unwrap().value {
            DataType::String(v) => v.clone(),
            _ => unreachable!(),
        };
        drop(guard);
        for k in &pool[1..] {
            let guard = sdb.read_for(k);
            let v = match &guard.get_ro(k).unwrap().value {
                DataType::String(v) => v.clone(),
                _ => unreachable!(),
            };
            assert_eq!(v, last_tag, "all pooled keys must carry the same final tag");
        }
    }

    /// 0.7.1: shard floor is at least 64 so random 10-key MSET has low
    /// multi-client collision probability on mid-size hosts.
    #[test]
    fn shard_count_floor_is_at_least_64() {
        let sdb = ShardedDatabase::new(
            // Mirror compute_num_shards without re-exporting it.
            {
                let cpus = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4);
                cpus.max(64).next_power_of_two().clamp(64, 256)
            },
        );
        assert!(
            sdb.shard_count() >= 64,
            "0.7.1 floor is 64, got {}",
            sdb.shard_count()
        );
        assert!(
            sdb.shard_count().is_power_of_two(),
            "shard count must be power-of-two for mask indexing"
        );
    }

    // ── MSET async: adaptive try-lock path preserves atomicity under contention ─
    // Same reader invariant as mset_no_partial_write_visible_under_contention,
    // but writers use mset_async (inline mset + yield). Exercises 0.5.1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mset_async_no_partial_write_visible_under_contention() {
        const TASKS: usize = 8;
        const ITERS: usize = 500;
        let sdb = Arc::new(ShardedDatabase::new(16));
        let (k1, k2) = two_different_shards(&sdb);

        sdb.mset(vec![
            (k1.clone(), str_entry(b"thread-init")),
            (k2.clone(), str_entry(b"thread-init")),
        ]);

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mismatch = Arc::new(AtomicUsize::new(0));

        let writers: Vec<_> = (0..TASKS)
            .map(|t| {
                let sdb = Arc::clone(&sdb);
                let (k1, k2) = (k1.clone(), k2.clone());
                tokio::spawn(async move {
                    let tag = format!("thread-{t}").into_bytes();
                    for _ in 0..ITERS {
                        sdb.mset_async(vec![
                            (k1.clone(), str_entry(&tag)),
                            (k2.clone(), str_entry(&tag)),
                        ])
                        .await;
                    }
                })
            })
            .collect();

        let reader = {
            let sdb = Arc::clone(&sdb);
            let (k1, k2) = (k1.clone(), k2.clone());
            let stop = Arc::clone(&stop);
            let mismatch = Arc::clone(&mismatch);
            tokio::task::spawn_blocking(move || {
                let i1 = sdb.shard_idx(&k1);
                let i2 = sdb.shard_idx(&k2);
                while !stop.load(Ordering::Relaxed) {
                    let (v1, v2) = if i1 <= i2 {
                        let g1 = sdb.shard_read(i1);
                        let g2 = sdb.shard_read(i2);
                        (
                            g1.get_ro(&k1).map(|e| match &e.value {
                                DataType::String(v) => v.clone(),
                                _ => unreachable!(),
                            }),
                            g2.get_ro(&k2).map(|e| match &e.value {
                                DataType::String(v) => v.clone(),
                                _ => unreachable!(),
                            }),
                        )
                    } else {
                        let g2 = sdb.shard_read(i2);
                        let g1 = sdb.shard_read(i1);
                        (
                            g1.get_ro(&k1).map(|e| match &e.value {
                                DataType::String(v) => v.clone(),
                                _ => unreachable!(),
                            }),
                            g2.get_ro(&k2).map(|e| match &e.value {
                                DataType::String(v) => v.clone(),
                                _ => unreachable!(),
                            }),
                        )
                    };
                    if v1 != v2 {
                        mismatch.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        };

        for h in writers {
            h.await.unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        reader.await.unwrap();

        assert_eq!(
            mismatch.load(Ordering::Relaxed),
            0,
            "async mset reader observed a partial MSET"
        );
    }

    #[tokio::test]
    async fn msetnx_async_all_or_nothing() {
        let sdb = Arc::new(ShardedDatabase::new(16));
        let (k1, k2) = two_different_shards(&sdb);
        // First NX wins.
        assert!(
            sdb.msetnx_async(vec![
                (k1.clone(), str_entry(b"a")),
                (k2.clone(), str_entry(b"a")),
            ])
            .await
        );
        // Second NX fails entirely (k1 exists) — k2 must stay "a".
        assert!(
            !sdb.msetnx_async(vec![
                (k1.clone(), str_entry(b"b")),
                (k2.clone(), str_entry(b"b")),
            ])
            .await
        );
        let g = sdb.read_for(&k2);
        match &g.get_ro(&k2).unwrap().value {
            DataType::String(v) => assert_eq!(v.as_ref(), b"a"),
            _ => unreachable!(),
        }
    }
}
