//! Core data types for nexrade-cache.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

/// Atomic-backed integer cell — the storage for `DataType::Int`.
///
/// `value` holds the integer itself. `version` is a per-cell monotonic
/// counter bumped by `checked_add`'s CAS fast path (see
/// `ShardedDatabase::incr_int`) so `key_version()` (WATCH support) can
/// observe a fast-path mutation without touching the shard's shared
/// `key_versions` HashMap — which would require the exclusive write lock
/// this fast path exists to avoid.
///
/// `AtomicI64`/`AtomicU64` implement neither `Clone` nor value-semantics
/// `Serialize`/`Deserialize` derivation in a way that's safe to blanket-apply
/// to an `Arc`-wrapped cell (deriving `Clone` on `Arc<AtomicIntCell>` would
/// clone the *pointer*, aliasing a live, mutating cell — see the manual
/// `Clone` impl on `DataType` below). This type's own `Clone` impl does a
/// snapshot-by-value load instead.
#[derive(Debug)]
pub struct AtomicIntCell {
    value: AtomicI64,
    version: AtomicU64,
}

impl AtomicIntCell {
    pub fn new(value: i64) -> Self {
        Self {
            value: AtomicI64::new(value),
            version: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn load(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn store(&self, value: i64) {
        self.value.store(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// Read-lock-safe fast path for INCR/DECR/INCRBY/DECRBY: add `delta` via
    /// a CAS retry loop instead of the caller having to hold an exclusive
    /// write lock for a plain load+add+store. Returns `None` on `i64`
    /// overflow (mirrors `i64::checked_add`) and leaves the cell unmutated —
    /// overflow is rechecked against the latest value on every retry, so a
    /// concurrent writer can't cause this to observe stale data and wrongly
    /// accept (or reject) the add.
    ///
    /// On success, bumps the per-cell `version` counter so `key_version()`
    /// (WATCH) observes the mutation without touching
    /// `Database::key_versions`, which would require a write lock and
    /// defeat the point of this method.
    pub fn checked_add(&self, delta: i64) -> Option<i64> {
        let mut current = self.value.load(Ordering::Relaxed);
        loop {
            let new_val = current.checked_add(delta)?;
            match self.value.compare_exchange_weak(
                current,
                new_val,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.version.fetch_add(1, Ordering::Relaxed);
                    return Some(new_val);
                }
                Err(actual) => current = actual,
            }
        }
    }
}

// Snapshot-by-value: loads both atomics and builds a fresh, independent
// cell. Never used to alias a live cell — see `DataType`'s manual `Clone`.
impl Clone for AtomicIntCell {
    fn clone(&self) -> Self {
        Self {
            value: AtomicI64::new(self.value.load(Ordering::Relaxed)),
            version: AtomicU64::new(self.version.load(Ordering::Relaxed)),
        }
    }
}

// Serializes/deserializes the loaded `i64` value only — the version counter
// is per-process fast-path bookkeeping, not durable state, so RDB round-trips
// reset it to 0 (a freshly-deserialized cell has no fast-path history to
// convey, same as `key_versions` itself, which is `#[serde(skip)]` on
// `Database`).
impl Serialize for AtomicIntCell {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.load())
    }
}

impl<'de> Deserialize<'de> for AtomicIntCell {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = i64::deserialize(deserializer)?;
        Ok(AtomicIntCell::new(value))
    }
}

/// All supported Redis-compatible data types.
#[derive(Debug, Serialize, Deserialize)]
pub enum DataType {
    /// Plain string (or binary blob)
    String(Vec<u8>),
    /// Integer fast-path representation — created only by `INCR`/`DECR`/
    /// `INCRBY`/`DECRBY` on a vacant key or an existing integer-valued
    /// `String`. Any command that writes a string value directly (`SET`,
    /// `APPEND`, `SETRANGE`, `GETSET`, ...) demotes back to `String` via the
    /// normal `Database::insert()` replace path — see `command/string.rs`.
    /// Redis-compatible `TYPE`/`OBJECT ENCODING` still report this as
    /// `string`/`int`, matching real Redis's int-encoded string.
    Int(Arc<AtomicIntCell>),
    /// Doubly-linked list — elements stored as `Bytes` so clones are O(1) refcount bumps.
    List(VecDeque<Bytes>),
    /// Unordered set of unique strings
    Set(HashSet<Vec<u8>>),
    /// Sorted set: member → score mapping, ordered by score
    ZSet(ZSetData),
    /// Hash map of field → value
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    /// Bit array (stored as Vec<u8>)
    Bitmap(Vec<u8>),
    /// HyperLogLog approximation (stored as raw bytes)
    HyperLogLog(Vec<u8>),
    /// Stream with consumer-group support
    Stream(StreamData),
    /// Geospatial index (member → longitude/latitude)
    Geo(GeoData),
}

// Manual `Clone` because `Int`'s `Arc<AtomicIntCell>` must never alias a
// live, mutating cell — a derived `Clone` would just bump the `Arc`'s
// refcount, so a `Store::snapshot()`/`COPY`/`RENAME` clone would silently
// share the same cell as the original, and a subsequent INCR on either would
// corrupt the other. Every other variant is a plain owned collection, so
// `.clone()` there is the same deep copy `#[derive(Clone)]` would produce.
impl Clone for DataType {
    fn clone(&self) -> Self {
        match self {
            DataType::String(v) => DataType::String(v.clone()),
            DataType::Int(cell) => DataType::Int(Arc::new((**cell).clone())),
            DataType::List(v) => DataType::List(v.clone()),
            DataType::Set(v) => DataType::Set(v.clone()),
            DataType::ZSet(v) => DataType::ZSet(v.clone()),
            DataType::Hash(v) => DataType::Hash(v.clone()),
            DataType::Bitmap(v) => DataType::Bitmap(v.clone()),
            DataType::HyperLogLog(v) => DataType::HyperLogLog(v.clone()),
            DataType::Stream(v) => DataType::Stream(v.clone()),
            DataType::Geo(v) => DataType::Geo(v.clone()),
        }
    }
}

impl DataType {
    /// Render this value's string-representable bytes, if it has one.
    /// `String` returns its bytes directly (clone); `Int` formats the
    /// current atomic value via `itoa` (no `to_string()` allocation +
    /// re-validate). Every other variant returns `None` — callers should
    /// treat that as `WrongType`, mirroring what a bare
    /// `DataType::String(v) => ..., _ => Err(WrongType)` match already did
    /// before `Int` existed.
    pub fn as_string_bytes(&self) -> Option<Vec<u8>> {
        match self {
            DataType::String(v) => Some(v.clone()),
            DataType::Int(cell) => {
                let mut buf = itoa::Buffer::new();
                Some(buf.format(cell.load()).as_bytes().to_vec())
            }
            _ => None,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            DataType::String(_) => "string",
            DataType::Int(_) => "string", // Redis-compatible: TYPE never says "int"
            DataType::List(_) => "list",
            DataType::Set(_) => "set",
            DataType::ZSet(_) => "zset",
            DataType::Hash(_) => "hash",
            DataType::Bitmap(_) => "string", // Redis bitmap is a string type
            DataType::HyperLogLog(_) => "string",
            DataType::Stream(_) => "stream",
            DataType::Geo(_) => "zset", // Redis GEO is backed by a sorted set
        }
    }

    pub fn encoding(&self) -> &'static str {
        match self {
            DataType::String(v) => {
                // Redis uses `int` encoding when the value fits in a long integer
                if std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .is_some()
                {
                    "int"
                } else if v.len() <= 44 {
                    "embstr"
                } else {
                    "raw"
                }
            }
            // Always int-encoded by construction — no parse needed.
            DataType::Int(_) => "int",
            DataType::List(_) => "listpack",
            DataType::Set(s) => {
                if s.len() <= 128 {
                    "listpack"
                } else {
                    "hashtable"
                }
            }
            DataType::ZSet(z) => {
                if z.members.len() <= 128 {
                    "listpack"
                } else {
                    "skiplist"
                }
            }
            DataType::Hash(h) => {
                if h.len() <= 128 {
                    "listpack"
                } else {
                    "hashtable"
                }
            }
            DataType::Bitmap(_) => "raw",
            DataType::HyperLogLog(_) => "raw",
            DataType::Stream(_) => "stream",
            DataType::Geo(_) => "skiplist",
        }
    }
}

// ── Sorted Set ────────────────────────────────────────────────────────────────

/// Sorted set data structure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ZSetData {
    /// member → score
    pub members: HashMap<Vec<u8>, OrderedFloat<f64>>,
    /// Sorted index: (score, member) → ()
    pub by_score: BTreeMap<(OrderedFloat<f64>, Vec<u8>), ()>,
}

impl ZSetData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, member: Vec<u8>, score: f64) -> bool {
        let score = OrderedFloat(score);
        // Single `members.entry()` probe instead of contains_key + get +
        // insert. Also skips the by_score remove/insert churn when the
        // score is unchanged (a no-op reinsert previously always paid for
        // a BTreeMap round trip for nothing).
        match self.members.entry(member.clone()) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                let old_score = *o.get();
                if old_score != score {
                    self.by_score.remove(&(old_score, member.clone()));
                    self.by_score.insert((score, member), ());
                }
                *o.get_mut() = score;
                false
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(score);
                self.by_score.insert((score, member), ());
                true
            }
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        if let Some(score) = self.members.remove(member) {
            self.by_score.remove(&(score, member.to_vec()));
            Some(score.0)
        } else {
            None
        }
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.members.get(member).map(|s| s.0)
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn range_by_rank(&self, start: isize, stop: isize, rev: bool) -> Vec<(Vec<u8>, f64)> {
        let len = self.len() as isize;
        let start = normalize_index(start, len).min(len as usize);
        let stop = normalize_index(stop, len);
        let mut entries: Vec<_> = self
            .by_score
            .keys()
            .map(|(s, m)| (m.clone(), s.0))
            .collect();
        if rev {
            entries.reverse();
        }
        if start >= entries.len() || start > stop {
            return vec![];
        }
        let stop = stop.min(entries.len().saturating_sub(1));
        entries[start..=stop].to_vec()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn range_by_score(
        &self,
        min: f64,
        min_excl: bool,
        max: f64,
        max_excl: bool,
        rev: bool,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Vec<u8>, f64)> {
        let min_key = (OrderedFloat(min), vec![]);
        // Use take_while on an open-ended range instead of an upper key bound.
        // A fixed-length sentinel like [0xFF; 256] would exclude members with
        // score == max whose byte representation exceeds that sentinel in the
        // BTreeMap ordering, silently dropping valid entries.
        let entries: Vec<_> = self
            .by_score
            .range(min_key..)
            .take_while(|((s, _), _)| s.0 <= max)
            .map(|((s, m), _)| (m.clone(), s.0))
            .filter(|(_, s)| {
                let ok_min = if min_excl { *s > min } else { *s >= min };
                let ok_max = if max_excl { *s < max } else { *s <= max };
                ok_min && ok_max
            })
            .collect();
        let entries = if rev {
            entries.into_iter().rev().collect::<Vec<_>>()
        } else {
            entries
        };
        let entries = entries.into_iter().skip(offset);
        match count {
            Some(n) => entries.take(n).collect(),
            None => entries.collect(),
        }
    }
}

// ── Stream ────────────────────────────────────────────────────────────────────

/// A single stream entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Pending-entry record (for XPENDING).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEntry {
    /// Name of the consumer that received the message.
    pub consumer: Vec<u8>,
    /// Millisecond timestamp of the last delivery.
    pub delivery_time_ms: u64,
    /// How many times this entry has been delivered.
    pub delivery_count: u64,
}

/// A named consumer within a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Consumer {
    pub name: Vec<u8>,
    /// Entry IDs currently pending (delivered but not ACK'd) for this consumer.
    pub pending_ids: Vec<String>,
}

/// A consumer group attached to a stream key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroup {
    pub name: Vec<u8>,
    /// Last entry ID delivered to any consumer in this group.
    pub last_delivered_id: String,
    /// entry_id → pending info (not yet ACK'd).
    pub pending: BTreeMap<String, PendingEntry>,
    /// consumer name → consumer state.
    pub consumers: HashMap<Vec<u8>, Consumer>,
}

impl ConsumerGroup {
    pub fn new(name: Vec<u8>, last_id: String) -> Self {
        Self {
            name,
            last_delivered_id: last_id,
            pending: BTreeMap::new(),
            consumers: HashMap::new(),
        }
    }
}

/// Full stream data: entries + consumer groups.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamData {
    pub entries: Vec<StreamEntry>,
    /// group name → group state
    pub groups: HashMap<Vec<u8>, ConsumerGroup>,
}

impl StreamData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn estimated_size(&self) -> usize {
        self.entries
            .iter()
            .map(|e| {
                e.id.len()
                    + e.fields
                        .iter()
                        .map(|(k, v)| k.len() + v.len())
                        .sum::<usize>()
            })
            .sum()
    }
}

// ── Geospatial ────────────────────────────────────────────────────────────────

/// A single geospatial member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoPoint {
    pub longitude: f64,
    pub latitude: f64,
}

/// Geospatial index (member name → coordinate).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GeoData {
    pub members: HashMap<Vec<u8>, GeoPoint>,
}

impl GeoData {
    pub fn new() -> Self {
        Self::default()
    }
}

// ── Utility ───────────────────────────────────────────────────────────────────

fn normalize_index(idx: isize, len: isize) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx as usize
    }
}

/// Current millisecond timestamp.
pub fn now_ms() -> u64 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
    #[cfg(target_arch = "wasm32")]
    {
        0
    }
}

// ── HyperLogLog ───────────────────────────────────────────────────────────────

/// Number of registers in an HLL. Matches Redis default precision (2^14).
pub const HLL_PRECISION: u32 = 14;
/// Number of registers stored per HLL.
pub const HLL_REGISTERS: usize = 1 << 14; // 16384
/// Bits per register.
pub const HLL_BITS: u32 = 6;
/// Maximum register value (2^6 - 1). The HLL rank never exceeds 51, but we
/// store it in a u8 so the in-memory representation is one byte per register.
pub const HLL_REGISTER_MAX: u8 = (1u8 << HLL_BITS) - 1;

/// FNV-1a 64-bit hash — used both for shard selection and for HLL element
/// hashing. Deterministic and fast.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut h = OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    // Apply a MurmurHash3-style finalizer to spread the high bits more
    // evenly. FNV-1a alone gives poor distribution in the top bits for
    // short ASCII inputs (which HLL elements typically are).
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

/// Hash `element` and update the HLL register array in place. Returns true if
/// any register was updated.
pub fn hll_add(registers: &mut [u8; HLL_REGISTERS], element: &[u8]) -> bool {
    let h = fnv1a_64(element);
    let idx = (h >> (64 - HLL_PRECISION)) as usize;
    let rest = h & ((1u64 << (64 - HLL_PRECISION)) - 1);
    // ρ(w) — position of the leftmost 1-bit in `rest`, 1-indexed.
    // For a 50-bit `rest`, leading_zeros(rest) over the full u64 = 14 + zeros
    // in the 50-bit window, so position = 64 - leading_zeros(rest).
    // When `rest == 0`, ρ is conventionally 64 - p + 1 = 51.
    let rank: u8 = if rest == 0 {
        (64 - HLL_PRECISION + 1) as u8
    } else {
        (64 - rest.leading_zeros()) as u8
    };
    let rank = rank.min(HLL_REGISTER_MAX);
    if rank > registers[idx] {
        registers[idx] = rank;
        true
    } else {
        false
    }
}

/// Estimate the cardinality of an HLL. Uses the standard HyperLogLog estimator
/// with linear-counting correction for small cardinalities.
pub fn hll_count(registers: &[u8; HLL_REGISTERS]) -> u64 {
    let m = registers.len() as f64;
    let mut sum: f64 = 0.0;
    let mut zeros: u32 = 0;
    for &r in registers {
        sum += 2f64.powi(-(r as i32));
        if r == 0 {
            zeros += 1;
        }
    }
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let mut estimate = alpha * m * m / sum;

    if estimate <= 5.0 * m && zeros > 0 {
        // Linear counting: more accurate for small cardinalities.
        estimate = m * (m / zeros as f64).ln();
    } else if estimate > (2f64.powi(32)) / 30.0 {
        // Large range correction.
        estimate = -(2_f64.powi(32)) * (1.0 - estimate / 2_f64.powi(32)).ln();
    }

    estimate.max(0.0).round() as u64
}

/// Merge `other` into `dest`, taking the per-register max. Both arrays must be
/// of length `HLL_REGISTERS`.
pub fn hll_merge_into(dest: &mut [u8; HLL_REGISTERS], other: &[u8; HLL_REGISTERS]) {
    for (d, o) in dest.iter_mut().zip(other.iter()) {
        if *o > *d {
            *d = *o;
        }
    }
}
