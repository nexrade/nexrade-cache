//! Core data types for nexrade-cache.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

/// All supported Redis-compatible data types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    /// Plain string (or binary blob)
    String(Vec<u8>),
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

impl DataType {
    pub fn type_name(&self) -> &'static str {
        match self {
            DataType::String(_) => "string",
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
        let is_new = !self.members.contains_key(&member);
        if let Some(&old_score) = self.members.get(&member) {
            self.by_score.remove(&(old_score, member.clone()));
        }
        self.members.insert(member.clone(), score);
        self.by_score.insert((score, member), ());
        is_new
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
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
