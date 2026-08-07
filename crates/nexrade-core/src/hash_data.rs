//! Dual-encoded hash storage for short-hash HGETALL locality.
//!
//! - [`HashData::Compact`]: contiguous length-prefixed field/value pairs.
//! - [`HashData::Hashtable`]: `HashMap` for large / write-heavy hashes.
//!
//! Format (Compact): `[u32_le flen][field][u32_le vlen][value]…`
//! Thresholds are process-wide atomics so `CONFIG SET` can retune promote.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Promote Compact → Hashtable when any of these is exceeded (defaults).
pub const HASH_COMPACT_MAX_ENTRIES: usize = 512;
pub const HASH_COMPACT_MAX_SIZE: usize = 8192;
pub const HASH_COMPACT_MAX_VALUE: usize = 64;
/// After HDEL, Hashtable → Compact when at or under this entry count.
pub const HASH_DEMOTE_ENTRIES: usize = 128;

static MAX_ENTRIES: AtomicUsize = AtomicUsize::new(HASH_COMPACT_MAX_ENTRIES);
static MAX_SIZE: AtomicUsize = AtomicUsize::new(HASH_COMPACT_MAX_SIZE);
static MAX_VALUE: AtomicUsize = AtomicUsize::new(HASH_COMPACT_MAX_VALUE);
static DEMOTE_ENTRIES: AtomicUsize = AtomicUsize::new(HASH_DEMOTE_ENTRIES);

#[derive(Debug, Clone, Copy)]
pub struct HashThresholds {
    pub max_entries: usize,
    pub max_size: usize,
    pub max_value: usize,
    pub demote_entries: usize,
}

impl Default for HashThresholds {
    fn default() -> Self {
        Self {
            max_entries: HASH_COMPACT_MAX_ENTRIES,
            max_size: HASH_COMPACT_MAX_SIZE,
            max_value: HASH_COMPACT_MAX_VALUE,
            demote_entries: HASH_DEMOTE_ENTRIES,
        }
    }
}

#[inline]
pub fn hash_thresholds() -> HashThresholds {
    HashThresholds {
        max_entries: MAX_ENTRIES.load(Ordering::Relaxed),
        max_size: MAX_SIZE.load(Ordering::Relaxed),
        max_value: MAX_VALUE.load(Ordering::Relaxed),
        demote_entries: DEMOTE_ENTRIES.load(Ordering::Relaxed),
    }
}

pub fn set_hash_thresholds(t: HashThresholds) {
    MAX_ENTRIES.store(t.max_entries, Ordering::Relaxed);
    MAX_SIZE.store(t.max_size, Ordering::Relaxed);
    MAX_VALUE.store(t.max_value, Ordering::Relaxed);
    DEMOTE_ENTRIES.store(t.demote_entries, Ordering::Relaxed);
}

/// Large-hash encoding: `HashMap` plus a cached field+value byte total so
/// `payload_bytes()` stays O(1) after Compact → Hashtable promote.
///
/// **Wire format**: `map` only (`bytes` skipped). HashData uses the default
/// (externally tagged) enum representation for bincode compatibility.
#[derive(Debug, Clone, Serialize)]
pub struct HashtableHash {
    map: HashMap<Vec<u8>, Vec<u8>>,
    #[serde(skip)]
    bytes: usize,
}

impl<'de> Deserialize<'de> for HashtableHash {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            map: HashMap<Vec<u8>, Vec<u8>>,
        }
        let w = Wire::deserialize(deserializer)?;
        Ok(Self::from_map(w.map))
    }
}

impl HashtableHash {
    fn with_capacity(n: usize) -> Self {
        Self {
            map: HashMap::with_capacity(n),
            bytes: 0,
        }
    }

    fn from_map(map: HashMap<Vec<u8>, Vec<u8>>) -> Self {
        let bytes = map.iter().map(|(k, v)| k.len() + v.len()).sum();
        Self { map, bytes }
    }

    #[inline]
    fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    fn payload_bytes(&self) -> usize {
        self.bytes
    }

    /// Insert field/value. Returns `(is_new, payload_delta)`.
    fn insert(&mut self, field: Vec<u8>, val: Vec<u8>) -> (bool, isize) {
        let fl = field.len();
        let vl = val.len();
        match self.map.insert(field, val) {
            None => {
                self.bytes += fl + vl;
                (true, (fl + vl) as isize)
            }
            Some(old) => {
                let ol = old.len();
                self.bytes = self.bytes.saturating_sub(ol).saturating_add(vl);
                // field length already counted
                (false, vl as isize - ol as isize)
            }
        }
    }

    fn get(&self, field: &[u8]) -> Option<Vec<u8>> {
        self.map.get(field).cloned()
    }

    fn contains_key(&self, field: &[u8]) -> bool {
        self.map.contains_key(field)
    }

    fn remove(&mut self, field: &[u8]) -> Option<(Vec<u8>, isize)> {
        let v = self.map.remove(field)?;
        let delta = -((field.len() + v.len()) as isize);
        self.bytes = self
            .bytes
            .saturating_sub(field.len())
            .saturating_sub(v.len());
        Some((v, delta))
    }

    fn drain_into_pairs(self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        self.map.into_iter()
    }
}

/// Dual-encoded hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HashData {
    Compact(CompactHash),
    Hashtable(HashtableHash),
}

impl Default for HashData {
    fn default() -> Self {
        Self::new()
    }
}

impl HashData {
    pub fn new() -> Self {
        HashData::Compact(CompactHash::new())
    }

    pub fn from_map(m: HashMap<Vec<u8>, Vec<u8>>) -> Self {
        HashData::Hashtable(HashtableHash::from_map(m))
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            HashData::Compact(c) => c.len,
            HashData::Hashtable(h) => h.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encoding_name(&self) -> &'static str {
        match self {
            HashData::Compact(_) => "listpack",
            HashData::Hashtable(_) => "hashtable",
        }
    }

    #[inline]
    pub fn payload_bytes(&self) -> usize {
        match self {
            HashData::Compact(c) => c.bytes,
            HashData::Hashtable(h) => h.payload_bytes(),
        }
    }

    pub fn estimated_size(&self) -> usize {
        match self {
            HashData::Compact(c) => {
                c.buf.capacity()
                    + std::mem::size_of::<CompactHash>()
                    + std::mem::size_of::<Arc<Vec<u8>>>()
            }
            HashData::Hashtable(h) => {
                h.payload_bytes() + h.len() * 32 + std::mem::size_of::<HashtableHash>()
            }
        }
    }

    pub fn ensure_hashtable(&mut self) {
        if let HashData::Compact(c) = self {
            let mut h = HashtableHash::with_capacity(c.len);
            for (f, v) in c.iter_pairs() {
                h.insert(f, v);
            }
            // Prefer Compact's precomputed sum if the walk drifted.
            h.bytes = c.bytes;
            *self = HashData::Hashtable(h);
        }
    }

    pub fn maybe_demote(&mut self) {
        let thr = hash_thresholds();
        {
            let HashData::Hashtable(h) = self else {
                return;
            };
            if h.len() > thr.demote_entries {
                return;
            }
            let mut total = 0usize;
            for (k, v) in h.map.iter() {
                if v.len() > thr.max_value || k.len() > thr.max_value {
                    return;
                }
                total = total.saturating_add(k.len() + v.len());
                if total > thr.max_size {
                    return;
                }
            }
        }
        let HashData::Hashtable(h) = std::mem::replace(self, HashData::Compact(CompactHash::new()))
        else {
            unreachable!()
        };
        let total = h.payload_bytes();
        let mut c = CompactHash::with_capacity(h.len(), total);
        for (k, v) in h.drain_into_pairs() {
            c.append_pair(&k, &v);
        }
        *self = HashData::Compact(c);
    }

    /// Insert field/value. Returns `(is_new, payload_delta)`.
    pub fn insert(&mut self, field: Vec<u8>, val: Vec<u8>) -> (bool, isize) {
        // Hashtable: one HashMap insert, no pre-scan.
        if matches!(self, HashData::Hashtable(_)) {
            return match self {
                HashData::Hashtable(h) => h.insert(field, val),
                _ => unreachable!(),
            };
        }
        // Compact: only pay contains_key when a promote decision needs is_new.
        // Oversized field/value always promote; size/count promote only for new fields.
        let thr = hash_thresholds();
        if field.len() > thr.max_value || val.len() > thr.max_value {
            self.ensure_hashtable();
            return match self {
                HashData::Hashtable(h) => h.insert(field, val),
                _ => unreachable!(),
            };
        }
        let maybe_need_promote = match self {
            HashData::Compact(c) => {
                c.len >= thr.max_entries || c.bytes + field.len() + val.len() > thr.max_size
            }
            HashData::Hashtable(_) => false,
        };
        if maybe_need_promote && !self.contains_key(&field) {
            self.ensure_hashtable();
            return match self {
                HashData::Hashtable(h) => h.insert(field, val),
                _ => unreachable!(),
            };
        }
        match self {
            HashData::Compact(c) => c.insert(field, val),
            HashData::Hashtable(h) => h.insert(field, val),
        }
    }

    pub fn get(&self, field: &[u8]) -> Option<Vec<u8>> {
        match self {
            HashData::Compact(c) => c.get(field),
            HashData::Hashtable(h) => h.get(field),
        }
    }

    pub fn contains_key(&self, field: &[u8]) -> bool {
        match self {
            HashData::Compact(c) => c.contains_key(field),
            HashData::Hashtable(h) => h.contains_key(field),
        }
    }

    /// Remove field. Returns `(value, payload_delta)` when present.
    pub fn remove(&mut self, field: &[u8]) -> Option<(Vec<u8>, isize)> {
        let v = match self {
            HashData::Compact(c) => c.remove(field),
            HashData::Hashtable(h) => h.remove(field),
        };
        if v.is_some() {
            self.maybe_demote();
        }
        v
    }

    /// Collect all field/value pairs as owned Vecs (AOF / HGETALL helpers).
    pub fn to_pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            HashData::Compact(c) => c.iter_pairs().collect(),
            HashData::Hashtable(h) => h.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        }
    }

    /// Iterate `(field, value)` as borrowed slices — no allocation per element.
    ///
    /// `to_pairs` clones every field *and* value, which is the right shape when
    /// the caller keeps them all. `HSCAN` does not: it inspects every field to
    /// pick the handful that belong on one page, so cloning the whole hash per
    /// page was the dominant cost of a scan (and made a full walk quadratic
    /// even after the sort was removed).
    pub fn iter_pairs_ref(&self) -> Box<dyn Iterator<Item = (&[u8], &[u8])> + '_> {
        match self {
            HashData::Compact(c) => Box::new(c.iter_pairs_ref()),
            HashData::Hashtable(h) => {
                Box::new(h.map.iter().map(|(k, v)| (k.as_slice(), v.as_slice())))
            }
        }
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        match self {
            HashData::Compact(c) => c.iter_pairs().map(|(k, _)| k).collect(),
            HashData::Hashtable(h) => h.map.keys().cloned().collect(),
        }
    }

    pub fn values(&self) -> Vec<Vec<u8>> {
        match self {
            HashData::Compact(c) => c.iter_pairs().map(|(_, v)| v).collect(),
            HashData::Hashtable(h) => h.map.values().cloned().collect(),
        }
    }

    /// Arc-clone Compact buffer for HGETALL framing outside the lock.
    pub fn hgetall_snap(&self) -> HashGetAllSnap {
        match self {
            HashData::Compact(c) => HashGetAllSnap::Compact {
                buf: Arc::clone(&c.buf),
                count: c.len,
                need: c.hgetall_resp_need(),
            },
            HashData::Hashtable(h) => HashGetAllSnap::Hashtable {
                pairs: h.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            },
        }
    }
}

/// Snapshot for HGETALL framing outside the shard lock.
pub enum HashGetAllSnap {
    Compact {
        buf: Arc<Vec<u8>>,
        count: usize,
        need: usize,
    },
    Hashtable {
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
}

/// Contiguous length-prefixed field/value pairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactHash {
    buf: Arc<Vec<u8>>,
    len: usize,
    /// Sum of field + value byte lengths (no length prefixes).
    bytes: usize,
}

impl CompactHash {
    fn new() -> Self {
        Self {
            buf: Arc::new(Vec::new()),
            len: 0,
            bytes: 0,
        }
    }

    fn with_capacity(n: usize, payload: usize) -> Self {
        Self {
            buf: Arc::new(Vec::with_capacity(n * 8 + payload)),
            len: 0,
            bytes: 0,
        }
    }

    fn buf_mut(&mut self) -> &mut Vec<u8> {
        Arc::make_mut(&mut self.buf)
    }

    fn append_pair(&mut self, field: &[u8], val: &[u8]) {
        let buf = self.buf_mut();
        buf.extend_from_slice(&(field.len() as u32).to_le_bytes());
        buf.extend_from_slice(field);
        buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
        buf.extend_from_slice(val);
        self.len += 1;
        self.bytes += field.len() + val.len();
    }

    fn insert(&mut self, field: Vec<u8>, val: Vec<u8>) -> (bool, isize) {
        // redis-benchmark HSET hammers a single field repeatedly after the
        // first create. Prefer:
        //   1. scan once for an existing field
        //   2. same-length value → in-place overwrite (no realloc)
        //   3. different length → rebuild without that pair + append
        //   4. missing → append (no full-buffer rebuild)
        let mut off = 0usize;
        for _ in 0..self.len {
            let fl = u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap()) as usize;
            let f = &self.buf[off + 4..off + 4 + fl];
            let vo = off + 4 + fl;
            let vl = u32::from_le_bytes(self.buf[vo..vo + 4].try_into().unwrap()) as usize;
            if f == field.as_slice() {
                let old_end = vo + 4 + vl;
                if vl == val.len() {
                    // In-place value overwrite — common steady-state HSET.
                    let buf = self.buf_mut();
                    buf[vo + 4..old_end].copy_from_slice(&val);
                    return (false, 0);
                }
                // Different value length: drop old pair, append new at end.
                let mut new_buf = Vec::with_capacity(self.buf.len() - vl + val.len());
                new_buf.extend_from_slice(&self.buf[..off]);
                new_buf.extend_from_slice(&self.buf[old_end..]);
                new_buf.extend_from_slice(&(field.len() as u32).to_le_bytes());
                new_buf.extend_from_slice(&field);
                new_buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
                new_buf.extend_from_slice(&val);
                let delta = val.len() as isize - vl as isize;
                self.bytes = self.bytes - vl + val.len();
                self.buf = Arc::new(new_buf);
                return (false, delta);
            }
            off = vo + 4 + vl;
        }
        // New field: append only.
        let delta = (field.len() + val.len()) as isize;
        self.append_pair(&field, &val);
        (true, delta)
    }

    fn get(&self, field: &[u8]) -> Option<Vec<u8>> {
        let mut off = 0usize;
        for _ in 0..self.len {
            let fl = u32::from_le_bytes(self.buf[off..off + 4].try_into().ok()?) as usize;
            let f = &self.buf[off + 4..off + 4 + fl];
            let vo = off + 4 + fl;
            let vl = u32::from_le_bytes(self.buf[vo..vo + 4].try_into().ok()?) as usize;
            if f == field {
                return Some(self.buf[vo + 4..vo + 4 + vl].to_vec());
            }
            off = vo + 4 + vl;
        }
        None
    }

    fn contains_key(&self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    fn remove(&mut self, field: &[u8]) -> Option<(Vec<u8>, isize)> {
        let mut found = None;
        let mut new_buf = Vec::with_capacity(self.buf.len());
        let mut off = 0usize;
        let mut new_len = 0usize;
        let mut new_bytes = 0usize;
        for _ in 0..self.len {
            let fl = u32::from_le_bytes(self.buf[off..off + 4].try_into().ok()?) as usize;
            let f = &self.buf[off + 4..off + 4 + fl];
            let vo = off + 4 + fl;
            let vl = u32::from_le_bytes(self.buf[vo..vo + 4].try_into().ok()?) as usize;
            let v = &self.buf[vo + 4..vo + 4 + vl];
            if f == field && found.is_none() {
                found = Some((v.to_vec(), -((fl + vl) as isize)));
            } else {
                new_buf.extend_from_slice(&self.buf[off..vo + 4 + vl]);
                new_len += 1;
                new_bytes += fl + vl;
            }
            off = vo + 4 + vl;
        }
        if found.is_some() {
            self.buf = Arc::new(new_buf);
            self.len = new_len;
            self.bytes = new_bytes;
        }
        found
    }

    fn iter_pairs(&self) -> CompactHashIter<'_> {
        CompactHashIter {
            buf: &self.buf,
            off: 0,
            remaining: self.len,
        }
    }

    /// Borrowing variant of [`Self::iter_pairs`] — no allocation per element.
    fn iter_pairs_ref(&self) -> CompactHashRefIter<'_> {
        CompactHashRefIter {
            buf: &self.buf,
            off: 0,
            remaining: self.len,
        }
    }

    fn hgetall_resp_need(&self) -> usize {
        // 2*N bulks: field + value. Framing ~12 each + payload.
        let n = self.len * 2;
        16 + n * 12 + self.bytes
    }
}

pub struct CompactHashIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl Iterator for CompactHashIter<'_> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let fl = u32::from_le_bytes(self.buf[self.off..self.off + 4].try_into().ok()?) as usize;
        let f = self.buf[self.off + 4..self.off + 4 + fl].to_vec();
        let vo = self.off + 4 + fl;
        let vl = u32::from_le_bytes(self.buf[vo..vo + 4].try_into().ok()?) as usize;
        let v = self.buf[vo + 4..vo + 4 + vl].to_vec();
        self.off = vo + 4 + vl;
        self.remaining -= 1;
        Some((f, v))
    }
}

/// Borrowing counterpart to [`CompactHashIter`], yielding `(&field, &value)`
/// slices into the compact buffer instead of fresh `Vec`s.
///
/// Exists for `HSCAN`: page selection only needs to *inspect* every field to
/// decide which handful belong on the page, so cloning all of them first is
/// pure waste. See [`HashData::iter_pairs_ref`].
pub struct CompactHashRefIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl<'a> Iterator for CompactHashRefIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let fl = u32::from_le_bytes(self.buf[self.off..self.off + 4].try_into().ok()?) as usize;
        let f = &self.buf[self.off + 4..self.off + 4 + fl];
        let vo = self.off + 4 + fl;
        let vl = u32::from_le_bytes(self.buf[vo..vo + 4].try_into().ok()?) as usize;
        let v = &self.buf[vo + 4..vo + 4 + vl];
        self.off = vo + 4 + vl;
        self.remaining -= 1;
        Some((f, v))
    }
}

/// Walk Compact HGETALL region as bulk slices (field, value, field, value…).
pub struct CompactHashBulkIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining_bulks: usize,
}

impl CompactHashBulkIter<'_> {
    pub fn new(buf: &[u8], pairs: usize) -> CompactHashBulkIter<'_> {
        CompactHashBulkIter {
            buf,
            off: 0,
            remaining_bulks: pairs * 2,
        }
    }
}

impl<'a> Iterator for CompactHashBulkIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_bulks == 0 {
            return None;
        }
        let n = u32::from_le_bytes(self.buf[self.off..self.off + 4].try_into().ok()?) as usize;
        let slice = &self.buf[self.off + 4..self.off + 4 + n];
        self.off += 4 + n;
        self.remaining_bulks -= 1;
        Some(slice)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining_bulks, Some(self.remaining_bulks))
    }
}

impl ExactSizeIterator for CompactHashBulkIter<'_> {
    fn len(&self) -> usize {
        self.remaining_bulks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_insert_get_remove() {
        let mut h = HashData::new();
        assert!(h.insert(b"a".to_vec(), b"1".to_vec()).0);
        assert!(!h.insert(b"a".to_vec(), b"2".to_vec()).0);
        assert_eq!(h.get(b"a").unwrap(), b"2");
        assert!(matches!(h, HashData::Compact(_)));
        assert_eq!(h.remove(b"a").unwrap().0, b"2");
        assert!(h.is_empty());
    }

    #[test]
    fn promote_on_max_value() {
        let mut h = HashData::new();
        let big = vec![b'x'; HASH_COMPACT_MAX_VALUE + 1];
        assert!(h.insert(b"f".to_vec(), big).0);
        assert!(matches!(h, HashData::Hashtable(_)));
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn promote_on_entries() {
        let prev = hash_thresholds();
        set_hash_thresholds(HashThresholds {
            max_entries: 3,
            ..prev
        });
        let mut h = HashData::new();
        h.insert(b"a".to_vec(), b"1".to_vec());
        h.insert(b"b".to_vec(), b"2".to_vec());
        h.insert(b"c".to_vec(), b"3".to_vec());
        assert!(matches!(h, HashData::Compact(_)));
        h.insert(b"d".to_vec(), b"4".to_vec());
        assert!(matches!(h, HashData::Hashtable(_)));
        set_hash_thresholds(prev);
    }

    #[test]
    fn demote_after_remove() {
        let mut h = HashData::new();
        let big = vec![b'y'; HASH_COMPACT_MAX_VALUE + 1];
        h.insert(b"big".to_vec(), big);
        assert!(matches!(h, HashData::Hashtable(_)));
        h.remove(b"big");
        h.insert(b"a".to_vec(), b"1".to_vec());
        // still hashtable until remove triggers demote
        h.remove(b"a");
        // empty hashtable may demote
        assert!(h.is_empty());
    }

    #[test]
    fn hashtable_payload_bytes_tracks_insert_remove() {
        let mut h = HashData::new();
        let big = vec![b'z'; HASH_COMPACT_MAX_VALUE + 1];
        let _ = h.insert(b"f".to_vec(), big.clone());
        assert!(matches!(h, HashData::Hashtable(_)));
        assert_eq!(h.payload_bytes(), 1 + big.len());
        let _ = h.insert(b"g".to_vec(), b"ab".to_vec());
        assert_eq!(h.payload_bytes(), 1 + big.len() + 1 + 2);
        h.remove(b"f");
        assert_eq!(h.payload_bytes(), 1 + 2);
        // Wire-compatible round-trip recomputes bytes from the map.
        let encoded = bincode::serde::encode_to_vec(&h, bincode::config::standard()).unwrap();
        let (decoded, _): (HashData, usize) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded.payload_bytes(), h.payload_bytes());
        assert_eq!(decoded.get(b"g").unwrap(), b"ab");
    }
}
