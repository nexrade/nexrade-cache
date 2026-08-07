//! Dual-encoded set storage for short-set SMEMBERS locality.
//!
//! - [`SetData::Compact`]: contiguous length-prefixed members.
//! - [`SetData::Hashtable`]: `HashSet` for large sets.
//!
//! Format (Compact): `[u32_le len][bytes]…`
//! Thresholds are process-wide atomics so `CONFIG SET` can retune promote.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use indexmap::IndexSet;
use serde::{Deserialize, Serialize};

/// Promote Compact → Hashtable when any of these is exceeded (defaults).
pub const SET_COMPACT_MAX_ENTRIES: usize = 512;
pub const SET_COMPACT_MAX_SIZE: usize = 8192;
pub const SET_COMPACT_MAX_VALUE: usize = 64;
/// After SREM/SPOP, Hashtable → Compact when at or under this entry count.
pub const SET_DEMOTE_ENTRIES: usize = 128;

static MAX_ENTRIES: AtomicUsize = AtomicUsize::new(SET_COMPACT_MAX_ENTRIES);
static MAX_SIZE: AtomicUsize = AtomicUsize::new(SET_COMPACT_MAX_SIZE);
static MAX_VALUE: AtomicUsize = AtomicUsize::new(SET_COMPACT_MAX_VALUE);
static DEMOTE_ENTRIES: AtomicUsize = AtomicUsize::new(SET_DEMOTE_ENTRIES);

#[derive(Debug, Clone, Copy)]
pub struct SetThresholds {
    pub max_entries: usize,
    pub max_size: usize,
    pub max_value: usize,
    pub demote_entries: usize,
}

impl Default for SetThresholds {
    fn default() -> Self {
        Self {
            max_entries: SET_COMPACT_MAX_ENTRIES,
            max_size: SET_COMPACT_MAX_SIZE,
            max_value: SET_COMPACT_MAX_VALUE,
            demote_entries: SET_DEMOTE_ENTRIES,
        }
    }
}

#[inline]
pub fn set_thresholds() -> SetThresholds {
    SetThresholds {
        max_entries: MAX_ENTRIES.load(Ordering::Relaxed),
        max_size: MAX_SIZE.load(Ordering::Relaxed),
        max_value: MAX_VALUE.load(Ordering::Relaxed),
        demote_entries: DEMOTE_ENTRIES.load(Ordering::Relaxed),
    }
}

pub fn set_set_thresholds(t: SetThresholds) {
    MAX_ENTRIES.store(t.max_entries, Ordering::Relaxed);
    MAX_SIZE.store(t.max_size, Ordering::Relaxed);
    MAX_VALUE.store(t.max_value, Ordering::Relaxed);
    DEMOTE_ENTRIES.store(t.demote_entries, Ordering::Relaxed);
}

/// Large-set encoding: an `IndexSet` plus a cached member-byte total so
/// `payload_bytes()` stays O(1) after Compact → Hashtable promote.
///
/// **Why `IndexSet` and not `HashSet`** (1.4.2): `SPOP` and `SRANDMEMBER` need
/// a *random member by index*, and `HashSet` offers no indexed access — the
/// previous implementation used `set.iter().nth(idx)`, an O(n) walk of ~n/2
/// entries per call. Measured on the release binary before this change:
///
/// | Cardinality | `SPOP` | `SRANDMEMBER` |
/// |---|---|---|
/// | 1 000 | 157 895 rps | 200 000 rps |
/// | 40 000 | **42 254 rps** | **10 526 rps** |
///
/// A `SADD`-producer / `SPOP`-consumer work queue — a common Redis pattern
/// that Redis itself serves in O(1) — therefore degraded as the queue grew.
/// `IndexSet` keeps hash-set semantics (uniqueness, O(1) contains/insert)
/// while adding O(1) `get_index` and O(1) `swap_remove_index`.
///
/// **Iteration order is insertion order**, where `HashSet` was arbitrary.
/// Redis makes no ordering promise for `SMEMBERS`/`SSCAN`, so this is not an
/// observable contract change — but it does mean set iteration is now
/// deterministic, which is strictly easier to reason about.
///
/// **Wire format**: `set` only (`bytes` skipped). SetData uses the default
/// (externally tagged) enum representation for bincode compatibility. An
/// `IndexSet` serialises as a sequence exactly as `HashSet` did, so RDB and
/// AOF files interoperate in both directions across this change.
#[derive(Debug, Clone, Serialize)]
pub struct HashtableSet {
    set: IndexSet<Vec<u8>>,
    #[serde(skip)]
    bytes: usize,
}

impl<'de> Deserialize<'de> for HashtableSet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            set: IndexSet<Vec<u8>>,
        }
        let w = Wire::deserialize(deserializer)?;
        Ok(Self::from_index_set(w.set))
    }
}

impl HashtableSet {
    fn with_capacity(n: usize) -> Self {
        Self {
            set: IndexSet::with_capacity(n),
            bytes: 0,
        }
    }

    fn from_set(set: HashSet<Vec<u8>>) -> Self {
        Self::from_index_set(set.into_iter().collect())
    }

    fn from_index_set(set: IndexSet<Vec<u8>>) -> Self {
        let bytes = set.iter().map(|m| m.len()).sum();
        Self { set, bytes }
    }

    #[inline]
    fn len(&self) -> usize {
        self.set.len()
    }

    #[inline]
    fn payload_bytes(&self) -> usize {
        self.bytes
    }

    /// Returns `(is_new, payload_delta)`.
    fn insert(&mut self, member: Vec<u8>) -> (bool, isize) {
        let n = member.len();
        if self.set.insert(member) {
            self.bytes += n;
            (true, n as isize)
        } else {
            (false, 0)
        }
    }

    fn contains(&self, member: &[u8]) -> bool {
        self.set.contains(member)
    }

    /// Returns `(removed, payload_delta)`.
    ///
    /// Uses `swap_remove` (O(1)) rather than `shift_remove` (O(n)). That
    /// permutes the index of at most one other member, which is fine because
    /// nothing depends on a stable index across mutations — indices are only
    /// used transiently to pick a random member.
    fn remove(&mut self, member: &[u8]) -> (bool, isize) {
        if self.set.swap_remove(member) {
            let n = member.len();
            self.bytes = self.bytes.saturating_sub(n);
            (true, -(n as isize))
        } else {
            (false, 0)
        }
    }

    /// Member at `idx` in O(1). `None` if out of range.
    #[inline]
    fn get_index(&self, idx: usize) -> Option<&Vec<u8>> {
        self.set.get_index(idx)
    }

    /// Remove and return the member at `idx` in O(1).
    fn swap_remove_index(&mut self, idx: usize) -> Option<(Vec<u8>, isize)> {
        let m = self.set.swap_remove_index(idx)?;
        let n = m.len();
        self.bytes = self.bytes.saturating_sub(n);
        Some((m, -(n as isize)))
    }

    fn drain(self) -> impl Iterator<Item = Vec<u8>> {
        self.set.into_iter()
    }
}

/// Dual-encoded set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SetData {
    Compact(CompactSet),
    Hashtable(HashtableSet),
}

impl Default for SetData {
    fn default() -> Self {
        Self::new()
    }
}

impl SetData {
    pub fn new() -> Self {
        SetData::Compact(CompactSet::new())
    }

    pub fn from_hashset(s: HashSet<Vec<u8>>) -> Self {
        // Prefer Compact when small enough.
        let thr = set_thresholds();
        if s.len() <= thr.max_entries {
            let mut total = 0usize;
            let mut ok = true;
            for m in &s {
                if m.len() > thr.max_value {
                    ok = false;
                    break;
                }
                total = total.saturating_add(m.len());
                if total > thr.max_size {
                    ok = false;
                    break;
                }
            }
            if ok {
                let mut c = CompactSet::with_capacity(s.len(), total);
                for m in s {
                    c.append_member(&m);
                }
                return SetData::Compact(c);
            }
        }
        SetData::Hashtable(HashtableSet::from_set(s))
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            SetData::Compact(c) => c.len,
            SetData::Hashtable(s) => s.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encoding_name(&self) -> &'static str {
        match self {
            SetData::Compact(_) => "listpack",
            SetData::Hashtable(_) => "hashtable",
        }
    }

    #[inline]
    pub fn payload_bytes(&self) -> usize {
        match self {
            SetData::Compact(c) => c.bytes,
            SetData::Hashtable(s) => s.payload_bytes(),
        }
    }

    pub fn ensure_hashtable(&mut self) {
        if let SetData::Compact(c) = self {
            let mut s = HashtableSet::with_capacity(c.len);
            let payload = c.bytes;
            for m in c.iter_members() {
                s.insert(m);
            }
            s.bytes = payload;
            *self = SetData::Hashtable(s);
        }
    }

    pub fn maybe_demote(&mut self) {
        let thr = set_thresholds();
        {
            let SetData::Hashtable(s) = self else {
                return;
            };
            if s.len() > thr.demote_entries {
                return;
            }
            for m in s.set.iter() {
                if m.len() > thr.max_value {
                    return;
                }
            }
            if s.payload_bytes() > thr.max_size {
                return;
            }
        }
        let SetData::Hashtable(s) = std::mem::replace(self, SetData::Compact(CompactSet::new()))
        else {
            unreachable!()
        };
        let total = s.payload_bytes();
        let mut c = CompactSet::with_capacity(s.len(), total);
        for m in s.drain() {
            c.append_member(&m);
        }
        *self = SetData::Compact(c);
    }

    fn should_promote(&self, member_len: usize, is_new: bool) -> bool {
        let thr = set_thresholds();
        match self {
            SetData::Compact(c) => {
                member_len > thr.max_value
                    || (is_new && c.len + 1 > thr.max_entries)
                    || c.bytes + member_len > thr.max_size
            }
            SetData::Hashtable(_) => false,
        }
    }

    /// Insert member. Returns `(is_new, payload_delta)`.
    pub fn insert(&mut self, member: Vec<u8>) -> (bool, isize) {
        let is_new = !self.contains(&member);
        if !is_new {
            return (false, 0);
        }
        if self.should_promote(member.len(), true) {
            self.ensure_hashtable();
        }
        match self {
            SetData::Compact(c) => c.insert(member),
            SetData::Hashtable(s) => s.insert(member),
        }
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        match self {
            SetData::Compact(c) => c.contains(member),
            SetData::Hashtable(s) => s.contains(member),
        }
    }

    /// Remove member. Returns `(removed, payload_delta)`.
    pub fn remove(&mut self, member: &[u8]) -> (bool, isize) {
        let (removed, delta) = match self {
            SetData::Compact(c) => c.remove(member),
            SetData::Hashtable(s) => s.remove(member),
        };
        if removed {
            self.maybe_demote();
        }
        (removed, delta)
    }

    pub fn to_vec(&self) -> Vec<Vec<u8>> {
        match self {
            SetData::Compact(c) => c.iter_members().collect(),
            SetData::Hashtable(s) => s.set.iter().cloned().collect(),
        }
    }

    /// Iterate members as borrowed slices — no allocation per element.
    /// See [`crate::hash_data::HashData::iter_pairs_ref`] for the rationale.
    pub fn iter_ref(&self) -> Box<dyn Iterator<Item = &[u8]> + '_> {
        match self {
            SetData::Compact(c) => Box::new(c.iter_members_ref()),
            SetData::Hashtable(s) => Box::new(s.set.iter().map(|m| m.as_slice())),
        }
    }

    pub fn to_hashset(&self) -> HashSet<Vec<u8>> {
        match self {
            SetData::Compact(c) => c.iter_members().collect(),
            // Was `s.set.clone()` when the backing store was a `HashSet`.
            // `IndexSet` needs an explicit rebuild; callers want set algebra
            // (SINTER/SUNION/SDIFF), not order, so a plain `HashSet` is still
            // the right return type.
            SetData::Hashtable(s) => s.set.iter().cloned().collect(),
        }
    }

    /// Pop one member at `idx` (0..len). Returns None if empty.
    ///
    /// O(1) for the Hashtable encoding as of 1.4.2 (was an O(n)
    /// `iter().nth(idx)` walk — see [`HashtableSet`]). Compact stays a linear
    /// scan, which is correct: it is a contiguous length-prefixed buffer with
    /// no index, and it is bounded to a few hundred small members by the
    /// promote thresholds.
    pub fn remove_at(&mut self, idx: usize) -> Option<Vec<u8>> {
        if self.is_empty() || idx >= self.len() {
            return None;
        }
        match self {
            SetData::Compact(c) => {
                let m = c.nth(idx)?;
                let _ = c.remove(&m);
                self.maybe_demote();
                Some(m)
            }
            SetData::Hashtable(s) => {
                let (m, _delta) = s.swap_remove_index(idx)?;
                self.maybe_demote();
                Some(m)
            }
        }
    }

    /// Member at `idx` (0..len) without removing it. `None` if out of range.
    ///
    /// Exists so `SRANDMEMBER` can pick one member without materialising the
    /// whole set. Before 1.4.2 it called `to_vec()`, cloning **every** member
    /// on every call — measured at 200 000 rps on a 1 000-member set but
    /// **10 526 rps** at 40 000 members, a 19× degradation that was worse than
    /// the `SPOP` bug it was found alongside.
    pub fn get_at(&self, idx: usize) -> Option<&[u8]> {
        match self {
            // Linear, but bounded by the Compact promote thresholds.
            SetData::Compact(c) => c.nth_ref(idx),
            SetData::Hashtable(s) => s.get_index(idx).map(|m| m.as_slice()),
        }
    }
}

/// Contiguous length-prefixed members.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactSet {
    buf: Arc<Vec<u8>>,
    len: usize,
    bytes: usize,
}

impl CompactSet {
    fn new() -> Self {
        Self {
            buf: Arc::new(Vec::new()),
            len: 0,
            bytes: 0,
        }
    }

    fn with_capacity(n: usize, payload: usize) -> Self {
        Self {
            buf: Arc::new(Vec::with_capacity(n * 4 + payload)),
            len: 0,
            bytes: 0,
        }
    }

    fn buf_mut(&mut self) -> &mut Vec<u8> {
        Arc::make_mut(&mut self.buf)
    }

    fn append_member(&mut self, member: &[u8]) {
        let buf = self.buf_mut();
        buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
        buf.extend_from_slice(member);
        self.len += 1;
        self.bytes += member.len();
    }

    fn insert(&mut self, member: Vec<u8>) -> (bool, isize) {
        if self.contains(&member) {
            return (false, 0);
        }
        let n = member.len() as isize;
        self.append_member(&member);
        (true, n)
    }

    fn contains(&self, member: &[u8]) -> bool {
        let mut off = 0usize;
        for _ in 0..self.len {
            let n = u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap()) as usize;
            if &self.buf[off + 4..off + 4 + n] == member {
                return true;
            }
            off += 4 + n;
        }
        false
    }

    fn remove(&mut self, member: &[u8]) -> (bool, isize) {
        let mut found = false;
        let mut new_buf = Vec::with_capacity(self.buf.len());
        let mut off = 0usize;
        let mut new_len = 0usize;
        let mut new_bytes = 0usize;
        for _ in 0..self.len {
            let n = u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap()) as usize;
            let m = &self.buf[off + 4..off + 4 + n];
            if !found && m == member {
                found = true;
            } else {
                new_buf.extend_from_slice(&self.buf[off..off + 4 + n]);
                new_len += 1;
                new_bytes += n;
            }
            off += 4 + n;
        }
        if found {
            let delta = new_bytes as isize - self.bytes as isize;
            self.buf = Arc::new(new_buf);
            self.len = new_len;
            self.bytes = new_bytes;
            (true, delta)
        } else {
            (false, 0)
        }
    }

    fn nth(&self, idx: usize) -> Option<Vec<u8>> {
        self.nth_ref(idx).map(|m| m.to_vec())
    }

    /// Borrowing variant of [`Self::nth`] — no allocation. Used by
    /// `SRANDMEMBER`, which only needs to read one member.
    fn nth_ref(&self, idx: usize) -> Option<&[u8]> {
        if idx >= self.len {
            return None;
        }
        let mut off = 0usize;
        for i in 0..self.len {
            let n = u32::from_le_bytes(self.buf[off..off + 4].try_into().ok()?) as usize;
            if i == idx {
                return Some(&self.buf[off + 4..off + 4 + n]);
            }
            off += 4 + n;
        }
        None
    }

    fn iter_members(&self) -> CompactSetIter<'_> {
        CompactSetIter {
            buf: &self.buf,
            off: 0,
            remaining: self.len,
        }
    }

    /// Borrowing variant of [`Self::iter_members`] — no allocation per element.
    fn iter_members_ref(&self) -> CompactSetRefIter<'_> {
        CompactSetRefIter {
            buf: &self.buf,
            off: 0,
            remaining: self.len,
        }
    }
}

pub struct CompactSetIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

/// Borrowing counterpart to [`CompactSetIter`], yielding slices into the
/// compact buffer. Used by `SSCAN`, which inspects every member to select one
/// page and therefore should not clone them all first.
pub struct CompactSetRefIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl<'a> Iterator for CompactSetRefIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let n = u32::from_le_bytes(self.buf[self.off..self.off + 4].try_into().ok()?) as usize;
        let m = &self.buf[self.off + 4..self.off + 4 + n];
        self.off += 4 + n;
        self.remaining -= 1;
        Some(m)
    }
}

impl Iterator for CompactSetIter<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let n = u32::from_le_bytes(self.buf[self.off..self.off + 4].try_into().ok()?) as usize;
        let m = self.buf[self.off + 4..self.off + 4 + n].to_vec();
        self.off += 4 + n;
        self.remaining -= 1;
        Some(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_insert_contains_remove() {
        let mut s = SetData::new();
        assert!(s.insert(b"a".to_vec()).0);
        assert!(!s.insert(b"a".to_vec()).0);
        assert!(s.contains(b"a"));
        assert!(matches!(s, SetData::Compact(_)));
        assert!(s.remove(b"a").0);
        assert!(s.is_empty());
    }

    #[test]
    fn promote_on_max_value() {
        let mut s = SetData::new();
        let big = vec![b'x'; SET_COMPACT_MAX_VALUE + 1];
        assert!(s.insert(big).0);
        assert!(matches!(s, SetData::Hashtable(_)));
    }

    #[test]
    fn promote_on_entries() {
        let prev = set_thresholds();
        set_set_thresholds(SetThresholds {
            max_entries: 3,
            ..prev
        });
        let mut s = SetData::new();
        s.insert(b"a".to_vec());
        s.insert(b"b".to_vec());
        s.insert(b"c".to_vec());
        assert!(matches!(s, SetData::Compact(_)));
        s.insert(b"d".to_vec());
        assert!(matches!(s, SetData::Hashtable(_)));
        set_set_thresholds(prev);
    }

    #[test]
    fn from_hashset_picks_compact_when_small() {
        let mut hs = HashSet::new();
        hs.insert(b"x".to_vec());
        hs.insert(b"y".to_vec());
        let s = SetData::from_hashset(hs);
        assert!(matches!(s, SetData::Compact(_)));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn hashtable_payload_bytes_tracks_insert_remove() {
        let mut s = SetData::new();
        let big = vec![b'x'; SET_COMPACT_MAX_VALUE + 1];
        let _ = s.insert(big.clone());
        assert!(matches!(s, SetData::Hashtable(_)));
        assert_eq!(s.payload_bytes(), big.len());
        let _ = s.insert(b"ab".to_vec());
        assert_eq!(s.payload_bytes(), big.len() + 2);
        let _ = s.remove(&big);
        assert_eq!(s.payload_bytes(), 2);
        let encoded = bincode::serde::encode_to_vec(&s, bincode::config::standard()).unwrap();
        let (decoded, _): (SetData, usize) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded.payload_bytes(), 2);
        assert!(decoded.contains(b"ab"));
    }
}
