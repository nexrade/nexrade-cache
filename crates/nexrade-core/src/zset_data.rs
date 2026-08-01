//! Dual-encoded sorted-set storage for short-zset locality.
//!
//! - [`ZSetData::Compact`]: contiguous score+member pairs (OBJECT ENCODING
//!   `listpack`), sorted by `(score, member)`.
//! - [`ZSetData::Skiplist`]: `HashMap` + `BTreeMap` for large zsets
//!   (OBJECT ENCODING `skiplist`).
//!
//! Compact format: `[f64_le score][u32_le mlen][member]…`
//! Thresholds are process-wide atomics so `CONFIG SET` can retune promote.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

/// Promote Compact → Skiplist when any of these is exceeded (defaults).
/// Aligned with Redis `zset-max-listpack-entries` / `zset-max-listpack-value`.
pub const ZSET_COMPACT_MAX_ENTRIES: usize = 128;
pub const ZSET_COMPACT_MAX_SIZE: usize = 8192;
pub const ZSET_COMPACT_MAX_VALUE: usize = 64;
/// After ZREM/ZPOP, Skiplist → Compact when at or under this entry count.
pub const ZSET_DEMOTE_ENTRIES: usize = 64;

static MAX_ENTRIES: AtomicUsize = AtomicUsize::new(ZSET_COMPACT_MAX_ENTRIES);
static MAX_SIZE: AtomicUsize = AtomicUsize::new(ZSET_COMPACT_MAX_SIZE);
static MAX_VALUE: AtomicUsize = AtomicUsize::new(ZSET_COMPACT_MAX_VALUE);
static DEMOTE_ENTRIES: AtomicUsize = AtomicUsize::new(ZSET_DEMOTE_ENTRIES);

#[derive(Debug, Clone, Copy)]
pub struct ZSetThresholds {
    pub max_entries: usize,
    pub max_size: usize,
    pub max_value: usize,
    pub demote_entries: usize,
}

impl Default for ZSetThresholds {
    fn default() -> Self {
        Self {
            max_entries: ZSET_COMPACT_MAX_ENTRIES,
            max_size: ZSET_COMPACT_MAX_SIZE,
            max_value: ZSET_COMPACT_MAX_VALUE,
            demote_entries: ZSET_DEMOTE_ENTRIES,
        }
    }
}

#[inline]
pub fn zset_thresholds() -> ZSetThresholds {
    ZSetThresholds {
        max_entries: MAX_ENTRIES.load(Ordering::Relaxed),
        max_size: MAX_SIZE.load(Ordering::Relaxed),
        max_value: MAX_VALUE.load(Ordering::Relaxed),
        demote_entries: DEMOTE_ENTRIES.load(Ordering::Relaxed),
    }
}

pub fn set_zset_thresholds(t: ZSetThresholds) {
    MAX_ENTRIES.store(t.max_entries, Ordering::Relaxed);
    MAX_SIZE.store(t.max_size, Ordering::Relaxed);
    MAX_VALUE.store(t.max_value, Ordering::Relaxed);
    DEMOTE_ENTRIES.store(t.demote_entries, Ordering::Relaxed);
}

/// Dual-encoded sorted set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ZSetData {
    Compact(CompactZSet),
    Skiplist(SkiplistZSet),
}

impl Default for ZSetData {
    fn default() -> Self {
        Self::new()
    }
}

impl ZSetData {
    pub fn new() -> Self {
        ZSetData::Compact(CompactZSet::new())
    }

    /// Prefer Compact when the pair set is small enough; else Skiplist.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (Vec<u8>, f64)>) -> Self {
        let thr = zset_thresholds();
        let mut items: Vec<(Vec<u8>, f64)> = pairs.into_iter().collect();
        // Dedupe by last-write-wins on member (matches successive inserts).
        let mut map: HashMap<Vec<u8>, f64> = HashMap::with_capacity(items.len());
        for (m, s) in items.drain(..) {
            map.insert(m, s);
        }
        if map.len() <= thr.max_entries {
            let mut total = 0usize;
            let mut ok = true;
            for m in map.keys() {
                if m.len() > thr.max_value {
                    ok = false;
                    break;
                }
                total = total.saturating_add(m.len() + 8);
                if total > thr.max_size {
                    ok = false;
                    break;
                }
            }
            if ok {
                let mut c = CompactZSet::with_capacity(map.len(), total);
                let mut ordered: Vec<(OrderedFloat<f64>, Vec<u8>)> =
                    map.into_iter().map(|(m, s)| (OrderedFloat(s), m)).collect();
                ordered.sort_unstable();
                for (s, m) in ordered {
                    c.append_pair(s.0, &m);
                }
                return ZSetData::Compact(c);
            }
        }
        let mut sk = SkiplistZSet::new();
        for (m, s) in map {
            sk.insert(m, s);
        }
        ZSetData::Skiplist(sk)
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            ZSetData::Compact(c) => c.len,
            ZSetData::Skiplist(s) => s.members.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encoding_name(&self) -> &'static str {
        match self {
            ZSetData::Compact(_) => "listpack",
            ZSetData::Skiplist(_) => "skiplist",
        }
    }

    #[inline]
    pub fn payload_bytes(&self) -> usize {
        match self {
            ZSetData::Compact(c) => c.bytes,
            // Skiplist cache counts member lens only (matches Compact); +8 was
            // promote-threshold padding, not live_bytes payload.
            ZSetData::Skiplist(s) => s.payload_bytes(),
        }
    }

    pub fn ensure_skiplist(&mut self) {
        if let ZSetData::Compact(c) = self {
            let mut sk = SkiplistZSet::with_capacity(c.len);
            let payload = c.bytes;
            for (m, s) in c.iter_pairs() {
                sk.insert(m, s);
            }
            sk.bytes = payload;
            *self = ZSetData::Skiplist(sk);
        }
    }

    pub fn maybe_demote(&mut self) {
        let thr = zset_thresholds();
        {
            let ZSetData::Skiplist(s) = self else {
                return;
            };
            if s.members.len() > thr.demote_entries {
                return;
            }
            for m in s.members.keys() {
                if m.len() > thr.max_value {
                    return;
                }
            }
            // Compact promote uses member_len + 8; keep same threshold math.
            let total = s
                .members
                .keys()
                .map(|m| m.len() + 8)
                .fold(0usize, usize::saturating_add);
            if total > thr.max_size {
                return;
            }
        }
        let ZSetData::Skiplist(s) = std::mem::replace(self, ZSetData::Compact(CompactZSet::new()))
        else {
            unreachable!()
        };
        let total = s.payload_bytes();
        let mut c = CompactZSet::with_capacity(s.members.len(), total);
        for (score, member) in s.by_score.keys() {
            c.append_pair(score.0, member);
        }
        *self = ZSetData::Compact(c);
    }

    /// Insert / update member score. Returns true if newly added.
    pub fn insert(&mut self, member: Vec<u8>, score: f64) -> bool {
        // Skiplist: HashMap path — no pre-scan / promote.
        if matches!(self, ZSetData::Skiplist(_)) {
            return match self {
                ZSetData::Skiplist(s) => s.insert(member, score),
                _ => unreachable!(),
            };
        }
        // Compact: promote only when a *new* member would overflow, or
        // member is oversize. Avoids a full score() pre-scan on the hot
        // redis-benchmark ZADD path (always-new members under threshold).
        let thr = zset_thresholds();
        if member.len() > thr.max_value {
            self.ensure_skiplist();
            return match self {
                ZSetData::Skiplist(s) => s.insert(member, score),
                _ => unreachable!(),
            };
        }
        let maybe_need_promote = match self {
            ZSetData::Compact(c) => {
                c.len >= thr.max_entries || c.bytes + member.len() + 8 > thr.max_size
            }
            ZSetData::Skiplist(_) => false,
        };
        if maybe_need_promote && self.score(&member).is_none() {
            self.ensure_skiplist();
            return match self {
                ZSetData::Skiplist(s) => s.insert(member, score),
                _ => unreachable!(),
            };
        }
        match self {
            ZSetData::Compact(c) => c.insert(member, score),
            ZSetData::Skiplist(s) => s.insert(member, score),
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let removed = match self {
            ZSetData::Compact(c) => c.remove(member),
            ZSetData::Skiplist(s) => s.remove(member),
        };
        if removed.is_some() {
            self.maybe_demote();
        }
        removed
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            ZSetData::Compact(c) => c.score(member),
            ZSetData::Skiplist(s) => s.score(member),
        }
    }

    /// 0-based rank of `member` in score-ascending order. None if missing.
    pub fn rank(&self, member: &[u8], rev: bool) -> Option<usize> {
        let score = self.score(member)?;
        let n = self.len();
        let pos = match self {
            ZSetData::Compact(c) => c
                .iter_pairs()
                .position(|(m, s)| s == score && m == member)?,
            ZSetData::Skiplist(s) => s
                .by_score
                .keys()
                .position(|(sc, m)| sc.0 == score && m.as_slice() == member)?,
        };
        Some(if rev { n - pos - 1 } else { pos })
    }

    /// Lowest (score, member) in score order.
    pub fn first(&self) -> Option<(f64, Vec<u8>)> {
        match self {
            ZSetData::Compact(c) => c.iter_pairs().next().map(|(m, s)| (s, m)),
            ZSetData::Skiplist(s) => s.by_score.keys().next().map(|(sc, m)| (sc.0, m.clone())),
        }
    }

    /// Highest (score, member) in score order.
    pub fn last(&self) -> Option<(f64, Vec<u8>)> {
        match self {
            ZSetData::Compact(c) => {
                let mut last = None;
                for (m, s) in c.iter_pairs() {
                    last = Some((s, m));
                }
                last
            }
            ZSetData::Skiplist(s) => s
                .by_score
                .keys()
                .next_back()
                .map(|(sc, m)| (sc.0, m.clone())),
        }
    }

    /// Pop the extreme member (min if `max == false`).
    pub fn pop_extreme(&mut self, max: bool) -> Option<(f64, Vec<u8>)> {
        let (score, member) = if max { self.last()? } else { self.first()? };
        self.remove(&member)?;
        Some((score, member))
    }

    /// All (member, score) pairs in arbitrary order (owned).
    pub fn to_pairs(&self) -> Vec<(Vec<u8>, f64)> {
        match self {
            ZSetData::Compact(c) => c.iter_pairs().collect(),
            ZSetData::Skiplist(s) => s.members.iter().map(|(m, sc)| (m.clone(), sc.0)).collect(),
        }
    }

    /// All (member, score) pairs in score-ascending order.
    pub fn to_pairs_sorted(&self) -> Vec<(Vec<u8>, f64)> {
        match self {
            ZSetData::Compact(c) => c.iter_pairs().collect(),
            ZSetData::Skiplist(s) => s.by_score.keys().map(|(sc, m)| (m.clone(), sc.0)).collect(),
        }
    }

    /// Member keys only (arbitrary order).
    pub fn member_keys(&self) -> Vec<Vec<u8>> {
        match self {
            ZSetData::Compact(c) => c.iter_pairs().map(|(m, _)| m).collect(),
            ZSetData::Skiplist(s) => s.members.keys().cloned().collect(),
        }
    }

    pub fn range_by_rank(&self, start: isize, stop: isize, rev: bool) -> Vec<(Vec<u8>, f64)> {
        let len = self.len() as isize;
        let start = normalize_index(start, len).min(len as usize);
        let stop = normalize_index(stop, len);
        let mut entries = self.to_pairs_sorted();
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
        let entries: Vec<_> = self
            .to_pairs_sorted()
            .into_iter()
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

    /// Members in score order whose member bytes fall in the lex range.
    pub fn range_by_lex_members(
        &self,
        min_b: Option<&[u8]>,
        min_excl: bool,
        max_b: Option<&[u8]>,
        max_excl: bool,
    ) -> Vec<Vec<u8>> {
        // Lex sort is by member bytes alone (Redis BYLEX semantics).
        let mut members: Vec<Vec<u8>> = self.member_keys();
        members.sort_unstable();
        members
            .into_iter()
            .filter(|m| {
                let after_min = match min_b {
                    None => true,
                    Some(b) => {
                        if min_excl {
                            m.as_slice() > b
                        } else {
                            m.as_slice() >= b
                        }
                    }
                };
                let before_max = match max_b {
                    None => true,
                    Some(b) => {
                        if max_excl {
                            m.as_slice() < b
                        } else {
                            m.as_slice() <= b
                        }
                    }
                };
                after_min && before_max
            })
            .collect()
    }
}

fn normalize_index(idx: isize, len: isize) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx as usize
    }
}

// ── Skiplist (HashMap + BTreeMap) ─────────────────────────────────────────────

/// Large-zset encoding: member→score map + sorted (score, member) index.
///
/// Caches sum of member byte lengths for O(1) `payload_bytes()`. Wire format
/// is `{members, by_score}` only — `bytes` is recomputed on load. ZSetData
/// uses the default (externally tagged) enum representation for bincode
/// compatibility (internally-tagged `#[serde(tag = "e")]` cannot round-trip
/// non-trivial content under bincode).
#[derive(Debug, Clone, Serialize, Default)]
pub struct SkiplistZSet {
    pub members: HashMap<Vec<u8>, OrderedFloat<f64>>,
    pub by_score: BTreeMap<(OrderedFloat<f64>, Vec<u8>), ()>,
    #[serde(skip)]
    bytes: usize,
}

impl<'de> Deserialize<'de> for SkiplistZSet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            members: HashMap<Vec<u8>, OrderedFloat<f64>>,
            by_score: BTreeMap<(OrderedFloat<f64>, Vec<u8>), ()>,
        }
        let w = Wire::deserialize(deserializer)?;
        let bytes = w.members.keys().map(|m| m.len()).sum();
        Ok(Self {
            members: w.members,
            by_score: w.by_score,
            bytes,
        })
    }
}

impl SkiplistZSet {
    fn new() -> Self {
        Self::default()
    }

    fn with_capacity(n: usize) -> Self {
        Self {
            members: HashMap::with_capacity(n),
            by_score: BTreeMap::new(),
            bytes: 0,
        }
    }

    #[inline]
    fn payload_bytes(&self) -> usize {
        self.bytes
    }

    fn insert(&mut self, member: Vec<u8>, score: f64) -> bool {
        let score = OrderedFloat(score);
        let mlen = member.len();
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
                self.bytes += mlen;
                true
            }
        }
    }

    fn remove(&mut self, member: &[u8]) -> Option<f64> {
        if let Some(score) = self.members.remove(member) {
            self.by_score.remove(&(score, member.to_vec()));
            self.bytes = self.bytes.saturating_sub(member.len());
            Some(score.0)
        } else {
            None
        }
    }

    fn score(&self, member: &[u8]) -> Option<f64> {
        self.members.get(member).map(|s| s.0)
    }
}

// ── Compact (listpack-like) ───────────────────────────────────────────────────

/// Contiguous score+member pairs, kept sorted by (score, member).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactZSet {
    buf: Arc<Vec<u8>>,
    len: usize,
    /// Sum of member byte lengths (excludes score/len prefixes) for promote.
    bytes: usize,
}

impl CompactZSet {
    fn new() -> Self {
        Self {
            buf: Arc::new(Vec::new()),
            len: 0,
            bytes: 0,
        }
    }

    fn with_capacity(n: usize, payload: usize) -> Self {
        // 8 score + 4 len + payload per entry
        Self {
            buf: Arc::new(Vec::with_capacity(n * 12 + payload)),
            len: 0,
            bytes: 0,
        }
    }

    fn buf_mut(&mut self) -> &mut Vec<u8> {
        Arc::make_mut(&mut self.buf)
    }

    fn append_pair(&mut self, score: f64, member: &[u8]) {
        let b = self.buf_mut();
        b.extend_from_slice(&score.to_le_bytes());
        b.extend_from_slice(&(member.len() as u32).to_le_bytes());
        b.extend_from_slice(member);
        self.len += 1;
        self.bytes += member.len();
    }

    fn iter_pairs(&self) -> CompactZSetIter<'_> {
        CompactZSetIter {
            buf: &self.buf,
            pos: 0,
        }
    }

    fn score(&self, member: &[u8]) -> Option<f64> {
        self.iter_pairs()
            .find(|(m, _)| m.as_slice() == member)
            .map(|(_, s)| s)
    }

    /// Compare `(score, member)` for sorted order — matches Redis listpack zset.
    fn cmp_score_member(
        score_a: f64,
        mem_a: &[u8],
        score_b: f64,
        mem_b: &[u8],
    ) -> std::cmp::Ordering {
        OrderedFloat(score_a)
            .cmp(&OrderedFloat(score_b))
            .then_with(|| mem_a.cmp(mem_b))
    }

    fn insert(&mut self, member: Vec<u8>, score: f64) -> bool {
        // Single scan: find existing member (for update / same-score no-op)
        // and the sorted splice position for the new (score, member).
        // Skip the existing member when computing insert_off so the target is
        // already in post-removal coordinates.
        let mut off = 0usize;
        let mut found_off: Option<(usize, f64, usize)> = None; // (off, old_score, pair_len)
        let mut insert_off = self.buf.len(); // default: append (pre-removal coords)
        let mut insert_off_set = false;
        let mut found_plen = 0usize;

        for _ in 0..self.len {
            let mut score_bytes = [0u8; 8];
            score_bytes.copy_from_slice(&self.buf[off..off + 8]);
            let s = f64::from_le_bytes(score_bytes);
            let mlen = u32::from_le_bytes(self.buf[off + 8..off + 12].try_into().unwrap()) as usize;
            let m = &self.buf[off + 12..off + 12 + mlen];
            let plen = 12 + mlen;

            if m == member.as_slice() {
                found_off = Some((off, s, plen));
                found_plen = plen;
                off += plen;
                continue; // exclude self from insert-position scan
            }
            if !insert_off_set
                && Self::cmp_score_member(score, &member, s, m) == std::cmp::Ordering::Less
            {
                // insert_off in post-removal coords if a prior pair was removed.
                insert_off = if found_off.is_some() {
                    off - found_plen
                } else {
                    off
                };
                insert_off_set = true;
            }
            off += plen;
        }
        // If we never set insert_off and a member was found, the append
        // position is end-after-removal.
        if !insert_off_set {
            insert_off = self.buf.len() - found_plen;
        }

        if let Some((old_off, old_score, old_plen)) = found_off {
            // Same score → pure no-op (common redis-benchmark re-ZADD shape).
            if old_score == score || (old_score.is_nan() && score.is_nan()) {
                return false;
            }
            // Score change: drop old pair, splice at new sorted position.
            // `insert_off` is already in post-removal coordinates.
            let target = insert_off;
            let new_pair_len = 12 + member.len();
            let mut without = Vec::with_capacity(self.buf.len() - old_plen);
            without.extend_from_slice(&self.buf[..old_off]);
            without.extend_from_slice(&self.buf[old_off + old_plen..]);
            let mut new_buf = Vec::with_capacity(without.len() + new_pair_len);
            new_buf.extend_from_slice(&without[..target]);
            new_buf.extend_from_slice(&score.to_le_bytes());
            new_buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
            new_buf.extend_from_slice(&member);
            new_buf.extend_from_slice(&without[target..]);
            self.buf = Arc::new(new_buf);
            // len / member-payload bytes unchanged on score-only update.
            return false;
        }

        // New member: splice at sorted position (or append).
        // insert_off is still in full-buffer coordinates (nothing removed).
        let pair_len = 12 + member.len();
        // Recompute insert_off without the found-skip adjustment (no found).
        // We used pre-removal coords when found_off is None — but insert_off
        // was set from `off` directly, and the !insert_off_set branch set
        // insert_off = buf.len() - 0. Good.
        if insert_off == self.buf.len() {
            self.append_pair(score, &member);
            return true;
        }
        let mut new_buf = Vec::with_capacity(self.buf.len() + pair_len);
        new_buf.extend_from_slice(&self.buf[..insert_off]);
        new_buf.extend_from_slice(&score.to_le_bytes());
        new_buf.extend_from_slice(&(member.len() as u32).to_le_bytes());
        new_buf.extend_from_slice(&member);
        new_buf.extend_from_slice(&self.buf[insert_off..]);
        self.buf = Arc::new(new_buf);
        self.len += 1;
        self.bytes += member.len();
        true
    }

    fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let mut off = 0usize;
        for _ in 0..self.len {
            let mut score_bytes = [0u8; 8];
            score_bytes.copy_from_slice(&self.buf[off..off + 8]);
            let s = f64::from_le_bytes(score_bytes);
            let mlen = u32::from_le_bytes(self.buf[off + 8..off + 12].try_into().unwrap()) as usize;
            let m = &self.buf[off + 12..off + 12 + mlen];
            let plen = 12 + mlen;
            if m == member {
                let mut new_buf = Vec::with_capacity(self.buf.len() - plen);
                new_buf.extend_from_slice(&self.buf[..off]);
                new_buf.extend_from_slice(&self.buf[off + plen..]);
                self.buf = Arc::new(new_buf);
                self.len -= 1;
                self.bytes -= mlen;
                return Some(s);
            }
            off += plen;
        }
        None
    }
}

struct CompactZSetIter<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Iterator for CompactZSetIter<'_> {
    type Item = (Vec<u8>, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 12 > self.buf.len() {
            // need at least 8+4
            if self.pos >= self.buf.len() {
                return None;
            }
        }
        if self.pos + 8 > self.buf.len() {
            return None;
        }
        let mut score_bytes = [0u8; 8];
        score_bytes.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        let score = f64::from_le_bytes(score_bytes);
        self.pos += 8;
        if self.pos + 4 > self.buf.len() {
            return None;
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        let mlen = u32::from_le_bytes(len_bytes) as usize;
        self.pos += 4;
        if self.pos + mlen > self.buf.len() {
            return None;
        }
        let member = self.buf[self.pos..self.pos + mlen].to_vec();
        self.pos += mlen;
        Some((member, score))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_insert_score_remove() {
        let mut z = ZSetData::new();
        assert!(z.insert(b"a".to_vec(), 1.0));
        assert!(!z.insert(b"a".to_vec(), 2.0));
        assert_eq!(z.score(b"a"), Some(2.0));
        assert!(matches!(z, ZSetData::Compact(_)));
        assert_eq!(z.remove(b"a"), Some(2.0));
        assert!(z.is_empty());
    }

    #[test]
    fn promote_on_max_value() {
        let mut z = ZSetData::new();
        let big = vec![b'x'; ZSET_COMPACT_MAX_VALUE + 1];
        assert!(z.insert(big, 1.0));
        assert!(matches!(z, ZSetData::Skiplist(_)));
        assert_eq!(z.encoding_name(), "skiplist");
    }

    #[test]
    fn promote_on_entries() {
        let prev = zset_thresholds();
        set_zset_thresholds(ZSetThresholds {
            max_entries: 3,
            ..prev
        });
        let mut z = ZSetData::new();
        z.insert(b"a".to_vec(), 1.0);
        z.insert(b"b".to_vec(), 2.0);
        z.insert(b"c".to_vec(), 3.0);
        assert!(matches!(z, ZSetData::Compact(_)));
        z.insert(b"d".to_vec(), 4.0);
        assert!(matches!(z, ZSetData::Skiplist(_)));
        set_zset_thresholds(prev);
    }

    #[test]
    fn range_by_rank_sorted() {
        let mut z = ZSetData::new();
        z.insert(b"c".to_vec(), 3.0);
        z.insert(b"a".to_vec(), 1.0);
        z.insert(b"b".to_vec(), 2.0);
        let r = z.range_by_rank(0, -1, false);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0, b"a");
        assert_eq!(r[2].0, b"c");
        let rev = z.range_by_rank(0, 0, true);
        assert_eq!(rev[0].0, b"c");
    }

    #[test]
    fn from_pairs_picks_compact() {
        let z = ZSetData::from_pairs(vec![(b"x".to_vec(), 1.0), (b"y".to_vec(), 2.0)]);
        assert!(matches!(z, ZSetData::Compact(_)));
        assert_eq!(z.len(), 2);
    }

    #[test]
    fn rank_and_extremes() {
        let mut z = ZSetData::new();
        z.insert(b"a".to_vec(), 1.0);
        z.insert(b"b".to_vec(), 2.0);
        z.insert(b"c".to_vec(), 3.0);
        assert_eq!(z.rank(b"a", false), Some(0));
        assert_eq!(z.rank(b"c", false), Some(2));
        assert_eq!(z.rank(b"a", true), Some(2));
        assert_eq!(z.first().unwrap().1, b"a");
        assert_eq!(z.last().unwrap().1, b"c");
        let (s, m) = z.pop_extreme(false).unwrap();
        assert_eq!((s, m.as_slice()), (1.0, &b"a"[..]));
        assert_eq!(z.len(), 2);
    }

    #[test]
    fn skiplist_payload_bytes_tracks_and_roundtrips() {
        let mut z = ZSetData::new();
        let big = vec![b'm'; ZSET_COMPACT_MAX_VALUE + 1];
        z.insert(big.clone(), 1.0);
        assert!(matches!(z, ZSetData::Skiplist(_)));
        assert_eq!(z.payload_bytes(), big.len());
        z.insert(b"ab".to_vec(), 2.0);
        assert_eq!(z.payload_bytes(), big.len() + 2);
        z.remove(&big);
        assert_eq!(z.payload_bytes(), 2);
        let encoded = bincode::serde::encode_to_vec(&z, bincode::config::standard()).unwrap();
        let (decoded, _): (ZSetData, usize) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded.payload_bytes(), 2);
        assert_eq!(decoded.score(b"ab"), Some(2.0));
    }
}
