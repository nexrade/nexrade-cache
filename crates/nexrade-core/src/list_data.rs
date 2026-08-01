//! Dual-encoded list storage for short-list LRANGE performance.
//!
//! - [`ListData::Compact`]: contiguous length-prefixed blob (cache-friendly).
//! - [`ListData::Linked`]: `VecDeque<Bytes>` plus a cached payload-byte total
//!   ([`LinkedList`]) for large / mid-list-mutation heavy lists.
//!
//! Not Redis listpack on the wire — simple `[u32_le len][bytes]…` packing is
//! enough for sequential LRANGE framing without pointer-chasing.
//!
//! Thresholds are process-wide atomics (defaults below) so `CONFIG SET` can
//! retune promote/demote without taking the config lock on every push.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Promote Compact → Linked when any of these is exceeded on push (defaults).
pub const LIST_COMPACT_MAX_ENTRIES: usize = 512;
pub const LIST_COMPACT_MAX_SIZE: usize = 8192;
pub const LIST_COMPACT_MAX_ELEM: usize = 256;
/// After shrink, Linked → Compact when at or under this entry count (and
/// size/elem still fit Compact limits).
pub const LIST_DEMOTE_ENTRIES: usize = 128;

static MAX_ENTRIES: AtomicUsize = AtomicUsize::new(LIST_COMPACT_MAX_ENTRIES);
static MAX_SIZE: AtomicUsize = AtomicUsize::new(LIST_COMPACT_MAX_SIZE);
static MAX_ELEM: AtomicUsize = AtomicUsize::new(LIST_COMPACT_MAX_ELEM);
static DEMOTE_ENTRIES: AtomicUsize = AtomicUsize::new(LIST_DEMOTE_ENTRIES);

/// Live Compact promote/demote thresholds (lock-free reads on the hot path).
#[derive(Debug, Clone, Copy)]
pub struct ListThresholds {
    pub max_entries: usize,
    pub max_size: usize,
    pub max_elem: usize,
    pub demote_entries: usize,
}

impl Default for ListThresholds {
    fn default() -> Self {
        Self {
            max_entries: LIST_COMPACT_MAX_ENTRIES,
            max_size: LIST_COMPACT_MAX_SIZE,
            max_elem: LIST_COMPACT_MAX_ELEM,
            demote_entries: LIST_DEMOTE_ENTRIES,
        }
    }
}

/// Snapshot of current process-wide thresholds.
#[inline]
pub fn list_thresholds() -> ListThresholds {
    ListThresholds {
        max_entries: MAX_ENTRIES.load(Ordering::Relaxed),
        max_size: MAX_SIZE.load(Ordering::Relaxed),
        max_elem: MAX_ELEM.load(Ordering::Relaxed),
        demote_entries: DEMOTE_ENTRIES.load(Ordering::Relaxed),
    }
}

/// Apply new thresholds (used by CONFIG SET). Zero is rejected by the caller.
pub fn set_list_thresholds(t: ListThresholds) {
    MAX_ENTRIES.store(t.max_entries, Ordering::Relaxed);
    MAX_SIZE.store(t.max_size, Ordering::Relaxed);
    MAX_ELEM.store(t.max_elem, Ordering::Relaxed);
    DEMOTE_ENTRIES.store(t.demote_entries, Ordering::Relaxed);
}

/// Large-list encoding: `VecDeque` plus a cached payload-byte total so
/// `payload_bytes()` stays O(1) after Compact → Linked promote. Without the
/// cache, every LPUSH/RPUSH under maxmemory would re-scan the whole deque
/// (catastrophic under redis-benchmark after the list exceeds Compact caps).
///
/// **Wire format**: `items` only (`bytes` is `#[serde(skip)]` and recomputed
/// on load via [`LinkedList::from_deque`]-style rebuild in a custom
/// deserializer). ListData uses the default (externally tagged) enum
/// representation — bincode cannot round-trip `#[serde(tag = "e")]` enums
/// with non-trivial content (`AnyNotSupported`).
#[derive(Debug, Clone, Serialize)]
pub struct LinkedList {
    pub items: VecDeque<Bytes>,
    /// Sum of element byte lengths (mirrors CompactList::bytes).
    #[serde(skip)]
    bytes: usize,
}

impl<'de> Deserialize<'de> for LinkedList {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            items: VecDeque<Bytes>,
        }
        let w = Wire::deserialize(deserializer)?;
        Ok(Self::from_deque(w.items))
    }
}

impl LinkedList {
    fn with_capacity(n: usize, payload: usize) -> Self {
        Self {
            items: VecDeque::with_capacity(n),
            bytes: payload,
        }
    }

    fn from_deque(items: VecDeque<Bytes>) -> Self {
        let bytes = items.iter().map(|b| b.len()).sum();
        Self { items, bytes }
    }

    #[inline]
    fn len(&self) -> usize {
        self.items.len()
    }

    #[inline]
    fn push_front(&mut self, v: Bytes) {
        self.bytes += v.len();
        self.items.push_front(v);
    }

    #[inline]
    fn push_back(&mut self, v: Bytes) {
        self.bytes += v.len();
        self.items.push_back(v);
    }

    #[inline]
    fn pop_front(&mut self) -> Option<Bytes> {
        let v = self.items.pop_front()?;
        self.bytes = self.bytes.saturating_sub(v.len());
        Some(v)
    }

    #[inline]
    fn pop_back(&mut self) -> Option<Bytes> {
        let v = self.items.pop_back()?;
        self.bytes = self.bytes.saturating_sub(v.len());
        Some(v)
    }
}

/// Dual-encoded list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ListData {
    /// Contiguous payload: concatenated `[u32_le len][bytes]…`.
    Compact(CompactList),
    /// Current storage — O(1) ends, good for large lists.
    Linked(LinkedList),
}

impl Default for ListData {
    fn default() -> Self {
        Self::new()
    }
}

impl ListData {
    /// New empty list starts Compact (LRANGE hot path).
    pub fn new() -> Self {
        ListData::Compact(CompactList::new())
    }

    pub fn from_linked(d: VecDeque<Bytes>) -> Self {
        ListData::Linked(LinkedList::from_deque(d))
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            ListData::Compact(c) => c.len,
            ListData::Linked(d) => d.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encoding_name(&self) -> &'static str {
        match self {
            ListData::Compact(_) => "listpack",
            ListData::Linked(_) => "linkedlist",
        }
    }

    /// Total element payload bytes (for maxmemory accounting). O(1) for both
    /// encodings — Linked caches the sum so post-promote LPUSH stays fast.
    #[inline]
    pub fn payload_bytes(&self) -> usize {
        match self {
            ListData::Compact(c) => c.bytes,
            ListData::Linked(d) => d.bytes,
        }
    }

    pub fn estimated_size(&self) -> usize {
        match self {
            ListData::Compact(c) => {
                c.buf.capacity()
                    + c.offsets.capacity() * std::mem::size_of::<u32>()
                    + std::mem::size_of::<CompactList>()
            }
            ListData::Linked(d) => {
                d.bytes + d.len() * std::mem::size_of::<Bytes>() + std::mem::size_of::<LinkedList>()
            }
        }
    }

    /// Convert to Linked in place (no-op if already Linked).
    pub fn ensure_linked(&mut self) {
        if let ListData::Compact(c) = self {
            // Leave headroom so post-promote RPUSH/LPUSH don't reallocate
            // the VecDeque on the next few hundred pushes (redis-benchmark
            // keeps writing the same key long after Compact promotes).
            let payload = c.bytes;
            let mut d = LinkedList::with_capacity(c.len + c.len / 2 + 16, 0);
            for e in c.iter_elements() {
                d.push_back(e);
            }
            // Prefer the precomputed Compact sum if the walk drifted.
            d.bytes = payload;
            *self = ListData::Linked(d);
        }
    }

    /// Convert Linked → Compact when under demote thresholds.
    /// No-op when already Compact or still too large.
    pub fn maybe_demote(&mut self) {
        let thr = list_thresholds();
        let ListData::Linked(d) = self else {
            return;
        };
        if d.len() > thr.demote_entries {
            return;
        }
        let mut total = 0usize;
        for e in d.items.iter() {
            if e.len() > thr.max_elem {
                return;
            }
            total = total.saturating_add(e.len());
            if total > thr.max_size {
                return;
            }
        }
        let mut c = CompactList::with_capacity(d.len(), total);
        for e in d.items.drain(..) {
            c.push_back(e);
        }
        *self = ListData::Compact(c);
    }

    fn should_promote_after_push(&self, elem_len: usize) -> bool {
        // Linked is the large encoding — never promote further. Bail before
        // loading the process-wide atomics on the steady-state write path.
        let ListData::Compact(c) = self else {
            return false;
        };
        let thr = list_thresholds();
        elem_len > thr.max_elem || c.len + 1 > thr.max_entries || c.bytes + elem_len > thr.max_size
    }

    pub fn push_front(&mut self, v: Bytes) {
        // Steady-state after promote: skip threshold atomics entirely.
        if let ListData::Linked(d) = self {
            d.push_front(v);
            return;
        }
        if self.should_promote_after_push(v.len()) {
            self.ensure_linked();
            if let ListData::Linked(d) = self {
                d.push_front(v);
            }
            return;
        }
        if let ListData::Compact(c) = self {
            c.push_front(v);
        }
    }

    pub fn push_back(&mut self, v: Bytes) {
        if let ListData::Linked(d) = self {
            d.push_back(v);
            return;
        }
        if self.should_promote_after_push(v.len()) {
            self.ensure_linked();
            if let ListData::Linked(d) = self {
                d.push_back(v);
            }
            return;
        }
        if let ListData::Compact(c) = self {
            c.push_back(v);
        }
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        let v = match self {
            ListData::Compact(c) => c.pop_front(),
            ListData::Linked(d) => d.pop_front(),
        };
        if v.is_some() {
            self.maybe_demote();
        }
        v
    }

    pub fn pop_back(&mut self) -> Option<Bytes> {
        let v = match self {
            ListData::Compact(c) => c.pop_back(),
            ListData::Linked(d) => d.pop_back(),
        };
        if v.is_some() {
            self.maybe_demote();
        }
        v
    }

    pub fn get(&self, i: usize) -> Option<Bytes> {
        match self {
            ListData::Compact(c) => c.get(i),
            ListData::Linked(d) => d.items.get(i).cloned(),
        }
    }

    /// Random-access set. Converts Compact → Linked first (v1 mid-list policy).
    /// Returns `(ok, payload_delta)`.
    pub fn set(&mut self, i: usize, v: Bytes) -> (bool, isize) {
        self.ensure_linked();
        match self {
            ListData::Linked(d) => {
                if i < d.len() {
                    let old_len = d.items[i].len();
                    let delta = v.len() as isize - old_len as isize;
                    d.bytes = d.bytes.saturating_sub(old_len).saturating_add(v.len());
                    d.items[i] = v;
                    (true, delta)
                } else {
                    (false, 0)
                }
            }
            ListData::Compact(_) => unreachable!(),
        }
    }

    /// Insert relative to a pivot. Converts to Linked.
    pub fn insert_before(&mut self, pivot: &[u8], v: Bytes) -> Option<usize> {
        self.ensure_linked();
        match self {
            ListData::Linked(d) => {
                if let Some(pos) = d.items.iter().position(|x| x.as_ref() == pivot) {
                    d.bytes += v.len();
                    d.items.insert(pos, v);
                    Some(d.len())
                } else {
                    None
                }
            }
            ListData::Compact(_) => unreachable!(),
        }
    }

    pub fn insert_after(&mut self, pivot: &[u8], v: Bytes) -> Option<usize> {
        self.ensure_linked();
        match self {
            ListData::Linked(d) => {
                if let Some(pos) = d.items.iter().position(|x| x.as_ref() == pivot) {
                    d.bytes += v.len();
                    d.items.insert(pos + 1, v);
                    Some(d.len())
                } else {
                    None
                }
            }
            ListData::Compact(_) => unreachable!(),
        }
    }

    /// Remove up to `count` occurrences of `elem` (Redis LREM semantics).
    /// `count > 0` head→tail, `< 0` tail→head, `0` all. Converts to Linked.
    /// Returns `(removed_count, payload_delta)` where `payload_delta` is
    /// `−removed_count * elem.len()` (content-only).
    pub fn rem(&mut self, count: i64, elem: &[u8]) -> (usize, isize) {
        self.ensure_linked();
        let removed = match self {
            ListData::Linked(d) => {
                let mut removed = 0usize;
                if count == 0 {
                    let mut kept = VecDeque::with_capacity(d.len());
                    let mut new_bytes = 0usize;
                    for x in d.items.drain(..) {
                        if x.as_ref() == elem {
                            removed += 1;
                        } else {
                            new_bytes += x.len();
                            kept.push_back(x);
                        }
                    }
                    d.items = kept;
                    d.bytes = new_bytes;
                    removed
                } else if count > 0 {
                    let mut keep = VecDeque::with_capacity(d.len());
                    let mut new_bytes = 0usize;
                    let mut left = count as usize;
                    for x in d.items.drain(..) {
                        if left > 0 && x.as_ref() == elem {
                            left -= 1;
                            removed += 1;
                        } else {
                            new_bytes += x.len();
                            keep.push_back(x);
                        }
                    }
                    d.items = keep;
                    d.bytes = new_bytes;
                    removed
                } else {
                    let mut keep = VecDeque::with_capacity(d.len());
                    let mut new_bytes = 0usize;
                    let mut left = count.unsigned_abs() as usize;
                    for x in d.items.drain(..).rev() {
                        if left > 0 && x.as_ref() == elem {
                            left -= 1;
                            removed += 1;
                        } else {
                            new_bytes += x.len();
                            keep.push_front(x);
                        }
                    }
                    d.items = keep;
                    d.bytes = new_bytes;
                    removed
                }
            }
            ListData::Compact(_) => unreachable!(),
        };
        if removed > 0 {
            self.maybe_demote();
        }
        // All removed elements equal `elem`, so delta is known without a
        // second payload_bytes load.
        let delta = -((removed * elem.len()) as isize);
        (removed, delta)
    }

    /// Keep only indices [start, stop] inclusive. Converts to Linked, then
    /// may demote back to Compact when the result is small.
    /// Returns the signed payload change (negative when elements are dropped).
    pub fn trim(&mut self, start: usize, stop: usize) -> isize {
        self.ensure_linked();
        let delta = if let ListData::Linked(d) = self {
            if start >= d.len() || start > stop {
                let dropped = d.bytes as isize;
                d.items.clear();
                d.bytes = 0;
                -dropped
            } else {
                let stop = stop.min(d.len() - 1);
                let mut dropped: isize = 0;
                // Drop tail then head, adjusting the cached byte total.
                while d.len() > stop + 1 {
                    if let Some(v) = d.pop_back() {
                        dropped += v.len() as isize;
                    }
                }
                for _ in 0..start {
                    if let Some(v) = d.pop_front() {
                        dropped += v.len() as isize;
                    }
                }
                -dropped
            }
        } else {
            0
        };
        self.maybe_demote();
        delta
    }

    /// Collect elements in [start, stop] inclusive as Bytes (for LRANGE).
    pub fn range_bytes(&self, start: usize, stop: usize) -> Vec<Bytes> {
        if self.is_empty() || start > stop {
            return Vec::new();
        }
        let stop = stop.min(self.len() - 1);
        if start >= self.len() {
            return Vec::new();
        }
        match self {
            ListData::Compact(c) => c.range_bytes(start, stop),
            ListData::Linked(d) => d.items.range(start..=stop).cloned().collect(),
        }
    }

    /// All elements as owned Bytes (AOF / SORT / DUMP helpers).
    pub fn to_vec_bytes(&self) -> Vec<Bytes> {
        match self {
            ListData::Compact(c) => c.iter_elements().collect(),
            ListData::Linked(d) => d.items.iter().cloned().collect(),
        }
    }

    /// Iterate element lengths without allocating (memory estimate helpers).
    pub fn iter_lens(&self) -> impl Iterator<Item = usize> + '_ {
        // Materialize small for simplicity — lists under compact cap are small.
        let lens: Vec<usize> = match self {
            ListData::Compact(c) => c.iter_elements().map(|b| b.len()).collect(),
            ListData::Linked(d) => d.items.iter().map(|b| b.len()).collect(),
        };
        lens.into_iter()
    }
}

/// Contiguous length-prefixed list with an offset table for O(1) random access.
///
/// Live data lives in `buf[head..]`. Spare capacity before `head` makes
/// `push_front` amortized O(1) (redis-benchmark LPUSH) instead of a full
/// buffer rebuild + offset rewrite every call.
///
/// The offset table is a ring-style `Vec` with spare slots before
/// `off_head` so prepending an offset is amortized O(1) too (plain
/// `Vec::insert(0, …)` would be O(n) and dominate pipelined LPUSH once
/// the list grows past a few dozen elements).
///
/// `buf` is `Arc`-shared so concurrent LRANGE can snap a region with a
/// refcount bump (no memcpy) under a brief read lock, then frame outside.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactList {
    /// Backing storage; live range is `buf[head..]`. Shared via Arc so
    /// readers can clone cheaply; writers use `Arc::make_mut`.
    buf: Arc<Vec<u8>>,
    /// Start of live data in `buf` (unused prefix is headroom for LPUSH).
    #[serde(default)]
    head: usize,
    len: usize,
    /// Sum of element byte lengths.
    bytes: usize,
    /// Absolute byte offset of each element's length prefix in `buf`.
    /// Live entries are `offsets[off_head .. off_head + len]`.
    offsets: Vec<u32>,
    /// Start of live offsets in `offsets` (spare prefix = LPUSH headroom).
    #[serde(default)]
    off_head: usize,
}

/// Owned snap of a Compact range for lock-free RESP framing.
#[derive(Debug, Clone)]
pub struct CompactRangeSnap {
    /// Shared compact buffer (refcount; not a data copy).
    pub buf: Arc<Vec<u8>>,
    /// Inclusive start offset of the length-prefixed region in `buf`.
    pub begin: usize,
    /// Exclusive end offset of the region in `buf`.
    pub end: usize,
    pub count: usize,
    /// Exact RESP bulk-array budget for this range.
    pub need: usize,
}

impl CompactList {
    pub fn new() -> Self {
        // Seed front headroom so the first few LPUSHes don't pay
        // ensure_front_room / ensure_off_front_room reallocs (redis-benchmark
        // LPUSH starts empty and hammer-pushes the same key).
        const FRONT_SPARE: usize = 128;
        const OFF_SPARE: usize = 16;
        let mut buf = Vec::with_capacity(FRONT_SPARE + 256);
        buf.resize(FRONT_SPARE, 0);
        let mut offsets = Vec::with_capacity(OFF_SPARE + 16);
        offsets.resize(OFF_SPARE, 0);
        Self {
            buf: Arc::new(buf),
            head: FRONT_SPARE,
            len: 0,
            bytes: 0,
            offsets,
            off_head: OFF_SPARE,
        }
    }

    fn with_capacity(n_elems: usize, payload: usize) -> Self {
        Self {
            buf: Arc::new(Vec::with_capacity(n_elems * 4 + payload)),
            head: 0,
            len: 0,
            bytes: 0,
            offsets: Vec::with_capacity(n_elems),
            off_head: 0,
        }
    }

    #[inline]
    fn buf_mut(&mut self) -> &mut Vec<u8> {
        Arc::make_mut(&mut self.buf)
    }

    /// Live offset at logical index `i` (0..len).
    #[inline]
    fn off_at(&self, i: usize) -> u32 {
        self.offsets[self.off_head + i]
    }

    /// Ensure at least `need` free bytes before `head` for push_front.
    fn ensure_front_room(&mut self, need: usize) {
        if self.head >= need {
            return;
        }
        let live = self.buf.len() - self.head;
        let spare = need.saturating_mul(8).max(live / 2).max(64).max(need);
        let mut new_buf = Vec::with_capacity(spare + live + 64);
        new_buf.resize(spare, 0);
        new_buf.extend_from_slice(&self.buf[self.head..]);
        let delta = spare as i64 - self.head as i64;
        // Only rewrite the live offset window.
        for o in &mut self.offsets[self.off_head..self.off_head + self.len] {
            *o = (*o as i64 + delta) as u32;
        }
        self.buf = Arc::new(new_buf);
        self.head = spare;
    }

    /// Ensure at least one free slot before `off_head` for offset prepend.
    fn ensure_off_front_room(&mut self) {
        if self.off_head > 0 {
            return;
        }
        // Grow spare prefix for amortized O(1) LPUSH of offsets.
        let spare = self.len.max(8);
        let mut new_off = Vec::with_capacity(spare + self.len + 8);
        new_off.resize(spare, 0);
        new_off.extend_from_slice(&self.offsets[self.off_head..self.off_head + self.len]);
        self.offsets = new_off;
        self.off_head = spare;
    }

    fn push_back(&mut self, v: Bytes) {
        let need = 4 + v.len();
        let off = self.buf.len() as u32;
        // Keep the live window tight at the end so push stays O(1).
        let end = self.off_head + self.len;
        if end < self.offsets.len() {
            self.offsets.truncate(end);
        }
        self.offsets.push(off);
        {
            let buf = self.buf_mut();
            let rem = buf.capacity().saturating_sub(buf.len());
            if rem < need {
                // Grow by at least 2× capacity (or 64 B floor) so RPUSH under
                // redis-benchmark amortizes reallocs across the Compact phase.
                let min_cap = buf.len().saturating_add(need);
                let target = min_cap.max(buf.capacity().saturating_mul(2)).max(64);
                buf.reserve(target.saturating_sub(buf.len()));
            }
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(&v);
        }
        self.len += 1;
        self.bytes += v.len();
    }

    fn push_front(&mut self, v: Bytes) {
        let insert = 4 + v.len();
        self.ensure_front_room(insert);
        self.ensure_off_front_room();
        self.head -= insert;
        let head = self.head;
        {
            let buf = self.buf_mut();
            buf[head..head + 4].copy_from_slice(&(v.len() as u32).to_le_bytes());
            buf[head + 4..head + insert].copy_from_slice(&v);
        }
        // Existing absolute offsets stay valid; only prepend the new head.
        self.off_head -= 1;
        self.offsets[self.off_head] = head as u32;
        self.len += 1;
        self.bytes += v.len();
    }

    fn pop_front(&mut self) -> Option<Bytes> {
        if self.len == 0 {
            return None;
        }
        let off = self.head;
        let n = u32::from_le_bytes(self.buf[off..off + 4].try_into().ok()?) as usize;
        let val = Bytes::copy_from_slice(&self.buf[off + 4..off + 4 + n]);
        let drained = 4 + n;
        self.head += drained;
        self.off_head += 1;
        self.len -= 1;
        self.bytes -= n;
        // Reclaim dead prefix under LPUSH/LPOP churn.
        if self.head > 4096 && self.head > (self.buf.len() - self.head) {
            let live = self.buf.len() - self.head;
            let mut new_buf = Vec::with_capacity(live);
            new_buf.extend_from_slice(&self.buf[self.head..]);
            for o in &mut self.offsets[self.off_head..self.off_head + self.len] {
                *o -= self.head as u32;
            }
            self.buf = Arc::new(new_buf);
            self.head = 0;
        }
        // Compact offset table if the dead prefix is large.
        if self.off_head > 64 && self.off_head > self.len {
            let mut new_off = Vec::with_capacity(self.len);
            new_off.extend_from_slice(&self.offsets[self.off_head..self.off_head + self.len]);
            self.offsets = new_off;
            self.off_head = 0;
        }
        Some(val)
    }

    fn pop_back(&mut self) -> Option<Bytes> {
        if self.len == 0 {
            return None;
        }
        let last_start = self.off_at(self.len - 1) as usize;
        let last_n =
            u32::from_le_bytes(self.buf[last_start..last_start + 4].try_into().ok()?) as usize;
        let val = Bytes::copy_from_slice(&self.buf[last_start + 4..last_start + 4 + last_n]);
        self.buf_mut().truncate(last_start);
        // Drop the last live offset (and any trailing garbage).
        self.offsets.truncate(self.off_head + self.len - 1);
        self.len -= 1;
        self.bytes -= last_n;
        Some(val)
    }

    fn get(&self, i: usize) -> Option<Bytes> {
        if i >= self.len {
            return None;
        }
        let off = self.off_at(i) as usize;
        let n = u32::from_le_bytes(self.buf[off..off + 4].try_into().ok()?) as usize;
        Some(Bytes::copy_from_slice(&self.buf[off + 4..off + 4 + n]))
    }

    fn range_bytes(&self, start: usize, stop: usize) -> Vec<Bytes> {
        self.range_iter(start, stop)
            .map(Bytes::copy_from_slice)
            .collect()
    }

    /// Zero-copy range walk — yields `&[u8]` slices into the compact buffer.
    pub fn range_iter(&self, start: usize, stop: usize) -> CompactRangeIter<'_> {
        debug_assert!(start <= stop && stop < self.len);
        let off = self.off_at(start) as usize;
        CompactRangeIter {
            buf: &self.buf,
            off,
            remaining: stop - start + 1,
        }
    }

    /// Contiguous byte slice covering elements `[start, stop]` inclusive.
    pub fn range_region(&self, start: usize, stop: usize) -> &[u8] {
        debug_assert!(start <= stop && stop < self.len);
        let begin = self.off_at(start) as usize;
        let end_off = self.off_at(stop) as usize;
        let end_n = u32::from_le_bytes(self.buf[end_off..end_off + 4].try_into().unwrap()) as usize;
        &self.buf[begin..end_off + 4 + end_n]
    }

    /// Snapshot a Compact range for LRANGE: Arc clone of the buffer (no
    /// payload memcpy) + O(1) RESP size estimate. Caller frames outside the
    /// shard lock.
    pub fn range_snap(&self, start: usize, stop: usize) -> CompactRangeSnap {
        debug_assert!(start <= stop && stop < self.len);
        let begin = self.off_at(start) as usize;
        let end_off = self.off_at(stop) as usize;
        let end_n = u32::from_le_bytes(self.buf[end_off..end_off + 4].try_into().unwrap()) as usize;
        let end = end_off + 4 + end_n;
        let count = stop - start + 1;
        let region_len = end - begin;
        // Each element contributes a u32 length prefix in the region.
        let payload = region_len.saturating_sub(count * 4);
        // "*N\r\n" ≤ 16; per-elem "$len\r\n" + "\r\n" ≤ 12 for small lengths.
        let need = 16 + count * 12 + payload;
        CompactRangeSnap {
            buf: Arc::clone(&self.buf),
            begin,
            end,
            count,
            need,
        }
    }

    /// Exact payload + framing budget for RESP bulk array of this range.
    pub fn range_resp_need(&self, start: usize, stop: usize) -> usize {
        let count = stop - start + 1;
        let begin = self.off_at(start) as usize;
        let end_off = self.off_at(stop) as usize;
        let end_n = u32::from_le_bytes(self.buf[end_off..end_off + 4].try_into().unwrap()) as usize;
        let region_len = (end_off + 4 + end_n) - begin;
        let payload = region_len.saturating_sub(count * 4);
        16 + count * 12 + payload
    }

    fn iter_elements(&self) -> CompactIter<'_> {
        let off = if self.len == 0 {
            self.head
        } else {
            self.off_at(0) as usize
        };
        CompactIter {
            buf: &self.buf,
            off,
            remaining: self.len,
        }
    }
}

impl Default for CompactList {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CompactIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl Iterator for CompactIter<'_> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let n = u32::from_le_bytes(self.buf[self.off..self.off + 4].try_into().ok()?) as usize;
        let val = Bytes::copy_from_slice(&self.buf[self.off + 4..self.off + 4 + n]);
        self.off += 4 + n;
        self.remaining -= 1;
        Some(val)
    }
}

/// Zero-copy iterator over a Compact range — each item is a slice into the
/// contiguous list buffer (valid for the lifetime of the CompactList borrow).
pub struct CompactRangeIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl<'a> Iterator for CompactRangeIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let n = u32::from_le_bytes(self.buf[self.off..self.off + 4].try_into().ok()?) as usize;
        let slice = &self.buf[self.off + 4..self.off + 4 + n];
        self.off += 4 + n;
        self.remaining -= 1;
        Some(slice)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for CompactRangeIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    // Tests share process-wide atomic thresholds — serialise the ones that
    // touch them so parallel runs can't observe a half-mutated state.
    static THRESHOLDS_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn compact_push_pop_roundtrip() {
        let mut l = ListData::new();
        l.push_back(Bytes::from_static(b"a"));
        l.push_back(Bytes::from_static(b"b"));
        l.push_front(Bytes::from_static(b"z"));
        assert!(matches!(l, ListData::Compact(_)));
        assert_eq!(l.len(), 3);
        assert_eq!(l.get(0).unwrap().as_ref(), b"z");
        assert_eq!(l.get(2).unwrap().as_ref(), b"b");
        assert_eq!(l.pop_front().unwrap().as_ref(), b"z");
        assert_eq!(l.pop_back().unwrap().as_ref(), b"b");
        assert_eq!(l.pop_front().unwrap().as_ref(), b"a");
        assert!(l.is_empty());
    }

    #[test]
    fn promote_on_max_elem() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        set_list_thresholds(ListThresholds::default());
        let mut l = ListData::new();
        let big = Bytes::from(vec![b'x'; LIST_COMPACT_MAX_ELEM + 1]);
        l.push_back(big);
        assert!(matches!(l, ListData::Linked(_)));
        assert_eq!(l.len(), 1);
        set_list_thresholds(prev);
    }

    #[test]
    fn mid_list_set_converts_to_linked() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        set_list_thresholds(ListThresholds::default());
        let mut l = ListData::new();
        l.push_back(Bytes::from_static(b"a"));
        l.push_back(Bytes::from_static(b"b"));
        assert!(l.set(1, Bytes::from_static(b"B")).0);
        assert!(matches!(l, ListData::Linked(_)));
        assert_eq!(l.get(1).unwrap().as_ref(), b"B");
        set_list_thresholds(prev);
    }

    #[test]
    fn range_bytes() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        set_list_thresholds(ListThresholds::default());
        let mut l = ListData::new();
        for s in [b"a" as &[u8], b"b", b"c", b"d"] {
            l.push_back(Bytes::copy_from_slice(s));
        }
        let r = l.range_bytes(1, 2);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].as_ref(), b"b");
        assert_eq!(r[1].as_ref(), b"c");
        set_list_thresholds(prev);
    }

    #[test]
    fn offset_table_random_access() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        set_list_thresholds(ListThresholds::default());
        let mut l = ListData::new();
        for i in 0..64u8 {
            l.push_back(Bytes::copy_from_slice(&[i]));
        }
        assert!(matches!(l, ListData::Compact(_)));
        for i in 0..64u8 {
            assert_eq!(l.get(i as usize).unwrap().as_ref(), &[i]);
        }
        let r = l.range_bytes(50, 55);
        assert_eq!(r.len(), 6);
        assert_eq!(r[0].as_ref(), &[50]);
        assert_eq!(r[5].as_ref(), &[55]);
        set_list_thresholds(prev);
    }

    #[test]
    fn demote_after_pop_shrink() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        set_list_thresholds(ListThresholds::default());
        let mut l = ListData::new();
        let big = Bytes::from(vec![b'x'; LIST_COMPACT_MAX_ELEM + 1]);
        l.push_back(big);
        assert!(matches!(l, ListData::Linked(_)));
        let _ = l.pop_front();
        for s in [b"a" as &[u8], b"b", b"c"] {
            l.push_back(Bytes::copy_from_slice(s));
        }
        l.push_back(Bytes::from_static(b"d"));
        let _ = l.pop_back(); // triggers maybe_demote
        assert!(
            matches!(l, ListData::Compact(_)),
            "expected demote to Compact, got {}",
            l.encoding_name()
        );
        assert_eq!(l.len(), 3);
        assert_eq!(l.get(0).unwrap().as_ref(), b"a");
        set_list_thresholds(prev);
    }

    #[test]
    fn demote_after_trim() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        set_list_thresholds(ListThresholds::default());
        let mut l = ListData::new();
        let max = list_thresholds().max_entries;
        for i in 0..(max + 2) {
            l.push_back(Bytes::copy_from_slice(format!("e{i}").as_bytes()));
        }
        assert!(matches!(l, ListData::Linked(_)));
        l.trim(0, 3);
        assert!(matches!(l, ListData::Compact(_)));
        assert_eq!(l.len(), 4);
        set_list_thresholds(prev);
    }

    #[test]
    fn thresholds_config_affects_promote() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        set_list_thresholds(ListThresholds {
            max_entries: 3,
            max_size: prev.max_size,
            max_elem: prev.max_elem,
            demote_entries: prev.demote_entries,
        });
        let mut l = ListData::new();
        l.push_back(Bytes::from_static(b"a"));
        l.push_back(Bytes::from_static(b"b"));
        l.push_back(Bytes::from_static(b"c"));
        assert!(matches!(l, ListData::Compact(_)));
        l.push_back(Bytes::from_static(b"d"));
        assert!(matches!(l, ListData::Linked(_)));
        set_list_thresholds(prev);
    }

    #[test]
    fn linked_payload_bytes_tracks_and_roundtrips() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        // Stay Linked after shrink so we exercise the Linked wire path.
        set_list_thresholds(ListThresholds {
            demote_entries: 0,
            ..ListThresholds::default()
        });
        let mut l = ListData::new();
        let big = Bytes::from(vec![b'x'; LIST_COMPACT_MAX_ELEM + 1]);
        l.push_back(big.clone());
        assert!(matches!(l, ListData::Linked(_)));
        assert_eq!(l.payload_bytes(), big.len());
        l.push_front(Bytes::from_static(b"ab"));
        assert_eq!(l.payload_bytes(), big.len() + 2);
        let _ = l.pop_back();
        assert!(matches!(l, ListData::Linked(_)));
        assert_eq!(l.payload_bytes(), 2);

        let encoded = bincode::serde::encode_to_vec(&l, bincode::config::standard()).unwrap();
        let (decoded, _): (ListData, usize) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded.payload_bytes(), 2);
        assert_eq!(decoded.get(0).unwrap().as_ref(), b"ab");
        assert!(matches!(decoded, ListData::Linked(_)));
        set_list_thresholds(prev);
    }

    #[test]
    fn rem_returns_known_payload_delta() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        set_list_thresholds(ListThresholds {
            demote_entries: 0,
            ..ListThresholds::default()
        });
        let mut l = ListData::new();
        for s in [b"aa" as &[u8], b"bb", b"aa", b"cc", b"aa"] {
            l.push_back(Bytes::copy_from_slice(s));
        }
        let before = l.payload_bytes();
        let (n, delta) = l.rem(2, b"aa");
        assert_eq!(n, 2);
        assert_eq!(delta, -4); // 2 × "aa"
        assert_eq!(l.payload_bytes() as isize, before as isize + delta);
        assert_eq!(l.len(), 3);
        set_list_thresholds(prev);
    }

    #[test]
    fn trim_returns_known_payload_delta() {
        let _g = THRESHOLDS_LOCK.lock().unwrap();
        let prev = list_thresholds();
        set_list_thresholds(ListThresholds {
            demote_entries: 0,
            ..ListThresholds::default()
        });
        let mut l = ListData::new();
        for s in [b"a" as &[u8], b"bb", b"ccc", b"dddd", b"eeeee"] {
            l.push_back(Bytes::copy_from_slice(s));
        }
        let before = l.payload_bytes();
        // Keep indices [1, 2] → "bb" + "ccc" = 5; drop "a"(1) + "dddd"(4) + "eeeee"(5) = 10
        let delta = l.trim(1, 2);
        assert_eq!(delta, -10);
        assert_eq!(l.payload_bytes() as isize, before as isize + delta);
        assert_eq!(l.len(), 2);
        assert_eq!(l.get(0).unwrap().as_ref(), b"bb");
        set_list_thresholds(prev);
    }
}
