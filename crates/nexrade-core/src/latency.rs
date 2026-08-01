//! Per-event latency samples for the Redis `LATENCY` command family.
//!
//! Distinct from [`crate::slowlog::SlowLog`]: the slowlog only records
//! commands that cross a configurable threshold (default 10 ms). The
//! latency monitor samples every timed command into a fixed-size ring so
//! `LATENCY LATEST` / `HISTORY` / `HISTOGRAM` have something to report
//! even when the slowlog is quiet.
//!
//! Design goals (0.7.4):
//!   * Cheap under redis-benchmark — one atomic max + a short Mutex hold
//!     only when recording (the connection already times commands for
//!     metrics / slowlog).
//!   * Fixed memory: at most `HISTORY_CAP` samples per event name and
//!     `MAX_EVENTS` distinct event names.
//!   * Event names are lowercased command names (matches Redis's
//!     convention of one event per command family).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::db::unix_secs;

/// Samples retained per event for `LATENCY HISTORY`.
const HISTORY_CAP: usize = 160;
/// Distinct event names retained (LRU-ish: oldest insert is dropped when full).
const MAX_EVENTS: usize = 64;

/// One latency sample.
#[derive(Debug, Clone, Copy)]
pub struct LatencySample {
    pub timestamp: u64,
    pub duration_us: u64,
}

/// Aggregate state for a single event name.
#[derive(Debug, Clone)]
struct EventState {
    history: VecDeque<LatencySample>,
    /// Most recent sample's duration (µs).
    latest_us: u64,
    /// Timestamp of the most recent sample.
    latest_ts: u64,
    /// All-time max for this event (µs).
    all_time_us: u64,
}

impl EventState {
    fn new() -> Self {
        Self {
            history: VecDeque::with_capacity(HISTORY_CAP),
            latest_us: 0,
            latest_ts: 0,
            all_time_us: 0,
        }
    }

    fn push(&mut self, sample: LatencySample) {
        self.latest_us = sample.duration_us;
        self.latest_ts = sample.timestamp;
        if sample.duration_us > self.all_time_us {
            self.all_time_us = sample.duration_us;
        }
        if self.history.len() >= HISTORY_CAP {
            self.history.pop_front();
        }
        self.history.push_back(sample);
    }
}

struct LatencyInner {
    events: HashMap<String, EventState>,
    /// Insertion order for MAX_EVENTS eviction (oldest first).
    order: VecDeque<String>,
}

/// Shared latency monitor. Cloneable (Arc-internal).
#[derive(Clone)]
pub struct LatencyMonitor {
    inner: Arc<Mutex<LatencyInner>>,
    /// Global all-time max duration in µs (lock-free read for DOCTOR).
    global_max_us: Arc<AtomicU64>,
    /// Total samples recorded (lock-free).
    total_samples: Arc<AtomicU64>,
}

impl LatencyMonitor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LatencyInner {
                events: HashMap::new(),
                order: VecDeque::new(),
            })),
            global_max_us: Arc::new(AtomicU64::new(0)),
            total_samples: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record a sample for `event` (lowercased command name). No-op when
    /// `duration_us == 0` (untimed commands under the no-metrics/no-slowlog
    /// fast path).
    pub fn record(&self, event: &str, duration_us: u64) {
        if duration_us == 0 || event.is_empty() {
            return;
        }
        self.total_samples.fetch_add(1, Ordering::Relaxed);
        // Atomic max.
        let mut cur = self.global_max_us.load(Ordering::Relaxed);
        while duration_us > cur {
            match self.global_max_us.compare_exchange_weak(
                cur,
                duration_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }

        let sample = LatencySample {
            timestamp: unix_secs(),
            duration_us,
        };
        let mut g = self.inner.lock();
        if !g.events.contains_key(event) {
            if g.events.len() >= MAX_EVENTS {
                // Evict oldest event name.
                if let Some(old) = g.order.pop_front() {
                    g.events.remove(&old);
                }
            }
            g.order.push_back(event.to_string());
            g.events.insert(event.to_string(), EventState::new());
        }
        if let Some(state) = g.events.get_mut(event) {
            state.push(sample);
        }
    }

    /// `LATENCY LATEST` shape: vec of (event, timestamp, latest_us, all_time_us).
    pub fn latest(&self) -> Vec<(String, u64, u64, u64)> {
        let g = self.inner.lock();
        let mut rows: Vec<_> = g
            .events
            .iter()
            .map(|(name, s)| (name.clone(), s.latest_ts, s.latest_us, s.all_time_us))
            .collect();
        // Stable order by event name (BTreeMap-like for clients that sort).
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    /// `LATENCY HISTORY <event>`: vec of (timestamp, duration_us), oldest first.
    pub fn history(&self, event: &str) -> Vec<(u64, u64)> {
        let g = self.inner.lock();
        match g.events.get(event) {
            Some(s) => s
                .history
                .iter()
                .map(|sample| (sample.timestamp, sample.duration_us))
                .collect(),
            None => Vec::new(),
        }
    }

    /// `LATENCY HISTOGRAM [event ...]`: raw sample values for the named
    /// events (or all events when the list is empty). Redis 7.2+ returns a
    /// map of event → {calls, histogram_usec → {bucket → count}}. We emit
    /// a simplified but compatible shape: for each event a map of
    /// `calls` + a flat `histogram_usec` map of power-of-two buckets.
    pub fn histogram(&self, events: &[&str]) -> BTreeMap<String, LatencyHistogram> {
        let g = self.inner.lock();
        let names: Vec<String> = if events.is_empty() {
            g.events.keys().cloned().collect()
        } else {
            events.iter().map(|s| s.to_string()).collect()
        };
        let mut out = BTreeMap::new();
        for name in names {
            if let Some(state) = g.events.get(&name) {
                out.insert(name, LatencyHistogram::from_samples(&state.history));
            }
        }
        out
    }

    /// Reset one or all events. Returns the number of events cleared.
    pub fn reset(&self, events: &[&str]) -> usize {
        let mut g = self.inner.lock();
        if events.is_empty() {
            let n = g.events.len();
            g.events.clear();
            g.order.clear();
            self.global_max_us.store(0, Ordering::Relaxed);
            // Keep total_samples as a lifetime counter.
            return n;
        }
        let mut n = 0;
        for e in events {
            if g.events.remove(*e).is_some() {
                g.order.retain(|x| x != *e);
                n += 1;
            }
        }
        n
    }

    pub fn global_max_us(&self) -> u64 {
        self.global_max_us.load(Ordering::Relaxed)
    }

    pub fn total_samples(&self) -> u64 {
        self.total_samples.load(Ordering::Relaxed)
    }

    pub fn event_count(&self) -> usize {
        self.inner.lock().events.len()
    }
}

impl Default for LatencyMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Power-of-two histogram buckets for `LATENCY HISTOGRAM`.
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    pub calls: u64,
    /// bucket_upper_us → count (inclusive upper bound, power of two).
    pub buckets: BTreeMap<u64, u64>,
}

impl LatencyHistogram {
    fn from_samples(samples: &VecDeque<LatencySample>) -> Self {
        let mut buckets: BTreeMap<u64, u64> = BTreeMap::new();
        for s in samples {
            let b = next_pow2_us(s.duration_us);
            *buckets.entry(b).or_insert(0) += 1;
        }
        Self {
            calls: samples.len() as u64,
            buckets,
        }
    }
}

/// Smallest power of two ≥ `us`, floored at 1.
fn next_pow2_us(us: u64) -> u64 {
    if us <= 1 {
        return 1;
    }
    let mut v = us - 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v.saturating_add(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_latest() {
        let m = LatencyMonitor::new();
        m.record("get", 100);
        m.record("get", 200);
        m.record("set", 50);
        let latest = m.latest();
        assert_eq!(latest.len(), 2);
        let get = latest.iter().find(|(n, _, _, _)| n == "get").unwrap();
        assert_eq!(get.2, 200); // latest
        assert_eq!(get.3, 200); // all-time max
        let set = latest.iter().find(|(n, _, _, _)| n == "set").unwrap();
        assert_eq!(set.2, 50);
        assert_eq!(set.3, 50);
    }

    #[test]
    fn history_order_and_cap() {
        let m = LatencyMonitor::new();
        for i in 0..200 {
            m.record("get", i + 1);
        }
        let h = m.history("get");
        assert_eq!(h.len(), HISTORY_CAP);
        // Oldest retained is sample 200-160+1 = 41.
        assert_eq!(h[0].1, (200 - HISTORY_CAP + 1) as u64);
        assert_eq!(h[h.len() - 1].1, 200);
    }

    #[test]
    fn histogram_buckets() {
        let m = LatencyMonitor::new();
        m.record("get", 1);
        m.record("get", 2);
        m.record("get", 3);
        m.record("get", 100);
        let hist = m.histogram(&["get"]);
        let h = hist.get("get").unwrap();
        assert_eq!(h.calls, 4);
        // 1 → bucket 1; 2 → 2; 3 → 4; 100 → 128
        assert_eq!(h.buckets.get(&1), Some(&1));
        assert_eq!(h.buckets.get(&2), Some(&1));
        assert_eq!(h.buckets.get(&4), Some(&1));
        assert_eq!(h.buckets.get(&128), Some(&1));
    }

    #[test]
    fn reset_all_and_one() {
        let m = LatencyMonitor::new();
        m.record("get", 10);
        m.record("set", 20);
        assert_eq!(m.reset(&["get"]), 1);
        assert!(m.history("get").is_empty());
        assert!(!m.history("set").is_empty());
        assert_eq!(m.reset(&[]), 1);
        assert_eq!(m.event_count(), 0);
        assert_eq!(m.global_max_us(), 0);
    }

    #[test]
    fn next_pow2() {
        assert_eq!(next_pow2_us(0), 1);
        assert_eq!(next_pow2_us(1), 1);
        assert_eq!(next_pow2_us(2), 2);
        assert_eq!(next_pow2_us(3), 4);
        assert_eq!(next_pow2_us(5), 8);
        assert_eq!(next_pow2_us(100), 128);
        assert_eq!(next_pow2_us(128), 128);
        assert_eq!(next_pow2_us(129), 256);
    }
}
