//! Slow query log — shared across all connections via Db.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::db::unix_secs;

#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    pub id: u64,
    pub timestamp: u64,
    pub duration_us: u64,
    pub args: Vec<String>,
    pub client_addr: String,
}

#[derive(Clone)]
pub struct SlowLog {
    inner: Arc<SlowLogShared>,
}

struct SlowLogShared {
    entries: Mutex<SlowLogInner>,
    threshold_us: AtomicU64,
    max_len: AtomicUsize,
}

struct SlowLogInner {
    entries: VecDeque<SlowLogEntry>,
    next_id: u64,
}

impl SlowLog {
    pub fn new(threshold_us: u64, max_len: usize) -> Self {
        Self {
            inner: Arc::new(SlowLogShared {
                entries: Mutex::new(SlowLogInner {
                    entries: VecDeque::new(),
                    next_id: 0,
                }),
                threshold_us: AtomicU64::new(threshold_us),
                max_len: AtomicUsize::new(max_len),
            }),
        }
    }

    pub fn threshold_us(&self) -> u64 {
        self.inner.threshold_us.load(Ordering::Relaxed)
    }

    pub fn max_len(&self) -> usize {
        self.inner.max_len.load(Ordering::Relaxed)
    }

    pub fn set_threshold(&self, us: u64) {
        self.inner.threshold_us.store(us, Ordering::Relaxed);
    }

    pub fn set_max_len(&self, len: usize) {
        self.inner.max_len.store(len, Ordering::Relaxed);
    }

    pub fn record(&self, duration: Duration, args: Vec<String>, client_addr: String) {
        let us = duration.as_micros() as u64;
        if us < self.inner.threshold_us.load(Ordering::Relaxed) {
            return;
        }
        let max = self.inner.max_len.load(Ordering::Relaxed);
        let mut inner = self.inner.entries.lock();
        let id = inner.next_id;
        inner.next_id += 1;
        inner.entries.push_front(SlowLogEntry {
            id,
            timestamp: unix_secs(),
            duration_us: us,
            args,
            client_addr,
        });
        while inner.entries.len() > max {
            inner.entries.pop_back();
        }
    }

    pub fn get(&self, count: usize) -> Vec<SlowLogEntry> {
        self.inner
            .entries
            .lock()
            .entries
            .iter()
            .take(count)
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.entries.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.entries.lock().entries.is_empty()
    }

    pub fn reset(&self) {
        self.inner.entries.lock().entries.clear();
    }
}
