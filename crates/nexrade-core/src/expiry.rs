//! TTL and expiry management.

use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
fn system_now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(target_arch = "wasm32")]
fn system_now_ms() -> u128 {
    0
}

/// Expiry information for a key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Expiry {
    /// Unix timestamp in milliseconds at which the key expires.
    pub expires_at_ms: u128,
}

impl Expiry {
    /// Create expiry from duration relative to now.
    pub fn from_duration(ttl: Duration) -> Self {
        Self {
            expires_at_ms: system_now_ms() + ttl.as_millis(),
        }
    }

    /// Create expiry from absolute millisecond unix timestamp.
    pub fn from_ms(ms: u64) -> Self {
        Self {
            expires_at_ms: ms as u128,
        }
    }

    /// Create expiry from absolute second unix timestamp.
    pub fn from_secs(secs: u64) -> Self {
        Self {
            expires_at_ms: (secs as u128) * 1000,
        }
    }

    /// Is this key currently expired?
    pub fn is_expired(&self) -> bool {
        system_now_ms() >= self.expires_at_ms
    }

    /// Remaining TTL in milliseconds, or 0 if already expired.
    pub fn remaining_ms(&self) -> u64 {
        let now = system_now_ms();
        if now >= self.expires_at_ms {
            0
        } else {
            (self.expires_at_ms - now) as u64
        }
    }

    /// Remaining TTL in seconds, rounded up.
    pub fn remaining_secs(&self) -> u64 {
        self.remaining_ms().div_ceil(1000)
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Instant-based expiry for in-memory tracking (not serialized).
#[derive(Debug, Clone, Copy)]
pub struct InstantExpiry {
    pub deadline: Instant,
}

#[cfg(not(target_arch = "wasm32"))]
impl InstantExpiry {
    pub fn new(ttl: Duration) -> Self {
        Self {
            deadline: Instant::now() + ttl,
        }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.deadline
    }
}
