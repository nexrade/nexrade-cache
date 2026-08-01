//! Redis-compatible keyspace / keyevent notifications.
//!
//! `notify-keyspace-events` is a single bitmap flag string. Each character
//! toggles a class of events on/off; we mirror Redis's vocabulary so
//! existing client configuration drops in without translation:
//!
//! | Char | Class |
//! |------|-------|
//! | `K`  | Keyspace events, published to `__keyspace@<db>__:<key>` |
//! | `E`  | Keyevent events, published to `__keyevent@<db>__:<event>` |
//! | `g`  | Generic commands (DEL, EXPIRE, RENAME, …) |
//! | `$`  | String commands (SET, INCR, APPEND, …) |
//! | `l`  | List commands (LPUSH, RPUSH, LSET, LPOP, RPOP, …) |
//! | `h`  | Hash commands (HSET, HDEL, HINCRBY, …) |
//! | `z`  | Sorted set commands (ZADD, ZREM, …) |
//! | `x`  | Expiration events (lazy + active) |
//! | `e`  | Eviction events (`maxmemory` policy) |
//! | `t`  | Stream commands (XADD, XDEL, XTRIM, XGROUP, XREADGROUP, XACK, …) |
//! | `A`  | Alias for "g$lshzxet" — all events except the keyspace prefix |
//!
//! Per-event subset uses a second char on the command class (e.g. `Ex` =
//! expiration keyevent). We honor the documented subset:
//!
//! * `g`: del, expire, rename, ...
//! * `$`: set, incr, decr, append, setrange, getdel, getset, mset, msetnx
//! * `l`: lpush, rpush, lset, lrem, ltrim, lpop, rpop, linsert, lmove, rpoplpush
//! * `h`: hset, hdel, hmset, hincrby, hincrbyfloat, hsetnx
//! * `z`: zadd, zrem, zincrby, zremrangebyscore, zremrangebyrank, zunionstore, zinterstore, zdiffstore
//! * `x`: expired (lazy + active)
//! * `e`: evicted (maxmemory)
//! * `t`: stream (xadd, xtrim, xdel, xreadgroup, xack, xclaim, xautoclaim, xsetid, xgroup)
//!
//! Implementation lives in `db::Db::notify_keyspace_event` which is called
//! from the dispatch path after every successful write / expiry / eviction.
//! It is a no-op when the bitmask is 0 (the common case).

use std::sync::atomic::{AtomicU64, Ordering};

/// Bitmap of notify-keyspace-events flags (see module docs).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotifyFlags(pub u64);

impl NotifyFlags {
    pub const K_GENERIC: u64 = 1 << 0;
    pub const K_STRING: u64 = 1 << 1;
    pub const K_LIST: u64 = 1 << 2;
    pub const K_HASH: u64 = 1 << 3;
    pub const K_ZSET: u64 = 1 << 4;
    pub const K_EXPIRED: u64 = 1 << 5;
    pub const K_EVICTED: u64 = 1 << 6;
    pub const K_STREAM: u64 = 1 << 7;
    /// `K` — emit keyspace events to `__keyspace@<db>__:<key>`.
    pub const PREFIX_KEYSPACE: u64 = 1 << 32;
    /// `E` — emit keyevent events to `__keyevent@<db>__:<event>`.
    pub const PREFIX_KEYEVENT: u64 = 1 << 33;

    /// Empty mask (no notifications).
    pub const fn empty() -> Self {
        Self(0)
    }

    pub fn parse(s: &str) -> Self {
        let mut flags = 0u64;
        for ch in s.chars() {
            match ch {
                'g' => flags |= Self::K_GENERIC,
                '$' => flags |= Self::K_STRING,
                'l' => flags |= Self::K_LIST,
                'h' => flags |= Self::K_HASH,
                'z' => flags |= Self::K_ZSET,
                'x' => flags |= Self::K_EXPIRED,
                'e' => flags |= Self::K_EVICTED,
                't' => flags |= Self::K_STREAM,
                'K' => flags |= Self::PREFIX_KEYSPACE,
                'E' => flags |= Self::PREFIX_KEYEVENT,
                'A' => {
                    flags |= Self::K_GENERIC
                        | Self::K_STRING
                        | Self::K_LIST
                        | Self::K_HASH
                        | Self::K_ZSET
                        | Self::K_EXPIRED
                        | Self::K_EVICTED
                        | Self::K_STREAM;
                }
                _ => {} // Redis silently ignores unknown chars
            }
        }
        Self(flags)
    }

    pub fn as_str(self) -> String {
        let mut out = String::new();
        if self.0 & Self::PREFIX_KEYSPACE != 0 {
            out.push('K');
        }
        if self.0 & Self::PREFIX_KEYEVENT != 0 {
            out.push('E');
        }
        if self.0 & Self::K_GENERIC != 0 {
            out.push('g');
        }
        if self.0 & Self::K_STRING != 0 {
            out.push('$');
        }
        if self.0 & Self::K_LIST != 0 {
            out.push('l');
        }
        if self.0 & Self::K_HASH != 0 {
            out.push('h');
        }
        if self.0 & Self::K_ZSET != 0 {
            out.push('z');
        }
        if self.0 & Self::K_EXPIRED != 0 {
            out.push('x');
        }
        if self.0 & Self::K_EVICTED != 0 {
            out.push('e');
        }
        if self.0 & Self::K_STREAM != 0 {
            out.push('t');
        }
        out
    }

    pub fn contains(self, other: u64) -> bool {
        self.0 & other != 0
    }
}

/// Classify a dispatch command into (bit, event-name) for the keyevent
/// channel. Returns `None` if the command does not map to a keyspace event.
pub fn classify(cmd: &str) -> Option<(u64, &'static str)> {
    match cmd {
        // Generic
        "DEL" | "UNLINK" => Some((NotifyFlags::K_GENERIC, "del")),
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => Some((NotifyFlags::K_GENERIC, "expire")),
        "PERSIST" => Some((NotifyFlags::K_GENERIC, "persist")),
        "RENAME" | "RENAMENX" => Some((NotifyFlags::K_GENERIC, "rename")),
        "COPY" => Some((NotifyFlags::K_GENERIC, "copy")),
        "FLUSHDB" | "FLUSHALL" => Some((NotifyFlags::K_GENERIC, "flushdb")),

        // String
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "MSET" | "MSETNX" => {
            Some((NotifyFlags::K_STRING, "set"))
        }
        "GETSET" => Some((NotifyFlags::K_STRING, "getset")),
        "GETDEL" | "GETEX" => Some((NotifyFlags::K_STRING, "getdel")),
        "INCR" | "INCRBY" | "INCRBYFLOAT" | "DECR" | "DECRBY" => {
            Some((NotifyFlags::K_STRING, "incr"))
        }
        "APPEND" | "SETRANGE" => Some((NotifyFlags::K_STRING, "append")),

        // List
        "LPUSH" | "LPUSHX" => Some((NotifyFlags::K_LIST, "lpush")),
        "RPUSH" | "RPUSHX" => Some((NotifyFlags::K_LIST, "rpush")),
        "LPOP" => Some((NotifyFlags::K_LIST, "lpop")),
        "RPOP" => Some((NotifyFlags::K_LIST, "rpop")),
        "LSET" => Some((NotifyFlags::K_LIST, "lset")),
        "LREM" => Some((NotifyFlags::K_LIST, "lrem")),
        "LTRIM" => Some((NotifyFlags::K_LIST, "ltrim")),
        "LINSERT" => Some((NotifyFlags::K_LIST, "linsert")),
        "LMOVE" | "RPOPLPUSH" => Some((NotifyFlags::K_LIST, "lmove")),

        // Hash
        "HSET" | "HSETNX" | "HMSET" => Some((NotifyFlags::K_HASH, "hset")),
        "HDEL" => Some((NotifyFlags::K_HASH, "hdel")),
        "HINCRBY" | "HINCRBYFLOAT" => Some((NotifyFlags::K_HASH, "hincrby")),

        // Sorted set
        "ZADD" => Some((NotifyFlags::K_ZSET, "zadd")),
        "ZINCRBY" => Some((NotifyFlags::K_ZSET, "zincrby")),
        "ZREM" => Some((NotifyFlags::K_ZSET, "zrem")),
        "ZREMRANGEBYSCORE" => Some((NotifyFlags::K_ZSET, "zremrangebyscore")),
        "ZREMRANGEBYRANK" => Some((NotifyFlags::K_ZSET, "zremrangebyrank")),
        "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" => Some((NotifyFlags::K_ZSET, "zunionstore")),

        // Stream
        "XADD" => Some((NotifyFlags::K_STREAM, "xadd")),
        "XDEL" => Some((NotifyFlags::K_STREAM, "xdel")),
        "XTRIM" => Some((NotifyFlags::K_STREAM, "xtrim")),
        "XREADGROUP" => Some((NotifyFlags::K_STREAM, "xreadgroup")),
        "XACK" => Some((NotifyFlags::K_STREAM, "xack")),
        "XCLAIM" => Some((NotifyFlags::K_STREAM, "xclaim")),
        "XAUTOCLAIM" => Some((NotifyFlags::K_STREAM, "xautoclaim")),
        "XSETID" => Some((NotifyFlags::K_STREAM, "xsetid")),
        "XGROUP" => Some((NotifyFlags::K_STREAM, "xgroup")),

        _ => None,
    }
}

/// Lock-free mirror of `config.notify_keyspace_events` (a packed u64). The
/// dispatch path reads it on every successful write; when the bitmask is 0
/// (the common case) the load is the only cost.
///
/// Cloned as an `Arc` so every `Db` handle shares the same flag word —
/// `CONFIG SET notify-keyspace-events` on one connection is visible to all.
#[derive(Debug, Default, Clone)]
pub struct NotifyFlagsAtomic(pub std::sync::Arc<AtomicU64>);

impl NotifyFlagsAtomic {
    pub fn new(value: NotifyFlags) -> Self {
        Self(std::sync::Arc::new(AtomicU64::new(value.0)))
    }

    #[inline]
    pub fn load(&self) -> NotifyFlags {
        NotifyFlags(self.0.load(Ordering::Relaxed))
    }

    pub fn store(&self, value: NotifyFlags) {
        self.0.store(value.0, Ordering::Relaxed);
    }
}
