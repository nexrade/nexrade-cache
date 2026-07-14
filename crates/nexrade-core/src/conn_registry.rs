//! Server-wide registry of live client connections.
//!
//! `CLIENT LIST` / `CLIENT INFO` / `CLIENT KILL` / `CLIENT PAUSE` /
//! `CLIENT UNPAUSE` all need to read across every live TCP connection — so
//! they can't just look at the calling connection. This module owns that
//! cross-connection view.
//!
//! Pattern mirrors `TrackingRegistry` (`tracking.rs`): `Db` holds a
//! `ConnectionRegistry` (cheap to clone, Arc-internal), and each
//! connection holds onto two `Arc`s it got back from `register`:
//!
//! - `meta`: an `Arc<RwLock<ClientMeta>>` — used by the connection itself
//!   to update `last_cmd`, `idle_instant`, `qbuf`/etc on every command.
//! - `kill_flag`: an `Arc<AtomicBool>` — the connection polls it at the
//!   top of its main loop; when set, the loop exits and the connection
//!   closes. `CLIENT KILL ID n` and friends flip this flag.
//!
//! The registry itself is a single `parking_lot::RwLock<HashMap<u64, _>>`
//! taken only on connect/disconnect, `CLIENT LIST`, `CLIENT KILL`, and
//! the very brief `is_paused()` read inside `dispatch_tracked`. None of
//! these are hot paths, so contention is not a concern.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Per-connection metadata exposed via `CLIENT LIST`.
#[derive(Debug)]
pub struct ClientMeta {
    pub id: u64,
    pub addr: SocketAddr,
    pub name: String,
    pub db_index: usize,
    pub user: String,
    pub authenticated: bool,
    pub subscriptions: usize,
    pub pattern_subscriptions: usize,
    pub tracking_enabled: bool,
    pub last_cmd: String,
    pub idle_instant: Instant,
    pub created_instant: Instant,
    /// Bitfield — see `CLIENT_FLAG_*` constants below.
    pub flags: u32,
    /// Approximate current read-buffer length. Recorded on each connection
    /// tick and updated by the writer whenever the buffer changes state.
    pub qbuf: usize,
    /// `read_buf.capacity() - read_buf.len()` — Redis uses 0 as a
    /// placeholder; we approximate via the connection's `BytesMut`.
    pub qbuf_free: usize,
    /// Number of WATCH keys held by this connection (used for `watch=`).
    pub watch_keys: usize,
    /// `multi=-1` or the size of the queued transactions (0..N).
    pub multi: i64,
}

// Bitflag values for `ClientMeta.flags`. These match the official Redis
// CLIENT type constants (see `server.h` in upstream).
pub const CLIENT_FLAG_MASTER: u32 = 1; // Note: not used — we're always primary
pub const CLIENT_FLAG_SLAVE: u32 = 1 << 1;
pub const CLIENT_FLAG_PUBSUB: u32 = 1 << 2;
pub const CLIENT_FLAG_MULTI: u32 = 1 << 3;
// pub const CLIENT_FLAG_MONITOR: u32 = 1 << 4;
pub const CLIENT_FLAG_TRACKING: u32 = 1 << 5;
pub const CLIENT_FLAG_BLOCKED: u32 = 1 << 6;
pub const CLIENT_FLAG_NO_EVICT: u32 = 1 << 7;
pub const CLIENT_FLAG_NO_TOUCH: u32 = 1 << 8;

/// Server-wide registry of live connections. Clone-cheap (Arc-internal).
#[derive(Clone)]
pub struct ConnectionRegistry {
    inner: Arc<RwLock<HashMap<u64, Arc<RwLock<ClientMeta>>>>>,
    /// `Some(deadline)` while a `CLIENT PAUSE <ms>` is in effect.
    paused_until: Arc<RwLock<Option<Instant>>>,
    kill_flags: Arc<RwLock<HashMap<u64, Arc<AtomicBool>>>>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            paused_until: Arc::new(RwLock::new(None)),
            kill_flags: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new connection. Returns `(meta, kill_flag)` — the caller
    /// (the connection handler) holds these for the connection's lifetime
    /// and drops them on disconnect via `unregister`.
    pub fn register(
        &self,
        id: u64,
        addr: SocketAddr,
    ) -> (Arc<RwLock<ClientMeta>>, Arc<AtomicBool>) {
        let meta = Arc::new(RwLock::new(ClientMeta {
            id,
            addr,
            name: String::new(),
            db_index: 0,
            user: "default".to_string(),
            authenticated: false,
            subscriptions: 0,
            pattern_subscriptions: 0,
            tracking_enabled: false,
            last_cmd: String::new(),
            idle_instant: Instant::now(),
            created_instant: Instant::now(),
            flags: 0,
            qbuf: 0,
            qbuf_free: 0,
            watch_keys: 0,
            multi: -1,
        }));
        let kill_flag = Arc::new(AtomicBool::new(false));
        {
            let mut g = self.inner.write();
            g.insert(id, meta.clone());
        }
        {
            let mut g = self.kill_flags.write();
            g.insert(id, kill_flag.clone());
        }
        (meta, kill_flag)
    }

    /// Remove a connection from the registry. The caller drops the
    /// returned `Arc`s as a follow-up.
    pub fn unregister(&self, id: u64) {
        {
            let mut g = self.inner.write();
            g.remove(&id);
        }
        {
            let mut g = self.kill_flags.write();
            g.remove(&id);
        }
    }

    /// Snapshot of all currently-registered client metadata, in arbitrary
    /// order. Caller takes a read-lock on each meta to format a line.
    pub fn snapshot(&self) -> Vec<Arc<RwLock<ClientMeta>>> {
        let g = self.inner.read();
        g.values().cloned().collect()
    }

    /// Lookup a single client's meta by id.
    pub fn meta(&self, id: u64) -> Option<Arc<RwLock<ClientMeta>>> {
        self.inner.read().get(&id).cloned()
    }

    /// Mark a client for termination. The connection's outer loop polls
    /// its kill flag at the top of each iteration; the connection will
    /// exit on its next read.
    pub fn request_kill(&self, id: u64) -> bool {
        let g = self.kill_flags.read();
        if let Some(flag) = g.get(&id) {
            flag.store(true, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Set the `paused_until` deadline. After `Instant::now()`, writes are
    /// allowed again. `Duration::ZERO` means "no pause".
    pub fn pause_for(&self, dur: Duration) {
        if dur.is_zero() {
            *self.paused_until.write() = None;
        } else {
            *self.paused_until.write() = Some(Instant::now() + dur);
        }
    }

    pub fn unpause(&self) {
        *self.paused_until.write() = None;
    }

    /// True if a pause deadline is set and has not yet elapsed.
    pub fn is_paused(&self) -> bool {
        match *self.paused_until.read() {
            Some(deadline) => Instant::now() < deadline,
            None => false,
        }
    }
}

impl Default for ConnectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a `flags` bitfield to the single-character Redis encoding
/// used by `CLIENT LIST`'s `flags=` field. The flag-letter mapping is
/// canonical and stable upstream.
pub fn flags_letters(flags: u32) -> String {
    let mut s = String::new();
    if flags & CLIENT_FLAG_SLAVE != 0 {
        s.push('S');
    }
    if flags & CLIENT_FLAG_MASTER != 0 {
        s.push('M');
    }
    if flags & CLIENT_FLAG_PUBSUB != 0 {
        s.push('P');
    }
    if flags & CLIENT_FLAG_MULTI != 0 {
        s.push('x');
    }
    if flags & CLIENT_FLAG_TRACKING != 0 {
        s.push('t');
    }
    if flags & CLIENT_FLAG_BLOCKED != 0 {
        s.push('b');
    }
    if flags & CLIENT_FLAG_NO_EVICT != 0 {
        s.push('e');
    }
    if flags & CLIENT_FLAG_NO_TOUCH != 0 {
        s.push('u');
    }
    if s.is_empty() {
        s.push('N');
    }
    s
}

/// Format a single `CLIENT LIST` line for one meta. Field order matches
/// `redis-cli CLIENT LIST` exactly so existing tooling parses it
/// without surprises.
pub fn format_client_list_line(meta: &ClientMeta) -> String {
    use std::fmt::Write;
    let now = Instant::now();
    let age = now.duration_since(meta.created_instant).as_secs();
    let idle = now.duration_since(meta.idle_instant).as_secs();
    let name = if meta.name.is_empty() { "" } else { &meta.name };
    let last_cmd = if meta.last_cmd.is_empty() {
        "client"
    } else {
        &meta.last_cmd
    };
    let flags = flags_letters(meta.flags);

    let mut out = String::with_capacity(256);
    let _ = write!(
        out,
        "id={} addr={} laddr= fd=0 name={} age={} idle={} flags={} db={} sub={} psub={} multi={} watch={} qbuf={} qbuf-free={} argv-mem=0 multi-mem=0 tot-mem=0 rbs=16384 rbp=0 obl=0 oll=0 omem=0 events=r cmd={} user={} library-name= library-ver=",
        meta.id,
        meta.addr,
        name,
        age,
        idle,
        flags,
        meta.db_index,
        meta.subscriptions,
        meta.pattern_subscriptions,
        meta.multi,
        meta.watch_keys,
        meta.qbuf,
        meta.qbuf_free,
        last_cmd,
        meta.user,
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn fake_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379)
    }

    #[test]
    fn register_unregister_roundtrip() {
        let reg = ConnectionRegistry::new();
        let (m1, k1) = reg.register(1, fake_addr());
        assert!(m1.read().id == 1);
        assert!(!k1.load(Ordering::Acquire));
        assert!(reg.meta(1).is_some());

        reg.unregister(1);
        assert!(reg.meta(1).is_none());
    }

    #[test]
    fn snapshot_returns_all_meta() {
        let reg = ConnectionRegistry::new();
        reg.register(10, fake_addr());
        reg.register(20, fake_addr());
        reg.register(30, fake_addr());
        let snap = reg.snapshot();
        assert_eq!(snap.len(), 3);
    }

    #[test]
    fn request_kill_sets_flag() {
        let reg = ConnectionRegistry::new();
        let (_m, k) = reg.register(1, fake_addr());
        assert!(!k.load(Ordering::Acquire));
        assert!(reg.request_kill(1));
        assert!(k.load(Ordering::Acquire));
        // request_kill for an unknown id returns false.
        assert!(!reg.request_kill(999));
    }

    #[test]
    fn pause_unpause_roundtrip() {
        let reg = ConnectionRegistry::new();
        assert!(!reg.is_paused());
        reg.pause_for(Duration::from_millis(200));
        assert!(reg.is_paused());
        reg.unpause();
        assert!(!reg.is_paused());
    }

    #[test]
    fn pause_with_zero_duration_clears() {
        let reg = ConnectionRegistry::new();
        reg.pause_for(Duration::from_secs(60));
        reg.pause_for(Duration::ZERO);
        assert!(!reg.is_paused());
    }

    #[test]
    fn flags_letters_empty_is_n() {
        assert_eq!(flags_letters(0), "N");
        assert_eq!(flags_letters(CLIENT_FLAG_PUBSUB), "P");
        assert_eq!(flags_letters(CLIENT_FLAG_PUBSUB | CLIENT_FLAG_MULTI), "Px");
        assert_eq!(
            flags_letters(CLIENT_FLAG_TRACKING | CLIENT_FLAG_NO_EVICT | CLIENT_FLAG_BLOCKED),
            "tbe"
        );
    }

    #[test]
    fn format_line_includes_idle_age() {
        let reg = ConnectionRegistry::new();
        let (m, _k) = reg.register(7, fake_addr());
        {
            let mut g = m.write();
            g.name = "worker-1".to_string();
            g.last_cmd = "set".to_string();
            g.db_index = 2;
        }
        let line = format_client_list_line(&m.read());
        assert!(line.contains("id=7"));
        assert!(line.contains("addr=127.0.0.1:6379"));
        assert!(line.contains("name=worker-1"));
        assert!(line.contains("db=2"));
        assert!(line.contains("cmd=set"));
        assert!(line.contains("user=default"));
        assert!(line.contains("flags=N"));
        assert!(line.contains("age="));
        assert!(line.contains("idle="));
    }
}
