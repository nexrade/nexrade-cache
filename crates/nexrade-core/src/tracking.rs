//! CLIENT TRACKING — server-assisted client-side caching (Redis 6.2+).
//!
//! A client opts in with `CLIENT TRACKING ON`. From then on, every key it
//! reads is remembered by the server; when any client writes to that key,
//! an out-of-band "invalidate" push is sent back so the caching client can
//! evict its local copy. `BCAST` mode skips the per-key bookkeeping and
//! instead matches writes against a set of key prefixes.
//!
//! Delivery reuses the same "push frame on an out-of-band channel" model
//! the pub/sub implementation already uses: each connection registers an
//! mpsc sender at accept time, and the connection's main loop selects on
//! it alongside the socket read so pushes are flushed promptly rather than
//! only between requests.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;

/// A pending invalidation, delivered out-of-band to a tracking client's
/// connection.
#[derive(Debug, Clone)]
pub enum TrackingPush {
    /// One or more keys were invalidated.
    Keys(Vec<Vec<u8>>),
    /// The whole keyspace was flushed (FLUSHALL/FLUSHDB) — the client
    /// should discard its entire local cache.
    FlushAll,
}

/// Options accompanying `CLIENT TRACKING ON`.
#[derive(Debug, Clone, Default)]
pub struct TrackingOptions {
    pub bcast: bool,
    pub optin: bool,
    pub optout: bool,
    pub noloop: bool,
    /// Redirect invalidations to another client's connection (0 = self).
    pub redirect: Option<u64>,
    /// BCAST key prefixes; empty means "match every key".
    pub prefixes: Vec<Vec<u8>>,
}

struct ClientState {
    enabled: bool,
    opts: TrackingOptions,
    tx: mpsc::Sender<TrackingPush>,
    /// Set by `CLIENT CACHING YES|NO`; consumed by the very next dispatched
    /// command (read or write), matching Redis's "affects the next
    /// command" semantics for OPTIN/OPTOUT mode.
    caching_override: Option<bool>,
}

/// Server-wide tracking registry. Cloning is cheap (Arc-internal).
#[derive(Clone)]
pub struct TrackingRegistry {
    inner: Arc<RwLock<Inner>>,
    /// Number of clients with tracking currently enabled. Mirrors the
    /// count of `state.enabled == true` entries in `inner.clients` so
    /// `on_write`/`track_read` — called on every write/read command,
    /// including from clients that never touch `CLIENT TRACKING` — can
    /// skip the registry lock entirely with a single relaxed atomic load
    /// when nobody has tracking enabled (the overwhelmingly common case).
    /// Same pattern as the `is_replica`/`propagate_subscribers` atomic
    /// mirrors elsewhere in this codebase: a real lock backs the source of
    /// truth, this is just a fast-path hint that's always safe to
    /// under-trust for one extra command (worst case: one stale lock
    /// acquire that finds nothing to do).
    enabled_count: Arc<AtomicUsize>,
}

struct Inner {
    clients: HashMap<u64, ClientState>,
    /// key -> client ids tracking it (non-BCAST mode only).
    key_index: HashMap<Vec<u8>, HashSet<u64>>,
}

impl TrackingRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                clients: HashMap::new(),
                key_index: HashMap::new(),
            })),
            enabled_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Register a connection's push channel. Call once per connection at
    /// accept time (tracking starts disabled).
    pub fn register(&self, client_id: u64, tx: mpsc::Sender<TrackingPush>) {
        self.inner.write().clients.insert(
            client_id,
            ClientState {
                enabled: false,
                opts: TrackingOptions::default(),
                tx,
                caching_override: None,
            },
        );
    }

    /// Deregister on disconnect — drops the client from every tracked key.
    pub fn unregister(&self, client_id: u64) {
        let mut g = self.inner.write();
        if let Some(state) = g.clients.remove(&client_id) {
            if state.enabled {
                self.enabled_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        // `retain` drops the key entry entirely once its set is empty,
        // instead of leaving a permanent empty-set behind — otherwise any
        // key this client ever read (and nobody subsequently wrote) leaks
        // in `key_index` for the life of the server.
        g.key_index.retain(|_, set| {
            set.remove(&client_id);
            !set.is_empty()
        });
    }

    pub fn exists(&self, client_id: u64) -> bool {
        self.inner.read().clients.contains_key(&client_id)
    }

    /// Number of keys currently tracked in `key_index`. Test-only —
    /// exposed to assert that `disable`/`unregister` don't leak empty-set
    /// entries (see the regression tests below).
    #[cfg(test)]
    fn key_index_len(&self) -> usize {
        self.inner.read().key_index.len()
    }

    /// `CLIENT TRACKING ON ...`.
    pub fn enable(&self, client_id: u64, opts: TrackingOptions) -> Result<(), &'static str> {
        let mut g = self.inner.write();
        let Some(state) = g.clients.get_mut(&client_id) else {
            return Err("client not registered");
        };
        if !state.enabled {
            self.enabled_count.fetch_add(1, Ordering::Relaxed);
        }
        state.enabled = true;
        state.opts = opts;
        Ok(())
    }

    /// `CLIENT TRACKING OFF`.
    pub fn disable(&self, client_id: u64) {
        let mut g = self.inner.write();
        if let Some(state) = g.clients.get_mut(&client_id) {
            if state.enabled {
                self.enabled_count.fetch_sub(1, Ordering::Relaxed);
            }
            state.enabled = false;
            state.opts = TrackingOptions::default();
        }
        // See `unregister`'s comment — `retain` drops now-empty key
        // entries instead of leaking them.
        g.key_index.retain(|_, set| {
            set.remove(&client_id);
            !set.is_empty()
        });
    }

    pub fn is_enabled(&self, client_id: u64) -> bool {
        self.inner
            .read()
            .clients
            .get(&client_id)
            .map(|s| s.enabled)
            .unwrap_or(false)
    }

    pub fn options(&self, client_id: u64) -> Option<TrackingOptions> {
        self.inner
            .read()
            .clients
            .get(&client_id)
            .filter(|s| s.enabled)
            .map(|s| s.opts.clone())
    }

    /// `CLIENT CACHING YES|NO` — applies to the very next command only,
    /// under OPTIN/OPTOUT tracking mode.
    pub fn set_caching_override(&self, client_id: u64, yes: bool) {
        if let Some(state) = self.inner.write().clients.get_mut(&client_id) {
            state.caching_override = Some(yes);
        }
    }

    /// Record that `client_id` read `keys` — call after a successful read
    /// when tracking applies. No-op for BCAST clients (they don't need
    /// per-key bookkeeping) or when OPTIN/OPTOUT mode excludes this read.
    /// Consumes any pending `CLIENT CACHING` override.
    pub fn track_read(&self, client_id: u64, keys: &[&[u8]]) {
        if keys.is_empty() || self.enabled_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut g = self.inner.write();
        let Some(state) = g.clients.get_mut(&client_id) else {
            return;
        };
        if !state.enabled || state.opts.bcast {
            return;
        }
        let caching_override = state.caching_override.take();
        let should_track = if state.opts.optin {
            caching_override.unwrap_or(false)
        } else if state.opts.optout {
            caching_override.unwrap_or(true)
        } else {
            true
        };
        if !should_track {
            return;
        }
        for k in keys {
            g.key_index.entry(k.to_vec()).or_default().insert(client_id);
        }
    }

    /// Called after a successful write to `keys` — notify every client
    /// tracking any of them (one-shot: a key is dropped from tracking
    /// once invalidated, matching Redis — the client must re-read to
    /// re-arm), plus any BCAST clients whose prefix matches.
    pub fn on_write(&self, keys: &[&[u8]], writer_client_id: u64) {
        if keys.is_empty() || self.enabled_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut targets: HashSet<u64> = HashSet::new();
        {
            let mut g = self.inner.write();
            for k in keys {
                if let Some(set) = g.key_index.remove(k.as_ref()) {
                    targets.extend(set);
                }
            }
            for (id, state) in g.clients.iter() {
                if !state.enabled || !state.opts.bcast {
                    continue;
                }
                let matches = state.opts.prefixes.is_empty()
                    || keys.iter().any(|k| {
                        state
                            .opts
                            .prefixes
                            .iter()
                            .any(|p| k.starts_with(p.as_slice()))
                    });
                if matches {
                    targets.insert(*id);
                }
            }
        }
        if targets.is_empty() {
            return;
        }
        let owned_keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        let g = self.inner.read();
        for id in targets {
            let Some(state) = g.clients.get(&id) else {
                continue;
            };
            if id == writer_client_id && state.opts.noloop {
                continue;
            }
            let dest = state.opts.redirect.unwrap_or(id);
            let Some(dest_state) = g.clients.get(&dest) else {
                continue;
            };
            let _ = dest_state
                .tx
                .try_send(TrackingPush::Keys(owned_keys.clone()));
        }
    }

    /// FLUSHALL/FLUSHDB — notify every enabled tracking client and drop
    /// all per-key bookkeeping.
    pub fn flush_all(&self) {
        {
            let g = self.inner.read();
            for state in g.clients.values() {
                if !state.enabled {
                    continue;
                }
                let dest = state.opts.redirect.unwrap_or_default();
                let dest_state = if dest != 0 {
                    g.clients.get(&dest)
                } else {
                    Some(state)
                };
                if let Some(dest_state) = dest_state {
                    let _ = dest_state.tx.try_send(TrackingPush::FlushAll);
                }
            }
        }
        self.inner.write().key_index.clear();
    }
}

impl Default for TrackingRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_then_write_invalidates() {
        let reg = TrackingRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(1, TrackingOptions::default()).unwrap();
        reg.track_read(1, &[b"foo"]);
        reg.on_write(&[b"foo"], 999);
        let push = rx.try_recv().unwrap();
        assert!(matches!(push, TrackingPush::Keys(k) if k == vec![b"foo".to_vec()]));
    }

    #[test]
    fn untracked_key_write_does_not_notify() {
        let reg = TrackingRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(1, TrackingOptions::default()).unwrap();
        // No track_read call — this key was never read.
        reg.on_write(&[b"foo"], 999);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn noloop_skips_self_notification() {
        let reg = TrackingRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(
            1,
            TrackingOptions {
                noloop: true,
                ..Default::default()
            },
        )
        .unwrap();
        reg.track_read(1, &[b"foo"]);
        // Client 1 itself performs the write.
        reg.on_write(&[b"foo"], 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn bcast_matches_prefix_without_read() {
        let reg = TrackingRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(
            1,
            TrackingOptions {
                bcast: true,
                prefixes: vec![b"user:".to_vec()],
                ..Default::default()
            },
        )
        .unwrap();
        // No read needed for BCAST.
        reg.on_write(&[b"user:1"], 999);
        let push = rx.try_recv().unwrap();
        assert!(matches!(push, TrackingPush::Keys(_)));

        // Non-matching prefix: no push.
        reg.on_write(&[b"session:1"], 999);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn redirect_delivers_to_target() {
        let reg = TrackingRegistry::new();
        let (tx1, mut rx1) = mpsc::channel(8);
        let (tx2, mut rx2) = mpsc::channel(8);
        reg.register(1, tx1);
        reg.register(2, tx2);
        reg.enable(
            1,
            TrackingOptions {
                redirect: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
        reg.track_read(1, &[b"foo"]);
        reg.on_write(&[b"foo"], 999);
        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn flush_all_notifies_and_clears_index() {
        let reg = TrackingRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(1, TrackingOptions::default()).unwrap();
        reg.track_read(1, &[b"foo"]);
        reg.flush_all();
        assert!(matches!(rx.try_recv().unwrap(), TrackingPush::FlushAll));
        // Index cleared — a write to "foo" now should not notify again.
        reg.on_write(&[b"foo"], 999);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn optin_only_tracks_after_caching_yes() {
        let reg = TrackingRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(
            1,
            TrackingOptions {
                optin: true,
                ..Default::default()
            },
        )
        .unwrap();
        // Without CLIENT CACHING YES, the read isn't tracked.
        reg.track_read(1, &[b"foo"]);
        reg.on_write(&[b"foo"], 999);
        assert!(rx.try_recv().is_err());

        // With the override set, the next read is tracked.
        reg.set_caching_override(1, true);
        reg.track_read(1, &[b"bar"]);
        reg.on_write(&[b"bar"], 999);
        assert!(rx.try_recv().is_ok());

        // Override is consumed — a third read goes back to untracked.
        reg.track_read(1, &[b"baz"]);
        reg.on_write(&[b"baz"], 999);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn optout_tracks_unless_caching_no() {
        let reg = TrackingRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(
            1,
            TrackingOptions {
                optout: true,
                ..Default::default()
            },
        )
        .unwrap();
        // Default (no override): tracked.
        reg.track_read(1, &[b"foo"]);
        reg.on_write(&[b"foo"], 999);
        assert!(rx.try_recv().is_ok());

        // With CLIENT CACHING NO: not tracked.
        reg.set_caching_override(1, false);
        reg.track_read(1, &[b"bar"]);
        reg.on_write(&[b"bar"], 999);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn disable_stops_future_reads_from_being_tracked() {
        let reg = TrackingRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(1, TrackingOptions::default()).unwrap();
        reg.disable(1);
        reg.track_read(1, &[b"foo"]);
        reg.on_write(&[b"foo"], 999);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn disable_without_a_write_does_not_leak_key_index_entry() {
        // Regression test: a client reads a key, then disables tracking
        // (or disconnects) without that key ever being written. Before the
        // fix, `disable`/`unregister` only removed the client id from each
        // key's HashSet, leaving a permanent empty-set entry behind.
        let reg = TrackingRegistry::new();
        let (tx, _rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(1, TrackingOptions::default()).unwrap();
        reg.track_read(1, &[b"foo", b"bar"]);
        assert_eq!(reg.key_index_len(), 2, "both keys should be tracked");

        reg.disable(1);
        assert_eq!(
            reg.key_index_len(),
            0,
            "key_index must not retain empty-set entries after disable()"
        );
    }

    #[test]
    fn unregister_without_a_write_does_not_leak_key_index_entry() {
        // Same scenario as above, but for the disconnect path.
        let reg = TrackingRegistry::new();
        let (tx, _rx) = mpsc::channel(8);
        reg.register(1, tx);
        reg.enable(1, TrackingOptions::default()).unwrap();
        reg.track_read(1, &[b"foo"]);
        assert_eq!(reg.key_index_len(), 1);

        reg.unregister(1);
        assert_eq!(
            reg.key_index_len(),
            0,
            "key_index must not retain empty-set entries after unregister()"
        );
    }

    #[test]
    fn disable_only_drops_entries_that_become_empty() {
        // A key tracked by two clients must survive one of them disabling —
        // only the entry should shrink, not disappear, until the last
        // tracking client is gone.
        let reg = TrackingRegistry::new();
        let (tx1, _rx1) = mpsc::channel(8);
        let (tx2, mut rx2) = mpsc::channel(8);
        reg.register(1, tx1);
        reg.register(2, tx2);
        reg.enable(1, TrackingOptions::default()).unwrap();
        reg.enable(2, TrackingOptions::default()).unwrap();
        reg.track_read(1, &[b"shared"]);
        reg.track_read(2, &[b"shared"]);
        assert_eq!(reg.key_index_len(), 1);

        reg.disable(1);
        assert_eq!(
            reg.key_index_len(),
            1,
            "entry must survive while client 2 still tracks it"
        );
        reg.on_write(&[b"shared"], 999);
        assert!(rx2.try_recv().is_ok(), "client 2 should still be notified");
    }
}
