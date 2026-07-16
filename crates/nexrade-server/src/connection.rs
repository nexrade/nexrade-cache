//! Individual client connection handler.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use crate::stream::Stream;

use nexrade_core::command::{dispatch_tracked, dispatch_with_user, is_write_command};
use nexrade_core::conn_registry::{
    ClientMeta, CLIENT_FLAG_MULTI, CLIENT_FLAG_PUBSUB, CLIENT_FLAG_TRACKING,
};
use nexrade_core::db::Db;
use nexrade_core::error::NexradeError;
use nexrade_core::persistence::Snapshot;
use nexrade_core::pubsub::Message;
use nexrade_core::replication::ReplicationRole;
use nexrade_core::resp::{Resp, RespParser, SegBuf};
use nexrade_core::tracking::{TrackingOptions, TrackingPush};
use nexrade_core::transaction::Transaction;
use nexrade_lua::{FunctionRegistry, LuaEngine};
use nexrade_metrics::Metrics;

/// Outcome of a single socket read, used by `read_with_timeout` so the
/// caller's `tokio::select!` arm can match without nested `match`es.
enum ReadOutcome {
    Data(usize),
    Closed,
    Error(std::io::Error),
    IdleTimeout,
}

/// Read from `stream` into `buf`, honoring an optional idle timeout.
/// Factored out of the main loop so it can be raced against the CLIENT
/// TRACKING push channel via `tokio::select!`.
async fn read_with_timeout(
    stream: &mut Stream,
    buf: &mut BytesMut,
    idle_timeout: Option<Duration>,
) -> ReadOutcome {
    let read = stream.read_buf(buf);
    let result = match idle_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, read).await {
            Ok(r) => r,
            Err(_) => return ReadOutcome::IdleTimeout,
        },
        None => read.await,
    };
    match result {
        Ok(0) => ReadOutcome::Closed,
        Ok(n) => ReadOutcome::Data(n),
        Err(e) => ReadOutcome::Error(e),
    }
}

/// Ensures per-connection registry/stat cleanup happens exactly once, on
/// any exit path out of `Connection::run()` — normal return, an early
/// `break`, or a panic unwinding through this task. Before this guard
/// existed, the cleanup was a plain statement block after the main loop,
/// which a panic would skip entirely, leaking a phantom `CLIENT LIST`
/// entry, a permanently-inflated `active_connections` counter, and a dead
/// `TrackingRegistry` entry for the panicked connection. Only viable
/// because the release profile is `panic = "unwind"` (see `Cargo.toml`) —
/// under `panic = "abort"` nothing unwinds far enough to run `Drop` impls
/// at all, so this guard would be a no-op safety net in that configuration.
struct ConnectionCleanup {
    db: Db,
    client_id: u64,
    metrics: Option<Metrics>,
}

impl Drop for ConnectionCleanup {
    fn drop(&mut self) {
        self.db.tracking.unregister(self.client_id);
        self.db.connections.unregister(self.client_id);
        self.db.stats.disconnect();
        if let Some(ref m) = self.metrics {
            m.record_connection(false);
        }
    }
}

/// State for a single connected client.
pub struct Connection {
    db: Db,
    stream: Stream,
    addr: SocketAddr,
    db_index: usize,
    authenticated: bool,
    /// User the connection authenticated as (after AUTH). Defaults to
    /// `"default"` for unauthenticated connections, used for ACL checks.
    authenticated_user: String,
    parser: RespParser,
    read_buf: BytesMut,
    write_buf: SegBuf,
    transaction: Transaction,
    subscriptions: Vec<Vec<u8>>,
    /// Pattern subscriptions (PSUBSCRIBE) — tracked separately from
    /// exact subscriptions so `CLIENT LIST`'s `psub=N` field is correct.
    pattern_subscriptions: Vec<Vec<u8>>,
    client_id: u64,
    /// Live per-connection metadata, registered into `db.connections` at
    /// connect time. Used to drive `CLIENT LIST`/`CLIENT INFO` output and
    /// to update `last_cmd`/`idle_instant`/`flags` on every command.
    meta: Option<Arc<parking_lot::RwLock<ClientMeta>>>,
    /// Set by `CLIENT KILL`. The outer loop polls this at the top of each
    /// iteration and exits cleanly when it's true.
    kill_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Sender half of the pub/sub relay channel (cloned into relay tasks).
    /// Bounded to 256 so a slow client cannot accumulate unbounded memory.
    pubsub_tx: mpsc::Sender<Message>,
    /// Receiver half — polled in subscribe mode.
    pubsub_rx: mpsc::Receiver<Message>,
    /// Receiver half of the CLIENT TRACKING invalidation channel — this
    /// connection is registered in `db.tracking` under `client_id` at
    /// connect time (see `Connection::new`) and unregistered on drop.
    tracking_rx: mpsc::Receiver<TrackingPush>,
    /// RESP protocol version negotiated via HELLO (2 or 3).
    resp_version: u8,
    /// Shared Lua scripting engine (ScriptCache is Arc-backed, safe to clone).
    lua: LuaEngine,
    /// Shared FUNCTION library registry (Arc-backed, safe to clone).
    functions: FunctionRegistry,
    metrics: Option<Metrics>,
    /// Cached `Metrics::handles_for(cmd)` result for the most recently
    /// recorded command name, so a run of same-named commands (the norm
    /// under pipelining — e.g. redis-benchmark sends batches of one
    /// command) skips `MetricVec::with_label_values`'s per-call FNV hash +
    /// `RwLock::read()` + `HashMap` lookup on every repeat. `None` until
    /// the first command is recorded, or whenever `metrics` is `None`.
    cached_handles: Option<(String, nexrade_metrics::CommandMetricHandles)>,
    /// Last command dispatched by this connection, captured per-command
    /// but flushed to the per-connection `ClientMeta` only once per
    /// pipeline batch. Used by `CLIENT LIST`/`INFO` to populate `cmd=`.
    /// Reused across commands to avoid per-call `String` allocations.
    last_cmd: String,
    /// Last response sent by this connection — exposed via `last_write_instant`
    /// in the registry for idle-time accounting. Updated per-batch.
    last_write_instant: std::time::Instant,
}

impl Connection {
    pub fn new(
        db: Db,
        stream: Stream,
        addr: SocketAddr,
        lua: LuaEngine,
        functions: FunctionRegistry,
        metrics: Option<Metrics>,
    ) -> Self {
        let requires_auth = db.config.lock().requirepass.is_some();
        let client_id = db.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (pubsub_tx, pubsub_rx) = mpsc::channel(256);
        let (tracking_tx, tracking_rx) = mpsc::channel(256);
        db.tracking.register(client_id, tracking_tx);
        let (meta, kill_flag) = db.connections.register(client_id, addr);
        Self {
            db,
            stream,
            addr,
            db_index: 0,
            authenticated: !requires_auth,
            authenticated_user: "default".to_string(),
            parser: RespParser::new(),
            read_buf: BytesMut::with_capacity(4096),
            write_buf: SegBuf::with_capacity(4096),
            transaction: Transaction::new(),
            subscriptions: Vec::new(),
            pattern_subscriptions: Vec::new(),
            client_id,
            meta: Some(meta),
            kill_flag: Some(kill_flag),
            pubsub_tx,
            pubsub_rx,
            tracking_rx,
            lua,
            functions,
            metrics,
            cached_handles: None,
            resp_version: 2,
            last_cmd: String::with_capacity(8),
            last_write_instant: Instant::now(),
        }
    }

    /// Main connection loop.
    pub async fn run(mut self) {
        self.db.stats.connect();
        if let Some(ref m) = self.metrics {
            m.record_connection(true);
        }
        info!("new connection from {}", self.addr);

        // Guarantees the registry/stat cleanup below runs exactly once on
        // every exit path — normal return, an early `break`, or a panic
        // unwinding through this task (relies on the release profile being
        // `panic = "unwind"`, see Cargo.toml). Without this, a panic
        // anywhere in the loop below would skip the cleanup entirely,
        // leaking a phantom `CLIENT LIST` entry, a permanently-inflated
        // `active_connections` counter, and a dead tracking-registry
        // entry, one per panicked connection.
        let _cleanup = ConnectionCleanup {
            db: self.db.clone(),
            client_id: self.client_id,
            metrics: self.metrics.clone(),
        };

        let idle_timeout = {
            let t = self.db.config.lock().timeout;
            if t > 0 {
                Some(Duration::from_secs(t))
            } else {
                None
            }
        };

        'outer: loop {
            // Check whether `CLIENT KILL` has marked this connection for
            // termination. Atomic load, very cheap; matches Redis's
            // best-effort semantics.
            if self
                .kill_flag
                .as_ref()
                .is_some_and(|f| f.load(Ordering::Acquire))
            {
                debug!("client {} killed via CLIENT KILL", self.client_id);
                break;
            }

            // Read more data — with optional idle timeout. Also select on
            // the CLIENT TRACKING invalidation channel so pushes reach the
            // client promptly even while it's idle between requests,
            // rather than only being flushed after its next command.
            let n = tokio::select! {
                biased;
                push = self.tracking_rx.recv() => {
                    if let Some(push) = push {
                        self.write_tracking_push(push).await;
                    }
                    continue 'outer;
                }
                // Wake periodically to check whether CLIENT KILL has
                // requested termination. Without this arm, an idle
                // connection wouldn't notice a kill until the next
                // read returns data — which can be "never" if the
                // connection is genuinely idle. 50ms is a reasonable
                // latency / wakeup-rate tradeoff.
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                    if self.kill_flag.as_ref()
                        .is_some_and(|f| f.load(Ordering::Acquire))
                    {
                        debug!("client {} killed via CLIENT KILL (timer tick)", self.client_id);
                        break;
                    }
                    continue 'outer;
                }
                read_result = read_with_timeout(&mut self.stream, &mut self.read_buf, idle_timeout) => {
                    match read_result {
                        ReadOutcome::Data(n) => n,
                        ReadOutcome::Closed => {
                            debug!("connection closed by {}", self.addr);
                            break;
                        }
                        ReadOutcome::Error(e) => {
                            error!("read error from {}: {}", self.addr, e);
                            break;
                        }
                        ReadOutcome::IdleTimeout => {
                            debug!("idle timeout for {}", self.addr);
                            break;
                        }
                    }
                }
            };

            trace!("read {} bytes from {}", n, self.addr);
            self.parser.feed(&self.read_buf[self.read_buf.len() - n..]);
            self.read_buf.clear();

            // ── Pipeline: parse all available commands, accumulate responses ──
            // Write the entire batch in a single write_all at the end.
            self.write_buf.clear();
            let mut quit = false;
            let mut subscribe_args: Option<(Vec<Resp>, bool)> = None;
            let mut had_commands = false;

            'inner: loop {
                let resp = match self.parser.parse_one() {
                    Ok(Some(r)) => r,
                    Ok(None) => break 'inner,
                    Err(e) => {
                        Resp::error(e.to_string()).write_to(&mut self.write_buf);
                        quit = true;
                        break 'inner;
                    }
                };

                let args = match resp {
                    Resp::Array(Some(args)) => args,
                    _ => {
                        Resp::error("protocol error: expected array").write_to(&mut self.write_buf);
                        continue;
                    }
                };

                if args.is_empty() {
                    continue;
                }

                let cmd_name = if let Some(src) = args[0].as_str() {
                    // Inlined uppercase conversion into a small stack-only
                    // buffer. Owned `String` (not borrowed from `self.cmd_buf`)
                    // so subsequent `&mut self` calls don't conflict.
                    // Cmd names are short (<16 bytes typical); the
                    // per-command `String` allocation is dwarfed by the
                    // savings from no longer re-parsing the cmd name
                    // inside dispatch.
                    let mut out = String::with_capacity(src.len().max(8));
                    out.push_str(src);
                    out.make_ascii_uppercase();
                    out
                } else {
                    String::new()
                };

                // SUBSCRIBE takes over the connection — flush current batch first.
                if (cmd_name == "SUBSCRIBE" || cmd_name == "PSUBSCRIBE") && self.authenticated {
                    self.db.stats.record_command();
                    subscribe_args = Some((args, cmd_name == "PSUBSCRIBE"));
                    break 'inner;
                }

                // PSYNC takes over the connection — flush current batch and enter
                // replica-streaming mode.
                if cmd_name == "PSYNC" && self.authenticated {
                    self.db.stats.record_command();
                    // Flush any pending output first.
                    self.write_buf.finalize();
                    if !self.write_buf.is_empty() {
                        if let Err(e) = self.stream.write_all_buf(&mut self.write_buf).await {
                            error!("write error to {}: {}", self.addr, e);
                            break 'outer;
                        }
                        self.write_buf.clear();
                    }
                    self.run_psync_mode().await;
                    break 'outer;
                }

                let start = Instant::now();
                // Capture just the arg count — `args` itself is moved
                // into dispatch below, but for slowlog we only need
                // the length.
                let args_count = args.len();
                // Must be computed before `args` moves into dispatch below —
                // see `resp3_should_nest_pairs` doc comment.
                let resp3_nest_pairs = Connection::resp3_should_nest_pairs(&cmd_name, &args);

                // Check read-only enforcement for replicas. Uses the
                // lock-free atomic mirror (`is_replica_fast`) instead of
                // `is_replica()`'s RwLockReadGuard — this runs on every
                // command in the hot pipeline loop.
                let is_replica = self.db.replication.is_replica_fast();
                let response = if cmd_name == "AUTH" {
                    self.handle_auth(&args).await
                } else if !self.authenticated {
                    Resp::error("NOAUTH Authentication required")
                } else if is_replica && is_write_command(&cmd_name) {
                    Resp::Error(NexradeError::ReadOnly.to_string())
                } else if cmd_name == "SELECT" {
                    self.handle_select(&args)
                } else if cmd_name == "CLIENT" {
                    // Only TRACKING/CACHING/TRACKINGINFO need direct
                    // access to the connection handler. Everything else
                    // (LIST/INFO/KILL/PAUSE/UNPAUSE/SETNAME/GETNAME/ID/
                    // HELP/NO-EVICT/REPLY) flows through normal dispatch
                    // into `cmd_client`, which has access to
                    // `db.connections`.
                    let sub = args
                        .get(1)
                        .and_then(|a| a.as_str())
                        .unwrap_or("")
                        .to_ascii_uppercase();
                    match sub.as_str() {
                        "TRACKING" => self.handle_client_tracking(&args),
                        "CACHING" => self.handle_client_caching(&args),
                        "TRACKINGINFO" => self.client_trackinginfo(),
                        _ => {
                            // SETNAME mutates per-connection state
                            // outside the registry — capture the name
                            // before dispatch consumes `args`, then sync
                            // it through the meta. `cmd_client` itself
                            // also updates meta; this is a belt-and-
                            // suspenders sync in case `cmd_client`'s
                            // path bypasses the meta writer.
                            let setname_value: Option<String> = if sub == "SETNAME" {
                                args.get(2).and_then(|a| a.as_str()).map(str::to_string)
                            } else {
                                None
                            };
                            let resp = dispatch_tracked(
                                &self.db,
                                args,
                                self.db_index,
                                Some(self.addr),
                                &self.authenticated_user,
                                self.client_id,
                                &cmd_name,
                            )
                            .await;
                            if let Some(name) = setname_value {
                                if let Some(m) = self.meta.as_ref() {
                                    m.write().name = name;
                                }
                            }
                            resp
                        }
                    }
                } else if cmd_name == "UNSUBSCRIBE" || cmd_name == "PUNSUBSCRIBE" {
                    Resp::array(vec![
                        Resp::bulk_str("unsubscribe"),
                        Resp::null(),
                        Resp::int(0),
                    ])
                } else if cmd_name == "HELLO" {
                    self.handle_hello(&args)
                } else if cmd_name == "WATCH" {
                    self.handle_watch(&args)
                } else if cmd_name == "UNWATCH" {
                    self.handle_unwatch()
                } else if cmd_name == "MULTI" {
                    self.handle_multi()
                } else if cmd_name == "EXEC" {
                    self.handle_exec().await
                } else if cmd_name == "DISCARD" {
                    self.handle_discard()
                } else if cmd_name == "QUIT" {
                    quit = true;
                    Resp::ok()
                } else if cmd_name == "EVAL" {
                    self.handle_eval(&args).await
                } else if cmd_name == "EVALSHA" {
                    self.handle_evalsha(&args).await
                } else if cmd_name == "SCRIPT" {
                    self.handle_script(&args)
                } else if cmd_name == "FUNCTION" {
                    self.handle_function(&args)
                } else if cmd_name == "FCALL" || cmd_name == "FCALL_RO" {
                    self.handle_fcall(&args).await
                } else if self.transaction.active {
                    self.transaction.queue(args);
                    Resp::SimpleString("QUEUED".to_string())
                } else {
                    self.db.stats.record_command();
                    // Serialize the command for replication propagation before args
                    // are consumed.
                    let repl_bytes: Option<bytes::Bytes> = if is_write_command(&cmd_name)
                        && self.db.replication.propagate_tx.is_some()
                        && self.db.replication.propagate_subscriber_count() > 0
                    {
                        Some(Resp::Array(Some(args.clone())).serialize())
                    } else {
                        None
                    };

                    let resp = dispatch_tracked(
                        &self.db,
                        args,
                        self.db_index,
                        Some(self.addr),
                        &self.authenticated_user,
                        self.client_id,
                        &cmd_name,
                    )
                    .await;

                    // Propagate to replicas if write succeeded.
                    if !matches!(resp, Resp::Error(_)) {
                        if let Some(cmd_bytes) = repl_bytes {
                            if let Some(ref tx) = self.db.replication.propagate_tx {
                                let len = cmd_bytes.len() as u64;
                                if tx.send(cmd_bytes).is_ok() {
                                    self.db
                                        .replication
                                        .replication_offset
                                        .fetch_add(len, std::sync::atomic::Ordering::Release);
                                }
                            }
                        }
                    }

                    resp
                };

                // Update per-connection metadata after every command so
                // CLIENT LIST/INFO reflect fresh state.
                self.record_last_cmd(&cmd_name);
                had_commands = true;

                // Record metrics and slow log.
                let elapsed = start.elapsed();
                if let Some(ref m) = self.metrics {
                    let is_error = matches!(&response, Resp::Error(_));
                    // Cache the per-command metric handles across
                    // consecutive same-named commands (the common case in
                    // a pipelined batch) so we pay `with_label_values`'s
                    // FNV hash + lock + HashMap lookup once per distinct
                    // command name instead of on every single command.
                    let handles = match self.cached_handles.as_ref() {
                        Some((name, h)) if name == &cmd_name => h,
                        _ => {
                            let h = m.handles_for(&cmd_name);
                            self.cached_handles = Some((cmd_name.clone(), h));
                            &self.cached_handles.as_ref().unwrap().1
                        }
                    };
                    Metrics::record_with_handles(handles, elapsed.as_secs_f64(), is_error);
                }
                let sl = &self.db.slowlog;
                if elapsed.as_micros() as u64 >= sl.threshold_us() {
                    // Slow path: `args` was moved into dispatch above and
                    // already dropped, so we synthesize a minimal
                    // representation from the parts we still have.
                    // Slowlog's purpose is post-hoc debugging — the
                    // full arg payload is dispensable for the 99% case.
                    let sl_args = vec![cmd_name.clone(), format!("({} args)", args_count)];
                    sl.record(elapsed, sl_args, self.addr.to_string());
                }

                let response = Connection::upgrade_to_resp3(
                    &cmd_name,
                    response,
                    self.resp_version,
                    resp3_nest_pairs,
                );

                response.write_to_for_version(&mut self.write_buf, self.resp_version);

                if quit {
                    break 'inner;
                }
            }

            // One syscall for the whole pipeline batch.
            self.write_buf.finalize();
            if !self.write_buf.is_empty() {
                if let Err(e) = self.stream.write_all_buf(&mut self.write_buf).await {
                    error!("write error to {}: {}", self.addr, e);
                    break 'outer;
                }
            }

            // Flush per-connection metadata once per batch (cheap). If
            // the batch was empty, nothing to flush.
            if had_commands {
                self.refresh_meta_after_batch();
            }

            if quit {
                break 'outer;
            }

            if let Some((args, is_pattern)) = subscribe_args {
                self.do_subscribe(&args, is_pattern).await;
                self.run_subscribe_mode().await;
                break 'outer;
            }
        }

        // `_cleanup`'s Drop performs unregister/disconnect bookkeeping.
        info!("connection closed: {}", self.addr);
    }

    // ── Authentication ────────────────────────────────────────────────────────

    async fn handle_auth(&mut self, args: &[Resp]) -> Resp {
        // AUTH <password>                    (legacy single-password)
        // AUTH <user> <password>              (ACL form, Redis 6+)
        let user = args.get(1).and_then(|a| a.as_str()).unwrap_or("default");
        let pass = if args.len() >= 3 {
            args.get(2).and_then(|a| a.as_str()).unwrap_or("")
        } else {
            args.get(1).and_then(|a| a.as_str()).unwrap_or("")
        };

        // First, try the ACL form. If ACL doesn't have the user (e.g.
        // `default` with no password), fall back to legacy `requirepass`.
        match self.db.acl.authenticate(user, pass) {
            Ok(()) => {
                self.authenticated = true;
                self.authenticated_user = user.to_string();
                if let Some(m) = self.meta.as_ref() {
                    m.write().user = user.to_string();
                }
                return Resp::ok();
            }
            Err(_) => {
                // Fall through to legacy single-password check.
            }
        }

        // Legacy path: global requirepass. Used when ACL has no user / no
        // password for the requested user.
        let config = self.db.config.lock();
        match &config.requirepass {
            None => {
                // If ACL has the user but auth failed, surface that error.
                if self.db.acl.get_user(user).is_some() {
                    Resp::error("WRONGPASS invalid username-password pair or user is disabled")
                } else {
                    Resp::error("ERR Client sent AUTH, but no password is set")
                }
            }
            Some(expected) => {
                // AUTH <pass>           (legacy): user is "default".
                // AUTH <user> <pass>     (ACL form): user must exist; pass
                //                         must match legacy pass when no ACL
                //                         password is configured.
                if pass == expected.as_str() {
                    self.authenticated = true;
                    self.authenticated_user = user.to_string();
                    if let Some(m) = self.meta.as_ref() {
                        m.write().user = user.to_string();
                    }
                    Resp::ok()
                } else {
                    Resp::error("WRONGPASS invalid username-password pair or user is disabled")
                }
            }
        }
    }

    // ── SELECT ────────────────────────────────────────────────────────────────

    fn handle_select(&mut self, args: &[Resp]) -> Resp {
        let idx_str = args.get(1).and_then(|a| a.as_str()).unwrap_or("0");
        match idx_str.parse::<usize>() {
            Ok(idx) if idx < self.db.db_count() => {
                self.db_index = idx;
                if let Some(m) = self.meta.as_ref() {
                    m.write().db_index = idx;
                }
                Resp::ok()
            }
            Ok(_) => Resp::error("ERR DB index is out of range"),
            Err(_) => Resp::error("ERR value is not an integer or out of range"),
        }
    }

    /// Per-command: just record the cmd name into the connection's
    /// reusable `last_cmd` buffer. No locks. Called inside the inner
    /// pipelined loop on every command.
    #[inline]
    fn record_last_cmd(&mut self, cmd_name: &str) {
        self.last_cmd.clear();
        self.last_cmd.push_str(cmd_name);
    }

    /// Once per pipeline batch: copy the gathered state into the
    /// per-connection `ClientMeta`. This is the only point that takes
    /// the meta write-lock. Replaces the per-command refresh that
    /// previously dominated pipelined throughput.
    fn refresh_meta_after_batch(&mut self) {
        let Some(meta) = self.meta.as_ref() else {
            return;
        };
        let now = std::time::Instant::now();
        let tracking_enabled = self.db.tracking.is_enabled(self.client_id);
        let user = self.authenticated_user.clone();
        let subscriptions = self.subscriptions.len();
        let pattern_subscriptions = self.pattern_subscriptions.len();
        let watch_keys = self.transaction.watch_keys.len();
        let multi = if self.transaction.active {
            self.transaction.queue.len() as i64
        } else {
            -1
        };
        let is_pubsub = !self.subscriptions.is_empty() || !self.pattern_subscriptions.is_empty();
        let is_multi = self.transaction.active;

        let qbuf = self.read_buf.len();
        let qbuf_free = self.read_buf.capacity().saturating_sub(self.read_buf.len());
        let mut g = meta.write();
        g.last_cmd.clear();
        g.last_cmd.push_str(&self.last_cmd);
        g.idle_instant = now;
        g.qbuf = qbuf;
        g.qbuf_free = qbuf_free;
        g.user = user;
        g.authenticated = self.authenticated;
        g.db_index = self.db_index;
        g.subscriptions = subscriptions;
        g.pattern_subscriptions = pattern_subscriptions;
        g.watch_keys = watch_keys;
        g.multi = multi;
        g.tracking_enabled = tracking_enabled;
        // Flag bits: derive from current state rather than toggling on
        // every command. Same observable result as the old per-command
        // code that flipped these bits — except we only clear the bits
        // *this* function manages (PUBSUB/MULTI/TRACKING) and preserve
        // everything else (NO_EVICT, NO_TOUCH, ...) set elsewhere via
        // `CLIENT NO-EVICT`/`CLIENT NO-TOUCH`, which this function must
        // not clobber.
        const MANAGED: u32 = CLIENT_FLAG_PUBSUB | CLIENT_FLAG_MULTI | CLIENT_FLAG_TRACKING;
        g.flags &= !MANAGED;
        if is_pubsub {
            g.flags |= CLIENT_FLAG_PUBSUB;
        }
        if is_multi {
            g.flags |= CLIENT_FLAG_MULTI;
        }
        if tracking_enabled {
            g.flags |= CLIENT_FLAG_TRACKING;
        }
        self.last_write_instant = now;
    }

    // ── CLIENT per-connection commands ────────────────────────────────────────

    /// Build a TRACKINGINFO response for this connection's tracking
    /// state. Kept as a separate method so the response shape matches
    /// `cmd_client`'s implementation.
    fn client_trackinginfo(&self) -> Resp {
        let opts = self.db.tracking.options(self.client_id);
        let enabled = opts.is_some();
        let flags: Vec<Resp> = if let Some(ref o) = opts {
            let mut f = Vec::new();
            if o.bcast {
                f.push(Resp::bulk_str("bcast"));
            }
            if o.optin {
                f.push(Resp::bulk_str("optin"));
            }
            if o.optout {
                f.push(Resp::bulk_str("optout"));
            }
            if o.noloop {
                f.push(Resp::bulk_str("noloop"));
            }
            f
        } else {
            vec![Resp::bulk_str("off")]
        };
        Resp::array(vec![
            Resp::bulk_str("flags"),
            Resp::array(flags),
            Resp::bulk_str("redirect"),
            Resp::int(if enabled {
                opts.as_ref()
                    .and_then(|o| o.redirect)
                    .map(|r| r as i64)
                    .unwrap_or(0)
            } else {
                -1
            }),
            Resp::bulk_str("prefixes"),
            Resp::array(
                opts.map(|o| {
                    o.prefixes
                        .into_iter()
                        .map(|p| Resp::bulk(Bytes::from(p)))
                        .collect()
                })
                .unwrap_or_default(),
            ),
        ])
    }

    // ── Transactions ──────────────────────────────────────────────────────────

    fn handle_multi(&mut self) -> Resp {
        if self.transaction.active {
            return Resp::error(NexradeError::NestedMulti.to_string());
        }
        self.transaction.begin();
        Resp::ok()
    }

    async fn handle_exec(&mut self) -> Resp {
        if !self.transaction.active {
            return Resp::error(NexradeError::ExecWithoutMulti.to_string());
        }
        if self.transaction.error {
            self.transaction.discard();
            return Resp::error(NexradeError::ExecAbort.to_string());
        }

        if !self.transaction.watch_keys.is_empty() {
            let sdb = self.db.store.db(self.db_index);
            let dirty = self
                .transaction
                .watch_keys
                .iter()
                .any(|(key, version)| sdb.read_for(key).key_version(key) != *version);
            if dirty {
                self.transaction.discard();
                return Resp::null_array();
            }
        }

        let cmds = std::mem::take(&mut self.transaction.queue);
        self.transaction.discard();

        let mut results = Vec::with_capacity(cmds.len());
        for cmd_args in cmds {
            // Queued commands must run under the authenticated user's ACL,
            // not the anonymous "default" identity — otherwise a
            // restricted user could bypass a command/key restriction by
            // wrapping it in MULTI/EXEC.
            let result = dispatch_with_user(
                &self.db,
                cmd_args,
                self.db_index,
                Some(self.addr),
                &self.authenticated_user,
            )
            .await;
            results.push(result);
        }
        Resp::array(results)
    }

    fn handle_discard(&mut self) -> Resp {
        if !self.transaction.active {
            return Resp::error(NexradeError::DiscardWithoutMulti.to_string());
        }
        self.transaction.discard();
        Resp::ok()
    }

    fn handle_watch(&mut self, args: &[Resp]) -> Resp {
        if self.transaction.active {
            return Resp::error("ERR WATCH inside MULTI is not allowed".to_string());
        }
        if args.len() < 2 {
            return Resp::error(NexradeError::WrongArity("watch".to_string()).to_string());
        }
        let sdb = self.db.store.db(self.db_index);
        for arg in args.iter().skip(1) {
            if let Some(key) = arg.as_bytes() {
                let version = sdb.read_for(key).key_version(key);
                if !self.transaction.watch(key.to_vec(), version) {
                    return Resp::error("ERR too many watched keys".to_string());
                }
            }
        }
        Resp::ok()
    }

    fn handle_unwatch(&mut self) -> Resp {
        self.transaction.unwatch();
        Resp::ok()
    }

    // ── Pub/Sub ───────────────────────────────────────────────────────────────

    async fn do_subscribe(&mut self, args: &[Resp], is_pattern: bool) {
        for arg in args.iter().skip(1) {
            let channel_bytes: Bytes = match arg {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => Bytes::from(s.clone().into_bytes()),
                _ => continue,
            };
            let channel_vec = channel_bytes.to_vec();
            let target = if is_pattern {
                &mut self.pattern_subscriptions
            } else {
                &mut self.subscriptions
            };

            if !target.contains(&channel_vec) {
                let mut rx = self.db.pubsub.subscribe(channel_vec.clone());
                target.push(channel_vec.clone());

                let tx = self.pubsub_tx.clone();
                tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok(msg) => {
                                // try_send drops the message if the client is
                                // too slow (buffer full) rather than blocking.
                                let _ = tx.try_send(msg);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        }
                    }
                });
            }

            let sub_count = target.len() as i64;
            let kind = if is_pattern {
                "psubscribe"
            } else {
                "subscribe"
            };
            let payload = vec![
                Resp::bulk_str(kind),
                Resp::bulk(channel_bytes),
                Resp::int(sub_count),
            ];
            let frame = if self.resp_version >= 3 {
                Resp::Push(payload)
            } else {
                Resp::array(payload)
            };
            self.write_buf.clear();
            frame.write_to_for_version(&mut self.write_buf, self.resp_version);
            self.write_buf.finalize();
            let _ = self.stream.write_all_buf(&mut self.write_buf).await;
        }
    }

    async fn do_unsubscribe(&mut self, args: &[Resp], is_pattern: bool) {
        let default_target: Vec<Vec<u8>> = if is_pattern {
            self.pattern_subscriptions.clone()
        } else {
            self.subscriptions.clone()
        };
        let channels_to_unsub: Vec<Vec<u8>> = if args.len() <= 1 {
            default_target
        } else {
            args.iter()
                .skip(1)
                .filter_map(|a| match a {
                    Resp::BulkString(Some(b)) => Some(b.to_vec()),
                    Resp::SimpleString(s) => Some(s.as_bytes().to_vec()),
                    _ => None,
                })
                .collect()
        };
        let target = if is_pattern {
            &mut self.pattern_subscriptions
        } else {
            &mut self.subscriptions
        };

        for channel in channels_to_unsub {
            target.retain(|s| s != &channel);
            self.db.pubsub.unsubscribe(&channel);
            let count = target.len() as i64;
            let kind = if is_pattern {
                "punsubscribe"
            } else {
                "unsubscribe"
            };
            let payload = vec![
                Resp::bulk_str(kind),
                Resp::bulk(Bytes::from(channel)),
                Resp::int(count),
            ];
            let frame = if self.resp_version >= 3 {
                Resp::Push(payload)
            } else {
                Resp::array(payload)
            };
            self.write_buf.clear();
            frame.write_to_for_version(&mut self.write_buf, self.resp_version);
            self.write_buf.finalize();
            let _ = self.stream.write_all_buf(&mut self.write_buf).await;
        }
    }

    async fn run_subscribe_mode(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.pubsub_rx.recv() => {
                    let payload = vec![
                        Resp::bulk_str("message"),
                        Resp::bulk(Bytes::from(msg.channel)),
                        Resp::bulk(Bytes::from(msg.payload)),
                    ];
                    let frame = if self.resp_version >= 3 {
                        Resp::Push(payload)
                    } else {
                        Resp::array(payload)
                    };
                    self.write_buf.clear();
                    frame.write_to_for_version(&mut self.write_buf, self.resp_version);
                    self.write_buf.finalize();
                    if self.stream.write_all_buf(&mut self.write_buf).await.is_err() {
                        return;
                    }
                }

                result = self.stream.read_buf(&mut self.read_buf) => {
                    match result {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            self.parser.feed(&self.read_buf[self.read_buf.len() - n..]);
                            self.read_buf.clear();
                        }
                    }

                    loop {
                        let resp = match self.parser.parse_one() {
                            Ok(Some(r)) => r,
                            Ok(None) => break,
                            Err(_) => return,
                        };
                        let args = match resp {
                            Resp::Array(Some(a)) if !a.is_empty() => a,
                            _ => continue,
                        };
                        let cmd = args[0].as_str().unwrap_or("").to_ascii_uppercase();

                        match cmd.as_str() {
                            "SUBSCRIBE" | "PSUBSCRIBE" => {
                                self.do_subscribe(&args, cmd == "PSUBSCRIBE").await;
                            }
                            "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
                                self.do_unsubscribe(&args, cmd == "PUNSUBSCRIBE").await;
                                if self.subscriptions.is_empty() {
                                    return;
                                }
                            }
                            "PING" => {
                                let payload = args.get(1).cloned().unwrap_or(Resp::bulk_str(""));
                                let inner = vec![Resp::bulk_str("pong"), payload];
                                let frame = if self.resp_version >= 3 {
                                    Resp::Push(inner)
                                } else {
                                    Resp::array(inner)
                                };
                                self.write_buf.clear();
                                frame.write_to_for_version(&mut self.write_buf, self.resp_version);
                                self.write_buf.finalize();
                                let _ = self.stream.write_all_buf(&mut self.write_buf).await;
                            }
                            "QUIT" | "RESET" => {
                                let _ = self.stream
                                    .write_all(&Resp::ok().serialize_for_version(self.resp_version))
                                    .await;
                                return;
                            }
                            _ => {
                                let err = Resp::error("ERR Command not allowed in subscribe mode");
                                self.write_buf.clear();
                                err.write_to(&mut self.write_buf);
                                self.write_buf.finalize();
                                let _ = self.stream.write_all_buf(&mut self.write_buf).await;
                            }
                        }
                    }
                }
            }
        }
    }

    // ── Replication streaming ─────────────────────────────────────────────────

    /// Handle a replica connection after receiving PSYNC.
    ///
    /// 1. Send `+FULLRESYNC <replid> <offset>\r\n`
    /// 2. Serialize current store as a bincode snapshot wrapped in Redis bulk format
    /// 3. Subscribe to the propagation broadcast channel
    /// 4. Forward propagated write commands until the replica disconnects
    async fn run_psync_mode(&mut self) {
        use std::sync::atomic::Ordering;
        use tokio::io::AsyncWriteExt;

        let replication_id = self.db.replication.replication_id.clone();
        let current_offset = self
            .db
            .replication
            .replication_offset
            .load(Ordering::Relaxed);

        // Step 1 — FULLRESYNC response.
        let fullresync = format!("+FULLRESYNC {} {}\r\n", replication_id, current_offset);
        if let Err(e) = self.stream.write_all(fullresync.as_bytes()).await {
            error!("PSYNC: failed to send FULLRESYNC to {}: {}", self.addr, e);
            return;
        }

        // Step 2 — Serialize the current store as an RDB bulk string.
        let dbs = self.db.store.snapshot_dbs();
        let snapshot = Snapshot::new(dbs);
        let rdb_bytes = match bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
        {
            Ok(b) => b,
            Err(e) => {
                error!("PSYNC: failed to serialize snapshot: {}", e);
                return;
            }
        };
        // Redis RDB bulk format: $<len>\r\n<bytes>   (no trailing \r\n)
        let header = format!("${}\r\n", rdb_bytes.len());
        if let Err(e) = self.stream.write_all(header.as_bytes()).await {
            error!("PSYNC: failed to send RDB header to {}: {}", self.addr, e);
            return;
        }
        if let Err(e) = self.stream.write_all(&rdb_bytes).await {
            error!("PSYNC: failed to send RDB to {}: {}", self.addr, e);
            return;
        }

        // Step 3 — Register replica and subscribe to the broadcast channel.
        let replica_id = self.db.replication.register_replica(self.addr);
        let mut rx = match self.db.replication.subscribe_propagation() {
            Some(r) => r,
            None => {
                warn!("PSYNC: propagate channel not available (not a primary?)");
                self.db.replication.unregister_replica(replica_id);
                return;
            }
        };

        info!("replica {} connected from {}", replica_id, self.addr);

        // Step 4 — Stream propagated write commands to the replica.
        loop {
            // Stop streaming if this server was demoted to a replica while we
            // were in PSYNC mode (e.g. REPLICAOF issued on another connection).
            if self.db.replication.current_role() != ReplicationRole::Primary {
                info!(
                    "PSYNC: server demoted to replica, stopping stream to {}",
                    self.addr
                );
                break;
            }

            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(cmd_bytes) => {
                            if let Err(e) = self.stream.write_all(&cmd_bytes).await {
                                debug!("PSYNC: write error to replica {}: {}", self.addr, e);
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("PSYNC: replica {} lagged {} messages", self.addr, n);
                            // Continue — we miss messages but keep the connection alive.
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            debug!("PSYNC: propagation channel closed");
                            break;
                        }
                    }
                }
                // Detect replica disconnection via a read that returns 0.
                result = self.stream.read_buf(&mut self.read_buf) => {
                    match result {
                        Ok(0) | Err(_) => {
                            debug!("replica {} disconnected", self.addr);
                            break;
                        }
                        Ok(_) => {
                            // Replicas may send REPLCONF ACK — parse and handle.
                            let n = self.read_buf.len();
                            self.parser.feed(&self.read_buf[..]);
                            self.read_buf.clear();
                            trace!("replica {} sent {} bytes", self.addr, n);
                            loop {
                                match self.parser.parse_one() {
                                    Ok(Some(Resp::Array(Some(args)))) if !args.is_empty() => {
                                        let cmd = args[0].as_str().unwrap_or("").to_ascii_uppercase();
                                        if cmd == "REPLCONF" {
                                            // Handle ACK; ignore result.
                                            let _ = dispatch_with_user(&self.db, args, 0, Some(self.addr), &self.authenticated_user).await;
                                        }
                                    }
                                    Ok(None) => break,
                                    _ => break,
                                }
                            }
                        }
                    }
                }
            }
        }

        self.db.replication.unregister_replica(replica_id);
        info!("replica {} ({}) disconnected", replica_id, self.addr);
    }

    // ── Lua scripting ─────────────────────────────────────────────────────────

    async fn handle_eval(&self, args: &[Resp]) -> Resp {
        if args.len() < 3 {
            return Resp::error("ERR wrong number of arguments for 'EVAL' command");
        }
        let script = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return Resp::error("ERR script must be a string"),
        };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return Resp::error("ERR numkeys must be a non-negative integer"),
        };
        let keys: Vec<Vec<u8>> = args[3..3 + numkeys.min(args.len().saturating_sub(3))]
            .iter()
            .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
            .collect();
        let argv: Vec<Vec<u8>> = args[(3 + numkeys).min(args.len())..]
            .iter()
            .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
            .collect();

        match self
            .lua
            .eval(
                &script,
                keys,
                argv,
                self.db.clone(),
                self.db_index,
                &self.authenticated_user,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => Resp::error(e.to_string()),
        }
    }

    async fn handle_evalsha(&self, args: &[Resp]) -> Resp {
        if args.len() < 3 {
            return Resp::error("ERR wrong number of arguments for 'EVALSHA' command");
        }
        let sha = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return Resp::error("ERR sha1 must be a string"),
        };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return Resp::error("ERR numkeys must be a non-negative integer"),
        };
        let keys: Vec<Vec<u8>> = args[3..3 + numkeys.min(args.len().saturating_sub(3))]
            .iter()
            .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
            .collect();
        let argv: Vec<Vec<u8>> = args[(3 + numkeys).min(args.len())..]
            .iter()
            .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
            .collect();

        match self
            .lua
            .evalsha(
                &sha,
                keys,
                argv,
                self.db.clone(),
                self.db_index,
                &self.authenticated_user,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => Resp::error(e.to_string()),
        }
    }

    // ── CLIENT TRACKING ───────────────────────────────────────────────────────

    /// Write a single out-of-band invalidation push immediately (not
    /// batched with the request/response pipeline — tracking pushes must
    /// reach the client even while it's otherwise idle).
    async fn write_tracking_push(&mut self, push: TrackingPush) {
        let payload = match push {
            TrackingPush::Keys(keys) => vec![
                Resp::bulk_str("invalidate"),
                Resp::array(
                    keys.into_iter()
                        .map(|k| Resp::bulk(Bytes::from(k)))
                        .collect(),
                ),
            ],
            TrackingPush::FlushAll => vec![Resp::bulk_str("invalidate"), Resp::null()],
        };
        // Invalidation pushes are RESP3-only (Redis behaviour); RESP2
        // clients using tracking via REDIRECT read them as pub/sub
        // messages on a dedicated connection, which we don't model here,
        // so we just skip delivery for RESP2 connections.
        if self.resp_version < 3 {
            return;
        }
        let frame = Resp::Push(payload);
        self.write_buf.clear();
        frame.write_to_for_version(&mut self.write_buf, self.resp_version);
        self.write_buf.finalize();
        let _ = self.stream.write_all_buf(&mut self.write_buf).await;
    }

    /// `CLIENT TRACKING ON|OFF [REDIRECT id] [PREFIX p ...] [BCAST]
    /// [OPTIN] [OPTOUT] [NOLOOP]`.
    fn handle_client_tracking(&mut self, args: &[Resp]) -> Resp {
        let mode = match args.get(2).and_then(|a| a.as_str()) {
            Some(s) => s.to_ascii_uppercase(),
            None => return Resp::error("ERR wrong number of arguments for 'client|tracking'"),
        };
        match mode.as_str() {
            "OFF" => {
                self.db.tracking.disable(self.client_id);
                Resp::ok()
            }
            "ON" => {
                let mut opts = TrackingOptions::default();
                let mut i = 3;
                while i < args.len() {
                    let opt = match args[i].as_str() {
                        Some(s) => s.to_ascii_uppercase(),
                        None => return Resp::error("ERR syntax error"),
                    };
                    match opt.as_str() {
                        "BCAST" => {
                            opts.bcast = true;
                            i += 1;
                        }
                        "OPTIN" => {
                            opts.optin = true;
                            i += 1;
                        }
                        "OPTOUT" => {
                            opts.optout = true;
                            i += 1;
                        }
                        "NOLOOP" => {
                            opts.noloop = true;
                            i += 1;
                        }
                        "REDIRECT" => {
                            let id = match args
                                .get(i + 1)
                                .and_then(|a| a.as_str())
                                .and_then(|s| s.parse::<u64>().ok())
                            {
                                Some(id) => id,
                                None => return Resp::error("ERR syntax error"),
                            };
                            if id != 0 && !self.db.tracking.exists(id) {
                                return Resp::error(
                                    "ERR The client ID you want redirect to does not exist",
                                );
                            }
                            if id != 0 {
                                opts.redirect = Some(id);
                            }
                            i += 2;
                        }
                        "PREFIX" => {
                            let prefix = match args.get(i + 1).and_then(|a| a.as_bytes()) {
                                Some(p) => p.to_vec(),
                                None => return Resp::error("ERR syntax error"),
                            };
                            opts.prefixes.push(prefix);
                            i += 2;
                        }
                        _ => return Resp::error("ERR syntax error"),
                    }
                }
                if opts.optin && opts.optout {
                    return Resp::error("ERR You can't specify both OPTIN mode and OPTOUT mode");
                }
                if !opts.prefixes.is_empty() && !opts.bcast {
                    return Resp::error("ERR PREFIX option requires BCAST mode to be enabled");
                }
                match self.db.tracking.enable(self.client_id, opts) {
                    Ok(()) => Resp::ok(),
                    Err(e) => Resp::error(format!("ERR {e}")),
                }
            }
            _ => Resp::error("ERR syntax error"),
        }
    }

    /// `CLIENT CACHING YES|NO` — one-shot override for the next command,
    /// only meaningful under OPTIN/OPTOUT tracking mode.
    fn handle_client_caching(&mut self, args: &[Resp]) -> Resp {
        let Some(opts) = self.db.tracking.options(self.client_id) else {
            return Resp::error(
                "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
            );
        };
        if !opts.optin && !opts.optout {
            return Resp::error(
                "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
            );
        }
        let yes = match args.get(2).and_then(|a| a.as_str()) {
            Some(s) if s.eq_ignore_ascii_case("YES") => true,
            Some(s) if s.eq_ignore_ascii_case("NO") => false,
            _ => return Resp::error("ERR syntax error"),
        };
        if (opts.optin && !yes) || (opts.optout && yes) {
            return Resp::error(
                "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
            );
        }
        self.db.tracking.set_caching_override(self.client_id, yes);
        Resp::ok()
    }

    // ── RESP3 per-command upgrades ───────────────────────────────────────────────

    /// For the handful of commands whose RESP3 array-vs-nested-pairs shape
    /// depends on an option that isn't recoverable from the response alone
    /// (WITHSCORES presence, or whether COUNT was explicitly passed), decide
    /// intent from the original request args. Must be called before `args`
    /// moves into `dispatch_tracked` — cheap no-op for every other command.
    fn resp3_should_nest_pairs(cmd_name: &str, args: &[Resp]) -> bool {
        match cmd_name {
            "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZINTER" | "ZUNION"
            | "ZDIFF" => args.iter().skip(1).any(|a| {
                a.as_str()
                    .is_some_and(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            }),
            // args[0]=cmd, args[1]=key, args[2]=optional COUNT.
            "ZPOPMIN" | "ZPOPMAX" => args.len() >= 3,
            _ => false,
        }
    }

    /// Convert command responses into the shape Redis 7.x returns in RESP3 mode
    /// for the small set of commands where it matters. Most commands already
    /// return the same bytes in both RESP versions, so this is a no-op for them.
    ///
    /// Specifically:
    /// - `HGETALL` in RESP3 returns a Map (`%N`), not a flat array of
    ///   `[field, value, field, value, ...]`. redis-py 8.0+ raises on the
    ///   legacy shape when running in RESP3 mode.
    /// - `HKEYS`, `HVALS`, `SMEMBERS` in RESP3 return a Set (`~N`).
    /// - `XREAD` / `XREADGROUP` in RESP3 return a Map `{stream: entries}`
    ///   instead of an array of `[stream, entries]` pairs.
    /// - `ZRANGE` / `ZREVRANGE` / `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` (with
    ///   WITHSCORES), `ZINTER` / `ZUNION` / `ZDIFF` (with WITHSCORES), and
    ///   `ZPOPMIN` / `ZPOPMAX` (with an explicit COUNT) in RESP3 return a
    ///   nested list of `[member, score]` pairs instead of the flat
    ///   `[member, score, member, score, ...]` array. Whether to nest is
    ///   decided by `resp3_should_nest_pairs` from the *original request*
    ///   (WITHSCORES/COUNT presence) rather than guessed from the response
    ///   shape — guessing from an even array length is wrong: e.g. a plain
    ///   `ZUNION` with no WITHSCORES that happens to return an even number
    ///   of members must NOT be nested.
    fn upgrade_to_resp3(cmd_name: &str, resp: Resp, resp_version: u8, nest_pairs: bool) -> Resp {
        if resp_version < 3 {
            return resp;
        }
        // Match on `resp` by value (not `&resp`) so each arm can consume
        // `items`/`streams` directly via `into_iter()` instead of cloning
        // the whole response vec (and every `Resp` inside it) just to hand
        // back an owned copy. The old `&resp` version cloned on every RESP3
        // HGETALL/HKEYS/SMEMBERS/XREAD/ZRANGE-family response — for a
        // 100-member ZRANGE WITHSCORES that's a ~20-30% throughput hit,
        // confirmed via redis-benchmark (105-115k -> 80-82k rps, 3 rounds).
        match (cmd_name, resp) {
            ("HGETALL", Resp::Array(Some(items))) if items.len() % 2 == 0 => {
                let mut pairs = Vec::with_capacity(items.len() / 2);
                let mut iter = items.into_iter();
                while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                    pairs.push((k, v));
                }
                Resp::Map(pairs)
            }
            ("HKEYS" | "HVALS" | "SMEMBERS", Resp::Array(Some(items))) => Resp::Set(items),
            ("XREAD" | "XREADGROUP", Resp::Array(Some(streams))) => {
                // Outer shape: array of 2-tuples → map of stream → entries.
                let mut pairs = Vec::with_capacity(streams.len());
                for stream in streams {
                    if let Resp::Array(Some(mut parts)) = stream {
                        if parts.len() == 2 {
                            let entries = parts.pop().unwrap();
                            let key = parts.pop().unwrap();
                            pairs.push((key, entries));
                        }
                    }
                }
                Resp::Map(pairs)
            }
            (
                "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZINTER" | "ZUNION"
                | "ZDIFF" | "ZPOPMIN" | "ZPOPMAX",
                Resp::Array(Some(items)),
            ) if nest_pairs && items.len() % 2 == 0 => {
                // Flat alternating [member, score, ...] → nested [[member, score], ...].
                let mut pairs = Vec::with_capacity(items.len() / 2);
                let mut iter = items.into_iter();
                while let (Some(m), Some(s)) = (iter.next(), iter.next()) {
                    pairs.push(Resp::array(vec![m, s]));
                }
                Resp::array(pairs)
            }
            (_, other) => other,
        }
    }

    // ── HELLO / protocol negotiation ──────────────────────────────────────────

    fn handle_hello(&mut self, args: &[Resp]) -> Resp {
        let version: u8 = args
            .get(1)
            .and_then(|a| a.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(self.resp_version);

        if version != 2 && version != 3 {
            return Resp::error("NOPROTO unsupported protocol version".to_string());
        }

        // AUTH inside HELLO
        if let Some(auth_arg) = args.get(2).and_then(|a| a.as_str()) {
            if auth_arg.eq_ignore_ascii_case("AUTH") {
                let password = args.get(4).and_then(|a| a.as_str()).unwrap_or("");
                let cfg = self.db.config.lock();
                match &cfg.requirepass {
                    None => {}
                    Some(pass) if pass == password => {
                        self.authenticated = true;
                    }
                    _ => {
                        return Resp::error(
                            "WRONGPASS invalid username-password pair or user is disabled",
                        )
                    }
                }
            }
        }

        self.resp_version = version;

        let server_info = vec![
            (Resp::bulk_str("server"), Resp::bulk_str("nexrade-cache")),
            (
                Resp::bulk_str("version"),
                Resp::bulk_str(env!("CARGO_PKG_VERSION")),
            ),
            (Resp::bulk_str("proto"), Resp::Integer(version as i64)),
            (Resp::bulk_str("id"), Resp::Integer(self.client_id as i64)),
            (Resp::bulk_str("mode"), Resp::bulk_str("standalone")),
            (Resp::bulk_str("role"), Resp::bulk_str("master")),
            (Resp::bulk_str("modules"), Resp::Array(Some(vec![]))),
        ];

        if version == 3 {
            Resp::Map(server_info)
        } else {
            // RESP2: flatten map to array
            let flat: Vec<Resp> = server_info.into_iter().flat_map(|(k, v)| [k, v]).collect();
            Resp::Array(Some(flat))
        }
    }

    fn handle_script(&self, args: &[Resp]) -> Resp {
        let subcmd = args
            .get(1)
            .and_then(|a| a.as_str())
            .unwrap_or("")
            .to_ascii_uppercase();
        match subcmd.as_str() {
            "LOAD" => {
                let script = match args.get(2).and_then(|a| a.as_str()) {
                    Some(s) => s.to_string(),
                    None => return Resp::error("ERR wrong number of arguments for 'SCRIPT|LOAD'"),
                };
                let sha = self.lua.script_load(script);
                Resp::bulk_str(&sha)
            }
            "EXISTS" => {
                let shas: Vec<&str> = args[2..].iter().filter_map(|a| a.as_str()).collect();
                let results = self.lua.script_exists(&shas);
                Resp::array(results.iter().map(|&e| Resp::int(e as i64)).collect())
            }
            "FLUSH" => {
                self.lua.script_flush();
                Resp::ok()
            }
            _ => Resp::error(format!("ERR unknown SCRIPT subcommand '{}'", subcmd)),
        }
    }

    // ── FUNCTION ──────────────────────────────────────────────────────────────

    fn handle_function(&self, args: &[Resp]) -> Resp {
        let subcmd = args
            .get(1)
            .and_then(|a| a.as_str())
            .unwrap_or("")
            .to_ascii_uppercase();
        match subcmd.as_str() {
            "LOAD" => {
                // FUNCTION LOAD [REPLACE] <source>
                let mut idx = 2;
                let mut replace = false;
                if args
                    .get(idx)
                    .and_then(|a| a.as_str())
                    .map(|s| s.eq_ignore_ascii_case("REPLACE"))
                    .unwrap_or(false)
                {
                    replace = true;
                    idx += 1;
                }
                let source = match args.get(idx).and_then(|a| a.as_str()) {
                    Some(s) => s.to_string(),
                    None => {
                        return Resp::error(
                            "ERR wrong number of arguments for 'FUNCTION|LOAD' command",
                        )
                    }
                };
                match self.functions.load(&source, replace) {
                    Ok(r) => r,
                    Err(e) => Resp::error(e.to_string()),
                }
            }
            "DELETE" => {
                let name = match args.get(2).and_then(|a| a.as_str()) {
                    Some(s) => s,
                    None => {
                        return Resp::error(
                            "ERR wrong number of arguments for 'FUNCTION|DELETE' command",
                        )
                    }
                };
                if self.functions.delete(name) {
                    Resp::ok()
                } else {
                    Resp::error("ERR Library not found")
                }
            }
            "LIST" => Resp::array(self.functions.list()),
            "FLUSH" => {
                self.functions.flush();
                Resp::ok()
            }
            "DUMP" => Resp::bulk(bytes::Bytes::from(self.functions.dump())),
            "RESTORE" => {
                let payload = match args.get(2).and_then(|a| a.as_bytes()) {
                    Some(b) => b.to_vec(),
                    None => {
                        return Resp::error(
                            "ERR wrong number of arguments for 'FUNCTION|RESTORE' command",
                        )
                    }
                };
                let mode = match args.get(3).and_then(|a| a.as_str()) {
                    None => nexrade_lua::FunctionRestoreMode::Append,
                    Some(s) if s.eq_ignore_ascii_case("FLUSH") => {
                        nexrade_lua::FunctionRestoreMode::Flush
                    }
                    Some(s) if s.eq_ignore_ascii_case("APPEND") => {
                        nexrade_lua::FunctionRestoreMode::Append
                    }
                    Some(s) if s.eq_ignore_ascii_case("REPLACE") => {
                        nexrade_lua::FunctionRestoreMode::Replace
                    }
                    Some(_) => return Resp::error("ERR unsupported FUNCTION RESTORE policy"),
                };
                match self.functions.restore(&payload, mode) {
                    Ok(r) => r,
                    Err(e) => Resp::error(e.to_string()),
                }
            }
            "STATS" => {
                let stats = self.functions.stats();
                Resp::array(vec![
                    Resp::bulk_str("running_script"),
                    Resp::null(),
                    Resp::bulk_str("engines"),
                    Resp::array(vec![
                        Resp::bulk_str("LUA"),
                        Resp::array(vec![
                            Resp::bulk_str("libraries_count"),
                            Resp::int(stats.total_libraries as i64),
                            Resp::bulk_str("functions_count"),
                            Resp::int(stats.total_functions as i64),
                        ]),
                    ]),
                ])
            }
            "HELP" => Resp::array(vec![
                Resp::bulk_str("FUNCTION LOAD [REPLACE] <source> -- Load a library"),
                Resp::bulk_str("FUNCTION DELETE <name>           -- Delete a library"),
                Resp::bulk_str("FUNCTION LIST                    -- List loaded libraries"),
                Resp::bulk_str("FUNCTION FLUSH                   -- Remove all libraries"),
                Resp::bulk_str("FUNCTION DUMP                     -- Serialise all libraries"),
                Resp::bulk_str("FUNCTION RESTORE <payload> [FLUSH|APPEND|REPLACE]"),
                Resp::bulk_str("FUNCTION STATS                   -- Show engine stats"),
            ]),
            _ => Resp::error(format!("ERR unknown FUNCTION subcommand '{}'", subcmd)),
        }
    }

    /// FCALL / FCALL_RO <function> <numkeys> [key ...] [arg ...]
    async fn handle_fcall(&self, args: &[Resp]) -> Resp {
        if args.len() < 3 {
            return Resp::error("ERR wrong number of arguments for 'FCALL' command");
        }
        let func = match args[1].as_str() {
            Some(s) => s.to_string(),
            None => return Resp::error("ERR function name must be a string"),
        };
        let numkeys: usize = match args[2].as_str().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return Resp::error("ERR numkeys must be a non-negative integer"),
        };
        let keys: Vec<Vec<u8>> = args[3..3 + numkeys.min(args.len().saturating_sub(3))]
            .iter()
            .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
            .collect();
        let argv: Vec<Vec<u8>> = args[(3 + numkeys).min(args.len())..]
            .iter()
            .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
            .collect();

        match self
            .functions
            .call(
                &func,
                keys,
                argv,
                self.db.clone(),
                self.db_index,
                &self.authenticated_user,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => Resp::error(e.to_string()),
        }
    }
}

#[cfg(test)]
mod panic_isolation_tests {
    use super::*;
    use std::panic::AssertUnwindSafe;

    /// Proves the load-bearing part of Fix 1: `ConnectionCleanup`'s `Drop`
    /// impl runs and performs the registry/stat cleanup even when the
    /// task it's guarding unwinds from a panic instead of returning
    /// normally. The full "one client panics, others keep running"
    /// behavior follows from Tokio's own per-task `catch_unwind` wrapping
    /// once `panic = "unwind"` is set (see Cargo.toml) — well-established
    /// Tokio behavior, not something specific to this codebase to reprove.
    /// What *is* specific here is that the guard actually fires and
    /// cleans up `db.tracking` / `db.connections` / `db.stats` correctly,
    /// which this test drives directly via `catch_unwind`.
    #[test]
    fn connection_cleanup_runs_on_panic_unwind() {
        let db = Db::default();
        let client_id = db.next_client_id.fetch_add(1, Ordering::Relaxed);

        let (tracking_tx, _tracking_rx) = mpsc::channel(8);
        db.tracking.register(client_id, tracking_tx);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (meta, _kill_flag) = db.connections.register(client_id, addr);

        db.stats.connect();
        assert_eq!(db.stats.active_connections.load(Ordering::Relaxed), 1);
        assert!(db.tracking.exists(client_id));
        assert!(db.connections.meta(client_id).is_some());

        let cleanup = ConnectionCleanup {
            db: db.clone(),
            client_id,
            metrics: None,
        };

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = cleanup;
            drop(meta); // release our own Arc so the registry's is the only one left
            panic!("simulated connection task panic");
        }));
        assert!(result.is_err(), "the panic should have propagated");

        // The guard's Drop must have run despite the unwind.
        assert!(
            !db.tracking.exists(client_id),
            "tracking registry entry must be cleaned up after a panic"
        );
        assert!(
            db.connections.meta(client_id).is_none(),
            "connection registry entry must be cleaned up after a panic"
        );
        assert_eq!(
            db.stats.active_connections.load(Ordering::Relaxed),
            0,
            "active_connections must be decremented after a panic"
        );
    }
}
