//! TCP listener — accepts connections and spawns handlers.

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::signal;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{error, info, warn};

use nexrade_core::command::{dispatch, parse_cmd_name};
use nexrade_core::db::{unix_secs, Db, ServerConfig};
use nexrade_core::health::HealthPhase;
use nexrade_core::persistence::{AofReader, AofSync, AofWriter, Snapshot};
use nexrade_core::replication::ReplicationRole;
use nexrade_core::resp::{Resp, RespParser};
use nexrade_lua::{FunctionRegistry, LuaEngine};
use nexrade_metrics::Metrics;

use crate::connection::Connection;
#[cfg(feature = "tls")]
use crate::stream::Stream;

/// Long-lived background tasks registered for a bounded join on shutdown
/// (0.5.0 MSET Option B foundation). Connection handlers stay fire-and-forget.
struct BgTasks {
    handles: Vec<(&'static str, JoinHandle<()>)>,
}

impl BgTasks {
    fn new() -> Self {
        Self {
            handles: Vec::with_capacity(8),
        }
    }

    fn push(&mut self, name: &'static str, handle: JoinHandle<()>) {
        self.handles.push((name, handle));
    }

    /// Await every registered task with a per-task timeout. Logs panics and
    /// timeouts; never hangs forever. Call after the accept loop has exited
    /// so tasks that select on the shutdown watch can complete.
    async fn drain(self) {
        const PER_TASK: Duration = Duration::from_secs(2);
        for (name, handle) in self.handles {
            match time::timeout(PER_TASK, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) if e.is_panic() => {
                    error!("background task {} panicked during shutdown: {:?}", name, e);
                }
                Ok(Err(e)) => {
                    warn!("background task {} join error: {:?}", name, e);
                }
                Err(_) => {
                    warn!(
                        "background task {} did not finish within {:?} — continuing shutdown",
                        name, PER_TASK
                    );
                }
            }
        }
    }
}

/// Spawn `conn.run()` as its own task, plus a thin supervisor task that
/// awaits its `JoinHandle` solely to detect and log a panic. Needed
/// because the release profile is `panic = "unwind"` (see Cargo.toml) —
/// Tokio isolates a panicking task instead of crashing the process, but a
/// discarded `JoinHandle` means the panic would otherwise vanish with zero
/// trace in the logs. Isolation itself needs no code here; it's a
/// property of `tokio::spawn` once unwinding is enabled.
///
/// Connection tasks are **not** registered in [`BgTasks`] — we do not join
/// every client on shutdown.
fn spawn_connection(conn: Connection, addr: std::net::SocketAddr) {
    tokio::spawn(async move {
        if let Err(e) = tokio::spawn(async move { conn.run().await }).await {
            if e.is_panic() {
                error!("connection task for {} panicked: {:?}", addr, e);
            }
        }
    });
}

/// Spawn a long-running background task, log panics, and return the outer
/// supervisor handle so [`BgTasks`] can join it on shutdown.
fn spawn_supervised<F>(name: &'static str, fut: F) -> JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = tokio::spawn(fut).await {
            if e.is_panic() {
                error!("{} task panicked: {:?}", name, e);
            }
        }
    })
}

pub struct Listener {
    pub db: Db,
    pub config: ServerConfig,
    metrics: Option<Metrics>,
    /// Broadcast side of the unified shutdown signal. Owned here (rather than
    /// created inside `run`) so callers can hand receivers to the operations
    /// HTTP servers *before* `run` consumes `self` — those servers are started
    /// first, and previously had no way to observe the drain.
    shutdown_tx: watch::Sender<bool>,
}

impl Listener {
    pub fn new(db: Db, metrics: Option<Metrics>) -> Self {
        let config = db.config.lock().clone();
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            db,
            config,
            metrics,
            shutdown_tx,
        }
    }

    /// Subscribe to the unified shutdown signal.
    ///
    /// Flips to `true` once on SIGINT, SIGTERM, or the `SHUTDOWN` command.
    /// Intended for long-lived side listeners (metrics, health) that must stop
    /// answering before the process exits — a `/readyz` endpoint that outlives
    /// the drain reports `ready=true` to a load balancer that should already
    /// have been told to stop sending traffic.
    pub fn shutdown_subscriber(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Start the server and block until shutdown.
    pub async fn run(self) -> anyhow::Result<()> {
        // Mark the lifecycle as Recovering before any persistence work so
        // the operations HTTP surface and `INFO health` can report this
        // state. Recovery will transition to Ready after this function
        // finishes successfully; a startup failure transitions to Failed.
        //
        // This is also the `Starting -> Recovering` edge: a `Db` is born in
        // `Starting`, and this is the only place that advances it.
        self.db.lifecycle().set_phase(HealthPhase::Recovering);

        // A legacy full AOF has no checkpoint boundary. Replaying it after a
        // current RDB repeats non-idempotent commands (INCR, LPUSH, XADD,
        // ...), so reject the ambiguous recovery configuration before we
        // bind a port or mutate in-memory state. Operators must choose one
        // authoritative recovery source for now.
        //
        // Treat `aof_path = ""` (a common TOML convention for "disabled")
        // the same as `aof_path` being absent: no AOF replay, no AOF
        // writer. Without this guard, an empty-string config would
        // attempt `AofWriter::open("")` and fail at startup with a
        // confusing `os error 2` instead of a clean "AOF disabled".
        let rdb_path = self.config.persistence.rdb_path.as_deref();
        // Treat `aof_path = ""` (the documented TOML "disabled" convention)
        // and whitespace-only paths the same as `aof_path` being absent.
        // Trim before passing to the OS layer so `"  /tmp/foo  "` doesn't
        // reach `AofWriter::open` with stray whitespace and fail with a
        // confusing OS error.
        let aof_path = self
            .config
            .persistence
            .aof_path
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty());
        let both_exist = if let (Some(rdb), Some(aof)) = (rdb_path, aof_path) {
            std::path::Path::new(rdb).exists() && std::path::Path::new(aof).exists()
        } else {
            false
        };
        if both_exist {
            anyhow::bail!(
                "refusing ambiguous recovery: both RDB ({:?}) and full AOF ({:?}) exist; \
                 choose exactly one authoritative source because replaying the full AOF after \
                 the RDB duplicates non-idempotent writes",
                rdb_path,
                aof_path
            );
        }

        // Validate and recover persistence before accepting any client. A
        // corrupted configured artifact is never a reason to start with an
        // empty or partially replayed database.
        let has_rdb = if let Some(rdb) = rdb_path {
            std::path::Path::new(rdb).exists()
        } else {
            false
        };
        if has_rdb {
            let rdb = rdb_path.expect("has_rdb requires configured RDB path");
            let snapshot = Snapshot::load(rdb)
                .map_err(|e| anyhow::anyhow!("failed to load configured RDB {rdb}: {e}"))?;
            let count = snapshot.databases.len();
            for (db_index, database) in snapshot.databases {
                let sdb = self.db.store.db(db_index);
                for (key, entry) in database.entries {
                    sdb.write_for(&key).insert(key, entry);
                }
            }
            // Record the actual snapshot creation time, never the current
            // time, so the snapshot-age readiness signal stays accurate.
            if snapshot.created_at > 0 {
                self.db
                    .lifecycle()
                    .record_snapshot_loaded(snapshot.created_at);
                self.db
                    .stats
                    .last_save_time
                    .store(snapshot.created_at, Ordering::Relaxed);
            }
            info!("loaded RDB snapshot ({} database(s))", count);
        }

        if let Some(aof) = aof_path {
            if std::path::Path::new(aof).exists() {
                let mut reader = AofReader::open(aof)
                    .map_err(|e| anyhow::anyhow!("failed to open configured AOF {aof}: {e}"))?;
                let mut count = 0u64;
                let mut current_db: usize = 0;
                loop {
                    let Some(cmd_bytes) = reader.next_command().map_err(|e| {
                        anyhow::anyhow!("failed to replay configured AOF {aof}: {e}")
                    })?
                    else {
                        break;
                    };
                    let mut parser = RespParser::new();
                    parser.feed(&cmd_bytes);
                    let Some(Resp::Array(Some(args))) = parser
                        .parse_one()
                        .map_err(|e| anyhow::anyhow!("AOF parser inconsistency in {aof}: {e}"))?
                    else {
                        anyhow::bail!("AOF command {} in {aof} is not a RESP array", count + 1);
                    };
                    let cmd_upper = parse_cmd_name(&args).unwrap_or_default();
                    if cmd_upper == "SELECT" {
                        current_db = args
                            .get(1)
                            .and_then(|a| a.as_str())
                            .and_then(|s| s.parse::<usize>().ok())
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "invalid SELECT in AOF {aof} at command {}",
                                    count + 1
                                )
                            })?;
                        continue;
                    }
                    let r = dispatch(&self.db, args, current_db).await;
                    if let Resp::Error(e) = r {
                        anyhow::bail!("AOF replay error in {aof} at command {}: {e}", count + 1);
                    }
                    count += 1;
                }
                self.db.stats.dirty_keys.store(0, Ordering::Relaxed);
                info!("AOF replay: {} commands applied", count);
            }

            let writer = AofWriter::open(aof)
                .map_err(|e| anyhow::anyhow!("failed to open configured AOF writer {aof}: {e}"))?;
            *self.db.aof_writer.lock() = Some(writer);
            self.db.stats.aof_enabled.store(true, Ordering::Relaxed);
            info!("AOF enabled: {}", aof);
        }

        // ── TLS material is validated and its port bound BEFORE readiness ───
        //
        // Ordering is deliberate. This used to sit ~80 lines below, *after*
        // `set_phase(Ready)`, so even once the failures became fatal the
        // instance had already advertised `ready=true` to every probe before
        // discovering it could not serve TLS. Validating here means a TLS
        // misconfiguration is caught while the phase is still `Recovering`.
        //
        // Every failure is fatal (1.3.0). Previously each arm only logged
        // `error!`/`warn!` and startup continued, which meant an operator who
        // set `tls_enabled = true` with an unreadable cert, a bad key, or an
        // occupied TLS port got a server that:
        //
        //   - printed `TLS  ON` in the startup banner,
        //   - never bound the TLS port at all, and
        //   - exited 0 and served plaintext as if nothing were wrong.
        //
        // A client configured for TLS then fails to connect while the instance
        // looks healthy, and a client with a fallback path could silently
        // downgrade to plaintext. Asking for TLS and quietly not getting it is
        // the one outcome that must never be silent, so this now matches how
        // `nexrade-cache` already treats a metrics or health bind failure:
        // refuse to start.
        //
        // (The comment previously here claimed cert/key presence was "checked
        // in `start_server` before this point". No such validation existed
        // anywhere — hence the explicit check below.)
        #[cfg(feature = "tls")]
        let tls_setup = if self.config.tls_enabled {
            // Run the same validation `--preflight` runs, first, so the two
            // agree by construction and the error text is identical whether
            // an operator hits it in a deploy check or at startup.
            let problems = validate_tls_config(&self.config);
            if !problems.is_empty() {
                anyhow::bail!(
                    "refusing to start: TLS is enabled but misconfigured, and \
                     starting anyway would serve only plaintext:\n  - {}",
                    problems.join("\n  - ")
                );
            }
            // validate_tls_config guarantees both are Some in this branch.
            let (Some(cert), Some(key)) = (&self.config.tls_cert, &self.config.tls_key) else {
                unreachable!("validate_tls_config rejects a missing cert or key")
            };
            let tls_port = self.config.tls_port.unwrap_or(6380);
            let tls_addr = format!("{}:{}", self.config.bind, tls_port);
            let acceptor = nexrade_tls::TlsAcceptor::from_pem_files(cert, key)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "failed to initialize TLS for {tls_addr} \
                         (cert {cert:?}, key {key:?}): {e}"
                    )
                })?;
            let tls_listener = TcpListener::bind(&tls_addr)
                .await
                .map_err(|e| anyhow::anyhow!("failed to bind TLS listener on {tls_addr}: {e}"))?;
            Some((tls_listener, acceptor, tls_addr))
        } else {
            None
        };

        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("nexrade-cache listening on {}", addr);

        // Record the actual snapshot creation time on the LifecycleState, not
        // an artificial "now" — already done above when the RDB was loaded.
        // Do NOT overwrite last_save_time here; it is now sourced from the
        // loaded snapshot's created_at or the next successful save.

        // Set server start time
        let _ = self.db.stats.start_time.set(std::time::Instant::now());

        // Persistence recovery has completed. Transition lifecycle from
        // Recovering to Ready so the operations HTTP surface and the INFO
        // health section can report the new state.
        self.db.lifecycle().set_phase(HealthPhase::Ready);

        let mut bg = BgTasks::new();

        // Unified shutdown signal — fires on SIGINT (Ctrl+C), SIGTERM, or the
        // SHUTDOWN command. `db.shutdown` is a `Notify`, whose `notify_one()`
        // wakes exactly one waiter — insufficient once there's a second
        // (TLS) accept loop that also needs to stop. So a single relay task
        // owns the one `notified()` wait and fans the signal out to every
        // accept loop via a `watch` channel instead.
        //
        // The channel is owned by `Listener` (created in `new`) rather than
        // here, so the operations HTTP servers — started before `run` is
        // called — can already hold receivers via `shutdown_subscriber()`.
        let shutdown_tx = self.shutdown_tx;
        let shutdown_rx = shutdown_tx.subscribe();

        // Spawn background tasks
        let db_clone = self.db.clone();
        let metrics_clone = self.metrics.clone();
        let hz = self.config.hz;
        let mut tick_shutdown = shutdown_rx.clone();
        bg.push(
            "background-tick",
            spawn_supervised("background-tick", async move {
                run_background_tasks(db_clone, hz, metrics_clone, &mut tick_shutdown).await;
            }),
        );

        // Spawn replication background task (replica→primary handshake + streaming).
        let db_repl = self.db.clone();
        let our_port = self.config.port;
        let mut repl_shutdown = shutdown_rx.clone();
        bg.push(
            "replication",
            spawn_supervised("replication", async move {
                run_replication_task(db_repl, our_port, &mut repl_shutdown).await;
            }),
        );

        let db = self.db.clone();
        let metrics = self.metrics.clone();
        let max_clients = self.config.max_clients;
        let lua_time_limit = Duration::from_millis(self.config.lua_time_limit);
        let lua_engine = LuaEngine::new(lua_time_limit).expect("failed to create Lua engine");
        let function_registry = FunctionRegistry::new();

        let db_shutdown = db.clone();
        bg.push(
            "shutdown-relay",
            spawn_supervised("shutdown-relay", async move {
                tokio::select! {
                    _ = await_shutdown_signal() => {
                        info!("received shutdown signal (OS) — shutting down");
                    }
                    _ = db_shutdown.shutdown.notified() => {
                        info!("received SHUTDOWN command — shutting down");
                    }
                }
                let _ = shutdown_tx.send(true);
            }),
        );

        // ── Optional TLS listener, running alongside the plain-TCP one ──────
        //
        // The socket is already bound and the cert/key already validated (see
        // the pre-readiness block above); all that remains is to start serving
        // on it.
        #[cfg(feature = "tls")]
        if let Some((tls_listener, acceptor, tls_addr)) = tls_setup {
            info!("nexrade-cache TLS listening on {}", tls_addr);
            let db = db.clone();
            let metrics = metrics.clone();
            let lua_engine = lua_engine.clone();
            let function_registry = function_registry.clone();
            let mut shutdown_rx = shutdown_rx.clone();
            bg.push(
                "tls-accept",
                tokio::spawn(async move {
                    run_tls_accept_loop(
                        tls_listener,
                        acceptor,
                        db,
                        metrics,
                        lua_engine,
                        function_registry,
                        max_clients,
                        &mut shutdown_rx,
                    )
                    .await;
                }),
            );
        }

        let mut shutdown_rx = shutdown_rx;
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            let _ = stream.set_nodelay(true);
                            let active = db.stats.active_connections.load(std::sync::atomic::Ordering::Relaxed);
                            if active >= max_clients as u64 {
                                warn!("max clients reached ({}), rejecting {}", max_clients, addr);
                                drop(stream);
                                continue;
                            }

                            let conn = Connection::new(
                                db.clone(),
                                crate::stream::Stream::Plain(stream),
                                addr,
                                lua_engine.clone(),
                                function_registry.clone(),
                                metrics.clone(),
                            );
                            spawn_connection(conn, addr);
                        }
                        Err(e) => {
                            error!("accept error: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }

        // Join long-lived background tasks before RDB/AOF so future deferred
        // writers (0.5.1) can drain without racing process exit. Bounded so a
        // stuck task cannot hang SHUTDOWN forever. Tick / replication / TLS
        // accept select on the shutdown watch so they normally exit promptly.
        info!("draining background tasks…");
        bg.drain().await;

        // Mark lifecycle as Draining so the operations HTTP surface, INFO
        // health, and operators see the right state throughout shutdown.
        self.db.lifecycle().set_phase(HealthPhase::Draining);

        // Enter draining mode: block new mutations while we finalize.
        #[cfg(not(target_arch = "wasm32"))]
        let _shutdown_quiesce = db.persistence.quiesce().await;

        // ── Save on shutdown (like Redis SHUTDOWN SAVE) ───────────────────────
        // Extract path before the save so we don't hold the config lock during I/O.
        let rdb_path = db.config.lock().persistence.rdb_path.clone();
        let dirty = db.stats.dirty_keys.load(Ordering::Relaxed);
        let nosave = db.stats.shutdown_nosave.load(Ordering::Acquire);
        let mut shutdown_save_ok = true;
        if !nosave && dirty > 0 {
            if let Some(ref path) = rdb_path {
                info!("saving {} dirty key(s) before shutdown…", dirty);
                let dbs = db.store.snapshot_dbs();
                let snapshot = Snapshot::new(dbs);
                match snapshot.save(path) {
                    Ok(()) => {
                        info!("shutdown RDB save complete");
                        db.stats.bgsave_last_status.store(0, Ordering::Relaxed);
                    }
                    Err(e) => {
                        error!("shutdown RDB save failed: {}", e);
                        db.stats.bgsave_last_status.store(1, Ordering::Relaxed);
                        shutdown_save_ok = false;
                    }
                }
            }
        }

        if nosave {
            warn!("SHUTDOWN NOSAVE requested — skipping final RDB snapshot");
        }

        // Flush + fsync AOF before exit. Fail shutdown if AOF is enabled and
        // this final sync cannot be completed.
        let mut aof_fsync_ok = true;
        if let Some(ref mut w) = *db.aof_writer.lock() {
            if let Err(e) = w.fsync() {
                error!("shutdown AOF fsync failed: {}", e);
                aof_fsync_ok = false;
            }
        }

        if !shutdown_save_ok || !aof_fsync_ok {
            anyhow::bail!("shutdown persistence failed; dirty state may be unrecoverable");
        }

        info!("server shut down gracefully");
        Ok(())
    }
}

/// Validate the TLS portion of a config without binding anything.
///
/// Returns one human-readable error string per problem found, in the order
/// startup would hit them. An empty vec means the TLS settings would let
/// `Listener::run` reach its bind step.
///
/// This exists so `--preflight` and startup cannot drift: both call this,
/// and the actual PEM parsing is delegated to `nexrade_tls`, which is the
/// same code path that builds the live acceptor. Before 1.3.1 preflight
/// performed *no* TLS validation at all, so a config with a missing or
/// unparseable certificate reported `preflight: OK` and then exited 1 on
/// the next deploy step — precisely the failure preflight exists to catch.
///
/// Port collisions are checked but not bound: preflight is non-mutating, so
/// it cannot prove a port is free. It only catches the statically-decidable
/// case where TLS and plaintext are pointed at the same port, which
/// otherwise surfaces as a bare `Address already in use` that never
/// mentions TLS.
pub fn validate_tls_config(config: &ServerConfig) -> Vec<String> {
    let mut errors = Vec::new();

    if !config.tls_enabled {
        // A cert configured while TLS is off is a live footgun: the operator
        // believes TLS is on (the paths are right there in the file) but the
        // server serves plaintext only. Not fatal — disabling TLS while
        // leaving the paths in place is a legitimate way to stage a rollout —
        // so this is surfaced by the caller as a warning, not an error.
        return errors;
    }

    #[cfg(not(feature = "tls"))]
    {
        errors.push(
            "tls.enabled = true but this binary was built without the `tls` \
             feature; it cannot serve TLS and would only serve plaintext"
                .to_string(),
        );
    }

    #[cfg(feature = "tls")]
    {
        // Mirror `Listener::run`'s ordering: presence first, then parse.
        match (&config.tls_cert, &config.tls_key) {
            (None, None) => errors.push(
                "tls.enabled = true but neither tls.cert nor tls.key is set; \
                 startup will refuse rather than serve plaintext"
                    .to_string(),
            ),
            (None, Some(_)) => errors
                .push("tls.enabled = true and tls.key is set but tls.cert is missing".to_string()),
            (Some(_), None) => errors
                .push("tls.enabled = true and tls.cert is set but tls.key is missing".to_string()),
            (Some(cert), Some(key)) => {
                // Delegates to the same parser the live acceptor uses, so a
                // key that doesn't match its certificate is caught here too.
                if let Err(e) = nexrade_tls::validate_pem_files(cert, key) {
                    errors.push(format!(
                        "TLS cert/key unusable (cert {cert:?}, key {key:?}): {e}"
                    ));
                }
            }
        }

        let tls_port = config.tls_port.unwrap_or(6380);
        if tls_port == config.port {
            errors.push(format!(
                "tls.port ({tls_port}) is the same as the plaintext port; \
                 the second bind fails with 'Address already in use'"
            ));
        }
    }

    errors
}

/// Accept loop for the TLS listener — mirrors the plain-TCP loop in
/// `Listener::run`, except each accepted socket is upgraded via
/// `TlsAcceptor::accept` before a `Connection` is spawned for it. Runs as
/// its own task so a slow/failed TLS handshake never blocks the plain
/// listener, and stops on the same shared shutdown signal.
#[cfg(feature = "tls")]
#[allow(clippy::too_many_arguments)]
async fn run_tls_accept_loop(
    listener: TcpListener,
    acceptor: nexrade_tls::TlsAcceptor,
    db: Db,
    metrics: Option<Metrics>,
    lua_engine: LuaEngine,
    function_registry: FunctionRegistry,
    max_clients: usize,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((tcp_stream, addr)) => {
                        let _ = tcp_stream.set_nodelay(true);
                        let active = db.stats.active_connections.load(Ordering::Relaxed);
                        if active >= max_clients as u64 {
                            warn!("max clients reached ({}), rejecting TLS {}", max_clients, addr);
                            drop(tcp_stream);
                            continue;
                        }

                        let acceptor = acceptor.clone();
                        let db = db.clone();
                        let metrics = metrics.clone();
                        let lua_engine = lua_engine.clone();
                        let function_registry = function_registry.clone();
                        tokio::spawn(async move {
                            let tls_stream = match acceptor.accept(tcp_stream).await {
                                Ok(s) => s,
                                Err(e) => {
                                    warn!("TLS handshake failed with {}: {}", addr, e);
                                    return;
                                }
                            };
                            let conn = Connection::new(
                                db,
                                Stream::Tls(Box::new(tls_stream)),
                                addr,
                                lua_engine,
                                function_registry,
                                metrics,
                            );
                            spawn_connection(conn, addr);
                        });
                    }
                    Err(e) => {
                        error!("TLS accept error: {}", e);
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }
}

/// Background periodic tasks.
async fn run_background_tasks(
    db: Db,
    hz: u32,
    metrics: Option<Metrics>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    let interval = Duration::from_millis(1000 / hz.max(1) as u64);
    let mut ticker = time::interval(interval);
    // Tracks how many ticks have elapsed (used for 1-second AOF fsync).
    let mut ticks: u64 = 0;

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown_rx.changed() => {
                return;
            }
        }
        ticks += 1;

        // Refresh the cached LRU clock at `hz` frequency. Per-GET
        // `entry.lru_clock = clock.now()` is now a relaxed atomic load
        // instead of `SystemTime::now()` (a syscall).
        db.lru_clock.store(unix_secs() as u32, Ordering::Relaxed);

        // Update ops/sec every second (every hz ticks).
        if ticks % hz.max(1) as u64 == 0 {
            let current = db.stats.total_commands.load(Ordering::Relaxed);
            let prev = db.stats.prev_commands.swap(current, Ordering::Relaxed);
            db.stats
                .ops_per_sec
                .store(current.saturating_sub(prev), Ordering::Relaxed);
        }

        // Snapshot all config values we need this tick in a single lock acquisition.
        let (active_expire, save_rules, rdb_path, aof_sync) = {
            let cfg = db.config.lock();
            (
                cfg.active_expire,
                cfg.persistence.rdb_save_rules.clone(),
                cfg.persistence.rdb_path.clone(),
                cfg.persistence.aof_sync.clone(),
            )
        };

        // Active key expiry — fire keyspace `expired` events when enabled.
        // 0.7.3: adaptive cycle scales the per-tick budget so a write storm
        // with many short TTLs doesn't pile up expired keys faster than the
        // background loop can drain them.
        if active_expire {
            // Expiry mutates the store outside command dispatch, so it must
            // participate in the same admission protocol as user writes.
            #[cfg(not(target_arch = "wasm32"))]
            let _mutation_permit = match db.persistence.enter_mutation() {
                Some(permit) => permit,
                None => continue,
            };
            let expired = db.store.active_expire_cycle();
            if !expired.is_empty() && db.notify_flags.load().0 != 0 {
                use nexrade_core::notify::NotifyFlags;
                let flags = db.notify_flags.load();
                if flags.contains(NotifyFlags::K_EXPIRED) {
                    for (db_idx, key) in expired {
                        db.notify_keyspace_event(db_idx, "expired", &key);
                    }
                }
            }
        }

        // ── RDB save rules (Redis-compatible) ─────────────────────────────────
        // Check each rule: if dirty_keys >= min_changes AND seconds since last
        // save >= window, trigger a background save.
        let dirty = db.stats.dirty_keys.load(Ordering::Relaxed);
        if dirty > 0 {
            let last_save = db.stats.last_save_time.load(Ordering::Relaxed);
            let elapsed_secs = unix_secs().saturating_sub(last_save);

            let should_save = save_rules
                .iter()
                .any(|(secs, min_changes)| elapsed_secs >= *secs && dirty >= *min_changes as u64);

            if should_save {
                if let Some(path) = rdb_path {
                    // Prevent concurrent background saves.
                    use std::sync::atomic::Ordering::AcqRel;
                    if !db.stats.bgsave_in_progress.swap(true, AcqRel) {
                        let save_job = match db.rdb_save_job.clone().try_lock_owned() {
                            Ok(guard) => guard,
                            Err(_) => {
                                db.stats.bgsave_in_progress.store(false, Ordering::Release);
                                continue;
                            }
                        };
                        let mutation_quiesce = db.persistence.quiesce().await;
                        let dirty_at_capture = db.stats.dirty_keys.load(Ordering::Relaxed);
                        let dbs = db.store.snapshot_dbs();
                        drop(mutation_quiesce);
                        let stats = db.stats.clone();
                        tokio::spawn(async move {
                            let _save_job = save_job;
                            // snapshot.save() does blocking file I/O — run on the
                            // blocking thread pool so we don't stall the async runtime.
                            let result =
                                tokio::task::spawn_blocking(move || Snapshot::new(dbs).save(&path))
                                    .await;
                            match result {
                                Ok(Ok(())) => {
                                    info!(
                                        "auto BGSAVE completed ({} dirty key(s))",
                                        dirty_at_capture
                                    );
                                    stats
                                        .dirty_keys
                                        .fetch_update(
                                            Ordering::AcqRel,
                                            Ordering::Relaxed,
                                            |current| {
                                                Some(current.saturating_sub(dirty_at_capture))
                                            },
                                        )
                                        .ok();
                                    stats.last_save_time.store(unix_secs(), Ordering::Relaxed);
                                    stats.bgsave_last_status.store(0, Ordering::Relaxed);
                                }
                                Ok(Err(e)) => {
                                    error!("auto BGSAVE failed: {}", e);
                                    stats.bgsave_last_status.store(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    error!("auto BGSAVE task panicked: {}", e);
                                    stats.bgsave_last_status.store(1, Ordering::Relaxed);
                                }
                            }
                            stats.bgsave_in_progress.store(false, Ordering::Release);
                        });
                    }
                }
            }
        }

        // ── AOF EverySec fsync ────────────────────────────────────────────────
        if aof_sync == AofSync::EverySec && ticks % hz.max(1) as u64 == 0 {
            // Every ~1 second
            if let Some(ref mut w) = *db.aof_writer.lock() {
                if let Err(e) = w.fsync() {
                    error!("AOF fsync error: {}", e);
                    db.fail_aof("EverySec fsync", &e);
                }
            }
        }

        // ── AOF "no" periodic flush (not fsync) ─────────────────────────────
        // `appendfsync no` intentionally skips fsync — the OS decides when to
        // persist. But successful `append` already handed data to the kernel
        // because `AofWriter` now uses `File` directly. This flush ensures
        // OS buffers are pushed periodically, matching the "at least once a
        // second" guarantee without `EverySec`'s fsync cost.
        if aof_sync == AofSync::No && ticks % hz.max(1) as u64 == 0 {
            if let Some(ref mut w) = *db.aof_writer.lock() {
                if let Err(e) = w.flush() {
                    error!("AOF flush error: {}", e);
                    db.fail_aof("appendfsync no flush", &e);
                }
            }
        }

        // Update Prometheus gauges from live stats
        if let Some(ref m) = metrics {
            use std::sync::atomic::Ordering;

            let active = db.stats.active_connections.load(Ordering::Relaxed) as f64;
            m.connected_clients
                .with_label_values::<&str>(&[])
                .set(active);

            let pubsub_count = db.pubsub.channel_count() as f64;
            m.pubsub_channels
                .with_label_values::<&str>(&[])
                .set(pubsub_count);

            for db_idx in 0..db.store.db_count {
                let key_count = db.store.db(db_idx).len() as f64;
                m.set_db_keys(&db_idx.to_string(), key_count);
            }
        }
    }
}

// ── Replication task (runs on replicas) ──────────────────────────────────────

/// Background task that maintains the replica→primary connection.
///
/// When the server is configured as a replica this task connects to the primary,
/// performs the PING / REPLCONF / PSYNC handshake, loads the RDB snapshot, then
/// continuously reads and applies propagated write commands.
///
/// On disconnection the task waits 5 seconds and retries.
/// Exits when `shutdown_rx` signals server shutdown (0.5.0 drain).
async fn run_replication_task(db: Db, our_port: u16, shutdown_rx: &mut watch::Receiver<bool>) {
    loop {
        // Wait until we are actually in replica mode (or shutdown).
        {
            let role = db.replication.role.read().clone();
            if role != ReplicationRole::Replica {
                tokio::select! {
                    _ = db.replication.replica_notify.notified() => {
                        continue;
                    }
                    _ = shutdown_rx.changed() => {
                        return;
                    }
                }
            }
        }

        let maybe_primary = db.replication.replica_of.read().clone();
        let (host, port) = match maybe_primary {
            Some(p) => p,
            None => {
                tokio::select! {
                    _ = db.replication.replica_notify.notified() => {
                        continue;
                    }
                    _ = shutdown_rx.changed() => {
                        return;
                    }
                }
            }
        };

        info!("replication: connecting to primary {}:{}", host, port);

        match connect_to_primary(&db, &host, port, our_port).await {
            Ok(()) => {
                info!("replication: primary connection closed, will retry in 5s");
            }
            Err(e) => {
                warn!("replication: error connecting to primary {}: {}", host, e);
            }
        }
        db.replication
            .primary_link_up
            .store(false, Ordering::Relaxed);

        // Wait before retrying, but also watch for role changes / shutdown.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            _ = db.replication.replica_notify.notified() => {}
            _ = shutdown_rx.changed() => {
                return;
            }
        }
    }
}

/// Perform the full replica handshake and then stream commands from the primary.
async fn connect_to_primary(db: &Db, host: &str, port: u16, our_port: u16) -> anyhow::Result<()> {
    let addr = format!("{}:{}", host, port);
    let mut stream = TcpStream::connect(&addr).await?;

    // ── PING ─────────────────────────────────────────────────────────────────
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let pong = read_line(&mut stream).await?;
    if !pong.starts_with("+PONG") {
        anyhow::bail!("expected PONG, got: {}", pong);
    }

    // ── REPLCONF listening-port ───────────────────────────────────────────────
    let replconf_port = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        our_port.to_string().len(),
        our_port
    );
    stream.write_all(replconf_port.as_bytes()).await?;
    let ok1 = read_line(&mut stream).await?;
    if !ok1.starts_with("+OK") {
        anyhow::bail!("REPLCONF listening-port: expected OK, got: {}", ok1);
    }

    // ── REPLCONF capa psync2 ──────────────────────────────────────────────────
    stream
        .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        .await?;
    let ok2 = read_line(&mut stream).await?;
    if !ok2.starts_with("+OK") {
        anyhow::bail!("REPLCONF capa: expected OK, got: {}", ok2);
    }

    // ── PSYNC ? -1 (full resync) ──────────────────────────────────────────────
    stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .await?;
    let fullresync = read_line(&mut stream).await?;
    if !fullresync.starts_with("+FULLRESYNC") {
        anyhow::bail!("expected FULLRESYNC, got: {}", fullresync);
    }
    // Parse: +FULLRESYNC <replid> <offset>
    let parts: Vec<&str> = fullresync.trim().splitn(3, ' ').collect();
    if parts.len() == 3 {
        if let Ok(offset) = parts[2].parse::<u64>() {
            db.replication
                .replication_offset
                .store(offset, Ordering::Relaxed);
        }
    }

    // ── Read RDB bulk string: $<len>\r\n<bytes> ───────────────────────────────
    let rdb_header = read_line(&mut stream).await?;
    let rdb_len: usize = rdb_header
        .trim_start_matches('$')
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid RDB bulk header: {}", rdb_header))?;

    let mut rdb_buf = vec![0u8; rdb_len];
    stream.read_exact(&mut rdb_buf).await?;

    // Deserialize and load into store.
    match bincode::serde::decode_from_slice::<Snapshot, _>(&rdb_buf, bincode::config::standard())
        .map(|(v, _)| v)
    {
        Ok(snapshot) => {
            let count = snapshot.databases.len();
            for (db_index, database) in snapshot.databases {
                let sdb = db.store.db(db_index);
                for (key, entry) in database.entries {
                    sdb.write_for(&key).insert(key, entry);
                }
            }
            info!("replication: loaded RDB snapshot ({} database(s))", count);
        }
        Err(e) => {
            warn!("replication: failed to deserialize RDB snapshot: {}", e);
        }
    }

    // ── Stream propagated commands ────────────────────────────────────────────
    info!(
        "replication: entering streaming mode from {}:{}",
        host, port
    );
    db.replication
        .primary_link_up
        .store(true, Ordering::Relaxed);
    let mut parser = RespParser::new();
    let mut buf = bytes::BytesMut::with_capacity(4096);

    loop {
        // Check if we are still in replica mode.
        if db.replication.current_role() != ReplicationRole::Replica {
            info!("replication: role changed to primary, stopping replica task");
            return Ok(());
        }

        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            break; // Primary closed connection.
        }
        let bytes_received = n as u64;

        let data = buf.split().freeze();
        parser.feed(&data);

        loop {
            match parser.parse_one() {
                Ok(Some(Resp::Array(Some(args)))) if !args.is_empty() => {
                    let cmd = args[0].as_str().unwrap_or("").to_ascii_uppercase();
                    if cmd == "REPLCONF" {
                        // Primary may send REPLCONF GETACK — respond with our offset.
                        let sub = args
                            .get(1)
                            .and_then(|a| a.as_str())
                            .unwrap_or("")
                            .to_uppercase();
                        if sub == "GETACK" {
                            let offset = db.replication.replication_offset.load(Ordering::Relaxed);
                            let ack = format!(
                                "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                                offset.to_string().len(),
                                offset
                            );
                            stream.write_all(ack.as_bytes()).await?;
                        }
                        continue;
                    }
                    // System-internal: replication-propagated commands were
                    // already authorized by the primary when their original
                    // user ran them; the replica mirrors state, not
                    // independently re-authorizes. See `dispatch()`'s doc
                    // comment for why user-facing paths must NEVER use
                    // this helper.
                    let result = dispatch(db, args, 0).await;
                    if let Resp::Error(e) = result {
                        warn!(
                            "replication: error applying propagated command {}: {}",
                            cmd, e
                        );
                    }
                }
                Ok(None) => break,
                Ok(_) => {} // Ignore non-array responses.
                Err(_) => break,
            }
        }

        // Advance offset by the raw bytes received from the primary's replication
        // stream — consistent with how the primary counts propagated bytes.
        db.replication
            .replication_offset
            .fetch_add(bytes_received, Ordering::Relaxed);
    }

    Ok(())
}

/// Read a single CRLF-terminated line from a TCP stream.
async fn read_line(stream: &mut TcpStream) -> anyhow::Result<String> {
    let mut line = Vec::new();
    let mut one = [0u8; 1];
    loop {
        stream.read_exact(&mut one).await?;
        if one[0] == b'\n' && line.last() == Some(&b'\r') {
            line.pop(); // Remove trailing \r
            break;
        }
        line.push(one[0]);
    }
    Ok(String::from_utf8_lossy(&line).to_string())
}

/// Waits for SIGINT (Ctrl+C) **or** SIGTERM (`kill <pid>`).
///
/// On Unix both signals are handled so process managers, Docker, systemd, and
/// test harnesses that use `SIGTERM` all trigger a clean shutdown.
/// On non-Unix platforms only SIGINT is available.
async fn await_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM");
        tokio::select! {
            _ = signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}
