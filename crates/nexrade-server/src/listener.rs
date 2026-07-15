//! TCP listener — accepts connections and spawns handlers.

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::signal;
#[cfg(feature = "tls")]
use tokio::sync::watch;
use tokio::time;
use tracing::{error, info, warn};

use nexrade_core::command::{dispatch, parse_cmd_name};
use nexrade_core::db::{unix_secs, Db, ServerConfig};
use nexrade_core::persistence::{AofReader, AofSync, AofWriter, Snapshot};
use nexrade_core::replication::ReplicationRole;
use nexrade_core::resp::{Resp, RespParser};
use nexrade_lua::{FunctionRegistry, LuaEngine};
use nexrade_metrics::Metrics;

use crate::connection::Connection;
#[cfg(feature = "tls")]
use crate::stream::Stream;

/// Spawn `conn.run()` as its own task, plus a thin supervisor task that
/// awaits its `JoinHandle` solely to detect and log a panic. Needed
/// because the release profile is `panic = "unwind"` (see Cargo.toml) —
/// Tokio isolates a panicking task instead of crashing the process, but a
/// discarded `JoinHandle` means the panic would otherwise vanish with zero
/// trace in the logs. Isolation itself needs no code here; it's a
/// property of `tokio::spawn` once unwinding is enabled.
fn spawn_connection(conn: Connection, addr: std::net::SocketAddr) {
    tokio::spawn(async move {
        if let Err(e) = tokio::spawn(async move { conn.run().await }).await {
            if e.is_panic() {
                error!("connection task for {} panicked: {:?}", addr, e);
            }
        }
    });
}

/// Same idea as `spawn_connection`, generalized for the handful of
/// long-running background tasks (tick loop, replication, shutdown relay)
/// that also currently discard their `JoinHandle`. Logging only — no
/// auto-restart, since a panicked background task's state may need
/// resynchronization that this helper has no way to know about.
fn spawn_supervised<F>(name: &'static str, fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = tokio::spawn(fut).await {
            if e.is_panic() {
                error!("{} task panicked: {:?}", name, e);
            }
        }
    });
}

pub struct Listener {
    pub db: Db,
    pub config: ServerConfig,
    metrics: Option<Metrics>,
}

impl Listener {
    pub fn new(db: Db, metrics: Option<Metrics>) -> Self {
        let config = db.config.lock().clone();
        Self {
            db,
            config,
            metrics,
        }
    }

    /// Start the server and block until shutdown.
    pub async fn run(self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("nexrade-cache listening on {}", addr);

        // ── Load RDB snapshot ─────────────────────────────────────────────────
        if let Some(ref rdb_path) = self.config.persistence.rdb_path {
            if std::path::Path::new(rdb_path).exists() {
                match Snapshot::load(rdb_path) {
                    Ok(snapshot) => {
                        let count = snapshot.databases.len();
                        for (db_index, database) in snapshot.databases {
                            let sdb = self.db.store.db(db_index);
                            for (key, entry) in database.entries {
                                sdb.write_for(&key).insert(key, entry);
                            }
                        }
                        info!("loaded RDB snapshot ({} database(s))", count);
                    }
                    Err(e) => warn!("failed to load RDB snapshot: {}", e),
                }
            }
        }

        // ── Replay AOF (after RDB, same as Redis) ────────────────────────────
        if let Some(ref aof_path) = self.config.persistence.aof_path {
            if std::path::Path::new(aof_path).exists() {
                match AofReader::open(aof_path) {
                    Ok(mut reader) => {
                        let mut count = 0u64;
                        let mut current_db: usize = 0;
                        while let Some(cmd_bytes) = reader.next_command() {
                            let mut parser = RespParser::new();
                            parser.feed(&cmd_bytes);
                            if let Ok(Some(Resp::Array(Some(args)))) = parser.parse_one() {
                                // Track SELECT so writes go to the right database.
                                let cmd_upper = parse_cmd_name(&args).unwrap_or_default();
                                if cmd_upper == "SELECT" {
                                    if let Some(idx) = args
                                        .get(1)
                                        .and_then(|a| a.as_str())
                                        .and_then(|s| s.parse::<usize>().ok())
                                    {
                                        current_db = idx;
                                    }
                                    continue;
                                }
                                // System-internal: replay AOF commands under
                                // "default" since each was already authorized at
                                // the time it was originally written. See
                                // `dispatch()`'s doc comment for why a real
                                // connection path must NEVER use this helper.
                                let r = dispatch(&self.db, args, current_db).await;
                                if let nexrade_core::resp::Resp::Error(e) = r {
                                    tracing::warn!("AOF replay error (cmd {}): {}", count + 1, e);
                                }
                                count += 1;
                            }
                        }
                        // AOF replay inflates dirty counter — reset it.
                        self.db.stats.dirty_keys.store(0, Ordering::Relaxed);
                        info!("AOF replay: {} commands applied", count);
                    }
                    Err(e) => warn!("failed to open AOF for replay: {}", e),
                }
            }

            // Open the AOF writer for ongoing appends.
            match AofWriter::open(aof_path) {
                Ok(writer) => {
                    *self.db.aof_writer.lock() = Some(writer);
                    self.db.stats.aof_enabled.store(true, Ordering::Relaxed);
                    info!("AOF enabled: {}", aof_path);
                }
                Err(e) => warn!("failed to open AOF writer: {}", e),
            }
        }

        // Record initial last_save_time so save rules start from now.
        self.db
            .stats
            .last_save_time
            .store(unix_secs(), Ordering::Relaxed);

        // Set server start time
        let _ = self.db.stats.start_time.set(std::time::Instant::now());

        // Spawn background tasks
        let db_clone = self.db.clone();
        let metrics_clone = self.metrics.clone();
        let hz = self.config.hz;
        spawn_supervised("background-tick", async move {
            run_background_tasks(db_clone, hz, metrics_clone).await;
        });

        // Spawn replication background task (replica→primary handshake + streaming).
        let db_repl = self.db.clone();
        let our_port = self.config.port;
        spawn_supervised("replication", async move {
            run_replication_task(db_repl, our_port).await;
        });

        let db = self.db.clone();
        let metrics = self.metrics.clone();
        let max_clients = self.config.max_clients;
        let lua_time_limit = Duration::from_millis(self.config.lua_time_limit);
        let lua_engine = LuaEngine::new(lua_time_limit).expect("failed to create Lua engine");
        let function_registry = FunctionRegistry::new();

        // Unified shutdown signal — fires on SIGINT (Ctrl+C), SIGTERM, or the
        // SHUTDOWN command. `db.shutdown` is a `Notify`, whose `notify_one()`
        // wakes exactly one waiter — insufficient once there's a second
        // (TLS) accept loop that also needs to stop. So a single relay task
        // owns the one `notified()` wait and fans the signal out to every
        // accept loop via a `watch` channel instead.
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let db_shutdown = db.clone();
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
        });

        // ── Optional TLS listener, running alongside the plain-TCP one ──────
        // `config.tls_enabled` requires both `tls_cert` and `tls_key` to be
        // set (checked in `nexrade-cache`'s `start_server` before this
        // point); if either is missing here we just skip starting it rather
        // than failing the whole server.
        #[cfg(feature = "tls")]
        if self.config.tls_enabled {
            if let (Some(cert), Some(key)) = (&self.config.tls_cert, &self.config.tls_key) {
                let tls_port = self.config.tls_port.unwrap_or(6380);
                let tls_addr = format!("{}:{}", self.config.bind, tls_port);
                match nexrade_tls::TlsAcceptor::from_pem_files(cert, key).await {
                    Ok(acceptor) => match TcpListener::bind(&tls_addr).await {
                        Ok(tls_listener) => {
                            info!("nexrade-cache TLS listening on {}", tls_addr);
                            let db = db.clone();
                            let metrics = metrics.clone();
                            let lua_engine = lua_engine.clone();
                            let function_registry = function_registry.clone();
                            let mut shutdown_rx = shutdown_rx.clone();
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
                            });
                        }
                        Err(e) => error!("failed to bind TLS listener on {}: {}", tls_addr, e),
                    },
                    Err(e) => error!("failed to initialize TLS ({}): {}", tls_addr, e),
                }
            } else {
                warn!("TLS enabled but tls-cert or tls-key not set, TLS listener skipped");
            }
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

        // ── Save on shutdown (like Redis SHUTDOWN SAVE) ───────────────────────
        // Extract path before the save so we don't hold the config lock during I/O.
        let rdb_path = db.config.lock().persistence.rdb_path.clone();
        let dirty = db.stats.dirty_keys.load(Ordering::Relaxed);
        if dirty > 0 {
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
                    }
                }
            }
        }

        // Flush + fsync AOF before exit.
        if let Some(ref mut w) = *db.aof_writer.lock() {
            let _ = w.fsync();
        }

        info!("server shut down gracefully");
        Ok(())
    }
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
async fn run_background_tasks(db: Db, hz: u32, metrics: Option<Metrics>) {
    let interval = Duration::from_millis(1000 / hz.max(1) as u64);
    let mut ticker = time::interval(interval);
    // Tracks how many ticks have elapsed (used for 1-second AOF fsync).
    let mut ticks: u64 = 0;

    loop {
        ticker.tick().await;
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

        // Active key expiry
        if active_expire {
            db.store.active_expire(20);
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
                        let dbs = db.store.snapshot_dbs();
                        let stats = db.stats.clone();
                        tokio::spawn(async move {
                            // snapshot.save() does blocking file I/O — run on the
                            // blocking thread pool so we don't stall the async runtime.
                            let result =
                                tokio::task::spawn_blocking(move || Snapshot::new(dbs).save(&path))
                                    .await;
                            match result {
                                Ok(Ok(())) => {
                                    info!("auto BGSAVE completed ({} dirty key(s))", dirty);
                                    stats.dirty_keys.store(0, Ordering::Relaxed);
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
                }
            }
        }

        // ── AOF "no" periodic flush (not fsync) ─────────────────────────────
        // `appendfsync no` intentionally skips fsync — the OS decides when to
        // persist. But `AofWriter::append` only writes into an in-process
        // `BufWriter`, which never even reaches the kernel until `flush()` is
        // called. Without this tick, a `kill -9` or panic (not just an OS
        // crash) can lose data that real Redis's `no` policy would already
        // have handed to the kernel via `write(2)`. This flush (no fsync)
        // restores that "hands it to the OS at least once a second" property
        // without paying `EverySec`'s fsync durability cost.
        if aof_sync == AofSync::No && ticks % hz.max(1) as u64 == 0 {
            if let Some(ref mut w) = *db.aof_writer.lock() {
                if let Err(e) = w.flush() {
                    error!("AOF flush error: {}", e);
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
async fn run_replication_task(db: Db, our_port: u16) {
    loop {
        // Wait until we are actually in replica mode.
        {
            let role = db.replication.role.read().clone();
            if role != ReplicationRole::Replica {
                // Block until notified that the role has changed.
                db.replication.replica_notify.notified().await;
                continue;
            }
        }

        let maybe_primary = db.replication.replica_of.read().clone();
        let (host, port) = match maybe_primary {
            Some(p) => p,
            None => {
                // Role changed back to primary — wait again.
                db.replication.replica_notify.notified().await;
                continue;
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

        // Wait before retrying, but also watch for role changes.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            _ = db.replication.replica_notify.notified() => {}
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
