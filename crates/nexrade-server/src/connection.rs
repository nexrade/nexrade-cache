//! Individual client connection handler.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use nexrade_core::command::{dispatch, dispatch_with_addr, is_write_command};
use nexrade_core::db::Db;
use nexrade_core::error::NexradeError;
use nexrade_core::persistence::Snapshot;
use nexrade_core::pubsub::Message;
use nexrade_core::replication::ReplicationRole;
use nexrade_core::resp::{Resp, RespParser, SegBuf};
use nexrade_core::transaction::Transaction;
use nexrade_lua::LuaEngine;
use nexrade_metrics::Metrics;

/// State for a single connected client.
pub struct Connection {
    db: Db,
    stream: TcpStream,
    addr: SocketAddr,
    db_index: usize,
    authenticated: bool,
    parser: RespParser,
    read_buf: BytesMut,
    write_buf: SegBuf,
    transaction: Transaction,
    subscriptions: Vec<Vec<u8>>,
    client_id: u64,
    client_name: String,
    /// Sender half of the pub/sub relay channel (cloned into relay tasks).
    /// Bounded to 256 so a slow client cannot accumulate unbounded memory.
    pubsub_tx: mpsc::Sender<Message>,
    /// Receiver half — polled in subscribe mode.
    pubsub_rx: mpsc::Receiver<Message>,
    /// RESP protocol version negotiated via HELLO (2 or 3).
    resp_version: u8,
    /// Shared Lua scripting engine (ScriptCache is Arc-backed, safe to clone).
    lua: LuaEngine,
    metrics: Option<Metrics>,
}

impl Connection {
    pub fn new(
        db: Db,
        stream: TcpStream,
        addr: SocketAddr,
        lua: LuaEngine,
        metrics: Option<Metrics>,
    ) -> Self {
        let requires_auth = db.config.lock().requirepass.is_some();
        let client_id = db.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (pubsub_tx, pubsub_rx) = mpsc::channel(256);
        Self {
            db,
            stream,
            addr,
            db_index: 0,
            authenticated: !requires_auth,
            parser: RespParser::new(),
            read_buf: BytesMut::with_capacity(4096),
            write_buf: SegBuf::with_capacity(4096),
            transaction: Transaction::new(),
            subscriptions: Vec::new(),
            client_id,
            client_name: String::new(),
            pubsub_tx,
            pubsub_rx,
            lua,
            metrics,
            resp_version: 2,
        }
    }

    /// Main connection loop.
    pub async fn run(mut self) {
        self.db.stats.connect();
        if let Some(ref m) = self.metrics {
            m.record_connection(true);
        }
        info!("new connection from {}", self.addr);

        let idle_timeout = {
            let t = self.db.config.lock().timeout;
            if t > 0 {
                Some(Duration::from_secs(t))
            } else {
                None
            }
        };

        'outer: loop {
            // Read more data — with optional idle timeout.
            let n = if let Some(timeout) = idle_timeout {
                match tokio::time::timeout(timeout, self.stream.read_buf(&mut self.read_buf)).await
                {
                    Ok(Ok(0)) => {
                        debug!("connection closed by {}", self.addr);
                        break;
                    }
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => {
                        error!("read error from {}: {}", self.addr, e);
                        break;
                    }
                    Err(_) => {
                        debug!("idle timeout for {}", self.addr);
                        break;
                    }
                }
            } else {
                match self.stream.read_buf(&mut self.read_buf).await {
                    Ok(0) => {
                        debug!("connection closed by {}", self.addr);
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        error!("read error from {}: {}", self.addr, e);
                        break;
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

                let cmd_name = args[0].as_str().unwrap_or("").to_ascii_uppercase();

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

                // Pre-capture slowlog args before args are consumed by dispatch.
                let sl_args: Vec<String> = args
                    .iter()
                    .map(|a| a.as_str().unwrap_or("?").to_string())
                    .collect();

                // Check read-only enforcement for replicas.
                let is_replica = self.db.replication.is_replica();
                let response = if cmd_name == "AUTH" {
                    self.handle_auth(&args).await
                } else if !self.authenticated {
                    Resp::error("NOAUTH Authentication required")
                } else if is_replica && is_write_command(&cmd_name) {
                    Resp::Error(NexradeError::ReadOnly.to_string())
                } else if cmd_name == "SELECT" {
                    self.handle_select(&args)
                } else if cmd_name == "CLIENT" {
                    self.handle_client(&args)
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
                } else if self.transaction.active {
                    self.transaction.queue(args);
                    Resp::SimpleString("QUEUED".to_string())
                } else {
                    self.db.stats.record_command();
                    // Serialize the command for replication propagation before args
                    // are consumed.
                    let repl_bytes: Option<bytes::Bytes> = if is_write_command(&cmd_name) {
                        if let Some(ref tx) = self.db.replication.propagate_tx {
                            if tx.receiver_count() > 0 {
                                Some(Resp::Array(Some(args.clone())).serialize())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    let resp =
                        dispatch_with_addr(&self.db, args, self.db_index, Some(self.addr)).await;

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

                // Record metrics and slow log.
                let elapsed = start.elapsed();
                if let Some(ref m) = self.metrics {
                    let is_error = matches!(&response, Resp::Error(_));
                    m.record_command(&cmd_name, elapsed.as_secs_f64(), is_error);
                }
                let sl = &self.db.slowlog;
                if elapsed.as_micros() as u64 >= sl.threshold_us() {
                    sl.record(elapsed, sl_args, self.addr.to_string());
                }

                response.write_to(&mut self.write_buf);

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

            if quit {
                break 'outer;
            }

            if let Some((args, is_pattern)) = subscribe_args {
                self.do_subscribe(&args, is_pattern).await;
                self.run_subscribe_mode().await;
                break 'outer;
            }
        }

        self.db.stats.disconnect();
        if let Some(ref m) = self.metrics {
            m.record_connection(false);
        }
        info!("connection closed: {}", self.addr);
    }

    // ── Authentication ────────────────────────────────────────────────────────

    async fn handle_auth(&mut self, args: &[Resp]) -> Resp {
        let config = self.db.config.lock();
        match &config.requirepass {
            None => Resp::error("ERR Client sent AUTH, but no password is set"),
            Some(pass) => {
                let provided = args.get(1).and_then(|a| a.as_str()).unwrap_or("");
                if provided == pass.as_str() {
                    self.authenticated = true;
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
                Resp::ok()
            }
            Ok(_) => Resp::error("ERR DB index is out of range"),
            Err(_) => Resp::error("ERR value is not an integer or out of range"),
        }
    }

    // ── CLIENT per-connection commands ────────────────────────────────────────

    fn handle_client(&mut self, args: &[Resp]) -> Resp {
        let sub = args
            .get(1)
            .and_then(|a| a.as_str())
            .unwrap_or("")
            .to_ascii_uppercase();
        match sub.as_str() {
            "SETNAME" => {
                match args.get(2).and_then(|a| a.as_str()) {
                    Some(name) => {
                        if name.contains(' ') {
                            return Resp::error(
                                "ERR Client names cannot contain spaces, newlines or special characters.",
                            );
                        }
                        self.client_name = name.to_string();
                        Resp::ok()
                    }
                    None => Resp::error(
                        "ERR wrong number of arguments for 'client|setname' command",
                    ),
                }
            }
            "GETNAME" => {
                if self.client_name.is_empty() {
                    Resp::null()
                } else {
                    Resp::bulk_str(self.client_name.clone())
                }
            }
            "ID" => Resp::int(self.client_id as i64),
            "INFO" => Resp::bulk_str(format!(
                "id={} addr={} laddr=127.0.0.1:6379 fd=0 name={} age=0 idle=0 flags=N db={} sub={} psub=0 multi=-1 watch=0 qbuf=0 qbuf-free=20512 argv-mem=0 multi-mem=0 tot-mem=0 rbs=16384 rbp=0 obl=0 oll=0 omem=0 events=r cmd=client|info user=default library-name= library-ver=\n",
                self.client_id, self.addr, self.client_name, self.db_index, self.subscriptions.len()
            )),
            "LIST" => Resp::bulk_str(format!(
                "id={} addr={} laddr=127.0.0.1:6379 fd=0 name={} age=0 idle=0 flags=N db={} sub={} psub=0 multi=-1 cmd=client|list\n",
                self.client_id, self.addr, self.client_name, self.db_index, self.subscriptions.len()
            )),
            "NO-EVICT" | "CACHING" | "UNPAUSE" | "PAUSE" | "REPLY" | "TRACKING" => Resp::ok(),
            "KILL" => Resp::int(0),
            _ => Resp::ok(),
        }
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
            let result = dispatch(&self.db, cmd_args, self.db_index).await;
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

            if !self.subscriptions.contains(&channel_vec) {
                let mut rx = self.db.pubsub.subscribe(channel_vec.clone());
                self.subscriptions.push(channel_vec.clone());

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

            let sub_count = self.subscriptions.len() as i64;
            let kind = if is_pattern {
                "psubscribe"
            } else {
                "subscribe"
            };
            let frame = Resp::array(vec![
                Resp::bulk_str(kind),
                Resp::bulk(channel_bytes),
                Resp::int(sub_count),
            ]);
            self.write_buf.clear();
            frame.write_to(&mut self.write_buf);
            self.write_buf.finalize();
            let _ = self.stream.write_all_buf(&mut self.write_buf).await;
        }
    }

    async fn do_unsubscribe(&mut self, args: &[Resp], is_pattern: bool) {
        let channels_to_unsub: Vec<Vec<u8>> = if args.len() <= 1 {
            self.subscriptions.clone()
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

        for channel in channels_to_unsub {
            self.subscriptions.retain(|s| s != &channel);
            self.db.pubsub.unsubscribe(&channel);
            let count = self.subscriptions.len() as i64;
            let kind = if is_pattern {
                "punsubscribe"
            } else {
                "unsubscribe"
            };
            let frame = Resp::array(vec![
                Resp::bulk_str(kind),
                Resp::bulk(Bytes::from(channel)),
                Resp::int(count),
            ]);
            self.write_buf.clear();
            frame.write_to(&mut self.write_buf);
            self.write_buf.finalize();
            let _ = self.stream.write_all_buf(&mut self.write_buf).await;
        }
    }

    async fn run_subscribe_mode(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.pubsub_rx.recv() => {
                    let frame = Resp::array(vec![
                        Resp::bulk_str("message"),
                        Resp::bulk(Bytes::from(msg.channel)),
                        Resp::bulk(Bytes::from(msg.payload)),
                    ]);
                    self.write_buf.clear();
                    frame.write_to(&mut self.write_buf);
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
                                let frame = Resp::array(vec![Resp::bulk_str("pong"), payload]);
                                self.write_buf.clear();
                                frame.write_to(&mut self.write_buf);
                                self.write_buf.finalize();
                                let _ = self.stream.write_all_buf(&mut self.write_buf).await;
                            }
                            "QUIT" | "RESET" => {
                                let _ = self.stream.write_all(&Resp::ok().serialize()).await;
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
                                            let _ = dispatch_with_addr(&self.db, args, 0, Some(self.addr)).await;
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
            .eval(&script, keys, argv, self.db.clone(), self.db_index)
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
            .evalsha(&sha, keys, argv, self.db.clone(), self.db_index)
            .await
        {
            Ok(r) => r,
            Err(e) => Resp::error(e.to_string()),
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
            (Resp::bulk_str("version"), Resp::bulk_str("0.1.0")),
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
}
