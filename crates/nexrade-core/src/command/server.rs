//! Server/connection command handlers.

use std::sync::atomic::Ordering;

use crate::command::{get_bytes_vec, get_str};
use crate::db::unix_secs;
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::persistence::Snapshot;
use crate::replication::ReplicationRole;
use crate::resp::Resp;
use crate::store::glob_match;

pub async fn cmd_ping(args: &[Resp]) -> Result<Resp> {
    if args.len() == 1 {
        Ok(Resp::SimpleString("PONG".to_string()))
    } else {
        Ok(args[1].clone())
    }
}

pub async fn cmd_echo(args: &[Resp]) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("echo".to_string()));
    }
    Ok(args[1].clone())
}

pub async fn cmd_quit() -> Result<Resp> {
    Ok(Resp::ok())
}

pub async fn cmd_select(args: &[Resp]) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("select".to_string()));
    }
    // The actual DB index switch is handled by the connection handler
    Ok(Resp::ok())
}

pub async fn cmd_dbsize(db: &Db, db_index: usize) -> Result<Resp> {
    Ok(Resp::int(db.store.db(db_index).len() as i64))
}

pub async fn cmd_flushdb(db: &Db, _args: &[Resp], db_index: usize) -> Result<Resp> {
    db.store.flush_db(db_index);
    Ok(Resp::ok())
}

pub async fn cmd_flushall(db: &Db, _args: &[Resp]) -> Result<Resp> {
    db.store.flush_all();
    Ok(Resp::ok())
}

pub async fn cmd_info(db: &Db, args: &[Resp]) -> Result<Resp> {
    let section = args
        .get(1)
        .and_then(|a| a.as_str())
        .unwrap_or("all")
        .to_lowercase();

    let mut info = String::new();

    if section == "all" || section == "server" {
        info.push_str("# Server\r\n");
        info.push_str("redis_version:7.0.0\r\n");
        info.push_str("nexrade_version:0.1.0\r\n");
        info.push_str("os:Linux\r\n");
        info.push_str("arch_bits:64\r\n");
        info.push_str("multiplexing_api:epoll\r\n");
        info.push_str("atomicvar_api:atomic-builtin\r\n");
        let cfg = db.config.lock();
        info.push_str(&format!("tcp_port:{}\r\n", cfg.port));
        info.push_str(&format!("uptime_in_seconds:{}\r\n", db.stats.uptime_secs()));
        info.push_str(&format!("hz:{}\r\n", cfg.hz));
        drop(cfg);
        info.push_str("executable:nexrade\r\n");
        info.push_str("\r\n");
    }

    if section == "all" || section == "clients" {
        info.push_str("# Clients\r\n");
        info.push_str(&format!(
            "connected_clients:{}\r\n",
            db.stats.active_connections.load(Ordering::Relaxed)
        ));
        info.push_str(&format!(
            "total_connections_received:{}\r\n",
            db.stats.total_connections.load(Ordering::Relaxed)
        ));
        info.push_str("client_recent_max_input_buffer:0\r\n");
        info.push_str("\r\n");
    }

    if section == "all" || section == "stats" {
        info.push_str("# Stats\r\n");
        info.push_str(&format!(
            "total_commands_processed:{}\r\n",
            db.stats.total_commands.load(Ordering::Relaxed)
        ));
        info.push_str(&format!(
            "keyspace_hits:{}\r\n",
            db.stats.keyspace_hits.load(Ordering::Relaxed)
        ));
        info.push_str(&format!(
            "keyspace_misses:{}\r\n",
            db.stats.keyspace_misses.load(Ordering::Relaxed)
        ));
        info.push_str(&format!(
            "instantaneous_ops_per_sec:{}\r\n",
            db.stats.ops_per_sec.load(Ordering::Relaxed)
        ));
        info.push_str("\r\n");
    }

    if section == "all" || section == "memory" {
        let mem = db.store.estimated_memory_bytes();
        info.push_str("# Memory\r\n");
        info.push_str(&format!("used_memory:{}\r\n", mem));
        if mem >= 1_073_741_824 {
            info.push_str(&format!(
                "used_memory_human:{:.2}G\r\n",
                mem as f64 / 1_073_741_824.0
            ));
        } else if mem >= 1_048_576 {
            info.push_str(&format!(
                "used_memory_human:{:.2}M\r\n",
                mem as f64 / 1_048_576.0
            ));
        } else if mem >= 1024 {
            info.push_str(&format!(
                "used_memory_human:{:.2}K\r\n",
                mem as f64 / 1024.0
            ));
        } else {
            info.push_str(&format!("used_memory_human:{}B\r\n", mem));
        }
        info.push_str("used_memory_rss:0\r\n");
        info.push_str("mem_fragmentation_ratio:1.0\r\n");
        info.push_str("\r\n");
    }

    if section == "all" || section == "keyspace" {
        info.push_str("# Keyspace\r\n");
        for i in 0..db.store.db_count {
            let sdb = db.store.db(i);
            let count = sdb.len();
            if count > 0 {
                let expires = sdb.expiry_count();
                info.push_str(&format!(
                    "db{}:keys={},expires={},avg_ttl=0\r\n",
                    i, count, expires
                ));
            }
        }
        info.push_str("\r\n");
    }

    if section == "all" || section == "replication" {
        info.push_str("# Replication\r\n");
        let repl = &db.replication;
        let role = repl.current_role();
        match role {
            ReplicationRole::Primary => {
                info.push_str("role:master\r\n");
                let replicas = repl.connected_replicas.read();
                info.push_str(&format!("connected_slaves:{}\r\n", replicas.len()));
                for (i, r) in replicas.iter().enumerate() {
                    info.push_str(&format!(
                        "slave{}:ip={},port={},state=online,offset={},lag=0\r\n",
                        i,
                        r.addr.ip(),
                        r.addr.port(),
                        r.offset,
                    ));
                }
                drop(replicas);
                info.push_str(&format!("master_replid:{}\r\n", repl.replication_id));
                info.push_str(&format!(
                    "master_repl_offset:{}\r\n",
                    repl.replication_offset.load(Ordering::Relaxed)
                ));
                info.push_str("master_failover_state:no-failover\r\n");
            }
            ReplicationRole::Replica => {
                info.push_str("role:slave\r\n");
                let ro = repl.replica_of.read();
                if let Some((ref host, port)) = *ro {
                    info.push_str(&format!("master_host:{}\r\n", host));
                    info.push_str(&format!("master_port:{}\r\n", port));
                    let link_status = if repl
                        .primary_link_up
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        "up"
                    } else {
                        "down"
                    };
                    info.push_str(&format!("master_link_status:{}\r\n", link_status));
                }
                drop(ro);
                info.push_str(&format!("master_replid:{}\r\n", repl.replication_id));
                info.push_str(&format!(
                    "master_repl_offset:{}\r\n",
                    repl.replication_offset.load(Ordering::Relaxed)
                ));
                info.push_str("connected_slaves:0\r\n");
            }
        }
        info.push_str("\r\n");
    }

    Ok(Resp::bulk_str(info))
}

pub async fn cmd_config(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("config".to_string()));
    }
    let sub = get_str(args, 1, "CONFIG")?.to_uppercase();
    match sub.as_str() {
        "GET" => {
            let pattern = args.get(2).and_then(|a| a.as_str()).unwrap_or("*");
            let cfg = db.config.lock();
            let requirepass_str = cfg.requirepass.as_deref().unwrap_or("").to_string();
            let maxmemory_str = cfg.max_memory.map_or("0".to_string(), |m| m.to_string());
            let appendonly_str = if cfg.persistence.aof_path.is_some() {
                "yes"
            } else {
                "no"
            };
            let save_str = cfg
                .save_rules
                .iter()
                .map(|(s, c)| format!("{} {}", s, c))
                .collect::<Vec<_>>()
                .join(" ");
            let pairs: &[(&str, &dyn std::fmt::Display)] = &[
                ("bind", &cfg.bind as &dyn std::fmt::Display),
                ("port", &cfg.port),
                ("databases", &cfg.databases),
                ("maxclients", &cfg.max_clients),
                ("hz", &cfg.hz),
                ("loglevel", &cfg.loglevel),
                ("slowlog-log-slower-than", &cfg.slowlog_log_slower_than),
                ("slowlog-max-len", &cfg.slowlog_max_len),
                ("timeout", &cfg.timeout),
                ("maxmemory", &maxmemory_str),
                ("maxmemory-policy", &cfg.maxmemory_policy),
                ("requirepass", &requirepass_str),
                ("activerehashing", &"yes"),
                ("appendonly", &appendonly_str),
                ("save", &save_str),
            ];
            let mut result = Vec::new();
            for (key, val) in pairs {
                if glob_match(pattern.as_bytes(), key.as_bytes()) {
                    result.push(Resp::bulk_str(*key));
                    result.push(Resp::bulk_str(val.to_string()));
                }
            }
            Ok(Resp::array(result))
        }
        "SET" => {
            if args.len() < 4 {
                return Err(NexradeError::WrongArity("config set".to_string()));
            }
            let key = get_str(args, 2, "CONFIG SET")?.to_lowercase();
            let val = get_str(args, 3, "CONFIG SET")?;
            let mut cfg = db.config.lock();
            match key.as_str() {
                "requirepass" => {
                    cfg.requirepass = if val.is_empty() {
                        None
                    } else {
                        Some(val.to_string())
                    };
                }
                "maxmemory" => {
                    let bytes = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid maxmemory value".to_string())
                    })?;
                    cfg.max_memory = if bytes == 0 { None } else { Some(bytes) };
                }
                "maxmemory-policy" => {
                    cfg.maxmemory_policy = val.parse().unwrap_or_default();
                }
                "hz" => {
                    cfg.hz = val
                        .parse::<u32>()
                        .map_err(|_| NexradeError::Generic("Invalid hz value".to_string()))?;
                }
                "timeout" => {
                    cfg.timeout = val
                        .parse::<u64>()
                        .map_err(|_| NexradeError::Generic("Invalid timeout value".to_string()))?;
                }
                "slowlog-log-slower-than" => {
                    let us = val.parse::<i64>().map_err(|_| {
                        NexradeError::Generic("Invalid slowlog-log-slower-than value".to_string())
                    })?;
                    cfg.slowlog_log_slower_than = us;
                    drop(cfg);
                    db.slowlog.set_threshold(us.max(0) as u64);
                    return Ok(Resp::ok());
                }
                "slowlog-max-len" => {
                    let len = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid slowlog-max-len value".to_string())
                    })?;
                    cfg.slowlog_max_len = len;
                    drop(cfg);
                    db.slowlog.set_max_len(len);
                    return Ok(Resp::ok());
                }
                "loglevel" => {
                    cfg.loglevel = val.to_string();
                }
                _ => {
                    return Err(NexradeError::Generic(format!(
                        "Unsupported CONFIG parameter: {}",
                        key
                    )));
                }
            }
            Ok(Resp::ok())
        }
        "REWRITE" => Ok(Resp::ok()),
        "RESETSTAT" => {
            db.stats.total_commands.store(0, Ordering::Relaxed);
            db.stats.keyspace_hits.store(0, Ordering::Relaxed);
            db.stats.keyspace_misses.store(0, Ordering::Relaxed);
            db.stats.total_connections.store(0, Ordering::Relaxed);
            db.stats.ops_per_sec.store(0, Ordering::Relaxed);
            Ok(Resp::ok())
        }
        _ => Err(NexradeError::Generic(format!(
            "unknown subcommand '{}'",
            sub
        ))),
    }
}

pub async fn cmd_publish(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("publish".to_string()));
    }
    let channel = get_bytes_vec(args, 1, "PUBLISH")?;
    let payload = get_bytes_vec(args, 2, "PUBLISH")?;
    let count = db.pubsub.publish(channel, payload);
    Ok(Resp::int(count as i64))
}

pub async fn cmd_pubsub(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("pubsub".to_string()));
    }
    let sub = get_str(args, 1, "PUBSUB")?.to_uppercase();
    match sub.as_str() {
        "CHANNELS" => {
            let pattern = args.get(2).and_then(|a| a.as_str());
            let channels = db.pubsub.channel_names();
            let result: Vec<Resp> = channels
                .into_iter()
                .filter(|ch| {
                    pattern
                        .map(|p| glob_match(p.as_bytes(), ch))
                        .unwrap_or(true)
                })
                .map(|ch| Resp::bulk(bytes::Bytes::from(ch)))
                .collect();
            Ok(Resp::array(result))
        }
        "NUMSUB" => {
            let mut result = Vec::new();
            for i in 2..args.len() {
                let ch = get_bytes_vec(args, i, "PUBSUB")?;
                let count = db.pubsub.subscriber_count(&ch);
                result.push(Resp::bulk(bytes::Bytes::from(ch)));
                result.push(Resp::int(count as i64));
            }
            Ok(Resp::array(result))
        }
        "NUMPAT" => Ok(Resp::int(0)),
        _ => Ok(Resp::array(vec![])),
    }
}

pub async fn cmd_command(_args: &[Resp]) -> Result<Resp> {
    // Return basic command count
    Ok(Resp::int(200))
}

pub async fn cmd_save(db: &Db) -> Result<Resp> {
    let rdb_path = db.config.lock().persistence.rdb_path.clone();
    if let Some(path) = rdb_path {
        let dbs = db.store.snapshot_dbs();
        let snapshot = Snapshot::new(dbs);
        snapshot
            .save(&path)
            .map_err(|e| NexradeError::Generic(e.to_string()))?;
        db.stats.dirty_keys.store(0, Ordering::Relaxed);
        db.stats
            .last_save_time
            .store(unix_secs(), Ordering::Relaxed);
    }
    Ok(Resp::ok())
}

pub async fn cmd_bgsave(db: &Db) -> Result<Resp> {
    use std::sync::atomic::Ordering::AcqRel;
    if db.stats.bgsave_in_progress.swap(true, AcqRel) {
        return Ok(Resp::SimpleString(
            "Background saving already in progress".to_string(),
        ));
    }
    let rdb_path = db.config.lock().persistence.rdb_path.clone();
    if let Some(path) = rdb_path {
        let dbs = db.store.snapshot_dbs();
        let stats = db.stats.clone();
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || Snapshot::new(dbs).save(&path)).await;
            match result {
                Ok(Ok(())) => {
                    tracing::info!("BGSAVE completed");
                    stats.dirty_keys.store(0, Ordering::Relaxed);
                    stats.last_save_time.store(unix_secs(), Ordering::Relaxed);
                }
                Ok(Err(e)) => tracing::error!("BGSAVE failed: {}", e),
                Err(e) => tracing::error!("BGSAVE task panicked: {}", e),
            }
            stats.bgsave_in_progress.store(false, Ordering::Release);
        });
    } else {
        db.stats.bgsave_in_progress.store(false, Ordering::Release);
    }
    Ok(Resp::SimpleString("Background saving started".to_string()))
}

pub async fn cmd_bgrewriteaof(db: &Db) -> Result<Resp> {
    let aof_path = db.config.lock().persistence.aof_path.clone();
    let Some(path) = aof_path else {
        return Ok(Resp::error("ERR AOF not enabled"));
    };
    let dbs = db.store.snapshot_dbs();
    let db_clone = db.clone();
    tokio::spawn(async move {
        let path2 = path.clone();
        let result = tokio::task::spawn_blocking(move || {
            crate::persistence::AofWriter::rewrite(&path, &dbs)
        })
        .await;
        match result {
            Ok(Ok(())) => {
                tracing::info!("BGREWRITEAOF completed");
                // Re-open the AOF writer on the rewritten file.
                match crate::persistence::AofWriter::open(&path2) {
                    Ok(writer) => *db_clone.aof_writer.lock() = Some(writer),
                    Err(e) => tracing::error!("failed to reopen AOF after rewrite: {}", e),
                }
            }
            Ok(Err(e)) => tracing::error!("BGREWRITEAOF failed: {}", e),
            Err(e) => tracing::error!("BGREWRITEAOF task panicked: {}", e),
        }
    });
    Ok(Resp::SimpleString(
        "Background append only file rewriting started".to_string(),
    ))
}

pub async fn cmd_lastsave(db: &Db) -> Result<Resp> {
    Ok(Resp::int(
        db.stats.last_save_time.load(Ordering::Relaxed) as i64
    ))
}

pub async fn cmd_debug(args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("debug".to_string()));
    }
    let sub = get_str(args, 1, "DEBUG")?.to_uppercase();
    match sub.as_str() {
        "SLEEP" => Ok(Resp::ok()),
        "SET-ACTIVE-EXPIRE" => Ok(Resp::ok()),
        "JMAP" => Ok(Resp::ok()),
        "RELOAD" => Ok(Resp::ok()),
        "LOADAOF" => Ok(Resp::ok()),
        "QUICKLIST-PACKED-THRESHOLD" => Ok(Resp::ok()),
        "CHANGE-REPL-ID" => Ok(Resp::ok()),
        "AOFSTATS" => Ok(Resp::ok()),
        _ => Ok(Resp::ok()),
    }
}

pub async fn cmd_shutdown(db: &Db, args: &[Resp]) -> Result<Resp> {
    let nosave = args
        .get(1)
        .and_then(|a| a.as_str())
        .is_some_and(|s| s.eq_ignore_ascii_case("NOSAVE"));
    if nosave {
        db.stats.dirty_keys.store(0, Ordering::Relaxed);
    }
    db.shutdown.notify_one();
    Ok(Resp::ok())
}

pub async fn cmd_slowlog(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("slowlog".to_string()));
    }
    let sub = get_str(args, 1, "SLOWLOG")?.to_uppercase();
    match sub.as_str() {
        "GET" => {
            let count = args
                .get(2)
                .and_then(|a| a.as_str())
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(128);
            let entries = db.slowlog.get(count);
            let result = entries
                .into_iter()
                .map(|e| {
                    Resp::array(vec![
                        Resp::int(e.id as i64),
                        Resp::int(e.timestamp as i64),
                        Resp::int(e.duration_us as i64),
                        Resp::array(e.args.iter().map(Resp::bulk_str).collect()),
                        Resp::bulk_str(e.client_addr),
                        Resp::bulk_str(""),
                    ])
                })
                .collect();
            Ok(Resp::array(result))
        }
        "LEN" => Ok(Resp::int(db.slowlog.len() as i64)),
        "RESET" => {
            db.slowlog.reset();
            Ok(Resp::ok())
        }
        _ => Ok(Resp::array(vec![])),
    }
}

pub async fn cmd_memory(_db: &Db, args: &[Resp], _db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("memory".to_string()));
    }
    let sub = get_str(args, 1, "MEMORY")?.to_uppercase();
    match sub.as_str() {
        "USAGE" => Ok(Resp::int(64)), // estimate
        "DOCTOR" => Ok(Resp::bulk_str("Memory is OK")),
        "STATS" => Ok(Resp::array(vec![])),
        "MALLOC-STATS" => Ok(Resp::bulk_str("malloc stats not available")),
        "HELP" => Ok(Resp::array(vec![Resp::bulk_str("MEMORY USAGE <key>")])),
        _ => Ok(Resp::int(0)),
    }
}

pub async fn cmd_latency(args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("latency".to_string()));
    }
    let sub = get_str(args, 1, "LATENCY")?.to_uppercase();
    match sub.as_str() {
        "LATEST" => Ok(Resp::array(vec![])),
        "HISTORY" => Ok(Resp::array(vec![])),
        "RESET" => Ok(Resp::int(0)),
        _ => Ok(Resp::array(vec![])),
    }
}

pub async fn cmd_acl(args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("acl".to_string()));
    }
    let sub = get_str(args, 1, "ACL")?.to_uppercase();
    match sub.as_str() {
        "WHOAMI" => Ok(Resp::bulk_str("default")),
        "LIST" => Ok(Resp::array(vec![Resp::bulk_str(
            "user default on nopass ~* &* +@all",
        )])),
        "USERS" => Ok(Resp::array(vec![Resp::bulk_str("default")])),
        "CAT" => Ok(Resp::array(vec![])),
        "GETUSER" => Ok(Resp::array(vec![])),
        "SETUSER" => Ok(Resp::ok()),
        "DELUSER" => Ok(Resp::int(0)),
        "SAVE" => Ok(Resp::ok()),
        "LOAD" => Ok(Resp::ok()),
        "LOG" => Ok(Resp::array(vec![])),
        "GENPASS" => Ok(Resp::bulk_str(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )),
        _ => Ok(Resp::array(vec![])),
    }
}

pub async fn cmd_reset() -> Result<Resp> {
    Ok(Resp::SimpleString("RESET".to_string()))
}

// ── Replication commands ──────────────────────────────────────────────────────

/// REPLICAOF NO ONE | REPLICAOF <host> <port>
pub async fn cmd_replicaof(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("replicaof".to_string()));
    }
    let first = get_str(args, 1, "REPLICAOF")?.to_uppercase();
    if first == "NO" {
        // REPLICAOF NO ONE — promote to primary.
        *db.replication.role.write() = ReplicationRole::Primary;
        *db.replication.replica_of.write() = None;
        db.replication.replica_notify.notify_one();
        return Ok(Resp::ok());
    }

    // REPLICAOF <host> <port>
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("replicaof".to_string()));
    }
    // Re-read the raw argument (not uppercased) as the host.
    let host = get_str(args, 1, "REPLICAOF")?.to_string();
    let port_str = get_str(args, 2, "REPLICAOF")?;
    let port: u16 = port_str
        .parse()
        .map_err(|_| NexradeError::Generic("invalid port".to_string()))?;

    *db.replication.role.write() = ReplicationRole::Replica;
    *db.replication.replica_of.write() = Some((host, port));
    // Notify the background replication task to (re-)connect.
    db.replication.replica_notify.notify_one();
    Ok(Resp::ok())
}

/// REPLCONF <subcommand> [<arg> ...]
pub async fn cmd_replconf(
    db: &Db,
    args: &[Resp],
    peer_addr: Option<std::net::SocketAddr>,
) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("replconf".to_string()));
    }
    let sub = get_str(args, 1, "REPLCONF")?.to_uppercase();
    match sub.as_str() {
        "LISTENING-PORT" | "CAPA" => {
            // Acknowledge capability and port advertisements from replicas.
            Ok(Resp::ok())
        }
        "ACK" => {
            // Replica is reporting its replication offset.
            let offset_str = args
                .get(2)
                .and_then(|a| a.as_str())
                .ok_or_else(|| NexradeError::WrongArity("REPLCONF ACK".to_string()))?;
            let offset = offset_str.parse::<u64>().map_err(|_| {
                NexradeError::Generic("ERR value is not an integer or out of range".to_string())
            })?;
            if let Some(addr) = peer_addr {
                db.replication.update_replica_offset(addr, offset);
            }
            Ok(Resp::ok())
        }
        _ => Ok(Resp::ok()),
    }
}

/// PSYNC <replid> <offset>
///
/// Returns `Ok(Resp::SimpleString("PSYNC_FULLRESYNC"))` as a sentinel so the
/// connection handler can take over and stream the RDB + ongoing commands.
pub async fn cmd_psync(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("psync".to_string()));
    }
    let current_offset = db.replication.replication_offset.load(Ordering::Relaxed);
    // Signal the connection handler to enter replication streaming mode.
    // The actual FULLRESYNC response + RDB sending is handled there to avoid
    // holding a Db borrow across await points while writing raw bytes.
    let sentinel = format!(
        "PSYNC_FULLRESYNC {} {}",
        db.replication.replication_id, current_offset
    );
    Ok(Resp::SimpleString(sentinel))
}

pub async fn cmd_client(args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("client".to_string()));
    }
    let sub = get_str(args, 1, "CLIENT")?.to_uppercase();
    match sub.as_str() {
        "GETNAME" => Ok(Resp::null()),
        "SETNAME" => Ok(Resp::ok()),
        "ID" => Ok(Resp::int(1)),
        "INFO" => Ok(Resp::bulk_str("id=1 addr=127.0.0.1:6379 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 obl=0 oll=0 omem=0 events=r cmd=client")),
        "LIST" => Ok(Resp::bulk_str("")),
        "KILL" => Ok(Resp::int(0)),
        "NO-EVICT" => Ok(Resp::ok()),
        "CACHING" => Ok(Resp::ok()),
        "UNPAUSE" => Ok(Resp::ok()),
        "PAUSE" => Ok(Resp::ok()),
        "REPLY" => Ok(Resp::ok()),
        "TRACKING" => Ok(Resp::ok()),
        _ => Ok(Resp::ok()),
    }
}

pub async fn cmd_cluster(args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("cluster".to_string()));
    }
    let sub = get_str(args, 1, "CLUSTER")?.to_uppercase();
    match sub.as_str() {
        "INFO" => Ok(Resp::bulk_str("cluster_enabled:0\r\ncluster_state:ok\r\n")),
        "MYID" => Ok(Resp::bulk_str("0000000000000000000000000000000000000000")),
        _ => Ok(Resp::ok()),
    }
}

pub async fn cmd_hello(_args: &[Resp]) -> Result<Resp> {
    // HELLO protocol negotiation (simplified)
    Ok(Resp::array(vec![
        Resp::bulk_str("server"),
        Resp::bulk_str("nexrade"),
        Resp::bulk_str("version"),
        Resp::bulk_str("0.1.0"),
        Resp::bulk_str("proto"),
        Resp::int(2),
        Resp::bulk_str("id"),
        Resp::int(1),
        Resp::bulk_str("mode"),
        Resp::bulk_str("standalone"),
        Resp::bulk_str("role"),
        Resp::bulk_str("master"),
        Resp::bulk_str("modules"),
        Resp::array(vec![]),
    ]))
}
