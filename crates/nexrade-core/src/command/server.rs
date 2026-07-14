//! Server/connection command handlers.

use std::sync::atomic::Ordering;

use crate::cluster;
use crate::command::{get_bytes_vec, get_i64, get_str};
use crate::conn_registry::{format_client_list_line, CLIENT_FLAG_NO_EVICT};
use crate::db::unix_secs;
use crate::db::Db;
use crate::error::{NexradeError, Result};
#[cfg(not(target_arch = "wasm32"))]
use crate::persistence::Snapshot;
#[cfg(not(target_arch = "wasm32"))]
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

/// Map the `bgsave_last_status` numeric (0 ok, 1 err) to Redis's string form
/// for the `INFO persistence` section. Anything other than the explicit
/// error code is reported as `ok` so a stale value never misleads the
/// operator.
fn bgsave_status_str(code: u8) -> &'static str {
    if code == 1 {
        "err"
    } else {
        "ok"
    }
}

/// Same shape for `aof_rewrite_last_status`. `in_progress` is a separate
/// flag (`aof_rewrite_in_progress`), so a stale 0 doesn't conflict with an
/// actively-running rewrite — the field name is past-tense.
fn aof_rewrite_status_str(code: u8) -> &'static str {
    if code == 1 {
        "err"
    } else {
        "ok"
    }
}

/// `aof_last_write_status` — Redis reports `ok` whenever AOF is enabled
/// (since failed writes propagate as `ERR` to the connected client at
/// write time). Reporting `err` here when AOF is off keeps the field
/// honest about whether any AOF path is even active.
fn aof_last_write_status(aof_enabled: bool) -> &'static str {
    if aof_enabled {
        "ok"
    } else {
        "err"
    }
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
        // Use the package version that was compiled in, so `INFO server`
        // can never drift from the workspace version at release time.
        info.push_str(&format!(
            "nexrade_version:{}\r\n",
            env!("CARGO_PKG_VERSION")
        ));
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
        let rss = crate::resource::resident_set_size();
        info.push_str(&format!("used_memory_rss:{}\r\n", rss));
        // Fragmentation ratio = RSS / live dataset bytes, same definition
        // Redis uses. Falls back to 1.0 when either side is unavailable
        // (e.g. unsupported platform, or an empty dataset) rather than
        // reporting a misleading 0.
        let frag_ratio = if mem > 0 && rss > 0 {
            rss as f64 / mem as f64
        } else {
            1.0
        };
        info.push_str(&format!("mem_fragmentation_ratio:{:.2}\r\n", frag_ratio));
        info.push_str("\r\n");
    }

    #[cfg(not(target_arch = "wasm32"))]
    if section == "all" || section == "persistence" {
        info.push_str("# Persistence\r\n");
        info.push_str("loading:0\r\n");
        info.push_str(&format!(
            "rdb_changes_since_last_save:{}\r\n",
            db.stats.dirty_keys.load(Ordering::Relaxed)
        ));
        info.push_str(&format!(
            "rdb_bgsave_in_progress:{}\r\n",
            db.stats.bgsave_in_progress.load(Ordering::Relaxed) as u8
        ));
        info.push_str(&format!(
            "rdb_last_save_time:{}\r\n",
            db.stats.last_save_time.load(Ordering::Relaxed)
        ));
        info.push_str(&format!(
            "rdb_last_bgsave_status:{}\r\n",
            bgsave_status_str(db.stats.bgsave_last_status.load(Ordering::Relaxed))
        ));
        info.push_str("rdb_last_cow_size:0\r\n");
        info.push_str(&format!(
            "aof_enabled:{}\r\n",
            db.stats.aof_enabled.load(Ordering::Relaxed) as u8
        ));
        info.push_str(&format!(
            "aof_rewrite_in_progress:{}\r\n",
            db.stats.aof_rewrite_in_progress.load(Ordering::Relaxed) as u8
        ));
        info.push_str(&format!(
            "aof_last_bgrewrite_status:{}\r\n",
            aof_rewrite_status_str(db.stats.aof_rewrite_last_status.load(Ordering::Relaxed))
        ));
        info.push_str(&format!(
            "aof_last_write_status:{}\r\n",
            aof_last_write_status(db.stats.aof_enabled.load(Ordering::Relaxed))
        ));
        info.push_str("\r\n");
    }
    #[cfg(target_arch = "wasm32")]
    if section == "all" || section == "persistence" {
        info.push_str("# Persistence\r\nloading:0\r\naof_enabled:0\r\n\r\n");
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

    #[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(target_arch = "wasm32")]
    if section == "all" || section == "replication" {
        info.push_str("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n\r\n");
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
            let appendonly_str = {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    if cfg.persistence.aof_path.is_some() {
                        "yes"
                    } else {
                        "no"
                    }
                }
                #[cfg(target_arch = "wasm32")]
                {
                    "no"
                }
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
                    db.max_memory_limit.store(
                        cfg.max_memory.unwrap_or(0),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                "maxmemory-policy" => {
                    cfg.maxmemory_policy = val.parse().unwrap_or_default();
                    db.maxmemory_policy.store(
                        cfg.maxmemory_policy.clone() as u8,
                        std::sync::atomic::Ordering::Relaxed,
                    );
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

#[cfg(not(target_arch = "wasm32"))]
pub async fn cmd_save(db: &Db) -> Result<Resp> {
    let rdb_path = db.config.lock().persistence.rdb_path.clone();
    if let Some(path) = rdb_path {
        let dbs = db.store.snapshot_dbs();
        let snapshot = Snapshot::new(dbs);
        match snapshot.save(&path) {
            Ok(()) => {
                db.stats.dirty_keys.store(0, Ordering::Relaxed);
                db.stats
                    .last_save_time
                    .store(unix_secs(), Ordering::Relaxed);
                db.stats.bgsave_last_status.store(0, Ordering::Relaxed);
                Ok(Resp::ok())
            }
            Err(e) => {
                // Same flag path as BGSAVE — the operator looking at
                // `INFO persistence` shouldn't have to know which save
                // command was used to see a failure.
                db.stats.bgsave_last_status.store(1, Ordering::Relaxed);
                Err(NexradeError::Generic(e.to_string()))
            }
        }
    } else {
        Ok(Resp::ok())
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn cmd_save(_db: &Db) -> Result<Resp> {
    Ok(Resp::error("ERR persistence not available in WASM mode"))
}

#[cfg(not(target_arch = "wasm32"))]
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
                    stats.bgsave_last_status.store(0, Ordering::Relaxed);
                }
                Ok(Err(e)) => {
                    tracing::error!("BGSAVE failed: {}", e);
                    // Stays at the existing time (no successful save), but
                    // the status flips to `err` so `INFO persistence`
                    // reflects the failure rather than stale `ok`.
                    stats.bgsave_last_status.store(1, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::error!("BGSAVE task panicked: {}", e);
                    stats.bgsave_last_status.store(1, Ordering::Relaxed);
                }
            }
            stats.bgsave_in_progress.store(false, Ordering::Release);
        });
    } else {
        db.stats.bgsave_in_progress.store(false, Ordering::Release);
    }
    Ok(Resp::SimpleString("Background saving started".to_string()))
}

#[cfg(target_arch = "wasm32")]
pub async fn cmd_bgsave(_db: &Db) -> Result<Resp> {
    Ok(Resp::error("ERR persistence not available in WASM mode"))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn cmd_bgrewriteaof(db: &Db) -> Result<Resp> {
    use std::sync::atomic::Ordering::AcqRel;

    // Only one concurrent rewrite is allowed. Like Redis, a second
    // BGREWRITEAOF while one is in flight is rejected (rather than
    // queueing), so the existing `aof_rewrite_in_progress` flag doubles
    // as a concurrency lock for the rewrite.
    if db.stats.aof_rewrite_in_progress.swap(true, AcqRel) {
        return Ok(Resp::SimpleString(
            "Background append only file rewriting already in progress".to_string(),
        ));
    }

    let aof_path = db.config.lock().persistence.aof_path.clone();
    let Some(path) = aof_path else {
        // Refund the flag — we never actually started.
        db.stats
            .aof_rewrite_in_progress
            .store(false, Ordering::Release);
        return Ok(Resp::error("ERR AOF not enabled"));
    };
    let dbs = db.store.snapshot_dbs();
    let acl_lines = db.acl.list();
    let db_clone = db.clone();
    let stats = db.stats.clone();
    tokio::spawn(async move {
        let path2 = path.clone();
        let result = tokio::task::spawn_blocking(move || {
            crate::persistence::AofWriter::rewrite(&path, &dbs, &acl_lines)
        })
        .await;
        match result {
            Ok(Ok(())) => {
                tracing::info!("BGREWRITEAOF completed");
                stats.aof_rewrite_last_status.store(0, Ordering::Relaxed);
                // Re-open the AOF writer on the rewritten file.
                match crate::persistence::AofWriter::open(&path2) {
                    Ok(writer) => *db_clone.aof_writer.lock() = Some(writer),
                    Err(e) => tracing::error!("failed to reopen AOF after rewrite: {}", e),
                }
            }
            Ok(Err(e)) => {
                tracing::error!("BGREWRITEAOF failed: {}", e);
                stats.aof_rewrite_last_status.store(1, Ordering::Relaxed);
            }
            Err(e) => {
                tracing::error!("BGREWRITEAOF task panicked: {}", e);
                stats.aof_rewrite_last_status.store(1, Ordering::Relaxed);
            }
        }
        stats
            .aof_rewrite_in_progress
            .store(false, Ordering::Release);
    });
    Ok(Resp::SimpleString(
        "Background append only file rewriting started".to_string(),
    ))
}

#[cfg(target_arch = "wasm32")]
pub async fn cmd_bgrewriteaof(_db: &Db) -> Result<Resp> {
    Ok(Resp::error("ERR persistence not available in WASM mode"))
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

/// `WAIT numreplicas timeout`
///
/// Blocks until the write offset has been acknowledged by `numreplicas`
/// replicas, or `timeout` ms elapses. In standalone mode (no replicas)
/// Redis returns 0 — that's what we return here too.
pub async fn cmd_wait(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("wait".to_string()));
    }
    // We don't honor numreplicas/timeout because we have no replicas — but
    // accepting the arguments (and validating them) keeps redis-py happy.
    let _num = get_i64(args, 1, "WAIT")?;
    let _timeout = get_i64(args, 2, "WAIT")?;

    // Suppress unused warning on `db` (used implicitly via context).
    let _ = db;

    Ok(Resp::int(0))
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

pub async fn cmd_memory(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("memory".to_string()));
    }
    let sub = get_str(args, 1, "MEMORY")?.to_uppercase();
    match sub.as_str() {
        "USAGE" => {
            // MEMORY USAGE <key> [SAMPLES <count>]
            if args.len() < 3 {
                return Err(NexradeError::WrongArity("memory usage".to_string()));
            }
            let key = get_bytes_vec(args, 2, "MEMORY")?;

            // Optional SAMPLES <count>.
            let mut _samples: Option<i64> = None;
            let mut i = 3;
            while i < args.len() {
                let opt = get_str(args, i, "MEMORY")?.to_uppercase();
                if opt == "SAMPLES" && i + 1 < args.len() {
                    let n = get_i64(args, i + 1, "MEMORY")?;
                    if !(1..=1000).contains(&n) {
                        return Err(NexradeError::Generic(
                            "ERR samples must be between 1 and 1000".to_string(),
                        ));
                    }
                    _samples = Some(n);
                    i += 2;
                } else {
                    return Err(NexradeError::Generic("syntax error".to_string()));
                }
            }

            let sdb = db.store.db(db_index).read_for(&key);
            match sdb.get_ro(&key) {
                Some(e) => {
                    let sz = entry_memory_bytes(&key, e);
                    Ok(Resp::int(sz as i64))
                }
                None => Ok(Resp::null()),
            }
        }
        "STATS" => Ok(memory_stats(db)),
        "DOCTOR" => Ok(memory_doctor(db)),
        "PURGE" => {
            // We don't have an explicit allocator that can release pages back
            // to the OS, but we can drop free lists / defragment. Without
            // jemalloc stats this is a no-op, so we just report a status.
            Ok(Resp::SimpleString("OK".to_string()))
        }
        "MALLOC-STATS" => Ok(Resp::bulk_str(
            "Stats not available in this build".to_string(),
        )),
        "HELP" => Ok(Resp::array(vec![
            Resp::bulk_str(
                "MEMORY USAGE <key> [SAMPLES <count>] -- Estimate memory usage of a key",
            ),
            Resp::bulk_str("MEMORY STATS                         -- Show memory usage statistics"),
            Resp::bulk_str("MEMORY DOCTOR                        -- Output memory problems report"),
            Resp::bulk_str("MEMORY PURGE                         -- Try to free memory"),
            Resp::bulk_str(
                "MEMORY MALLOC-STATS                  -- Show allocator statistics (if available)",
            ),
            Resp::bulk_str("MEMORY HELP                          -- Show this help"),
        ])),
        _ => Ok(Resp::int(0)),
    }
}

/// Approximate the in-memory footprint of a single entry — same formula
/// used by `Database::insert` to update the `live_bytes` counter.
fn entry_memory_bytes(key: &[u8], entry: &crate::store::Entry) -> usize {
    use crate::types::DataType;
    const OVERHEAD: usize = 64;
    let value_sz = match &entry.value {
        DataType::String(v) => v.len(),
        // Fixed-size atomic cell — no `.len()` to call; 8 bytes for the i64.
        DataType::Int(_) => 8,
        DataType::List(l) => l.iter().map(|b| b.len()).sum(),
        DataType::Set(s) => s.iter().map(|v| v.len()).sum(),
        DataType::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len()).sum(),
        DataType::Bitmap(v) => v.len(),
        DataType::HyperLogLog(v) => v.len(),
        DataType::ZSet(z) => z.members.keys().map(|m| m.len() + 8).sum(),
        DataType::Stream(s) => s.estimated_size(),
        DataType::Geo(g) => g.members.len() * 24,
    };
    OVERHEAD + key.len() + value_sz
}

/// `MEMORY STATS` — returns allocator stats as a flat array of field/value
/// pairs, mirroring Redis's layout. `peak.allocated` etc. are best-effort
/// approximations from our own counters.
fn memory_stats(db: &Db) -> Resp {
    let live = db.store.estimated_memory_bytes();
    let total_keys = db.store.total_keys();
    let per_key = if total_keys > 0 { live / total_keys } else { 0 };
    let rss = crate::resource::resident_set_size();
    // allocator.resident should reflect real RSS, not just our own
    // live-bytes estimate — fall back to `live` when RSS can't be read
    // (unsupported platform) so the field is never a misleading 0.
    let resident = if rss > 0 { rss } else { live };
    let frag_bytes = resident.saturating_sub(live);
    let frag_ratio = if live > 0 && rss > 0 {
        rss as f64 / live as f64
    } else {
        1.0
    };
    // (key, value) pairs flattened into a single array.
    let mut pairs: Vec<Resp> = Vec::with_capacity(30);
    let fields: &[(&str, String)] = &[
        ("peak.allocated", live.to_string()),
        ("total.allocated", live.to_string()),
        ("startup.allocated", "0".to_string()),
        ("replication.backlog", "0".to_string()),
        ("clients.normal", "0".to_string()),
        ("cluster.links", "0".to_string()),
        ("keys.count", total_keys.to_string()),
        ("keys.bytes-per-key", per_key.to_string()),
        ("dataset.bytes", live.to_string()),
        ("used.memory.peak", live.to_string()),
        ("used.memory.dataset.percent", "0".to_string()),
        ("allocator.allocated", live.to_string()),
        ("allocator.active", resident.to_string()),
        ("allocator.resident", resident.to_string()),
        (
            "allocator-fragmentation.ratio",
            format!("{:.2}", frag_ratio),
        ),
        ("allocator-fragmentation.bytes", frag_bytes.to_string()),
    ];
    for (k, v) in fields {
        pairs.push(Resp::bulk_str(*k));
        pairs.push(Resp::bulk_str(v.clone()));
    }
    Resp::array(pairs)
}

/// `MEMORY DOCTOR` — return observations + recommendations based on current
/// store stats. Real Redis produces a longer multi-line report; we emit a
/// concise version with the same shape.
fn memory_doctor(db: &Db) -> Resp {
    let mut lines: Vec<String> = Vec::new();
    lines.push(
        "Hi, nexrade-cache memory doctor. I'm running in a small VM with limited memory."
            .to_string(),
    );
    let live = db.store.estimated_memory_bytes();
    lines.push(format!("- Total live-bytes across shards: {live}"));
    let total_keys = db.store.total_keys();
    lines.push(format!("- Total keys: {total_keys}"));

    let ratio = if total_keys > 0 { live / total_keys } else { 0 };
    let ratio_str = if ratio > 4096 {
        format!("- Average per-key overhead is {ratio} bytes — check for unusually large values.")
    } else {
        format!("- Average per-key overhead is {ratio} bytes (looks healthy).")
    };
    lines.push(ratio_str);

    lines.push(
        "Recommendations: bump maxmemory if you're seeing OOM, or switch to a more aggressive \
         maxmemory-policy if eviction is hurting your workload."
            .to_string(),
    );

    Resp::bulk_str(lines.join("\n"))
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

pub async fn cmd_acl(db: &Db, args: &[Resp], authenticated_user: &str) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("acl".to_string()));
    }
    let sub = get_str(args, 1, "ACL")?.to_uppercase();
    match sub.as_str() {
        // Report the connection's actual authenticated identity, not
        // some arbitrary user from the ACL list.
        "WHOAMI" => Ok(Resp::bulk_str(authenticated_user.to_string())),
        "LIST" => Ok(Resp::array(
            db.acl.list().into_iter().map(Resp::bulk_str).collect(),
        )),
        "USERS" => Ok(Resp::array(
            db.acl
                .list_users()
                .into_iter()
                .map(Resp::bulk_str)
                .collect::<Vec<_>>(),
        )),
        "CAT" => {
            // ACL CAT [category]
            if args.len() == 2 {
                Ok(Resp::array(
                    crate::acl::category_names()
                        .into_iter()
                        .map(Resp::bulk_str)
                        .collect(),
                ))
            } else {
                let cat = get_str(args, 2, "ACL")?;
                let map = crate::acl::command_category_map();
                match map.get(cat) {
                    Some(cmds) => Ok(Resp::array(
                        cmds.iter().map(|s| Resp::bulk_str(*s)).collect(),
                    )),
                    None => Err(NexradeError::Generic(format!(
                        "ERR unknown command category '{cat}'"
                    ))),
                }
            }
        }
        "GENPASS" => {
            // ACL GENPASS [bits]
            let bits = if args.len() >= 3 {
                Some(get_i64(args, 2, "ACL")? as usize)
            } else {
                None
            };
            let bits = bits.unwrap_or(64);
            Ok(Resp::bulk_str(db.acl.genpass(bits)))
        }
        "GETUSER" => {
            // ACL GETUSER username
            if args.len() < 3 {
                return Err(NexradeError::WrongArity("acl".to_string()));
            }
            let name = get_str(args, 2, "ACL")?;
            match db.acl.get_user(name) {
                Some(u) => {
                    let mut out: Vec<Resp> = Vec::new();
                    for (key, val) in [
                        (
                            "flags",
                            vec![Resp::bulk_str(if u.enabled { "on" } else { "off" })],
                        ),
                        (
                            "passwords",
                            vec![match u.password_hash {
                                Some(h) => Resp::bulk_str(h),
                                None => Resp::null(),
                            }],
                        ),
                        ("categories", vec![Resp::bulk_str("+@all")]),
                        (
                            "commands",
                            u.command_rules
                                .iter()
                                .map(|(r, s)| {
                                    Resp::bulk_str(if *s {
                                        format!("+{r}")
                                    } else {
                                        format!("-{r}")
                                    })
                                })
                                .collect::<Vec<_>>(),
                        ),
                        (
                            "keys",
                            u.key_patterns
                                .iter()
                                .map(|p| Resp::bulk_str(format!("~{p}")))
                                .collect::<Vec<_>>(),
                        ),
                        (
                            "selectors",
                            u.command_rules
                                .iter()
                                .map(|(r, s)| {
                                    Resp::array(vec![Resp::bulk_str(if *s {
                                        format!("+@{r}")
                                    } else {
                                        format!("-@{r}")
                                    })])
                                })
                                .collect::<Vec<_>>(),
                        ),
                    ] {
                        out.push(Resp::bulk_str(key));
                        for v in val {
                            out.push(v);
                        }
                    }
                    Ok(Resp::array(out))
                }
                None => Err(NexradeError::Generic(format!("ERR no such user '{name}'"))),
            }
        }
        "SETUSER" => {
            // ACL SETUSER username [rule ...]
            if args.len() < 3 {
                return Err(NexradeError::WrongArity("acl".to_string()));
            }
            let name = get_str(args, 2, "ACL")?;
            // Collect rule tokens 3..args.len() as &str slices via Resp::as_str.
            let rules: Vec<&str> = (3..args.len()).filter_map(|i| args[i].as_str()).collect();
            db.acl
                .setuser(name, &rules)
                .map_err(|e| NexradeError::Prefixed(e.to_string()))?;
            Ok(Resp::ok())
        }
        "DELUSER" => {
            // ACL DELUSER username [username ...]
            let mut deleted = 0i64;
            for i in 2..args.len() {
                if let Ok(name) = get_str(args, i, "ACL") {
                    if db.acl.deluser(name) {
                        deleted += 1;
                    }
                }
            }
            Ok(Resp::int(deleted))
        }
        "LOG" => {
            // ACL LOG [count | RESET]
            if args.len() >= 3 {
                let opt = get_str(args, 2, "ACL")?.to_uppercase();
                if opt == "RESET" {
                    db.acl.acl_log_reset();
                    return Ok(Resp::ok());
                }
            }
            let count = if args.len() >= 3 {
                Some(get_i64(args, 2, "ACL")? as usize)
            } else {
                None
            };
            let log = db.acl.acl_log(count);
            let out: Vec<Resp> = log
                .into_iter()
                .map(|e| {
                    Resp::array(vec![
                        Resp::int(e.timestamp_ms),
                        Resp::int(0), // reason length placeholder
                        Resp::bulk_str(e.user.to_string()),
                        Resp::bulk_str(e.reason),
                    ])
                })
                .collect();
            Ok(Resp::array(out))
        }
        "SAVE" | "LOAD" => {
            // Persistence is handled by the persistence layer; we don't
            // need to do anything specific here. Reply OK.
            Ok(Resp::ok())
        }
        "DRYRUN" => {
            // ACL DRYRUN <user> <command> [args ...]
            if args.len() < 4 {
                return Err(NexradeError::WrongArity("acl".to_string()));
            }
            let user = get_str(args, 2, "ACL")?;
            let cmd = get_str(args, 3, "ACL")?.to_ascii_uppercase();
            let keys: Vec<&[u8]> = (4..args.len())
                .filter_map(|i| args[i].as_bytes())
                .map(|b| b.as_ref())
                .collect();
            match db.acl.check_permission(user, &cmd, &keys) {
                Ok(()) => Ok(Resp::SimpleString("OK".to_string())),
                Err(e) => Err(NexradeError::Prefixed(e.to_string())),
            }
        }
        _ => Err(NexradeError::Generic(format!(
            "ERR unknown ACL subcommand '{sub}'"
        ))),
    }
}

pub async fn cmd_reset() -> Result<Resp> {
    Ok(Resp::SimpleString("RESET".to_string()))
}

// ── Replication commands ──────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
/// REPLICAOF NO ONE | REPLICAOF <host> <port>
pub async fn cmd_replicaof(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("replicaof".to_string()));
    }
    let first = get_str(args, 1, "REPLICAOF")?.to_uppercase();
    if first == "NO" {
        // REPLICAOF NO ONE — promote to primary.
        db.replication.set_role(ReplicationRole::Primary);
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

    db.replication.set_role(ReplicationRole::Replica);
    *db.replication.replica_of.write() = Some((host, port));
    // Notify the background replication task to (re-)connect.
    db.replication.replica_notify.notify_one();
    Ok(Resp::ok())
}

#[cfg(not(target_arch = "wasm32"))]
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
                NexradeError::Generic("value is not an integer or out of range".to_string())
            })?;
            if let Some(addr) = peer_addr {
                db.replication.update_replica_offset(addr, offset);
            }
            Ok(Resp::ok())
        }
        _ => Ok(Resp::ok()),
    }
}

#[cfg(not(target_arch = "wasm32"))]
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

pub async fn cmd_client(db: &Db, args: &[Resp], caller_id: u64) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("client".to_string()));
    }
    let sub = get_str(args, 1, "CLIENT")?.to_uppercase();
    match sub.as_str() {
        "ID" => Ok(Resp::int(caller_id as i64)),
        "GETNAME" => Ok(client_getname(db, caller_id)),
        "SETNAME" => client_setname(db, args, caller_id),
        "INFO" => Ok(client_info(db, caller_id)),
        "LIST" => Ok(client_list(db)),
        "KILL" => client_kill(db, args, caller_id),
        "PAUSE" => client_pause(db, args),
        "UNPAUSE" => {
            db.connections.unpause();
            Ok(Resp::ok())
        }
        "NO-EVICT" => client_no_evict(db, caller_id),
        // The subcommands below are intercepted by the connection
        // handler (see `connection.rs::handle_client`) and never reach
        // here. They're matched explicitly to keep `cmd_client` from
        // silently swallowing them as `unknown`.
        "REPLY" => Ok(Resp::ok()),
        "TRACKING" | "CACHING" | "TRACKINGINFO" => Err(NexradeError::Generic(
            "ERR CLIENT subcommand not supported via dispatch".to_string(),
        )),
        "HELP" => Ok(client_help()),
        other => Err(NexradeError::Generic(format!(
            "ERR unknown CLIENT subcommand '{other}'"
        ))),
    }
}

fn client_getname(db: &Db, caller_id: u64) -> Resp {
    match db.connections.meta(caller_id) {
        Some(m) => {
            let g = m.read();
            if g.name.is_empty() {
                Resp::null()
            } else {
                Resp::bulk_str(g.name.clone())
            }
        }
        None => Resp::null(),
    }
}

fn client_setname(db: &Db, args: &[Resp], caller_id: u64) -> Result<Resp> {
    let name = match args.get(2).and_then(|a| a.as_str()) {
        Some(s) => s,
        None => {
            return Err(NexradeError::WrongArity("client|setname".to_string()));
        }
    };
    if name.contains(' ') || name.contains('\n') || name.contains('\r') {
        return Err(NexradeError::Prefixed(
            "ERR Client names cannot contain spaces, newlines or special characters.".to_string(),
        ));
    }
    if let Some(m) = db.connections.meta(caller_id) {
        m.write().name = name.to_string();
    }
    Ok(Resp::ok())
}

fn client_info(db: &Db, caller_id: u64) -> Resp {
    match db.connections.meta(caller_id) {
        Some(m) => {
            let g = m.read();
            // Single line, no trailing newline — matches Redis behavior.
            Resp::bulk_str(format_client_list_line(&g))
        }
        None => Resp::bulk_str(""),
    }
}

fn client_list(db: &Db) -> Resp {
    let snapshot = db.connections.snapshot();
    if snapshot.is_empty() {
        return Resp::bulk_str("");
    }
    let mut lines: Vec<String> = snapshot
        .iter()
        .map(|m| format_client_list_line(&m.read()))
        .collect();
    // Sort by id for deterministic output (matches Redis, which sorts by
    // ascending client id).
    lines.sort_by(|a, b| {
        let aid = a
            .split_whitespace()
            .find_map(|f| f.strip_prefix("id="))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let bid = b
            .split_whitespace()
            .find_map(|f| f.strip_prefix("id="))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        aid.cmp(&bid)
    });
    let joined = lines.join("\n");
    Resp::bulk_str(joined)
}

#[derive(Default, Debug)]
struct KillFilter {
    id: Option<u64>,
    typ: Option<String>,
    addr: Option<String>,
    laddr: Option<String>,
    user: Option<String>,
    skipme: bool, // default true
}

fn parse_kill_filter(args: &[Resp], caller_id: u64) -> Result<KillFilter> {
    let mut f = KillFilter {
        skipme: true,
        ..Default::default()
    };
    let mut i = 2;
    while i < args.len() {
        let opt = match args[i].as_str() {
            Some(s) => s.to_ascii_uppercase(),
            None => return Err(NexradeError::Generic("ERR syntax error".to_string())),
        };
        match opt.as_str() {
            "ID" => {
                let v = args.get(i + 1).and_then(|a| a.as_str()).unwrap_or("");
                let id: u64 = v.parse().map_err(|_| NexradeError::NotInteger)?;
                f.id = Some(id);
                i += 2;
            }
            "TYPE" => {
                f.typ = Some(
                    args.get(i + 1)
                        .and_then(|a| a.as_str())
                        .map(|s| s.to_ascii_uppercase())
                        .unwrap_or_default(),
                );
                i += 2;
            }
            "ADDR" => {
                f.addr = Some(
                    args.get(i + 1)
                        .and_then(|a| a.as_str())
                        .unwrap_or("")
                        .to_string(),
                );
                i += 2;
            }
            "LADDR" => {
                f.laddr = Some(
                    args.get(i + 1)
                        .and_then(|a| a.as_str())
                        .unwrap_or("")
                        .to_string(),
                );
                i += 2;
            }
            "USER" => {
                f.user = Some(
                    args.get(i + 1)
                        .and_then(|a| a.as_str())
                        .unwrap_or("")
                        .to_string(),
                );
                i += 2;
            }
            "SKIPME" => {
                let v = args.get(i + 1).and_then(|a| a.as_str()).unwrap_or("YES");
                f.skipme = !v.eq_ignore_ascii_case("NO");
                i += 2;
            }
            _ => return Err(NexradeError::Generic("ERR syntax error".to_string())),
        }
    }
    // Default filter when none specified: kill the calling client only.
    if f.id.is_none()
        && f.typ.is_none()
        && f.addr.is_none()
        && f.laddr.is_none()
        && f.user.is_none()
    {
        f.id = Some(caller_id);
    }
    let _ = caller_id; // Used implicitly by skipme-check below.
    Ok(f)
}

fn client_kill(db: &Db, args: &[Resp], caller_id: u64) -> Result<Resp> {
    let filter = parse_kill_filter(args, caller_id)?;
    let snap = db.connections.snapshot();
    let mut killed = 0i64;
    for meta in snap {
        let m = meta.read();
        if filter.skipme && m.id == caller_id {
            continue;
        }
        if let Some(want) = filter.id {
            if m.id != want {
                continue;
            }
        }
        if let Some(ref want) = filter.addr {
            if m.addr.to_string() != *want {
                continue;
            }
        }
        if let Some(ref want) = filter.user {
            if m.user != *want {
                continue;
            }
        }
        // TYPE filtering: we only model normal + pubsub. master/replica
        // would map to the actual role; in this server it's always primary
        // unless replica_of is set, which isn't tracked in meta (out of scope).
        if let Some(ref want) = filter.typ {
            let matches = match want.as_str() {
                "NORMAL" => true,
                "PUBSUB" => m.flags & crate::conn_registry::CLIENT_FLAG_PUBSUB != 0,
                "MASTER" => true, // this server has no replica tracking in meta
                "REPLICA" | "SLAVE" => false,
                _ => continue,
            };
            if !matches {
                continue;
            }
        }
        drop(m); // release read lock before requesting kill.
        if db.connections.request_kill(meta.read().id) {
            killed += 1;
        }
    }
    Ok(Resp::int(killed))
}

fn client_pause(db: &Db, args: &[Resp]) -> Result<Resp> {
    let ms: u64 = if args.len() < 3 {
        30_000 // default per Redis spec
    } else {
        let s = get_str(args, 2, "CLIENT")?;
        s.parse().map_err(|_| NexradeError::NotInteger)?
    };
    if ms == 0 {
        return Err(NexradeError::Prefixed(
            "ERR CLIENT PAUSE 0 is invalid".to_string(),
        ));
    }
    db.connections
        .pause_for(std::time::Duration::from_millis(ms));
    Ok(Resp::ok())
}

fn client_no_evict(db: &Db, caller_id: u64) -> Result<Resp> {
    if let Some(m) = db.connections.meta(caller_id) {
        m.write().flags |= CLIENT_FLAG_NO_EVICT;
    }
    Ok(Resp::ok())
}

fn client_help() -> Resp {
    Resp::array(vec![
        Resp::bulk_str("CLIENT <subcommand> [<arg> ...]. Subcommands are:"),
        Resp::bulk_str("ID"),
        Resp::bulk_str("INFO"),
        Resp::bulk_str("LIST"),
        Resp::bulk_str("GETNAME"),
        Resp::bulk_str("SETNAME <name>"),
        Resp::bulk_str("KILL [ID <id>] [TYPE <normal|master|replica|pubsub>] [ADDR <ip:port>] [USER <username>] [SKIPME yes/no]"),
        Resp::bulk_str("PAUSE <timeout>"),
        Resp::bulk_str("UNPAUSE"),
        Resp::bulk_str("NO-EVICT"),
        Resp::bulk_str("HELP"),
    ])
}

pub async fn cmd_cluster(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("cluster".to_string()));
    }
    let sub = get_str(args, 1, "CLUSTER")?.to_uppercase();
    match sub.as_str() {
        "INFO" => Ok(cluster_info(db)),
        "MYID" => Ok(Resp::bulk_str(db.cluster_node_id.clone())),
        "KEYSLOT" => {
            let key = get_bytes_vec(args, 2, "CLUSTER")?;
            Ok(Resp::Integer(cluster::keyslot(&key) as i64))
        }
        "NODES" => Ok(cluster_nodes(db)),
        "COUNTKEYSINSLOT" => {
            let slot: u16 = get_str(args, 2, "CLUSTER")?
                .parse()
                .map_err(|_| NexradeError::NotInteger)?;
            Ok(Resp::int(db.store.count_keys_in_slot(slot) as i64))
        }
        "GETKEYSINSLOT" => {
            let slot: u16 = get_str(args, 2, "CLUSTER")?
                .parse()
                .map_err(|_| NexradeError::NotInteger)?;
            let count: usize = get_str(args, 3, "CLUSTER")?
                .parse()
                .map_err(|_| NexradeError::NotInteger)?;
            let keys = db.store.get_keys_in_slot(slot, count);
            let arr: Vec<Resp> = keys
                .into_iter()
                .map(|k| Resp::bulk(bytes::Bytes::from(k)))
                .collect();
            Ok(Resp::array(arr))
        }
        "SLOTS" => Ok(cluster_slots(db)),
        "HELP" => Ok(cluster_help()),
        // Anything else: pretend we don't know it — Redis does the same.
        _ => Ok(Resp::ok()),
    }
}

/// Full `CLUSTER INFO` text. Field set mirrors Redis 7.x.
fn cluster_info(db: &Db) -> Resp {
    let enabled = db.cluster_enabled.load(Ordering::Relaxed);
    let state = "ok";
    let mut s = String::new();
    use std::fmt::Write;
    let _ = write!(s, "cluster_enabled:{}\r\n", if enabled { 1 } else { 0 });
    let _ = write!(s, "cluster_state:{state}\r\n");
    let _ = write!(s, "cluster_slots_assigned:{}\r\n", cluster::CLUSTER_SLOTS);
    let _ = write!(s, "cluster_slots_ok:{}\r\n", cluster::CLUSTER_SLOTS);
    let _ = write!(s, "cluster_slots_pfail:0\r\n");
    let _ = write!(s, "cluster_slots_fail:0\r\n");
    let _ = write!(s, "cluster_known_nodes:1\r\n");
    let _ = write!(s, "cluster_size:1\r\n");
    let _ = write!(s, "cluster_current_epoch:1\r\n");
    let _ = write!(s, "cluster_my_epoch:1\r\n");
    let _ = write!(s, "cluster_stats_messages_ping_sent:0\r\n");
    let _ = write!(s, "cluster_stats_messages_pong_sent:0\r\n");
    let _ = write!(s, "cluster_stats_messages_meet_sent:0\r\n");
    let _ = write!(s, "cluster_stats_messages_ping_received:0\r\n");
    let _ = write!(s, "cluster_stats_messages_pong_received:0\r\n");
    let _ = write!(s, "cluster_stats_messages_meet_received:0\r\n");
    let _ = write!(s, "cluster_stats_messages_auth_req_sent:0\r\n");
    let _ = write!(s, "cluster_stats_messages_auth_req_received:0\r\n");
    let _ = write!(s, "cluster_stats_messages_update_sent:0\r\n");
    let _ = write!(s, "cluster_stats_messages_update_received:0\r\n");
    Resp::bulk_str(s)
}

/// Single self-line `CLUSTER NODES` output.
fn cluster_nodes(db: &Db) -> Resp {
    let cfg = db.config.lock();
    let addr = format!("{}:{}", cfg.bind, cfg.port);
    let port = cfg.port;
    let node_id = db.cluster_node_id.clone();
    drop(cfg);
    // C L <ip>:<port>@<port> myself,master - 0 0 1 connected 0-16383
    let line = format!(
        "{node_id} {addr}@{port} myself,master - 0 0 1 connected 0-16383\r\n",
        node_id = node_id,
        addr = addr,
        port = port,
    );
    Resp::bulk_str(line)
}

/// `CLUSTER SLOTS` — a single range covering all 16384 slots on this node.
fn cluster_slots(db: &Db) -> Resp {
    let cfg = db.config.lock();
    let addr = format!("{}:{}", cfg.bind, cfg.port);
    drop(cfg);
    let node_id = db.cluster_node_id.clone();
    // Single range: [[0, 16383, [host, port, node_id]]]
    Resp::array(vec![Resp::array(vec![
        Resp::Integer(0),
        Resp::Integer(cluster::CLUSTER_SLOTS as i64 - 1),
        Resp::array(vec![
            Resp::bulk_str(addr),
            Resp::Integer(db.config.lock().port as i64),
            Resp::bulk_str(node_id),
        ]),
    ])])
}

fn cluster_help() -> Resp {
    Resp::array(vec![
        Resp::bulk_str("CLUSTER <subcommand> [<arg> ...]. Subcommands are:"),
        Resp::bulk_str("INFO"),
        Resp::bulk_str("MYID"),
        Resp::bulk_str("KEYSLOT <key>"),
        Resp::bulk_str("NODES"),
        Resp::bulk_str("COUNTKEYSINSLOT <slot>"),
        Resp::bulk_str("GETKEYSINSLOT <slot> <count>"),
        Resp::bulk_str("SLOTS"),
        Resp::bulk_str("HELP"),
    ])
}

pub async fn cmd_hello(_args: &[Resp]) -> Result<Resp> {
    // HELLO protocol negotiation (simplified)
    Ok(Resp::array(vec![
        Resp::bulk_str("server"),
        Resp::bulk_str("nexrade"),
        Resp::bulk_str("version"),
        Resp::bulk_str(env!("CARGO_PKG_VERSION")),
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
