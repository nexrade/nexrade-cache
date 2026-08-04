//! Server/connection command handlers.

use std::sync::atomic::Ordering;

use crate::cluster;
use crate::command::{get_bytes_vec, get_i64, get_str};
use crate::conn_registry::{format_client_list_line, CLIENT_FLAG_NO_EVICT};
#[cfg(not(target_arch = "wasm32"))]
use crate::db::unix_secs;
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::hash_data::{hash_thresholds, set_hash_thresholds};
use crate::list_data::{list_thresholds, set_list_thresholds};
#[cfg(not(target_arch = "wasm32"))]
use crate::persistence::Snapshot;
#[cfg(not(target_arch = "wasm32"))]
use crate::replication::ReplicationRole;
use crate::resp::Resp;
use crate::set_data::{set_set_thresholds, set_thresholds};
use crate::store::glob_match;
use crate::zset_data::{set_zset_thresholds, zset_thresholds};

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

// Map the `bgsave_last_status` numeric (0 ok, 1 err) to Redis's string form
// for the `INFO persistence` section. Anything other than the explicit
// error code is reported as `ok` so a stale value never misleads the
// operator. Host-only: the whole INFO persistence block is gated to
// `#[cfg(not(target_arch = "wasm32"))]`, so the helpers are too.
#[cfg(not(target_arch = "wasm32"))]
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
#[cfg(not(target_arch = "wasm32"))]
fn aof_rewrite_status_str(code: u8) -> &'static str {
    if code == 1 {
        "err"
    } else {
        "ok"
    }
}

/// `aof_last_write_status` — `ok` when AOF is enabled and the last append
/// (or ALWAYS fsync) succeeded; `err` when AOF is disabled or the last
/// write failed.
#[cfg(not(target_arch = "wasm32"))]
fn aof_last_write_status(db: &Db) -> &'static str {
    if !db.stats.aof_enabled.load(Ordering::Relaxed) {
        return "err";
    }
    match db.stats.aof_last_write_status.load(Ordering::Relaxed) {
        0 => "ok",
        _ => "err",
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
        // redis-py / go-redis / jedis probe these for capability hints.
        info.push_str("redis_mode:standalone\r\n");
        // Use the package version that was compiled in, so `INFO server`
        // can never drift from the workspace version at release time.
        info.push_str(&format!(
            "nexrade_version:{}\r\n",
            env!("CARGO_PKG_VERSION")
        ));
        // Stable per-process id (cluster node id is also stable across
        // restarts; process_id is the OS pid for this run).
        info.push_str(&format!("process_id:{}\r\n", std::process::id()));
        info.push_str(&format!("run_id:{}\r\n", db.cluster_node_id));
        #[cfg(target_os = "linux")]
        info.push_str("os:Linux\r\n");
        #[cfg(target_os = "macos")]
        info.push_str("os:Darwin\r\n");
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        info.push_str("os:unknown\r\n");
        info.push_str("arch_bits:64\r\n");
        info.push_str("multiplexing_api:epoll\r\n");
        info.push_str("atomicvar_api:atomic-builtin\r\n");
        let cfg = db.config.lock();
        info.push_str(&format!("tcp_port:{}\r\n", cfg.port));
        info.push_str(&format!("uptime_in_seconds:{}\r\n", db.stats.uptime_secs()));
        info.push_str(&format!(
            "uptime_in_days:{}\r\n",
            db.stats.uptime_secs() / 86_400
        ));
        info.push_str(&format!("hz:{}\r\n", cfg.hz));
        info.push_str(&format!("configured_hz:{}\r\n", cfg.hz));
        drop(cfg);
        info.push_str("executable:nexrade\r\n");
        info.push_str("config_file:\r\n");
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
        // Peak = current for now (we don't track a high-water mark yet).
        info.push_str(&format!("used_memory_peak:{}\r\n", mem));
        let maxmem = db.max_memory_limit.load(Ordering::Relaxed);
        info.push_str(&format!("maxmemory:{}\r\n", maxmem));
        info.push_str("\r\n");
    }

    if section == "all" || section == "cpu" {
        info.push_str("# CPU\r\n");
        let (user, sys) = crate::resource::process_cpu_seconds();
        info.push_str(&format!("used_cpu_sys:{:.6}\r\n", sys));
        info.push_str(&format!("used_cpu_user:{:.6}\r\n", user));
        info.push_str("used_cpu_sys_children:0.000000\r\n");
        info.push_str("used_cpu_user_children:0.000000\r\n");
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
            aof_last_write_status(db)
        ));
        info.push_str(&format!(
            "aof_write_failed:{}\r\n",
            db.stats.aof_failed.load(Ordering::Relaxed) as u8
        ));
        info.push_str(&format!(
            "aof_last_write_error_time:{}\r\n",
            db.stats.aof_failed_time.load(Ordering::Relaxed)
        ));
        if let Some(error) = db.stats.aof_failed_msg.lock().as_deref() {
            info.push_str(&format!("aof_last_write_error:{}\r\n", error));
        }
        info.push_str("\r\n");
    }
    #[cfg(target_arch = "wasm32")]
    if section == "all" || section == "persistence" {
        info.push_str("# Persistence\r\nloading:0\r\naof_enabled:0\r\n\r\n");
    }

    #[cfg(not(target_arch = "wasm32"))]
    if section == "all" || section == "health" {
        let report = crate::health::health_report(db);
        info.push_str("# Health\r\n");
        info.push_str(&format!("live:{}\r\n", report.live as u8));
        info.push_str(&format!("ready:{}\r\n", report.ready as u8));
        info.push_str(&format!("phase:{}\r\n", report.phase.as_str()));
        info.push_str(&format!(
            "persistence_mode:{}\r\n",
            report.persistence.persistence_mode
        ));
        info.push_str(&format!(
            "active_background_job:{}\r\n",
            match report.active_background_job {
                crate::health::ActiveBackgroundJob::None => "none",
                crate::health::ActiveBackgroundJob::Bgsave => "bgsave",
                crate::health::ActiveBackgroundJob::AofRewrite => "aof_rewrite",
            }
        ));
        if let Some(age) = report.persistence.snapshot_age_seconds {
            info.push_str(&format!("snapshot_age_seconds:{}\r\n", age));
        } else {
            info.push_str("snapshot_age_seconds:\r\n");
        }
        info.push_str(&format!("replication_role:{}\r\n", report.replication.role));
        info.push_str(&format!(
            "replication_primary_link_status:{}\r\n",
            report.replication.primary_link_status
        ));
        info.push_str(&format!(
            "replication_connected_replicas:{}\r\n",
            report.replication.connected_replicas
        ));
        info.push_str(&format!(
            "replication_max_offset_lag:{}\r\n",
            report.replication.max_replica_offset_lag
        ));
        if report.reasons.is_empty() {
            info.push_str("readiness_reasons:0\r\n");
        } else {
            info.push_str(&format!("readiness_reasons:{}\r\n", report.reasons.len()));
            for r in &report.reasons {
                let code = serde_json::to_value(r.code)
                    .ok()
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_else(|| "unknown".to_string());
                info.push_str(&format!(
                    "readiness_reason:{}\r\nreadiness_reason_message:{}\r\n",
                    code, r.message
                ));
            }
        }
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
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                for (i, r) in replicas.iter().enumerate() {
                    // 0.8.0: lag = seconds since last ACK (0 if never/fresh).
                    let lag_s = if r.last_ack_ms == 0 || now_ms <= r.last_ack_ms {
                        0
                    } else {
                        (now_ms - r.last_ack_ms) / 1000
                    };
                    info.push_str(&format!(
                        "slave{}:ip={},port={},state=online,offset={},lag={}\r\n",
                        i,
                        r.addr.ip(),
                        r.addr.port(),
                        r.offset,
                        lag_s,
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
            let appendfsync_str = {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    match cfg.persistence.aof_sync {
                        crate::persistence::AofSync::Always => "always",
                        crate::persistence::AofSync::EverySec => "everysec",
                        crate::persistence::AofSync::No => "no",
                    }
                }
                #[cfg(target_arch = "wasm32")]
                {
                    "everysec"
                }
            };
            let rdb_path_str = {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    cfg.persistence
                        .rdb_path
                        .as_deref()
                        .unwrap_or("nexrade.rdb")
                        .to_string()
                }
                #[cfg(target_arch = "wasm32")]
                {
                    "nexrade.rdb".to_string()
                }
            };
            let aof_path_str = {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    cfg.persistence
                        .aof_path
                        .as_deref()
                        .unwrap_or("appendonly.aof")
                        .to_string()
                }
                #[cfg(target_arch = "wasm32")]
                {
                    "appendonly.aof".to_string()
                }
            };
            // `dir` / `dbfilename` derived from rdb_path for Redis-compat probes.
            let (dir_str, dbfilename_str) = {
                let p = std::path::Path::new(&rdb_path_str);
                let dir = p
                    .parent()
                    .and_then(|d| d.to_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or(".")
                    .to_string();
                let file = p
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or("nexrade.rdb")
                    .to_string();
                (dir, file)
            };
            let lt = list_thresholds();
            let ht = hash_thresholds();
            let st = set_thresholds();
            let zt = zset_thresholds();
            let notify_str = cfg.notify_keyspace_events.as_str();
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
                ("appendfsync", &appendfsync_str),
                ("appendfilename", &aof_path_str),
                ("dir", &dir_str),
                ("dbfilename", &dbfilename_str),
                ("save", &save_str),
                ("list-max-listpack-entries", &lt.max_entries),
                ("list-max-listpack-size", &lt.max_size),
                ("list-max-listpack-value", &lt.max_elem),
                ("list-demote-entries", &lt.demote_entries),
                ("hash-max-listpack-entries", &ht.max_entries),
                ("hash-max-listpack-size", &ht.max_size),
                ("hash-max-listpack-value", &ht.max_value),
                ("hash-demote-entries", &ht.demote_entries),
                ("set-max-listpack-entries", &st.max_entries),
                ("set-max-listpack-size", &st.max_size),
                ("set-max-listpack-value", &st.max_value),
                ("set-demote-entries", &st.demote_entries),
                ("zset-max-listpack-entries", &zt.max_entries),
                ("zset-max-listpack-size", &zt.max_size),
                ("zset-max-listpack-value", &zt.max_value),
                ("zset-demote-entries", &zt.demote_entries),
                ("notify-keyspace-events", &notify_str),
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
                "appendfsync" => {
                    #[cfg(not(target_arch = "wasm32"))]
                    {
                        use crate::persistence::AofSync;
                        cfg.persistence.aof_sync = match val.to_ascii_lowercase().as_str() {
                            "always" => AofSync::Always,
                            "everysec" => AofSync::EverySec,
                            "no" => AofSync::No,
                            _ => {
                                return Err(NexradeError::Generic(
                                    "ERR invalid appendfsync value — use always, everysec, or no"
                                        .to_string(),
                                ));
                            }
                        };
                    }
                    #[cfg(target_arch = "wasm32")]
                    {
                        let _ = val;
                    }
                }
                "maxclients" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid maxclients value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid maxclients value".to_string(),
                        ));
                    }
                    cfg.max_clients = n;
                }
                "save" => {
                    // Redis: CONFIG SET save "900 1 300 10" or empty to disable.
                    let rules = parse_save_config(val)?;
                    cfg.save_rules = rules.clone();
                    cfg.persistence.rdb_save_rules = rules;
                }
                "list-max-listpack-entries" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid list-max-listpack-entries value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid list-max-listpack-entries value".to_string(),
                        ));
                    }
                    cfg.list_max_listpack_entries = n;
                    let mut t = list_thresholds();
                    t.max_entries = n;
                    set_list_thresholds(t);
                }
                "list-max-listpack-size" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid list-max-listpack-size value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid list-max-listpack-size value".to_string(),
                        ));
                    }
                    cfg.list_max_listpack_size = n;
                    let mut t = list_thresholds();
                    t.max_size = n;
                    set_list_thresholds(t);
                }
                "list-max-listpack-value" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid list-max-listpack-value value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid list-max-listpack-value value".to_string(),
                        ));
                    }
                    cfg.list_max_listpack_value = n;
                    let mut t = list_thresholds();
                    t.max_elem = n;
                    set_list_thresholds(t);
                }
                "list-demote-entries" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid list-demote-entries value".to_string())
                    })?;
                    cfg.list_demote_entries = n;
                    let mut t = list_thresholds();
                    t.demote_entries = n;
                    set_list_thresholds(t);
                }
                "hash-max-listpack-entries" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid hash-max-listpack-entries value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid hash-max-listpack-entries value".to_string(),
                        ));
                    }
                    cfg.hash_max_listpack_entries = n;
                    let mut t = hash_thresholds();
                    t.max_entries = n;
                    set_hash_thresholds(t);
                }
                "hash-max-listpack-size" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid hash-max-listpack-size value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid hash-max-listpack-size value".to_string(),
                        ));
                    }
                    cfg.hash_max_listpack_size = n;
                    let mut t = hash_thresholds();
                    t.max_size = n;
                    set_hash_thresholds(t);
                }
                "hash-max-listpack-value" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid hash-max-listpack-value value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid hash-max-listpack-value value".to_string(),
                        ));
                    }
                    cfg.hash_max_listpack_value = n;
                    let mut t = hash_thresholds();
                    t.max_value = n;
                    set_hash_thresholds(t);
                }
                "hash-demote-entries" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid hash-demote-entries value".to_string())
                    })?;
                    cfg.hash_demote_entries = n;
                    let mut t = hash_thresholds();
                    t.demote_entries = n;
                    set_hash_thresholds(t);
                }
                "set-max-listpack-entries" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid set-max-listpack-entries value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid set-max-listpack-entries value".to_string(),
                        ));
                    }
                    cfg.set_max_listpack_entries = n;
                    let mut t = set_thresholds();
                    t.max_entries = n;
                    set_set_thresholds(t);
                }
                "set-max-listpack-size" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid set-max-listpack-size value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid set-max-listpack-size value".to_string(),
                        ));
                    }
                    cfg.set_max_listpack_size = n;
                    let mut t = set_thresholds();
                    t.max_size = n;
                    set_set_thresholds(t);
                }
                "set-max-listpack-value" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid set-max-listpack-value value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid set-max-listpack-value value".to_string(),
                        ));
                    }
                    cfg.set_max_listpack_value = n;
                    let mut t = set_thresholds();
                    t.max_value = n;
                    set_set_thresholds(t);
                }
                "set-demote-entries" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid set-demote-entries value".to_string())
                    })?;
                    cfg.set_demote_entries = n;
                    let mut t = set_thresholds();
                    t.demote_entries = n;
                    set_set_thresholds(t);
                }
                "zset-max-listpack-entries" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid zset-max-listpack-entries value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid zset-max-listpack-entries value".to_string(),
                        ));
                    }
                    cfg.zset_max_listpack_entries = n;
                    let mut t = zset_thresholds();
                    t.max_entries = n;
                    set_zset_thresholds(t);
                }
                "zset-max-listpack-size" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid zset-max-listpack-size value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid zset-max-listpack-size value".to_string(),
                        ));
                    }
                    cfg.zset_max_listpack_size = n;
                    let mut t = zset_thresholds();
                    t.max_size = n;
                    set_zset_thresholds(t);
                }
                "zset-max-listpack-value" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid zset-max-listpack-value value".to_string())
                    })?;
                    if n == 0 {
                        return Err(NexradeError::Generic(
                            "Invalid zset-max-listpack-value value".to_string(),
                        ));
                    }
                    cfg.zset_max_listpack_value = n;
                    let mut t = zset_thresholds();
                    t.max_value = n;
                    set_zset_thresholds(t);
                }
                "zset-demote-entries" => {
                    let n = val.parse::<usize>().map_err(|_| {
                        NexradeError::Generic("Invalid zset-demote-entries value".to_string())
                    })?;
                    cfg.zset_demote_entries = n;
                    let mut t = zset_thresholds();
                    t.demote_entries = n;
                    set_zset_thresholds(t);
                }
                "notify-keyspace-events" => {
                    // Redis silently accepts any character; unknown chars are
                    // ignored. Empty string disables notifications.
                    let flags = crate::notify::NotifyFlags::parse(val);
                    cfg.notify_keyspace_events = flags;
                    db.notify_flags.store(flags);
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
        "REWRITE" => {
            let path = cfg_clone_config_path(db);
            let Some(path) = path else {
                return Err(NexradeError::Generic(
                    "ERR CONFIG REWRITE failed: no config file path was set at startup \
                     (pass --config /path/to/nexrade.toml)"
                        .to_string(),
                ));
            };
            let snapshot = {
                let cfg = db.config.lock();
                render_config_toml(&cfg)
            };
            std::fs::write(&path, snapshot)
                .map_err(|e| NexradeError::Generic(format!("ERR CONFIG REWRITE failed: {e}")))?;
            Ok(Resp::ok())
        }
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

fn cfg_clone_config_path(db: &Db) -> Option<String> {
    db.config.lock().config_file_path.clone()
}

/// Parse Redis `CONFIG SET save "900 1 300 10"` value into rule pairs.
fn parse_save_config(val: &str) -> Result<Vec<(u64, usize)>> {
    let trimmed = val.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.len() % 2 != 0 {
        return Err(NexradeError::Generic(
            "ERR wrong number of arguments for CONFIG SET 'save'".to_string(),
        ));
    }
    let mut rules = Vec::with_capacity(parts.len() / 2);
    for chunk in parts.chunks(2) {
        let secs: u64 = chunk[0].parse().map_err(|_| {
            NexradeError::Generic("ERR Invalid argument for CONFIG SET 'save'".to_string())
        })?;
        let changes: usize = chunk[1].parse().map_err(|_| {
            NexradeError::Generic("ERR Invalid argument for CONFIG SET 'save'".to_string())
        })?;
        rules.push((secs, changes));
    }
    Ok(rules)
}

/// Render a TOML snapshot of the live config for CONFIG REWRITE.
fn render_config_toml(cfg: &crate::db::ServerConfig) -> String {
    let mut out = String::new();
    out.push_str("# Generated by CONFIG REWRITE — nexrade-cache\n");
    out.push_str(&format!("bind = {:?}\n", cfg.bind));
    out.push_str(&format!("port = {}\n", cfg.port));
    out.push_str(&format!("databases = {}\n", cfg.databases));
    out.push_str(&format!("max_clients = {}\n", cfg.max_clients));
    out.push_str(&format!("hz = {}\n", cfg.hz));
    out.push_str(&format!("timeout = {}\n", cfg.timeout));
    out.push_str(&format!("loglevel = {:?}\n", cfg.loglevel));
    out.push_str(&format!("tcp_backlog = {}\n", cfg.tcp_backlog));
    if let Some(m) = cfg.max_memory {
        out.push_str(&format!("maxmemory = {}\n", m));
    } else {
        out.push_str("maxmemory = 0\n");
    }
    out.push_str(&format!(
        "maxmemory_policy = {:?}\n",
        cfg.maxmemory_policy.as_str()
    ));
    out.push_str(&format!(
        "notify_keyspace_events = {:?}\n",
        cfg.notify_keyspace_events.as_str()
    ));
    if let Some(ref p) = cfg.requirepass {
        out.push_str(&format!("requirepass = {:?}\n", p));
    }
    out.push_str(&format!(
        "slowlog_log_slower_than = {}\n",
        cfg.slowlog_log_slower_than
    ));
    out.push_str(&format!("slowlog_max_len = {}\n", cfg.slowlog_max_len));
    out.push_str(&format!(
        "list_max_listpack_entries = {}\n",
        cfg.list_max_listpack_entries
    ));
    out.push_str(&format!(
        "list_max_listpack_size = {}\n",
        cfg.list_max_listpack_size
    ));
    out.push_str(&format!(
        "list_max_listpack_value = {}\n",
        cfg.list_max_listpack_value
    ));
    out.push_str(&format!(
        "list_demote_entries = {}\n",
        cfg.list_demote_entries
    ));
    out.push_str(&format!(
        "hash_max_listpack_entries = {}\n",
        cfg.hash_max_listpack_entries
    ));
    out.push_str(&format!(
        "hash_max_listpack_size = {}\n",
        cfg.hash_max_listpack_size
    ));
    out.push_str(&format!(
        "hash_max_listpack_value = {}\n",
        cfg.hash_max_listpack_value
    ));
    out.push_str(&format!(
        "hash_demote_entries = {}\n",
        cfg.hash_demote_entries
    ));
    out.push_str(&format!(
        "set_max_listpack_entries = {}\n",
        cfg.set_max_listpack_entries
    ));
    out.push_str(&format!(
        "set_max_listpack_size = {}\n",
        cfg.set_max_listpack_size
    ));
    out.push_str(&format!(
        "set_max_listpack_value = {}\n",
        cfg.set_max_listpack_value
    ));
    out.push_str(&format!(
        "set_demote_entries = {}\n",
        cfg.set_demote_entries
    ));
    out.push_str(&format!(
        "zset_max_listpack_entries = {}\n",
        cfg.zset_max_listpack_entries
    ));
    out.push_str(&format!(
        "zset_max_listpack_size = {}\n",
        cfg.zset_max_listpack_size
    ));
    out.push_str(&format!(
        "zset_max_listpack_value = {}\n",
        cfg.zset_max_listpack_value
    ));
    out.push_str(&format!(
        "zset_demote_entries = {}\n",
        cfg.zset_demote_entries
    ));
    // save_rules = [[secs, changes], ...]
    out.push_str("save_rules = [");
    for (i, (s, c)) in cfg.save_rules.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&format!("[{}, {}]", s, c));
    }
    out.push_str("]\n");
    out.push_str("\n[persistence]\n");
    if let Some(ref p) = cfg.persistence.rdb_path {
        out.push_str(&format!("rdb_path = {:?}\n", p));
    }
    if let Some(ref p) = cfg.persistence.aof_path {
        out.push_str(&format!("aof_path = {:?}\n", p));
    }
    let sync = {
        #[cfg(not(target_arch = "wasm32"))]
        {
            match cfg.persistence.aof_sync {
                crate::persistence::AofSync::Always => "always",
                crate::persistence::AofSync::EverySec => "everysec",
                crate::persistence::AofSync::No => "no",
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            "everysec"
        }
    };
    out.push_str(&format!("aof_sync = {:?}\n", sync));
    out.push_str("\n[metrics]\n");
    out.push_str(&format!("enabled = {}\n", cfg.metrics_enabled));
    out.push_str(&format!("port = {}\n", cfg.metrics_port));
    out.push_str("\n[tls]\n");
    out.push_str(&format!("enabled = {}\n", cfg.tls_enabled));
    if let Some(ref c) = cfg.tls_cert {
        out.push_str(&format!("cert = {:?}\n", c));
    }
    if let Some(ref k) = cfg.tls_key {
        out.push_str(&format!("key = {:?}\n", k));
    }
    if let Some(p) = cfg.tls_port {
        out.push_str(&format!("port = {}\n", p));
    }
    out.push_str("\n[lua]\n");
    out.push_str(&format!("time_limit = {}\n", cfg.lua_time_limit));
    out
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
        "NUMPAT" => Ok(Resp::int(db.pubsub.pattern_count() as i64)),
        _ => Ok(Resp::array(vec![])),
    }
}

// COMMAND is implemented in `command_table` (static metadata table).

#[cfg(not(target_arch = "wasm32"))]
pub async fn cmd_save(db: &Db) -> Result<Resp> {
    let _save_job = match db.rdb_save_job.clone().try_lock_owned() {
        Ok(guard) => guard,
        Err(_) => {
            return Err(NexradeError::Generic(
                "ERR RDB save already in progress".to_string(),
            ));
        }
    };
    let _mutation_quiesce = db.persistence.quiesce().await;
    let rdb_path = db.config.lock().persistence.rdb_path.clone();
    if let Some(path) = rdb_path {
        let dirty_at_capture = db.stats.dirty_keys.load(Ordering::Relaxed);
        let dbs = db.store.snapshot_dbs();
        drop(_mutation_quiesce);
        let snapshot = Snapshot::new(dbs);
        match snapshot.save(&path) {
            Ok(()) => {
                db.stats
                    .dirty_keys
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(dirty_at_capture))
                    })
                    .ok();
                db.stats
                    .last_save_time
                    .store(unix_secs(), Ordering::Relaxed);
                db.stats.bgsave_last_status.store(0, Ordering::Relaxed);
                Ok(Resp::ok())
            }
            Err(e) => {
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
    let save_job = match db.rdb_save_job.clone().try_lock_owned() {
        Ok(guard) => guard,
        Err(_) => {
            db.stats.bgsave_in_progress.store(false, Ordering::Release);
            return Ok(Resp::SimpleString(
                "Background saving already in progress".to_string(),
            ));
        }
    };
    let rdb_path = db.config.lock().persistence.rdb_path.clone();
    if let Some(path) = rdb_path {
        let mutation_quiesce = db.persistence.quiesce().await;
        let dirty_at_capture = db.stats.dirty_keys.load(Ordering::Relaxed);
        let dbs = db.store.snapshot_dbs();
        drop(mutation_quiesce);
        let stats = db.stats.clone();
        tokio::spawn(async move {
            let _save_job = save_job;
            let result = tokio::task::spawn_blocking(move || Snapshot::new(dbs).save(&path)).await;
            match result {
                Ok(Ok(())) => {
                    tracing::info!("BGSAVE completed");
                    stats
                        .dirty_keys
                        .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                            Some(current.saturating_sub(dirty_at_capture))
                        })
                        .ok();
                    stats.last_save_time.store(unix_secs(), Ordering::Relaxed);
                    stats.bgsave_last_status.store(0, Ordering::Relaxed);
                }
                Ok(Err(e)) => {
                    tracing::error!("BGSAVE failed: {}", e);
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

    // Serialize rewrite so we don't race with normal mutations.
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

    // Take exclusive access for the complete rewrite + writer handoff. The
    // owned guard is moved into the background task, so no write can slip
    // between snapshot generation, rename, and replacement-writer install.
    let quiesce = db.persistence.quiesce().await;

    // Flush/sync and detach the existing writer so concurrent appends wait
    // on the gate and are included in the next write after handoff.
    let old_writer = {
        let mut guard = db.aof_writer.lock();
        guard.take()
    };
    if let Some(mut w) = old_writer {
        if let Err(e) = w.fsync() {
            tracing::error!("pre-rewrite AOF fsync failed: {}", e);
            db.fail_aof("pre-rewrite fsync", &e);
            db.stats
                .aof_rewrite_in_progress
                .store(false, Ordering::Release);
            return Err(NexradeError::Generic(
                "AOF fsync failed before rewrite; rewrite aborted".to_string(),
            ));
        }
    }

    let dbs = db.store.snapshot_dbs();
    let acl_lines = db.acl.list();
    let db_clone = db.clone();
    let stats = db.stats.clone();
    tokio::spawn(async move {
        let _quiesce = quiesce;
        let path2 = path.clone();
        let result = tokio::task::spawn_blocking(move || {
            crate::persistence::AofWriter::rewrite(&path, &dbs, &acl_lines)
        })
        .await;
        match result {
            Ok(Ok(())) => {
                tracing::info!("BGREWRITEAOF completed");
                stats.aof_rewrite_last_status.store(0, Ordering::Relaxed);
                stats
                    .last_rewrite_time
                    .store(unix_secs(), Ordering::Relaxed);
                // Re-open the AOF writer on the rewritten file.
                match crate::persistence::AofWriter::open(&path2) {
                    Ok(writer) => *db_clone.aof_writer.lock() = Some(writer),
                    Err(e) => {
                        tracing::error!("failed to reopen AOF after rewrite: {}", e);
                        db_clone.fail_aof("post-rewrite writer open", &e);
                    }
                }
            }
            Ok(Err(e)) => {
                tracing::error!("BGREWRITEAOF failed: {}", e);
                stats.aof_rewrite_last_status.store(1, Ordering::Relaxed);
                match crate::persistence::AofWriter::open(&path2) {
                    Ok(writer) => *db_clone.aof_writer.lock() = Some(writer),
                    Err(reopen) => {
                        tracing::error!(
                            "failed to restore AOF writer after rewrite failure: {}",
                            reopen
                        );
                        db_clone.fail_aof("rewrite failure recovery", &reopen);
                    }
                }
            }
            Err(e) => {
                tracing::error!("BGREWRITEAOF task panicked: {}", e);
                stats.aof_rewrite_last_status.store(1, Ordering::Relaxed);
                match crate::persistence::AofWriter::open(&path2) {
                    Ok(writer) => *db_clone.aof_writer.lock() = Some(writer),
                    Err(reopen) => {
                        tracing::error!(
                            "failed to restore AOF writer after rewrite panic: {}",
                            reopen
                        );
                        db_clone.fail_aof("rewrite panic recovery", &reopen);
                    }
                }
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
    db.stats.shutdown_nosave.store(nosave, Ordering::Release);
    db.shutdown.notify_one();
    Ok(Resp::ok())
}

/// `WAIT numreplicas timeout`
///
/// Blocks until at least `numreplicas` connected replicas have acknowledged
/// an offset ≥ the primary's current replication offset, or until `timeout`
/// milliseconds elapse (0 = wait forever). Returns the number of replicas
/// that have caught up (may be less than `numreplicas` on timeout).
///
/// 0.8.0: real implementation. Previously always returned 0.
/// Standalone (no replicas) still returns 0 immediately, matching Redis.
#[cfg(not(target_arch = "wasm32"))]
pub async fn cmd_wait(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("wait".to_string()));
    }
    let num = get_i64(args, 1, "WAIT")?;
    let timeout_ms = get_i64(args, 2, "WAIT")?;
    if num < 0 || timeout_ms < 0 {
        return Err(NexradeError::Generic("ERR timeout is negative".to_string()));
    }
    let num = num as usize;
    // Target = current primary offset. Writes that land after WAIT starts
    // are not required to be acknowledged (Redis semantics).
    let target = db
        .replication
        .replication_offset
        .load(std::sync::atomic::Ordering::Relaxed);

    // Fast path: already satisfied, or no replicas / num==0.
    let mut acked = db.replication.replicas_at_or_beyond(target);
    if num == 0 || acked >= num {
        return Ok(Resp::int(acked as i64));
    }
    if db.replication.propagate_subscriber_count() == 0 {
        return Ok(Resp::int(0));
    }

    // Ask replicas to report their offset now.
    db.replication.request_acks();

    let deadline = if timeout_ms == 0 {
        None
    } else {
        Some(tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms as u64))
    };

    loop {
        // Re-check after each wake / poll.
        acked = db.replication.replicas_at_or_beyond(target);
        if acked >= num {
            return Ok(Resp::int(acked as i64));
        }
        // Wait for the next ACK or a short poll interval (in case a
        // replica ACKed between our check and the notify).
        let notified = db.replication.ack_notify.notified();
        tokio::pin!(notified);
        // Enable before re-check (classic notify race).
        notified.as_mut().enable();
        acked = db.replication.replicas_at_or_beyond(target);
        if acked >= num {
            return Ok(Resp::int(acked as i64));
        }

        if let Some(dl) = deadline {
            let remaining = dl.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(Resp::int(acked as i64));
            }
            // Also re-request acks periodically so a silent replica is poked.
            db.replication.request_acks();
            match tokio::time::timeout(
                remaining.min(std::time::Duration::from_millis(50)),
                notified,
            )
            .await
            {
                Ok(()) => continue, // woke by ACK
                Err(_) => {
                    // Timeout of this slice — loop will re-check deadline.
                    continue;
                }
            }
        } else {
            // Wait forever, but poke GETACK every 100ms.
            db.replication.request_acks();
            let _ = tokio::time::timeout(std::time::Duration::from_millis(100), notified).await;
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn cmd_wait(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("wait".to_string()));
    }
    let _ = (db, get_i64(args, 1, "WAIT")?, get_i64(args, 2, "WAIT")?);
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
        "HELP" => Ok(Resp::array(vec![
            Resp::bulk_str("SLOWLOG <subcommand> [<arg> ...]"),
            Resp::bulk_str("SLOWLOG GET [count] — show the slowlog (most recent first)"),
            Resp::bulk_str("SLOWLOG LEN — length of the slowlog"),
            Resp::bulk_str("SLOWLOG RESET — clear the slowlog"),
            Resp::bulk_str("SLOWLOG HELP — this help"),
        ])),
        _ => Err(NexradeError::Generic(format!(
            "ERR unknown subcommand or wrong number of arguments for '{sub}'. Try SLOWLOG HELP."
        ))),
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
        DataType::List(l) => l.payload_bytes(),
        DataType::Set(s) => s.payload_bytes(),
        DataType::Hash(h) => h.payload_bytes(),
        DataType::Bitmap(v) => v.len(),
        DataType::HyperLogLog(v) => v.len(),
        DataType::ZSet(z) => z.payload_bytes(),
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

pub async fn cmd_latency(db: &Db, args: &[Resp]) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("latency".to_string()));
    }
    let sub = get_str(args, 1, "LATENCY")?.to_uppercase();
    match sub.as_str() {
        "LATEST" => {
            // Shape: array of [event, timestamp, latest_us, all_time_us].
            // 0.7.4: sourced from the dedicated LatencyMonitor (every timed
            // command), not only the slowlog ring.
            let rows: Vec<Resp> = db
                .latency
                .latest()
                .into_iter()
                .map(|(name, ts, latest, max)| {
                    Resp::array(vec![
                        Resp::bulk_str(name),
                        Resp::int(ts as i64),
                        Resp::int(latest as i64),
                        Resp::int(max as i64),
                    ])
                })
                .collect();
            Ok(Resp::array(rows))
        }
        "HISTORY" => {
            // LATENCY HISTORY <event>
            let event = args
                .get(2)
                .and_then(|a| a.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            let rows: Vec<Resp> = db
                .latency
                .history(&event)
                .into_iter()
                .map(|(ts, us)| Resp::array(vec![Resp::int(ts as i64), Resp::int(us as i64)]))
                .collect();
            Ok(Resp::array(rows))
        }
        "HISTOGRAM" => {
            // LATENCY HISTOGRAM [event ...]. Redis 7.2+ map shape:
            // { event → { calls, histogram_usec → { bucket → count } } }
            let events: Vec<&str> = (2..args.len())
                .filter_map(|i| args.get(i).and_then(|a| a.as_str()))
                .collect();
            let hist = db.latency.histogram(&events);
            let mut outer: Vec<Resp> = Vec::with_capacity(hist.len() * 2);
            for (name, h) in hist {
                outer.push(Resp::bulk_str(name));
                let mut inner: Vec<Resp> = Vec::with_capacity(4);
                inner.push(Resp::bulk_str("calls"));
                inner.push(Resp::int(h.calls as i64));
                inner.push(Resp::bulk_str("histogram_usec"));
                let mut buckets: Vec<Resp> = Vec::with_capacity(h.buckets.len() * 2);
                for (b, c) in h.buckets {
                    buckets.push(Resp::bulk_str(b.to_string()));
                    buckets.push(Resp::int(c as i64));
                }
                // RESP2 flat array; RESP3 Map upgrade happens in the
                // connection layer for known map-shaped commands.
                inner.push(Resp::array(buckets));
                outer.push(Resp::array(inner));
            }
            Ok(Resp::array(outer))
        }
        "RESET" => {
            // LATENCY RESET [event ...]
            let events: Vec<&str> = (2..args.len())
                .filter_map(|i| args.get(i).and_then(|a| a.as_str()))
                .collect();
            let n = db.latency.reset(&events);
            Ok(Resp::int(n as i64))
        }
        "DOCTOR" => {
            let max = db.latency.global_max_us();
            let samples = db.latency.total_samples();
            let events = db.latency.event_count();
            let msg = if samples == 0 {
                "I'm here to keep free latency for you. No samples recorded yet                  (command timing is off when both metrics and slowlog are disabled)."
                    .to_string()
            } else if max < 1_000 {
                format!(
                    "Dave, this is your latency doctor speaking. {samples} samples                      across {events} events; all-time max is {max} µs. Looking good."
                )
            } else if max < 10_000 {
                format!(
                    "Dave, this is your latency doctor speaking. {samples} samples                      across {events} events; all-time max is {max} µs                      ({:.1} ms). Mild spikes — check slowlog for outliers.",
                    max as f64 / 1000.0
                )
            } else {
                format!(
                    "Dave, this is your latency doctor speaking. {samples} samples                      across {events} events; all-time max is {max} µs                      ({:.1} ms). Investigate with LATENCY LATEST / HISTORY                      and SLOWLOG GET.",
                    max as f64 / 1000.0
                )
            };
            Ok(Resp::bulk_str(msg))
        }
        "HELP" => Ok(Resp::array(vec![
            Resp::bulk_str("LATENCY <subcommand> [<arg> ...]"),
            Resp::bulk_str("LATENCY LATEST — latest events from the latency monitor"),
            Resp::bulk_str("LATENCY HISTORY <event> — time series for one event"),
            Resp::bulk_str(
                "LATENCY HISTOGRAM [event ...] — power-of-two bucket histogram (0.7.4+)",
            ),
            Resp::bulk_str("LATENCY RESET [event ...] — clear latency samples"),
            Resp::bulk_str("LATENCY DOCTOR — human-readable report"),
            Resp::bulk_str("LATENCY HELP — this help"),
        ])),
        _ => Err(NexradeError::Generic(format!(
            "ERR unknown subcommand or wrong number of arguments for '{sub}'. Try LATENCY HELP."
        ))),
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
    // Connection-layer state (MULTI/WATCH/AUTH/SELECT/tracking/pubsub) is
    // cleared by `Connection::handle_reset`. The dispatch-table arm is only
    // reached for non-connection callers (tests / WASM); return the Redis
    // reply shape either way.
    Ok(Resp::SimpleString("RESET".to_string()))
}

/// `TIME` — return the server's notion of time as `[unix_secs, microseconds]`.
pub async fn cmd_time() -> Result<Resp> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let dur = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Ok(Resp::array(vec![
            Resp::bulk_str(dur.as_secs().to_string()),
            Resp::bulk_str(dur.subsec_micros().to_string()),
        ]))
    }
    #[cfg(target_arch = "wasm32")]
    {
        Ok(Resp::array(vec![Resp::bulk_str("0"), Resp::bulk_str("0")]))
    }
}

/// `ROLE` — report this instance's replication role.
///
/// Primary shape: `["master", offset, [[host, port, offset], ...]]`
/// Replica shape: `["slave", host, port, state, offset]`
pub async fn cmd_role(db: &Db) -> Result<Resp> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        match db.replication.current_role() {
            ReplicationRole::Primary => {
                let offset = db.replication.replication_offset.load(Ordering::Relaxed) as i64;
                let replicas = db.replication.connected_replicas.read();
                let slave_list: Vec<Resp> = replicas
                    .iter()
                    .map(|r| {
                        let host = r.addr.ip().to_string();
                        let port = r.addr.port().to_string();
                        Resp::array(vec![
                            Resp::bulk_str(host),
                            Resp::bulk_str(port),
                            Resp::bulk_str(r.offset.to_string()),
                        ])
                    })
                    .collect();
                Ok(Resp::array(vec![
                    Resp::bulk_str("master"),
                    Resp::int(offset),
                    Resp::array(slave_list),
                ]))
            }
            ReplicationRole::Replica => {
                let (host, port) = db
                    .replication
                    .replica_of
                    .read()
                    .clone()
                    .unwrap_or_else(|| ("?".to_string(), 0));
                let state = if db.replication.primary_link_up.load(Ordering::Relaxed) {
                    "connected"
                } else {
                    "connect"
                };
                let offset = db.replication.replication_offset.load(Ordering::Relaxed) as i64;
                Ok(Resp::array(vec![
                    Resp::bulk_str("slave"),
                    Resp::bulk_str(host),
                    Resp::int(port as i64),
                    Resp::bulk_str(state),
                    Resp::int(offset),
                ]))
            }
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = db;
        Ok(Resp::array(vec![
            Resp::bulk_str("master"),
            Resp::int(0),
            Resp::array(vec![]),
        ]))
    }
}

// ── Replication commands ──────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
/// `REPLICAOF NO ONE` | `REPLICAOF <host> <port>`
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
/// `REPLCONF <subcommand> [<arg> ...]`
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
/// `PSYNC <replid> <offset>`
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
        "SETINFO" => client_setinfo(db, args, caller_id),
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

/// `CLIENT SETINFO LIB-NAME <name>` / `CLIENT SETINFO LIB-VER <ver>`
///
/// Redis 7.2+ — redis-py 5+/8 send these on connect. We store the values on
/// the connection meta so `CLIENT LIST` / `CLIENT INFO` can surface them.
fn client_setinfo(db: &Db, args: &[Resp], caller_id: u64) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("client|setinfo".to_string()));
    }
    let attr = get_str(args, 2, "CLIENT")?.to_uppercase();
    let val = match args.get(3).and_then(|a| a.as_str()) {
        Some(s) => s,
        None => {
            return Err(NexradeError::WrongArity("client|setinfo".to_string()));
        }
    };
    // Redis rejects spaces / newlines in these attributes.
    if val.contains(' ') || val.contains('\n') || val.contains('\r') {
        return Err(NexradeError::Prefixed(
            "ERR Client attributes cannot contain spaces, newlines or special characters."
                .to_string(),
        ));
    }
    let Some(m) = db.connections.meta(caller_id) else {
        return Ok(Resp::ok());
    };
    let mut g = m.write();
    match attr.as_str() {
        "LIB-NAME" => g.lib_name = val.to_string(),
        "LIB-VER" => g.lib_ver = val.to_string(),
        other => {
            return Err(NexradeError::Generic(format!(
                "ERR Unrecognized CLIENT SETINFO attribute '{other}'"
            )));
        }
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
        Resp::bulk_str("SETINFO <LIB-NAME|LIB-VER> <value>"),
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
        "LINKS" => {
            // 0.9.1: standalone emits no cluster gossip, so there are no
            // inbound/outbound cluster links. Return an empty array so
            // redis-cli --cluster info and other probes see a clean
            // "no links" report.
            Ok(Resp::array(Vec::new()))
        }
        "COUNT-FAILURE-REPORTS" => {
            // 0.9.1: standalone has no failure tracking; return 0 for
            // any node id. Tools that compare across replicas can rely on
            // this being a stable integer.
            Ok(Resp::int(0))
        }
        "FAILOVER-CHECK-RAYS" => Err(NexradeError::Generic(
            "ERR FAILOVER-CHECK-RAYS is not available in standalone              (no quorum to check against)"
                .to_string(),
        )),
        "HELP" => Ok(cluster_help()),
        // 0.9.0: explicit hard-error for multi-node subcommands. The
        // probe-only subcommands above (INFO / MYID / KEYSLOT / NODES /
        // SLOTS / COUNTKEYSINSLOT / GETKEYSINSLOT / HELP) still work so
        // tools like `redis-cli --cluster check` can inspect the server,
        // but anything that would mutate gossip state is rejected
        // because nexrade-cache is a standalone server (see
        // docs/cluster-compat.md).
        "MEET" | "FORGET" | "REPLICATE" | "RESET" | "SETSLOT"
        | "SAVECONFIG" | "FAILOVER" | "ADDSLOTS" | "ADDSLOTSRANGE"
        | "DELSLOTS" | "DELSLOTSRANGE" | "BUMPEPOCH" => Err(
            NexradeError::Generic(format!(
                "ERR CLUSTER {sub} is not supported by nexrade-cache                  (standalone server). See docs/cluster-compat.md for guidance.",
            )),
        ),
        // Anything else: still pretend we do not know it (Redis does
        // the same — unknown subcommands return OK so probes do not
        // error on newer protocol features).
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
