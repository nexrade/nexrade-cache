pub mod bit;
pub mod generic;
pub mod geo;
pub mod hash;
pub mod list;
pub mod server;
pub mod set;
pub mod stream;
pub mod string;
pub mod zset;

use std::sync::atomic::Ordering;

use crate::db::Db;
use crate::error::{NexradeError, Result};
#[cfg(not(target_arch = "wasm32"))]
use crate::persistence::AofSync;
use crate::resp::Resp;

/// Parse the command name from a RESP array.
pub fn parse_cmd_name(args: &[Resp]) -> Result<String> {
    args.first()
        .and_then(|a| a.as_str())
        .map(|s| s.to_uppercase())
        .ok_or_else(|| NexradeError::ProtocolError("empty command".to_string()))
}

/// Get a string argument at index.
pub fn get_str<'a>(args: &'a [Resp], idx: usize, cmd: &str) -> Result<&'a str> {
    args.get(idx)
        .and_then(|a| a.as_str())
        .ok_or_else(|| NexradeError::WrongArity(cmd.to_lowercase()))
}

/// Get a bytes argument at index.
pub fn get_bytes(args: &[Resp], idx: usize, cmd: &str) -> Result<bytes::Bytes> {
    args.get(idx)
        .and_then(|a| a.as_bytes().cloned())
        .ok_or_else(|| NexradeError::WrongArity(cmd.to_lowercase()))
}

/// Get a bytes argument as Vec<u8>.
pub fn get_bytes_vec(args: &[Resp], idx: usize, cmd: &str) -> Result<Vec<u8>> {
    args.get(idx)
        .and_then(|a| match a {
            Resp::BulkString(Some(b)) => Some(b.to_vec()),
            Resp::SimpleString(s) => Some(s.as_bytes().to_vec()),
            _ => None,
        })
        .ok_or_else(|| NexradeError::WrongArity(cmd.to_lowercase()))
}

/// Get an i64 argument at index.
pub fn get_i64(args: &[Resp], idx: usize, cmd: &str) -> Result<i64> {
    let s = get_str(args, idx, cmd)?;
    s.parse::<i64>().map_err(|_| NexradeError::NotInteger)
}

/// Get an f64 argument at index.
pub fn get_f64(args: &[Resp], idx: usize, cmd: &str) -> Result<f64> {
    let s = get_str(args, idx, cmd)?;
    s.parse::<f64>().map_err(|_| NexradeError::NotFloat)
}

/// Returns true if the command mutates data (used for dirty tracking and AOF).
pub fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        // String
        "SET" | "GETSET" | "GETDEL" | "GETEX" | "MSET" | "MSETNX" | "SETNX" | "SETEX"
        | "PSETEX" | "INCR" | "INCRBY" | "INCRBYFLOAT" | "DECR" | "DECRBY" | "APPEND"
        | "SETRANGE"
        // List
        | "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LPOP" | "RPOP" | "LSET" | "LINSERT"
        | "LREM" | "LTRIM" | "LMOVE" | "RPOPLPUSH" | "BLPOP" | "BRPOP"
        // Hash
        | "HSET" | "HMSET" | "HDEL" | "HSETNX" | "HINCRBY" | "HINCRBYFLOAT"
        // Set
        | "SADD" | "SREM" | "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE" | "SMOVE" | "SPOP"
        // ZSet
        | "ZADD" | "ZINCRBY" | "ZREM" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZPOPMIN"
        | "ZPOPMAX" | "ZUNIONSTORE" | "ZINTERSTORE"
        // Generic
        | "DEL" | "UNLINK" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST"
        | "RENAME" | "RENAMENX" | "COPY" | "MOVE" | "RESTORE" | "SORT"
        // Server
        | "FLUSHDB" | "FLUSHALL"
        // Stream
        | "XADD" | "XTRIM" | "XDEL" | "XGROUP" | "XREADGROUP" | "XACK"
        // Bitmap
        | "SETBIT" | "BITOP" | "BITFIELD"
        // Geo
        | "GEOADD"
    )
}

/// Dispatch a command to the appropriate handler.
///
/// `peer_addr` is the remote socket address of the connection and is forwarded
/// to replication commands (e.g. REPLCONF ACK).
pub async fn dispatch(db: &Db, args: Vec<Resp>, db_index: usize) -> Resp {
    dispatch_with_addr(db, args, db_index, None).await
}

/// Dispatch with an optional peer address (used by the connection handler).
pub async fn dispatch_with_addr(
    db: &Db,
    args: Vec<Resp>,
    db_index: usize,
    peer_addr: Option<std::net::SocketAddr>,
) -> Resp {
    let cmd = parse_cmd_name(&args).unwrap_or_default();
    let is_write = is_write_command(&cmd);

    #[cfg(not(target_arch = "wasm32"))]
    let aof_bytes: Option<Vec<u8>> = if is_write && db.stats.aof_enabled.load(Ordering::Relaxed) {
        let select_cmd = Resp::Array(Some(vec![
            Resp::bulk_str("SELECT"),
            Resp::bulk_str(db_index.to_string()),
        ]));
        let mut bytes = select_cmd.serialize().to_vec();
        bytes.extend_from_slice(&Resp::Array(Some(args.clone())).serialize());
        Some(bytes)
    } else {
        None
    };

    // Enforce maxmemory before write commands.
    if is_write {
        let (max_mem, policy) = {
            let cfg = db.config.lock();
            (cfg.max_memory, cfg.maxmemory_policy.clone())
        };
        if let Some(limit) = max_mem {
            if limit > 0 {
                db.store.evict_if_needed(&policy, limit);
            }
        }
    }

    let result = match dispatch_inner(db, args, db_index, peer_addr).await {
        Ok(resp) => resp,
        Err(e) => Resp::Error(e.to_string()),
    };

    if is_write && !matches!(result, Resp::Error(_)) {
        db.stats.dirty_keys.fetch_add(1, Ordering::Relaxed);

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(bytes) = aof_bytes {
            let aof_sync = db.config.lock().persistence.aof_sync.clone();
            let mut writer_guard = db.aof_writer.lock();
            if let Some(ref mut w) = *writer_guard {
                if let Err(e) = w.append(&bytes) {
                    tracing::error!("AOF append error: {}", e);
                } else if aof_sync == AofSync::Always {
                    if let Err(e) = w.fsync() {
                        tracing::error!("AOF fsync error: {}", e);
                    }
                }
            }
        }
    }

    result
}

async fn dispatch_inner(
    db: &Db,
    args: Vec<Resp>,
    db_index: usize,
    peer_addr: Option<std::net::SocketAddr>,
) -> Result<Resp> {
    if args.is_empty() {
        return Err(NexradeError::ProtocolError("empty command".to_string()));
    }

    let cmd = parse_cmd_name(&args)?;

    match cmd.as_str() {
        // --- String commands ---
        "SET" => string::cmd_set(db, &args, db_index).await,
        "GET" => string::cmd_get(db, &args, db_index).await,
        "GETSET" => string::cmd_getset(db, &args, db_index).await,
        "GETDEL" => string::cmd_getdel(db, &args, db_index).await,
        "GETEX" => string::cmd_getex(db, &args, db_index).await,
        "MSET" => string::cmd_mset(db, &args, db_index).await,
        "MSETNX" => string::cmd_msetnx(db, &args, db_index).await,
        "MGET" => string::cmd_mget(db, &args, db_index).await,
        "SETNX" => string::cmd_setnx(db, &args, db_index).await,
        "SETEX" => string::cmd_setex(db, &args, db_index).await,
        "PSETEX" => string::cmd_psetex(db, &args, db_index).await,
        "INCR" => string::cmd_incr(db, &args, db_index).await,
        "INCRBY" => string::cmd_incrby(db, &args, db_index).await,
        "INCRBYFLOAT" => string::cmd_incrbyfloat(db, &args, db_index).await,
        "DECR" => string::cmd_decr(db, &args, db_index).await,
        "DECRBY" => string::cmd_decrby(db, &args, db_index).await,
        "APPEND" => string::cmd_append(db, &args, db_index).await,
        "STRLEN" => string::cmd_strlen(db, &args, db_index).await,
        "GETRANGE" | "SUBSTR" => string::cmd_getrange(db, &args, db_index).await,
        "SETRANGE" => string::cmd_setrange(db, &args, db_index).await,

        // --- List commands ---
        "LPUSH" => list::cmd_lpush(db, &args, db_index).await,
        "RPUSH" => list::cmd_rpush(db, &args, db_index).await,
        "LPUSHX" => list::cmd_lpushx(db, &args, db_index).await,
        "RPUSHX" => list::cmd_rpushx(db, &args, db_index).await,
        "LPOP" => list::cmd_lpop(db, &args, db_index).await,
        "RPOP" => list::cmd_rpop(db, &args, db_index).await,
        "LLEN" => list::cmd_llen(db, &args, db_index).await,
        "LRANGE" => list::cmd_lrange(db, &args, db_index).await,
        "LINDEX" => list::cmd_lindex(db, &args, db_index).await,
        "LSET" => list::cmd_lset(db, &args, db_index).await,
        "LINSERT" => list::cmd_linsert(db, &args, db_index).await,
        "LREM" => list::cmd_lrem(db, &args, db_index).await,
        "LTRIM" => list::cmd_ltrim(db, &args, db_index).await,
        "LMOVE" => list::cmd_lmove(db, &args, db_index).await,
        "RPOPLPUSH" => list::cmd_rpoplpush(db, &args, db_index).await,
        "LPOS" => list::cmd_lpos(db, &args, db_index).await,
        "BLPOP" => list::cmd_blpop(db, &args, db_index).await,
        "BRPOP" => list::cmd_brpop(db, &args, db_index).await,

        // --- Hash commands ---
        "HSET" => hash::cmd_hset(db, &args, db_index).await,
        "HMSET" => hash::cmd_hmset(db, &args, db_index).await,
        "HGET" => hash::cmd_hget(db, &args, db_index).await,
        "HMGET" => hash::cmd_hmget(db, &args, db_index).await,
        "HDEL" => hash::cmd_hdel(db, &args, db_index).await,
        "HEXISTS" => hash::cmd_hexists(db, &args, db_index).await,
        "HGETALL" => hash::cmd_hgetall(db, &args, db_index).await,
        "HKEYS" => hash::cmd_hkeys(db, &args, db_index).await,
        "HVALS" => hash::cmd_hvals(db, &args, db_index).await,
        "HLEN" => hash::cmd_hlen(db, &args, db_index).await,
        "HSETNX" => hash::cmd_hsetnx(db, &args, db_index).await,
        "HINCRBY" => hash::cmd_hincrby(db, &args, db_index).await,
        "HINCRBYFLOAT" => hash::cmd_hincrbyfloat(db, &args, db_index).await,
        "HSCAN" => hash::cmd_hscan(db, &args, db_index).await,
        "HRANDFIELD" => hash::cmd_hrandfield(db, &args, db_index).await,

        // --- Set commands ---
        "SADD" => set::cmd_sadd(db, &args, db_index).await,
        "SREM" => set::cmd_srem(db, &args, db_index).await,
        "SISMEMBER" => set::cmd_sismember(db, &args, db_index).await,
        "SMISMEMBER" => set::cmd_smismember(db, &args, db_index).await,
        "SMEMBERS" => set::cmd_smembers(db, &args, db_index).await,
        "SCARD" => set::cmd_scard(db, &args, db_index).await,
        "SUNION" => set::cmd_sunion(db, &args, db_index).await,
        "SUNIONSTORE" => set::cmd_sunionstore(db, &args, db_index).await,
        "SINTER" => set::cmd_sinter(db, &args, db_index).await,
        "SINTERSTORE" => set::cmd_sinterstore(db, &args, db_index).await,
        "SDIFF" => set::cmd_sdiff(db, &args, db_index).await,
        "SDIFFSTORE" => set::cmd_sdiffstore(db, &args, db_index).await,
        "SMOVE" => set::cmd_smove(db, &args, db_index).await,
        "SRANDMEMBER" => set::cmd_srandmember(db, &args, db_index).await,
        "SPOP" => set::cmd_spop(db, &args, db_index).await,
        "SSCAN" => set::cmd_sscan(db, &args, db_index).await,

        // --- Sorted Set commands ---
        "ZADD" => zset::cmd_zadd(db, &args, db_index).await,
        "ZCARD" => zset::cmd_zcard(db, &args, db_index).await,
        "ZSCORE" => zset::cmd_zscore(db, &args, db_index).await,
        "ZMSCORE" => zset::cmd_zmscore(db, &args, db_index).await,
        "ZINCRBY" => zset::cmd_zincrby(db, &args, db_index).await,
        "ZRANK" => zset::cmd_zrank(db, &args, db_index).await,
        "ZREVRANK" => zset::cmd_zrevrank(db, &args, db_index).await,
        "ZRANGE" => zset::cmd_zrange(db, &args, db_index).await,
        "ZREVRANGE" => zset::cmd_zrevrange(db, &args, db_index).await,
        "ZRANGEBYSCORE" => zset::cmd_zrangebyscore(db, &args, db_index).await,
        "ZREVRANGEBYSCORE" => zset::cmd_zrevrangebyscore(db, &args, db_index).await,
        "ZRANGEBYLEX" => zset::cmd_zrangebylex(db, &args, db_index).await,
        "ZCOUNT" => zset::cmd_zcount(db, &args, db_index).await,
        "ZLEXCOUNT" => zset::cmd_zlexcount(db, &args, db_index).await,
        "ZREM" => zset::cmd_zrem(db, &args, db_index).await,
        "ZREMRANGEBYRANK" => zset::cmd_zremrangebyrank(db, &args, db_index).await,
        "ZREMRANGEBYSCORE" => zset::cmd_zremrangebyscore(db, &args, db_index).await,
        "ZPOPMIN" => zset::cmd_zpopmin(db, &args, db_index).await,
        "ZPOPMAX" => zset::cmd_zpopmax(db, &args, db_index).await,
        "ZRANDMEMBER" => zset::cmd_zrandmember(db, &args, db_index).await,
        "ZUNIONSTORE" => zset::cmd_zunionstore(db, &args, db_index).await,
        "ZINTERSTORE" => zset::cmd_zinterstore(db, &args, db_index).await,
        "ZSCAN" => zset::cmd_zscan(db, &args, db_index).await,

        // --- Generic key commands ---
        "DEL" => generic::cmd_del(db, &args, db_index).await,
        "UNLINK" => generic::cmd_unlink(db, &args, db_index).await,
        "EXISTS" => generic::cmd_exists(db, &args, db_index).await,
        "EXPIRE" => generic::cmd_expire(db, &args, db_index).await,
        "PEXPIRE" => generic::cmd_pexpire(db, &args, db_index).await,
        "EXPIREAT" => generic::cmd_expireat(db, &args, db_index).await,
        "PEXPIREAT" => generic::cmd_pexpireat(db, &args, db_index).await,
        "EXPIRETIME" => generic::cmd_expiretime(db, &args, db_index).await,
        "PEXPIRETIME" => generic::cmd_pexpiretime(db, &args, db_index).await,
        "TTL" => generic::cmd_ttl(db, &args, db_index).await,
        "PTTL" => generic::cmd_pttl(db, &args, db_index).await,
        "PERSIST" => generic::cmd_persist(db, &args, db_index).await,
        "TYPE" => generic::cmd_type(db, &args, db_index).await,
        "RENAME" => generic::cmd_rename(db, &args, db_index).await,
        "RENAMENX" => generic::cmd_renamenx(db, &args, db_index).await,
        "KEYS" => generic::cmd_keys(db, &args, db_index).await,
        "SCAN" => generic::cmd_scan(db, &args, db_index).await,
        "RANDOMKEY" => generic::cmd_randomkey(db, &args, db_index).await,
        "COPY" => generic::cmd_copy(db, &args, db_index).await,
        "MOVE" => generic::cmd_move(db, &args, db_index).await,
        "OBJECT" => generic::cmd_object(db, &args, db_index).await,
        "DUMP" => generic::cmd_dump(db, &args, db_index).await,
        "RESTORE" => generic::cmd_restore(db, &args, db_index).await,
        "SORT" => generic::cmd_sort(db, &args, db_index).await,
        "TOUCH" => generic::cmd_touch(db, &args, db_index).await,

        // --- Server commands ---
        "PING" => server::cmd_ping(&args).await,
        "ECHO" => server::cmd_echo(&args).await,
        "QUIT" => server::cmd_quit().await,
        "SELECT" => server::cmd_select(&args).await,
        "DBSIZE" => server::cmd_dbsize(db, db_index).await,
        "FLUSHDB" => server::cmd_flushdb(db, &args, db_index).await,
        "FLUSHALL" => server::cmd_flushall(db, &args).await,
        "INFO" => server::cmd_info(db, &args).await,
        "CONFIG" => server::cmd_config(db, &args).await,
        "COMMAND" => server::cmd_command(&args).await,
        "SAVE" => server::cmd_save(db).await,
        "BGSAVE" => server::cmd_bgsave(db).await,
        "BGREWRITEAOF" => server::cmd_bgrewriteaof(db).await,
        "LASTSAVE" => server::cmd_lastsave(db).await,
        "DEBUG" => server::cmd_debug(&args).await,
        "SHUTDOWN" => server::cmd_shutdown(db, &args).await,
        "SLOWLOG" => server::cmd_slowlog(db, &args).await,
        "MEMORY" => server::cmd_memory(db, &args, db_index).await,
        "LATENCY" => server::cmd_latency(&args).await,
        "ACL" => server::cmd_acl(&args).await,
        "RESET" => server::cmd_reset().await,
        "CLIENT" => server::cmd_client(&args).await,
        "CLUSTER" => server::cmd_cluster(&args).await,
        "HELLO" => server::cmd_hello(&args).await,
        "PUBLISH" => server::cmd_publish(db, &args).await,
        "PUBSUB" => server::cmd_pubsub(db, &args).await,

        // --- Replication commands ---
        #[cfg(not(target_arch = "wasm32"))]
        "REPLICAOF" | "SLAVEOF" => server::cmd_replicaof(db, &args).await,
        #[cfg(not(target_arch = "wasm32"))]
        "REPLCONF" => server::cmd_replconf(db, &args, peer_addr).await,
        #[cfg(not(target_arch = "wasm32"))]
        "PSYNC" => server::cmd_psync(db, &args).await,

        // --- Stream commands ---
        "XADD" => stream::cmd_xadd(db, &args, db_index).await,
        "XLEN" => stream::cmd_xlen(db, &args, db_index).await,
        "XRANGE" => stream::cmd_xrange(db, &args, db_index).await,
        "XREVRANGE" => stream::cmd_xrevrange(db, &args, db_index).await,
        "XREAD" => stream::cmd_xread(db, &args, db_index).await,
        "XTRIM" => stream::cmd_xtrim(db, &args, db_index).await,
        "XDEL" => stream::cmd_xdel(db, &args, db_index).await,
        "XGROUP" => stream::cmd_xgroup(db, &args, db_index).await,
        "XREADGROUP" => stream::cmd_xreadgroup(db, &args, db_index).await,
        "XACK" => stream::cmd_xack(db, &args, db_index).await,
        "XPENDING" => stream::cmd_xpending(db, &args, db_index).await,

        // --- Bitmap commands ---
        "SETBIT" => bit::cmd_setbit(db, &args, db_index).await,
        "GETBIT" => bit::cmd_getbit(db, &args, db_index).await,
        "BITCOUNT" => bit::cmd_bitcount(db, &args, db_index).await,
        "BITOP" => bit::cmd_bitop(db, &args, db_index).await,
        "BITPOS" => bit::cmd_bitpos(db, &args, db_index).await,
        "BITFIELD" => bit::cmd_bitfield(db, &args, db_index).await,

        // --- Geo commands ---
        "GEOADD" => geo::cmd_geoadd(db, &args, db_index).await,
        "GEOPOS" => geo::cmd_geopos(db, &args, db_index).await,
        "GEODIST" => geo::cmd_geodist(db, &args, db_index).await,
        "GEOHASH" => geo::cmd_geohash(db, &args, db_index).await,
        "GEORADIUS" => geo::cmd_georadius(db, &args, db_index).await,
        "GEORADIUSBYMEMBER" => geo::cmd_georadiusbymember(db, &args, db_index).await,
        "GEOSEARCH" => geo::cmd_geosearch(db, &args, db_index).await,

        _ => {
            let args_preview = args
                .iter()
                .skip(1)
                .take(3)
                .filter_map(|a| a.as_str())
                .map(|s| format!("'{s}'"))
                .collect::<Vec<_>>()
                .join(" ");
            let suffix = if args_preview.is_empty() {
                String::new()
            } else {
                format!(", with args beginning with: {args_preview} ")
            };
            Err(NexradeError::UnknownCommand(cmd, suffix))
        }
    }
}
