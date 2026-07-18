pub mod bit;
pub mod generic;
pub mod geo;
pub mod hash;
pub mod hll;
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

/// Parse the command name from a RESP array into a fresh uppercase
/// `String`. The caller may reuse the same `String` allocation across
/// calls — see `parse_cmd_name_into` below for the hot-path version.
pub fn parse_cmd_name(args: &[Resp]) -> Result<String> {
    let mut buf = String::with_capacity(8);
    parse_cmd_name_into(args, &mut buf)?;
    Ok(buf)
}

/// Hot-path variant of `parse_cmd_name` that uppercases the input
/// ASCII-only into the caller's `String`. Avoids both the Unicode
/// codepath of `to_uppercase()` and the allocation-per-call.
pub fn parse_cmd_name_into<'a>(args: &[Resp], out: &'a mut String) -> Result<&'a str> {
    let arg0 = args
        .first()
        .and_then(|a| a.as_str())
        .ok_or_else(|| NexradeError::ProtocolError("empty command".to_string()))?;
    out.clear();
    out.push_str(arg0);
    out.make_ascii_uppercase();
    Ok(out.as_str())
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
        | "LMPOP" | "BLMPOP"
        // Hash
        | "HSET" | "HMSET" | "HDEL" | "HSETNX" | "HINCRBY" | "HINCRBYFLOAT"
        // Set
        | "SADD" | "SREM" | "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE" | "SMOVE" | "SPOP"
        // ZSet
        | "ZADD" | "ZINCRBY" | "ZREM" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZPOPMIN"
        | "ZPOPMAX" | "ZUNIONSTORE" | "ZINTERSTORE" | "ZRANGESTORE" | "ZDIFFSTORE" | "ZMPOP" | "BZMPOP"
        // Generic
        | "DEL" | "UNLINK" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST"
        | "RENAME" | "RENAMENX" | "COPY" | "MOVE" | "RESTORE" | "SORT"
        // Server
        | "FLUSHDB" | "FLUSHALL"
        // Stream
        | "XADD" | "XTRIM" | "XDEL" | "XGROUP" | "XREADGROUP" | "XACK" | "XCLAIM" | "XAUTOCLAIM"
        // Bitmap
        | "SETBIT" | "BITOP" | "BITFIELD"
        // Geo
        | "GEOADD"
        // HyperLogLog
        | "PFADD" | "PFMERGE"
    )
}

/// Dispatch a command as the implicit `"default"` ACL user.
///
/// **System-internal only** — AOF replay, replication apply, embedded
/// (WASM) callers, and library tests. These contexts don't have a
/// real authentication identity to carry: the original command was
/// already authorized at the time it was originally executed (AOF
/// /replication replay), the caller is a trusted in-process embedder
/// (WASM), or the user is irrelevant to the test's intent. Replaying
/// these commands under `"default"` is intentional — re-checking ACL
/// with a different identity would either weaken enforcement (since
/// `"default"` is full-access in the strictest sense) or break valid
/// commands at replay time.
///
/// **For user-facing command paths**, use `dispatch_with_user` (or
/// `dispatch_tracked`) which take the connection's authenticated
/// identity. Hard-coding a known-vulnerable pattern at a real user
/// path by accidentally reaching for this function is the whole
/// reason the type system doesn't make it more convenient; if you
/// have to ask "is this `dispatch()` call safe?" the answer is "no,
/// unless it's documented as system-internal in this comment."
pub async fn dispatch(db: &Db, args: Vec<Resp>, db_index: usize) -> Resp {
    dispatch_inner_callable(db, args, db_index, None, "default").await
}

/// Dispatch with an optional peer address (used by the connection handler)
/// and an ACL user (used by the embedded API and library tests; the
/// connection handler passes its authenticated user via `dispatch_with_user`).
pub async fn dispatch_with_addr(
    db: &Db,
    args: Vec<Resp>,
    db_index: usize,
    peer_addr: Option<std::net::SocketAddr>,
) -> Resp {
    dispatch_inner_callable(db, args, db_index, peer_addr, "default").await
}

/// Dispatch for a specific authenticated user. `client_id` is 0 for
/// contexts without a real client connection (embedded API, Lua, tests) —
/// CLIENT TRACKING never applies to those since nothing is registered
/// under id 0.
pub async fn dispatch_with_user(
    db: &Db,
    args: Vec<Resp>,
    db_index: usize,
    peer_addr: Option<std::net::SocketAddr>,
    user: &str,
) -> Resp {
    dispatch_inner_callable(db, args, db_index, peer_addr, user).await
}

/// Internal entry used by the public helpers above — parses `cmd`
/// itself so callers don't have to.
async fn dispatch_inner_callable(
    db: &Db,
    args: Vec<Resp>,
    db_index: usize,
    peer_addr: Option<std::net::SocketAddr>,
    user: &str,
) -> Resp {
    let mut cmd_buf = String::with_capacity(8);
    let cmd = match parse_cmd_name_into(&args, &mut cmd_buf) {
        Ok(s) => s,
        Err(e) => return Resp::Error(e.to_string()),
    };
    dispatch_tracked(db, args, db_index, peer_addr, user, 0, cmd).await
}

/// Dispatch for a specific authenticated user + client id + pre-parsed
/// uppercase cmd name. The connection handler reuses a per-connection
/// `String` to avoid the per-command `String` allocation that
/// `parse_cmd_name` would otherwise incur on the hot path.
pub async fn dispatch_tracked(
    db: &Db,
    args: Vec<Resp>,
    db_index: usize,
    peer_addr: Option<std::net::SocketAddr>,
    user: &str,
    client_id: u64,
    cmd: &str,
) -> Resp {
    let is_write = is_write_command(cmd);
    let is_flush = matches!(cmd, "FLUSHALL" | "FLUSHDB");

    // CLIENT PAUSE — gate writes server-wide during the pause window.
    // Placed before AOF/replication/dispatch so a paused write never
    // touches the store, never appends to AOF, never propagates.
    if is_write && db.connections.is_paused() {
        return Resp::Error(
            NexradeError::Prefixed("PAUSE Write pause in effect, please retry later".to_string())
                .to_string(),
        );
    }

    // Key extraction is only needed when:
    //   * ACL is not fully open (must pass keys into check_permission), or
    //   * CLIENT TRACKING is enabled for someone (must feed on_write/track_read).
    // Under redis-benchmark neither is true — skip the Vec allocation.
    let need_keys = !is_flush && (!db.acl.is_open() || db.tracking.enabled_count() > 0);
    let touched_keys: Vec<Vec<u8>> = if need_keys {
        extract_keys(cmd, &args)
            .into_iter()
            .map(|k| k.to_vec())
            .collect()
    } else {
        Vec::new()
    };
    let key_refs: Vec<&[u8]> = touched_keys.iter().map(|k| k.as_slice()).collect();

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

    // Enforce maxmemory before write commands. Lock-free fast path:
    // if `db.max_memory_limit` is 0 (the default), there's nothing to
    // enforce and we never take the config lock.
    if is_write {
        let limit = db.max_memory_limit.load(Ordering::Relaxed);
        if limit > 0 {
            let policy_u8 = db.maxmemory_policy.load(Ordering::Relaxed);
            // Decode u8 → MaxMemoryPolicy. NoEviction (0) is a no-op
            // in `evict_if_needed`; we still call it because the
            // dispatcher expects a uniform control flow.
            let policy = match policy_u8 {
                1 => crate::db::MaxMemoryPolicy::AllKeysRandom,
                2 => crate::db::MaxMemoryPolicy::AllKeysLru,
                3 => crate::db::MaxMemoryPolicy::VolatileRandom,
                4 => crate::db::MaxMemoryPolicy::VolatileLru,
                5 => crate::db::MaxMemoryPolicy::VolatileTtl,
                _ => crate::db::MaxMemoryPolicy::NoEviction,
            };
            db.store.evict_if_needed(&policy, limit);
        }
    }

    let result = match dispatch_inner(
        db, args, db_index, peer_addr, user, client_id, &key_refs, cmd,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => Resp::Error(e.to_string()),
    };

    let succeeded = !matches!(result, Resp::Error(_));

    if is_write && succeeded {
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

    // CLIENT TRACKING bookkeeping: reads arm invalidation for this client,
    // writes fire invalidation pushes to every client tracking the touched
    // keys. FLUSHALL/FLUSHDB use a dedicated broadcast instead of per-key.
    if succeeded {
        if is_flush {
            db.tracking.flush_all();
        } else if !key_refs.is_empty() {
            if is_write {
                db.tracking.on_write(&key_refs, client_id);
            } else {
                db.tracking.track_read(client_id, &key_refs);
            }
        }
    }

    result
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_inner(
    db: &Db,
    args: Vec<Resp>,
    db_index: usize,
    peer_addr: Option<std::net::SocketAddr>,
    authenticated_user: &str,
    client_id: u64,
    keys: &[&[u8]],
    cmd: &str,
) -> Result<Resp> {
    if args.is_empty() {
        return Err(NexradeError::ProtocolError("empty command".to_string()));
    }

    // ACL check: enforce command + key-pattern permissions on the
    // authenticated user. The connection handler decides who the caller is
    // and passes that name in. `keys` and `cmd` are pre-computed by the
    // caller — saves one `extract_keys` allocation and one `String`
    // allocation per dispatch.
    if let Err(e) = db.acl.check_permission(authenticated_user, cmd, keys) {
        // AclError's Display already carries its own reply-code prefix
        // (WRONGPASS / NOPERM) — use Prefixed so we don't double it up.
        return Err(NexradeError::Prefixed(e.to_string()));
    }

    match cmd {
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
        "LMPOP" => list::cmd_lmpop(db, &args, db_index).await,
        "BLMPOP" => list::cmd_blmpop(db, &args, db_index).await,

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
        "ZRANGESTORE" => zset::cmd_zrangestore(db, &args, db_index).await,
        "ZMPOP" => zset::cmd_zmpop(db, &args, db_index).await,
        "BZMPOP" => zset::cmd_bzmpop(db, &args, db_index).await,
        "ZINTER" => zset::cmd_zinter(db, &args, db_index).await,
        "ZUNION" => zset::cmd_zunion(db, &args, db_index).await,
        "ZDIFF" => zset::cmd_zdiff(db, &args, db_index).await,
        "ZDIFFSTORE" => zset::cmd_zdiffstore(db, &args, db_index).await,
        "ZINTERCARD" => zset::cmd_zintercard(db, &args, db_index).await,

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
        "SORT_RO" => generic::cmd_sort_ro(db, &args, db_index).await,
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
        "WAIT" => server::cmd_wait(db, &args).await,
        "MEMORY" => server::cmd_memory(db, &args, db_index).await,
        "LATENCY" => server::cmd_latency(&args).await,
        "ACL" => server::cmd_acl(db, &args, authenticated_user).await,
        "RESET" => server::cmd_reset().await,
        "TIME" => server::cmd_time().await,
        "ROLE" => server::cmd_role(db).await,
        "CLIENT" => server::cmd_client(db, &args, client_id).await,
        "CLUSTER" => server::cmd_cluster(db, &args).await,
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
        "XINFO" => stream::cmd_xinfo(db, &args, db_index).await,
        "XCLAIM" => stream::cmd_xclaim(db, &args, db_index).await,
        "XAUTOCLAIM" => stream::cmd_xautoclaim(db, &args, db_index).await,

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

        // --- HyperLogLog commands ---
        "PFADD" => hll::cmd_pfadd(db, &args, db_index).await,
        "PFCOUNT" => hll::cmd_pfcount(db, &args, db_index).await,
        "PFMERGE" => hll::cmd_pfmerge(db, &args, db_index).await,

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
            Err(NexradeError::UnknownCommand(cmd.to_string(), suffix))
        }
    }
}

/// Best-effort key extraction for ACL pattern checks. Returns the bulk-string
/// arguments that look like keys, for commands where keys occupy known
/// positions in the argument vector. Commands not listed here either don't
/// take keys or have a shape where we can't reliably extract them; those
/// pass an empty slice and rely on the per-command allow/deny rules.
fn extract_keys<'a>(cmd: &str, args: &'a [Resp]) -> Vec<&'a [u8]> {
    let get = |idx: usize| -> Option<&'a [u8]> {
        args.get(idx).and_then(|r| r.as_bytes().map(|b| b.as_ref()))
    };

    // (start_index, stride, count) — count = None means "up to end".
    let spec: &[(usize, usize, Option<usize>)] = match cmd {
        // Single-key commands.
        "GET" | "GETSET" | "GETDEL" | "GETEX" | "SET" | "SETNX" | "SETEX" | "PSETEX" | "STRLEN"
        | "GETRANGE" | "SETRANGE" | "APPEND" | "INCR" | "INCRBY" | "INCRBYFLOAT" | "DECR"
        | "DECRBY" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "EXPIRETIME"
        | "PEXPIRETIME" | "TTL" | "PTTL" | "PERSIST" | "DUMP" | "RESTORE" | "TYPE" | "OBJECT"
        | "RENAMENX" | "TOUCH" | "MOVE" | "BITCOUNT" | "BITPOS" | "GETBIT" | "SETBIT"
        | "BITFIELD" | "LPUSH" | "LPUSHX" | "RPUSH" | "RPUSHX" | "LPOP" | "RPOP" | "LLEN"
        | "LRANGE" | "LINDEX" | "LINSERT" | "LSET" | "LREM" | "LTRIM" | "LMPOP" | "BLMPOP"
        | "HSET" | "HMSET" | "HGET" | "HMGET" | "HDEL" | "HEXISTS" | "HGETALL" | "HKEYS"
        | "HVALS" | "HLEN" | "HSETNX" | "HINCRBY" | "HINCRBYFLOAT" | "HSCAN" | "HRANDFIELD"
        | "SADD" | "SREM" | "SISMEMBER" | "SMISMEMBER" | "SMEMBERS" | "SCARD" | "SRANDMEMBER"
        | "SPOP" | "SSCAN" | "SMOVE" | "ZADD" | "ZREM" | "ZSCORE" | "ZMSCORE" | "ZINCRBY"
        | "ZCARD" | "ZCOUNT" | "ZLEXCOUNT" | "ZRANGE" | "ZRANGEBYSCORE" | "ZRANGEBYLEX"
        | "ZREVRANGE" | "ZREVRANGEBYSCORE" | "ZRANK" | "ZREVRANK" | "ZPOPMIN" | "ZPOPMAX"
        | "ZRANDMEMBER" | "ZSCAN" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "PFADD"
        | "PFCOUNT" | "PFMERGE" | "GEOADD" | "GEOPOS" | "GEODIST" | "GEOHASH" | "XADD" | "XLEN"
        | "XRANGE" | "XREVRANGE" | "XTRIM" | "XDEL" | "XGROUP" | "XACK" | "XPENDING" | "XCLAIM"
        | "XAUTOCLAIM" | "WAIT" => &[(1, 0, Some(1))],

        // XINFO STREAM|GROUPS|CONSUMERS <key> — key is at index 2.
        "XINFO" => &[(2, 0, Some(1))],

        // Multi-key commands: take every remaining key position.
        "DEL" | "UNLINK" | "EXISTS" => &[(1, 1, None)],
        "MGET" => &[(1, 1, None)],
        "MSET" => &[(1, 2, None)], // (k1, v1, k2, v2, …)
        "MSETNX" => &[(1, 2, None)],
        "RENAME" | "COPY" => &[(1, 1, Some(2))], // src, dst
        "LMOVE" | "BLMOVE" => &[(1, 1, Some(2))],

        // BITOP op destkey key [key …]
        "BITOP" => &[(2, 1, None)],

        // Sorted-set multi-key ops.
        "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" | "ZRANGESTORE" => &[(2, 1, None)],
        "ZUNION" | "ZINTER" | "ZDIFF" | "ZMPOP" | "BZMPOP" => &[(2, 1, None)],
        "ZINTERCARD" => &[(2, 1, None)],

        // Stream: XREAD/XREADGROUP layout is: COUNT? BLOCK? GROUP group consumer
        // COUNT? NOACK? STREAMS k1 k2 … id1 id2 … — keys aren't easily
        // extractable here; we just say "no keys" so the @keyspace check
        // is bypassed.

        // Commands without keys (return empty).
        _ => &[],
    };

    let mut out: Vec<&[u8]> = Vec::new();
    for (start, stride, count) in spec {
        let mut idx = *start;
        let mut taken = 0usize;
        while idx < args.len() {
            if let Some(b) = get(idx) {
                out.push(b);
            }
            idx += 1 + stride.saturating_sub(1);
            taken += 1;
            if let Some(limit) = count {
                if taken >= *limit {
                    break;
                }
            }
        }
    }
    out
}
