pub mod bit;
pub mod command_table;
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

/// How long a write waits for an in-flight persistence capture to finish
/// before giving up and reporting `MISCONF`.
///
/// The capture itself is an in-memory `snapshot_dbs()` (file I/O happens
/// after the guard drops), so it completes in well under a millisecond for
/// typical datasets. This ceiling exists only so a genuinely stuck
/// coordinator still surfaces an error instead of hanging the client
/// forever; it is not a latency target for the normal path.
#[cfg(not(target_arch = "wasm32"))]
const PERSISTENCE_QUIESCE_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Encode a SCAN-family element as an opaque cursor token.
///
/// The token is a 64-bit FNV-1a hash of the element bytes, and callers
/// iterate in **hash order** rather than byte order. That combination is
/// what makes a resumable cursor possible within the constraints:
///
/// * **Must be a decimal integer on the wire.** Cursors are opaque to
///   *application* code, but not to client libraries: `redis-py` runs
///   `int(cursor)` on the reply before handing it back, and other
///   clients parse into an integer type. A hex or byte-encoded cursor
///   raises `ValueError: invalid literal for int()` and breaks every
///   standard client, so the encoding cannot carry the raw key bytes.
/// * **Order-preserving in the iteration order.** Since SCAN promises no
///   particular ordering, we sort by `(token(k), k)` and let the hash
///   define the sequence. The cursor is then a position in that order,
///   so when the boundary element disappears mid-iteration (TTL, `DEL`,
///   a concurrent writer) the server resumes at the first element whose
///   token sorts after it instead of restarting at page 1 — the original
///   cursor bug, where a vanished boundary silently re-served the prefix
///   that had already been handed out.
/// * **Never collides with the terminator.** `0` is reserved to mean
///   "iteration complete", so the whole low 32-bit range is lifted out of
///   the way (see below) — a live element never hashes to 0.
///
/// The predecessor hashed the element but then searched a *byte*-sorted
/// list for an exact token match, mixing two incompatible orderings, and
/// parsed the cursor through `get_i64` — where the ~35% of `u64` hashes
/// above `i64::MAX` overflowed and silently reset the scan to 0.
///
/// Known limit: two distinct elements with the same 64-bit hash that
/// straddle a page boundary can leave the second unreturned. At 64 bits
/// that is negligible, and it is the only ordering-related omission left.
///
/// # Why this is the same hash the store shards on
///
/// This is bit-for-bit `store::fnv1a` (same offset basis, same prime), and
/// that is load-bearing rather than incidental: `ShardedDatabase::shard_idx`
/// is `fnv1a(key) & (num_shards - 1)`, so **the low bits of a token are
/// exactly the shard index of its key**. `scan_page` relies on that to page
/// one shard at a time instead of locking the whole keyspace. Changing
/// either hash without the other silently breaks that correspondence.
///
/// The reserved-value remap must therefore preserve the low bits. Sending a
/// hash of 0 to `1` would move it from shard 0 to shard 1; adding `2^32`
/// keeps every low bit intact (`(1 << 32) & (n - 1) == 0` for any
/// `n <= 2^32`), which is why the whole sub-`2^32` range is shifted up
/// rather than special-casing 0 alone.
pub(crate) fn scan_cursor_token(key: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = FNV_OFFSET;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    // Keep the low 32 bits clear of the "iteration complete" sentinel while
    // preserving them exactly, so shard_idx(key) survives the remap.
    if h < (1u64 << 32) {
        h + (1u64 << 32)
    } else {
        h
    }
}

/// Sort key defining the order `SCAN` walks the keyspace in.
///
/// This is `scan_cursor_token(key).reverse_bits()`, and the reversal is the
/// whole point — do not "simplify" it away.
///
/// A token's *low* bits are the shard index (see `scan_cursor_token`).
/// Reversing moves them to the *top* of the sort key, which buys two
/// properties that ordering by the raw token does not have:
///
/// * **Shard contiguity.** Every shard's keys form exactly one contiguous
///   run, so `SCAN` can hold a single shard's read lock, drain it, and move
///   on. Ordering by the raw token interleaves shards (the shard bits are
///   the least significant, so they change fastest), which would force every
///   page to touch every shard.
/// * **Immunity to a change in shard count.** The ordering never references
///   `num_shards`, and growing the shard count only *subdivides* an existing
///   run instead of interleaving it. This matters because the shard count is
///   derived from the CPU count (`store::compute_num_shards`) and is never
///   persisted, so a client can legitimately resume a cursor against a
///   server that has a different number of shards — after a restart on
///   another host, say. Ordering by `(shard_idx, token)` looks equivalent
///   but silently omits up to ~75% of the keyspace when that happens.
///
/// This is the same reverse-binary trick real Redis uses for its bucket
/// cursors, arrived at from the other direction. The visible consequence is
/// that shards are visited in bit-reversed index order (0, 32, 16, 48, …)
/// rather than 0, 1, 2, … — irrelevant, because `SCAN` guarantees no
/// particular ordering.
pub(crate) fn scan_order_key(key: &[u8]) -> u64 {
    scan_cursor_token(key).reverse_bits()
}

/// Parse a client-supplied SCAN cursor.
///
/// Returns `None` for cursor `0` (start of iteration) and for anything
/// unparseable, which Redis also treats as a fresh scan rather than an
/// error. Accepts the full `u64` range: the tokens we hand out routinely
/// exceed `i64::MAX`, so parsing through a signed type would reject or
/// wrap the majority of live cursors.
pub(crate) fn decode_scan_cursor(token: &str) -> Option<u64> {
    match token.trim().parse::<u64>() {
        Ok(0) | Err(_) => None,
        Ok(n) => Some(n),
    }
}

// Historical note: `scan_sort_by_token`, `scan_start_offset`, and
// `scan_start_offset_by` used to live here. They ordered an entire collection
// and then sliced one page out of it, which is what made a full
// `HSCAN`/`SSCAN`/`ZSCAN` walk quadratic. `scan_select_page` below replaced
// all three in 1.3.0; the ordering they defined is preserved inside it.

/// One page of a SCAN-family iteration, plus the cursor to resume from.
pub(crate) struct ScanPage<T> {
    /// The page's elements, in `(token, bytes)` order.
    pub items: Vec<T>,
    /// Cursor to hand the client: `0` when iteration is complete.
    pub next_cursor: u64,
}

/// Select one page of a SCAN-family iteration **without ordering the whole
/// collection**.
///
/// Yields the `limit` smallest elements strictly after `cursor` in
/// `(token, bytes)` order — exactly the elements that materialising,
/// `scan_sort_by_token`-ing, and slicing the collection produced, in the same
/// order, with the same `next_cursor`.
///
/// **Why this exists.** `HSCAN`/`SSCAN`/`ZSCAN` used to sort the entire
/// collection on *every* page: O(n log n) per page, O(n² log n) for a full
/// walk. Measured before this change, one `HSCAN COUNT 10` page against a
/// 100 k-field hash cost 20 ms, and walking the hash took 19 s. A page is tiny
/// next to `n`, so this is a selection problem, not a sorting one.
///
/// Keeps a bounded max-heap of the best `limit` candidates, so cost is
/// O(n log limit) time and O(limit) space in a single pass with one token hash
/// per element. `next_cursor` is derived in the same pass: it is the last
/// returned element's token, or `0` if nothing sorts after it.
pub(crate) fn scan_select_page<T, F>(
    items: impl IntoIterator<Item = T>,
    cursor: Option<u64>,
    limit: usize,
    elem: F,
) -> ScanPage<T>
where
    F: Fn(&T) -> &[u8],
{
    use std::collections::BinaryHeap;

    // Comparison key matching `scan_sort_by_token`'s `(token, bytes)` order.
    // The heap is a max-heap on it, so the root is the worst kept candidate.
    struct Cand<T> {
        token: u64,
        bytes: Vec<u8>,
        item: T,
    }
    impl<T> PartialEq for Cand<T> {
        fn eq(&self, other: &Self) -> bool {
            self.token == other.token && self.bytes == other.bytes
        }
    }
    impl<T> Eq for Cand<T> {}
    impl<T> PartialOrd for Cand<T> {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl<T> Ord for Cand<T> {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.token
                .cmp(&other.token)
                .then_with(|| self.bytes.cmp(&other.bytes))
        }
    }

    if limit == 0 {
        return ScanPage {
            items: Vec::new(),
            next_cursor: 0,
        };
    }

    let mut heap: BinaryHeap<Cand<T>> = BinaryHeap::with_capacity(limit + 1);
    // Tracks whether any candidate was rejected for being *worse* than the kept
    // set. If so, elements remain beyond this page and iteration continues.
    let mut more_beyond_page = false;

    for item in items {
        let bytes_ref = elem(&item);
        let token = scan_cursor_token(bytes_ref);
        // Strictly-greater-than resume, so iteration advances whether or not
        // the boundary element still exists.
        if let Some(c) = cursor {
            if token <= c {
                continue;
            }
        }
        if heap.len() < limit {
            let bytes = bytes_ref.to_vec();
            heap.push(Cand { token, bytes, item });
            continue;
        }
        // Cheap reject before allocating a comparison key: anything ordering
        // after the current root cannot make this page, but does mean the
        // iteration has more to walk.
        let worse_than_root = {
            let root = heap.peek().expect("non-empty when len == limit");
            token > root.token || (token == root.token && bytes_ref > root.bytes.as_slice())
        };
        if worse_than_root {
            more_beyond_page = true;
            continue;
        }
        let bytes = bytes_ref.to_vec();
        heap.push(Cand { token, bytes, item });
        // The evicted root also sorts after the page we are keeping.
        heap.pop();
        more_beyond_page = true;
    }

    // Pop yields largest-first; reverse for ascending page order.
    let mut ordered: Vec<Cand<T>> = Vec::with_capacity(heap.len());
    while let Some(c) = heap.pop() {
        ordered.push(c);
    }
    ordered.reverse();

    let next_cursor = match ordered.last() {
        Some(last) if more_beyond_page => last.token,
        // Page exhausted the collection (or it was empty): iteration complete.
        _ => 0,
    };
    ScanPage {
        items: ordered.into_iter().map(|c| c.item).collect(),
        next_cursor,
    }
}

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

/// Get a bytes argument as `Vec<u8>`.
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
        | "LREM" | "LTRIM" | "LMOVE" | "RPOPLPUSH" | "BLPOP" | "BRPOP" | "BLMOVE"
        | "LMPOP" | "BLMPOP"
        // Hash
        | "HSET" | "HMSET" | "HDEL" | "HSETNX" | "HINCRBY" | "HINCRBYFLOAT"
        // Set
        | "SADD" | "SREM" | "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE" | "SMOVE" | "SPOP"
        // ZSet
        | "ZADD" | "ZINCRBY" | "ZREM" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZPOPMIN"
        | "ZPOPMAX" | "ZUNIONSTORE" | "ZINTERSTORE" | "ZRANGESTORE" | "ZDIFFSTORE" | "ZMPOP" | "BZMPOP"
        | "BZPOPMIN" | "BZPOPMAX"
        // Generic
        | "DEL" | "UNLINK" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST"
        | "RENAME" | "RENAMENX" | "COPY" | "MOVE" | "RESTORE" | "SORT"
        // Server
        | "FLUSHDB" | "FLUSHALL"
        // Stream
        | "XADD" | "XTRIM" | "XDEL" | "XGROUP" | "XREADGROUP" | "XACK" | "XCLAIM" | "XAUTOCLAIM"
        | "XSETID"
        // Bitmap
        | "SETBIT" | "BITOP" | "BITFIELD"
        // Geo
        | "GEOADD" | "GEOSEARCHSTORE"
        // HyperLogLog
        | "PFADD" | "PFMERGE"
    )
}

/// Blocking commands can wait indefinitely before they mutate. They
/// should acquire the mutation permit only at the actual mutation,
/// not while parked.
#[cfg(not(target_arch = "wasm32"))]
fn is_blocking_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "BLPOP" | "BRPOP" | "BLMOVE" | "BLMPOP" | "BZMPOP" | "BZPOPMIN" | "BZPOPMAX" | "XREADGROUP"
    )
}
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
    // Forwards through to `dispatch_tracked`, which in turn forwards
    // through to per-command handlers that read it on host
    // (e.g. `cmd_replconf`). The body itself only forwards. The wasm
    // build has no such handlers, so the parameter is unused there;
    // cfg_attr gates the unused-variable suppression to wasm only.
    #[cfg_attr(target_arch = "wasm32", allow(unused_variables))] peer_addr: Option<
        std::net::SocketAddr,
    >,
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
    // Read in the per-command dispatch arm (e.g. `cmd_replconf`).
    // The body itself does not reference it; on wasm the consumer is
    // not built, so the allow(dead_code) below scopes the suppression
    // to the wasm build only.
    #[cfg_attr(target_arch = "wasm32", allow(unused_variables))] peer_addr: Option<
        std::net::SocketAddr,
    >,
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

    #[cfg(not(target_arch = "wasm32"))]
    if is_write && db.stats.aof_failed.load(Ordering::Acquire) {
        return Resp::Error(
            "MISCONF AOF persistence is in an error state; writes are disabled until operator recovery"
                .to_string(),
        );
    }

    // A persistence operation closes admission while it captures a coherent
    // snapshot or replaces the AOF. Normal writes use the coordinator's
    // atomic fast path and keep the permit through AOF append/fsync.
    // Blocking commands (BLPOP, BRPOP, BLMOVE, BLMPOP, BZMPOP, XREADGROUP)
    // acquire the permit only at their actual mutation, not while parked.
    //
    // The capture window is short (in-memory `snapshot_dbs()`; the file I/O
    // runs after the guard drops), so a writer that arrives during it waits
    // rather than failing. Erroring here meant a routine background BGSAVE
    // rejected concurrent writes — a benchmark writing through one auto-save
    // saw ~54% of writes fail with `MISCONF`. `MISCONF` is meant to signal
    // "persistence is broken, an operator must intervene" (the AOF-failure
    // latch above), not "the server is mid-checkpoint".
    //
    // The timeout keeps the genuinely-stuck case reportable: if a quiesce
    // outlasts it, we still return MISCONF rather than blocking forever.
    #[cfg(not(target_arch = "wasm32"))]
    let _persistence_write_permit = if is_write && !is_blocking_write_command(cmd) {
        match db
            .persistence
            .enter_mutation_waiting(PERSISTENCE_QUIESCE_WRITE_TIMEOUT)
            .await
        {
            Some(permit) => Some(permit),
            None => {
                return Resp::Error(
                    "MISCONF persistence is quiescing; writes are temporarily disabled".to_string(),
                );
            }
        }
    } else {
        None
    };

    // Key extraction is only needed when:
    //   * ACL is not fully open (must pass keys into check_permission), or
    //   * CLIENT TRACKING is enabled for someone (must feed on_write/track_read), or
    //   * keyspace notifications are enabled (must feed notify_command_keys).
    // Under redis-benchmark neither is true — skip the Vec allocation.
    let need_keys = !is_flush
        && (!db.acl.is_open() || db.tracking.enabled_count() > 0 || db.notify_flags.load().0 != 0);
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
    // Helper: decode policy once so pre- and post-write share the same path.
    let maxmem = if is_write {
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
            // Pre-write eviction — fire keyspace `evicted` events when enabled.
            let pre = db.store.evict_if_needed(&policy, limit);
            if !pre.is_empty() && db.notify_flags.load().0 != 0 {
                use crate::notify::NotifyFlags;
                let flags = db.notify_flags.load();
                if flags.contains(NotifyFlags::K_EVICTED) {
                    for k in &pre {
                        db.notify_keyspace_event(db_index, "evicted", k);
                    }
                }
            }
            Some((policy, limit))
        } else {
            None
        }
    } else {
        None
    };

    let result = match dispatch_inner(
        db, args, db_index, peer_addr, user, client_id, &key_refs, cmd,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => Resp::Error(e.to_string()),
    };

    let succeeded = !matches!(result, Resp::Error(_));

    // Post-write: a single fat write can push live_bytes past the cap; re-check
    // so maxmemory is enforced without waiting for the next command.
    // Evicted keys fire keyspace `evicted` events when the `e` flag is on.
    if succeeded {
        if let Some((policy, limit)) = maxmem {
            let evicted = db.store.evict_if_needed(&policy, limit);
            if !evicted.is_empty() && db.notify_flags.load().0 != 0 {
                use crate::notify::NotifyFlags;
                let flags = db.notify_flags.load();
                if flags.contains(NotifyFlags::K_EVICTED) {
                    for k in &evicted {
                        // We don't know which db each key came from after a
                        // multi-db sweep; the common case is the current db.
                        db.notify_keyspace_event(db_index, "evicted", k);
                    }
                }
            }
        }
    }

    if is_write && succeeded {
        db.stats.dirty_keys.fetch_add(1, Ordering::Relaxed);

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(bytes) = aof_bytes {
            let aof_sync = db.config.lock().persistence.aof_sync.clone();
            let mut writer_guard = db.aof_writer.lock();
            let write_result = match writer_guard.as_mut() {
                Some(writer) => writer.append(&bytes).and_then(|()| {
                    if aof_sync == AofSync::Always {
                        writer.fsync()
                    } else {
                        Ok(())
                    }
                }),
                None => Err(NexradeError::Generic(
                    "AOF writer is unavailable".to_string(),
                )),
            };
            if let Err(e) = write_result {
                tracing::error!("AOF write error: {}", e);
                db.fail_aof("append or sync", &e);
                return Resp::Error(format!(
                    "MISCONF AOF persistence failed after in-memory mutation: {e}; writes are now disabled"
                ));
            }
            db.stats.aof_last_write_status.store(0, Ordering::Relaxed);
        }
    }

    // CLIENT TRACKING bookkeeping: reads arm invalidation for this client,
    // writes fire invalidation pushes to every client tracking the touched
    // keys. FLUSHALL/FLUSHDB use a dedicated broadcast instead of per-key.
    // Keyspace notifications (notify-keyspace-events) share the same key list.
    if succeeded {
        if is_flush {
            db.tracking.flush_all();
        } else if !key_refs.is_empty() {
            if is_write {
                db.tracking.on_write(&key_refs, client_id);
                db.notify_command_keys(db_index, cmd, &key_refs);
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
    // Read by the per-command dispatch arm (e.g. `cmd_replconf`); the
    // body itself only does ACL / dispatch work and does not reference
    // it, but the parameter is forwarded to per-command handlers that
    // need it. The wasm build does not include those handlers, so the
    // parameter is unused there; allow(dead_code) suppresses that
    // warning on wasm only.
    #[cfg_attr(target_arch = "wasm32", allow(unused_variables))] peer_addr: Option<
        std::net::SocketAddr,
    >,
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
        "LCS" => string::cmd_lcs(db, &args, db_index).await,

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
        "BLMOVE" => list::cmd_blmove(db, &args, db_index).await,
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
        "SINTERCARD" => set::cmd_sintercard(db, &args, db_index).await,
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
        "BZPOPMIN" => zset::cmd_bzpopmin(db, &args, db_index).await,
        "BZPOPMAX" => zset::cmd_bzpopmax(db, &args, db_index).await,
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
        "COMMAND" => command_table::cmd_command(&args).await,
        "SAVE" => server::cmd_save(db).await,
        "BGSAVE" => server::cmd_bgsave(db).await,
        "BGREWRITEAOF" => server::cmd_bgrewriteaof(db).await,
        "LASTSAVE" => server::cmd_lastsave(db).await,
        "DEBUG" => server::cmd_debug(&args).await,
        "SHUTDOWN" => server::cmd_shutdown(db, &args).await,
        "SLOWLOG" => server::cmd_slowlog(db, &args).await,
        "WAIT" => server::cmd_wait(db, &args).await,
        "MEMORY" => server::cmd_memory(db, &args, db_index).await,
        "LATENCY" => server::cmd_latency(db, &args).await,
        "ACL" => server::cmd_acl(db, &args, authenticated_user).await,
        "RESET" => server::cmd_reset().await,
        "TIME" => server::cmd_time().await,
        "ROLE" => server::cmd_role(db).await,
        "CLIENT" => server::cmd_client(db, &args, client_id).await,
        "CLUSTER" => server::cmd_cluster(db, &args).await,
        "HELLO" => server::cmd_hello(&args).await,
        "PUBLISH" => server::cmd_publish(db, &args).await,
        // Standalone alias: sharded publish maps to the same global bus.
        "SPUBLISH" => server::cmd_publish(db, &args).await,
        "PUBSUB" => server::cmd_pubsub(db, &args).await,

        // 0.8.1: SENTINEL is explicitly unsupported. nexrade-cache is a
        // standalone server; it does not run a Sentinel cell, accept
        // `SENTINEL MONITOR`/`SENTINEL MASTER`/`SENTINEL FAILOVER` /
        // pub/sub-based failover, or participate in a discovery
        // quorum. Redis Sentinel clients (redis-py Sentinel, Jedis, etc.)
        // treat any non-OK SENTINEL reply as a non-Sentinel endpoint,
        // so a clean explicit error is friendlier than the generic
        // "unknown command" they would otherwise receive. Use the
        // process manager (systemd / docker / k8s) to fail over the
        // nexrade-cache process; for redis-py, set `sentinel=None` or
        // pin to the standalone client.
        "SENTINEL" => Err(NexradeError::Generic(
            "ERR SENTINEL is not supported by nexrade-cache (standalone server). \
             See docs/redis-sentinel-compat.md for failover guidance."
                .to_string(),
        )),

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
        "XSETID" => stream::cmd_xsetid(db, &args, db_index).await,

        // --- Bitmap commands ---
        "SETBIT" => bit::cmd_setbit(db, &args, db_index).await,
        "GETBIT" => bit::cmd_getbit(db, &args, db_index).await,
        "BITCOUNT" => bit::cmd_bitcount(db, &args, db_index).await,
        "BITOP" => bit::cmd_bitop(db, &args, db_index).await,
        "BITPOS" => bit::cmd_bitpos(db, &args, db_index).await,
        "BITFIELD" => bit::cmd_bitfield(db, &args, db_index).await,
        "BITFIELD_RO" => bit::cmd_bitfield_ro(db, &args, db_index).await,

        // --- Geo commands ---
        "GEOADD" => geo::cmd_geoadd(db, &args, db_index).await,
        "GEOPOS" => geo::cmd_geopos(db, &args, db_index).await,
        "GEODIST" => geo::cmd_geodist(db, &args, db_index).await,
        "GEOHASH" => geo::cmd_geohash(db, &args, db_index).await,
        "GEORADIUS" => geo::cmd_georadius(db, &args, db_index).await,
        "GEORADIUSBYMEMBER" => geo::cmd_georadiusbymember(db, &args, db_index).await,
        "GEOSEARCH" => geo::cmd_geosearch(db, &args, db_index).await,
        "GEOSEARCHSTORE" => geo::cmd_geosearchstore(db, &args, db_index).await,

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
        | "XAUTOCLAIM" | "XSETID" | "WAIT" | "BZPOPMIN" | "BZPOPMAX" | "BITFIELD_RO" => {
            &[(1, 0, Some(1))]
        }

        // XINFO STREAM|GROUPS|CONSUMERS <key> — key is at index 2.
        "XINFO" => &[(2, 0, Some(1))],

        // Multi-key commands: take every remaining key position.
        "DEL" | "UNLINK" | "EXISTS" => &[(1, 1, None)],
        "MGET" => &[(1, 1, None)],
        "MSET" => &[(1, 2, None)], // (k1, v1, k2, v2, …)
        "MSETNX" => &[(1, 2, None)],
        "RENAME" | "COPY" => &[(1, 1, Some(2))], // src, dst
        "LMOVE" | "BLMOVE" | "LCS" => &[(1, 1, Some(2))],
        // GEOSEARCHSTORE dest source …
        "GEOSEARCHSTORE" => &[(1, 1, Some(2))],

        // BITOP op destkey key [key …]
        "BITOP" => &[(2, 1, None)],

        // Sorted-set multi-key ops.
        "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" | "ZRANGESTORE" => &[(2, 1, None)],
        "ZUNION" | "ZINTER" | "ZDIFF" | "ZMPOP" | "BZMPOP" => &[(2, 1, None)],
        "ZINTERCARD" | "SINTERCARD" => &[(2, 1, None)],

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
