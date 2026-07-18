//! List command handlers.

use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::timeout;

use crate::command::{get_bytes, get_bytes_vec, get_f64, get_i64, get_str};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::DataType;

fn get_or_create_list<'a>(
    db: &'a mut crate::store::Database,
    key: &[u8],
) -> Result<&'a mut VecDeque<Bytes>> {
    // Single HashMap lookup (via `get_or_insert_with`) instead of the old
    // contains_key + insert + get_mut (up to 3 lookups) — matches
    // get_or_create_hash/get_or_create_set/get_or_create_zset.
    let entry = db.get_or_insert_with(key, || Entry::new(DataType::List(VecDeque::new())));
    match &mut entry.value {
        DataType::List(l) => Ok(l),
        _ => Err(NexradeError::WrongType),
    }
}

pub async fn cmd_lpush(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    push(db, args, db_index, true, false, "LPUSH").await
}

pub async fn cmd_rpush(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    push(db, args, db_index, false, false, "RPUSH").await
}

pub async fn cmd_lpushx(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    push(db, args, db_index, true, true, "LPUSHX").await
}

pub async fn cmd_rpushx(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    push(db, args, db_index, false, true, "RPUSHX").await
}

async fn push(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    left: bool,
    only_if_exists: bool,
    cmd: &str,
) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    // Borrow key as Bytes (refcount) — write_for only needs &[u8].
    let key = get_bytes(args, 1, cmd)?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    if only_if_exists && !store_db.contains_key(&key) {
        return Ok(Resp::int(0));
    }

    let list = get_or_create_list(&mut store_db, &key)?;
    for i in 2..args.len() {
        // Compact copy into a dedicated allocation. Refcount-cloning the
        // RESP parse buffer would pin the whole pipeline batch in memory
        // for the lifetime of the list element — catastrophic under
        // redis-benchmark LPUSH.
        let val = Bytes::copy_from_slice(&get_bytes(args, i, cmd)?);
        if left {
            list.push_front(val);
        } else {
            list.push_back(val);
        }
    }
    let len = list.len() as i64;
    drop(store_db);

    // Notify blocking pop waiters (no-op when nobody is parked).
    db.notify_list_waiters();

    Ok(Resp::int(len))
}

pub async fn cmd_lpop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    pop(db, args, db_index, true, "LPOP").await
}

pub async fn cmd_rpop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    pop(db, args, db_index, false, "RPOP").await
}

async fn pop(db: &Db, args: &[Resp], db_index: usize, left: bool, cmd: &str) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let key = get_bytes_vec(args, 1, cmd)?;
    let count = if args.len() >= 3 {
        let n = get_i64(args, 2, cmd)?;
        if n < 0 {
            return Err(NexradeError::Generic(
                "ERR value is not an integer or out of range".to_string(),
            ));
        }
        Some(n as usize)
    } else {
        None
    };

    let mut store_db = db.store.db(db_index).write_for(&key);

    match store_db.get_mut(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                if let Some(n) = count {
                    let mut results = Vec::with_capacity(n.min(list.len()));
                    for _ in 0..n {
                        let val = if left {
                            list.pop_front()
                        } else {
                            list.pop_back()
                        };
                        match val {
                            Some(v) => results.push(Resp::bulk(v)),
                            None => break,
                        }
                    }
                    Ok(Resp::array(results))
                } else {
                    let val = if left {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    };
                    Ok(val.map(Resp::bulk).unwrap_or(Resp::null()))
                }
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_llen(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("llen".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LLEN")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::List(l) => Ok(Resp::int(l.len() as i64)),
            _ => Err(NexradeError::WrongType),
        },
    }
}

/// Pre-serialized RESP empty array — shared, zero-alloc empty LRANGE.
static EMPTY_ARRAY: &[u8] = b"*0\r\n";

// Thread-local serialize buffer for LRANGE. `split().freeze()` returns the
// payload while leaving capacity on the local buffer for the next call —
// under redis-benchmark LRANGE_N this eliminates a multi-KB allocate+free
// per request (the main non-pipe cost vs Redis).
thread_local! {
    static LRANGE_BUF: std::cell::RefCell<bytes::BytesMut> =
        std::cell::RefCell::new(bytes::BytesMut::with_capacity(8192));
}

pub async fn cmd_lrange(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("lrange".to_string()));
    }
    // Key as Bytes — avoid Vec allocation on the hot read path.
    let key = get_bytes(args, 1, "LRANGE")?;
    let start = get_i64(args, 2, "LRANGE")?;
    let stop = get_i64(args, 3, "LRANGE")?;

    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::Raw(Bytes::from_static(EMPTY_ARRAY))),
        Some(e) => match &e.value {
            DataType::List(list) => {
                let len = list.len() as isize;
                let start = normalize_idx(start as isize, len);
                let stop = normalize_idx(stop as isize, len);

                if start >= list.len() || start > stop {
                    return Ok(Resp::Raw(Bytes::from_static(EMPTY_ARRAY)));
                }
                let stop = stop.min(list.len() - 1);

                // Size without a second walk: framing + a conservative
                // per-element payload budget. redis-benchmark LRANGE uses
                // 1–3 byte elements; real apps with large elements grow
                // amortised via BytesMut. Skipping the pre-sum walk is the
                // main remaining CPU win on the non-pipe path.
                let count = stop - start + 1;
                // "*N\r\n" (~8) + per-elem "$n\r\n" + "\r\n" (~8) + ~8 data
                let need = 8 + count * 24;

                // Hold the read lock while serializing so concurrent
                // LPUSH/LPOP can't race the view we sized against.
                let raw = LRANGE_BUF.with(|cell| {
                    let mut buf = cell.borrow_mut();
                    buf.clear();
                    let cap = buf.capacity();
                    if cap < need {
                        buf.reserve(need - cap);
                    }
                    Resp::write_bulk_array_into(&mut buf, list.range(start..=stop));
                    // split() returns the written front and keeps capacity
                    // on `buf` for the next LRANGE on this worker thread.
                    buf.split().freeze()
                });
                Ok(Resp::Raw(raw))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_lindex(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("lindex".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LINDEX")?;
    let idx = get_i64(args, 2, "LINDEX")?;

    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &e.value {
            DataType::List(list) => {
                let len = list.len() as isize;
                let i = normalize_idx(idx as isize, len);
                Ok(list
                    .get(i)
                    .map(|v| Resp::bulk(v.clone()))
                    .unwrap_or(Resp::null()))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_lset(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("lset".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LSET")?;
    let idx = get_i64(args, 2, "LSET")?;
    let val = get_bytes(args, 3, "LSET")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        None => Err(NexradeError::Generic("no such key".to_string())),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                let len = list.len() as isize;
                let i = normalize_idx(idx as isize, len);
                if i >= list.len() {
                    return Err(NexradeError::IndexOutOfRange);
                }
                list[i] = val;
                Ok(Resp::ok())
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_linsert(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 5 {
        return Err(NexradeError::WrongArity("linsert".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LINSERT")?;
    let where_opt = get_str(args, 2, "LINSERT")?.to_uppercase();
    let pivot = get_bytes(args, 3, "LINSERT")?;
    let element = get_bytes(args, 4, "LINSERT")?;

    let before = match where_opt.as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return Err(NexradeError::SyntaxError),
    };

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        None => Ok(Resp::int(-1)),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                let pos = list.iter().position(|v| v == &pivot);
                match pos {
                    None => Ok(Resp::int(-1)),
                    Some(i) => {
                        let insert_at = if before { i } else { i + 1 };
                        list.insert(insert_at, element);
                        Ok(Resp::int(list.len() as i64))
                    }
                }
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_lrem(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("lrem".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LREM")?;
    let count = get_i64(args, 2, "LREM")?;
    let element = get_bytes(args, 3, "LREM")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                let mut removed = 0i64;
                let limit = count.unsigned_abs() as usize;
                let reverse = count < 0;

                if reverse {
                    let mut i = list.len();
                    while i > 0 {
                        i -= 1;
                        if list[i] == element && (limit == 0 || removed < limit as i64) {
                            list.remove(i);
                            removed += 1;
                        }
                    }
                } else {
                    let mut i = 0;
                    while i < list.len() {
                        if list[i] == element && (limit == 0 || removed < limit as i64) {
                            list.remove(i);
                            removed += 1;
                        } else {
                            i += 1;
                        }
                    }
                }
                Ok(Resp::int(removed))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_ltrim(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("ltrim".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LTRIM")?;
    let start = get_i64(args, 2, "LTRIM")?;
    let stop = get_i64(args, 3, "LTRIM")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        None => Ok(Resp::ok()),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                let len = list.len() as isize;
                let start = normalize_idx(start as isize, len);
                let stop = normalize_idx(stop as isize, len);

                if start >= list.len() || start > stop {
                    list.clear();
                } else {
                    let stop = stop.min(list.len() - 1);
                    *list = list.drain(start..=stop).collect();
                }
                Ok(Resp::ok())
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_lmove(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 5 {
        return Err(NexradeError::WrongArity("lmove".to_string()));
    }
    let src = get_bytes_vec(args, 1, "LMOVE")?;
    let dst = get_bytes_vec(args, 2, "LMOVE")?;
    let wherefrom = get_str(args, 3, "LMOVE")?.to_uppercase();
    let whereto = get_str(args, 4, "LMOVE")?.to_uppercase();

    let from_left = wherefrom == "LEFT";
    let to_left = whereto == "LEFT";

    match db
        .store
        .db(db_index)
        .lmove_atomic(&src, dst, from_left, to_left)
    {
        Some(val) => {
            db.notify_list_waiters();
            Ok(Resp::bulk(val))
        }
        None => Ok(Resp::null()),
    }
}

pub async fn cmd_rpoplpush(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // RPOPLPUSH src dst → LMOVE src dst RIGHT LEFT
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("rpoplpush".to_string()));
    }
    let src = get_bytes_vec(args, 1, "RPOPLPUSH")?;
    let dst = get_bytes_vec(args, 2, "RPOPLPUSH")?;
    match db.store.db(db_index).lmove_atomic(&src, dst, false, true) {
        Some(val) => {
            db.notify_list_waiters();
            Ok(Resp::bulk(val))
        }
        None => Ok(Resp::null()),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn cmd_blpop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    blocking_pop(db, args, db_index, true, "BLPOP").await
}

#[cfg(target_arch = "wasm32")]
pub async fn cmd_blpop(_db: &Db, _args: &[Resp], _db_index: usize) -> Result<Resp> {
    Err(NexradeError::Generic(
        "ERR blocking commands not supported in WASM mode".to_string(),
    ))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn cmd_brpop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    blocking_pop(db, args, db_index, false, "BRPOP").await
}

#[cfg(target_arch = "wasm32")]
pub async fn cmd_brpop(_db: &Db, _args: &[Resp], _db_index: usize) -> Result<Resp> {
    Err(NexradeError::Generic(
        "ERR blocking commands not supported in WASM mode".to_string(),
    ))
}

#[cfg(not(target_arch = "wasm32"))]
async fn blocking_pop(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    left: bool,
    cmd: &str,
) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }

    let timeout_secs = get_f64(args, args.len() - 1, cmd)?;
    let keys: Vec<Vec<u8>> = (1..args.len() - 1)
        .map(|i| get_bytes_vec(args, i, cmd))
        .collect::<Result<_>>()?;

    let dur = if timeout_secs == 0.0 {
        Duration::from_secs(u64::MAX)
    } else {
        Duration::from_secs_f64(timeout_secs)
    };

    let result = timeout(dur, async {
        let _parked = db.park_list_waiter();
        loop {
            {
                let sdb = db.store.db(db_index);
                for key in &keys {
                    let mut shard = sdb.write_for(key);
                    if let Some(e) = shard.get_mut(key) {
                        if let DataType::List(list) = &mut e.value {
                            if !list.is_empty() {
                                let val = if left {
                                    list.pop_front()
                                } else {
                                    list.pop_back()
                                };
                                if let Some(v) = val {
                                    return Resp::array(vec![
                                        Resp::bulk(Bytes::copy_from_slice(key)),
                                        Resp::bulk(v),
                                    ]);
                                }
                            }
                        }
                    }
                }
            }
            db.list_chan.notified().await;
        }
    })
    .await;

    match result {
        Ok(resp) => Ok(resp),
        Err(_) => Ok(Resp::null_array()),
    }
}

/// LPOS key element [RANK rank] [COUNT num] [MAXLEN maxlen]
pub async fn cmd_lpos(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("lpos".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LPOS")?;
    let element = get_bytes(args, 2, "LPOS")?;

    // Parse optional arguments
    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0;
    let mut i = 3;
    while i < args.len() {
        match args[i].as_str().map(|s| s.to_uppercase()).as_deref() {
            Some("RANK") => {
                let r = get_i64(args, i + 1, "LPOS")?;
                if r == 0 {
                    return Err(NexradeError::Generic("RANK can't be zero: use 1 to start from the first match, 2 from the second, ...".to_string()));
                }
                rank = r;
                i += 2;
            }
            Some("COUNT") => {
                let n = get_i64(args, i + 1, "LPOS")?;
                if n < 0 {
                    return Err(NexradeError::Generic(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
                count = Some(n as usize);
                i += 2;
            }
            Some("MAXLEN") => {
                let n = get_i64(args, i + 1, "LPOS")?;
                if n < 0 {
                    return Err(NexradeError::Generic(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
                maxlen = n as usize;
                i += 2;
            }
            _ => return Err(NexradeError::SyntaxError),
        }
    }

    let store_db = db.store.db(db_index).read_for(&key);
    let list = match store_db.get_ro(&key) {
        None => {
            return Ok(if count.is_some() {
                Resp::array(vec![])
            } else {
                Resp::null()
            })
        }
        Some(e) => match &e.value {
            DataType::List(l) => l,
            _ => return Err(NexradeError::WrongType),
        },
    };

    let limit = if maxlen == 0 {
        list.len()
    } else {
        maxlen.min(list.len())
    };
    let want = count.unwrap_or(1);
    let mut results: Vec<Resp> = Vec::new();
    let mut matches_seen: i64 = 0;

    if rank >= 0 {
        for (idx, v) in list.iter().enumerate().take(limit) {
            if v == &element {
                matches_seen += 1;
                if matches_seen >= rank {
                    results.push(Resp::int(idx as i64));
                    if results.len() >= want && count.is_some() {
                        break;
                    }
                }
            }
        }
    } else {
        // Negative rank — search from the tail
        let abs_rank = rank.unsigned_abs() as i64;
        for (idx, v) in list.iter().enumerate().rev().take(limit) {
            if v == &element {
                matches_seen += 1;
                if matches_seen >= abs_rank {
                    results.push(Resp::int(idx as i64));
                    if results.len() >= want && count.is_some() {
                        break;
                    }
                }
            }
        }
        results.reverse();
    }

    if count.is_some() {
        Ok(Resp::array(results))
    } else {
        Ok(results.into_iter().next().unwrap_or(Resp::null()))
    }
}

// ── LMPOP / BLMPOP ───────────────────────────────────────────────────────────

/// `LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]`
///
/// Pops `count` elements from the first non-empty list among the given keys.
/// Returns `[key, [popped...]]` or nil array if all keys are empty/missing.
pub async fn cmd_lmpop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    lmpop_once(db, args, db_index, None).await
}

/// `BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]`
///
/// Blocking variant — waits up to `timeout` seconds for any of the keys to
/// receive a push.
pub async fn cmd_blmpop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 5 {
        return Err(NexradeError::WrongArity("blmpop".to_string()));
    }
    let timeout_secs = get_f64(args, 1, "BLMPOP")?;
    let numkeys = parse_numkeys(args, 2, "BLMPOP")?;
    let (keys, rest_start) = parse_keys(args, 3, numkeys, "BLMPOP")?;
    let (left, count) = parse_lmpop_tail(&args[rest_start..], "BLMPOP")?;

    // Fast path: try once before blocking.
    if let Some(resp) = lmpop_attempt(db, db_index, &keys, left, count)? {
        return Ok(resp);
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let dur = if timeout_secs == 0.0 {
            std::time::Duration::from_secs(u64::MAX)
        } else {
            std::time::Duration::from_secs_f64(timeout_secs)
        };
        match tokio::time::timeout(dur, async {
            let _parked = db.park_list_waiter();
            loop {
                db.list_chan.notified().await;
                if let Some(resp) = lmpop_attempt(db, db_index, &keys, left, count)? {
                    return Ok::<Resp, NexradeError>(resp);
                }
            }
        })
        .await
        {
            Ok(resp) => Ok(resp?),
            Err(_) => Ok(Resp::null_array()),
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (timeout_secs, left, count);
        Ok(Resp::null_array())
    }
}

async fn lmpop_once(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    _block_ms: Option<u64>,
) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("lmpop".to_string()));
    }
    let numkeys = parse_numkeys(args, 1, "LMPOP")?;
    let (keys, rest_start) = parse_keys(args, 2, numkeys, "LMPOP")?;
    let (left, count) = parse_lmpop_tail(&args[rest_start..], "LMPOP")?;
    Ok(lmpop_attempt(db, db_index, &keys, left, count)?.unwrap_or_else(Resp::null_array))
}

fn lmpop_attempt(
    db: &Db,
    db_index: usize,
    keys: &[Vec<u8>],
    left: bool,
    count: usize,
) -> Result<Option<Resp>> {
    for key in keys {
        let mut store_db = db.store.db(db_index).write_for(key);
        if let Some(entry) = store_db.get_mut(key) {
            if let DataType::List(list) = &mut entry.value {
                if list.is_empty() {
                    continue;
                }
                let mut popped: Vec<Resp> = Vec::with_capacity(count);
                for _ in 0..count {
                    let v = if left {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    };
                    match v {
                        Some(b) => popped.push(Resp::bulk(b)),
                        None => break,
                    }
                }
                return Ok(Some(Resp::array(vec![
                    Resp::bulk(Bytes::copy_from_slice(key)),
                    Resp::array(popped),
                ])));
            }
        }
    }
    Ok(None)
}

fn parse_numkeys(args: &[Resp], idx: usize, cmd: &str) -> Result<usize> {
    let n = get_i64(args, idx, cmd)?;
    if n <= 0 {
        return Err(NexradeError::Generic(
            "numkeys should be greater than 0".to_string(),
        ));
    }
    Ok(n as usize)
}

fn parse_keys(args: &[Resp], idx: usize, n: usize, cmd: &str) -> Result<(Vec<Vec<u8>>, usize)> {
    if args.len() < idx + n {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let keys: Vec<Vec<u8>> = (idx..idx + n)
        .map(|i| get_bytes_vec(args, i, cmd))
        .collect::<Result<_>>()?;
    Ok((keys, idx + n))
}

fn parse_lmpop_tail(args: &[Resp], cmd: &str) -> Result<(bool, usize)> {
    if args.is_empty() {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let dir = get_str(args, 0, cmd)?.to_ascii_uppercase();
    let left = match dir.as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return Err(NexradeError::Generic("syntax error".to_string())),
    };
    let mut count = 1usize;
    let mut i = 1;
    if i < args.len() && get_str(args, i, cmd)?.eq_ignore_ascii_case("COUNT") {
        i += 1;
        if i >= args.len() {
            return Err(NexradeError::WrongArity(cmd.to_string()));
        }
        let n = get_i64(args, i, cmd)?;
        if n < 0 {
            return Err(NexradeError::Generic("value is out of range".to_string()));
        }
        count = n as usize;
        i += 1;
    }
    if i != args.len() {
        return Err(NexradeError::Generic("syntax error".to_string()));
    }
    Ok((left, count))
}

fn normalize_idx(idx: isize, len: isize) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx as usize
    }
}
