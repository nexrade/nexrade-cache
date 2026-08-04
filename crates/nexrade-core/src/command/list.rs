//! List command handlers.

#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

use bytes::Bytes;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::timeout;

use crate::command::{get_bytes, get_bytes_vec, get_f64, get_i64, get_str};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::list_data::ListData;
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::DataType;

fn get_or_create_list<'a>(
    db: &'a mut crate::store::Database,
    key: &[u8],
) -> Result<&'a mut ListData> {
    let entry = db.get_or_insert_with(key, || Entry::new(DataType::List(ListData::new())));
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
    // Borrow key — no Bytes clone. write_for / get_or_insert only need &[u8].
    let key = match args.get(1).and_then(|a| a.as_bytes()) {
        Some(b) => b,
        None => return Err(NexradeError::WrongArity(cmd.to_lowercase())),
    };
    let mut store_db = db.store.db(db_index).write_for(key.as_ref());

    if only_if_exists && !store_db.contains_key(key.as_ref()) {
        return Ok(Resp::int(0));
    }

    let (len, delta) = {
        let list = get_or_create_list(&mut store_db, key.as_ref())?;
        // Known delta: sum of pushed element lengths (payload_bytes is
        // content-only; Compact→Linked promote does not change it).
        let mut delta: isize = 0;
        for i in 2..args.len() {
            // Compact copy into a dedicated allocation. Refcount-cloning the
            // RESP parse buffer would pin the whole pipeline batch in memory
            // for the lifetime of the list element — catastrophic under
            // redis-benchmark LPUSH.
            let val = Bytes::copy_from_slice(
                args.get(i)
                    .and_then(|a| a.as_bytes())
                    .ok_or_else(|| NexradeError::WrongArity(cmd.to_lowercase()))?
                    .as_ref(),
            );
            delta += val.len() as isize;
            if left {
                list.push_front(val);
            } else {
                list.push_back(val);
            }
        }
        (list.len() as i64, delta)
    };
    store_db.adjust_live_bytes(delta);
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

    let mut payload_delta: isize = 0;
    // Emptiness is computed while the list is already borrowed — the GC
    // probe below is then gated so the hot non-empty pop pays nothing.
    let mut emptied = false;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                // Known delta: −sum of popped element lengths (no before/after
                // payload_bytes loads on the hot path).
                let mut delta: isize = 0;
                let resp = if let Some(n) = count {
                    let mut results = Vec::with_capacity(n.min(list.len()));
                    for _ in 0..n {
                        let val = if left {
                            list.pop_front()
                        } else {
                            list.pop_back()
                        };
                        match val {
                            Some(v) => {
                                delta -= v.len() as isize;
                                results.push(Resp::bulk(v));
                            }
                            None => break,
                        }
                    }
                    Ok(Resp::array(results))
                } else {
                    match if left {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    } {
                        Some(v) => {
                            delta -= v.len() as isize;
                            Ok(Resp::bulk(v))
                        }
                        None => Ok(Resp::null()),
                    }
                };
                payload_delta = delta;
                emptied = list.is_empty();
                resp
            }
            _ => Err(NexradeError::WrongType),
        },
    };
    store_db.adjust_live_bytes(payload_delta);
    if emptied {
        store_db.remove_empty_key(&key);
    }
    result
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
//
// 32 KiB covers typical LRANGE_100 (~few KB) and LRANGE_300/600 headroom
// without realloc on the multi-client non-pipe path (0.5.5).
thread_local! {
    static LRANGE_BUF: std::cell::RefCell<bytes::BytesMut> =
        std::cell::RefCell::new(bytes::BytesMut::with_capacity(32 * 1024));
}

pub async fn cmd_lrange(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("lrange".to_string()));
    }
    // Key as Bytes — avoid Vec allocation on the hot read path.
    let key = get_bytes(args, 1, "LRANGE")?;
    let start = get_i64(args, 2, "LRANGE")?;
    let stop = get_i64(args, 3, "LRANGE")?;

    // Snapshot under the read lock, then frame *outside* so concurrent
    // LRANGE clients don't pin the shard for the whole RESP walk.
    //
    // Compact: Arc-clone the buffer (no payload memcpy) + O(1) size
    // estimate. Linked: cheap Bytes clones (refcount) of the range + exact
    // RESP need so framing never grows mid-walk (0.5.5).
    enum Snap {
        Empty,
        WrongType,
        Compact(crate::list_data::CompactRangeSnap),
        Linked { elems: Vec<Bytes>, need: usize },
    }

    let snap = {
        let store_db = db.store.db(db_index).read_for(&key);
        match store_db.get_ro(&key) {
            None => Snap::Empty,
            Some(e) => match &e.value {
                DataType::List(list) => {
                    let len = list.len() as isize;
                    let start = normalize_idx(start as isize, len);
                    let stop = normalize_idx(stop as isize, len);
                    if start >= list.len() || start > stop {
                        Snap::Empty
                    } else {
                        let stop = stop.min(list.len() - 1);
                        match list {
                            crate::list_data::ListData::Compact(c) => {
                                Snap::Compact(c.range_snap(start, stop))
                            }
                            crate::list_data::ListData::Linked(d) => {
                                let count = stop - start + 1;
                                let mut elems = Vec::with_capacity(count);
                                let mut payload = 0usize;
                                for b in d.items.range(start..=stop) {
                                    payload += b.len();
                                    elems.push(b.clone());
                                }
                                // Match CompactRangeSnap budget: array hdr +
                                // per-elem framing + payload (see range_snap).
                                let need = 16 + count * 12 + payload;
                                Snap::Linked { elems, need }
                            }
                        }
                    }
                }
                _ => Snap::WrongType,
            },
        }
        // store_db drops here — shard lock released before framing.
    };

    match snap {
        Snap::Empty => Ok(Resp::Raw(Bytes::from_static(EMPTY_ARRAY))),
        Snap::WrongType => Err(NexradeError::WrongType),
        Snap::Compact(s) => {
            let raw = LRANGE_BUF.with(|cell| {
                let mut buf = cell.borrow_mut();
                buf.clear();
                let cap = buf.capacity();
                if cap < s.need {
                    buf.reserve(s.need - cap);
                }
                let region = &s.buf[s.begin..s.end];
                Resp::write_bulk_array_into(
                    &mut buf,
                    CompactRegionIter {
                        buf: region,
                        off: 0,
                        remaining: s.count,
                    },
                );
                buf.split().freeze()
            });
            Ok(Resp::Raw(raw))
        }
        Snap::Linked { elems, need } => {
            let raw = LRANGE_BUF.with(|cell| {
                let mut buf = cell.borrow_mut();
                buf.clear();
                let cap = buf.capacity();
                if cap < need {
                    buf.reserve(need - cap);
                }
                Resp::write_bulk_array_into(&mut buf, elems.iter());
                buf.split().freeze()
            });
            Ok(Resp::Raw(raw))
        }
    }
}

/// Walk a snapshotted Compact length-prefixed region (owned `Vec<u8>`).
struct CompactRegionIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl<'a> Iterator for CompactRegionIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let n = u32::from_le_bytes(self.buf[self.off..self.off + 4].try_into().ok()?) as usize;
        let slice = &self.buf[self.off + 4..self.off + 4 + n];
        self.off += 4 + n;
        self.remaining -= 1;
        Some(slice)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for CompactRegionIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
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
                Ok(list.get(i).map(Resp::bulk).unwrap_or(Resp::null()))
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
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Err(NexradeError::Generic("no such key".to_string())),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                let len = list.len() as isize;
                let i = normalize_idx(idx as isize, len);
                let (ok, d) = list.set(i, val);
                if !ok {
                    return Err(NexradeError::IndexOutOfRange);
                }
                payload_delta = d;
                Ok(Resp::ok())
            }
            _ => Err(NexradeError::WrongType),
        },
    };
    store_db.adjust_live_bytes(payload_delta);
    result
}

pub async fn cmd_linsert(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 5 {
        return Err(NexradeError::WrongArity("linsert".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LINSERT")?;
    let where_opt = get_str(args, 2, "LINSERT")?.to_uppercase();
    let pivot = get_bytes(args, 3, "LINSERT")?;
    let element = get_bytes(args, 4, "LINSERT")?;

    let insert_before = match where_opt.as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return Err(NexradeError::SyntaxError),
    };

    let mut store_db = db.store.db(db_index).write_for(&key);
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::int(-1)),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                let elen = element.len() as isize;
                let n = if insert_before {
                    list.insert_before(&pivot, element)
                } else {
                    list.insert_after(&pivot, element)
                };
                // Successful insert always adds exactly the element payload.
                payload_delta = if n.is_some() { elen } else { 0 };
                Ok(n.map(|len| Resp::int(len as i64)).unwrap_or(Resp::int(-1)))
            }
            _ => Err(NexradeError::WrongType),
        },
    };
    store_db.adjust_live_bytes(payload_delta);
    result
}

pub async fn cmd_lrem(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("lrem".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LREM")?;
    let count = get_i64(args, 2, "LREM")?;
    let element = get_bytes(args, 3, "LREM")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let mut emptied = false;
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                let (removed, d) = list.rem(count, &element);
                payload_delta = d;
                emptied = list.is_empty();
                Ok(Resp::int(removed as i64))
            }
            _ => Err(NexradeError::WrongType),
        },
    };
    store_db.adjust_live_bytes(payload_delta);
    if emptied {
        store_db.remove_empty_key(&key);
    }
    result
}

pub async fn cmd_ltrim(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("ltrim".to_string()));
    }
    let key = get_bytes_vec(args, 1, "LTRIM")?;
    let start = get_i64(args, 2, "LTRIM")?;
    let stop = get_i64(args, 3, "LTRIM")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let mut emptied = false;
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::ok()),
        Some(e) => match &mut e.value {
            DataType::List(list) => {
                let len = list.len() as isize;
                let start = normalize_idx(start as isize, len);
                let stop = normalize_idx(stop as isize, len);

                payload_delta = if start >= list.len() || start > stop {
                    list.trim(1, 0) // empty
                } else {
                    let stop = stop.min(list.len() - 1);
                    list.trim(start, stop)
                };
                emptied = list.is_empty();
                Ok(Resp::ok())
            }
            _ => Err(NexradeError::WrongType),
        },
    };
    store_db.adjust_live_bytes(payload_delta);
    if emptied {
        store_db.remove_empty_key(&key);
    }
    result
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
        .lmove_atomic(&src, dst, from_left, to_left)?
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
    match db.store.db(db_index).lmove_atomic(&src, dst, false, true)? {
        Some(val) => {
            db.notify_list_waiters();
            Ok(Resp::bulk(val))
        }
        None => Ok(Resp::null()),
    }
}

/// `BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout`
///
/// Blocking LMOVE. Parks on `list_chan` until an element appears in
/// `source` (or timeout). Returns the moved element, or a null array on
/// timeout (Redis-compatible).
#[cfg(not(target_arch = "wasm32"))]
pub async fn cmd_blmove(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 6 {
        return Err(NexradeError::WrongArity("blmove".to_string()));
    }
    let src = get_bytes_vec(args, 1, "BLMOVE")?;
    let dst = get_bytes_vec(args, 2, "BLMOVE")?;
    let wherefrom = get_str(args, 3, "BLMOVE")?.to_uppercase();
    let whereto = get_str(args, 4, "BLMOVE")?.to_uppercase();
    let timeout_secs = get_f64(args, 5, "BLMOVE")?;

    if wherefrom != "LEFT" && wherefrom != "RIGHT" {
        return Err(NexradeError::SyntaxError);
    }
    if whereto != "LEFT" && whereto != "RIGHT" {
        return Err(NexradeError::SyntaxError);
    }
    let from_left = wherefrom == "LEFT";
    let to_left = whereto == "LEFT";

    // Fast path: element already present.
    if let Some(val) = db
        .store
        .db(db_index)
        .lmove_atomic(&src, dst.clone(), from_left, to_left)?
    {
        db.notify_list_waiters();
        return Ok(Resp::bulk(val));
    }

    let dur = if timeout_secs == 0.0 {
        Duration::from_secs(u64::MAX)
    } else {
        Duration::from_secs_f64(timeout_secs)
    };

    let result = timeout(dur, async {
        let _parked = db.park_list_waiter();
        loop {
            // Register for the next wake *before* re-checking so a producer
            // that notifies between the empty check and park cannot be lost.
            let notified = db.list_chan.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            // Acquire mutation permit only for the actual mutation
            #[cfg(not(target_arch = "wasm32"))]
            let _permit = match db.persistence.enter_mutation() {
                Some(p) => p,
                None => {
                    return Err(NexradeError::Generic(
                        "MISCONF persistence is quiescing; writes are temporarily disabled"
                            .to_string(),
                    ))
                }
            };

            match db
                .store
                .db(db_index)
                .lmove_atomic(&src, dst.clone(), from_left, to_left)
            {
                Ok(Some(val)) => {
                    db.notify_list_waiters();
                    return Ok(Resp::bulk(val));
                }
                Ok(None) => {}
                Err(e) => return Err(e),
            }
            notified.await;
        }
    })
    .await;

    match result {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(e)) => Err(e),
        Err(_) => Ok(Resp::null_array()),
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn cmd_blmove(_db: &Db, _args: &[Resp], _db_index: usize) -> Result<Resp> {
    Err(NexradeError::Generic(
        "ERR blocking commands not supported in WASM mode".to_string(),
    ))
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
            // Register for the next wake *before* re-checking emptiness so a
            // producer that notifies between the empty check and park cannot
            // be lost (tokio Notify is not a condition variable; missed
            // notify_waiters() does not queue).
            let notified = db.list_chan.notified();
            tokio::pin!(notified);
            // Ensure the Notified future is registered with the Notify.
            notified.as_mut().enable();

            {
                let sdb = db.store.db(db_index);
                for key in &keys {
                    // Acquire mutation permit only for the actual mutation
                    #[cfg(not(target_arch = "wasm32"))]
                    let _permit =
                        match db.persistence.enter_mutation() {
                            Some(p) => p,
                            None => return Resp::Error(
                                "MISCONF persistence is quiescing; writes are temporarily disabled"
                                    .to_string(),
                            ),
                        };
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
                                    let emptied = list.is_empty();
                                    shard.adjust_live_bytes(-(v.len() as isize));
                                    if emptied {
                                        shard.remove_empty_key(key);
                                    }
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
            notified.await;
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

    let elems = list.to_vec_bytes();
    let limit = if maxlen == 0 {
        elems.len()
    } else {
        maxlen.min(elems.len())
    };
    let want = count.unwrap_or(1);
    let mut results: Vec<Resp> = Vec::new();
    let mut matches_seen: i64 = 0;

    if rank >= 0 {
        for (idx, v) in elems.iter().enumerate().take(limit) {
            if v.as_ref() == element.as_ref() {
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
        for (idx, v) in elems.iter().enumerate().rev().take(limit) {
            if v.as_ref() == element.as_ref() {
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
                // Register before re-check (see blocking_pop).
                let notified = db.list_chan.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                #[cfg(not(target_arch = "wasm32"))]
                let _permit = match db.persistence.enter_mutation() {
                    Some(p) => p,
                    None => {
                        return Err(NexradeError::Generic(
                            "MISCONF persistence is quiescing; writes are temporarily disabled"
                                .to_string(),
                        ))
                    }
                };
                if let Some(resp) = lmpop_attempt(db, db_index, &keys, left, count)? {
                    return Ok::<Resp, NexradeError>(resp);
                }
                notified.await;
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
                let mut delta: isize = 0;
                let mut popped: Vec<Resp> = Vec::with_capacity(count);
                for _ in 0..count {
                    let v = if left {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    };
                    match v {
                        Some(b) => {
                            delta -= b.len() as isize;
                            popped.push(Resp::bulk(b));
                        }
                        None => break,
                    }
                }
                let emptied = list.is_empty();
                store_db.adjust_live_bytes(delta);
                if emptied {
                    store_db.remove_empty_key(key);
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
