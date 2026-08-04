//! Hash command handlers.

use bytes::Bytes;

use super::string::format_float;

use crate::command::{
    decode_scan_cursor, get_bytes_vec, get_f64, get_i64, get_str, scan_cursor_token,
    scan_start_offset_by,
};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::hash_data::{CompactHashBulkIter, HashData, HashGetAllSnap};
use crate::resp::Resp;
use crate::store::{glob_match, Entry};
use crate::types::DataType;

fn get_or_create_hash<'a>(
    db: &'a mut crate::store::Database,
    key: &[u8],
) -> Result<&'a mut HashData> {
    let entry = db.get_or_insert_with(key, || Entry::new(DataType::Hash(HashData::new())));
    match &mut entry.value {
        DataType::Hash(h) => Ok(h),
        _ => Err(NexradeError::WrongType),
    }
}

pub async fn cmd_hset(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 || !args.len().is_multiple_of(2) {
        return Err(NexradeError::WrongArity("hset".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HSET")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    let (added, delta) = {
        let hash = get_or_create_hash(&mut store_db, &key)?;
        let mut added = 0i64;
        let mut delta: isize = 0;
        let mut i = 2;
        while i + 1 < args.len() {
            let field = get_bytes_vec(args, i, "HSET")?;
            let val = get_bytes_vec(args, i + 1, "HSET")?;
            let (is_new, d) = hash.insert(field, val);
            delta += d;
            if is_new {
                added += 1;
            }
            i += 2;
        }
        (added, delta)
    };
    store_db.adjust_live_bytes(delta);
    Ok(Resp::int(added))
}

pub async fn cmd_hmset(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // HMSET is deprecated but still supported (same as HSET)
    if args.len() < 4 || !args.len().is_multiple_of(2) {
        return Err(NexradeError::WrongArity("hmset".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HMSET")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    let delta = {
        let hash = get_or_create_hash(&mut store_db, &key)?;
        let mut delta: isize = 0;
        let mut i = 2;
        while i + 1 < args.len() {
            let field = get_bytes_vec(args, i, "HMSET")?;
            let val = get_bytes_vec(args, i + 1, "HMSET")?;
            delta += hash.insert(field, val).1;
            i += 2;
        }
        delta
    };
    store_db.adjust_live_bytes(delta);
    Ok(Resp::ok())
}

pub async fn cmd_hget(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("hget".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HGET")?;
    let field = get_bytes_vec(args, 2, "HGET")?;

    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &e.value {
            DataType::Hash(h) => Ok(h
                .get(&field)
                .map(|v| Resp::bulk(Bytes::from(v)))
                .unwrap_or(Resp::null())),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_hmget(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("hmget".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HMGET")?;
    let store_db = db.store.db(db_index).read_for(&key);

    let results: Vec<Resp> = (2..args.len())
        .map(|i| {
            let field = get_bytes_vec(args, i, "HMGET").ok()?;
            match store_db.get_ro(&key) {
                Some(e) => match &e.value {
                    DataType::Hash(h) => h.get(&field).map(|v| Resp::bulk(Bytes::from(v))),
                    _ => None,
                },
                None => None,
            }
        })
        .map(|v| v.unwrap_or(Resp::null()))
        .collect();

    Ok(Resp::array(results))
}

pub async fn cmd_hdel(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("hdel".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HDEL")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    let mut emptied = false;
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::Hash(h) => {
                let mut removed = 0i64;
                let mut delta: isize = 0;
                for i in 2..args.len() {
                    if let Ok(f) = get_bytes_vec(args, i, "HDEL") {
                        if let Some((_v, d)) = h.remove(&f) {
                            removed += 1;
                            delta += d;
                        }
                    }
                }
                payload_delta = delta;
                emptied = h.is_empty();
                Ok(Resp::int(removed))
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

pub async fn cmd_hexists(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("hexists".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HEXISTS")?;
    let field = get_bytes_vec(args, 2, "HEXISTS")?;

    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::Hash(h) => Ok(Resp::int(h.contains_key(&field) as i64)),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_hgetall(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("hgetall".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HGETALL")?;

    let snap = {
        let store_db = db.store.db(db_index).read_for(&key);
        match store_db.get_ro(&key) {
            None => None,
            Some(e) => match &e.value {
                DataType::Hash(h) => {
                    if h.is_empty() {
                        None
                    } else {
                        Some(Ok(h.hgetall_snap()))
                    }
                }
                _ => Some(Err(NexradeError::WrongType)),
            },
        }
    };

    match snap {
        None => Ok(Resp::array(vec![])),
        Some(Err(e)) => Err(e),
        // Array (not Raw) so connection-layer RESP3 can upgrade HGETALL → Map.
        // Compact still wins: Arc snap under a brief lock, then frame outside.
        Some(Ok(HashGetAllSnap::Compact {
            buf,
            count,
            need: _,
        })) => {
            let mut items = Vec::with_capacity(count * 2);
            for bulk in CompactHashBulkIter::new(&buf, count) {
                items.push(Resp::bulk(Bytes::copy_from_slice(bulk)));
            }
            Ok(Resp::array(items))
        }
        Some(Ok(HashGetAllSnap::Hashtable { pairs })) => {
            let mut items = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in pairs {
                items.push(Resp::bulk(Bytes::from(k)));
                items.push(Resp::bulk(Bytes::from(v)));
            }
            Ok(Resp::array(items))
        }
    }
}

pub async fn cmd_hkeys(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("hkeys".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HKEYS")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::Hash(h) => Ok(Resp::array(
                h.keys()
                    .into_iter()
                    .map(|k| Resp::bulk(Bytes::from(k)))
                    .collect(),
            )),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_hvals(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("hvals".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HVALS")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::Hash(h) => Ok(Resp::array(
                h.values()
                    .into_iter()
                    .map(|v| Resp::bulk(Bytes::from(v)))
                    .collect(),
            )),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_hlen(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("hlen".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HLEN")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::Hash(h) => Ok(Resp::int(h.len() as i64)),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_hsetnx(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("hsetnx".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HSETNX")?;
    let field = get_bytes_vec(args, 2, "HSETNX")?;
    let val = get_bytes_vec(args, 3, "HSETNX")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let (resp, delta) = {
        let hash = get_or_create_hash(&mut store_db, &key)?;
        if hash.contains_key(&field) {
            (Ok(Resp::int(0)), 0isize)
        } else {
            let (_new, d) = hash.insert(field, val);
            (Ok(Resp::int(1)), d)
        }
    };
    store_db.adjust_live_bytes(delta);
    resp
}

pub async fn cmd_hincrby(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("hincrby".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HINCRBY")?;
    let field = get_bytes_vec(args, 2, "HINCRBY")?;
    let delta = get_i64(args, 3, "HINCRBY")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let (new_val, payload_delta) = {
        let hash = get_or_create_hash(&mut store_db, &key)?;
        let current: i64 = match hash.get(&field) {
            None => 0,
            Some(v) => std::str::from_utf8(&v)
                .map_err(|_| NexradeError::NotInteger)?
                .parse()
                .map_err(|_| NexradeError::NotInteger)?,
        };
        let new_val = current.checked_add(delta).ok_or(NexradeError::Overflow)?;
        let (_is_new, d) = hash.insert(field, new_val.to_string().into_bytes());
        (new_val, d)
    };
    store_db.adjust_live_bytes(payload_delta);
    Ok(Resp::int(new_val))
}

pub async fn cmd_hincrbyfloat(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("hincrbyfloat".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HINCRBYFLOAT")?;
    let field = get_bytes_vec(args, 2, "HINCRBYFLOAT")?;
    let delta = get_f64(args, 3, "HINCRBYFLOAT")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let (s, payload_delta) = {
        let hash = get_or_create_hash(&mut store_db, &key)?;
        let current: f64 = match hash.get(&field) {
            None => 0.0,
            Some(v) => std::str::from_utf8(&v)
                .map_err(|_| NexradeError::NotFloat)?
                .parse()
                .map_err(|_| NexradeError::NotFloat)?,
        };
        let new_val = current + delta;
        if new_val.is_nan() || new_val.is_infinite() {
            return Err(NexradeError::Generic(
                "increment would produce NaN or Infinity".to_string(),
            ));
        }
        let s = format_float(new_val);
        let (_is_new, d) = hash.insert(field, s.as_bytes().to_vec());
        (s, d)
    };
    store_db.adjust_live_bytes(payload_delta);
    Ok(Resp::bulk_str(s))
}

/// `HSCAN key cursor [MATCH pattern] [COUNT count]`
pub async fn cmd_hscan(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("hscan".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HSCAN")?;
    // Hex-encoded boundary field bytes (opaque to clients). Empty
    // cursor means "start from the beginning". See `decode_scan_cursor`.
    let cursor: Option<u64> = get_str(args, 2, "HSCAN").ok().and_then(decode_scan_cursor);

    let mut pattern: Option<Vec<u8>> = None;
    let mut count: usize = 10;
    let mut i = 3;
    while i < args.len() {
        let opt = get_str(args, i, "HSCAN")?.to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                pattern = Some(get_bytes_vec(args, i + 1, "HSCAN")?);
                i += 2;
            }
            "COUNT" => {
                let n = get_i64(args, i + 1, "HSCAN")?;
                if n <= 0 {
                    return Err(NexradeError::Generic("syntax error".to_string()));
                }
                count = n as usize;
                i += 2;
            }
            _ => i += 1,
        }
    }

    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![Resp::bulk_str("0"), Resp::array(vec![])])),
        Some(e) => match &e.value {
            DataType::Hash(h) => {
                let pat = pattern.unwrap_or_else(|| b"*".to_vec());
                let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = h
                    .to_pairs()
                    .into_iter()
                    .filter(|(k, _)| glob_match(&pat, k.as_slice()))
                    .collect();
                // Field-space cursor, not an offset — see `scan_cursor_token`.
                // Ordering by the cursor token makes the token a position in
                // the walked order, so a deleted boundary field cannot shift
                // the resume point.
                // Byte sort then a stable cached-key sort on the token:
                // one hash per element instead of one per comparison.
                pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                pairs.sort_by_cached_key(|p| scan_cursor_token(&p.0));
                let start = scan_start_offset_by(&pairs, cursor, |p| &p.0);
                let end = (start + count).min(pairs.len());
                let next = if end >= pairs.len() {
                    0
                } else {
                    scan_cursor_token(&pairs[end - 1].0)
                };
                let mut items = Vec::with_capacity((end - start) * 2);
                for (k, v) in &pairs[start..end] {
                    items.push(Resp::bulk(Bytes::copy_from_slice(k)));
                    items.push(Resp::bulk(Bytes::copy_from_slice(v)));
                }
                Ok(Resp::array(vec![
                    Resp::bulk_str(next.to_string()),
                    Resp::array(items),
                ]))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_hrandfield(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("hrandfield".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HRANDFIELD")?;
    let count = if args.len() >= 3 {
        Some(get_i64(args, 2, "HRANDFIELD")?)
    } else {
        None
    };

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &e.value {
            DataType::Hash(h) => {
                let fields = h.keys();
                match count {
                    None => {
                        let idx = pseudo_rand_idx(fields.len());
                        Ok(fields
                            .get(idx)
                            .map(|k| Resp::bulk(Bytes::from(k.clone())))
                            .unwrap_or(Resp::null()))
                    }
                    Some(n) => {
                        let result: Vec<Resp> = if n < 0 {
                            let count = n.unsigned_abs() as usize;
                            (0..count)
                                .map(|_| {
                                    let idx = pseudo_rand_idx(fields.len());
                                    fields
                                        .get(idx)
                                        .map(|k| Resp::bulk(Bytes::from(k.clone())))
                                        .unwrap_or(Resp::null())
                                })
                                .collect()
                        } else {
                            fields
                                .into_iter()
                                .take(n as usize)
                                .map(|k| Resp::bulk(Bytes::from(k)))
                                .collect()
                        };
                        Ok(Resp::array(result))
                    }
                }
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

fn pseudo_rand_idx(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos() as usize)
                % len
        }
        #[cfg(target_arch = "wasm32")]
        {
            0
        }
    }
}
