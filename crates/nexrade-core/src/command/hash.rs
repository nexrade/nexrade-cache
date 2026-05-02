//! Hash command handlers.

use std::collections::HashMap;

use bytes::Bytes;

use super::string::format_float;

use crate::command::{get_bytes_vec, get_f64, get_i64};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::DataType;

fn get_or_create_hash<'a>(
    db: &'a mut crate::store::Database,
    key: &[u8],
) -> Result<&'a mut HashMap<Vec<u8>, Vec<u8>>> {
    if !db.contains_key(key) {
        db.insert(key.to_vec(), Entry::new(DataType::Hash(HashMap::new())));
    }
    match db.get_mut(key) {
        Some(e) => match &mut e.value {
            DataType::Hash(h) => Ok(h),
            _ => Err(NexradeError::WrongType),
        },
        None => unreachable!(),
    }
}

pub async fn cmd_hset(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 || !args.len() % 2 == 0 {
        return Err(NexradeError::WrongArity("hset".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HSET")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    let hash = get_or_create_hash(&mut store_db, &key)?;

    let mut added = 0i64;
    let mut i = 2;
    while i + 1 < args.len() {
        let field = get_bytes_vec(args, i, "HSET")?;
        let val = get_bytes_vec(args, i + 1, "HSET")?;
        if hash.insert(field, val).is_none() {
            added += 1;
        }
        i += 2;
    }
    Ok(Resp::int(added))
}

pub async fn cmd_hmset(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // HMSET is deprecated but still supported (same as HSET)
    if args.len() < 4 || !args.len() % 2 == 0 {
        return Err(NexradeError::WrongArity("hmset".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HMSET")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    let hash = get_or_create_hash(&mut store_db, &key)?;

    let mut i = 2;
    while i + 1 < args.len() {
        let field = get_bytes_vec(args, i, "HMSET")?;
        let val = get_bytes_vec(args, i + 1, "HMSET")?;
        hash.insert(field, val);
        i += 2;
    }
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
                .map(|v| Resp::bulk(Bytes::from(v.clone())))
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
                    DataType::Hash(h) => h.get(&field).map(|v| Resp::bulk(Bytes::from(v.clone()))),
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

    match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::Hash(h) => {
                let mut removed = 0i64;
                for i in 2..args.len() {
                    if let Ok(f) = get_bytes_vec(args, i, "HDEL") {
                        if h.remove(&f).is_some() {
                            removed += 1;
                        }
                    }
                }
                Ok(Resp::int(removed))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
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
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::Hash(h) => {
                let mut items = Vec::with_capacity(h.len() * 2);
                for (k, v) in h {
                    items.push(Resp::bulk(Bytes::from(k.clone())));
                    items.push(Resp::bulk(Bytes::from(v.clone())));
                }
                Ok(Resp::array(items))
            }
            _ => Err(NexradeError::WrongType),
        },
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
                    .map(|k| Resp::bulk(Bytes::from(k.clone())))
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
                    .map(|v| Resp::bulk(Bytes::from(v.clone())))
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
    let hash = get_or_create_hash(&mut store_db, &key)?;

    match hash.entry(field) {
        std::collections::hash_map::Entry::Occupied(_) => Ok(Resp::int(0)),
        std::collections::hash_map::Entry::Vacant(e) => {
            e.insert(val);
            Ok(Resp::int(1))
        }
    }
}

pub async fn cmd_hincrby(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("hincrby".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HINCRBY")?;
    let field = get_bytes_vec(args, 2, "HINCRBY")?;
    let delta = get_i64(args, 3, "HINCRBY")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let hash = get_or_create_hash(&mut store_db, &key)?;

    let current: i64 = match hash.get(&field) {
        None => 0,
        Some(v) => std::str::from_utf8(v)
            .map_err(|_| NexradeError::NotInteger)?
            .parse()
            .map_err(|_| NexradeError::NotInteger)?,
    };

    let new_val = current.checked_add(delta).ok_or(NexradeError::Overflow)?;
    hash.insert(field, new_val.to_string().into_bytes());
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
    let hash = get_or_create_hash(&mut store_db, &key)?;

    let current: f64 = match hash.get(&field) {
        None => 0.0,
        Some(v) => std::str::from_utf8(v)
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
    hash.insert(field, s.as_bytes().to_vec());
    Ok(Resp::bulk_str(s))
}

pub async fn cmd_hscan(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // Simplified HSCAN — returns all fields (cursor always 0)
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("hscan".to_string()));
    }
    let key = get_bytes_vec(args, 1, "HSCAN")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    match store_db.get(&key) {
        None => Ok(Resp::array(vec![Resp::bulk_str("0"), Resp::array(vec![])])),
        Some(e) => match &e.value {
            DataType::Hash(h) => {
                let mut items = Vec::with_capacity(h.len() * 2);
                for (k, v) in h {
                    items.push(Resp::bulk(Bytes::from(k.clone())));
                    items.push(Resp::bulk(Bytes::from(v.clone())));
                }
                Ok(Resp::array(vec![Resp::bulk_str("0"), Resp::array(items)]))
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
                let fields: Vec<_> = h.keys().cloned().collect();
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
                            // Negative count: allow duplicates, return exactly |n| elements.
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
                            // Positive count: no duplicates, up to n elements.
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
