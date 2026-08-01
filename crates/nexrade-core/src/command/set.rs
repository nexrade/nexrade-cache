//! Set command handlers.

use std::collections::HashSet;

use bytes::Bytes;

use crate::command::{get_bytes_vec, get_i64, get_str};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::set_data::SetData;
use crate::store::{glob_match, Entry};
use crate::types::DataType;

fn get_or_create_set<'a>(
    db: &'a mut crate::store::Database,
    key: &[u8],
) -> Result<&'a mut SetData> {
    let entry = db.get_or_insert_with(key, || Entry::new(DataType::Set(SetData::new())));
    match &mut entry.value {
        DataType::Set(s) => Ok(s),
        _ => Err(NexradeError::WrongType),
    }
}

pub async fn cmd_sadd(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("sadd".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SADD")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    let (added, delta) = {
        let set = get_or_create_set(&mut store_db, &key)?;
        let mut added = 0i64;
        let mut delta: isize = 0;
        for i in 2..args.len() {
            let member = get_bytes_vec(args, i, "SADD")?;
            let (is_new, d) = set.insert(member);
            delta += d;
            if is_new {
                added += 1;
            }
        }
        (added, delta)
    };
    store_db.adjust_live_bytes(delta);
    Ok(Resp::int(added))
}

pub async fn cmd_srem(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("srem".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SREM")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    let mut emptied = false;
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::Set(s) => {
                let mut removed = 0i64;
                let mut delta: isize = 0;
                for i in 2..args.len() {
                    if let Ok(m) = get_bytes_vec(args, i, "SREM") {
                        let (ok, d) = s.remove(&m);
                        if ok {
                            removed += 1;
                            delta += d;
                        }
                    }
                }
                payload_delta = delta;
                emptied = s.is_empty();
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

pub async fn cmd_sismember(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("sismember".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SISMEMBER")?;
    let member = get_bytes_vec(args, 2, "SISMEMBER")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::Set(s) => Ok(Resp::int(s.contains(&member) as i64)),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_smismember(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("smismember".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SMISMEMBER")?;
    let store_db = db.store.db(db_index).read_for(&key);

    let results: Vec<Resp> = (2..args.len())
        .map(|i| {
            let member = get_bytes_vec(args, i, "SMISMEMBER").ok()?;
            match store_db.get_ro(&key) {
                Some(e) => match &e.value {
                    DataType::Set(s) => Some(Resp::int(s.contains(&member) as i64)),
                    _ => None,
                },
                None => Some(Resp::int(0)),
            }
        })
        .map(|v| v.unwrap_or(Resp::int(0)))
        .collect();

    Ok(Resp::array(results))
}

pub async fn cmd_smembers(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("smembers".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SMEMBERS")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::Set(s) => Ok(Resp::array(
                s.to_vec()
                    .into_iter()
                    .map(|m| Resp::bulk(Bytes::from(m)))
                    .collect(),
            )),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_scard(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("scard".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SCARD")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::Set(s) => Ok(Resp::int(s.len() as i64)),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_sunion(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("sunion".to_string()));
    }
    let result = collect_union(db, args, db_index, 1).await?;
    Ok(Resp::array(
        result
            .into_iter()
            .map(|m| Resp::bulk(Bytes::from(m)))
            .collect(),
    ))
}

pub async fn cmd_sunionstore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("sunionstore".to_string()));
    }
    let dst = get_bytes_vec(args, 1, "SUNIONSTORE")?;
    let result = collect_union(db, args, db_index, 2).await?;
    let count = result.len() as i64;
    let mut dst_shard = db.store.db(db_index).write_for(&dst);
    dst_shard.insert(
        dst,
        Entry::new(DataType::Set(SetData::from_hashset(result))),
    );
    Ok(Resp::int(count))
}

pub async fn cmd_sinter(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("sinter".to_string()));
    }
    let result = collect_inter(db, args, db_index, 1).await?;
    Ok(Resp::array(
        result
            .into_iter()
            .map(|m| Resp::bulk(Bytes::from(m)))
            .collect(),
    ))
}

pub async fn cmd_sinterstore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("sinterstore".to_string()));
    }
    let dst = get_bytes_vec(args, 1, "SINTERSTORE")?;
    let result = collect_inter(db, args, db_index, 2).await?;
    let count = result.len() as i64;
    let mut dst_shard = db.store.db(db_index).write_for(&dst);
    dst_shard.insert(
        dst,
        Entry::new(DataType::Set(SetData::from_hashset(result))),
    );
    Ok(Resp::int(count))
}

pub async fn cmd_sdiff(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("sdiff".to_string()));
    }
    let result = collect_diff(db, args, db_index, 1).await?;
    Ok(Resp::array(
        result
            .into_iter()
            .map(|m| Resp::bulk(Bytes::from(m)))
            .collect(),
    ))
}

pub async fn cmd_sdiffstore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("sdiffstore".to_string()));
    }
    let dst = get_bytes_vec(args, 1, "SDIFFSTORE")?;
    let result = collect_diff(db, args, db_index, 2).await?;
    let count = result.len() as i64;
    let mut dst_shard = db.store.db(db_index).write_for(&dst);
    dst_shard.insert(
        dst,
        Entry::new(DataType::Set(SetData::from_hashset(result))),
    );
    Ok(Resp::int(count))
}

async fn collect_union(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    start: usize,
) -> Result<HashSet<Vec<u8>>> {
    let mut result: HashSet<Vec<u8>> = HashSet::new();
    let sdb = db.store.db(db_index);
    for i in start..args.len() {
        let key = get_bytes_vec(args, i, "SUNION")?;
        if let Some(e) = sdb.write_for(&key).get(&key) {
            match &e.value {
                DataType::Set(s) => result.extend(s.to_vec()),
                _ => return Err(NexradeError::WrongType),
            }
        }
    }
    Ok(result)
}

async fn collect_inter(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    start: usize,
) -> Result<HashSet<Vec<u8>>> {
    let sdb = db.store.db(db_index);
    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::new();
    for i in start..args.len() {
        let key = get_bytes_vec(args, i, "SINTER")?;
        match sdb.write_for(&key).get(&key) {
            None => return Ok(HashSet::new()),
            Some(e) => match &e.value {
                DataType::Set(s) => sets.push(s.to_hashset()),
                _ => return Err(NexradeError::WrongType),
            },
        }
    }

    if sets.is_empty() {
        return Ok(HashSet::new());
    }

    let mut result = sets[0].clone();
    for s in &sets[1..] {
        result = result.intersection(s).cloned().collect();
    }
    Ok(result)
}

async fn collect_diff(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    start: usize,
) -> Result<HashSet<Vec<u8>>> {
    let sdb = db.store.db(db_index);
    let first_key = get_bytes_vec(args, start, "SDIFF")?;
    let mut result = match sdb.write_for(&first_key).get(&first_key) {
        None => HashSet::new(),
        Some(e) => match &e.value {
            DataType::Set(s) => s.to_hashset(),
            _ => return Err(NexradeError::WrongType),
        },
    };

    for i in (start + 1)..args.len() {
        let key = get_bytes_vec(args, i, "SDIFF")?;
        if let Some(e) = sdb.write_for(&key).get(&key) {
            match &e.value {
                DataType::Set(s) => {
                    for m in s.to_vec() {
                        result.remove(&m);
                    }
                }
                _ => return Err(NexradeError::WrongType),
            }
        }
    }
    Ok(result)
}

pub async fn cmd_smove(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("smove".to_string()));
    }
    let src = get_bytes_vec(args, 1, "SMOVE")?;
    let dst = get_bytes_vec(args, 2, "SMOVE")?;
    let member = get_bytes_vec(args, 3, "SMOVE")?;

    let result = db.store.db(db_index).smove_atomic(&src, dst, member);
    Ok(Resp::int(result))
}

pub async fn cmd_srandmember(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("srandmember".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SRANDMEMBER")?;
    let count = if args.len() >= 3 {
        Some(get_i64(args, 2, "SRANDMEMBER")?)
    } else {
        None
    };

    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &e.value {
            DataType::Set(s) => {
                let members = s.to_vec();
                match count {
                    None => {
                        let idx = pseudo_rand_idx(members.len());
                        Ok(members
                            .get(idx)
                            .map(|m| Resp::bulk(Bytes::from(m.clone())))
                            .unwrap_or(Resp::null()))
                    }
                    Some(n) => {
                        let result: Vec<Resp> = if n < 0 {
                            let count = n.unsigned_abs() as usize;
                            (0..count)
                                .map(|_| {
                                    let idx = pseudo_rand_idx(members.len());
                                    members
                                        .get(idx)
                                        .map(|m| Resp::bulk(Bytes::from(m.clone())))
                                        .unwrap_or(Resp::null())
                                })
                                .collect()
                        } else {
                            members
                                .into_iter()
                                .take(n as usize)
                                .map(|m| Resp::bulk(Bytes::from(m)))
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

pub async fn cmd_spop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("spop".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SPOP")?;
    let count = if args.len() >= 3 {
        let n = get_i64(args, 2, "SPOP")?;
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
    let mut emptied = false;
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &mut e.value {
            DataType::Set(s) => {
                let mut delta: isize = 0;
                let resp = if let Some(n) = count {
                    let mut result = Vec::new();
                    for _ in 0..n {
                        if s.is_empty() {
                            break;
                        }
                        let idx = pseudo_rand_idx(s.len());
                        if let Some(m) = s.remove_at(idx) {
                            delta -= m.len() as isize;
                            result.push(Resp::bulk(Bytes::from(m)));
                        }
                    }
                    Ok(Resp::array(result))
                } else if s.is_empty() {
                    Ok(Resp::null())
                } else {
                    let idx = pseudo_rand_idx(s.len());
                    match s.remove_at(idx) {
                        Some(m) => {
                            delta -= m.len() as isize;
                            Ok(Resp::bulk(Bytes::from(m)))
                        }
                        None => Ok(Resp::null()),
                    }
                };
                payload_delta = delta;
                emptied = s.is_empty();
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

/// `SSCAN key cursor [MATCH pattern] [COUNT count]`
pub async fn cmd_sscan(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("sscan".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SSCAN")?;
    let cursor: u64 = get_i64(args, 2, "SSCAN")
        .ok()
        .map(|n| n.max(0) as u64)
        .unwrap_or(0);

    let mut pattern: Option<Vec<u8>> = None;
    let mut count: usize = 10;
    let mut i = 3;
    while i < args.len() {
        let opt = get_str(args, i, "SSCAN")?.to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                pattern = Some(get_bytes_vec(args, i + 1, "SSCAN")?);
                i += 2;
            }
            "COUNT" => {
                let n = get_i64(args, i + 1, "SSCAN")?;
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
            DataType::Set(s) => {
                let pat = pattern.unwrap_or_else(|| b"*".to_vec());
                let mut members: Vec<Vec<u8>> = s
                    .to_vec()
                    .into_iter()
                    .filter(|m| glob_match(&pat, m.as_slice()))
                    .collect();
                members.sort();
                let start = (cursor as usize).min(members.len());
                let end = (start + count).min(members.len());
                let next = if end >= members.len() {
                    0u64
                } else {
                    end as u64
                };
                let items: Vec<Resp> = members[start..end]
                    .iter()
                    .map(|m| Resp::bulk(Bytes::copy_from_slice(m)))
                    .collect();
                Ok(Resp::array(vec![
                    Resp::bulk_str(next.to_string()),
                    Resp::array(items),
                ]))
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

/// `SINTERCARD numkeys key [key ...] [LIMIT limit]`
pub async fn cmd_sintercard(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("sintercard".to_string()));
    }
    let num_keys = get_i64(args, 1, "SINTERCARD")?;
    if num_keys <= 0 {
        return Err(NexradeError::Generic(
            "numkeys should be greater than 0".to_string(),
        ));
    }
    let num_keys = num_keys as usize;
    let keys_start = 2;
    if args.len() < keys_start + num_keys {
        return Err(NexradeError::WrongArity("sintercard".to_string()));
    }
    let keys: Vec<Vec<u8>> = (keys_start..keys_start + num_keys)
        .map(|i| get_bytes_vec(args, i, "SINTERCARD"))
        .collect::<Result<_>>()?;

    let mut limit: Option<usize> = None;
    if args.len() > keys_start + num_keys {
        let opt = get_str(args, keys_start + num_keys, "SINTERCARD")?.to_uppercase();
        if opt != "LIMIT" {
            return Err(NexradeError::Generic("syntax error".to_string()));
        }
        let raw_limit = get_i64(args, keys_start + num_keys + 1, "SINTERCARD")? as usize;
        if raw_limit > 0 {
            limit = Some(raw_limit);
        }
        if args.len() != keys_start + num_keys + 2 {
            return Err(NexradeError::Generic("syntax error".to_string()));
        }
    } else if args.len() != keys_start + num_keys {
        return Err(NexradeError::Generic("syntax error".to_string()));
    }

    let sdb = db.store.db(db_index);
    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::with_capacity(keys.len());
    let mut smallest = usize::MAX;
    let mut smallest_idx = 0;
    for (i, key) in keys.iter().enumerate() {
        match sdb.read_for(key).get_ro(key) {
            None => return Ok(Resp::int(0)),
            Some(e) => match &e.value {
                DataType::Set(s) => {
                    if s.len() < smallest {
                        smallest = s.len();
                        smallest_idx = i;
                    }
                    sets.push(s.to_hashset());
                }
                _ => return Err(NexradeError::WrongType),
            },
        }
    }
    if sets.is_empty() {
        return Ok(Resp::int(0));
    }

    let mut other_idx = Vec::with_capacity(sets.len());
    for (i, _) in sets.iter().enumerate() {
        if i != smallest_idx {
            other_idx.push(i);
        }
    }
    let mut count = 0i64;
    for member in &sets[smallest_idx] {
        if other_idx.iter().all(|&i| sets[i].contains(member)) {
            count += 1;
            if let Some(lim) = limit {
                if count as usize >= lim {
                    break;
                }
            }
        }
    }
    Ok(Resp::int(count))
}
