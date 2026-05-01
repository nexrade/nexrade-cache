//! Set command handlers.

use std::collections::HashSet;

use bytes::Bytes;

use crate::command::{get_bytes_vec, get_i64};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::DataType;

fn get_or_create_set<'a>(
    db: &'a mut crate::store::Database,
    key: &[u8],
) -> Result<&'a mut HashSet<Vec<u8>>> {
    if !db.contains_key(key) {
        db.insert(key.to_vec(), Entry::new(DataType::Set(HashSet::new())));
    }
    match db.get_mut(key) {
        Some(e) => match &mut e.value {
            DataType::Set(s) => Ok(s),
            _ => Err(NexradeError::WrongType),
        },
        None => unreachable!(),
    }
}

pub async fn cmd_sadd(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("sadd".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SADD")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    let set = get_or_create_set(&mut store_db, &key)?;

    let mut added = 0i64;
    for i in 2..args.len() {
        let member = get_bytes_vec(args, i, "SADD")?;
        if set.insert(member) {
            added += 1;
        }
    }
    Ok(Resp::int(added))
}

pub async fn cmd_srem(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("srem".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SREM")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::Set(s) => {
                let mut removed = 0i64;
                for i in 2..args.len() {
                    if let Ok(m) = get_bytes_vec(args, i, "SREM") {
                        if s.remove(&m) {
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
                s.iter()
                    .map(|m| Resp::bulk(Bytes::from(m.clone())))
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
    dst_shard.insert(dst, Entry::new(DataType::Set(result.into_iter().collect())));
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
    dst_shard.insert(dst, Entry::new(DataType::Set(result.into_iter().collect())));
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
    dst_shard.insert(dst, Entry::new(DataType::Set(result.into_iter().collect())));
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
                DataType::Set(s) => result.extend(s.iter().cloned()),
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
                DataType::Set(s) => sets.push(s.clone()),
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
            DataType::Set(s) => s.clone(),
            _ => return Err(NexradeError::WrongType),
        },
    };

    for i in (start + 1)..args.len() {
        let key = get_bytes_vec(args, i, "SDIFF")?;
        if let Some(e) = sdb.write_for(&key).get(&key) {
            match &e.value {
                DataType::Set(s) => {
                    for m in s {
                        result.remove(m);
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
                let members: Vec<_> = s.iter().cloned().collect();
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
                            // Negative count: allow duplicates, return exactly |n| elements.
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
                            // Positive count: no duplicates, up to n elements.
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
    match store_db.get_mut(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &mut e.value {
            DataType::Set(s) => {
                if let Some(n) = count {
                    let mut result = Vec::new();
                    for _ in 0..n {
                        let idx = pseudo_rand_idx(s.len());
                        if let Some(m) = s.iter().nth(idx).cloned() {
                            s.remove(&m);
                            result.push(Resp::bulk(Bytes::from(m)));
                        }
                    }
                    Ok(Resp::array(result))
                } else {
                    let idx = pseudo_rand_idx(s.len());
                    if let Some(m) = s.iter().nth(idx).cloned() {
                        s.remove(&m);
                        Ok(Resp::bulk(Bytes::from(m)))
                    } else {
                        Ok(Resp::null())
                    }
                }
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_sscan(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("sscan".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SSCAN")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![Resp::bulk_str("0"), Resp::array(vec![])])),
        Some(e) => match &e.value {
            DataType::Set(s) => {
                let items: Vec<Resp> = s
                    .iter()
                    .map(|m| Resp::bulk(Bytes::from(m.clone())))
                    .collect();
                Ok(Resp::array(vec![Resp::bulk_str("0"), Resp::array(items)]))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

fn pseudo_rand_idx(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as usize)
        % len
}
