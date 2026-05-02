//! Generic key command handlers.

use std::time::Duration;

use bytes::Bytes;

use crate::command::{get_bytes_vec, get_i64, get_str};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::expiry::Expiry;
use crate::resp::Resp;

pub async fn cmd_del(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("del".to_string()));
    }
    let sdb = db.store.db(db_index);
    let mut deleted = 0i64;
    for i in 1..args.len() {
        let key = get_bytes_vec(args, i, "DEL")?;
        if sdb.write_for(&key).remove(&key).is_some() {
            deleted += 1;
        }
    }
    Ok(Resp::int(deleted))
}

/// UNLINK — like DEL but frees memory asynchronously.
/// Removes the keys from the keyspace immediately (so other clients can't
/// see them), then drops the values in a background task.
pub async fn cmd_unlink(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("unlink".to_string()));
    }
    let sdb = db.store.db(db_index);
    let mut evicted = Vec::new();
    for i in 1..args.len() {
        let key = get_bytes_vec(args, i, "UNLINK")?;
        if let Some(entry) = sdb.write_for(&key).remove(&key) {
            evicted.push(entry);
        }
    }
    let count = evicted.len() as i64;
    // Drop the values on a background thread so large allocations don't
    // block the async executor.
    if !evicted.is_empty() {
        #[cfg(not(target_arch = "wasm32"))]
        tokio::task::spawn_blocking(move || drop(evicted));
        #[cfg(target_arch = "wasm32")]
        drop(evicted);
    }
    Ok(Resp::int(count))
}

pub async fn cmd_exists(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("exists".to_string()));
    }
    let sdb = db.store.db(db_index);
    let mut count = 0i64;
    for i in 1..args.len() {
        let key = get_bytes_vec(args, i, "EXISTS")?;
        if sdb.read_for(&key).get_ro(&key).is_some() {
            count += 1;
        }
    }
    Ok(Resp::int(count))
}

pub async fn cmd_expire(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    set_expire(db, args, db_index, false, false, "EXPIRE").await
}

pub async fn cmd_pexpire(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    set_expire(db, args, db_index, true, false, "PEXPIRE").await
}

pub async fn cmd_expireat(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    set_expire(db, args, db_index, false, true, "EXPIREAT").await
}

pub async fn cmd_pexpireat(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    set_expire(db, args, db_index, true, true, "PEXPIREAT").await
}

async fn set_expire(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    millis: bool,
    absolute: bool,
    cmd: &str,
) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let key = get_bytes_vec(args, 1, cmd)?;
    let val = get_i64(args, 2, cmd)?;

    // Relative expire with non-positive timeout is an error (matches Redis 7+).
    if !absolute && val <= 0 {
        return Err(NexradeError::Generic(format!(
            "invalid expire time in '{}'",
            cmd.to_lowercase()
        )));
    }
    // Absolute timestamp must be positive.
    if absolute && val < 0 {
        return Err(NexradeError::Generic(format!(
            "invalid expire time in '{}'",
            cmd.to_lowercase()
        )));
    }

    let expiry = if absolute {
        if millis {
            Expiry::from_ms(val as u64)
        } else {
            Expiry::from_secs(val as u64)
        }
    } else if millis {
        Expiry::from_duration(Duration::from_millis(val as u64))
    } else {
        Expiry::from_duration(Duration::from_secs(val as u64))
    };

    let mut store_db = db.store.db(db_index).write_for(&key);
    if store_db.get(&key).is_none() {
        return Ok(Resp::int(0));
    }
    store_db.set_expiry(&key, Some(expiry));
    Ok(Resp::int(1))
}

pub async fn cmd_expiretime(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    get_expire_time(db, args, db_index, false, "EXPIRETIME").await
}

pub async fn cmd_pexpiretime(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    get_expire_time(db, args, db_index, true, "PEXPIRETIME").await
}

async fn get_expire_time(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    millis: bool,
    cmd: &str,
) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let key = get_bytes_vec(args, 1, cmd)?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::int(-2)),
        Some(e) => match &e.expiry {
            None => Ok(Resp::int(-1)),
            Some(exp) => {
                if millis {
                    let ms = exp.expires_at_ms.min(i64::MAX as u128) as i64;
                    Ok(Resp::int(ms))
                } else {
                    let secs = (exp.expires_at_ms / 1000).min(i64::MAX as u128) as i64;
                    Ok(Resp::int(secs))
                }
            }
        },
    }
}

pub async fn cmd_ttl(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    get_ttl(db, args, db_index, false, "TTL").await
}

pub async fn cmd_pttl(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    get_ttl(db, args, db_index, true, "PTTL").await
}

async fn get_ttl(db: &Db, args: &[Resp], db_index: usize, millis: bool, cmd: &str) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let key = get_bytes_vec(args, 1, cmd)?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::int(-2)),
        Some(e) => match &e.expiry {
            None => Ok(Resp::int(-1)),
            Some(exp) => {
                if millis {
                    Ok(Resp::int(exp.remaining_ms() as i64))
                } else {
                    Ok(Resp::int(exp.remaining_secs() as i64))
                }
            }
        },
    }
}

pub async fn cmd_persist(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("persist".to_string()));
    }
    let key = get_bytes_vec(args, 1, "PERSIST")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    let expiry_state = store_db.get(&key).map(|e| e.expiry.is_some());
    match expiry_state {
        Some(true) => {
            store_db.set_expiry(&key, None);
            Ok(Resp::int(1))
        }
        _ => Ok(Resp::int(0)),
    }
}

pub async fn cmd_type(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("type".to_string()));
    }
    let key = get_bytes_vec(args, 1, "TYPE")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::SimpleString("none".to_string())),
        Some(e) => Ok(Resp::SimpleString(e.value.type_name().to_string())),
    }
}

pub async fn cmd_rename(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("rename".to_string()));
    }
    let src = get_bytes_vec(args, 1, "RENAME")?;
    let dst = get_bytes_vec(args, 2, "RENAME")?;

    db.store
        .db(db_index)
        .rename(&src, dst)
        .ok_or(NexradeError::NoKey)?;
    Ok(Resp::ok())
}

pub async fn cmd_renamenx(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("renamenx".to_string()));
    }
    let src = get_bytes_vec(args, 1, "RENAMENX")?;
    let dst = get_bytes_vec(args, 2, "RENAMENX")?;

    match db.store.db(db_index).renamenx(&src, dst) {
        Some(v) => Ok(Resp::int(v)),
        None => Err(NexradeError::NoKey),
    }
}

pub async fn cmd_keys(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("keys".to_string()));
    }
    let pattern = get_bytes_vec(args, 1, "KEYS")?;
    let keys = db.store.db(db_index).keys_matching(&pattern);

    Ok(Resp::array(
        keys.into_iter()
            .map(|k| Resp::bulk(Bytes::from(k)))
            .collect(),
    ))
}

pub async fn cmd_scan(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("scan".to_string()));
    }
    // Simplified SCAN: cursor-based but we always return all in one shot (cursor=0)
    let mut pattern: Option<Vec<u8>> = None;
    let mut count: usize = 10;
    let mut type_filter: Option<String> = None;

    let mut i = 2;
    while i < args.len() {
        let opt = get_str(args, i, "SCAN")?.to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                pattern = Some(get_bytes_vec(args, i + 1, "SCAN")?);
                i += 2;
            }
            "COUNT" => {
                let n = get_i64(args, i + 1, "SCAN")?;
                if n <= 0 {
                    return Err(NexradeError::Generic("ERR syntax error".to_string()));
                }
                count = n as usize;
                i += 2;
            }
            "TYPE" => {
                type_filter = Some(get_str(args, i + 1, "SCAN")?.to_lowercase());
                i += 2;
            }
            _ => i += 1,
        }
    }

    let sdb = db.store.db(db_index);
    let pat = pattern.unwrap_or_else(|| b"*".to_vec());
    let mut keys = sdb.keys_matching(&pat);

    if let Some(ref t) = type_filter {
        let sdb2 = db.store.db(db_index);
        keys.retain(|k| {
            sdb2.read_for(k)
                .get_ro(k)
                .map(|e| e.value.type_name() == t.as_str())
                .unwrap_or(false)
        });
    }

    // count is a hint only — since we always return cursor=0 (single shot),
    // we must return all matching keys or scan_iter will terminate early.
    let _ = count;
    let result: Vec<Resp> = keys
        .into_iter()
        .map(|k| Resp::bulk(Bytes::from(k)))
        .collect();

    Ok(Resp::array(vec![Resp::bulk_str("0"), Resp::array(result)]))
}

pub async fn cmd_randomkey(db: &Db, _args: &[Resp], db_index: usize) -> Result<Resp> {
    let key = db.store.db(db_index).random_key();
    Ok(key
        .map(|k| Resp::bulk(Bytes::from(k)))
        .unwrap_or(Resp::null()))
}

pub async fn cmd_copy(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("copy".to_string()));
    }
    let src = get_bytes_vec(args, 1, "COPY")?;
    let dst = get_bytes_vec(args, 2, "COPY")?;
    let replace = args.iter().any(|a| {
        a.as_str()
            .is_some_and(|s| s.eq_ignore_ascii_case("REPLACE"))
    });

    let copied = db.store.db(db_index).copy_entry(&src, dst, replace);
    Ok(Resp::int(if copied { 1 } else { 0 }))
}

pub async fn cmd_move(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("move".to_string()));
    }
    let key = get_bytes_vec(args, 1, "MOVE")?;
    let target_db_i = get_i64(args, 2, "MOVE")?;
    if target_db_i < 0 || target_db_i as usize >= db.db_count() {
        return Err(NexradeError::Generic(
            "ERR DB index is out of range".to_string(),
        ));
    }
    let target_db = target_db_i as usize;

    if target_db == db_index {
        return Ok(Resp::int(0));
    }

    let entry = {
        let mut src_shard = db.store.db(db_index).write_for(&key);
        src_shard.remove(&key)
    };

    match entry {
        None => Ok(Resp::int(0)),
        Some(e) => {
            let mut target_shard = db.store.db(target_db).write_for(&key);
            if target_shard.contains_key(&key) {
                // Put it back
                drop(target_shard);
                let mut src_shard = db.store.db(db_index).write_for(&key);
                src_shard.insert(key, e);
                return Ok(Resp::int(0));
            }
            target_shard.insert(key, e);
            Ok(Resp::int(1))
        }
    }
}

pub async fn cmd_object(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("object".to_string()));
    }
    let sub = get_str(args, 1, "OBJECT")?.to_uppercase();
    let key = get_bytes_vec(args, 2, "OBJECT")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match sub.as_str() {
        "ENCODING" => match store_db.get(&key) {
            None => Err(NexradeError::NoKey),
            Some(e) => Ok(Resp::bulk_str(e.value.encoding())),
        },
        "REFCOUNT" => Ok(Resp::int(1)),
        "IDLETIME" => Ok(Resp::int(0)),
        "FREQ" => Ok(Resp::int(0)),
        "HELP" => Ok(Resp::array(vec![
            Resp::bulk_str("OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
            Resp::bulk_str("ENCODING <key>"),
            Resp::bulk_str("REFCOUNT <key>"),
            Resp::bulk_str("IDLETIME <key>"),
            Resp::bulk_str("FREQ <key>"),
        ])),
        _ => Err(NexradeError::Generic(format!(
            "unknown subcommand '{}' for OBJECT",
            sub
        ))),
    }
}

pub async fn cmd_dump(_db: &Db, _args: &[Resp], _db_index: usize) -> Result<Resp> {
    // Simplified: return null (serialized format would be complex)
    Ok(Resp::null())
}

pub async fn cmd_restore(_db: &Db, _args: &[Resp], _db_index: usize) -> Result<Resp> {
    Ok(Resp::ok())
}

pub async fn cmd_sort(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("sort".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SORT")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    let mut items: Vec<Vec<u8>> = match store_db.get(&key) {
        None => return Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            crate::types::DataType::List(l) => l.iter().map(|v| v.to_vec()).collect(),
            crate::types::DataType::Set(s) => s.iter().cloned().collect(),
            _ => return Err(NexradeError::WrongType),
        },
    };

    let alpha = args
        .iter()
        .any(|a| a.as_str().is_some_and(|s| s.eq_ignore_ascii_case("ALPHA")));
    let desc = args
        .iter()
        .any(|a| a.as_str().is_some_and(|s| s.eq_ignore_ascii_case("DESC")));

    if alpha {
        items.sort();
    } else {
        items.sort_by(|a, b| {
            let a_n: f64 = std::str::from_utf8(a)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let b_n: f64 = std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            a_n.partial_cmp(&b_n).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    if desc {
        items.reverse();
    }

    Ok(Resp::array(
        items
            .into_iter()
            .map(|v| Resp::bulk(Bytes::from(v)))
            .collect(),
    ))
}

pub async fn cmd_touch(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("touch".to_string()));
    }
    let sdb = db.store.db(db_index);
    let mut touched = 0i64;
    for i in 1..args.len() {
        let key = get_bytes_vec(args, i, "TOUCH")?;
        if sdb.read_for(&key).get_ro(&key).is_some() {
            touched += 1;
        }
    }
    Ok(Resp::int(touched))
}
