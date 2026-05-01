//! String command handlers.

use std::time::Duration;

use bytes::Bytes;

use crate::command::{get_bytes_vec, get_f64, get_i64, get_str};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::expiry::Expiry;
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::DataType;

pub async fn cmd_set(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("set".to_string()));
    }

    let key = get_bytes_vec(args, 1, "SET")?;
    let value = get_bytes_vec(args, 2, "SET")?;

    let mut expiry: Option<Expiry> = None;
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut keepttl = false;

    let mut i = 3;
    while i < args.len() {
        let opt = get_str(args, i, "SET")?.to_uppercase();
        match opt.as_str() {
            "EX" => {
                let secs = get_i64(args, i + 1, "SET")?;
                if secs <= 0 {
                    return Err(NexradeError::Generic(
                        "invalid expire time in SET".to_string(),
                    ));
                }
                expiry = Some(Expiry::from_duration(Duration::from_secs(secs as u64)));
                i += 2;
            }
            "PX" => {
                let ms = get_i64(args, i + 1, "SET")?;
                if ms <= 0 {
                    return Err(NexradeError::Generic(
                        "invalid expire time in SET".to_string(),
                    ));
                }
                expiry = Some(Expiry::from_duration(Duration::from_millis(ms as u64)));
                i += 2;
            }
            "EXAT" => {
                let secs = get_i64(args, i + 1, "SET")?;
                if secs < 0 {
                    return Err(NexradeError::Generic(
                        "invalid expire time in SET".to_string(),
                    ));
                }
                expiry = Some(Expiry::from_secs(secs as u64));
                i += 2;
            }
            "PXAT" => {
                let ms = get_i64(args, i + 1, "SET")?;
                if ms < 0 {
                    return Err(NexradeError::Generic(
                        "invalid expire time in SET".to_string(),
                    ));
                }
                expiry = Some(Expiry::from_ms(ms as u64));
                i += 2;
            }
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GET" => {
                get = true;
                i += 1;
            }
            "KEEPTTL" => {
                keepttl = true;
                i += 1;
            }
            _ => {
                return Err(NexradeError::SyntaxError);
            }
        }
    }

    let mut store_db = db.store.db(db_index).write_for(&key);

    // GET option: return old value before SET
    let old_value = if get {
        match store_db.get(&key) {
            Some(e) => match &e.value {
                DataType::String(v) => Some(Resp::bulk(Bytes::from(v.clone()))),
                _ => return Err(NexradeError::WrongType),
            },
            None => Some(Resp::null()),
        }
    } else {
        None
    };

    let exists = store_db.contains_key(&key);

    if nx && exists {
        return Ok(if get {
            old_value.unwrap()
        } else {
            Resp::null()
        });
    }
    if xx && !exists {
        return Ok(if get {
            old_value.unwrap()
        } else {
            Resp::null()
        });
    }

    // KEEPTTL: preserve the expiry already on the key (if any).
    let effective_expiry = if keepttl {
        store_db.get(&key).and_then(|e| e.expiry.clone())
    } else {
        expiry
    };

    let entry = match effective_expiry {
        Some(exp) => Entry::with_expiry(DataType::String(value), exp),
        None => Entry::new(DataType::String(value)),
    };
    store_db.insert(key, entry);

    Ok(if get { old_value.unwrap() } else { Resp::ok() })
}

pub async fn cmd_get(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("get".to_string()));
    }
    let key = get_bytes_vec(args, 1, "GET")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &e.value {
            DataType::String(v) => Ok(Resp::bulk(Bytes::from(v.clone()))),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_getset(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("getset".to_string()));
    }
    let key = get_bytes_vec(args, 1, "GETSET")?;
    let value = get_bytes_vec(args, 2, "GETSET")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let old = match store_db.get(&key) {
        None => Resp::null(),
        Some(e) => match &e.value {
            DataType::String(v) => Resp::bulk(Bytes::from(v.clone())),
            _ => return Err(NexradeError::WrongType),
        },
    };
    store_db.insert(key, Entry::new(DataType::String(value)));
    Ok(old)
}

pub async fn cmd_getdel(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("getdel".to_string()));
    }
    let key = get_bytes_vec(args, 1, "GETDEL")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    match store_db.remove(&key) {
        None => Ok(Resp::null()),
        Some(e) => match e.value {
            DataType::String(v) => Ok(Resp::bulk(Bytes::from(v))),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_getex(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("getex".to_string()));
    }
    let key = get_bytes_vec(args, 1, "GETEX")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    let old = match store_db.get(&key) {
        None => return Ok(Resp::null()),
        Some(e) => match &e.value {
            DataType::String(v) => Bytes::from(v.clone()),
            _ => return Err(NexradeError::WrongType),
        },
    };

    if args.len() > 2 {
        let opt = get_str(args, 2, "GETEX")?.to_uppercase();
        let expiry = match opt.as_str() {
            "EX" => {
                if args.len() != 4 {
                    return Err(NexradeError::SyntaxError);
                }
                let secs = get_i64(args, 3, "GETEX")?;
                if secs <= 0 {
                    return Err(NexradeError::Generic(
                        "invalid expire time in GETEX".to_string(),
                    ));
                }
                Some(Expiry::from_duration(Duration::from_secs(secs as u64)))
            }
            "PX" => {
                if args.len() != 4 {
                    return Err(NexradeError::SyntaxError);
                }
                let ms = get_i64(args, 3, "GETEX")?;
                if ms <= 0 {
                    return Err(NexradeError::Generic(
                        "invalid expire time in GETEX".to_string(),
                    ));
                }
                Some(Expiry::from_duration(Duration::from_millis(ms as u64)))
            }
            "EXAT" => {
                if args.len() != 4 {
                    return Err(NexradeError::SyntaxError);
                }
                let secs = get_i64(args, 3, "GETEX")?;
                if secs < 0 {
                    return Err(NexradeError::Generic(
                        "invalid expire time in GETEX".to_string(),
                    ));
                }
                Some(Expiry::from_secs(secs as u64))
            }
            "PXAT" => {
                if args.len() != 4 {
                    return Err(NexradeError::SyntaxError);
                }
                let ms = get_i64(args, 3, "GETEX")?;
                if ms < 0 {
                    return Err(NexradeError::Generic(
                        "invalid expire time in GETEX".to_string(),
                    ));
                }
                Some(Expiry::from_ms(ms as u64))
            }
            "PERSIST" => {
                if args.len() != 3 {
                    return Err(NexradeError::SyntaxError);
                }
                None
            }
            _ => return Err(NexradeError::SyntaxError),
        };

        store_db.set_expiry(&key, expiry);
    }

    Ok(Resp::bulk(old))
}

pub async fn cmd_mset(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(NexradeError::WrongArity("mset".to_string()));
    }
    let sdb = db.store.db(db_index);
    let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
    let mut i = 1;
    while i + 1 < args.len() {
        let key = get_bytes_vec(args, i, "MSET")?;
        let val = get_bytes_vec(args, i + 1, "MSET")?;
        pairs.push((key, Entry::new(DataType::String(val))));
        i += 2;
    }
    sdb.mset(pairs);
    Ok(Resp::ok())
}

pub async fn cmd_msetnx(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(NexradeError::WrongArity("msetnx".to_string()));
    }
    let sdb = db.store.db(db_index);

    let mut pairs = Vec::new();
    let mut i = 1;
    while i + 1 < args.len() {
        let key = get_bytes_vec(args, i, "MSETNX")?;
        let val = get_bytes_vec(args, i + 1, "MSETNX")?;
        pairs.push((key, Entry::new(DataType::String(val))));
        i += 2;
    }
    let ok = sdb.msetnx(pairs);
    Ok(Resp::int(if ok { 1 } else { 0 }))
}

pub async fn cmd_mget(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("mget".to_string()));
    }
    let sdb = db.store.db(db_index);
    let mut results = Vec::new();
    for i in 1..args.len() {
        let key = get_bytes_vec(args, i, "MGET")?;
        let val = match sdb.read_for(&key).get_ro(&key) {
            None => Resp::null(),
            Some(e) => match &e.value {
                DataType::String(v) => Resp::bulk(Bytes::from(v.clone())),
                _ => Resp::null(),
            },
        };
        results.push(val);
    }
    Ok(Resp::array(results))
}

pub async fn cmd_setnx(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("setnx".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SETNX")?;
    let val = get_bytes_vec(args, 2, "SETNX")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    if store_db.contains_key(&key) {
        return Ok(Resp::int(0));
    }
    store_db.insert(key, Entry::new(DataType::String(val)));
    Ok(Resp::int(1))
}

pub async fn cmd_setex(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("setex".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SETEX")?;
    let secs = get_i64(args, 2, "SETEX")?;
    let val = get_bytes_vec(args, 3, "SETEX")?;

    if secs <= 0 {
        return Err(NexradeError::Generic(
            "invalid expire time in SETEX".to_string(),
        ));
    }

    let mut store_db = db.store.db(db_index).write_for(&key);
    store_db.insert(
        key,
        Entry::with_expiry(
            DataType::String(val),
            Expiry::from_duration(Duration::from_secs(secs as u64)),
        ),
    );
    Ok(Resp::ok())
}

pub async fn cmd_psetex(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("psetex".to_string()));
    }
    let key = get_bytes_vec(args, 1, "PSETEX")?;
    let ms = get_i64(args, 2, "PSETEX")?;
    let val = get_bytes_vec(args, 3, "PSETEX")?;

    if ms <= 0 {
        return Err(NexradeError::Generic(
            "invalid expire time in PSETEX".to_string(),
        ));
    }

    let mut store_db = db.store.db(db_index).write_for(&key);
    store_db.insert(
        key,
        Entry::with_expiry(
            DataType::String(val),
            Expiry::from_duration(Duration::from_millis(ms as u64)),
        ),
    );
    Ok(Resp::ok())
}

pub async fn cmd_incr(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    incr_by(db, args, db_index, 1, "INCR").await
}

pub async fn cmd_decr(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    incr_by(db, args, db_index, -1, "DECR").await
}

pub async fn cmd_incrby(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("incrby".to_string()));
    }
    let delta = get_i64(args, 2, "INCRBY")?;
    incr_by(db, args, db_index, delta, "INCRBY").await
}

pub async fn cmd_decrby(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("decrby".to_string()));
    }
    let delta = get_i64(args, 2, "DECRBY")?;
    incr_by(db, args, db_index, -delta, "DECRBY").await
}

async fn incr_by(db: &Db, args: &[Resp], db_index: usize, delta: i64, cmd: &str) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let key = get_bytes_vec(args, 1, cmd)?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    let current: i64 = match store_db.get(&key) {
        None => 0,
        Some(e) => match &e.value {
            DataType::String(v) => std::str::from_utf8(v)
                .map_err(|_| NexradeError::NotInteger)?
                .parse()
                .map_err(|_| NexradeError::NotInteger)?,
            _ => return Err(NexradeError::WrongType),
        },
    };

    let new_val = current.checked_add(delta).ok_or(NexradeError::Overflow)?;

    store_db.insert(
        key,
        Entry::new(DataType::String(new_val.to_string().into_bytes())),
    );
    Ok(Resp::int(new_val))
}

pub async fn cmd_incrbyfloat(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("incrbyfloat".to_string()));
    }
    let key = get_bytes_vec(args, 1, "INCRBYFLOAT")?;
    let delta = get_f64(args, 2, "INCRBYFLOAT")?;

    let mut store_db = db.store.db(db_index).write_for(&key);

    let current: f64 = match store_db.get(&key) {
        None => 0.0,
        Some(e) => match &e.value {
            DataType::String(v) => std::str::from_utf8(v)
                .map_err(|_| NexradeError::NotFloat)?
                .parse()
                .map_err(|_| NexradeError::NotFloat)?,
            _ => return Err(NexradeError::WrongType),
        },
    };

    let new_val = current + delta;
    if new_val.is_nan() || new_val.is_infinite() {
        return Err(NexradeError::Generic(
            "increment would produce NaN or Infinity".to_string(),
        ));
    }

    // Format like Redis (strip trailing zeros)
    let s = format_float(new_val);
    let resp = Resp::bulk_str(s.clone());
    store_db.insert(key, Entry::new(DataType::String(s.into_bytes())));
    Ok(resp)
}

pub(crate) fn format_float(f: f64) -> String {
    let s = format!("{:.17}", f);
    // Trim trailing zeros after decimal point
    if s.contains('.') {
        let s = s.trim_end_matches('0');
        let s = s.trim_end_matches('.');
        s.to_string()
    } else {
        s
    }
}

pub async fn cmd_append(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("append".to_string()));
    }
    let key = get_bytes_vec(args, 1, "APPEND")?;
    let val = get_bytes_vec(args, 2, "APPEND")?;

    let mut store_db = db.store.db(db_index).write_for(&key);

    match store_db.get_mut(&key) {
        Some(e) => match &mut e.value {
            DataType::String(v) => {
                v.extend_from_slice(&val);
                let len = v.len() as i64;
                Ok(Resp::int(len))
            }
            _ => Err(NexradeError::WrongType),
        },
        None => {
            let len = val.len() as i64;
            store_db.insert(key, Entry::new(DataType::String(val)));
            Ok(Resp::int(len))
        }
    }
}

pub async fn cmd_strlen(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("strlen".to_string()));
    }
    let key = get_bytes_vec(args, 1, "STRLEN")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::String(v) => Ok(Resp::int(v.len() as i64)),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_getrange(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("getrange".to_string()));
    }
    let key = get_bytes_vec(args, 1, "GETRANGE")?;
    let start = get_i64(args, 2, "GETRANGE")?;
    let end = get_i64(args, 3, "GETRANGE")?;

    let store_db = db.store.db(db_index).read_for(&key);

    let bytes = match store_db.get_ro(&key) {
        None => return Ok(Resp::bulk_str("")),
        Some(e) => match &e.value {
            DataType::String(v) => v.clone(),
            _ => return Err(NexradeError::WrongType),
        },
    };

    let len = bytes.len() as isize;
    let start = normalize_range_idx(start as isize, len);
    let end = normalize_range_idx(end as isize, len);

    if start > end || start >= len as usize {
        return Ok(Resp::bulk_str(""));
    }

    let end = end.min(len as usize - 1);
    Ok(Resp::bulk(Bytes::from(bytes[start..=end].to_vec())))
}

pub async fn cmd_setrange(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("setrange".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SETRANGE")?;
    let offset = get_i64(args, 2, "SETRANGE")?;
    let patch = get_bytes_vec(args, 3, "SETRANGE")?;

    if offset < 0 {
        return Err(NexradeError::Generic("offset is out of range".to_string()));
    }
    const MAX_SETRANGE_OFFSET: i64 = 512 * 1024 * 1024 - 1; // 512 MB limit (matches Redis)
    if offset > MAX_SETRANGE_OFFSET {
        return Err(NexradeError::Generic(
            "string exceeds maximum allowed size (512MB)".to_string(),
        ));
    }
    let offset = offset as usize;

    let mut store_db = db.store.db(db_index).write_for(&key);

    let mut bytes = match store_db.get(&key) {
        None => vec![],
        Some(e) => match &e.value {
            DataType::String(v) => v.clone(),
            _ => return Err(NexradeError::WrongType),
        },
    };

    let end = offset + patch.len();
    if end > bytes.len() {
        bytes.resize(end, 0);
    }
    bytes[offset..end].copy_from_slice(&patch);
    let len = bytes.len() as i64;
    store_db.insert(key, Entry::new(DataType::String(bytes)));
    Ok(Resp::int(len))
}

fn normalize_range_idx(idx: isize, len: isize) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx as usize
    }
}
