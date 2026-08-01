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

    // Borrow key bytes — plain SET only needs `&[u8]` for shard + HashMap.
    // Clone into owned Bytes only when the flagged path needs a Bytes key
    // for `write_for`/`insert` (still a refcount bump, not a payload copy).
    let key_bytes = match args.get(1).and_then(|a| a.as_bytes()) {
        Some(b) => b,
        None => return Err(NexradeError::WrongArity("set".to_string())),
    };
    // Compact copy — do NOT refcount-clone the RESP buffer slice.
    // Cloned Bytes from the parser pin the whole pipeline-batch buffer
    // until the key is overwritten, which kills pipelined SET throughput.
    let value = match args.get(2).and_then(|a| a.as_bytes()) {
        Some(b) => Bytes::copy_from_slice(b),
        None => return Err(NexradeError::WrongArity("set".to_string())),
    };

    let mut expiry: Option<Expiry> = None;
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut keepttl = false;
    let mut ifeq: Option<Vec<u8>> = None;
    let mut ifgt: Option<Vec<u8>> = None;
    let mut iflt: Option<Vec<u8>> = None;

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
            "IFEQ" => {
                ifeq = Some(get_bytes_vec(args, i + 1, "SET")?);
                i += 2;
            }
            "IFGT" => {
                ifgt = Some(get_bytes_vec(args, i + 1, "SET")?);
                i += 2;
            }
            "IFLT" => {
                iflt = Some(get_bytes_vec(args, i + 1, "SET")?);
                i += 2;
            }
            _ => {
                return Err(NexradeError::SyntaxError);
            }
        }
    }

    // IFEQ/IFGT/IFLT are mutually exclusive with each other and with NX/XX.
    let cond_count = (ifeq.is_some() as u8) + (ifgt.is_some() as u8) + (iflt.is_some() as u8);
    if cond_count > 1 {
        return Err(NexradeError::SyntaxError);
    }
    if cond_count == 1 && (nx || xx) {
        return Err(NexradeError::SyntaxError);
    }

    // Plain SET k v — no flags. Avoid key Bytes clone + key.to_vec() + full
    // Entry rebuild on the redis-benchmark hot path (existing same-length
    // string overwrite).
    let plain = !nx
        && !xx
        && !get
        && !keepttl
        && expiry.is_none()
        && ifeq.is_none()
        && ifgt.is_none()
        && iflt.is_none();
    if plain {
        let mut store_db = db.store.db(db_index).write_for(key_bytes.as_ref());
        store_db.set_plain(key_bytes.as_ref(), value);
        return Ok(Resp::ok());
    }

    let key = key_bytes.clone();
    let mut store_db = db.store.db(db_index).write_for(&key);

    // GET option: return old value before SET
    let old_value = if get {
        match store_db.get(&key) {
            Some(e) => match e.value.as_string_bytes() {
                Some(v) => Some(Resp::bulk(v)),
                None => return Err(NexradeError::WrongType),
            },
            None => Some(Resp::null()),
        }
    } else {
        None
    };

    // Only pay for the existence lookup when NX/XX actually need it — the
    // common `SET k v` path (no flags) skips straight to IFEQ/KEEPTTL/insert,
    // saving one HashMap probe per SET.
    if nx || xx {
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
    }

    // IFEQ / IFGT / IFLT: compare against the current value. If the
    // comparison fails (including when the key doesn't exist), the SET is
    // a no-op and we return nil. With GET the old value is still returned
    // (Redis 7.4 behaviour).
    if ifeq.is_some() || ifgt.is_some() || iflt.is_some() {
        let passes = match store_db.get(&key) {
            Some(e) => {
                let cur = match e.value.as_string_bytes() {
                    Some(v) => v,
                    None => return Err(NexradeError::WrongType),
                };
                let cur: &[u8] = &cur;
                if let Some(target) = &ifeq {
                    cur == target.as_slice()
                } else if let Some(target) = &ifgt {
                    compare_gt(cur, target)
                } else if let Some(target) = &iflt {
                    compare_lt(cur, target)
                } else {
                    false
                }
            }
            None => false,
        };
        if !passes {
            return Ok(if get {
                old_value.unwrap()
            } else {
                Resp::null()
            });
        }
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
    // Convert the `Bytes` key into an owned `Vec<u8>` only at insertion,
    // since `HashMap::insert` requires owned keys. The earlier lookup
    // and shard-acquire paths used the cheaper `Bytes::clone`.
    store_db.insert(key.to_vec(), entry);

    Ok(if get { old_value.unwrap() } else { Resp::ok() })
}

/// Compare two byte strings. If both parse as f64, compare numerically;
/// otherwise fall back to lex comparison.
fn compare_gt(a: &[u8], b: &[u8]) -> bool {
    if let (Some(an), Some(bn)) = (
        std::str::from_utf8(a)
            .ok()
            .and_then(|s| s.parse::<f64>().ok()),
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<f64>().ok()),
    ) {
        an > bn
    } else {
        a > b
    }
}

fn compare_lt(a: &[u8], b: &[u8]) -> bool {
    if let (Some(an), Some(bn)) = (
        std::str::from_utf8(a)
            .ok()
            .and_then(|s| s.parse::<f64>().ok()),
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<f64>().ok()),
    ) {
        an < bn
    } else {
        a < b
    }
}

pub async fn cmd_get(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("get".to_string()));
    }
    let key = match args.get(1).and_then(|a| a.as_bytes()) {
        Some(b) => b,
        None => return Err(NexradeError::WrongArity("get".to_string())),
    };
    let store_db = db.store.db(db_index).read_for(key);

    match store_db.get_ro(key) {
        None => Ok(Resp::null()),
        Some(e) => match e.value.as_string_bytes() {
            Some(v) => Ok(Resp::bulk(v)),
            None => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_getset(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("getset".to_string()));
    }
    let key = get_bytes_vec(args, 1, "GETSET")?;
    let value = Bytes::copy_from_slice(&crate::command::get_bytes(args, 2, "GETSET")?);

    let mut store_db = db.store.db(db_index).write_for(&key);
    let old = match store_db.get(&key) {
        None => Resp::null(),
        Some(e) => match e.value.as_string_bytes() {
            Some(v) => Resp::bulk(v),
            None => return Err(NexradeError::WrongType),
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
        Some(e) => match e.value.as_string_bytes() {
            Some(v) => Ok(Resp::bulk(v)),
            None => Err(NexradeError::WrongType),
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
        Some(e) => match e.value.as_string_bytes() {
            Some(v) => v,
            None => return Err(NexradeError::WrongType),
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
        // Compact copies (same as SET) — avoid pinning the parse buffer and
        // avoid Vec→Bytes double allocation via get_bytes_vec + Bytes::from.
        let key = match args.get(i).and_then(|a| a.as_bytes()) {
            Some(b) => b.to_vec(),
            None => return Err(NexradeError::WrongArity("mset".to_string())),
        };
        let val = match args.get(i + 1).and_then(|a| a.as_bytes()) {
            Some(b) => Bytes::copy_from_slice(b),
            None => return Err(NexradeError::WrongArity("mset".to_string())),
        };
        pairs.push((key, Entry::new(DataType::String(val))));
        i += 2;
    }
    sdb.mset_async(pairs).await;
    Ok(Resp::ok())
}

pub async fn cmd_msetnx(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(NexradeError::WrongArity("msetnx".to_string()));
    }
    let sdb = db.store.db(db_index);

    let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
    let mut i = 1;
    while i + 1 < args.len() {
        let key = match args.get(i).and_then(|a| a.as_bytes()) {
            Some(b) => b.to_vec(),
            None => return Err(NexradeError::WrongArity("msetnx".to_string())),
        };
        let val = match args.get(i + 1).and_then(|a| a.as_bytes()) {
            Some(b) => Bytes::copy_from_slice(b),
            None => return Err(NexradeError::WrongArity("msetnx".to_string())),
        };
        pairs.push((key, Entry::new(DataType::String(val))));
        i += 2;
    }
    let ok = sdb.msetnx_async(pairs).await;
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
            Some(e) => match e.value.as_string_bytes() {
                Some(v) => Resp::bulk(v),
                None => Resp::null(),
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
    store_db.insert(key, Entry::new(DataType::String(Bytes::from(val))));
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
            DataType::String(Bytes::from(val)),
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
            DataType::String(Bytes::from(val)),
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

    // Lock acquisition is now the dispatcher's call, not ours: on an
    // already-promoted, non-expired `Int` key it takes only a shared read
    // lock and mutates the cell via CAS; otherwise it falls back to the
    // exclusive write-lock slow path (creation, promotion, expiry) — see
    // `ShardedDatabase::incr_int`. Either way, TTL is preserved (the fast
    // path never touches the `Entry` at all; the slow path mutates it in
    // place, same as before).
    let new_val = db.store.db(db_index).incr_int(&key, delta)?;
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
        Some(e) => match e.value.as_string_bytes() {
            Some(v) => std::str::from_utf8(&v)
                .map_err(|_| NexradeError::NotFloat)?
                .parse()
                .map_err(|_| NexradeError::NotFloat)?,
            None => return Err(NexradeError::WrongType),
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
    store_db.insert(
        key,
        Entry::new(DataType::String(Bytes::from(s.into_bytes()))),
    );
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

    // Vacant path: insert accounts for full entry (overhead + key + value).
    // Existing key: in-place mut — adjust live_bytes by payload growth only.
    let (resp, delta) = match store_db.get_mut(&key) {
        Some(e) => match &mut e.value {
            DataType::String(v) => {
                let old_len = v.len();
                let mut owned = v.to_vec();
                owned.extend_from_slice(&val);
                let len = owned.len() as i64;
                let d = (owned.len() as isize) - (old_len as isize);
                e.value = DataType::String(Bytes::from(owned));
                (Ok(Resp::int(len)), d)
            }
            // APPEND always demotes an int-encoded key to a raw String —
            // real Redis does the same (int encoding can't be appended to
            // in place). Build the concatenated bytes and replace the entry.
            // Int payload is fixed at 8 bytes in estimate_entry_size.
            DataType::Int(cell) => {
                let mut buf = itoa::Buffer::new();
                let mut owned = buf.format(cell.load()).as_bytes().to_vec();
                owned.extend_from_slice(&val);
                let len = owned.len() as i64;
                let d = (owned.len() as isize) - 8;
                e.value = DataType::String(Bytes::from(owned));
                (Ok(Resp::int(len)), d)
            }
            _ => (Err(NexradeError::WrongType), 0),
        },
        None => {
            let len = val.len() as i64;
            store_db.insert(key, Entry::new(DataType::String(Bytes::from(val))));
            (Ok(Resp::int(len)), 0)
        }
    };
    store_db.adjust_live_bytes(delta);
    resp
}

pub async fn cmd_strlen(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("strlen".to_string()));
    }
    let key = get_bytes_vec(args, 1, "STRLEN")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match e.value.as_string_bytes() {
            Some(v) => Ok(Resp::int(v.len() as i64)),
            None => Err(NexradeError::WrongType),
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
        Some(e) => match e.value.as_string_bytes() {
            Some(v) => v,
            None => return Err(NexradeError::WrongType),
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
        Some(e) => match e.value.as_string_bytes() {
            Some(v) => v.to_vec(),
            None => return Err(NexradeError::WrongType),
        },
    };

    let end = offset + patch.len();
    if end > bytes.len() {
        bytes.resize(end, 0);
    }
    bytes[offset..end].copy_from_slice(&patch);
    let len = bytes.len() as i64;
    store_db.insert(key, Entry::new(DataType::String(Bytes::from(bytes))));
    Ok(Resp::int(len))
}

fn normalize_range_idx(idx: isize, len: isize) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx as usize
    }
}

/// `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]`
///
/// Longest Common Subsequence of two string values (Redis 7). Binary-safe
/// over the raw bytes of each key. Missing keys are treated as empty strings.
pub async fn cmd_lcs(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("lcs".to_string()));
    }
    let key1 = get_bytes_vec(args, 1, "LCS")?;
    let key2 = get_bytes_vec(args, 2, "LCS")?;

    let mut want_len = false;
    let mut want_idx = false;
    let mut min_match_len: usize = 0;
    let mut with_match_len = false;
    let mut i = 3;
    while i < args.len() {
        let opt = get_str(args, i, "LCS")?.to_uppercase();
        match opt.as_str() {
            "LEN" => {
                want_len = true;
                i += 1;
            }
            "IDX" => {
                want_idx = true;
                i += 1;
            }
            "MINMATCHLEN" => {
                let n = get_i64(args, i + 1, "LCS")?;
                if n < 0 {
                    return Err(NexradeError::Generic(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
                min_match_len = n as usize;
                i += 2;
            }
            "WITHMATCHLEN" => {
                with_match_len = true;
                i += 1;
            }
            _ => return Err(NexradeError::SyntaxError),
        }
    }

    // Load both values (treat missing as empty; WrongType if non-string).
    let sdb = db.store.db(db_index);
    let a = {
        let g = sdb.read_for(&key1);
        match g.get_ro(&key1) {
            None => Vec::new(),
            Some(e) => e
                .value
                .as_string_bytes()
                .map(|b| b.to_vec())
                .ok_or(NexradeError::WrongType)?,
        }
    };
    let b = {
        let g = sdb.read_for(&key2);
        match g.get_ro(&key2) {
            None => Vec::new(),
            Some(e) => e
                .value
                .as_string_bytes()
                .map(|b| b.to_vec())
                .ok_or(NexradeError::WrongType)?,
        }
    };

    let (lcs_bytes, matches) = compute_lcs(&a, &b);

    if want_len && !want_idx {
        return Ok(Resp::int(lcs_bytes.len() as i64));
    }

    if want_idx {
        // Filter matches by MINMATCHLEN, optionally attach length.
        let mut match_items: Vec<Resp> = Vec::new();
        for (a_start, a_end, b_start, b_end) in matches.into_iter().rev() {
            // Inclusive end indices (Redis reports inclusive ranges).
            let len = a_end - a_start;
            if len < min_match_len {
                continue;
            }
            let mut row = vec![
                Resp::array(vec![
                    Resp::int(a_start as i64),
                    Resp::int((a_end - 1) as i64),
                ]),
                Resp::array(vec![
                    Resp::int(b_start as i64),
                    Resp::int((b_end - 1) as i64),
                ]),
            ];
            if with_match_len {
                row.push(Resp::int(len as i64));
            }
            match_items.push(Resp::array(row));
        }
        return Ok(Resp::array(vec![
            Resp::bulk_str("matches"),
            Resp::array(match_items),
            Resp::bulk_str("len"),
            Resp::int(lcs_bytes.len() as i64),
        ]));
    }

    // Default: return the LCS bulk string.
    Ok(Resp::bulk(Bytes::from(lcs_bytes)))
}

/// Contiguous LCS match range: (a_start, a_end_excl, b_start, b_end_excl).
type LcsMatch = (usize, usize, usize, usize);

/// Compute LCS of two byte strings.
/// Returns (lcs_bytes, contiguous match ranges).
fn compute_lcs(a: &[u8], b: &[u8]) -> (Vec<u8>, Vec<LcsMatch>) {
    let n = a.len();
    let m = b.len();
    // DP table of (n+1) × (m+1). For large strings this is O(n*m) memory —
    // Redis has the same bound; keep it simple for the compatibility surface.
    let mut dp = vec![vec![0usize; m + 1]; n + 1];
    for i in 1..=n {
        for j in 1..=m {
            if a[i - 1] == b[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }

    // Backtrack to recover LCS bytes and per-character alignments.
    let mut i = n;
    let mut j = m;
    let mut pairs: Vec<(usize, usize)> = Vec::new(); // (a_idx, b_idx)
    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            pairs.push((i - 1, j - 1));
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] >= dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    pairs.reverse();

    let lcs_bytes: Vec<u8> = pairs.iter().map(|&(ai, _)| a[ai]).collect();

    // Collapse consecutive alignments into contiguous match ranges.
    let mut matches: Vec<LcsMatch> = Vec::new();
    if !pairs.is_empty() {
        let (mut a0, mut b0) = pairs[0];
        let mut a1 = a0 + 1;
        let mut b1 = b0 + 1;
        for &(ai, bi) in pairs.iter().skip(1) {
            if ai == a1 && bi == b1 {
                a1 += 1;
                b1 += 1;
            } else {
                matches.push((a0, a1, b0, b1));
                a0 = ai;
                b0 = bi;
                a1 = ai + 1;
                b1 = bi + 1;
            }
        }
        matches.push((a0, a1, b0, b1));
    }

    (lcs_bytes, matches)
}
