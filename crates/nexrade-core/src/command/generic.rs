//! Generic key command handlers.

use bytes::Bytes;

use crate::command::{decode_scan_cursor, get_bytes_vec, get_i64, get_str, scan_order_key};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::expiry::Expiry;
use crate::resp::Resp;

/// Current unix time in milliseconds. Mirrors `expiry::system_now_ms` (which
/// is private to that module) so the EXPIRE family can resolve relative
/// timeouts and detect past deadlines without a lossy cast.
#[cfg(not(target_arch = "wasm32"))]
fn now_unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(target_arch = "wasm32")]
fn now_unix_ms() -> u128 {
    0
}

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

    // Resolve to an absolute unix-millisecond deadline, kept signed so a
    // negative relative timeout or a past absolute timestamp stays negative
    // (a lossy `as u64` cast would wrap it into the far future). Redis does
    // NOT reject a non-positive timeout or a past timestamp: it deletes the
    // key instead (see the past-deadline handling below), so there is no
    // early "invalid expire time" error here.
    let unit_ms: i128 = if millis { 1 } else { 1000 };
    let target_ms: i128 = if absolute {
        (val as i128) * unit_ms
    } else {
        (now_unix_ms() as i128) + (val as i128) * unit_ms
    };

    // Parse conditional options: NX | XX | GT | LT (Redis 7.0+).
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut i = 3;
    while i < args.len() {
        let opt = get_str(args, i, cmd)?.to_uppercase();
        match opt.as_str() {
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GT" => {
                gt = true;
                i += 1;
            }
            "LT" => {
                lt = true;
                i += 1;
            }
            _ => {
                return Err(NexradeError::SyntaxError);
            }
        }
    }
    let cond_count = (nx as u8) + (xx as u8) + (gt as u8) + (lt as u8);
    if cond_count > 1 {
        return Err(NexradeError::SyntaxError);
    }

    let mut store_db = db.store.db(db_index).write_for(&key);

    // Key must exist for any expire command to apply. Return 0 immediately
    // if not, regardless of NX/XX/GT/LT flags.
    let entry = match store_db.get(&key) {
        None => return Ok(Resp::int(0)),
        Some(e) => e,
    };

    // Current expiry as a signed ms deadline; `None` means the key is
    // persistent, which GT/LT must treat as an *infinite* TTL (Redis docs:
    // "A non-volatile key is treated as an infinite TTL for the purpose of
    // GT/LT").
    let current_expiry_ms: Option<i128> = entry
        .expiry
        .as_ref()
        .map(|e| e.expires_at_ms.min(i128::MAX as u128) as i128);

    // Evaluate conditional flags against the new target deadline.
    if nx && current_expiry_ms.is_some() {
        return Ok(Resp::int(0));
    }
    if xx && current_expiry_ms.is_none() {
        return Ok(Resp::int(0));
    }
    if gt {
        match current_expiry_ms {
            // Persistent key = infinite TTL. A finite new expiry is never
            // greater than infinity, so GT never applies here.
            None => return Ok(Resp::int(0)),
            Some(cur) => {
                if target_ms <= cur {
                    return Ok(Resp::int(0));
                }
            }
        }
    }
    if lt {
        match current_expiry_ms {
            // Persistent key = infinite TTL. Any finite new expiry is less
            // than infinity, so LT always applies here.
            None => {}
            Some(cur) => {
                if target_ms >= cur {
                    return Ok(Resp::int(0));
                }
            }
        }
    }

    // A resolved deadline at or before now means "delete immediately" — a
    // non-positive relative timeout or an absolute timestamp in the past.
    // Redis deletes the key (emitting a `del`, not `expired`) and still
    // returns 1. Only reached once the NX/XX/GT/LT condition has passed.
    if target_ms <= now_unix_ms() as i128 {
        store_db.remove(&key);
        return Ok(Resp::int(1));
    }

    // Clamp the (now strictly-positive) deadline back into the unsigned
    // millisecond domain the store uses.
    let expiry = Expiry::from_ms(target_ms.min(u64::MAX as i128) as u64);
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
    // Cursor is an opaque decimal u64 (see `scan_cursor_token`). Parsed
    // via `get_str` + `decode_scan_cursor` rather than `get_i64` because
    // the tokens we emit routinely exceed `i64::MAX` — the previous
    // signed parse overflowed on ~35% of hashes and silently restarted
    // the scan from page 1.
    let cursor: Option<u64> = get_str(args, 1, "SCAN").ok().and_then(decode_scan_cursor);

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
                    return Err(NexradeError::Generic("syntax error".to_string()));
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
    // Page directly out of the store rather than materialising the keyspace.
    //
    // The cursor is a *key-space* position, not an index into a list. It used
    // to be a plain offset, which silently broke the SCAN guarantee ("every
    // key present for the whole iteration is returned at least once") whenever
    // keys were deleted mid-iteration — the canonical `scan_iter` +
    // pipelined-delete cleanup pattern. Deleting the first page shrank the
    // list under the cursor, every later key shifted down into the slots
    // already passed, and the next round started beyond the new end: it
    // returned nothing, reported cursor 0 ("done"), and half the keyspace was
    // never visited. Seeding 40 keys and deleting each page returned 20 of 40.
    //
    // Anchoring the cursor to the boundary *element* means deletions cannot
    // shift the resume point. The cursor stays a decimal integer on the wire
    // (client libraries call `int()` on it) and is the `scan_order_key` of the
    // last key handed out; `scan_page` resumes at the first key ordered after
    // it. See `scan_order_key` for why that ordering is bit-reversed.
    //
    // The previous implementation called `keys_matching()` — write-locking all
    // 64-256 shards and sorting the whole keyspace — on every call, which made
    // a full traversal O(n^2). `scan_page` serves a page from a single shard
    // under a read lock.
    let pat = pattern.unwrap_or_else(|| b"*".to_vec());
    let (keys, next) = sdb.scan_page(
        cursor.unwrap_or(0),
        count,
        &pat,
        scan_order_key,
        // TYPE is checked inside the shard guard, so it costs no extra locks.
        // The old code took one `read_for` per surviving key.
        |_, entry| match type_filter {
            Some(ref t) => entry.value.type_name() == t.as_str(),
            None => true,
        },
    );

    let page: Vec<Resp> = keys
        .into_iter()
        .map(|k| Resp::bulk(Bytes::from(k)))
        .collect();
    Ok(Resp::array(vec![
        Resp::bulk_str(next.to_string()),
        Resp::array(page),
    ]))
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

/// Magic + version for nexrade-native DUMP payloads.
/// Not Redis RDB-compatible — clients must treat this as opaque and only
/// RESTORE it into nexrade-cache (or another process that understands `NEXD`).
const DUMP_MAGIC: &[u8; 4] = b"NEXD";
const DUMP_VERSION: u8 = 1;

/// `DUMP key` — serialize the entry as a custom nexrade payload.
/// Returns null when the key is missing (Redis-compatible).
pub async fn cmd_dump(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("dump".to_string()));
    }
    let key = get_bytes_vec(args, 1, "DUMP")?;
    let store_db = db.store.db(db_index).read_for(&key);
    let entry = match store_db.get_ro(&key) {
        None => return Ok(Resp::null()),
        Some(e) => e,
    };
    // Snapshot so we don't hold the shard lock across bincode.
    let snapshot = entry.clone();
    drop(store_db);

    let body = bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
        .map_err(|e| NexradeError::Generic(format!("DUMP serialize failed: {e}")))?;
    let mut out = Vec::with_capacity(5 + body.len());
    out.extend_from_slice(DUMP_MAGIC);
    out.push(DUMP_VERSION);
    out.extend_from_slice(&body);
    Ok(Resp::bulk(Bytes::from(out)))
}

/// `RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ freq]`
///
/// `ttl` is relative milliseconds (0 = no expiry), unless `ABSTTL` is set, in
/// which case it is an absolute unix-ms deadline. Only nexrade `NEXD` payloads
/// produced by `DUMP` are accepted — Redis RDB dumps are rejected with a clear
/// error so clients don't silently get a no-op.
pub async fn cmd_restore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("restore".to_string()));
    }
    let key = get_bytes_vec(args, 1, "RESTORE")?;
    let ttl_ms = get_i64(args, 2, "RESTORE")?;
    let payload = match &args[3] {
        Resp::BulkString(Some(b)) => b.as_ref(),
        _ => {
            return Err(NexradeError::Generic(
                "ERR RESTORE payload must be a bulk string".to_string(),
            ))
        }
    };

    let mut replace = false;
    let mut absttl = false;
    let mut i = 4;
    while i < args.len() {
        let opt = get_str(args, i, "RESTORE")?.to_uppercase();
        match opt.as_str() {
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            "ABSTTL" => {
                absttl = true;
                i += 1;
            }
            // IDLETIME / FREQ are accepted and ignored (Redis accepts them for
            // LFU/LRU bookkeeping we don't persist in the DUMP payload).
            "IDLETIME" | "FREQ" => {
                i += 2;
            }
            _ => {
                return Err(NexradeError::Generic("syntax error".to_string()));
            }
        }
    }

    if payload.len() < 5 || &payload[..4] != DUMP_MAGIC {
        return Err(NexradeError::Generic(
            "ERR DUMP payload version or checksum are wrong — \
             nexrade DUMP uses a custom NEXD format, not Redis RDB"
                .to_string(),
        ));
    }
    if payload[4] != DUMP_VERSION {
        return Err(NexradeError::Generic(format!(
            "ERR unsupported DUMP version {}",
            payload[4]
        )));
    }
    let (mut entry, _): (crate::store::Entry, usize) =
        bincode::serde::decode_from_slice(&payload[5..], bincode::config::standard())
            .map_err(|e| NexradeError::Generic(format!("DUMP payload is corrupt: {e}")))?;

    // Apply the TTL argument, overriding whatever expiry was in the payload.
    if ttl_ms < 0 {
        return Err(NexradeError::Generic(
            "ERR Invalid TTL value, must be >= 0".to_string(),
        ));
    }
    if ttl_ms == 0 && !absttl {
        entry.expiry = None;
    } else {
        let deadline_ms = if absttl {
            ttl_ms as u128
        } else {
            now_unix_ms().saturating_add(ttl_ms as u128)
        };
        // Past absolute deadline → key would already be expired; refuse rather
        // than insert a ghost that lazy-expire would drop on first access.
        if deadline_ms <= now_unix_ms() {
            return Err(NexradeError::Generic(
                "ERR Invalid TTL value, must be >= 0".to_string(),
            ));
        }
        entry.expiry = Some(Expiry::from_ms(deadline_ms as u64));
    }

    let mut store_db = db.store.db(db_index).write_for(&key);
    if !replace && store_db.get(&key).is_some() {
        return Err(NexradeError::Generic(
            "BUSYKEY Target key name already exists.".to_string(),
        ));
    }
    store_db.insert(key, entry);
    Ok(Resp::ok())
}

pub async fn cmd_sort(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    sort_inner(db, args, db_index, true).await
}

/// `SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET ...]]
///             [ASC | DESC] [ALPHA]`
///
/// Read-only variant — same options as SORT except STORE is not allowed.
/// Mirrors Redis 7.4 semantics.
pub async fn cmd_sort_ro(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args
        .iter()
        .any(|a| a.as_str().is_some_and(|s| s.eq_ignore_ascii_case("STORE")))
    {
        return Err(NexradeError::Generic(
            "ERR SORT_RO does not support STORE".to_string(),
        ));
    }
    sort_inner(db, args, db_index, false).await
}

async fn sort_inner(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    take_write_lock: bool,
) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("sort".to_string()));
    }
    let key = get_bytes_vec(args, 1, "SORT")?;

    // Acquire either a read or write lock depending on the variant. We drop
    // the guard before sorting to avoid holding the shard lock across the
    // (potentially expensive) sort. The lock is only needed for type-check
    // and copy.
    enum ListOrSet {
        List(Vec<Vec<u8>>),
        Set(Vec<Vec<u8>>),
    }
    let items: ListOrSet = if take_write_lock {
        let mut store_db = db.store.db(db_index).write_for(&key);
        match store_db.get(&key) {
            None => return Ok(Resp::array(vec![])),
            Some(e) => match &e.value {
                crate::types::DataType::List(l) => {
                    ListOrSet::List(l.to_vec_bytes().into_iter().map(|v| v.to_vec()).collect())
                }
                crate::types::DataType::Set(s) => ListOrSet::Set(s.to_vec()),
                _ => return Err(NexradeError::WrongType),
            },
        }
    } else {
        let store_db = db.store.db(db_index).read_for(&key);
        match store_db.get_ro(&key) {
            None => return Ok(Resp::array(vec![])),
            Some(e) => match &e.value {
                crate::types::DataType::List(l) => {
                    ListOrSet::List(l.to_vec_bytes().into_iter().map(|v| v.to_vec()).collect())
                }
                crate::types::DataType::Set(s) => ListOrSet::Set(s.to_vec()),
                _ => return Err(NexradeError::WrongType),
            },
        }
    };

    // Pull the BY / LIMIT / GET / ASC|DESC / ALPHA / STORE options.
    let mut alpha = false;
    let mut desc = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut by_pattern: Option<Vec<u8>> = None;
    let mut get_patterns: Vec<Vec<u8>> = Vec::new();
    let mut store_dst: Option<Vec<u8>> = None;
    let mut i = 2;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "ASC" => {
                desc = false;
                i += 1;
            }
            "DESC" => {
                desc = true;
                i += 1;
            }
            "ALPHA" => {
                alpha = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(NexradeError::Generic("syntax error".to_string()));
                }
                let off = get_i64(args, i + 1, "SORT")?;
                let cnt = get_i64(args, i + 2, "SORT")?;
                if off < 0 || cnt < 0 {
                    return Err(NexradeError::Generic("syntax error".to_string()));
                }
                limit_offset = off as usize;
                limit_count = Some(cnt as usize);
                i += 3;
            }
            "BY" => {
                if i + 1 >= args.len() {
                    return Err(NexradeError::Generic("syntax error".to_string()));
                }
                by_pattern = Some(get_bytes_vec(args, i + 1, "SORT")?);
                i += 2;
            }
            "GET" => {
                if i + 1 >= args.len() {
                    return Err(NexradeError::Generic("syntax error".to_string()));
                }
                get_patterns.push(get_bytes_vec(args, i + 1, "SORT")?);
                i += 2;
            }
            "STORE" => {
                if !take_write_lock {
                    // SORT_RO already rejected STORE above, but be defensive.
                    return Err(NexradeError::Generic(
                        "ERR SORT_RO does not support STORE".to_string(),
                    ));
                }
                if i + 1 >= args.len() {
                    return Err(NexradeError::Generic("syntax error".to_string()));
                }
                store_dst = Some(get_bytes_vec(args, i + 1, "SORT")?);
                i += 2;
            }
            _ => {
                return Err(NexradeError::Generic("syntax error".to_string()));
            }
        }
    }

    let mut items: Vec<Vec<u8>> = match items {
        ListOrSet::List(v) | ListOrSet::Set(v) => v,
    };

    // Resolve sort weights. Without BY, the element itself is the weight.
    // With BY pattern (e.g. "weight_*"), substitute '*' with the element
    // and look up that key's string value.
    let sdb = db.store.db(db_index);
    let lookup_string = |k: &[u8]| -> Option<Vec<u8>> {
        sdb.read_for(k)
            .get_ro(k)
            .and_then(|e| e.value.as_string_bytes().map(|b| b.to_vec()))
    };
    let expand_pattern = |pat: &[u8], elem: &[u8]| -> Vec<u8> {
        // Redis: first '*' in the pattern is replaced by the element.
        if let Some(pos) = pat.iter().position(|&b| b == b'*') {
            let mut out = Vec::with_capacity(pat.len() + elem.len());
            out.extend_from_slice(&pat[..pos]);
            out.extend_from_slice(elem);
            out.extend_from_slice(&pat[pos + 1..]);
            out
        } else {
            // No '*' → constant key for every element (Redis: don't sort).
            pat.to_vec()
        }
    };

    let nosort = by_pattern
        .as_ref()
        .is_some_and(|p| p.eq_ignore_ascii_case(b"nosort"));

    if !nosort {
        items.sort_by(|a, b| {
            let weight = |elem: &[u8]| -> (Option<f64>, Vec<u8>) {
                let raw = if let Some(ref pat) = by_pattern {
                    lookup_string(&expand_pattern(pat, elem)).unwrap_or_default()
                } else {
                    elem.to_vec()
                };
                if alpha {
                    (None, raw)
                } else {
                    let n = std::str::from_utf8(&raw)
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok());
                    (n, raw)
                }
            };
            let (an, ar) = weight(a);
            let (bn, br) = weight(b);
            let ord = if alpha {
                ar.cmp(&br)
            } else {
                match (an, bn) {
                    (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => ar.cmp(&br),
                }
            };
            if desc {
                ord.reverse()
            } else {
                ord
            }
        });
    } else if desc {
        items.reverse();
    }

    // LIMIT
    let end = match limit_count {
        Some(c) => (limit_offset + c).min(items.len()),
        None => items.len(),
    };
    let start = limit_offset.min(items.len());
    let page = if start >= end {
        Vec::new()
    } else {
        items[start..end].to_vec()
    };

    // Build reply rows: plain elements, or GET-expanded values.
    let mut rows: Vec<Resp> = Vec::new();
    if get_patterns.is_empty() {
        for elem in &page {
            rows.push(Resp::bulk(Bytes::from(elem.clone())));
        }
    } else {
        for elem in &page {
            for pat in &get_patterns {
                if pat == b"#" {
                    rows.push(Resp::bulk(Bytes::from(elem.clone())));
                } else {
                    let k = expand_pattern(pat, elem);
                    match lookup_string(&k) {
                        Some(v) => rows.push(Resp::bulk(Bytes::from(v))),
                        None => rows.push(Resp::null()),
                    }
                }
            }
        }
    }

    // STORE → write as a list and return the element count.
    if let Some(dst) = store_dst {
        use crate::list_data::ListData;
        use crate::store::Entry;
        use crate::types::DataType;
        let mut list = ListData::new();
        // When GET is used, Redis stores the GET results; otherwise the
        // sorted elements themselves.
        if get_patterns.is_empty() {
            for elem in page {
                list.push_back(Bytes::from(elem));
            }
        } else {
            for r in &rows {
                if let Resp::BulkString(Some(b)) = r {
                    list.push_back(b.clone());
                } else {
                    // Null GET results are stored as empty strings (Redis).
                    list.push_back(Bytes::new());
                }
            }
        }
        let n = list.len() as i64;
        let mut shard = db.store.db(db_index).write_for(&dst);
        if list.is_empty() {
            shard.remove(&dst);
            Ok(Resp::int(0))
        } else {
            shard.insert(dst, Entry::new(DataType::List(list)));
            Ok(Resp::int(n))
        }
    } else {
        Ok(Resp::array(rows))
    }
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
