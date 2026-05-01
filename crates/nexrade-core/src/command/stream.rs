//! Stream command handlers — XADD, XREAD, XRANGE, XREVRANGE, XTRIM, XDEL,
//! XLEN, XGROUP, XREADGROUP, XACK, XPENDING.

use bytes::Bytes;

use crate::command::{get_bytes_vec, get_i64, get_str};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::{
    now_ms, Consumer, ConsumerGroup, DataType, PendingEntry, StreamData, StreamEntry,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn next_stream_id() -> String {
    format!("{}-0", now_ms())
}

fn format_stream_entry(entry: &StreamEntry) -> Resp {
    let mut fields = Vec::new();
    for (k, v) in &entry.fields {
        fields.push(Resp::bulk(Bytes::from(k.clone())));
        fields.push(Resp::bulk(Bytes::from(v.clone())));
    }
    Resp::array(vec![Resp::bulk_str(entry.id.clone()), Resp::array(fields)])
}

/// Get or insert a default StreamData entry.
macro_rules! get_stream_mut {
    ($store_db:expr, $key:expr) => {{
        if $store_db.get(&$key[..]).is_none() {
            $store_db.insert(
                $key.clone(),
                Entry::new(DataType::Stream(StreamData::new())),
            );
        }
        match $store_db.get_mut(&$key[..]) {
            Some(e) => match &mut e.value {
                DataType::Stream(s) => s,
                _ => return Err(NexradeError::WrongType),
            },
            None => unreachable!(),
        }
    }};
}

// ── XADD ─────────────────────────────────────────────────────────────────────

pub async fn cmd_xadd(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 5 {
        return Err(NexradeError::WrongArity("xadd".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XADD")?;
    let id_str = get_str(args, 2, "XADD")?;
    let id = if id_str == "*" {
        next_stream_id()
    } else {
        id_str.to_string()
    };

    let mut fields = Vec::new();
    let mut i = 3;
    while i + 1 < args.len() {
        fields.push((
            get_bytes_vec(args, i, "XADD")?,
            get_bytes_vec(args, i + 1, "XADD")?,
        ));
        i += 2;
    }
    let entry = StreamEntry {
        id: id.clone(),
        fields,
    };

    let mut store_db = db.store.db(db_index).write_for(&key);
    let stream = get_stream_mut!(store_db, key);
    stream.entries.push(entry);
    Ok(Resp::bulk_str(id))
}

// ── XLEN ─────────────────────────────────────────────────────────────────────

pub async fn cmd_xlen(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("xlen".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XLEN")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::Stream(s) => Ok(Resp::int(s.entries.len() as i64)),
            _ => Err(NexradeError::WrongType),
        },
    }
}

// ── XRANGE ───────────────────────────────────────────────────────────────────

pub async fn cmd_xrange(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("xrange".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XRANGE")?;
    let start = get_str(args, 2, "XRANGE")?;
    let end = get_str(args, 3, "XRANGE")?;
    let count = if args.len() >= 6 && get_str(args, 4, "XRANGE")?.to_uppercase() == "COUNT" {
        let n = get_i64(args, 5, "XRANGE")?;
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
    match store_db.get(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::Stream(s) => {
                let entries: Vec<Resp> = s
                    .entries
                    .iter()
                    .filter(|e| {
                        (start == "-" || e.id.as_str() >= start)
                            && (end == "+" || e.id.as_str() <= end)
                    })
                    .take(count.unwrap_or(usize::MAX))
                    .map(format_stream_entry)
                    .collect();
                Ok(Resp::array(entries))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

// ── XREVRANGE ────────────────────────────────────────────────────────────────

pub async fn cmd_xrevrange(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("xrevrange".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XREVRANGE")?;
    let end = get_str(args, 2, "XREVRANGE")?;
    let start = get_str(args, 3, "XREVRANGE")?;
    let count = if args.len() >= 6 && get_str(args, 4, "XREVRANGE")?.to_uppercase() == "COUNT" {
        let n = get_i64(args, 5, "XREVRANGE")?;
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
    match store_db.get(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::Stream(s) => {
                let mut entries: Vec<Resp> = s
                    .entries
                    .iter()
                    .filter(|e| {
                        (start == "-" || e.id.as_str() >= start)
                            && (end == "+" || e.id.as_str() <= end)
                    })
                    .take(count.unwrap_or(usize::MAX))
                    .map(format_stream_entry)
                    .collect();
                entries.reverse();
                Ok(Resp::array(entries))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

// ── XREAD ────────────────────────────────────────────────────────────────────

pub async fn cmd_xread(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("xread".to_string()));
    }
    let mut i = 1;
    let mut count: Option<usize> = None;

    while i < args.len() {
        let opt = get_str(args, i, "XREAD")?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                let n = get_i64(args, i + 1, "XREAD")?;
                if n < 0 {
                    return Err(NexradeError::Generic(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
                count = Some(n as usize);
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => {
                i += 1;
            }
        }
    }

    let remaining = args.len() - i;
    if remaining == 0 || remaining % 2 != 0 {
        return Err(NexradeError::WrongArity("xread".to_string()));
    }
    let num_streams = remaining / 2;
    let mut results = Vec::new();

    for j in 0..num_streams {
        let key = get_bytes_vec(args, i + j, "XREAD")?;
        let last_id = get_str(args, i + num_streams + j, "XREAD")?;
        let mut store_db = db.store.db(db_index).write_for(&key);
        let entries: Vec<Resp> = match store_db.get(&key) {
            None => vec![],
            Some(e) => match &e.value {
                DataType::Stream(s) => s
                    .entries
                    .iter()
                    .filter(|e| e.id.as_str() > last_id)
                    .take(count.unwrap_or(usize::MAX))
                    .map(format_stream_entry)
                    .collect(),
                _ => return Err(NexradeError::WrongType),
            },
        };
        results.push(Resp::array(vec![
            Resp::bulk(Bytes::from(key)),
            Resp::array(entries),
        ]));
    }
    Ok(Resp::array(results))
}

// ── XTRIM ────────────────────────────────────────────────────────────────────

pub async fn cmd_xtrim(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("xtrim".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XTRIM")?;
    let strategy = get_str(args, 2, "XTRIM")?.to_uppercase();
    let threshold = get_i64(args, 3, "XTRIM")? as usize;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::Stream(s) => {
                let old_len = s.entries.len();
                if strategy == "MAXLEN" && s.entries.len() > threshold {
                    let to_remove = s.entries.len() - threshold;
                    s.entries.drain(..to_remove);
                }
                Ok(Resp::int((old_len - s.entries.len()) as i64))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

// ── XDEL ─────────────────────────────────────────────────────────────────────

pub async fn cmd_xdel(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("xdel".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XDEL")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::Stream(s) => {
                let mut deleted = 0i64;
                for i in 2..args.len() {
                    if let Ok(id) = get_str(args, i, "XDEL") {
                        let before = s.entries.len();
                        s.entries.retain(|e| e.id != id);
                        deleted += (before - s.entries.len()) as i64;
                    }
                }
                Ok(Resp::int(deleted))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

// ── XGROUP ───────────────────────────────────────────────────────────────────

pub async fn cmd_xgroup(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("xgroup".to_string()));
    }
    let sub = get_str(args, 1, "XGROUP")?.to_uppercase();

    match sub.as_str() {
        "CREATE" => xgroup_create(db, args, db_index).await,
        "SETID" => xgroup_setid(db, args, db_index).await,
        "DESTROY" => xgroup_destroy(db, args, db_index).await,
        "CREATECONSUMER" => xgroup_createconsumer(db, args, db_index).await,
        "DELCONSUMER" => xgroup_delconsumer(db, args, db_index).await,
        _ => Err(NexradeError::Generic(format!(
            "unknown XGROUP subcommand '{}'",
            sub
        ))),
    }
}

async fn xgroup_create(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // XGROUP CREATE key group id [MKSTREAM]
    if args.len() < 5 {
        return Err(NexradeError::WrongArity("XGROUP CREATE".to_string()));
    }
    let key = get_bytes_vec(args, 2, "XGROUP CREATE")?;
    let group = get_bytes_vec(args, 3, "XGROUP CREATE")?;
    let last_id = get_str(args, 4, "XGROUP CREATE")?;
    let mkstream = args.iter().any(|a| {
        a.as_str()
            .map(|s| s.eq_ignore_ascii_case("MKSTREAM"))
            .unwrap_or(false)
    });

    let mut store_db = db.store.db(db_index).write_for(&key);

    // Auto-create stream if MKSTREAM
    if store_db.get(&key).is_none() {
        if mkstream {
            store_db.insert(key.clone(), Entry::new(DataType::Stream(StreamData::new())));
        } else {
            return Err(NexradeError::Generic(
                "ERR The XGROUP subcommand requires the key to exist".to_string(),
            ));
        }
    }

    match store_db.get_mut(&key) {
        Some(e) => match &mut e.value {
            DataType::Stream(s) => {
                if s.groups.contains_key(&group) {
                    return Err(NexradeError::Generic(
                        "BUSYGROUP Consumer Group name already exists".to_string(),
                    ));
                }
                // '$' means "last entry in the stream"
                let resolved_id = if last_id == "$" {
                    s.entries
                        .last()
                        .map(|e| e.id.clone())
                        .unwrap_or_else(|| "0-0".to_string())
                } else {
                    last_id.to_string()
                };
                s.groups
                    .insert(group.clone(), ConsumerGroup::new(group, resolved_id));
                Ok(Resp::ok())
            }
            _ => Err(NexradeError::WrongType),
        },
        None => Err(NexradeError::NoKey),
    }
}

async fn xgroup_setid(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // XGROUP SETID key group id
    if args.len() < 5 {
        return Err(NexradeError::WrongArity("XGROUP SETID".to_string()));
    }
    let key = get_bytes_vec(args, 2, "XGROUP SETID")?;
    let group = get_bytes_vec(args, 3, "XGROUP SETID")?;
    let last_id = get_str(args, 4, "XGROUP SETID")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        Some(e) => match &mut e.value {
            DataType::Stream(s) => match s.groups.get_mut(&group) {
                None => Err(NexradeError::Generic(
                    "-NOGROUP No such consumer group".to_string(),
                )),
                Some(g) => {
                    let resolved = if last_id == "$" {
                        s.entries
                            .last()
                            .map(|e| e.id.clone())
                            .unwrap_or_else(|| "0-0".to_string())
                    } else {
                        last_id.to_string()
                    };
                    g.last_delivered_id = resolved;
                    Ok(Resp::ok())
                }
            },
            _ => Err(NexradeError::WrongType),
        },
        None => Err(NexradeError::NoKey),
    }
}

async fn xgroup_destroy(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("XGROUP DESTROY".to_string()));
    }
    let key = get_bytes_vec(args, 2, "XGROUP DESTROY")?;
    let group = get_bytes_vec(args, 3, "XGROUP DESTROY")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        Some(e) => match &mut e.value {
            DataType::Stream(s) => Ok(Resp::int(
                s.groups.remove(&group).map(|_| 1i64).unwrap_or(0),
            )),
            _ => Err(NexradeError::WrongType),
        },
        None => Ok(Resp::int(0)),
    }
}

async fn xgroup_createconsumer(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 5 {
        return Err(NexradeError::WrongArity(
            "XGROUP CREATECONSUMER".to_string(),
        ));
    }
    let key = get_bytes_vec(args, 2, "XGROUP CREATECONSUMER")?;
    let group = get_bytes_vec(args, 3, "XGROUP CREATECONSUMER")?;
    let consumer = get_bytes_vec(args, 4, "XGROUP CREATECONSUMER")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        Some(e) => match &mut e.value {
            DataType::Stream(s) => match s.groups.get_mut(&group) {
                None => Err(NexradeError::Generic(
                    "-NOGROUP No such consumer group".to_string(),
                )),
                Some(g) => {
                    let is_new = !g.consumers.contains_key(&consumer);
                    if is_new {
                        g.consumers.insert(
                            consumer.clone(),
                            Consumer {
                                name: consumer,
                                pending_ids: vec![],
                            },
                        );
                    }
                    Ok(Resp::int(is_new as i64))
                }
            },
            _ => Err(NexradeError::WrongType),
        },
        None => Err(NexradeError::NoKey),
    }
}

async fn xgroup_delconsumer(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 5 {
        return Err(NexradeError::WrongArity("XGROUP DELCONSUMER".to_string()));
    }
    let key = get_bytes_vec(args, 2, "XGROUP DELCONSUMER")?;
    let group = get_bytes_vec(args, 3, "XGROUP DELCONSUMER")?;
    let consumer = get_bytes_vec(args, 4, "XGROUP DELCONSUMER")?;
    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        Some(e) => match &mut e.value {
            DataType::Stream(s) => match s.groups.get_mut(&group) {
                None => Err(NexradeError::Generic(
                    "-NOGROUP No such consumer group".to_string(),
                )),
                Some(g) => {
                    let pending_count = g
                        .consumers
                        .get(&consumer)
                        .map(|c| c.pending_ids.len())
                        .unwrap_or(0);
                    // Remove pending entries belonging to this consumer
                    if let Some(c) = g.consumers.remove(&consumer) {
                        for id in &c.pending_ids {
                            g.pending.remove(id);
                        }
                    }
                    Ok(Resp::int(pending_count as i64))
                }
            },
            _ => Err(NexradeError::WrongType),
        },
        None => Ok(Resp::int(0)),
    }
}

// ── XREADGROUP ───────────────────────────────────────────────────────────────

pub async fn cmd_xreadgroup(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // XREADGROUP GROUP group consumer [COUNT n] [NOACK] STREAMS key id
    if args.len() < 7 {
        return Err(NexradeError::WrongArity("xreadgroup".to_string()));
    }
    if get_str(args, 1, "XREADGROUP")?.to_uppercase() != "GROUP" {
        return Err(NexradeError::SyntaxError);
    }
    let group = get_bytes_vec(args, 2, "XREADGROUP")?;
    let consumer = get_bytes_vec(args, 3, "XREADGROUP")?;

    let mut i = 4;
    let mut count: Option<usize> = None;
    let mut noack = false;
    while i < args.len() {
        let opt = get_str(args, i, "XREADGROUP")?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                count = Some(get_i64(args, i + 1, "XREADGROUP")? as usize);
                i += 2;
            }
            "NOACK" => {
                noack = true;
                i += 1;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => {
                i += 1;
            }
        }
    }

    let remaining = args.len() - i;
    if remaining == 0 || remaining % 2 != 0 {
        return Err(NexradeError::WrongArity("xreadgroup".to_string()));
    }
    let num_streams = remaining / 2;
    let mut results = Vec::new();

    for j in 0..num_streams {
        let key = get_bytes_vec(args, i + j, "XREADGROUP")?;
        let id = get_str(args, i + num_streams + j, "XREADGROUP")?;
        let now = now_ms();

        let mut store_db = db.store.db(db_index).write_for(&key);
        let entries_resp: Vec<Resp> = match store_db.get_mut(&key) {
            None => vec![],
            Some(e) => match &mut e.value {
                DataType::Stream(s) => {
                    let g = s.groups.get_mut(&group).ok_or_else(|| {
                        NexradeError::Generic("-NOGROUP No such consumer group".to_string())
                    })?;

                    // Ensure consumer exists
                    g.consumers
                        .entry(consumer.clone())
                        .or_insert_with(|| Consumer {
                            name: consumer.clone(),
                            pending_ids: vec![],
                        });

                    let to_deliver: Vec<StreamEntry> = if id == ">" {
                        // Deliver new entries after last_delivered_id
                        let last = g.last_delivered_id.clone();
                        let entries: Vec<_> = s
                            .entries
                            .iter()
                            .filter(|e| e.id.as_str() > last.as_str())
                            .take(count.unwrap_or(usize::MAX))
                            .cloned()
                            .collect();
                        if let Some(last_entry) = entries.last() {
                            g.last_delivered_id = last_entry.id.clone();
                        }
                        entries
                    } else {
                        // Re-deliver pending entries for this consumer with id > given id
                        let consumer_pending: Vec<String> = g
                            .consumers
                            .get(&consumer)
                            .map(|c| {
                                c.pending_ids
                                    .iter()
                                    .filter(|pid| pid.as_str() > id)
                                    .cloned()
                                    .collect()
                            })
                            .unwrap_or_default();
                        s.entries
                            .iter()
                            .filter(|e| consumer_pending.contains(&e.id))
                            .take(count.unwrap_or(usize::MAX))
                            .cloned()
                            .collect()
                    };

                    if !noack {
                        for entry in &to_deliver {
                            let pending = PendingEntry {
                                consumer: consumer.clone(),
                                delivery_time_ms: now,
                                delivery_count: g
                                    .pending
                                    .get(&entry.id)
                                    .map(|p| p.delivery_count + 1)
                                    .unwrap_or(1),
                            };
                            g.pending.insert(entry.id.clone(), pending);
                            if let Some(c) = g.consumers.get_mut(&consumer) {
                                if !c.pending_ids.contains(&entry.id) {
                                    c.pending_ids.push(entry.id.clone());
                                }
                            }
                        }
                    }

                    to_deliver.iter().map(format_stream_entry).collect()
                }
                _ => return Err(NexradeError::WrongType),
            },
        };

        results.push(Resp::array(vec![
            Resp::bulk(Bytes::from(key)),
            Resp::array(entries_resp),
        ]));
    }
    Ok(Resp::array(results))
}

// ── XACK ─────────────────────────────────────────────────────────────────────

pub async fn cmd_xack(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // XACK key group id [id ...]
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("xack".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XACK")?;
    let group = get_bytes_vec(args, 2, "XACK")?;

    let ids: Vec<String> = (3..args.len())
        .filter_map(|i| get_str(args, i, "XACK").ok().map(|s| s.to_string()))
        .collect();

    let mut store_db = db.store.db(db_index).write_for(&key);
    let mut acked = 0i64;

    match store_db.get_mut(&key) {
        None => {}
        Some(e) => match &mut e.value {
            DataType::Stream(s) => {
                if let Some(g) = s.groups.get_mut(&group) {
                    for id in &ids {
                        if let Some(pending) = g.pending.remove(id) {
                            // Remove from consumer's pending list
                            if let Some(c) = g.consumers.get_mut(&pending.consumer) {
                                c.pending_ids.retain(|pid| pid != id);
                            }
                            acked += 1;
                        }
                    }
                }
            }
            _ => return Err(NexradeError::WrongType),
        },
    }
    Ok(Resp::int(acked))
}

// ── XPENDING ─────────────────────────────────────────────────────────────────

pub async fn cmd_xpending(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // XPENDING key group [start end count [consumer]]
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("xpending".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XPENDING")?;
    let group = get_bytes_vec(args, 2, "XPENDING")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::Stream(s) => {
                let g = match s.groups.get(&group) {
                    None => {
                        return Err(NexradeError::Generic(
                            "-NOGROUP No such consumer group".to_string(),
                        ))
                    }
                    Some(g) => g,
                };

                if args.len() == 3 {
                    // Summary form: count, min-id, max-id, consumer counts
                    let count = g.pending.len() as i64;
                    let min_id = g.pending.keys().next().cloned().unwrap_or_default();
                    let max_id = g.pending.keys().next_back().cloned().unwrap_or_default();
                    let mut consumer_counts: std::collections::HashMap<&[u8], i64> =
                        std::collections::HashMap::new();
                    for p in g.pending.values() {
                        *consumer_counts.entry(p.consumer.as_slice()).or_insert(0) += 1;
                    }
                    let consumers_resp: Vec<Resp> = consumer_counts
                        .iter()
                        .map(|(name, cnt)| {
                            Resp::array(vec![
                                Resp::bulk(Bytes::from(name.to_vec())),
                                Resp::bulk_str(cnt.to_string()),
                            ])
                        })
                        .collect();
                    Ok(Resp::array(vec![
                        Resp::int(count),
                        if min_id.is_empty() {
                            Resp::null()
                        } else {
                            Resp::bulk_str(min_id)
                        },
                        if max_id.is_empty() {
                            Resp::null()
                        } else {
                            Resp::bulk_str(max_id)
                        },
                        Resp::array(consumers_resp),
                    ]))
                } else {
                    // Range form: start end count [consumer]
                    let start = get_str(args, 3, "XPENDING")?;
                    let end = get_str(args, 4, "XPENDING")?;
                    let count = get_i64(args, 5, "XPENDING")? as usize;
                    let consumer_filter = if args.len() >= 7 {
                        Some(get_bytes_vec(args, 6, "XPENDING")?)
                    } else {
                        None
                    };

                    let entries: Vec<Resp> = g
                        .pending
                        .iter()
                        .filter(|(id, _)| {
                            (start == "-" || id.as_str() >= start)
                                && (end == "+" || id.as_str() <= end)
                        })
                        .filter(|(_, p)| {
                            consumer_filter
                                .as_ref()
                                .map(|c| &p.consumer == c)
                                .unwrap_or(true)
                        })
                        .take(count)
                        .map(|(id, p)| {
                            Resp::array(vec![
                                Resp::bulk_str(id.clone()),
                                Resp::bulk(Bytes::from(p.consumer.clone())),
                                Resp::int(p.delivery_time_ms as i64),
                                Resp::int(p.delivery_count as i64),
                            ])
                        })
                        .collect();
                    Ok(Resp::array(entries))
                }
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}
