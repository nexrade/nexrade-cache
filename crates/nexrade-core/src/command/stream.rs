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

/// Stream entry id parsed into its (milliseconds, sequence) numeric components.
///
/// Redis stores IDs as `<ms>-<seq>` and compares them as numbers — `10-0` is
/// strictly greater than `2-0`. Previously this module compared the raw string
/// lex order, which silently mis-ordered entries whenever the ms parts had
/// different digit counts (e.g. `9-0 > 10-0` was wrongly true).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub u64, pub u64);

impl StreamId {
    pub const MIN: StreamId = StreamId(0, 0);
    pub const MAX: StreamId = StreamId(u64::MAX, u64::MAX);

    /// Parse a Redis-formatted id (`ms-seq`). Returns None on malformed input.
    pub fn parse(s: &str) -> Option<Self> {
        let (ms, seq) = s.split_once('-')?;
        let ms: u64 = ms.parse().ok()?;
        let seq: u64 = seq.parse().ok()?;
        Some(StreamId(ms, seq))
    }

    /// Render back to the Redis wire format.
    pub fn to_wire(self) -> String {
        format!("{}-{}", self.0, self.1)
    }
}

/// Parse a Redis stream-id range bound. `-` becomes MIN (inclusive start),
/// `+` becomes MAX (inclusive end), `$` becomes MAX (XREAD "last entry").
fn parse_id_bound(s: &str, is_start: bool, allow_dollar: bool) -> Result<StreamId> {
    if is_start && s == "-" {
        return Ok(StreamId::MIN);
    }
    if (is_start || allow_dollar) && s == "$" {
        return Ok(StreamId::MAX);
    }
    if !is_start && s == "+" {
        return Ok(StreamId::MAX);
    }
    StreamId::parse(s).ok_or_else(|| {
        NexradeError::Generic(
            "ERR Invalid stream ID specified as stream command argument".to_string(),
        )
    })
}

/// Largest parsed id in the stream, or None if empty / all entries are
/// malformed. Used for `XADD ... ms-*` validation.
fn last_parsed_id(s: &StreamData) -> Option<StreamId> {
    s.entries
        .iter()
        .filter_map(|e| StreamId::parse(&e.id))
        .max()
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

    let mut fields = Vec::new();
    let mut i = 3;
    while i + 1 < args.len() {
        fields.push((
            get_bytes_vec(args, i, "XADD")?,
            get_bytes_vec(args, i + 1, "XADD")?,
        ));
        i += 2;
    }

    // Pre-read stream to compute the auto-id under the lock-free path.
    let last = {
        let sdb = db.store.db(db_index).read_for(&key);
        sdb.get_ro(&key).and_then(|e| match &e.value {
            DataType::Stream(s) => last_parsed_id(s),
            _ => None,
        })
    };

    let id = if id_str == "*" {
        // Auto-id: ms = now, seq = 0 (or higher if another entry already
        // landed in this same ms).
        let ms = now_ms();
        let seq = match last {
            Some(StreamId(lm, ls)) if lm == ms => ls + 1,
            _ => 0,
        };
        StreamId(ms, seq).to_wire()
    } else {
        // Explicit id may be `ms-seq`, `ms-*`, or `*-seq`.
        let resolved = resolve_xadd_id(id_str, last)?;
        resolved.to_wire()
    };

    let entry = StreamEntry {
        id: id.clone(),
        fields,
    };

    let mut store_db = db.store.db(db_index).write_for(&key);
    let stream = get_stream_mut!(store_db, key);
    stream.entries.push(entry);

    // Wake any XREAD/XREADGROUP BLOCK callers waiting on this shard. The
    // re-check inside their wait loop filters by key, so cross-stream
    // wakeups are harmless.
    #[cfg(not(target_arch = "wasm32"))]
    db.notify_stream_waiters();

    Ok(Resp::bulk_str(id))
}

/// Resolve an explicit `XADD` id spec into a concrete numeric `StreamId`.
/// Spec forms accepted: `ms-seq`, `ms-*` (auto-seq), `*-seq` (auto-ms).
fn resolve_xadd_id(spec: &str, last: Option<StreamId>) -> Result<StreamId> {
    let (ms_part, seq_part) = spec.split_once('-').ok_or_else(|| {
        NexradeError::Generic(
            "ERR Invalid stream ID specified as stream command argument".to_string(),
        )
    })?;

    let ms_now = now_ms();
    let ms: u64 = if ms_part == "*" {
        // Auto-ms: if last entry is from the current ms, must match it;
        // otherwise use now.
        match last {
            Some(StreamId(lm, _)) if lm >= ms_now => lm,
            _ => ms_now,
        }
    } else {
        ms_part.parse().map_err(|_| {
            NexradeError::Generic(
                "ERR Invalid stream ID specified as stream command argument".to_string(),
            )
        })?
    };

    let seq: u64 = if seq_part == "*" {
        match last {
            Some(StreamId(lm, ls)) if lm == ms => ls + 1,
            _ => 0,
        }
    } else {
        seq_part.parse().map_err(|_| {
            NexradeError::Generic(
                "ERR Invalid stream ID specified as stream command argument".to_string(),
            )
        })?
    };

    let id = StreamId(ms, seq);
    if let Some(prev) = last {
        if id <= prev {
            return Err(NexradeError::Generic(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_string(),
            ));
        }
    } else if ms == 0 && seq == 0 {
        // Redis disallows `0-0` as the very first entry in a stream.
        return Err(NexradeError::Generic(
            "ERR The ID specified in XADD must be greater than 0-0".to_string(),
        ));
    }
    Ok(id)
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
    let start_raw = get_str(args, 2, "XRANGE")?;
    let end_raw = get_str(args, 3, "XRANGE")?;
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

    let start = parse_id_bound(start_raw, true, false)?;
    let end = parse_id_bound(end_raw, false, false)?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::Stream(s) => {
                let entries: Vec<Resp> = s
                    .entries
                    .iter()
                    .filter(|e| match StreamId::parse(&e.id) {
                        Some(id) => id >= start && id <= end,
                        None => false,
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
    let end_raw = get_str(args, 2, "XREVRANGE")?;
    let start_raw = get_str(args, 3, "XREVRANGE")?;
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

    // XREVRANGE swaps start/end argument order but keeps the inclusive semantics.
    let start = parse_id_bound(start_raw, true, false)?;
    let end = parse_id_bound(end_raw, false, false)?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::Stream(s) => {
                let mut entries: Vec<Resp> = s
                    .entries
                    .iter()
                    .filter(|e| match StreamId::parse(&e.id) {
                        Some(id) => id >= start && id <= end,
                        None => false,
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
    let mut block_ms: Option<u64> = None;

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
            "BLOCK" => {
                let ms = get_i64(args, i + 1, "XREAD")?;
                if ms < 0 {
                    return Err(NexradeError::Generic(
                        "ERR value is out of range".to_string(),
                    ));
                }
                // 0 means "block forever" — represent as u64::MAX sentinel.
                block_ms = Some(if ms == 0 { u64::MAX } else { ms as u64 });
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

    // Collect requested keys and cursor ids once (errors fail fast).
    let mut key_specs: Vec<(Vec<u8>, StreamId)> = Vec::with_capacity(num_streams);
    for j in 0..num_streams {
        let key = get_bytes_vec(args, i + j, "XREAD")?;
        let last_id_raw = get_str(args, i + num_streams + j, "XREAD")?;
        // XREAD supports `-`, `+`, `$`, or any concrete `<ms>-<seq>`.
        let last_id = parse_id_bound(last_id_raw, true, true)?;
        key_specs.push((key, last_id));
    }

    let snapshot = || -> Result<Resp> {
        let mut results = Vec::with_capacity(key_specs.len());
        for (key, last_id) in &key_specs {
            let mut store_db = db.store.db(db_index).write_for(key);
            let entries: Vec<Resp> = match store_db.get(key) {
                None => vec![],
                Some(e) => match &e.value {
                    DataType::Stream(s) => s
                        .entries
                        .iter()
                        .filter(|e| match StreamId::parse(&e.id) {
                            Some(id) => id > *last_id,
                            None => false,
                        })
                        .take(count.unwrap_or(usize::MAX))
                        .map(format_stream_entry)
                        .collect(),
                    _ => return Err(NexradeError::WrongType),
                },
            };
            results.push(Resp::array(vec![
                Resp::bulk(Bytes::from(key.clone())),
                Resp::array(entries),
            ]));
        }
        Ok(Resp::array(results))
    };

    // Fast path: no BLOCK requested, or we already have entries on first read.
    let initial = snapshot()?;
    if block_ms.is_none() || has_entries(&initial) {
        return Ok(initial);
    }

    // Block until something arrives or the timeout elapses.
    #[cfg(not(target_arch = "wasm32"))]
    {
        let timeout_dur = std::time::Duration::from_millis(block_ms.unwrap());
        match tokio::time::timeout(timeout_dur, async {
            let _parked = db.park_stream_waiter();
            loop {
                db.stream_notify.notified().await;
                let snap = snapshot()?;
                if has_entries(&snap) {
                    return Ok::<Resp, NexradeError>(snap);
                }
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
        let _ = block_ms;
        Ok(Resp::null_array())
    }
}

/// True if `XREAD` reply contains at least one entry (per-stream).
fn has_entries(reply: &Resp) -> bool {
    let Resp::Array(Some(streams)) = reply else {
        return false;
    };
    for stream in streams {
        let Resp::Array(Some(parts)) = stream else {
            continue;
        };
        if parts.len() < 2 {
            continue;
        }
        if let Resp::Array(Some(entries)) = &parts[1] {
            if !entries.is_empty() {
                return true;
            }
        }
    }
    false
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
    // XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] [NOACK] STREAMS key id
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
    let mut block_ms: Option<u64> = None;
    while i < args.len() {
        let opt = get_str(args, i, "XREADGROUP")?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                count = Some(get_i64(args, i + 1, "XREADGROUP")? as usize);
                i += 2;
            }
            "BLOCK" => {
                let ms = get_i64(args, i + 1, "XREADGROUP")?;
                if ms < 0 {
                    return Err(NexradeError::Generic(
                        "ERR value is out of range".to_string(),
                    ));
                }
                block_ms = Some(if ms == 0 { u64::MAX } else { ms as u64 });
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

    let mut key_specs: Vec<(Vec<u8>, String)> = Vec::with_capacity(num_streams);
    for j in 0..num_streams {
        let key = get_bytes_vec(args, i + j, "XREADGROUP")?;
        let id = get_str(args, i + num_streams + j, "XREADGROUP")?.to_string();
        key_specs.push((key, id));
    }

    // Synchronous attempt. Pending-id re-delivery (`> {id}`) never blocks, so
    // we only need to gate the "new entries" path on BLOCK.
    let read_once = |key_specs: &[(Vec<u8>, String)]| -> Result<Resp> {
        let mut results = Vec::with_capacity(key_specs.len());
        let now = now_ms();
        for (key, id) in key_specs {
            let mut store_db = db.store.db(db_index).write_for(key);
            let entries_resp: Vec<Resp> = match store_db.get_mut(key) {
                None => vec![],
                Some(e) => match &mut e.value {
                    DataType::Stream(s) => {
                        let g = s.groups.get_mut(&group).ok_or_else(|| {
                            NexradeError::Prefixed("NOGROUP No such consumer group".to_string())
                        })?;

                        g.consumers
                            .entry(consumer.clone())
                            .or_insert_with(|| Consumer {
                                name: consumer.clone(),
                                pending_ids: vec![],
                            });

                        let to_deliver: Vec<StreamEntry> = if id == ">" {
                            let last = match StreamId::parse(&g.last_delivered_id) {
                                Some(id) => id,
                                None => StreamId::MIN,
                            };
                            let entries: Vec<_> = s
                                .entries
                                .iter()
                                .filter(|e| match StreamId::parse(&e.id) {
                                    Some(id) => id > last,
                                    None => false,
                                })
                                .take(count.unwrap_or(usize::MAX))
                                .cloned()
                                .collect();
                            if let Some(last_entry) = entries.last() {
                                g.last_delivered_id = last_entry.id.clone();
                            }
                            entries
                        } else {
                            let cursor = match StreamId::parse(id) {
                                Some(id) => id,
                                None => StreamId::MIN,
                            };
                            let consumer_pending: Vec<String> = g
                                .consumers
                                .get(&consumer)
                                .map(|c| {
                                    c.pending_ids
                                        .iter()
                                        .filter(|pid| {
                                            StreamId::parse(pid)
                                                .map(|pid| pid > cursor)
                                                .unwrap_or(false)
                                        })
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
                Resp::bulk(Bytes::from(key.clone())),
                Resp::array(entries_resp),
            ]));
        }
        Ok(Resp::array(results))
    };

    // We have to be careful: if any key uses `id == ">"`, an immediate read
    // may legitimately return empty (no new entries yet) — that's the case
    // we want to block on. Re-delivery reads (`id != ">"`) should never
    // block.
    let needs_blocking = block_ms.is_some() && key_specs.iter().any(|(_, id)| id == ">");

    let initial = read_once(&key_specs)?;
    if !needs_blocking || has_entries(&initial) {
        return Ok(initial);
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let timeout_dur = std::time::Duration::from_millis(block_ms.unwrap());
        match tokio::time::timeout(timeout_dur, async {
            let _parked = db.park_stream_waiter();
            loop {
                db.stream_notify.notified().await;
                let snap = read_once(&key_specs)?;
                if has_entries(&snap) {
                    return Ok::<Resp, NexradeError>(snap);
                }
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
        let _ = block_ms;
        Ok(Resp::null_array())
    }
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
                            let parsed = StreamId::parse(id);
                            let lo = if start == "-" {
                                StreamId::MIN
                            } else {
                                match StreamId::parse(start) {
                                    Some(v) => v,
                                    None => StreamId::MIN,
                                }
                            };
                            let hi = if end == "+" {
                                StreamId::MAX
                            } else {
                                match StreamId::parse(end) {
                                    Some(v) => v,
                                    None => StreamId::MAX,
                                }
                            };
                            match parsed {
                                Some(id) => id >= lo && id <= hi,
                                None => false,
                            }
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

// ── XINFO ──────────────────────────────────────────────────────────────────────

pub async fn cmd_xinfo(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // XINFO STREAM key | XINFO GROUPS key | XINFO CONSUMERS key group
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("xinfo".to_string()));
    }
    let sub = get_str(args, 1, "XINFO")?.to_uppercase();
    match sub.as_str() {
        "STREAM" => xinfo_stream(db, args, db_index).await,
        "GROUPS" => xinfo_groups(db, args, db_index).await,
        "CONSUMERS" => xinfo_consumers(db, args, db_index).await,
        "HELP" => Ok(Resp::array(vec![
            Resp::bulk_str("XINFO <subcommand> [<arg> ...]. Subcommands are:"),
            Resp::bulk_str("STREAM <key>"),
            Resp::bulk_str("GROUPS <key>"),
            Resp::bulk_str("CONSUMERS <key> <group>"),
        ])),
        _ => Err(NexradeError::Generic(format!(
            "ERR unknown XINFO subcommand or wrong number of arguments for '{}'",
            sub
        ))),
    }
}

/// `NOGROUP` reply matching Redis's wording for a missing group under a
/// known stream key.
fn nogroup_err(key: &[u8], group: &[u8]) -> NexradeError {
    NexradeError::Generic(format!(
        "NOGROUP No such consumer group '{}' for key name '{}'",
        String::from_utf8_lossy(group),
        String::from_utf8_lossy(key)
    ))
}

fn no_such_key_err() -> NexradeError {
    NexradeError::Generic("ERR no such key".to_string())
}

async fn xinfo_stream(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("XINFO STREAM".to_string()));
    }
    let key = get_bytes_vec(args, 2, "XINFO STREAM")?;
    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Err(no_such_key_err()),
        Some(e) => match &e.value {
            DataType::Stream(s) => {
                let first = s.entries.first().map(format_stream_entry);
                let last = s.entries.last().map(format_stream_entry);
                let last_id = s
                    .entries
                    .last()
                    .map(|e| e.id.clone())
                    .unwrap_or_else(|| "0-0".to_string());
                Ok(Resp::array(vec![
                    Resp::bulk_str("length"),
                    Resp::int(s.entries.len() as i64),
                    Resp::bulk_str("last-generated-id"),
                    Resp::bulk_str(last_id.clone()),
                    Resp::bulk_str("max-deleted-entry-id"),
                    Resp::bulk_str("0-0"),
                    Resp::bulk_str("entries-added"),
                    Resp::int(s.entries.len() as i64),
                    Resp::bulk_str("groups"),
                    Resp::int(s.groups.len() as i64),
                    Resp::bulk_str("first-entry"),
                    first.unwrap_or_else(Resp::null),
                    Resp::bulk_str("last-entry"),
                    last.unwrap_or_else(Resp::null),
                ]))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

async fn xinfo_groups(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("XINFO GROUPS".to_string()));
    }
    let key = get_bytes_vec(args, 2, "XINFO GROUPS")?;
    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Err(no_such_key_err()),
        Some(e) => match &e.value {
            DataType::Stream(s) => {
                let groups: Vec<Resp> = s
                    .groups
                    .values()
                    .map(|g| {
                        Resp::array(vec![
                            Resp::bulk_str("name"),
                            Resp::bulk(Bytes::from(g.name.clone())),
                            Resp::bulk_str("consumers"),
                            Resp::int(g.consumers.len() as i64),
                            Resp::bulk_str("pending"),
                            Resp::int(g.pending.len() as i64),
                            Resp::bulk_str("last-delivered-id"),
                            Resp::bulk_str(g.last_delivered_id.clone()),
                            Resp::bulk_str("entries-read"),
                            Resp::null(),
                            Resp::bulk_str("lag"),
                            Resp::null(),
                        ])
                    })
                    .collect();
                Ok(Resp::array(groups))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

async fn xinfo_consumers(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("XINFO CONSUMERS".to_string()));
    }
    let key = get_bytes_vec(args, 2, "XINFO CONSUMERS")?;
    let group = get_bytes_vec(args, 3, "XINFO CONSUMERS")?;
    let now = now_ms();
    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Err(no_such_key_err()),
        Some(e) => match &e.value {
            DataType::Stream(s) => {
                let g = s
                    .groups
                    .get(&group)
                    .ok_or_else(|| nogroup_err(&key, &group))?;
                // Idle time for a consumer is derived from the most recent
                // delivery timestamp across its pending entries (Redis tracks
                // a dedicated seen-time; we approximate from pending state,
                // which is the only per-consumer time this model records).
                let consumers: Vec<Resp> = g
                    .consumers
                    .values()
                    .map(|c| {
                        let last_seen = g
                            .pending
                            .values()
                            .filter(|p| p.consumer == c.name)
                            .map(|p| p.delivery_time_ms)
                            .max();
                        let idle = last_seen.map(|t| now.saturating_sub(t)).unwrap_or(0);
                        Resp::array(vec![
                            Resp::bulk_str("name"),
                            Resp::bulk(Bytes::from(c.name.clone())),
                            Resp::bulk_str("pending"),
                            Resp::int(c.pending_ids.len() as i64),
                            Resp::bulk_str("idle"),
                            Resp::int(idle as i64),
                            Resp::bulk_str("inactive"),
                            Resp::int(idle as i64),
                        ])
                    })
                    .collect();
                Ok(Resp::array(consumers))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

// ── XCLAIM ─────────────────────────────────────────────────────────────────────

pub async fn cmd_xclaim(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // XCLAIM key group consumer min-idle-time id [id ...]
    //   [IDLE ms] [TIME ms-unix] [RETRYCOUNT n] [FORCE] [JUSTID]
    if args.len() < 6 {
        return Err(NexradeError::WrongArity("xclaim".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XCLAIM")?;
    let group = get_bytes_vec(args, 2, "XCLAIM")?;
    let consumer = get_bytes_vec(args, 3, "XCLAIM")?;
    let min_idle = get_i64(args, 4, "XCLAIM")?.max(0) as u64;

    // Collect id args until the first option token.
    let mut ids: Vec<String> = Vec::new();
    let mut idx = 5;
    while idx < args.len() {
        let s = get_str(args, idx, "XCLAIM")?;
        if StreamId::parse(s).is_some() {
            ids.push(s.to_string());
            idx += 1;
        } else {
            break;
        }
    }

    let mut set_idle: Option<u64> = None;
    let mut set_time: Option<u64> = None;
    let mut set_retry: Option<u64> = None;
    let mut force = false;
    let mut justid = false;
    while idx < args.len() {
        let opt = get_str(args, idx, "XCLAIM")?.to_uppercase();
        match opt.as_str() {
            "IDLE" => {
                set_idle = Some(get_i64(args, idx + 1, "XCLAIM")?.max(0) as u64);
                idx += 2;
            }
            "TIME" => {
                set_time = Some(get_i64(args, idx + 1, "XCLAIM")?.max(0) as u64);
                idx += 2;
            }
            "RETRYCOUNT" => {
                set_retry = Some(get_i64(args, idx + 1, "XCLAIM")?.max(0) as u64);
                idx += 2;
            }
            "FORCE" => {
                force = true;
                idx += 1;
            }
            "JUSTID" => {
                justid = true;
                idx += 1;
            }
            "LASTID" => {
                idx += 2;
            }
            _ => return Err(NexradeError::SyntaxError),
        }
    }

    let now = now_ms();
    let mut store_db = db.store.db(db_index).write_for(&key);
    let e = match store_db.get_mut(&key) {
        None => return Err(nogroup_err(&key, &group)),
        Some(e) => e,
    };
    let s = match &mut e.value {
        DataType::Stream(s) => s,
        _ => return Err(NexradeError::WrongType),
    };
    // Snapshot which ids exist and their entries (a claimed-but-deleted entry
    // is dropped from the PEL, matching Redis).
    let existing: std::collections::HashSet<String> =
        s.entries.iter().map(|e| e.id.clone()).collect();
    let entry_by_id: std::collections::HashMap<String, StreamEntry> = s
        .entries
        .iter()
        .map(|e| (e.id.clone(), e.clone()))
        .collect();

    let g = s
        .groups
        .get_mut(&group)
        .ok_or_else(|| nogroup_err(&key, &group))?;

    g.consumers
        .entry(consumer.clone())
        .or_insert_with(|| Consumer {
            name: consumer.clone(),
            pending_ids: vec![],
        });

    let mut claimed: Vec<Resp> = Vec::new();
    for id in &ids {
        let existing_pending = g.pending.get(id).cloned();
        let is_pending = existing_pending.is_some();

        // Without FORCE, only already-pending entries can be claimed.
        if !is_pending && !force {
            continue;
        }
        // FORCE requires the entry to still exist in the stream.
        if !is_pending && !existing.contains(id) {
            continue;
        }
        // Pending but the underlying entry was deleted: drop from the PEL.
        if is_pending && !existing.contains(id) {
            if let Some(p) = g.pending.remove(id) {
                if let Some(c) = g.consumers.get_mut(&p.consumer) {
                    c.pending_ids.retain(|pid| pid != id);
                }
            }
            continue;
        }

        // Enforce min-idle-time against the current pending record.
        if let Some(ref p) = existing_pending {
            let idle = now.saturating_sub(p.delivery_time_ms);
            if idle < min_idle {
                continue;
            }
        }

        let delivery_time_ms = match (set_time, set_idle) {
            (Some(t), _) => t,
            (None, Some(i)) => now.saturating_sub(i),
            (None, None) => now,
        };
        let base_retry = existing_pending
            .as_ref()
            .map(|p| p.delivery_count)
            .unwrap_or(0);
        let delivery_count = match set_retry {
            Some(r) => r,
            None if justid => base_retry,
            None => base_retry + 1,
        };

        // Reassign ownership.
        if let Some(ref p) = existing_pending {
            if let Some(prev) = g.consumers.get_mut(&p.consumer) {
                prev.pending_ids.retain(|pid| pid != id);
            }
        }
        g.pending.insert(
            id.clone(),
            PendingEntry {
                consumer: consumer.clone(),
                delivery_time_ms,
                delivery_count,
            },
        );
        if let Some(c) = g.consumers.get_mut(&consumer) {
            if !c.pending_ids.iter().any(|pid| pid == id) {
                c.pending_ids.push(id.clone());
            }
        }

        if justid {
            claimed.push(Resp::bulk_str(id.clone()));
        } else if let Some(entry) = entry_by_id.get(id) {
            claimed.push(format_stream_entry(entry));
        }
    }

    Ok(Resp::array(claimed))
}

// ── XAUTOCLAIM ─────────────────────────────────────────────────────────────────

pub async fn cmd_xautoclaim(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]
    if args.len() < 6 {
        return Err(NexradeError::WrongArity("xautoclaim".to_string()));
    }
    let key = get_bytes_vec(args, 1, "XAUTOCLAIM")?;
    let group = get_bytes_vec(args, 2, "XAUTOCLAIM")?;
    let consumer = get_bytes_vec(args, 3, "XAUTOCLAIM")?;
    let min_idle = get_i64(args, 4, "XAUTOCLAIM")?.max(0) as u64;
    let start_str = get_str(args, 5, "XAUTOCLAIM")?;
    let start = if start_str == "-" || start_str == "0" {
        StreamId::MIN
    } else {
        StreamId::parse(start_str).ok_or_else(|| {
            NexradeError::Generic(
                "ERR Invalid stream ID specified as stream command argument".to_string(),
            )
        })?
    };

    let mut count = 100usize;
    let mut justid = false;
    let mut idx = 6;
    while idx < args.len() {
        let opt = get_str(args, idx, "XAUTOCLAIM")?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                count = get_i64(args, idx + 1, "XAUTOCLAIM")?.max(1) as usize;
                idx += 2;
            }
            "JUSTID" => {
                justid = true;
                idx += 1;
            }
            _ => return Err(NexradeError::SyntaxError),
        }
    }

    let now = now_ms();
    let mut store_db = db.store.db(db_index).write_for(&key);
    let e = match store_db.get_mut(&key) {
        None => return Err(nogroup_err(&key, &group)),
        Some(e) => e,
    };
    let s = match &mut e.value {
        DataType::Stream(s) => s,
        _ => return Err(NexradeError::WrongType),
    };
    let existing: std::collections::HashSet<String> =
        s.entries.iter().map(|e| e.id.clone()).collect();
    let entry_by_id: std::collections::HashMap<String, StreamEntry> = s
        .entries
        .iter()
        .map(|e| (e.id.clone(), e.clone()))
        .collect();

    let g = s
        .groups
        .get_mut(&group)
        .ok_or_else(|| nogroup_err(&key, &group))?;
    g.consumers
        .entry(consumer.clone())
        .or_insert_with(|| Consumer {
            name: consumer.clone(),
            pending_ids: vec![],
        });

    // Iterate the PEL in id order from `start`, claiming up to `count`.
    let mut pel_ids: Vec<String> = g
        .pending
        .keys()
        .filter(|id| StreamId::parse(id).map(|p| p >= start).unwrap_or(false))
        .cloned()
        .collect();
    pel_ids.sort_by_key(|id| StreamId::parse(id).unwrap_or(StreamId::MIN));

    let mut claimed: Vec<Resp> = Vec::new();
    let mut deleted: Vec<Resp> = Vec::new();
    let mut cursor = StreamId::MIN.to_wire();

    for id in pel_ids {
        if claimed.len() >= count {
            // Next call resumes from this id.
            cursor = id;
            break;
        }
        let p = match g.pending.get(&id) {
            Some(p) => p.clone(),
            None => continue,
        };

        // Entry deleted from the stream: prune from PEL, report as deleted.
        if !existing.contains(&id) {
            if let Some(prev) = g.consumers.get_mut(&p.consumer) {
                prev.pending_ids.retain(|pid| pid != &id);
            }
            g.pending.remove(&id);
            deleted.push(Resp::bulk_str(id.clone()));
            continue;
        }

        let idle = now.saturating_sub(p.delivery_time_ms);
        if idle < min_idle {
            continue;
        }

        // Reassign to the claiming consumer.
        if let Some(prev) = g.consumers.get_mut(&p.consumer) {
            prev.pending_ids.retain(|pid| pid != &id);
        }
        let delivery_count = if justid {
            p.delivery_count
        } else {
            p.delivery_count + 1
        };
        g.pending.insert(
            id.clone(),
            PendingEntry {
                consumer: consumer.clone(),
                delivery_time_ms: now,
                delivery_count,
            },
        );
        if let Some(c) = g.consumers.get_mut(&consumer) {
            if !c.pending_ids.iter().any(|pid| pid == &id) {
                c.pending_ids.push(id.clone());
            }
        }

        if justid {
            claimed.push(Resp::bulk_str(id.clone()));
        } else if let Some(entry) = entry_by_id.get(&id) {
            claimed.push(format_stream_entry(entry));
        }
    }

    // Reply: [next-cursor, claimed-entries, deleted-ids].
    Ok(Resp::array(vec![
        Resp::bulk_str(cursor),
        Resp::array(claimed),
        Resp::array(deleted),
    ]))
}
