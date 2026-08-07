//! Sorted Set command handlers.

use bytes::Bytes;

use crate::command::{decode_scan_cursor, get_f64, scan_select_page};

use crate::command::string::format_float;
use crate::command::{get_bytes_vec, get_i64, get_str};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::{glob_match, Entry};
use crate::types::{DataType, ZSetData};

fn get_or_create_zset<'a>(
    db: &'a mut crate::store::Database,
    key: &[u8],
) -> Result<&'a mut ZSetData> {
    // Single `entries.entry()` lookup instead of contains_key + insert +
    // get_mut (see `Database::get_or_insert_with`).
    let entry = db.get_or_insert_with(key, || Entry::new(DataType::ZSet(ZSetData::new())));
    match &mut entry.value {
        DataType::ZSet(z) => Ok(z),
        _ => Err(NexradeError::WrongType),
    }
}

/// Parse a Redis score bound string.
/// Returns `(value, is_exclusive)` where `is_exclusive` corresponds to `(` prefix.
fn parse_score_bound(s: &str) -> Result<(f64, bool)> {
    match s {
        "+inf" | "+INF" => Ok((f64::INFINITY, false)),
        "-inf" | "-INF" => Ok((f64::NEG_INFINITY, false)),
        s if s.starts_with('(') => {
            let v = s[1..].parse::<f64>().map_err(|_| NexradeError::NotFloat)?;
            Ok((v, true))
        }
        s => {
            let v = s.parse::<f64>().map_err(|_| NexradeError::NotFloat)?;
            Ok((v, false))
        }
    }
}

pub async fn cmd_zadd(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("zadd".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZADD")?;

    // Parse options: NX, XX, GT, LT, CH, INCR
    let mut i = 2;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut incr = false;

    while i < args.len() {
        let opt = get_str(args, i, "ZADD")?.to_uppercase();
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
            "CH" => {
                ch = true;
                i += 1;
            }
            "INCR" => {
                incr = true;
                i += 1;
            }
            _ => break,
        }
    }

    // NX is incompatible with GT/LT/INCR; GT and LT are mutually exclusive.
    if nx && (gt || lt || incr) {
        return Err(NexradeError::SyntaxError);
    }
    if gt && lt {
        return Err(NexradeError::SyntaxError);
    }

    // INCR requires exactly one score-member pair.
    if incr {
        if args.len() - i != 2 {
            return Err(NexradeError::Generic(
                "ERR INCR option supports a single increment-element pair".to_string(),
            ));
        }
    } else if (i + 1) >= args.len() || (args.len() - i) % 2 != 0 {
        return Err(NexradeError::WrongArity("zadd".to_string()));
    }

    let mut store_db = db.store.db(db_index).write_for(&key);
    let zset = get_or_create_zset(&mut store_db, &key)?;
    // Payload is member bytes only (score is not counted). New members
    // contribute +member.len(); score-only updates contribute 0.
    let mut payload_delta: isize = 0;

    // INCR path: atomic score increment, returns the new score as a bulk
    // string. Only one (score, member) pair is allowed.
    if incr {
        let (delta, _) = parse_score_bound(get_str(args, i, "ZADD")?)?;
        let member = get_bytes_vec(args, i + 1, "ZADD")?;
        let mlen = member.len() as isize;
        let old = zset.score(&member).unwrap_or(0.0);
        let new_score = old + delta;
        let added = zset.insert(member, new_score);
        if added {
            payload_delta += mlen;
        }
        store_db.adjust_live_bytes(payload_delta);
        // Drop the shard lock before notifying waiters.
        drop(store_db);
        db.notify_zset_waiters();
        return Ok(Resp::bulk_str(format_float(new_score)));
    }

    let mut added = 0i64;
    let mut changed = 0i64;

    // Fast path: plain ZADD with no NX/XX/GT/LT — insert does one scan
    // and returns is_new; same-score is a no-op with changed=0.
    let need_existing = nx || xx || gt || lt || ch;
    while i + 1 < args.len() {
        let (score, _) = parse_score_bound(get_str(args, i, "ZADD")?)?;
        let member = get_bytes_vec(args, i + 1, "ZADD")?;
        let mlen = member.len() as isize;
        i += 2;

        if need_existing {
            let existing_score = zset.score(&member);

            if nx && existing_score.is_some() {
                continue;
            }
            if xx && existing_score.is_none() {
                continue;
            }
            if gt {
                if let Some(old) = existing_score {
                    if score <= old {
                        continue;
                    }
                }
            }
            if lt {
                if let Some(old) = existing_score {
                    if score >= old {
                        continue;
                    }
                }
            }

            let is_new = zset.insert(member, score);
            if is_new {
                added += 1;
                payload_delta += mlen;
            } else if existing_score.map(|old| old != score).unwrap_or(false) {
                changed += 1;
            }
        } else if zset.insert(member, score) {
            added += 1;
            payload_delta += mlen;
        }
    }

    store_db.adjust_live_bytes(payload_delta);

    // `added`/`changed` already track every successful insert — no separate
    // mutated flag. Wake BZMPOP only when the zset actually changed.
    if added > 0 || changed > 0 {
        // Release the shard lock before waking waiters so they can re-acquire.
        drop(store_db);
        db.notify_zset_waiters();
    }

    Ok(Resp::int(if ch { added + changed } else { added }))
}

pub async fn cmd_zcard(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 2 {
        return Err(NexradeError::WrongArity("zcard".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZCARD")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::ZSet(z) => Ok(Resp::int(z.len() as i64)),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zscore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("zscore".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZSCORE")?;
    let member = get_bytes_vec(args, 2, "ZSCORE")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &e.value {
            DataType::ZSet(z) => Ok(z
                .score(&member)
                .map(|s| Resp::bulk_str(format_float(s)))
                .unwrap_or(Resp::null())),
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zmscore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("zmscore".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZMSCORE")?;
    let store_db = db.store.db(db_index).read_for(&key);

    let results: Vec<Resp> = (2..args.len())
        .map(|i| {
            let member = get_bytes_vec(args, i, "ZMSCORE").ok()?;
            match store_db.get_ro(&key) {
                Some(e) => match &e.value {
                    DataType::ZSet(z) => z.score(&member).map(|s| Resp::bulk_str(format_float(s))),
                    _ => None,
                },
                None => Some(Resp::null()),
            }
        })
        .map(|v| v.unwrap_or(Resp::null()))
        .collect();

    Ok(Resp::array(results))
}

pub async fn cmd_zincrby(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("zincrby".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZINCRBY")?;
    let (delta, _) = parse_score_bound(get_str(args, 2, "ZINCRBY")?)?;
    let member = get_bytes_vec(args, 3, "ZINCRBY")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let (new_score, payload_delta) = {
        let zset = get_or_create_zset(&mut store_db, &key)?;
        let mlen = member.len() as isize;
        let old = zset.score(&member).unwrap_or(0.0);
        let new_score = old + delta;
        // insert returns is_new; payload grows by member len only for a new
        // member (score is not counted).
        let is_new = zset.insert(member, new_score);
        (new_score, if is_new { mlen } else { 0 })
    };
    store_db.adjust_live_bytes(payload_delta);
    drop(store_db);
    // Score change (or new member) must wake BZMPOP waiters.
    db.notify_zset_waiters();
    Ok(Resp::bulk_str(format_float(new_score)))
}

pub async fn cmd_zrank(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    rank(db, args, db_index, false, "ZRANK").await
}

pub async fn cmd_zrevrank(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    rank(db, args, db_index, true, "ZREVRANK").await
}

async fn rank(db: &Db, args: &[Resp], db_index: usize, rev: bool, cmd: &str) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let key = get_bytes_vec(args, 1, cmd)?;
    let member = get_bytes_vec(args, 2, cmd)?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &e.value {
            DataType::ZSet(z) => match z.rank(&member, rev) {
                Some(rank) => Ok(Resp::int(rank as i64)),
                None => Ok(Resp::null()),
            },
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zrange(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("zrange".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZRANGE")?;
    let start = get_i64(args, 2, "ZRANGE")?;
    let stop = get_i64(args, 3, "ZRANGE")?;
    let withscores = args
        .get(4)
        .and_then(|a| a.as_str())
        .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
        .unwrap_or(false);

    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::ZSet(z) => {
                let entries = z.range_by_rank(start as isize, stop as isize, false);
                let result = format_zrange_result(entries, withscores);
                Ok(Resp::array(result))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zrevrange(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("zrevrange".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZREVRANGE")?;
    let start = get_i64(args, 2, "ZREVRANGE")?;
    let stop = get_i64(args, 3, "ZREVRANGE")?;
    let withscores = args
        .get(4)
        .and_then(|a| a.as_str())
        .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
        .unwrap_or(false);

    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::ZSet(z) => {
                let entries = z.range_by_rank(start as isize, stop as isize, true);
                let result = format_zrange_result(entries, withscores);
                Ok(Resp::array(result))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zrangebyscore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    range_by_score(db, args, db_index, false, "ZRANGEBYSCORE").await
}

pub async fn cmd_zrevrangebyscore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    range_by_score(db, args, db_index, true, "ZREVRANGEBYSCORE").await
}

async fn range_by_score(
    db: &Db,
    args: &[Resp],
    db_index: usize,
    rev: bool,
    cmd: &str,
) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let key = get_bytes_vec(args, 1, cmd)?;
    let (min_str, max_str) = if rev {
        (get_str(args, 3, cmd)?, get_str(args, 2, cmd)?)
    } else {
        (get_str(args, 2, cmd)?, get_str(args, 3, cmd)?)
    };
    let (min, min_excl) = parse_score_bound(min_str)?;
    let (max, max_excl) = parse_score_bound(max_str)?;

    let mut withscores = false;
    let mut offset = 0usize;
    let mut count: Option<usize> = None;

    let mut i = 4;
    while i < args.len() {
        let opt = get_str(args, i, cmd)?.to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            "LIMIT" => {
                offset = get_i64(args, i + 1, cmd)? as usize;
                count = Some(get_i64(args, i + 2, cmd)? as usize);
                i += 3;
            }
            _ => {
                i += 1;
            }
        }
    }

    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::ZSet(z) => {
                let entries = z.range_by_score(min, min_excl, max, max_excl, rev, offset, count);
                let result = format_zrange_result(entries, withscores);
                Ok(Resp::array(result))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zrangebylex(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("zrangebylex".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZRANGEBYLEX")?;
    let min_s = get_str(args, 2, "ZRANGEBYLEX")?;
    let max_s = get_str(args, 3, "ZRANGEBYLEX")?;

    let min_excl = min_s.starts_with('(');
    let max_excl = max_s.starts_with('(');
    // "-" = no lower bound; "+" = no upper bound (None means unbounded).
    // Using an Option avoids the [0xFF;256] sentinel which would silently
    // exclude members whose bytes exceed that fixed-length value.
    let min_b: Option<Vec<u8>> = if min_s == "-" {
        None
    } else {
        Some(min_s.trim_start_matches(['[', '(']).as_bytes().to_vec())
    };
    let max_b: Option<Vec<u8>> = if max_s == "+" {
        None
    } else {
        Some(max_s.trim_start_matches(['[', '(']).as_bytes().to_vec())
    };

    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &e.value {
            DataType::ZSet(z) => {
                let result: Vec<Resp> = z
                    .range_by_lex_members(min_b.as_deref(), min_excl, max_b.as_deref(), max_excl)
                    .into_iter()
                    .map(|m| Resp::bulk(Bytes::from(m)))
                    .collect();
                Ok(Resp::array(result))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zcount(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("zcount".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZCOUNT")?;
    let (min, min_excl) = parse_score_bound(get_str(args, 2, "ZCOUNT")?)?;
    let (max, max_excl) = parse_score_bound(get_str(args, 3, "ZCOUNT")?)?;

    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::ZSet(z) => {
                let count = z
                    .range_by_score(min, min_excl, max, max_excl, false, 0, None)
                    .len() as i64;
                Ok(Resp::int(count))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zlexcount(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("zlexcount".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZLEXCOUNT")?;
    let min_s = get_str(args, 2, "ZLEXCOUNT")?;
    let max_s = get_str(args, 3, "ZLEXCOUNT")?;

    let min_excl = min_s.starts_with('(');
    let max_excl = max_s.starts_with('(');
    let min_b: Option<Vec<u8>> = if min_s == "-" {
        None
    } else {
        Some(min_s.trim_start_matches(['[', '(']).as_bytes().to_vec())
    };
    let max_b: Option<Vec<u8>> = if max_s == "+" {
        None
    } else {
        Some(max_s.trim_start_matches(['[', '(']).as_bytes().to_vec())
    };

    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &e.value {
            DataType::ZSet(z) => {
                let count = z
                    .range_by_lex_members(min_b.as_deref(), min_excl, max_b.as_deref(), max_excl)
                    .len() as i64;
                Ok(Resp::int(count))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zrem(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("zrem".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZREM")?;
    let mut store_db = db.store.db(db_index).write_for(&key);

    let mut emptied = false;
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::ZSet(z) => {
                let mut removed = 0i64;
                let mut delta: isize = 0;
                for i in 2..args.len() {
                    if let Ok(m) = get_bytes_vec(args, i, "ZREM") {
                        let mlen = m.len() as isize;
                        if z.remove(&m).is_some() {
                            removed += 1;
                            delta -= mlen;
                        }
                    }
                }
                payload_delta = delta;
                emptied = z.is_empty();
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

pub async fn cmd_zremrangebyrank(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("zremrangebyrank".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZREMRANGEBYRANK")?;
    let start = get_i64(args, 2, "ZREMRANGEBYRANK")?;
    let stop = get_i64(args, 3, "ZREMRANGEBYRANK")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let mut emptied = false;
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::ZSet(z) => {
                let to_remove: Vec<_> = z
                    .range_by_rank(start as isize, stop as isize, false)
                    .into_iter()
                    .map(|(m, _)| m)
                    .collect();
                let count = to_remove.len() as i64;
                let mut delta: isize = 0;
                for m in to_remove {
                    delta -= m.len() as isize;
                    z.remove(&m);
                }
                payload_delta = delta;
                emptied = z.is_empty();
                Ok(Resp::int(count))
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

pub async fn cmd_zremrangebyscore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("zremrangebyscore".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZREMRANGEBYSCORE")?;
    let (min, min_excl) = parse_score_bound(get_str(args, 2, "ZREMRANGEBYSCORE")?)?;
    let (max, max_excl) = parse_score_bound(get_str(args, 3, "ZREMRANGEBYSCORE")?)?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let mut emptied = false;
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::ZSet(z) => {
                let to_remove: Vec<_> = z
                    .range_by_score(min, min_excl, max, max_excl, false, 0, None)
                    .into_iter()
                    .map(|(m, _)| m)
                    .collect();
                let count = to_remove.len() as i64;
                let mut delta: isize = 0;
                for m in to_remove {
                    delta -= m.len() as isize;
                    z.remove(&m);
                }
                payload_delta = delta;
                emptied = z.is_empty();
                Ok(Resp::int(count))
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

pub async fn cmd_zpopmin(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    zpop(db, args, db_index, false, "ZPOPMIN").await
}

pub async fn cmd_zpopmax(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    zpop(db, args, db_index, true, "ZPOPMAX").await
}

async fn zpop(db: &Db, args: &[Resp], db_index: usize, max: bool, cmd: &str) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let key = get_bytes_vec(args, 1, cmd)?;
    let count = if args.len() >= 3 {
        let n = get_i64(args, 2, cmd)?;
        if n < 0 {
            return Err(NexradeError::Generic(
                "ERR value is not an integer or out of range".to_string(),
            ));
        }
        n as usize
    } else {
        1
    };

    let mut store_db = db.store.db(db_index).write_for(&key);
    let mut emptied = false;
    let mut payload_delta: isize = 0;
    let result = match store_db.get_mut(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &mut e.value {
            DataType::ZSet(z) => {
                let mut delta: isize = 0;
                let mut result = Vec::new();
                for _ in 0..count {
                    if let Some((score, member)) = z.pop_extreme(max) {
                        delta -= member.len() as isize;
                        result.push(Resp::bulk(Bytes::from(member)));
                        result.push(Resp::bulk_str(format_float(score)));
                    } else {
                        break;
                    }
                }
                payload_delta = delta;
                emptied = z.is_empty();
                Ok(Resp::array(result))
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

/// `ZRANDMEMBER key [count [WITHSCORES]]`
///
/// Without count: single bulk (or null). With count: flat array of members
/// (or `[member, score, …]` when WITHSCORES). RESP3 nesting is applied by
/// the connection layer when WITHSCORES is present.
pub async fn cmd_zrandmember(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("zrandmember".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZRANDMEMBER")?;

    // Parse optional count + WITHSCORES. Redis requires count when WITHSCORES
    // is used: `ZRANDMEMBER key WITHSCORES` is a syntax error.
    let mut count: Option<i64> = None;
    let mut withscores = false;
    let mut i = 2;
    if i < args.len() {
        // First optional arg is count (integer). WITHSCORES alone is illegal.
        match get_str(args, i, "ZRANDMEMBER") {
            Ok(s) if s.eq_ignore_ascii_case("WITHSCORES") => {
                return Err(NexradeError::SyntaxError);
            }
            Ok(s) => {
                let n: i64 = s.parse().map_err(|_| NexradeError::NotInteger)?;
                count = Some(n);
                i += 1;
            }
            Err(_) => {}
        }
    }
    if i < args.len() {
        if get_str(args, i, "ZRANDMEMBER")?.eq_ignore_ascii_case("WITHSCORES") {
            withscores = true;
            i += 1;
        } else {
            return Err(NexradeError::SyntaxError);
        }
    }
    if i != args.len() {
        return Err(NexradeError::SyntaxError);
    }

    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Ok(if count.is_some() {
            Resp::array(vec![])
        } else {
            Resp::null()
        }),
        Some(e) => match &e.value {
            DataType::ZSet(z) => {
                // Snapshot (member, score) pairs so WITHSCORES can look up scores
                // without a second pass under the lock.
                let pairs: Vec<(Vec<u8>, f64)> = z.to_pairs();
                if pairs.is_empty() {
                    return Ok(if count.is_some() {
                        Resp::array(vec![])
                    } else {
                        Resp::null()
                    });
                }

                let push_pair = |out: &mut Vec<Resp>, m: &[u8], score: f64| {
                    out.push(Resp::bulk(Bytes::copy_from_slice(m)));
                    if withscores {
                        out.push(Resp::bulk_str(format_float(score)));
                    }
                };

                match count {
                    None => {
                        let idx = pseudo_rand_idx(pairs.len());
                        let (m, s) = &pairs[idx];
                        if withscores {
                            // Unreachable: WITHSCORES without count is rejected
                            // above. Kept for exhaustiveness.
                            Ok(Resp::array(vec![
                                Resp::bulk(Bytes::from(m.clone())),
                                Resp::bulk_str(format_float(*s)),
                            ]))
                        } else {
                            Ok(Resp::bulk(Bytes::from(m.clone())))
                        }
                    }
                    Some(n) => {
                        let mut result = Vec::new();
                        if n < 0 {
                            // Negative count: allow duplicates, return exactly |n|.
                            let take = n.unsigned_abs() as usize;
                            for _ in 0..take {
                                let idx = pseudo_rand_idx(pairs.len());
                                let (m, s) = &pairs[idx];
                                push_pair(&mut result, m, *s);
                            }
                        } else {
                            // Positive count: no duplicates, up to n elements.
                            // Iterate in hash-map order (pseudo-random enough;
                            // matches prior behaviour before WITHSCORES).
                            for (m, s) in pairs.into_iter().take(n as usize) {
                                push_pair(&mut result, &m, s);
                            }
                        }
                        Ok(Resp::array(result))
                    }
                }
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

/// `BZPOPMIN timeout key [key ...]` — blocking ZPOPMIN across keys.
pub async fn cmd_bzpopmin(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    bzpop(db, args, db_index, false, "BZPOPMIN").await
}

/// `BZPOPMAX timeout key [key ...]` — blocking ZPOPMAX across keys.
pub async fn cmd_bzpopmax(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    bzpop(db, args, db_index, true, "BZPOPMAX").await
}

async fn bzpop(db: &Db, args: &[Resp], db_index: usize, max: bool, cmd: &str) -> Result<Resp> {
    // BZPOPMIN/MAX timeout key [key ...] — timeout is last arg (Redis shape).
    if args.len() < 3 {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let timeout_secs = get_f64(args, args.len() - 1, cmd)?;
    let keys: Vec<Vec<u8>> = (1..args.len() - 1)
        .map(|i| get_bytes_vec(args, i, cmd))
        .collect::<Result<_>>()?;

    if let Some(resp) = bzpop_attempt(db, db_index, &keys, max)? {
        return Ok(resp);
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let dur = if timeout_secs == 0.0 {
            std::time::Duration::from_secs(u64::MAX)
        } else {
            std::time::Duration::from_secs_f64(timeout_secs)
        };
        match tokio::time::timeout(dur, async {
            let _parked = db.park_zset_waiter();
            loop {
                // Register for the next wake *before* re-checking emptiness
                // so a producer that notifies between empty-check and park
                // cannot be lost (tokio Notify is edge-triggered).
                let notified = db.zset_chan.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                #[cfg(not(target_arch = "wasm32"))]
                let _permit = match db.persistence.enter_mutation() {
                    Some(p) => p,
                    None => {
                        return Err(NexradeError::Generic(
                            "MISCONF persistence is quiescing; writes are temporarily disabled"
                                .to_string(),
                        ))
                    }
                };
                if let Some(resp) = bzpop_attempt(db, db_index, &keys, max)? {
                    return Ok::<Resp, NexradeError>(resp);
                }
                notified.await;
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
        let _ = timeout_secs;
        Ok(Resp::null_array())
    }
}

/// Try to pop one member from the first non-empty zset among `keys`.
/// Returns `[key, member, score]` on success, matching Redis BZPOPMIN/MAX.
fn bzpop_attempt(db: &Db, db_index: usize, keys: &[Vec<u8>], max: bool) -> Result<Option<Resp>> {
    for key in keys {
        let mut store_db = db.store.db(db_index).write_for(key);
        if let Some(entry) = store_db.get_mut(key) {
            if let DataType::ZSet(z) = &mut entry.value {
                if let Some((score, member)) = z.pop_extreme(max) {
                    let emptied = z.is_empty();
                    store_db.adjust_live_bytes(-(member.len() as isize));
                    if emptied {
                        store_db.remove_empty_key(key);
                    }
                    return Ok(Some(Resp::array(vec![
                        Resp::bulk(Bytes::copy_from_slice(key)),
                        Resp::bulk(Bytes::from(member)),
                        Resp::bulk_str(format_float(score)),
                    ])));
                }
            } else {
                return Err(NexradeError::WrongType);
            }
        }
    }
    Ok(None)
}

#[derive(Clone, Copy, PartialEq)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

fn parse_aggregate_weights(
    args: &[Resp],
    start: usize,
    num_keys: usize,
    cmd: &str,
) -> Result<(Vec<f64>, Aggregate)> {
    let mut weights = vec![1.0f64; num_keys];
    let mut agg = Aggregate::Sum;
    let mut i = start;
    while i < args.len() {
        let opt = get_str(args, i, cmd)?.to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                i += 1;
                for w in weights.iter_mut() {
                    let (v, _) = parse_score_bound(get_str(args, i, cmd)?)?;
                    *w = v;
                    i += 1;
                }
            }
            "AGGREGATE" => {
                i += 1;
                agg = match get_str(args, i, cmd)?.to_uppercase().as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return Err(NexradeError::SyntaxError),
                };
                i += 1;
            }
            _ => return Err(NexradeError::SyntaxError),
        }
    }
    Ok((weights, agg))
}

/// Same as `parse_aggregate_weights` but also accepts WITHSCORES at the end —
/// used by the non-store ZINTER / ZUNION variants.
fn parse_weights_agg_withscores(
    args: &[Resp],
    start: usize,
    num_keys: usize,
    cmd: &str,
) -> Result<(Vec<f64>, Aggregate, bool)> {
    let mut weights = vec![1.0f64; num_keys];
    let mut agg = Aggregate::Sum;
    let mut withscores = false;
    let mut i = start;
    while i < args.len() {
        let opt = get_str(args, i, cmd)?.to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                i += 1;
                for w in weights.iter_mut() {
                    let (v, _) = parse_score_bound(get_str(args, i, cmd)?)?;
                    *w = v;
                    i += 1;
                }
            }
            "AGGREGATE" => {
                i += 1;
                agg = match get_str(args, i, cmd)?.to_uppercase().as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return Err(NexradeError::SyntaxError),
                };
                i += 1;
            }
            "WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            _ => return Err(NexradeError::SyntaxError),
        }
    }
    Ok((weights, agg, withscores))
}

fn parse_numkeys_for_set_op(args: &[Resp], idx: usize, cmd: &str) -> Result<usize> {
    let n = get_i64(args, idx, cmd)?;
    if n <= 0 {
        return Err(NexradeError::Generic(format!(
            "ERR at least 1 input key is needed for {cmd}"
        )));
    }
    Ok(n as usize)
}

/// Convert a `ZSetData` to a RESP array, optionally with scores interleaved.
fn zset_to_array(z: &crate::types::ZSetData, withscores: bool) -> Resp {
    // Iterate in score-ascending order (Redis default).
    let sorted = z.to_pairs_sorted();
    let mut out: Vec<Resp> = Vec::with_capacity(sorted.len() * (if withscores { 2 } else { 1 }));
    for (member, score) in sorted {
        out.push(Resp::bulk(Bytes::from(member)));
        if withscores {
            out.push(Resp::bulk_str(format_float(score)));
        }
    }
    Resp::array(out)
}

/// Build the union of the given zsets with the supplied weights and aggregator.
fn compute_zunion_result(
    db: &Db,
    db_index: usize,
    keys: &[Vec<u8>],
    weights: &[f64],
    agg: Aggregate,
) -> Result<ZSetData> {
    let sdb = db.store.db(db_index);
    let mut result = ZSetData::new();
    for (i, key) in keys.iter().enumerate() {
        if let Some(e) = sdb.write_for(key).get(key) {
            if let DataType::ZSet(z) = &e.value {
                let weight = weights.get(i).copied().unwrap_or(1.0);
                for (member, score) in z.to_pairs() {
                    let weighted = score * weight;
                    let new_score = apply_aggregate(agg, result.score(&member), weighted);
                    result.insert(member, new_score);
                }
            }
        }
    }
    Ok(result)
}

/// Build the intersection of the given zsets. Returns an empty zset if any
/// input key is missing.
fn compute_zinter_result(
    db: &Db,
    db_index: usize,
    keys: &[Vec<u8>],
    weights: &[f64],
    agg: Aggregate,
) -> Result<ZSetData> {
    let sdb = db.store.db(db_index);
    let mut sets: Vec<ZSetData> = Vec::new();
    for key in keys.iter() {
        match sdb.write_for(key).get(key) {
            None => return Ok(ZSetData::new()),
            Some(e) => match &e.value {
                DataType::ZSet(z) => sets.push(z.clone()),
                _ => return Err(NexradeError::WrongType),
            },
        }
    }
    let mut result = ZSetData::new();
    if sets.is_empty() {
        return Ok(result);
    }
    for (member, score) in sets[0].to_pairs() {
        let mut acc = score * weights.first().copied().unwrap_or(1.0);
        let mut in_all = true;
        for (j, other) in sets[1..].iter().enumerate() {
            if let Some(s) = other.score(&member) {
                let weighted = s * weights.get(j + 1).copied().unwrap_or(1.0);
                acc = apply_aggregate(agg, Some(acc), weighted);
            } else {
                in_all = false;
                break;
            }
        }
        if in_all {
            result.insert(member, acc);
        }
    }
    Ok(result)
}

/// Build `keys[0] - keys[1] - ... - keys[n]`.
fn compute_zdiff_result(db: &Db, db_index: usize, keys: &[Vec<u8>]) -> Result<ZSetData> {
    let sdb = db.store.db(db_index);
    let first = match keys.first() {
        Some(k) => k,
        None => return Ok(ZSetData::new()),
    };
    let mut result = match sdb.write_for(first).get(first) {
        Some(e) => match &e.value {
            DataType::ZSet(z) => z.clone(),
            _ => return Err(NexradeError::WrongType),
        },
        None => return Ok(ZSetData::new()),
    };
    for key in keys.iter().skip(1) {
        if let Some(e) = sdb.write_for(key).get(key) {
            if let DataType::ZSet(z) = &e.value {
                for member in z.member_keys() {
                    result.remove(&member);
                }
            }
        }
    }
    Ok(result)
}

fn apply_aggregate(agg: Aggregate, existing: Option<f64>, new: f64) -> f64 {
    match (agg, existing) {
        (_, None) => new,
        (Aggregate::Sum, Some(e)) => e + new,
        (Aggregate::Min, Some(e)) => e.min(new),
        (Aggregate::Max, Some(e)) => e.max(new),
    }
}

pub async fn cmd_zunionstore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("zunionstore".to_string()));
    }
    let dst = get_bytes_vec(args, 1, "ZUNIONSTORE")?;
    let num_keys = parse_numkeys_for_set_op(args, 2, "ZUNIONSTORE")?;
    let keys_start = 3;
    let keys: Vec<Vec<u8>> = (keys_start..keys_start + num_keys)
        .map(|i| get_bytes_vec(args, i, "ZUNIONSTORE"))
        .collect::<Result<_>>()?;
    let (weights, agg) =
        parse_aggregate_weights(args, keys_start + num_keys, num_keys, "ZUNIONSTORE")?;

    let result = compute_zunion_result(db, db_index, &keys, &weights, agg)?;

    let count = result.len() as i64;
    let sdb = db.store.db(db_index);
    let mut dst_shard = sdb.write_for(&dst);
    dst_shard.insert(dst, Entry::new(DataType::ZSet(result)));
    drop(dst_shard);
    if count > 0 {
        db.notify_zset_waiters();
    }
    Ok(Resp::int(count))
}

/// `ZUNION numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]`
///
/// Non-store variant — returns the resulting zset members (optionally with
/// scores) as an array instead of writing them to a destination key.
pub async fn cmd_zunion(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("zunion".to_string()));
    }
    let num_keys = parse_numkeys_for_set_op(args, 1, "ZUNION")?;
    let keys_start = 2;
    let keys: Vec<Vec<u8>> = (keys_start..keys_start + num_keys)
        .map(|i| get_bytes_vec(args, i, "ZUNION"))
        .collect::<Result<_>>()?;
    let (weights, agg, withscores) =
        parse_weights_agg_withscores(args, keys_start + num_keys, num_keys, "ZUNION")?;

    let result = compute_zunion_result(db, db_index, &keys, &weights, agg)?;
    Ok(zset_to_array(&result, withscores))
}

pub async fn cmd_zinterstore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("zinterstore".to_string()));
    }
    let dst = get_bytes_vec(args, 1, "ZINTERSTORE")?;
    let num_keys = parse_numkeys_for_set_op(args, 2, "ZINTERSTORE")?;
    let keys_start = 3;
    let keys: Vec<Vec<u8>> = (keys_start..keys_start + num_keys)
        .map(|i| get_bytes_vec(args, i, "ZINTERSTORE"))
        .collect::<Result<_>>()?;
    let (weights, agg) =
        parse_aggregate_weights(args, keys_start + num_keys, num_keys, "ZINTERSTORE")?;

    let result = compute_zinter_result(db, db_index, &keys, &weights, agg)?;

    let count = result.len() as i64;
    let sdb = db.store.db(db_index);
    let mut dst_shard = sdb.write_for(&dst);
    dst_shard.insert(dst, Entry::new(DataType::ZSet(result)));
    drop(dst_shard);
    if count > 0 {
        db.notify_zset_waiters();
    }
    Ok(Resp::int(count))
}

/// `ZINTER numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]`
///
/// Non-store variant of ZINTERSTORE.
pub async fn cmd_zinter(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("zinter".to_string()));
    }
    let num_keys = parse_numkeys_for_set_op(args, 1, "ZINTER")?;
    let keys_start = 2;
    let keys: Vec<Vec<u8>> = (keys_start..keys_start + num_keys)
        .map(|i| get_bytes_vec(args, i, "ZINTER"))
        .collect::<Result<_>>()?;
    let (weights, agg, withscores) =
        parse_weights_agg_withscores(args, keys_start + num_keys, num_keys, "ZINTER")?;

    let result = compute_zinter_result(db, db_index, &keys, &weights, agg)?;
    Ok(zset_to_array(&result, withscores))
}

/// `ZSCAN key cursor [MATCH pattern] [COUNT count]`
pub async fn cmd_zscan(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("zscan".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZSCAN")?;
    let cursor: Option<u64> = get_str(args, 2, "ZSCAN").ok().and_then(decode_scan_cursor);

    let mut pattern: Option<Vec<u8>> = None;
    let mut count: usize = 10;
    let mut i = 3;
    while i < args.len() {
        let opt = get_str(args, i, "ZSCAN")?.to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                pattern = Some(get_bytes_vec(args, i + 1, "ZSCAN")?);
                i += 2;
            }
            "COUNT" => {
                let n = get_i64(args, i + 1, "ZSCAN")?;
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
            DataType::ZSet(z) => {
                let pat = pattern.unwrap_or_else(|| b"*".to_vec());
                // Member-space cursor, see `scan_cursor_token`. Ordering by
                // the token makes the cursor a position in the walked order,
                // so a removed boundary member cannot shift the resume point.
                //
                // 1.3.0: bounded-heap page selection replaces sorting the whole
                // zset on every page, which made a complete walk O(n² log n).
                // Order and cursor semantics are unchanged.
                // Borrowed member slices in; only the page is copied.
                let page = scan_select_page(
                    z.iter_pairs_ref().filter(|(m, _)| glob_match(&pat, m)),
                    cursor,
                    count,
                    |p: &(&[u8], f64)| p.0,
                );
                let mut items = Vec::with_capacity(page.items.len() * 2);
                for (m, score) in &page.items {
                    items.push(Resp::bulk(Bytes::copy_from_slice(m)));
                    items.push(Resp::bulk_str(format_float(*score)));
                }
                Ok(Resp::array(vec![
                    Resp::bulk_str(page.next_cursor.to_string()),
                    Resp::array(items),
                ]))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

fn format_zrange_result(entries: Vec<(Vec<u8>, f64)>, withscores: bool) -> Vec<Resp> {
    let mut result = Vec::new();
    for (member, score) in entries {
        result.push(Resp::bulk(Bytes::from(member)));
        if withscores {
            result.push(Resp::bulk_str(format_float(score)));
        }
    }
    result
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

// ── ZMPOP / BZMPOP ───────────────────────────────────────────────────────────

/// `ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]`
///
/// Pops `count` members from the first non-empty sorted set among the given
/// keys. Returns `[key, [[member, score], ...]]` or nil array if all empty.
pub async fn cmd_zmpop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    zmpop_once(db, args, db_index).await
}

/// `BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]`
pub async fn cmd_bzmpop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 5 {
        return Err(NexradeError::WrongArity("bzmpop".to_string()));
    }
    let timeout_secs = get_f64(args, 1, "BZMPOP")?;
    let numkeys = parse_numkeys_z(args, 2, "BZMPOP")?;
    let (keys, rest_start) = parse_keys_z(args, 3, numkeys, "BZMPOP")?;
    let (min, count) = parse_zmpop_tail(&args[rest_start..], "BZMPOP")?;

    if let Some(resp) = zmpop_attempt(db, db_index, &keys, min, count)? {
        return Ok(resp);
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let dur = if timeout_secs == 0.0 {
            std::time::Duration::from_secs(u64::MAX)
        } else {
            std::time::Duration::from_secs_f64(timeout_secs)
        };
        // Wait on the dedicated zset notify so ZADD / ZINCRBY / Z*STORE actually
        // wake us. list_chan only fires on list/stream pushes.
        match tokio::time::timeout(dur, async {
            let _parked = db.park_zset_waiter();
            loop {
                // Register before re-check (see bzpop).
                let notified = db.zset_chan.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                #[cfg(not(target_arch = "wasm32"))]
                let _permit = match db.persistence.enter_mutation() {
                    Some(p) => p,
                    None => {
                        return Err(NexradeError::Generic(
                            "MISCONF persistence is quiescing; writes are temporarily disabled"
                                .to_string(),
                        ))
                    }
                };
                if let Some(resp) = zmpop_attempt(db, db_index, &keys, min, count)? {
                    return Ok::<Resp, NexradeError>(resp);
                }
                notified.await;
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
        let _ = (timeout_secs, min, count);
        Ok(Resp::null_array())
    }
}

async fn zmpop_once(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("zmpop".to_string()));
    }
    let numkeys = parse_numkeys_z(args, 1, "ZMPOP")?;
    let (keys, rest_start) = parse_keys_z(args, 2, numkeys, "ZMPOP")?;
    let (min, count) = parse_zmpop_tail(&args[rest_start..], "ZMPOP")?;
    Ok(zmpop_attempt(db, db_index, &keys, min, count)?.unwrap_or_else(Resp::null_array))
}

fn zmpop_attempt(
    db: &Db,
    db_index: usize,
    keys: &[Vec<u8>],
    min: bool,
    count: usize,
) -> Result<Option<Resp>> {
    for key in keys {
        let mut store_db = db.store.db(db_index).write_for(key);
        if let Some(entry) = store_db.get_mut(key) {
            if let DataType::ZSet(z) = &mut entry.value {
                if z.is_empty() {
                    continue;
                }
                let mut delta: isize = 0;
                let mut popped: Vec<Resp> = Vec::with_capacity(count);
                for _ in 0..count {
                    // min=true → pop lowest; min=false → pop highest
                    let Some((score, member)) = z.pop_extreme(!min) else {
                        break;
                    };
                    delta -= member.len() as isize;
                    popped.push(Resp::array(vec![
                        Resp::bulk(bytes::Bytes::from(member)),
                        Resp::bulk_str(format_float(score)),
                    ]));
                }
                if popped.is_empty() {
                    continue;
                }
                let emptied = z.is_empty();
                store_db.adjust_live_bytes(delta);
                if emptied {
                    store_db.remove_empty_key(key);
                }
                return Ok(Some(Resp::array(vec![
                    Resp::bulk(bytes::Bytes::copy_from_slice(key)),
                    Resp::array(popped),
                ])));
            }
        }
    }
    Ok(None)
}

fn parse_numkeys_z(args: &[Resp], idx: usize, cmd: &str) -> Result<usize> {
    let n = get_i64(args, idx, cmd)?;
    if n <= 0 {
        return Err(NexradeError::Generic(
            "numkeys should be greater than 0".to_string(),
        ));
    }
    Ok(n as usize)
}

fn parse_keys_z(args: &[Resp], idx: usize, n: usize, cmd: &str) -> Result<(Vec<Vec<u8>>, usize)> {
    if args.len() < idx + n {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let keys: Vec<Vec<u8>> = (idx..idx + n)
        .map(|i| get_bytes_vec(args, i, cmd))
        .collect::<Result<_>>()?;
    Ok((keys, idx + n))
}

fn parse_zmpop_tail(args: &[Resp], cmd: &str) -> Result<(bool, usize)> {
    if args.is_empty() {
        return Err(NexradeError::WrongArity(cmd.to_string()));
    }
    let dir = get_str(args, 0, cmd)?.to_ascii_uppercase();
    let min = match dir.as_str() {
        "MIN" => true,
        "MAX" => false,
        _ => {
            return Err(NexradeError::Generic("syntax error".to_string()));
        }
    };
    let mut count = 1usize;
    let mut i = 1;
    if i < args.len() && get_str(args, i, cmd)?.eq_ignore_ascii_case("COUNT") {
        i += 1;
        if i >= args.len() {
            return Err(NexradeError::WrongArity(cmd.to_string()));
        }
        let n = get_i64(args, i, cmd)?;
        if n < 0 {
            return Err(NexradeError::Generic("value is out of range".to_string()));
        }
        count = n as usize;
        i += 1;
    }
    if i != args.len() {
        return Err(NexradeError::Generic("syntax error".to_string()));
    }
    Ok((min, count))
}

/// `ZDIFFSTORE dst numkeys key [key ...]`
pub async fn cmd_zdiffstore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("zdiffstore".to_string()));
    }
    let dst = get_bytes_vec(args, 1, "ZDIFFSTORE")?;
    let num_keys = parse_numkeys_for_set_op(args, 2, "ZDIFFSTORE")?;
    let keys_start = 3;
    let keys: Vec<Vec<u8>> = (keys_start..keys_start + num_keys)
        .map(|i| get_bytes_vec(args, i, "ZDIFFSTORE"))
        .collect::<Result<_>>()?;
    if args.len() != keys_start + num_keys {
        return Err(NexradeError::Generic("syntax error".to_string()));
    }

    let result = compute_zdiff_result(db, db_index, &keys)?;
    let count = result.len() as i64;
    let sdb = db.store.db(db_index);
    let mut dst_shard = sdb.write_for(&dst);
    dst_shard.insert(dst, Entry::new(DataType::ZSet(result)));
    drop(dst_shard);
    if count > 0 {
        db.notify_zset_waiters();
    }
    Ok(Resp::int(count))
}

/// `ZDIFF numkeys key [key ...] [WITHSCORES]`
///
/// Non-store variant — returns members (optionally with scores) of the set
/// difference.
pub async fn cmd_zdiff(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("zdiff".to_string()));
    }
    let num_keys = parse_numkeys_for_set_op(args, 1, "ZDIFF")?;
    let keys_start = 2;
    let keys: Vec<Vec<u8>> = (keys_start..keys_start + num_keys)
        .map(|i| get_bytes_vec(args, i, "ZDIFF"))
        .collect::<Result<_>>()?;

    let mut withscores = false;
    if args.len() > keys_start + num_keys {
        let opt = get_str(args, keys_start + num_keys, "ZDIFF")?.to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => withscores = true,
            _ => return Err(NexradeError::Generic("syntax error".to_string())),
        }
        if args.len() != keys_start + num_keys + 1 {
            return Err(NexradeError::Generic("syntax error".to_string()));
        }
    } else if args.len() != keys_start + num_keys {
        return Err(NexradeError::Generic("syntax error".to_string()));
    }

    let result = compute_zdiff_result(db, db_index, &keys)?;
    Ok(zset_to_array(&result, withscores))
}

/// `ZINTERCARD numkeys key [key ...] [LIMIT limit]`
///
/// Returns the cardinality of the intersection of the given sorted sets.
/// `LIMIT limit` caps the count early (without performing the full
/// computation).
pub async fn cmd_zintercard(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("zintercard".to_string()));
    }
    let num_keys = parse_numkeys_for_set_op(args, 1, "ZINTERCARD")?;
    let keys_start = 2;
    let keys: Vec<Vec<u8>> = (keys_start..keys_start + num_keys)
        .map(|i| get_bytes_vec(args, i, "ZINTERCARD"))
        .collect::<Result<_>>()?;

    let mut limit: Option<usize> = None;
    if args.len() > keys_start + num_keys {
        let opt = get_str(args, keys_start + num_keys, "ZINTERCARD")?.to_uppercase();
        if opt != "LIMIT" {
            return Err(NexradeError::Generic("syntax error".to_string()));
        }
        // LIMIT 0 means "unlimited" (matches Redis and what redis-py sends).
        let raw_limit = get_i64(args, keys_start + num_keys + 1, "ZINTERCARD")? as usize;
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
    // Short-circuit on the smallest input set so we can bound the work.
    let mut sets: Vec<ZSetData> = Vec::new();
    let mut smallest = usize::MAX;
    let mut smallest_idx = 0;
    for (i, key) in keys.iter().enumerate() {
        let s = match sdb.write_for(key).get(key) {
            None => return Ok(Resp::int(0)),
            Some(e) => match &e.value {
                DataType::ZSet(z) => {
                    if z.len() < smallest {
                        smallest = z.len();
                        smallest_idx = i;
                    }
                    z.clone()
                }
                _ => return Err(NexradeError::WrongType),
            },
        };
        sets.push(s);
    }
    if sets.is_empty() {
        return Ok(Resp::int(0));
    }

    // Iterate the smallest set, filtering by membership in all others.
    let mut count = 0i64;
    let mut other_idx = Vec::with_capacity(sets.len());
    for (i, _) in sets.iter().enumerate() {
        if i != smallest_idx {
            other_idx.push(i);
        }
    }
    let base = &sets[smallest_idx];
    for member in base.member_keys() {
        if other_idx.iter().all(|&i| sets[i].score(&member).is_some()) {
            count += 1;
            if let Some(l) = limit {
                if count as usize >= l {
                    break;
                }
            }
        }
    }

    Ok(Resp::int(count))
}

/// `ZRANGESTORE dst src start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count]`
///
/// Writes the result of a range query on `src` into `dst` as a fresh sorted
/// set (overwrites if it exists). Returns the number of elements written.
pub async fn cmd_zrangestore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 5 {
        return Err(NexradeError::WrongArity("zrangestore".to_string()));
    }
    let dst = get_bytes_vec(args, 1, "ZRANGESTORE")?;
    let src = get_bytes_vec(args, 2, "ZRANGESTORE")?;
    let start_str = get_str(args, 3, "ZRANGESTORE")?;
    let stop_str = get_str(args, 4, "ZRANGESTORE")?;

    // Parse optional modifiers: BYSCORE | BYLEX, REV, LIMIT offset count.
    let mut byscore = false;
    let mut bylex = false;
    let mut rev = false;
    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut i = 5;
    while i < args.len() {
        let opt = get_str(args, i, "ZRANGESTORE")?.to_ascii_uppercase();
        match opt.as_str() {
            "BYSCORE" => {
                byscore = true;
                i += 1;
            }
            "BYLEX" => {
                bylex = true;
                i += 1;
            }
            "REV" => {
                rev = true;
                i += 1;
            }
            "LIMIT" => {
                offset = get_i64(args, i + 1, "ZRANGESTORE")? as usize;
                count = Some(get_i64(args, i + 2, "ZRANGESTORE")? as usize);
                i += 3;
            }
            _ => {
                return Err(NexradeError::Generic("syntax error".to_string()));
            }
        }
    }
    if byscore && bylex {
        return Err(NexradeError::Generic("syntax error".to_string()));
    }

    // Read entries from src.
    let entries: Vec<(Vec<u8>, f64)> = {
        let store_db = db.store.db(db_index).read_for(&src);
        match store_db.get_ro(&src) {
            None => vec![],
            Some(e) => match &e.value {
                DataType::ZSet(z) => {
                    if byscore {
                        let (min, min_excl) = parse_score_bound(start_str)?;
                        let (max, max_excl) = parse_score_bound(stop_str)?;
                        z.range_by_score(min, min_excl, max, max_excl, rev, offset, count)
                    } else if bylex {
                        range_by_lex(z, start_str, stop_str, rev, offset, count)
                    } else {
                        let start = start_str.parse::<isize>().map_err(|_| {
                            NexradeError::Generic(
                                "ERR value is not an integer or out of range".to_string(),
                            )
                        })?;
                        let stop = stop_str.parse::<isize>().map_err(|_| {
                            NexradeError::Generic(
                                "ERR value is not an integer or out of range".to_string(),
                            )
                        })?;
                        z.range_by_rank(start, stop, rev)
                    }
                }
                _ => return Err(NexradeError::WrongType),
            },
        }
    };

    // Write to dst (overwrite).
    let mut dst_shard = db.store.db(db_index).write_for(&dst);
    let mut new_z = ZSetData::new();
    for (member, score) in &entries {
        new_z.insert(member.clone(), *score);
    }
    let count = new_z.len() as i64;
    dst_shard.insert(dst, Entry::new(DataType::ZSet(new_z)));
    drop(dst_shard);
    if count > 0 {
        db.notify_zset_waiters();
    }

    Ok(Resp::int(count))
}

fn range_by_lex(
    z: &crate::types::ZSetData,
    min_s: &str,
    max_s: &str,
    rev: bool,
    offset: usize,
    count: Option<usize>,
) -> Vec<(Vec<u8>, f64)> {
    let min_excl = min_s.starts_with('(');
    let max_excl = max_s.starts_with('(');
    let min_bytes: Option<Vec<u8>> = if min_s == "-" {
        None
    } else {
        Some(min_s.trim_start_matches(['[', '(']).as_bytes().to_vec())
    };
    let max_bytes: Option<Vec<u8>> = if max_s == "+" {
        None
    } else {
        Some(max_s.trim_start_matches(['[', '(']).as_bytes().to_vec())
    };
    // Members filtered by lex range, then paired with scores.
    let members = z.range_by_lex_members(
        min_bytes.as_deref(),
        min_excl,
        max_bytes.as_deref(),
        max_excl,
    );
    let mut out: Vec<(Vec<u8>, f64)> = members
        .into_iter()
        .filter_map(|m| z.score(&m).map(|s| (m, s)))
        .collect();
    if rev {
        out.reverse();
    }
    out.into_iter()
        .skip(offset)
        .take(count.unwrap_or(usize::MAX))
        .collect()
}
