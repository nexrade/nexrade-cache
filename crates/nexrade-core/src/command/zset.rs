//! Sorted Set command handlers.

use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::command::get_f64;

use crate::command::string::format_float;
use crate::command::{get_bytes_vec, get_i64, get_str};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::Entry;
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

    // INCR path: atomic score increment, returns the new score as a bulk
    // string. Only one (score, member) pair is allowed.
    if incr {
        let (delta, _) = parse_score_bound(get_str(args, i, "ZADD")?)?;
        let member = get_bytes_vec(args, i + 1, "ZADD")?;
        let old = zset.score(&member).unwrap_or(0.0);
        let new_score = old + delta;
        zset.insert(member, new_score);
        return Ok(Resp::bulk_str(format_float(new_score)));
    }

    let mut added = 0i64;
    let mut changed = 0i64;

    while i + 1 < args.len() {
        let (score, _) = parse_score_bound(get_str(args, i, "ZADD")?)?;
        let member = get_bytes_vec(args, i + 1, "ZADD")?;
        i += 2;

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
        } else if existing_score.map(|old| old != score).unwrap_or(false) {
            changed += 1;
        }
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
    let zset = get_or_create_zset(&mut store_db, &key)?;

    let old = zset.score(&member).unwrap_or(0.0);
    let new_score = old + delta;
    zset.insert(member, new_score);
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
            DataType::ZSet(z) => {
                let score = match z.score(&member) {
                    None => return Ok(Resp::null()),
                    Some(s) => s,
                };
                let rank = z
                    .by_score
                    .keys()
                    .position(|(s, m)| s.0 == score && m == &member)
                    .ok_or_else(|| {
                        NexradeError::Generic(
                        "ERR internal: member found in members map but missing from score index"
                            .to_string(),
                    )
                    })?;
                let rank = if rev { z.len() - rank - 1 } else { rank };
                Ok(Resp::int(rank as i64))
            }
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
                    .by_score
                    .keys()
                    .map(|(_, m)| m)
                    .filter(|m| {
                        let after_min = match &min_b {
                            None => true,
                            Some(b) => {
                                if min_excl {
                                    m.as_slice() > b.as_slice()
                                } else {
                                    m.as_slice() >= b.as_slice()
                                }
                            }
                        };
                        let before_max = match &max_b {
                            None => true,
                            Some(b) => {
                                if max_excl {
                                    m.as_slice() < b.as_slice()
                                } else {
                                    m.as_slice() <= b.as_slice()
                                }
                            }
                        };
                        after_min && before_max
                    })
                    .map(|m| Resp::bulk(Bytes::from(m.clone())))
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
                    .by_score
                    .keys()
                    .map(|(_, m)| m)
                    .filter(|m| {
                        let after_min = match &min_b {
                            None => true,
                            Some(b) => {
                                if min_excl {
                                    m.as_slice() > b.as_slice()
                                } else {
                                    m.as_slice() >= b.as_slice()
                                }
                            }
                        };
                        let before_max = match &max_b {
                            None => true,
                            Some(b) => {
                                if max_excl {
                                    m.as_slice() < b.as_slice()
                                } else {
                                    m.as_slice() <= b.as_slice()
                                }
                            }
                        };
                        after_min && before_max
                    })
                    .count() as i64;
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

    match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::ZSet(z) => {
                let mut removed = 0i64;
                for i in 2..args.len() {
                    if let Ok(m) = get_bytes_vec(args, i, "ZREM") {
                        if z.remove(&m).is_some() {
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

pub async fn cmd_zremrangebyrank(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("zremrangebyrank".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZREMRANGEBYRANK")?;
    let start = get_i64(args, 2, "ZREMRANGEBYRANK")?;
    let stop = get_i64(args, 3, "ZREMRANGEBYRANK")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::ZSet(z) => {
                let to_remove: Vec<_> = z
                    .range_by_rank(start as isize, stop as isize, false)
                    .into_iter()
                    .map(|(m, _)| m)
                    .collect();
                let count = to_remove.len() as i64;
                for m in to_remove {
                    z.remove(&m);
                }
                Ok(Resp::int(count))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zremrangebyscore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("zremrangebyscore".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZREMRANGEBYSCORE")?;
    let (min, min_excl) = parse_score_bound(get_str(args, 2, "ZREMRANGEBYSCORE")?)?;
    let (max, max_excl) = parse_score_bound(get_str(args, 3, "ZREMRANGEBYSCORE")?)?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    match store_db.get_mut(&key) {
        None => Ok(Resp::int(0)),
        Some(e) => match &mut e.value {
            DataType::ZSet(z) => {
                let to_remove: Vec<_> = z
                    .range_by_score(min, min_excl, max, max_excl, false, 0, None)
                    .into_iter()
                    .map(|(m, _)| m)
                    .collect();
                let count = to_remove.len() as i64;
                for m in to_remove {
                    z.remove(&m);
                }
                Ok(Resp::int(count))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
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
    match store_db.get_mut(&key) {
        None => Ok(Resp::array(vec![])),
        Some(e) => match &mut e.value {
            DataType::ZSet(z) => {
                let mut result = Vec::new();
                for _ in 0..count {
                    let entry = if max {
                        z.by_score.keys().next_back().cloned()
                    } else {
                        z.by_score.keys().next().cloned()
                    };
                    if let Some((score, member)) = entry {
                        z.remove(&member);
                        result.push(Resp::bulk(Bytes::from(member)));
                        result.push(Resp::bulk_str(format_float(score.0)));
                    } else {
                        break;
                    }
                }
                Ok(Resp::array(result))
            }
            _ => Err(NexradeError::WrongType),
        },
    }
}

pub async fn cmd_zrandmember(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("zrandmember".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZRANDMEMBER")?;
    let count = if args.len() >= 3 {
        Some(get_i64(args, 2, "ZRANDMEMBER")?)
    } else {
        None
    };

    let store_db = db.store.db(db_index).read_for(&key);
    match store_db.get_ro(&key) {
        None => Ok(Resp::null()),
        Some(e) => match &e.value {
            DataType::ZSet(z) => {
                let members: Vec<_> = z.members.keys().cloned().collect();
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
    use std::collections::BTreeMap;
    // Iterate in score-ascending order (Redis default).
    let mut sorted: BTreeMap<(OrderedFloat<f64>, Vec<u8>), ()> = BTreeMap::new();
    for (m, s) in &z.members {
        sorted.insert((*s, m.clone()), ());
    }
    let mut out: Vec<Resp> = Vec::with_capacity(sorted.len() * (if withscores { 2 } else { 1 }));
    for (score, member) in sorted.keys() {
        out.push(Resp::bulk(Bytes::from(member.clone())));
        if withscores {
            out.push(Resp::bulk_str(format_float(score.0)));
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
                for (member, &score) in &z.members {
                    let weighted = score.0 * weight;
                    let new_score = apply_aggregate(agg, result.score(member), weighted);
                    result.insert(member.clone(), new_score);
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
    for (member, &score) in &sets[0].members {
        let mut acc = score.0 * weights.first().copied().unwrap_or(1.0);
        let mut in_all = true;
        for (j, other) in sets[1..].iter().enumerate() {
            if let Some(s) = other.score(member) {
                let weighted = s * weights.get(j + 1).copied().unwrap_or(1.0);
                acc = apply_aggregate(agg, Some(acc), weighted);
            } else {
                in_all = false;
                break;
            }
        }
        if in_all {
            result.insert(member.clone(), acc);
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
                for member in z.members.keys() {
                    result.members.remove(member);
                    if let Some(&s) = z.members.get(member) {
                        result.by_score.remove(&(s, member.clone()));
                    }
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

pub async fn cmd_zscan(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("zscan".to_string()));
    }
    let key = get_bytes_vec(args, 1, "ZSCAN")?;
    let store_db = db.store.db(db_index).read_for(&key);

    match store_db.get_ro(&key) {
        None => Ok(Resp::array(vec![Resp::bulk_str("0"), Resp::array(vec![])])),
        Some(e) => match &e.value {
            DataType::ZSet(z) => {
                let mut items = Vec::new();
                for (member, &score) in &z.members {
                    items.push(Resp::bulk(Bytes::from(member.clone())));
                    items.push(Resp::bulk_str(format!("{}", score.0)));
                }
                Ok(Resp::array(vec![Resp::bulk_str("0"), Resp::array(items)]))
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
        // ZMPOP is rare enough that we just poll on the generic list_notify.
        // For better semantics, we'd want a dedicated zset notify, but list_notify
        // wakes on any data change which is acceptable here.
        match tokio::time::timeout(dur, async {
            loop {
                db.list_notify.notified().await;
                if let Some(resp) = zmpop_attempt(db, db_index, &keys, min, count)? {
                    return Ok::<Resp, NexradeError>(resp);
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
    use std::collections::BTreeSet;
    for key in keys {
        let mut store_db = db.store.db(db_index).write_for(key);
        if let Some(entry) = store_db.get_mut(key) {
            if let DataType::ZSet(z) = &mut entry.value {
                if z.members.is_empty() {
                    continue;
                }
                // Snapshot scores+members, then pop `count` from the chosen end.
                let mut sorted: BTreeSet<(OrderedFloat<f64>, Vec<u8>)> = BTreeSet::new();
                for (m, s) in z.members.iter() {
                    sorted.insert((*s, m.clone()));
                }
                let take: Box<dyn Iterator<Item = _>> = if min {
                    Box::new(sorted.into_iter().take(count))
                } else {
                    Box::new(sorted.into_iter().rev().take(count))
                };
                let mut popped: Vec<Resp> = Vec::with_capacity(count);
                for (score, member) in take {
                    z.members.remove(&member);
                    z.by_score.remove(&(score, member.clone()));
                    popped.push(Resp::array(vec![
                        Resp::bulk(bytes::Bytes::from(member)),
                        Resp::bulk_str(format_float(score.0)),
                    ]));
                }
                if popped.is_empty() {
                    continue;
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
                    if z.members.len() < smallest {
                        smallest = z.members.len();
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
    for member in base.members.keys() {
        if other_idx.iter().all(|&i| sets[i].score(member).is_some()) {
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
        return Err(NexradeError::Generic("ERR syntax error".to_string()));
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
    use std::collections::BTreeMap;
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
    // Lex sort: use BTreeMap keyed by member. Members with same score appear
    // in lex order. For BYLEX we filter by member bytes alone.
    let mut lex_sorted: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
    for (member, score) in &z.members {
        if let Some(lo) = &min_bytes {
            if min_excl {
                if member.as_slice() <= lo.as_slice() {
                    continue;
                }
            } else if member.as_slice() < lo.as_slice() {
                continue;
            }
        }
        if let Some(hi) = &max_bytes {
            if max_excl {
                if member.as_slice() >= hi.as_slice() {
                    continue;
                }
            } else if member.as_slice() > hi.as_slice() {
                continue;
            }
        }
        lex_sorted.insert(member.clone(), score.0);
    }
    let mut out: Vec<(Vec<u8>, f64)> = lex_sorted.into_iter().collect();
    if rev {
        out.reverse();
    }
    out.into_iter()
        .skip(offset)
        .take(count.unwrap_or(usize::MAX))
        .collect()
}
