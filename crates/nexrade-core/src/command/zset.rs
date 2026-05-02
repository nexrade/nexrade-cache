//! Sorted Set command handlers.

use bytes::Bytes;

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
    if !db.contains_key(key) {
        db.insert(key.to_vec(), Entry::new(DataType::ZSet(ZSetData::new())));
    }
    match db.get_mut(key) {
        Some(e) => match &mut e.value {
            DataType::ZSet(z) => Ok(z),
            _ => Err(NexradeError::WrongType),
        },
        None => unreachable!(),
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

    // Parse options: NX, XX, GT, LT, CH
    let mut i = 2;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;

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
            _ => break,
        }
    }

    // NX is incompatible with GT/LT; GT and LT are mutually exclusive.
    if nx && (gt || lt) {
        return Err(NexradeError::SyntaxError);
    }
    if gt && lt {
        return Err(NexradeError::SyntaxError);
    }

    if (i + 1) >= args.len() || !(args.len() - i) % 2 == 0 {
        return Err(NexradeError::WrongArity("zadd".to_string()));
    }

    let mut store_db = db.store.db(db_index).write_for(&key);
    let zset = get_or_create_zset(&mut store_db, &key)?;

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
    let num_keys_i = get_i64(args, 2, "ZUNIONSTORE")?;
    if num_keys_i <= 0 {
        return Err(NexradeError::Generic(
            "ERR at least 1 input key is needed for ZUNIONSTORE".to_string(),
        ));
    }
    let num_keys = num_keys_i as usize;
    let (weights, agg) = parse_aggregate_weights(args, 3 + num_keys, num_keys, "ZUNIONSTORE")?;

    let mut result = ZSetData::new();
    let sdb = db.store.db(db_index);

    for (i, &weight) in weights.iter().enumerate().take(num_keys) {
        let key = get_bytes_vec(args, 3 + i, "ZUNIONSTORE")?;
        if let Some(e) = sdb.write_for(&key).get(&key) {
            if let DataType::ZSet(z) = &e.value {
                for (member, &score) in &z.members {
                    let weighted = score.0 * weight;
                    let new_score = apply_aggregate(agg, result.score(member), weighted);
                    result.insert(member.clone(), new_score);
                }
            }
        }
    }

    let count = result.len() as i64;
    let mut dst_shard = sdb.write_for(&dst);
    dst_shard.insert(dst, Entry::new(DataType::ZSet(result)));
    Ok(Resp::int(count))
}

pub async fn cmd_zinterstore(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("zinterstore".to_string()));
    }
    let dst = get_bytes_vec(args, 1, "ZINTERSTORE")?;
    let num_keys_i = get_i64(args, 2, "ZINTERSTORE")?;
    if num_keys_i <= 0 {
        return Err(NexradeError::Generic(
            "ERR at least 1 input key is needed for ZINTERSTORE".to_string(),
        ));
    }
    let num_keys = num_keys_i as usize;
    let (weights, agg) = parse_aggregate_weights(args, 3 + num_keys, num_keys, "ZINTERSTORE")?;

    let sdb = db.store.db(db_index);
    let mut sets: Vec<ZSetData> = Vec::new();

    for (i, _weight) in weights.iter().enumerate().take(num_keys) {
        let key = get_bytes_vec(args, 3 + i, "ZINTERSTORE")?;
        match sdb.write_for(&key).get(&key) {
            None => {
                let mut dst_shard = sdb.write_for(&dst);
                dst_shard.insert(dst, Entry::new(DataType::ZSet(ZSetData::new())));
                return Ok(Resp::int(0));
            }
            Some(e) => match &e.value {
                DataType::ZSet(z) => sets.push(z.clone()),
                _ => return Err(NexradeError::WrongType),
            },
        }
    }

    let mut result = ZSetData::new();
    if !sets.is_empty() {
        for (member, &score) in &sets[0].members {
            let mut acc = score.0 * weights[0];
            let mut in_all = true;
            for (j, other) in sets[1..].iter().enumerate() {
                if let Some(s) = other.score(member) {
                    let weighted = s * weights[j + 1];
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
    }

    let count = result.len() as i64;
    let mut dst_shard = sdb.write_for(&dst);
    dst_shard.insert(dst, Entry::new(DataType::ZSet(result)));
    Ok(Resp::int(count))
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
