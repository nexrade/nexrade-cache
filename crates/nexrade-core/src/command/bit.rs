//! Bitmap command handlers: SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS, BITFIELD.

use crate::command::{get_i64, get_str};
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::DataType;
use bytes::Bytes;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Grow the bitmap Vec to hold at least `byte_idx + 1` bytes.
fn ensure_bitmap(bits: &mut Vec<u8>, byte_idx: usize) {
    if bits.len() <= byte_idx {
        bits.resize(byte_idx + 1, 0);
    }
}

/// Get the byte representation of a store entry that may be String, Bitmap,
/// or Int (an int-encoded string is still a string for bit-op purposes, same
/// as real Redis). Returns owned bytes since `Int`'s decimal-ASCII form has
/// no `Vec<u8>` to borrow — matches how most call sites already `.clone()`
/// the borrowed form immediately anyway.
fn get_bitmap_bytes(entry: &Entry) -> Result<Vec<u8>> {
    match &entry.value {
        DataType::String(v) => Ok(v.to_vec()),
        DataType::Bitmap(v) => Ok(v.clone()),
        DataType::Int(cell) => Ok(cell.load().to_string().into_bytes()),
        _ => Err(NexradeError::WrongType),
    }
}

// ── SETBIT ────────────────────────────────────────────────────────────────────

pub async fn cmd_setbit(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 4 {
        return Err(NexradeError::WrongArity("setbit".to_string()));
    }
    let key = crate::command::get_bytes_vec(args, 1, "SETBIT")?;
    let offset = get_i64(args, 2, "SETBIT")?;
    let value = get_i64(args, 3, "SETBIT")?;

    if !(0..=4_294_967_295).contains(&offset) {
        return Err(NexradeError::BitError);
    }
    if value != 0 && value != 1 {
        return Err(NexradeError::Generic(
            "bit is not an integer or out of range".to_string(),
        ));
    }

    let byte_idx = (offset / 8) as usize;
    let bit_pos = 7 - (offset % 8) as u8; // Redis stores MSB first

    let mut store_db = db.store.db(db_index).write_for(&key);
    // SETBIT mutates raw bytes in place, which an atomic `Int` cell has no
    // bytes to offer — demote to a plain `String` first (same decimal bytes
    // GET would have returned) so the shared match below can mutate a
    // `Vec<u8>` uniformly. No-op for every other variant.
    if let Some(entry) = store_db.get_mut(&key) {
        if let DataType::Int(cell) = &entry.value {
            entry.value = DataType::String(Bytes::from(cell.load().to_string().into_bytes()));
        }
    }
    let old_bit = match store_db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            DataType::String(s) => {
                let mut bits = s.to_vec();
                ensure_bitmap(&mut bits, byte_idx);
                let old = (bits[byte_idx] >> bit_pos) & 1;
                if value == 1 {
                    bits[byte_idx] |= 1 << bit_pos;
                } else {
                    bits[byte_idx] &= !(1 << bit_pos);
                }
                entry.value = DataType::String(Bytes::from(bits));
                old as i64
            }
            DataType::Bitmap(v) => {
                ensure_bitmap(v, byte_idx);
                let old = (v[byte_idx] >> bit_pos) & 1;
                if value == 1 {
                    v[byte_idx] |= 1 << bit_pos;
                } else {
                    v[byte_idx] &= !(1 << bit_pos);
                }
                old as i64
            }
            _ => return Err(NexradeError::WrongType),
        },
        None => {
            let mut bits = vec![0u8; byte_idx + 1];
            if value == 1 {
                bits[byte_idx] |= 1 << bit_pos;
            }
            store_db.insert(key, Entry::new(DataType::Bitmap(bits)));
            0
        }
    };

    Ok(Resp::int(old_bit))
}

// ── GETBIT ────────────────────────────────────────────────────────────────────

pub async fn cmd_getbit(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() != 3 {
        return Err(NexradeError::WrongArity("getbit".to_string()));
    }
    let key = crate::command::get_bytes_vec(args, 1, "GETBIT")?;
    let offset = get_i64(args, 2, "GETBIT")?;

    if offset < 0 {
        return Err(NexradeError::BitError);
    }

    let byte_idx = (offset / 8) as usize;
    let bit_pos = 7 - (offset % 8) as u8;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let bit = match store_db.get(&key) {
        None => 0,
        Some(entry) => {
            let bytes = get_bitmap_bytes(entry)?;
            if byte_idx >= bytes.len() {
                0
            } else {
                ((bytes[byte_idx] >> bit_pos) & 1) as i64
            }
        }
    };

    Ok(Resp::int(bit))
}

// ── BITCOUNT ──────────────────────────────────────────────────────────────────

pub async fn cmd_bitcount(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("bitcount".to_string()));
    }
    let key = crate::command::get_bytes_vec(args, 1, "BITCOUNT")?;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let bytes = match store_db.get(&key) {
        None => return Ok(Resp::int(0)),
        Some(entry) => get_bitmap_bytes(entry)?.clone(),
    };

    let slice = if args.len() >= 4 {
        let start = get_i64(args, 2, "BITCOUNT")? as isize;
        let end = get_i64(args, 3, "BITCOUNT")? as isize;
        let unit = if args.len() >= 5 {
            get_str(args, 4, "BITCOUNT")?.to_uppercase()
        } else {
            "BYTE".to_string()
        };

        if unit == "BIT" {
            // BIT range: count bits in the given bit range
            if bytes.is_empty() {
                return Ok(Resp::int(0));
            }
            let len = (bytes.len() * 8) as isize;
            let max_bit = (len - 1) as usize;
            let s = normalize_signed(start, len).min(max_bit);
            let e = normalize_signed(end, len).min(max_bit);
            if s > e {
                return Ok(Resp::int(0));
            }
            let mut count = 0i64;
            for bit_idx in s..=e {
                let byte = bit_idx / 8;
                let pos = 7 - (bit_idx % 8);
                if (bytes[byte] >> pos) & 1 == 1 {
                    count += 1;
                }
            }
            return Ok(Resp::int(count));
        } else {
            if bytes.is_empty() {
                return Ok(Resp::int(0));
            }
            let len = bytes.len() as isize;
            let s = normalize_signed(start, len);
            let e = normalize_signed(end, len).min(bytes.len().saturating_sub(1));
            if s > e {
                return Ok(Resp::int(0));
            }
            &bytes[s..=e]
        }
    } else {
        &bytes[..]
    };

    let count: i64 = slice.iter().map(|b| b.count_ones() as i64).sum();
    Ok(Resp::int(count))
}

// ── BITPOS ────────────────────────────────────────────────────────────────────

pub async fn cmd_bitpos(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 3 {
        return Err(NexradeError::WrongArity("bitpos".to_string()));
    }
    let key = crate::command::get_bytes_vec(args, 1, "BITPOS")?;
    let bit = get_i64(args, 2, "BITPOS")?;
    if bit != 0 && bit != 1 {
        return Err(NexradeError::Generic(
            "bit argument must be 0 or 1".to_string(),
        ));
    }
    let target = bit == 1;

    let mut store_db = db.store.db(db_index).write_for(&key);
    let bytes = match store_db.get(&key) {
        None => {
            // No key: first set bit is -1, first clear bit is 0
            return Ok(Resp::int(if target { -1 } else { 0 }));
        }
        Some(entry) => get_bitmap_bytes(entry)?.clone(),
    };

    let (start_byte, end_byte, use_bit_unit) = if args.len() >= 4 {
        let s = get_i64(args, 3, "BITPOS")? as isize;
        let unit = if args.len() >= 6 {
            get_str(args, 5, "BITPOS")?.to_uppercase() == "BIT"
        } else {
            false
        };
        let e = if args.len() >= 5 {
            get_i64(args, 4, "BITPOS")? as isize
        } else {
            // Default end: last byte or last bit depending on unit
            if unit {
                (bytes.len() * 8) as isize - 1
            } else {
                bytes.len() as isize - 1
            }
        };
        // Normalize using the appropriate length (bits vs bytes)
        let len = if unit {
            (bytes.len() * 8) as isize
        } else {
            bytes.len() as isize
        };
        (normalize_signed(s, len), normalize_signed(e, len), unit)
    } else {
        (0, bytes.len().saturating_sub(1), false)
    };

    if use_bit_unit {
        let total_bits = bytes.len() * 8;
        let s = start_byte.min(total_bits);
        let e = end_byte.min(total_bits.saturating_sub(1));
        for bit_idx in s..=e {
            let byte_i = bit_idx / 8;
            let bit_i = 7 - (bit_idx % 8);
            let found = (bytes[byte_i] >> bit_i) & 1 == 1;
            if found == target {
                return Ok(Resp::int(bit_idx as i64));
            }
        }
        return Ok(Resp::int(-1));
    }

    let end_byte = end_byte.min(bytes.len().saturating_sub(1));
    for (off, &byte) in bytes[start_byte..=end_byte].iter().enumerate() {
        let byte_idx = start_byte + off;
        for bit_i in (0..8u8).rev() {
            let pos = 7 - bit_i;
            let found = (byte >> pos) & 1 == 1;
            if found == target {
                return Ok(Resp::int((byte_idx * 8 + bit_i as usize) as i64));
            }
        }
    }
    Ok(Resp::int(-1))
}

// ── BITOP ─────────────────────────────────────────────────────────────────────

pub async fn cmd_bitop(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 4 {
        return Err(NexradeError::WrongArity("bitop".to_string()));
    }
    let op = get_str(args, 1, "BITOP")?.to_uppercase();
    let destkey = crate::command::get_bytes_vec(args, 2, "BITOP")?;

    if op == "NOT" && args.len() != 4 {
        return Err(NexradeError::Generic(
            "BITOP NOT must be called with a single source key".to_string(),
        ));
    }

    let src_keys: Vec<Vec<u8>> = (3..args.len())
        .map(|i| crate::command::get_bytes_vec(args, i, "BITOP"))
        .collect::<Result<_>>()?;

    let sdb = db.store.db(db_index);

    // Collect all source bitmaps (acquire and release each lock independently).
    let mut sources: Vec<Vec<u8>> = Vec::new();
    for key in &src_keys {
        match sdb.write_for(key).get(key) {
            None => sources.push(vec![]),
            Some(entry) => sources.push(get_bitmap_bytes(entry)?.clone()),
        }
    }

    let max_len = sources.iter().map(|s| s.len()).max().unwrap_or(0);
    let mut result = vec![0u8; max_len];

    match op.as_str() {
        "AND" => {
            result = vec![0xFF; max_len];
            for src in &sources {
                for (i, b) in result.iter_mut().enumerate() {
                    *b &= src.get(i).copied().unwrap_or(0);
                }
            }
        }
        "OR" => {
            for src in &sources {
                for (i, b) in result.iter_mut().enumerate() {
                    *b |= src.get(i).copied().unwrap_or(0);
                }
            }
        }
        "XOR" => {
            for src in &sources {
                for (i, b) in result.iter_mut().enumerate() {
                    *b ^= src.get(i).copied().unwrap_or(0);
                }
            }
        }
        "NOT" => {
            for (i, b) in result.iter_mut().enumerate() {
                *b = !sources[0].get(i).copied().unwrap_or(0);
            }
        }
        _ => {
            return Err(NexradeError::Generic(format!(
                "unknown BITOP operation: {}",
                op
            )))
        }
    }

    let len = result.len() as i64;
    let mut dst_shard = sdb.write_for(&destkey);
    dst_shard.insert(destkey, Entry::new(DataType::Bitmap(result)));
    Ok(Resp::int(len))
}

// ── BITFIELD ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
enum OverflowMode {
    Wrap,
    Sat,
    Fail,
}

pub async fn cmd_bitfield(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("bitfield".to_string()));
    }
    let key = crate::command::get_bytes_vec(args, 1, "BITFIELD")?;
    let mut responses: Vec<Resp> = Vec::new();
    let mut overflow = OverflowMode::Wrap;
    let mut i = 2;

    while i < args.len() {
        let sub = get_str(args, i, "BITFIELD")?.to_uppercase();
        match sub.as_str() {
            "OVERFLOW" => {
                i += 1;
                overflow = match get_str(args, i, "BITFIELD")?.to_uppercase().as_str() {
                    "SAT" => OverflowMode::Sat,
                    "FAIL" => OverflowMode::Fail,
                    _ => OverflowMode::Wrap,
                };
                i += 1;
            }
            "GET" => {
                let (bits, signed, bit_offset) = parse_type_offset(args, i + 1)?;
                i += 3;
                let mut store_db = db.store.db(db_index).write_for(&key);
                let val = match store_db.get(&key) {
                    None => 0i64,
                    Some(entry) => {
                        let bytes = get_bitmap_bytes(entry)?.clone();
                        read_bitfield(&bytes, bit_offset, bits, signed)
                    }
                };
                responses.push(Resp::int(val));
            }
            "SET" => {
                let (bits, signed, bit_offset) = parse_type_offset(args, i + 1)?;
                let new_val = get_i64(args, i + 3, "BITFIELD")?;
                i += 4;
                let mut store_db = db.store.db(db_index).write_for(&key);
                // Demote an atomic Int cell to a plain String first — see
                // the comment in cmd_setbit for why in-place byte mutation
                // can't operate directly on the atomic representation.
                if let Some(entry) = store_db.get_mut(&key) {
                    if let DataType::Int(cell) = &entry.value {
                        entry.value =
                            DataType::String(Bytes::from(cell.load().to_string().into_bytes()));
                    }
                }
                let old_val = match store_db.get_mut(&key) {
                    None => {
                        let byte_len = (bit_offset + bits).div_ceil(8);
                        let mut v = vec![0u8; byte_len];
                        write_bitfield(&mut v, bit_offset, bits, signed, new_val, overflow);
                        store_db.insert(key.clone(), Entry::new(DataType::Bitmap(v)));
                        0i64
                    }
                    Some(entry) => match &mut entry.value {
                        DataType::String(s) => {
                            let mut v = s.to_vec();
                            let byte_len = (bit_offset + bits).div_ceil(8);
                            if v.len() < byte_len {
                                v.resize(byte_len, 0);
                            }
                            let old = read_bitfield(&v, bit_offset, bits, signed);
                            write_bitfield(&mut v, bit_offset, bits, signed, new_val, overflow);
                            entry.value = DataType::String(Bytes::from(v));
                            old
                        }
                        DataType::Bitmap(v) => {
                            let byte_len = (bit_offset + bits).div_ceil(8);
                            if v.len() < byte_len {
                                v.resize(byte_len, 0);
                            }
                            let old = read_bitfield(v, bit_offset, bits, signed);
                            write_bitfield(v, bit_offset, bits, signed, new_val, overflow);
                            old
                        }
                        _ => return Err(NexradeError::WrongType),
                    },
                };
                responses.push(Resp::int(old_val));
            }
            "INCRBY" => {
                let (bits, signed, bit_offset) = parse_type_offset(args, i + 1)?;
                let incr = get_i64(args, i + 3, "BITFIELD")?;
                i += 4;
                let mut store_db = db.store.db(db_index).write_for(&key);
                // Same Int-cell demotion as BITFIELD SET above.
                if let Some(entry) = store_db.get_mut(&key) {
                    if let DataType::Int(cell) = &entry.value {
                        entry.value =
                            DataType::String(Bytes::from(cell.load().to_string().into_bytes()));
                    }
                }
                let new_val = match store_db.get_mut(&key) {
                    None => {
                        let byte_len = (bit_offset + bits).div_ceil(8);
                        let mut v = vec![0u8; byte_len];
                        let result = overflow_add(0, incr, bits, signed, overflow);
                        if let Some(r) = result {
                            write_bitfield(&mut v, bit_offset, bits, signed, r, overflow);
                            store_db.insert(key.clone(), Entry::new(DataType::Bitmap(v)));
                            Some(r)
                        } else {
                            None
                        }
                    }
                    Some(entry) => match &mut entry.value {
                        DataType::String(s) => {
                            let mut v = s.to_vec();
                            let byte_len = (bit_offset + bits).div_ceil(8);
                            if v.len() < byte_len {
                                v.resize(byte_len, 0);
                            }
                            let cur = read_bitfield(&v, bit_offset, bits, signed);
                            let out =
                                if let Some(r) = overflow_add(cur, incr, bits, signed, overflow) {
                                    write_bitfield(&mut v, bit_offset, bits, signed, r, overflow);
                                    Some(r)
                                } else {
                                    None
                                };
                            entry.value = DataType::String(Bytes::from(v));
                            out
                        }
                        DataType::Bitmap(v) => {
                            let byte_len = (bit_offset + bits).div_ceil(8);
                            if v.len() < byte_len {
                                v.resize(byte_len, 0);
                            }
                            let cur = read_bitfield(v, bit_offset, bits, signed);
                            if let Some(r) = overflow_add(cur, incr, bits, signed, overflow) {
                                write_bitfield(v, bit_offset, bits, signed, r, overflow);
                                Some(r)
                            } else {
                                None
                            }
                        }
                        _ => return Err(NexradeError::WrongType),
                    },
                };
                responses.push(match new_val {
                    Some(v) => Resp::int(v),
                    None => Resp::null(),
                });
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(Resp::Array(Some(responses)))
}

// ── BITFIELD helpers ──────────────────────────────────────────────────────────

/// Parse `u8` / `i8` / `u16` / … type descriptor and bit offset.
/// Returns (bit_width, is_signed, bit_offset).
fn parse_type_offset(args: &[Resp], start: usize) -> Result<(usize, bool, usize)> {
    let type_str = args
        .get(start)
        .and_then(|a| a.as_str())
        .ok_or_else(|| NexradeError::WrongArity("bitfield".to_string()))?;
    let (signed, bits_str) = if type_str.starts_with('i') || type_str.starts_with('I') {
        (true, &type_str[1..])
    } else if type_str.starts_with('u') || type_str.starts_with('U') {
        (false, &type_str[1..])
    } else {
        return Err(NexradeError::Generic(format!(
            "invalid BITFIELD type: {}",
            type_str
        )));
    };
    let bits: usize = bits_str.parse().map_err(|_| NexradeError::BitError)?;
    if bits == 0 || bits > 64 || (!signed && bits > 63) {
        return Err(NexradeError::BitError);
    }
    let offset_str = args
        .get(start + 1)
        .and_then(|a| a.as_str())
        .ok_or_else(|| NexradeError::WrongArity("bitfield".to_string()))?;
    let offset: usize = offset_str.parse().map_err(|_| NexradeError::BitError)?;
    Ok((bits, signed, offset))
}

fn read_bitfield(bytes: &[u8], bit_offset: usize, bits: usize, signed: bool) -> i64 {
    let mut val: u64 = 0;
    for i in 0..bits {
        let global_bit = bit_offset + i;
        let byte_i = global_bit / 8;
        let bit_i = 7 - (global_bit % 8);
        if byte_i < bytes.len() && (bytes[byte_i] >> bit_i) & 1 == 1 {
            val |= 1 << (bits - 1 - i);
        }
    }
    if signed && bits < 64 && (val >> (bits - 1)) & 1 == 1 {
        // Sign-extend
        val |= !0u64 << bits;
    }
    val as i64
}

fn write_bitfield(
    bytes: &mut [u8],
    bit_offset: usize,
    bits: usize,
    _signed: bool,
    value: i64,
    _overflow: OverflowMode,
) {
    let v = value as u64;
    for i in 0..bits {
        let global_bit = bit_offset + i;
        let byte_i = global_bit / 8;
        let bit_i = 7 - (global_bit % 8);
        if byte_i < bytes.len() {
            if (v >> (bits - 1 - i)) & 1 == 1 {
                bytes[byte_i] |= 1 << bit_i;
            } else {
                bytes[byte_i] &= !(1 << bit_i);
            }
        }
    }
}

fn overflow_add(cur: i64, incr: i64, bits: usize, signed: bool, mode: OverflowMode) -> Option<i64> {
    let result = cur.wrapping_add(incr);
    if signed {
        let min = -(1i64 << (bits - 1));
        let max = (1i64 << (bits - 1)) - 1;
        match mode {
            OverflowMode::Wrap => {
                let range = 1i64 << bits;
                Some(((result - min).rem_euclid(range)) + min)
            }
            OverflowMode::Sat => Some(result.clamp(min, max)),
            OverflowMode::Fail => {
                if result < min || result > max {
                    None
                } else {
                    Some(result)
                }
            }
        }
    } else {
        let max = if bits == 64 {
            u64::MAX as i64
        } else {
            (1i64 << bits) - 1
        };
        match mode {
            OverflowMode::Wrap => {
                let range = if bits == 64 { u64::MAX } else { 1u64 << bits };
                Some((result as u64).wrapping_rem(range) as i64)
            }
            OverflowMode::Sat => Some(result.clamp(0, max)),
            OverflowMode::Fail => {
                if result < 0 || result > max {
                    None
                } else {
                    Some(result)
                }
            }
        }
    }
}

// ── index helpers ─────────────────────────────────────────────────────────────

fn normalize_signed(idx: isize, len: isize) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx as usize
    }
}
