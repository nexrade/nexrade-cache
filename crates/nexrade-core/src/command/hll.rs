//! HyperLogLog commands — PFADD, PFCOUNT, PFMERGE.
//!
//! The register array is stored as `DataType::HyperLogLog(Vec<u8>)` (one byte
//! per register, 16384 registers total). We also accept `DataType::String` of
//! the same length so that values restored via `AOF rewrite → SET key bytes`
//! still work with `PFCOUNT` / `PFMERGE`.
//!
//! Values copied out of Redis keep their 16-byte `HYLL` header; the dense
//! (6-bit packed) form is decoded on read. The sparse form is rejected with a
//! message pointing at `scripts/migrate_from_redis.py`, which converts those
//! on the source side.

use crate::command::get_bytes_vec;
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::{hll_add, hll_count, hll_merge_into, DataType, HLL_REGISTERS, HLL_REGISTER_MAX};

/// Redis's own HyperLogLog blob header: magic(4) + encoding(1) + padding(3)
/// + cached cardinality(8) = 16 bytes.
const REDIS_HLL_MAGIC: &[u8; 4] = b"HYLL";
const REDIS_HLL_HEADER: usize = 16;
/// Redis encoding byte: 0 = dense (6-bit packed registers), 1 = sparse (RLE).
const REDIS_HLL_ENC_DENSE: u8 = 0;
const REDIS_HLL_ENC_SPARSE: u8 = 1;
/// Dense Redis form: 16384 registers packed 6 bits each, LSB-first into a
/// contiguous bit stream (16384 * 6 / 8 = 12288 bytes).
const REDIS_HLL_DENSE_BYTES: usize = HLL_REGISTERS * 6 / 8;

/// Unpack Redis's 6-bit dense register array.
///
/// The registers form one little-endian bit stream: register `i` occupies
/// stream bits `[6i, 6i+6)`, so a register straddles a byte boundary whenever
/// `6i % 8 > 2`. Verified against `PFDEBUG GETREG` on redis:7.
fn unpack_redis_dense(payload: &[u8]) -> [u8; HLL_REGISTERS] {
    let mut out = [0u8; HLL_REGISTERS];
    for (i, slot) in out.iter_mut().enumerate() {
        let start = i * 6;
        let byte = start / 8;
        let off = start % 8;
        // `off` cycles 0, 6, 4, 2 — only 4 and 6 spill into the next byte.
        let mut v = (payload[byte] >> off) as u16;
        if off > 2 {
            v |= (payload[byte + 1] as u16) << (8 - off);
        }
        *slot = (v & 0x3f) as u8;
    }
    out
}

/// Decode a Redis HyperLogLog blob into nexrade's dense register array.
///
/// Returns `Err` with an operator-actionable message for the encodings we do
/// not implement. The sparse (RLE) form is what Redis produces for every HLL
/// below `hll_sparse_max_bytes` (~3 KB), i.e. the common case; decoding its
/// run-length ops is deliberately not attempted here — `scripts/
/// migrate_from_redis.py` converts those on the *source* side with
/// `PFDEBUG GETREG`, which is exact by construction.
fn decode_redis_hll(bytes: &[u8]) -> Result<[u8; HLL_REGISTERS]> {
    if bytes.len() < REDIS_HLL_HEADER || &bytes[..4] != REDIS_HLL_MAGIC {
        return Err(NexradeError::Generic(format!(
            "WRONGTYPE Key is not a valid HyperLogLog value (expected {} bytes, got {})",
            HLL_REGISTERS,
            bytes.len()
        )));
    }
    match bytes[4] {
        REDIS_HLL_ENC_DENSE => {
            let payload = &bytes[REDIS_HLL_HEADER..];
            if payload.len() != REDIS_HLL_DENSE_BYTES {
                return Err(NexradeError::Generic(format!(
                    "WRONGTYPE Key is not a valid HyperLogLog value \
                     (Redis dense header with {} register bytes, expected {})",
                    payload.len(),
                    REDIS_HLL_DENSE_BYTES
                )));
            }
            Ok(unpack_redis_dense(payload))
        }
        REDIS_HLL_ENC_SPARSE => Err(NexradeError::Generic(
            "WRONGTYPE Key is not a valid HyperLogLog value (Redis sparse encoding). \
             Migrate it with scripts/migrate_from_redis.py, or rebuild the key with PFADD."
                .to_string(),
        )),
        other => Err(NexradeError::Generic(format!(
            "WRONGTYPE Key is not a valid HyperLogLog value (unknown Redis encoding {other})"
        ))),
    }
}

/// Read the register array from an entry, regardless of whether it is stored
/// as `HyperLogLog` or `String`. Returns an error if the bytes do not look
/// like a register array (wrong length, or values outside the HLL range).
fn hll_registers_from(entry: &Entry) -> Result<[u8; HLL_REGISTERS]> {
    let bytes: &[u8] = match &entry.value {
        DataType::HyperLogLog(v) => v,
        DataType::String(v) => v,
        _ => return Err(NexradeError::WrongType),
    };
    if bytes.len() != HLL_REGISTERS {
        // A value copied straight out of Redis (migration, DUMP payload, AOF
        // rewrite) still carries Redis's 16-byte HYLL header. Decoding it here
        // is what keeps PFCOUNT/PFADD/PFMERGE working on migrated keys.
        return decode_redis_hll(bytes);
    }
    // Validate register values are within the 6-bit range.
    let mut out = [0u8; HLL_REGISTERS];
    for (i, &b) in bytes.iter().enumerate() {
        if b > HLL_REGISTER_MAX {
            return Err(NexradeError::Generic(
                "WRONGTYPE Key is not a valid HyperLogLog value".to_string(),
            ));
        }
        out[i] = b;
    }
    Ok(out)
}

fn empty_registers() -> [u8; HLL_REGISTERS] {
    [0u8; HLL_REGISTERS]
}

// ── PFADD ─────────────────────────────────────────────────────────────────────

/// `PFADD key element [element ...]`
///
/// Returns 1 if at least one HLL register was modified, 0 otherwise. Creates
/// the key with an empty HLL if it doesn't exist.
pub async fn cmd_pfadd(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    // Arity is -2, not -3: `PFADD key` with no elements is legal in Redis and
    // means "create the key if absent, otherwise report whether anything
    // changed". Rejecting it here contradicted `command_table.rs`, which
    // declares -2.
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("pfadd".to_string()));
    }
    let key = get_bytes_vec(args, 1, "PFADD")?;
    let elements: Vec<Vec<u8>> = (2..args.len())
        .map(|i| get_bytes_vec(args, i, "PFADD"))
        .collect::<Result<_>>()?;

    let mut store_db = db.store.db(db_index).write_for(&key);

    let mut registers = match store_db.get(&key) {
        None => empty_registers(),
        Some(entry) => hll_registers_from(entry)?,
    };

    let mut changed = 0i64;
    for el in &elements {
        if hll_add(&mut registers, el) {
            changed += 1;
        }
    }

    if changed > 0 || store_db.get(&key).is_none() {
        // Persist the (possibly unchanged) registers. `Entry::new` carries no
        // expiry and `insert` replaces the whole entry, so carry the existing
        // TTL across — PFADD updates the value in place and must not make a
        // volatile key permanent.
        let prev_expiry = store_db.get(&key).and_then(|e| e.expiry.clone());
        store_db.insert(
            key.clone(),
            Entry::new(DataType::HyperLogLog(registers.to_vec())),
        );
        if let Some(expiry) = prev_expiry {
            store_db.set_expiry(&key, Some(expiry));
        }
        // PFADD returns 1 on first creation too.
        Ok(Resp::int(1))
    } else {
        Ok(Resp::int(0))
    }
}

// ── PFCOUNT ───────────────────────────────────────────────────────────────────

/// `PFCOUNT key [key ...]`
///
/// Returns the approximated cardinality of the union of HLLs stored at the
/// given keys. When multiple keys are supplied, the union is computed in
/// memory (without mutating any key).
pub async fn cmd_pfcount(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("pfcount".to_string()));
    }

    let keys: Vec<Vec<u8>> = (1..args.len())
        .map(|i| get_bytes_vec(args, i, "PFCOUNT"))
        .collect::<Result<_>>()?;

    if keys.len() == 1 {
        let key = &keys[0];
        let store_db = db.store.db(db_index).read_for(key);
        let count = match store_db.get_ro(key) {
            None => 0,
            Some(entry) => {
                let regs = hll_registers_from(entry)?;
                hll_count(&regs)
            }
        };
        return Ok(Resp::int(count as i64));
    }

    // Multiple keys: union in memory.
    let mut accum = empty_registers();
    let mut any_data = false;
    for key in &keys {
        let store_db = db.store.db(db_index).read_for(key);
        match store_db.get_ro(key) {
            None => {} // missing key contributes nothing
            Some(entry) => {
                let regs = hll_registers_from(entry)?;
                hll_merge_into(&mut accum, &regs);
                any_data = true;
            }
        }
    }

    if !any_data {
        Ok(Resp::int(0))
    } else {
        Ok(Resp::int(hll_count(&accum) as i64))
    }
}

// ── PFMERGE ───────────────────────────────────────────────────────────────────

/// `PFMERGE destkey [sourcekey ...]`
///
/// Merges the HLLs at `sourcekey` into the HLL stored at `destkey`. Creates
/// `destkey` if it doesn't exist.
pub async fn cmd_pfmerge(db: &Db, args: &[Resp], db_index: usize) -> Result<Resp> {
    if args.len() < 2 {
        return Err(NexradeError::WrongArity("pfmerge".to_string()));
    }

    let dest = get_bytes_vec(args, 1, "PFMERGE")?;
    let sources: Vec<Vec<u8>> = (2..args.len())
        .map(|i| get_bytes_vec(args, i, "PFMERGE"))
        .collect::<Result<_>>()?;

    let mut accum = empty_registers();

    for src in &sources {
        let store_db = db.store.db(db_index).read_for(src);
        if let Some(entry) = store_db.get_ro(src) {
            let regs = hll_registers_from(entry)?;
            hll_merge_into(&mut accum, &regs);
        }
    }

    let mut dest_shard = db.store.db(db_index).write_for(&dest);
    match dest_shard.get_mut(&dest) {
        Some(entry) => {
            // Take the existing bytes (if any) out so we can rebuild with
            // the merged register values, then assign back as HyperLogLog.
            let existing: Option<Vec<u8>> = match &mut entry.value {
                DataType::HyperLogLog(v) if v.len() == HLL_REGISTERS => Some(std::mem::take(v)),
                // Redis treats an HLL-sized string as register storage, so this
                // arm is correct as written.
                DataType::String(s) if s.len() == HLL_REGISTERS => Some(s.to_vec()),
                // Anything else is a type error, NOT an empty HLL to be
                // replaced. Returning `None` here let `existing` fall through
                // to `unwrap_or_else(|| accum.to_vec())`, so `PFMERGE dest src`
                // overwrote and destroyed whatever `dest` held while replying
                // +OK — `SET dest hello` followed by `PFMERGE dest src` lost
                // the string entirely. Redis answers WRONGTYPE and leaves the
                // destination untouched.
                DataType::HyperLogLog(_) | DataType::String(_) | DataType::Bitmap(_) => {
                    return Err(NexradeError::Generic(
                        "Key is not a valid HyperLogLog string value.".to_string(),
                    ));
                }
                _ => return Err(NexradeError::WrongType),
            };
            let mut merged = existing.unwrap_or_else(|| accum.to_vec());
            for (i, b) in accum.iter().enumerate() {
                if *b > merged[i] {
                    merged[i] = *b;
                }
            }
            entry.value = DataType::HyperLogLog(merged);
        }
        None => {
            dest_shard.insert(dest, Entry::new(DataType::HyperLogLog(accum.to_vec())));
        }
    }

    Ok(Resp::ok())
}
