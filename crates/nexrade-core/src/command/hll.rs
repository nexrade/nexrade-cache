//! HyperLogLog commands — PFADD, PFCOUNT, PFMERGE.
//!
//! The register array is stored as `DataType::HyperLogLog(Vec<u8>)` (one byte
//! per register, 16384 registers total). We also accept `DataType::String` of
//! the same length so that values restored via `AOF rewrite → SET key bytes`
//! still work with `PFCOUNT` / `PFMERGE`.

use crate::command::get_bytes_vec;
use crate::db::Db;
use crate::error::{NexradeError, Result};
use crate::resp::Resp;
use crate::store::Entry;
use crate::types::{hll_add, hll_count, hll_merge_into, DataType, HLL_REGISTERS, HLL_REGISTER_MAX};

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
        return Err(NexradeError::Generic(format!(
            "WRONGTYPE Key is not a valid HyperLogLog value (expected {} bytes, got {})",
            HLL_REGISTERS,
            bytes.len()
        )));
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
    if args.len() < 3 {
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
        // Persist the (possibly unchanged) registers.
        store_db.insert(key, Entry::new(DataType::HyperLogLog(registers.to_vec())));
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
                DataType::String(s) if s.len() == HLL_REGISTERS => Some(s.to_vec()),
                DataType::HyperLogLog(_) | DataType::String(_) => None,
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
