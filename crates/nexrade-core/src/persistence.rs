//! Persistence: RDB snapshots and Append-Only File (AOF).

#[cfg(not(target_arch = "wasm32"))]
use std::fs::{File, OpenOptions};
#[cfg(not(target_arch = "wasm32"))]
use std::io::{BufReader, BufWriter, Read, Write};
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use bytes::Bytes;
#[cfg(not(target_arch = "wasm32"))]
use crc32fast::Hasher;
#[cfg(not(target_arch = "wasm32"))]
use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use tracing::info;

#[cfg(not(target_arch = "wasm32"))]
use crate::error::Result;
#[cfg(not(target_arch = "wasm32"))]
use crate::resp::Resp;
#[cfg(not(target_arch = "wasm32"))]
use crate::store::Database;
#[cfg(not(target_arch = "wasm32"))]
use crate::types::DataType;

#[cfg(not(target_arch = "wasm32"))]
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Create a unique temporary path in `destination`'s directory. Keeping the
/// temp file beside its final path guarantees that the later rename stays on
/// the same filesystem and is therefore atomic.
#[cfg(not(target_arch = "wasm32"))]
fn temp_path(destination: &Path, suffix: &str) -> PathBuf {
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    let name = destination
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("nexrade"));
    let serial = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    parent
        .join(format!(
            ".{}.{}.{}.tmp",
            name.to_string_lossy(),
            std::process::id(),
            serial
        ))
        .with_extension(suffix)
}

/// Publish a fully written temporary file. The caller has already flushed and
/// synced the temporary file; this helper supplies the metadata durability
/// step that file-only fsync cannot provide after an atomic rename.
#[cfg(not(target_arch = "wasm32"))]
fn publish_durable(temp: &Path, destination: &Path) -> Result<()> {
    std::fs::rename(temp, destination)?;

    #[cfg(unix)]
    {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
    }

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
/// Serializable snapshot of all databases.
#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u32,
    pub created_at: u64,
    pub databases: Vec<(usize, Database)>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Snapshot {
    pub const VERSION: u32 = 4;
    /// Magic header (`b"nexd"` + 0x01) so `load` and `verify` can
    /// reject foreign files before bincode tries to deserialize random
    /// bytes. Version byte leaves room for future header changes.
    pub const MAGIC: [u8; 5] = *b"nexd\x01";

    pub fn new(databases: Vec<(usize, Database)>) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            version: Self::VERSION,
            created_at,
            databases,
        }
    }

    /// Save snapshot to a file (RDB-like).
    ///
    /// 0.8.2 layout: `[MAGIC 5B][bincode payload][CRC32C 4B LE]`.
    /// The CRC is computed over the bincode payload so `verify` can
    /// re-read the file and detect any bit-flip (truncation, fs
    /// corruption, partial overwrite) before bincode attempts to
    /// deserialize random bytes.
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let destination = path.as_ref();
        let tmp_path = temp_path(destination, "rdbtmp");
        {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            let encoded = bincode::serde::encode_to_vec(self, bincode::config::standard())
                .map_err(|e| crate::error::NexradeError::Generic(e.to_string()))?;
            let mut hasher = Hasher::new();
            hasher.update(&encoded);
            let crc = hasher.finalize();
            writer.write_all(&Self::MAGIC)?;
            writer.write_all(&encoded)?;
            writer.write_all(&crc.to_le_bytes())?;
            writer.flush()?;
            // fsync the temp file's data to disk *before* the rename below.
            // `flush()` only empties the userspace `BufWriter` into the OS
            // page cache — without this, a real power-loss crash can persist
            // the rename (a metadata operation) before the file's data
            // blocks are durable, leaving the final RDB path pointing at a
            // truncated or garbage file. `rename()` itself is still atomic
            // (no reader ever observes a half-written file at `path`), but
            // atomicity alone doesn't guarantee the post-rename content
            // survives a crash without this fsync.
            writer.get_ref().sync_all()?;
        }
        publish_durable(&tmp_path, destination)?;
        info!("snapshot saved to {:?}", destination);
        Ok(())
    }

    /// Load a snapshot from file. 0.8.2: validates the magic header,
    /// version, AND CRC32C checksum before calling bincode so any
    /// corruption surfaces as a clear "integrity check failed" error
    /// instead of a confusing bincode decode panic.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        let (snapshot, integrity_err) = Self::decode_and_check(&buf)?;
        if let Some(reason) = integrity_err {
            return Err(crate::error::NexradeError::Generic(reason));
        }
        if snapshot.version != Self::VERSION {
            return Err(crate::error::NexradeError::Generic(format!(
                "unsupported RDB snapshot version {} (this build expects {}); \
                 upgrade via AOF replay or reseed after FLUSHALL",
                snapshot.version,
                Self::VERSION,
            )));
        }
        info!("snapshot loaded from {:?}", path.as_ref());
        Ok(snapshot)
    }

    /// Decode and validate a snapshot buffer without touching the
    /// filesystem. Returns `(snapshot, integrity_error)`. If
    /// `integrity_error.is_some()` the snapshot is **not** returned —
    /// callers must surface the error rather than load a corrupt
    /// database.
    pub fn decode_and_check(buf: &[u8]) -> Result<(Self, Option<String>)> {
        if buf.len() < Self::MAGIC.len() + 4 {
            return Err(crate::error::NexradeError::Generic(
                "snapshot is too small to be a valid file (header + CRC missing)".to_string(),
            ));
        }
        if buf[..Self::MAGIC.len()] != Self::MAGIC {
            return Err(crate::error::NexradeError::Generic(format!(
                "snapshot magic mismatch: got {:?}. This file is not a nexrade-cache snapshot.",
                &buf[..Self::MAGIC.len()],
            )));
        }
        let payload_end = buf.len() - 4;
        let payload = &buf[Self::MAGIC.len()..payload_end];
        let stored_crc = u32::from_le_bytes([
            buf[payload_end],
            buf[payload_end + 1],
            buf[payload_end + 2],
            buf[payload_end + 3],
        ]);
        let mut hasher = Hasher::new();
        hasher.update(payload);
        let actual_crc = hasher.finalize();
        if stored_crc != actual_crc {
            return Err(crate::error::NexradeError::Generic(format!(
                "snapshot CRC32C mismatch: stored {:#010x}, computed {:#010x}. \
                 The file is corrupted (truncated, partially overwritten, or bit-flipped). \
                 Re-take the snapshot from the primary before restarting.",
                stored_crc, actual_crc,
            )));
        }
        let (snapshot, consumed) =
            bincode::serde::decode_from_slice(payload, bincode::config::standard()).map_err(
                |e| {
                    crate::error::NexradeError::Generic(format!(
                        "snapshot payload is well-formed but the bincode encoding is wrong: {e}. \
                 This usually means the file was written by an incompatible nexrade build."
                    ))
                },
            )?;
        if consumed != payload.len() {
            return Err(crate::error::NexradeError::Generic(format!(
                "snapshot has {} unexpected trailing payload byte(s)",
                payload.len() - consumed
            )));
        }
        Ok((snapshot, None))
    }

    /// Verify a snapshot file's integrity header and checksum without
    /// committing to a deserialization. Returns Ok with summary info if
    /// the file is intact, or an Err describing the corruption. Useful
    /// for offline integrity checks in backup scripts.
    pub fn verify<P: AsRef<Path>>(path: P) -> Result<SnapshotInfo> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        let (snap, err) = Self::decode_and_check(&buf)?;
        if let Some(reason) = err {
            return Err(crate::error::NexradeError::Generic(reason));
        }
        Ok(SnapshotInfo {
            path: path.as_ref().display().to_string(),
            version: snap.version,
            created_at: snap.created_at,
            database_count: snap.databases.len(),
            entry_count: snap.databases.iter().map(|(_, d)| d.len()).sum(),
        })
    }
}

/// Lightweight summary returned by `Snapshot::verify` so backup scripts
/// can report what the file contains.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub path: String,
    pub version: u32,
    pub created_at: u64,
    pub database_count: usize,
    pub entry_count: usize,
}

#[cfg(not(target_arch = "wasm32"))]
/// AOF writer — appends raw RESP commands to a file.
pub struct AofWriter {
    writer: File,
}

#[cfg(not(target_arch = "wasm32"))]
impl AofWriter {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Self { writer: file })
    }

    /// Append a RESP-encoded command to the AOF file.
    pub fn append(&mut self, cmd_bytes: &[u8]) -> Result<()> {
        self.writer.write_all(cmd_bytes)?;
        Ok(())
    }

    /// Flush is retained for the `appendfsync no` tick. AOF writes use the
    /// underlying `File` directly, so successful `append` already reached
    /// the OS rather than being held in a process-local buffer.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// fsync to ensure durability.
    pub fn fsync(&mut self) -> Result<()> {
        self.writer.sync_all()?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl AofWriter {
    /// Rewrite the AOF by serializing the current database state as RESP
    /// commands into a temp file, then atomically replacing the existing AOF.
    /// This compacts the file and removes all superseded commands.
    pub fn rewrite<P: AsRef<Path>>(
        path: P,
        databases: &[(usize, Database)],
        acl_lines: &[String],
    ) -> Result<()> {
        let destination = path.as_ref();
        let tmp = temp_path(destination, "aofrewrite");
        {
            let file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            let mut w = BufWriter::new(file);

            // Emit ACL state first, before any data — so on replay the
            // users are configured before any command that uses them.
            for line in acl_lines {
                let mut args: Vec<Resp> = vec![Resp::bulk_str("ACL"), Resp::bulk_str("SETUSER")];
                // Reconstruct an ACL SETUSER call from a stored line. The
                // canonical format is: "user <name> [on|off] [#<hash>] ~<pat>
                // [+|-]<cmd|@cat>..." — the first token "user" is the
                // command name when used as a SETUSER payload.
                let mut parts = line.split_whitespace();
                if parts.next() != Some("user") {
                    continue;
                }
                let Some(name) = parts.next() else { continue };
                args.push(Resp::bulk_str(name));
                for tok in parts {
                    args.push(Resp::bulk_str(tok));
                }
                w.write_all(&Resp::Array(Some(args)).serialize())?;
            }

            for (db_index, database) in databases {
                // SELECT to switch to the right database.
                let select = Resp::Array(Some(vec![
                    Resp::bulk_str("SELECT"),
                    Resp::bulk_str(db_index.to_string()),
                ]));
                w.write_all(&select.serialize())?;

                for (key, entry) in &database.entries {
                    if entry.is_expired() {
                        continue;
                    }
                    let key_bytes = Bytes::copy_from_slice(key);

                    // Emit the appropriate restore command for each data type.
                    let cmd: Option<Resp> = match &entry.value {
                        DataType::String(v) => Some(Resp::Array(Some(vec![
                            Resp::bulk_str("SET"),
                            Resp::bulk(key_bytes.clone()),
                            Resp::bulk(Bytes::copy_from_slice(v)),
                        ]))),
                        // Int-encoded keys compact down to a plain SET with
                        // their decimal representation — AOF replay via SET
                        // then behaves exactly like a fresh INCR would have
                        // (re-promotes to Int on the next INCR), which is
                        // the same fresh-start semantics every other
                        // compacted key gets.
                        DataType::Int(cell) => Some(Resp::Array(Some(vec![
                            Resp::bulk_str("SET"),
                            Resp::bulk(key_bytes.clone()),
                            Resp::bulk_str(cell.load().to_string()),
                        ]))),
                        DataType::List(l) if !l.is_empty() => {
                            let mut args =
                                vec![Resp::bulk_str("RPUSH"), Resp::bulk(key_bytes.clone())];
                            args.extend(l.to_vec_bytes().into_iter().map(Resp::bulk));
                            Some(Resp::Array(Some(args)))
                        }
                        DataType::Set(s) if !s.is_empty() => {
                            let mut args =
                                vec![Resp::bulk_str("SADD"), Resp::bulk(key_bytes.clone())];
                            args.extend(s.to_vec().into_iter().map(|v| Resp::bulk(Bytes::from(v))));
                            Some(Resp::Array(Some(args)))
                        }
                        DataType::Hash(h) if !h.is_empty() => {
                            let mut args =
                                vec![Resp::bulk_str("HSET"), Resp::bulk(key_bytes.clone())];
                            for (f, v) in h.to_pairs() {
                                args.push(Resp::bulk(Bytes::from(f)));
                                args.push(Resp::bulk(Bytes::from(v)));
                            }
                            Some(Resp::Array(Some(args)))
                        }
                        DataType::ZSet(z) if !z.is_empty() => {
                            let mut args =
                                vec![Resp::bulk_str("ZADD"), Resp::bulk(key_bytes.clone())];
                            for (member, score) in z.to_pairs() {
                                args.push(Resp::bulk_str(score.to_string()));
                                args.push(Resp::bulk(Bytes::from(member)));
                            }
                            Some(Resp::Array(Some(args)))
                        }
                        DataType::Stream(entries)
                            if !entries.entries.is_empty() || !entries.last_id.is_empty() =>
                        {
                            for se in &entries.entries {
                                let mut args = vec![
                                    Resp::bulk_str("XADD"),
                                    Resp::bulk(key_bytes.clone()),
                                    Resp::bulk_str(&se.id),
                                ];
                                for (f, v) in &se.fields {
                                    args.push(Resp::bulk(Bytes::copy_from_slice(f)));
                                    args.push(Resp::bulk(Bytes::copy_from_slice(v)));
                                }
                                w.write_all(&Resp::Array(Some(args)).serialize())?;
                            }
                            // Restore last-generated-id if it outruns the last
                            // entry (e.g. after XDEL of the top, or pure XSETID).
                            let last_entry_id = entries.entries.last().map(|e| e.id.as_str());
                            let last_gen = entries.last_generated_id();
                            if last_entry_id != Some(last_gen) && last_gen != "0-0" {
                                let mut args = vec![
                                    Resp::bulk_str("XSETID"),
                                    Resp::bulk(key_bytes.clone()),
                                    Resp::bulk_str(last_gen.to_string()),
                                ];
                                if entries.entries_added > 0 {
                                    args.push(Resp::bulk_str("ENTRIESADDED"));
                                    args.push(Resp::bulk_str(entries.entries_added.to_string()));
                                }
                                w.write_all(&Resp::Array(Some(args)).serialize())?;
                            }
                            // Restore consumer-group state, including PEL.
                            // Sequence per group:
                            //   1. XGROUP CREATE key group <last_delivered_id>
                            //   2. XGROUP CREATECONSUMER key group <consumer>
                            //      for every known consumer
                            //   3. XCLAIM key group consumer 0 <id> TIME <ms>
                            //      RETRYCOUNT <n> FORCE JUSTID
                            //      for every pending entry (FORCE creates the
                            //      PEL slot even though no prior delivery
                            //      happened in this process).
                            for (group_name, group) in &entries.groups {
                                let cg_args = vec![
                                    Resp::bulk_str("XGROUP"),
                                    Resp::bulk_str("CREATE"),
                                    Resp::bulk(key_bytes.clone()),
                                    Resp::bulk(Bytes::copy_from_slice(group_name)),
                                    Resp::bulk_str(&group.last_delivered_id),
                                ];
                                w.write_all(&Resp::Array(Some(cg_args)).serialize())?;

                                for cname in group.consumers.keys() {
                                    let cc_args = vec![
                                        Resp::bulk_str("XGROUP"),
                                        Resp::bulk_str("CREATECONSUMER"),
                                        Resp::bulk(key_bytes.clone()),
                                        Resp::bulk(Bytes::copy_from_slice(group_name)),
                                        Resp::bulk(Bytes::copy_from_slice(cname)),
                                    ];
                                    w.write_all(&Resp::Array(Some(cc_args)).serialize())?;
                                }

                                for (id, pel) in &group.pending {
                                    let claim_args = vec![
                                        Resp::bulk_str("XCLAIM"),
                                        Resp::bulk(key_bytes.clone()),
                                        Resp::bulk(Bytes::copy_from_slice(group_name)),
                                        Resp::bulk(Bytes::copy_from_slice(&pel.consumer)),
                                        Resp::bulk_str("0"),
                                        Resp::bulk_str(id.clone()),
                                        Resp::bulk_str("TIME"),
                                        Resp::bulk_str(pel.delivery_time_ms.to_string()),
                                        Resp::bulk_str("RETRYCOUNT"),
                                        Resp::bulk_str(pel.delivery_count.to_string()),
                                        Resp::bulk_str("FORCE"),
                                        Resp::bulk_str("JUSTID"),
                                    ];
                                    w.write_all(&Resp::Array(Some(claim_args)).serialize())?;
                                }
                            }
                            None // already written above
                        }
                        DataType::Bitmap(v) if !v.is_empty() => Some(Resp::Array(Some(vec![
                            // Bitmaps are stored as raw bytes. Re-emit them as a
                            // string so GETBIT/SETBIT continue to work after replay
                            // (those commands accept both String and Bitmap).
                            Resp::bulk_str("SET"),
                            Resp::bulk(key_bytes.clone()),
                            Resp::bulk(Bytes::copy_from_slice(v)),
                        ]))),
                        DataType::HyperLogLog(v) if !v.is_empty() => Some(Resp::Array(Some(
                            // HyperLogLog registers are raw bytes. SET preserves them;
                            // PFCOUNT/PFADD accept both HyperLogLog and String types
                            // of the correct register-array length, so replay still
                            // works correctly.
                            vec![
                                Resp::bulk_str("SET"),
                                Resp::bulk(key_bytes.clone()),
                                Resp::bulk(Bytes::copy_from_slice(v)),
                            ],
                        ))),
                        DataType::Geo(g) if !g.members.is_empty() => {
                            // Emit a single GEOADD with all (lon, lat, member)
                            // triples to avoid one rewrite line per member.
                            let mut args =
                                vec![Resp::bulk_str("GEOADD"), Resp::bulk(key_bytes.clone())];
                            for (member, pt) in &g.members {
                                args.push(Resp::bulk_str(format!("{:.17}", pt.longitude)));
                                args.push(Resp::bulk_str(format!("{:.17}", pt.latitude)));
                                args.push(Resp::bulk(Bytes::copy_from_slice(member)));
                            }
                            Some(Resp::Array(Some(args)))
                        }
                        _ => None, // empty entries are skipped
                    };

                    if let Some(c) = cmd {
                        w.write_all(&c.serialize())?;
                    }

                    // Emit PEXPIREAT for keys with TTL.
                    if let Some(ref exp) = entry.expiry {
                        let expire_cmd = Resp::Array(Some(vec![
                            Resp::bulk_str("PEXPIREAT"),
                            Resp::bulk(key_bytes),
                            Resp::bulk_str(exp.expires_at_ms.to_string()),
                        ]));
                        w.write_all(&expire_cmd.serialize())?;
                    }
                }
            }
            w.flush()?;
            // See the matching comment in `Snapshot::save` — fsync the temp
            // file's data before the atomic rename so a power-loss crash
            // can't leave the final AOF path pointing at undurable content.
            w.get_ref().sync_all()?;
        }
        publish_durable(&tmp, destination)?;
        info!("AOF rewrite complete: {:?}", destination);
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// AOF reader — replays commands from the file.
pub struct AofReader {
    reader: BufReader<File>,
    /// Parser holding fed-but-unconsumed bytes. This **must** persist across
    /// `next_command` calls: one 8 KiB read usually spans many commands, so a
    /// per-call parser silently discarded every command after the first one
    /// in each chunk.
    parser: crate::resp::RespParser,
    /// Raw bytes mirroring `parser`'s unconsumed buffer, so a command can be
    /// returned with its exact original framing.
    pending: Vec<u8>,
    /// File offset of `pending[0]`, for error messages.
    consumed: usize,
    /// Set once the file has signalled EOF; `pending` then drains without
    /// further reads.
    eof: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl AofReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        Ok(Self {
            reader: BufReader::new(file),
            parser: crate::resp::RespParser::new(),
            pending: Vec::new(),
            consumed: 0,
            eof: false,
        })
    }

    /// Read the next raw RESP command. `Ok(None)` is the only clean EOF;
    /// an incomplete tail or malformed command is a recovery error rather
    /// than a silently accepted partial replay.
    ///
    /// Memory: bounded by the largest single RESP command plus one 8 KiB
    /// read chunk — each command is drained before the next is parsed, so a
    /// multi-GB AOF does not need multi-GB of memory.
    pub fn next_command(&mut self) -> Result<Option<Vec<u8>>> {
        // 8 KiB chunk matches the connection read_buf and is well below
        // the largest realistic single command (multi-frame blobs span
        // multiple reads below).
        let mut scratch = [0u8; 8 * 1024];
        loop {
            // Try to satisfy the call from bytes already buffered.
            if !self.pending.is_empty() {
                let before = self.parser.buffered_len();
                match self.parser.parse_one() {
                    Ok(Some(_)) => {
                        // The parser advanced past exactly one command; the
                        // same number of raw bytes is that command's framing.
                        let len = before - self.parser.buffered_len();
                        debug_assert!(len > 0 && len <= self.pending.len());
                        let cmd: Vec<u8> = self.pending.drain(..len).collect();
                        self.consumed += len;
                        return Ok(Some(cmd));
                    }
                    Ok(None) => { /* incomplete — need more bytes */ }
                    Err(e) => {
                        return Err(crate::error::NexradeError::Generic(format!(
                            "AOF parse error at byte {}: {e}",
                            self.consumed
                        )));
                    }
                }
            }

            if self.eof {
                // No more bytes will arrive. An empty buffer is the clean end
                // of the file; leftovers are a truncated or corrupt frame.
                if self.pending.is_empty() {
                    return Ok(None);
                }
                return Err(crate::error::NexradeError::Generic(format!(
                    "AOF is truncated at byte {}: incomplete RESP command",
                    self.consumed
                )));
            }

            let n = self.reader.read(&mut scratch)?;
            if n == 0 {
                self.eof = true;
            } else {
                self.parser.feed(&scratch[..n]);
                self.pending.extend_from_slice(&scratch[..n]);
            }
        }
    }

    /// Stream-parse the entire AOF, returning once clean EOF is reached
    /// or any error surfaces. Used by `--preflight` for non-mutating
    /// sanity validation without applying commands.
    pub fn scan_to_eof(&mut self) -> Result<()> {
        while let Some(_bytes) = self.next_command()? {
            // Each call already validated the framed RESP bytes; we
            // don't need to look at the contents for preflight.
        }
        Ok(())
    }
}

/// Persistence configuration.
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Path for RDB snapshot.
    pub rdb_path: Option<String>,
    /// Path for AOF file.
    pub aof_path: Option<String>,
    /// AOF sync policy.
    pub aof_sync: AofSync,
    /// RDB save rules: (seconds, min_changes)
    pub rdb_save_rules: Vec<(u64, usize)>,
    // max_snapshot_age_secs and max_replication_lag_secs used to live
    // here as well, but `HealthConfig` is the sole owner (1.2.1+).
    // Threshold values flow through `[health]` in TOML, `--health-*` on
    // the CLI, and `health.max_snapshot_age_secs` /
    // `max_replication_lag_secs` at runtime. Keeping duplicates here
    // would have been a footgun: whichever side was read by the
    // health snapshot would silently shadow the other, and there was
    // no migration story if defaults ever diverged.
}

#[derive(Debug, Clone, PartialEq)]
pub enum AofSync {
    Always,
    EverySec,
    No,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            rdb_path: Some("nexrade.rdb".to_string()),
            aof_path: None,
            aof_sync: AofSync::EverySec,
            rdb_save_rules: vec![(900, 1), (300, 10), (60, 10000)],
        }
    }
}
