//! Persistence: RDB snapshots and Append-Only File (AOF).

#[cfg(not(target_arch = "wasm32"))]
use std::fs::{File, OpenOptions};
#[cfg(not(target_arch = "wasm32"))]
use std::io::{BufReader, BufWriter, Read, Write};
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

#[cfg(not(target_arch = "wasm32"))]
use bytes::Bytes;
use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use tracing::{error, info};

#[cfg(not(target_arch = "wasm32"))]
use crate::error::Result;
#[cfg(not(target_arch = "wasm32"))]
use crate::resp::Resp;
#[cfg(not(target_arch = "wasm32"))]
use crate::store::Database;
#[cfg(not(target_arch = "wasm32"))]
use crate::types::DataType;

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
    pub const VERSION: u32 = 2;

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
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let tmp_path = format!("{}.tmp", path.as_ref().display());
        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            let encoded = bincode::serde::encode_to_vec(self, bincode::config::standard())
                .map_err(|e| crate::error::NexradeError::Generic(e.to_string()))?;
            writer.write_all(&encoded)?;
            writer.flush()?;
        }
        std::fs::rename(&tmp_path, path.as_ref())?;
        info!("snapshot saved to {:?}", path.as_ref());
        Ok(())
    }

    /// Load a snapshot from file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        let snapshot: Self = bincode::serde::decode_from_slice(&buf, bincode::config::standard())
            .map(|(v, _)| v)
            .map_err(|e| crate::error::NexradeError::Generic(e.to_string()))?;
        info!("snapshot loaded from {:?}", path.as_ref());
        Ok(snapshot)
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// AOF writer — appends raw RESP commands to a file.
pub struct AofWriter {
    writer: BufWriter<File>,
}

#[cfg(not(target_arch = "wasm32"))]
impl AofWriter {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Append a RESP-encoded command to the AOF file.
    pub fn append(&mut self, cmd_bytes: &[u8]) -> Result<()> {
        self.writer.write_all(cmd_bytes)?;
        Ok(())
    }

    /// Flush buffered writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// fsync to ensure durability.
    pub fn fsync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
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
        let tmp = format!("{}.rewrite.tmp", path.as_ref().display());
        {
            let file = File::create(&tmp)?;
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
                            args.extend(l.iter().map(|v| Resp::bulk(v.clone())));
                            Some(Resp::Array(Some(args)))
                        }
                        DataType::Set(s) if !s.is_empty() => {
                            let mut args =
                                vec![Resp::bulk_str("SADD"), Resp::bulk(key_bytes.clone())];
                            args.extend(s.iter().map(|v| Resp::bulk(Bytes::copy_from_slice(v))));
                            Some(Resp::Array(Some(args)))
                        }
                        DataType::Hash(h) if !h.is_empty() => {
                            let mut args =
                                vec![Resp::bulk_str("HSET"), Resp::bulk(key_bytes.clone())];
                            for (f, v) in h {
                                args.push(Resp::bulk(Bytes::copy_from_slice(f)));
                                args.push(Resp::bulk(Bytes::copy_from_slice(v)));
                            }
                            Some(Resp::Array(Some(args)))
                        }
                        DataType::ZSet(z) if !z.is_empty() => {
                            let mut args =
                                vec![Resp::bulk_str("ZADD"), Resp::bulk(key_bytes.clone())];
                            for (member, score) in &z.members {
                                args.push(Resp::bulk_str(score.0.to_string()));
                                args.push(Resp::bulk(Bytes::copy_from_slice(member)));
                            }
                            Some(Resp::Array(Some(args)))
                        }
                        DataType::Stream(entries) if !entries.entries.is_empty() => {
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
                            // Restore consumer-group state. Pending entries and
                            // consumer names are not preserved (PEL would require
                            // XCLAIM replay); only the group's last_delivered_id
                            // is restored, which is sufficient to resume reads at
                            // the correct offset.
                            for (group_name, group) in &entries.groups {
                                let cg_args = vec![
                                    Resp::bulk_str("XGROUP"),
                                    Resp::bulk_str("CREATE"),
                                    Resp::bulk(key_bytes.clone()),
                                    Resp::bulk(Bytes::copy_from_slice(group_name)),
                                    Resp::bulk_str(&group.last_delivered_id),
                                ];
                                w.write_all(&Resp::Array(Some(cg_args)).serialize())?;
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
        }
        std::fs::rename(&tmp, path.as_ref())?;
        info!("AOF rewrite complete: {:?}", path.as_ref());
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// AOF reader — replays commands from the file.
pub struct AofReader {
    data: Vec<u8>,
    pos: usize,
}

#[cfg(not(target_arch = "wasm32"))]
impl AofReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        Ok(Self { data, pos: 0 })
    }

    /// Read the next raw RESP value as bytes, returns None at EOF.
    pub fn next_command(&mut self) -> Option<Vec<u8>> {
        if self.pos >= self.data.len() {
            return None;
        }
        // Read until we have a complete RESP array
        let mut parser = crate::resp::RespParser::new();
        let start = self.pos;
        loop {
            if self.pos >= self.data.len() {
                return None;
            }
            parser.feed(&self.data[self.pos..self.pos + 1]);
            self.pos += 1;
            match parser.parse_one() {
                Ok(Some(_)) => return Some(self.data[start..self.pos].to_vec()),
                Ok(None) => continue,
                Err(e) => {
                    error!("AOF parse error: {}", e);
                    return None;
                }
            }
        }
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
