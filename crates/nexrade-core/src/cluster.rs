//! Redis CLUSTER primitives — CRC16 slot hashing and helper utilities
//! for `CLUSTER KEYSLOT` / `CLUSTER NODES` / `CLUSTER INFO` and the
//! `MOVED`/`CROSSSLOT` redirection replies.
//!
//! The hash slot math is **standard** Redis (`CRC16-CCITT XMODEM` over
//! either the whole key or just the substring between `{...}` if the
//! key contains a hash tag). We do not yet run a real multi-node
//! cluster — this server is single-shard. `CLUSTER NODES` reports a
//! single self line covering slots 0..=16383, so clients (redis-py
//! `Cluster`, `redis-cli --cluster`) can probe without erroring.

pub const CLUSTER_SLOTS: u16 = 16384;
pub const NODE_ID_LEN: usize = 40;

/// CRC-16/CCITT-XMODEM — same algorithm as Redis's `keyHashSlot()` in
/// `cluster.c`. Polynomial 0x1021, init 0, no input/output reflection,
/// no final XOR. Table is byte-for-byte identical to Redis's
/// `crc16tab[256]`.
const CRC16_TAB: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b,
    0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
    0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738,
    0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
    0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
    0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290, 0x22f3, 0x32d2,
    0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
    0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827,
    0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d,
    0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

/// Compute the hash of `data` (after stripping the hash tag, if any).
pub fn crc16_hash(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc = (crc << 8) ^ CRC16_TAB[(((crc >> 8) ^ b as u16) & 0xff) as usize];
    }
    crc
}

/// Strip the hash tag from `key` if present. A hash tag is the
/// substring between the *first* `{` and the *first* `}` after it. If
/// no tag is present, the whole key is returned.
///
/// Behavior summary:
/// - `foo`           → `foo`
/// - `{user1000}.x`  → `user1000`
/// - `a{b}c{d}`      → `b`         (first `{...}` wins)
/// - `{}foo`         → `{}foo`     (empty tag — Redis hashes whole key)
/// - `a{b`           → `a{b`       (no closing `}`)
pub fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{') {
        if let Some(close) = key[open + 1..].iter().position(|&b| b == b'}') {
            // Redis-style: an empty tag `{}` hashes the whole key. A
            // tag with content hashes only the content.
            let inner = &key[open + 1..open + 1 + close];
            if !inner.is_empty() {
                return inner;
            }
        }
    }
    key
}

/// Returns the slot for `key` — CRC16 of the (possibly tag-stripped)
/// key, modulo 16384.
pub fn keyslot(key: &[u8]) -> u16 {
    crc16_hash(extract_hash_tag(key)) % CLUSTER_SLOTS
}

/// Whether `keyslot` is currently served by this server. For a
/// single-shard server this is always true. Returns `true` to keep the
/// call sites clean.
pub fn self_assigned_slot(_db: &crate::db::Db, _slot: u16) -> bool {
    // Future: consult `db.cluster_node_id` and a slot map.
    true
}

/// Generate a stable, hex-encoded 40-character node id for this process.
/// We concatenate two halves of a UUIDv4 with the hex representation of
/// the process start time (in nanoseconds) — easy and collision-free
/// enough for a single-node server.
pub fn generate_node_id() -> String {
    use uuid::Uuid;
    let bytes = Uuid::new_v4();
    let mut out = String::with_capacity(40);
    out.push_str(&hex::encode(bytes.as_bytes()));
    // 32 chars from UUID. Pad to 40 with a deterministic process-local
    // counter — process PID + startup nanos.
    use std::time::SystemTime;
    let extra = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    let combined = extra ^ pid;
    out.push_str(&format!("{:08x}", (combined & 0xFFFFFFFF) as u32));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyslot_known_vectors() {
        // From Redis docs (https://redis.io/commands/cluster-keyslot/):
        assert_eq!(keyslot(b"somekey"), 11058);
        assert_eq!(keyslot(b"foo{hash_tag}"), 2515);
        // Same hash tag → same slot.
        assert_eq!(keyslot(b"bar{hash_tag}"), 2515);
        // Other canonical vectors:
        assert_eq!(keyslot(b"foo"), 12182);
        // Empty key → 0; `{}` (2-byte empty-tag key) hashes as a
        // non-empty key — concrete value matches Redis.
        assert_eq!(keyslot(b""), 0);
        assert_eq!(keyslot(b"{}"), 15257);
    }

    #[test]
    fn hash_tag_extraction() {
        assert_eq!(extract_hash_tag(b"foo"), b"foo");
        assert_eq!(extract_hash_tag(b"{user1000}.following"), b"user1000");
        assert_eq!(extract_hash_tag(b"a{b}c"), b"b");
        // First `}` wins
        assert_eq!(extract_hash_tag(b"a{b}c{d}"), b"b");
        // No closing `}` -> whole key
        assert_eq!(extract_hash_tag(b"a{b"), b"a{b");
        // Empty tag -> whole key (Redis quirk)
        assert_eq!(extract_hash_tag(b"{}foo"), b"{}foo");
    }

    #[test]
    fn node_id_is_40_hex_chars() {
        let id = generate_node_id();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
