//! End-to-end tests for CLUSTER slot commands and slot math.

use nexrade_core::cluster::{crc16_hash, extract_hash_tag, keyslot};
use nexrade_core::command::dispatch_with_user;
use nexrade_core::db::Db;
use nexrade_core::resp::Resp;

fn cmd(args: &[&str]) -> Vec<Resp> {
    args.iter().map(|s| Resp::bulk_str(*s)).collect()
}

async fn run(db: &Db, args: Vec<Resp>) -> Resp {
    dispatch_with_user(db, args, 0, None, "default").await
}

#[test]
fn crc16_known_vectors() {
    // Empty input → 0.
    assert_eq!(crc16_hash(b""), 0);
    // Standard CCITT-XMODEM test vector from Redis's crc16.c comment:
    // "Output for 123456789 : 31C3"
    assert_eq!(crc16_hash(b"123456789"), 0x31C3);
    // Single 'a' (0x61) → 0x7C87 (matches Redis's CRC16 table).
    assert_eq!(crc16_hash(b"a"), 0x7C87);
}

#[test]
fn keyslot_known_vectors() {
    // From Redis docs (https://redis.io/commands/cluster-keyslot/):
    assert_eq!(keyslot(b"somekey"), 11058);
    assert_eq!(keyslot(b"foo{hash_tag}"), 2515);
    // Same hash tag → same slot.
    assert_eq!(keyslot(b"bar{hash_tag}"), 2515);
    // Other canonical vectors:
    assert_eq!(keyslot(b"foo"), 12182);
    // Empty key → 0; `{}` (2-byte empty-tag key) hashes as a non-empty
    // key — concrete value matches Redis.
    assert_eq!(keyslot(b""), 0);
    assert_eq!(keyslot(b"{}"), 15257);
}

#[test]
fn hash_tag_extraction_canonical() {
    assert_eq!(extract_hash_tag(b"foo"), b"foo");
    assert_eq!(extract_hash_tag(b"{user1000}.following"), b"user1000");
    assert_eq!(extract_hash_tag(b"a{b}c"), b"b");
    // First '}' wins when multiple are present
    assert_eq!(extract_hash_tag(b"a{b}c{d}"), b"b");
    // Empty tag → hash whole key
    assert_eq!(extract_hash_tag(b"{}foo"), b"{}foo");
    // No closing brace → hash whole key
    assert_eq!(extract_hash_tag(b"a{b"), b"a{b");
}

#[tokio::test]
async fn cluster_myid_returns_40_char_hex() {
    let db = Db::default();
    let resp = run(&db, cmd(&["CLUSTER", "MYID"])).await;
    match resp {
        Resp::BulkString(Some(b)) => {
            let s = std::str::from_utf8(&b).unwrap();
            assert_eq!(s.len(), 40, "got: {s}");
            assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
        }
        other => panic!("expected bulk string, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_keyslot_known_vectors() {
    let db = Db::default();
    let resp = run(&db, cmd(&["CLUSTER", "KEYSLOT", "foo"])).await;
    assert!(matches!(resp, Resp::Integer(12182)));
}

#[tokio::test]
async fn cluster_info_contains_required_fields() {
    let db = Db::default();
    let resp = run(&db, cmd(&["CLUSTER", "INFO"])).await;
    match resp {
        Resp::BulkString(Some(b)) => {
            let s = std::str::from_utf8(&b).unwrap();
            assert!(s.contains("cluster_enabled:0"));
            assert!(s.contains("cluster_state:ok"));
            assert!(s.contains("cluster_slots_assigned:16384"));
            assert!(s.contains("cluster_known_nodes:1"));
        }
        other => panic!("expected bulk string, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_nodes_self_line() {
    let db = Db::default();
    let resp = run(&db, cmd(&["CLUSTER", "NODES"])).await;
    match resp {
        Resp::BulkString(Some(b)) => {
            let s = std::str::from_utf8(&b).unwrap();
            assert!(s.contains("myself,master"));
            assert!(s.ends_with("connected 0-16383\r\n"));
        }
        other => panic!("expected bulk string, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_countkeysinslot_empty_returns_zero() {
    let db = Db::default();
    let resp = run(&db, cmd(&["CLUSTER", "COUNTKEYSINSLOT", "0"])).await;
    assert!(matches!(resp, Resp::Integer(0)));
}

#[tokio::test]
async fn cluster_countkeysinslot_returns_correct_count() {
    let db = Db::default();
    // Seed keys; the slot for "foo" is 12182, for "bar" is 5061.
    let _: Resp = run(&db, cmd(&["SET", "foo", "1"])).await;
    let _: Resp = run(&db, cmd(&["SET", "bar", "2"])).await;
    let resp = run(&db, cmd(&["CLUSTER", "COUNTKEYSINSLOT", "12182"])).await;
    assert!(matches!(resp, Resp::Integer(1)));
    let resp = run(&db, cmd(&["CLUSTER", "COUNTKEYSINSLOT", "5061"])).await;
    assert!(matches!(resp, Resp::Integer(1)));
    let resp = run(&db, cmd(&["CLUSTER", "COUNTKEYSINSLOT", "1"])).await;
    assert!(matches!(resp, Resp::Integer(0)));
}

#[tokio::test]
async fn cluster_getkeysinslot_returns_matching_keys() {
    let db = Db::default();
    let _: Resp = run(&db, cmd(&["SET", "foo", "1"])).await;
    let _: Resp = run(&db, cmd(&["SET", "bar", "2"])).await;
    let resp = run(&db, cmd(&["CLUSTER", "GETKEYSINSLOT", "12182", "10"])).await;
    match resp {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            assert!(matches!(&items[0], Resp::BulkString(Some(b)) if b.as_ref() == b"foo"));
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_slots_returns_single_range() {
    let db = Db::default();
    let resp = run(&db, cmd(&["CLUSTER", "SLOTS"])).await;
    match resp {
        Resp::Array(Some(ranges)) => {
            assert_eq!(ranges.len(), 1);
        }
        other => panic!("expected array, got {other:?}"),
    }
}
