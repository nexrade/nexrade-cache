//! 0.8.2 — backup / restore: snapshot integrity (magic + CRC32C),
//! corruption errors, and offline verify.

use std::fs;
use std::io::Write;

use nexrade_core::persistence::Snapshot;
use nexrade_core::store::Database;

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nexrade-082-{name}-{}-{}.rdb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    p
}

fn empty_snap() -> Snapshot {
    Snapshot::new(vec![(0, Database::new())])
}

#[test]
fn save_load_round_trip() {
    let p = tmp_path("rt");
    let snap = empty_snap();
    snap.save(&p).expect("save");
    let loaded = Snapshot::load(&p).expect("load");
    assert_eq!(loaded.version, Snapshot::VERSION);
    assert_eq!(loaded.databases.len(), 1);
    fs::remove_file(&p).ok();
}

#[test]
fn verify_reports_intact_summary() {
    let p = tmp_path("verify");
    let snap = empty_snap();
    snap.save(&p).expect("save");
    let info = Snapshot::verify(&p).expect("verify");
    assert_eq!(info.version, Snapshot::VERSION);
    assert_eq!(info.database_count, 1);
    assert!(info.entry_count == 0, "empty db has 0 entries");
    fs::remove_file(&p).ok();
}

#[test]
fn truncated_file_fails_with_clear_crc_error() {
    let p = tmp_path("trunc");
    let snap = empty_snap();
    snap.save(&p).expect("save");
    // Truncate the file by one byte (eats 1 byte off the CRC tail).
    let bytes = fs::read(&p).expect("read");
    assert!(bytes.len() > 8);
    fs::write(&p, &bytes[..bytes.len() - 1]).expect("trunc write");
    let r = Snapshot::load(&p);
    let msg = match r {
        Err(e) => format!("{e}"),
        Ok(_) => panic!("truncated file must fail to load"),
    };
    assert!(
        msg.to_ascii_lowercase().contains("crc32c")
            || msg.to_ascii_lowercase().contains("too small"),
        "expected CRC or size error, got {msg}"
    );
    fs::remove_file(&p).ok();
}

#[test]
fn foreign_file_rejected_by_magic() {
    let p = tmp_path("foreign");
    // Write a non-snapshot file (just "hello" + 4 bytes).
    let mut f = fs::File::create(&p).expect("create");
    f.write_all(b"hello world garbage data here").unwrap();
    f.write_all(&[0u8; 4]).unwrap();
    drop(f);
    let r = Snapshot::load(&p);
    let msg = match r {
        Err(e) => format!("{e}"),
        Ok(_) => panic!("foreign file must fail"),
    };
    assert!(
        msg.to_ascii_lowercase().contains("magic"),
        "expected magic error, got {msg}"
    );
    fs::remove_file(&p).ok();
}

#[test]
fn bit_flip_in_payload_detected() {
    let p = tmp_path("flip");
    let snap = empty_snap();
    snap.save(&p).expect("save");
    let mut bytes = fs::read(&p).expect("read");
    // Flip a byte in the middle of the payload (skip the MAGIC header).
    assert!(bytes.len() > 12);
    bytes[8] ^= 0xFF;
    fs::write(&p, &bytes).expect("write flip");
    let r = Snapshot::load(&p);
    let msg = match r {
        Err(e) => format!("{e}"),
        Ok(_) => panic!("bit-flipped file must fail"),
    };
    assert!(
        msg.to_ascii_lowercase().contains("crc32c"),
        "expected CRC error, got {msg}"
    );
    fs::remove_file(&p).ok();
}

#[test]
fn verify_on_corrupt_file_returns_error() {
    let p = tmp_path("verify_corrupt");
    let snap = empty_snap();
    snap.save(&p).expect("save");
    let mut bytes = fs::read(&p).expect("read");
    let tail = bytes.len() - 5;
    bytes[tail] ^= 0xFF; // last byte of CRC
    fs::write(&p, &bytes).unwrap();
    assert!(Snapshot::verify(&p).is_err());
    fs::remove_file(&p).ok();
}

#[test]
fn empty_file_rejected() {
    let p = tmp_path("empty");
    fs::File::create(&p).unwrap();
    let r = Snapshot::load(&p);
    let msg = match r {
        Err(e) => format!("{e}"),
        Ok(_) => panic!("empty must fail"),
    };
    assert!(
        msg.to_ascii_lowercase().contains("too small"),
        "expected too-small error, got {msg}"
    );
    fs::remove_file(&p).ok();
}

#[test]
fn snapshot_rejects_crc_valid_trailing_payload() {
    let p = tmp_path("trailing");
    let snap = empty_snap();
    snap.save(&p).expect("save");
    let mut bytes = fs::read(&p).expect("read");
    let payload_end = bytes.len() - 4;
    bytes.insert(payload_end, 0x00);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[Snapshot::MAGIC.len()..bytes.len() - 4]);
    let crc = hasher.finalize();
    let len = bytes.len();
    bytes[len - 4..].copy_from_slice(&crc.to_le_bytes());
    fs::write(&p, &bytes).expect("write trailing payload");
    let error = Snapshot::load(&p).expect_err("trailing payload must fail");
    assert!(format!("{error}").contains("trailing"));
    fs::remove_file(&p).ok();
}

#[test]
fn concurrent_saves_publish_complete_snapshots() {
    let p = tmp_path("concurrent");
    let path_a = p.clone();
    let path_b = p.clone();
    let first = std::thread::spawn(move || empty_snap().save(path_a));
    let second = std::thread::spawn(move || empty_snap().save(path_b));
    first.join().unwrap().expect("first save");
    second.join().unwrap().expect("second save");
    Snapshot::load(&p).expect("final snapshot is complete");
    fs::remove_file(&p).ok();
}

#[test]
fn save_atomic_rename_does_not_leak_tmp() {
    // On every successful save, no `.tmp` sibling should remain.
    let p = tmp_path("atomic");
    let snap = empty_snap();
    snap.save(&p).expect("save");
    let mut tmp = p.clone();
    tmp.as_mut_os_string().push(".tmp");
    assert!(!tmp.exists(), "tmp file should have been renamed away");
    fs::remove_file(&p).ok();
}
