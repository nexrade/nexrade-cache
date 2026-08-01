//! Persistence-recovery safety tests for strict AOF parsing.

use std::fs;

use nexrade_core::persistence::AofReader;

fn tmp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "nexrade-aof-safety-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn aof_reader_accepts_clean_eof_after_complete_command() {
    let path = tmp_path("clean");
    let command = b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n";
    fs::write(&path, command).unwrap();
    let mut reader = AofReader::open(&path).unwrap();
    assert_eq!(reader.next_command().unwrap().unwrap(), command);
    assert!(reader.next_command().unwrap().is_none());
    fs::remove_file(path).ok();
}

#[test]
fn aof_reader_rejects_truncated_tail() {
    let path = tmp_path("truncated");
    fs::write(&path, b"*2\r\n$3\r\nSET\r\n$5\r\nval").unwrap();
    let mut reader = AofReader::open(&path).unwrap();
    let error = reader.next_command().expect_err("truncated AOF must fail");
    assert!(format!("{error}").contains("truncated"));
    fs::remove_file(path).ok();
}

#[test]
fn aof_reader_rejects_malformed_command() {
    let path = tmp_path("malformed");
    fs::write(&path, b"*not-a-number\r\n").unwrap();
    let mut reader = AofReader::open(&path).unwrap();
    let error = reader.next_command().expect_err("invalid AOF must fail");
    assert!(format!("{error}").contains("AOF parse error"));
    fs::remove_file(path).ok();
}
