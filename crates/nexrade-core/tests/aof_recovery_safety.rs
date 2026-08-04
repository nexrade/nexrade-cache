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

/// A command whose bulk payload crosses the 8 KiB internal read chunk
/// must still parse and yield the exact bytes the caller wrote — including
/// the framing header. A byte-at-a-time read of the file (the worst
/// case for the underlying `BufReader`) exercises both the chunk
/// refill and the parser's growing-buffer behaviour.
#[test]
fn aof_reader_handles_command_larger_than_read_chunk() {
    let path = tmp_path("big-blob");
    let payload_len: usize = 32 * 1024; // 4x the 8 KiB read chunk
    let mut file_bytes = Vec::new();
    file_bytes.extend_from_slice(format!("*2\r\n$3\r\nSET\r\n${payload_len}\r\n").as_bytes());
    file_bytes.resize(file_bytes.len() + payload_len, b'X');
    file_bytes.extend_from_slice(b"\r\n");
    fs::write(&path, &file_bytes).unwrap();

    let mut reader = AofReader::open(&path).unwrap();
    let cmd = reader.next_command().unwrap().expect("command present");
    assert_eq!(cmd, file_bytes, "raw command bytes must round-trip");
    assert!(reader.next_command().unwrap().is_none());
    fs::remove_file(path).ok();
}

/// Every command in a multi-command AOF must be returned.
///
/// Regression: `next_command` built a fresh parser and buffer per call,
/// but the previous call had already pulled an 8 KiB chunk out of the
/// `BufReader` and discarded everything past the first command in it. A
/// real AOF — which always begins `SELECT` followed by the writes — replayed
/// only its first command and reported "AOF replay: 1 commands applied",
/// silently dropping the entire dataset. Because the tail was never parsed,
/// a corrupt AOF also started clean instead of being rejected.
#[test]
fn aof_reader_returns_every_command_in_one_chunk() {
    let path = tmp_path("multi");
    let select = b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n".to_vec();
    let set_a = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n".to_vec();
    let set_b = b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n".to_vec();
    let mut file = Vec::new();
    for c in [&select, &set_a, &set_b] {
        file.extend_from_slice(c);
    }
    fs::write(&path, &file).unwrap();

    let mut reader = AofReader::open(&path).unwrap();
    let mut got = Vec::new();
    while let Some(cmd) = reader.next_command().unwrap() {
        got.push(cmd);
    }
    fs::remove_file(path).ok();

    assert_eq!(
        got,
        vec![select, set_a, set_b],
        "all three commands must be returned in order; a short read means \
         the tail of the AOF is being silently dropped"
    );
}

/// Garbage after a run of valid commands must be rejected, not ignored.
/// This is the operator-visible contract: a corrupt AOF fails startup
/// rather than bringing the server up with a partial dataset.
#[test]
fn aof_reader_rejects_garbage_after_valid_commands() {
    let path = tmp_path("multi-corrupt");
    let mut file = Vec::new();
    file.extend_from_slice(b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n");
    file.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
    file.extend_from_slice(b"\xde\xad\xbe\xef\xca\xfe\xba\xbe");
    fs::write(&path, &file).unwrap();

    let mut reader = AofReader::open(&path).unwrap();
    let mut err = None;
    loop {
        match reader.next_command() {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    fs::remove_file(path).ok();
    assert!(
        err.is_some(),
        "trailing garbage must surface as an error, not a clean EOF"
    );
}
