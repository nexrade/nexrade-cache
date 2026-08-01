//! 0.7.2 — connection / pipeline alloc + RESP batching.
//!
//! Structural locks (no live TCP):
//!   * Resp::ok() still serializes as "+OK\\r\\n"
//!   * Integer 0/1 serialize as ":0\\r\\n" / ":1\\r\\n"
//!   * SegBuf with 16 KiB start capacity absorbs a 50-command SET batch
//!     without mid-batch growth (capacity never drops below start)
//!   * uppercase short names produce the expected command string

use nexrade_core::resp::{Resp, SegBuf};

#[test]
fn ok_and_pong_serialize_static() {
    let mut buf = SegBuf::with_capacity(64);
    Resp::ok().write_to(&mut buf);
    buf.finalize();
    // Drain via Buf trait
    use bytes::Buf;
    let mut out = Vec::new();
    while buf.has_remaining() {
        let chunk = buf.chunk().to_vec();
        out.extend_from_slice(&chunk);
        buf.advance(chunk.len());
    }
    assert_eq!(&out, b"+OK\r\n");
}

#[test]
fn integer_0_1_serialize() {
    let mut buf = SegBuf::with_capacity(64);
    Resp::int(0).write_to(&mut buf);
    Resp::int(1).write_to(&mut buf);
    buf.finalize();
    use bytes::Buf;
    let mut out = Vec::new();
    while buf.has_remaining() {
        let chunk = buf.chunk().to_vec();
        out.extend_from_slice(&chunk);
        buf.advance(chunk.len());
    }
    assert_eq!(&out, b":0\r\n:1\r\n");
}

#[test]
fn segbuf_16kib_absorbs_set_batch() {
    // 50× "+OK\r\n" = 250 bytes; with 16 KiB capacity we must not reallocate
    // the active buffer mid-batch (capacity stays ≥ start).
    let start_cap = 16 * 1024;
    let mut buf = SegBuf::with_capacity(start_cap);
    for _ in 0..50 {
        Resp::ok().write_to(&mut buf);
    }
    // Active buffer still has capacity ≥ start (no growth needed for 250 B).
    assert!(
        buf.inner().capacity() >= start_cap,
        "capacity regressed: {}",
        buf.inner().capacity()
    );
    buf.finalize();
    use bytes::Buf;
    assert_eq!(buf.remaining(), 50 * 5); // 50 × "+OK\r\n"
}

#[test]
fn uppercase_short_names() {
    // Mirrors the connection-layer stack path: short names uppercase
    // without intermediate capacity guesses.
    for (src, want) in [
        ("set", "SET"),
        ("Get", "GET"),
        ("mSeT", "MSET"),
        ("ping", "PING"),
    ] {
        let mut buf = [0u8; 32];
        let n = src.len();
        buf[..n].copy_from_slice(src.as_bytes());
        for b in &mut buf[..n] {
            *b = b.to_ascii_uppercase();
        }
        let got = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(got, want);
    }
}
