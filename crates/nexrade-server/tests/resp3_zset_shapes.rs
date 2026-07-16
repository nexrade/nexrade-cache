//! End-to-end test for the RESP3 pairing-shape fix in
//! `Connection::upgrade_to_resp3` (see `resp3_should_nest_pairs`).
//!
//! Under RESP3, several ZSET commands must nest `[member, score]` pairs
//! into a `[[member, score], ...]` array *only* when the request actually
//! asked for scores (WITHSCORES) or an explicit COUNT — never guessed from
//! the response array's parity. This drives real TCP connections against a
//! real `Listener`, sends `HELLO 3`, and inspects the raw RESP3 wire bytes
//! (`*N\r\n` nesting) rather than going through a client library, so the
//! exact shape bug (or its absence) is directly visible.

use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::persistence::PersistenceConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Bind a throwaway listener to claim a free OS-assigned port, then drop
/// it — `Listener::run` binds by address itself, so this is the standard
/// way to hand it a free port (see `tls_listener.rs` / `acl_multi_exec.rs`).
async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// RESP-encode a command, matching the wire format the server expects.
fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Read one RESP reply, growing the buffer until a full frame is available.
/// Minimal but sufficient for the short replies used in this test.
async fn read_reply(stream: &mut TcpStream) -> String {
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

async fn start_server() -> u16 {
    let port = free_port().await;
    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 1,
        metrics_enabled: false,
        persistence: PersistenceConfig {
            rdb_path: None,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Db::new(config);
    let listener = nexrade_server::Listener::new(db, None);
    tokio::spawn(async move {
        let _ = listener.run().await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    port
}

async fn hello3(conn: &mut TcpStream) {
    conn.write_all(&encode_command(&["HELLO", "3"]))
        .await
        .unwrap();
    let _ = read_reply(conn).await;
}

#[tokio::test]
async fn zrange_withscores_nests_pairs_under_resp3() {
    let port = start_server().await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    hello3(&mut conn).await;

    conn.write_all(&encode_command(&["ZADD", "z", "1", "a", "2", "b"]))
        .await
        .unwrap();
    let _ = read_reply(&mut conn).await;

    conn.write_all(&encode_command(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    // Outer array of 2 nested pairs: *2\r\n *2\r\n $1\r\na\r\n $1\r\n1\r\n *2\r\n $1\r\nb\r\n $1\r\n2\r\n
    assert!(
        reply.starts_with("*2\r\n*2\r\n"),
        "expected nested pairs under RESP3 WITHSCORES, got: {reply:?}"
    );
}

#[tokio::test]
async fn zpopmin_no_count_stays_flat_under_resp3() {
    let port = start_server().await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    hello3(&mut conn).await;

    conn.write_all(&encode_command(&["ZADD", "z", "1", "a", "2", "b"]))
        .await
        .unwrap();
    let _ = read_reply(&mut conn).await;

    conn.write_all(&encode_command(&["ZPOPMIN", "z"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    // Flat [member, score]: *2\r\n $1\r\na\r\n $1\r\n1\r\n — no nested *2 right after the outer.
    assert!(
        reply.starts_with("*2\r\n$"),
        "ZPOPMIN with no COUNT should stay flat under RESP3, got: {reply:?}"
    );
}

#[tokio::test]
async fn zpopmin_with_count_nests_pairs_under_resp3() {
    let port = start_server().await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    hello3(&mut conn).await;

    conn.write_all(&encode_command(&["ZADD", "z", "1", "a", "2", "b"]))
        .await
        .unwrap();
    let _ = read_reply(&mut conn).await;

    conn.write_all(&encode_command(&["ZPOPMIN", "z", "1"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    // Nested [[member, score]]: *1\r\n *2\r\n $1\r\na\r\n $1\r\n1\r\n
    assert!(
        reply.starts_with("*1\r\n*2\r\n"),
        "ZPOPMIN with explicit COUNT should nest under RESP3, got: {reply:?}"
    );
}

#[tokio::test]
async fn zunion_without_withscores_is_not_nested_even_with_even_member_count() {
    // Regression test for the pre-existing bug: guessing nesting from
    // array parity wrongly nested a plain ZUNION with an even member
    // count into fake [member, member] pairs.
    let port = start_server().await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    hello3(&mut conn).await;

    conn.write_all(&encode_command(&[
        "ZADD", "k1", "1", "a", "2", "b", "3", "c", "4", "d",
    ]))
    .await
    .unwrap();
    let _ = read_reply(&mut conn).await;

    conn.write_all(&encode_command(&["ZUNION", "1", "k1"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    // 4 members, even count, no WITHSCORES: must stay a flat array of bulk
    // strings, i.e. *4\r\n$... not *4\r\n*2\r\n...
    assert!(
        reply.starts_with("*4\r\n$"),
        "ZUNION with no WITHSCORES and an even member count must not be nested, got: {reply:?}"
    );
}

#[tokio::test]
async fn zunion_with_withscores_nests_pairs_under_resp3() {
    let port = start_server().await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    hello3(&mut conn).await;

    conn.write_all(&encode_command(&["ZADD", "k1", "1", "a", "2", "b"]))
        .await
        .unwrap();
    let _ = read_reply(&mut conn).await;

    conn.write_all(&encode_command(&["ZUNION", "1", "k1", "WITHSCORES"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("*2\r\n*2\r\n"),
        "ZUNION WITHSCORES should nest pairs under RESP3, got: {reply:?}"
    );
}
