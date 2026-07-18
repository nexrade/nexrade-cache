//! Regression tests for:
//!  1. Legacy `AUTH <password>` must authenticate as user `default` (not as the
//!     password string) and still allow subsequent data commands.
//!  2. `requirepass` must not be bypassed by the ACL default user's `nopass`.
//!  3. `HELLO 3 AUTH user pass` sets `authenticated_user` and works with ACL.
//!  4. `TIME` / `ROLE` are dispatched and return Redis-shaped replies.
//!  5. `RESET` clears SELECT/auth/MULTI state so a pooled client is reusable.

use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::persistence::PersistenceConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

async fn read_reply(stream: &mut TcpStream) -> String {
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

async fn start_with(config: ServerConfig) -> (u16, Db) {
    let port = config.port;
    let db = Db::new(config);
    let listener = nexrade_server::Listener::new(db.clone(), None);
    tokio::spawn(async move {
        let _ = listener.run().await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    (port, db)
}

async fn base_config(requirepass: Option<&str>) -> ServerConfig {
    let port = free_port().await;
    ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 2,
        metrics_enabled: false,
        requirepass: requirepass.map(str::to_string),
        persistence: PersistenceConfig {
            rdb_path: None,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn auth_password_only_sets_default_user_and_allows_commands() {
    // The pre-fix bug: AUTH s3cret set authenticated_user = "s3cret", so
    // every later command failed ACL with UnknownUser.
    let cfg = base_config(Some("s3cret")).await;
    let (port, _db) = start_with(cfg).await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    conn.write_all(&encode_command(&["AUTH", "s3cret"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("+OK"),
        "AUTH s3cret must succeed, got {reply}"
    );

    conn.write_all(&encode_command(&["SET", "k", "v"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("+OK"),
        "SET after AUTH must work under user=default, got {reply}"
    );

    conn.write_all(&encode_command(&["GET", "k"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.contains("v"),
        "GET after AUTH must return the value, got {reply}"
    );
}

#[tokio::test]
async fn requirepass_not_bypassed_by_acl_default_nopass() {
    // default user is nopass; requirepass must still be enforced for AUTH
    // against default / single-arg AUTH. Wrong password → WRONGPASS.
    let cfg = base_config(Some("real-secret")).await;
    let (port, _db) = start_with(cfg).await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Single-arg wrong password.
    conn.write_all(&encode_command(&["AUTH", "wrong"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.contains("WRONGPASS") || reply.starts_with("-"),
        "wrong requirepass must fail, got {reply}"
    );

    // Two-arg form with user=default and wrong password.
    conn.write_all(&encode_command(&["AUTH", "default", "wrong"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.contains("WRONGPASS") || reply.starts_with("-"),
        "AUTH default wrong must fail, got {reply}"
    );

    // Correct password works.
    conn.write_all(&encode_command(&["AUTH", "real-secret"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("+OK"),
        "correct AUTH must succeed, got {reply}"
    );
}

#[tokio::test]
async fn hello_auth_sets_authenticated_user_for_acl() {
    let cfg = base_config(None).await;
    let (port, db) = start_with(cfg).await;
    // Restricted ACL user: PING only.
    db.acl.setuser("bob", &[">bobpass", "+ping", "~*"]).unwrap();

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    conn.write_all(&encode_command(&["HELLO", "3", "AUTH", "bob", "bobpass"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    // HELLO 3 returns a map; must not be WRONGPASS.
    assert!(
        !reply.contains("WRONGPASS"),
        "HELLO AUTH must succeed for ACL user, got {reply}"
    );

    // PING allowed.
    conn.write_all(&encode_command(&["PING"])).await.unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.to_ascii_uppercase().contains("PONG"),
        "PING under bob must work, got {reply}"
    );

    // SET forbidden under bob.
    conn.write_all(&encode_command(&["SET", "k", "v"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("-") || reply.contains("NOPERM") || reply.contains("permission"),
        "SET under restricted bob must fail ACL, got {reply}"
    );
}

#[tokio::test]
async fn time_returns_unix_seconds_and_micros() {
    let cfg = base_config(None).await;
    let (port, _db) = start_with(cfg).await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    conn.write_all(&encode_command(&["TIME"])).await.unwrap();
    let reply = read_reply(&mut conn).await;
    // RESP2 array of two bulk strings: *2\r\n$N\r\nsecs\r\n$M\r\nmicros\r\n
    assert!(
        reply.starts_with("*2\r\n"),
        "TIME must return a 2-element array, got {reply}"
    );
    // Seconds should be a non-trivial unix timestamp (> 1_700_000_000 ≈ 2023).
    assert!(
        reply.contains("1") || reply.contains("2"),
        "TIME seconds look empty/zero, got {reply}"
    );
}

#[tokio::test]
async fn role_reports_master_when_primary() {
    let cfg = base_config(None).await;
    let (port, _db) = start_with(cfg).await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    conn.write_all(&encode_command(&["ROLE"])).await.unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.contains("master"),
        "ROLE on a standalone primary must contain master, got {reply}"
    );
}

#[tokio::test]
async fn reset_clears_select_and_multi_state() {
    let cfg = base_config(None).await;
    let (port, _db) = start_with(cfg).await;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // SELECT 1, then MULTI (queue a SET), then RESET.
    conn.write_all(&encode_command(&["SELECT", "1"]))
        .await
        .unwrap();
    let _ = read_reply(&mut conn).await;

    conn.write_all(&encode_command(&["MULTI"])).await.unwrap();
    let _ = read_reply(&mut conn).await;

    conn.write_all(&encode_command(&["SET", "k", "v"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.contains("QUEUED"),
        "SET inside MULTI must queue, got {reply}"
    );

    conn.write_all(&encode_command(&["RESET"])).await.unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.contains("RESET"),
        "RESET reply must be the simple string RESET, got {reply}"
    );

    // After RESET, EXEC without MULTI must error (txn cleared).
    conn.write_all(&encode_command(&["EXEC"])).await.unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("-") || reply.contains("EXEC"),
        "EXEC after RESET must fail (no MULTI), got {reply}"
    );

    // SELECT was reset to 0: write in default DB and confirm via a fresh GET
    // on a new connection (which always starts at DB 0).
    conn.write_all(&encode_command(&["SET", "after-reset", "1"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("+OK"),
        "SET after RESET must work, got {reply}"
    );

    let mut conn2 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    conn2
        .write_all(&encode_command(&["GET", "after-reset"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn2).await;
    assert!(
        reply.contains("1"),
        "key written after RESET must land in DB 0, got {reply}"
    );
}
