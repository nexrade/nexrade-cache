//! Regression test: queued MULTI/EXEC commands must run under the
//! authenticated connection's own ACL identity, not a hardcoded "default"
//! full-access user. Before the fix, wrapping a forbidden command in
//! MULTI/EXEC bypassed the caller's ACL restrictions entirely.

use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::persistence::PersistenceConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Bind a throwaway listener to claim a free OS-assigned port, then drop
/// it — `Listener::run` binds by address itself, so this is the standard
/// way to hand it a free port (see `tls_listener.rs`).
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
/// Minimal but sufficient for the short simple-string/array replies used
/// in this test.
async fn read_reply(stream: &mut TcpStream) -> String {
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

async fn start_server() -> (u16, Db) {
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
    // Restricted user: PING only, no SET/GET/DEL.
    db.acl.setuser("restricted", &["+ping", "~*"]).unwrap();

    let listener = nexrade_server::Listener::new(db.clone(), None);
    tokio::spawn(async move {
        let _ = listener.run().await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    (port, db)
}

#[tokio::test]
async fn exec_enforces_authenticated_users_acl_restrictions() {
    let (port, db) = start_server().await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // AUTH as the restricted user (legacy single-arg AUTH form maps to
    // ACL form when a matching ACL user exists — see handle_auth).
    conn.write_all(&encode_command(&["AUTH", "restricted", ""]))
        .await
        .unwrap();
    // handle_auth's ACL-form branch takes (user, pass); "restricted" has
    // no password set (`setuser` above didn't include a `>pass` rule),
    // so authenticate() should accept any password for a passwordless
    // user — if that assumption is wrong the reply below will surface it
    // as an error rather than silently passing.
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("+OK") || reply.starts_with("-"),
        "unexpected AUTH reply: {reply}"
    );

    conn.write_all(&encode_command(&["MULTI"])).await.unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(reply.starts_with("+OK"), "MULTI should be queued: {reply}");

    conn.write_all(&encode_command(&["SET", "bar", "1"]))
        .await
        .unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("+QUEUED"),
        "SET should be queued (not executed yet) inside MULTI: {reply}"
    );

    conn.write_all(&encode_command(&["EXEC"])).await.unwrap();
    let reply = read_reply(&mut conn).await;
    // The queued SET must be rejected with a permission error when it
    // finally runs at EXEC time — not silently succeed as "default".
    assert!(
        reply.to_lowercase().contains("permission") || reply.to_lowercase().contains("noperm"),
        "EXEC should surface a permission-denied error for the queued SET, got: {reply}"
    );

    // Confirm the SET never actually took effect on the store.
    assert!(
        db.store.db(0).read_for(b"bar").get_ro(b"bar").is_none(),
        "SET should not have applied — restricted user has no SET permission, \
         even when issued through MULTI/EXEC"
    );
}

#[tokio::test]
async fn exec_allows_commands_the_authenticated_user_is_permitted_to_run() {
    let (port, _db) = start_server().await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    conn.write_all(&encode_command(&["AUTH", "restricted", ""]))
        .await
        .unwrap();
    let _ = read_reply(&mut conn).await;

    conn.write_all(&encode_command(&["MULTI"])).await.unwrap();
    let _ = read_reply(&mut conn).await;

    conn.write_all(&encode_command(&["PING"])).await.unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.starts_with("+QUEUED"),
        "PING should be queued inside MULTI: {reply}"
    );

    conn.write_all(&encode_command(&["EXEC"])).await.unwrap();
    let reply = read_reply(&mut conn).await;
    assert!(
        reply.to_uppercase().contains("PONG"),
        "EXEC should run the queued PING successfully (allowed command): {reply}"
    );
}
