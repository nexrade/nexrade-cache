//! HTTP-level tests for the operations surface. Each test boots a
//! `MetricsServer` or `HealthServer` against a randomly-chosen port,
//! drives it over a real TCP socket, and asserts the response. The
//! servers are independent so each test owns a fresh listener.

use std::net::SocketAddr;
use std::time::Duration;

use nexrade_core::db::Db;
use nexrade_core::health::HealthPhase;
use nexrade_metrics::{HealthServer, Metrics, MetricsServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn http_get(addr: SocketAddr, request: &str) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 64 * 1024];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("response within 2s")
        .expect("read ok");
    let raw = String::from_utf8_lossy(&buf[..n]).into_owned();
    // Status line: "HTTP/1.1 <code> <reason>\r\n..."
    let mut lines = raw.split("\r\n");
    let status = lines.next().unwrap_or("").to_string();
    let code: u16 = status
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // Skip headers until empty line.
    let mut body_lines = Vec::new();
    let mut in_body = false;
    for line in lines {
        if in_body {
            body_lines.push(line.to_string());
        } else if line.is_empty() {
            in_body = true;
        }
    }
    (code, status, body_lines.join("\n"))
}

async fn spawn_metrics_server(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    let handle = MetricsServer::start(addr, Metrics::new(), None).await;
    handle.expect("metrics server should bind")
}

async fn spawn_health_server(
    addr: SocketAddr,
    db: Db,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::watch::Sender<bool>,
) {
    let (tx, rx) = tokio::sync::watch::channel(false);
    let handle = HealthServer::start(addr, db, Some(rx))
        .await
        .expect("health server should bind");
    (handle, tx)
}

#[tokio::test]
async fn metrics_endpoint_serves_prometheus_text() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let metrics = Metrics::new();
    // Touch a label so a series exists and the body is non-empty.
    metrics.record_command("PING", 0.0001, false);
    let handle = MetricsServer::start(addr, metrics, None)
        .await
        .expect("metrics server should bind");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, status, body) = http_get(
        addr,
        "GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(
        code, 200,
        "GET /metrics should be 200, got {status}\n{body}"
    );
    assert!(
        body.contains("nexrade_commands_total"),
        "expected nexrade_commands_total in metrics body, got: {body}"
    );
    assert!(
        body.contains("cmd=\"PING\""),
        "expected PING-labeled series after record_command, got: {body}"
    );
}

#[tokio::test]
async fn metrics_endpoint_returns_404_for_unknown_path() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let handle = spawn_metrics_server(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, status, _body) = http_get(
        addr,
        "GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    // MetricsServer only serves /metrics; other routes must be 404.
    assert_eq!(
        code, 404,
        "GET /healthz on metrics server should be 404, got {status}"
    );
}

#[tokio::test]
async fn metrics_endpoint_returns_405_for_post() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let handle = spawn_metrics_server(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, status, _body) = http_get(
        addr,
        "POST /metrics HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(code, 405, "POST /metrics should be 405, got {status}");
}

#[tokio::test]
async fn metrics_endpoint_returns_400_for_malformed_request_line() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let handle = spawn_metrics_server(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, status, _body) = http_get(addr, "garbage\r\n\r\n").await;
    handle.abort();

    assert_eq!(code, 400, "malformed request should be 400, got {status}");
}

#[tokio::test]
async fn health_endpoint_serves_json_in_ready_state() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    db.lifecycle().set_phase(HealthPhase::Ready);
    let (handle, _tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, status, body) = http_get(
        addr,
        "GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(
        code, 200,
        "ready /healthz should be 200, got {status}\n{body}"
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&body).expect("healthz body must be valid JSON");
    assert_eq!(parsed["live"], serde_json::Value::Bool(true));
    assert_eq!(parsed["ready"], serde_json::Value::Bool(true));
    assert_eq!(parsed["phase"], serde_json::Value::String("ready".into()));
}

#[tokio::test]
async fn health_endpoint_returns_503_when_not_ready() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    // Default phase is Starting (1.3.0+; was Recovering), which is live but
    // not ready. The status code is unchanged — only the phase string differs.
    let (handle, _tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, status, body) = http_get(
        addr,
        "GET /readyz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(
        code, 503,
        "Starting /readyz should be 503, got {status}\n{body}"
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&body).expect("readyz body must be valid JSON");
    assert_eq!(parsed["ready"], serde_json::Value::Bool(false));
    assert_eq!(
        parsed["phase"],
        serde_json::Value::String("starting".into()),
        "a fresh Db reports the Starting phase"
    );
}

#[tokio::test]
async fn health_endpoint_returns_404_for_unknown_path() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    let (handle, _tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, status, _body) = http_get(
        addr,
        "GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(
        code, 404,
        "GET /metrics on health server should be 404, got {status}"
    );
}

#[tokio::test]
async fn health_endpoint_returns_405_for_post() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    let (handle, _tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, status, _body) = http_get(
        addr,
        "POST /healthz HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(code, 405, "POST /healthz should be 405, got {status}");
}

#[tokio::test]
async fn health_server_stops_on_shutdown_signal() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    let (handle, tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger shutdown.
    tx.send(true).unwrap();

    // Server task should exit promptly.
    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("server exits in 2s");
    result.expect("server task ok");
}

#[tokio::test]
async fn metrics_construction_is_idempotent() {
    // Regression: earlier builds used process-global Prometheus registries,
    // so the second `Metrics::new()` panicked on duplicate registration.
    // Two independently constructed instances must coexist.
    let _a = Metrics::new();
    let _b = Metrics::new();
}

// ─── F5: request line split across TCP segments ───────────────────────────────
//
// Through 1.2.3 both handlers issued a single `read()` and parsed whatever had
// arrived. TCP makes no promise the request line lands in one segment, so a
// client whose write was split — or which dribbled bytes deliberately — got a
// spurious 400 on a perfectly valid request.

/// Write a request one slice at a time with a pause between, forcing the
/// server to see several distinct reads before the line terminator.
///
/// Stops writing as soon as the request line is complete. The server answers
/// the moment it has that line and closes the connection (`Connection: close`),
/// so any later write races the close — and on Windows writing to a socket the
/// peer has already closed raises RST, which discards the *already-received*
/// response and makes the subsequent read fail with WSAECONNABORTED (10053)
/// instead of returning the buffered bytes. Sending only up to the terminator
/// keeps the dribble behaviour the test is after without provoking that reset.
async fn http_get_dribbled(addr: SocketAddr, chunks: &[&str]) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    for chunk in chunks {
        if stream.write_all(chunk.as_bytes()).await.is_err() {
            break;
        }
        let _ = stream.flush().await;
        // The request line ends at the first newline; everything after it is
        // header material the handler does not need in order to reply.
        if chunk.contains('\n') {
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let mut buf = vec![0u8; 64 * 1024];
    let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
        .await
        .expect("response within 3s")
        .expect("read ok");
    let raw = String::from_utf8_lossy(&buf[..n]).into_owned();
    let status = raw.split("\r\n").next().unwrap_or("").to_string();
    let code: u16 = status
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (code, raw)
}

#[tokio::test]
async fn metrics_accepts_request_line_split_across_segments() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let handle = spawn_metrics_server(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Split mid-path: "GET /met" + "rics HTTP/1.1" + terminator.
    let (code, raw) = http_get_dribbled(
        addr,
        &["GET /met", "rics HTTP/1.1\r\n", "Host: x\r\n", "\r\n"],
    )
    .await;
    handle.abort();

    assert_eq!(
        code, 200,
        "a split request line must still be served, got:\n{raw}"
    );
}

#[tokio::test]
async fn health_accepts_request_line_split_across_segments() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    db.lifecycle().set_phase(HealthPhase::Ready);
    let (handle, _tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, raw) = http_get_dribbled(
        addr,
        &["GET /heal", "thz HTTP/1.1\r\n", "Host: x\r\n", "\r\n"],
    )
    .await;
    handle.abort();

    assert_eq!(
        code, 200,
        "a split request line must still be served, got:\n{raw}"
    );
}

#[tokio::test]
async fn metrics_returns_400_for_unterminated_request_line() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let handle = spawn_metrics_server(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a partial line then half-close: EOF with bytes buffered and no
    // terminator is a truncated request, not a parseable one.
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(b"GET /metrics HTTP/1.1").await.unwrap();
    stream.shutdown().await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
        .await
        .expect("response within 3s")
        .expect("read ok");
    let raw = String::from_utf8_lossy(&buf[..n]).into_owned();
    handle.abort();

    assert!(
        raw.starts_with("HTTP/1.1 400"),
        "unterminated request line should be 400, got:\n{raw}"
    );
}

// ─── F7: the metrics server observes the shutdown signal ──────────────────────
//
// `HealthServer` already accepted a receiver; `MetricsServer` had no shutdown
// parameter at all, so its accept loop ran until process exit.

#[tokio::test]
async fn metrics_server_stops_on_shutdown_signal() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let (tx, rx) = tokio::sync::watch::channel(false);
    let handle = MetricsServer::start(addr, Metrics::new(), Some(rx))
        .await
        .expect("metrics server should bind");
    tokio::time::sleep(Duration::from_millis(50)).await;

    tx.send(true).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("metrics server exits within 2s of the shutdown signal");
    result.expect("server task ok");
}

#[tokio::test]
async fn metrics_server_stops_serving_after_shutdown() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let (tx, rx) = tokio::sync::watch::channel(false);
    let handle = MetricsServer::start(addr, Metrics::new(), Some(rx))
        .await
        .expect("metrics server should bind");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Serving before the drain.
    let (code, _, _) = http_get(
        addr,
        "GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(code, 200, "should serve before shutdown");

    tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

    // The listener socket is closed once the accept loop returns, so a new
    // connection must not be answered with a 200. Either the connect fails
    // outright or the read yields nothing — both are acceptable; a served
    // 200 is not.
    let after = tokio::time::timeout(Duration::from_secs(2), async {
        let mut stream = TcpStream::connect(addr).await.ok()?;
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .ok()?;
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.ok()?;
        Some(String::from_utf8_lossy(&buf[..n]).into_owned())
    })
    .await
    .unwrap_or(None);

    match after {
        // Connect failed, or the socket closed with no bytes — both mean the
        // listener is gone, which is the point.
        None => {}
        Some(body) => assert!(
            !body.contains("200 OK"),
            "metrics server still served 200 after shutdown:\n{body}"
        ),
    }
}

// ─── F3: lifecycle transitions are one-way, Failed is terminal ────────────────

#[tokio::test]
async fn health_endpoint_keeps_503_after_failed_even_if_ready_is_attempted() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = Db::new(nexrade_core::db::ServerConfig::default());
    db.lifecycle().set_phase(HealthPhase::Recovering);
    db.lifecycle().set_phase(HealthPhase::Failed);
    // A permanent startup failure must not be maskable by a later Ready. Prior
    // to 1.3.0 `set_phase` was an unconditional store, so this flipped the
    // instance back to live+ready and `/healthz` returned 200.
    db.lifecycle().set_phase(HealthPhase::Ready);

    let (handle, _tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, status, body) = http_get(
        addr,
        "GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(
        code, 503,
        "Failed is terminal; /healthz must stay 503, got {status}\n{body}"
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&body).expect("healthz body must be valid JSON");
    assert_eq!(parsed["live"], serde_json::Value::Bool(false));
    assert_eq!(parsed["phase"], serde_json::Value::String("failed".into()));
}

// ─── S3: the unauthenticated probe body is minimal by default ─────────────────
//
// `/healthz` and `/readyz` have no authentication of any kind. Through 1.2.3
// they serialized the entire `HealthReport`, which carries
// `rdb_configured_path`, `aof_configured_path`, and the AOF failure message —
// filesystem layout and internal error text handed to any client that can
// reach the port. 1.3.0 restricts the default body to liveness, readiness,
// phase, and reason *codes*; `health.expose_details = true` opts back in.

/// A `Db` whose persistence paths are distinctive enough to grep for in a body.
fn db_with_secret_paths(expose_details: bool) -> Db {
    let mut config = nexrade_core::db::ServerConfig::default();
    config.persistence.rdb_path = Some("/srv/secret-topology/nexrade.rdb".to_string());
    config.persistence.aof_path = Some("/srv/secret-topology/nexrade.aof".to_string());
    config.health.expose_details = expose_details;
    Db::new(config)
}

#[tokio::test]
async fn healthz_does_not_disclose_persistence_paths_by_default() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = db_with_secret_paths(false);
    db.lifecycle().set_phase(HealthPhase::Ready);
    let (handle, _tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, _status, body) = http_get(
        addr,
        "GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(code, 200);
    assert!(
        !body.contains("secret-topology"),
        "the default probe body must not disclose persistence paths:\n{body}"
    );
    assert!(
        !body.contains("rdb_configured_path") && !body.contains("aof_configured_path"),
        "path fields must be absent entirely:\n{body}"
    );
    // Still useful to an orchestrator. `ready` is false here: the config names
    // an AOF path whose writer was never opened (no listener ran), which is a
    // legitimate AofUnavailable reason — the point is that liveness, phase, and
    // the reason code all survive redaction.
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    assert_eq!(parsed["live"], serde_json::Value::Bool(true));
    assert_eq!(parsed["phase"], serde_json::Value::String("ready".into()));
    assert!(parsed["reasons"].is_array(), "reason codes are present");
}

#[tokio::test]
async fn readyz_reports_reason_codes_without_messages_by_default() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = db_with_secret_paths(false);
    db.lifecycle().set_phase(HealthPhase::Ready);
    // Latch an AOF failure whose message would otherwise be published.
    db.fail_aof(
        "append",
        "/srv/secret-topology/nexrade.aof: disk quota exceeded",
    );
    let (handle, _tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, _status, body) = http_get(
        addr,
        "GET /readyz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(code, 503, "an AOF failure keeps readiness off");
    assert!(
        !body.contains("disk quota exceeded") && !body.contains("secret-topology"),
        "the failure message must not reach an unauthenticated probe:\n{body}"
    );
    // The machine-readable code still says *why*, from a closed enum.
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    let reasons = parsed["reasons"].as_array().expect("reasons array");
    assert!(
        reasons.iter().any(|r| r == "aof_failed"),
        "reason code must still be present:\n{body}"
    );
}

#[tokio::test]
async fn expose_details_restores_the_full_report() {
    let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
    let db = db_with_secret_paths(true);
    db.lifecycle().set_phase(HealthPhase::Ready);
    let (handle, _tx) = spawn_health_server(addr, db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (code, _status, body) = http_get(
        addr,
        "GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    handle.abort();

    assert_eq!(code, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    assert_eq!(
        parsed["persistence"]["rdb_configured_path"],
        serde_json::Value::String("/srv/secret-topology/nexrade.rdb".into()),
        "opting in restores the pre-1.3.0 body"
    );
    assert!(
        parsed["replication"].is_object(),
        "the full report includes replication detail"
    );
}

#[tokio::test]
async fn probe_status_codes_are_identical_in_both_modes() {
    // The contract change is the body only. An orchestrator keying on the
    // status code must see no difference.
    for expose in [false, true] {
        let addr: SocketAddr = format!("127.0.0.1:{}", pick_free_port()).parse().unwrap();
        let db = db_with_secret_paths(expose);
        // Starting: live but not ready.
        let (handle, _tx) = spawn_health_server(addr, db).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (healthz, _, _) = http_get(
            addr,
            "GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        )
        .await;
        let (readyz, _, _) = http_get(
            addr,
            "GET /readyz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        )
        .await;
        handle.abort();

        assert_eq!(healthz, 200, "expose_details={expose}: live -> 200");
        assert_eq!(readyz, 503, "expose_details={expose}: not ready -> 503");
    }
}
