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
    let handle = MetricsServer::start(addr, Metrics::new()).await;
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
    let handle = MetricsServer::start(addr, metrics)
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
    // Default phase is Recovering, which is live but not ready.
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
        "Recovering /readyz should be 503, got {status}\n{body}"
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&body).expect("readyz body must be valid JSON");
    assert_eq!(parsed["ready"], serde_json::Value::Bool(false));
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
