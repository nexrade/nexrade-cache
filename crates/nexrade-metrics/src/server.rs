//! HTTP servers for Prometheus metrics scraping and operations health/readiness
//! probes. The two surfaces are independent: `MetricsServer` is a pure
//! Prometheus endpoint; `HealthServer` answers `/healthz` and `/readyz` from
//! the live `Db` health snapshot. Both use bounded raw-Tokio HTTP — no
//! additional HTTP framework dependency.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info};

use nexrade_core::db::Db;
use nexrade_core::health::health_report;

use crate::counters::Metrics;

/// Maximum time to read the HTTP request line before responding with `400`.
const HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Maximum bytes to read for the HTTP request line.
const HTTP_HEADER_MAX_BYTES: usize = 8 * 1024;

/// Bound the number of concurrent HTTP connections per listener. Each
/// connection runs in its own task; this is a safety cap, not a pool.
const HTTP_MAX_CONCURRENT: usize = 256;

/// Outcome of reading an HTTP request head off the wire.
enum ReadHead {
    /// A complete request line was received; the payload is the first line.
    Line(String),
    /// The peer closed before sending anything. No response is warranted.
    Closed,
    /// Malformed, oversized, or too slow. The payload is the `400` reason.
    Bad(&'static str),
}

/// Read from `stream` until the end of the HTTP request line is seen.
///
/// The previous implementation issued a single `read()` and parsed whatever
/// arrived. TCP makes no promise that a request line arrives in one segment —
/// a client that dribbles bytes, or any peer whose write is split across
/// segments, got a spurious `400`. This accumulates until it sees a newline,
/// then hands back just that first line.
///
/// Bounded three ways so a slow or hostile peer cannot pin the task: the total
/// deadline (`HTTP_HEADER_READ_TIMEOUT`) spans *all* reads rather than
/// resetting per read, the buffer is capped at `HTTP_HEADER_MAX_BYTES`, and
/// EOF without a terminator is an error rather than a parse of a partial line.
async fn read_request_head(stream: &mut tokio::net::TcpStream) -> ReadHead {
    let deadline = tokio::time::Instant::now() + HTTP_HEADER_READ_TIMEOUT;
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];

    loop {
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line_bytes = &buf[..pos];
            // Tolerate a bare LF as well as CRLF; strip the CR if present.
            let line_bytes = line_bytes.strip_suffix(b"\r").unwrap_or(line_bytes);
            return match std::str::from_utf8(line_bytes) {
                Ok(s) => ReadHead::Line(s.to_string()),
                Err(_) => ReadHead::Bad("invalid request line"),
            };
        }
        if buf.len() >= HTTP_HEADER_MAX_BYTES {
            return ReadHead::Bad("request line too long");
        }

        let read = tokio::time::timeout_at(deadline, stream.read(&mut chunk)).await;
        match read {
            Ok(Ok(0)) => {
                // Clean EOF. Nothing buffered means an idle probe (or a health
                // checker's TCP-connect test) — stay silent. Bytes buffered
                // without a newline is a truncated request.
                return if buf.is_empty() {
                    ReadHead::Closed
                } else {
                    ReadHead::Bad("unterminated request line")
                };
            }
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => return ReadHead::Closed,
            Err(_) => return ReadHead::Bad("read timeout"),
        }
    }
}

pub struct MetricsServer;

impl MetricsServer {
    /// Start the Prometheus `/metrics` HTTP server. Returns after the
    /// listener is bound; the accept loop runs in a background task.
    /// Returns the `JoinHandle` so a caller can await graceful shutdown
    /// (the loop exits when `shutdown_rx` flips to `true`).
    ///
    /// Pass `None` for `shutdown_rx` only when the server should live for the
    /// whole process; the loop then runs until the task is dropped.
    pub async fn start(
        addr: SocketAddr,
        metrics: Metrics,
        shutdown_rx: Option<watch::Receiver<bool>>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("failed to bind metrics server on {}: {}", addr, e);
                return None;
            }
        };
        info!("metrics server listening on http://{}/metrics", addr);
        Some(tokio::spawn(async move {
            Self::run(listener, metrics, shutdown_rx).await;
        }))
    }

    async fn run(
        listener: TcpListener,
        metrics: Metrics,
        mut shutdown_rx: Option<watch::Receiver<bool>>,
    ) {
        let counter = std::sync::Arc::new(tokio::sync::Semaphore::new(HTTP_MAX_CONCURRENT));
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (stream, _) = match res {
                        Ok(p) => p,
                        Err(e) => {
                            error!("metrics accept error: {}", e);
                            continue;
                        }
                    };
                    let metrics = metrics.clone();
                    let permit = match counter.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    tokio::spawn(async move {
                        Self::handle(stream, metrics).await;
                        drop(permit);
                    });
                }
                _ = async {
                    if let Some(ref mut rx) = shutdown_rx {
                        let _ = rx.changed().await;
                    } else {
                        // No shutdown signal; sleep forever.
                        std::future::pending::<()>().await;
                    }
                } => {
                    info!("metrics server shutting down");
                    return;
                }
            }
        }
    }

    async fn handle(mut stream: tokio::net::TcpStream, metrics: Metrics) {
        let first_line = match read_request_head(&mut stream).await {
            ReadHead::Line(l) => l,
            ReadHead::Closed => return,
            ReadHead::Bad(reason) => {
                let _ = write_response(
                    &mut stream,
                    400,
                    "Bad Request",
                    "text/plain; charset=utf-8",
                    reason.as_bytes(),
                )
                .await;
                return;
            }
        };
        let (method, path, _rest) = match parse_request_line(&first_line) {
            Some(t) => t,
            None => {
                let _ = write_response(
                    &mut stream,
                    400,
                    "Bad Request",
                    "text/plain; charset=utf-8",
                    b"malformed request line",
                )
                .await;
                return;
            }
        };
        match (method, path) {
            ("GET", "/metrics") => {
                let body = metrics.render();
                let _ = write_response(
                    &mut stream,
                    200,
                    "OK",
                    "text/plain; version=0.0.4",
                    body.as_bytes(),
                )
                .await;
            }
            ("GET", _) => {
                let _ = write_response(
                    &mut stream,
                    404,
                    "Not Found",
                    "text/plain; charset=utf-8",
                    b"not found",
                )
                .await;
            }
            (_, _) => {
                let _ = write_response(
                    &mut stream,
                    405,
                    "Method Not Allowed",
                    "text/plain; charset=utf-8",
                    b"method not allowed",
                )
                .await;
            }
        }
    }
}

pub struct HealthServer;

impl HealthServer {
    /// Start the health/readiness HTTP server. Returns after the listener
    /// is bound; the accept loop runs in a background task. Returns the
    /// `JoinHandle` so a caller can await graceful shutdown (the loop
    /// exits when `shutdown_rx` flips to `true`).
    pub async fn start(
        addr: SocketAddr,
        db: Db,
        shutdown_rx: Option<watch::Receiver<bool>>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("failed to bind health server on {}: {}", addr, e);
                return None;
            }
        };
        info!(
            "health server listening on http://{}/healthz and /readyz",
            addr
        );
        Some(tokio::spawn(async move {
            Self::run(listener, db, shutdown_rx).await;
        }))
    }

    async fn run(listener: TcpListener, db: Db, mut shutdown_rx: Option<watch::Receiver<bool>>) {
        let counter = std::sync::Arc::new(tokio::sync::Semaphore::new(HTTP_MAX_CONCURRENT));
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (stream, _) = match res {
                        Ok(p) => p,
                        Err(e) => {
                            error!("health accept error: {}", e);
                            continue;
                        }
                    };
                    let db = db.clone();
                    let permit = match counter.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    tokio::spawn(async move {
                        Self::handle(stream, db).await;
                        drop(permit);
                    });
                }
                _ = async {
                    if let Some(ref mut rx) = shutdown_rx {
                        let _ = rx.changed().await;
                    } else {
                        // No shutdown signal; sleep forever.
                        std::future::pending::<()>().await;
                    }
                } => {
                    info!("health server shutting down");
                    return;
                }
            }
        }
    }

    async fn handle(mut stream: tokio::net::TcpStream, db: Db) {
        let first_line = match read_request_head(&mut stream).await {
            ReadHead::Line(l) => l,
            ReadHead::Closed => return,
            ReadHead::Bad(reason) => {
                let _ = write_response(
                    &mut stream,
                    400,
                    "Bad Request",
                    "text/plain; charset=utf-8",
                    reason.as_bytes(),
                )
                .await;
                return;
            }
        };
        let (method, path, _rest) = match parse_request_line(&first_line) {
            Some(t) => t,
            None => {
                let _ = write_response(
                    &mut stream,
                    400,
                    "Bad Request",
                    "text/plain; charset=utf-8",
                    b"malformed request line",
                )
                .await;
                return;
            }
        };
        let report = health_report(&db);
        // S3 (1.3.0): this surface has no authentication of any kind, so by
        // default it serves only the minimal probe shape (liveness, readiness,
        // phase, reason codes). `health.expose_details = true` opts back into
        // the full report, which carries the configured RDB and AOF paths plus
        // the AOF failure text.
        let expose_details = db.config.lock().health.expose_details;
        let probe_body = || -> Vec<u8> {
            if expose_details {
                serde_json::to_vec(&report).unwrap_or_default()
            } else {
                serde_json::to_vec(&nexrade_core::health::ProbeReport::from(&report))
                    .unwrap_or_default()
            }
        };
        match (method, path) {
            ("GET", "/healthz") => {
                let body = probe_body();
                let status = if report.live { 200 } else { 503 };
                let _ = write_response(
                    &mut stream,
                    status,
                    if report.live {
                        "OK"
                    } else {
                        "Service Unavailable"
                    },
                    "application/json",
                    &body,
                )
                .await;
            }
            ("GET", "/readyz") => {
                let body = probe_body();
                let status = if report.ready { 200 } else { 503 };
                let _ = write_response(
                    &mut stream,
                    status,
                    if report.ready {
                        "OK"
                    } else {
                        "Service Unavailable"
                    },
                    "application/json",
                    &body,
                )
                .await;
            }
            ("GET", _) => {
                let _ = write_response(
                    &mut stream,
                    404,
                    "Not Found",
                    "text/plain; charset=utf-8",
                    b"not found",
                )
                .await;
            }
            (_, _) => {
                let _ = write_response(
                    &mut stream,
                    405,
                    "Method Not Allowed",
                    "text/plain; charset=utf-8",
                    b"method not allowed",
                )
                .await;
            }
        }
    }
}

/// Parse a single HTTP request line into (method, path, rest). Returns
/// `None` for malformed input so callers can map to 400.
fn parse_request_line(line: &str) -> Option<(&str, &str, &str)> {
    let mut parts = line.splitn(3, ' ');
    let method = parts.next()?;
    let path = parts.next()?;
    let rest = parts.next().unwrap_or("");
    if method.is_empty() || path.is_empty() {
        return None;
    }
    Some((method, path, rest))
}

async fn write_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status,
        reason,
        content_type,
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}
