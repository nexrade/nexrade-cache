//! Tiny HTTP server for Prometheus metrics scraping.

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::counters::Metrics;

pub struct MetricsServer;

impl MetricsServer {
    /// Start the metrics HTTP server in the background.
    pub async fn start(port: u16, metrics: Metrics) {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        tokio::spawn(async move {
            match TcpListener::bind(addr).await {
                Ok(listener) => {
                    info!("metrics server listening on http://{}/metrics", addr);
                    loop {
                        match listener.accept().await {
                            Ok((mut stream, _)) => {
                                let metrics = metrics.clone();
                                tokio::spawn(async move {
                                    let mut buf = [0u8; 1024];
                                    let _ = stream.read(&mut buf).await;

                                    let body = metrics.render();
                                    let response = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
                                        body.len(),
                                        body
                                    );
                                    let _ = stream.write_all(response.as_bytes()).await;
                                });
                            }
                            Err(e) => {
                                error!("metrics accept error: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("failed to bind metrics server on {}: {}", addr, e);
                }
            }
        });
    }
}
