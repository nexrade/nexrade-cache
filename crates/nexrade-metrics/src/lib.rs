//! Built-in observability for nexrade-cache.
//!
//! Provides:
//! - Prometheus metrics endpoint at /metrics
//! - OpenTelemetry tracing via tracing-opentelemetry
//! - Structured JSON logging
//!
//! # Quick start
//!
//! ```rust,no_run
//! use std::net::SocketAddr;
//! use nexrade_metrics::{MetricsServer, Metrics, init_tracing};
//!
//! #[tokio::main]
//! async fn main() {
//!     init_tracing();
//!     let addr: SocketAddr = "0.0.0.0:9091".parse().unwrap();
//!     // `None` runs the accept loop until the process exits. Pass a
//!     // `watch::Receiver<bool>` to stop it when the server drains.
//!     let _handle = MetricsServer::start(addr, Metrics::new(), None).await;
//! }
//! ```

pub mod counters;
pub mod server;
pub mod tracing_setup;

pub use counters::{CommandMetricHandles, Metrics};
pub use server::{HealthServer, MetricsServer};
pub use tracing_setup::init_tracing;
