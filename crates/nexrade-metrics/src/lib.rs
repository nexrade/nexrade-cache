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
//! use nexrade_metrics::{MetricsServer, Metrics, init_tracing};
//!
//! #[tokio::main]
//! async fn main() {
//!     init_tracing();
//!     MetricsServer::start(9091, Metrics::new()).await;
//! }
//! ```

pub mod counters;
pub mod server;
pub mod tracing_setup;

pub use counters::{CommandMetricHandles, Metrics};
pub use server::MetricsServer;
pub use tracing_setup::init_tracing;
