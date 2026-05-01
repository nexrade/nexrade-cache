//! Tracing and logging initialization.

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize the tracing subscriber with structured JSON or pretty output.
///
/// Respects RUST_LOG environment variable (e.g. `RUST_LOG=nexrade=debug,warn`).
pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let use_json = std::env::var("NEXRADE_LOG_JSON")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);

    if use_json {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt::layer().compact().with_ansi(true))
            .init();
    }
}

/// Initialize tracing for tests (non-panicking).
pub fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter(EnvFilter::new("debug"))
        .try_init();
}
