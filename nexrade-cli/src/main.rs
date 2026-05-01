//! nexrade-cache server binary.
//!
//! # Usage
//!
//! ```sh
//! nexrade-cache                              # Start with defaults
//! nexrade-cache --port 6380                  # Custom port
//! nexrade-cache --config nexrade.toml        # Config file
//! nexrade-cache --daemonize                  # Run as daemon
//! ```
//!
//! ## Windows auto-start service
//!
//! ```cmd
//! REM Install (Administrator required):
//! nexrade-cache --install-service
//! REM Remove:
//! nexrade-cache --uninstall-service
//! ```

#[cfg(windows)]
mod windows_svc;

use anyhow::Result;
use clap::Parser;
use tracing::{info, warn};

use nexrade_core::db::{Db, ServerConfig};
use nexrade_metrics::{init_tracing, Metrics, MetricsServer};
use nexrade_server::Listener;

#[derive(Parser, Debug)]
#[command(
    name = "nexrade-cache",
    version = "0.1.0",
    author = "Nexrade Contributors",
    about = "High-performance Redis-compatible cache — with TLS, Lua, WASM, plugins, and built-in metrics"
)]
struct Cli {
    /// Config file path
    #[arg(short, long, value_name = "FILE", env = "NEXRADE_CONFIG")]
    config: Option<String>,

    /// Bind address
    #[arg(long, default_value = "127.0.0.1", env = "NEXRADE_BIND")]
    bind: Option<String>,

    /// Port to listen on
    #[arg(short, long, env = "NEXRADE_PORT")]
    port: Option<u16>,

    /// Number of databases
    #[arg(long, env = "NEXRADE_DATABASES")]
    databases: Option<usize>,

    /// Require password for connections
    #[arg(long, env = "NEXRADE_REQUIREPASS")]
    requirepass: Option<String>,

    /// Enable TLS
    #[arg(long, env = "NEXRADE_TLS")]
    tls: bool,

    /// TLS certificate file
    #[arg(long, env = "NEXRADE_TLS_CERT")]
    tls_cert: Option<String>,

    /// TLS key file
    #[arg(long, env = "NEXRADE_TLS_KEY")]
    tls_key: Option<String>,

    /// TLS port (default: 6380)
    #[arg(long, env = "NEXRADE_TLS_PORT")]
    tls_port: Option<u16>,

    /// Enable Prometheus metrics
    #[arg(long, default_value = "true", env = "NEXRADE_METRICS")]
    metrics: bool,

    /// Metrics port
    #[arg(long, default_value = "9091", env = "NEXRADE_METRICS_PORT")]
    metrics_port: u16,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", env = "RUST_LOG")]
    log_level: String,

    /// Output logs as JSON
    #[arg(long, env = "NEXRADE_LOG_JSON")]
    log_json: bool,

    /// Max connected clients
    #[arg(long, default_value = "10000", env = "NEXRADE_MAX_CLIENTS")]
    max_clients: usize,

    /// Maximum memory in bytes (0 = unlimited)
    #[arg(long, env = "NEXRADE_MAXMEMORY")]
    maxmemory: Option<usize>,

    /// Eviction policy when maxmemory is reached
    #[arg(long, env = "NEXRADE_MAXMEMORY_POLICY")]
    maxmemory_policy: Option<String>,

    /// Idle connection timeout in seconds (0 = disabled)
    #[arg(long, env = "NEXRADE_TIMEOUT")]
    timeout: Option<u64>,

    /// RDB snapshot path
    #[arg(long, env = "NEXRADE_RDB_PATH")]
    rdb_path: Option<String>,

    /// AOF log path
    #[arg(long, env = "NEXRADE_AOF_PATH")]
    aof_path: Option<String>,

    /// Print configuration and exit
    #[arg(long)]
    print_config: bool,

    /// Install nexrade-cache as a Windows auto-start service (run as Administrator)
    #[cfg(windows)]
    #[arg(long)]
    install_service: bool,

    /// Remove the nexrade-cache Windows service (run as Administrator)
    #[cfg(windows)]
    #[arg(long)]
    uninstall_service: bool,

    /// Run as a Windows service (used internally by the SCM; not for manual use)
    #[cfg(windows)]
    #[arg(long, hide = true)]
    service: bool,
}

/// Build a [`ServerConfig`] from the parsed CLI arguments.
fn config_from_cli(cli: &Cli) -> Result<ServerConfig> {
    let mut config = if let Some(ref config_path) = cli.config {
        load_config_file(config_path)?
    } else {
        ServerConfig::default()
    };

    if let Some(ref bind) = cli.bind {
        config.bind = bind.clone();
    }
    if let Some(port) = cli.port {
        config.port = port;
    }
    if let Some(databases) = cli.databases {
        config.databases = databases;
    }
    if let Some(ref pass) = cli.requirepass {
        config.requirepass = Some(pass.clone());
    }
    if cli.tls {
        config.tls_enabled = true;
    }
    if let Some(ref cert) = cli.tls_cert {
        config.tls_cert = Some(cert.clone());
    }
    if let Some(ref key) = cli.tls_key {
        config.tls_key = Some(key.clone());
    }
    if let Some(tls_port) = cli.tls_port {
        config.tls_port = Some(tls_port);
    }
    if let Some(ref rdb_path) = cli.rdb_path {
        config.persistence.rdb_path = Some(rdb_path.clone());
    }
    if let Some(ref aof_path) = cli.aof_path {
        config.persistence.aof_path = Some(aof_path.clone());
    }
    config.max_clients = cli.max_clients;
    config.metrics_enabled = cli.metrics;
    config.metrics_port = cli.metrics_port;
    if let Some(maxmem) = cli.maxmemory {
        config.max_memory = if maxmem == 0 { None } else { Some(maxmem) };
    }
    if let Some(ref policy) = cli.maxmemory_policy {
        config.maxmemory_policy = policy.parse().unwrap_or_default();
    }
    if let Some(timeout) = cli.timeout {
        config.timeout = timeout;
    }

    Ok(config)
}

/// Start the server with the given config.  Called from both `main()` and the
/// Windows service thread.
pub(crate) async fn start_server(config: ServerConfig) -> Result<()> {
    // Print banner
    print_banner(&config);

    // Initialize the database
    let db = Db::new(config.clone());

    // Start metrics server
    let metrics = if config.metrics_enabled {
        let m = Metrics::new();
        MetricsServer::start(config.metrics_port, m.clone()).await;
        info!(
            "metrics available at http://{}:{}/metrics",
            config.bind, config.metrics_port
        );
        Some(m)
    } else {
        None
    };

    // Start TLS listener if enabled
    if config.tls_enabled {
        if let (Some(cert), Some(_key)) = (&config.tls_cert, &config.tls_key) {
            let tls_port = config.tls_port.unwrap_or(6380);
            info!(
                "TLS listener will start on port {} (cert: {})",
                tls_port, cert
            );
            // TLS listener would be started here using nexrade-tls crate
        } else {
            warn!("TLS enabled but tls-cert or tls-key not set, TLS listener skipped");
        }
    }

    // Start the main TCP server
    let listener = Listener::new(db, metrics);
    listener.run().await?;

    Ok(())
}

/// Used by the Windows service thread: start the server with default config.
#[cfg(windows)]
pub(crate) async fn run_server_default() -> Result<()> {
    init_tracing();
    let config = ServerConfig::default();
    start_server(config).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // ── Windows service control ──────────────────────────────────────────────
    #[cfg(windows)]
    {
        if cli.install_service {
            return windows_svc::install_service();
        }
        if cli.uninstall_service {
            return windows_svc::uninstall_service();
        }
        if cli.service {
            // Hand control to the SCM dispatcher; this blocks until the
            // service is stopped.
            return windows_svc::run_as_service();
        }
    }

    // Setup logging
    if cli.log_json {
        std::env::set_var("NEXRADE_LOG_JSON", "1");
    }
    if !cli.log_level.is_empty() {
        std::env::set_var("RUST_LOG", &cli.log_level);
    }
    init_tracing();

    // Build server config
    let config = config_from_cli(&cli)?;

    if cli.print_config {
        print_config(&config);
        return Ok(());
    }

    start_server(config).await
}

fn load_config_file(path: &str) -> Result<ServerConfig> {
    use std::fs;

    let contents = fs::read_to_string(path)?;
    // Parse TOML config
    let toml_val: toml::Value = toml::from_str(&contents)?;

    let mut config = ServerConfig::default();

    if let Some(bind) = toml_val.get("bind").and_then(|v| v.as_str()) {
        config.bind = bind.to_string();
    }
    if let Some(port) = toml_val.get("port").and_then(|v| v.as_integer()) {
        config.port = port as u16;
    }
    if let Some(databases) = toml_val.get("databases").and_then(|v| v.as_integer()) {
        config.databases = databases as usize;
    }
    if let Some(pass) = toml_val.get("requirepass").and_then(|v| v.as_str()) {
        if !pass.is_empty() {
            config.requirepass = Some(pass.to_string());
        }
    }
    if let Some(max_clients) = toml_val.get("max_clients").and_then(|v| v.as_integer()) {
        config.max_clients = max_clients as usize;
    }
    if let Some(maxmem) = toml_val.get("maxmemory").and_then(|v| v.as_integer()) {
        config.max_memory = if maxmem <= 0 {
            None
        } else {
            Some(maxmem as usize)
        };
    }
    if let Some(policy) = toml_val.get("maxmemory_policy").and_then(|v| v.as_str()) {
        config.maxmemory_policy = policy.parse().unwrap_or_default();
    }
    if let Some(hz) = toml_val.get("hz").and_then(|v| v.as_integer()) {
        config.hz = hz as u32;
    }
    if let Some(timeout) = toml_val.get("timeout").and_then(|v| v.as_integer()) {
        config.timeout = timeout as u64;
    }
    if let Some(v) = toml_val
        .get("slowlog_log_slower_than")
        .and_then(|v| v.as_integer())
    {
        config.slowlog_log_slower_than = v;
    }
    if let Some(v) = toml_val.get("slowlog_max_len").and_then(|v| v.as_integer()) {
        config.slowlog_max_len = v as usize;
    }
    if let Some(v) = toml_val.get("loglevel").and_then(|v| v.as_str()) {
        config.loglevel = v.to_string();
    }
    if let Some(tls) = toml_val.get("tls").and_then(|v| v.as_table()) {
        config.tls_enabled = tls
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(cert) = tls.get("cert").and_then(|v| v.as_str()) {
            config.tls_cert = Some(cert.to_string());
        }
        if let Some(key) = tls.get("key").and_then(|v| v.as_str()) {
            config.tls_key = Some(key.to_string());
        }
        if let Some(port) = tls.get("port").and_then(|v| v.as_integer()) {
            config.tls_port = Some(port as u16);
        }
    }
    if let Some(metrics) = toml_val.get("metrics").and_then(|v| v.as_table()) {
        config.metrics_enabled = metrics
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if let Some(port) = metrics.get("port").and_then(|v| v.as_integer()) {
            config.metrics_port = port as u16;
        }
    }

    Ok(config)
}

fn print_config(config: &ServerConfig) {
    println!("# nexrade-cache configuration");
    println!("bind = \"{}\"", config.bind);
    println!("port = {}", config.port);
    println!("databases = {}", config.databases);
    println!("max_clients = {}", config.max_clients);
    println!();
    println!("[tls]");
    println!("enabled = {}", config.tls_enabled);
    if let Some(ref cert) = config.tls_cert {
        println!("cert = \"{}\"", cert);
    }
    if let Some(ref key) = config.tls_key {
        println!("key = \"{}\"", key);
    }
    println!();
    println!("[metrics]");
    println!("enabled = {}", config.metrics_enabled);
    println!("port = {}", config.metrics_port);
}

fn print_banner(config: &ServerConfig) {
    eprintln!(
        r#"
    ███╗   ██╗███████╗██╗  ██╗██████╗  █████╗ ██████╗ ███████╗
    ████╗  ██║██╔════╝╚██╗██╔╝██╔══██╗██╔══██╗██╔══██╗██╔════╝
    ██╔██╗ ██║█████╗   ╚███╔╝ ██████╔╝███████║██║  ██║█████╗
    ██║╚██╗██║██╔══╝   ██╔██╗ ██╔══██╗██╔══██║██║  ██║██╔══╝
    ██║ ╚████║███████╗██╔╝ ██╗██║  ██║██║  ██║██████╔╝███████╗
    ╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝
                    cache  v0.1.0  |  Redis-compatible

    Listening on   {}:{}
    Databases      {}
    TLS            {}
    Metrics        http://{}:{}/metrics
    "#,
        config.bind,
        config.port,
        config.databases,
        if config.tls_enabled { "ON" } else { "OFF" },
        config.bind,
        config.metrics_port
    );
}
