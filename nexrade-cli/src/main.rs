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
use tracing::info;

use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::persistence::AofSync;
use nexrade_metrics::{init_tracing, Metrics, MetricsServer};
use nexrade_server::Listener;

#[derive(Parser, Debug)]
#[command(
    name = "nexrade-cache",
    version = env!("CARGO_PKG_VERSION"),
    author = "Nexrade Contributors",
    about = "High-performance Redis-compatible cache — with TLS, Lua, WASM, plugins, and built-in metrics"
)]
struct Cli {
    /// Config file path
    #[arg(short, long, value_name = "FILE", env = "NEXRADE_CONFIG")]
    config: Option<String>,

    /// Bind address
    #[arg(long, env = "NEXRADE_BIND")]
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

    /// Enable Prometheus metrics (overrides the config file's [metrics].enabled)
    #[arg(
        long,
        env = "NEXRADE_METRICS",
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    metrics: Option<bool>,

    /// Metrics port (overrides the config file's [metrics].port)
    #[arg(long, env = "NEXRADE_METRICS_PORT")]
    metrics_port: Option<u16>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", env = "RUST_LOG")]
    log_level: String,

    /// Output logs as JSON
    #[arg(long, env = "NEXRADE_LOG_JSON")]
    log_json: bool,

    /// Max connected clients
    #[arg(long, env = "NEXRADE_MAX_CLIENTS")]
    max_clients: Option<usize>,

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
    if let Some(max_clients) = cli.max_clients {
        config.max_clients = max_clients;
    }
    if let Some(metrics) = cli.metrics {
        config.metrics_enabled = metrics;
    }
    if let Some(metrics_port) = cli.metrics_port {
        config.metrics_port = metrics_port;
    }
    if let Some(maxmem) = cli.maxmemory {
        config.max_memory = if maxmem == 0 { None } else { Some(maxmem) };
    }
    if let Some(ref policy) = cli.maxmemory_policy {
        config.maxmemory_policy = policy.parse().unwrap_or_default();
    }
    if let Some(timeout) = cli.timeout {
        config.timeout = timeout;
    }

    // Relative RDB/AOF paths (the default `rdb_path` is just "nexrade.rdb")
    // resolve against the current working directory, which is whatever
    // directory the shell happened to be in when the process was launched
    // — e.g. `C:\Windows\System32` for an Admin-elevated prompt on Windows,
    // or wherever a service manager's default CWD is. Anchor them to the
    // executable's own directory instead, so the save file always lands
    // next to the binary regardless of how/where it was launched.
    if let Some(ref rdb_path) = config.persistence.rdb_path {
        config.persistence.rdb_path = Some(resolve_persistence_path(rdb_path));
    }
    if let Some(ref aof_path) = config.persistence.aof_path {
        config.persistence.aof_path = Some(resolve_persistence_path(aof_path));
    }

    Ok(config)
}

/// Resolve a possibly-relative persistence file path against the directory
/// containing the running executable. Absolute paths (including ones the
/// user explicitly passed via `--rdb-path`/`--aof-path` or a config file)
/// are returned unchanged — this only affects the relative default.
fn resolve_persistence_path(path: &str) -> String {
    let p = std::path::Path::new(path);
    if p.is_absolute() {
        return path.to_string();
    }
    match std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.to_path_buf()))
    {
        Some(dir) => dir.join(p).to_string_lossy().into_owned(),
        None => path.to_string(),
    }
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

    // Start the main TCP server. `Listener::run` also starts a second,
    // TLS-upgraded accept loop on `tls_port` when `config.tls_enabled` is
    // set (see `nexrade_server::listener`) — both listeners run
    // concurrently and share the same shutdown signal.
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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
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

    // Enable ANSI escape codes on Windows
    #[cfg(windows)]
    {
        let _ = nexrade_cache::windows_ansi::enable_ansi_support();
    }

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
    if let Some(v) = toml_val.get("tcp_backlog").and_then(|v| v.as_integer()) {
        config.tcp_backlog = v as u32;
    }
    // Slow log accepts either flat `slowlog_log_slower_than` / `slowlog_max_len`
    // keys or a `[slowlog]` table with `log_slower_than` / `max_len`. The table
    // form matches nexrade.example.toml; the flat form is kept for back-compat.
    if let Some(v) = toml_val
        .get("slowlog_log_slower_than")
        .and_then(|v| v.as_integer())
    {
        config.slowlog_log_slower_than = v;
    }
    if let Some(v) = toml_val.get("slowlog_max_len").and_then(|v| v.as_integer()) {
        config.slowlog_max_len = v as usize;
    }
    if let Some(slowlog) = toml_val.get("slowlog").and_then(|v| v.as_table()) {
        if let Some(v) = slowlog.get("log_slower_than").and_then(|v| v.as_integer()) {
            config.slowlog_log_slower_than = v;
        }
        if let Some(v) = slowlog.get("max_len").and_then(|v| v.as_integer()) {
            config.slowlog_max_len = v as usize;
        }
    }
    if let Some(v) = toml_val.get("loglevel").and_then(|v| v.as_str()) {
        config.loglevel = v.to_string();
    }
    // Top-level `save_rules = [[seconds, changes], ...]`. This drives both the
    // background-save loop (via persistence.rdb_save_rules) and `CONFIG GET
    // save` (via the top-level save_rules field), so keep the two in sync.
    if let Some(rules) = parse_save_rules(&toml_val) {
        config.save_rules = rules.clone();
        config.persistence.rdb_save_rules = rules;
    }
    if let Some(lua) = toml_val.get("lua").and_then(|v| v.as_table()) {
        if let Some(v) = lua.get("time_limit_ms").and_then(|v| v.as_integer()) {
            config.lua_time_limit = v as u64;
        }
    }
    if let Some(persistence) = toml_val.get("persistence").and_then(|v| v.as_table()) {
        // Empty string disables the file (matches the CLI flag semantics).
        if let Some(rdb) = persistence.get("rdb_path").and_then(|v| v.as_str()) {
            config.persistence.rdb_path = if rdb.is_empty() {
                None
            } else {
                Some(rdb.to_string())
            };
        }
        if let Some(aof) = persistence.get("aof_path").and_then(|v| v.as_str()) {
            config.persistence.aof_path = if aof.is_empty() {
                None
            } else {
                Some(aof.to_string())
            };
        }
        if let Some(sync) = persistence.get("aof_sync").and_then(|v| v.as_str()) {
            config.persistence.aof_sync = match sync.to_ascii_lowercase().as_str() {
                "always" => AofSync::Always,
                "no" => AofSync::No,
                _ => AofSync::EverySec,
            };
        }
        // A `save_rules` (or `rdb_save_rules`, the name the README uses)
        // nested under `[persistence]` also works.
        let nested_rules = parse_save_rules_value(persistence.get("save_rules"))
            .or_else(|| parse_save_rules_value(persistence.get("rdb_save_rules")));
        if let Some(rules) = nested_rules {
            config.save_rules = rules.clone();
            config.persistence.rdb_save_rules = rules;
        }
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

/// Parse a top-level `save_rules = [[seconds, min_changes], ...]` value.
fn parse_save_rules(toml_val: &toml::Value) -> Option<Vec<(u64, usize)>> {
    parse_save_rules_value(toml_val.get("save_rules"))
}

/// Parse a `save_rules` array-of-pairs from an optional TOML value. Each inner
/// entry must be `[seconds, min_changes]`; malformed entries are skipped.
fn parse_save_rules_value(val: Option<&toml::Value>) -> Option<Vec<(u64, usize)>> {
    let arr = val?.as_array()?;
    let mut rules = Vec::with_capacity(arr.len());
    for entry in arr {
        if let Some(pair) = entry.as_array() {
            if let (Some(secs), Some(changes)) = (
                pair.first().and_then(|v| v.as_integer()),
                pair.get(1).and_then(|v| v.as_integer()),
            ) {
                rules.push((secs as u64, changes as usize));
            }
        }
    }
    Some(rules)
}

fn print_config(config: &ServerConfig) {
    println!("# nexrade-cache configuration");
    println!("bind = \"{}\"", config.bind);
    println!("port = {}", config.port);
    println!("databases = {}", config.databases);
    println!("max_clients = {}", config.max_clients);
    println!("tcp_backlog = {}", config.tcp_backlog);
    println!("loglevel = \"{}\"", config.loglevel);
    println!("hz = {}", config.hz);
    println!("timeout = {}", config.timeout);
    let save_str = config
        .save_rules
        .iter()
        .map(|(s, c)| format!("[{}, {}]", s, c))
        .collect::<Vec<_>>()
        .join(", ");
    println!("save_rules = [{}]", save_str);
    println!();
    println!("[persistence]");
    println!(
        "rdb_path = {}",
        config
            .persistence
            .rdb_path
            .as_deref()
            .map(|p| format!("\"{}\"", p))
            .unwrap_or_else(|| "\"\"".to_string())
    );
    println!(
        "aof_path = {}",
        config
            .persistence
            .aof_path
            .as_deref()
            .map(|p| format!("\"{}\"", p))
            .unwrap_or_else(|| "\"\"".to_string())
    );
    println!(
        "aof_sync = \"{}\"",
        match config.persistence.aof_sync {
            AofSync::Always => "always",
            AofSync::EverySec => "everysec",
            AofSync::No => "no",
        }
    );
    println!();
    println!("[slowlog]");
    println!("log_slower_than = {}", config.slowlog_log_slower_than);
    println!("max_len = {}", config.slowlog_max_len);
    println!();
    println!("[lua]");
    println!("time_limit_ms = {}", config.lua_time_limit);
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
                    cache  v{}  |  Redis-compatible

    Listening on   {}:{}
    Databases      {}
    TLS            {}
    Metrics        http://{}:{}/metrics
    "#,
        // CARGO_PKG_VERSION is the workspace version (from
        // `[workspace.package].version` propagated via
        // `version.workspace = true` in our Cargo.toml), so the banner
        // stays in sync with `nexrade-cli --version` and the actual
        // release artifact.
        env!("CARGO_PKG_VERSION"),
        config.bind,
        config.port,
        config.databases,
        if config.tls_enabled { "ON" } else { "OFF" },
        config.bind,
        config.metrics_port
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write `contents` to a uniquely-named temp file and return its path.
    /// `tag` keeps parallel tests from colliding on the same filename.
    fn temp_config(tag: &str, contents: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "nexrade_cfg_test_{}_{}.toml",
            tag,
            std::process::id()
        ));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn persistence_table_is_parsed() {
        let path = temp_config(
            "persist",
            r#"
[persistence]
rdb_path = "/data/dump.rdb"
aof_path = "/data/appendonly.aof"
aof_sync = "always"
"#,
        );
        let cfg = load_config_file(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.persistence.rdb_path.as_deref(), Some("/data/dump.rdb"));
        assert_eq!(
            cfg.persistence.aof_path.as_deref(),
            Some("/data/appendonly.aof")
        );
        assert_eq!(cfg.persistence.aof_sync, AofSync::Always);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn empty_rdb_path_disables_persistence() {
        let path = temp_config(
            "empty_rdb",
            r#"
[persistence]
rdb_path = ""
"#,
        );
        let cfg = load_config_file(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.persistence.rdb_path, None);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn save_rules_sync_both_fields() {
        let path = temp_config(
            "save_rules",
            r#"
save_rules = [[100, 5], [50, 20]]
"#,
        );
        let cfg = load_config_file(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.save_rules, vec![(100, 5), (50, 20)]);
        // Must also drive the background-save loop, which reads
        // persistence.rdb_save_rules — not the top-level field.
        assert_eq!(cfg.persistence.rdb_save_rules, vec![(100, 5), (50, 20)]);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn slowlog_table_form_is_parsed() {
        let path = temp_config(
            "slowlog",
            r#"
[slowlog]
log_slower_than = 999
max_len = 64
"#,
        );
        let cfg = load_config_file(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.slowlog_log_slower_than, 999);
        assert_eq!(cfg.slowlog_max_len, 64);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn lua_and_tcp_backlog_are_parsed() {
        let path = temp_config(
            "lua",
            r#"
tcp_backlog = 2048

[lua]
time_limit_ms = 8000
"#,
        );
        let cfg = load_config_file(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.lua_time_limit, 8000);
        assert_eq!(cfg.tcp_backlog, 2048);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn cli_flag_overrides_file_but_absent_flag_keeps_file() {
        // File sets a non-default bind + max_clients; with no CLI flags those
        // file values must survive (regression test for CLI defaults that used
        // to clobber the file).
        let path = temp_config(
            "override",
            r#"
bind = "0.0.0.0"
max_clients = 500

[metrics]
enabled = false
port = 9099
"#,
        );
        let cli = Cli {
            config: Some(path.to_str().unwrap().to_string()),
            bind: None,
            port: None,
            databases: None,
            requirepass: None,
            tls: false,
            tls_cert: None,
            tls_key: None,
            tls_port: None,
            metrics: None,
            metrics_port: None,
            log_level: "info".to_string(),
            log_json: false,
            max_clients: None,
            maxmemory: None,
            maxmemory_policy: None,
            timeout: None,
            rdb_path: None,
            aof_path: None,
            print_config: false,
        };
        let cfg = config_from_cli(&cli).unwrap();
        assert_eq!(cfg.bind, "0.0.0.0");
        assert_eq!(cfg.max_clients, 500);
        assert!(!cfg.metrics_enabled);
        assert_eq!(cfg.metrics_port, 9099);
        std::fs::remove_file(&path).ok();
    }
}
