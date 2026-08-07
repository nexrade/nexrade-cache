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

// Match Redis's allocator on non-Windows hosts. Gives measurable wins on
// LRANGE (large buffer allocs) and pipelined write batches.
#[cfg(all(not(windows), not(target_arch = "wasm32")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(windows)]
mod windows_svc;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::persistence::AofSync;
use nexrade_metrics::{init_tracing, HealthServer, Metrics, MetricsServer};
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

    /// Metrics server bind address (overrides the config file's [metrics].bind).
    #[arg(long, env = "NEXRADE_METRICS_BIND")]
    metrics_bind: Option<String>,

    /// Metrics server port (overrides the config file's [metrics].port)
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

    /// Run preflight checks (config, persistence validation) and exit.
    /// Performs the same non-mutating validation as startup but does not
    /// bind listeners, open files, or start tasks.
    #[arg(long)]
    preflight: bool,

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

    /// Enable the operations health and readiness HTTP surface
    /// (`/healthz` and `/readyz`).
    #[arg(long, env = "NEXRADE_HEALTH")]
    health: bool,

    /// Health server bind address (overrides the config file's [health].bind).
    #[arg(long, env = "NEXRADE_HEALTH_BIND")]
    health_bind: Option<String>,

    /// Health server port (overrides the config file's [health].port).
    #[arg(long, env = "NEXRADE_HEALTH_PORT")]
    health_port: Option<u16>,

    /// Maximum snapshot age in seconds before the instance becomes not-ready
    /// (overrides the config file's [health].max_snapshot_age_secs).
    /// `0` disables this check.
    #[arg(long, env = "NEXRADE_HEALTH_MAX_SNAPSHOT_AGE")]
    health_max_snapshot_age: Option<u64>,

    /// Maximum replica ACK age in seconds before the instance becomes not-ready
    /// (overrides the config file's [health].max_replication_lag_secs).
    /// `0` disables this check.
    #[arg(long, env = "NEXRADE_HEALTH_MAX_REPLICATION_LAG")]
    health_max_replication_lag: Option<u64>,
}

/// Build a [`ServerConfig`] from the parsed CLI arguments.
fn config_from_cli(cli: &Cli) -> Result<ServerConfig> {
    let mut config = if let Some(ref config_path) = cli.config {
        let mut c = load_config_file(config_path)?;
        c.config_file_path = Some(config_path.clone());
        c
    } else {
        ServerConfig::default()
    };

    if let Some(ref bind) = cli.bind {
        config.bind = bind.clone();
    }
    // After all `bind` overrides (file, CLI) have landed, default
    // `metrics_bind` to the Redis listener's address when nothing
    // explicitly set it. Prevents the metrics endpoint from
    // accidentally drifting onto the wrong interface, while still
    // allowing `--metrics-bind` / `[metrics].bind` to override it.
    if config.metrics_bind.is_empty() {
        config.metrics_bind = config.bind.clone();
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
    if let Some(ref metrics_bind) = cli.metrics_bind {
        config.metrics_bind = metrics_bind.clone();
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
    if cli.health {
        config.health.enabled = true;
    }
    if let Some(ref bind) = cli.health_bind {
        config.health.bind = bind.clone();
    }
    if let Some(port) = cli.health_port {
        config.health.port = port;
    }
    if let Some(age) = cli.health_max_snapshot_age {
        config.health.max_snapshot_age_secs = if age == 0 { None } else { Some(age) };
    }
    if let Some(lag) = cli.health_max_replication_lag {
        config.health.max_replication_lag_secs = if lag == 0 { None } else { Some(lag) };
    }

    // Relative RDB/AOF paths (the default `rdb_path` is just "nexrade.rdb")
    // resolve against the current working directory, which is whatever
    // directory the shell happened to be in when the process was launched
    // — e.g. `C:\Windows\System32` for an Admin-elevated prompt on Windows,
    // or wherever a service manager's default CWD is. Anchor them to the
    // executable's own directory instead, so the save file always lands
    // next to the binary regardless of how/where it was launched.
    //
    // An empty string (`--rdb-path ""`) is the documented "disable
    // persistence" sentinel — skip the resolution so it doesn't get
    // joined to the exe directory and end up as a real file path.
    // A listener-side filter also strips empty paths, but doing it
    // here keeps the recovery-source check from rejecting startup
    // because two empty paths both resolved to the same file.
    if let Some(ref rdb_path) = config.persistence.rdb_path {
        if rdb_path.is_empty() {
            config.persistence.rdb_path = None;
        } else {
            config.persistence.rdb_path = Some(resolve_persistence_path(rdb_path));
        }
    }
    if let Some(ref aof_path) = config.persistence.aof_path {
        if aof_path.is_empty() {
            config.persistence.aof_path = None;
        } else {
            config.persistence.aof_path = Some(resolve_persistence_path(aof_path));
        }
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
    start_server_with(config, |_| {}).await
}

/// `start_server`, but invokes `on_db` with a clone of the [`Db`] as soon as it
/// exists — before the listener starts serving.
///
/// The Windows service needs this: its SCM stop handler runs on a different
/// thread than the tokio runtime and must be able to call
/// `db.shutdown.notify_one()` to trigger the same graceful path as the
/// `SHUTDOWN` command (drain tasks, save RDB, fsync AOF). Without a handle it
/// could only kill the process, losing anything not yet persisted.
pub(crate) async fn start_server_with<F>(config: ServerConfig, on_db: F) -> Result<()>
where
    F: FnOnce(Db),
{
    // Print banner
    print_banner(&config);

    // Initialize the database
    let db = Db::new(config.clone());
    on_db(db.clone());

    // Build the listener up front — before the operations servers — so they can
    // subscribe to its shutdown watch. `Listener::new` only clones config and
    // creates the channel; it binds no sockets, so nothing observable happens
    // earlier than it used to. `run()` (which consumes it) is still called
    // last, after both operations listeners are bound.
    //
    // Cloning `db` here keeps the lifecycle state addressable after `run`
    // consumes the listener: a startup failure (corrupt RDB, invalid TLS,
    // occupied port, …) must flip the lifecycle to `Failed` so `/healthz`
    // returns 503 — otherwise a process that bound health but failed to bind
    // the data plane would falsely advertise `live=true` to probes.
    let db_for_failure = db.clone();

    // Start metrics server (Prometheus /metrics). Both metrics and
    // health servers are spawned as background tasks: their `start`
    // returns once the listener is bound so the main thread can move
    // on to bind the redis port and start recovery.
    //
    // `metrics_addr` is resolved here but the server is started *after* the
    // `Listener` exists, so it can be handed a shutdown receiver too.
    let metrics = if config.metrics_enabled {
        Some(Metrics::new())
    } else {
        None
    };

    let listener = Listener::new(db, metrics.clone());

    let metrics_handle = if let Some(ref m) = metrics {
        // Use the dedicated `metrics_bind` field rather than `bind` so
        // the data-plane and observability listeners can be scoped to
        // different interfaces (e.g. Redis on 0.0.0.0 with requirepass,
        // metrics on a sidecar-only address). The previous code wrote
        // the metrics bind into `bind` and silently rebounded Redis.
        let metrics_addr = format!("{}:{}", config.metrics_bind, config.metrics_port)
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:9091".parse().unwrap());
        let handle = MetricsServer::start(
            metrics_addr,
            m.clone(),
            Some(listener.shutdown_subscriber()),
        )
        .await;
        info!(
            "metrics available at http://{}:{}/metrics",
            config.metrics_bind, config.metrics_port
        );
        // Surface bind failure as a startup error: an operator who
        // configured `[metrics] enabled = true` expects `/metrics` to
        // be reachable. If the listener failed to bind (port in use,
        // permission denied) we must not silently continue.
        if handle.is_none() {
            anyhow::bail!(
                "failed to bind metrics server on {}:{} — port in use, \
                 permissions, or interface missing?",
                config.metrics_bind,
                config.metrics_port
            );
        }
        handle
    } else {
        None
    };

    // Start health server (operations /healthz, /readyz) — bound to the
    // configured `health.bind` so loopback production profiles are not
    // reachable on the public interface.
    //
    // The shutdown receiver is what makes `/readyz` stop answering when the
    // data plane drains. Passing `None` here (as every release through 1.2.3
    // did) left the accept loop running past the drain, so a load balancer
    // could still be told `ready=true` by a server that had stopped serving.
    let health_handle = if config.health.enabled {
        let health_addr = format!("{}:{}", config.health.bind, config.health.port)
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:9090".parse().unwrap());
        let handle = HealthServer::start(
            health_addr,
            db_for_failure.clone(),
            Some(listener.shutdown_subscriber()),
        )
        .await;
        info!(
            "health endpoints available at http://{}:{}/healthz and /readyz",
            config.health.bind, config.health.port
        );
        // Mirror the metrics bind-failure surfacing: an operator who
        // explicitly enabled `[health]` expects the endpoints to work.
        if handle.is_none() {
            anyhow::bail!(
                "failed to bind health server on {}:{} — port in use, \
                 permissions, or interface missing?",
                config.health.bind,
                config.health.port
            );
        }
        handle
    } else {
        None
    };

    // Start the main TCP server. `Listener::run` also starts a second,
    // TLS-upgraded accept loop on `tls_port` when `config.tls_enabled` is
    // set (see `nexrade_server::listener`) — both listeners run
    // concurrently and share the same shutdown signal.
    if let Err(e) = listener.run().await {
        db_for_failure
            .lifecycle()
            .set_phase(nexrade_core::health::HealthPhase::Failed);
        return Err(e.context("startup failed"));
    }

    // `run()` has returned, so the shutdown watch has fired and the operations
    // servers are exiting. Join them (bounded) rather than dropping the handles:
    // an abrupt process exit mid-response looks to a probe like a connection
    // reset instead of a clean close.
    join_ops_server("health", health_handle).await;
    join_ops_server("metrics", metrics_handle).await;

    Ok(())
}

/// Grace period for an operations HTTP server to finish in-flight work after
/// the shutdown signal fires. Short by design: these serve small, local,
/// bounded bodies.
const OPS_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// Await an operations server's exit, bounded by [`OPS_SHUTDOWN_GRACE`].
///
/// A server that overruns is left for the OS to reclaim at process exit rather
/// than blocking shutdown — the same trade-off `BgTasks` makes for data-plane
/// background tasks.
async fn join_ops_server(name: &str, handle: Option<tokio::task::JoinHandle<()>>) {
    let Some(handle) = handle else { return };
    match tokio::time::timeout(OPS_SHUTDOWN_GRACE, handle).await {
        Ok(Ok(())) => info!("{} server stopped", name),
        Ok(Err(e)) if e.is_panic() => {
            tracing::error!("{} server panicked during shutdown: {:?}", name, e)
        }
        Ok(Err(_)) => {}
        Err(_) => tracing::warn!(
            "{} server did not stop within {:?} — continuing shutdown",
            name,
            OPS_SHUTDOWN_GRACE
        ),
    }
}

// Cap workers: redis-benchmark single-key write pipelines (LPUSH/SET) regress
// hard when every core contends on one shard. 4 matches prior measured tables
// and beats Redis on pipelined LPUSH after Compact front-headroom.
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup logging (env vars only here; init_tracing() happens below on
    // every path, including the Windows-service one, so log output is
    // consistent regardless of how the process was launched).
    if cli.log_json {
        std::env::set_var("NEXRADE_LOG_JSON", "1");
    }
    if !cli.log_level.is_empty() {
        std::env::set_var("RUST_LOG", &cli.log_level);
    }

    // ── Windows service control ──────────────────────────────────────────────
    #[cfg(windows)]
    {
        if cli.install_service {
            // Bake the --config path (if any) into the service's launch
            // command so it's re-applied on every SCM-triggered start,
            // including after a machine reboot — see `install_service`.
            return windows_svc::install_service(cli.config.as_deref());
        }
        if cli.uninstall_service {
            return windows_svc::uninstall_service();
        }
        if cli.service {
            // Running under the SCM: build the config exactly like a
            // normal run does (respecting whatever --config path the SCM
            // launch command carries), then hand control to the SCM
            // dispatcher — this call blocks until the service is stopped.
            init_tracing();
            let config = config_from_cli(&cli)?;
            return windows_svc::run_as_service(config);
        }
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

    if cli.preflight {
        return run_preflight(&config);
    }

    start_server(config).await
}

/// Non-mutating startup checks — same validations the server runs before
/// bind, but exits before binding listeners, opening files, or starting
/// tasks. Exit code 0 means "would start cleanly", non-zero means a
/// startup-relevant problem was found. Safe to run on any host as part
/// of a deploy pipeline.
fn run_preflight(config: &ServerConfig) -> Result<()> {
    use std::path::Path;

    let mut errors: Vec<String> = Vec::new();

    // Bind address sanity: must parse to a SocketAddr (the CLI layer
    // already validated the format; this is the post-merge check).
    let bind_addr = format!("{}:{}", config.bind, config.port);
    if bind_addr.parse::<std::net::SocketAddr>().is_err() {
        errors.push(format!("bind address not parseable: {bind_addr}"));
    }

    // Recovery-source rule: RDB and AOF both configured is rejected at
    // startup because the AOF replay would duplicate non-idempotent
    // writes. Surface this here too so a deploy can fail before the
    // server even reaches startup.
    let rdb = config
        .persistence
        .rdb_path
        .as_deref()
        .filter(|p| !p.is_empty());
    let aof = config
        .persistence
        .aof_path
        .as_deref()
        .filter(|p| !p.is_empty());
    if rdb.is_some() && aof.is_some() {
        errors.push(
            "RDB and AOF sources both configured; pick exactly one \
             authoritative recovery mode (the RDB+AOF combination is \
             rejected at startup to avoid replaying non-idempotent writes)"
                .to_string(),
        );
    }

    // Persistence path parents: must exist and be writable when a path
    // is configured. We don't create them — preflight is non-mutating.
    for (label, path) in [("rdb_path", rdb), ("aof_path", aof)] {
        if let Some(p) = path {
            let parent = Path::new(p).parent().unwrap_or_else(|| Path::new("."));
            if parent.as_os_str().is_empty() {
                errors.push(format!("{label} '{p}' has no parent directory"));
            } else if !parent.exists() {
                errors.push(format!(
                    "{label} '{p}' parent directory '{}' does not exist",
                    parent.display()
                ));
            } else if std::fs::metadata(parent)
                .map(|m| !m.is_dir())
                .unwrap_or(true)
            {
                errors.push(format!(
                    "{label} '{p}' parent '{}' is not a directory",
                    parent.display()
                ));
            }
        }
    }

    // RDB sanity: if the configured path already exists, verify the
    // header + CRC32C (without decoding). Snapshot::verify is the same
    // path the standalone recovery code uses; reusing it here means
    // there is no second parser to drift.
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(rdb_path) = rdb {
        if Path::new(rdb_path).exists() {
            if let Err(e) = nexrade_core::persistence::Snapshot::verify(rdb_path) {
                errors.push(format!("RDB '{rdb_path}' failed integrity check: {e}"));
            }
        }
    }

    // AOF sanity: if the configured path already exists, stream-parse
    // through clean EOF to catch truncation early. We deliberately
    // don't apply the commands — preflight is non-mutating.
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(aof_path) = aof {
        if Path::new(aof_path).exists() {
            match nexrade_core::persistence::AofReader::open(aof_path) {
                Ok(mut reader) => {
                    if let Err(e) = reader.scan_to_eof() {
                        errors.push(format!("AOF '{aof_path}' parse failure: {e}"));
                    }
                }
                Err(e) => errors.push(format!("AOF '{aof_path}' open failure: {e}")),
            }
        }
    }

    // TLS sanity: startup refuses to run with TLS enabled but unusable
    // (1.3.0), so preflight must catch the same cases — otherwise a deploy
    // check passes and the very next step exits 1. Delegated to
    // nexrade_server so there is one validator, not two.
    for e in nexrade_server::validate_tls_config(config) {
        errors.push(e);
    }

    // A cert/key configured while TLS is off is not a startup failure, but
    // it is almost always a mistake: the operator sees the paths in the
    // config and believes the port is encrypted. Warn without failing.
    if !config.tls_enabled && (config.tls_cert.is_some() || config.tls_key.is_some()) {
        eprintln!(
            "preflight: warning: tls.cert/tls.key are set but tls.enabled = false; \
             this instance serves plaintext only"
        );
    }

    if errors.is_empty() {
        eprintln!("preflight: OK");
        Ok(())
    } else {
        for e in &errors {
            eprintln!("preflight: {e}");
        }
        Err(anyhow::anyhow!(
            "preflight failed ({} error(s))",
            errors.len()
        ))
    }
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
    // Accept both `maxmemory` (Redis spelling) and `max_memory` (example
    // config / snake_case). Prefer the Redis spelling if both are present.
    let maxmem = toml_val
        .get("maxmemory")
        .or_else(|| toml_val.get("max_memory"))
        .and_then(|v| v.as_integer());
    if let Some(maxmem) = maxmem {
        config.max_memory = if maxmem <= 0 {
            None
        } else {
            Some(maxmem as usize)
        };
    }
    // Same for the policy key: `maxmemory_policy` or `max_memory_policy`.
    if let Some(policy) = toml_val
        .get("maxmemory_policy")
        .or_else(|| toml_val.get("max_memory_policy"))
        .and_then(|v| v.as_str())
    {
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
        if let Some(bind) = metrics.get("bind").and_then(|v| v.as_str()) {
            config.metrics_bind = bind.to_string();
        }
        if let Some(port) = metrics.get("port").and_then(|v| v.as_integer()) {
            config.metrics_port = port as u16;
        }
    }
    if let Some(health) = toml_val.get("health").and_then(|v| v.as_table()) {
        config.health.enabled = health
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(bind) = health.get("bind").and_then(|v| v.as_str()) {
            config.health.bind = bind.to_string();
        }
        if let Some(port) = health.get("port").and_then(|v| v.as_integer()) {
            config.health.port = port as u16;
        }
        if let Some(age) = health
            .get("max_snapshot_age_secs")
            .and_then(|v| v.as_integer())
        {
            config.health.max_snapshot_age_secs = if age < 0 { None } else { Some(age as u64) };
        }
        if let Some(lag) = health
            .get("max_replication_lag_secs")
            .and_then(|v| v.as_integer())
        {
            config.health.max_replication_lag_secs = if lag < 0 { None } else { Some(lag as u64) };
        }
        // S3: opt back into the pre-1.3.0 full-report probe body, which
        // includes persistence paths and the AOF failure message.
        if let Some(expose) = health.get("expose_details").and_then(|v| v.as_bool()) {
            config.health.expose_details = expose;
        }
    }

    // List dual-encoding thresholds (same keys as CONFIG REWRITE / CONFIG SET).
    // Zero is rejected (same as CONFIG SET).
    if let Some(v) = toml_val
        .get("list_max_listpack_entries")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.list_max_listpack_entries = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("list_max_listpack_size")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.list_max_listpack_size = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("list_max_listpack_value")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.list_max_listpack_value = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("list_demote_entries")
        .and_then(|v| v.as_integer())
    {
        // demote_entries may be 0 (disable demote)
        if v >= 0 {
            config.list_demote_entries = v as usize;
        }
    }

    // Hash / set / zset dual-encoding thresholds (CONFIG REWRITE / CONFIG SET).
    if let Some(v) = toml_val
        .get("hash_max_listpack_entries")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.hash_max_listpack_entries = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("hash_max_listpack_size")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.hash_max_listpack_size = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("hash_max_listpack_value")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.hash_max_listpack_value = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("hash_demote_entries")
        .and_then(|v| v.as_integer())
    {
        if v >= 0 {
            config.hash_demote_entries = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("set_max_listpack_entries")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.set_max_listpack_entries = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("set_max_listpack_size")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.set_max_listpack_size = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("set_max_listpack_value")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.set_max_listpack_value = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("set_demote_entries")
        .and_then(|v| v.as_integer())
    {
        if v >= 0 {
            config.set_demote_entries = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("zset_max_listpack_entries")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.zset_max_listpack_entries = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("zset_max_listpack_size")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.zset_max_listpack_size = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("zset_max_listpack_value")
        .and_then(|v| v.as_integer())
    {
        if v > 0 {
            config.zset_max_listpack_value = v as usize;
        }
    }
    if let Some(v) = toml_val
        .get("zset_demote_entries")
        .and_then(|v| v.as_integer())
    {
        if v >= 0 {
            config.zset_demote_entries = v as usize;
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
    // Print the *resolved* TLS port, including the 6380 default when the
    // config omits it. Previously this line was missing entirely, so
    // `--print-config` output was not a round-trippable config and hid the
    // field involved in a `tls.port == port` collision.
    println!("port = {}", config.tls_port.unwrap_or(6380));
    if let Some(ref cert) = config.tls_cert {
        println!("cert = \"{}\"", cert);
    }
    if let Some(ref key) = config.tls_key {
        println!("key = \"{}\"", key);
    }
    println!();
    println!("[metrics]");
    println!("enabled = {}", config.metrics_enabled);
    println!("bind = \"{}\"", config.metrics_bind);
    println!("port = {}", config.metrics_port);
    println!();
    println!("[health]");
    println!("enabled = {}", config.health.enabled);
    println!("bind = \"{}\"", config.health.bind);
    println!("port = {}", config.health.port);
    println!(
        "max_snapshot_age_secs = {}",
        config
            .health
            .max_snapshot_age_secs
            .map(|v| v.to_string())
            .unwrap_or_else(|| "disabled".to_string())
    );
    println!(
        "max_replication_lag_secs = {}",
        config
            .health
            .max_replication_lag_secs
            .map(|v| v.to_string())
            .unwrap_or_else(|| "disabled".to_string())
    );
    println!("expose_details = {}", config.health.expose_details);
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
    Health         {}
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
        config.metrics_bind,
        config.metrics_port,
        if config.health.enabled {
            format!(
                "http://{}:{}/healthz and /readyz",
                config.health.bind, config.health.port
            )
        } else {
            "OFF".to_string()
        }
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
    fn list_thresholds_are_parsed() {
        let path = temp_config(
            "list_thr",
            r#"
list_max_listpack_entries = 64
list_max_listpack_size = 4096
list_max_listpack_value = 128
list_demote_entries = 32
"#,
        );
        let cfg = load_config_file(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.list_max_listpack_entries, 64);
        assert_eq!(cfg.list_max_listpack_value, 128);
        assert_eq!(cfg.list_demote_entries, 32);
        std::fs::remove_file(&path).ok();
    }

    // Audit fix F1 (1.2.1): `--metrics-bind` and `[metrics].bind` must
    // NOT clobber the Redis listener's `bind`. They populate the
    // dedicated `metrics_bind` field, leaving `bind` alone.
    #[test]
    fn metrics_bind_does_not_overwrite_redis_bind() {
        // File sets Redis bind = 0.0.0.0 and metrics_bind = 127.0.0.1;
        // both must survive `load_config_file`.
        let path = temp_config(
            "metrics_bind_isolated",
            r#"
bind = "0.0.0.0"
port = 16389

[metrics]
enabled = true
bind = "127.0.0.1"
port = 9091
"#,
        );
        let cfg = load_config_file(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.bind, "0.0.0.0", "Redis bind must stay 0.0.0.0");
        assert_eq!(
            cfg.metrics_bind, "127.0.0.1",
            "metrics_bind must be 127.0.0.1, not the Redis bind"
        );
        assert_eq!(cfg.metrics_port, 9091);
        assert!(cfg.metrics_enabled);
        std::fs::remove_file(&path).ok();
    }

    // Audit fix F1: with no `[metrics].bind` set, `metrics_bind`
    // defaults to `bind` after all overrides land. This preserves
    // 1.1.x behavior where metrics + Redis shared the same bind.
    #[test]
    fn metrics_bind_defaults_to_redis_bind_when_unset() {
        // No [metrics].bind in the file — after config_from_cli,
        // metrics_bind should mirror the Redis bind.
        let path = temp_config(
            "metrics_bind_default",
            r#"
bind = "10.0.0.5"
port = 6379

[metrics]
enabled = true
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
            metrics_bind: None,
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
            preflight: false,
            health: false,
            health_bind: None,
            health_port: None,
            health_max_snapshot_age: None,
            health_max_replication_lag: None,
            // Declared #[cfg(windows)] on `Cli`, so they must be gated here
            // too or this literal fails to compile on a Windows host (E0063).
            // The ubuntu-only CI test job cannot catch that.
            #[cfg(windows)]
            install_service: false,
            #[cfg(windows)]
            uninstall_service: false,
            #[cfg(windows)]
            service: false,
        };
        let cfg = config_from_cli(&cli).unwrap();
        assert_eq!(cfg.bind, "10.0.0.5");
        assert_eq!(
            cfg.metrics_bind, "10.0.0.5",
            "metrics_bind must default to Redis bind when not set"
        );
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
            metrics_bind: None,
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
            preflight: false,
            health: false,
            health_bind: None,
            health_port: None,
            health_max_snapshot_age: None,
            health_max_replication_lag: None,
            // Declared #[cfg(windows)] on `Cli`, so they must be gated here
            // too or this literal fails to compile on a Windows host (E0063).
            // The ubuntu-only CI test job cannot catch that.
            #[cfg(windows)]
            install_service: false,
            #[cfg(windows)]
            uninstall_service: false,
            #[cfg(windows)]
            service: false,
        };
        let cfg = config_from_cli(&cli).unwrap();
        assert_eq!(cfg.bind, "0.0.0.0");
        assert_eq!(cfg.max_clients, 500);
        assert!(!cfg.metrics_enabled);
        assert_eq!(cfg.metrics_port, 9099);
        std::fs::remove_file(&path).ok();
    }

    // `--rdb-path ""` and `--aof-path ""` are the documented "disable
    // persistence" sentinels. Resolution must treat them as None, not as
    // a relative path to be joined to the executable directory — otherwise
    // both empty paths resolve to the same file and the recovery-source
    // rule rejects startup with "both RDB and AOF exist".
    #[test]
    fn empty_persistence_paths_disable_persistence() {
        let path = temp_config(
            "empty_persistence",
            r##"
bind = "127.0.0.1"
port = 16499

[persistence]
rdb_path = "nexrade.rdb"
aof_path = ""

[metrics]
enabled = false
"##,
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
            metrics_bind: None,
            metrics_port: None,
            log_level: "info".to_string(),
            log_json: false,
            max_clients: None,
            maxmemory: None,
            maxmemory_policy: None,
            timeout: None,
            rdb_path: Some("".to_string()),
            aof_path: Some("".to_string()),
            print_config: false,
            preflight: false,
            health: false,
            health_bind: None,
            health_port: None,
            health_max_snapshot_age: None,
            health_max_replication_lag: None,
            // Declared #[cfg(windows)] on `Cli`, so they must be gated here
            // too or this literal fails to compile on a Windows host (E0063).
            // The ubuntu-only CI test job cannot catch that.
            #[cfg(windows)]
            install_service: false,
            #[cfg(windows)]
            uninstall_service: false,
            #[cfg(windows)]
            service: false,
        };
        let cfg = config_from_cli(&cli).unwrap();
        assert!(
            cfg.persistence.rdb_path.is_none(),
            "--rdb-path \"\" must disable RDB, got {:?}",
            cfg.persistence.rdb_path
        );
        assert!(
            cfg.persistence.aof_path.is_none(),
            "--aof-path \"\" must disable AOF, got {:?}",
            cfg.persistence.aof_path
        );
        std::fs::remove_file(&path).ok();
    }
}
