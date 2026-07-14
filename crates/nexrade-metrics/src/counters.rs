//! Prometheus metric definitions.

use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, GaugeVec,
    HistogramVec, Registry,
};
use std::sync::Arc;

/// All Prometheus metrics for nexrade-cache.
#[derive(Clone)]
pub struct Metrics {
    pub registry: Arc<Registry>,

    /// Total commands processed, labeled by command name.
    pub commands_total: CounterVec,
    /// Commands that resulted in an error.
    pub command_errors_total: CounterVec,
    /// Command execution latency in seconds (histogram).
    pub command_duration_seconds: HistogramVec,

    /// Number of connected clients.
    pub connected_clients: GaugeVec,
    /// Total connections accepted.
    pub connections_total: CounterVec,

    /// Keyspace hits and misses.
    pub keyspace_hits_total: CounterVec,
    pub keyspace_misses_total: CounterVec,

    /// Number of keys per database.
    pub db_keys: GaugeVec,

    /// Memory usage estimate.
    pub memory_used_bytes: GaugeVec,

    /// Pub/sub metrics.
    pub pubsub_channels: GaugeVec,
    pub pubsub_messages_total: CounterVec,

    /// AOF/RDB persistence metrics.
    pub rdb_saves_total: CounterVec,
    pub aof_appends_total: CounterVec,
}

/// Pre-resolved per-command metric handles — see `Metrics::handles_for`.
/// Each field is an `Arc`-backed handle from the underlying `*Vec`, so
/// cloning `CommandMetricHandles` itself (e.g. to cache it on a
/// `Connection`) is cheap.
#[derive(Clone)]
pub struct CommandMetricHandles {
    total: prometheus::Counter,
    duration: prometheus::Histogram,
    errors: prometheus::Counter,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Arc::new(Registry::new());

        macro_rules! reg {
            ($e:expr) => {{
                let m = $e;
                registry.register(Box::new(m.clone())).unwrap();
                m
            }};
        }

        let commands_total = reg!(register_counter_vec!(
            "nexrade_commands_total",
            "Total number of commands processed",
            &["cmd"]
        )
        .unwrap());

        let command_errors_total = reg!(register_counter_vec!(
            "nexrade_command_errors_total",
            "Total number of command errors",
            &["cmd"]
        )
        .unwrap());

        let command_duration_seconds = reg!(register_histogram_vec!(
            "nexrade_command_duration_seconds",
            "Command execution latency",
            &["cmd"],
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        .unwrap());

        let connected_clients = reg!(register_gauge_vec!(
            "nexrade_connected_clients",
            "Number of active client connections",
            &[]
        )
        .unwrap());

        let connections_total = reg!(register_counter_vec!(
            "nexrade_connections_total",
            "Total number of connections accepted",
            &[]
        )
        .unwrap());

        let keyspace_hits_total = reg!(register_counter_vec!(
            "nexrade_keyspace_hits_total",
            "Number of successful lookups of keys in the main dictionary",
            &["db"]
        )
        .unwrap());

        let keyspace_misses_total = reg!(register_counter_vec!(
            "nexrade_keyspace_misses_total",
            "Number of failed lookups of keys in the main dictionary",
            &["db"]
        )
        .unwrap());

        let db_keys = reg!(register_gauge_vec!(
            "nexrade_db_keys",
            "Number of keys in each database",
            &["db"]
        )
        .unwrap());

        let memory_used_bytes = reg!(register_gauge_vec!(
            "nexrade_memory_used_bytes",
            "Estimated memory used by nexrade",
            &[]
        )
        .unwrap());

        let pubsub_channels = reg!(register_gauge_vec!(
            "nexrade_pubsub_channels",
            "Number of active pub/sub channels",
            &[]
        )
        .unwrap());

        let pubsub_messages_total = reg!(register_counter_vec!(
            "nexrade_pubsub_messages_total",
            "Total pub/sub messages published",
            &["channel"]
        )
        .unwrap());

        let rdb_saves_total =
            reg!(
                register_counter_vec!("nexrade_rdb_saves_total", "Total RDB snapshot saves", &[])
                    .unwrap()
            );

        let aof_appends_total = reg!(register_counter_vec!(
            "nexrade_aof_appends_total",
            "Total AOF append operations",
            &[]
        )
        .unwrap());

        Self {
            registry,
            commands_total,
            command_errors_total,
            command_duration_seconds,
            connected_clients,
            connections_total,
            keyspace_hits_total,
            keyspace_misses_total,
            db_keys,
            memory_used_bytes,
            pubsub_channels,
            pubsub_messages_total,
            rdb_saves_total,
            aof_appends_total,
        }
    }

    pub fn record_command(&self, cmd: &str, duration_secs: f64, error: bool) {
        self.commands_total.with_label_values(&[cmd]).inc();
        self.command_duration_seconds
            .with_label_values(&[cmd])
            .observe(duration_secs);
        if error {
            self.command_errors_total.with_label_values(&[cmd]).inc();
        }
    }

    /// Fetch (or create) the metric handles for `cmd`. Each handle is a
    /// cheap-to-clone `Arc`-backed metric — the intended use is to cache
    /// the returned struct (e.g. on a `Connection`, keyed by the last-seen
    /// command name) and call `record_with_handles` for consecutive
    /// same-named commands, skipping `MetricVec::with_label_values`'s
    /// per-call FNV hash + `RwLock::read()` + `HashMap` lookup + `Arc`
    /// clone (paid 2-3x per command otherwise: once each for
    /// `commands_total`, `command_duration_seconds`, and
    /// `command_errors_total`). This is exactly the pattern a pipelined
    /// batch hits, since redis-benchmark-style workloads send runs of the
    /// same command repeatedly.
    ///
    /// Fetching `command_errors_total`'s handle unconditionally (instead of
    /// only on error, as `record_command` does) eagerly creates that
    /// command's error-counter series at 0 rather than on first error —
    /// same eventual reported value, just created a bit earlier.
    pub fn handles_for(&self, cmd: &str) -> CommandMetricHandles {
        CommandMetricHandles {
            total: self.commands_total.with_label_values(&[cmd]),
            duration: self.command_duration_seconds.with_label_values(&[cmd]),
            errors: self.command_errors_total.with_label_values(&[cmd]),
        }
    }

    /// Record using pre-fetched handles from `handles_for` — no label
    /// lookup, just three direct metric updates.
    pub fn record_with_handles(handles: &CommandMetricHandles, duration_secs: f64, error: bool) {
        handles.total.inc();
        handles.duration.observe(duration_secs);
        if error {
            handles.errors.inc();
        }
    }

    pub fn record_connection(&self, connected: bool) {
        if connected {
            self.connections_total.with_label_values::<&str>(&[]).inc();
            self.connected_clients.with_label_values::<&str>(&[]).inc();
        } else {
            self.connected_clients.with_label_values::<&str>(&[]).dec();
        }
    }

    pub fn record_keyspace_hit(&self, db: &str) {
        self.keyspace_hits_total.with_label_values(&[db]).inc();
    }

    pub fn record_keyspace_miss(&self, db: &str) {
        self.keyspace_misses_total.with_label_values(&[db]).inc();
    }

    pub fn set_db_keys(&self, db: &str, count: f64) {
        self.db_keys.with_label_values(&[db]).set(count);
    }

    /// Render all metrics in Prometheus text format.
    pub fn render(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let mut buffer = Vec::new();
        let mf = self.registry.gather();
        encoder.encode(&mf, &mut buffer).unwrap_or_default();
        String::from_utf8(buffer).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
