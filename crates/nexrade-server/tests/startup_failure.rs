//! 1.3.0 (C5) — server-level startup-failure integration tests.
//!
//! Until now these modes were covered **only** by `scripts/operator_drill.py`,
//! which spawns the release binary as a subprocess and therefore cannot run
//! under `cargo test`. That left the `Listener::run` error paths — the ones
//! that decide whether a damaged instance refuses to start or comes up
//! advertising itself as healthy — outside CI's reach.
//!
//! Each test drives the real `Listener::run` against a real filesystem and a
//! real TCP bind, and asserts two things:
//!
//!   1. `run()` returns `Err` (startup is refused), and
//!   2. the lifecycle does **not** report `Ready`.
//!
//! (2) matters as much as (1): a process that fails to start but still
//! advertises `ready=true` to a load balancer is the failure mode 1.2.1's F4
//! and 1.3.0's F3 were about.

use std::io::Write;

use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::health::HealthPhase;
use nexrade_core::persistence::{PersistenceConfig, Snapshot};
use nexrade_core::store::Entry;
use nexrade_core::types::DataType;

/// A temp directory removed on drop. Mirrors the helper in `tls_listener.rs`
/// rather than pulling in `tempfile` for test-only use.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "nexrade-startup-{}-{}-{}",
            tag,
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&p).expect("create temp dir");
        TempDir(p)
    }

    fn path(&self, name: &str) -> std::path::PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// A config that binds loopback on a free port with **no** persistence
/// configured, so each test opts into exactly the artifact it is testing.
async fn base_config() -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1".to_string(),
        port: free_port().await,
        persistence: PersistenceConfig {
            rdb_path: None,
            aof_path: None,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn write_file(path: &std::path::Path, bytes: &[u8]) {
    let mut f = std::fs::File::create(path).expect("create test file");
    f.write_all(bytes).expect("write test file");
}

/// Run the listener to completion, expecting startup to fail.
///
/// A successful `run()` would block until shutdown, so a timeout here means
/// the server *started* — which is itself a test failure, reported as such
/// rather than hanging CI.
async fn expect_startup_failure(db: Db, what: &str) -> String {
    let listener = nexrade_server::Listener::new(db.clone(), None);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), listener.run()).await;

    let err = match outcome {
        Ok(Ok(())) => panic!("{what}: startup unexpectedly SUCCEEDED; it must be refused"),
        Ok(Err(e)) => format!("{e:#}"),
        Err(_) => panic!(
            "{what}: startup neither failed nor returned within 10s — \
             the server appears to have started and begun serving"
        ),
    };

    // The instance must never advertise readiness after a refused startup.
    assert_ne!(
        db.lifecycle().phase(),
        HealthPhase::Ready,
        "{what}: lifecycle reached Ready despite a failed startup"
    );
    let report = nexrade_core::health::health_report(&db);
    assert!(
        !report.ready,
        "{what}: health_report says ready=true despite a failed startup"
    );

    err
}

// ─── Occupied port ────────────────────────────────────────────────────────────

#[tokio::test]
async fn startup_fails_when_the_port_is_already_taken() {
    // Hold the port for the duration of the test so the bind genuinely races
    // a live socket rather than a TIME_WAIT remnant.
    let squatter = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let taken = squatter.local_addr().unwrap().port();

    let mut config = base_config().await;
    config.port = taken;
    let db = Db::new(config);

    let err = expect_startup_failure(db, "occupied port").await;
    let lower = err.to_lowercase();
    assert!(
        lower.contains("address") || lower.contains("in use") || lower.contains("bind"),
        "error should name the bind failure, got: {err}"
    );

    drop(squatter);
}

#[tokio::test]
async fn startup_failure_does_not_leave_the_phase_at_ready() {
    // Explicit regression for the 1.2.0 defect (`set_phase(Failed)` never
    // called on startup failure) combined with 1.3.0's F3: whatever phase a
    // refused startup lands in, it must not be Ready, and Failed must stick.
    let squatter = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = base_config().await;
    config.port = squatter.local_addr().unwrap().port();
    let db = Db::new(config);

    let _ = expect_startup_failure(db.clone(), "phase after bind failure").await;

    // The CLI records Failed when `run()` returns Err; do the same here and
    // confirm the terminal phase cannot be walked back.
    db.lifecycle().set_phase(HealthPhase::Failed);
    assert_eq!(db.lifecycle().phase(), HealthPhase::Failed);
    db.lifecycle().set_phase(HealthPhase::Ready);
    assert_eq!(
        db.lifecycle().phase(),
        HealthPhase::Failed,
        "Failed is terminal: a failed startup must not be maskable by a later Ready"
    );
    assert!(!nexrade_core::health::health_report(&db).live);
}

// ─── Corrupt / damaged RDB ────────────────────────────────────────────────────

#[tokio::test]
async fn startup_fails_on_a_garbage_rdb() {
    let dir = TempDir::new("bad-rdb");
    let rdb = dir.path("nexrade.rdb");
    write_file(&rdb, b"this is definitely not a NEXD snapshot");

    let mut config = base_config().await;
    config.persistence.rdb_path = Some(rdb.to_string_lossy().into_owned());
    let db = Db::new(config);

    let err = expect_startup_failure(db, "garbage RDB").await;
    assert!(
        err.to_lowercase().contains("rdb"),
        "error should name the RDB, got: {err}"
    );
}

#[tokio::test]
async fn startup_fails_on_a_truncated_rdb() {
    let dir = TempDir::new("trunc-rdb");
    let rdb = dir.path("nexrade.rdb");

    // Build a real snapshot, then cut it in half — the drill's "damaged RDB"
    // case. A valid header followed by a truncated body must not be accepted
    // as an empty-but-fine database.
    {
        let seed = Db::new(ServerConfig::default());
        let sdb = seed.store.db(0);
        for i in 0..64 {
            let key = format!("key:{i}");
            let entry = Entry::new(DataType::String(format!("value-{i}").into()));
            sdb.write_for(key.as_bytes())
                .insert(key.clone().into_bytes(), entry);
        }
        Snapshot::new(seed.store.snapshot_dbs())
            .save(&rdb)
            .expect("write seed snapshot");
    }

    let full = std::fs::read(&rdb).expect("read seed snapshot");
    assert!(full.len() > 16, "seed snapshot should be non-trivial");
    write_file(&rdb, &full[..full.len() / 2]);

    let err = expect_startup_failure(db_with_rdb(&rdb).await, "truncated RDB").await;
    assert!(
        err.to_lowercase().contains("rdb"),
        "error should name the RDB, got: {err}"
    );
}

async fn db_with_rdb(rdb: &std::path::Path) -> Db {
    let mut config = base_config().await;
    config.persistence.rdb_path = Some(rdb.to_string_lossy().into_owned());
    Db::new(config)
}

// ─── Corrupt / truncated AOF ──────────────────────────────────────────────────

#[tokio::test]
async fn startup_fails_on_a_garbage_aof() {
    let dir = TempDir::new("bad-aof");
    let aof = dir.path("nexrade.aof");
    write_file(&aof, b"*not-a-resp-array\r\ngarbage\r\n");

    let mut config = base_config().await;
    config.persistence.aof_path = Some(aof.to_string_lossy().into_owned());
    let db = Db::new(config);

    let err = expect_startup_failure(db, "garbage AOF").await;
    assert!(
        err.to_lowercase().contains("aof"),
        "error should name the AOF, got: {err}"
    );
}

#[tokio::test]
async fn startup_fails_on_an_aof_with_a_truncated_tail() {
    let dir = TempDir::new("trunc-aof");
    let aof = dir.path("nexrade.aof");

    // Two complete commands then a half-written third. This is the case the
    // 1.2.2 replay fix made detectable: before it, the tail was never parsed,
    // so a truncated AOF started "clean" with silent data loss.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\nqux\r\n");
    bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\ntrun");
    write_file(&aof, &bytes);

    let mut config = base_config().await;
    config.persistence.aof_path = Some(aof.to_string_lossy().into_owned());
    let db = Db::new(config);

    let err = expect_startup_failure(db, "truncated AOF").await;
    assert!(
        err.to_lowercase().contains("aof"),
        "error should name the AOF, got: {err}"
    );
}

#[tokio::test]
async fn startup_fails_when_an_aof_command_replays_as_an_error() {
    let dir = TempDir::new("bad-cmd-aof");
    let aof = dir.path("nexrade.aof");

    // Well-formed RESP that is not a valid command. Replay must refuse rather
    // than skip it: silently dropping a command means the recovered dataset
    // does not match the log.
    write_file(
        &aof,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n\
          *2\r\n$18\r\nNOSUCHCOMMANDATALL\r\n$1\r\nx\r\n",
    );

    let mut config = base_config().await;
    config.persistence.aof_path = Some(aof.to_string_lossy().into_owned());
    let db = Db::new(config);

    let err = expect_startup_failure(db, "unknown AOF command").await;
    assert!(
        err.to_lowercase().contains("aof"),
        "error should name the AOF, got: {err}"
    );
}

// ─── Ambiguous recovery source ────────────────────────────────────────────────

#[tokio::test]
async fn startup_fails_when_both_rdb_and_aof_exist() {
    let dir = TempDir::new("ambiguous");
    let rdb = dir.path("nexrade.rdb");
    let aof = dir.path("nexrade.aof");

    {
        let seed = Db::new(ServerConfig::default());
        let sdb = seed.store.db(0);
        sdb.write_for(b"seed").insert(
            b"seed".to_vec(),
            Entry::new(DataType::String(b"value".as_slice().into())),
        );
        Snapshot::new(seed.store.snapshot_dbs())
            .save(&rdb)
            .expect("write snapshot");
    }
    write_file(&aof, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

    let mut config = base_config().await;
    config.persistence.rdb_path = Some(rdb.to_string_lossy().into_owned());
    config.persistence.aof_path = Some(aof.to_string_lossy().into_owned());
    let db = Db::new(config);

    let err = expect_startup_failure(db, "ambiguous RDB+AOF").await;
    let lower = err.to_lowercase();
    assert!(
        lower.contains("ambiguous"),
        "error should explain the ambiguity, got: {err}"
    );
}

// ─── Unavailable persistence path ─────────────────────────────────────────────

#[tokio::test]
async fn startup_fails_when_the_aof_path_is_unwritable() {
    let dir = TempDir::new("unwritable-aof");
    // A path whose *parent* does not exist: the AOF writer cannot create it.
    let aof = dir.path("no-such-subdir/nexrade.aof");

    let mut config = base_config().await;
    config.persistence.aof_path = Some(aof.to_string_lossy().into_owned());
    let db = Db::new(config);

    let err = expect_startup_failure(db, "unwritable AOF path").await;
    assert!(
        err.to_lowercase().contains("aof"),
        "error should name the AOF, got: {err}"
    );
}

// ─── Invalid / missing TLS material ───────────────────────────────────────────
//
// Through 1.2.3 every one of these only logged `error!`/`warn!` and startup
// continued: the banner printed `TLS  ON`, the TLS port was never bound, and
// the process served plaintext and exited 0. Confirmed against the 1.3.0
// release binary before the fix — a garbage cert produced
// `ERROR ... failed to initialize TLS (127.0.0.1:7412): no certificates found`
// while the server kept happily running.

#[cfg(feature = "tls")]
#[tokio::test]
async fn startup_fails_when_the_tls_cert_is_not_a_certificate() {
    let dir = TempDir::new("bad-cert");
    let cert = dir.path("cert.pem");
    let key = dir.path("key.pem");
    write_file(&cert, b"this is not a certificate\n");
    write_file(&key, b"this is not a key\n");

    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(cert.to_string_lossy().into_owned());
    config.tls_key = Some(key.to_string_lossy().into_owned());
    config.tls_port = Some(free_port().await);
    let db = Db::new(config);

    let err = expect_startup_failure(db, "invalid TLS cert").await;
    assert!(
        err.to_lowercase().contains("tls"),
        "error should name TLS, got: {err}"
    );
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn startup_fails_when_the_tls_cert_file_is_missing() {
    let dir = TempDir::new("missing-cert");
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(dir.path("nope-cert.pem").to_string_lossy().into_owned());
    config.tls_key = Some(dir.path("nope-key.pem").to_string_lossy().into_owned());
    config.tls_port = Some(free_port().await);
    let db = Db::new(config);

    let err = expect_startup_failure(db, "missing TLS cert file").await;
    assert!(
        err.to_lowercase().contains("tls"),
        "error should name TLS, got: {err}"
    );
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn startup_fails_when_tls_is_enabled_without_cert_or_key() {
    // The old code logged "TLS enabled but tls-cert or tls-key not set, TLS
    // listener skipped" at warn level and started anyway.
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = None;
    config.tls_key = None;
    let db = Db::new(config);

    let err = expect_startup_failure(db, "TLS enabled without material").await;
    let lower = err.to_lowercase();
    assert!(lower.contains("tls"), "error should name TLS, got: {err}");
    assert!(
        lower.contains("cert") || lower.contains("key"),
        "error should name the missing setting, got: {err}"
    );
}

// ─── Control: a clean config does start ───────────────────────────────────────
//
// Without this, every assertion above would still pass if `run()` had been
// broken into always returning Err.

#[tokio::test]
async fn a_clean_config_reaches_ready() {
    let dir = TempDir::new("clean");
    let rdb = dir.path("nexrade.rdb");
    // Configured but absent: nothing to recover, so startup proceeds.
    let mut config = base_config().await;
    config.persistence.rdb_path = Some(rdb.to_string_lossy().into_owned());
    let db = Db::new(config);

    let listener = nexrade_server::Listener::new(db.clone(), None);
    let shutdown = listener.shutdown_subscriber();
    assert!(!*shutdown.borrow(), "shutdown starts un-signalled");

    let run = tokio::spawn(async move { listener.run().await });

    // Poll for Ready rather than sleeping a fixed interval.
    let mut reached = false;
    for _ in 0..100 {
        if db.lifecycle().phase() == HealthPhase::Ready {
            reached = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        reached,
        "a clean config must reach Ready; phase is {:?}",
        db.lifecycle().phase()
    );
    assert!(nexrade_core::health::health_report(&db).live);

    // Shut down via the same path a SIGTERM would take.
    db.shutdown.notify_one();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), run).await;
}
