//! 1.3.1 — `validate_tls_config` and its agreement with startup.
//!
//! `--preflight` exists so a deploy gate can reject a config *before* the
//! rollout reaches a node. Through 1.3.0 it validated bind addresses and
//! persistence artifacts but performed **no TLS validation at all**, so a
//! config with a missing, unparseable, or mismatched certificate reported
//! `preflight: OK` and then exited 1 at the next step.
//!
//! Two properties are asserted here:
//!
//!   1. `validate_tls_config` flags each broken configuration, and
//!   2. it **agrees with `Listener::run`** — anything it accepts starts, and
//!      anything it rejects is refused.
//!
//! (2) is the one that matters. A second validator that merely looks
//! plausible is worse than none, because it turns a loud startup failure
//! into a green deploy gate. The agreement tests below drive the real
//! `Listener::run` against the same configs, so the two cannot drift apart
//! without a test going red.

use std::io::Write;

use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::persistence::PersistenceConfig;
use nexrade_server::validate_tls_config;
use rustls_pki_types::pem::PemObject;

// A matched self-signed cert/key pair for `localhost`, valid 2026–2036.
// Same pair as `tls_listener.rs` — duplicated rather than shared because
// integration test binaries can't import each other's helpers.
const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDJTCCAg2gAwIBAgIUSwUDV56Mbkch2NGUtu5KEMYFx5AwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDcxMjE3MDU1OFoXDTM2MDcw
OTE3MDU1OFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA19JAd+hj2j0euA1hEi2tql8h31TfLRWo3E4lsAYWcgh2
yZ6tUEw2ZqufcH1xBM6T6Ceho3e6cDuFNBM0sZAQVdyY2cOwJwjhirANrtuX2UUi
45VSoFDlnzs2uUV+6GC4DbzOKEQmWRU1ZPJU1nj8jF/BIaHBftf6RyLhrf04lLof
lYgbX6WjFOPiiq1KYyqDoLWCAiW0rXquHj3cBu9ChdbsLtHwyXHF/RXRtmiN5VrT
iEzm/62vyEIMaJpg4GpO+crQrSj7coW69Ex1fpAzEL7UzA99esgVL8YUhMRZBG1A
ekE36XruPDcffe+2BHpKz7eZB88y+NUKHBNI1qXLzQIDAQABo28wbTAdBgNVHQ4E
FgQUmyTqUDLG9vH1gCakWuGM94hLmjIwHwYDVR0jBBgwFoAUmyTqUDLG9vH1gCak
WuGM94hLmjIwDwYDVR0TAQH/BAUwAwEB/zAaBgNVHREEEzARgglsb2NhbGhvc3SH
BH8AAAEwDQYJKoZIhvcNAQELBQADggEBAF64xg1k7n9bdjqedKjE80paEHJRASel
+TeFLk3so6WUrQXKGaf60KOZeMrBhSt3wDt/Zyh+dbaJxwsdefOqPuOFfO6unZZW
8zlRR3QGUku+rykqCNL/gXiA/QcYaY1INFMHosFZ3jociFrRyLzOdsmWhGTLVYE9
+12P2/9PGpIJBaENXMuX/4Ak9ZdCCx2xl0jOT8kfyVJSzGgCymC3tvCP/f2aT/8o
BlE/Z7JYmzr3oASoXjTsZYvUa/w2ls56rscYfcSsLdX0sUt/JQ6xsUgu8k4YmEne
V49U7WMShH1lIQWixrg2F2JX1pE26cg5Ww7jhuSim3UC/YNUtz+7vq4=
-----END CERTIFICATE-----
";

const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDX0kB36GPaPR64
DWESLa2qXyHfVN8tFajcTiWwBhZyCHbJnq1QTDZmq59wfXEEzpPoJ6Gjd7pwO4U0
EzSxkBBV3JjZw7AnCOGKsA2u25fZRSLjlVKgUOWfOza5RX7oYLgNvM4oRCZZFTVk
8lTWePyMX8EhocF+1/pHIuGt/TiUuh+ViBtfpaMU4+KKrUpjKoOgtYICJbSteq4e
PdwG70KF1uwu0fDJccX9FdG2aI3lWtOITOb/ra/IQgxommDgak75ytCtKPtyhbr0
THV+kDMQvtTMD316yBUvxhSExFkEbUB6QTfpeu48Nx9977YEekrPt5kHzzL41Qoc
E0jWpcvNAgMBAAECggEAAd5ZGNCr8hmhpDpj+LddhvaXb/wQWyNvXmzsyIQ6ET/c
sT3p3Zb5JiYkJu3EMiRpua/JbT+SWYWWw+A/PPRRw4X/jbIEiS/M6BmINBF+vO6x
KvSe9tVd7Kr0/DLXD9pDPyLWkTA53JaV1BQM2nE9ccqYcVUlU3+WkS8C5uv2SE9Y
sIDQhvVXd631JGhKcSmHtDBdszdIso4LSV+SZ7CG94znYaAw2E+ffEkR5nAKImGY
yEYeUFFBmRWno1c/+5CLByg+YyB31+TWVVkn+dzOscbcdexxgaOyOGWRGHJ8+1Rn
uBfr24RXPEq3xDaFQ1fBRYijl4d6mbfwv05LttJrvwKBgQD5v3Wyfs1ZHch2EkQk
xqo5yikaYaQAz+0N0ycMF1TQmN/PH3DZkoJJsSPallO4EYGjPRqCFZk4/EgY/uNl
B7G1cPvABNqFlTHg3RUJozXh3YdVhlZCfA5mvFCss0hUIFlGY3s3OVQ0yxw1b1ew
6vD0pSY8Ph1uH53X2GRbs//rnwKBgQDdOV6KshrkF89bvD1zdcD4HivnWqn6Zzyc
Bh+WSxlb97qLIhIGhTVDXfeVMlxQVhzfeA0nQW63aCxm/1RRYvlCp5d3+Rx/AwWI
QzXr5lHXnJ9tMxBMKe0Tc5M/flQFDhvUdSYf8w+kQkQBnFvCD0D+oaLMip8HGjpJ
t4NRSKZREwKBgA2jig7sY9R5Duh7yOLlQoiTZLk/GdC9iimWHWzInWYi4x4Rjn0j
RiA2H0ohqYLE2fqLLLZr7YkyJdHPoaVzzR2mhOkQmspuwmGQUUTMd/XUvj5Kbs2E
rtinchRsWgfWGGoCpsj2RYX4jZrRcM2FlxEVL8hccAkCiwEtnRVw+AnrAoGBAMeA
my/9Gp8kkc2q3sgnI1Ue8Hz9mFjHjTMvmoDRTRdROxuKKDNVIgmUzlfwSKvyXKty
+nmyWoRwH8rq7EFRPnTL6p85OmeYc/7EjfYliR0mk+fIqyPkk3Z9Pgd+h4rfhF1/
IFijvDFnySiit2U0mGqJneVUBcJD9tjP9E7zc3mdAoGBANQj3vMdW4KvIBxa2wwh
htScGiicRB6U9p+W2CZazT1JLlsZdCEOeKtAx3PISyK2+RhD7ZBAercpUfv1AMCx
hCjx+MmzRaMlJvc+wDpucqlrexMOykx27XM6ba/9ZW6xNUOj5JEGGITD2trrXF0g
cW9SbfE95F/R26mT1TRNDTTv
-----END PRIVATE KEY-----
";

// An unrelated but **genuinely valid** RSA-2048 private key. It parses
// cleanly on its own; it simply isn't the partner of TEST_CERT_PEM.
//
// The validity matters: an earlier draft of this file used fabricated
// base64 here, which failed to parse at all — so the "mismatch" test was
// silently re-testing the unparseable-cert path and would have passed even
// against a validator that only checked PEM syntax. Verified with
// `openssl pkey -in <file> -noout`, which accepts this key and rejected the
// fabricated one.
const OTHER_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCn83+QJXHFKqHb
hfy1kyaaubeJ8cjUmkaSBmpiMDwFJkAHHbgpVXOa4k0CVpcD9mzs1TL//Y61Axbc
GfZ+h6gddG5un4udMw66ZXPbN/8GSxb5Mj+2Pw4Q4zwviZgrOVCrxSxXk8Jo2Qck
SlO6vkSSbh8SdRqSD+zVJhk88bUcvAHyLRubTTSUqrterszHXf7rEs5iUlwpcB4f
G+axMu3mZ5cvXCwcAs4cRdd9XLLT+SXNp6eI1KNV+gfYAwHIVe+N0FLG9wPpZDZB
8WGOJqT7Q/Fac0JqDQ9GVIL17Zaj+MRmiAO6Td61VXHDF+L6FtG3mbg/KtKfhmVI
2hKV1exnAgMBAAECggEAO8S1SDxzFOn7iM5RZOK1kcl2rhIL2NEAPTYoRMIKkgCP
N5kVcSw2RE+1fRgIjQH0uIGUsTHwR62ORIy+wj+Pjc/h/t8rRammW6IADWsLiBdP
2SXPM5GT9WGQiRBLxVITydUU1yO/wyU82+bRjjro1z12NFDVQFaHf0EcKSqRO+R9
c4xdvNbaFJb2a225D5uGlR6BFis0igJ1oCSNGsGHmXOwifUhUjn3kUj5ARiM124c
Ih2KOksRxg50Ft1Z7qkkXqTPdPsP5ygA0V3DclgZYWwO/HYrDYbyJ8jTa4nJhyKl
2g5vtxkrf1knLlQAtOKUxD20CED5Q1wKKzyVuV2EoQKBgQDr3UeXkJJkREVfW6AE
imuNpTpW0EFyRRNgCA+BMeEnPr9TnMGqQKJyF6QqtPlk5jmnrH4N5IwHTEOP8F5l
aJci1u47r1gK+SWHRGMs4NK5GqZroQdzRBQhmYdYLEoZg+0nMGsowKKN4/dTRvq3
LXquJAeb/dOssnwQivAaa7y0xwKBgQC2SgDDCheVLFUD6PnYNfWj2d/A03Scc0bS
KNQVjUvr7m+ssXe5nofF3hCpL+xiq0e7jTxHR/1gLROeenXNvaSnO14q8KurBOoo
u4Z6+DQkUu5WiTdNhNets4e+bcu3jdd0LGPMlaygV7JbrHVB39DQS0TJ6lGHKc79
2qie/0grYQKBgDO5Xw3Z4oCiiCQVT84vHM7/QP/ww6lvhIQ2wE+wxJN6qzKG2eGg
Mv+aN6I19csuwc4Hgc1CJYMkMlzKdaj/esVlJPFpzoD5ikVTtfwNgaieM4i+04dv
koqbxJaNf+KAj+1cLOPO+tbq+z4D/s9U5eZyeEi5LUZeDd8C2QyyO7vZAoGBAKbx
l0kIURi5BRMTpt0wbcqlmpoKDl3J5S5LXhBu2v0z3OqXjUJdwZKhETkhPqgOnR9S
9cWCVLZkEfetx32pFMZjRJam21FAqwKq2zp7XaV2nfh6qj9AThYyuTrZaxyrtooa
rTuMSBCAwEPc6XZu99oLVPBmvEvBKmSqgIs82GeBAoGAG9BjcxAkCMxoiiqchgZy
+09uavSMl7okC+UuJWlDs7L9Lces/Kr5s2izka2rx+UfmfhD7xZksQrZvxHhj+6k
Hn90gGon8/b3TUB/CsIXnyYRA4jHoCy0rmew7ZYHDs4I0mlPXa2genBTPVqLy2RS
W6Cj4O5DSvPbGPEtsds85I8=
-----END PRIVATE KEY-----
";

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "nexrade-tlsvalid-{}-{}-{}",
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

    /// Write `cert.pem` / `key.pem` and return their paths as strings.
    fn write_pair(&self, cert: &str, key: &str) -> (String, String) {
        let cp = self.path("cert.pem");
        let kp = self.path("key.pem");
        write_file(&cp, cert.as_bytes());
        write_file(&kp, key.as_bytes());
        (
            cp.to_string_lossy().into_owned(),
            kp.to_string_lossy().into_owned(),
        )
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn write_file(path: &std::path::Path, bytes: &[u8]) {
    let mut f = std::fs::File::create(path).expect("create test file");
    f.write_all(bytes).expect("write test file");
}

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

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

// ─── The validator flags each broken configuration ────────────────────────────

#[tokio::test]
async fn tls_disabled_is_always_valid() {
    let config = base_config().await;
    assert!(
        validate_tls_config(&config).is_empty(),
        "TLS off must never be a validation error"
    );
}

#[tokio::test]
async fn a_cert_configured_while_tls_is_off_is_not_an_error() {
    // Staging a rollout by landing the cert before flipping `enabled` is a
    // legitimate workflow. The CLI warns about it; the validator must not
    // fail it, or the warning becomes a hard deploy block.
    let dir = TempDir::new("off-with-cert");
    let (cert, key) = dir.write_pair(TEST_CERT_PEM, TEST_KEY_PEM);
    let mut config = base_config().await;
    config.tls_enabled = false;
    config.tls_cert = Some(cert);
    config.tls_key = Some(key);
    assert!(
        validate_tls_config(&config).is_empty(),
        "cert present + tls disabled must be a warning, not an error"
    );
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn a_matched_pair_on_a_distinct_port_validates() {
    let dir = TempDir::new("good");
    let (cert, key) = dir.write_pair(TEST_CERT_PEM, TEST_KEY_PEM);
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(cert);
    config.tls_key = Some(key);
    config.tls_port = Some(free_port().await);

    let problems = validate_tls_config(&config);
    assert!(
        problems.is_empty(),
        "a valid TLS config must produce no errors, got: {problems:?}"
    );
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn missing_both_cert_and_key_is_flagged() {
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = None;
    config.tls_key = None;

    let problems = validate_tls_config(&config);
    assert_eq!(problems.len(), 1, "expected one problem, got {problems:?}");
    let lower = problems[0].to_lowercase();
    assert!(lower.contains("cert") && lower.contains("key"));
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn a_cert_without_a_key_is_flagged() {
    let dir = TempDir::new("cert-only");
    let (cert, _key) = dir.write_pair(TEST_CERT_PEM, TEST_KEY_PEM);
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(cert);
    config.tls_key = None;
    config.tls_port = Some(free_port().await);

    let problems = validate_tls_config(&config);
    assert_eq!(problems.len(), 1, "expected one problem, got {problems:?}");
    assert!(
        problems[0].to_lowercase().contains("key"),
        "the error should name the missing key, got: {}",
        problems[0]
    );
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn a_key_without_a_cert_is_flagged() {
    let dir = TempDir::new("key-only");
    let (_cert, key) = dir.write_pair(TEST_CERT_PEM, TEST_KEY_PEM);
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = None;
    config.tls_key = Some(key);
    config.tls_port = Some(free_port().await);

    let problems = validate_tls_config(&config);
    assert_eq!(problems.len(), 1, "expected one problem, got {problems:?}");
    assert!(
        problems[0].to_lowercase().contains("cert"),
        "the error should name the missing cert, got: {}",
        problems[0]
    );
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn a_nonexistent_cert_file_is_flagged() {
    let dir = TempDir::new("absent");
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(dir.path("nope-cert.pem").to_string_lossy().into_owned());
    config.tls_key = Some(dir.path("nope-key.pem").to_string_lossy().into_owned());
    config.tls_port = Some(free_port().await);

    let problems = validate_tls_config(&config);
    assert_eq!(problems.len(), 1, "expected one problem, got {problems:?}");
    assert!(problems[0].to_lowercase().contains("tls"));
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn an_unparseable_cert_is_flagged() {
    let dir = TempDir::new("garbage");
    let (cert, key) = dir.write_pair("this is not a certificate\n", "this is not a key\n");
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(cert);
    config.tls_key = Some(key);
    config.tls_port = Some(free_port().await);

    let problems = validate_tls_config(&config);
    assert_eq!(problems.len(), 1, "expected one problem, got {problems:?}");
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn a_key_that_does_not_match_its_cert_is_flagged() {
    // The case a file-existence check cannot catch: both files parse, but
    // rustls refuses the pair. Only reachable by doing the real
    // `with_single_cert` build, which is why the validator delegates to
    // nexrade_tls rather than reimplementing PEM checks.
    let dir = TempDir::new("mismatch");

    // Guard the guard: prove OTHER_KEY_PEM is a *valid* key before relying
    // on it as the mismatch case. Without this, replacing it with malformed
    // bytes would turn this test into a duplicate of
    // `an_unparseable_cert_is_flagged` and nothing would notice. (That is
    // exactly what an earlier draft of this file did.)
    let ok_dir = TempDir::new("mismatch-control");
    let (other_as_cert_pair_cert, other_key_alone) =
        ok_dir.write_pair(TEST_CERT_PEM, OTHER_KEY_PEM);
    assert!(
        nexrade_tls::validate_pem_files(&other_as_cert_pair_cert, &other_key_alone).is_err(),
        "precondition: the pair must be rejected"
    );
    assert!(
        std::fs::read_to_string(&other_key_alone)
            .expect("read key")
            .contains("BEGIN PRIVATE KEY"),
        "precondition: OTHER_KEY_PEM must be a PEM private key"
    );
    // The decisive check: this key pairs successfully with *its own* cert
    // shape, i.e. it is parseable. We assert parseability directly.
    assert!(
        rustls_pki_types::PrivateKeyDer::from_pem_file(&other_key_alone).is_ok(),
        "precondition: OTHER_KEY_PEM must itself parse as a private key, \
         otherwise this test duplicates the unparseable-cert case"
    );

    let (cert, key) = dir.write_pair(TEST_CERT_PEM, OTHER_KEY_PEM);
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(cert);
    config.tls_key = Some(key);
    config.tls_port = Some(free_port().await);

    let problems = validate_tls_config(&config);
    assert_eq!(
        problems.len(),
        1,
        "a mismatched cert/key pair must be flagged, got {problems:?}"
    );
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_port_equal_to_the_plaintext_port_is_flagged() {
    // Pre-1.3.1 this surfaced only as a bare `Address already in use` from
    // the second bind, with nothing in the message mentioning TLS.
    let dir = TempDir::new("port-collision");
    let (cert, key) = dir.write_pair(TEST_CERT_PEM, TEST_KEY_PEM);
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(cert);
    config.tls_key = Some(key);
    config.tls_port = Some(config.port);

    let problems = validate_tls_config(&config);
    assert_eq!(problems.len(), 1, "expected one problem, got {problems:?}");
    let lower = problems[0].to_lowercase();
    assert!(
        lower.contains("port"),
        "the error should name the port collision, got: {}",
        problems[0]
    );
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn the_default_tls_port_is_checked_when_none_is_set() {
    // `tls_port = None` means 6380. If the plaintext port is also 6380 the
    // collision is real even though no TLS port was written down.
    let dir = TempDir::new("default-port");
    let (cert, key) = dir.write_pair(TEST_CERT_PEM, TEST_KEY_PEM);
    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(cert);
    config.tls_key = Some(key);
    config.port = 6380;
    config.tls_port = None;

    let problems = validate_tls_config(&config);
    assert_eq!(
        problems.len(),
        1,
        "the implicit default TLS port must be checked too, got {problems:?}"
    );
}

// ─── Agreement with startup ───────────────────────────────────────────────────
//
// The reason the validator exists is that preflight and startup disagreed.
// A second validator that drifts is worse than none: it turns a loud startup
// failure into a green deploy gate. These tests pin the two together.

/// Drive the real `Listener::run` and report whether startup was refused.
async fn startup_is_refused(config: ServerConfig) -> bool {
    let db = Db::new(config);
    let listener = nexrade_server::Listener::new(db, None);
    match tokio::time::timeout(std::time::Duration::from_secs(10), listener.run()).await {
        Ok(Ok(())) => false,
        Ok(Err(_)) => true,
        // Timed out: `run()` is serving, i.e. startup was NOT refused.
        Err(_) => false,
    }
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn every_config_the_validator_rejects_is_also_refused_by_startup() {
    let dir = TempDir::new("agree-bad");
    let (good_cert, good_key) = dir.write_pair(TEST_CERT_PEM, TEST_KEY_PEM);

    let garbage_cert = dir.path("garbage-cert.pem");
    let garbage_key = dir.path("garbage-key.pem");
    write_file(&garbage_cert, b"not a certificate\n");
    write_file(&garbage_key, b"not a key\n");

    let other_key = dir.path("other-key.pem");
    write_file(&other_key, OTHER_KEY_PEM.as_bytes());

    let s = |p: &std::path::Path| Some(p.to_string_lossy().into_owned());

    // (label, cert, key, tls_port_is_plaintext_port)
    let cases: Vec<(&str, Option<String>, Option<String>, bool)> = vec![
        ("no cert or key", None, None, false),
        ("cert without key", Some(good_cert.clone()), None, false),
        ("key without cert", None, Some(good_key.clone()), false),
        (
            "missing files",
            s(&dir.path("absent-cert.pem")),
            s(&dir.path("absent-key.pem")),
            false,
        ),
        ("unparseable", s(&garbage_cert), s(&garbage_key), false),
        (
            "cert/key mismatch",
            Some(good_cert.clone()),
            s(&other_key),
            false,
        ),
        (
            "tls.port == port",
            Some(good_cert.clone()),
            Some(good_key.clone()),
            true,
        ),
    ];

    for (label, cert, key, collide) in cases {
        let mut config = base_config().await;
        config.tls_enabled = true;
        config.tls_cert = cert;
        config.tls_key = key;
        config.tls_port = Some(if collide {
            config.port
        } else {
            free_port().await
        });

        let problems = validate_tls_config(&config);
        assert!(
            !problems.is_empty(),
            "{label}: the validator must reject this config"
        );
        assert!(
            startup_is_refused(config).await,
            "{label}: the validator rejects this config but startup ACCEPTED it — \
             preflight would block a deploy that would actually have worked"
        );
    }
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn a_config_the_validator_accepts_actually_starts() {
    // The control. Without it, every assertion above would still pass if
    // `validate_tls_config` had been written to reject everything — and
    // preflight would reject all TLS configs while startup accepted them.
    let dir = TempDir::new("agree-good");
    let (cert, key) = dir.write_pair(TEST_CERT_PEM, TEST_KEY_PEM);

    let mut config = base_config().await;
    config.tls_enabled = true;
    config.tls_cert = Some(cert);
    config.tls_key = Some(key);
    config.tls_port = Some(free_port().await);

    assert!(
        validate_tls_config(&config).is_empty(),
        "precondition: this config must validate"
    );
    assert!(
        !startup_is_refused(config).await,
        "the validator accepted this config but startup refused it — \
         a deploy gate would pass and then the rollout would fail"
    );
}
