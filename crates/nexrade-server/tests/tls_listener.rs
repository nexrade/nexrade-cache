//! End-to-end test for the TLS listener: starts a real `Listener` with
//! `tls_enabled = true`, connects to it as a genuine TLS client (via
//! `tokio-rustls`), and verifies a command round-trips correctly. Also
//! verifies the plain-TCP listener keeps working unaffected when TLS is
//! enabled alongside it.

#![cfg(feature = "tls")]

use std::io::Write;
use std::sync::Arc;

use nexrade_core::db::{Db, ServerConfig};
use nexrade_core::persistence::PersistenceConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Self-signed test certificate for "localhost", valid 2026-2036. Generated
// with `openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem
// -days 3650 -nodes -subj "/CN=localhost" -addext "subjectAltName=IP:127.0.0.1"`.
// Test-only; not used anywhere outside this file.
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

/// Bind a throwaway listener on 127.0.0.1 to claim a free OS-assigned port,
/// then drop it. `Listener::run` binds by address itself (it doesn't accept
/// a pre-bound socket), so this is the standard way to hand it a free port.
async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// A temp directory that's removed on drop — avoids pulling in the
/// `tempfile` crate for one test-only helper.
struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Write the embedded test cert/key to a fresh temp dir, returning their paths.
fn write_test_cert() -> (std::path::PathBuf, std::path::PathBuf, TempDir) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir_path = std::env::temp_dir().join(format!(
        "nexrade-tls-test-{}-{}",
        std::process::id(),
        unique
    ));
    std::fs::create_dir_all(&dir_path).expect("create temp dir for test cert");
    let dir = TempDir(dir_path);
    let cert_path = dir.0.join("cert.pem");
    let key_path = dir.0.join("key.pem");
    std::fs::File::create(&cert_path)
        .unwrap()
        .write_all(TEST_CERT_PEM.as_bytes())
        .unwrap();
    std::fs::File::create(&key_path)
        .unwrap()
        .write_all(TEST_KEY_PEM.as_bytes())
        .unwrap();
    (cert_path, key_path, dir)
}

/// A `rustls` server-certificate verifier that accepts our specific
/// self-signed test cert (and nothing else) — the equivalent of a client
/// pinning exactly one known certificate, so the test doesn't need a real
/// CA chain.
#[derive(Debug)]
struct AcceptTestCert {
    expected: rustls_pki_types::CertificateDer<'static>,
}

impl rustls::client::danger::ServerCertVerifier for AcceptTestCert {
    fn verify_server_cert(
        &self,
        end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.expected.as_ref() {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(
                "unexpected certificate presented".into(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn test_tls_connector() -> tokio_rustls::TlsConnector {
    use rustls_pki_types::pem::PemObject;
    let cert = rustls_pki_types::CertificateDer::from_pem_slice(TEST_CERT_PEM.as_bytes())
        .expect("parse embedded test cert");
    let verifier = Arc::new(AcceptTestCert { expected: cert });
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

/// RESP-encode a command, matching the wire format the server expects.
fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

#[tokio::test]
async fn tls_listener_accepts_and_serves_commands() {
    let (cert_path, key_path, _tmp) = write_test_cert();
    let plain_port = free_port().await;
    let tls_port = free_port().await;

    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port: plain_port,
        databases: 1,
        tls_enabled: true,
        tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        tls_key: Some(key_path.to_string_lossy().into_owned()),
        tls_port: Some(tls_port),
        metrics_enabled: false,
        persistence: PersistenceConfig {
            rdb_path: None,
            ..Default::default()
        },
        ..Default::default()
    };

    let db = Db::new(config);
    let listener = nexrade_server::Listener::new(db, None);
    tokio::spawn(async move {
        let _ = listener.run().await;
    });

    // Give the accept loops a moment to bind before connecting.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // ── TLS client ────────────────────────────────────────────────────────
    let tcp = TcpStream::connect(("127.0.0.1", tls_port))
        .await
        .expect("connect TCP for TLS upgrade");
    let connector = test_tls_connector();
    let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .expect("TLS handshake should succeed against the real TLS listener");

    tls.write_all(&encode_command(&["PING"])).await.unwrap();
    let mut buf = [0u8; 64];
    let n = tls.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"+PONG\r\n", "PING over TLS should return PONG");

    tls.write_all(&encode_command(&["SET", "k", "v"]))
        .await
        .unwrap();
    let n = tls.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n", "SET over TLS should succeed");

    tls.write_all(&encode_command(&["GET", "k"])).await.unwrap();
    let n = tls.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b"$1\r\nv\r\n",
        "GET over TLS should return the value set moments ago"
    );

    // ── Plain TCP client still works alongside the TLS listener ────────────
    let mut plain = TcpStream::connect(("127.0.0.1", plain_port))
        .await
        .expect("connect plain TCP");
    plain.write_all(&encode_command(&["PING"])).await.unwrap();
    let n = plain.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b"+PONG\r\n",
        "plain-TCP listener should keep working when TLS is also enabled"
    );

    // Data set over the TLS connection should be visible from the plain
    // connection too — same `Db`, same store, just a different transport.
    plain
        .write_all(&encode_command(&["GET", "k"]))
        .await
        .unwrap();
    let n = plain.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b"$1\r\nv\r\n",
        "plain connection should see data written over the TLS connection"
    );
}

#[tokio::test]
async fn plain_tcp_rejects_raw_tls_handshake_bytes() {
    // Sanity check for the reverse direction: a client that tries to speak
    // TLS to the *plain* port should not get a valid RESP response (the
    // server will see it as garbage RESP input, not silently upgrade).
    let (cert_path, key_path, _tmp) = write_test_cert();
    let plain_port = free_port().await;
    let tls_port = free_port().await;

    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port: plain_port,
        databases: 1,
        tls_enabled: true,
        tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        tls_key: Some(key_path.to_string_lossy().into_owned()),
        tls_port: Some(tls_port),
        metrics_enabled: false,
        persistence: PersistenceConfig {
            rdb_path: None,
            ..Default::default()
        },
        ..Default::default()
    };

    let db = Db::new(config);
    let listener = nexrade_server::Listener::new(db, None);
    tokio::spawn(async move {
        let _ = listener.run().await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let tcp = TcpStream::connect(("127.0.0.1", plain_port))
        .await
        .expect("connect TCP");
    let connector = test_tls_connector();
    let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();

    // The plain listener has no TLS awareness at all — it just treats the
    // ClientHello bytes as (invalid/incomplete) RESP input and waits for
    // more, while the TLS client waits for a ServerHello that will never
    // arrive. Neither side errors on its own, so the "handshake never
    // completes" _is_ the pass condition here; bound the wait rather than
    // asserting on a `Result` that would otherwise never resolve.
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        connector.connect(server_name, tcp),
    )
    .await;
    match outcome {
        Ok(Ok(_)) => panic!("TLS handshake against the plain-TCP port should not succeed"),
        Ok(Err(_)) | Err(_) => {
            // Either the handshake errored out, or it never completed
            // within the timeout — both mean the plain listener did not
            // silently upgrade to TLS.
        }
    }
}
