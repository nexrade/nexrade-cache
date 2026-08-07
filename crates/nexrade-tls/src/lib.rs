//! Built-in TLS for nexrade-cache.
//!
//! Uses `rustls` (pure-Rust TLS) for zero-dependency security.
//!
//! # Usage
//!
//! ```rust,no_run
//! use nexrade_tls::TlsAcceptor;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let acceptor = TlsAcceptor::from_pem_files("cert.pem", "key.pem").await?;
//!     // Use acceptor.accept(tcp_stream) to upgrade connections.
//!     Ok(())
//! }
//! ```

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::ServerConfig;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tracing::info;

/// A TLS acceptor wrapping a rustls configuration.
#[derive(Clone)]
pub struct TlsAcceptor {
    inner: Arc<tokio_rustls::TlsAcceptor>,
}

/// Build a rustls `ServerConfig` from PEM files.
///
/// Shared by [`TlsAcceptor::from_pem_files`] and [`validate_pem_files`] so
/// that a preflight check and a real startup can never disagree about
/// whether a certificate is usable — there is exactly one parser.
fn server_config_from_pem_files(cert_path: &Path, key_path: &Path) -> Result<ServerConfig> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("failed to build TLS server config")
}

/// Verify that a cert/key pair would produce a working TLS listener,
/// without binding a socket or retaining any state.
///
/// This performs the *entire* configuration build that startup performs —
/// file open, PEM parse, and the rustls `with_single_cert` step that
/// rejects a key which doesn't match its certificate. Anything this
/// accepts, startup accepts; anything this rejects, startup rejects.
///
/// Synchronous on purpose: it is pure filesystem + parsing work, so it can
/// be called from `--preflight` without a Tokio runtime.
pub fn validate_pem_files<P: AsRef<Path>>(cert_path: P, key_path: P) -> Result<()> {
    server_config_from_pem_files(cert_path.as_ref(), key_path.as_ref()).map(|_| ())
}

impl TlsAcceptor {
    /// Build from PEM certificate and key files.
    pub async fn from_pem_files<P: AsRef<Path>>(cert_path: P, key_path: P) -> Result<Self> {
        let config = server_config_from_pem_files(cert_path.as_ref(), key_path.as_ref())?;

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        info!("TLS configured from {:?}", cert_path.as_ref());
        Ok(Self {
            inner: Arc::new(acceptor),
        })
    }

    /// Upgrade a plain TCP stream to TLS.
    pub async fn accept(&self, stream: TcpStream) -> Result<TlsStream<TcpStream>> {
        self.inner
            .accept(stream)
            .await
            .context("TLS handshake failed")
    }
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .with_context(|| format!("failed to open cert file: {:?}", path))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("failed to parse certificates")?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found in {:?}", path);
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path)
        .with_context(|| format!("no private key found in {:?}", path))
}

/// Generate a self-signed certificate for development.
/// Returns (cert_pem, key_pem) as strings.
///
/// Note: This requires the `rcgen` crate at call site — provided as a utility
/// in the CLI binary which includes rcgen as an optional dev dependency.
pub fn self_signed_cert_hint() -> &'static str {
    "To generate a self-signed cert for testing, run:\n  \
     openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes\n  \
     or use `nexrade-cache gencert` (requires --features dev)"
}

#[cfg(test)]
mod tests {
    // TLS tests require actual cert files; skip in unit tests.
}
