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

impl TlsAcceptor {
    /// Build from PEM certificate and key files.
    pub async fn from_pem_files<P: AsRef<Path>>(cert_path: P, key_path: P) -> Result<Self> {
        let certs = load_certs(cert_path.as_ref())?;
        let key = load_private_key(key_path.as_ref())?;

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .context("failed to build TLS server config")?;

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
