//! Transport abstraction so `Connection` can run over either a plain TCP
//! socket or a TLS-upgraded one without duplicating the connection loop.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

#[cfg(feature = "tls")]
use tokio_rustls::server::TlsStream;

/// Either a plain TCP stream or a TLS-upgraded one. Implements
/// `AsyncRead`/`AsyncWrite` by delegating to whichever variant is held, so
/// `Connection` can stay written against a single concrete type instead of
/// being generic over the transport.
pub enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<TlsStream<TcpStream>>),
}

impl Stream {
    /// Disable Nagle's algorithm on the underlying TCP socket. No-op
    /// concept for TLS since the handshake already happened on a
    /// `TcpStream` with `set_nodelay` applied before the upgrade — kept
    /// here so callers don't need to match on the variant themselves.
    pub fn set_nodelay(&self, nodelay: bool) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.set_nodelay(nodelay),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.get_ref().0.set_nodelay(nodelay),
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}
