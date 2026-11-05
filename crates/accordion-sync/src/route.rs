//! How an account's requests reach its server: directly, as at home, or
//! through the account's own tunnel, as away (decision 23).
//!
//! Each new connection tries the way that worked last, and starts the other
//! a moment later if the first hasn't finished, the way browsers try IPv6
//! and IPv4. Whichever completes a TLS handshake for the server's name
//! first wins. A different device at the same address on someone else's
//! network can't pass the certificate check, so the app never sends a
//! request to it. At home the direct way usually wins, and away the tunnel
//! does, with nothing to turn on or off.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::tunnel::{Tunnel, TunnelSetup, TunnelStream};

/// How long the way that worked last gets before the other one starts too.
const HEAD_START: Duration = Duration::from_millis(300);

/// How long one way gets to connect and finish its TLS handshake. A
/// tunnel whose handshake doesn't finish in this time is started again
/// next time, in case home's address changed.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A way to the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Via {
    /// Straight to the server's address, as at home.
    Direct,
    /// Through the account's tunnel, as away from home.
    Tunnel,
}

impl Via {
    fn other(self) -> Self {
        match self {
            Self::Direct => Self::Tunnel,
            Self::Tunnel => Self::Direct,
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            Self::Direct => 1,
            Self::Tunnel => 2,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Direct),
            2 => Some(Self::Tunnel),
            _ => None,
        }
    }
}

/// What one account's connections need.
pub(crate) struct Route {
    host: String,
    port: u16,
    /// For HTTPS. Plain HTTP is only for a development server on this
    /// computer.
    tls: Option<TlsConnector>,
    tunnel: Option<TunnelSetup>,
    running: tokio::sync::Mutex<Option<Arc<Tunnel>>>,
    /// The way that worked last.
    last: AtomicU8,
    /// Only this way, for tests and for trying the tunnel at home.
    only: Option<Via>,
}

impl Route {
    pub(crate) fn new(
        uri: &Uri,
        tls: Option<Arc<ClientConfig>>,
        tunnel: Option<TunnelSetup>,
        only: Option<Via>,
    ) -> Self {
        let https = uri.scheme_str() == Some("https");
        Self {
            host: uri
                .host()
                .unwrap_or_default()
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned(),
            port: uri.port_u16().unwrap_or(if https { 443 } else { 80 }),
            tls: tls.filter(|_| https).map(TlsConnector::from),
            tunnel,
            running: tokio::sync::Mutex::new(None),
            last: AtomicU8::new(0),
            only,
        }
    }

    /// The way that worked last, if any has.
    pub(crate) fn last(&self) -> Option<Via> {
        Via::from_u8(self.last.load(Ordering::Relaxed))
    }

    /// Whether the account has a tunnel to try.
    pub(crate) fn has_tunnel(&self) -> bool {
        self.tunnel.is_some()
    }

    async fn connect(&self) -> io::Result<Stream> {
        let ways: Vec<Via> = match (self.only, &self.tunnel) {
            (Some(only), _) => vec![only],
            (None, None) => vec![Via::Direct],
            (None, Some(_)) => {
                let first = self.last().unwrap_or(Via::Direct);
                vec![first, first.other()]
            }
        };
        let first = self.attempt(ways[0]);
        let Some(&second_way) = ways.get(1) else {
            return first.await;
        };
        let second = async {
            tokio::time::sleep(HEAD_START).await;
            self.attempt(second_way).await
        };
        let mut first = std::pin::pin!(first);
        let mut second = std::pin::pin!(second);
        // The first to connect wins. If one fails, the other carries on,
        // without waiting out the head start.
        tokio::select! {
            result = &mut first => match result {
                Ok(stream) => Ok(stream),
                Err(first_err) => self.attempt(second_way).await.map_err(|_| first_err),
            },
            result = &mut second => match result {
                Ok(stream) => Ok(stream),
                Err(_) => first.await,
            },
        }
    }

    /// Connects one way, and finishes the TLS handshake.
    async fn attempt(&self, via: Via) -> io::Result<Stream> {
        let attempt = async {
            match via {
                Via::Direct => {
                    let tcp = TcpStream::connect((self.host.as_str(), self.port)).await?;
                    tcp.set_nodelay(true)?;
                    self.secure(Inner::Tcp(tcp)).await
                }
                Via::Tunnel => {
                    let tunnel = self.tunnel().await?;
                    let stream = tunnel.connect(self.port).await?;
                    self.secure(Inner::Tunnel(stream)).await
                }
            }
        };
        let result = tokio::time::timeout(CONNECT_TIMEOUT, attempt)
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("{via:?} didn't answer"),
                ))
            });
        match &result {
            Ok(_) => self.last.store(via.to_u8(), Ordering::Relaxed),
            // Start the tunnel again next time, in case home moved.
            Err(_) if via == Via::Tunnel => *self.running.lock().await = None,
            Err(_) => {}
        }
        result
    }

    /// The account's tunnel, started the first time it's needed.
    async fn tunnel(&self) -> io::Result<Arc<Tunnel>> {
        let Some(setup) = &self.tunnel else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "this account has no tunnel",
            ));
        };
        let mut running = self.running.lock().await;
        if let Some(tunnel) = &*running {
            return Ok(Arc::clone(tunnel));
        }
        let tunnel = Arc::new(Tunnel::start(setup).await?);
        *running = Some(Arc::clone(&tunnel));
        Ok(tunnel)
    }

    async fn secure(&self, inner: Inner) -> io::Result<Stream> {
        let Some(tls) = &self.tls else {
            return Ok(Stream::Plain(inner));
        };
        let name = ServerName::try_from(self.host.clone())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        Ok(Stream::Tls(Box::new(tls.connect(name, inner).await?)))
    }
}

/// The connector the HTTP client opens connections with.
#[derive(Clone)]
pub(crate) struct Connector(pub(crate) Arc<Route>);

impl tower_service::Service<Uri> for Connector {
    type Response = TokioIo<Stream>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<TokioIo<Stream>>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Self::Future {
        // Every request goes to the account's own server, so the route
        // already knows where.
        let route = Arc::clone(&self.0);
        Box::pin(async move { route.connect().await.map(TokioIo::new) })
    }
}

/// A connection, before TLS.
pub(crate) enum Inner {
    Tcp(TcpStream),
    Tunnel(TunnelStream),
}

/// A connection to the server, either way.
pub(crate) enum Stream {
    Plain(Inner),
    Tls(Box<TlsStream<Inner>>),
}

impl Connection for Stream {
    fn connected(&self) -> Connected {
        match self {
            Self::Tls(tls) if tls.get_ref().1.alpn_protocol() == Some(b"h2") => {
                Connected::new().negotiated_h2()
            }
            _ => Connected::new(),
        }
    }
}

macro_rules! each {
    ($self:ident, $inner:ident => $body:expr) => {
        match $self.get_mut() {
            Inner::Tcp($inner) => {
                let $inner = Pin::new($inner);
                $body
            }
            Inner::Tunnel($inner) => {
                let $inner = Pin::new($inner);
                $body
            }
        }
    };
}

impl AsyncRead for Inner {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        each!(self, inner => inner.poll_read(cx, buf))
    }
}

impl AsyncWrite for Inner {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        each!(self, inner => inner.poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        each!(self, inner => inner.poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        each!(self, inner => inner.poll_shutdown(cx))
    }
}

macro_rules! either {
    ($self:ident, $inner:ident => $body:expr) => {
        match $self.get_mut() {
            Stream::Plain($inner) => {
                let $inner = Pin::new($inner);
                $body
            }
            Stream::Tls($inner) => {
                let $inner = Pin::new($inner.as_mut());
                $body
            }
        }
    };
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        either!(self, inner => inner.poll_read(cx, buf))
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        either!(self, inner => inner.poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        either!(self, inner => inner.poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        either!(self, inner => inner.poll_shutdown(cx))
    }
}
