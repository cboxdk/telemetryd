//! Terminating TLS ourselves, for the deployments that have nowhere to put a proxy.
//!
//! [ADR-004](../../docs/adr/0004-auth-and-network-binding.md) said to terminate at a
//! reverse proxy, and for a public edge that is still the better answer: anything
//! internet-facing already has an ingress holding certificates for several services.
//! What that reasoning missed is the *internal* case — a relay on a private network, a
//! container talking to another container — where there is no proxy, nobody is going to
//! add one, and the traffic carries bearer tokens and every log line in clear.
//!
//! # Bring a certificate; generating one is not the same thing
//!
//! `server.tls.cert_file` and `key_file` take a certificate you already have: from your
//! internal CA, from certbot, from whatever issues certificates in your environment.
//! That is the configuration that actually works, because the clients — OTLP SDKs —
//! verify what they are given.
//!
//! A self-signed certificate the clients do not trust gives encryption without
//! authentication. It stops passive capture and not an active attacker, which is the
//! threat that motivates encrypting an internal network in the first place; and in
//! practice it ends with someone setting `insecure_skip_verify` on every SDK, which
//! looks secure and is not. So telemetryd will generate one only when asked explicitly,
//! and says what it is worth when it does.
//!
//! The other half of this was built first: `tls.ca_file` is how telemetryd *trusts* a
//! private authority when it dials out. Same CA, both ends.
//!
//! # Why the handshake does not happen in `accept`
//!
//! `axum::serve::Listener::accept` returns a ready connection and cannot fail, so the
//! obvious implementation performs the handshake inline. That would serialise every
//! handshake behind the accept loop: one client that opens a connection and then says
//! nothing stalls every other client, with no request ever reaching a timeout layer
//! because no request exists yet. On something deliberately internet-facing that is a
//! denial of service with a single socket.
//!
//! So handshakes run in their own tasks, bounded by a timeout, and only completed
//! connections reach `accept`.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use telemetryd_core::{Error, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// How long a client has to complete a handshake before its task is dropped.
///
/// Generous enough for a slow mobile link, short enough that a client which connects
/// and then goes silent cannot hold a task indefinitely.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How many completed connections may wait to be accepted.
///
/// Backpressure rather than an unbounded queue: if the server cannot keep up, the
/// handshake tasks block here instead of the process accumulating connections it has
/// not begun to serve.
const READY_BACKLOG: usize = 64;

/// How many handshakes may be in flight at once.
///
/// Without this, every accepted TCP connection spawns a task, and the handshake timeout
/// bounds how *long* each one lives rather than how *many* there are — so an attacker
/// opening connections faster than they complete gets one task, one rustls state
/// machine and its buffers, per socket. That is a cheap denial of service against
/// something this ADR now deliberately points at the internet.
///
/// Saturation blocks the accept loop instead, which pushes the queue back into the
/// kernel where it belongs and is bounded by the listen backlog. The number is high
/// enough that ordinary bursts never touch it: handshakes take milliseconds, so 256 in
/// flight is thousands per second sustained.
const CONCURRENT_HANDSHAKES: usize = 256;

/// Build a rustls server configuration from a PEM certificate chain and key.
///
/// The provider is named explicitly rather than taken from the process default. A
/// default provider is installed by whichever crate gets there first, and "whichever
/// crate gets there first" is not a property to hang a TLS configuration on.
pub fn server_config(cert_file: &str, key_file: &str) -> Result<rustls::ServerConfig> {
    let cert_pem = std::fs::read(cert_file)
        .map_err(|e| Error::io(format!("reading server.tls.cert_file at {cert_file}"), e))?;
    let key_pem = std::fs::read(key_file)
        .map_err(|e| Error::io(format!("reading server.tls.key_file at {key_file}"), e))?;

    let chain: Vec<rustls::pki_types::CertificateDer<'static>> = ureq::tls::parse_pem(&cert_pem)
        .filter_map(|item| match item {
            Ok(ureq::tls::PemItem::Certificate(cert)) => {
                Some(rustls::pki_types::CertificateDer::from(cert.der().to_vec()))
            }
            _ => None,
        })
        .collect();
    if chain.is_empty() {
        return Err(Error::Config(format!(
            "{cert_file} contains no certificates. server.tls.cert_file must be a PEM \
             certificate chain, leaf first."
        )));
    }

    let key = ureq::tls::PrivateKey::from_pem(&key_pem).map_err(|e| {
        Error::Config(format!(
            "{key_file} contains no usable private key: {e}. It must be a PEM key, \
             unencrypted — telemetryd cannot prompt for a passphrase at startup."
        ))
    })?;
    // rustls needs to know which of the three encodings the DER is in, and ureq parses
    // that but does not export the type naming it. The PEM label carries the same fact
    // and is part of the format rather than of a crate's API, so read it directly.
    let der = key.der().to_vec();
    let text = String::from_utf8_lossy(&key_pem);
    let key = if text.contains("BEGIN RSA PRIVATE KEY") {
        rustls::pki_types::PrivateKeyDer::Pkcs1(der.into())
    } else if text.contains("BEGIN EC PRIVATE KEY") {
        rustls::pki_types::PrivateKeyDer::Sec1(der.into())
    } else {
        // `-----BEGIN PRIVATE KEY-----`, which is PKCS#8 and what almost everything
        // emits today. Also the right thing to attempt for a label we do not know:
        // rustls rejects a wrong guess with a clear error rather than misbehaving.
        rustls::pki_types::PrivateKeyDer::Pkcs8(der.into())
    };

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Config(format!("selecting TLS protocol versions: {e}")))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| {
            Error::Config(format!(
                "the certificate and key in {cert_file} and {key_file} do not form a \
                 usable pair: {e}"
            ))
        })
}

/// Generate a certificate for `names` if one is not already stored, and return the
/// paths to it.
///
/// Kept beside the data rather than in a temporary directory, so a restart does not
/// hand clients a different certificate every time — which, for anything pinning or
/// caching, looks exactly like an attack.
///
/// # What this is worth
///
/// Encryption, and not authentication. A client has no way to know the certificate is
/// ours, so it has to be told to skip verification — and that instruction outlives this
/// certificate, leaving the deployment open to an active attacker even after a real one
/// is installed. Against passive capture, which is a real and common threat, it is a
/// genuine improvement over plain HTTP. It is not a substitute for an authority the
/// clients trust, and the log line said on generation says so.
pub fn ensure_self_signed(dir: &std::path::Path, names: &[String]) -> Result<(PathBuf, PathBuf)> {
    let cert_path = dir.join("self-signed.pem");
    let key_path = dir.join("self-signed.key");
    if cert_path.is_file() && key_path.is_file() {
        return Ok((cert_path, key_path));
    }

    // rcgen defaults to 1975–4096, which is not a validity window so much as its
    // absence: it makes a leaked key valid forever and trips clients that sanity-check
    // the range. A decade is long enough that a certificate managed by hand does not
    // become an annual outage, and bounded enough to be a real statement.
    let mut params = rcgen::CertificateParams::new(names.to_vec())
        .map_err(|e| Error::Config(format!("preparing a self-signed certificate: {e}")))?;
    let now = time::OffsetDateTime::now_utc();
    // An hour back, because a client whose clock is a little behind ours would
    // otherwise reject a certificate generated seconds ago.
    params.not_before = now - time::Duration::hours(1);
    params.not_after = now + time::Duration::days(3653);

    let signing_key = rcgen::KeyPair::generate()
        .map_err(|e| Error::Config(format!("generating a key pair: {e}")))?;
    let certificate = params
        .self_signed(&signing_key)
        .map_err(|e| Error::Config(format!("generating a self-signed certificate: {e}")))?;

    std::fs::create_dir_all(dir)
        .map_err(|e| Error::io(format!("creating {}", dir.display()), e))?;
    std::fs::write(&cert_path, certificate.pem())
        .map_err(|e| Error::io(format!("writing {}", cert_path.display()), e))?;
    // The key is written before anything serves with it, and only the owner may read
    // it. A private key at 0644 next to the data directory is the kind of thing nobody
    // notices until it matters.
    std::fs::write(&key_path, signing_key.serialize_pem())
        .map_err(|e| Error::io(format!("writing {}", key_path.display()), e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| Error::io(format!("securing {}", key_path.display()), e))?;
    }

    tracing::warn!(
        names = names.join(", "),
        certificate = %cert_path.display(),
        "generated a self-signed certificate. It encrypts the connection but cannot \
         prove this server's identity, so clients must be told to skip verification — \
         and that setting will outlive this certificate. Use one from an authority your \
         clients already trust wherever you can."
    );
    Ok((cert_path, key_path))
}

/// A listener that hands `axum::serve` connections which have already handshaken.
///
/// `Debug` is written by hand throughout this module: the fields are sockets and
/// channels with nothing readable in them, and the bound address is the only thing
/// worth printing.
pub struct TlsListener {
    ready: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    local: SocketAddr,
}

impl TlsListener {
    /// Bind and start accepting. The returned listener yields only established
    /// TLS connections.
    pub async fn bind(addr: SocketAddr, config: rustls::ServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Error::io(format!("binding {addr}"), e))?;
        let local = listener.local_addr().unwrap_or(addr);

        let acceptor = TlsAcceptor::from(Arc::new(config));
        let (tx, ready) = mpsc::channel(READY_BACKLOG);
        let handshakes = Arc::new(tokio::sync::Semaphore::new(CONCURRENT_HANDSHAKES));

        tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    // Per-connection errors are transient — a client that vanished
                    // between the SYN and the accept, or a momentary descriptor
                    // shortage. Logging and continuing is what the trait asks for;
                    // returning would silently stop serving.
                    Err(error) => {
                        tracing::debug!(%error, "accepting a TCP connection failed");
                        continue;
                    }
                };

                // Acquired *before* spawning, so saturation stops us accepting rather
                // than letting the task count grow. `acquire_owned` cannot fail here:
                // the semaphore lives as long as this loop and is never closed.
                let Ok(permit) = Arc::clone(&handshakes).acquire_owned().await else {
                    return;
                };

                let acceptor = acceptor.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    // Held for the whole handshake; dropped with the task.
                    let _permit = permit;
                    let handshake =
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream));
                    match handshake.await {
                        Ok(Ok(stream)) => {
                            // A send error means the server is shutting down and the
                            // receiver is gone; dropping the connection is correct.
                            let _ = tx.send((stream, peer)).await;
                        }
                        // Debug rather than warn: a failed handshake is ordinary on a
                        // public address — health checkers, port scanners, and clients
                        // that do not trust our certificate all land here, and at warn
                        // they would bury everything else.
                        Ok(Err(error)) => {
                            tracing::debug!(%peer, %error, "TLS handshake failed");
                        }
                        Err(_) => {
                            tracing::debug!(
                                %peer,
                                seconds = HANDSHAKE_TIMEOUT.as_secs(),
                                "TLS handshake timed out"
                            );
                        }
                    }
                });
            }
        });

        Ok(Self { ready, local })
    }

    /// The address actually bound, which differs from the requested one when the
    /// configuration asked for port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            if let Some(connection) = self.ready.recv().await {
                return connection;
            }
            // The channel closes only if the accept task died, which it does not do on
            // its own. Never returning is the honest answer — `accept` cannot report an
            // error, and pretending to have a connection would be worse.
            std::future::pending::<()>().await;
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local)
    }
}

/// One listener type for both deployments.
///
/// `axum::serve` is generic over its listener, so a plain branch would give the two
/// paths different concrete types and force the graceful-shutdown logic to be written
/// twice. Written twice is written differently, eventually — and the half that drifts
/// is the one that flushes the write-ahead log on the way out.
pub enum Bound {
    Plain(TcpListener),
    // Boxed because the TLS variant is much the larger of the two, and every accepted
    // connection would otherwise carry the difference.
    Tls(Box<TlsListener>),
}

impl Bound {
    /// The address actually bound. `None` only if the OS refuses to report it.
    #[must_use]
    pub fn local_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Plain(listener) => listener.local_addr().ok(),
            Self::Tls(listener) => Some(listener.local_addr()),
        }
    }
}

/// Either kind of accepted connection.
///
/// Both variants are `Unpin`, which is what lets the delegation below be a plain match
/// rather than a pin projection.
pub enum Connection {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl tokio::io::AsyncRead for Connection {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for Connection {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

impl axum::serve::Listener for Bound {
    type Io = Connection;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self {
            // Delegated to axum's own implementation for `TcpListener` rather than
            // reimplemented: it already does the log-and-retry the trait requires, and
            // a second copy would be a second thing to get wrong.
            Self::Plain(listener) => {
                let (stream, peer) = axum::serve::Listener::accept(listener).await;
                (Connection::Plain(stream), peer)
            }
            Self::Tls(listener) => {
                let (stream, peer) = axum::serve::Listener::accept(listener.as_mut()).await;
                (Connection::Tls(Box::new(stream)), peer)
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        match self {
            Self::Plain(listener) => listener.local_addr(),
            Self::Tls(listener) => Ok(listener.local_addr()),
        }
    }
}

impl std::fmt::Debug for TlsListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsListener")
            .field("local", &self.local)
            // The receiver is a channel of live sockets. `finish_non_exhaustive`
            // rather than printing it: there is nothing readable in there, and the
            // lint that asks for every field is asking for the wrong thing here.
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Bound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("Bound::Plain"),
            Self::Tls(_) => f.write_str("Bound::Tls"),
        }
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("Connection::Plain"),
            Self::Tls(_) => f.write_str("Connection::Tls"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The two failures an operator actually hits, and both must name the file.
    #[test]
    fn a_missing_or_empty_certificate_is_refused_by_name() {
        let dir = std::env::temp_dir().join("telemetryd-tls-test");
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.pem");
        std::fs::write(&empty, b"not a certificate\n").unwrap();
        let path = empty.to_str().unwrap();

        let error = server_config("/nonexistent/cert.pem", path).unwrap_err();
        assert!(
            error.to_string().contains("cert_file"),
            "the error must name the setting: {error}"
        );

        let error = server_config(path, path).unwrap_err();
        assert!(
            error.to_string().contains("no certificates"),
            "an empty chain must say so: {error}"
        );
        let _ = std::fs::remove_file(&empty);
    }
}
