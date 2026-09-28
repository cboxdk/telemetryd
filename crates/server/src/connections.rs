//! Serving connections, with the limits `axum::serve` does not set.
//!
//! `axum::serve` builds hyper without a timer, and without one hyper's header-read
//! timeout does nothing: a client could open a connection, send one header byte every
//! twenty seconds and hold it for ever. Nothing capped how many it held, either — a
//! thousand such sockets used up the file descriptors a default unit gets, and then
//! `/healthz` and ingest stopped answering for everyone. The request timeout could not
//! help: it starts once the headers are in, which these never are.
//!
//! So connections are served here, through hyper's HTTP/1 builder — the protocol
//! telemetryd speaks — with a timer, a deadline for the request head, and a ceiling on
//! how many are open at once.
//! At the ceiling the accept loop waits instead of accepting, which leaves the excess in
//! the kernel's backlog rather than in this process.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::sync::{Semaphore, watch};
use tower::ServiceExt;

use crate::tls::Bound;

/// How long a client has to send a complete request head, from the moment the
/// connection is ready for one — including while an idle keep-alive connection waits
/// for the next.
pub(crate) const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Which protocol a listener speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protocol {
    /// Everything on the main port, WebSocket upgrades included.
    Http1,
    /// OTLP/gRPC: HTTP/2 with prior knowledge, or negotiated over TLS.
    Http2,
}

/// How many requests one HTTP/2 connection may have in flight — a gRPC exporter sends
/// a handful, and each one holds an ingest slot while it is stored.
const MAX_CONCURRENT_STREAMS: u32 = 16;

/// How long an HTTP/2 connection may go without starting a request before it is asked
/// to close. HTTP/1's header timeout closes an idle keep-alive connection; HTTP/2 has no
/// such thing, and its keep-alive pings only find peers that have gone. Without this a
/// client holding connections open and sending nothing kept their slots for good.
const HTTP2_IDLE: Duration = Duration::from_secs(60);

/// The connections both listeners may hold between them — one budget, so the gRPC
/// port cannot spend the descriptors the main one and the store need.
pub(crate) fn connection_permits() -> Arc<Semaphore> {
    let limit = connection_limit();
    tracing::debug!(limit, "serving at most this many connections at once");
    Arc::new(Semaphore::new(limit))
}

/// Serve `app` on `listener` until `shutdown` resolves, then let open connections finish
/// their current request and return once every one has closed.
pub(crate) async fn serve(
    listener: Bound,
    app: Router,
    protocol: Protocol,
    permits: Arc<Semaphore>,
    shutdown: impl Future<Output = ()>,
) {
    // Dropping `stop` tells every connection to finish; `done` closes once they all have.
    let (stop, stopped) = watch::channel(());
    let (done, finished) = watch::channel(());

    let mut listener = listener;
    let mut shutdown = pin!(shutdown);
    loop {
        let permit = tokio::select! {
            permit = Arc::clone(&permits).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
            () = &mut shutdown => break,
        };
        let (io, _) = tokio::select! {
            accepted = axum::serve::Listener::accept(&mut listener) => accepted,
            () = &mut shutdown => break,
        };

        // When the connection last started a request, for closing an idle HTTP/2 one.
        let started_at = std::time::Instant::now();
        let last_request = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let touched = Arc::clone(&last_request);
        let service = TowerToHyperService::new(app.clone().map_request(
            move |request: hyper::Request<Incoming>| {
                let at = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                touched.store(at, std::sync::atomic::Ordering::Relaxed);
                request.map(Body::new)
            },
        ));
        let stop = stop.clone();
        let finished = finished.clone();
        tokio::spawn(async move {
            let stopping = stop.closed();
            if protocol == Protocol::Http1 {
                let mut builder = hyper::server::conn::http1::Builder::new();
                builder
                    .timer(TokioTimer::new())
                    .header_read_timeout(HEADER_READ_TIMEOUT);
                // Upgrades for the live tail's WebSocket.
                let connection = builder
                    .serve_connection(TokioIo::new(io), service)
                    .with_upgrades();
                drive(
                    pin!(connection),
                    stopping,
                    hyper::server::conn::http1::UpgradeableConnection::graceful_shutdown,
                )
                .await;
            } else {
                let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
                builder
                    .timer(TokioTimer::new())
                    .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
                    // A peer that stops answering pings is gone; its streams are freed.
                    .keep_alive_interval(Some(Duration::from_secs(30)))
                    .keep_alive_timeout(Duration::from_secs(20));
                let connection = builder.serve_connection(TokioIo::new(io), service);
                let idle = async move {
                    let mut tick = tokio::time::interval(Duration::from_secs(5));
                    loop {
                        tick.tick().await;
                        let last = Duration::from_millis(
                            last_request.load(std::sync::atomic::Ordering::Relaxed),
                        );
                        if started_at.elapsed().saturating_sub(last) >= HTTP2_IDLE {
                            return;
                        }
                    }
                };
                let stopping = async move {
                    tokio::select! {
                        () = stopping => {}
                        () = idle => {}
                    }
                };
                drive(
                    pin!(connection),
                    stopping,
                    hyper::server::conn::http2::Connection::graceful_shutdown,
                )
                .await;
            }
            drop(permit);
            drop(finished);
        });
    }

    drop(stopped);
    drop(finished);
    done.closed().await;
}

/// Run a connection to its end, asking it to finish once `stopping` resolves.
async fn drive<C, E>(
    mut connection: std::pin::Pin<&mut C>,
    stopping: impl Future<Output = ()>,
    graceful: impl Fn(std::pin::Pin<&mut C>),
) where
    C: Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    let mut stopping = pin!(stopping);
    let mut asked = false;
    loop {
        tokio::select! {
            result = connection.as_mut() => {
                if let Err(error) = result {
                    tracing::trace!(%error, "connection ended with an error");
                }
                return;
            }
            () = &mut stopping, if !asked => {
                graceful(connection.as_mut());
                asked = true;
            }
        }
    }
}

/// How many connections may be open at once: half the file descriptors this process
/// may hold, so the other half stays for the write-ahead log, segments and upstream
/// calls. Read from `/proc/self/limits` where there is one.
fn connection_limit() -> usize {
    const FLOOR: usize = 64;
    const CEILING: usize = 16_384;
    const WITHOUT_PROC: usize = 1_024;
    std::fs::read_to_string("/proc/self/limits")
        .ok()
        .and_then(|limits| open_files_soft_limit(&limits))
        .map_or(WITHOUT_PROC, |files| (files / 2).clamp(FLOOR, CEILING))
}

/// The soft limit on the `Max open files` line of `/proc/self/limits`.
fn open_files_soft_limit(limits: &str) -> Option<usize> {
    let line = limits
        .lines()
        .find(|line| line.starts_with("Max open files"))?;
    line.trim_start_matches("Max open files")
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn the_open_file_limit_is_read_from_proc() {
        let limits = "Limit                     Soft Limit           Hard Limit           Units     \n\
                      Max cpu time              unlimited            unlimited            seconds   \n\
                      Max open files            1024                 524288               files     \n";
        assert_eq!(open_files_soft_limit(limits), Some(1024));
        assert_eq!(
            open_files_soft_limit("Max open files  unlimited  unlimited  files"),
            None
        );
        assert_eq!(open_files_soft_limit(""), None);
    }
}
