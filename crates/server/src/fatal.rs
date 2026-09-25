//! What a panic means depends on what it interrupted.
//!
//! The release build unwinds, so a panic in a request handler becomes a `500` for that
//! request and the server keeps serving everyone else. That is right for queries: they
//! only read, so nothing they leave behind is inconsistent. It was not always so — the
//! build used to abort on any panic, and one malformed query parameter could end the
//! process for every client at once.
//!
//! Work that changes what is stored is different. A panic halfway through a seal can
//! leave records drained from the buffer but not yet published, still in the write-ahead
//! log but invisible to queries; carrying on would serve that state until the next
//! restart and build on it. Stopping is the safe answer, because on the way back up the
//! write-ahead log replays and the store is consistent again. So storage work is joined
//! through [`storage`], which ends the process on a panic and passes everything else on.

use telemetryd_core::Error;
use tokio::task::JoinError;

/// Join storage work, ending the process if it panicked.
///
/// A task that was cancelled rather than panicking — only at runtime shutdown — comes
/// back as an error naming the work, so the caller can report it like any other failure.
pub(crate) fn storage<T>(joined: Result<T, JoinError>, work: &'static str) -> Result<T, Error> {
    match joined {
        Ok(value) => Ok(value),
        Err(e) if e.is_panic() => {
            tracing::error!(
                work,
                "storage work panicked; stopping so the write-ahead log can restore a \
                 consistent store on restart"
            );
            std::process::abort()
        }
        Err(_) => Err(Error::Config(format!(
            "{work} was cancelled before it finished"
        ))),
    }
}
