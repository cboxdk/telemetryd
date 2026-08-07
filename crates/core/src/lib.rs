//! Shared foundations for telemetryd: configuration, the error taxonomy, and the
//! secret-handling primitives used across every other crate.
//!
//! This crate is deliberately transport-agnostic — it knows nothing about axum or
//! HTTP. The mapping from [`Error`] to an HTTP response lives in `telemetryd-server`.

pub mod config;
pub mod error;
pub mod matcher;
pub mod record;
pub mod secret;
pub mod signal;
pub mod span;

pub use config::Config;
pub use error::{Error, Result};
pub use matcher::{LabelMatcher, MatchOp, matches_all};
pub use record::{APP_LABEL, LEVEL_LABEL, Labels, LogRecord, Severity, UNKNOWN_APP};
pub use secret::{Secret, TokenSet};
pub use signal::Signal;
pub use span::{SpanEvent, SpanKind, SpanRecord, SpanStatus};

/// Crate version, surfaced by `/status`, `telemetryd version` and the `User-Agent`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// On-disk storage format version. Bumped whenever a data directory written by an
/// older build can no longer be read; startup refuses to open a mismatched directory
/// rather than guessing.
pub const STORAGE_FORMAT_VERSION: u32 = 1;

/// Canonical documentation link included in every "unsupported feature" error, so a
/// user hitting a subset boundary always has somewhere to go.
pub const COMPATIBILITY_DOC: &str =
    "https://github.com/cboxdk/telemetryd/blob/main/COMPATIBILITY.md";
