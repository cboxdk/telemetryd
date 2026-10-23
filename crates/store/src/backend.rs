//! What the query engines need from a store, whichever store it is.
//!
//! The engines used to take the local [`RecordStore`] by type. With ClickHouse as an
//! alternative they take these traits instead, and each backend answers them its own
//! way: the local store from segments on disk, ClickHouse with SQL. The contract is the
//! local store's — same records, same order, same limits, same errors — so an answer
//! does not depend on which one is configured.

use telemetryd_core::{LabelMatcher, Labels, LogRecord, Result};

use crate::logs::LogSchema;
use crate::records::{RecordStore, Scan};

/// A record predicate, applied after the store's own selection.
pub type RecordFilter<'a, R> = &'a (dyn Fn(&R) -> bool + Sync);

/// A line filter on the stored body, as the query wrote it: `|= "text"` or
/// `!= "text"`.
///
/// Carried down as data so a backend can turn it into its own condition — the local
/// store has the Arrow prefilter for the same thing, ClickHouse a `LIKE`. Like that
/// prefilter it may select too much, never too little: the record predicate still runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyFilter {
    pub contains: bool,
    pub text: String,
}

/// Logs, as the Loki API and LogQL read them.
pub trait LogStore: Send + Sync {
    /// Records in the scan's range whose stream matches, that `extra` accepts, in the
    /// scan's order and within its limit.
    ///
    /// # Errors
    /// As [`RecordStore::scan`]: a ceiling exceeded, or a store that cannot be read.
    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: RecordFilter<'_, LogRecord>,
    ) -> Result<Vec<LogRecord>>;

    /// Distinct stream label names with data in the range.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn label_names(&self, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>>;

    /// Distinct values of one stream label in the range.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn label_values(&self, name: &str, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>>;

    /// Distinct streams in the range that match.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn streams(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>>;
}

impl LogStore for RecordStore<LogSchema> {
    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: RecordFilter<'_, LogRecord>,
    ) -> Result<Vec<LogRecord>> {
        Self::scan(self, request, matchers, extra)
    }

    fn label_names(&self, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>> {
        Ok(Self::label_names(self, start_nanos, end_nanos))
    }

    fn label_values(&self, name: &str, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>> {
        Self::label_values(self, name, start_nanos, end_nanos)
    }

    fn streams(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>> {
        Self::streams(self, start_nanos, end_nanos, matchers)
    }
}
