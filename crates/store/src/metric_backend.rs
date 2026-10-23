//! What the PromQL engine needs from a store, whichever store it is.
//!
//! The engine used to take the local [`RecordStore`] by type. With ClickHouse as an
//! alternative it takes this trait, and each backend answers it its own way: the local
//! store from segments on disk, ClickHouse with SQL. The contract is the local store's —
//! the same samples, the same series, the same errors — so an answer does not depend on
//! which one is configured.

use std::ops::ControlFlow;

use telemetryd_core::{LabelMatcher, Labels, MetricSample, Result};

use crate::folds::StreamFold;
use crate::metrics::{MetricSchema, SeriesRun};
use crate::records::{RecordStore, Scan};

/// Every matching series' summary over one window, in label order.
pub type Folded = Vec<(Labels, StreamFold)>;

/// Metric samples, as the Prometheus API and PromQL read them.
pub trait MetricStore: Send + Sync {
    /// Hand `visit` every series matching `matchers` with samples inside `windows` —
    /// sorted, disjoint, inclusive `(start, end)` pairs. `Ok(false)` when the store
    /// cannot read this way and the caller should read the span whole instead.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn scan_series(
        &self,
        windows: &[(u64, u64)],
        matchers: &[LabelMatcher],
        visit: &mut dyn FnMut(SeriesRun<'_>) -> ControlFlow<()>,
    ) -> Result<bool>;

    /// Every matching series' counter summary over `[start, end]`, or `None` when the
    /// store declines and the caller should read the samples.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn fold_window(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
        whole_reads_allowed: usize,
        gauges: bool,
    ) -> Result<Option<Folded>>;

    /// [`Self::fold_window`] for several windows at once, in their order, or `None` when
    /// any one is declined. A store that can answer them together — one read for the
    /// summaries every window draws on — does; the default asks one at a time.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn fold_windows(
        &self,
        windows: &[(u64, u64)],
        matchers: &[LabelMatcher],
        whole_reads_allowed: usize,
        gauges: bool,
    ) -> Result<Option<Vec<Folded>>> {
        let mut out = Vec::with_capacity(windows.len());
        for &(start, end) in windows {
            match self.fold_window(start, end, matchers, whole_reads_allowed, gauges)? {
                Some(folded) => out.push(folded),
                None => return Ok(None),
            }
        }
        Ok(Some(out))
    }

    /// Every matching sample in `[start, end]` that `extra` accepts, refusing more than
    /// `ceiling` (`0`: no ceiling).
    ///
    /// # Errors
    /// Past the ceiling, as [`RecordStore::query_bounded`], or when the store cannot be
    /// read.
    fn query_bounded(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&MetricSample) -> bool + Sync),
        ceiling: usize,
    ) -> Result<Vec<MetricSample>>;

    /// Distinct label names of series with samples in the range.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn label_names(&self, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>>;

    /// Distinct values of one label in the range.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn label_values(&self, name: &str, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>>;

    /// Distinct series in the range that match, in label order.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn streams(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>>;

    /// Samples in the scan's range and order, within its limit, that `extra` accepts:
    /// what an export reads.
    ///
    /// # Errors
    /// As [`RecordStore::scan`].
    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&MetricSample) -> bool + Sync),
    ) -> Result<Vec<MetricSample>>;

    /// The earliest sample held, if any: what a query reaching further back clips to.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn oldest_sample_nanos(&self) -> Result<Option<u64>>;
}

impl MetricStore for RecordStore<MetricSchema> {
    fn scan_series(
        &self,
        windows: &[(u64, u64)],
        matchers: &[LabelMatcher],
        visit: &mut dyn FnMut(SeriesRun<'_>) -> ControlFlow<()>,
    ) -> Result<bool> {
        Self::scan_series(self, windows, matchers, visit)
    }

    fn fold_window(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
        whole_reads_allowed: usize,
        gauges: bool,
    ) -> Result<Option<Folded>> {
        Self::fold_window(
            self,
            start_nanos,
            end_nanos,
            matchers,
            whole_reads_allowed,
            gauges,
        )
    }

    fn query_bounded(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&MetricSample) -> bool + Sync),
        ceiling: usize,
    ) -> Result<Vec<MetricSample>> {
        Self::query_bounded(self, start_nanos, end_nanos, matchers, extra, ceiling)
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

    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&MetricSample) -> bool + Sync),
    ) -> Result<Vec<MetricSample>> {
        Self::scan(self, request, matchers, extra)
    }

    fn oldest_sample_nanos(&self) -> Result<Option<u64>> {
        Ok(self.status().oldest_record_nanos)
    }
}
