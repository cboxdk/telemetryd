//! Moving a single node's data directory into ClickHouse.
//!
//! The node is stopped; its data directory is opened as the store it was, write-ahead
//! log replayed, and every record in the range is written to ClickHouse through the
//! same write path an instance in front of it uses — the records themselves, not an
//! OTLP rendering of them, so nothing an OTLP round trip would lose is lost.
//!
//! # Windows by time, never by count
//!
//! A signal is read oldest first in windows of time, inclusive at both ends, the next
//! starting one nanosecond after the last. A window holding more than a batch is
//! halved and read again, down to a single nanosecond, which is then taken whole: no
//! record is left behind at a boundary because records share a timestamp — a scrape
//! writes thousands of samples at one.
//!
//! # Resumable
//!
//! After each window is written, ClickHouse is told in `telemetryd_meta` how far the
//! import of this directory has come, and a run that starts again starts from there.
//! An interruption between a window's insert and that note repeats the window: at
//! most one batch is written twice.

use std::time::Instant;

use serde::Serialize;
use telemetryd_core::{Error, Result, Signal};

use super::ClickHouse;
use super::metric_folds::{read_meta, refold_from, write_meta};
use super::metrics_schema::HOUR_NANOS;
use crate::{Order, Scan, Store};

/// What to import, and from where.
#[derive(Debug, Clone)]
pub struct ImportOptions {
    pub signals: Vec<Signal>,
    /// Unix nanoseconds, inclusive.
    pub start: u64,
    pub end: u64,
    /// Records per insert.
    pub batch: usize,
    /// Names this source in `telemetryd_meta`, so a second run resumes the first.
    pub source: String,
    /// Start from `start` even where an earlier run got further.
    pub restart: bool,
}

/// One step of an import, for a person or a program watching.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ImportEvent {
    /// A signal begins, from `from` (Unix nanoseconds): `resumed` when an earlier run
    /// had got that far.
    Start {
        signal: &'static str,
        from: u64,
        to: u64,
        resumed: bool,
    },
    /// A window was written: `records` in it, `through` the end of what is done.
    Progress {
        signal: &'static str,
        records: u64,
        total: u64,
        through: u64,
        records_per_second: f64,
    },
    /// A signal is done.
    Done {
        signal: &'static str,
        records: u64,
        seconds: f64,
    },
}

/// How many records were written, per signal.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ImportSummary {
    pub logs: u64,
    pub traces: u64,
    pub metrics: u64,
}

/// The windows a scan starts with and may grow to: a minute, and thirty days, so an
/// empty stretch of a sparse store is crossed in a few reads.
const FIRST_WINDOW: u64 = 60_000_000_000;
const WIDEST_WINDOW: u64 = 30 * 24 * HOUR_NANOS;

/// Copy `source`'s records in the range into `destination`.
///
/// # Errors
/// When the source cannot be read or ClickHouse refuses a write; the next run resumes
/// from the last window written.
pub fn import(
    source: &Store,
    destination: &ClickHouse,
    options: &ImportOptions,
    report: &mut dyn FnMut(&ImportEvent),
) -> Result<ImportSummary> {
    let mut summary = ImportSummary::default();
    for signal in &options.signals {
        let written = match signal {
            Signal::Logs => one_signal(destination, options, *signal, report, |scan| {
                let records = source.logs().scan(scan, &[], &|_| true)?;
                Ok(Batch::Logs(records))
            })?,
            Signal::Traces => one_signal(destination, options, *signal, report, |scan| {
                let records = source.traces().scan(scan, &[], &|_| true)?;
                Ok(Batch::Spans(records))
            })?,
            Signal::Metrics => one_signal(destination, options, *signal, report, |scan| {
                let records = source.metrics().scan(scan, &[], &|_| true)?;
                Ok(Batch::Samples(records))
            })?,
        };
        match signal {
            Signal::Logs => summary.logs = written,
            Signal::Traces => summary.traces = written,
            Signal::Metrics => summary.metrics = written,
        }
    }
    Ok(summary)
}

enum Batch {
    Logs(Vec<telemetryd_core::LogRecord>),
    Spans(Vec<telemetryd_core::span::SpanRecord>),
    Samples(Vec<telemetryd_core::metric::MetricSample>),
}

impl Batch {
    fn len(&self) -> usize {
        match self {
            Self::Logs(r) => r.len(),
            Self::Spans(r) => r.len(),
            Self::Samples(r) => r.len(),
        }
    }

    fn write(&self, destination: &ClickHouse, batch: usize) -> Result<()> {
        let batch = batch.max(1);
        match self {
            Self::Logs(records) => records
                .chunks(batch)
                .try_for_each(|chunk| destination.logs.append(chunk)),
            Self::Spans(records) => records
                .chunks(batch)
                .try_for_each(|chunk| destination.spans.append(chunk)),
            Self::Samples(records) => records
                .chunks(batch)
                .try_for_each(|chunk| destination.metrics.append(chunk)),
        }
    }

    /// The earliest sample's hour, for having its summaries made again.
    fn first_hour(&self) -> Option<u64> {
        match self {
            Self::Samples(samples) => samples
                .iter()
                .map(|s| super::metrics_schema::bucket_of(s.timestamp_nanos, HOUR_NANOS))
                .min(),
            _ => None,
        }
    }
}

fn signal_name(signal: Signal) -> &'static str {
    match signal {
        Signal::Logs => "logs",
        Signal::Traces => "traces",
        Signal::Metrics => "metrics",
    }
}

/// The key in `telemetryd_meta` holding how far the import of `source` has come.
fn progress_key(source: &str, signal: Signal) -> String {
    format!("import:{source}:{}", signal_name(signal))
}

fn one_signal(
    destination: &ClickHouse,
    options: &ImportOptions,
    signal: Signal,
    report: &mut dyn FnMut(&ImportEvent),
    read: impl Fn(Scan<'_>) -> Result<Batch>,
) -> Result<u64> {
    let name = signal_name(signal);
    let client = destination.client();
    let key = progress_key(&options.source, signal);
    let done_through = if options.restart {
        None
    } else {
        read_meta(client, &key)?.and_then(|v| v.parse::<u64>().ok())
    };
    let mut cursor = options.start;
    let resumed = done_through.is_some_and(|through| through >= cursor);
    if let Some(through) = done_through.filter(|t| *t >= cursor) {
        cursor = through.saturating_add(1);
    }
    report(&ImportEvent::Start {
        signal: name,
        from: cursor,
        to: options.end,
        resumed,
    });
    let started = Instant::now();
    let cap = options.batch.max(1);
    let mut width = FIRST_WINDOW;
    let mut total = 0u64;
    let mut first_hour: Option<u64> = None;
    while cursor <= options.end {
        let last = cursor.saturating_add(width - 1).min(options.end);
        let scan = |abort_over: usize| Scan {
            abort_over,
            start_nanos: cursor,
            end_nanos: last,
            limit: 0,
            order: Order::Ascending,
            exact_key: None,
            required_text: None,
            body_filters: &[],
            columns: None,
        };
        // A single nanosecond is taken whole, however many records share it.
        let ceiling = if last == cursor { 0 } else { cap };
        let batch = match read(scan(ceiling)) {
            Ok(batch) => batch,
            // More than a batch in the window: half the window, the same start.
            Err(Error::BadRequest(_)) if ceiling != 0 => {
                width = (width / 2).max(1);
                continue;
            }
            Err(error) => return Err(error),
        };
        let records = batch.len();
        batch.write(destination, cap)?;
        if let Some(hour) = batch.first_hour() {
            first_hour = Some(first_hour.map_or(hour, |h| h.min(hour)));
        }
        write_meta(client, &key, &last.to_string())?;
        total += records as u64;
        report(&ImportEvent::Progress {
            signal: name,
            records: records as u64,
            total,
            through: last,
            records_per_second: rate(total, started),
        });
        if last == options.end {
            break;
        }
        cursor = last + 1;
        if records < cap / 4 {
            width = width.saturating_mul(2).min(WIDEST_WINDOW);
        }
    }
    // Samples written into hours already summarised: have them summarised again.
    if let Some(hour) = first_hour {
        refold_from(client, hour)?;
    }
    report(&ImportEvent::Done {
        signal: name,
        records: total,
        seconds: started.elapsed().as_secs_f64(),
    });
    Ok(total)
}

fn rate(records: u64, started: Instant) -> f64 {
    let seconds = started.elapsed().as_secs_f64();
    if seconds > 0.0 {
        #[allow(clippy::cast_precision_loss)]
        let records = records as f64;
        records / seconds
    } else {
        0.0
    }
}

/// The earliest and latest record the source holds for a signal, if any.
///
/// # Errors
/// When the source cannot be read.
pub fn source_bounds(source: &Store, signal: Signal) -> Result<Option<(u64, u64)>> {
    let one = |order: Order| Scan {
        abort_over: 0,
        start_nanos: 0,
        end_nanos: u64::MAX,
        limit: 1,
        order,
        exact_key: None,
        required_text: None,
        body_filters: &[],
        columns: None,
    };
    let bound = |order: Order| -> Result<Option<u64>> {
        Ok(match signal {
            Signal::Logs => source
                .logs()
                .scan(one(order), &[], &|_| true)?
                .first()
                .map(|r| r.timestamp_nanos),
            Signal::Traces => source
                .traces()
                .scan(one(order), &[], &|_| true)?
                .first()
                .map(|r| r.start_nanos),
            Signal::Metrics => source
                .metrics()
                .scan(one(order), &[], &|_| true)?
                .first()
                .map(|r| r.timestamp_nanos),
        })
    };
    Ok(bound(Order::Ascending)?.zip(bound(Order::Descending)?))
}
