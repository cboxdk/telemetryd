//! The HTTP interface, over the same blocking client the relay uses.
//!
//! HTTP rather than the native protocol: it needs no new dependency, goes through any
//! proxy or load balancer in front of a cluster, and the trust roots it verifies against
//! are the ones every other outbound connection here already uses.

use std::io::{BufRead, BufReader};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;

use telemetryd_core::config::ClickHouseConfig;
use telemetryd_core::{Error, Result};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a readiness probe waits for ClickHouse before calling it unreachable.
const PING_TIMEOUT: Duration = Duration::from_secs(2);

/// What a request was for, as counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Insert,
    Query,
    Schema,
}

impl Operation {
    pub const ALL: [Self; 3] = [Self::Insert, Self::Query, Self::Schema];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Query => "query",
            Self::Schema => "schema",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Every request this process made to ClickHouse, shared by every clone of a client.
#[derive(Debug, Default)]
pub struct Stats {
    succeeded: [AtomicU64; 3],
    failed: [AtomicU64; 3],
    nanos: [AtomicU64; 3],
    bytes_sent: AtomicU64,
    rows_written: AtomicU64,
    last_error: Mutex<Option<(std::time::SystemTime, String)>>,
}

/// A copy of [`Stats`] at one moment, for `/status` and `/metrics`.
#[derive(Debug, Clone, Serialize)]
pub struct StatsSnapshot {
    pub url: String,
    pub database: String,
    /// Per operation: `(operation, succeeded, failed, seconds)`.
    pub requests: Vec<(&'static str, u64, u64, f64)>,
    pub bytes_sent: u64,
    pub rows_written: u64,
    /// The most recent failure, as Unix seconds and what ClickHouse or the connection
    /// said. Kept after recovery: a write that failed an hour ago is still worth knowing
    /// about.
    pub last_error: Option<(u64, String)>,
}

impl Stats {
    fn record(&self, operation: Operation, started: Instant, outcome: &Result<()>) {
        let i = operation.index();
        let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.nanos[i].fetch_add(nanos, Ordering::Relaxed);
        match outcome {
            Ok(()) => {
                self.succeeded[i].fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                self.failed[i].fetch_add(1, Ordering::Relaxed);
                *self
                    .last_error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((std::time::SystemTime::now(), error.to_string()));
            }
        }
    }

    pub fn count_rows(&self, rows: usize) {
        self.rows_written.fetch_add(rows as u64, Ordering::Relaxed);
    }
}

/// A connection to one ClickHouse endpoint, as configured.
#[derive(Clone)]
pub struct Client {
    url: String,
    shown: String,
    database: String,
    user: String,
    password: String,
    timeout: Duration,
    stats: Arc<Stats>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("url", &self.shown)
            .field("database", &self.database)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// # Errors
    /// When the password names a secret that cannot be read.
    pub fn new(config: &ClickHouseConfig) -> Result<Self> {
        let url = config.url.trim().trim_end_matches('/').to_owned();
        Ok(Self {
            shown: telemetryd_core::http::redact_url(&url),
            url: format!("{url}/"),
            database: config.database.clone(),
            user: config.user.clone(),
            password: if config.password.is_empty() {
                String::new()
            } else {
                config.password.resolve()?
            },
            timeout: config.timeout,
            stats: Arc::new(Stats::default()),
        })
    }

    #[must_use]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        let stats = &self.stats;
        #[allow(clippy::cast_precision_loss)]
        let requests = Operation::ALL
            .iter()
            .map(|op| {
                let i = op.index();
                (
                    op.as_str(),
                    stats.succeeded[i].load(Ordering::Relaxed),
                    stats.failed[i].load(Ordering::Relaxed),
                    stats.nanos[i].load(Ordering::Relaxed) as f64 / 1e9,
                )
            })
            .collect();
        let last_error = stats
            .last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .map(|(at, message)| {
                let secs = at
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs());
                (secs, message)
            });
        StatsSnapshot {
            url: self.shown.clone(),
            database: self.database.clone(),
            requests,
            bytes_sent: stats.bytes_sent.load(Ordering::Relaxed),
            rows_written: stats.rows_written.load(Ordering::Relaxed),
            last_error,
        }
    }

    /// Whether ClickHouse answers at all, within [`PING_TIMEOUT`]: what readiness asks.
    ///
    /// # Errors
    /// [`Error::Unavailable`], naming what went wrong.
    pub fn ping(&self) -> Result<()> {
        let mut response = ureq::get(&format!("{}ping", self.url))
            .config()
            .tls_config(telemetryd_core::http::tls())
            .timeout_global(Some(PING_TIMEOUT))
            .http_status_as_error(false)
            .max_redirects(0)
            .build()
            .call()
            .map_err(|e| unavailable(&self.shown, "ping", &e.to_string()))?;
        if response.status().is_success() {
            return Ok(());
        }
        let body = response.body_mut().read_to_string().unwrap_or_default();
        Err(unavailable(
            &self.shown,
            "ping",
            &format!("HTTP {}: {}", response.status().as_u16(), body.trim()),
        ))
    }

    /// The same connection, naming another database.
    #[must_use]
    pub fn in_database(&self, database: &str) -> Self {
        Self {
            database: database.to_owned(),
            ..self.clone()
        }
    }

    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Run a statement that returns nothing worth reading: DDL, mostly.
    ///
    /// # Errors
    /// [`Error::Unavailable`] when ClickHouse cannot be reached or refuses it.
    pub fn execute(&self, sql: &str) -> Result<()> {
        self.execute_with(sql, &[])
    }

    /// [`Self::execute`], with settings for this statement.
    ///
    /// # Errors
    /// As [`Self::execute`].
    pub fn execute_with(&self, sql: &str, settings: &[(&str, &str)]) -> Result<()> {
        let started = Instant::now();
        let outcome = self.post(settings, sql.as_bytes(), sql).map(|_| ());
        self.stats.record(Operation::Schema, started, &outcome);
        outcome
    }

    /// Insert rows already encoded as `JSONEachRow`, acknowledged once ClickHouse holds
    /// them.
    ///
    /// `async_insert` lets many small batches from many instances become one part on the
    /// server instead of one each — the difference between a table that keeps up with a
    /// fleet and one that refuses inserts for having too many parts. Waiting for it is
    /// what keeps the acknowledgement honest: the sender is told "stored" only once a
    /// flush has put the rows in the table.
    ///
    /// # Errors
    /// [`Error::Unavailable`], which a sender retries.
    pub fn insert(&self, table: &str, rows: &[u8]) -> Result<()> {
        let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let started = Instant::now();
        self.stats
            .bytes_sent
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        let outcome = self
            .post(
                &[
                    ("query", sql.as_str()),
                    ("async_insert", "1"),
                    ("wait_for_async_insert", "1"),
                    ("input_format_skip_unknown_fields", "0"),
                ],
                rows,
                &sql,
            )
            .map(|_| ());
        self.stats.record(Operation::Insert, started, &outcome);
        outcome
    }

    /// [`Self::insert`] without `async_insert`: for a batch telemetryd assembled itself,
    /// large and one at a time, which a server-side buffer would only copy.
    ///
    /// # Errors
    /// [`Error::Unavailable`].
    pub fn insert_bulk(&self, table: &str, rows: &[u8]) -> Result<()> {
        let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let started = Instant::now();
        self.stats
            .bytes_sent
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        let outcome = self
            .post(&[("query", sql.as_str())], rows, &sql)
            .map(|_| ());
        self.stats.record(Operation::Insert, started, &outcome);
        outcome
    }

    /// Run a query and hand each `JSONEachRow` line to `each` as it arrives, so a large
    /// answer is never held whole. `each` may stop the read early.
    ///
    /// # Errors
    /// [`Error::Unavailable`] for a transport or server failure, and whatever `each`
    /// returns.
    pub fn query_lines(
        &self,
        sql: &str,
        mut each: impl FnMut(&[u8]) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let started = Instant::now();
        let outcome = self.read_lines(sql, &mut each);
        self.stats.record(Operation::Query, started, &outcome);
        outcome
    }

    /// Run a query in `RowBinary` and hand each fixed-width row to `each` as it
    /// arrives: for the samples a PromQL query reads, which are many and small, and
    /// which JSON would spend most of its time spelling out.
    ///
    /// A response that does not end on a row boundary is an error: ClickHouse reports a
    /// failure that happens after it started answering by appending its message to the
    /// rows, and this is how that is noticed rather than read as data.
    ///
    /// # Errors
    /// [`Error::Unavailable`] for a transport or server failure, and whatever `each`
    /// returns.
    pub fn query_rows(
        &self,
        sql: &str,
        width: usize,
        mut each: impl FnMut(&[u8]) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let started = Instant::now();
        let outcome = (|| {
            let body = format!("{sql}\nFORMAT RowBinary");
            let response = self.post(&[("max_query_size", "67108864")], body.as_bytes(), sql)?;
            let mut reader = BufReader::with_capacity(1 << 20, response.into_body().into_reader());
            let mut row = vec![0u8; width];
            loop {
                let mut filled = 0;
                while filled < width {
                    let read = std::io::Read::read(&mut reader, &mut row[filled..])
                        .map_err(|e| unavailable(&self.shown, sql, &e.to_string()))?;
                    if read == 0 {
                        break;
                    }
                    filled += read;
                }
                if filled == 0 {
                    return Ok(());
                }
                if filled < width {
                    let tail = String::from_utf8_lossy(&row[..filled]).into_owned();
                    return Err(unavailable(
                        &self.shown,
                        sql,
                        &format!("the answer ended partway through a row: {tail}"),
                    ));
                }
                if each(&row)?.is_break() {
                    return Ok(());
                }
            }
        })();
        self.stats.record(Operation::Query, started, &outcome);
        outcome
    }

    fn read_lines(
        &self,
        sql: &str,
        each: &mut impl FnMut(&[u8]) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let body = format!("{sql}\nFORMAT JSONEachRow");
        let response = self.post(
            &[
                ("output_format_json_quote_64bit_integers", "0"),
                ("output_format_json_quote_64bit_floats", "0"),
                // A query naming every series it reads is long.
                ("max_query_size", "67108864"),
            ],
            body.as_bytes(),
            sql,
        )?;
        let mut reader = BufReader::new(response.into_body().into_reader());
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader
                .read_until(b'\n', &mut line)
                .map_err(|e| unavailable(&self.shown, sql, &e.to_string()))?;
            if read == 0 {
                return Ok(());
            }
            let row = line.strip_suffix(b"\n").unwrap_or(&line);
            if row.is_empty() {
                continue;
            }
            if each(row)?.is_break() {
                return Ok(());
            }
        }
    }

    fn post(
        &self,
        params: &[(&str, &str)],
        body: &[u8],
        what: &str,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        let mut request = ureq::post(&self.url)
            .config()
            .tls_config(telemetryd_core::http::tls())
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_global(Some(self.timeout))
            .http_status_as_error(false)
            .max_redirects(0)
            .build()
            .query("database", &self.database)
            .header("x-clickhouse-user", &self.user);
        if !self.password.is_empty() {
            request = request.header("x-clickhouse-key", &self.password);
        }
        for (name, value) in params {
            request = request.query(*name, *value);
        }
        let mut response = request
            .send(body)
            .map_err(|e| unavailable(&self.shown, what, &e.to_string()))?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let detail = response
            .body_mut()
            .read_to_string()
            .unwrap_or_default()
            .trim()
            .to_owned();
        Err(unavailable(
            &self.shown,
            what,
            &format!("HTTP {status}: {detail}"),
        ))
    }
}

fn unavailable(shown: &str, what: &str, detail: &str) -> Error {
    let what: String = what.chars().take(120).collect();
    Error::Unavailable(format!("ClickHouse at {shown} failed ({what}): {detail}"))
}
