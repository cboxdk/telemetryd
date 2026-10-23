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
    /// Per operation.
    pub requests: Vec<RequestStats>,
    pub bytes_sent: u64,
    pub rows_written: u64,
    /// The most recent failure. Kept after recovery: a write that failed an hour ago
    /// is still worth knowing about.
    pub last_error: Option<LastError>,
}

/// The requests of one kind this process made to ClickHouse.
#[derive(Debug, Clone, Serialize)]
pub struct RequestStats {
    /// `insert`, `query` or `schema`.
    pub operation: &'static str,
    pub succeeded: u64,
    pub failed: u64,
    /// Time spent waiting on them, all together.
    pub seconds: f64,
}

/// The most recent failed request to ClickHouse.
#[derive(Debug, Clone, Serialize)]
pub struct LastError {
    /// Unix seconds.
    pub at: u64,
    /// What ClickHouse or the connection said.
    pub message: String,
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
            // What went wrong is kept where it happened, in full; see `Client::failed`.
            Err(_) => {
                self.failed[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn count_rows(&self, rows: usize) {
        self.rows_written.fetch_add(rows as u64, Ordering::Relaxed);
    }
}

/// How long a readiness answer is reused: a probe every second from every load
/// balancer should not be a request to ClickHouse each.
const PING_REUSE: Duration = Duration::from_secs(1);

/// A readiness answer and when it was had.
type Readiness = (Instant, std::result::Result<(), String>);

/// A connection to one ClickHouse endpoint, as configured.
///
/// One `ureq` agent behind every clone, so connections — and their TLS sessions — are
/// kept and reused across requests rather than made for each.
#[derive(Clone)]
pub struct Client {
    agent: ureq::Agent,
    url: String,
    shown: String,
    database: String,
    user: String,
    password: String,
    timeout: Duration,
    stats: Arc<Stats>,
    /// The last readiness answer and when it was had.
    readiness: Arc<Mutex<Option<Readiness>>>,
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
        // No redirects: one to `http://` would send the password in cleartext, and one
        // to another host would send it somewhere nobody configured.
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .tls_config(telemetryd_core::http::tls())
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .http_status_as_error(false)
            .max_redirects(0)
            .build()
            .into();
        Ok(Self {
            agent,
            readiness: Arc::new(Mutex::new(None)),
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
                RequestStats {
                    operation: op.as_str(),
                    succeeded: stats.succeeded[i].load(Ordering::Relaxed),
                    failed: stats.failed[i].load(Ordering::Relaxed),
                    seconds: stats.nanos[i].load(Ordering::Relaxed) as f64 / 1e9,
                }
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
                LastError { at: secs, message }
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

    /// Whether this instance can use ClickHouse right now: what readiness asks.
    ///
    /// A query, not ClickHouse's `/ping`, which answers without credentials or a
    /// database: an instance with the wrong password, or a database nobody created,
    /// would be ready by it and refuse every write. Within `PING_TIMEOUT`, and reused
    /// for `PING_REUSE`.
    ///
    /// # Errors
    /// [`Error::Unavailable`], saying no more than that ClickHouse is not usable; the
    /// reason is logged and kept in `/status`.
    pub fn ping(&self) -> Result<()> {
        let mut held = self
            .readiness
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((at, answer)) = held.as_ref()
            && at.elapsed() < PING_REUSE
        {
            return answer.clone().map_err(Error::Unavailable);
        }
        let answer = self
            .request(
                &[("readonly", "2")],
                b"SELECT 1",
                "readiness",
                Some(PING_TIMEOUT),
            )
            .map(|_| ())
            .map_err(|failure| {
                tracing::warn!(%failure, "ClickHouse is not usable; not ready");
                "ClickHouse is not answering this instance's queries; its log says why".to_owned()
            });
        *held = Some((Instant::now(), answer.clone()));
        answer.map_err(Error::Unavailable)
    }

    /// Run a read and discard its answer, failing with everything ClickHouse said: for
    /// an operator's check, not for a client.
    ///
    /// # Errors
    /// [`Error::Unavailable`], in full.
    pub fn probe(&self, sql: &str) -> Result<()> {
        let settings = self.read_settings();
        self.request(&settings, sql.as_bytes(), sql, None)
            .map(|_| ())
            .map_err(Failure::detailed)
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
        // An operator's statement, at startup or reload: what ClickHouse said is theirs
        // to read in full.
        let outcome = self
            .request(settings, sql.as_bytes(), sql, None)
            .map(|_| ())
            .map_err(Failure::detailed);
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
        let body = compress(rows);
        self.stats
            .bytes_sent
            .fetch_add(body.bytes.len() as u64, Ordering::Relaxed);
        let outcome = self
            .request_encoded(
                &[
                    ("query", sql.as_str()),
                    ("async_insert", "1"),
                    ("wait_for_async_insert", "1"),
                    // ClickHouse's own window is 50–200 ms, which across a fleet and every
                    // materialized view behind a table is many small parts a second to
                    // merge. Up to half a second: fewer, larger parts, an acknowledgement
                    // still well inside a sender's patience.
                    ("async_insert_busy_timeout_min_ms", "200"),
                    ("async_insert_busy_timeout_max_ms", "500"),
                    ("input_format_skip_unknown_fields", "0"),
                ],
                &body,
                &sql,
                None,
            )
            .map(|_| ())
            .map_err(Failure::brief);
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
        let body = compress(rows);
        self.stats
            .bytes_sent
            .fetch_add(body.bytes.len() as u64, Ordering::Relaxed);
        let outcome = self
            .request_encoded(&[("query", sql.as_str())], &body, &sql, None)
            .map(|_| ())
            .map_err(Failure::brief);
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
            let response = self
                .request(&self.read_settings(), body.as_bytes(), sql, None)
                .map_err(Failure::brief)?;
            let mut reader = BufReader::with_capacity(1 << 20, response.into_body().into_reader());
            let mut row = vec![0u8; width];
            loop {
                let mut filled = 0;
                while filled < width {
                    let read = std::io::Read::read(&mut reader, &mut row[filled..])
                        .map_err(|e| self.failed(sql, &e.to_string()).brief())?;
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
                    return Err(self
                        .failed(
                            sql,
                            &format!("the answer ended partway through a row: {tail}"),
                        )
                        .brief());
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
        let mut settings = self.read_settings();
        settings.push(("output_format_json_quote_64bit_integers", "0".to_owned()));
        settings.push(("output_format_json_quote_64bit_floats", "0".to_owned()));
        let response = self
            .request(&settings, body.as_bytes(), sql, None)
            .map_err(Failure::brief)?;
        let mut reader = BufReader::new(response.into_body().into_reader());
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader
                .read_until(b'\n', &mut line)
                .map_err(|e| self.failed(sql, &e.to_string()).brief())?;
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

    /// What every read is sent with.
    ///
    /// - `readonly = 2`: a read runs with read rights only, whatever the account may do.
    /// - `max_execution_time`: ClickHouse stops what telemetryd has stopped waiting for,
    ///   so a query refused at the timeout does not go on loading the shared server.
    /// - `cancel_http_readonly_queries_on_client_close`: and stops a read whose caller
    ///   went away.
    /// - `max_query_size`: a query naming every series or trace it reads is long.
    fn read_settings(&self) -> Vec<(&'static str, String)> {
        vec![
            ("readonly", "2".to_owned()),
            (
                "max_execution_time",
                self.timeout.as_secs().max(1).to_string(),
            ),
            (
                "cancel_http_readonly_queries_on_client_close",
                "1".to_owned(),
            ),
            ("max_query_size", "67108864".to_owned()),
        ]
    }

    /// [`Self::request`] with a body [`compress`]ed, as `Content-Encoding` says.
    fn request_encoded<V: AsRef<str>>(
        &self,
        params: &[(&str, V)],
        body: &Encoded,
        what: &str,
        timeout: Option<Duration>,
    ) -> std::result::Result<ureq::http::Response<ureq::Body>, Failure> {
        self.send(params, &body.bytes, body.encoding, what, timeout)
    }

    /// One request, its failure kept whole for the caller to word.
    fn request<V: AsRef<str>>(
        &self,
        params: &[(&str, V)],
        body: &[u8],
        what: &str,
        timeout: Option<Duration>,
    ) -> std::result::Result<ureq::http::Response<ureq::Body>, Failure> {
        self.send(params, body, None, what, timeout)
    }

    fn send<V: AsRef<str>>(
        &self,
        params: &[(&str, V)],
        body: &[u8],
        encoding: Option<&str>,
        what: &str,
        timeout: Option<Duration>,
    ) -> std::result::Result<ureq::http::Response<ureq::Body>, Failure> {
        // Every bound telemetryd sends is UTC, and so are the hour and day columns:
        // `toStartOfHour` and `toDate` must not read them in the server's own zone.
        let mut request = self
            .agent
            .post(&self.url)
            .config()
            .timeout_global(Some(timeout.unwrap_or(self.timeout)))
            .build()
            .query("database", &self.database)
            .query("session_timezone", "UTC")
            .header("x-clickhouse-user", &self.user);
        if !self.password.is_empty() {
            request = request.header("x-clickhouse-key", &self.password);
        }
        if let Some(encoding) = encoding {
            request = request.header("content-encoding", encoding);
        }
        for (name, value) in params {
            request = request.query(*name, value.as_ref());
        }
        let mut response = request
            .send(body)
            .map_err(|e| self.failed(what, &e.to_string()))?;
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
        Err(self.failed(what, &format!("HTTP {status}: {detail}")))
    }

    fn failed(&self, what: &str, detail: &str) -> Failure {
        *self
            .stats
            .last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((
            std::time::SystemTime::now(),
            format!(
                "{} ({}): {detail}",
                self.shown,
                what.chars().take(120).collect::<String>()
            ),
        ));
        Failure {
            shown: self.shown.clone(),
            what: what.chars().take(120).collect(),
            detail: detail.to_owned(),
        }
    }
}

/// A request that failed: where, doing what, and what was said.
#[derive(Debug)]
struct Failure {
    shown: String,
    what: String,
    detail: String,
}

impl Failure {
    /// In full, for the operator: at startup and on reload.
    fn detailed(self) -> Error {
        Error::Unavailable(format!(
            "ClickHouse at {} failed ({}): {}",
            self.shown, self.what, self.detail
        ))
    }

    /// Without the host, the SQL or ClickHouse's own text, for whoever sent the request
    /// — an ingest client, a dashboard — who is owed a reason but not the database's
    /// internals. The whole is logged.
    fn brief(self) -> Error {
        tracing::warn!(
            clickhouse = %self.shown,
            request = %self.what,
            detail = %self.detail,
            "a request to ClickHouse failed"
        );
        let class = if let Some(rest) = self.detail.strip_prefix("HTTP ") {
            let status = rest.split(':').next().unwrap_or_default();
            match exception_name(&self.detail) {
                Some(name) => format!("refused it ({name})"),
                None => format!("refused it (HTTP {status})"),
            }
        } else {
            "could not be reached".to_owned()
        };
        Error::Unavailable(format!(
            "the ClickHouse store {class}; retry shortly — the instance's log says more"
        ))
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClickHouse at {} failed ({}): {}",
            self.shown, self.what, self.detail
        )
    }
}

/// The name ClickHouse gives an exception, `MEMORY_LIMIT_EXCEEDED` in
/// `… (MEMORY_LIMIT_EXCEEDED) (version …)`: what a client can be told without the text
/// around it.
fn exception_name(detail: &str) -> Option<&str> {
    detail
        .split('(')
        .filter_map(|part| part.split(')').next())
        .find(|name| {
            name.len() > 3
                && name
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
        })
}

/// A request body, compressed or not, and what `Content-Encoding` says it is.
struct Encoded {
    bytes: Vec<u8>,
    encoding: Option<&'static str>,
}

/// Rows below this are sent as they are: compressing a few hundred bytes buys nothing.
const COMPRESS_FROM: usize = 4096;

/// zstd at its fastest level: JSON rows shrink about tenfold, for a few milliseconds of
/// one core per megabyte — the difference between terabytes a month crossing the
/// network to ClickHouse and a tenth of that. Sent as it was if compression fails.
fn compress(rows: &[u8]) -> Encoded {
    if rows.len() >= COMPRESS_FROM
        && let Ok(bytes) = zstd::bulk::compress(rows, 1)
    {
        return Encoded {
            bytes,
            encoding: Some("zstd"),
        };
    }
    Encoded {
        bytes: rows.to_vec(),
        encoding: None,
    }
}
