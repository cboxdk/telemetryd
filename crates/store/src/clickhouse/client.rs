//! The HTTP interface, over the same blocking client the relay uses.
//!
//! HTTP rather than the native protocol: it needs no new dependency, goes through any
//! proxy or load balancer in front of a cluster, and the trust roots it verifies against
//! are the ones every other outbound connection here already uses.

use std::io::{BufRead, BufReader};
use std::ops::ControlFlow;
use std::time::Duration;

use telemetryd_core::config::ClickHouseConfig;
use telemetryd_core::{Error, Result};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// A connection to one ClickHouse endpoint, as configured.
#[derive(Clone)]
pub struct Client {
    url: String,
    shown: String,
    database: String,
    user: String,
    password: String,
    timeout: Duration,
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
        })
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
        self.post(&[], sql.as_bytes(), sql).map(|_| ())
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
        self.post(
            &[
                ("query", sql.as_str()),
                ("async_insert", "1"),
                ("wait_for_async_insert", "1"),
                ("input_format_skip_unknown_fields", "0"),
            ],
            rows,
            &sql,
        )
        .map(|_| ())
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
        let body = format!("{sql}\nFORMAT JSONEachRow");
        let response = self.post(
            &[
                ("output_format_json_quote_64bit_integers", "0"),
                ("output_format_json_quote_64bit_floats", "0"),
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
