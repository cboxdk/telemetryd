//! Moving a time range in or out, as OTLP (ADR-012).
//!
//! Export and import are the same engine pointed at different ends. Both read a range
//! from a Loki-compatible API and turn it into OTLP requests; export writes them to a
//! file, import posts them to an instance. That is why importing from another
//! telemetryd, importing from a different backend, and copying between instances are
//! one code path rather than three — telemetryd serves the same API it reads.
//!
//! # Windows, not offsets
//!
//! Read APIs cap results per request, so a range is walked in windows. The cursor is
//! the timestamp of the oldest entry seen, moved back one nanosecond each time, which
//! terminates because time is finite and every window is strictly older than the last.
//!
//! Paging by *offset* would be the other option and it is wrong here: entries arrive
//! while you page, so offsets shift under you and you would skip or repeat rows with
//! no way to tell which.
//!
//! # Progress goes to stderr, data to stdout
//!
//! That one rule is what lets `telemetryd export | gzip > dump.gz` show a live meter
//! without corrupting a byte of the output.

use std::io::{IsTerminal, Write};

use anyhow::{Context, bail};
use clap::{Args, ValueEnum};
use serde_json::Value;

/// Entries per request. Well under any sensible server-side cap, and small enough that
/// one window's worth is a reasonable unit of progress.
const WINDOW_LIMIT: usize = 5_000;
/// The selector that means "everything". Anything else is a subset the caller asked
/// for, which only the query path can express.
const DEFAULT_SELECTOR: &str = r#"{app=~".+"}"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Progress {
    /// A live meter on a terminal, periodic lines when it is not one.
    Auto,
    /// Force the live meter.
    Tty,
    /// One line every few seconds. A log file, not a redraw.
    Plain,
    /// NDJSON events on stderr, for a program that is watching.
    Json,
    None,
}

impl Progress {
    /// Resolve `auto` against whether stderr is actually a terminal.
    ///
    /// Getting this wrong is the difference between readable output and a smear: a
    /// redraw with carriage returns belongs on a terminal and is garbage in a log file.
    fn resolve(self) -> Self {
        match self {
            Self::Auto if std::io::stderr().is_terminal() => Self::Tty,
            Self::Auto => Self::Plain,
            other => other,
        }
    }
}

#[derive(Debug, Args)]
pub struct ExportArgs {
    /// How far back to read.
    #[arg(long, default_value = "1h", value_name = "DURATION")]
    pub since: humantime::Duration,

    /// LogQL selector for what to export.
    ///
    /// The default takes everything the store will admit to holding. A selector must
    /// require at least one value, so `{app=~".+"}` rather than `{}`.
    #[arg(long, default_value = r#"{app=~".+"}"#, value_name = "LOGQL")]
    pub query: String,

    /// Base URL of the instance to read from.
    #[arg(long, default_value = "http://127.0.0.1:4319", value_name = "URL")]
    pub url: String,

    /// Token for `--url`, if it needs one.
    #[arg(
        long,
        env = "TELEMETRYD_AUTH_QUERY_TOKEN",
        value_name = "TOKEN",
        hide_env_values = true
    )]
    pub token: Option<String>,

    /// Which signal. `logs`, `traces` or `metrics`.
    ///
    /// Anything but `logs` uses telemetryd's own export endpoint, which reads records
    /// rather than re-deriving them from a query language — full fidelity, and the only
    /// way to enumerate traces at all. It therefore needs the source to *be* a
    /// telemetryd; `logs` also works against any Loki-compatible backend.
    #[arg(long, default_value = "logs", value_name = "SIGNAL")]
    pub signal: String,

    /// Where to write. `-` or omitted is stdout.
    #[arg(long, value_name = "PATH", conflicts_with = "to")]
    pub output: Option<String>,

    /// Post straight to another instance instead of writing a file.
    ///
    /// The full-fidelity path between two telemetryds: records are read through
    /// `/api/v1/export` rather than re-derived from a query language, and all three
    /// signals come across. `import --from` is the other direction and goes through the
    /// read APIs, which cannot carry metrics.
    #[arg(long, value_name = "URL", conflicts_with = "output")]
    pub to: Option<String>,

    /// Ingest token for `--to`.
    #[arg(
        long,
        env = "TELEMETRYD_AUTH_INGEST_TOKEN",
        value_name = "TOKEN",
        hide_env_values = true
    )]
    pub to_token: Option<String>,

    #[arg(long, value_enum, default_value_t = Progress::Auto)]
    pub progress: Progress,
}

#[derive(Debug, Args)]
pub struct ImportArgs {
    /// Read from a Loki-compatible API — another telemetryd, or anything serving it.
    #[arg(long, value_name = "URL", conflicts_with = "file")]
    pub from: Option<String>,

    /// Read from a file written by `telemetryd export`. `-` is stdin.
    #[arg(long, value_name = "PATH", conflicts_with = "from")]
    pub file: Option<String>,

    #[arg(long, default_value = "1h", value_name = "DURATION")]
    pub since: humantime::Duration,

    #[arg(long, default_value = r#"{app=~".+"}"#, value_name = "LOGQL")]
    pub query: String,

    /// Token for `--from`, if it needs one.
    #[arg(long, value_name = "TOKEN", hide_env_values = true)]
    pub from_token: Option<String>,

    /// Which signal to pull. `logs` or `traces`.
    ///
    /// Metrics are not offered from a foreign backend: a range query returns points at
    /// whatever `step` was asked for rather than the samples that were stored, so it
    /// would be a lossy path dressed as a migration path. Have the source send OTLP
    /// instead.
    #[arg(long, default_value = "logs", value_name = "SIGNAL")]
    pub signal: String,

    /// The instance to write into.
    #[arg(long, default_value = "http://127.0.0.1:4319", value_name = "URL")]
    pub url: String,

    /// Ingest token for `--url`.
    #[arg(
        long,
        env = "TELEMETRYD_AUTH_INGEST_TOKEN",
        value_name = "TOKEN",
        hide_env_values = true
    )]
    pub token: Option<String>,

    #[arg(long, value_enum, default_value_t = Progress::Auto)]
    pub progress: Progress,

    /// Import a range older than the destination's retention anyway.
    ///
    /// Refused by default because the reaper would delete it, possibly while the import
    /// is still running — an import that appears to succeed and silently produces
    /// nothing is worse than one that refuses.
    #[arg(long)]
    pub allow_expiring: bool,
}

/// Where progress is reported, and how.
struct Reporter {
    mode: Progress,
    records: u64,
    requests: u64,
    started: std::time::Instant,
    last: std::time::Instant,
    high_water: Option<u64>,
}

impl Reporter {
    fn new(mode: Progress) -> Self {
        let now = std::time::Instant::now();
        Self {
            mode: mode.resolve(),
            records: 0,
            requests: 0,
            started: now,
            last: now,
            high_water: None,
        }
    }

    fn advance(&mut self, records: u64, oldest_nanos: Option<u64>) {
        self.records += records;
        self.requests += 1;
        if let Some(oldest) = oldest_nanos {
            self.high_water = Some(oldest);
        }

        match self.mode {
            Progress::None | Progress::Auto => {}
            Progress::Tty => {
                // `\r` and no newline: one line that rewrites itself.
                eprint!(
                    "\r  {} records in {} requests, {:.0}/s   ",
                    self.records,
                    self.requests,
                    self.rate()
                );
                let _ = std::io::stderr().flush();
            }
            Progress::Plain => {
                if self.last.elapsed() >= std::time::Duration::from_secs(5) {
                    self.last = std::time::Instant::now();
                    eprintln!(
                        "  {} records in {} requests, {:.0}/s",
                        self.records,
                        self.requests,
                        self.rate()
                    );
                }
            }
            Progress::Json => {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "progress",
                        "records": self.records,
                        "requests": self.requests,
                        "high_water_nanos": self.high_water.map(|n| n.to_string()),
                    })
                );
            }
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn rate(&self) -> f64 {
        let seconds = self.started.elapsed().as_secs_f64().max(0.001);
        self.records as f64 / seconds
    }

    /// The last thing written. Carries the high-water mark either way, because that is
    /// what makes a failed transfer resumable.
    fn finish(&self, error: Option<&str>) {
        match self.mode {
            Progress::None | Progress::Auto => {}
            Progress::Json => eprintln!(
                "{}",
                serde_json::json!({
                    "event": if error.is_some() { "failed" } else { "done" },
                    "records": self.records,
                    "requests": self.requests,
                    "high_water_nanos": self.high_water.map(|n| n.to_string()),
                    "error": error,
                })
            ),
            Progress::Tty => {
                eprintln!(
                    "\r  {} records in {} requests{}   ",
                    self.records,
                    self.requests,
                    error
                        .map(|e| format!(" — stopped: {e}"))
                        .unwrap_or_default()
                );
            }
            Progress::Plain => eprintln!(
                "  {} records in {} requests{}",
                self.records,
                self.requests,
                error
                    .map(|e| format!(" — stopped: {e}"))
                    .unwrap_or_default()
            ),
        }
    }
}

fn get(url: &str, token: Option<&str>) -> anyhow::Result<Value> {
    let mut request = ureq::get(url).header("user-agent", super::status::user_agent());
    if let Some(token) = token {
        request = request.header("authorization", &format!("Bearer {token}"));
    }
    let mut response = request
        .config()
        .http_status_as_error(false)
        .build()
        .call()
        .with_context(|| format!("could not reach {url}"))?;

    let status = response.status().as_u16();
    let body = response.body_mut().read_to_string()?;
    if status != 200 {
        let detail: String = body.trim().chars().take(400).collect();
        bail!("{url} answered {status}: {detail}");
    }
    serde_json::from_str(&body).context("parsing the response")
}

/// One window of logs, converted to an OTLP request, plus the oldest timestamp in it.
///
/// Returns `None` when the window was empty, which is how the walk terminates.
fn window(
    base: &str,
    query: &str,
    token: Option<&str>,
    start_nanos: u128,
    end_nanos: u128,
) -> anyhow::Result<Option<(Value, u64, u64)>> {
    let url = format!(
        "{base}/loki/api/v1/query_range?query={}&start={start_nanos}&end={end_nanos}&limit={WINDOW_LIMIT}&direction=backward",
        super::query::urlencode(query)
    );
    let response = get(&url, token)?;

    let streams = response
        .get("data")
        .and_then(|d| d.get("result"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut resources = Vec::new();
    let mut count = 0u64;
    let mut oldest = u64::MAX;

    for stream in &streams {
        let labels = stream.get("stream").and_then(Value::as_object);
        let Some(values) = stream.get("values").and_then(Value::as_array) else {
            continue;
        };
        if values.is_empty() {
            continue;
        }

        let attributes: Vec<Value> = labels
            .map(|labels| {
                labels
                    .iter()
                    .filter(|(key, _)| key.as_str() != "level")
                    .map(|(key, value)| {
                        serde_json::json!({"key": dotted(key), "value": {"stringValue": value}})
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut records = Vec::with_capacity(values.len());
        for entry in values {
            let Some(pair) = entry.as_array() else {
                continue;
            };
            let timestamp = pair.first().and_then(Value::as_str).unwrap_or("0");
            let text = pair.get(1).and_then(Value::as_str).unwrap_or("");
            if let Ok(nanos) = timestamp.parse::<u64>() {
                oldest = oldest.min(nanos);
            }

            let mut record = serde_json::Map::new();
            record.insert("timeUnixNano".into(), Value::String(timestamp.to_owned()));
            if let Some(level) = labels.and_then(|l| l.get("level")).and_then(Value::as_str) {
                record.insert(
                    "severityNumber".into(),
                    serde_json::json!(severity_number(level)),
                );
                record.insert("severityText".into(), Value::String(level.to_uppercase()));
            }
            record.insert("body".into(), serde_json::json!({"stringValue": text}));
            // The third element is structured metadata — the per-record attributes.
            // Dropping it would export a line and lose everything about it.
            if let Some(extra) = pair.get(2).and_then(Value::as_object) {
                record.insert(
                    "attributes".into(),
                    Value::Array(
                        extra
                            .iter()
                            .map(|(key, value)| {
                                let text = value.as_str().map_or_else(
                                    || value.to_string(),
                                    std::borrow::ToOwned::to_owned,
                                );
                                serde_json::json!({"key": key, "value": {"stringValue": text}})
                            })
                            .collect(),
                    ),
                );
            }
            records.push(Value::Object(record));
            count += 1;
        }

        resources.push(serde_json::json!({
            "resource": {"attributes": attributes},
            "scopeLogs": [{"logRecords": records}],
        }));
    }

    if count == 0 {
        return Ok(None);
    }
    Ok(Some((
        serde_json::json!({"resourceLogs": resources}),
        count,
        oldest,
    )))
}

/// The `severityNumber` a level maps back to.
///
/// Ingest derives `level` from the severity, so a record exported without one comes
/// back as `level="unknown"` — the round-trip test caught exactly that, with matching
/// record counts and different content. Counting is not comparing.
///
/// The lowest number of each range, because that is what maps back to the same level.
fn severity_number(level: &str) -> i32 {
    match level {
        "trace" => 1,
        "debug" => 5,
        "info" => 9,
        "warn" => 13,
        "error" => 17,
        "fatal" => 21,
        _ => 0,
    }
}

/// Stream labels are stored with dots sanitised away; put the known ones back so a
/// foreign receiver recognises them. Same table as the encoder, same reasoning.
fn dotted(name: &str) -> &str {
    match name {
        "service_name" => "service.name",
        "service_namespace" => "service.namespace",
        "service_version" => "service.version",
        "deployment_environment" => "deployment.environment",
        "deployment_environment_name" => "deployment.environment.name",
        other => other,
    }
}

fn now_nanos() -> anyhow::Result<u128> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("reading the clock")?
        .as_nanos())
}

/// Walk the range oldest-ward, handing each window to `emit`.
fn walk(
    base: &str,
    query: &str,
    token: Option<&str>,
    since: std::time::Duration,
    reporter: &mut Reporter,
    mut emit: impl FnMut(&Value) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let end = now_nanos()?;
    let start = end.saturating_sub(since.as_nanos());
    let mut cursor = end;

    loop {
        let Some((batch, count, oldest)) = window(base, query, token, start, cursor)? else {
            break;
        };
        emit(&batch)?;
        reporter.advance(count, Some(oldest));

        // Strictly older next time, or a window whose entries all share a timestamp
        // would repeat forever.
        let next = u128::from(oldest).saturating_sub(1);
        if next <= start {
            break;
        }
        cursor = next;
    }
    Ok(())
}

/// Walk telemetryd's own export endpoint, which returns records rather than a
/// rendering of them.
///
/// The cursor is the newest timestamp returned, advanced by one nanosecond — the same
/// discipline as the Loki walk and for the same reason: records arrive while you page,
/// so an offset would shift underneath you.
fn walk_native(
    base: &str,
    signal: &str,
    token: Option<&str>,
    since: std::time::Duration,
    reporter: &mut Reporter,
    emit: &mut dyn FnMut(&str) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let end = now_nanos()?;
    let mut cursor = end.saturating_sub(since.as_nanos());

    loop {
        let url = format!("{base}/api/v1/export?signal={signal}&start={cursor}&end={end}");
        let body = get_text(&url, token)?;
        if body.trim().is_empty() {
            break;
        }

        let mut newest = 0u64;
        let mut records = 0u64;
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            let batch: Value = serde_json::from_str(line).context("parsing the export")?;
            newest = newest.max(newest_nanos(&batch));
            records += count_any(&batch);
            emit(line)?;
        }
        reporter.advance(records, Some(newest));

        if newest == 0 {
            break;
        }
        let next = u128::from(newest).saturating_add(1);
        if next >= end {
            break;
        }
        cursor = next;
    }
    Ok(())
}

fn get_text(url: &str, token: Option<&str>) -> anyhow::Result<String> {
    let mut request = ureq::get(url).header("user-agent", super::status::user_agent());
    if let Some(token) = token {
        request = request.header("authorization", &format!("Bearer {token}"));
    }
    let mut response = request
        .config()
        .http_status_as_error(false)
        .build()
        .call()
        .with_context(|| format!("could not reach {url}"))?;
    let status = response.status().as_u16();
    let body = response.body_mut().read_to_string()?;
    if status != 200 {
        let detail: String = body.trim().chars().take(400).collect();
        bail!("{url} answered {status}: {detail}");
    }
    Ok(body)
}

/// The newest `timeUnixNano` anywhere in an OTLP batch, across all three shapes.
fn newest_nanos(batch: &Value) -> u64 {
    fn walk(value: &Value, newest: &mut u64) {
        match value {
            Value::Object(map) => {
                for (key, inner) in map {
                    if matches!(key.as_str(), "timeUnixNano" | "endTimeUnixNano")
                        && let Some(nanos) = inner.as_str().and_then(|s| s.parse::<u64>().ok())
                    {
                        *newest = (*newest).max(nanos);
                    }
                    walk(inner, newest);
                }
            }
            Value::Array(items) => items.iter().for_each(|item| walk(item, newest)),
            _ => {}
        }
    }
    let mut newest = 0;
    walk(batch, &mut newest);
    newest
}

/// Which ingest endpoint a batch belongs to, decided by its envelope.
///
/// An export file names its signal in every line, so routing by content rather than by
/// a flag means a traces file cannot be posted to the logs endpoint by getting the
/// invocation wrong — and a file holding more than one signal just works.
fn endpoint_for(batch: &Value) -> Option<&'static str> {
    if batch.get("resourceLogs").is_some() {
        Some("/v1/logs")
    } else if batch.get("resourceSpans").is_some() {
        Some("/v1/traces")
    } else if batch.get("resourceMetrics").is_some() {
        Some("/v1/metrics")
    } else {
        None
    }
}

/// Records in a batch, whichever signal it holds.
fn count_any(batch: &Value) -> u64 {
    let logs = count_records(batch);
    let spans: u64 = batch
        .get("resourceSpans")
        .and_then(Value::as_array)
        .map_or(0, |resources| {
            resources
                .iter()
                .filter_map(|r| r.get("scopeSpans").and_then(Value::as_array))
                .flatten()
                .filter_map(|s| s.get("spans").and_then(Value::as_array))
                .map(|s| s.len() as u64)
                .sum()
        });
    let points: u64 = batch
        .get("resourceMetrics")
        .and_then(Value::as_array)
        .map_or(0, |resources| {
            resources
                .iter()
                .filter_map(|r| r.get("scopeMetrics").and_then(Value::as_array))
                .flatten()
                .filter_map(|s| s.get("metrics").and_then(Value::as_array))
                .map(|m| m.len() as u64)
                .sum()
        });
    logs + spans + points
}

pub fn export(args: &ExportArgs) -> anyhow::Result<()> {
    let base = args.url.trim_end_matches('/');
    let mut reporter = Reporter::new(args.progress);

    // Posting to a destination is the same walk with a different sink — which is the
    // whole point, and the reason an earlier version of this refused to offer it was
    // not a good one. ADR-012 said telemetryd never writes to a foreign store; relay
    // mode has been posting OTLP upstream since it shipped. The rule that survives is
    // narrower: never write to a store you were only asked to read from. A destination
    // named on the command line is not that.
    if let Some(destination) = &args.to {
        let destination = destination.trim_end_matches('/').to_owned();
        let token = args.to_token.clone();
        let mut emit = |line: &str| -> anyhow::Result<()> {
            let batch: Value = serde_json::from_str(line).context("parsing the export")?;
            let Some(endpoint) = endpoint_for(&batch) else {
                bail!("the export produced something that is not an OTLP request");
            };
            post(&format!("{destination}{endpoint}"), token.as_deref(), line)
        };
        let outcome = walk_native(
            base,
            &args.signal,
            args.token.as_deref(),
            args.since.into(),
            &mut reporter,
            &mut emit,
        );
        return match outcome {
            Ok(()) => {
                reporter.finish(None);
                Ok(())
            }
            Err(error) => {
                reporter.finish(Some(&error.to_string()));
                Err(error)
            }
        };
    }

    let mut sink: Box<dyn Write> = match args.output.as_deref() {
        None | Some("-") => Box::new(std::io::BufWriter::new(std::io::stdout().lock())),
        Some(path) => Box::new(std::io::BufWriter::new(
            std::fs::File::create(path).with_context(|| format!("creating {path}"))?,
        )),
    };

    let outcome = if args.signal == "logs" && args.query != DEFAULT_SELECTOR {
        // A selector means the caller wants a subset, and only the query path can
        // answer that.
        walk(
            base,
            &args.query,
            args.token.as_deref(),
            args.since.into(),
            &mut reporter,
            |batch| {
                // One request-shaped object per line. NDJSON streams, survives being
                // cut in half, and every tool on the machine can read it.
                writeln!(sink, "{batch}").context("writing the export")
            },
        )
    } else {
        let mut emit = |line: &str| writeln!(sink, "{line}").context("writing the export");
        walk_native(
            base,
            &args.signal,
            args.token.as_deref(),
            args.since.into(),
            &mut reporter,
            &mut emit,
        )
    };

    sink.flush().ok();
    match outcome {
        Ok(()) => {
            reporter.finish(None);
            Ok(())
        }
        Err(error) => {
            reporter.finish(Some(&error.to_string()));
            Err(error)
        }
    }
}

fn post(url: &str, token: Option<&str>, body: &str) -> anyhow::Result<()> {
    let mut request = ureq::post(url)
        .config()
        .http_status_as_error(false)
        .build()
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", &format!("Bearer {token}"));
    }
    let mut response = request
        .send(body)
        .with_context(|| format!("could not reach {url}"))?;
    let status = response.status().as_u16();
    if (200..300).contains(&status) {
        return Ok(());
    }
    let detail: String = response
        .body_mut()
        .read_to_string()
        .unwrap_or_default()
        .trim()
        .chars()
        .take(400)
        .collect();
    bail!("{url} answered {status}: {detail}")
}

/// Refuse a range the destination will delete out from under the import.
///
/// Ingest applies no age limit, so old records go in perfectly well — and then the
/// reaper removes anything past `retention.logs`, possibly while the import is still
/// running. An import that appears to succeed and produces nothing is the worst of the
/// available outcomes.
fn check_retention(
    base: &str,
    token: Option<&str>,
    since: std::time::Duration,
) -> anyhow::Result<()> {
    let Ok(status) = get(&format!("{base}/status"), token) else {
        // The admin surface may be guarded by a token this command was not given. Not
        // being able to check is not a reason to refuse.
        return Ok(());
    };
    let window = status
        .get("retention")
        .and_then(|r| r.get("logs"))
        .and_then(Value::as_str)
        .and_then(|text| humantime::parse_duration(text).ok());

    if let Some(window) = window
        && since > window
    {
        bail!(
            "the destination keeps logs for {}, and this would import {} of history — \
             everything older than the window would be deleted by the reaper, possibly \
             while the import is still running.\n\
             Raise retention.logs there, or pass --allow-expiring if that is what you \
             meant.",
            humantime::format_duration(window),
            humantime::format_duration(since),
        )
    }
    Ok(())
}

/// Pull traces from a foreign Tempo-compatible backend.
///
/// Two requests deep, and unavoidably so: search returns trace *ids* with their start
/// times, and the spans come from fetching each id. That is N+1 requests per window and
/// slower than logs by a wide margin — but it is correct, and correct is what was
/// missing.
///
/// This exists because the reason given for leaving it out was wrong. "A trace search
/// API answers which traces match, not every trace in a window" was written into an ADR
/// and two release notes as justification, and it is false: search without a query
/// enumerates the window, which is exactly what Tempo does and what telemetryd copied.
fn walk_traces(
    base: &str,
    token: Option<&str>,
    since: std::time::Duration,
    reporter: &mut Reporter,
    mut emit: impl FnMut(&Value) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let end_nanos = now_nanos()?;
    let start_nanos = end_nanos.saturating_sub(since.as_nanos());
    let start_s = start_nanos / 1_000_000_000;
    let mut cursor_s = end_nanos.div_ceil(1_000_000_000);

    // Trace ids already taken, because the cursor cannot be time.
    //
    // Search takes *seconds*, so a window ending at the oldest trace's second still
    // contains that trace — the first version rounded the end up "so a trace on the
    // boundary is not lost", and turned the walk into a loop that re-fetched the same
    // 44 traces 182 times. Rounding down instead would lose every trace sharing that
    // second. Ids are the only thing precise enough to make progress on.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    loop {
        let url = format!("{base}/api/search?start={start_s}&end={cursor_s}&limit={WINDOW_LIMIT}");
        let found = get(&url, token)?;
        let traces = found
            .get("traces")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if traces.is_empty() {
            break;
        }

        let mut oldest_s = u128::MAX;
        let mut fresh = 0u64;
        let mut spans = 0u64;

        for summary in &traces {
            let Some(id) = summary.get("traceID").and_then(Value::as_str) else {
                continue;
            };
            if let Some(started) = summary.get("startTimeUnixNano").and_then(|v| {
                v.as_str()
                    .and_then(|s| s.parse::<u128>().ok())
                    .or_else(|| v.as_u64().map(u128::from))
            }) {
                oldest_s = oldest_s.min(started / 1_000_000_000);
            }
            if !seen.insert(id.to_owned()) {
                continue;
            }
            fresh += 1;

            let trace = get(&format!("{base}/api/traces/{id}"), token)?;
            let Some(batches) = trace.get("batches").cloned() else {
                continue;
            };
            let batch = serde_json::json!({"resourceSpans": batches});
            spans += count_any(&batch);
            emit(&batch)?;
        }
        reporter.advance(spans, None);

        // A window that produced nothing new is the end of the walk: either the range
        // is exhausted or every trace in it is already taken.
        if fresh == 0 {
            break;
        }
        if oldest_s == u128::MAX || oldest_s <= start_s {
            break;
        }
        // Keep the oldest second in range — traces sharing it may not all have fitted
        // under `limit` — and let the id set stop the repeat.
        cursor_s = oldest_s;
    }
    Ok(())
}

pub fn import(args: &ImportArgs) -> anyhow::Result<()> {
    let destination = args.url.trim_end_matches('/');
    let logs_url = format!("{destination}/v1/logs");
    let mut reporter = Reporter::new(args.progress);

    if !args.allow_expiring {
        check_retention(destination, args.token.as_deref(), args.since.into())?;
    }

    let traces_url = format!("{destination}/v1/traces");
    let outcome = match (&args.from, &args.file) {
        (Some(from), _) if args.signal == "traces" => walk_traces(
            from.trim_end_matches('/'),
            args.from_token.as_deref(),
            args.since.into(),
            &mut reporter,
            |batch| post(&traces_url, args.token.as_deref(), &batch.to_string()),
        ),
        (Some(from), _) => walk(
            from.trim_end_matches('/'),
            &args.query,
            args.from_token.as_deref(),
            args.since.into(),
            &mut reporter,
            |batch| post(&logs_url, args.token.as_deref(), &batch.to_string()),
        ),
        (None, Some(path)) => import_file(path, destination, args.token.as_deref(), &mut reporter),
        (None, None) => bail!("import needs --from <url> or --file <path>"),
    };

    match outcome {
        Ok(()) => {
            reporter.finish(None);
            Ok(())
        }
        Err(error) => {
            reporter.finish(Some(&error.to_string()));
            Err(error)
        }
    }
}

/// Replay an export file, one line at a time.
///
/// Line by line rather than parsed whole: a 4 GB dump should not need 4 GB of memory,
/// and NDJSON exists precisely so it does not have to.
fn import_file(
    path: &str,
    destination: &str,
    token: Option<&str>,
    reporter: &mut Reporter,
) -> anyhow::Result<()> {
    use std::io::BufRead;

    let reader: Box<dyn BufRead> = if path == "-" {
        Box::new(std::io::BufReader::new(std::io::stdin().lock()))
    } else {
        Box::new(std::io::BufReader::new(
            std::fs::File::open(path).with_context(|| format!("opening {path}"))?,
        ))
    };

    for (number, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("reading {path}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let batch: Value = serde_json::from_str(&line)
            .with_context(|| format!("{path} line {} is not JSON", number + 1))?;
        let Some(endpoint) = endpoint_for(&batch) else {
            bail!(
                "{path} line {} is not an OTLP request — expected resourceLogs, \
                 resourceSpans or resourceMetrics",
                number + 1
            );
        };
        let count = count_any(&batch);
        post(&format!("{destination}{endpoint}"), token, &line)?;
        reporter.advance(count, None);
    }
    Ok(())
}

fn count_records(batch: &Value) -> u64 {
    batch
        .get("resourceLogs")
        .and_then(Value::as_array)
        .map_or(0, |resources| {
            resources
                .iter()
                .filter_map(|resource| resource.get("scopeLogs").and_then(Value::as_array))
                .flatten()
                .filter_map(|scope| scope.get("logRecords").and_then(Value::as_array))
                .map(|records| records.len() as u64)
                .sum()
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_sanitised_label_goes_back_out_dotted() {
        assert_eq!(dotted("service_name"), "service.name");
        // A producer's own underscore is not a dot in disguise.
        assert_eq!(dotted("order_id"), "order_id");
    }

    /// The round trip: a level has to survive as a level, not as the absence of one.
    #[test]
    fn a_level_maps_back_to_a_severity_that_maps_back_to_the_level() {
        for level in ["trace", "debug", "info", "warn", "error", "fatal"] {
            let number = severity_number(level);
            assert_eq!(
                telemetryd_core::record::Severity::from_otlp_number(number).as_str(),
                level,
                "{level} did not survive the round trip through severityNumber"
            );
        }
        assert_eq!(severity_number("nonsense"), 0);
    }

    /// Routing by content, so a traces file cannot land on the logs endpoint because
    /// someone typed the wrong flag.
    #[test]
    fn a_batch_is_routed_by_what_it_holds() {
        assert_eq!(
            endpoint_for(&serde_json::json!({"resourceLogs": []})),
            Some("/v1/logs")
        );
        assert_eq!(
            endpoint_for(&serde_json::json!({"resourceSpans": []})),
            Some("/v1/traces")
        );
        assert_eq!(
            endpoint_for(&serde_json::json!({"resourceMetrics": []})),
            Some("/v1/metrics")
        );
        assert_eq!(endpoint_for(&serde_json::json!({"nonsense": []})), None);
    }

    #[test]
    fn records_are_counted_out_of_a_batch() {
        let batch = serde_json::json!({"resourceLogs": [
            {"scopeLogs": [{"logRecords": [{}, {}]}]},
            {"scopeLogs": [{"logRecords": [{}]}]},
        ]});
        assert_eq!(count_records(&batch), 3);
        assert_eq!(count_records(&serde_json::json!({})), 0);
    }

    #[test]
    fn auto_progress_follows_whether_stderr_is_a_terminal() {
        // Under `cargo test` stderr is captured, so this resolves to the line-oriented
        // form — which is exactly the behaviour that matters in CI and under systemd.
        assert_eq!(Progress::Auto.resolve(), Progress::Plain);
        // An explicit choice is never second-guessed.
        assert_eq!(Progress::Json.resolve(), Progress::Json);
        assert_eq!(Progress::None.resolve(), Progress::None);
    }
}
