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

    /// Where to write. `-` or omitted is stdout.
    #[arg(long, value_name = "PATH")]
    pub output: Option<String>,

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

pub fn export(args: &ExportArgs) -> anyhow::Result<()> {
    let base = args.url.trim_end_matches('/');
    let mut reporter = Reporter::new(args.progress);

    let mut sink: Box<dyn Write> = match args.output.as_deref() {
        None | Some("-") => Box::new(std::io::BufWriter::new(std::io::stdout().lock())),
        Some(path) => Box::new(std::io::BufWriter::new(
            std::fs::File::create(path).with_context(|| format!("creating {path}"))?,
        )),
    };

    let outcome = walk(
        base,
        &args.query,
        args.token.as_deref(),
        args.since.into(),
        &mut reporter,
        |batch| {
            // One request-shaped object per line. NDJSON streams, survives being cut in
            // half, and every tool on the machine can read it.
            writeln!(sink, "{batch}").context("writing the export")
        },
    );

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

pub fn import(args: &ImportArgs) -> anyhow::Result<()> {
    let destination = args.url.trim_end_matches('/');
    let logs_url = format!("{destination}/v1/logs");
    let mut reporter = Reporter::new(args.progress);

    if !args.allow_expiring {
        check_retention(destination, args.token.as_deref(), args.since.into())?;
    }

    let outcome = match (&args.from, &args.file) {
        (Some(from), _) => walk(
            from.trim_end_matches('/'),
            &args.query,
            args.from_token.as_deref(),
            args.since.into(),
            &mut reporter,
            |batch| post(&logs_url, args.token.as_deref(), &batch.to_string()),
        ),
        (None, Some(path)) => import_file(path, &logs_url, args.token.as_deref(), &mut reporter),
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
    logs_url: &str,
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
        let count = count_records(&batch);
        post(logs_url, token, &line)?;
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
