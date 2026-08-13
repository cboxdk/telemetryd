//! `telemetryd query` — run a LogQL query from the shell.
//!
//! Two jobs, one command.
//!
//! **Debugging.** When something is wrong the UI is often the thing you cannot reach —
//! it is a browser away, or it is the component that is broken. A query you can run
//! over SSH, and pipe into `grep` and `wc`, is the difference between diagnosing a
//! problem and describing it.
//!
//! **Getting your data out.** A self-hosted tool that can only be read through its own
//! interface has quietly become a place data goes in. `--output json` writes one JSON
//! object per line, which every other tool on the machine can read.
//!
//! It speaks to a running instance rather than reading the data directory, so it sees
//! records that are still buffered and it respects the configured tokens. To query a
//! backup, start an instance on the copy — see the backup guide.

use anyhow::{Context, bail};
use clap::{Args, ValueEnum};
use std::io::Write;

use serde_json::Value;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Output {
    /// One line per record, timestamp first. For reading.
    Text,
    /// One JSON object per line. For piping.
    Json,
}

/// Everything, so the command works with no argument at all.
///
/// `{}` would be the obvious spelling and the server refuses it: a selector with no
/// matcher that requires a value selects the whole store, which is a mistake to make
/// expensive rather than easy. `{app=~".+"}` says the same thing deliberately, and is
/// what the UI's own default sends.
const EVERYTHING: &str = r#"{app=~".+"}"#;

/// Shown under `--help`. Written out rather than described because the first question is
/// never "what is the grammar" — it is "what does one of these look like".
const EXAMPLES: &str = r#"Examples:
  telemetryd query
      Everything from the last hour, newest first. Start here: it answers
      "is anything arriving at all" without composing a query.

  telemetryd query '{app="checkout"}'
      One application. `app` comes from the OTLP resource attribute
      `service.name`; `telemetryd status` lists the ones that exist.

  telemetryd query '{app="checkout", level="error"}' --since 24h
      Errors only. `level` is derived from OTLP severity and is always
      present: debug, info, warn, error, fatal.

  telemetryd query '{app="checkout"} |= "declined"'
      Lines containing a substring. Case-sensitive, and not a regex.

  telemetryd query '{app=~".+"} |~ "timeout|refused"' --limit 20
      A regex over the line. Label matchers like `app=~` are anchored to
      the whole value; line filters like `|~` are not.

  telemetryd query '{app="api"} | json | level="error"'
      Parse a JSON body and filter on a field inside it. `| logfmt` does
      the same for key=value lines.

  telemetryd query '{app="api"}' --output json | jq -r .body
      One JSON object per line, for piping.

Not supported here: metric queries over logs (`rate`, `count_over_time`,
`sum`). Use the Prometheus API for those — telemetryd names the construct
it refused rather than reporting a syntax error."#;

#[derive(Debug, Args)]
#[command(after_help = EXAMPLES, after_long_help = EXAMPLES)]
pub struct QueryArgs {
    /// A LogQL query. Omit it to see everything, which is the useful default when
    /// the question is "is my data arriving".
    #[arg(value_name = "LOGQL")]
    pub query: Option<String>,

    /// How far back to look.
    #[arg(long, default_value = "1h", value_name = "DURATION")]
    pub since: humantime::Duration,

    /// Most recent records first, up to this many.
    #[arg(long, default_value_t = 100)]
    pub limit: usize,

    /// Oldest first. The order to read in when exporting.
    #[arg(long)]
    pub forward: bool,

    #[arg(long, value_enum, default_value_t = Output::Text)]
    pub output: Output,

    /// Base URL of the running instance.
    #[arg(long, default_value = "http://127.0.0.1:4319", value_name = "URL")]
    pub url: String,

    /// Query token, if the instance requires one.
    ///
    /// Prefer the environment variable: a token passed as an argument is visible in
    /// `ps` output and shell history.
    #[arg(
        long,
        env = "TELEMETRYD_AUTH_QUERY_TOKEN",
        value_name = "TOKEN",
        hide_env_values = true
    )]
    pub token: Option<String>,
}

pub fn run(args: &QueryArgs) -> anyhow::Result<()> {
    // No argument, or an empty one, means everything. Before this, `telemetryd query`
    // was a usage error and `telemetryd query ''` reached the server and came back with
    // "the `query` parameter is required" — a message about an HTTP parameter, to
    // somebody who had typed a command.
    let query = args
        .query
        .as_deref()
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .unwrap_or(EVERYTHING);

    let base = args.url.trim_end_matches('/');
    if base.starts_with("https://") {
        bail!(
            "`telemetryd query` speaks plain HTTP only.\n\
             telemetryd does not terminate TLS — query the instance \
             directly, e.g. --url http://127.0.0.1:4319."
        );
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("reading the clock")?
        .as_nanos();
    let since: std::time::Duration = args.since.into();
    let start = now.saturating_sub(since.as_nanos());

    let url = format!(
        "{base}/loki/api/v1/query_range?query={}&start={start}&end={now}&limit={}&direction={}",
        urlencode(query),
        args.limit,
        if args.forward { "forward" } else { "backward" },
    );

    let mut request = ureq::get(&url).header("user-agent", super::status::user_agent());
    // Same as `status`: a local instance's token is in the configuration on this
    // machine, and only a loopback target is ever given it.
    let token = args
        .token
        .clone()
        .or_else(|| super::local_token::find(&args.url, super::local_token::Surface::Query));
    if let Some(token) = &token {
        request = request.header("authorization", &format!("Bearer {token}"));
    }

    // `http_status_as_error(false)` so a 4xx arrives as a response with a body rather
    // than as an error without one. The body is the useful part: an unsupported LogQL
    // construct names itself there, and inventing a vaguer message on top of it helps
    // nobody.
    let body = match request
        .config()
        .tls_config(telemetryd_core::http::tls())
        .http_status_as_error(false)
        .build()
        .call()
    {
        Ok(mut response) => {
            let status = response.status().as_u16();
            let body = response.body_mut().read_to_string()?;
            if status != 200 {
                bail!(explain(status, base, &body));
            }
            body
        }
        Err(error) => {
            bail!(
                "could not reach telemetryd at {base}: {error}\n\
                 Is it running? `telemetryd status --url {base}` checks."
            );
        }
    };

    let parsed: Value = serde_json::from_str(&body).context("parsing the response")?;
    print(&parsed, args.output, args.forward);
    Ok(())
}

/// Write one line, or stop quietly when the reader has gone.
///
/// `crate::out::outln!` panics on a closed pipe — "failed printing to stdout: Broken pipe" — and
/// this command's whole purpose is to be piped: its own documentation says to send it
/// through `grep` and `wc`. `telemetryd query … | head -20` printed a panic and a
/// backtrace instead of twenty lines.
///
/// Exiting 0 is what every other Unix tool does when its reader leaves. The alternative
/// is treating a perfectly ordinary shell idiom as a crash.
fn line(out: &mut impl Write, text: &str) -> std::ops::ControlFlow<()> {
    match writeln!(out, "{text}") {
        Ok(()) => std::ops::ControlFlow::Continue(()),
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => std::ops::ControlFlow::Break(()),
        Err(_) => std::ops::ControlFlow::Break(()),
    }
}

/// One record, lifted out of the per-stream response so the whole result can be ordered
/// by time.
struct Row<'a> {
    nanos: i128,
    timestamp: &'a str,
    text: &'a str,
    labels: Option<&'a serde_json::Map<String, Value>>,
    metadata: Option<&'a Value>,
}

fn print(response: &Value, output: Output, forward: bool) {
    let streams = response
        .get("data")
        .and_then(|data| data.get("result"))
        .and_then(Value::as_array);
    let Some(streams) = streams else {
        return;
    };

    // Flatten before printing.
    //
    // The Loki response is grouped by stream, and printing it in that shape emits every
    // record of one stream, then every record of the next. A single application with
    // both info and error lines is two streams, so `telemetryd query` printed
    // 10:16:35, 10:16:32, 10:16:34, 10:16:33 — timestamps on every line and no order
    // between them. On a command whose stated job is debugging over SSH, that is the
    // output being wrong, not merely unsorted.
    let mut rows: Vec<Row<'_>> = Vec::new();
    for stream in streams {
        let labels = stream.get("stream").and_then(Value::as_object);
        let Some(values) = stream.get("values").and_then(Value::as_array) else {
            continue;
        };
        for entry in values {
            let Some(pair) = entry.as_array() else {
                continue;
            };
            let timestamp = pair.first().and_then(Value::as_str).unwrap_or("0");
            rows.push(Row {
                // Unparseable sorts oldest rather than being dropped: a record whose
                // timestamp we cannot read is still a record somebody is looking for.
                nanos: timestamp.parse::<i128>().unwrap_or(0),
                timestamp,
                text: pair.get(1).and_then(Value::as_str).unwrap_or(""),
                labels,
                metadata: pair.get(2),
            });
        }
    }
    // Same order the request asked the server for, applied across streams rather than
    // within one.
    if forward {
        rows.sort_by_key(|row| row.nanos);
    } else {
        rows.sort_by_key(|row| std::cmp::Reverse(row.nanos));
    }

    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());

    for row in rows {
        match output {
            Output::Text => {
                let app = row
                    .labels
                    .and_then(|l| l.get("app"))
                    .and_then(Value::as_str)
                    .unwrap_or("-");
                if line(
                    &mut out,
                    &format!("{} {app} {}", rfc3339(row.timestamp), row.text),
                )
                .is_break()
                {
                    return;
                }
            }
            Output::Json => {
                // One object per line: the shape every other tool on the machine
                // can read, and the reason this counts as an export path.
                let mut record = serde_json::Map::new();
                record.insert("timestamp".into(), Value::String(rfc3339(row.timestamp)));
                record.insert(
                    "timestamp_nanos".into(),
                    Value::String(row.timestamp.into()),
                );
                record.insert("line".into(), Value::String(row.text.into()));
                if let Some(labels) = row.labels {
                    record.insert("labels".into(), Value::Object(labels.clone()));
                }
                // The third tuple element is structured metadata, when present.
                if let Some(extra) = row.metadata {
                    record.insert("metadata".into(), extra.clone());
                }
                if line(&mut out, &Value::Object(record).to_string()).is_break() {
                    return;
                }
            }
        }
    }
}

/// Nanoseconds since the epoch, as a readable timestamp.
///
/// Falls back to the raw value rather than losing it: a timestamp we cannot format is
/// still the only copy of when something happened.
fn rfc3339(nanos: &str) -> String {
    let Ok(nanos) = nanos.parse::<i128>() else {
        return nanos.to_owned();
    };
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()
        .and_then(|at| {
            at.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| nanos.to_string())
}

/// Turn a failed response into something worth reading.
///
/// The server's own message is more specific than anything that could be invented
/// here — telemetryd implements a subset of each query language and names the
/// construct it does not support — so it is quoted rather than replaced.
fn explain(code: u16, base: &str, body: &str) -> String {
    let detail = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|parsed| {
            parsed
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| body.trim().chars().take(400).collect());

    match code {
        400 => format!("the query was rejected: {detail}"),
        401 => {
            "authentication required. Set TELEMETRYD_AUTH_QUERY_TOKEN, or pass --token.".to_owned()
        }
        404 => format!("{base} answered 404. Is that a telemetryd instance?"),
        _ => format!("telemetryd answered {code}: {detail}"),
    }
}

/// Percent-encode a query string value.
///
/// Written out rather than pulled in: the alternative is a dependency for one
/// function, on a binary whose whole promise is being one static file.
pub fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len() * 3);
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[usize::from(byte >> 4)] as char);
                out.push(HEX[usize::from(byte & 0x0f)] as char);
            }
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The defect: `println!` panics on a closed pipe, and this command exists to be
    /// piped — its own module documentation says to send it through `grep` and `wc`.
    /// `telemetryd query … | head -20` printed a panic and a backtrace.
    #[test]
    fn a_closed_pipe_stops_output_rather_than_panicking() {
        struct Closed;
        impl Write for Closed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        assert!(
            line(&mut Closed, "anything").is_break(),
            "a reader that has gone away means stop, not crash"
        );

        // And an open one keeps going, or the fix would be worse than the bug.
        let mut open = Vec::new();
        assert!(line(&mut open, "hello").is_continue());
        assert_eq!(open, b"hello\n");
    }

    #[test]
    fn a_query_survives_the_url() {
        // Braces, quotes, spaces and pipes all have meaning in a URL and none in
        // LogQL — getting this wrong turns a valid query into a 400.
        let encoded = urlencode(r#"{app="checkout"} |= "a b""#);
        assert!(!encoded.contains('{'), "{encoded}");
        assert!(!encoded.contains('"'), "{encoded}");
        assert!(!encoded.contains(' '), "{encoded}");
        assert!(
            encoded.contains("%7B") && encoded.contains("%22"),
            "{encoded}"
        );
    }

    #[test]
    fn unreserved_characters_are_left_alone() {
        assert_eq!(urlencode("abcXYZ019-_.~"), "abcXYZ019-_.~");
    }

    #[test]
    fn a_timestamp_becomes_readable_and_never_disappears() {
        assert!(rfc3339("1760000000000000000").starts_with("2025-"));
        // Unparseable input is returned rather than dropped: it is still the only
        // record of when the thing happened.
        assert_eq!(rfc3339("not-a-number"), "not-a-number");
    }
}
