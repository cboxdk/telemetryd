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
use serde_json::Value;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Output {
    /// One line per record, timestamp first. For reading.
    Text,
    /// One JSON object per line. For piping.
    Json,
}

#[derive(Debug, Args)]
pub struct QueryArgs {
    /// A LogQL query, e.g. '{app="checkout"} |= "declined"'.
    #[arg(value_name = "LOGQL")]
    pub query: String,

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
    let base = args.url.trim_end_matches('/');
    if base.starts_with("https://") {
        bail!(
            "`telemetryd query` speaks plain HTTP only.\n\
             telemetryd does not terminate TLS (see ADR-004) — query the instance \
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
        urlencode(&args.query),
        args.limit,
        if args.forward { "forward" } else { "backward" },
    );

    let mut request = ureq::get(&url).header("user-agent", super::status::user_agent());
    if let Some(token) = &args.token {
        request = request.header("authorization", &format!("Bearer {token}"));
    }

    // `http_status_as_error(false)` so a 4xx arrives as a response with a body rather
    // than as an error without one. The body is the useful part: an unsupported LogQL
    // construct names itself there, and inventing a vaguer message on top of it helps
    // nobody.
    let body = match request.config().http_status_as_error(false).build().call() {
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
    print(&parsed, args.output);
    Ok(())
}

fn print(response: &Value, output: Output) {
    let streams = response
        .get("data")
        .and_then(|data| data.get("result"))
        .and_then(Value::as_array);
    let Some(streams) = streams else {
        return;
    };

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
            let line = pair.get(1).and_then(Value::as_str).unwrap_or("");

            match output {
                Output::Text => {
                    let app = labels
                        .and_then(|l| l.get("app"))
                        .and_then(Value::as_str)
                        .unwrap_or("-");
                    println!("{} {app} {line}", rfc3339(timestamp));
                }
                Output::Json => {
                    // One object per line: the shape every other tool on the machine
                    // can read, and the reason this counts as an export path.
                    let mut record = serde_json::Map::new();
                    record.insert("timestamp".into(), Value::String(rfc3339(timestamp)));
                    record.insert("timestamp_nanos".into(), Value::String(timestamp.into()));
                    record.insert("line".into(), Value::String(line.into()));
                    if let Some(labels) = labels {
                        record.insert("labels".into(), Value::Object(labels.clone()));
                    }
                    // The third tuple element is structured metadata, when present.
                    if let Some(extra) = pair.get(2) {
                        record.insert("metadata".into(), extra.clone());
                    }
                    println!("{}", Value::Object(record));
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
fn urlencode(value: &str) -> String {
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
