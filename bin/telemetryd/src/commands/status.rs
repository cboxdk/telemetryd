//! `telemetryd status` — pretty-print `/status` from a running instance.

use anyhow::{Context, bail};
use clap::Args;
use serde_json::Value;

#[derive(Debug, Args)]
pub struct StatusArgs {
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

    /// Print the raw JSON instead of the summary.
    #[arg(long)]
    pub json: bool,
}

pub fn run(args: &StatusArgs) -> anyhow::Result<()> {
    let url = format!("{}/status", args.url.trim_end_matches('/'));

    // Plain HTTP only, deliberately: v1 terminates TLS at a reverse proxy,
    // and keeping a TLS stack out of the binary is what keeps the musl builds a
    // straightforward static link.
    // This used to refuse https outright, on the grounds that telemetryd does not
    // terminate TLS. That confused the server with the client: the server
    // still need not, but a TLS-terminating proxy in front is *recommended*, and
    // refusing to talk to one made the recommended deployment unqueryable.
    let mut request = ureq::get(&url).header("user-agent", user_agent());
    if let Some(token) = &args.token {
        request = request.header("authorization", &format!("Bearer {token}"));
    }

    let body = match request
        .config()
        .tls_config(telemetryd_core::http::tls())
        .build()
        .call()
    {
        Ok(mut response) => response
            .body_mut()
            .read_to_string()
            .context("reading the response body")?,
        Err(ureq::Error::StatusCode(401)) => bail!(
            "{url} requires a query token.\n\
             Pass --token, or set TELEMETRYD_AUTH_QUERY_TOKEN."
        ),
        Err(ureq::Error::StatusCode(code)) => bail!("{url} returned HTTP {code}"),
        Err(error) => bail!(
            "could not reach {url}: {error}\n\
             Is telemetryd running? Start it with `telemetryd serve`."
        ),
    };

    if args.json {
        crate::out::outln!("{body}");
        return Ok(());
    }

    let status: Value = serde_json::from_str(&body)
        .with_context(|| format!("{url} did not return JSON — is that a telemetryd instance?"))?;
    print_summary(&status);
    Ok(())
}

pub(crate) fn user_agent() -> String {
    format!("telemetryd/{}", telemetryd_core::VERSION)
}

// Every value here comes from a JSON number and is rendered for a human: byte
// counts and an uptime in seconds, all far below any lossy boundary.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn print_summary(status: &Value) {
    let text = |path: &str| {
        status
            .pointer(path)
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_owned()
    };
    let number = |path: &str| status.pointer(path).and_then(Value::as_f64).unwrap_or(0.0);
    let flag = |path: &str| {
        status
            .pointer(path)
            .and_then(Value::as_bool)
            .unwrap_or(false)
    };

    crate::out::outln!(
        "telemetryd {}, up {}",
        text("/version"),
        humantime::format_duration(std::time::Duration::from_secs(
            number("/uptime_seconds").max(0.0) as u64
        ))
    );
    crate::out::outln!("  listen       {}", text("/listen"));
    crate::out::outln!(
        "  auth         ingest: {}, query: {}",
        text("/auth/ingest"),
        text("/auth/query")
    );
    crate::out::outln!("  data dir     {}", text("/storage/data_dir"));

    let used = number("/storage/disk_used_bytes") as u64;
    let budget = number("/storage/disk_budget_bytes") as u64;
    let ratio = number("/storage/disk_used_ratio") * 100.0;
    crate::out::outln!(
        "  disk         {} of {} ({ratio:.1}%)",
        bytesize::ByteSize::b(used),
        bytesize::ByteSize::b(budget),
    );

    if let Some(wal) = status
        .pointer("/wal")
        .or_else(|| status.pointer("/storage/wal"))
        .and_then(Value::as_object)
    {
        crate::out::outln!("  wal");
        for (signal, stats) in wal {
            crate::out::outln!(
                "    {signal:<8}   {} records, {} segments",
                stats
                    .get("appended_records")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                stats.get("segments").and_then(Value::as_u64).unwrap_or(0),
            );
        }
    }

    // Anything that needs attention goes last, where it is read.
    let mut alerts = Vec::new();
    if flag("/storage/over_budget") {
        alerts.push(format!(
            "disk usage ({}) is over the configured budget ({}); the reaper is \
             dropping the oldest data",
            bytesize::ByteSize::b(used),
            bytesize::ByteSize::b(budget)
        ));
    }
    if flag("/insecure") {
        alerts.push("running with --insecure: unauthenticated access is permitted".to_owned());
    }
    if let Some(truncations) = status
        .pointer("/storage/wal_truncations")
        .and_then(Value::as_array)
        && !truncations.is_empty()
    {
        alerts.push(format!(
            "{} write-ahead log tail(s) were repaired at startup — a previous run \
             crashed and some accepted records were lost",
            truncations.len()
        ));
    }

    if !alerts.is_empty() {
        crate::out::outln!("\nAttention:");
        for alert in alerts {
            crate::out::outln!("  ! {alert}");
        }
    }
}
