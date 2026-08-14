//! `telemetryd status` — pretty-print `/status` from a running instance.

use anyhow::{Context, bail};
use clap::Args;
use serde_json::Value;

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Base URL of the running instance.
    #[arg(long, default_value = "http://127.0.0.1:4319", value_name = "URL")]
    pub url: String,

    /// Admin token, if the instance requires one.
    ///
    /// `/status` is guarded by the admin token; an instance with none configured
    /// accepts the query token here instead.
    ///
    /// Prefer the environment variable: a token passed as an argument is visible in
    /// `ps` output and shell history.
    #[arg(
        long,
        env = "TELEMETRYD_AUTH_ADMIN_TOKEN",
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
    // Nothing supplied, and the instance is on this machine: read the token out of the
    // configuration sitting right there rather than asking someone to paste their own
    // credential back at the tool that wrote it.
    let token = args
        .token
        .clone()
        .or_else(|| super::local_token::find(&args.url, super::local_token::Surface::Admin));

    let mut request = ureq::get(&url).header("user-agent", user_agent());
    if let Some(token) = &token {
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
        // `/status` is guarded by the *admin* token, falling back to the query token
        // only when no admin token is configured. Naming the query one sent people to
        // the wrong credential on every instance `telemetryd init` had set up, which is
        // now all of them.
        Err(ureq::Error::StatusCode(401)) => bail!(
            "{url} requires the admin token.\n\
             Pass --token, or set TELEMETRYD_AUTH_ADMIN_TOKEN.\n\
             \n\
             It guards /status and /metrics, which describe the deployment rather than \
             the telemetry. An instance with no admin token configured accepts the \
             query token here instead."
        ),
        Err(ureq::Error::StatusCode(code)) => bail!("{url} returned HTTP {code}"),
        Err(error) => bail!(
            "could not reach {url}: {error}\n\
             Is telemetryd running? Start it with `telemetryd serve`."
        ),
    };

    let status: Value = serde_json::from_str(&body)
        .with_context(|| format!("{url} did not return JSON — is that a telemetryd instance?"))?;

    // Since 0.34.0 `/status` answers a caller with no admin credential rather than
    // refusing it, with an identity document instead of the deployment picture. That is
    // the right answer for a discovery client and the wrong one here: printing it would
    // render a summary of question marks, which reads as a broken server rather than as
    // a missing token. Detected by the absence of the deployment, not by the presence of
    // `product` — a future field is not what decides this.
    if status.get("storage").is_none() {
        bail!(
            "{url} answered with identity only, which means it did not accept an admin \
             token.\n\
             Pass --token, or set TELEMETRYD_AUTH_ADMIN_TOKEN.\n\
             \n\
             It guards /status and /metrics, which describe the deployment rather than \
             the telemetry. An instance with no admin token configured accepts the \
             query token here instead."
        );
    }

    if args.json {
        crate::out::outln!("{body}");
        return Ok(());
    }

    print_summary(&status);
    Ok(())
}

pub(crate) fn user_agent() -> String {
    format!("telemetryd/{}", telemetryd_core::VERSION)
}

/// Prints the series lines and returns the warning they imply, if any.
///
/// One function because the two are the same reading — the line says where the instance
/// stands and the warning says it out loud once standing there costs data — and splitting
/// them would mean deriving the same four numbers twice.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn report_series(status: &Value) -> Option<String> {
    let number = |pointer: &str| {
        status
            .pointer(pointer)
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
    };
    // Series usage, beside the disk line.
    //
    // The number that matters most on a healthy-looking instance, and the one that was
    // nowhere: a deployment sat at 20,005 series against a 20,000 per-app cap, refusing
    // every new log stream, while `status` reported 0.3% of the disk used and nothing
    // else. The ceiling lived in the configuration file and the usage lived in
    // `/metrics`, and putting the two on one line is the difference between an hour of
    // diagnosis and a glance.
    //
    // Both caps, because either one alone is misleading: the global limit is what the
    // total is measured against, and the per-app limit is what any single app hits first
    // — and it is the per-app one that bites, since one app is usually all there is.
    let series = number("/storage/series_active") as u64;
    let (worst_app, worst) = biggest_app(status);
    let max_series = limit(status, "max_series");
    let max_per_app = limit(status, "max_series_per_app");
    if max_series > 0 {
        crate::out::outln!(
            "  series       {series} of {max_series} ({:.1}%)",
            share(series, max_series)
        );
        if let Some(app) = &worst_app {
            crate::out::outln!(
                "               {worst} of {max_per_app} ({:.1}%) for {app}, the largest",
                share(worst, max_per_app)
            );
        }
    }

    // A full series limit is a silent stop: everything already flowing keeps flowing
    // while every *new* stream is refused, so the instance looks healthy and one signal
    // quietly never arrives. Warned at 90%, because the useful moment is before it is
    // full — after that the data is already being lost.
    let rejected = number("/storage/series_rejected") as u64;
    if max_series > 0 && share(series, max_series) >= 90.0 {
        return Some(series_alert(
            "series",
            series,
            max_series,
            "limits.max_series",
            rejected,
        ));
    } else if let Some(app) = &worst_app
        && max_per_app > 0
        && share(worst, max_per_app) >= 90.0
    {
        return Some(series_alert(
            app,
            worst,
            max_per_app,
            "limits.max_series_per_app",
            rejected,
        ));
    }
    None
}

#[allow(clippy::cast_precision_loss)]
fn share(used: u64, cap: u64) -> f64 {
    if cap == 0 {
        0.0
    } else {
        used as f64 / cap as f64 * 100.0
    }
}

fn limit(status: &Value, key: &str) -> u64 {
    status
        .pointer(&format!("/limits/{key}"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

/// The app holding the most series, which is the one that hits the per-app cap first.
///
/// Read from `storage.series_by_app` rather than from `apps`. `apps` describes what is
/// stored and is derived from sealed segments, so it is empty for the first minutes of an
/// instance's life and lags by a seal interval after that — while the per-app limit is
/// enforced from the first record. An instance that had already refused 31 records
/// reported `apps: []`, which would have made this line and its warning silent in exactly
/// the situation they exist for.
fn biggest_app(status: &Value) -> (Option<String>, u64) {
    let Some(apps) = status
        .pointer("/storage/series_by_app")
        .and_then(Value::as_object)
    else {
        return (None, 0);
    };
    apps.iter()
        .filter_map(|(app, series)| Some((app.clone(), series.as_u64()?)))
        .max_by_key(|(_, series)| *series)
        .map_or((None, 0), |(app, series)| (Some(app), series))
}

/// Says what is full, how full, what to change, and — when it has already cost
/// something — how much. A cardinality ceiling is the one limit whose consequence is
/// invisible from the data, so the number of refused records is the part that makes it
/// concrete.
fn series_alert(what: &str, used: u64, cap: u64, setting: &str, rejected: u64) -> String {
    let cost = if rejected > 0 {
        format!("; {rejected} records refused so far")
    } else {
        String::new()
    };
    format!(
        "{what} is at {used} of {cap} series ({:.0}%){cost}. New streams are refused \
         once it is full, silently from the sender's point of view — raise {setting} or \
         reduce label cardinality",
        share(used, cap)
    )
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

    let series_alert = report_series(status);

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
    alerts.extend(series_alert);
    // The unit file is the one thing an upgrade never touches, so the machine can be
    // running a version whose service integration it does not have. Reported here
    // because this is what an operator runs when something is not behaving.
    let stale = crate::commands::service::missing_directives();
    if !stale.is_empty() {
        alerts.push(format!(
            "the installed service unit predates this version and is missing {}; \
             reinstall it with `sudo telemetryd service install` followed by \
             `sudo systemctl daemon-reload`",
            stale.join(", ")
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
