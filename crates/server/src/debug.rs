//! `GET /debug` — a page that answers "is my data arriving", without composing a query.
//!
//! # Why
//!
//! The first minutes after pointing an application at telemetryd, the only question is
//! whether anything is landing. Answering it required constructing a LogQL selector,
//! URL-encoding it, and guessing a time window — three chances to be wrong about the
//! tooling while trying to learn something about the data. Every one of those went wrong
//! at least once while this file was being written: a window against a fixed timestamp
//! that retention had already reaped, `since` where the endpoint wanted `start`, an empty
//! result that meant the query was malformed rather than that nothing had arrived.
//!
//! So the page shows the last lines with no input at all, and the query box is for
//! narrowing rather than for starting.
//!
//! # It is a debug page, not a dashboard
//!
//! No charts, no time picker beyond three choices, and logs only. Metrics and traces
//! belong to `laravel-telemetry-ui`, which is the product; adding them here would start a
//! second one. What this owes the reader is a fast answer to one question.
//!
//! # Authentication: a token you type, on a page you can reach
//!
//! This is meant to be opened from a browser against a real deployment, so it signs you
//! in rather than answering `401` and leaving you to construct a header. No token
//! configured — a default local instance — and it is simply open.
//!
//! The first version of this file put the page behind the router's bearer-token layer,
//! which made it unreachable from anywhere but the machine it runs on. That was a scoping
//! decision presented as a security property, and it was not mine to make.
//!
//! The credential is the **admin token**, falling back to the query token when no admin
//! token is set — the same rule `/status` and `/metrics` already use, so there is one
//! answer to "what does the operator's credential open".
//!
//! # Why a cookie, and why that is not the thing I objected to
//!
//! The token is held in an `HttpOnly` cookie, so no script can read it — which is the
//! whole objection to putting a bearer token in `localStorage`, answered by the mechanism
//! rather than by refusing the feature. `SameSite=Strict` keeps it off cross-site
//! requests, `Secure` is set whenever the request arrived over TLS (including through a
//! proxy that says so), and the token never appears in a URL, where it would land in
//! access logs and browser history.
//!
//! # Escaping is load-bearing here
//!
//! This renders log bodies, and log bodies are attacker-controlled by definition: that is
//! what a telemetry backend is for. Anyone who can make an application log a string can
//! choose that string. Everything interpolated goes through one escaping helper, the page declares
//! `default-src 'none'`, and there is no script and no inline handler — so even a mistake
//! in the escaping has nowhere to execute.

use axum::extract::{Form, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::error::ApiError;
use crate::state::AppState;

/// Where the signed-in token lives between requests.
const COOKIE: &str = "telemetryd_debug";

#[derive(Debug, Deserialize)]
pub struct LoginForm {
    pub token: String,
}

/// What the operator's credential opens here.
///
/// The admin token, falling back to the query token when no admin token is configured —
/// identical to `/status` and `/metrics`, so an operator has one credential to think
/// about rather than a rule per page.
fn accepts(state: &AppState, presented: &str) -> bool {
    if !state.admin_tokens.is_empty() {
        return state.admin_tokens.verify(presented);
    }
    state.query_tokens.verify(presented)
}

/// Whether anything guards this instance at all.
fn guarded(state: &AppState) -> bool {
    !state.admin_tokens.is_empty() || !state.query_tokens.is_empty()
}

/// The credential on this request: the header if a tool sent one, otherwise the cookie a
/// browser is carrying. Never a query parameter — that is the one place a token must not
/// go, because it survives in access logs, referrers and history.
fn presented(headers: &HeaderMap) -> Option<String> {
    if let Some(bearer) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    {
        return Some(bearer.trim().to_owned());
    }
    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find(|(name, _)| *name == COOKIE)
        .map(|(_, value)| value.to_owned())
}

/// `Secure` only when the connection actually was, or a plain-HTTP deployment would set a
/// cookie the browser then refuses to send back — and the page would never sign in.
fn over_tls(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|proto| proto.eq_ignore_ascii_case("https"))
}

/// The page shown when there is no valid credential.
///
/// A form rather than a `401` body, because this is opened by a person in a browser and
/// the answer to "you need a token" is a place to put one. The token posts in a form
/// body, never a query string — a URL is logged by the proxy, kept in history and sent
/// in a `Referer`.
fn sign_in(problem: &str) -> String {
    let banner = if problem.is_empty() {
        String::new()
    } else {
        format!("<p class=\"err\">{}</p>", escape(problem))
    };
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<meta name=\"robots\" content=\"noindex\"><title>telemetryd debug</title>\
<style>{STYLE}</style></head><body><main style=\"max-width:26rem\">\
<h1>telemetryd debug</h1>\
<p class=\"lede\">This instance requires a token. Use the <strong>admin</strong> token, \
or the query token if no admin token is configured — the same credential \
<code>/status</code> takes.</p>\
{banner}\
<form method=\"post\" action=\"/debug/login\">\
<input name=\"token\" type=\"password\" placeholder=\"admin token\" \
aria-label=\"admin token\" autocomplete=\"current-password\" autofocus>\
<button type=\"submit\">Sign in</button></form>\
<footer>Kept in a cookie that scripts cannot read, scoped to this page, and marked \
<code>Secure</code> when you arrived over TLS. It is never put in a URL.</footer>\
</main></body></html>"
    )
}

/// `POST /debug/login`
pub async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> Response {
    if !accepts(&state, form.token.trim()) {
        state
            .metrics
            .incr("telemetryd_auth_failures_total", &[("surface", "debug")]);
        // Same page, same status shape as any other refusal here: a message beside the
        // field rather than a bare 401 the browser renders as a wall of text.
        return (
            StatusCode::UNAUTHORIZED,
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            sign_in("That token was not accepted."),
        )
            .into_response();
    }

    let secure = if over_tls(&headers) { "; Secure" } else { "" };
    (
        StatusCode::SEE_OTHER,
        [
            (header::LOCATION, "/debug".to_owned()),
            (
                header::SET_COOKIE,
                format!(
                    "{COOKIE}={}; HttpOnly; SameSite=Strict; Path=/debug{secure}",
                    form.token.trim()
                ),
            ),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
        (),
    )
        .into_response()
}

/// `GET /debug/logout`
pub async fn logout(headers: HeaderMap) -> Response {
    let secure = if over_tls(&headers) { "; Secure" } else { "" };
    (
        StatusCode::SEE_OTHER,
        [
            (header::LOCATION, "/debug".to_owned()),
            (
                header::SET_COOKIE,
                format!("{COOKIE}=; HttpOnly; SameSite=Strict; Path=/debug; Max-Age=0{secure}"),
            ),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
        (),
    )
        .into_response()
}

/// Lines shown. Enough to see a pattern, small enough that the page stays readable and
/// the query behind it stays cheap.
const LINES: usize = 100;

#[derive(Debug, Deserialize)]
pub struct DebugParams {
    /// A query in the language of the selected signal. Absent means everything.
    #[serde(default)]
    pub q: Option<String>,
    /// Minutes to look back. Three choices rather than a picker.
    #[serde(default)]
    pub mins: Option<u64>,
    /// Which signal to show.
    #[serde(default)]
    pub signal: Signal,
}

/// The three tabs.
///
/// Separate rather than one merged timeline: each has its own query language, and a
/// single box that sometimes takes LogQL and sometimes TraceQL would be a worse version
/// of three that each take one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Signal {
    #[default]
    Logs,
    Traces,
    Metrics,
}

impl Signal {
    const fn slug(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Traces => "traces",
            Self::Metrics => "metrics",
        }
    }

    const fn language(self) -> &'static str {
        match self {
            Self::Logs => "LogQL",
            Self::Traces => "TraceQL",
            Self::Metrics => "PromQL",
        }
    }

    /// Everything, in this signal's language. Each is spelled so the parser accepts it —
    /// `{}` is a valid TraceQL spanset but a refused LogQL selector, which is exactly the
    /// kind of difference a shared default would paper over.
    const fn everything(self) -> &'static str {
        match self {
            Self::Logs => r#"{app=~".+"}"#,
            Self::Traces => "{}",
            Self::Metrics => "",
        }
    }
}

const WINDOWS: &[(u64, &str)] = &[(15, "15 min"), (60, "1 hour"), (1440, "24 hours")];

/// HTML text escaping.
///
/// Applied to every interpolated value without exception — log bodies, label values, and
/// the query the reader typed, which comes back to them in the input field. Ampersand
/// first, or it would double-escape the entities produced after it.
fn escape(raw: &str) -> String {
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Percent-encode for a URL query value.
fn urlencode(raw: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(raw.len());
    for byte in raw.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

/// One row of the table, whichever signal produced it.
///
/// Four columns for all three: time, who, a short classifier, and the detail. A trace is
/// not a log line, but "when, from what, how did it go, what was it" is the same question
/// asked three times, and one shape keeps the page readable.
struct Row {
    nanos: i128,
    who: String,
    tag: String,
    detail: String,
}

pub async fn debug(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<DebugParams>,
) -> Result<Response, ApiError> {
    // Sign-in before anything is read. An instance with no tokens is open, exactly as
    // every other read surface is.
    let signed_in = guarded(&state);
    if signed_in {
        let ok = presented(&headers).is_some_and(|token| accepts(&state, &token));
        if !ok {
            return Ok((
                StatusCode::UNAUTHORIZED,
                [
                    (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                sign_in(""),
            )
                .into_response());
        }
    }

    let signal = params.signal;
    let mins = params.mins.unwrap_or(15).clamp(1, 10_080);
    let typed = params.q.unwrap_or_default();
    let query = typed.trim();

    let now = telemetryd_store::now_nanos();
    let start = now.saturating_sub(mins.saturating_mul(60).saturating_mul(1_000_000_000));

    let store = std::sync::Arc::clone(&state.store);
    let owned = query.to_owned();
    // On a blocking thread for the same reason every other read is: scanning opens
    // Parquet files, and a runtime worker is the wrong place for that.
    let outcome = tokio::task::spawn_blocking(move || collect(&store, signal, &owned, start, now))
        .await
        .map_err(|e| telemetryd_core::Error::Config(format!("debug query panicked: {e}")))?;

    let (rows, suggestions, error) = match outcome {
        Ok((rows, suggestions)) => (rows, suggestions, None),
        // Shown beside the input rather than returned as a 400. The page exists because a
        // mistake in the tooling should not look like an absence of data, and a parse
        // error is the most common mistake there is.
        Err(problem) => (Vec::new(), Vec::new(), Some(problem.to_string())),
    };

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            // No script, no image, no stylesheet, no connection — from a page that
            // renders text an attacker chose. The escaping is the defence; this is what
            // stands there if the escaping is ever wrong. Autocomplete is a `<datalist>`
            // precisely so this can stay as it is.
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'",
            ),
            (header::CACHE_CONTROL, "no-store"),
            (header::VARY, "Authorization"),
        ],
        page(
            &rows,
            &suggestions,
            signal,
            query,
            mins,
            error.as_deref(),
            signed_in,
        ),
    )
        .into_response())
}

/// Run the signal's query and gather the suggestions for its input field.
///
/// Suggestions come from this instance's own labels rather than from a fixed list: the
/// useful completion is the name of an application that is actually sending, and no
/// generic example can know it.
fn collect(
    store: &telemetryd_store::Store,
    signal: Signal,
    query: &str,
    start: u64,
    end: u64,
) -> telemetryd_core::Result<(Vec<Row>, Vec<String>)> {
    let query = if query.is_empty() {
        signal.everything()
    } else {
        query
    };

    match signal {
        Signal::Logs => logs(store, query, start, end),
        Signal::Traces => traces(store, query, start, end),
        Signal::Metrics => metrics(store, query, start, end),
    }
}

fn logs(
    store: &telemetryd_store::Store,
    query: &str,
    start: u64,
    end: u64,
) -> telemetryd_core::Result<(Vec<Row>, Vec<String>)> {
    {
        {
            let parsed = telemetryd_query::logql::parse(query)?;
            let response = telemetryd_query::loki::query_range(
                store.logs(),
                &telemetryd_query::loki::QueryRangeRequest {
                    query: parsed,
                    start_nanos: start,
                    end_nanos: end,
                    limit: LINES,
                    direction: telemetryd_query::loki::Direction::Backward,
                },
            )?;
            let mut rows = Vec::new();
            for stream in response.data.result {
                let app = stream
                    .stream
                    .get("app")
                    .map_or_else(|| "-".to_owned(), ToOwned::to_owned);
                let level = stream
                    .stream
                    .get("level")
                    .map_or_else(String::new, ToOwned::to_owned);
                for entry in stream.values {
                    rows.push(Row {
                        nanos: entry.timestamp().parse::<i128>().unwrap_or(0),
                        who: app.clone(),
                        tag: level.clone(),
                        detail: entry.line().to_owned(),
                    });
                }
            }
            let apps = store.logs().label_values("app", start, end)?;
            let mut suggestions: Vec<String> = apps
                .iter()
                .map(|app| format!(r#"{{app="{app}"}}"#))
                .collect();
            for app in &apps {
                suggestions.push(format!(r#"{{app="{app}", level="error"}}"#));
            }
            suggestions.push(r#"{app=~".+"} |= "error""#.to_owned());
            suggestions.push(r#"{app=~".+"} | json"#.to_owned());
            Ok((rows, suggestions))
        }
    }
}

fn traces(
    store: &telemetryd_store::Store,
    query: &str,
    start: u64,
    end: u64,
) -> telemetryd_core::Result<(Vec<Row>, Vec<String>)> {
    {
        {
            let parsed = telemetryd_query::traceql::parse(query)?;
            let response = telemetryd_query::tempo::search(
                store.traces(),
                &telemetryd_query::tempo::SearchRequest {
                    query: parsed,
                    start_nanos: start,
                    end_nanos: end,
                    limit: LINES,
                    min_duration_nanos: None,
                    max_duration_nanos: None,
                },
            )?;
            let rows = response
                .traces
                .into_iter()
                .map(|trace| Row {
                    nanos: trace.start_time_unix_nano.parse::<i128>().unwrap_or(0),
                    who: trace.root_service_name,
                    // Duration is the classifier for a trace the way severity is for a
                    // log line: it is what you scan the column for.
                    tag: format!("{:.0}ms", trace.duration_ms),
                    detail: format!("{}  {}", trace.root_trace_name, trace.trace_id),
                })
                .collect();
            let suggestions = vec![
                "{}".to_owned(),
                "{ status = error }".to_owned(),
                "{ duration > 100ms }".to_owned(),
                "{ duration > 1s }".to_owned(),
            ];
            Ok((rows, suggestions))
        }
    }
}

fn metrics(
    store: &telemetryd_store::Store,
    query: &str,
    start: u64,
    end: u64,
) -> telemetryd_core::Result<(Vec<Row>, Vec<String>)> {
    {
        {
            // Not a chart and not a range query: the names that exist, and the newest
            // sample of each. That is the whole "is it arriving" question for a metric,
            // and a graph is `laravel-telemetry-ui`'s job.
            let names = store.metrics().label_values(
                telemetryd_core::metric::METRIC_NAME_LABEL,
                start,
                end,
            )?;
            let mut rows = Vec::new();
            for name in names.iter().take(LINES) {
                if !query.is_empty() && !name.contains(query) {
                    continue;
                }
                let response = telemetryd_query::prometheus::instant(
                    store.metrics(),
                    &telemetryd_query::prometheus::InstantParams {
                        query: Some(name.clone()),
                        time: None,
                        timeout: None,
                    },
                    end,
                )?;
                let series = response.data.result.len();
                let value = response
                    .data
                    .result
                    .first()
                    .map_or_else(|| "—".to_owned(), |sample| sample.value.1.clone());
                rows.push(Row {
                    nanos: i128::from(end),
                    who: name.clone(),
                    tag: format!("{series} series"),
                    detail: value,
                });
            }
            // Alphabetical, not by time: every row shares the same instant.
            rows.sort_by(|a, b| a.who.cmp(&b.who));
            Ok((rows, names))
        }
    }
}

/// The stylesheet, lifted out of [`page`] because it is half of it and none of its
/// logic. Inline rather than a file: the page must load with `default-src 'none'`, so
/// there is nothing to link to.
const STYLE: &str = ":root{color-scheme:light dark;--fg:#101418;--dim:#5c6773;--line:#e3e8ee;--code:#f5f7fa;--link:#0b5fd0;--err:#b3261e}@media(prefers-color-scheme:dark){:root{--fg:#e6edf3;--dim:#8b98a5;--line:#232a33;--code:#161b22;--link:#6cb0ff;--err:#f2b8b5}}*{box-sizing:border-box}body{margin:0;padding:2rem 1.25rem 4rem;color:var(--fg);font:15px/1.55 ui-sans-serif,-apple-system,\"Segoe UI\",Roboto,sans-serif}main{max-width:78rem;margin:0 auto}h1{font-size:1.15rem;margin:0 0 .2rem}h2{font-size:.75rem;text-transform:uppercase;letter-spacing:.07em;color:var(--dim);margin:1.8rem 0 .5rem}.lede{color:var(--dim);margin:0 0 1.2rem}form{display:flex;gap:.5rem;margin:0 0 .6rem}input{flex:1;padding:.5rem .6rem;border:1px solid var(--line);border-radius:6px;background:var(--code);color:var(--fg);font:13px/1.4 ui-monospace,SFMono-Regular,Menlo,monospace}button{padding:.5rem .9rem;border:1px solid var(--line);border-radius:6px;background:var(--code);color:var(--fg);cursor:pointer}nav{display:flex;gap:.9rem;margin:0 0 1rem;font-size:.85rem}nav.tabs{margin:0 0 1.1rem;font-size:.95rem}nav a{color:var(--link);text-decoration:none}nav a[aria-current]{color:var(--fg);font-weight:600;text-decoration:underline}table{width:100%;border-collapse:collapse;font-size:.85rem}tr{border-top:1px solid var(--line)}th{text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;color:var(--dim);font-weight:500;padding:.3rem .6rem .3rem 0}td{padding:.35rem .6rem .35rem 0;vertical-align:baseline}.t{color:var(--dim);white-space:nowrap;font:12px/1.5 ui-monospace,Menlo,monospace}.a{white-space:nowrap}.l{white-space:nowrap;font-size:.75rem;text-transform:uppercase;color:var(--dim)}.l-error,.l-fatal{color:var(--err);font-weight:600}.m{font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;word-break:break-word}.dim{color:var(--dim)}.err{color:var(--err)}code{background:var(--code);border-radius:4px;padding:.05rem .3rem;font-size:.85em}footer{margin-top:2.5rem;padding-top:1.2rem;border-top:1px solid var(--line);color:var(--dim);font-size:.85rem}";

/// Who is sending, counted from the rows on the page.
///
/// Not from `Store::app_usage`, which walks sealed segments only: on a fresh instance
/// nothing has sealed yet, so that panel read "nothing has arrived" directly above six
/// lines that had — on exactly the instance this page exists to serve. Counting what is
/// displayed also guarantees the two agree.
fn summary_table(rows: &[Row], signal: Signal, mins: u64) -> String {
    use std::fmt::Write as _;

    let mut counts: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for row in rows {
        *counts.entry(row.who.as_str()).or_default() += 1;
    }

    let mut summary = String::new();
    for (who, count) in &counts {
        // Each entry narrows to itself, in the language of the tab it is on.
        let narrowed = match signal {
            Signal::Logs => format!(r#"{{app="{who}"}}"#),
            Signal::Traces => format!(r#"{{ resource.service.name = "{who}" }}"#),
            Signal::Metrics => (*who).to_owned(),
        };
        let _ = write!(
            summary,
            "<tr><td><a href=\"/debug?signal={}&amp;mins={mins}&amp;q={}\">{}</a></td><td>{count}</td></tr>",
            signal.slug(),
            urlencode(&narrowed),
            escape(who),
        );
    }
    if summary.is_empty() {
        summary.push_str("<tr><td colspan=\"2\" class=\"dim\">Nothing in this window.</td></tr>");
    }
    summary
}

fn record_table(rows: &[Row], failed: bool) -> String {
    use std::fmt::Write as _;

    let mut lines = String::new();
    for row in rows {
        let _ = write!(
            lines,
            "<tr><td class=\"t\">{}</td><td class=\"a\">{}</td>\
             <td class=\"l l-{}\">{}</td><td class=\"m\">{}</td></tr>",
            escape(&stamp(row.nanos)),
            escape(&row.who),
            escape(&row.tag.to_ascii_lowercase()),
            escape(&row.tag),
            escape(&row.detail)
        );
    }
    // Suppressed when the query failed to parse: the banner above already says why the
    // table is empty, and "nothing in this window" would contradict it.
    if lines.is_empty() && !failed {
        let _ = write!(
            lines,
            "<tr><td colspan=\"4\" class=\"dim\">Nothing in this window. Widen it above, \
             or check that the exporter is pointed here — a <code>200</code> from an \
             exporter is not proof, because records are rejected individually and the \
             reason comes back in the response body.</td></tr>"
        );
    }
    lines
}

fn page(
    rows: &[Row],
    suggestions: &[String],
    signal: Signal,
    query: &str,
    mins: u64,
    error: Option<&str>,
    signed_in: bool,
) -> String {
    use std::fmt::Write as _;

    let link = |signal: Signal, mins: u64, query: &str| {
        format!(
            "/debug?signal={}&amp;mins={mins}&amp;q={}",
            signal.slug(),
            urlencode(query)
        )
    };

    // Switching signal drops the query rather than carrying it: a LogQL selector is not
    // a TraceQL spanset, and pasting one into the other's box produces a parse error
    // instead of a tab change.
    let mut tabs = String::new();
    for tab in [Signal::Logs, Signal::Traces, Signal::Metrics] {
        let current = if tab == signal {
            " aria-current=\"page\""
        } else {
            ""
        };
        let _ = write!(
            tabs,
            "<a href=\"{}\"{current}>{}</a>",
            link(tab, mins, ""),
            tab.slug()
        );
    }

    let mut windows = String::new();
    for (value, label) in WINDOWS {
        let current = if *value == mins {
            " aria-current=\"page\""
        } else {
            ""
        };
        let _ = write!(
            windows,
            "<a href=\"{}\"{current}>{label}</a>",
            link(signal, *value, query)
        );
    }

    // Native autocomplete, no JavaScript. A `<datalist>` is why the page can keep
    // `default-src 'none'` and still complete as you type, and the options are this
    // instance's own applications and metric names rather than a fixed list — the
    // completion worth having is the name of something that is actually sending.
    let mut options = String::new();
    for suggestion in suggestions.iter().take(80) {
        let _ = write!(options, "<option value=\"{}\">", escape(suggestion));
    }

    let summary = summary_table(rows, signal, mins);
    let lines = record_table(rows, error.is_some());

    let banner = error.map_or_else(String::new, |problem| {
        format!(
            "<p class=\"err\"><strong>That {} query did not parse.</strong> {}</p>",
            signal.language(),
            escape(problem)
        )
    });

    let (col_who, col_tag, col_detail, placeholder) = match signal {
        Signal::Logs => (
            "app",
            "level",
            "line",
            r#"{app="checkout", level="error"} |= "declined""#,
        ),
        Signal::Traces => (
            "service",
            "duration",
            "name and trace id",
            "{ status = error }",
        ),
        Signal::Metrics => ("metric", "series", "latest value", "http_requests"),
    };

    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<meta name=\"robots\" content=\"noindex\"><title>telemetryd debug</title><style>\
{STYLE}\
</style></head><body><main>\
<h1>telemetryd debug</h1>\
<p class=\"lede\">The last {LINES} records. This answers whether data is arriving, not \
what it means.</p>\
<nav class=\"tabs\">{tabs}</nav>\
<form method=\"get\" action=\"/debug\">\
<input type=\"hidden\" name=\"signal\" value=\"{slug}\">\
<input type=\"hidden\" name=\"mins\" value=\"{mins}\">\
<input name=\"q\" value=\"{query_value}\" list=\"suggestions\" placeholder='{placeholder}' \
aria-label=\"{language} query\" spellcheck=\"false\" autocomplete=\"off\">\
<datalist id=\"suggestions\">{options}</datalist>\
<button type=\"submit\">Run</button></form>\
<nav>{windows}</nav>\
{banner}\
<h2>{col_who}</h2><table>{summary}</table>\
<h2>Records</h2><table>\
<tr><th>time</th><th>{col_who}</th><th>{col_tag}</th><th>{col_detail}</th></tr>\
{lines}</table>\
<footer>{session}The same data is in <code>telemetryd query</code>, which is the thing \
to use over SSH.</footer>\
</main></body></html>",
        slug = signal.slug(),
        language = signal.language(),
        query_value = escape(query),
        session = if signed_in {
            "<a href=\"/debug/logout\">Sign out</a> · "
        } else {
            ""
        },
    )
}

/// Nanoseconds as a readable timestamp, falling back to the raw value.
fn stamp(nanos: i128) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()
        .and_then(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| nanos.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn everything_interpolated_is_escaped() {
        // The reason this page can render log bodies at all. Anyone who can make an
        // application log a string chooses that string, and this one is the string they
        // would choose.
        let hostile = r#"<img src=x onerror="alert(1)">&'"#;
        let escaped = escape(hostile);
        assert!(!escaped.contains('<'), "{escaped}");
        assert!(!escaped.contains('>'), "{escaped}");
        assert!(!escaped.contains('"'), "{escaped}");
        assert!(!escaped.contains('\''), "{escaped}");
        // Ampersand first, or the entities produced above would be escaped again — an
        // `&amp;amp;` on the page rather than the `&` somebody logged.
        assert!(escaped.ends_with("&amp;&#39;"), "{escaped}");
        assert!(!escaped.contains("&amp;amp;"), "double-escaped: {escaped}");
        assert_eq!(escape("plain text"), "plain text");
    }

    #[test]
    fn a_hostile_log_line_reaches_the_page_inert() {
        let rows = vec![Row {
            nanos: 1_700_000_000_000_000_000,
            who: "app".to_owned(),
            tag: "error".to_owned(),
            detail: "<script>alert(1)</script>".to_owned(),
        }];
        let html = page(&rows, &[], Signal::Logs, "", 15, None, false);
        assert!(!html.contains("<script>alert"), "the body was not escaped");
        assert!(html.contains("&lt;script&gt;alert"), "{html}");
    }

    #[test]
    fn the_query_comes_back_into_the_field_escaped() {
        // It is reflected input, which is the classic shape of this bug.
        let html = page(
            &[],
            &[],
            Signal::Logs,
            r#"{app="x"} |= "><script>"#,
            15,
            None,
            false,
        );
        assert!(!html.contains("|= \"><script>"), "reflected unescaped");
        assert!(html.contains("&gt;&lt;script&gt;"), "{html}");
    }

    #[test]
    fn a_query_that_does_not_parse_is_explained_rather_than_returned_as_an_error() {
        // The page exists because a mistake in the tooling should not look like an
        // absence of data.
        let html = page(
            &[],
            &[],
            Signal::Logs,
            "{oops",
            15,
            Some("expected `}`"),
            false,
        );
        assert!(html.contains("did not parse"), "{html}");
        assert!(!html.contains("No records in this window"), "{html}");
    }
}
