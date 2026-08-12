//! `GET /` — what this server is, and where its documentation lives.
//!
//! # Why
//!
//! Anyone who reaches a telemetryd deployment in a browser reaches it at `/`, and until
//! now that was a bare `404` — indistinguishable from a broken proxy, a wrong port or a
//! typo'd hostname. The first question a person has at that moment is "is this the right
//! thing, and is it up", and the second is "so what do I call". Both are answerable
//! without authenticating, because neither answer is about the deployment.
//!
//! # What it deliberately does not say
//!
//! No version, no uptime, no app names, no counts, no configuration. This endpoint is
//! open on an internet-facing port, and a version string there is a free lookup from
//! "which build is this" to "which advisories apply to it". The version is available on
//! `/status` and `/api/v1/status/buildinfo`, both of which want a token.
//!
//! # Why a table rather than prose
//!
//! [`SURFACES`] is the single source of truth for both renderings, and the contract test
//! walks it against the real router. An index that drifts from the routes it lists is
//! worse than no index, and this is the cheapest way to make drift fail the build.

use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};

/// One authentication surface, with the routes it guards.
#[derive(Debug)]
pub struct Surface {
    pub title: &'static str,
    /// The credential this surface wants, phrased as the operator thinks of it.
    pub guard: &'static str,
    pub routes: &'static [Route],
}

/// One line of the table. `paths` are alternatives for the same capability.
#[derive(Debug)]
pub struct Route {
    pub method: &'static str,
    pub paths: &'static [&'static str],
    pub note: &'static str,
}

/// The routes worth naming on a landing page.
///
/// A curated subset, not the contract — label and tag endpoints are omitted because
/// nobody arrives at `/` looking for them. `COMPATIBILITY.md` is the exhaustive list,
/// and the page says so rather than implying this table is complete.
pub const SURFACES: &[Surface] = &[
    Surface {
        title: "Send telemetry",
        guard: "ingest token",
        routes: &[
            Route {
                method: "POST",
                paths: &["/v1/logs", "/v1/traces", "/v1/metrics"],
                note: "OTLP over HTTP, JSON encoding — no protobuf, no gRPC",
            },
            Route {
                method: "POST",
                paths: &["/api/v1/write"],
                note: "Prometheus remote_write",
            },
        ],
    },
    Surface {
        title: "Read it back",
        guard: "query token",
        routes: &[
            Route {
                method: "GET",
                paths: &["/loki/api/v1/query_range"],
                note: "logs, LogQL subset",
            },
            Route {
                method: "GET",
                paths: &["/api/v1/query", "/api/v1/query_range"],
                note: "metrics, PromQL subset",
            },
            Route {
                method: "GET",
                paths: &["/api/search", "/api/traces/{trace_id}"],
                note: "traces, TraceQL subset",
            },
            Route {
                method: "GET",
                paths: &["/api/v1/export"],
                note: "everything, unaggregated, up to 200,000 records",
            },
            Route {
                method: "GET",
                paths: &["/loki/api/v1/tail"],
                note: "live tail, WebSocket",
            },
        ],
    },
    Surface {
        title: "Operate",
        guard: "admin token",
        routes: &[
            Route {
                method: "GET",
                paths: &["/status"],
                note: "what this instance holds, per app",
            },
            Route {
                method: "GET",
                paths: &["/metrics"],
                note: "Prometheus exposition of telemetryd itself",
            },
        ],
    },
    Surface {
        title: "Always open",
        guard: "no token",
        routes: &[
            Route {
                method: "GET",
                paths: &["/healthz"],
                note: "liveness, and never anything else",
            },
            Route {
                method: "GET",
                paths: &["/"],
                note: "this page",
            },
        ],
    },
];

/// Documentation, in the order a newcomer needs it.
const DOCS: &[(&str, &str)] = &[
    (
        "Quickstart",
        "https://github.com/cboxdk/telemetryd/blob/main/docs/quickstart.md",
    ),
    (
        "The API subset, endpoint by endpoint",
        "https://github.com/cboxdk/telemetryd/blob/main/COMPATIBILITY.md",
    ),
    (
        "Cookbook",
        "https://github.com/cboxdk/telemetryd/tree/main/docs/cookbook",
    ),
    (
        "Written for an agent to read",
        "https://github.com/cboxdk/telemetryd/blob/main/llms.txt",
    ),
];

/// The rest of the ecosystem this server was built for.
const CBOX: &[(&str, &str, &str)] = &[
    (
        "laravel-telemetry",
        "https://github.com/cboxdk/laravel-telemetry",
        "the exporter — point a Laravel app here",
    ),
    (
        "laravel-telemetry-ui",
        "https://github.com/cboxdk/laravel-telemetry-ui",
        "dashboards over these APIs",
    ),
    (
        "Cbox on GitHub",
        "https://github.com/cboxdk",
        "everything else",
    ),
];

/// Serve HTML to a browser and plain text to everything else.
///
/// Negotiated on `Accept` rather than on a user-agent string, and text is the default:
/// the overwhelmingly common non-browser caller is `curl`, which sends `*/*`, and a
/// screenful of markup is not what that caller wanted.
pub async fn index(headers: HeaderMap) -> Response {
    let wants_html = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"));

    if wants_html {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            html(),
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            text(),
        )
            .into_response()
    }
}

fn text() -> String {
    use std::fmt::Write as _;

    let mut out = String::from(
        "telemetryd — a single-binary observability backend.\n\
         \n\
         You have reached the server. It has no web interface of its own; what follows\n\
         are the APIs it answers. Send a credential as: Authorization: Bearer <token>\n",
    );

    for surface in SURFACES {
        let _ = write!(out, "\n  {} ({})\n", surface.title, surface.guard);
        for route in surface.routes {
            let _ = writeln!(
                out,
                "    {:<5} {:<44} {}",
                route.method,
                route.paths.join("  "),
                route.note
            );
        }
    }

    out.push_str("\nThe table above is the short version. The full surface, including what is\ndeliberately not supported, is in COMPATIBILITY.md.\n\n");
    for (title, url) in DOCS {
        let _ = writeln!(out, "  {title:<38} {url}");
    }
    out.push_str("\nFrom Cbox\n");
    for (title, url, note) in CBOX {
        let _ = writeln!(out, "  {title:<22} {url:<48} {note}");
    }
    out
}

fn html() -> String {
    use std::fmt::Write as _;

    let mut tables = String::new();
    for surface in SURFACES {
        let _ = write!(
            tables,
            "<section><h2>{}<span class=\"guard\">{}</span></h2><table>",
            surface.title, surface.guard
        );
        for route in surface.routes {
            let paths = route
                .paths
                .iter()
                .map(|path| format!("<code>{path}</code>"))
                .collect::<Vec<_>>()
                .join(" ");
            let _ = write!(
                tables,
                "<tr><th>{}</th><td>{}</td><td class=\"note\">{}</td></tr>",
                route.method, paths, route.note
            );
        }
        tables.push_str("</table></section>");
    }

    let docs = DOCS.iter().fold(String::new(), |mut acc, (title, url)| {
        let _ = write!(acc, "<li><a href=\"{url}\">{title}</a></li>");
        acc
    });

    let cbox = CBOX
        .iter()
        .fold(String::new(), |mut acc, (title, url, note)| {
            let _ = write!(
                acc,
                "<li><a href=\"{url}\">{title}</a> <span class=\"note\">{note}</span></li>"
            );
            acc
        });

    // Self-contained on purpose: one document, no stylesheet, no font, no script, no
    // image. An observability backend that phones a CDN on its own landing page would
    // be contradicting the thing it is for.
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<meta name=\"robots\" content=\"noindex\">\
<title>telemetryd</title><style>\
:root{{color-scheme:light dark;--fg:#101418;--dim:#5c6773;--line:#e3e8ee;--code:#f5f7fa;--link:#0b5fd0}}\
@media(prefers-color-scheme:dark){{:root{{--fg:#e6edf3;--dim:#8b98a5;--line:#232a33;--code:#161b22;--link:#6cb0ff}}}}\
*{{box-sizing:border-box}}\
body{{margin:0;padding:3rem 1.25rem 5rem;color:var(--fg);\
font:16px/1.6 ui-sans-serif,-apple-system,\"Segoe UI\",Roboto,sans-serif}}\
main{{max-width:52rem;margin:0 auto}}\
h1{{font-size:1.5rem;margin:0 0 .25rem}}\
.lede{{color:var(--dim);margin:0 0 2.5rem;max-width:38rem}}\
h2{{font-size:.8rem;text-transform:uppercase;letter-spacing:.07em;color:var(--dim);\
margin:2.25rem 0 .6rem;display:flex;align-items:baseline;gap:.6rem}}\
.guard{{font-size:.7rem;text-transform:none;letter-spacing:0;\
border:1px solid var(--line);border-radius:99px;padding:.05rem .5rem}}\
table{{width:100%;border-collapse:collapse;table-layout:fixed}}\
tr{{border-top:1px solid var(--line)}}\
th,td{{text-align:left;padding:.5rem .75rem .5rem 0;vertical-align:baseline;font-weight:400}}\
th{{color:var(--dim);font-size:.75rem;width:3.5rem;letter-spacing:.03em}}\
td:first-of-type{{width:19.5rem}}\
code{{background:var(--code);border-radius:4px;padding:.1rem .35rem;\
font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;white-space:nowrap}}\
.note{{color:var(--dim);font-size:.85rem}}\
a{{color:var(--link)}}\
ul{{list-style:none;padding:0;margin:0}}\
li{{padding:.3rem 0}}\
footer{{margin-top:3rem;padding-top:1.5rem;border-top:1px solid var(--line);\
color:var(--dim);font-size:.85rem}}\
@media(max-width:34rem){{td:last-child{{display:none}}code{{white-space:normal}}\
table{{table-layout:auto}}td:first-of-type{{width:auto}}}}\
</style></head><body><main>\
<h1>telemetryd</h1>\
<p class=\"lede\">A single-binary observability backend. You have reached the server; it \
has no web interface of its own. Send a credential as \
<code>Authorization: Bearer &lt;token&gt;</code>.</p>\
{tables}\
<section><h2>Documentation</h2><ul>{docs}</ul>\
<p class=\"note\">The tables above are the short version. The full surface, including \
what is deliberately not supported, is in COMPATIBILITY.md.</p></section>\
<section><h2>From Cbox</h2><ul>{cbox}</ul></section>\
<footer>No version, uptime or contents are reported here — this page is open, and those \
are not. They are on <code>/status</code>, which wants the admin token.</footer>\
</main></body></html>"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neither_rendering_leaks_what_the_page_promises_to_withhold() {
        // The footer makes a promise. This is the test that keeps it: the crate version
        // must not appear in either body, however the page is rendered.
        let version = env!("CARGO_PKG_VERSION");
        for body in [text(), html()] {
            assert!(
                !body.contains(version),
                "the open index printed the version"
            );
        }
    }

    #[test]
    fn every_documented_link_is_absolute_and_https() {
        // A relative link on this page resolves against the deployment's own host, where
        // the documentation does not live, and produces a 404 from telemetryd itself.
        for url in DOCS
            .iter()
            .map(|(_, url)| *url)
            .chain(CBOX.iter().map(|(_, url, _)| *url))
        {
            assert!(url.starts_with("https://"), "{url}");
        }
    }
}
