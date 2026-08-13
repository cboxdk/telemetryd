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
//! # Three renderings, one table
//!
//! HTML for a browser, JSON for a discovery client that sends `Accept:
//! application/json`, plain text for everything else. See [`Identity`] for why the JSON
//! form names the version and the storage format when the first version of this page
//! deliberately did not.
//!
//! # What it still does not say
//!
//! Nothing that describes *this deployment*: no uptime, no app names, no record counts,
//! no disk usage, no retention windows, no listen address, no relay configuration. Those
//! are on `/status` **with the admin token**, and the line between the two is identity
//! versus inventory rather than harmless versus sensitive.
//!
//! Since 0.34.0 the same [`Identity`] is also what `/status` answers a request that
//! holds no admin credential, so a client that went looking for the operational document
//! learns what it reached instead of only that it may not have it. Same four fields, one
//! definition, two doors.
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

/// The guard of a surface that wants nothing. Named rather than repeated so the JSON
/// rendering can turn exactly this case into `null` without matching on prose.
const NO_TOKEN: &str = "no token";

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
                note: "what this instance holds, per app — without a token, identity only",
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
        guard: NO_TOKEN,
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
    Surface {
        title: "Look at the data",
        guard: "admin token",
        routes: &[Route {
            method: "GET",
            paths: &["/debug"],
            note: "recent logs, traces and metrics — sign in with the token on the page",
        }],
    },
];

/// Documentation, in the order a newcomer needs it.
/// Documentation, in the order a newcomer needs it.
///
/// The documentation site first and the repository named as itself. Sending a reader to
/// a `.md` file on GitHub is sending them to the source of the docs rather than to the
/// docs, and it is the wrong front door for anyone who is not already contributing.
const DOCS: &[(&str, &str)] = &[
    ("Documentation", "https://cbox.dk/packages/telemetryd"),
    (
        "Quickstart",
        "https://cbox.dk/packages/telemetryd/quickstart",
    ),
    (
        "The API subset, endpoint by endpoint",
        "https://github.com/cboxdk/telemetryd/blob/main/COMPATIBILITY.md",
    ),
    (
        "Written for an agent to read",
        "https://github.com/cboxdk/telemetryd/blob/main/llms.txt",
    ),
    ("Source", "https://github.com/cboxdk/telemetryd"),
];

/// The rest of the ecosystem this server was built for.
const CBOX: &[(&str, &str, &str)] = &[
    (
        "laravel-telemetry",
        "https://cbox.dk/packages/laravel-telemetry",
        "the exporter — point a Laravel app here",
    ),
    (
        "laravel-telemetry-ui",
        "https://cbox.dk/packages/laravel-telemetry-ui",
        "dashboards over these APIs",
    ),
    (
        "Cbox packages",
        "https://cbox.dk/packages",
        "everything else",
    ),
];

/// Whether clicking this path in a browser leads anywhere useful.
///
/// Three things disqualify a path, and each would produce a worse experience than no
/// link at all: a `POST` route answers `405` to a browser's `GET`; a path with a
/// placeholder in it is not an address, it is a pattern; and the tail endpoint is a
/// WebSocket, so a plain navigation gets a `400` that reads like the query was wrong.
///
/// Everything else is worth a click even when it answers `401` — that is a real answer,
/// and it is the one that tells you a token is what you are missing.
fn is_navigable(method: &str, path: &str) -> bool {
    method == "GET" && !path.contains('{') && path != "/loki/api/v1/tail"
}

/// Serve HTML to a browser, JSON to a program that asks for it, plain text otherwise.
///
/// Negotiated on `Accept` rather than on a user-agent string, and text is the default:
/// the overwhelmingly common non-browser caller is `curl`, which sends `*/*`, and a
/// screenful of markup is not what that caller wanted.
pub async fn index(headers: HeaderMap) -> Response {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    // JSON is checked first: a discovery client sends `application/json` explicitly,
    // while a browser's Accept header names `text/html` and then `*/*`. Checking HTML
    // first would be right too, but only by accident of ordering — this way the more
    // specific request wins regardless of what else the header lists.
    if accept.contains("application/json") {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            json(),
        )
            .into_response()
    } else if accept.contains("text/html") {
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

/// The identity a client needs before it holds a credential.
///
/// Four fields, and every one of them is a fact about the *software*: it would read
/// identically on every telemetryd of this build anywhere in the world. Nothing here
/// varies with the deployment, which is what makes it safe to serve to a stranger — see
/// [`crate::routes::status_identity`] for the field-by-field line.
///
/// # Why the version is here, on an open endpoint
///
/// This page withheld it at first, on the reasoning that a version string on an
/// internet-facing port turns "which build is this" into "which advisories apply". That
/// reasoning was worth less than it cost. Anyone who can reach the port can fingerprint
/// the build from its behaviour anyway, and withholding it broke something real: a
/// client pointed at a URL got `401` from every guarded route and could not tell a
/// telemetryd that wants a token from a host with nothing on it. Silence and refusal
/// look identical, and only one of them is worth prompting a person about.
///
/// What stays behind the admin token is everything that describes *this deployment* —
/// app names, record counts, disk usage, retention, the listen address, whether a relay
/// is configured. Those are the facts an attacker cannot guess and an operator cares
/// about. The product name, its version and the shape of its API are none of them.
///
/// # Why it is its own type
///
/// It is served from two places — `/` for a browser or a discovery client, and `/status`
/// for a client that went looking for the operational document without a credential.
/// One definition, so the two can never disagree about what telemetryd says it is.
#[derive(Debug, serde::Serialize)]
pub struct Identity {
    /// Constant. The field a client matches on to know what it is talking to.
    pub product: &'static str,
    pub version: &'static str,
    /// What a client must agree with to read this instance's data at all.
    pub storage_format_version: u32,
    /// Which signals this build serves. A constant of the software: telemetryd does not
    /// let an operator switch one off, so naming them says nothing about the deployment.
    pub signals: [&'static str; 3],
}

/// What `GET /` answers: the identity, plus the map of the API.
///
/// The map is flattened onto the identity rather than nested under it, so `/` and
/// `/status` agree field-for-field on the four they share.
#[derive(Debug, serde::Serialize)]
pub struct IndexDocument {
    #[serde(flatten)]
    pub identity: Identity,
    pub surfaces: Vec<IdentitySurface>,
    pub docs: &'static str,
}

#[derive(Debug, serde::Serialize)]
pub struct IdentitySurface {
    pub title: &'static str,
    /// Which credential this surface wants. `null` when it wants none.
    pub auth: Option<&'static str>,
    pub routes: Vec<IdentityRoute>,
}

#[derive(Debug, serde::Serialize)]
pub struct IdentityRoute {
    pub method: &'static str,
    pub paths: &'static [&'static str],
    pub note: &'static str,
}

/// What telemetryd says it is, with nothing in it that varies between deployments.
pub fn identity() -> Identity {
    Identity {
        product: "telemetryd",
        version: telemetryd_core::VERSION,
        storage_format_version: telemetryd_core::STORAGE_FORMAT_VERSION,
        signals: ["logs", "metrics", "traces"],
    }
}

/// Built from the same table the two human renderings use, so the contract test that
/// walks it against the router covers this too.
pub fn index_document() -> IndexDocument {
    IndexDocument {
        identity: identity(),
        surfaces: SURFACES
            .iter()
            .map(|surface| IdentitySurface {
                title: surface.title,
                auth: (surface.guard != NO_TOKEN).then_some(surface.guard),
                routes: surface
                    .routes
                    .iter()
                    .map(|route| IdentityRoute {
                        method: route.method,
                        paths: route.paths,
                        note: route.note,
                    })
                    .collect(),
            })
            .collect(),
        docs: DOCS[0].1,
    }
}

fn json() -> String {
    // The document is a handful of static strings; a failure here is not reachable, and
    // an empty body would be a worse answer than the one thing that cannot go wrong.
    serde_json::to_string_pretty(&index_document()).unwrap_or_else(|_| "{}".to_owned())
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
                .map(|path| {
                    if is_navigable(route.method, path) {
                        // Relative, so the link works through whatever proxy, hostname
                        // and scheme the reader actually arrived on. An absolute URL
                        // built from the listen address would point at loopback for
                        // everyone but the operator standing on the box.
                        format!("<a href=\"{path}\"><code>{path}</code></a>")
                    } else {
                        format!("<code>{path}</code>")
                    }
                })
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
td a{{text-decoration:none}}\
td a code{{color:var(--link)}}\
td a:hover code{{text-decoration:underline}}\
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
<footer>This page is open, and names only what telemetryd is. What this particular \
deployment holds — apps, record counts, disk, retention — is on \
<a href=\"/status\"><code>/status</code></a>, which wants the admin token. \
Send <code>Accept: application/json</code> here for the same identity as a document.\
</footer>\
</main></body></html>"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_can_identify_the_product_and_its_wire_compatibility() {
        // The reason this endpoint exists in JSON at all: a client pointed at a URL it
        // has no credential for must be able to tell "telemetryd, needs a token" from
        // "nothing is listening here". Those three fields are that answer.
        let identity = identity();
        assert_eq!(identity.product, "telemetryd");
        assert_eq!(identity.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            identity.storage_format_version,
            telemetryd_core::STORAGE_FORMAT_VERSION
        );

        // Guarded surfaces name their credential; the open one says null rather than
        // the words "no token", so a client tests a field instead of parsing prose.
        let guards: Vec<_> = index_document()
            .surfaces
            .iter()
            .map(|surface| surface.auth)
            .collect();
        assert!(guards.contains(&Some("admin token")), "{guards:?}");
        assert!(guards.contains(&None), "{guards:?}");
    }

    /// The index is the identity plus the API map, not a second copy of the identity.
    ///
    /// `#[serde(flatten)]` is what makes that true on the wire, and a flatten that got
    /// removed would nest the four shared fields under `identity` — every client reading
    /// `/` would break, and no other test would notice.
    #[test]
    fn the_index_carries_the_identity_at_the_top_level() {
        let document = serde_json::to_value(index_document()).unwrap_or_default();
        for field in ["product", "version", "storage_format_version", "signals"] {
            assert!(
                document.get(field).is_some(),
                "the index no longer carries {field} at the top level: {document}"
            );
        }
        assert!(document.get("identity").is_none(), "{document}");
        assert!(document.get("surfaces").is_some(), "{document}");
    }

    #[test]
    fn only_paths_a_browser_can_actually_follow_are_linked() {
        // A link that lands on 405, or on a 400 that reads as a query mistake, is worse
        // than no link. A 401 is not in that category — it is the answer that tells you
        // a token is the thing you are missing.
        assert!(is_navigable("GET", "/status"));
        assert!(is_navigable("GET", "/metrics"));
        assert!(is_navigable("GET", "/healthz"));
        assert!(
            !is_navigable("POST", "/v1/logs"),
            "POST answers 405 to a GET"
        );
        assert!(
            !is_navigable("GET", "/api/traces/{trace_id}"),
            "a pattern is not an address"
        );
        assert!(
            !is_navigable("GET", "/loki/api/v1/tail"),
            "the WebSocket answers 400 to a navigation"
        );

        // And the rendering honours it: no anchor may wrap a placeholder.
        let page = html();
        assert!(page.contains("<a href=\"/status\">"), "{page}");
        assert!(!page.contains("<a href=\"/v1/logs\">"), "{page}");
        assert!(!page.contains("<a href=\"/api/traces/"), "{page}");
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
