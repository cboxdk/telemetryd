//! Refusing what a web page can make a browser send.
//!
//! telemetryd serves no CORS, so a page on another site cannot *read* its answers — but
//! a browser still sends the requests that need no preflight: a form or `text/plain`
//! POST, a `Blob` with no type, a WebSocket upgrade. Against the default instance —
//! loopback, no tokens — that was enough for any page a developer visited to stream
//! every live log line over `/loki/api/v1/tail`, or write records into `/v1/logs`. And
//! DNS rebinding, a hostname that resolves to `127.0.0.1` after the page loads, makes
//! even the reads same-origin as far as the browser can tell.
//!
//! Two checks close that, and neither touches a client that is not a browser: SDKs,
//! Prometheus, Grafana's backend and laravel-telemetry-ui send no `Origin` header, and
//! name the host they meant.

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::AppState;

/// Refuse a request a browser made on another site's behalf, and — while this instance
/// answers without a token — one that reached it under a name that is not its own.
pub async fn guard(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let headers = request.headers();

    // Browsers send `Origin` on every cross-origin POST and on every WebSocket upgrade,
    // whatever the page asked for, and a page cannot forge it. One that names another
    // site is a page acting through its visitor. Same-origin — the index and `/debug`,
    // served by telemetryd itself — names this host and passes.
    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin = origin.to_str().unwrap_or_default();
        if !same_origin(origin, headers) {
            return refuse(&format!(
                "a web page at {origin:?} cannot call telemetryd from a visitor's browser. \
                 telemetryd serves no CORS; query it from a server — Grafana, the UI, \
                 curl — which sends no Origin header"
            ));
        }
    }

    // DNS rebinding makes the page same-origin, so the name it used is what gives it
    // away: `attacker.example` pointed at 127.0.0.1 arrives as `Host: attacker.example`.
    // Checked only where it matters — a loopback listener with a surface anyone may use.
    // With every surface guarded a rebound page holds no token and gets nothing; on a
    // public bind the operator chose `--insecure` and the names clients use are theirs.
    if state.config.server.listen.ip().is_loopback()
        && state.any_surface_open()
        && let Some(host) = headers.get(header::HOST)
        && !loopback_name(host.to_str().unwrap_or_default())
    {
        return refuse(&format!(
            "this instance answers without a token on {listen}, so it only answers to \
             loopback names; {host:?} is not one. Use http://127.0.0.1:{port}, or set \
             tokens to serve other names",
            listen = state.config.server.listen,
            host = host.to_str().unwrap_or_default(),
            port = state.config.server.listen.port(),
        ));
    }

    next.run(request).await
}

/// Whether `origin` names the host this request was sent to — directly, or through a
/// proxy that says so in `X-Forwarded-Host`. A browser cannot set that header on a
/// cross-site request without a preflight, which telemetryd never grants.
fn same_origin(origin: &str, headers: &HeaderMap) -> bool {
    let Some((_, authority)) = origin.split_once("://") else {
        // `null` — a sandboxed frame or a file — is never this server.
        return false;
    };
    let authority = authority.trim_end_matches('/');
    [header::HOST.as_str(), "x-forwarded-host"]
        .iter()
        .filter_map(|name| headers.get(*name)?.to_str().ok())
        .any(|host| host.eq_ignore_ascii_case(authority))
}

/// Whether a `Host` header names this machine: `localhost` and its subdomains, which
/// resolve to loopback by definition, or a loopback address.
fn loopback_name(host: &str) -> bool {
    let name = if let Some(bracketed) = host.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or_default()
    } else {
        host.rsplit_once(':')
            .filter(|(_, port)| port.bytes().all(|b| b.is_ascii_digit()))
            .map_or(host, |(name, _)| name)
    };
    let name = name.to_ascii_lowercase();
    name == "localhost"
        || name.ends_with(".localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn refuse(message: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({
            "error": { "code": "forbidden", "message": message }
        })),
    )
        .into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        map
    }

    #[test]
    fn an_origin_matches_only_its_own_host() {
        let local = headers(&[("host", "127.0.0.1:4319")]);
        assert!(same_origin("http://127.0.0.1:4319", &local));
        assert!(!same_origin("https://attacker.example", &local));
        assert!(!same_origin("http://127.0.0.1:4320", &local));
        assert!(!same_origin("null", &local));

        // Behind a proxy that rewrites Host but says where the request was for.
        let proxied = headers(&[
            ("host", "127.0.0.1:4319"),
            ("x-forwarded-host", "telemetry.example"),
        ]);
        assert!(same_origin("https://telemetry.example", &proxied));
        assert!(!same_origin("https://attacker.example", &proxied));
    }

    #[test]
    fn loopback_names_are_recognised_and_nothing_else() {
        for host in [
            "localhost",
            "localhost:4319",
            "app.localhost:4319",
            "127.0.0.1:4319",
            "127.9.9.9",
            "[::1]:4319",
            "LOCALHOST",
        ] {
            assert!(loopback_name(host), "{host}");
        }
        for host in [
            "attacker.example",
            "localhost.attacker.example:4319",
            "10.0.0.1:4319",
            "[2001:db8::1]:4319",
            "",
        ] {
            assert!(!loopback_name(host), "{host}");
        }
    }
}
