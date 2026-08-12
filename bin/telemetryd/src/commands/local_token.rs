//! Finding the token for a local instance, so nobody has to paste their own credential.
//!
//! The configuration is on the box with the path to the token inside it. A CLI that
//! makes an operator go and read that file, then paste it back, is asking them to do
//! work it could do — and the reporter of this said so in about those words.
//!
//! # Loopback only, and that is the whole safety argument
//!
//! The token is read from *this machine's* configuration. Sending it to `--url` pointing
//! somewhere else would hand a local credential to a remote host that has no business
//! holding it — the classic shape of a credential leak, and easy to trigger by pasting a
//! colleague's URL. So the lookup happens only when the target is loopback, and a remote
//! URL falls back to asking, exactly as before.

use telemetryd_core::Config;

/// The surface a command needs to reach.
#[derive(Debug, Clone, Copy)]
pub enum Surface {
    /// `/status` and `/metrics`, guarded by the admin token — or the query token on an
    /// instance that configured no admin one.
    Admin,
    /// The read APIs.
    Query,
}

/// Whether a URL points at this machine.
///
/// Parsed rather than pattern-matched on the whole string: `https://127.0.0.1.evil.com`
/// contains `127.0.0.1` and is not loopback.
#[must_use]
pub fn is_loopback(url: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .rsplit_once(':')
        .map_or(
            rest.split(['/', '?', '#']).next().unwrap_or_default(),
            |(host, _)| host,
        )
        .trim_matches(['[', ']']);
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

/// Read the token for `surface` out of this machine's configuration.
///
/// Returns `None` for anything that is not a local instance, and for every failure along
/// the way: no configuration, no token configured, a token file this user cannot read.
/// All of those end with the command asking for `--token`, which is the behaviour that
/// existed before and is never worse than it.
#[must_use]
pub fn find(url: &str, surface: Surface) -> Option<String> {
    if !is_loopback(url) {
        return None;
    }
    let loaded = Config::load(None, &telemetryd_core::config::Overrides::default()).ok()?;
    let auth = &loaded.config.auth;
    match surface {
        // The same fallback the server applies: an instance with no admin token accepts
        // the query token on `/status`.
        Surface::Admin => auth
            .admin_token
            .first_value()
            .or_else(|| auth.query_token.first_value()),
        Surface::Query => auth.query_token.first_value(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hostname_that_merely_contains_a_loopback_address_is_not_one() {
        for url in [
            "http://127.0.0.1:4319",
            "http://localhost:4319",
            "https://localhost",
            "http://[::1]:4319",
            "http://127.0.0.1:4319/status?x=1",
        ] {
            assert!(is_loopback(url), "{url} should be loopback");
        }
        for url in [
            "https://127.0.0.1.evil.com/status",
            "https://telemetry.example.com",
            "http://10.0.0.5:4319",
            "https://localhost.attacker.net",
        ] {
            assert!(!is_loopback(url), "{url} must not be treated as loopback");
        }
    }

    #[test]
    fn a_remote_url_never_resolves_a_local_token() {
        // The failure this prevents: pasting a colleague's URL and handing them the
        // admin token off this machine.
        assert!(find("https://telemetry.example.com", Surface::Admin).is_none());
        assert!(find("https://127.0.0.1.evil.com", Surface::Query).is_none());
    }
}
