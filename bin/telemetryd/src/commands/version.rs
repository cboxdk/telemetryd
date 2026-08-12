//! `telemetryd version`, and `--check` for whether a newer release exists.

use std::time::Duration;

/// Only ever on request.
///
/// telemetryd's whole claim is that your telemetry does not leave your infrastructure,
/// and a binary that contacts GitHub on every start contradicts that even when all it
/// fetches is a version number. So there is no background check, no daily timer, and no
/// "we noticed an update" line in the server log — the operator asks, or nothing happens.
const RELEASES_API: &str = "https://api.github.com/repos/cboxdk/telemetryd/releases/latest";

/// Short on purpose: this is a convenience, and an unreachable network should cost a
/// second of someone's attention rather than hang a terminal or a script.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(8);

/// A GitHub release body is small; anything larger is not one, and is not worth parsing.
const MAX_BYTES: u64 = 64 * 1024;

pub fn run(check: bool) {
    crate::out::outln!("telemetryd {}", telemetryd_core::VERSION);
    crate::out::outln!(
        "storage format v{}",
        telemetryd_core::STORAGE_FORMAT_VERSION
    );
    crate::out::outln!("target {}", env!("TELEMETRYD_TARGET"));
    crate::out::outln!("compatibility {}", telemetryd_core::COMPATIBILITY_DOC);

    if check {
        crate::out::outln!("");
        report(&latest_release());
    }
}

/// What the check found, kept separate from printing so it can be tested without a
/// network.
#[derive(Debug, PartialEq, Eq)]
pub enum Check {
    UpToDate,
    Newer(String),
    /// Ahead of the newest published release — a development build.
    Unreleased(String),
    /// The check could not run. Not an error state: being offline is normal.
    Unavailable(String),
}

fn report(check: &Check) {
    match check {
        Check::UpToDate => crate::out::outln!("this is the newest release"),
        Check::Newer(latest) => {
            crate::out::outln!("a newer release is available: {latest}");
            crate::out::outln!("");
            crate::out::outln!("  brew upgrade cboxdk/tap/telemetryd");
            crate::out::outln!("  # or re-run the installer, which verifies the signature:");
            crate::out::outln!(
                "  curl -fsSL https://raw.githubusercontent.com/cboxdk/telemetryd/main/install.sh | sh"
            );
            crate::out::outln!("");
            // Said because replacing the file does not restart what is already running,
            // and a silent no-op upgrade is worse than none.
            crate::out::outln!("Running as a service? Restart it afterwards.");
        }
        Check::Unreleased(latest) => {
            crate::out::outln!("this build is ahead of the newest release ({latest})");
        }
        Check::Unavailable(why) => {
            crate::out::outln!("could not check for a newer release: {why}");
        }
    }
}

fn latest_release() -> Check {
    let response = ureq::get(RELEASES_API)
        .header("user-agent", &super::status::user_agent())
        .header("accept", "application/vnd.github+json")
        .config()
        .tls_config(telemetryd_core::http::tls())
        .timeout_connect(Some(CONNECT_TIMEOUT))
        .timeout_global(Some(TOTAL_TIMEOUT))
        .build()
        .call();

    let mut response = match response {
        Ok(response) => response,
        // Offline, rate-limited, DNS gone — all the same to the caller, and none of them
        // deserve a non-zero exit from a command whose main job already succeeded.
        Err(error) => return Check::Unavailable(error.to_string()),
    };

    let body = match response
        .body_mut()
        .with_config()
        .limit(MAX_BYTES)
        .read_to_string()
    {
        Ok(body) => body,
        Err(error) => return Check::Unavailable(error.to_string()),
    };

    let Some(tag) = tag_name(&body) else {
        return Check::Unavailable("the release feed had no tag_name".to_owned());
    };
    compare(telemetryd_core::VERSION, &tag)
}

/// Pull `tag_name` out without pulling in a JSON parser for one field.
///
/// The CLI does not otherwise depend on `serde_json`, and a dependency earns its place
/// by doing something hard. Finding one string in a document we asked for is not that.
fn tag_name(body: &str) -> Option<String> {
    let start = body.find("\"tag_name\"")? + "\"tag_name\"".len();
    let rest = &body[start..];
    let open = rest.find('"')? + 1;
    let rest = &rest[open..];
    let close = rest.find('"')?;
    Some(rest[..close].to_owned())
}

/// Compare `x.y.z` numerically.
///
/// Numeric per component, not lexicographic: `0.9.0` is older than `0.28.0`, and a
/// string comparison says the opposite. That is the bug this function exists to avoid,
/// and the version numbers here have already crossed the boundary where it bites.
fn compare(running: &str, tag: &str) -> Check {
    let parse = |text: &str| -> Option<Vec<u64>> {
        text.trim_start_matches('v')
            .split('.')
            .map(|part| part.split(['-', '+']).next().unwrap_or(part).parse().ok())
            .collect()
    };
    let (Some(mine), Some(theirs)) = (parse(running), parse(tag)) else {
        return Check::Unavailable(format!("could not compare {running} with {tag}"));
    };
    match mine.cmp(&theirs) {
        std::cmp::Ordering::Less => Check::Newer(tag.to_owned()),
        std::cmp::Ordering::Equal => Check::UpToDate,
        std::cmp::Ordering::Greater => Check::Unreleased(tag.to_owned()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn versions_compare_numerically_not_lexicographically() {
        // The whole reason this is not a string compare: "0.9.0" > "0.28.0" as text.
        assert_eq!(
            compare("0.9.0", "v0.28.0"),
            Check::Newer("v0.28.0".to_owned())
        );
        assert_eq!(
            compare("0.28.0", "v0.9.0"),
            Check::Unreleased("v0.9.0".to_owned())
        );
        assert_eq!(compare("0.28.0", "v0.28.0"), Check::UpToDate);
        assert_eq!(
            compare("1.0.0", "v0.28.0"),
            Check::Unreleased("v0.28.0".to_owned())
        );
    }

    #[test]
    fn a_prerelease_suffix_does_not_break_the_comparison() {
        assert_eq!(
            compare("0.28.0", "v0.29.0-beta.1"),
            Check::Newer("v0.29.0-beta.1".to_owned())
        );
    }

    #[test]
    fn nonsense_is_reported_rather_than_guessed() {
        // Better to say the check failed than to claim an upgrade exists, or that none
        // does, on a string nobody can parse.
        assert!(matches!(
            compare("0.28.0", "not-a-version"),
            Check::Unavailable(_)
        ));
    }

    #[test]
    fn the_tag_is_found_in_a_realistic_body() {
        let body =
            r#"{"url":"https://api.github.com/x","id":1,"tag_name":"v0.28.0","name":"0.28.0"}"#;
        assert_eq!(tag_name(body).unwrap(), "v0.28.0");
        assert!(tag_name(r#"{"name":"no tag here"}"#).is_none());
    }
}
