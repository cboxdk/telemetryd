//! The one place that decides who telemetryd trusts when it dials out.
//!
//! # Why this exists at all
//!
//! For most of this project's life `ureq` was declared `default-features = false`,
//! with a comment saying plain HTTP only — correct in M0, when it was the CLI's client
//! talking to localhost. Three things were then built on the same client that talk to
//! the outside world: the OIDC key fetch, relay shipping, and transfer's remote read
//! and write.
//!
//! None of them could work. `Cargo.lock` carried no TLS crate at all, so every https
//! request failed with "TLS required, but transport is unsecured" — while
//! `Config::validate` *demanded* https for both `auth.oidc.issuer` and
//! `relay.upstream`. The configuration required exactly what the client could not do,
//! so any valid production setup of either feature was unreachable.
//!
//! Nothing caught it because every test and every soak run points at loopback, where
//! plain HTTP is allowed on purpose. A green suite said nothing about the shipped
//! binary — the same lesson as the packaging work, in a new place.
//!
//! # Why it is a function and not eight call sites
//!
//! `ureq` lets TLS be configured per request, and there are eight places that dial out.
//! Eight independent trust decisions is one forgotten `.tls_config()` away from a
//! request that silently verifies against different roots than its neighbours. This is
//! the only constructor; call sites pass what it returns.
//!
//! It also makes the original bug unrepeatable. Because this module names `ureq::tls`,
//! stripping the TLS feature no longer produces a binary that fails at runtime against
//! every https URL — it fails to compile. The soak still checks the shipped binary
//! reaches the network layer on an https URL, since a build that compiles can still be
//! configured never to dial.
//!
//! # Which roots
//!
//! `webpki-roots` by default: the root set is compiled into the binary, so it behaves
//! identically on all four release targets — including static musl, where there is no
//! system trust store to read and a platform verifier would find nothing. The cost is
//! that root CA changes arrive with our releases rather than with the operating
//! system's, which is the right trade for a binary that is often the only thing on the
//! box.
//!
//! The `platform-verifier` feature switches to the host's own trust store instead, for
//! deployments where the upstream or the issuer is behind an internal CA. It is not the
//! default because that store is empty in most containers, which would turn a working
//! configuration into a confusing failure on the most common way to run this.
//!
//! Enabling the feature is not on its own sufficient — `TlsConfig::default()` hardcodes
//! `RootCerts::WebPki` — so the choice is made explicitly below.
//!

use std::sync::OnceLock;

use ureq::tls::{Certificate, RootCerts, TlsConfig};

/// The environment variable a CLI run reads its trust from.
///
/// `serve` takes it from `tls.ca_file` in the configuration, which this variable also
/// feeds through the usual precedence. `telemetryd import` and friends do not load a
/// configuration file at all, so for them this is the whole story.
pub const CA_FILE_ENV: &str = "TELEMETRYD_TLS_CA_FILE";

/// Resolved once, because parsing a PEM bundle per request would be silly and because
/// the answer must not differ between two requests in the same process.
///
/// A `OnceLock` rather than a parameter on [`tls`] is a deliberate trade. There are
/// eight call sites; threading trust through all of them is eight chances to pass the
/// wrong thing, and the failure would be silent — a request verifying against different
/// roots than its neighbours. One process, one answer, set before anything dials.
static TRUST: OnceLock<TlsConfig> = OnceLock::new();

/// Point outbound TLS at a specific set of authorities. Call once, before serving.
///
/// Returns the error rather than logging it: this runs before the logger exists, and a
/// CA file that cannot be read must stop startup rather than silently fall back to the
/// public roots — falling back would mean an operator who asked for a private CA gets
/// public trust instead, which is the opposite of what they configured.
pub fn init_trust(ca_file: &std::path::Path) -> Result<(), String> {
    let config = build(Some(ca_file))?;
    // Already set means someone called this twice; the first answer wins and that is
    // worth knowing rather than papering over.
    TRUST
        .set(config)
        .map_err(|_| "outbound TLS trust was already initialised".to_owned())
}

/// The TLS configuration every outbound request in telemetryd uses.
///
/// Pass it to `.config().tls_config(tls())` when building a request.
#[must_use]
pub fn tls() -> TlsConfig {
    if let Some(config) = TRUST.get() {
        return config.clone();
    }
    // Not initialised: a CLI run. Read the environment directly, and fall back to the
    // built-in roots if it is unset or unreadable — a CLI command that cannot parse a
    // bundle should say so when it fails to connect, not refuse to start.
    let from_env = std::env::var(CA_FILE_ENV).unwrap_or_default();
    let path = (!from_env.trim().is_empty()).then(|| std::path::PathBuf::from(from_env.trim()));
    build(path.as_deref()).unwrap_or_else(|_| build(None).unwrap_or_default())
}

fn build(ca_file: Option<&std::path::Path>) -> Result<TlsConfig, String> {
    let roots = match ca_file {
        Some(path) => {
            let shown = path.display();
            let pem = std::fs::read(path)
                .map_err(|e| format!("could not read the CA bundle at {shown}: {e}"))?;
            let certs: Vec<Certificate<'static>> = ureq::tls::parse_pem(&pem)
                .filter_map(|item| match item {
                    Ok(ureq::tls::PemItem::Certificate(cert)) => Some(cert),
                    _ => None,
                })
                .collect();
            if certs.is_empty() {
                return Err(format!(
                    "{shown} contains no certificates. tls.ca_file must be a PEM bundle \
                     of certificate authorities; a key or an empty file would leave \
                     nothing to verify against."
                ));
            }
            RootCerts::Specific(std::sync::Arc::new(certs))
        }
        None if cfg!(feature = "platform-verifier") => RootCerts::PlatformVerifier,
        None => RootCerts::WebPki,
    };
    Ok(TlsConfig::builder().root_certs(roots).build())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The failure this guards against is silent: verification off still connects, and
    /// the request succeeds against anyone who answers. Nothing downstream would notice.
    #[test]
    fn certificates_are_always_verified() {
        assert!(
            !tls().disable_verification(),
            "outbound TLS must verify certificates"
        );
    }

    /// Which store is a build-time choice, and the default has to be the portable one:
    /// static musl has no system trust store, so a platform verifier finds no roots.
    #[test]
    fn the_default_build_carries_its_own_roots() {
        // `RootCerts` is not `PartialEq`, so match on the variant rather than compare.
        let roots = tls();
        if cfg!(feature = "platform-verifier") {
            assert!(matches!(roots.root_certs(), RootCerts::PlatformVerifier));
        } else {
            assert!(matches!(roots.root_certs(), RootCerts::WebPki));
        }
    }

    /// A bundle that parses but contains no certificates is the dangerous case: it
    /// would leave nothing to verify against, and silently accepting it would mean an
    /// operator who configured a private CA gets no verification rather than more.
    #[test]
    fn a_bundle_with_no_certificates_is_refused() {
        let dir = std::env::temp_dir().join("telemetryd-http-test");
        std::fs::create_dir_all(&dir).expect("scratch directory");
        let path = dir.join("empty.pem");
        std::fs::write(
            &path,
            b"-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n",
        )
        .expect("write");

        let error =
            build(Some(path.as_path())).expect_err("a bundle with no certificates must be refused");
        assert!(error.contains("no certificates"), "{error}");

        let missing = build(Some(std::path::Path::new("/nonexistent/ca.pem")))
            .expect_err("an unreadable bundle must be refused");
        assert!(missing.contains("could not read"), "{missing}");
        let _ = std::fs::remove_file(&path);
    }

    /// Empty means "use the default", not "use a file called empty string".
    #[test]
    fn an_unset_bundle_falls_back_to_the_built_in_roots() {
        {
            let config = build(None).expect("an unset bundle is not an error");
            assert!(!config.disable_verification());
        }
    }
}
