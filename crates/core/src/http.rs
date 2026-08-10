//! The one place that decides who telemetryd trusts when it dials out.
//!
//! # Why this exists at all
//!
//! For most of this project's life `ureq` was declared `default-features = false`,
//! with a comment saying plain HTTP only — correct in M0, when it was the CLI's client
//! talking to localhost. Three things were then built on the same client that talk to
//! the outside world: the OIDC key fetch ([ADR-011]), relay shipping ([ADR-013]), and
//! transfer's remote read and write ([ADR-012]).
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
//! [ADR-011]: ../../docs/adr/0011-cbox-id-integration.md
//! [ADR-012]: ../../docs/adr/0012-import-and-export.md
//! [ADR-013]: ../../docs/adr/0013-relay-mode.md

use ureq::tls::{RootCerts, TlsConfig};

/// The TLS configuration every outbound request in telemetryd uses.
///
/// Pass it to `.config().tls_config(tls())` when building a request.
#[must_use]
pub fn tls() -> TlsConfig {
    let roots = if cfg!(feature = "platform-verifier") {
        RootCerts::PlatformVerifier
    } else {
        RootCerts::WebPki
    };
    TlsConfig::builder().root_certs(roots).build()
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
}
