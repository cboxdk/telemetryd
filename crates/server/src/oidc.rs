//! Validating Cbox ID access tokens.
//!
//! Tokens are checked against the issuer's published keys, in this process. telemetryd
//! never asks the provider about a token, and that is not a performance choice: this is
//! the thing you open when something is broken, so an identity provider that must be
//! reachable to read your logs is a dependency in exactly the wrong direction. Keys are
//! cached; a provider that goes down stops new tokens being *issued* while every token
//! already in hand keeps working. See [ADR-011](../../docs/adr/0011-cbox-id-integration.md).
//!
//! # The algorithm is never taken from the token
//!
//! A JWT header names its own algorithm, and believing it is how `alg: none` and
//! RS256-verified-as-HMAC happen. Here the header supplies only a **key id**; the key
//! is looked up, and the algorithm comes from *that key*. A token asking for something
//! else does not find a key that agrees, so it is rejected without the question ever
//! being asked. Cbox ID's own verifier works the same way, which is a good sign it is
//! the shape both sides expect.
//!
//! # What is checked
//!
//! Signature, `iss` exactly, `aud` exactly, and `exp`/`nbf` with a configured skew.
//! Cbox ID always sets `aud` — RFC 9068 §2.2 requires it on an `at+jwt` — so a token
//! without one is refused rather than waved through.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use telemetryd_core::config::OidcConfig;

use crate::auth::Surface;

/// A key the issuer publishes, with the algorithm it is for.
///
/// `Debug` prints the algorithm only. A public key is not a secret, but a struct that
/// dumps key material into a log line is a habit worth not forming.
struct VerificationKey {
    key: DecodingKey,
    algorithm: Algorithm,
}

impl std::fmt::Debug for VerificationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerificationKey")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct KeySet {
    keys: HashMap<String, Arc<VerificationKey>>,
    fetched_at: Option<Instant>,
    /// Last refresh attempt, successful or not. Rate-limits the unknown-`kid` refresh
    /// so an attacker cannot turn a stream of forged key ids into a stream of outbound
    /// requests.
    attempted_at: Option<Instant>,
}

/// Claims telemetryd reads. Everything else the token carries is ignored.
#[derive(Debug, Deserialize)]
struct Claims {
    /// Space-separated, as OAuth specifies and Cbox ID emits.
    #[serde(default)]
    scope: String,
    #[serde(default)]
    sub: String,
}

pub struct Oidc {
    config: OidcConfig,
    keys: RwLock<KeySet>,
}

impl std::fmt::Debug for Oidc {
    /// Issuer and key count only. The configuration carries no secret — tokens are
    /// verified with public keys — but a struct that prints its whole auth state into
    /// a log line is a habit worth not forming.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Oidc")
            .field("issuer", &self.config.issuer)
            .field("keys", &self.key_count())
            .finish_non_exhaustive()
    }
}

/// Why a token was refused, for the log line. Deliberately coarse in what it returns
/// to the caller — a 401 says "unauthorized" and nothing more — while staying specific
/// enough in the log to debug a misconfigured issuer.
#[derive(Debug, PartialEq, Eq)]
pub enum Rejected {
    NotAJwt,
    UnknownKey,
    BadSignatureOrClaims(String),
    NoMatchingScope,
}

impl Oidc {
    #[must_use]
    pub fn new(config: OidcConfig) -> Self {
        Self {
            config,
            keys: RwLock::new(KeySet::default()),
        }
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }

    /// Load the key set. Called at startup and on a timer.
    ///
    /// Returns the number of keys. A failure leaves whatever was already cached in
    /// place, which is the behaviour that keeps telemetryd readable while the issuer
    /// is down.
    pub fn refresh(&self) -> Result<usize, String> {
        let url = self.config.jwks_url();
        let body = fetch(&url)?;
        let parsed = parse_jwks(&body)?;
        let count = parsed.len();

        let mut keys = self
            .keys
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        keys.keys = parsed;
        keys.fetched_at = Some(Instant::now());
        keys.attempted_at = Some(Instant::now());
        Ok(count)
    }

    /// Decide whether a bearer token grants `surface`.
    pub fn authorize(&self, token: &str, surface: Surface) -> Result<String, Rejected> {
        let header = decode_header(token).map_err(|_| Rejected::NotAJwt)?;
        let kid = header.kid.ok_or(Rejected::NotAJwt)?;

        let key = if let Some(key) = self.key(&kid) {
            key
        } else {
            // A key id we have not seen is what a rotation looks like, so refresh once
            // and try again — but not on every forged id that arrives.
            self.refresh_if_due();
            self.key(&kid).ok_or(Rejected::UnknownKey)?
        };

        // The algorithm comes from the key, not from `header.alg`.
        let mut validation = Validation::new(key.algorithm);
        validation.set_issuer(&[self.config.issuer.trim_end_matches('/')]);
        validation.set_audience(&[self.config.expected_audience()]);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        validation.leeway = self.config.clock_skew.as_secs();

        let data = decode::<Claims>(token, &key.key, &validation)
            .map_err(|e| Rejected::BadSignatureOrClaims(e.to_string()))?;

        let wanted = match surface {
            Surface::Ingest => &self.config.scope_write,
            Surface::Query => &self.config.scope_read,
            Surface::Admin => &self.config.scope_admin,
        };
        if data.claims.scope.split(' ').any(|scope| scope == wanted) {
            Ok(data.claims.sub)
        } else {
            Err(Rejected::NoMatchingScope)
        }
    }

    /// Whether the cached key set is older than the configured interval.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        let keys = self
            .keys
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        keys.fetched_at
            .is_none_or(|at| at.elapsed() >= self.config.refresh_interval)
    }

    #[must_use]
    pub fn key_count(&self) -> usize {
        self.keys
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys
            .len()
    }

    fn key(&self, kid: &str) -> Option<Arc<VerificationKey>> {
        self.keys
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys
            .get(kid)
            .map(Arc::clone)
    }

    /// Refresh on an unknown key id, at most once a minute.
    fn refresh_if_due(&self) {
        const COOLDOWN: Duration = Duration::from_secs(60);
        {
            let keys = self
                .keys
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if keys.attempted_at.is_some_and(|at| at.elapsed() < COOLDOWN) {
                return;
            }
        }
        self.keys
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .attempted_at = Some(Instant::now());

        if let Err(error) = self.refresh() {
            tracing::warn!(%error, "could not refresh the Cbox ID key set");
        }
    }
}

fn fetch(url: &str) -> Result<String, String> {
    let mut response = ureq::get(url)
        .call()
        .map_err(|e| format!("fetching {url}: {e}"))?;
    response
        .body_mut()
        .read_to_string()
        .map_err(|e| format!("reading {url}: {e}"))
}

/// A JSON Web Key Set, reduced to what can verify a signature.
///
/// Keys whose type or algorithm telemetryd cannot verify are skipped rather than
/// failing the whole set: an issuer serving one encryption key alongside its signing
/// keys should not take authentication down.
fn parse_jwks(body: &str) -> Result<HashMap<String, Arc<VerificationKey>>, String> {
    #[derive(Deserialize)]
    struct Jwks {
        keys: Vec<Jwk>,
    }
    #[derive(Deserialize)]
    struct Jwk {
        kty: String,
        kid: Option<String>,
        alg: Option<String>,
        #[serde(rename = "use")]
        usage: Option<String>,
        // RSA
        n: Option<String>,
        e: Option<String>,
        // EC and OKP
        crv: Option<String>,
        x: Option<String>,
        y: Option<String>,
    }

    let jwks: Jwks = serde_json::from_str(body).map_err(|e| format!("parsing the key set: {e}"))?;
    let mut keys = HashMap::new();

    for jwk in jwks.keys {
        // An encryption key cannot verify a signature; including it would only create
        // a chance to pick the wrong one.
        if jwk.usage.as_deref() == Some("enc") {
            continue;
        }
        let Some(kid) = jwk.kid else { continue };

        let built = match (jwk.kty.as_str(), jwk.alg.as_deref(), jwk.crv.as_deref()) {
            ("RSA", alg, _) => {
                let (Some(n), Some(e)) = (jwk.n.as_deref(), jwk.e.as_deref()) else {
                    continue;
                };
                DecodingKey::from_rsa_components(n, e).ok().map(|key| {
                    let algorithm = match alg {
                        Some("RS384") => Algorithm::RS384,
                        Some("RS512") => Algorithm::RS512,
                        Some("PS256") => Algorithm::PS256,
                        _ => Algorithm::RS256,
                    };
                    VerificationKey { key, algorithm }
                })
            }
            ("EC", _, Some("P-256")) => {
                let (Some(x), Some(y)) = (jwk.x.as_deref(), jwk.y.as_deref()) else {
                    continue;
                };
                DecodingKey::from_ec_components(x, y)
                    .ok()
                    .map(|key| VerificationKey {
                        key,
                        algorithm: Algorithm::ES256,
                    })
            }
            ("OKP", _, Some("Ed25519")) => {
                let Some(x) = jwk.x.as_deref() else { continue };
                DecodingKey::from_ed_components(x)
                    .ok()
                    .map(|key| VerificationKey {
                        key,
                        algorithm: Algorithm::EdDSA,
                    })
            }
            _ => None,
        };

        if let Some(key) = built {
            keys.insert(kid, Arc::new(key));
        }
    }

    if keys.is_empty() {
        return Err("the key set contained no key telemetryd can verify with".to_owned());
    }
    Ok(keys)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn config() -> OidcConfig {
        OidcConfig {
            issuer: "https://id.example.com".to_owned(),
            ..OidcConfig::default()
        }
    }

    #[test]
    fn the_key_set_url_follows_the_issuer() {
        let mut config = config();
        assert_eq!(
            config.jwks_url(),
            "https://id.example.com/.well-known/jwks.json"
        );
        // A trailing slash is the kind of thing an operator pastes.
        config.issuer = "https://id.example.com/".to_owned();
        assert_eq!(
            config.jwks_url(),
            "https://id.example.com/.well-known/jwks.json"
        );
    }

    #[test]
    fn the_audience_defaults_to_the_issuer() {
        // Cbox ID mints `aud = resource ?? issuer`, so accepting the issuer is what
        // makes a token with no explicit resource work.
        let mut config = config();
        assert_eq!(config.expected_audience(), "https://id.example.com");
        config.audience = "https://telemetry.example.com".to_owned();
        assert_eq!(config.expected_audience(), "https://telemetry.example.com");
    }

    #[test]
    fn a_key_set_without_a_usable_key_is_an_error_not_an_empty_set() {
        // An empty set would silently reject every token, which looks identical to a
        // misconfigured issuer and is much harder to diagnose.
        let err = parse_jwks(r#"{"keys":[]}"#).unwrap_err();
        assert!(err.contains("no key"), "{err}");

        // A key with no id cannot be selected, so it is not a usable key.
        let err = parse_jwks(r#"{"keys":[{"kty":"RSA","n":"AQAB","e":"AQAB"}]}"#).unwrap_err();
        assert!(err.contains("no key"), "{err}");
    }

    #[test]
    fn an_encryption_key_is_never_a_verification_key() {
        let body = r#"{"keys":[
            {"kty":"RSA","kid":"enc","use":"enc","n":"AQAB","e":"AQAB"},
            {"kty":"RSA","kid":"sig","use":"sig","n":"AQAB","e":"AQAB"}
        ]}"#;
        let keys = parse_jwks(body).unwrap();
        assert!(keys.contains_key("sig"));
        assert!(
            !keys.contains_key("enc"),
            "an encryption key must never be a candidate for verifying a signature"
        );
    }

    #[test]
    fn an_unusable_key_does_not_take_the_set_down() {
        // One key telemetryd cannot verify with, alongside one it can. Authentication
        // should keep working rather than failing wholesale.
        let body = r#"{"keys":[
            {"kty":"oct","kid":"symmetric","k":"c2VjcmV0"},
            {"kty":"RSA","kid":"rsa","n":"AQAB","e":"AQAB"}
        ]}"#;
        let keys = parse_jwks(body).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains_key("rsa"));
    }

    #[test]
    fn a_token_that_is_not_a_jwt_is_refused_before_anything_else() {
        let oidc = Oidc::new(config());
        // Static tokens share the header, so this path runs for every one of them.
        assert_eq!(
            oidc.authorize("plain-static-token", Surface::Query),
            Err(Rejected::NotAJwt)
        );
    }

    #[test]
    fn an_unknown_key_id_is_refused_rather_than_guessed_at() {
        let oidc = Oidc::new(config());
        // A well-formed header naming a key we do not hold. With no keys cached and
        // no issuer reachable, this must refuse — never fall back to "try them all".
        let header = base64_url(br#"{"alg":"RS256","typ":"JWT","kid":"nope"}"#);
        let token = format!("{header}.{}.{}", base64_url(b"{}"), base64_url(b"sig"));
        assert!(matches!(
            oidc.authorize(&token, Surface::Query),
            Err(Rejected::UnknownKey | Rejected::BadSignatureOrClaims(_))
        ));
    }

    #[test]
    fn a_disabled_configuration_is_disabled() {
        let oidc = Oidc::new(OidcConfig::default());
        assert!(
            !oidc.is_enabled(),
            "an empty issuer must not enable anything"
        );
    }

    /// An issuer, in the test: a key pair, its JWK, and tokens signed with it.
    ///
    /// Signing a real token and verifying it through the real path is the only thing
    /// that proves the pieces agree — key parsing, algorithm selection, claim
    /// validation and scope mapping all have to line up, and each of them is easy to
    /// get individually right and jointly wrong.
    struct TestIssuer {
        encoding: jsonwebtoken::EncodingKey,
        jwks: String,
        kid: String,
    }

    impl TestIssuer {
        fn new() -> Self {
            use jsonwebtoken::EncodingKey;
            // A fixed 2048-bit key, so the suite does not spend a second generating
            // one and does not vary run to run.
            let der = include_bytes!("../tests/data/oidc-test-key.der");
            let (n, e) = rsa_public_components(der);
            let kid = "test-key-1".to_owned();
            let jwks = format!(
                r#"{{"keys":[{{"kty":"RSA","kid":"{kid}","use":"sig","alg":"RS256","n":"{n}","e":"{e}"}}]}}"#
            );
            Self {
                encoding: EncodingKey::from_rsa_der(der),
                jwks,
                kid,
            }
        }

        fn token(&self, claims: &serde_json::Value) -> String {
            let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
            header.kid = Some(self.kid.clone());
            jsonwebtoken::encode(&header, claims, &self.encoding).unwrap()
        }
    }

    /// Load a key set directly, so a test needs no network.
    fn loaded(config: OidcConfig, jwks: &str) -> Oidc {
        let oidc = Oidc::new(config);
        let keys = parse_jwks(jwks).unwrap();
        let mut set = oidc.keys.write().unwrap();
        set.keys = keys;
        set.fetched_at = Some(Instant::now());
        drop(set);
        oidc
    }

    fn claims(scope: &str) -> serde_json::Value {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        serde_json::json!({
            "iss": "https://id.example.com",
            "aud": "https://id.example.com",
            "sub": "user-1",
            "scope": scope,
            "iat": now,
            "exp": now + 900,
        })
    }

    #[test]
    fn a_genuine_token_grants_exactly_the_scope_it_carries() {
        let issuer = TestIssuer::new();
        let oidc = loaded(config(), &issuer.jwks);

        let read_only = issuer.token(&claims("telemetry:read"));
        assert_eq!(
            oidc.authorize(&read_only, Surface::Query).unwrap(),
            "user-1"
        );
        // Not a hierarchy: reading telemetry is not permission to write it or to see
        // the deployment.
        assert_eq!(
            oidc.authorize(&read_only, Surface::Ingest),
            Err(Rejected::NoMatchingScope)
        );
        assert_eq!(
            oidc.authorize(&read_only, Surface::Admin),
            Err(Rejected::NoMatchingScope)
        );

        // Several scopes in one token, as OAuth spells it.
        let both = issuer.token(&claims("telemetry:read telemetry:write"));
        assert!(oidc.authorize(&both, Surface::Query).is_ok());
        assert!(oidc.authorize(&both, Surface::Ingest).is_ok());
    }

    #[test]
    fn a_scope_that_merely_contains_the_name_does_not_count() {
        // `split(' ')` rather than `contains`: `telemetry:readonly` must not satisfy
        // `telemetry:read`, and a substring check would say it does.
        let issuer = TestIssuer::new();
        let oidc = loaded(config(), &issuer.jwks);
        let token = issuer.token(&claims("telemetry:readonly other:telemetry:read"));
        assert_eq!(
            oidc.authorize(&token, Surface::Query),
            Err(Rejected::NoMatchingScope)
        );
    }

    #[test]
    fn an_expired_token_is_refused() {
        let issuer = TestIssuer::new();
        let oidc = loaded(config(), &issuer.jwks);
        let mut expired = claims("telemetry:read");
        expired["exp"] = serde_json::json!(1_600_000_000);
        assert!(matches!(
            oidc.authorize(&issuer.token(&expired), Surface::Query),
            Err(Rejected::BadSignatureOrClaims(_))
        ));
    }

    #[test]
    fn a_token_from_another_issuer_or_for_another_audience_is_refused() {
        let issuer = TestIssuer::new();
        let oidc = loaded(config(), &issuer.jwks);

        let mut wrong_issuer = claims("telemetry:read");
        wrong_issuer["iss"] = serde_json::json!("https://evil.example.com");
        assert!(matches!(
            oidc.authorize(&issuer.token(&wrong_issuer), Surface::Query),
            Err(Rejected::BadSignatureOrClaims(_))
        ));

        // The confused-deputy case: a token minted for a different resource server on
        // the same issuer must not be replayable here.
        let mut wrong_audience = claims("telemetry:read");
        wrong_audience["aud"] = serde_json::json!("https://billing.example.com");
        assert!(matches!(
            oidc.authorize(&issuer.token(&wrong_audience), Surface::Query),
            Err(Rejected::BadSignatureOrClaims(_))
        ));
    }

    #[test]
    fn a_token_with_no_audience_is_refused_rather_than_waved_through() {
        // Cbox ID always sets one (RFC 9068 §2.2), so a token without it did not come
        // from a Cbox ID that is behaving.
        let issuer = TestIssuer::new();
        let oidc = loaded(config(), &issuer.jwks);
        let mut no_audience = claims("telemetry:read");
        no_audience.as_object_mut().unwrap().remove("aud");
        assert!(matches!(
            oidc.authorize(&issuer.token(&no_audience), Surface::Query),
            Err(Rejected::BadSignatureOrClaims(_))
        ));
    }

    #[test]
    fn a_token_signed_by_someone_else_is_refused() {
        // The same key id, a different key. This is the whole point of the exercise.
        let real = TestIssuer::new();
        let oidc = loaded(config(), &real.jwks);

        let attacker_key = jsonwebtoken::EncodingKey::from_secret(b"not the issuer's key");
        let mut header = jsonwebtoken::Header::new(Algorithm::HS256);
        header.kid = Some(real.kid.clone());
        let forged =
            jsonwebtoken::encode(&header, &claims("telemetry:admin"), &attacker_key).unwrap();

        // An HS256 token against an RSA key: the algorithm-confusion attack. It fails
        // because the algorithm comes from the *key*, never from the header.
        assert!(matches!(
            oidc.authorize(&forged, Surface::Admin),
            Err(Rejected::BadSignatureOrClaims(_))
        ));
    }

    /// Minimal DER walk to pull `n` and `e` out of a PKCS#1 RSA private key.
    ///
    /// Only used to build the JWK a test issuer publishes.
    fn rsa_public_components(der: &[u8]) -> (String, String) {
        // RSAPrivateKey ::= SEQUENCE { version, modulus, publicExponent, ... }
        let mut at = 0usize;
        let read_len = |bytes: &[u8], at: &mut usize| -> usize {
            let first = bytes[*at];
            *at += 1;
            if first & 0x80 == 0 {
                return usize::from(first);
            }
            let count = usize::from(first & 0x7f);
            let mut len = 0usize;
            for _ in 0..count {
                len = (len << 8) | usize::from(bytes[*at]);
                *at += 1;
            }
            len
        };
        assert_eq!(der[at], 0x30, "expected a SEQUENCE");
        at += 1;
        read_len(der, &mut at);
        assert_eq!(der[at], 0x02, "expected the version INTEGER");
        at += 1;
        let version_len = read_len(der, &mut at);
        at += version_len;

        let integer = |at: &mut usize| -> Vec<u8> {
            assert_eq!(der[*at], 0x02, "expected an INTEGER");
            *at += 1;
            let len = read_len(der, at);
            let value = der[*at..*at + len].to_vec();
            *at += len;
            // DER keeps a leading zero to mark the value positive; base64url of the
            // magnitude must not include it.
            value.into_iter().skip_while(|b| *b == 0).collect()
        };
        let modulus = integer(&mut at);
        let exponent = integer(&mut at);
        (base64_url(&modulus), base64_url(&exponent))
    }

    /// base64url without padding, written out rather than pulled in for a test.
    fn base64_url(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b = [
                chunk[0],
                *chunk.get(1).unwrap_or(&0),
                *chunk.get(2).unwrap_or(&0),
            ];
            let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
            for i in 0..=chunk.len() {
                out.push(ALPHABET[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
            }
        }
        out
    }
}
