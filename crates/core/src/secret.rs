//! Secret handling for the static bearer tokens guarding each surface.
//!
//! The invariant this module exists to enforce: **a token value can never reach a log
//! line, an error message, `/status`, or `telemetryd validate` output.** That is done
//! structurally rather than by discipline — [`Secret`] has no `Display`, its `Debug`
//! renders a placeholder, and its `Serialize` emits `"set"`/`"unset"`. A stray
//! `{:?}` or a `serde_json::to_string(&config)` therefore cannot leak.

use std::fmt;
use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::{Error, Result};

/// A configured secret *specification* — either a literal value or an indirection
/// (`file:/run/secrets/tok`, `env:MY_VAR`) that is resolved at startup.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Secret(String);

impl Secret {
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }

    /// Resolve any `file:` / `env:` indirection into the literal token value.
    ///
    /// A `file:` target that is group- or world-readable is loaded but produces a
    /// warning naming the mode — refusing outright would break too many legitimate
    /// setups, but staying silent would hide a real exposure.
    pub fn resolve(&self) -> Result<String> {
        let spec = self.0.trim();
        if let Some(path) = spec.strip_prefix("file:") {
            let path = Path::new(path);
            let value =
                std::fs::read_to_string(path).map_err(|source| Error::SecretUnreadable {
                    location: format!("file:{}", path.display()),
                    source,
                })?;
            warn_if_permissive(path);
            Ok(value.trim().to_owned())
        } else if let Some(var) = spec.strip_prefix("env:") {
            std::env::var(var).map_err(|_| Error::SecretMissing {
                location: format!("env:{var}"),
            })
        } else {
            Ok(spec.to_owned())
        }
    }
}

#[cfg(unix)]
fn warn_if_permissive(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let Ok(meta) = std::fs::metadata(path) else {
        return;
    };
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        tracing::warn!(
            path = %path.display(),
            mode = format!("{mode:04o}"),
            "token file is readable beyond its owner; consider chmod 600"
        );
    }
}

#[cfg(not(unix))]
fn warn_if_permissive(_path: &Path) {}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(de).map(Secret)
    }
}

impl Serialize for Secret {
    /// Serialises as a *status*, never a value. This is what makes it safe to dump the
    /// whole config as JSON in `/status` and `telemetryd validate`.
    fn serialize<S: Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        ser.serialize_str(if self.is_empty() { "unset" } else { "set" })
    }
}

/// Zero or more secrets guarding one surface. Accepts either a bare string or a list
/// in TOML, so `ingest_token = "abc"` and `ingest_token = ["old", "new"]` both work —
/// the list form exists so a token can be rotated with no rejection window.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(transparent)]
pub struct TokenSpecs(Vec<Secret>);

impl TokenSpecs {
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(Secret::is_empty)
    }

    /// The first configured token, in plain text, for a client that has to send one.
    ///
    /// Deliberately separate from `resolve`, which returns hashes because the server
    /// must never hold the plaintext longer than it takes to compare. This exists for
    /// the CLI talking to a *local* instance: the configuration is right there with the
    /// path to the token in it, and making an operator paste a credential they own is a
    /// small indignity the tool can spare them.
    ///
    /// Callers are responsible for not sending the result anywhere but loopback.
    pub fn first_value(&self) -> Option<String> {
        self.0
            .iter()
            .filter(|secret| !secret.is_empty())
            .find_map(|secret| secret.resolve().ok())
            .filter(|value| !value.is_empty())
    }

    /// Resolve every specification and pre-hash it for constant-time comparison.
    pub fn resolve(&self) -> Result<TokenSet> {
        let mut hashes = Vec::new();
        for secret in &self.0 {
            if secret.is_empty() {
                continue;
            }
            let value = secret.resolve()?;
            if value.is_empty() {
                return Err(Error::SecretEmpty);
            }
            hashes.push(digest(&value));
        }
        Ok(TokenSet(hashes))
    }
}

impl<'de> Deserialize<'de> for TokenSpecs {
    fn deserialize<D: Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            One(String),
            Many(Vec<String>),
        }

        Ok(match Repr::deserialize(de)? {
            Repr::One(s) => Self(vec![Secret(s)]),
            Repr::Many(v) => Self(v.into_iter().map(Secret).collect()),
        })
    }
}

impl Secret {
    /// Resolve the indirection and hash the result, ready for comparison.
    ///
    /// The plaintext exists only inside this call: it is hashed and dropped, so a
    /// relay client's credential is never held in memory in a form that could be
    /// logged.
    pub fn resolve_digest(&self) -> Result<[u8; 32]> {
        Ok(digest(&self.resolve()?))
    }
}

/// Resolved, pre-hashed tokens ready for request-time comparison.
#[derive(Clone, Default)]
pub struct TokenSet(Vec<[u8; 32]>);

impl TokenSet {
    /// `true` when this surface is unauthenticated.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Constant-time membership test.
    ///
    /// Both sides are SHA-256'd first so the comparison is over fixed-width input —
    /// that keeps the token's *length* from leaking through timing, which a direct
    /// byte comparison of variable-length strings would expose.
    pub fn verify(&self, presented: &str) -> bool {
        let candidate = digest(presented);
        let mut matched = subtle::Choice::from(0u8);
        for known in &self.0 {
            matched |= known.ct_eq(&candidate);
        }
        matched.into()
    }
}

/// Ingest credentials that each carry a fixed identity.
///
/// A relay decides what a client *is* from the credential it presented, so the lookup
/// answers "which app" rather than "was this valid at all".
///
/// Every entry is compared on every call. Returning as soon as one matches would make
/// the response time depend on a token's position in the list, and an attacker who can
/// measure that learns how close a guess is — the same reason `TokenSet::verify`
/// accumulates instead of returning early.
#[derive(Clone, Default)]
pub struct ClientTokens(Vec<([u8; 32], String)>);

impl ClientTokens {
    pub fn new(clients: Vec<([u8; 32], String)>) -> Self {
        Self(clients)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The app this credential is registered as, if it is one of ours.
    pub fn identify(&self, presented: &str) -> Option<&str> {
        let candidate = digest(presented);
        let mut found: Option<&str> = None;
        for (known, app) in &self.0 {
            let hit: bool = known.ct_eq(&candidate).into();
            if hit {
                found = Some(app);
            }
        }
        found
    }

    /// Whether this credential is one of ours at all, in constant time.
    pub fn verify(&self, presented: &str) -> bool {
        let candidate = digest(presented);
        let mut matched = subtle::Choice::from(0u8);
        for (known, _) in &self.0 {
            matched |= known.ct_eq(&candidate);
        }
        matched.into()
    }
}

impl fmt::Debug for ClientTokens {
    /// App names only. They are not secret — they end up on every record — but the
    /// digests have no business in a log line either.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientTokens")
            .field(
                "apps",
                &self.0.iter().map(|(_, app)| app).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenSet")
            .field("count", &self.0.len())
            .finish()
    }
}

fn digest(value: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn debug_and_serde_never_reveal_the_value() {
        let secret = Secret::new("super-secret-token");
        assert_eq!(format!("{secret:?}"), "Secret(<redacted>)");
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"set\"");
        assert_eq!(
            serde_json::to_string(&Secret::new("")).unwrap(),
            "\"unset\""
        );

        // The whole point: dumping a nested structure cannot leak it either.
        let nested = serde_json::to_string(&vec![secret]).unwrap();
        assert!(
            !nested.contains("super-secret"),
            "leaked through nested serialize: {nested}"
        );
    }

    #[test]
    fn token_set_verifies_any_configured_token() {
        let specs: TokenSpecs = serde_json::from_str(r#"["old-token", "new-token"]"#).unwrap();
        let set = specs.resolve().unwrap();
        assert!(set.verify("old-token"));
        assert!(set.verify("new-token"));
        assert!(!set.verify("other-token"));
        assert!(!set.verify(""));
    }

    #[test]
    fn accepts_scalar_or_list_form() {
        let one: TokenSpecs = serde_json::from_str(r#""solo""#).unwrap();
        assert!(one.resolve().unwrap().verify("solo"));

        let none: TokenSpecs = serde_json::from_str("[]").unwrap();
        assert!(none.resolve().unwrap().is_empty());
    }

    #[test]
    fn empty_token_set_authenticates_nothing() {
        let set = TokenSet::default();
        assert!(set.is_empty());
        assert!(!set.verify("anything"));
    }

    #[test]
    fn resolves_file_indirection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tok");
        std::fs::write(&path, "  from-file\n").unwrap();

        let secret = Secret::new(format!("file:{}", path.display()));
        assert_eq!(secret.resolve().unwrap(), "from-file");
    }

    #[test]
    fn missing_file_indirection_is_an_error_naming_the_location() {
        let secret = Secret::new("file:/nonexistent/telemetryd-token");
        let err = secret.resolve().unwrap_err().to_string();
        assert!(err.contains("/nonexistent/telemetryd-token"), "{err}");
    }
}
