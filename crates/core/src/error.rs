//! The telemetryd error taxonomy.
//!
//! Transport-agnostic by design: `telemetryd-server` maps these onto HTTP status
//! codes and a JSON body. The variant that matters most for the product is
//! [`Error::Unsupported`] — every point where we hit the edge of a query-language
//! subset returns it, naming the feature and linking `COMPATIBILITY.md`, so a user is
//! never left guessing whether they hit a bug or a boundary.

use std::path::PathBuf;

use serde::Serialize;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // ---- configuration ----
    #[error("configuration is invalid: {0}")]
    Config(String),

    #[error("could not read configuration file {path}")]
    ConfigUnreadable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("secret at {location} could not be read")]
    SecretUnreadable {
        location: String,
        #[source]
        source: std::io::Error,
    },

    #[error("secret at {location} is not set")]
    SecretMissing { location: String },

    #[error("a configured token resolved to an empty value")]
    SecretEmpty,

    #[error("relay delivery failed: {0}")]
    RelayDelivery(String),

    // ---- request handling ----
    #[error("authentication required")]
    Unauthorized,

    #[error("{0}")]
    BadRequest(String),

    #[error("{0}")]
    NotFound(String),

    #[error("{feature} is not supported by telemetryd")]
    Unsupported {
        feature: String,
        hint: Option<String>,
    },

    #[error("{limit} exceeded: {detail}")]
    LimitExceeded { limit: &'static str, detail: String },

    #[error("ingest queue is full")]
    Overloaded,

    // ---- storage ----
    #[error(
        "data directory {path} was written with storage format v{found}, this build speaks v{expected}"
    )]
    StorageVersionMismatch {
        path: PathBuf,
        found: u32,
        expected: u32,
    },

    #[error("data directory {path} is locked by another telemetryd process")]
    DataDirLocked { path: PathBuf },

    #[error("{context}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },

    #[error("write-ahead log is corrupt at {path}: {detail}")]
    WalCorrupt { path: PathBuf, detail: String },
}

impl Error {
    /// Construct an [`Error::Unsupported`] for a query-language feature outside our
    /// subset.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported {
            feature: feature.into(),
            hint: None,
        }
    }

    /// As [`Self::unsupported`], but with a suggested alternative — worth the extra
    /// words whenever there is a supported way to express the same intent.
    pub fn unsupported_with_hint(feature: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::Unsupported {
            feature: feature.into(),
            hint: Some(hint.into()),
        }
    }

    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    /// Stable, machine-readable error code. Clients (and our own contract tests) match
    /// on this rather than on message text, so messages stay free to improve.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Config(_) | Self::ConfigUnreadable { .. } => "config_invalid",
            Self::SecretUnreadable { .. } | Self::SecretMissing { .. } | Self::SecretEmpty => {
                "secret_unavailable"
            }
            Self::Unauthorized => "unauthorized",
            Self::BadRequest(_) => "bad_request",
            Self::NotFound(_) => "not_found",
            Self::Unsupported { .. } => "unsupported_feature",
            Self::LimitExceeded { .. } => "limit_exceeded",
            Self::Overloaded => "overloaded",
            Self::StorageVersionMismatch { .. } => "storage_version_mismatch",
            Self::DataDirLocked { .. } => "data_dir_locked",
            Self::Io { .. } => "io_error",
            Self::RelayDelivery(_) => "relay_delivery",
            Self::WalCorrupt { .. } => "wal_corrupt",
        }
    }

    /// Render the wire representation. Kept here (rather than in the HTTP layer) so
    /// the shape is identical everywhere an error can surface.
    pub fn to_body(&self) -> ErrorBody {
        let (feature, hint, docs) = match self {
            Self::Unsupported { feature, hint } => (
                Some(feature.clone()),
                hint.clone(),
                Some(crate::COMPATIBILITY_DOC),
            ),
            _ => (None, None, None),
        };
        ErrorBody {
            error: ErrorDetail {
                code: self.code(),
                message: self.to_string(),
                feature,
                hint,
                docs,
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docs: Option<&'static str>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_errors_name_the_feature_and_link_the_docs() {
        let err = Error::unsupported_with_hint(
            "PromQL function `predict_linear`",
            "use `deriv` over a range selector",
        );
        let json = serde_json::to_value(err.to_body()).unwrap();

        assert_eq!(json["error"]["code"], "unsupported_feature");
        assert_eq!(json["error"]["feature"], "PromQL function `predict_linear`");
        assert_eq!(json["error"]["hint"], "use `deriv` over a range selector");
        assert_eq!(json["error"]["docs"], crate::COMPATIBILITY_DOC);
    }

    #[test]
    fn ordinary_errors_carry_no_feature_or_docs_keys() {
        let json = serde_json::to_value(Error::Unauthorized.to_body()).unwrap();
        assert_eq!(json["error"]["code"], "unauthorized");
        assert!(json["error"].get("feature").is_none());
        assert!(json["error"].get("docs").is_none());
    }
}
