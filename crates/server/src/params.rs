//! Request parameters as the query APIs define them: repeatable, from the URL and a form.
//!
//! `match[]` is meant to repeat, and Prometheus also accepts parameters in an
//! `application/x-www-form-urlencoded` POST body. Deserialising into a struct or a map
//! kept one value per key — a repeated `match[]` either failed as a "duplicate field" or
//! silently kept the last — so every value is collected here and the handler asks for
//! what it wants.

use axum::extract::{FromRequest, Request};
use telemetryd_core::Error;

use crate::error::ApiError;

/// Every key/value pair, form body first and then URL, in order.
#[derive(Debug, Default)]
pub struct Pairs(Vec<(String, String)>);

impl Pairs {
    /// The first value of `key`, if any is non-empty. Body before URL, as Go's form
    /// parsing orders them.
    pub fn first(&self, key: &str) -> Option<String> {
        self.0
            .iter()
            .find(|(k, v)| k == key && !v.trim().is_empty())
            .map(|(_, v)| v.clone())
    }

    /// Every non-empty value of any of `keys`, in order.
    pub fn all(&self, keys: &[&str]) -> Vec<String> {
        self.0
            .iter()
            .filter(|(k, v)| keys.contains(&k.as_str()) && !v.trim().is_empty())
            .map(|(_, v)| v.clone())
            .collect()
    }
}

impl<S: Send + Sync> FromRequest<S> for Pairs {
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, ApiError> {
        let query = request.uri().query().unwrap_or_default().to_owned();
        let is_form = request
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/x-www-form-urlencoded"));
        let body = if is_form {
            axum::body::Bytes::from_request(request, state)
                .await
                .map_err(|e| Error::BadRequest(format!("could not read the form body: {e}")))?
        } else {
            axum::body::Bytes::new()
        };
        let mut pairs: Vec<(String, String)> =
            serde_urlencoded::from_bytes(&body).map_err(|e| bad(&e))?;
        pairs.extend(
            serde_urlencoded::from_str::<Vec<(String, String)>>(&query).map_err(|e| bad(&e))?,
        );
        Ok(Self(pairs))
    }
}

fn bad(error: &serde_urlencoded::de::Error) -> ApiError {
    Error::BadRequest(format!("invalid parameters: {error}")).into()
}
