//! Query engines for the Loki, Tempo and Prometheus API subsets.
//!
//! The contract: `cboxdk/laravel-telemetry-ui` works against telemetryd unchanged,
//! with a single base URL. The subset it needs — frozen as `COMPATIBILITY.md` in M4 —
//! *is* the spec, and contract tests enforce it.
//!
//! # The subset boundary is a first-class output
//!
//! Each language is parsed in full and then lowered to what telemetryd can execute.
//! Anything outside the subset returns [`telemetryd_core::Error::Unsupported`], naming
//! the feature and linking `COMPATIBILITY.md`. Parsing fully rather than pattern-matching
//! the supported grammar is deliberate: it is the difference between "`predict_linear`
//! is not supported by telemetryd" and "syntax error".
//!
//! | API        | Endpoints                                                        | Milestone |
//! |------------|------------------------------------------------------------------|-----------|
//! | Loki       | `query_range`, `labels`, `label/{name}/values`, `tail` (WebSocket)| M1        |
//! | Tempo      | `traces/{id}`, `search`, `search/tags`, `search/tag/{name}/values`| M2        |
//! | Prometheus | `query`, `query_range`, `labels`, `label/{name}/values`, `series` | M3        |

#![doc(html_root_url = "https://docs.rs/telemetryd-query")]
