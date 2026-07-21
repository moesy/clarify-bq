//! BigQuery load-job sink with ADC auth and Secret Manager access.
pub mod admin;
pub mod error;
pub mod secret;
pub mod token;

pub use admin::{BqSink, Column, TableSpec};
pub use error::SinkError;
pub use secret::{SecretRef, fetch_secret};
pub use token::{GcpAuthProvider, StaticTokenProvider, TokenProvider};
