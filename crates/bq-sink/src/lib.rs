//! BigQuery load-job sink with ADC auth and Secret Manager access.
pub mod error;
pub mod secret;
pub mod token;

pub use error::SinkError;
pub use secret::{SecretRef, fetch_secret};
pub use token::{GcpAuthProvider, StaticTokenProvider, TokenProvider};
