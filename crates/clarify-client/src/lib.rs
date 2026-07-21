//! Unofficial async client for the Clarify CRM REST API.
pub mod envelope;
pub mod error;
pub mod http;

pub use error::ClientError;
pub use http::ClarifyClient;
