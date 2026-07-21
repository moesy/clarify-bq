//! Unofficial async client for the Clarify CRM REST API.
pub mod envelope;
pub mod error;
pub mod http;
pub mod records;

pub use error::ClientError;
pub use http::ClarifyClient;
pub use records::{FetchStats, ItemSink};
