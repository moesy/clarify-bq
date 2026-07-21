//! Unofficial async client for the Clarify CRM REST API.
pub mod envelope;
pub mod error;
pub mod http;
pub mod records;
pub mod schemas;

pub use error::ClientError;
pub use http::ClarifyClient;
pub use records::{FetchStats, ItemSink};
pub use schemas::ObjectSchema;
