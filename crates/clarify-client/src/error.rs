use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("authentication rejected (HTTP {status}): {hint}")]
    Auth { status: u16, hint: String },
    #[error("HTTP {status} from {url} after {attempts} attempts")]
    Http { status: u16, url: String, attempts: u32 },
    #[error("retry budget exhausted for {url}")]
    RetriesExhausted { url: String },
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("unexpected response shape from {url}: {detail}")]
    Shape { url: String, detail: String },
}
