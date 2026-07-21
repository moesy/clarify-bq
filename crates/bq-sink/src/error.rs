use thiserror::Error;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("GCP auth: {0}")]
    Auth(String),
    #[error("HTTP {status} from {url}: {body}")]
    Http { status: u16, url: String, body: String },
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("config: {0}")]
    Config(String),
    #[error("BigQuery job {job_id} failed: {reason}")]
    JobFailed { job_id: String, reason: String },
}
