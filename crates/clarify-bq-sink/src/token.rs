use crate::SinkError;
use async_trait::async_trait;

#[async_trait]
pub trait TokenProvider: Send + Sync {
    async fn token(&self) -> Result<String, SinkError>;
}

/// ADC-backed provider. The token is re-fetched per call; `gcp_auth` caches and
/// refreshes internally, which keeps multi-hour runs safe across expiry.
pub struct GcpAuthProvider(std::sync::Arc<dyn gcp_auth::TokenProvider>);

impl GcpAuthProvider {
    pub async fn new() -> Result<Self, SinkError> {
        Ok(Self(
            gcp_auth::provider()
                .await
                .map_err(|e| SinkError::Auth(e.to_string()))?,
        ))
    }
}

#[async_trait]
impl TokenProvider for GcpAuthProvider {
    async fn token(&self) -> Result<String, SinkError> {
        let scopes = &["https://www.googleapis.com/auth/cloud-platform"];
        let t = self
            .0
            .token(scopes)
            .await
            .map_err(|e| SinkError::Auth(e.to_string()))?;
        Ok(t.as_str().to_string())
    }
}

pub struct StaticTokenProvider(pub String);

#[async_trait]
impl TokenProvider for StaticTokenProvider {
    async fn token(&self) -> Result<String, SinkError> {
        Ok(self.0.clone())
    }
}
