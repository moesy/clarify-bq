use crate::ClientError;
use std::time::Duration;

const MAX_ATTEMPTS: u32 = 5;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ClarifyClient {
    http: reqwest::Client,
    base: String,
    api_key: String,
    pub workspace: String,
}

impl ClarifyClient {
    pub fn new(base_url: String, api_key: String, workspace: String) -> Result<Self, ClientError> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self {
            http,
            base: base_url.trim_end_matches('/').to_string(),
            api_key,
            workspace,
        })
    }

    fn url(&self, path_and_query: &str) -> String {
        // links.next comes back absolute; everything else is workspace-relative.
        if path_and_query.starts_with("http://") || path_and_query.starts_with("https://") {
            path_and_query.to_string()
        } else {
            format!(
                "{}/workspaces/{}{}",
                self.base, self.workspace, path_and_query
            )
        }
    }

    pub async fn get_json(&self, path_and_query: &str) -> Result<serde_json::Value, ClientError> {
        let url = self.url(path_and_query);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let resp = self
                .http
                .get(&url)
                .header("Authorization", format!("api-key {}", self.api_key))
                .send()
                .await?;
            let status = resp.status();
            if status.is_success() {
                return Ok(resp.json().await?);
            }
            if status.as_u16() == 401 || status.as_u16() == 403 {
                return Err(ClientError::Auth {
                    status: status.as_u16(),
                    hint: "check the API key secret and its workspace permissions".into(),
                });
            }
            let retryable = status.as_u16() == 429 || status.is_server_error();
            if !retryable || attempt >= MAX_ATTEMPTS {
                return Err(ClientError::Http {
                    status: status.as_u16(),
                    url,
                    attempts: attempt,
                });
            }
            let delay = resp
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_secs(1 << (attempt - 1).min(5)));
            tracing::warn!(
                status = status.as_u16(),
                attempt,
                delay_s = delay.as_secs(),
                "retrying Clarify request"
            );
            tokio::time::sleep(delay).await;
        }
    }
}
