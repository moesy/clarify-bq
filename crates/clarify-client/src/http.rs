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
        // Clarify's edge rejects requests without a User-Agent (HTTP 403).
        let http = reqwest::Client::builder()
            .user_agent(concat!("clarify-bq/", env!("CARGO_PKG_VERSION")))
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
        // The API has been seen returning http:// next-links; when we talk to
        // an https base, upgrade them (port 80 is not served).
        if let Some(rest) = path_and_query.strip_prefix("http://") {
            if self.base.starts_with("https://") {
                return format!("https://{rest}");
            }
            return path_and_query.to_string();
        }
        if path_and_query.starts_with("https://") {
            path_and_query.to_string()
        } else {
            format!(
                "{}/workspaces/{}{}",
                self.base, self.workspace, path_and_query
            )
        }
    }

    #[cfg(test)]
    pub(crate) fn url_for_test(&self, p: &str) -> String {
        self.url(p)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_next_links_upgrade_to_https_only_on_https_base() {
        let secure =
            ClarifyClient::new("https://api.example/v1".into(), "k".into(), "acme".into()).unwrap();
        assert_eq!(
            secure.url_for_test("http://api.example/v1/workspaces/acme/x?page[offset]=500"),
            "https://api.example/v1/workspaces/acme/x?page[offset]=500"
        );
        // Local/test bases stay untouched.
        let plain =
            ClarifyClient::new("http://127.0.0.1:9999".into(), "k".into(), "acme".into()).unwrap();
        assert_eq!(
            plain.url_for_test("http://127.0.0.1:9999/workspaces/acme/x"),
            "http://127.0.0.1:9999/workspaces/acme/x"
        );
    }
}
