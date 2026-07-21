use crate::SinkError;
use crate::token::TokenProvider;
use base64::Engine;

#[derive(Debug, Clone)]
pub struct SecretRef {
    project: String,
    secret: String,
    version: String,
}

impl SecretRef {
    /// Accepts `projects/<p>/secrets/<name>` (implies latest) or
    /// `projects/<p>/secrets/<name>/versions/<v>`.
    pub fn parse(s: &str) -> Result<Self, SinkError> {
        let parts: Vec<&str> = s.trim_matches('/').split('/').collect();
        match parts.as_slice() {
            ["projects", p, "secrets", sec] => Ok(Self {
                project: p.to_string(),
                secret: sec.to_string(),
                version: "latest".into(),
            }),
            ["projects", p, "secrets", sec, "versions", v] => Ok(Self {
                project: p.to_string(),
                secret: sec.to_string(),
                version: v.to_string(),
            }),
            _ => Err(SinkError::Config(format!(
                "secret ref must look like projects/<p>/secrets/<name>[/versions/<v>], got {s:?}"
            ))),
        }
    }

    pub fn resource_name(&self) -> String {
        format!(
            "projects/{}/secrets/{}/versions/{}",
            self.project, self.secret, self.version
        )
    }
}

pub async fn fetch_secret(
    base_url: &str,
    provider: &dyn TokenProvider,
    secret: &SecretRef,
) -> Result<String, SinkError> {
    let url = format!(
        "{}/v1/{}:access",
        base_url.trim_end_matches('/'),
        secret.resource_name()
    );
    let token = provider.token().await?;
    let resp = reqwest::Client::new()
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() {
        return Err(SinkError::Http {
            status: status.as_u16(),
            url,
            body: resp.text().await.unwrap_or_default(),
        });
    }
    let body: serde_json::Value = resp.json().await?;
    let b64 = body["payload"]["data"]
        .as_str()
        .ok_or_else(|| SinkError::Config("secret payload missing".into()))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| SinkError::Config(format!("secret payload not base64: {e}")))?;
    String::from_utf8(bytes).map_err(|e| SinkError::Config(format!("secret not utf8: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_short_ref_as_latest() {
        let r = SecretRef::parse("projects/demo-proj/secrets/clarify-key").unwrap();
        assert_eq!(
            r.resource_name(),
            "projects/demo-proj/secrets/clarify-key/versions/latest"
        );
    }

    #[test]
    fn parses_full_ref() {
        let r = SecretRef::parse("projects/demo-proj/secrets/k/versions/7").unwrap();
        assert_eq!(r.resource_name(), "projects/demo-proj/secrets/k/versions/7");
    }

    #[test]
    fn rejects_garbage() {
        assert!(SecretRef::parse("clarify-key").is_err());
    }
}
