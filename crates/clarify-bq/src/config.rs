use crate::cli::ConnArgs;
use clarify_bq_sink::{SecretRef, TokenProvider, fetch_secret};

/// Where the Clarify API key comes from — resolution guarantees exactly one.
#[derive(Debug)]
pub enum ApiKeySource {
    /// CLARIFY_API_KEY env override (local escape hatch; no GCP needed).
    Env(String),
    /// Google Secret Manager reference.
    Secret(SecretRef),
}

#[derive(Debug)]
pub struct Config {
    pub workspace: String,
    pub project: String,
    pub dataset: String,
    pub location: String,
    pub key_source: ApiKeySource,
}

impl Config {
    /// `api_key_override` is the CLARIFY_API_KEY env value, passed explicitly
    /// so resolution stays testable without touching process env.
    pub fn resolve(conn: &ConnArgs, api_key_override: Option<String>) -> Result<Config, String> {
        if conn.workspace.trim().is_empty() {
            return Err("--workspace is required".into());
        }
        if conn.project.trim().is_empty() {
            return Err("--project is required".into());
        }
        if !conn
            .dataset
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(format!(
                "dataset {:?} is invalid: BigQuery dataset IDs allow letters, digits, underscore only",
                conn.dataset
            ));
        }
        let key_source = match (api_key_override, &conn.secret) {
            (Some(key), _) => ApiKeySource::Env(key),
            (None, Some(s)) => {
                ApiKeySource::Secret(SecretRef::parse(s).map_err(|e| e.to_string())?)
            }
            (None, None) => {
                return Err(
                    "either --secret (Secret Manager ref) or CLARIFY_API_KEY env must be set"
                        .into(),
                );
            }
        };
        Ok(Config {
            workspace: conn.workspace.clone(),
            project: conn.project.clone(),
            dataset: conn.dataset.clone(),
            location: conn.location.clone(),
            key_source,
        })
    }

    /// Fetch or return the API key. `provider` is only consulted for the
    /// Secret Manager arm.
    pub async fn api_key(
        &self,
        provider: &dyn TokenProvider,
        secretmanager_base: &str,
    ) -> Result<String, String> {
        match &self.key_source {
            ApiKeySource::Env(key) => Ok(key.clone()),
            ApiKeySource::Secret(secret) => fetch_secret(secretmanager_base, provider, secret)
                .await
                .map_err(|e| format!("reading {}: {e}", secret.resource_name())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(secret: Option<&str>) -> ConnArgs {
        ConnArgs {
            workspace: "acme".into(),
            project: "demo-proj".into(),
            secret: secret.map(String::from),
            dataset: "clarify_crm".into(),
            location: "US".into(),
        }
    }

    #[test]
    fn resolves_with_secret_ref() {
        let cfg = Config::resolve(&conn(Some("projects/demo-proj/secrets/k")), None).unwrap();
        assert!(matches!(cfg.key_source, ApiKeySource::Secret(_)));
    }

    #[test]
    fn api_key_override_wins_over_secret() {
        let cfg =
            Config::resolve(&conn(Some("projects/p/secrets/k")), Some("sk_local".into())).unwrap();
        assert!(matches!(cfg.key_source, ApiKeySource::Env(ref k) if k == "sk_local"));
    }

    #[test]
    fn missing_secret_and_override_is_config_error() {
        assert!(Config::resolve(&conn(None), None).is_err());
    }

    #[test]
    fn hyphenated_dataset_rejected() {
        let mut c = conn(Some("projects/p/secrets/k"));
        c.dataset = "clarify-crm".into();
        let err = Config::resolve(&c, None).unwrap_err();
        assert!(err.contains("underscore"));
    }
}
