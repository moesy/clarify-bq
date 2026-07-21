use crate::cli::ConnArgs;
use bq_sink::SecretRef;

#[derive(Debug)]
pub struct Config {
    pub workspace: String,
    pub project: String,
    pub dataset: String,
    pub location: String,
    pub secret: Option<SecretRef>,
    pub api_key_override: Option<String>,
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
        if !conn.dataset.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "dataset {:?} is invalid: BigQuery dataset IDs allow letters, digits, underscore only",
                conn.dataset
            ));
        }
        let secret = match (&conn.secret, &api_key_override) {
            (Some(s), _) => Some(SecretRef::parse(s).map_err(|e| e.to_string())?),
            (None, Some(_)) => None,
            (None, None) => {
                return Err(
                    "either --secret (Secret Manager ref) or CLARIFY_API_KEY env must be set".into(),
                );
            }
        };
        Ok(Config {
            workspace: conn.workspace.clone(),
            project: conn.project.clone(),
            dataset: conn.dataset.clone(),
            location: conn.location.clone(),
            secret,
            api_key_override,
        })
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
        assert!(cfg.secret.is_some());
        assert!(cfg.api_key_override.is_none());
    }

    #[test]
    fn api_key_override_makes_secret_optional() {
        let cfg = Config::resolve(&conn(None), Some("sk_local".into())).unwrap();
        assert_eq!(cfg.api_key_override.as_deref(), Some("sk_local"));
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
