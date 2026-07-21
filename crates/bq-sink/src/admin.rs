use crate::SinkError;
use crate::token::TokenProvider;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct Column {
    pub name: &'static str,
    pub ty: &'static str,
}

pub struct TableSpec {
    pub name: String,
    pub columns: Vec<Column>,
    /// None = never expire (the `runs` ledger).
    pub partition_expiration_days: Option<u32>,
}

pub struct BqSink {
    pub(crate) http: reqwest::Client,
    pub(crate) provider: Arc<dyn TokenProvider>,
    pub(crate) base: String,
    pub(crate) project: String,
    pub(crate) dataset: String,
    pub(crate) location: String,
}

impl BqSink {
    pub fn new(
        provider: Arc<dyn TokenProvider>,
        base_url: String,
        project: String,
        dataset: String,
        location: String,
    ) -> Self {
        Self {
            http: reqwest::Client::new(),
            provider,
            base: base_url.trim_end_matches('/').to_string(),
            project,
            dataset,
            location,
        }
    }

    pub fn project(&self) -> &str {
        &self.project
    }

    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    pub(crate) async fn bearer(&self) -> Result<String, SinkError> {
        self.provider.token().await
    }

    async fn api(
        &self,
        method: reqwest::Method,
        url: String,
        body: Option<serde_json::Value>,
    ) -> Result<(u16, serde_json::Value), SinkError> {
        let token = self.bearer().await?;
        let mut req = self.http.request(method, &url).bearer_auth(token);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await?;
        let status = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        Ok((status, body))
    }

    fn expect_ok(status: u16, url: &str, body: &serde_json::Value) -> Result<(), SinkError> {
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(SinkError::Http { status, url: url.to_string(), body: body.to_string() })
        }
    }

    pub async fn ensure_dataset(&self) -> Result<(), SinkError> {
        let get = format!(
            "{}/bigquery/v2/projects/{}/datasets/{}",
            self.base, self.project, self.dataset
        );
        let (status, body) = self.api(reqwest::Method::GET, get.clone(), None).await?;
        if status == 404 {
            tracing::warn!(
                dataset = %self.dataset,
                location = %self.location,
                "creating dataset — location is immutable after creation"
            );
            let url = format!("{}/bigquery/v2/projects/{}/datasets", self.base, self.project);
            let body = serde_json::json!({
                "datasetReference": {"projectId": self.project, "datasetId": self.dataset},
                "location": self.location
            });
            let (s, b) = self.api(reqwest::Method::POST, url.clone(), Some(body)).await?;
            return Self::expect_ok(s, &url, &b);
        }
        Self::expect_ok(status, &get, &body)
    }

    /// Used by `check`'s permission probe to clean up its scratch table.
    pub async fn delete_table(&self, name: &str) -> Result<(), SinkError> {
        let url = format!(
            "{}/bigquery/v2/projects/{}/datasets/{}/tables/{}",
            self.base, self.project, self.dataset, name
        );
        let (status, body) = self.api(reqwest::Method::DELETE, url.clone(), None).await?;
        if status == 404 {
            return Ok(());
        }
        Self::expect_ok(status, &url, &body)
    }

    fn partitioning_json(spec: &TableSpec) -> serde_json::Value {
        let mut tp = serde_json::json!({"type": "DAY", "field": "snapshot_at"});
        if let Some(days) = spec.partition_expiration_days {
            tp["expirationMs"] =
                serde_json::Value::String((days as u64 * 86_400_000).to_string());
        }
        tp
    }

    pub async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError> {
        let tbl_url = format!(
            "{}/bigquery/v2/projects/{}/datasets/{}/tables/{}",
            self.base, self.project, self.dataset, spec.name
        );
        let (status, body) = self.api(reqwest::Method::GET, tbl_url.clone(), None).await?;
        if status == 404 {
            let fields: Vec<_> = spec
                .columns
                .iter()
                .map(|c| serde_json::json!({"name": c.name, "type": c.ty}))
                .collect();
            let url = format!(
                "{}/bigquery/v2/projects/{}/datasets/{}/tables",
                self.base, self.project, self.dataset
            );
            let body = serde_json::json!({
                "tableReference": {
                    "projectId": self.project, "datasetId": self.dataset, "tableId": spec.name
                },
                "schema": {"fields": fields},
                "timePartitioning": Self::partitioning_json(spec),
                "clustering": {"fields": ["run_id"]}
            });
            let (s, b) = self.api(reqwest::Method::POST, url.clone(), Some(body)).await?;
            return Self::expect_ok(s, &url, &b);
        }
        Self::expect_ok(status, &tbl_url, &body)?;
        // Re-assert expiration so a changed --partition-expiration-days takes effect.
        if spec.partition_expiration_days.is_some() {
            let body = serde_json::json!({"timePartitioning": Self::partitioning_json(spec)});
            let (s, b) = self.api(reqwest::Method::PATCH, tbl_url.clone(), Some(body)).await?;
            return Self::expect_ok(s, &tbl_url, &b);
        }
        Ok(())
    }
}
