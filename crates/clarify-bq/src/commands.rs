use crate::cli::ExitCode;
use crate::config::{ApiKeySource, Config};
use crate::spool::RunSpool;
use clarify_bq_sink::{BqSink, SinkError, TableSpec, TokenProvider};
use clarify_bq_client::{ClarifyClient, ClientError};
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

fn client_error_exit(e: &ClientError) -> ExitCode {
    match e {
        ClientError::Auth { .. } => ExitCode::ConfigAuth,
        _ => ExitCode::Failed,
    }
}

/// `clarify-bq objects` — list discoverable object types.
pub async fn run_objects(client: &ClarifyClient) -> (ExitCode, String) {
    match client.fetch_schemas().await {
        Ok(schemas) => {
            let mut out = String::from("object\trelationships\n");
            let mut seen: Vec<&str> = Vec::new();
            for s in schemas.iter().filter(|s| s.object) {
                if seen.contains(&s.slug.as_str()) {
                    continue; // core/ and entities/ duplicates
                }
                seen.push(&s.slug);
                out.push_str(&format!("{}\t{}\n", s.slug, s.relationships.join(",")));
            }
            (ExitCode::Complete, out)
        }
        Err(e) => (client_error_exit(&e), e.to_string()),
    }
}

/// Report accumulator for `check`: every probe is recorded, any failure
/// flips the exit to ConfigAuth.
struct CheckReport {
    text: String,
    failed: bool,
}

impl CheckReport {
    fn new() -> Self {
        Self {
            text: String::new(),
            failed: false,
        }
    }

    fn step(&mut self, name: &str, result: Result<String, String>) {
        match result {
            Ok(detail) => self.text.push_str(&format!("ok    {name}: {detail}\n")),
            Err(e) => {
                self.failed = true;
                self.text.push_str(&format!("FAIL  {name}: {e}\n"));
            }
        }
    }

    fn finish(self) -> (ExitCode, String) {
        (
            if self.failed {
                ExitCode::ConfigAuth
            } else {
                ExitCode::Complete
            },
            self.text,
        )
    }
}

/// `clarify-bq check` — probe the real permissions on both sides, creating
/// nothing permanent.
pub async fn run_check(
    cfg: &Config,
    provider: &dyn TokenProvider,
    secretmanager_base: &str,
    clarify_base: &str,
    sink: &BqSink,
) -> (ExitCode, String) {
    let mut report = CheckReport::new();

    // 1. Clarify API key (Secret Manager unless env override).
    let api_key = match &cfg.key_source {
        ApiKeySource::Env(key) => {
            report.step(
                "secret",
                Ok("skipped (CLARIFY_API_KEY env override)".into()),
            );
            Some(key.clone())
        }
        ApiKeySource::Secret(secret) => match cfg.api_key(provider, secretmanager_base).await {
            Ok(key) => {
                report.step("secret", Ok(format!("read {}", secret.resource_name())));
                Some(key)
            }
            Err(e) => {
                report.step("secret", Err(e));
                None
            }
        },
    };

    // 2. Clarify schema fetch.
    if let Some(key) = api_key {
        let probe = async {
            let client = ClarifyClient::new(clarify_base.to_string(), key, cfg.workspace.clone())
                .map_err(|e| e.to_string())?;
            let schemas = client.fetch_schemas().await.map_err(|e| e.to_string())?;
            let mut slugs: Vec<&str> = schemas
                .iter()
                .filter(|s| s.object)
                .map(|s| s.slug.as_str())
                .collect();
            slugs.sort();
            slugs.dedup();
            Ok::<_, String>(format!("{} record objects discovered", slugs.len()))
        };
        report.step("clarify", probe.await);
    } else {
        report.step("clarify", Err("skipped: no API key".into()));
    }

    // 3. Dataset reachable + query permission. A missing dataset is not a
    // failure: the first backup run creates it.
    let sql = format!(
        "SELECT 1 FROM `{}.{}.INFORMATION_SCHEMA.TABLES` LIMIT 1",
        sink.project(),
        sink.dataset()
    );
    let mut dataset_absent = false;
    report.step(
        "dataset",
        match sink.query(&sql).await {
            Ok(_) => Ok(format!("{}.{} reachable", sink.project(), sink.dataset())),
            Err(SinkError::Http { status: 404, .. }) => {
                dataset_absent = true;
                Ok(format!(
                    "{}.{} does not exist yet; the first backup run will create it",
                    sink.project(),
                    sink.dataset()
                ))
            }
            Err(other) => Err(other.to_string()),
        },
    );

    // 4. Table create permission (scratch table, removed immediately).
    if dataset_absent {
        report.step("tables", Ok("skipped: dataset does not exist yet".into()));
        return report.finish();
    }
    let probe = async {
        let spec = TableSpec {
            name: "_clarify_bq_check".into(),
            ..crate::tables::spec_for("settings", Some(1))
        };
        sink.ensure_table(&spec).await.map_err(|e| e.to_string())?;
        sink.delete_table("_clarify_bq_check")
            .await
            .map_err(|e| e.to_string())?;
        Ok::<_, String>("create+delete _clarify_bq_check".to_string())
    };
    report.step("tables", probe.await);
    report.finish()
}

/// `clarify-bq views` — create/refresh the latest-snapshot flat views from the
/// live Clarify schemas.
pub async fn run_views(
    client: &ClarifyClient,
    sink: &BqSink,
    views_dataset: Option<String>,
) -> (ExitCode, String) {
    let schemas = match client.fetch_schemas().await {
        Ok(s) => s,
        Err(e) => return (client_error_exit(&e), e.to_string()),
    };
    let plan = match crate::plan::ResourcePlan::build(&schemas, &[], &[]) {
        Ok(p) => p,
        Err(e) => return (ExitCode::ConfigAuth, e),
    };
    match crate::views::refresh(
        sink,
        views_dataset,
        &plan,
        &crate::views::schema_defs(&schemas),
    )
    .await
    {
        Ok((dataset, n, errors)) => {
            let mut out = format!("{n} view(s) refreshed in {dataset}\n");
            for e in &errors {
                out.push_str(&format!("FAIL  {e}\n"));
            }
            (
                if errors.is_empty() {
                    ExitCode::Complete
                } else {
                    ExitCode::Partial
                },
                out,
            )
        }
        Err(e) => (ExitCode::ConfigAuth, e),
    }
}

/// `clarify-bq mark-complete <run_id>` — repair a run whose data loaded but
/// whose runs row failed to write. snapshot_at is derived from the UUIDv7.
pub async fn run_mark_complete(
    sink: &BqSink,
    run_id: &str,
    spool_root: &Path,
) -> (ExitCode, String) {
    let parsed = match uuid::Uuid::parse_str(run_id) {
        Ok(u) => u,
        Err(e) => return (ExitCode::ConfigAuth, format!("run_id is not a UUID: {e}")),
    };
    let Some(ts) = parsed.get_timestamp() else {
        return (
            ExitCode::ConfigAuth,
            "run_id is not a UUIDv7 (no timestamp)".into(),
        );
    };
    let (secs, nanos) = ts.to_unix();
    let snapshot_at =
        humantime::format_rfc3339_seconds(UNIX_EPOCH + Duration::new(secs, nanos)).to_string();

    let row = serde_json::json!({
        "run_id": run_id,
        "snapshot_at": snapshot_at,
        "finished_at": crate::runs::now_rfc3339(),
        "status": "complete",
        "resources": {"repaired": true},
    });
    let result = async {
        let spool = RunSpool::create(spool_root, &format!("repair-{run_id}"))
            .map_err(|e| format!("spool: {e}"))?;
        crate::runs::write_marker(sink, &spool, &row, run_id, "runs_repair", 1).await?;
        let _ = spool.remove();
        Ok::<_, String>(())
    }
    .await;
    match result {
        Ok(()) => (
            ExitCode::Complete,
            format!("marked run {run_id} complete (snapshot_at {snapshot_at})"),
        ),
        Err(e) => (ExitCode::Failed, e),
    }
}
