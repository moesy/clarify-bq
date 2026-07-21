use crate::cli::ExitCode;
use crate::config::Config;
use crate::spool::RunSpool;
use bq_sink::{BqSink, Column, SinkError, TableSpec, TokenProvider, fetch_secret};
use clarify_client::ClarifyClient;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

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
        Err(e @ clarify_client::ClientError::Auth { .. }) => (ExitCode::ConfigAuth, e.to_string()),
        Err(e) => (ExitCode::Failed, e.to_string()),
    }
}

fn check_table_spec() -> TableSpec {
    TableSpec {
        name: "_clarify_bq_check".into(),
        columns: vec![
            Column {
                name: "run_id",
                ty: "STRING",
            },
            Column {
                name: "snapshot_at",
                ty: "TIMESTAMP",
            },
            Column {
                name: "data",
                ty: "JSON",
            },
        ],
        partition_expiration_days: Some(1),
    }
}

/// `clarify-bq check` — probe the real permissions on both sides, creating
/// nothing permanent. Every probe is reported; any failure exits 3.
pub async fn run_check(
    cfg: &Config,
    provider: &dyn TokenProvider,
    secretmanager_base: &str,
    clarify_base: &str,
    sink: &BqSink,
) -> (ExitCode, String) {
    let mut report = String::new();
    let mut failed = false;
    let mut step = |report: &mut String, name: &str, result: Result<String, String>| match result {
        Ok(detail) => report.push_str(&format!("ok    {name}: {detail}\n")),
        Err(e) => {
            failed = true;
            report.push_str(&format!("FAIL  {name}: {e}\n"));
        }
    };

    // 1. Clarify API key (Secret Manager unless env override).
    let api_key = match (&cfg.api_key_override, &cfg.secret) {
        (Some(key), _) => {
            step(
                &mut report,
                "secret",
                Ok("skipped (CLARIFY_API_KEY env override)".into()),
            );
            Some(key.clone())
        }
        (None, Some(secret)) => match fetch_secret(secretmanager_base, provider, secret).await {
            Ok(key) => {
                step(
                    &mut report,
                    "secret",
                    Ok(format!("read {}", secret.resource_name())),
                );
                Some(key)
            }
            Err(e) => {
                step(&mut report, "secret", Err(e.to_string()));
                None
            }
        },
        (None, None) => unreachable!("Config::resolve guarantees one source"),
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
        step(&mut report, "clarify", probe.await);
    } else {
        step(&mut report, "clarify", Err("skipped: no API key".into()));
    }

    // 3. Dataset reachable + query permission. A missing dataset is not a
    // failure: the first backup run creates it.
    let sql = format!(
        "SELECT 1 FROM `{}.{}.INFORMATION_SCHEMA.TABLES` LIMIT 1",
        sink.project(),
        sink.dataset()
    );
    let mut dataset_absent = false;
    step(
        &mut report,
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
        step(
            &mut report,
            "tables",
            Ok("skipped: dataset does not exist yet".into()),
        );
        return (
            if failed {
                ExitCode::ConfigAuth
            } else {
                ExitCode::Complete
            },
            report,
        );
    }
    let probe = async {
        sink.ensure_table(&check_table_spec())
            .await
            .map_err(|e| e.to_string())?;
        sink.delete_table("_clarify_bq_check")
            .await
            .map_err(|e| e.to_string())?;
        Ok::<_, String>("create+delete _clarify_bq_check".to_string())
    };
    step(&mut report, "tables", probe.await);

    (
        if failed {
            ExitCode::ConfigAuth
        } else {
            ExitCode::Complete
        },
        report,
    )
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
        Err(e @ clarify_client::ClientError::Auth { .. }) => {
            return (ExitCode::ConfigAuth, e.to_string());
        }
        Err(e) => return (ExitCode::Failed, e.to_string()),
    };
    let plan = match crate::plan::ResourcePlan::build(&schemas, &[], &[]) {
        Ok(p) => p,
        Err(e) => return (ExitCode::ConfigAuth, e),
    };
    let table_names = match crate::tables::records_table_names(&plan.objects) {
        Ok(t) => t,
        Err(e) => return (ExitCode::ConfigAuth, e),
    };
    let objects: Vec<(String, String, serde_json::Value)> = plan
        .objects
        .iter()
        .filter_map(|s| {
            let table = table_names.iter().find(|(sl, _)| sl == &s.slug)?.1.clone();
            Some((s.slug.clone(), table, s.raw.clone()))
        })
        .collect();
    let views_dataset = views_dataset.unwrap_or_else(|| format!("{}_latest", sink.dataset()));
    let (n, errors) = crate::views::create_latest_views(
        sink,
        &views_dataset,
        &objects,
        &crate::views::AUX_TABLES,
        &crate::views::schema_defs(&schemas),
    )
    .await;
    let mut out = format!("{n} view(s) refreshed in {views_dataset}\n");
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
    let finished_at = crate::runs::now_rfc3339();

    let row = serde_json::json!({
        "run_id": run_id,
        "snapshot_at": snapshot_at,
        "finished_at": finished_at,
        "status": "complete",
        "resources": {"repaired": true},
    });
    let result = async {
        let spool = RunSpool::create(spool_root, &format!("repair-{run_id}"))
            .map_err(|e| format!("spool: {e}"))?;
        let mut w = spool.writer("runs").map_err(|e| format!("spool: {e}"))?;
        w.write_row(&row).map_err(|e| format!("spool: {e}"))?;
        let (path, _) = w.finish().map_err(|e| format!("spool: {e}"))?;
        sink.ensure_table(&crate::tables::spec_for("runs", None))
            .await
            .map_err(|e| e.to_string())?;
        sink.load_ndjson("runs", "runs_repair", &path, run_id)
            .await
            .map_err(|e| e.to_string())?;
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
