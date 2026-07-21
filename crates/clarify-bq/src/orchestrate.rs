use crate::cli::{BackupArgs, ExitCode};
use crate::plan::{Category, ResourcePlan};
use crate::runs::{
    ResourceOutcome, overall_status, parse_prev_counts, prev_run_sql, runs_row, shrink_violations,
};
use crate::spool::{RunSpool, sweep_orphans};
use crate::tables::{records_table_names, sanitize, spec_for};
use crate::{lock::RunLock, spool::SpoolWriter};
use bq_sink::BqSink;
use clarify_client::{ClarifyClient, ClientError, ObjectSchema};
use futures::StreamExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const FETCH_CONCURRENCY: usize = 4;

use crate::runs::now_rfc3339;

fn json_id(item: &serde_json::Value) -> String {
    item["id"].as_str().unwrap_or_default().to_string()
}

/// One spooled resource awaiting load.
struct SpoolProduct {
    resource: String,
    table: String,
    path: Option<PathBuf>,
    outcome: ResourceOutcome,
}

struct FetchCtx<'a> {
    client: &'a ClarifyClient,
    spool: &'a RunSpool,
    run_id: &'a str,
    snapshot_at: &'a str,
}

impl FetchCtx<'_> {
    fn base_row(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("run_id".into(), self.run_id.into());
        m.insert("snapshot_at".into(), self.snapshot_at.into());
        m
    }

    fn outcome(
        &self,
        resource: &str,
        table: &str,
        started: String,
        result: Result<(u64, Option<u64>, &'static str), String>,
    ) -> ResourceOutcome {
        let (status, count, expected, consistency, error) = match result {
            Ok((count, expected, consistency)) => (
                "ok".to_string(),
                count,
                expected,
                consistency.to_string(),
                None,
            ),
            Err(e) => ("failed".to_string(), 0, None, "n/a".to_string(), Some(e)),
        };
        ResourceOutcome {
            resource: resource.into(),
            table: table.into(),
            status,
            count,
            expected,
            consistency,
            error,
            fetch_started_at: started,
            fetch_ended_at: now_rfc3339(),
        }
    }
}

/// Fetch one object's records (and, when enabled, its per-record activities and
/// attachments). Returns one SpoolProduct per spool file produced.
async fn fetch_object(
    ctx: &FetchCtx<'_>,
    schema: &ObjectSchema,
    table: &str,
    with_activities: bool,
    with_attachments: bool,
) -> Vec<SpoolProduct> {
    let mut products = Vec::new();
    let slug = schema.slug.clone();
    let started = now_rfc3339();
    let mut record_ids: Vec<String> = Vec::new();

    let records_result = async {
        let mut w = ctx.spool.writer(table).map_err(|e| ClientError::Shape {
            url: table.into(),
            detail: format!("spool: {e}"),
        })?;
        let stats = ctx
            .client
            .fetch_records(&slug, &schema.relationships, &mut |item| {
                let id = json_id(item);
                let mut row = ctx.base_row();
                row.insert("record_id".into(), id.clone().into());
                row.insert("object".into(), slug.clone().into());
                row.insert("data".into(), item.clone());
                record_ids.push(id);
                w.write_row(&serde_json::Value::Object(row))
            })
            .await?;
        let (path, _) = w.finish().map_err(|e| ClientError::Shape {
            url: table.into(),
            detail: format!("spool: {e}"),
        })?;
        Ok::<_, ClientError>((stats, path))
    }
    .await;

    // Some discovered objects have no records endpoint at all (e.g. Clarify's
    // agent feature): a 404 means "not backupable", which must not fail every
    // scheduled run. Skip it, loudly.
    let records_ok = records_result.is_ok();
    match records_result {
        Ok((stats, path)) => products.push(SpoolProduct {
            resource: table.to_string(),
            table: table.to_string(),
            path: Some(path),
            outcome: ctx.outcome(
                table,
                table,
                started,
                Ok((stats.fetched, stats.expected, stats.consistency())),
            ),
        }),
        Err(ClientError::Http { status: 404, .. }) => {
            tracing::warn!(object = %slug, "skipping object: no records endpoint (404)");
            let mut o = ctx.outcome(
                table,
                table,
                started,
                Err("object has no records endpoint (404); skipped".into()),
            );
            o.status = "skipped".into();
            products.push(SpoolProduct {
                resource: table.to_string(),
                table: table.to_string(),
                path: None,
                outcome: o,
            });
        }
        Err(e) => products.push(SpoolProduct {
            resource: table.to_string(),
            table: table.to_string(),
            path: None,
            outcome: ctx.outcome(table, table, started, Err(e.to_string())),
        }),
    }

    for (enabled, kind, id_col) in [
        (with_activities, "activities", "activity_id"),
        (with_attachments, "attachments", "attachment_id"),
    ] {
        if !enabled {
            continue;
        }
        let resource = format!("{kind}:{slug}");
        let spool_key = format!("{kind}__{}", sanitize(&slug));
        let started = now_rfc3339();
        if !records_ok {
            // Without the record list there is nothing to fan out over.
            let mut o = ctx.outcome(&resource, kind, started, Err("records fetch failed".into()));
            o.status = "skipped".into();
            products.push(SpoolProduct {
                resource,
                table: kind.into(),
                path: None,
                outcome: o,
            });
            continue;
        }
        let result = fetch_per_record(ctx, kind, id_col, &slug, &record_ids, &spool_key).await;
        let (path, outcome) = match result {
            Ok((path, count, errors, last_err)) => {
                let consistency = if errors == 0 { "clean" } else { "dirty" };
                let mut o = ctx.outcome(&resource, kind, started, Ok((count, None, consistency)));
                if errors > 0 {
                    o.error = Some(format!("{errors} record feed(s) skipped; last: {last_err}"));
                }
                (Some(path), o)
            }
            Err(e) => (None, ctx.outcome(&resource, kind, started, Err(e))),
        };
        products.push(SpoolProduct {
            resource,
            table: kind.into(),
            path,
            outcome,
        });
    }
    products
}

/// Fan out over records; a single record's failing feed (server bug, deleted
/// mid-run) is skipped and reported, not fatal. Only every-record-failing is a
/// resource failure. Returns (spool path, rows, skipped feeds, last error).
async fn fetch_per_record(
    ctx: &FetchCtx<'_>,
    kind: &str,
    id_col: &str,
    slug: &str,
    record_ids: &[String],
    spool_key: &str,
) -> Result<(PathBuf, u64, u64, String), String> {
    let mut w = ctx
        .spool
        .writer(spool_key)
        .map_err(|e| format!("spool: {e}"))?;
    let mut count = 0u64;
    let mut errors = 0u64;
    let mut consecutive = 0u64;
    let mut last_err = String::new();
    for (i, rid) in record_ids.iter().enumerate() {
        // Circuit breaker: an endpoint that fails for record after record is
        // broken for the whole object (seen live: every recurrence rule's
        // activity feed 500s) — burning a full retry budget per record turns
        // one broken endpoint into a quarter hour of backoff.
        if consecutive >= FEED_BREAKER {
            let remaining = (record_ids.len() - i) as u64;
            errors += remaining;
            tracing::warn!(object = %slug, kind, remaining,
                "circuit open after {FEED_BREAKER} consecutive feed failures; \
                 not attempting the rest");
            last_err = format!(
                "circuit opened after {FEED_BREAKER} consecutive failures \
                 ({remaining} feed(s) not attempted); last: {last_err}"
            );
            break;
        }
        let write = |w: &mut SpoolWriter, item: &serde_json::Value| {
            let mut row = ctx.base_row();
            row.insert("object".into(), slug.into());
            row.insert("record_id".into(), rid.clone().into());
            row.insert(id_col.into(), json_id(item).into());
            row.insert("data".into(), item.clone());
            w.write_row(&serde_json::Value::Object(row))
        };
        let stats = match kind {
            "activities" => {
                ctx.client
                    .fetch_record_activities(slug, rid, &mut |item| write(&mut w, item))
                    .await
            }
            _ => {
                ctx.client
                    .fetch_record_attachments(slug, rid, &mut |item| write(&mut w, item))
                    .await
            }
        };
        match stats {
            Ok(stats) => {
                count += stats.fetched;
                consecutive = 0;
            }
            Err(e) => {
                errors += 1;
                consecutive += 1;
                last_err = e.to_string();
                tracing::warn!(object = %slug, record = %rid, kind, error = %last_err,
                    "skipping one record's feed");
            }
        }
    }
    let (path, _) = w.finish().map_err(|e| format!("spool: {e}"))?;
    Ok((path, count, errors, last_err))
}

/// Consecutive per-record feed failures before the rest of an object's feeds
/// are skipped. The runs row records them as skipped (consistency: dirty).
const FEED_BREAKER: u64 = 3;

/// Fetch a flat resource (lists, users, workflows, settings, schemas snapshot,
/// list rows) into one spool.
async fn fetch_flat(
    ctx: &FetchCtx<'_>,
    resource: &str,
    fetch: impl AsyncFnOnce(&mut SpoolWriter) -> Result<u64, String>,
) -> SpoolProduct {
    let started = now_rfc3339();
    let result = async {
        let mut w = ctx
            .spool
            .writer(resource)
            .map_err(|e| format!("spool: {e}"))?;
        let count = fetch(&mut w).await?;
        let (path, _) = w.finish().map_err(|e| format!("spool: {e}"))?;
        Ok::<_, String>((path, count))
    }
    .await;
    let (path, outcome) = match result {
        Ok((path, count)) => (
            Some(path),
            ctx.outcome(resource, resource, started, Ok((count, None, "clean"))),
        ),
        Err(e) => (None, ctx.outcome(resource, resource, started, Err(e))),
    };
    SpoolProduct {
        resource: resource.into(),
        table: resource.into(),
        path,
        outcome,
    }
}

pub struct RunResult {
    pub exit: ExitCode,
    pub summary: serde_json::Value,
}

pub async fn run_backup(
    client: &ClarifyClient,
    sink: &BqSink,
    args: &BackupArgs,
    spool_root: &Path,
) -> RunResult {
    let run_id = uuid::Uuid::now_v7().to_string();
    let snapshot_at = now_rfc3339();

    let fail = |msg: String, exit: ExitCode| RunResult {
        exit,
        summary: serde_json::json!({"run_id": run_id, "error": msg}),
    };

    // Lock before any network traffic: overlapping runs contend on rate limits.
    let _lock = if args.no_lock {
        None
    } else {
        match RunLock::acquire(spool_root) {
            Ok(Some(l)) => Some(l),
            Ok(None) => {
                return fail("another run holds the lock".into(), ExitCode::LockHeld);
            }
            Err(e) => return fail(format!("lockfile: {e}"), ExitCode::Failed),
        }
    };

    // Discover.
    let schemas = match client.fetch_schemas().await {
        Ok(s) => s,
        Err(e @ ClientError::Auth { .. }) => return fail(e.to_string(), ExitCode::ConfigAuth),
        Err(e) => return fail(e.to_string(), ExitCode::Failed),
    };
    let plan = match ResourcePlan::build(&schemas, &args.objects, &args.skip) {
        Ok(p) => p,
        Err(e) => return fail(e, ExitCode::ConfigAuth),
    };
    let table_names = match records_table_names(&plan.objects) {
        Ok(t) => t,
        Err(e) => return fail(e, ExitCode::ConfigAuth),
    };
    tracing::info!(run_id = %run_id, plan = %plan.describe(), "resolved backup plan");
    println!("plan: {}", plan.describe());

    if args.dry_run {
        return RunResult {
            exit: ExitCode::Complete,
            summary: serde_json::json!({
                "run_id": run_id, "dry_run": true, "plan": plan.describe(),
                "objects": table_names.iter().map(|(s, t)| {
                    serde_json::json!({"object": s, "table": t})
                }).collect::<Vec<_>>(),
            }),
        };
    }

    for removed in sweep_orphans(spool_root, &run_id).unwrap_or_default() {
        tracing::info!(orphan = %removed, "removed stale spool directory");
    }
    let spool = match RunSpool::create(spool_root, &run_id) {
        Ok(s) => s,
        Err(e) => return fail(format!("spool: {e}"), ExitCode::Failed),
    };
    let ctx = FetchCtx {
        client,
        spool: &spool,
        run_id: &run_id,
        snapshot_at: &snapshot_at,
    };

    // ---- Fetch phase ----
    let mut products: Vec<SpoolProduct> = Vec::new();
    if plan.includes(Category::Records) {
        let with_act = plan.includes(Category::Activities);
        let with_att = plan.includes(Category::Attachments);
        let mut object_jobs = futures::stream::iter(plan.objects.iter().map(|schema| {
            let table = table_names
                .iter()
                .find(|(s, _)| s == &schema.slug)
                .map(|(_, t)| t.clone())
                .expect("table name exists for every planned object");
            let ctx = &ctx;
            async move { fetch_object(ctx, schema, &table, with_act, with_att).await }
        }))
        .buffer_unordered(FETCH_CONCURRENCY);
        while let Some(mut p) = object_jobs.next().await {
            products.append(&mut p);
        }
    }
    if plan.includes(Category::Schemas) {
        let all = &schemas;
        products.push(
            fetch_flat(&ctx, "schemas", async |w| {
                for s in all {
                    let mut row = ctx.base_row();
                    row.insert("object".into(), s.slug.clone().into());
                    row.insert("data".into(), s.raw.clone());
                    w.write_row(&serde_json::Value::Object(row))
                        .map_err(|e| format!("spool: {e}"))?;
                }
                Ok(all.len() as u64)
            })
            .await,
        );
    }
    if plan.includes(Category::Lists) || plan.includes(Category::ListRows) {
        let mut lists: Vec<(String, String)> = Vec::new(); // (entity, list_id)
        let lists_product = fetch_flat(&ctx, "lists", async |w| {
            let mut sink_err = Ok::<(), String>(());
            let stats = ctx
                .client
                .fetch_linked("/lists", &mut |item| {
                    let entity = item["attributes"]["entity"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    let id = json_id(item);
                    lists.push((entity.clone(), id.clone()));
                    let mut row = ctx.base_row();
                    row.insert("list_id".into(), id.into());
                    row.insert("object".into(), entity.into());
                    row.insert("data".into(), item.clone());
                    w.write_row(&serde_json::Value::Object(row))
                })
                .await
                .map_err(|e| e.to_string());
            if let Err(e) = &stats {
                sink_err = Err(e.clone());
            }
            sink_err?;
            Ok(stats.unwrap().fetched)
        })
        .await;
        let lists_ok = lists_product.outcome.status == "ok";
        if plan.includes(Category::Lists) {
            products.push(lists_product);
        }
        if plan.includes(Category::ListRows) && lists_ok {
            // Lists can reference objects with no records endpoint (e.g. the
            // TAM feature's tam_* entities): skip those, and tolerate a
            // per-list 404 rather than failing the whole resource.
            // All discovered record objects, not just --objects-narrowed ones:
            // a deal list's rows still back up when only person records do.
            let queryable: Vec<&str> = schemas
                .iter()
                .filter(|s| s.object)
                .map(|s| s.slug.as_str())
                .collect();
            products.push(
                fetch_flat(&ctx, "list_rows", async |w| {
                    let mut count = 0u64;
                    for (entity, list_id) in &lists {
                        if !queryable.contains(&entity.as_str()) {
                            tracing::warn!(list = %list_id, object = %entity,
                                "skipping list rows: object has no records endpoint");
                            continue;
                        }
                        let fetched = ctx
                            .client
                            .fetch_list_rows(entity, list_id, &mut |item| {
                                let mut row = ctx.base_row();
                                row.insert("list_id".into(), list_id.clone().into());
                                row.insert("object".into(), entity.clone().into());
                                row.insert("record_id".into(), json_id(item).into());
                                row.insert("data".into(), item.clone());
                                w.write_row(&serde_json::Value::Object(row))
                            })
                            .await;
                        match fetched {
                            Ok(stats) => count += stats.fetched,
                            Err(ClientError::Http { status: 404, .. }) => {
                                tracing::warn!(list = %list_id, object = %entity,
                                    "skipping list rows: list not queryable (404)");
                            }
                            Err(e) => return Err(e.to_string()),
                        }
                    }
                    Ok(count)
                })
                .await,
            );
        }
    }
    for (cat, path, id_col) in [
        (Category::Users, "/users", "id"),
        (Category::Workflows, "/workflows", "id"),
    ] {
        if !plan.includes(cat) {
            continue;
        }
        let resource = cat.name();
        products.push(
            fetch_flat(&ctx, resource, async |w| {
                let stats = ctx
                    .client
                    .fetch_linked(path, &mut |item| {
                        let mut row = ctx.base_row();
                        row.insert(id_col.into(), json_id(item).into());
                        row.insert("data".into(), item.clone());
                        w.write_row(&serde_json::Value::Object(row))
                    })
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(stats.fetched)
            })
            .await,
        );
    }
    if plan.includes(Category::Settings) {
        products.push(
            fetch_flat(&ctx, "settings", async |w| {
                let doc = ctx
                    .client
                    .fetch_settings()
                    .await
                    .map_err(|e| e.to_string())?;
                let mut row = ctx.base_row();
                row.insert("data".into(), doc);
                w.write_row(&serde_json::Value::Object(row))
                    .map_err(|e| format!("spool: {e}"))?;
                Ok(1)
            })
            .await,
        );
    }

    // ---- Load phase ----
    if let Err(e) = sink.ensure_dataset().await {
        return fail(format!("ensure dataset: {e}"), ExitCode::Failed);
    }
    let expiration = (args.partition_expiration_days > 0).then_some(args.partition_expiration_days);
    let mut ensured: Vec<String> = Vec::new();
    for p in &mut products {
        let Some(path) = p.path.clone() else { continue };
        if p.outcome.status != "ok" {
            continue;
        }
        if !ensured.contains(&p.table) {
            if let Err(e) = sink.ensure_table(&spec_for(&p.table, expiration)).await {
                p.outcome.status = "failed".into();
                p.outcome.error = Some(format!("ensure table: {e}"));
                continue;
            }
            ensured.push(p.table.clone());
        }
        let job_key = sanitize(&p.resource);
        if let Err(e) = sink.load_ndjson(&p.table, &job_key, &path, &run_id).await {
            p.outcome.status = "failed".into();
            p.outcome.error = Some(format!("load: {e}"));
        }
    }

    let outcomes: Vec<ResourceOutcome> = products.iter().map(|p| p.outcome.clone()).collect();
    let status = overall_status(&outcomes);

    // ---- Shrink check ----
    let mut violations: Vec<String> = Vec::new();
    if !args.no_shrink_check && status == "complete" {
        let prev = match sink
            .query(&prev_run_sql(&sink_project(sink), &sink_dataset(sink)))
            .await
        {
            Ok(rows) => rows
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.as_str().map(parse_prev_counts))
                .unwrap_or_default(),
            Err(e) => {
                tracing::warn!(error = %e, "previous-run lookup failed; skipping shrink check");
                HashMap::new()
            }
        };
        let curr: HashMap<String, u64> = outcomes
            .iter()
            .filter(|o| o.status == "ok")
            .map(|o| (o.resource.clone(), o.count))
            .collect();
        violations = shrink_violations(&prev, &curr, args.shrink_threshold);
        for v in &violations {
            tracing::error!(violation = %v, "suspicious shrink versus previous complete run");
        }
    }

    // ---- Runs row (the completeness marker; written last, retried hard) ----
    let finished_at = now_rfc3339();
    let row = runs_row(&run_id, &snapshot_at, &finished_at, status, &outcomes);
    let marker = async {
        let w_path = {
            let mut w = spool.writer("runs").map_err(|e| format!("spool: {e}"))?;
            w.write_row(&row).map_err(|e| format!("spool: {e}"))?;
            w.finish().map_err(|e| format!("spool: {e}"))?.0
        };
        sink.ensure_table(&spec_for("runs", None))
            .await
            .map_err(|e| e.to_string())?;
        let mut last = String::new();
        for _ in 0..3 {
            match sink.load_ndjson("runs", "runs", &w_path, &run_id).await {
                Ok(_) => return Ok(()),
                Err(e) => last = e.to_string(),
            }
        }
        Err(last)
    }
    .await;
    if let Err(e) = marker {
        eprintln!(
            "DATA LOADED BUT UNMARKED, run_id={run_id}: the runs row could not be written ({e}). \
             Repair with: clarify-bq mark-complete {run_id}"
        );
        return RunResult {
            exit: ExitCode::Failed,
            summary: serde_json::json!({"run_id": run_id, "status": "unmarked", "error": e}),
        };
    }

    if status == "complete"
        && let Err(e) = spool.remove()
    {
        tracing::warn!(error = %e, "spool cleanup failed (will be swept next run)");
    }

    // ---- Latest views (best-effort: data freshness is dynamic, this only
    // keeps view columns in sync with the CRM schema) ----
    let mut views_summary = serde_json::Value::Null;
    if !args.no_views && status != "failed" {
        let views_dataset = args
            .views_dataset
            .clone()
            .unwrap_or_else(|| format!("{}_latest", sink_dataset(sink)));
        let view_objects: Vec<(String, String, serde_json::Value)> = plan
            .objects
            .iter()
            .filter_map(|s| {
                let table = table_names.iter().find(|(sl, _)| sl == &s.slug)?.1.clone();
                ensured
                    .contains(&table)
                    .then(|| (s.slug.clone(), table, s.raw.clone()))
            })
            .collect();
        let aux: Vec<&str> = ensured
            .iter()
            .map(String::as_str)
            .filter(|t| !t.starts_with("records_"))
            .chain(std::iter::once("runs"))
            .collect();
        let (n, view_errors) = crate::views::create_latest_views(
            sink,
            &views_dataset,
            &view_objects,
            &aux,
            &crate::views::schema_defs(&schemas),
        )
        .await;
        for e in &view_errors {
            tracing::error!(error = %e, "latest view refresh failed (data is safe; \
                re-run with: clarify-bq views)");
        }
        tracing::info!(dataset = %views_dataset, views = n, "latest views refreshed");
        views_summary = serde_json::json!({
            "dataset": views_dataset, "created": n, "errors": view_errors,
        });
    }

    let exit = match status {
        "failed" => ExitCode::Failed,
        "partial" => ExitCode::Partial,
        _ if !violations.is_empty() => ExitCode::ShrinkCheck,
        _ => ExitCode::Complete,
    };
    RunResult {
        exit,
        summary: serde_json::json!({
            "run_id": run_id,
            "snapshot_at": snapshot_at,
            "finished_at": finished_at,
            "status": status,
            "shrink_violations": violations,
            "views": views_summary,
            "resources": outcomes,
        }),
    }
}

// BqSink keeps its fields private to the bq-sink crate; the shrink-check SQL
// needs project/dataset, so thread them through args-free accessors here.
fn sink_project(sink: &BqSink) -> String {
    sink.project().to_string()
}
fn sink_dataset(sink: &BqSink) -> String {
    sink.dataset().to_string()
}
