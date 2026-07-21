use crate::cli::{BackupArgs, ExitCode};
use crate::plan::{Category, ResourcePlan};
use crate::runs::{
    ResourceOutcome, now_rfc3339, overall_status, parse_prev_counts, prev_run_sql, runs_row,
    shrink_violations, write_marker,
};
use crate::spool::{RunSpool, SpoolWriter, sweep_orphans};
use crate::tables::{records_table_names, sanitize, spec_for};
use crate::{lock::RunLock, views};
use clarify_bq_sink::BqSink;
use clarify_bq_client::{ClarifyClient, ClientError, ObjectSchema};
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const FETCH_CONCURRENCY: usize = 4;
/// Concurrent per-record feed requests within one object.
const FEED_CONCURRENCY: usize = 4;
/// Consecutive per-record feed failures before the rest of an object's feeds
/// are skipped. The runs row records them as skipped (consistency: dirty).
const FEED_BREAKER: u64 = 3;

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
    /// Spool one wrapper row: run_id/snapshot_at + extras + `data`.
    fn write_row(
        &self,
        w: &mut SpoolWriter,
        extras: Vec<(&str, serde_json::Value)>,
        data: serde_json::Value,
    ) -> std::io::Result<()> {
        let mut row = serde_json::Map::new();
        row.insert("run_id".into(), self.run_id.into());
        row.insert("snapshot_at".into(), self.snapshot_at.into());
        for (k, v) in extras {
            row.insert(k.into(), v);
        }
        row.insert("data".into(), data);
        w.write_row(&serde_json::Value::Object(row))
    }

    fn outcome_base(&self, resource: &str, table: &str, started: String) -> ResourceOutcome {
        ResourceOutcome {
            resource: resource.into(),
            table: table.into(),
            status: "ok".into(),
            count: 0,
            expected: None,
            consistency: "clean".into(),
            error: None,
            fetch_started_at: started,
            fetch_ended_at: now_rfc3339(),
        }
    }

    fn ok_outcome(
        &self,
        resource: &str,
        table: &str,
        started: String,
        count: u64,
        expected: Option<u64>,
        consistency: &str,
    ) -> ResourceOutcome {
        ResourceOutcome {
            count,
            expected,
            consistency: consistency.into(),
            ..self.outcome_base(resource, table, started)
        }
    }

    fn failed_outcome(
        &self,
        resource: &str,
        table: &str,
        started: String,
        error: String,
    ) -> ResourceOutcome {
        ResourceOutcome {
            status: "failed".into(),
            consistency: "n/a".into(),
            error: Some(error),
            ..self.outcome_base(resource, table, started)
        }
    }

    fn skipped_outcome(
        &self,
        resource: &str,
        table: &str,
        started: String,
        reason: String,
    ) -> ResourceOutcome {
        ResourceOutcome {
            status: "skipped".into(),
            consistency: "n/a".into(),
            error: Some(reason),
            ..self.outcome_base(resource, table, started)
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
        let mut w = ctx.spool.writer(table)?;
        let stats = ctx
            .client
            .fetch_records(&slug, &schema.relationships, &mut |item| {
                let id = json_id(&item);
                record_ids.push(id.clone());
                ctx.write_row(
                    &mut w,
                    vec![("record_id", id.into()), ("object", slug.clone().into())],
                    item,
                )
            })
            .await?;
        let (path, _) = w.finish()?;
        Ok::<_, ClientError>((stats, path))
    }
    .await;

    // Some discovered objects have no records endpoint at all (e.g. Clarify's
    // agent feature): a 404 means "not backupable", which must not fail every
    // scheduled run. Skip it, loudly.
    let records_ok = records_result.is_ok();
    let product = match records_result {
        Ok((stats, path)) => SpoolProduct {
            resource: table.to_string(),
            table: table.to_string(),
            path: Some(path),
            outcome: ctx.ok_outcome(
                table,
                table,
                started,
                stats.fetched,
                stats.expected,
                stats.consistency(),
            ),
        },
        Err(ClientError::Http { status: 404, .. }) => {
            tracing::warn!(object = %slug, "skipping object: no records endpoint (404)");
            SpoolProduct {
                resource: table.to_string(),
                table: table.to_string(),
                path: None,
                outcome: ctx.skipped_outcome(
                    table,
                    table,
                    started,
                    "object has no records endpoint (404); skipped".into(),
                ),
            }
        }
        Err(e) => SpoolProduct {
            resource: table.to_string(),
            table: table.to_string(),
            path: None,
            outcome: ctx.failed_outcome(table, table, started, e.to_string()),
        },
    };
    products.push(product);

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
            products.push(SpoolProduct {
                resource: resource.clone(),
                table: kind.into(),
                path: None,
                outcome: ctx.skipped_outcome(
                    &resource,
                    kind,
                    started,
                    "records fetch failed".into(),
                ),
            });
            continue;
        }
        let (path, outcome) =
            match fetch_per_record(ctx, kind, id_col, &slug, &record_ids, &spool_key).await {
                Ok(feed) => {
                    let consistency = if feed.skipped == 0 { "clean" } else { "dirty" };
                    let mut o =
                        ctx.ok_outcome(&resource, kind, started, feed.rows, None, consistency);
                    if feed.skipped > 0 {
                        o.error = Some(format!(
                            "{} record feed(s) skipped; last: {}",
                            feed.skipped, feed.last_err
                        ));
                    }
                    (Some(feed.path), o)
                }
                Err(e) => (None, ctx.failed_outcome(&resource, kind, started, e)),
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

struct FeedFetch {
    path: PathBuf,
    rows: u64,
    skipped: u64,
    last_err: String,
}

/// Fan out over records concurrently; a single record's failing feed (server
/// bug, deleted mid-run) is skipped and reported, not fatal. A run of
/// FEED_BREAKER consecutive failures means the endpoint is broken for the
/// whole object — the rest are skipped without being attempted.
async fn fetch_per_record(
    ctx: &FetchCtx<'_>,
    kind: &str,
    id_col: &str,
    slug: &str,
    record_ids: &[String],
    spool_key: &str,
) -> Result<FeedFetch, String> {
    let mut w = ctx
        .spool
        .writer(spool_key)
        .map_err(|e| format!("spool: {e}"))?;
    let mut feed = FeedFetch {
        path: PathBuf::new(),
        rows: 0,
        skipped: 0,
        last_err: String::new(),
    };
    let mut consecutive = 0u64;
    let mut processed = 0usize;

    // Each future fetches one record's feed into its own buffer; rows are
    // written on the ordered main loop, so the breaker stays deterministic.
    let mut feeds = futures::stream::iter(record_ids.iter().map(|rid| async move {
        let mut items: Vec<serde_json::Value> = Vec::new();
        let result = match kind {
            "activities" => {
                ctx.client
                    .fetch_record_activities(slug, rid, &mut |item| {
                        items.push(item);
                        Ok(())
                    })
                    .await
            }
            _ => {
                ctx.client
                    .fetch_record_attachments(slug, rid, &mut |item| {
                        items.push(item);
                        Ok(())
                    })
                    .await
            }
        };
        (rid, result.map(|_| items))
    }))
    .buffered(FEED_CONCURRENCY);

    while let Some((rid, result)) = feeds.next().await {
        processed += 1;
        match result {
            Ok(items) => {
                consecutive = 0;
                for item in items {
                    feed.rows += 1;
                    ctx.write_row(
                        &mut w,
                        vec![
                            ("object", slug.into()),
                            ("record_id", rid.clone().into()),
                            (id_col, json_id(&item).into()),
                        ],
                        item,
                    )
                    .map_err(|e| format!("spool: {e}"))?;
                }
            }
            Err(e) => {
                feed.skipped += 1;
                consecutive += 1;
                feed.last_err = e.to_string();
                tracing::warn!(object = %slug, record = %rid, kind, error = %feed.last_err,
                    "skipping one record's feed");
            }
        }
        if consecutive >= FEED_BREAKER {
            let remaining = (record_ids.len() - processed) as u64;
            feed.skipped += remaining;
            tracing::warn!(object = %slug, kind, remaining,
                "circuit open after {FEED_BREAKER} consecutive feed failures; \
                 not attempting the rest");
            feed.last_err = format!(
                "circuit opened after {FEED_BREAKER} consecutive failures \
                 ({remaining} feed(s) not attempted); last: {}",
                feed.last_err
            );
            break;
        }
    }
    drop(feeds);
    feed.path = w.finish().map_err(|e| format!("spool: {e}"))?.0;
    Ok(feed)
}

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
            ctx.ok_outcome(resource, resource, started, count, None, "clean"),
        ),
        Err(e) => (None, ctx.failed_outcome(resource, resource, started, e)),
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
        let mut objects: Vec<_> = table_names
            .iter()
            .map(|(s, t)| serde_json::json!({"object": s, "table": t}))
            .collect();
        objects.sort_by_key(|o| o["object"].as_str().unwrap_or_default().to_string());
        return RunResult {
            exit: ExitCode::Complete,
            summary: serde_json::json!({
                "run_id": run_id, "dry_run": true, "plan": plan.describe(),
                "objects": objects,
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
            let table = table_names[&schema.slug].clone();
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
                    ctx.write_row(w, vec![("object", s.slug.clone().into())], s.raw.clone())
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
            let stats = ctx
                .client
                .fetch_linked("/lists", &mut |item| {
                    let entity = item["attributes"]["entity"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    let id = json_id(&item);
                    lists.push((entity.clone(), id.clone()));
                    ctx.write_row(
                        w,
                        vec![("list_id", id.into()), ("object", entity.into())],
                        item,
                    )
                })
                .await
                .map_err(|e| e.to_string())?;
            Ok(stats.fetched)
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
            let queryable: HashSet<&str> = schemas
                .iter()
                .filter(|s| s.object)
                .map(|s| s.slug.as_str())
                .collect();
            products.push(
                fetch_flat(&ctx, "list_rows", async |w| {
                    let mut count = 0u64;
                    for (entity, list_id) in &lists {
                        if !queryable.contains(entity.as_str()) {
                            tracing::warn!(list = %list_id, object = %entity,
                                "skipping list rows: object has no records endpoint");
                            continue;
                        }
                        let fetched = ctx
                            .client
                            .fetch_list_rows(entity, list_id, &mut |item| {
                                ctx.write_row(
                                    w,
                                    vec![
                                        ("list_id", list_id.clone().into()),
                                        ("object", entity.clone().into()),
                                        ("record_id", json_id(&item).into()),
                                    ],
                                    item,
                                )
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
        products.push(
            fetch_flat(&ctx, cat.name(), async |w| {
                let stats = ctx
                    .client
                    .fetch_linked(path, &mut |item| {
                        ctx.write_row(w, vec![(id_col, json_id(&item).into())], item)
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
                ctx.write_row(w, vec![], doc)
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
    let mut ensured: HashSet<String> = HashSet::new();
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
            ensured.insert(p.table.clone());
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
            .query(&prev_run_sql(sink.project(), sink.dataset()))
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
    if let Err(e) = write_marker(sink, &spool, &row, &run_id, "runs", 3).await {
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
        views_summary = match views::refresh(
            sink,
            args.views_dataset.clone(),
            &plan,
            &views::schema_defs(&schemas),
        )
        .await
        {
            Ok((dataset, n, errors)) => {
                for e in &errors {
                    tracing::error!(error = %e, "latest view refresh failed (data is safe; \
                        re-run with: clarify-bq views)");
                }
                tracing::info!(dataset = %dataset, views = n, "latest views refreshed");
                serde_json::json!({"dataset": dataset, "created": n, "errors": errors})
            }
            Err(e) => {
                tracing::error!(error = %e, "latest view refresh failed");
                serde_json::json!({"errors": [e]})
            }
        };
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
