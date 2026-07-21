use std::collections::HashMap;

pub fn now_rfc3339() -> String {
    humantime::format_rfc3339_seconds(std::time::SystemTime::now()).to_string()
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ResourceOutcome {
    pub resource: String,
    pub table: String,
    pub status: String, // ok | failed | skipped
    pub count: u64,
    pub expected: Option<u64>,
    pub consistency: String, // clean | dirty | n/a
    pub error: Option<String>,
    pub fetch_started_at: String,
    pub fetch_ended_at: String,
}

impl ResourceOutcome {
    pub fn is_records(&self) -> bool {
        self.table.starts_with("records_")
    }
}

pub fn overall_status(outcomes: &[ResourceOutcome]) -> &'static str {
    let records_failed = outcomes
        .iter()
        .any(|o| o.is_records() && o.status == "failed");
    if records_failed {
        "failed"
    } else if outcomes.iter().any(|o| o.status == "failed") {
        "partial"
    } else {
        "complete"
    }
}

/// Compare this run's per-resource counts against the previous complete run.
/// Only resources present in both runs are compared — scope changes (--skip,
/// --objects) must not read as shrinkage.
pub fn shrink_violations(
    prev: &HashMap<String, u64>,
    curr: &HashMap<String, u64>,
    threshold_pct: f64,
) -> Vec<String> {
    let mut out = Vec::new();
    for (resource, &prev_n) in prev {
        if prev_n == 0 {
            continue;
        }
        if let Some(&curr_n) = curr.get(resource) {
            let drop_pct = 100.0 * (prev_n.saturating_sub(curr_n)) as f64 / prev_n as f64;
            if drop_pct > threshold_pct {
                out.push(format!(
                    "{resource}: {prev_n} -> {curr_n} ({drop_pct:.1}% drop exceeds {threshold_pct}%)"
                ));
            }
        }
    }
    out.sort();
    out
}

pub fn runs_row(
    run_id: &str,
    snapshot_at: &str,
    finished_at: &str,
    status: &str,
    outcomes: &[ResourceOutcome],
) -> serde_json::Value {
    serde_json::json!({
        "run_id": run_id,
        "snapshot_at": snapshot_at,
        "finished_at": finished_at,
        "status": status,
        "resources": outcomes,
    })
}

/// SQL for the previous complete run's resources JSON (newest by snapshot_at).
pub fn prev_run_sql(project: &str, dataset: &str) -> String {
    format!(
        "SELECT TO_JSON_STRING(resources) FROM `{project}.{dataset}.runs` \
         WHERE status = 'complete' ORDER BY snapshot_at DESC LIMIT 1"
    )
}

/// Parse the resources JSON from `prev_run_sql` into per-resource ok-counts.
pub fn parse_prev_counts(resources_json: &str) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    if let Ok(serde_json::Value::Array(items)) = serde_json::from_str(resources_json) {
        for item in items {
            if item["status"] == "ok"
                && let (Some(resource), Some(count)) =
                    (item["resource"].as_str(), item["count"].as_u64())
            {
                out.insert(resource.to_string(), count);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(resource: &str, table: &str, status: &str, count: u64) -> ResourceOutcome {
        ResourceOutcome {
            resource: resource.into(),
            table: table.into(),
            status: status.into(),
            count,
            expected: None,
            consistency: "clean".into(),
            error: None,
            fetch_started_at: "2026-07-20T00:00:00Z".into(),
            fetch_ended_at: "2026-07-20T00:00:01Z".into(),
        }
    }

    #[test]
    fn status_matrix() {
        let ok = outcome("records_person", "records_person", "ok", 5);
        let aux_fail = outcome("users", "users", "failed", 0);
        let rec_fail = outcome("records_person", "records_person", "failed", 0);
        assert_eq!(overall_status(std::slice::from_ref(&ok)), "complete");
        assert_eq!(overall_status(&[ok.clone(), aux_fail.clone()]), "partial");
        assert_eq!(overall_status(&[rec_fail, aux_fail]), "failed");
    }

    #[test]
    fn shrink_flags_only_real_drops() {
        let prev = HashMap::from([
            ("records_person".to_string(), 100u64),
            ("users".to_string(), 10),
            ("records_gone".to_string(), 50),
        ]);
        let curr = HashMap::from([
            ("records_person".to_string(), 90u64), // 10% drop > 5% → flag
            ("users".to_string(), 12),             // growth → fine
                                                   // records_gone absent → scope change, not compared
        ]);
        let v = shrink_violations(&prev, &curr, 5.0);
        assert_eq!(v.len(), 1);
        assert!(v[0].contains("records_person"));
        assert!(shrink_violations(&prev, &curr, 15.0).is_empty());
    }

    #[test]
    fn prev_counts_parse_ignores_failed_resources() {
        let json = r#"[
            {"resource":"records_person","status":"ok","count":100},
            {"resource":"users","status":"failed","count":0}
        ]"#;
        let counts = parse_prev_counts(json);
        assert_eq!(counts.get("records_person"), Some(&100));
        assert!(!counts.contains_key("users"));
    }
}
