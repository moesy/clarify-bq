use crate::tables::sanitize;
use bq_sink::BqSink;

/// CTE resolving the newest complete run at query time: the views need no
/// refresh for data freshness, only for column changes. The 45-day window
/// exists so partition pruning bounds every query to recent partitions.
fn latest_cte(project: &str, dataset: &str) -> String {
    format!(
        "WITH latest AS (SELECT run_id, snapshot_at FROM `{project}.{dataset}.runs` \
         WHERE status = 'complete' \
         AND snapshot_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY) \
         ORDER BY snapshot_at DESC LIMIT 1)"
    )
}

fn scalar_type(prop: &serde_json::Value) -> Option<&'static str> {
    let ty = match &prop["type"] {
        serde_json::Value::String(s) => Some(s.as_str()),
        serde_json::Value::Array(a) => a.iter().filter_map(|v| v.as_str()).find(|s| *s != "null"),
        _ => None,
    }?;
    Some(match ty {
        "string" if prop["format"] == "date-time" => "TIMESTAMP",
        "string" => "STRING",
        "integer" => "INT64",
        "number" => "FLOAT64",
        "boolean" => "BOOL",
        _ => return None, // object / array → JSON passthrough
    })
}

/// One SELECT expression per schema property. Scalars become typed columns,
/// many-to-one relationships become id columns, everything nested stays JSON.
fn column_expr(name: &str, prop: &serde_json::Value) -> Option<String> {
    if name.contains('"') || name.contains('\\') {
        return None;
    }
    let alias = sanitize(name);
    if ["run_id", "snapshot_at", "record_id", "object", "data"].contains(&alias.as_str())
        || alias.is_empty()
    {
        return None;
    }
    let attr_path = format!("$.attributes.\"{name}\"");
    if prop.get("xClarifyRelationship").is_some() {
        return Some(if scalar_type(prop) == Some("STRING") {
            // many-to-one: a plain id column, from attributes or the
            // relationships object (whichever the API populated).
            format!(
                "COALESCE(JSON_VALUE(data, '{attr_path}'), \
                 JSON_VALUE(data, '$.relationships.\"{name}\".data.id')) AS {alias}"
            )
        } else {
            // collection relationship: keep the {type,id} refs as JSON.
            format!("JSON_QUERY(data, '$.relationships.\"{name}\"') AS {alias}")
        });
    }
    Some(match scalar_type(prop) {
        Some("STRING") => format!("JSON_VALUE(data, '{attr_path}') AS {alias}"),
        Some(cast) => {
            format!("SAFE_CAST(JSON_VALUE(data, '{attr_path}') AS {cast}) AS {alias}")
        }
        None => format!("JSON_QUERY(data, '{attr_path}') AS {alias}"),
    })
}

/// Flat latest-snapshot view for one object, columns generated from its schema.
pub fn object_view_sql(
    project: &str,
    dataset: &str,
    views_dataset: &str,
    table: &str,
    slug: &str,
    schema_raw: &serde_json::Value,
) -> String {
    let mut cols = vec!["record_id".to_string(), "snapshot_at".to_string()];
    let mut seen: Vec<String> = Vec::new();
    if let Some(props) = schema_raw["attributes"]["properties"].as_object() {
        for (name, prop) in props {
            let alias = sanitize(name);
            if seen.contains(&alias) {
                continue;
            }
            if let Some(expr) = column_expr(name, prop) {
                seen.push(alias);
                cols.push(expr);
            }
        }
    }
    cols.push("data".to_string());
    format!(
        "CREATE OR REPLACE VIEW `{project}.{views_dataset}.{view}` AS {cte} \
         SELECT {cols} FROM `{project}.{dataset}.{table}` t, latest \
         WHERE t.run_id = latest.run_id \
         AND t.snapshot_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)",
        view = sanitize(slug),
        cte = latest_cte(project, dataset),
        cols = cols.join(", "),
    )
}

/// Latest-snapshot pass-through view for an aux table (users, lists, ...).
pub fn passthrough_view_sql(
    project: &str,
    dataset: &str,
    views_dataset: &str,
    table: &str,
) -> String {
    format!(
        "CREATE OR REPLACE VIEW `{project}.{views_dataset}.{table}` AS {cte} \
         SELECT t.* EXCEPT (run_id) FROM `{project}.{dataset}.{table}` t, latest \
         WHERE t.run_id = latest.run_id \
         AND t.snapshot_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)",
        cte = latest_cte(project, dataset),
    )
}

/// Create/refresh latest views. `objects` = (slug, records table, schema raw);
/// `aux_tables` = the aux tables that exist. Views are independent: one
/// failing does not block the rest. Returns (views written, errors).
pub async fn create_latest_views(
    sink: &BqSink,
    views_dataset: &str,
    objects: &[(String, String, serde_json::Value)],
    aux_tables: &[&str],
) -> (u64, Vec<String>) {
    if let Err(e) = sink.ensure_dataset_named(views_dataset).await {
        return (0, vec![format!("ensure views dataset: {e}")]);
    }
    let mut n = 0u64;
    let mut errors = Vec::new();
    for (slug, table, raw) in objects {
        let sql = object_view_sql(
            sink.project(),
            sink.dataset(),
            views_dataset,
            table,
            slug,
            raw,
        );
        match sink.query(&sql).await {
            Ok(_) => n += 1,
            Err(e) => errors.push(format!("view for {slug}: {e}")),
        }
    }
    for table in aux_tables {
        let sql = passthrough_view_sql(sink.project(), sink.dataset(), views_dataset, table);
        match sink.query(&sql).await {
            Ok(_) => n += 1,
            Err(e) => errors.push(format!("view for {table}: {e}")),
        }
    }
    (n, errors)
}

pub const AUX_TABLES: [&str; 9] = [
    "schemas",
    "lists",
    "list_rows",
    "users",
    "workflows",
    "settings",
    "activities",
    "attachments",
    "runs",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn person_schema() -> serde_json::Value {
        serde_json::json!({
            "id": "https://example.test/schemas/entities/person",
            "attributes": {
                "title": "person",
                "xClarifyNamespace": "objects",
                "properties": {
                    "_id": {"type": "string"},
                    "_created_at": {"type": "string", "format": "date-time"},
                    "score": {"type": ["number", "null"]},
                    "active": {"type": "boolean"},
                    "name": {"oneOf": [{"type": "null"}, {"$ref": "https://x/personName"}]},
                    "company_id": {"type": ["string", "null"],
                        "xClarifyRelationship": {"kind": "many-to-one", "entity": "company"}},
                    "deals": {"oneOf": [{"type": "null"}],
                        "xClarifyRelationship": {"kind": "many-to-many", "entity": "deal"}}
                }
            }
        })
    }

    #[test]
    fn object_view_flattens_types_relationships_and_json() {
        let sql = object_view_sql(
            "p",
            "d",
            "d_latest",
            "records_person",
            "person",
            &person_schema(),
        );
        assert!(sql.starts_with("CREATE OR REPLACE VIEW `p.d_latest.person` AS"));
        assert!(sql.contains(r#"JSON_VALUE(data, '$.attributes."_id"') AS _id"#));
        assert!(sql.contains(r#"SAFE_CAST(JSON_VALUE(data, '$.attributes."_created_at"') AS TIMESTAMP) AS _created_at"#));
        assert!(sql.contains(
            r#"SAFE_CAST(JSON_VALUE(data, '$.attributes."score"') AS FLOAT64) AS score"#
        ));
        assert!(
            sql.contains(
                r#"SAFE_CAST(JSON_VALUE(data, '$.attributes."active"') AS BOOL) AS active"#
            )
        );
        // nested object stays JSON
        assert!(sql.contains(r#"JSON_QUERY(data, '$.attributes."name"') AS name"#));
        // many-to-one → id column; collection → JSON refs
        assert!(sql.contains(r#"$.relationships."company_id".data.id')) AS company_id"#));
        assert!(sql.contains(r#"JSON_QUERY(data, '$.relationships."deals"') AS deals"#));
        // latest-run filter is dynamic and partition-bounded
        assert!(sql.contains("status = 'complete'"));
        assert!(sql.contains("ORDER BY snapshot_at DESC LIMIT 1"));
        assert!(sql.contains("t.snapshot_at >= TIMESTAMP_SUB"));
        // full history payload still available
        assert!(
            sql.trim_end().ends_with(
                "AND t.snapshot_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)"
            )
        );
    }

    #[test]
    fn passthrough_view_keeps_columns_drops_run_id() {
        let sql = passthrough_view_sql("p", "d", "d_latest", "users");
        assert!(sql.starts_with("CREATE OR REPLACE VIEW `p.d_latest.users` AS"));
        assert!(sql.contains("SELECT t.* EXCEPT (run_id)"));
    }
}
