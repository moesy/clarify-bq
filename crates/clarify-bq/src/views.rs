use crate::tables::sanitize;
use bq_sink::BqSink;
use clarify_client::ObjectSchema;
use std::collections::HashMap;

/// `$id` → schema attributes, for resolving `$ref`s (e.g. person.name →
/// core/personName). Built from ALL discovered schemas, value types included.
pub type SchemaDefs = HashMap<String, serde_json::Value>;

pub fn schema_defs(schemas: &[ObjectSchema]) -> SchemaDefs {
    schemas
        .iter()
        .filter_map(|s| {
            let id = s.raw["id"].as_str()?;
            Some((id.to_string(), s.raw["attributes"].clone()))
        })
        .collect()
}

/// Peel `oneOf [null, X]` wrappers and resolve `$ref`s (bounded depth).
fn effective<'a>(prop: &'a serde_json::Value, defs: &'a SchemaDefs) -> &'a serde_json::Value {
    let mut cur = prop;
    for _ in 0..3 {
        if let Some(variants) = cur["oneOf"].as_array()
            && let Some(v) = variants.iter().find(|v| v["type"] != "null")
        {
            cur = v;
            continue;
        }
        if let Some(r) = cur["$ref"].as_str()
            && let Some(def) = defs.get(r)
        {
            cur = def;
            continue;
        }
        break;
    }
    cur
}

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

fn is_datetime(prop: &serde_json::Value) -> bool {
    // The format is authoritative even when the declared type is loose
    // (seen live: _created_at is type "object", format "date-time").
    prop["format"] == "date-time"
}

fn scalar_expr(
    path: &str,
    alias: &str,
    prop: &serde_json::Value,
    eff: &serde_json::Value,
) -> Option<String> {
    if is_datetime(prop) || is_datetime(eff) {
        return Some(format!(
            "SAFE_CAST(JSON_VALUE(t.data, '{path}') AS TIMESTAMP) AS `{alias}`"
        ));
    }
    Some(match scalar_type(eff)? {
        "STRING" => format!("JSON_VALUE(t.data, '{path}') AS `{alias}`"),
        cast => format!("SAFE_CAST(JSON_VALUE(t.data, '{path}') AS {cast}) AS `{alias}`"),
    })
}

/// SELECT expressions for one schema property. Scalars become typed columns,
/// many-to-one relationships become id columns, `$ref`ed structs of scalars
/// (personName, ...) expand one level, real collections and the rest stay JSON.
fn column_exprs(name: &str, prop: &serde_json::Value, defs: &SchemaDefs) -> Vec<String> {
    if name.contains('"') || name.contains('\\') {
        return vec![];
    }
    let alias = sanitize(name);
    if ["run_id", "snapshot_at", "record_id", "object", "data"].contains(&alias.as_str())
        || alias.is_empty()
    {
        return vec![];
    }
    let attr_path = format!("$.attributes.\"{name}\"");
    if prop.get("xClarifyRelationship").is_some() {
        return vec![if scalar_type(prop) == Some("STRING") {
            // many-to-one: a plain id column, from attributes or the
            // relationships object (whichever the API populated).
            format!(
                "COALESCE(JSON_VALUE(t.data, '{attr_path}'), \
                 JSON_VALUE(t.data, '$.relationships.\"{name}\".data.id')) AS `{alias}`"
            )
        } else {
            // collection relationship: keep the {type,id} refs as JSON.
            format!("JSON_QUERY(t.data, '$.relationships.\"{name}\"') AS `{alias}`")
        }];
    }
    let eff = effective(prop, defs);
    if let Some(expr) = scalar_expr(&attr_path, &alias, prop, eff) {
        return vec![expr];
    }
    // A struct of scalars (not a collection wrapper) expands one level.
    if let Some(children) = eff["properties"].as_object()
        && !children.contains_key("items")
    {
        let expanded: Vec<String> = children
            .iter()
            .filter(|(c, _)| !c.contains('"') && !c.contains('\\'))
            .filter_map(|(c, cprop)| {
                let ceff = effective(cprop, defs);
                scalar_expr(
                    &format!("$.attributes.\"{name}\".\"{c}\""),
                    &format!("{alias}_{}", sanitize(c)),
                    cprop,
                    ceff,
                )
            })
            .collect();
        if !expanded.is_empty() {
            return expanded;
        }
    }
    vec![format!("JSON_QUERY(t.data, '{attr_path}') AS `{alias}`")]
}

/// Flat latest-snapshot view for one object, columns generated from its schema.
pub fn object_view_sql(
    project: &str,
    dataset: &str,
    views_dataset: &str,
    table: &str,
    slug: &str,
    schema_raw: &serde_json::Value,
    defs: &SchemaDefs,
) -> String {
    let mut cols = vec!["t.record_id".to_string(), "t.snapshot_at".to_string()];
    let mut seen: Vec<String> = Vec::new();
    if let Some(props) = schema_raw["attributes"]["properties"].as_object() {
        for (name, prop) in props {
            let alias = sanitize(name);
            if seen.contains(&alias) {
                continue;
            }
            let exprs = column_exprs(name, prop, defs);
            if !exprs.is_empty() {
                seen.push(alias);
                cols.extend(exprs);
            }
        }
    }
    cols.push("t.data".to_string());
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
    defs: &SchemaDefs,
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
            defs,
        );
        match sink.query(&sql).await {
            Ok(_) => n += 1,
            // Backing table absent (skipped object, never-run category): not
            // an error, there is simply nothing to view yet.
            Err(bq_sink::SinkError::Http { status: 404, .. }) => {
                tracing::info!(view = %slug, "skipping view: backing table does not exist");
            }
            Err(e) => errors.push(format!("view for {slug}: {e}")),
        }
    }
    for table in aux_tables {
        let sql = passthrough_view_sql(sink.project(), sink.dataset(), views_dataset, table);
        match sink.query(&sql).await {
            Ok(_) => n += 1,
            Err(bq_sink::SinkError::Http { status: 404, .. }) => {
                tracing::info!(view = %table, "skipping view: backing table does not exist");
            }
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

    fn defs() -> SchemaDefs {
        HashMap::from([(
            "https://x/schemas/core/personName".to_string(),
            serde_json::json!({
                "title": "personName",
                "properties": {
                    "first_name": {"type": ["string", "null"]},
                    "full_name": {"type": ["string", "null"]}
                }
            }),
        )])
    }

    fn person_schema() -> serde_json::Value {
        serde_json::json!({
            "id": "https://example.test/schemas/entities/person",
            "attributes": {
                "title": "person",
                "xClarifyNamespace": "objects",
                "properties": {
                    "_id": {"type": "string"},
                    // declared object, but format wins: TIMESTAMP
                    "_created_at": {"type": "object", "format": "date-time"},
                    "score": {"type": ["number", "null"]},
                    // reserved SQL keyword as a field name
                    "end": {"type": ["string", "null"]},
                    // $ref struct of scalars: expands one level
                    "name": {"oneOf": [{"type": "null"}, {"$ref": "https://x/schemas/core/personName"}]},
                    // unresolvable $ref stays JSON
                    "labels": {"oneOf": [{"type": "null"}, {"$ref": "https://x/unknown"}]},
                    "company_id": {"type": ["string", "null"],
                        "xClarifyRelationship": {"kind": "many-to-one", "entity": "company"}},
                    "deals": {"oneOf": [{"type": "null"}],
                        "xClarifyRelationship": {"kind": "many-to-many", "entity": "deal"}}
                }
            }
        })
    }

    #[test]
    fn object_view_flattens_expands_and_escapes() {
        let sql = object_view_sql(
            "p",
            "d",
            "d_latest",
            "records_person",
            "person",
            &person_schema(),
            &defs(),
        );
        assert!(sql.starts_with("CREATE OR REPLACE VIEW `p.d_latest.person` AS"));
        // qualified base columns: no ambiguity against the latest CTE
        assert!(sql.contains("SELECT t.record_id, t.snapshot_at,"));
        assert!(
            sql.trim_end().ends_with(
                "AND t.snapshot_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)"
            )
        );
        // every alias backticked; reserved keyword safe
        assert!(sql.contains(r#"JSON_VALUE(t.data, '$.attributes."end"') AS `end`"#));
        // format wins over declared type
        assert!(sql.contains(
            r#"SAFE_CAST(JSON_VALUE(t.data, '$.attributes."_created_at"') AS TIMESTAMP) AS `_created_at`"#
        ));
        assert!(sql.contains(
            r#"SAFE_CAST(JSON_VALUE(t.data, '$.attributes."score"') AS FLOAT64) AS `score`"#
        ));
        // $ref struct expanded one level
        assert!(sql.contains(
            r#"JSON_VALUE(t.data, '$.attributes."name"."full_name"') AS `name_full_name`"#
        ));
        assert!(sql.contains(
            r#"JSON_VALUE(t.data, '$.attributes."name"."first_name"') AS `name_first_name`"#
        ));
        // unresolvable ref → JSON passthrough
        assert!(sql.contains(r#"JSON_QUERY(t.data, '$.attributes."labels"') AS `labels`"#));
        // relationships
        assert!(sql.contains(r#"$.relationships."company_id".data.id')) AS `company_id`"#));
        assert!(sql.contains(r#"JSON_QUERY(t.data, '$.relationships."deals"') AS `deals`"#));
        // dynamic latest filter
        assert!(sql.contains("status = 'complete'"));
        assert!(sql.contains("ORDER BY snapshot_at DESC LIMIT 1"));
    }

    #[test]
    fn passthrough_view_keeps_columns_drops_run_id() {
        let sql = passthrough_view_sql("p", "d", "d_latest", "users");
        assert!(sql.starts_with("CREATE OR REPLACE VIEW `p.d_latest.users` AS"));
        assert!(sql.contains("SELECT t.* EXCEPT (run_id)"));
    }
}
