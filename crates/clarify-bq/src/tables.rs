use bq_sink::{Column, TableSpec};
use clarify_client::ObjectSchema;
use std::collections::HashMap;

pub fn sanitize(slug: &str) -> String {
    slug.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Map object slugs to their `records_*` table names, failing fast when two
/// slugs collide after sanitization (silently commingled data otherwise).
pub fn records_table_names(schemas: &[ObjectSchema]) -> Result<Vec<(String, String)>, String> {
    let mut seen: HashMap<String, String> = HashMap::new();
    let mut out = Vec::new();
    for s in schemas {
        let table = format!("records_{}", sanitize(&s.slug));
        if let Some(prev) = seen.insert(table.clone(), s.slug.clone()) {
            return Err(format!(
                "objects {prev:?} and {:?} both map to table {table:?} after sanitization; \
                 rename one in Clarify or back them up separately",
                s.slug
            ));
        }
        out.push((s.slug.clone(), table));
    }
    Ok(out)
}

const BASE: [Column; 2] = [
    Column {
        name: "run_id",
        ty: "STRING",
    },
    Column {
        name: "snapshot_at",
        ty: "TIMESTAMP",
    },
];

pub fn spec_for(table: &str, expiration_days: Option<u32>) -> TableSpec {
    let mut cols: Vec<Column> = BASE.to_vec();
    let extra: &[Column] = match table {
        t if t.starts_with("records_") => &[
            Column {
                name: "record_id",
                ty: "STRING",
            },
            Column {
                name: "object",
                ty: "STRING",
            },
        ],
        "schemas" => &[Column {
            name: "object",
            ty: "STRING",
        }],
        "lists" => &[
            Column {
                name: "list_id",
                ty: "STRING",
            },
            Column {
                name: "object",
                ty: "STRING",
            },
        ],
        "list_rows" => &[
            Column {
                name: "list_id",
                ty: "STRING",
            },
            Column {
                name: "object",
                ty: "STRING",
            },
            Column {
                name: "record_id",
                ty: "STRING",
            },
        ],
        "users" | "workflows" => &[Column {
            name: "id",
            ty: "STRING",
        }],
        "settings" => &[],
        "activities" => &[
            Column {
                name: "object",
                ty: "STRING",
            },
            Column {
                name: "record_id",
                ty: "STRING",
            },
            Column {
                name: "activity_id",
                ty: "STRING",
            },
        ],
        "attachments" => &[
            Column {
                name: "object",
                ty: "STRING",
            },
            Column {
                name: "record_id",
                ty: "STRING",
            },
            Column {
                name: "attachment_id",
                ty: "STRING",
            },
        ],
        "runs" => &[
            Column {
                name: "finished_at",
                ty: "TIMESTAMP",
            },
            Column {
                name: "status",
                ty: "STRING",
            },
            Column {
                name: "resources",
                ty: "JSON",
            },
        ],
        other => unreachable!("unknown table shape: {other}"),
    };
    cols.extend_from_slice(extra);
    if table != "runs" {
        cols.push(Column {
            name: "data",
            ty: "JSON",
        });
    }
    TableSpec {
        name: table.to_string(),
        columns: cols,
        // The runs ledger is tiny and is the completeness record: never expire it.
        partition_expiration_days: if table == "runs" {
            None
        } else {
            expiration_days
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(slug: &str) -> ObjectSchema {
        ObjectSchema {
            slug: slug.into(),
            relationships: vec![],
            object: true,
            raw: serde_json::json!({}),
        }
    }

    #[test]
    fn sanitizes_to_lowercase_underscore() {
        assert_eq!(sanitize("Sales-Lead"), "sales_lead");
        assert_eq!(sanitize("c_order"), "c_order");
    }

    #[test]
    fn collision_after_sanitization_fails_naming_both() {
        let err = records_table_names(&[schema("sales-lead"), schema("sales_lead")]).unwrap_err();
        assert!(err.contains("sales-lead") && err.contains("sales_lead"));
    }

    #[test]
    fn runs_table_never_expires() {
        assert!(
            spec_for("runs", Some(400))
                .partition_expiration_days
                .is_none()
        );
        assert_eq!(
            spec_for("records_person", Some(400)).partition_expiration_days,
            Some(400)
        );
    }
}
