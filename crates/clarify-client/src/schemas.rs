use crate::envelope::LinkedEnvelope;
use crate::{ClarifyClient, ClientError};

#[derive(Debug, Clone)]
pub struct ObjectSchema {
    /// Object slug used in `/objects/{slug}/...` paths (from the schema `title`).
    pub slug: String,
    /// Property names carrying `xClarifyRelationship` — the exact names the
    /// `include=` parameter accepts (invalid names are a 400).
    pub relationships: Vec<String>,
    /// True when this schema describes a record object
    /// (`xClarifyNamespace == "objects"`); value/type schemas are false.
    pub object: bool,
    pub raw: serde_json::Value,
}

fn extract(item: &serde_json::Value) -> ObjectSchema {
    // Schema items are JSON Schema documents: the slug is `title`, objectness
    // is `xClarifyNamespace`, and fields live under `properties`.
    let attrs = &item["attributes"];
    let object = attrs["xClarifyNamespace"] == "objects";
    let slug = attrs["title"]
        .as_str()
        .filter(|s| !s.is_empty())
        .or_else(|| item["id"].as_str())
        .unwrap_or_default()
        .to_string();
    let mut relationships: Vec<String> = attrs["properties"]
        .as_object()
        .map(|props| {
            props
                .iter()
                .filter(|(_, v)| v.get("xClarifyRelationship").is_some())
                .map(|(k, _)| k.clone())
                .collect()
        })
        .unwrap_or_default();
    relationships.sort();
    ObjectSchema {
        slug,
        relationships,
        object,
        raw: item.clone(),
    }
}

impl ClarifyClient {
    /// Discover every schema (objects and value types). Cursor-paginated:
    /// `links.next` is followed to exhaustion so custom objects past page one
    /// are never missed. The same object can appear under several schema URLs
    /// (core/ and entities/) — callers dedup by slug when planning.
    pub async fn fetch_schemas(&self) -> Result<Vec<ObjectSchema>, ClientError> {
        let mut out = Vec::new();
        let mut next: Option<String> = Some("/schemas".to_string());
        while let Some(url) = next {
            let body = self.get_json(&url).await?;
            let env: LinkedEnvelope =
                serde_json::from_value(body).map_err(|e| ClientError::Shape {
                    url: url.clone(),
                    detail: e.to_string(),
                })?;
            out.extend(env.data.iter().map(extract));
            next = env.links.next;
        }
        Ok(out)
    }
}
