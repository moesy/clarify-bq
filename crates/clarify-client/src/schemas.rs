use crate::envelope::LinkedEnvelope;
use crate::{ClarifyClient, ClientError};

#[derive(Debug, Clone)]
pub struct ObjectSchema {
    pub slug: String,
    pub relationships: Vec<String>,
    pub raw: serde_json::Value,
}

fn extract(item: &serde_json::Value) -> ObjectSchema {
    let attrs = &item["attributes"];
    let slug = attrs["entity"]
        .as_str()
        .or_else(|| attrs["name"].as_str())
        .or_else(|| item["id"].as_str())
        .unwrap_or_default()
        .to_string();
    let mut relationships: Vec<String> = attrs["fields"]
        .as_object()
        .map(|fields| {
            fields
                .iter()
                .filter(|(_, v)| v["type"] == "relationship")
                .map(|(k, _)| k.clone())
                .collect()
        })
        .unwrap_or_default();
    if relationships.is_empty() {
        if let Some(rels) = item["relationships"].as_object() {
            relationships = rels.keys().cloned().collect();
        }
    }
    relationships.sort();
    ObjectSchema { slug, relationships, raw: item.clone() }
}

impl ClarifyClient {
    /// Discover every object schema. Cursor-paginated: `links.next` is followed
    /// to exhaustion so custom objects past page one are never missed.
    pub async fn fetch_schemas(&self) -> Result<Vec<ObjectSchema>, ClientError> {
        let mut out = Vec::new();
        let mut next: Option<String> = Some("/schemas".to_string());
        while let Some(url) = next {
            let body = self.get_json(&url).await?;
            let env: LinkedEnvelope = serde_json::from_value(body).map_err(|e| {
                ClientError::Shape { url: url.clone(), detail: e.to_string() }
            })?;
            out.extend(env.data.iter().map(extract));
            next = env.links.next;
        }
        Ok(out)
    }
}
