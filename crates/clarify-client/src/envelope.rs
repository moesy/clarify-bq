use serde::Deserialize;

/// Envelope for `/objects/{object}/resources` and list-row endpoints:
/// carries `meta.total_records` but no `links`.
#[derive(Debug, Deserialize)]
pub struct ResourcesEnvelope {
    pub data: Vec<serde_json::Value>,
    #[serde(default)]
    pub meta: ResourcesMeta,
}

#[derive(Debug, Default, Deserialize)]
pub struct ResourcesMeta {
    pub total_records: Option<u64>,
}

/// Envelope for lists/users/workflows/activities/schemas: paginated via `links.next`.
#[derive(Debug, Deserialize)]
pub struct LinkedEnvelope {
    pub data: Vec<serde_json::Value>,
    #[serde(default)]
    pub links: Links,
}

#[derive(Debug, Default, Deserialize)]
pub struct Links {
    pub next: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_resources_envelope_without_links() {
        let json = r#"{"data":[{"type":"person","id":"rec_1","attributes":{"name":"Ada Test"},"relationships":{"company":{"data":{"type":"company","id":"rec_9"}}}}],"included":[],"meta":{"total_records":1}}"#;
        let env: ResourcesEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(env.data.len(), 1);
        assert_eq!(env.meta.total_records, Some(1));
        assert_eq!(env.data[0]["id"], "rec_1");
    }

    #[test]
    fn parses_linked_envelope_with_next() {
        let json = r#"{"data":[{"type":"list","id":"lst_1","attributes":{"entity":"person"}}],"links":{"next":"https://api.example/v1/x?page[offset]=50","prev":null},"meta":{"total_records":51,"total_pages":2}}"#;
        let env: LinkedEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(
            env.links.next.as_deref(),
            Some("https://api.example/v1/x?page[offset]=50")
        );
    }

    #[test]
    fn linked_envelope_tolerates_missing_links() {
        let env: LinkedEnvelope = serde_json::from_str(r#"{"data":[]}"#).unwrap();
        assert!(env.links.next.is_none());
        assert!(env.data.is_empty());
    }
}
