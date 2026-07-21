use crate::envelope::LinkedEnvelope;
use crate::records::{FetchStats, ItemSink};
use crate::{ClarifyClient, ClientError};

impl ClarifyClient {
    /// Page a links-envelope collection (lists, users, workflows, activities)
    /// by following `links.next` until null.
    pub async fn fetch_linked(
        &self,
        path: &str,
        on_item: ItemSink<'_>,
    ) -> Result<FetchStats, ClientError> {
        let mut fetched = 0u64;
        let mut next: Option<String> = Some(path.to_string());
        while let Some(url) = next {
            let body = self.get_json(&url).await?;
            let env: LinkedEnvelope = serde_json::from_value(body).map_err(|e| {
                ClientError::Shape { url: url.clone(), detail: e.to_string() }
            })?;
            for item in &env.data {
                on_item(item).map_err(|e| ClientError::Shape {
                    url: url.clone(),
                    detail: format!("sink error: {e}"),
                })?;
            }
            fetched += env.data.len() as u64;
            next = env.links.next;
        }
        Ok(FetchStats { fetched, expected: None })
    }

    /// Workspace settings: a plain document, not a JSON:API collection.
    pub async fn fetch_settings(&self) -> Result<serde_json::Value, ClientError> {
        self.get_json("/settings").await
    }

    pub async fn fetch_list_rows(
        &self,
        object: &str,
        list_id: &str,
        on_item: ItemSink<'_>,
    ) -> Result<FetchStats, ClientError> {
        self.fetch_resources_at(
            &format!("/objects/{object}/lists/{list_id}/resources"),
            &[],
            on_item,
        )
        .await
    }

    pub async fn fetch_record_activities(
        &self,
        object: &str,
        record_id: &str,
        on_item: ItemSink<'_>,
    ) -> Result<FetchStats, ClientError> {
        self.fetch_linked(&format!("/objects/{object}/records/{record_id}/activities"), on_item)
            .await
    }

    pub async fn fetch_record_attachments(
        &self,
        object: &str,
        record_id: &str,
        on_item: ItemSink<'_>,
    ) -> Result<FetchStats, ClientError> {
        self.fetch_linked(&format!("/objects/{object}/records/{record_id}/attachments"), on_item)
            .await
    }
}
