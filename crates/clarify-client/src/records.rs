use crate::envelope::ResourcesEnvelope;
use crate::{ClarifyClient, ClientError};

pub const PAGE_LIMIT: usize = 500;

#[derive(Debug, Clone, Copy)]
pub struct FetchStats {
    pub fetched: u64,
    pub expected: Option<u64>,
}

impl FetchStats {
    pub fn consistency(&self) -> &'static str {
        match self.expected {
            Some(e) if e != self.fetched => "dirty",
            _ => "clean",
        }
    }
}

/// Receives each fetched item, owned — no copies are left behind in the client.
pub type ItemSink<'a> = &'a mut (dyn FnMut(serde_json::Value) -> std::io::Result<()> + Send);

impl ClarifyClient {
    pub async fn fetch_records(
        &self,
        object: &str,
        include: &[String],
        on_item: ItemSink<'_>,
    ) -> Result<FetchStats, ClientError> {
        self.fetch_resources_at(&format!("/objects/{object}/resources"), include, on_item)
            .await
    }

    pub async fn fetch_resources_at(
        &self,
        path: &str,
        include: &[String],
        on_item: ItemSink<'_>,
    ) -> Result<FetchStats, ClientError> {
        let mut offset: u64 = 0;
        let mut fetched: u64 = 0;
        let mut expected: Option<u64> = None;
        let include_q = if include.is_empty() {
            String::new()
        } else {
            format!("&include={}", include.join(","))
        };
        loop {
            let q = format!(
                "{path}?page[limit]={PAGE_LIMIT}&page[offset]={offset}\
                 &sortOrder[column]=_created_at&sortOrder[dir]=ASC{include_q}"
            );
            let env: ResourcesEnvelope = self.get_parsed(&q).await?;
            expected = env.meta.total_records.or(expected);
            let n = env.data.len();
            for item in env.data {
                on_item(item)?;
            }
            fetched += n as u64;
            // Advance by the returned count, never by the requested limit: the
            // server may clamp the page size (spec: silent half-page skips otherwise).
            offset += n as u64;
            // A short page only ends the scan once we've seen everything the
            // server claims exists — a clamped page size also produces short
            // pages, and stopping there would silently drop the tail.
            if n == 0 || (n < PAGE_LIMIT && expected.is_none_or(|e| fetched >= e)) {
                break;
            }
        }
        Ok(FetchStats { fetched, expected })
    }
}
