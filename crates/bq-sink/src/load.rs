use crate::SinkError;
use crate::admin::BqSink;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Duration;

/// BigQuery rejects local media uploads over 100 MB; stay safely under.
pub const MAX_CHUNK_BYTES: u64 = 90 * 1024 * 1024;

pub fn job_id(run_id: &str, job_key: &str, chunk: usize) -> String {
    format!("clarify_bq_{}_{}_{}", run_id.replace('-', "_"), job_key, chunk)
}

/// Byte ranges on line boundaries, each at most `max_bytes` long.
pub fn split_points(path: &Path, max_bytes: u64) -> std::io::Result<Vec<(u64, u64)>> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut ranges = Vec::new();
    let (mut start, mut pos) = (0u64, 0u64);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line)? as u64;
        if n == 0 {
            if pos > start {
                ranges.push((start, pos));
            }
            return Ok(ranges);
        }
        if pos + n - start > max_bytes && pos > start {
            ranges.push((start, pos));
            start = pos;
        }
        pos += n;
    }
}

impl BqSink {
    pub async fn load_ndjson(
        &self,
        table: &str,
        job_key: &str,
        spool: &Path,
        run_id: &str,
    ) -> Result<u64, SinkError> {
        let mut total_rows = 0u64;
        let ranges = split_points(spool, MAX_CHUNK_BYTES)
            .map_err(|e| SinkError::Config(format!("spool read: {e}")))?;
        for (chunk_idx, (start, end)) in ranges.iter().enumerate() {
            let jid = job_id(run_id, job_key, chunk_idx);
            let mut buf = vec![0u8; (end - start) as usize];
            {
                let mut f = std::fs::File::open(spool)
                    .map_err(|e| SinkError::Config(format!("spool open: {e}")))?;
                f.seek(SeekFrom::Start(*start))
                    .and_then(|_| f.read_exact(&mut buf))
                    .map_err(|e| SinkError::Config(format!("spool chunk read: {e}")))?;
            }
            self.submit_chunk(table, &jid, buf).await?;
            total_rows += self.poll_job(&jid).await?;
            tracing::info!(table, job = %jid, chunk = chunk_idx, "load job committed");
        }
        Ok(total_rows)
    }

    async fn submit_chunk(&self, table: &str, jid: &str, data: Vec<u8>) -> Result<(), SinkError> {
        let config = serde_json::json!({
            "jobReference": {"projectId": self.project, "jobId": jid, "location": self.location},
            "configuration": {"load": {
                "destinationTable": {
                    "projectId": self.project, "datasetId": self.dataset, "tableId": table
                },
                "writeDisposition": "WRITE_APPEND",
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"]
            }}
        });
        let boundary = "clarify_bq_boundary";
        let mut body = Vec::new();
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{config}\r\n\
                 --{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(&data);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let url = format!(
            "{}/upload/bigquery/v2/projects/{}/jobs?uploadType=multipart",
            self.base, self.project
        );
        let token = self.bearer().await?;
        let resp = self
            .http
            .post(&url)
            .bearer_auth(token)
            .header("Content-Type", format!("multipart/related; boundary={boundary}"))
            .body(body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        // 409 = this job ID already exists (an earlier attempt of ours):
        // idempotent by design — fall through to polling the original job.
        if (200..300).contains(&status) || status == 409 {
            return Ok(());
        }
        Err(SinkError::Http {
            status,
            url,
            body: resp.text().await.unwrap_or_default(),
        })
    }

    async fn poll_job(&self, jid: &str) -> Result<u64, SinkError> {
        let url = format!(
            "{}/bigquery/v2/projects/{}/jobs/{}?location={}",
            self.base, self.project, jid, self.location
        );
        loop {
            let token = self.bearer().await?;
            let resp = self.http.get(&url).bearer_auth(token).send().await?;
            let status = resp.status().as_u16();
            let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
            if !(200..300).contains(&status) {
                return Err(SinkError::Http { status, url, body: body.to_string() });
            }
            if body["status"]["state"] == "DONE" {
                if let Some(err) = body["status"].get("errorResult").filter(|e| !e.is_null()) {
                    return Err(SinkError::JobFailed {
                        job_id: jid.to_string(),
                        reason: err["message"].as_str().unwrap_or("unknown").to_string(),
                    });
                }
                return Ok(body["statistics"]["load"]["outputRows"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Run a SQL query; rows come back as arrays of `rows[].f[].v` values.
    pub async fn query(&self, sql: &str) -> Result<Vec<Vec<serde_json::Value>>, SinkError> {
        let url = format!("{}/bigquery/v2/projects/{}/queries", self.base, self.project);
        let body = serde_json::json!({
            "query": sql, "useLegacySql": false, "location": self.location
        });
        let token = self.bearer().await?;
        let resp = self.http.post(&url).bearer_auth(token).json(&body).send().await?;
        let status = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        if !(200..300).contains(&status) {
            return Err(SinkError::Http { status, url, body: body.to_string() });
        }
        let empty = Vec::new();
        Ok(body["rows"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .map(|r| {
                r["f"].as_array()
                    .unwrap_or(&empty)
                    .iter()
                    .map(|c| c["v"].clone())
                    .collect()
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn splits_on_line_boundaries_under_max() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for i in 0..10 {
            writeln!(f, "{{\"n\":{i},\"pad\":\"XXXXXXXXXX\"}}").unwrap();
        }
        let ranges = split_points(f.path(), 60).unwrap();
        assert!(ranges.len() >= 5);
        let total: u64 = ranges.iter().map(|(s, e)| e - s).sum();
        assert_eq!(total, std::fs::metadata(f.path()).unwrap().len());
        for (s, e) in &ranges {
            assert!(e - s <= 60, "chunk {s}-{e} exceeds max");
        }
    }

    #[test]
    fn job_ids_are_deterministic_and_legal() {
        let id = job_id("0198f0aa-1111-7abc-8def-0123456789ab", "records_person", 0);
        assert_eq!(id, "clarify_bq_0198f0aa_1111_7abc_8def_0123456789ab_records_person_0");
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    }
}
