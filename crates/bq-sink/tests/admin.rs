use bq_sink::StaticTokenProvider;
use bq_sink::admin::{BqSink, Column, TableSpec};
use std::sync::Arc;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn sink(server: &MockServer) -> BqSink {
    BqSink::new(
        Arc::new(StaticTokenProvider("tok".into())),
        server.uri(),
        "proj".into(),
        "ds".into(),
        "US".into(),
    )
}

fn spec() -> TableSpec {
    TableSpec {
        name: "records_person".into(),
        columns: vec![
            Column { name: "run_id", ty: "STRING" },
            Column { name: "snapshot_at", ty: "TIMESTAMP" },
            Column { name: "record_id", ty: "STRING" },
            Column { name: "object", ty: "STRING" },
            Column { name: "data", ty: "JSON" },
        ],
        partition_expiration_days: Some(400),
    }
}

#[tokio::test]
async fn creates_missing_dataset_with_location() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bigquery/v2/projects/proj/datasets/ds"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bigquery/v2/projects/proj/datasets"))
        .and(body_partial_json(serde_json::json!({
            "datasetReference": {"projectId": "proj", "datasetId": "ds"},
            "location": "US"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    sink(&server).ensure_dataset().await.unwrap();
}

#[tokio::test]
async fn creates_missing_table_with_partitioning_clustering_expiration() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bigquery/v2/projects/proj/datasets/ds/tables/records_person"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bigquery/v2/projects/proj/datasets/ds/tables"))
        .and(body_partial_json(serde_json::json!({
            "schema": {"fields": [
                {"name": "run_id", "type": "STRING"},
                {"name": "snapshot_at", "type": "TIMESTAMP"},
                {"name": "record_id", "type": "STRING"},
                {"name": "object", "type": "STRING"},
                {"name": "data", "type": "JSON"}
            ]},
            "timePartitioning": {"type": "DAY", "field": "snapshot_at", "expirationMs": "34560000000"},
            "clustering": {"fields": ["run_id"]}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    sink(&server).ensure_table(&spec()).await.unwrap();
}

#[tokio::test]
async fn existing_table_gets_expiration_reasserted() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bigquery/v2/projects/proj/datasets/ds/tables/records_person"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/bigquery/v2/projects/proj/datasets/ds/tables/records_person"))
        .and(body_partial_json(serde_json::json!({
            "timePartitioning": {"type": "DAY", "field": "snapshot_at", "expirationMs": "34560000000"}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    sink(&server).ensure_table(&spec()).await.unwrap();
}
