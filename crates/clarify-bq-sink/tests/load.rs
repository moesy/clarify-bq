use clarify_bq_sink::StaticTokenProvider;
use clarify_bq_sink::admin::BqSink;
use std::io::Write;
use std::sync::Arc;
use wiremock::matchers::{method, path, query_param};
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

fn spool(lines: usize) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    for i in 0..lines {
        writeln!(
            f,
            "{{\"run_id\":\"r\",\"record_id\":\"rec_{i}\",\"data\":{{}}}}"
        )
        .unwrap();
    }
    f
}

#[tokio::test]
async fn submits_load_job_and_polls_to_done() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/upload/bigquery/v2/projects/proj/jobs"))
        .and(query_param("uploadType", "multipart"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jobReference": {"jobId": "whatever"}, "status": {"state": "RUNNING"}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/bigquery/v2/projects/proj/jobs/clarify_bq_run1_records_person_0",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": {"state": "DONE"},
            "statistics": {"load": {"outputRows": "3"}}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let f = spool(3);
    let rows = sink(&server)
        .load_ndjson("records_person", "records_person", f.path(), "run1")
        .await
        .unwrap();
    assert_eq!(rows, 3);
}

#[tokio::test]
async fn duplicate_job_id_is_idempotent_via_poll() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/upload/bigquery/v2/projects/proj/jobs"))
        .respond_with(ResponseTemplate::new(409))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/bigquery/v2/projects/proj/jobs/clarify_bq_run1_records_person_0",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": {"state": "DONE"},
            "statistics": {"load": {"outputRows": "3"}}
        })))
        .mount(&server)
        .await;
    let f = spool(3);
    let rows = sink(&server)
        .load_ndjson("records_person", "records_person", f.path(), "run1")
        .await
        .unwrap();
    assert_eq!(rows, 3);
}

#[tokio::test]
async fn failed_job_surfaces_error_result() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/upload/bigquery/v2/projects/proj/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": {"state": "RUNNING"}
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/bigquery/v2/projects/proj/jobs/clarify_bq_run1_records_person_0",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": {"state": "DONE", "errorResult": {"message": "synthetic parse error"}}
        })))
        .mount(&server)
        .await;
    let f = spool(1);
    let err = sink(&server)
        .load_ndjson("records_person", "records_person", f.path(), "run1")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("synthetic parse error"));
}
