use bq_sink::{BqSink, StaticTokenProvider};
use clarify_bq::cli::{ConnArgs, ExitCode};
use clarify_bq::commands::{run_check, run_mark_complete, run_objects};
use clarify_bq::config::Config;
use clarify_client::ClarifyClient;
use std::sync::Arc;
use wiremock::matchers::{method, path, path_regex};
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

async fn mock_schemas(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [
                {"type": "schema", "id": "https://example.test/schemas/core/person",
                 "attributes": {"title": "person", "xClarifyNamespace": "objects",
                    "properties": {"company_id": {"xClarifyRelationship": {"entity": "company"}}}}},
                {"type": "schema", "id": "https://example.test/schemas/entities/person",
                 "attributes": {"title": "person", "xClarifyNamespace": "objects",
                    "properties": {"company_id": {"xClarifyRelationship": {"entity": "company"}}}}},
                {"type": "schema", "id": "https://example.test/schemas/entities/deal",
                 "attributes": {"title": "deal", "xClarifyNamespace": "objects", "properties": {}}}
            ],
            "links": {"next": null}
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn objects_lists_discovered_slugs() {
    let server = MockServer::start().await;
    mock_schemas(&server).await;
    let client = ClarifyClient::new(server.uri(), "sk".into(), "acme".into()).unwrap();
    let (exit, out) = run_objects(&client).await;
    assert_eq!(exit, ExitCode::Complete);
    assert!(out.contains("person\tcompany_id"));
    assert!(out.contains("deal"));
    assert_eq!(
        out.matches("person").count(),
        1,
        "core/entities duplicates collapsed"
    );
}

#[tokio::test]
async fn check_reports_probes_and_fails_on_denied_dataset() {
    let clarify = MockServer::start().await;
    let gcp = MockServer::start().await;
    mock_schemas(&clarify).await;
    // Dataset query denied.
    Mock::given(method("POST"))
        .and(path("/bigquery/v2/projects/proj/queries"))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error": {"message": "denied"}
        })))
        .mount(&gcp)
        .await;
    // Table create probe succeeds.
    Mock::given(method("GET"))
        .and(path_regex(
            r"^/bigquery/v2/projects/proj/datasets/ds/tables/[^/]+$",
        ))
        .respond_with(ResponseTemplate::new(404))
        .mount(&gcp)
        .await;
    Mock::given(method("POST"))
        .and(path("/bigquery/v2/projects/proj/datasets/ds/tables"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&gcp)
        .await;
    Mock::given(method("DELETE"))
        .and(path_regex(
            r"^/bigquery/v2/projects/proj/datasets/ds/tables/[^/]+$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&gcp)
        .await;

    let conn = ConnArgs {
        workspace: "acme".into(),
        project: "proj".into(),
        secret: None,
        dataset: "ds".into(),
        location: "US".into(),
    };
    let cfg = Config::resolve(&conn, Some("sk_env".into())).unwrap();
    let s = sink(&gcp);
    let provider = StaticTokenProvider("tok".into());
    let (exit, report) = run_check(&cfg, &provider, &gcp.uri(), &clarify.uri(), &s).await;
    assert_eq!(exit, ExitCode::ConfigAuth, "report:\n{report}");
    assert!(report.contains("ok    secret: skipped"));
    assert!(report.contains("ok    clarify: 2 record objects"));
    assert!(report.contains("FAIL  dataset"));
    assert!(report.contains("ok    tables"));
}

#[tokio::test]
async fn check_treats_missing_dataset_as_informational() {
    let clarify = MockServer::start().await;
    let gcp = MockServer::start().await;
    mock_schemas(&clarify).await;
    Mock::given(method("POST"))
        .and(path("/bigquery/v2/projects/proj/queries"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": {"message": "Not found: Dataset proj:ds"}
        })))
        .mount(&gcp)
        .await;

    let conn = ConnArgs {
        workspace: "acme".into(),
        project: "proj".into(),
        secret: None,
        dataset: "ds".into(),
        location: "US".into(),
    };
    let cfg = Config::resolve(&conn, Some("sk_env".into())).unwrap();
    let s = sink(&gcp);
    let provider = StaticTokenProvider("tok".into());
    let (exit, report) = run_check(&cfg, &provider, &gcp.uri(), &clarify.uri(), &s).await;
    assert_eq!(exit, ExitCode::Complete, "report:\n{report}");
    assert!(report.contains("ok    dataset: proj.ds does not exist yet"));
    assert!(report.contains("ok    tables: skipped"));
}

#[tokio::test]
async fn mark_complete_loads_runs_row_with_derived_timestamp() {
    let gcp = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bigquery/v2/projects/proj/datasets/ds/tables/runs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&gcp)
        .await;
    Mock::given(method("POST"))
        .and(path("/upload/bigquery/v2/projects/proj/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": {"state": "RUNNING"}
        })))
        .mount(&gcp)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/bigquery/v2/projects/proj/jobs/[^/]+$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": {"state": "DONE"},
            "statistics": {"load": {"outputRows": "1"}}
        })))
        .mount(&gcp)
        .await;

    let run_id = uuid::Uuid::now_v7().to_string();
    let spool = tempfile::tempdir().unwrap();
    let (exit, msg) = run_mark_complete(&sink(&gcp), &run_id, spool.path()).await;
    assert_eq!(exit, ExitCode::Complete, "msg: {msg}");
    assert!(msg.contains(&run_id));

    // The uploaded body carries the runs row with status complete.
    let uploads: Vec<_> = gcp
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().starts_with("/upload/"))
        .collect();
    assert_eq!(uploads.len(), 1);
    let body = String::from_utf8_lossy(&uploads[0].body).to_string();
    assert!(body.contains("\"status\":\"complete\""));
    assert!(body.contains("\"repaired\":true"));
}

#[tokio::test]
async fn mark_complete_rejects_non_uuid() {
    let gcp = MockServer::start().await;
    let spool = tempfile::tempdir().unwrap();
    let (exit, _) = run_mark_complete(&sink(&gcp), "not-a-uuid", spool.path()).await;
    assert_eq!(exit, ExitCode::ConfigAuth);
}
