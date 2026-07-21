use bq_sink::{BqSink, StaticTokenProvider};
use clarify_bq::cli::{BackupArgs, ConnArgs, ExitCode, Format};
use clarify_bq::orchestrate::run_backup;
use clarify_client::ClarifyClient;
use std::sync::Arc;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn args(dry_run: bool) -> BackupArgs {
    BackupArgs {
        conn: ConnArgs {
            workspace: "acme".into(),
            project: "proj".into(),
            secret: None,
            dataset: "ds".into(),
            location: "US".into(),
        },
        objects: vec![],
        skip: vec![],
        dry_run,
        timeout: None,
        spool_dir: None,
        no_lock: true,
        shrink_threshold: 5.0,
        no_shrink_check: false,
        partition_expiration_days: 400,
        output: Format::Text,
    }
}

async fn mock_clarify_happy(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type": "schema", "id": "sch_1", "attributes": {
                "entity": "person",
                "fields": {"company": {"type": "relationship"}}
            }}],
            "links": {"next": null}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/objects/person/resources"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [
                {"type": "person", "id": "rec_1", "attributes": {"name": "Synthetic One"},
                 "relationships": {"company": {"data": null}}},
                {"type": "person", "id": "rec_2", "attributes": {"name": "Synthetic Two"},
                 "relationships": {"company": {"data": null}}}
            ],
            "included": [], "meta": {"total_records": 2}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/workspaces/acme/objects/person/records/[^/]+/activities$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type": "activity", "id": "act_1", "attributes": {"kind": "comment"}}],
            "links": {"next": null}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/workspaces/acme/objects/person/records/[^/]+/attachments$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [], "links": {"next": null}
        })))
        .mount(server)
        .await;
    for p in ["/workspaces/acme/lists", "/workspaces/acme/workflows"] {
        Mock::given(method("GET"))
            .and(path(p))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [], "links": {"next": null}
            })))
            .mount(server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type": "user", "id": "usr_1", "attributes": {"email": "synthetic@example.test"}}],
            "links": {"next": null}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "orgDescription": "synthetic"
        })))
        .mount(server)
        .await;
}

async fn mock_bq(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/bigquery/v2/projects/proj/datasets/ds"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/bigquery/v2/projects/proj/datasets/ds/tables/[^/]+$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(server)
        .await;
    Mock::given(method("PATCH"))
        .and(path_regex(r"^/bigquery/v2/projects/proj/datasets/ds/tables/[^/]+$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/upload/bigquery/v2/projects/proj/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": {"state": "RUNNING"}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/bigquery/v2/projects/proj/jobs/[^/]+$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": {"state": "DONE"},
            "statistics": {"load": {"outputRows": "1"}}
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bigquery/v2/projects/proj/queries"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "rows": []
        })))
        .mount(server)
        .await;
}

fn harness(clarify: &MockServer, bq: &MockServer) -> (ClarifyClient, BqSink) {
    let client =
        ClarifyClient::new(clarify.uri(), "sk_test".into(), "acme".into()).unwrap();
    let sink = BqSink::new(
        Arc::new(StaticTokenProvider("tok".into())),
        bq.uri(),
        "proj".into(),
        "ds".into(),
        "US".into(),
    );
    (client, sink)
}

fn outcome<'a>(summary: &'a serde_json::Value, resource: &str) -> &'a serde_json::Value {
    summary["resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["resource"] == resource)
        .unwrap_or_else(|| panic!("no outcome for {resource}"))
}

#[tokio::test]
async fn happy_path_backs_up_everything_and_marks_complete() {
    let clarify = MockServer::start().await;
    let bq = MockServer::start().await;
    mock_clarify_happy(&clarify).await;
    mock_bq(&bq).await;
    let (client, sink) = harness(&clarify, &bq);
    let spool = tempfile::tempdir().unwrap();

    let result = run_backup(&client, &sink, &args(false), spool.path()).await;
    assert_eq!(result.exit, ExitCode::Complete, "summary: {}", result.summary);
    assert_eq!(result.summary["status"], "complete");
    assert_eq!(outcome(&result.summary, "records_person")["count"], 2);
    assert_eq!(outcome(&result.summary, "activities:person")["count"], 2);
    assert_eq!(outcome(&result.summary, "users")["count"], 1);
    assert_eq!(outcome(&result.summary, "settings")["count"], 1);
    // Spool removed on success.
    assert_eq!(std::fs::read_dir(spool.path()).unwrap().flatten()
        .filter(|e| e.file_type().unwrap().is_dir()).count(), 0);
}

#[tokio::test]
async fn dry_run_writes_nothing() {
    let clarify = MockServer::start().await;
    let bq = MockServer::start().await; // no mocks: any BQ call would 404 and fail
    mock_clarify_happy(&clarify).await;
    let (client, sink) = harness(&clarify, &bq);
    let spool = tempfile::tempdir().unwrap();

    let result = run_backup(&client, &sink, &args(true), spool.path()).await;
    assert_eq!(result.exit, ExitCode::Complete);
    assert_eq!(result.summary["dry_run"], true);
    assert_eq!(bq.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn aux_failure_is_partial_exit() {
    let clarify = MockServer::start().await;
    let bq = MockServer::start().await;
    mock_clarify_happy(&clarify).await;
    mock_bq(&bq).await;
    // Override /users with a 403 (mounted later = higher priority via expect).
    clarify.reset().await;
    mock_clarify_happy(&clarify).await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/users"))
        .respond_with(ResponseTemplate::new(403))
        .with_priority(1)
        .mount(&clarify)
        .await;
    let (client, sink) = harness(&clarify, &bq);
    let spool = tempfile::tempdir().unwrap();

    let result = run_backup(&client, &sink, &args(false), spool.path()).await;
    assert_eq!(result.exit, ExitCode::Partial, "summary: {}", result.summary);
    assert_eq!(result.summary["status"], "partial");
    assert_eq!(outcome(&result.summary, "users")["status"], "failed");
    assert_eq!(outcome(&result.summary, "records_person")["status"], "ok");
}

#[tokio::test]
async fn records_failure_is_failed_exit() {
    let clarify = MockServer::start().await;
    let bq = MockServer::start().await;
    mock_clarify_happy(&clarify).await;
    mock_bq(&bq).await;
    clarify.reset().await;
    mock_clarify_happy(&clarify).await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/objects/person/resources"))
        .respond_with(ResponseTemplate::new(500).insert_header("Retry-After", "0"))
        .with_priority(1)
        .mount(&clarify)
        .await;
    let (client, sink) = harness(&clarify, &bq);
    let spool = tempfile::tempdir().unwrap();

    let result = run_backup(&client, &sink, &args(false), spool.path()).await;
    assert_eq!(result.exit, ExitCode::Failed, "summary: {}", result.summary);
    assert_eq!(result.summary["status"], "failed");
    assert_eq!(outcome(&result.summary, "records_person")["status"], "failed");
    assert_eq!(outcome(&result.summary, "activities:person")["status"], "skipped");
}
