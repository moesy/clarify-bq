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
        views_dataset: None,
        no_views: false,
        output: Format::Text,
    }
}

async fn mock_clarify_happy(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type": "schema", "id": "https://example.test/schemas/entities/person",
                "attributes": {
                    "title": "person",
                    "xClarifyNamespace": "objects",
                    "properties": {
                        "_id": {"type": "string"},
                        "company_id": {"type": ["string", "null"],
                            "xClarifyRelationship": {"kind": "many-to-one", "entity": "company"}}
                    }
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
        .and(path_regex(
            r"^/workspaces/acme/objects/person/records/[^/]+/activities$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type": "activity", "id": "act_1", "attributes": {"kind": "comment"}}],
            "links": {"next": null}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(
            r"^/workspaces/acme/objects/person/records/[^/]+/attachments$",
        ))
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
        .and(path_regex(r"^/bigquery/v2/projects/proj/datasets/[^/]+$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(
            r"^/bigquery/v2/projects/proj/datasets/ds/tables/[^/]+$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(server)
        .await;
    Mock::given(method("PATCH"))
        .and(path_regex(
            r"^/bigquery/v2/projects/proj/datasets/ds/tables/[^/]+$",
        ))
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
    let client = ClarifyClient::new(clarify.uri(), "sk_test".into(), "acme".into()).unwrap();
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
    assert_eq!(
        result.exit,
        ExitCode::Complete,
        "summary: {}",
        result.summary
    );
    assert_eq!(result.summary["status"], "complete");
    assert_eq!(outcome(&result.summary, "records_person")["count"], 2);
    assert_eq!(outcome(&result.summary, "activities:person")["count"], 2);
    assert_eq!(outcome(&result.summary, "users")["count"], 1);
    assert_eq!(outcome(&result.summary, "settings")["count"], 1);
    // Latest views refreshed into <dataset>_latest (person + aux + runs).
    assert_eq!(result.summary["views"]["dataset"], "ds_latest");
    assert!(result.summary["views"]["created"].as_u64().unwrap() >= 2);
    assert!(
        result.summary["views"]["errors"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let view_ddls: Vec<String> = bq
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path().ends_with("/queries"))
        .map(|r| String::from_utf8_lossy(&r.body).to_string())
        .filter(|b| b.contains("CREATE OR REPLACE VIEW"))
        .collect();
    assert!(
        view_ddls
            .iter()
            .any(|b| b.contains("`proj.ds_latest.person`")),
        "person view DDL issued"
    );
    // Spool removed on success.
    assert_eq!(
        std::fs::read_dir(spool.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_type().unwrap().is_dir())
            .count(),
        0
    );
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
    assert_eq!(
        result.exit,
        ExitCode::Partial,
        "summary: {}",
        result.summary
    );
    assert_eq!(result.summary["status"], "partial");
    assert_eq!(outcome(&result.summary, "users")["status"], "failed");
    assert_eq!(outcome(&result.summary, "records_person")["status"], "ok");
}

#[tokio::test]
async fn object_without_records_endpoint_is_skipped_not_failed() {
    let clarify = MockServer::start().await;
    let bq = MockServer::start().await;
    mock_clarify_happy(&clarify).await;
    mock_bq(&bq).await;
    // Discovery also returns an object whose records endpoint 404s (like
    // Clarify's agent feature).
    clarify.reset().await;
    mock_clarify_happy(&clarify).await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [
                {"type": "schema", "id": "https://example.test/schemas/entities/person",
                 "attributes": {"title": "person", "xClarifyNamespace": "objects",
                    "properties": {"company_id": {"xClarifyRelationship": {"entity": "company"}}}}},
                {"type": "schema", "id": "https://example.test/schemas/entities/ghost",
                 "attributes": {"title": "ghost", "xClarifyNamespace": "objects", "properties": {}}}
            ],
            "links": {"next": null}
        })))
        .with_priority(1)
        .mount(&clarify)
        .await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/objects/ghost/resources"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&clarify)
        .await;
    let (client, sink) = harness(&clarify, &bq);
    let spool = tempfile::tempdir().unwrap();

    let result = run_backup(&client, &sink, &args(false), spool.path()).await;
    assert_eq!(
        result.exit,
        ExitCode::Complete,
        "summary: {}",
        result.summary
    );
    assert_eq!(
        outcome(&result.summary, "records_ghost")["status"],
        "skipped"
    );
    assert_eq!(outcome(&result.summary, "records_person")["status"], "ok");
}

#[tokio::test]
async fn single_bad_record_feed_is_skipped_and_marked_dirty() {
    let clarify = MockServer::start().await;
    let bq = MockServer::start().await;
    mock_clarify_happy(&clarify).await;
    mock_bq(&bq).await;
    // rec_1's activity feed is broken server-side; rec_2's works.
    Mock::given(method("GET"))
        .and(path(
            "/workspaces/acme/objects/person/records/rec_1/activities",
        ))
        .respond_with(ResponseTemplate::new(404))
        .with_priority(1)
        .mount(&clarify)
        .await;
    let (client, sink) = harness(&clarify, &bq);
    let spool = tempfile::tempdir().unwrap();

    let result = run_backup(&client, &sink, &args(false), spool.path()).await;
    assert_eq!(
        result.exit,
        ExitCode::Complete,
        "summary: {}",
        result.summary
    );
    let act = outcome(&result.summary, "activities:person");
    assert_eq!(act["status"], "ok");
    assert_eq!(act["count"], 1, "rec_2's activity still captured");
    assert_eq!(act["consistency"], "dirty");
    assert!(
        act["error"]
            .as_str()
            .unwrap()
            .contains("1 record feed(s) skipped")
    );
}

#[tokio::test]
async fn feed_circuit_breaker_stops_after_three_consecutive_failures() {
    let clarify = MockServer::start().await;
    let bq = MockServer::start().await;
    mock_clarify_happy(&clarify).await;
    mock_bq(&bq).await;
    // Five records; every activity feed 404s (persistently broken endpoint).
    clarify.reset().await;
    mock_clarify_happy(&clarify).await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/objects/person/resources"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": (1..=5).map(|i| serde_json::json!({
                "type": "person", "id": format!("rec_{i}"),
                "attributes": {"name": format!("Synthetic {i}")},
                "relationships": {}
            })).collect::<Vec<_>>(),
            "included": [], "meta": {"total_records": 5}
        })))
        .with_priority(1)
        .mount(&clarify)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(
            r"^/workspaces/acme/objects/person/records/[^/]+/activities$",
        ))
        .respond_with(ResponseTemplate::new(404))
        .with_priority(1)
        .expect(3) // breaker must stop further attempts
        .mount(&clarify)
        .await;
    let (client, sink) = harness(&clarify, &bq);
    let spool = tempfile::tempdir().unwrap();

    let result = run_backup(&client, &sink, &args(false), spool.path()).await;
    assert_eq!(
        result.exit,
        ExitCode::Complete,
        "summary: {}",
        result.summary
    );
    let act = outcome(&result.summary, "activities:person");
    assert_eq!(act["status"], "ok");
    assert_eq!(act["consistency"], "dirty");
    assert_eq!(act["count"], 0);
    assert!(act["error"].as_str().unwrap().contains("circuit opened"));
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
    assert_eq!(
        outcome(&result.summary, "records_person")["status"],
        "failed"
    );
    assert_eq!(
        outcome(&result.summary, "activities:person")["status"],
        "skipped"
    );
}
