use clarify_client::ClarifyClient;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn page(ids: &[&str], total: u64) -> serde_json::Value {
    serde_json::json!({
        "data": ids.iter().map(|id| serde_json::json!({
            "type": "person", "id": id,
            "attributes": {"name": format!("Synthetic {id}")},
            "relationships": {"company": {"data": null}}
        })).collect::<Vec<_>>(),
        "included": [], "meta": {"total_records": total}
    })
}

#[tokio::test]
async fn pages_by_returned_count_and_terminates_on_short_page() {
    let server = MockServer::start().await;
    // Server clamps the limit: returns 2 per page even though 500 requested.
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/objects/person/resources"))
        .and(query_param("page[offset]", "0"))
        .and(query_param("page[limit]", "500"))
        .and(query_param("sortOrder[column]", "_created_at"))
        .and(query_param("sortOrder[dir]", "ASC"))
        .and(query_param("include", "company,deals"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(&["r1", "r2"], 3)))
        .expect(1)
        .mount(&server)
        .await;
    // Offset must advance by 2 (returned), not 500 (requested).
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/objects/person/resources"))
        .and(query_param("page[offset]", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(&["r3"], 3)))
        .expect(1)
        .mount(&server)
        .await;

    let client = ClarifyClient::new(server.uri(), "sk_test".into(), "acme".into()).unwrap();
    let mut seen = Vec::new();
    let stats = client
        .fetch_records("person", &["company".into(), "deals".into()], &mut |item| {
            seen.push(item["id"].as_str().unwrap().to_string());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(seen, vec!["r1", "r2", "r3"]);
    assert_eq!(stats.fetched, 3);
    assert_eq!(stats.expected, Some(3));
    assert_eq!(stats.consistency(), "clean");
}

#[tokio::test]
async fn count_drift_reports_dirty() {
    let server = MockServer::start().await;
    // total_records claims 5, but the data runs dry after 1: the next offset
    // returns an empty page (records deleted mid-run, or server truncation).
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/objects/person/resources"))
        .and(query_param("page[offset]", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(&["r1"], 5)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/objects/person/resources"))
        .and(query_param("page[offset]", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(&[], 5)))
        .mount(&server)
        .await;
    let client = ClarifyClient::new(server.uri(), "sk_test".into(), "acme".into()).unwrap();
    let stats = client
        .fetch_records("person", &[], &mut |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(stats.fetched, 1);
    assert_eq!(stats.consistency(), "dirty");
}
