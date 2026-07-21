use clarify_client::ClarifyClient;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn linked_collection_follows_next_until_null() {
    let server = MockServer::start().await;
    let next = format!("{}/workspaces/acme/lists?page[offset]=1", server.uri());
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/lists"))
        .and(query_param_is_missing("page[offset]"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type":"list","id":"lst_1","attributes":{"entity":"person","name":"Synthetic A"}}],
            "links": {"next": next}, "meta": {"total_records": 2, "total_pages": 2}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/lists"))
        .and(query_param("page[offset]", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type":"list","id":"lst_2","attributes":{"entity":"deal","name":"Synthetic B"}}],
            "links": {"next": null}, "meta": {"total_records": 2, "total_pages": 2}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = ClarifyClient::new(server.uri(), "sk_test".into(), "acme".into()).unwrap();
    let mut ids = Vec::new();
    let stats = client
        .fetch_linked("/lists", &mut |v| {
            ids.push(v["id"].as_str().unwrap().to_string());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(ids, vec!["lst_1", "lst_2"]);
    assert_eq!(stats.fetched, 2);
}

#[tokio::test]
async fn settings_is_returned_verbatim() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "orgDescription": "synthetic", "ingestionRules": []
        })))
        .mount(&server)
        .await;
    let client = ClarifyClient::new(server.uri(), "sk_test".into(), "acme".into()).unwrap();
    let doc = client.fetch_settings().await.unwrap();
    assert_eq!(doc["orgDescription"], "synthetic");
}

#[tokio::test]
async fn record_activities_path_is_correct() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/objects/person/records/rec_1/activities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type":"activity","id":"act_1","attributes":{"kind":"comment"}}],
            "links": {"next": null}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let client = ClarifyClient::new(server.uri(), "sk_test".into(), "acme".into()).unwrap();
    let mut n = 0;
    client
        .fetch_record_activities("person", "rec_1", &mut |_| {
            n += 1;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(n, 1);
}
