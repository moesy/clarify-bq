use clarify_client::ClarifyClient;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn follows_cursor_to_exhaustion_and_extracts_slugs() {
    let server = MockServer::start().await;
    let page2_url = format!("{}/workspaces/acme/schemas?cursor=abc", server.uri());
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/schemas"))
        .and(query_param_is_missing("cursor"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type": "schema", "id": "sch_1", "attributes": {
                "entity": "person",
                "fields": {"company": {"type": "relationship"}, "name": {"type": "text"}}
            }}],
            "links": {"next": page2_url}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/schemas"))
        .and(query_param("cursor", "abc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type": "schema", "id": "sch_2", "attributes": {
                "entity": "c_sales_order", "fields": {}
            }}],
            "links": {"next": null}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = ClarifyClient::new(server.uri(), "sk_test".into(), "acme".into()).unwrap();
    let schemas = client.fetch_schemas().await.unwrap();
    assert_eq!(schemas.len(), 2);
    assert_eq!(schemas[0].slug, "person");
    assert_eq!(schemas[0].relationships, vec!["company".to_string()]);
    assert_eq!(schemas[1].slug, "c_sales_order");
    assert!(schemas[1].relationships.is_empty());
}
