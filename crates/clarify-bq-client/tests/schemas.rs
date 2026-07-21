use clarify_bq_client::ClarifyClient;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn follows_cursor_and_extracts_object_slugs_from_titles() {
    let server = MockServer::start().await;
    let page2_url = format!("{}/workspaces/acme/schemas?cursor=abc", server.uri());
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/schemas"))
        .and(query_param_is_missing("cursor"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [
                // A real object schema: JSON Schema doc, slug in title,
                // relationships as properties with xClarifyRelationship.
                {"type": "schema", "id": "https://example.test/schemas/core/person",
                 "attributes": {
                    "$id": "https://example.test/schemas/core/person",
                    "title": "person",
                    "xClarifyNamespace": "objects",
                    "properties": {
                        "_id": {"type": "string"},
                        "company_id": {"type": ["string", "null"],
                            "xClarifyRelationship": {"kind": "many-to-one", "entity": "company"}},
                        "deals": {"oneOf": [{"type": "null"}],
                            "xClarifyRelationship": {"kind": "many-to-many", "entity": "deal"}}
                    }
                }},
                // A value schema: no namespace, no title — not an object.
                {"type": "schema", "id": "https://example.test/schemas/core/collectionOfStrings",
                 "attributes": {"$id": "https://example.test/schemas/core/collectionOfStrings",
                    "type": "object"}}
            ],
            "links": {"next": page2_url}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/schemas"))
        .and(query_param("cursor", "abc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"type": "schema", "id": "https://example.test/schemas/entities/c_sales_order",
                "attributes": {"title": "c_sales_order", "xClarifyNamespace": "objects",
                    "properties": {}}}],
            "links": {"next": null}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = ClarifyClient::new(server.uri(), "sk_test".into(), "acme".into()).unwrap();
    let schemas = client.fetch_schemas().await.unwrap();
    assert_eq!(schemas.len(), 3);
    assert_eq!(schemas[0].slug, "person");
    assert!(schemas[0].object);
    assert_eq!(
        schemas[0].relationships,
        vec!["company_id".to_string(), "deals".to_string()]
    );
    assert!(
        !schemas[1].object,
        "value schema must not count as an object"
    );
    assert_eq!(schemas[2].slug, "c_sales_order");
    assert!(schemas[2].object);
    assert!(schemas[2].relationships.is_empty());
}
