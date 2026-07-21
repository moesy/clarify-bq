use base64::Engine;
use bq_sink::{SecretRef, StaticTokenProvider, fetch_secret};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn accesses_secret_and_decodes_payload() {
    let server = MockServer::start().await;
    let b64 = base64::engine::general_purpose::STANDARD.encode("sk_synthetic_value");
    Mock::given(method("GET"))
        .and(path("/v1/projects/demo-proj/secrets/clarify-key/versions/latest:access"))
        .and(header("Authorization", "Bearer tok_test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "payload": {"data": b64}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let provider = StaticTokenProvider("tok_test".into());
    let secret = SecretRef::parse("projects/demo-proj/secrets/clarify-key").unwrap();
    let value = fetch_secret(&server.uri(), &provider, &secret).await.unwrap();
    assert_eq!(value, "sk_synthetic_value");
}
