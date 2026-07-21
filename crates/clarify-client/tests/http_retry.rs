use clarify_client::{ClarifyClient, ClientError};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> ClarifyClient {
    ClarifyClient::new(server.uri(), "sk_test_synthetic".into(), "acme".into()).unwrap()
}

#[tokio::test]
async fn sends_api_key_auth_header() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/users"))
        .and(header("Authorization", "api-key sk_test_synthetic"))
        .and(wiremock::matchers::header_exists("User-Agent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": []})))
        .expect(1)
        .mount(&server)
        .await;
    client(&server).get_json("/users").await.unwrap();
}

#[tokio::test]
async fn retries_429_honoring_retry_after_then_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/users"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": []})))
        .expect(1)
        .mount(&server)
        .await;
    client(&server).get_json("/users").await.unwrap();
}

#[tokio::test]
async fn retries_5xx_up_to_budget_then_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/users"))
        .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "0"))
        .expect(5)
        .mount(&server)
        .await;
    let err = client(&server).get_json("/users").await.unwrap_err();
    assert!(matches!(
        err,
        ClientError::Http {
            status: 503,
            attempts: 5,
            ..
        }
    ));
}

#[tokio::test]
async fn auth_failure_is_immediate_no_retry() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/workspaces/acme/users"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    let err = client(&server).get_json("/users").await.unwrap_err();
    assert!(matches!(err, ClientError::Auth { status: 401, .. }));
}
