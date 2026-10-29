#![cfg(not(feature = "local"))]

use std::sync::Arc;

use rmcp::{
    ServerHandler,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use tokio_util::sync::CancellationToken;

struct TestServer;
impl ServerHandler for TestServer {}

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;

async fn server(fail_factory: bool) -> (String, Arc<LocalSessionManager>, CancellationToken) {
    let sessions = Arc::new(LocalSessionManager::default());
    let config = StreamableHttpServerConfig::default();
    let cancellation = config.cancellation_token.clone();
    let service = StreamableHttpService::new(
        move || {
            if fail_factory {
                Err(std::io::Error::other("unavailable"))
            } else {
                Ok(TestServer)
            }
        },
        sessions.clone(),
        config,
    );
    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let shutdown = cancellation.clone();
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    (url, sessions, cancellation)
}

async fn post(client: &reqwest::Client, url: &str, body: &str) -> reqwest::Response {
    client
        .post(url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(body.to_owned())
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn stateful_rejected_initial_posts_do_not_create_sessions() {
    let (url, sessions, cancellation) = server(false).await;
    let client = reqwest::Client::new();
    for body in [
        r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        r#"{"jsonrpc":"2.0","id":1,"result":{}}"#,
        "invalid json",
    ] {
        for _ in 0..4 {
            assert!(post(&client, &url, body).await.status().is_client_error());
            assert!(sessions.sessions.read().await.is_empty());
        }
    }
    let response = post(&client, &url, INITIALIZE).await;
    assert_eq!(response.status(), 200);
    let session_id = response.headers()["mcp-session-id"].clone();
    assert_eq!(sessions.sessions.read().await.len(), 1);
    drop(response);
    let deleted = client
        .delete(&url)
        .header("mcp-session-id", session_id)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 202);
    assert!(sessions.sessions.read().await.is_empty());
    cancellation.cancel();
}

#[tokio::test]
async fn failed_service_factory_does_not_create_a_session() {
    let (url, sessions, cancellation) = server(true).await;
    let response = post(&reqwest::Client::new(), &url, INITIALIZE).await;
    assert_eq!(response.status(), 500);
    assert!(sessions.sessions.read().await.is_empty());
    cancellation.cancel();
}
