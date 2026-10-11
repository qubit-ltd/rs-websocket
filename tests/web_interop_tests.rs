use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::Router;
use axum::extract::{State, ws::WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use qubit_web::options::ServerOptions;
use qubit_web::server::{ServerContext, WebServer};
use qubit_web::ws::{WsSession, WsUpgradePolicy};
use qubit_websocket::{WsClient, WsErrorKind, WsMessage};
use tokio::sync::oneshot;
use tokio::time::timeout;

const TOKEN: &str = "integration-test-secret";
const ORIGIN: &str = "https://client.example";

#[derive(Clone)]
struct AppState {
    context: ServerContext,
    policy: WsUpgradePolicy,
}

async fn websocket(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        != Some("Bearer integration-test-secret")
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    state.policy.on_upgrade(
        upgrade.protocols(["echo.v1"]),
        &headers,
        state.context,
        |mut session: WsSession| async move {
            while let Some(Ok(message)) = session.recv().await {
                if session.try_send(message).is_err() {
                    break;
                }
            }
        },
    )
}

async fn start_server() -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let server = WebServer::bind_http(ServerOptions::new(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        0,
    )))
    .await
    .expect("loopback server should bind");
    let address = server.local_addr();
    let state = AppState {
        context: server.context(),
        policy: WsUpgradePolicy::new().allowed_origins([ORIGIN]),
    };
    let app = Router::new().route("/ws", get(websocket)).with_state(state);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let report = server
            .serve(app, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("loopback server should shut down cleanly");
        assert!(report.graceful);
        assert_eq!(report.unfinished_managed_sessions, 0);
    });
    (format!("ws://{address}/ws"), shutdown_tx, task)
}

fn client(token: Option<&str>, origin: Option<&str>) -> WsClient {
    let mut headers = HeaderMap::new();
    if let Some(token) = token {
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
    }
    if let Some(origin) = origin {
        headers.insert("origin", origin.parse().unwrap());
    }
    WsClient::builder()
        .headers(headers)
        .subprotocols(vec!["echo.v1".to_owned()])
        .handshake_timeout(Duration::from_secs(2))
        .build()
        .expect("client configuration should be valid")
}

#[tokio::test]
async fn qubit_web_server_interoperates_for_policy_messages_close_and_shutdown() {
    let (url, shutdown, server_task) = start_server().await;

    let unauthorized = match client(Some("wrong-secret"), Some(ORIGIN))
        .connect(&url)
        .await
    {
        Ok(_) => panic!("the server should reject an invalid bearer token"),
        Err(error) => error,
    };
    assert_eq!(unauthorized.status_code(), Some(401));

    let forbidden_origin = match client(Some(TOKEN), Some("https://untrusted.example"))
        .connect(&url)
        .await
    {
        Ok(_) => panic!("the server should reject an unapproved Origin"),
        Err(error) => error,
    };
    assert_eq!(forbidden_origin.status_code(), Some(403));

    let mut session = client(Some(TOKEN), Some(ORIGIN))
        .connect(&url)
        .await
        .expect("the allowed origin and bearer token should connect");
    session
        .send(WsMessage::Text("hello from the client".to_owned()))
        .await
        .expect("text should enter the bounded send queue");
    assert_eq!(
        timeout(Duration::from_secs(2), session.receive())
            .await
            .expect("text echo should be bounded")
            .expect("text frame should arrive")
            .expect("text echo should succeed"),
        WsMessage::Text("hello from the client".to_owned()),
    );

    session
        .send(WsMessage::Binary(bytes::Bytes::from_static(&[
            0, 1, 2, 255,
        ])))
        .await
        .expect("binary should enter the bounded send queue");
    assert_eq!(
        timeout(Duration::from_secs(2), session.receive())
            .await
            .expect("binary echo should be bounded")
            .expect("binary frame should arrive")
            .expect("binary echo should succeed"),
        WsMessage::Binary(bytes::Bytes::from_static(&[0, 1, 2, 255])),
    );

    session
        .close()
        .await
        .expect("client close handshake should complete");
    let _ = shutdown.send(());
    timeout(Duration::from_secs(3), server_task)
        .await
        .expect("qubit-web server shutdown should be bounded")
        .expect("server task should join");
}

#[tokio::test]
async fn connection_errors_do_not_leak_the_bearer_secret() {
    let error = match client(Some(TOKEN), Some(ORIGIN))
        .connect("ws://127.0.0.1:1/ws")
        .await
    {
        Ok(_) => panic!("an unused loopback port should refuse the connection"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::Connect);
    assert!(!format!("{error:?}").contains(TOKEN));
    assert!(!format!("{error}").contains(TOKEN));
}
