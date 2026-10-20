// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use qubit_web::options::ServerOptions;
use qubit_web::server::ServerContext;
use qubit_web::server::WebServer;
use qubit_web::ws::WsSession;
use qubit_web::ws::WsUpgradePolicy;
use qubit_websocket::WsClient;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsMessage;
use qubit_websocket::WsTermination;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::timeout;

const TOKEN: &str = "integration-test-secret";
const ORIGIN: &str = "https://client.example";

#[derive(Clone)]
struct AppState {
    context: ServerContext,
    policy: WsUpgradePolicy,
}

async fn websocket(State(state): State<AppState>, headers: HeaderMap, upgrade: WebSocketUpgrade) -> Response {
    if !authorized(&headers) {
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

async fn server_close_websocket(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    state.policy.on_upgrade(
        upgrade.protocols(["echo.v1"]),
        &headers,
        state.context,
        |mut session: WsSession| async move {
            session.close().await;
        },
    )
}

async fn oversized_websocket(State(state): State<AppState>, headers: HeaderMap, upgrade: WebSocketUpgrade) -> Response {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    state
        .policy
        .clone()
        .max_frame_bytes(16)
        .max_message_bytes(32)
        .on_upgrade(
            upgrade.protocols(["echo.v1"]),
            &headers,
            state.context,
            |mut session: WsSession| async move {
                let _ = session.recv().await;
            },
        )
}

fn authorized(headers: &HeaderMap) -> bool {
    headers.get(AUTHORIZATION).and_then(|value| value.to_str().ok()) == Some("Bearer integration-test-secret")
}

async fn start_server() -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let server = WebServer::bind_http(ServerOptions::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)))
        .await
        .expect("loopback server should bind");
    let address = server.local_addr();
    let state = AppState {
        context: server.context(),
        policy: WsUpgradePolicy::new().allowed_origins([ORIGIN]),
    };
    let app = Router::new()
        .route("/ws", get(websocket))
        .route("/server-close", get(server_close_websocket))
        .route("/oversized", get(oversized_websocket))
        .with_state(state);
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

fn client() -> WsClient {
    WsClient::builder()
        .subprotocols(vec!["echo.v1".to_owned()])
        .handshake_timeout(Duration::from_secs(2))
        .build()
        .expect("client configuration should be valid")
}

fn headers(token: &str, origin: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
    headers.insert("origin", origin.parse().unwrap());
    headers
}

#[tokio::test]
async fn test_qubit_web_server_interoperates_for_policy_messages_close_and_shutdown() {
    let (url, shutdown, server_task) = start_server().await;

    let unauthorized = match client().connect_with(&url, headers("wrong-secret", ORIGIN)).await {
        Ok(_) => panic!("the server should reject an invalid bearer token"),
        Err(error) => error,
    };
    assert_eq!(unauthorized.status_code(), Some(401));

    let forbidden_origin = match client()
        .connect_with(&url, headers(TOKEN, "https://untrusted.example"))
        .await
    {
        Ok(_) => panic!("the server should reject an unapproved Origin"),
        Err(error) => error,
    };
    assert_eq!(forbidden_origin.status_code(), Some(403));

    let mut session = client()
        .connect_with(&url, headers(TOKEN, ORIGIN))
        .await
        .expect("the allowed origin and bearer token should connect");
    assert_eq!(session.response_status_code(), 101);
    assert_eq!(session.selected_subprotocol(), Some("echo.v1"));
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
        .send(WsMessage::Binary(bytes::Bytes::from_static(&[0, 1, 2, 255])))
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

    session.close().await.expect("client close handshake should complete");
    let _ = shutdown.send(());
    timeout(Duration::from_secs(3), server_task)
        .await
        .expect("qubit-web server shutdown should be bounded")
        .expect("server task should join");
}

#[tokio::test]
async fn test_connection_errors_do_not_leak_the_bearer_secret() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local handshake fixture");
    let address = listener.local_addr().expect("fixture address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept TCP connection");
        drop(socket);
    });
    let error = match client()
        .connect_with(&format!("ws://{address}/ws"), headers(TOKEN, ORIGIN))
        .await
    {
        Ok(_) => panic!("the peer should interrupt the opening handshake"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::Transport);
    assert!(!format!("{error:?}").contains(TOKEN));
    assert!(!format!("{error}").contains(TOKEN));
    peer.await.expect("fixture task exits");
}

#[tokio::test]
async fn test_server_initiated_close_is_observable_as_closed() {
    let (base_url, shutdown, server_task) = start_server().await;
    let url = base_url.replace("/ws", "/server-close");
    let mut session = client()
        .connect_with(&url, headers(TOKEN, ORIGIN))
        .await
        .expect("server close route should connect");

    assert!(
        timeout(Duration::from_secs(2), session.receive())
            .await
            .expect("server close should be bounded")
            .is_none(),
        "server Close should end receive without an application message"
    );
    assert!(matches!(session.termination(), Some(WsTermination::Closed { .. })));

    let _ = shutdown.send(());
    timeout(Duration::from_secs(3), server_task)
        .await
        .expect("server shutdown should be bounded")
        .expect("server task should join");
}

#[tokio::test]
async fn test_server_enforces_small_message_limits_and_terminates_the_client() {
    let (base_url, shutdown, server_task) = start_server().await;
    let url = base_url.replace("/ws", "/oversized");
    let mut session = client()
        .connect_with(&url, headers(TOKEN, ORIGIN))
        .await
        .expect("limited route handshake should succeed");
    session
        .send(WsMessage::Text("x".repeat(64)))
        .await
        .expect("message should fit the client's default limits");

    let terminal = timeout(Duration::from_secs(2), session.receive())
        .await
        .expect("server limit enforcement should be bounded");
    match terminal {
        Some(Err(error)) => {
            assert_eq!(error.kind(), WsErrorKind::MessageTooLarge);
            assert_eq!(
                session.termination(),
                Some(WsTermination::Error {
                    kind: WsErrorKind::MessageTooLarge
                })
            );
            assert!(
                timeout(Duration::from_secs(2), session.receive())
                    .await
                    .expect("second terminal receive should be bounded")
                    .is_none(),
                "terminal error is delivered once"
            );
        }
        Some(Ok(message)) => panic!("server unexpectedly delivered {message:?}"),
        None => assert_eq!(session.termination(), Some(WsTermination::Closed { code: Some(1009) })),
    }

    let _ = shutdown.send(());
    timeout(Duration::from_secs(3), server_task)
        .await
        .expect("server shutdown should be bounded")
        .expect("server task should join");
}

#[tokio::test]
async fn test_server_shutdown_terminates_an_active_client_session() {
    let (url, shutdown, server_task) = start_server().await;
    let mut session = client()
        .connect_with(&url, headers(TOKEN, ORIGIN))
        .await
        .expect("active session should connect before server shutdown");

    let _ = shutdown.send(());
    assert!(
        timeout(Duration::from_secs(2), session.receive())
            .await
            .expect("server shutdown should terminate the active receive")
            .is_none(),
        "server shutdown should close the client session"
    );
    assert!(matches!(session.termination(), Some(WsTermination::Closed { .. })));
    timeout(Duration::from_secs(3), server_task)
        .await
        .expect("qubit-web should finish stopping within its deadline")
        .expect("server task should join");
}
