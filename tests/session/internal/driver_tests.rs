// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use bytes::Bytes;
use futures_util::SinkExt;
use qubit_websocket::WsClient;
use qubit_websocket::WsMessage;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::time;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

const DEADLINE: Duration = Duration::from_secs(2);

async fn peer_join<T>(peer: JoinHandle<T>) -> T {
    time::timeout(DEADLINE, peer)
        .await
        .expect("peer task deadline")
        .expect("peer task exits")
}

#[tokio::test]
async fn test_session_receives_text_and_binary_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Text("text".into()))
            .await
            .expect("send text message");
        websocket
            .send(Message::Binary(Bytes::from_static(b"bin")))
            .await
            .expect("send binary message");
        websocket.close(None).await.expect("close after application messages");
    });

    let client = WsClient::builder().build().expect("valid client");
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect timeout")
        .expect("connect");

    assert!(matches!(
        time::timeout(DEADLINE, session.receive())
            .await
            .expect("receive deadline"),
        Some(Ok(WsMessage::Text(text))) if text == "text"
    ));
    assert!(matches!(
        time::timeout(DEADLINE, session.receive())
            .await
            .expect("receive deadline"),
        Some(Ok(WsMessage::Binary(bytes))) if bytes == Bytes::from_static(b"bin")
    ));
    assert!(
        time::timeout(DEADLINE, session.receive())
            .await
            .expect("close deadline")
            .is_none()
    );
    peer_join(peer).await;
}
