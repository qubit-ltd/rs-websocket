// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use futures_util::SinkExt;
use futures_util::StreamExt;
use qubit_websocket::WsClient;
use qubit_websocket::WsMessage;
use qubit_websocket::WsSessionLimits;
use qubit_websocket::WsTermination;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
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

fn limits() -> WsSessionLimits {
    WsSessionLimits {
        max_inbound_frame_bytes: 64,
        max_inbound_message_bytes: 64,
        inbound_messages: 2,
        inbound_bytes: 128,
        outbound_messages: 2,
        outbound_bytes: 128,
        no_progress_timeout: Duration::from_secs(1),
        close_timeout: Duration::from_millis(250),
    }
}

#[tokio::test]
async fn test_receive_delivers_last_message_before_peer_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (send_tx, send_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(socket).await.expect("handshake");
        send_rx.await.expect("client is ready");
        websocket
            .send(Message::Text("last".into()))
            .await
            .expect("send last message");
        websocket.send(Message::Close(None)).await.expect("send close");
        let _ = time::timeout(DEADLINE, websocket.next()).await;
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    send_tx.send(()).expect("peer is waiting");

    assert_eq!(
        time::timeout(DEADLINE, session.receive())
            .await
            .expect("message deadline")
            .expect("last message")
            .expect("message result"),
        WsMessage::Text("last".into()),
    );
    assert!(
        time::timeout(DEADLINE, session.receive())
            .await
            .expect("close deadline")
            .is_none()
    );
    assert!(matches!(session.termination(), Some(WsTermination::Closed { .. })));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_cancelled_pending_receive_can_be_retried() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (send_tx, send_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(socket).await.expect("handshake");
        send_rx.await.expect("client cancels first receive");
        websocket
            .send(Message::Text("retry".into()))
            .await
            .expect("send retry message");
        websocket.send(Message::Close(None)).await.expect("send close");
        let _ = time::timeout(DEADLINE, websocket.next()).await;
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    assert!(
        time::timeout(Duration::from_millis(30), session.receive())
            .await
            .is_err()
    );
    send_tx.send(()).expect("peer is waiting");
    assert_eq!(
        time::timeout(DEADLINE, session.receive())
            .await
            .expect("retry deadline")
            .expect("retry message")
            .expect("message result"),
        WsMessage::Text("retry".into()),
    );
    assert!(
        time::timeout(DEADLINE, session.receive())
            .await
            .expect("close deadline")
            .is_none()
    );
    peer_join(peer).await;
}

#[tokio::test]
async fn test_receive_delivers_last_message_before_peer_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (send_tx, send_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(socket).await.expect("handshake");
        send_rx.await.expect("client is ready");
        websocket
            .send(Message::Text("last".into()))
            .await
            .expect("send last message");
        drop(websocket);
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    send_tx.send(()).expect("peer is waiting");

    assert_eq!(
        time::timeout(DEADLINE, session.receive())
            .await
            .expect("message deadline")
            .expect("last message")
            .expect("message result"),
        WsMessage::Text("last".into()),
    );
    assert!(
        time::timeout(DEADLINE, session.receive())
            .await
            .expect("error deadline")
            .expect("terminal error")
            .is_err()
    );
    assert!(session.receive().await.is_none());
    assert!(matches!(session.termination(), Some(WsTermination::Error { .. })));
    peer_join(peer).await;
}
