// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use super::super::WsClient;
use super::super::WsMessage;
use crate::error::WsErrorKind;
use crate::error::WsSendError;
use crate::error::WsTermination;
use crate::options::WsClientOptions;
use crate::options::WsSessionLimits;
use crate::session::internal::BoundedQueue;

#[test]
fn queue_reports_usage_and_capacity_errors() {
    let count_limited = BoundedQueue::new(1, 2);
    count_limited.try_push(WsMessage::Text("a".to_owned())).unwrap();
    assert_eq!(count_limited.usage(), (1, 1));
    assert_eq!(
        count_limited.try_push(WsMessage::Text("b".to_owned())),
        Err(WsSendError::Backpressure)
    );
    count_limited.close();
    assert_eq!(count_limited.usage(), (0, 0));
    assert_eq!(
        count_limited.try_push(WsMessage::Text("c".to_owned())),
        Err(WsSendError::Closed)
    );

    let byte_limited = BoundedQueue::new(2, 1);
    byte_limited.try_push(WsMessage::Text("x".to_owned())).unwrap();
    assert_eq!(
        byte_limited.try_push(WsMessage::Text("y".to_owned())),
        Err(WsSendError::Backpressure)
    );
}

#[tokio::test]
async fn sends_and_receives_echo_before_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        if let Some(Ok(message)) = websocket.next().await {
            websocket.send(message).await.unwrap();
            let _ = websocket.next().await;
            let _ = websocket.flush().await;
        }
    });
    let client = WsClient::builder().build().unwrap();
    let mut session = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.connect(&format!("ws://{address}/")),
    )
    .await
    .unwrap()
    .unwrap();
    session.try_send(WsMessage::Text("hi".into())).unwrap();
    assert!(
        matches!(tokio::time::timeout(std::time::Duration::from_secs(2), session.receive()).await.unwrap(), Some(Ok(WsMessage::Text(text))) if text == "hi")
    );
    session.close().await.unwrap();
    assert!(session.receive().await.is_none());
    peer.abort();
}

#[tokio::test]
async fn abrupt_tcp_eof_is_delivered_as_a_terminal_error_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let websocket = accept_async(socket).await.unwrap();
        drop(websocket);
    });

    let client = WsClient::builder()
        .limits(WsSessionLimits {
            close_timeout: std::time::Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.connect(&format!("ws://{address}/")),
    )
    .await
    .unwrap()
    .unwrap();

    let received = tokio::time::timeout(std::time::Duration::from_secs(2), session.receive())
        .await
        .expect("abrupt EOF should reach a terminal state");
    assert!(matches!(received, Some(Err(_))));
    assert!(session.receive().await.is_none());
    assert!(matches!(session.termination(), Some(WsTermination::Error { .. })));
    peer.await.unwrap();
}

#[tokio::test]
async fn explicit_peer_close_completes_the_termination() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket.close(None).await.unwrap();
    });

    let client = WsClient::builder()
        .limits(WsSessionLimits {
            close_timeout: std::time::Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.connect(&format!("ws://{address}/")),
    )
    .await
    .unwrap()
    .unwrap();

    let received = tokio::time::timeout(std::time::Duration::from_secs(2), session.receive())
        .await
        .expect("explicit peer Close should terminate the session");
    assert!(received.is_none());
    assert!(matches!(session.termination(), Some(WsTermination::Closed { .. })));
    peer.await.unwrap();
}

#[tokio::test]
async fn oversized_single_frame_reports_frame_too_large() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        let frame = tokio_tungstenite::tungstenite::protocol::frame::Frame::message(
            bytes::Bytes::from_static(b"12345"),
            tokio_tungstenite::tungstenite::protocol::frame::coding::OpCode::Data(
                tokio_tungstenite::tungstenite::protocol::frame::coding::Data::Binary,
            ),
            true,
        );
        websocket.send(Message::Frame(frame)).await.unwrap();
        let _ = websocket.next().await;
        websocket.flush().await.unwrap();
    });

    let client = WsClient::builder()
        .limits(WsSessionLimits {
            max_inbound_frame_bytes: 4,
            max_inbound_message_bytes: 8,
            inbound_bytes: 8,
            outbound_bytes: 8,
            close_timeout: std::time::Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = client.connect(&format!("ws://{address}/")).await.unwrap();
    let received = tokio::time::timeout(std::time::Duration::from_secs(2), session.receive())
        .await
        .expect("oversized frame should terminate the session");
    assert!(matches!(
        received,
        Some(Err(error)) if error.kind() == WsErrorKind::FrameTooLarge
    ));
    assert!(matches!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::FrameTooLarge
        })
    ));
    peer.await.unwrap();
}

#[tokio::test]
async fn oversized_reassembled_message_reports_message_too_large() {
    use tokio_tungstenite::tungstenite::protocol::frame::Frame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::Data;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::OpCode;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        for (payload, opcode, is_final) in [
            (b"123".as_slice(), Data::Binary, false),
            (b"456".as_slice(), Data::Continue, false),
            (b"789".as_slice(), Data::Continue, true),
        ] {
            let frame = Frame::message(bytes::Bytes::copy_from_slice(payload), OpCode::Data(opcode), is_final);
            websocket.send(Message::Frame(frame)).await.unwrap();
        }
        let _ = websocket.next().await;
        websocket.flush().await.unwrap();
    });

    let client = WsClient::builder()
        .limits(WsSessionLimits {
            max_inbound_frame_bytes: 4,
            max_inbound_message_bytes: 8,
            inbound_bytes: 8,
            outbound_bytes: 8,
            close_timeout: std::time::Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = client.connect(&format!("ws://{address}/")).await.unwrap();
    let received = tokio::time::timeout(std::time::Duration::from_secs(2), session.receive())
        .await
        .expect("oversized reassembled message should terminate the session");
    assert!(matches!(
        received,
        Some(Err(error)) if error.kind() == WsErrorKind::MessageTooLarge
    ));
    assert!(matches!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::MessageTooLarge
        })
    ));
    peer.await.unwrap();
}

#[tokio::test]
async fn no_progress_timeout_flushes_1013_close_after_queue_becomes_full() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (pong_seen_tx, pong_seen_rx) = tokio::sync::oneshot::channel();
    let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket.send(Message::Text("one".into())).await.unwrap();
        websocket.send(Message::Text("two".into())).await.unwrap();
        websocket.send(Message::Ping(b"ping".to_vec().into())).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(Ok(Message::Pong(payload))) = websocket.next().await {
                    assert_eq!(payload.as_ref(), b"ping");
                    break;
                }
            }
        })
        .await
        .expect("reader should flush Pong under queue backpressure");
        let _ = pong_seen_tx.send(());

        tokio::time::timeout(std::time::Duration::from_secs(2), continue_rx)
            .await
            .expect("test should allow queue to become full")
            .expect("test should release peer");
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            websocket.send(Message::Text("three".into())),
        )
        .await
        .expect("sending third message should be bounded")
        .unwrap();

        let close = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                match websocket.next().await {
                    Some(Ok(Message::Close(frame))) => break Ok(frame),
                    Some(Err(error)) => break Err(error),
                    None => break Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed),
                    _ => {}
                }
            }
        })
        .await
        .expect("driver should send a Close instead of resetting the TCP socket");
        let close = close.expect("peer should receive a close frame");
        assert!(
            matches!(close, Some(ref frame) if frame.code == CloseCode::Again),
            "received close frame: {close:?}"
        );
        websocket.flush().await.unwrap();
    });

    let client = WsClient::builder()
        .limits(WsSessionLimits {
            max_inbound_frame_bytes: 8,
            max_inbound_message_bytes: 8,
            inbound_messages: 1,
            inbound_bytes: 8,
            outbound_bytes: 8,
            no_progress_timeout: std::time::Duration::from_millis(100),
            close_timeout: std::time::Duration::from_secs(1),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = client.connect(&format!("ws://{address}/")).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), pong_seen_rx)
        .await
        .expect("server should observe Pong")
        .expect("server should signal Pong");
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), session.receive())
        .await
        .expect("first queued message should drain");
    assert!(matches!(first, Some(Ok(WsMessage::Text(text))) if text == "one"));
    continue_tx.send(()).expect("peer should wait before third message");
    peer.await.unwrap();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), session.receive())
            .await
            .expect("second queued message should drain"),
        Some(Ok(WsMessage::Text(text))) if text == "two"
    ));
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), session.receive())
            .await
            .expect("slow consumer terminal should arrive"),
        Some(Err(error)) if error.kind() == WsErrorKind::Timeout
    ));
}

#[tokio::test]
async fn cancelled_close_can_be_retried_to_stable_termination() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (close_seen_tx, close_seen_rx) = tokio::sync::oneshot::channel();
    let (confirm_tx, confirm_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        loop {
            if let Some(Ok(Message::Close(_))) = websocket.next().await {
                break;
            }
        }
        let _ = close_seen_tx.send(());
        confirm_rx.await.expect("test should release peer confirmation");
        websocket
            .flush()
            .await
            .expect("peer should flush the Close confirmation");
    });

    let client = WsClient::builder().build().unwrap();
    let mut session = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.connect(&format!("ws://{address}/")),
    )
    .await
    .unwrap()
    .unwrap();

    {
        let close = session.close();
        tokio::pin!(close);
        tokio::select! {
            _ = close_seen_rx => {},
            result = &mut close => panic!("close should wait for peer confirmation: {result:?}"),
        }
    }
    assert!(session.termination().is_none());
    confirm_tx.send(()).expect("peer is waiting for confirmation release");

    tokio::time::timeout(std::time::Duration::from_secs(2), session.close())
        .await
        .expect("retry should observe the same driver completion")
        .unwrap();
    peer.await.unwrap();
    let termination = session.termination().expect("driver termination should be stable");
    assert!(matches!(termination, WsTermination::Closed { .. }));
    tokio::time::timeout(std::time::Duration::from_secs(1), session.close())
        .await
        .expect("repeated close should be immediate")
        .unwrap();
    assert_eq!(session.termination(), Some(termination));
}

#[tokio::test]
async fn full_inbound_queue_keeps_control_frames_live_then_closes_with_1013() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket.send(Message::Text("one".into())).await.unwrap();
        websocket.send(Message::Text("two".into())).await.unwrap();
        websocket.send(Message::Ping(b"ping".to_vec().into())).await.unwrap();

        let pong = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(Ok(Message::Pong(payload))) = websocket.next().await {
                    break payload;
                }
            }
        })
        .await
        .expect("server should receive Pong while the app queue is full");
        assert_eq!(pong.as_ref(), b"ping");

        let close = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(Ok(Message::Close(Some(frame)))) = websocket.next().await {
                    break frame;
                }
            }
        })
        .await
        .expect("client should send a bounded-overload Close");
        assert_eq!(close.code, CloseCode::Again);
    });

    let options = WsClientOptions {
        limits: WsSessionLimits {
            inbound_messages: 1,
            inbound_bytes: 4,
            max_inbound_frame_bytes: 4,
            max_inbound_message_bytes: 4,
            no_progress_timeout: std::time::Duration::from_millis(100),
            close_timeout: std::time::Duration::from_secs(1),
            ..WsSessionLimits::default()
        },
        ..WsClientOptions::default()
    };
    let client = WsClient { options };
    let mut session = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.connect(&format!("ws://{address}/")),
    )
    .await
    .unwrap()
    .unwrap();

    peer.await.unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), session.receive())
        .await
        .expect("queued message should remain receivable");
    assert!(
        matches!(first, Some(Ok(WsMessage::Text(ref text))) if text == "one"),
        "unexpected first receive: {first:?}"
    );
    assert!(matches!(session.receive().await, Some(Err(error)) if error.kind() == WsErrorKind::Timeout));
    assert!(session.receive().await.is_none());
    assert!(matches!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::Timeout
        })
    ));
}

#[tokio::test]
async fn local_close_interrupts_a_blocked_inbound_enqueue() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (pong_seen_tx, pong_seen_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket.send(Message::Text("one".into())).await.unwrap();
        websocket.send(Message::Text("two".into())).await.unwrap();
        websocket.send(Message::Ping(b"ping".to_vec().into())).await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(Ok(Message::Pong(payload))) = websocket.next().await {
                    assert_eq!(payload.as_ref(), b"ping");
                    break;
                }
            }
        })
        .await
        .expect("Ping must be processed while enqueue is blocked");
        let _ = pong_seen_tx.send(());

        let _close = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(Ok(Message::Close(frame))) = websocket.next().await {
                    break frame;
                }
            }
        })
        .await
        .expect("local close must interrupt the blocked enqueue");
        websocket
            .flush()
            .await
            .expect("peer should send the Close confirmation");
    });

    let options = WsClientOptions {
        limits: WsSessionLimits {
            inbound_messages: 1,
            inbound_bytes: 4,
            max_inbound_frame_bytes: 4,
            max_inbound_message_bytes: 4,
            close_timeout: std::time::Duration::from_secs(1),
            ..WsSessionLimits::default()
        },
        ..WsClientOptions::default()
    };
    let client = WsClient { options };
    let mut session = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.connect(&format!("ws://{address}/")),
    )
    .await
    .unwrap()
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), pong_seen_rx)
        .await
        .expect("server should observe Pong")
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), session.close())
        .await
        .expect("close must finish while inbound enqueue is blocked")
        .unwrap();
    peer.await.unwrap();
    assert!(matches!(session.receive().await, Some(Ok(WsMessage::Text(text))) if text == "one"));
    assert!(session.receive().await.is_none());
}
