// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use futures_util::FutureExt;
use futures_util::SinkExt;
use futures_util::StreamExt;
use qubit_websocket::WsClient;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsMessage;
use qubit_websocket::WsSendError;
use qubit_websocket::WsSessionLimits;
use qubit_websocket::WsTermination;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::Frame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::Data;
use tokio_tungstenite::tungstenite::protocol::frame::coding::OpCode;

const DEADLINE: Duration = Duration::from_secs(2);

async fn peer_join<T>(peer: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(DEADLINE, peer)
        .await
        .expect("peer task deadline")
        .expect("peer task exits")
}

macro_rules! receive_until {
    ($session:expr) => {
        tokio::time::timeout(DEADLINE, $session.receive())
            .await
            .expect("receive deadline")
    };
}

fn limits() -> WsSessionLimits {
    WsSessionLimits {
        max_inbound_frame_bytes: 5,
        max_inbound_message_bytes: 5,
        inbound_messages: 2,
        inbound_bytes: 5,
        outbound_messages: 2,
        outbound_bytes: 5,
        no_progress_timeout: Duration::from_millis(100),
        close_timeout: Duration::from_millis(200),
    }
}

#[tokio::test]
async fn test_close_wakes_a_sender_waiting_on_both_outbound_limits() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        accepted_tx.send(()).ok();
        let _ = tokio::time::timeout(DEADLINE, finish_rx)
            .await
            .expect("peer release deadline");
        while let Some(Ok(message)) = tokio::time::timeout(DEADLINE, websocket.next())
            .await
            .expect("peer read deadline")
        {
            if matches!(message, Message::Close(_)) {
                websocket.flush().await.expect("flush close reply");
                break;
            }
        }
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect timeout")
        .expect("connect");
    tokio::time::timeout(DEADLINE, accepted_rx)
        .await
        .expect("peer accept deadline")
        .expect("peer accepted");

    // A sender held by the test keeps the outbound budget occupied until the
    // session is closed, making the blocked send deterministic.
    let sender = session.sender();
    session.try_send(WsMessage::Text("12345".into())).unwrap();
    session.try_send(WsMessage::Text("".into())).unwrap();
    let blocked_sender = sender.clone();
    let mut waiting = Box::pin(blocked_sender.send(WsMessage::Text("x".into())));
    assert!(
        waiting.as_mut().now_or_never().is_none(),
        "queue is full before the driver runs"
    );
    finish_tx.send(()).expect("release peer");
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("close resolves")
        .expect("close handshake");
    assert_eq!(
        tokio::time::timeout(DEADLINE, waiting.as_mut())
            .await
            .expect("blocked sender wakes"),
        Err(WsSendError::Closed)
    );
    assert_eq!(sender.try_send(WsMessage::Text("z".into())), Err(WsSendError::Closed));
    assert_eq!(
        sender.try_send(WsMessage::Text("123456".into())),
        Err(WsSendError::Closed)
    );
    assert_eq!(
        sender.send(WsMessage::Text("123456".into())).await,
        Err(WsSendError::Closed)
    );
    peer_join(peer).await;
}

#[tokio::test]
async fn test_dropping_session_closes_peer_while_sender_is_retained() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        connected_tx.send(()).ok();
        loop {
            match tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("peer read deadline")
                .expect("peer socket open")
            {
                Ok(Message::Close(_)) => {
                    websocket.flush().await.expect("flush Close reply");
                    break;
                }
                Ok(_) => {}
                other => panic!("expected a live WebSocket, got {other:?}"),
            }
        }
        tokio::time::timeout(DEADLINE, websocket.next())
            .await
            .expect("socket end deadline");
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect timeout")
        .expect("connect");
    let sender = session.sender();
    session
        .try_send(WsMessage::Text("12345".into()))
        .expect("fill count budget");
    session
        .try_send(WsMessage::Text("".into()))
        .expect("fill remaining budget");
    let mut waiting = Box::pin(sender.send(WsMessage::Text("x".into())));
    assert!(waiting.as_mut().now_or_never().is_none(), "sender waits on full queue");
    tokio::time::timeout(DEADLINE, connected_rx)
        .await
        .expect("peer connected")
        .expect("connection signal");

    drop(session);

    assert_eq!(
        tokio::time::timeout(DEADLINE, waiting.as_mut())
            .await
            .expect("blocked send wakes"),
        Err(WsSendError::Closed)
    );
    assert_eq!(sender.try_send(WsMessage::Text("x".into())), Err(WsSendError::Closed));
    assert_eq!(sender.send(WsMessage::Text("x".into())).await, Err(WsSendError::Closed));
    assert_eq!(
        sender.try_send(WsMessage::Text("123456".into())),
        Err(WsSendError::Closed)
    );
    assert_eq!(
        sender.send(WsMessage::Text("123456".into())).await,
        Err(WsSendError::Closed)
    );
    peer_join(peer).await;
}

#[tokio::test]
async fn test_dropping_session_keeps_reader_alive_for_peer_close_after_data() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (send_data_tx, send_data_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        let _ = tokio::time::timeout(DEADLINE, send_data_rx)
            .await
            .expect("send data signal");
        websocket
            .send(Message::Text("discard me".into()))
            .await
            .expect("send data during close");
        loop {
            match tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("peer read deadline")
                .expect("peer socket open")
            {
                Ok(Message::Close(_)) => {
                    websocket.flush().await.expect("flush Close reply");
                    break;
                }
                Ok(_) => {}
                other => panic!("expected Close after data, got {other:?}"),
            }
        }
        tokio::time::timeout(DEADLINE, websocket.next())
            .await
            .expect("socket end deadline");
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect timeout")
        .expect("connect");
    let sender = session.sender();

    drop(session);
    send_data_tx.send(()).expect("signal peer");

    assert_eq!(sender.try_send(WsMessage::Text("x".into())), Err(WsSendError::Closed));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_send_waiting_on_capacity_succeeds_after_writer_releases_a_permit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (first_seen_tx, first_seen_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        assert!(matches!(
            tokio::time::timeout(DEADLINE, websocket.next())
                .await.expect("first message deadline").expect("peer socket open"),
            Ok(Message::Text(text)) if text == "first"
        ));
        first_seen_tx.send(()).ok();
        loop {
            if let Message::Close(_) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("close deadline")
                .expect("peer socket open")
                .expect("valid frame")
            {
                websocket.flush().await.expect("flush close reply");
                break;
            }
        }
    });
    let client = WsClient::builder()
        .limits(WsSessionLimits {
            max_inbound_frame_bytes: 5,
            max_inbound_message_bytes: 5,
            inbound_messages: 1,
            inbound_bytes: 5,
            outbound_messages: 1,
            outbound_bytes: 5,
            ..WsSessionLimits::default()
        })
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    session
        .try_send(WsMessage::Text("first".into()))
        .expect("fill outbound budget");
    let sender = session.sender();
    let mut waiting = Box::pin(sender.send(WsMessage::Text("next".into())));
    assert!(
        waiting.as_mut().now_or_never().is_none(),
        "second send waits for capacity"
    );
    assert_eq!(
        tokio::time::timeout(DEADLINE, waiting.as_mut())
            .await
            .expect("send capacity deadline"),
        Ok(())
    );
    tokio::time::timeout(DEADLINE, first_seen_rx)
        .await
        .expect("peer receives first message")
        .expect("peer notification");
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("close deadline")
        .expect("close succeeds");
    peer_join(peer).await;
}

#[tokio::test]
async fn test_outbound_payload_uses_outbound_limit_independently_of_inbound_limits() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        while let Some(Ok(message)) = tokio::time::timeout(DEADLINE, websocket.next())
            .await
            .expect("peer read deadline")
        {
            if matches!(message, Message::Close(_)) {
                websocket.flush().await.expect("flush close reply");
                break;
            }
        }
    });
    let client = WsClient::builder()
        .limits(WsSessionLimits {
            max_inbound_frame_bytes: 2,
            max_inbound_message_bytes: 2,
            inbound_bytes: 2,
            outbound_bytes: 8,
            ..WsSessionLimits::default()
        })
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    session
        .try_send(WsMessage::Text("12345678".into()))
        .expect("outbound budget accepts payload above inbound limits");
    assert_eq!(
        session.try_send(WsMessage::Text("123456789".into())),
        Err(WsSendError::MessageTooLarge)
    );

    tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("close deadline")
        .expect("close succeeds");
    peer_join(peer).await;
}

#[tokio::test]
async fn test_cancelled_send_waiter_does_not_prevent_later_sends() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        let _ = tokio::time::timeout(DEADLINE, release_rx)
            .await
            .expect("peer release deadline");
        while let Some(Ok(message)) = tokio::time::timeout(DEADLINE, websocket.next())
            .await
            .expect("peer read deadline")
        {
            if matches!(message, Message::Close(_)) {
                websocket.flush().await.expect("flush close reply");
                break;
            }
        }
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    session.try_send(WsMessage::Text("12345".into())).unwrap();
    session.try_send(WsMessage::Text("".into())).unwrap();
    let sender = session.sender();
    let mut waiting = Box::pin(sender.send(WsMessage::Text("x".into())));
    assert!(
        waiting.as_mut().now_or_never().is_none(),
        "queue is full before the driver runs"
    );
    drop(waiting);
    release_tx.send(()).unwrap();
    tokio::time::timeout(DEADLINE, session.close()).await.unwrap().unwrap();
    assert_eq!(sender.try_send(WsMessage::Text("x".into())), Err(WsSendError::Closed));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_ping_is_answered_while_application_is_not_receiving() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket.send(Message::Text("q".into())).await.unwrap();
        websocket.send(Message::Ping(b"alive".to_vec().into())).await.unwrap();
        loop {
            if let Message::Pong(payload) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("Pong deadline")
                .expect("connection remains open")
                .expect("valid WebSocket frame")
            {
                assert_eq!(payload.as_ref(), b"alive");
                break;
            }
        }
        loop {
            if let Message::Close(_) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("Close deadline")
                .expect("connection remains open")
                .expect("valid WebSocket frame")
            {
                websocket.flush().await.unwrap();
                break;
            }
        }
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::timeout(DEADLINE, session.close()).await.unwrap().unwrap();
    assert!(matches!(receive_until!(&mut session), Some(Ok(WsMessage::Text(text))) if text == "q"));
    assert!(receive_until!(&mut session).is_none());
    peer_join(peer).await;
}

#[tokio::test]
async fn test_inbound_full_queue_times_out_with_1013_after_control_frame_progress() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket.send(Message::Text("four".into())).await.expect("send first");
        websocket
            .send(Message::Text("x".into()))
            .await
            .expect("send blocked message");
        websocket
            .send(Message::Ping(vec![b'p'; 125].into()))
            .await
            .expect("send Ping");
        loop {
            if let Message::Pong(payload) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("Pong deadline")
                .expect("peer socket open")
                .expect("valid frame")
            {
                assert_eq!(payload.as_ref(), vec![b'p'; 125].as_slice());
                break;
            }
        }
        loop {
            if let Message::Close(frame) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("overload Close deadline")
                .expect("peer socket open")
                .expect("valid frame")
            {
                assert_eq!(frame.map(|frame| u16::from(frame.code)), Some(1013));
                break;
            }
        }
    });
    let client = WsClient::builder()
        .limits(WsSessionLimits {
            max_inbound_frame_bytes: 125,
            max_inbound_message_bytes: 125,
            inbound_messages: 1,
            inbound_bytes: 125,
            outbound_messages: 1,
            outbound_bytes: 1,
            no_progress_timeout: Duration::from_millis(100),
            close_timeout: Duration::from_secs(1),
        })
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    peer_join(peer).await;
    assert!(matches!(receive_until!(&mut session), Some(Ok(WsMessage::Text(text))) if text == "four"));
    assert!(matches!(receive_until!(&mut session), Some(Err(error)) if error.kind() == WsErrorKind::Timeout));
    assert!(receive_until!(&mut session).is_none());
    assert_eq!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::Timeout
        })
    );
}

#[tokio::test]
async fn test_abrupt_peer_disconnect_delivers_one_error_then_none() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let websocket = accept_async(socket).await.expect("handshake");
        drop(websocket);
    });
    let client = WsClient::builder()
        .limits(WsSessionLimits {
            close_timeout: Duration::from_millis(50),
            ..WsSessionLimits::default()
        })
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    let first = tokio::time::timeout(DEADLINE, session.receive()).await.unwrap();
    assert!(matches!(first, Some(Err(error)) if error.kind() == WsErrorKind::Protocol));
    assert!(receive_until!(&mut session).is_none());
    assert!(matches!(session.termination(), Some(WsTermination::Error { .. })));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_last_handle_drop_closes_session_without_waiting() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        let mut close_seen = false;
        loop {
            match tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("Drop closes socket deadline")
            {
                None | Some(Err(_)) => break,
                Some(Ok(Message::Close(_))) => {
                    close_seen = true;
                    websocket.flush().await.expect("reply to Close");
                }
                Some(Ok(_)) => {}
            }
        }
        close_seen
    });
    let client = WsClient::builder().build().expect("valid client");
    let session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    drop(session.sender());
    drop(session);
    assert!(peer_join(peer).await, "peer must observe Close before socket shutdown");
}

#[tokio::test]
async fn test_close_timeout_is_reported_once_and_task_is_stopped() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (close_seen_tx, close_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        loop {
            if let Some(Ok(Message::Close(_))) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("peer Close deadline")
            {
                close_seen_tx.send(()).ok();
                let _ = tokio::time::timeout(DEADLINE, release_rx)
                    .await
                    .expect("peer release deadline");
                loop {
                    match tokio::time::timeout(DEADLINE, websocket.next())
                        .await
                        .expect("closed socket deadline")
                    {
                        None | Some(Err(_)) => break,
                        Some(Ok(_)) => {}
                    }
                }
                return;
            }
        }
    });
    let configured = WsSessionLimits {
        close_timeout: Duration::from_millis(50),
        ..WsSessionLimits::default()
    };
    let client = WsClient::builder().limits(configured).build().expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    let close_result = tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("bounded close must resolve");
    assert!(matches!(close_result, Err(error) if error.kind() == WsErrorKind::Timeout));
    tokio::time::timeout(DEADLINE, close_seen_rx)
        .await
        .expect("peer saw close")
        .unwrap();
    assert!(matches!(receive_until!(&mut session), Some(Err(error)) if error.kind() == WsErrorKind::Timeout));
    assert!(receive_until!(&mut session).is_none());
    assert_eq!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::Timeout
        })
    );
    release_tx.send(()).ok();
    peer_join(peer).await;
}

#[tokio::test]
async fn test_peer_first_close_is_closed_and_repeated_close_is_stable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Close(Some(
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                    reason: "done".into(),
                },
            )))
            .await
            .expect("peer sends Close");
        while let Some(frame) = tokio::time::timeout(DEADLINE, websocket.next())
            .await
            .expect("client Close deadline")
        {
            match frame {
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });
    let client = WsClient::builder().build().expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    assert!(receive_until!(&mut session).is_none());
    let termination = WsTermination::Closed { code: Some(1000) };
    assert_eq!(session.termination(), Some(termination));
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("close deadline")
        .expect("close after peer close");
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("repeated close deadline")
        .expect("repeated close");
    assert_eq!(session.termination(), Some(termination));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_peer_close_1009_is_preserved_in_public_termination() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Close(Some(
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Size,
                    reason: "too large".into(),
                },
            )))
            .await
            .expect("peer sends 1009 Close");
        while let Some(frame) = tokio::time::timeout(DEADLINE, websocket.next())
            .await
            .expect("client Close deadline")
        {
            match frame {
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });
    let client = WsClient::builder().build().expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    assert!(receive_until!(&mut session).is_none());
    assert_eq!(session.termination(), Some(WsTermination::Closed { code: Some(1009) }));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_peer_close_while_inbound_message_waits_for_capacity_is_preserved() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (close_confirmed_tx, close_confirmed_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Text("one".into()))
            .await
            .expect("send queued message");
        websocket
            .send(Message::Text("two".into()))
            .await
            .expect("send message waiting for capacity");
        websocket
            .send(Message::Ping(b"probe".to_vec().into()))
            .await
            .expect("send probe Ping while enqueue is blocked");
        tokio::time::timeout(DEADLINE, async {
            loop {
                if let Some(Ok(Message::Pong(payload))) = websocket.next().await {
                    assert_eq!(payload.as_ref(), b"probe");
                    break;
                }
            }
        })
        .await
        .expect("reader should process Ping while a message waits for capacity");
        websocket
            .send(Message::Close(Some(
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: CloseCode::Size,
                    reason: "peer stop".into(),
                },
            )))
            .await
            .expect("send peer Close while enqueue is blocked");

        while let Some(frame) = tokio::time::timeout(DEADLINE, websocket.next())
            .await
            .expect("client Close deadline")
        {
            match frame {
                Ok(Message::Close(_)) | Err(_) => {
                    close_confirmed_tx.send(()).expect("test waits for Close confirmation");
                    break;
                }
                Ok(_) => {}
            }
        }
    });
    let client = WsClient::builder()
        .limits(WsSessionLimits {
            inbound_messages: 1,
            inbound_bytes: 128,
            max_inbound_frame_bytes: 128,
            max_inbound_message_bytes: 128,
            no_progress_timeout: std::time::Duration::from_secs(30),
            close_timeout: std::time::Duration::from_secs(1),
            ..WsSessionLimits::default()
        })
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    tokio::time::timeout(DEADLINE, close_confirmed_rx)
        .await
        .expect("peer Close confirmation deadline")
        .expect("peer confirmed the Close");
    assert!(matches!(
        receive_until!(&mut session),
        Some(Ok(WsMessage::Text(text))) if text == "one"
    ));
    let close_result = receive_until!(&mut session);
    assert!(
        close_result.is_none(),
        "expected successful peer close, received {close_result:?} with termination {:?}",
        session.termination()
    );
    assert_eq!(session.termination(), Some(WsTermination::Closed { code: Some(1009) }));
    peer_join(peer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_local_close_waits_for_server_confirmation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (close_seen_tx, close_seen_rx) = tokio::sync::oneshot::channel();
    let (confirm_tx, confirm_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        loop {
            match tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("client frame deadline")
            {
                Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
                Some(Err(error)) => panic!("peer socket failed before Close: {error}"),
                None => panic!("peer socket ended before client Close"),
            }
        }
        close_seen_tx.send(()).expect("test still observes client Close");
        confirm_rx.await.expect("test releases server confirmation");
        websocket.flush().await.expect("flush server Close confirmation");
    });
    let client = WsClient::builder().build().expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    {
        let closing = session.close();
        tokio::pin!(closing);
        tokio::time::timeout(DEADLINE, async {
            tokio::select! {
                result = &mut closing => panic!("close completed before server confirmation: {result:?}"),
                result = close_seen_rx => result.expect("server observes client Close"),
            }
        })
        .await
        .expect("server Close observation deadline");
        confirm_tx.send(()).expect("server still waits for confirmation gate");
        tokio::time::timeout(DEADLINE, closing)
            .await
            .expect("close confirmation deadline")
            .expect("close handshake succeeds");
    }
    assert!(matches!(session.termination(), Some(WsTermination::Closed { .. })));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_cancelled_close_can_be_retried_after_peer_confirmation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (close_seen_tx, close_seen_rx) = tokio::sync::oneshot::channel();
    let (confirm_tx, confirm_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        loop {
            match tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("client frame deadline")
            {
                Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
                Some(Err(error)) => panic!("peer socket failed before Close: {error}"),
                None => panic!("peer socket ended before client Close"),
            }
        }
        close_seen_tx.send(()).ok();
        let _ = tokio::time::timeout(DEADLINE, confirm_rx)
            .await
            .expect("peer confirmation gate deadline");
        websocket.flush().await.expect("flush peer confirmation");
    });
    let client = WsClient::builder()
        .limits(WsSessionLimits {
            close_timeout: Duration::from_millis(500),
            ..WsSessionLimits::default()
        })
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    {
        let closing = session.close();
        tokio::pin!(closing);
        tokio::time::timeout(DEADLINE, async {
            tokio::select! {
                _ = close_seen_rx => {},
                result = &mut closing => panic!("close should wait for peer confirmation: {result:?}"),
            }
        })
        .await
        .expect("observe client Close");
    }
    assert_eq!(session.termination(), None, "cancelled close leaves task manageable");
    confirm_tx.send(()).expect("peer still waits for confirmation");
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("close retry deadline")
        .expect("retry close succeeds");
    let termination = session.termination().expect("stable terminal snapshot");
    assert!(matches!(termination, WsTermination::Closed { .. }));
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("repeated close deadline")
        .expect("repeated close succeeds");
    assert_eq!(session.termination(), Some(termination));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_fragmented_text_and_binary_messages_are_reassembled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Frame(Frame::message("hel", OpCode::Data(Data::Text), false)))
            .await
            .unwrap();
        websocket
            .send(Message::Frame(Frame::message("lo", OpCode::Data(Data::Continue), true)))
            .await
            .unwrap();
        websocket
            .send(Message::Frame(Frame::message(
                &b"\x00\x01"[..],
                OpCode::Data(Data::Binary),
                false,
            )))
            .await
            .unwrap();
        websocket
            .send(Message::Frame(Frame::message(
                &b"\x02\x03"[..],
                OpCode::Data(Data::Continue),
                true,
            )))
            .await
            .unwrap();
        loop {
            if let Some(Ok(Message::Close(_))) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("peer Close deadline")
            {
                websocket.flush().await.unwrap();
                break;
            }
        }
    });
    let client = WsClient::builder().build().expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    assert!(
        matches!(tokio::time::timeout(DEADLINE, session.receive()).await.unwrap(), Some(Ok(WsMessage::Text(text))) if text == "hello")
    );
    assert!(
        matches!(tokio::time::timeout(DEADLINE, session.receive()).await.unwrap(), Some(Ok(WsMessage::Binary(bytes))) if bytes.as_ref() == b"\x00\x01\x02\x03")
    );
    tokio::time::timeout(DEADLINE, session.close()).await.unwrap().unwrap();
    peer_join(peer).await;
}

#[tokio::test]
async fn test_oversized_frame_is_classified_as_frame_too_large() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Frame(Frame::message("12345", OpCode::Data(Data::Text), true)))
            .await
            .expect("send oversized frame");
        loop {
            if let Message::Close(_) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("close deadline")
                .expect("peer socket open")
                .expect("valid frame")
            {
                websocket.flush().await.expect("flush close reply");
                break;
            }
        }
    });
    let client = WsClient::builder()
        .limits(WsSessionLimits {
            max_inbound_frame_bytes: 4,
            max_inbound_message_bytes: 8,
            inbound_messages: 2,
            inbound_bytes: 8,
            outbound_messages: 2,
            outbound_bytes: 8,
            close_timeout: Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    assert!(matches!(receive_until!(&mut session), Some(Err(error)) if error.kind() == WsErrorKind::FrameTooLarge));
    assert!(receive_until!(&mut session).is_none());
    peer_join(peer).await;
}

#[tokio::test]
async fn test_oversized_reassembled_message_is_classified_separately() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Frame(Frame::message("ab", OpCode::Data(Data::Text), false)))
            .await
            .expect("send initial fragment");
        websocket
            .send(Message::Frame(Frame::message(
                "cd",
                OpCode::Data(Data::Continue),
                false,
            )))
            .await
            .expect("send middle fragment");
        websocket
            .send(Message::Frame(Frame::message("ef", OpCode::Data(Data::Continue), true)))
            .await
            .expect("send oversized continuation");
        loop {
            if let Message::Close(_) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("close deadline")
                .expect("peer socket open")
                .expect("valid frame")
            {
                websocket.flush().await.expect("flush close reply");
                break;
            }
        }
    });
    let client = WsClient::builder()
        .limits(WsSessionLimits {
            max_inbound_frame_bytes: 2,
            max_inbound_message_bytes: 4,
            inbound_messages: 1,
            inbound_bytes: 4,
            outbound_messages: 1,
            outbound_bytes: 4,
            close_timeout: Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    assert!(matches!(receive_until!(&mut session), Some(Err(error)) if error.kind() == WsErrorKind::MessageTooLarge));
    assert!(receive_until!(&mut session).is_none());
    peer_join(peer).await;
}

#[tokio::test]
async fn test_stalled_opening_handshake_times_out() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.expect("accept client");
        let _ = tokio::time::timeout(DEADLINE, release_rx)
            .await
            .expect("peer release deadline");
    });
    let client = WsClient::builder()
        .handshake_timeout(Duration::from_millis(50))
        .build()
        .expect("valid client");
    let error = match tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
    {
        Ok(_) => panic!("stalled handshake must time out"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::Timeout);
    release_tx.send(()).ok();
    peer_join(peer).await;
}

#[tokio::test]
async fn test_cancelling_connect_closes_the_accepted_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept client");
        accepted_tx.send(()).ok();
        let mut byte = [0; 1];
        loop {
            let count = tokio::time::timeout(DEADLINE, tokio::io::AsyncReadExt::read(&mut socket, &mut byte))
                .await
                .expect("socket release deadline")
                .expect("read client data");
            if count == 0 {
                break count;
            }
        }
    });
    let client = WsClient::builder().build().expect("valid client");
    let connecting = tokio::spawn(async move { client.connect(&format!("ws://{address}/")).await });
    tokio::time::timeout(DEADLINE, accepted_rx)
        .await
        .expect("peer accepted connection")
        .unwrap();
    connecting.abort();
    assert!(tokio::time::timeout(DEADLINE, connecting).await.unwrap().is_err());
    assert_eq!(peer_join(peer).await, 0, "cancelled connect releases TCP socket");
}

// Public session contracts migrated from the former crate-local test module.
#[tokio::test]
async fn test_sends_and_receives_echo_before_close() {
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
async fn test_abrupt_tcp_eof_is_delivered_as_a_terminal_error_once() {
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
async fn test_explicit_peer_close_completes_the_termination() {
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
async fn test_oversized_single_frame_reports_frame_too_large() {
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
async fn test_oversized_reassembled_message_reports_message_too_large() {
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
async fn test_no_progress_timeout_flushes_1013_close_after_queue_becomes_full() {
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
async fn test_cancelled_close_can_be_retried_to_stable_termination() {
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
async fn test_extra_inbound_data_reports_backpressure_and_closes_with_1013() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket.send(Message::Text("one".into())).await.unwrap();
        websocket.send(Message::Text("two".into())).await.unwrap();
        websocket.send(Message::Text("tri".into())).await.unwrap();

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

    let client = WsClient::builder()
        .limits(WsSessionLimits {
            inbound_messages: 1,
            inbound_bytes: 4,
            max_inbound_frame_bytes: 4,
            max_inbound_message_bytes: 4,
            no_progress_timeout: std::time::Duration::from_secs(30),
            close_timeout: std::time::Duration::from_secs(1),
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

    peer.await.unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), session.receive())
        .await
        .expect("queued message should remain receivable");
    assert!(
        matches!(first, Some(Ok(WsMessage::Text(ref text))) if text == "one"),
        "unexpected first receive: {first:?}"
    );
    assert!(matches!(session.receive().await, Some(Err(error)) if error.kind() == WsErrorKind::InboundBackpressure));
    assert!(session.receive().await.is_none());
    assert!(matches!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::InboundBackpressure
        })
    ));
}

#[tokio::test]
async fn test_local_close_interrupts_a_blocked_inbound_enqueue() {
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

    let client = WsClient::builder()
        .limits(WsSessionLimits {
            inbound_messages: 1,
            inbound_bytes: 4,
            max_inbound_frame_bytes: 4,
            max_inbound_message_bytes: 4,
            close_timeout: std::time::Duration::from_secs(1),
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
