use std::time::Duration;

use futures_util::FutureExt;
use futures_util::{SinkExt, StreamExt};
use qubit_websocket::{
    WsClient, WsErrorKind, WsMessage, WsSendError, WsSessionLimits, WsTermination,
};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    accept_async,
    tungstenite::{
        Message,
        protocol::frame::{
            Frame,
            coding::{Data, OpCode},
        },
    },
};

const DEADLINE: Duration = Duration::from_secs(2);

fn limits() -> WsSessionLimits {
    WsSessionLimits {
        max_frame_bytes: 5,
        max_message_bytes: 5,
        inbound_messages: 2,
        inbound_bytes: 5,
        outbound_messages: 2,
        outbound_bytes: 5,
        no_progress_timeout: Duration::from_millis(100),
        close_timeout: Duration::from_millis(200),
    }
}

#[tokio::test]
async fn close_wakes_a_sender_waiting_on_both_outbound_limits() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        accepted_tx.send(()).ok();
        let _ = finish_rx.await;
        while let Some(Ok(message)) = websocket.next().await {
            if matches!(message, Message::Close(_)) {
                websocket.flush().await.expect("flush close reply");
                break;
            }
        }
    });
    let client = WsClient::builder()
        .limits(limits())
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect timeout")
        .expect("connect");
    accepted_rx.await.expect("peer accepted");

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
    assert_eq!(
        sender.try_send(WsMessage::Text("z".into())),
        Err(WsSendError::Closed)
    );
    peer.await.expect("peer exits");
}

#[tokio::test]
async fn cancelled_send_waiter_does_not_prevent_later_sends() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        let _ = release_rx.await;
        while let Some(Ok(message)) = websocket.next().await {
            if matches!(message, Message::Close(_)) {
                websocket.flush().await.expect("flush close reply");
                break;
            }
        }
    });
    let client = WsClient::builder()
        .limits(limits())
        .build()
        .expect("valid client");
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
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sender.try_send(WsMessage::Text("x".into())),
        Err(WsSendError::Closed)
    );
    peer.await.unwrap();
}

#[tokio::test]
async fn ping_is_answered_while_application_is_not_receiving() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket.send(Message::Text("q".into())).await.unwrap();
        websocket
            .send(Message::Ping(b"alive".to_vec().into()))
            .await
            .unwrap();
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
    let client = WsClient::builder()
        .limits(limits())
        .build()
        .expect("valid client");
    let mut session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(session.receive().await, Some(Ok(WsMessage::Text(text))) if text == "q"));
    assert!(session.receive().await.is_none());
    peer.await.unwrap();
}

#[tokio::test]
async fn abrupt_peer_disconnect_delivers_one_error_then_none() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
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
    let first = tokio::time::timeout(DEADLINE, session.receive())
        .await
        .unwrap();
    assert!(matches!(first, Some(Err(error)) if error.kind() == WsErrorKind::Protocol));
    assert!(session.receive().await.is_none());
    assert!(matches!(
        session.termination(),
        Some(WsTermination::Error { .. })
    ));
    peer.await.unwrap();
}

#[tokio::test]
async fn last_handle_drop_closes_session_without_waiting() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let websocket = accept_async(socket).await.expect("handshake");
        drop(websocket);
    });
    let client = WsClient::builder().build().expect("valid client");
    let session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    drop(session.sender());
    drop(session);
    peer.await.unwrap();
}

#[tokio::test]
async fn close_timeout_is_reported_once_and_task_is_stopped() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (close_seen_tx, close_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        loop {
            if let Some(Ok(Message::Close(_))) = websocket.next().await {
                close_seen_tx.send(()).ok();
                let _ = release_rx.await;
                return;
            }
        }
    });
    let configured = WsSessionLimits {
        close_timeout: Duration::from_millis(50),
        ..WsSessionLimits::default()
    };
    let client = WsClient::builder()
        .limits(configured)
        .build()
        .expect("valid client");
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
    assert!(
        matches!(session.receive().await, Some(Err(error)) if error.kind() == WsErrorKind::Timeout)
    );
    assert!(session.receive().await.is_none());
    assert_eq!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::Timeout
        })
    );
    release_tx.send(()).ok();
    peer.await.expect("peer exits");
}

#[tokio::test]
async fn fragmented_text_and_binary_messages_are_reassembled() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Frame(Frame::message(
                "hel",
                OpCode::Data(Data::Text),
                false,
            )))
            .await
            .unwrap();
        websocket
            .send(Message::Frame(Frame::message(
                "lo",
                OpCode::Data(Data::Continue),
                true,
            )))
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
            if let Some(Ok(Message::Close(_))) = websocket.next().await {
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
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .unwrap()
        .unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn stalled_opening_handshake_times_out() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.expect("accept client");
        let _ = release_rx.await;
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
    peer.await.expect("peer exits");
}

#[tokio::test]
async fn cancelling_connect_closes_the_accepted_socket() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept client");
        accepted_tx.send(()).ok();
        let mut byte = [0; 1];
        loop {
            let count = tokio::time::timeout(
                DEADLINE,
                tokio::io::AsyncReadExt::read(&mut socket, &mut byte),
            )
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
    assert!(
        tokio::time::timeout(DEADLINE, connecting)
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(
        peer.await.expect("peer exits"),
        0,
        "cancelled connect releases TCP socket"
    );
}
