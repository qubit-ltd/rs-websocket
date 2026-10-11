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
    let client = WsClient::builder()
        .limits(limits())
        .build()
        .expect("valid client");
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
    assert_eq!(
        sender.try_send(WsMessage::Text("z".into())),
        Err(WsSendError::Closed)
    );
    peer_join(peer).await;
}

#[tokio::test]
async fn send_waiting_on_capacity_succeeds_after_writer_releases_a_permit() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
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
            max_frame_bytes: 5,
            max_message_bytes: 5,
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
async fn cancelled_send_waiter_does_not_prevent_later_sends() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
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
    peer_join(peer).await;
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
    assert!(matches!(receive_until!(&mut session), Some(Ok(WsMessage::Text(text))) if text == "q"));
    assert!(receive_until!(&mut session).is_none());
    peer_join(peer).await;
}

#[tokio::test]
async fn inbound_full_queue_times_out_with_1013_after_control_frame_progress() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Text("one".into()))
            .await
            .expect("send first");
        websocket
            .send(Message::Text("two".into()))
            .await
            .expect("send blocked message");
        websocket
            .send(Message::Ping(b"live".to_vec().into()))
            .await
            .expect("send Ping");
        loop {
            if let Message::Pong(payload) = tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("Pong deadline")
                .expect("peer socket open")
                .expect("valid frame")
            {
                assert_eq!(payload.as_ref(), b"live");
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
            max_frame_bytes: 4,
            max_message_bytes: 4,
            inbound_messages: 1,
            inbound_bytes: 4,
            outbound_messages: 1,
            outbound_bytes: 4,
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
    assert!(
        matches!(receive_until!(&mut session), Some(Ok(WsMessage::Text(text))) if text == "one")
    );
    assert!(
        matches!(receive_until!(&mut session), Some(Err(error)) if error.kind() == WsErrorKind::Timeout)
    );
    assert!(receive_until!(&mut session).is_none());
    assert_eq!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::Timeout
        })
    );
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
    assert!(receive_until!(&mut session).is_none());
    assert!(matches!(
        session.termination(),
        Some(WsTermination::Error { .. })
    ));
    peer_join(peer).await;
}

#[tokio::test]
async fn last_handle_drop_closes_session_without_waiting() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        loop {
            match tokio::time::timeout(DEADLINE, websocket.next())
                .await
                .expect("Drop closes socket deadline")
            {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
            }
        }
    });
    let client = WsClient::builder().build().expect("valid client");
    let session = tokio::time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    drop(session.sender());
    drop(session);
    peer_join(peer).await;
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
        matches!(receive_until!(&mut session), Some(Err(error)) if error.kind() == WsErrorKind::Timeout)
    );
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
async fn peer_first_close_is_normal_and_repeated_close_is_stable() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer =
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut websocket = accept_async(socket).await.expect("handshake");
            websocket.send(Message::Close(Some(
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                reason: "done".into(),
            },
        ))).await.expect("peer sends Close");
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
    let termination = WsTermination::Normal { code: Some(1000) };
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
async fn cancelled_close_can_be_retried_after_peer_confirmation() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
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
        }).await.expect("observe client Close");
    }
    assert_eq!(
        session.termination(),
        None,
        "cancelled close leaves task manageable"
    );
    confirm_tx
        .send(())
        .expect("peer still waits for confirmation");
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("close retry deadline")
        .expect("retry close succeeds");
    let termination = session.termination().expect("stable terminal snapshot");
    assert!(matches!(termination, WsTermination::Normal { .. }));
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .expect("repeated close deadline")
        .expect("repeated close succeeds");
    assert_eq!(session.termination(), Some(termination));
    peer_join(peer).await;
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
    tokio::time::timeout(DEADLINE, session.close())
        .await
        .unwrap()
        .unwrap();
    peer_join(peer).await;
}

#[tokio::test]
async fn oversized_frame_is_classified_as_frame_too_large() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Frame(Frame::message(
                "12345",
                OpCode::Data(Data::Text),
                true,
            )))
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
            max_frame_bytes: 4,
            max_message_bytes: 8,
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
    assert!(
        matches!(receive_until!(&mut session), Some(Err(error)) if error.kind() == WsErrorKind::FrameTooLarge)
    );
    assert!(receive_until!(&mut session).is_none());
    peer_join(peer).await;
}

#[tokio::test]
async fn oversized_reassembled_message_is_classified_separately() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Frame(Frame::message(
                "ab",
                OpCode::Data(Data::Text),
                false,
            )))
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
            .send(Message::Frame(Frame::message(
                "ef",
                OpCode::Data(Data::Continue),
                true,
            )))
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
            max_frame_bytes: 2,
            max_message_bytes: 4,
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
    assert!(
        matches!(receive_until!(&mut session), Some(Err(error)) if error.kind() == WsErrorKind::MessageTooLarge)
    );
    assert!(receive_until!(&mut session).is_none());
    peer_join(peer).await;
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
        peer_join(peer).await,
        0,
        "cancelled connect releases TCP socket"
    );
}
