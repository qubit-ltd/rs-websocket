// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::SinkExt;
use futures_util::StreamExt;
use qubit_websocket::WsClient;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsMessage;
use qubit_websocket::WsSendError;
use qubit_websocket::WsSessionLimits;
use qubit_websocket::WsTermination;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::task::yield_now;
use tokio::time;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::protocol::frame::Frame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::Data;
use tokio_tungstenite::tungstenite::protocol::frame::coding::OpCode;

mod session;

const DEADLINE: Duration = Duration::from_secs(2);

async fn peer_join<T>(peer: JoinHandle<T>) -> T {
    time::timeout(DEADLINE, peer)
        .await
        .expect("peer task deadline")
        .expect("peer task exits")
}

#[derive(Debug)]
enum PeerReadError {
    Eof,
    Deadline,
    WebSocket(TungsteniteError),
}

async fn peer_message<S>(websocket: &mut WebSocketStream<S>) -> Result<Message, PeerReadError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match websocket.next().await {
        Some(Ok(message)) => Ok(message),
        Some(Err(error)) => Err(PeerReadError::WebSocket(error)),
        None => Err(PeerReadError::Eof),
    }
}

async fn peer_close<S>(websocket: &mut WebSocketStream<S>) -> Result<(), PeerReadError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = Instant::now() + DEADLINE;
    let wait = async {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(PeerReadError::Deadline);
            }
            let message = match time::timeout(remaining, peer_message(websocket)).await {
                Ok(result) => result?,
                Err(_) => return Err(PeerReadError::Deadline),
            };
            if let Message::Close(_) = message {
                return Ok(());
            }
        }
    };
    wait.await
}

async fn peer_pong<S>(websocket: &mut WebSocketStream<S>) -> Result<Bytes, PeerReadError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = Instant::now() + DEADLINE;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(PeerReadError::Deadline);
        }
        let message = match time::timeout(remaining, peer_message(websocket)).await {
            Ok(result) => result?,
            Err(_) => return Err(PeerReadError::Deadline),
        };
        if let Message::Pong(payload) = message {
            return Ok(payload);
        }
    }
}

#[tokio::test]
async fn test_peer_message_fails_promptly_on_eof() {
    if std::env::var_os("WS_PEER_EOF_PROBE").is_some() {
        let (local, remote) = tokio::io::duplex(64);
        drop(remote);
        let mut websocket = WebSocketStream::from_raw_socket(local, Role::Server, None).await;
        match peer_message(&mut websocket).await {
            Err(PeerReadError::Eof) => {}
            Err(PeerReadError::Deadline) => panic!("peer_message has no deadline"),
            Err(PeerReadError::WebSocket(error)) => {
                assert!(matches!(error, TungsteniteError::Protocol(_) | TungsteniteError::Io(_)));
            }
            Ok(message) => panic!("received unexpected frame instead of EOF: {message:?}"),
        }
        return;
    }

    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .arg("--exact")
        .arg("test_peer_message_fails_promptly_on_eof")
        .arg("--nocapture")
        .env("WS_PEER_EOF_PROBE", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start EOF probe child");
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll EOF probe child") {
            let output = child.wait_with_output().expect("collect EOF probe output");
            assert!(
                status.success(),
                "EOF probe returned an unexpected error: {status}\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().expect("kill stuck EOF probe child");
    let output = child.wait_with_output().expect("collect stuck EOF probe output");
    panic!(
        "peer frame wait did not finish after EOF\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn test_peer_waits_fail_promptly_on_eof_and_error() {
    const PROBE_ENV: &str = "WS_PEER_WAIT_PROBE";
    if let Some(probe) = std::env::var_os(PROBE_ENV) {
        let probe = probe.to_string_lossy();
        let (local, remote) = tokio::io::duplex(64);
        let mut websocket = WebSocketStream::from_raw_socket(local, Role::Server, None).await;
        match probe.as_ref() {
            "close_eof" => {
                drop(remote);
                assert!(matches!(
                    peer_close(&mut websocket).await,
                    Err(PeerReadError::Eof | PeerReadError::WebSocket(_))
                ));
            }
            "close_read_error" => {
                let mut remote = remote;
                remote.write_all(&[0x83, 0x00]).await.expect("write invalid frame");
                assert!(matches!(
                    peer_close(&mut websocket).await,
                    Err(PeerReadError::WebSocket(_))
                ));
            }
            "close_unrelated" | "pong_unrelated" => {
                let wait_for_pong = probe.starts_with("pong_");
                let mut remote = WebSocketStream::from_raw_socket(remote, Role::Client, None).await;
                let sender = spawn(async move {
                    loop {
                        remote
                            .send(if wait_for_pong {
                                Message::Text("still alive".into())
                            } else {
                                Message::Ping(Bytes::from_static(b"still alive"))
                            })
                            .await
                            .expect("send unrelated Ping");
                        yield_now().await;
                    }
                });
                let result = if wait_for_pong {
                    peer_pong(&mut websocket).await.map(|_| ())
                } else {
                    peer_close(&mut websocket).await
                };
                sender.abort();
                assert!(matches!(result, Err(PeerReadError::Deadline)));
            }
            "close_silent" => {
                let _remote = remote;
                assert!(matches!(peer_close(&mut websocket).await, Err(PeerReadError::Deadline)));
            }
            "pong_eof" => {
                drop(remote);
                assert!(matches!(
                    peer_pong(&mut websocket).await,
                    Err(PeerReadError::Eof | PeerReadError::WebSocket(_))
                ));
            }
            "pong_read_error" => {
                let mut remote = remote;
                remote.write_all(&[0x83, 0x00]).await.expect("write invalid frame");
                assert!(matches!(
                    peer_pong(&mut websocket).await,
                    Err(PeerReadError::WebSocket(_))
                ));
            }
            "pong_silent" => {
                let _remote = remote;
                assert!(matches!(peer_pong(&mut websocket).await, Err(PeerReadError::Deadline)));
            }
            other => panic!("unknown Close probe: {other}"),
        }
        return;
    }

    for probe in [
        "close_eof",
        "close_read_error",
        "close_unrelated",
        "close_silent",
        "pong_eof",
        "pong_read_error",
        "pong_unrelated",
        "pong_silent",
    ] {
        let mut child = Command::new(std::env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg("test_peer_waits_fail_promptly_on_eof_and_error")
            .arg("--nocapture")
            .env(PROBE_ENV, probe)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start Close probe child");
        let deadline = Instant::now() + Duration::from_secs(3);
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll Close probe child") {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().expect("kill stuck Close probe child");
                let output = child.wait_with_output().expect("collect stuck Close probe output");
                panic!(
                    "{probe} peer wait probe exceeded its wall-clock deadline\nstdout: {}\nstderr: {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let output = child.wait_with_output().expect("collect Close probe output");
        assert!(
            status.success(),
            "{probe} peer wait probe failed: {status}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn test_control_traffic_does_not_extend_inbound_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let websocket = accept_async(socket).await.expect("handshake");
        let (mut sink, mut stream) = websocket.split();
        sink.send(Message::Text("queued".into()))
            .await
            .expect("queue first message");
        sink.send(Message::Text("waiting".into()))
            .await
            .expect("send waiting message");

        let started = Instant::now();
        let mut sent = 0usize;
        let mut pongs = 0usize;
        let mut send_errors = 0usize;
        let mut last_pong_elapsed = Duration::ZERO;
        let mut interval = time::interval(Duration::from_millis(1));
        let close = loop {
            tokio::select! {
                biased;
                frame = stream.next() => match frame {
                    Some(Ok(Message::Pong(_))) => {
                        pongs += 1;
                        last_pong_elapsed = started.elapsed();
                    }
                    Some(Ok(Message::Close(frame))) => break frame,
                    Some(Ok(_)) => {},
                    Some(Err(error)) => panic!("read client control response: {error}"),
                    None => panic!("client socket ended before slow-consumer Close"),
                },
                _ = interval.tick(), if send_errors == 0 => {
                    match sink.send(Message::Ping(sent.to_be_bytes().to_vec().into())).await {
                        Ok(()) => sent += 1,
                        Err(_) => send_errors += 1,
                    }
                },
            }
        };
        (sent, pongs, send_errors, last_pong_elapsed, close, started.elapsed())
    });
    let no_progress_timeout = Duration::from_millis(50);
    let client = WsClient::builder()
        .limits(WsSessionLimits {
            max_inbound_frame_bytes: 16,
            max_inbound_message_bytes: 16,
            inbound_messages: 1,
            inbound_bytes: 16,
            no_progress_timeout,
            close_timeout: Duration::from_secs(1),
            ..WsSessionLimits::default()
        })
        .build()
        .expect("valid client");
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    let (sent, pongs, send_errors, last_pong_elapsed, close, elapsed) = peer_join(peer).await;
    assert!(sent > 1, "peer sent {sent} Ping frames");
    assert!(pongs > 0, "peer observed {pongs} Pong frames");
    assert!(
        last_pong_elapsed >= no_progress_timeout * 3 / 4,
        "control traffic stopped too early: last Pong at {last_pong_elapsed:?}, sent={sent}, pongs={pongs}"
    );
    assert!(
        matches!(close, Some(ref frame) if frame.code == CloseCode::Again),
        "peer received slow-consumer Close: {close:?}"
    );
    eprintln!(
        "loopback control probe: sent={sent}, pongs={pongs}, send_errors={send_errors}, last_pong={last_pong_elapsed:?}, elapsed={elapsed:?}"
    );

    assert!(matches!(
        time::timeout(DEADLINE, session.receive()).await.expect("queued message deadline"),
        Some(Ok(WsMessage::Text(text))) if text == "queued"
    ));
    assert!(matches!(
        time::timeout(DEADLINE, session.receive()).await.expect("timeout terminal deadline"),
        Some(Err(error)) if error.kind() == WsErrorKind::Timeout
    ));
    assert_eq!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::Timeout
        })
    );
}

macro_rules! receive_until {
    ($session:expr) => {
        time::timeout(DEADLINE, $session.receive())
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
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        accepted_tx.send(()).ok();
        let _ = time::timeout(DEADLINE, finish_rx).await.expect("peer release deadline");
        while let Some(Ok(message)) = time::timeout(DEADLINE, websocket.next())
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect timeout")
        .expect("connect");
    time::timeout(DEADLINE, accepted_rx)
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
    time::timeout(DEADLINE, session.close())
        .await
        .expect("close resolves")
        .expect("close handshake");
    assert_eq!(
        time::timeout(DEADLINE, waiting.as_mut())
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
    let (connected_tx, connected_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        connected_tx.send(()).ok();
        loop {
            match time::timeout(DEADLINE, websocket.next())
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
        time::timeout(DEADLINE, websocket.next())
            .await
            .expect("socket end deadline");
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
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
    time::timeout(DEADLINE, connected_rx)
        .await
        .expect("peer connected")
        .expect("connection signal");

    drop(session);

    assert_eq!(
        time::timeout(DEADLINE, waiting.as_mut())
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
    let (send_data_tx, send_data_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        let _ = time::timeout(DEADLINE, send_data_rx).await.expect("send data signal");
        websocket
            .send(Message::Text("discard me".into()))
            .await
            .expect("send data during close");
        loop {
            match time::timeout(DEADLINE, websocket.next())
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
        time::timeout(DEADLINE, websocket.next())
            .await
            .expect("socket end deadline");
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
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
    let (first_seen_tx, first_seen_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        assert!(matches!(
            time::timeout(DEADLINE, websocket.next())
                .await.expect("first message deadline").expect("peer socket open"),
            Ok(Message::Text(text)) if text == "first"
        ));
        first_seen_tx.send(()).ok();
        peer_close(&mut websocket).await.expect("peer receives Close");
        websocket.flush().await.expect("flush close reply");
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
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
        time::timeout(DEADLINE, waiting.as_mut())
            .await
            .expect("send capacity deadline"),
        Ok(())
    );
    time::timeout(DEADLINE, first_seen_rx)
        .await
        .expect("peer receives first message")
        .expect("peer notification");
    time::timeout(DEADLINE, session.close())
        .await
        .expect("close deadline")
        .expect("close succeeds");
    peer_join(peer).await;
}

#[tokio::test]
async fn test_outbound_payload_uses_outbound_limit_independently_of_inbound_limits() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        while let Some(Ok(message)) = time::timeout(DEADLINE, websocket.next())
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
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

    time::timeout(DEADLINE, session.close())
        .await
        .expect("close deadline")
        .expect("close succeeds");
    peer_join(peer).await;
}

#[tokio::test]
async fn test_cancelled_send_waiter_does_not_prevent_later_sends() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (release_tx, release_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        let _ = time::timeout(DEADLINE, release_rx)
            .await
            .expect("peer release deadline");
        while let Some(Ok(message)) = time::timeout(DEADLINE, websocket.next())
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
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
    time::timeout(DEADLINE, session.close()).await.unwrap().unwrap();
    assert_eq!(sender.try_send(WsMessage::Text("x".into())), Err(WsSendError::Closed));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_ping_is_answered_while_application_is_not_receiving() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (pong_seen_tx, pong_seen_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket.send(Message::Text("q".into())).await.unwrap();
        websocket.send(Message::Ping(b"alive".to_vec().into())).await.unwrap();
        let payload = peer_pong(&mut websocket).await.expect("peer receives Pong");
        assert_eq!(payload.as_ref(), b"alive");
        pong_seen_tx.send(()).expect("test waits for Pong confirmation");
        peer_close(&mut websocket).await.expect("peer receives Close");
        websocket.flush().await.unwrap();
    });
    let client = WsClient::builder().limits(limits()).build().expect("valid client");
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    time::timeout(DEADLINE, pong_seen_rx)
        .await
        .expect("Pong confirmation deadline")
        .expect("peer confirmed Pong");
    time::timeout(DEADLINE, session.close()).await.unwrap().unwrap();
    assert!(matches!(
        receive_until!(&mut session),
        Some(Ok(WsMessage::Text(text))) if text == "q"
    ));
    assert!(receive_until!(&mut session).is_none());
    peer_join(peer).await;
}

#[tokio::test]
async fn test_inbound_full_queue_times_out_with_1013_after_control_frame_progress() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = spawn(async move {
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
            if let Message::Pong(payload) = time::timeout(DEADLINE, websocket.next())
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
            if let Message::Close(frame) = time::timeout(DEADLINE, websocket.next())
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    peer_join(peer).await;
    assert!(matches!(
        receive_until!(&mut session),
        Some(Ok(WsMessage::Text(text))) if text == "four"
    ));
    assert!(matches!(
        receive_until!(&mut session),
        Some(Err(error)) if error.kind() == WsErrorKind::Timeout
    ));
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
    let peer = spawn(async move {
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    let first = time::timeout(DEADLINE, session.receive()).await.unwrap();
    assert!(matches!(first, Some(Err(error)) if error.kind() == WsErrorKind::Protocol));
    assert!(receive_until!(&mut session).is_none());
    assert!(matches!(session.termination(), Some(WsTermination::Error { .. })));
    peer_join(peer).await;
}

#[tokio::test]
async fn test_last_handle_drop_closes_session_without_waiting() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        let mut close_seen = false;
        loop {
            match time::timeout(DEADLINE, websocket.next())
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
    let session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
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
    let (close_seen_tx, close_seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        peer_close(&mut websocket).await.expect("peer receives Close");
        close_seen_tx.send(()).ok();
        let _ = time::timeout(DEADLINE, release_rx)
            .await
            .expect("peer release deadline");
        loop {
            match time::timeout(DEADLINE, websocket.next())
                .await
                .expect("closed socket deadline")
            {
                None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            }
        }
    });
    let configured = WsSessionLimits {
        close_timeout: Duration::from_millis(50),
        ..WsSessionLimits::default()
    };
    let client = WsClient::builder().limits(configured).build().expect("valid client");
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    let close_result = time::timeout(DEADLINE, session.close())
        .await
        .expect("bounded close must resolve");
    assert!(matches!(close_result, Err(error) if error.kind() == WsErrorKind::Timeout));
    time::timeout(DEADLINE, close_seen_rx)
        .await
        .expect("peer saw close")
        .unwrap();
    assert!(matches!(
        receive_until!(&mut session),
        Some(Err(error)) if error.kind() == WsErrorKind::Timeout
    ));
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
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "done".into(),
            })))
            .await
            .expect("peer sends Close");
        while let Some(frame) = time::timeout(DEADLINE, websocket.next())
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    assert!(receive_until!(&mut session).is_none());
    let termination = WsTermination::Closed { code: Some(1000) };
    assert_eq!(session.termination(), Some(termination));
    time::timeout(DEADLINE, session.close())
        .await
        .expect("close deadline")
        .expect("close after peer close");
    time::timeout(DEADLINE, session.close())
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
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Size,
                reason: "too large".into(),
            })))
            .await
            .expect("peer sends 1009 Close");
        while let Some(frame) = time::timeout(DEADLINE, websocket.next())
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
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
    let (close_confirmed_tx, close_confirmed_rx) = oneshot::channel();
    let peer = spawn(async move {
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
        let payload = peer_pong(&mut websocket).await.expect("peer receives Pong");
        assert_eq!(payload.as_ref(), b"probe");
        websocket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Size,
                reason: "peer stop".into(),
            })))
            .await
            .expect("send peer Close while enqueue is blocked");

        while let Some(frame) = time::timeout(DEADLINE, websocket.next())
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
            no_progress_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(1),
            ..WsSessionLimits::default()
        })
        .build()
        .expect("valid client");
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    time::timeout(DEADLINE, close_confirmed_rx)
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
    let (close_seen_tx, close_seen_rx) = oneshot::channel();
    let (confirm_tx, confirm_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        loop {
            match time::timeout(DEADLINE, websocket.next())
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");

    {
        let closing = session.close();
        tokio::pin!(closing);
        time::timeout(DEADLINE, async {
            tokio::select! {
                result = &mut closing => {
                    panic!("close completed before server confirmation: {result:?}")
                },
                result = close_seen_rx => result.expect("server observes client Close"),
            }
        })
        .await
        .expect("server Close observation deadline");
        confirm_tx.send(()).expect("server still waits for confirmation gate");
        time::timeout(DEADLINE, closing)
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
    let (close_seen_tx, close_seen_rx) = oneshot::channel();
    let (confirm_tx, confirm_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        loop {
            match time::timeout(DEADLINE, websocket.next())
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
        let _ = time::timeout(DEADLINE, confirm_rx)
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    {
        let closing = session.close();
        tokio::pin!(closing);
        time::timeout(DEADLINE, async {
            tokio::select! {
                _ = close_seen_rx => {},
                result = &mut closing => {
                    panic!("close should wait for peer confirmation: {result:?}")
                },
            }
        })
        .await
        .expect("observe client Close");
    }
    assert_eq!(session.termination(), None, "cancelled close leaves task manageable");
    confirm_tx.send(()).expect("peer still waits for confirmation");
    time::timeout(DEADLINE, session.close())
        .await
        .expect("close retry deadline")
        .expect("retry close succeeds");
    let termination = session.termination().expect("stable terminal snapshot");
    assert!(matches!(termination, WsTermination::Closed { .. }));
    time::timeout(DEADLINE, session.close())
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
    let peer = spawn(async move {
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
        peer_close(&mut websocket)
            .await
            .expect("peer should observe client Close");
        time::timeout(DEADLINE, websocket.flush())
            .await
            .expect("peer Close flush deadline")
            .expect("peer should flush Close confirmation");
    });
    let client = WsClient::builder().build().expect("valid client");
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    assert!(matches!(
        time::timeout(DEADLINE, session.receive()).await.unwrap(),
        Some(Ok(WsMessage::Text(text))) if text == "hello"
    ));
    assert!(matches!(
        time::timeout(DEADLINE, session.receive()).await.unwrap(),
        Some(Ok(WsMessage::Binary(bytes))) if bytes.as_ref() == b"\x00\x01\x02\x03"
    ));
    time::timeout(DEADLINE, session.close()).await.unwrap().unwrap();
    peer_join(peer).await;
}

#[tokio::test]
async fn test_oversized_frame_is_classified_as_frame_too_large() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept client");
        let mut websocket = accept_async(socket).await.expect("handshake");
        websocket
            .send(Message::Frame(Frame::message("12345", OpCode::Data(Data::Text), true)))
            .await
            .expect("send oversized frame");
        peer_close(&mut websocket).await.expect("peer receives Close");
        websocket.flush().await.expect("flush close reply");
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    assert!(matches!(
        receive_until!(&mut session),
        Some(Err(error)) if error.kind() == WsErrorKind::FrameTooLarge
    ));
    assert!(receive_until!(&mut session).is_none());
    peer_join(peer).await;
}

#[tokio::test]
async fn test_oversized_reassembled_message_is_classified_separately() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let peer = spawn(async move {
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
        peer_close(&mut websocket).await.expect("peer receives Close");
        websocket.flush().await.expect("flush close reply");
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
    let mut session = time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
        .await
        .expect("connect deadline")
        .expect("connect");
    assert!(matches!(
        receive_until!(&mut session),
        Some(Err(error)) if error.kind() == WsErrorKind::MessageTooLarge
    ));
    assert!(receive_until!(&mut session).is_none());
    peer_join(peer).await;
}

#[tokio::test]
async fn test_stalled_opening_handshake_times_out() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let (release_tx, release_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (_socket, _) = listener.accept().await.expect("accept client");
        let _ = time::timeout(DEADLINE, release_rx)
            .await
            .expect("peer release deadline");
    });
    let client = WsClient::builder()
        .handshake_timeout(Duration::from_millis(50))
        .build()
        .expect("valid client");
    let error = match time::timeout(DEADLINE, client.connect(&format!("ws://{address}/")))
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
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept client");
        accepted_tx.send(()).ok();
        let mut byte = [0; 1];
        loop {
            let count = time::timeout(DEADLINE, socket.read(&mut byte))
                .await
                .expect("socket release deadline")
                .expect("read client data");
            if count == 0 {
                break count;
            }
        }
    });
    let client = WsClient::builder().build().expect("valid client");
    let connecting = spawn(async move { client.connect(&format!("ws://{address}/")).await });
    time::timeout(DEADLINE, accepted_rx)
        .await
        .expect("peer accepted connection")
        .unwrap();
    connecting.abort();
    assert!(time::timeout(DEADLINE, connecting).await.unwrap().is_err());
    assert_eq!(peer_join(peer).await, 0, "cancelled connect releases TCP socket");
}

// Public session contracts migrated from the former crate-local test module.
#[tokio::test]
async fn test_sends_and_receives_echo_before_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        if let Some(Ok(message)) = websocket.next().await {
            websocket.send(message).await.unwrap();
            let _ = websocket.next().await;
            let _ = websocket.flush().await;
        }
    });
    let client = WsClient::builder().build().unwrap();
    let mut session = time::timeout(Duration::from_secs(2), client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    session.try_send(WsMessage::Text("hi".into())).unwrap();
    assert!(matches!(
        time::timeout(Duration::from_secs(2), session.receive())
            .await
            .unwrap(),
        Some(Ok(WsMessage::Text(text))) if text == "hi"
    ));
    session.close().await.unwrap();
    assert!(session.receive().await.is_none());
    peer.abort();
}

#[tokio::test]
async fn test_abrupt_tcp_eof_is_delivered_as_a_terminal_error_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let websocket = accept_async(socket).await.unwrap();
        drop(websocket);
    });

    let client = WsClient::builder()
        .limits(WsSessionLimits {
            close_timeout: Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = time::timeout(Duration::from_secs(2), client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();

    let received = time::timeout(Duration::from_secs(2), session.receive())
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
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket.close(None).await.unwrap();
    });

    let client = WsClient::builder()
        .limits(WsSessionLimits {
            close_timeout: Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = time::timeout(Duration::from_secs(2), client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();

    let received = time::timeout(Duration::from_secs(2), session.receive())
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
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        let frame = Frame::message(Bytes::from_static(b"12345"), OpCode::Data(Data::Binary), true);
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
            close_timeout: Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = client.connect(&format!("ws://{address}/")).await.unwrap();
    let received = time::timeout(Duration::from_secs(2), session.receive())
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
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        for (payload, opcode, is_final) in [
            (b"123".as_slice(), Data::Binary, false),
            (b"456".as_slice(), Data::Continue, false),
            (b"789".as_slice(), Data::Continue, true),
        ] {
            let frame = Frame::message(Bytes::copy_from_slice(payload), OpCode::Data(opcode), is_final);
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
            close_timeout: Duration::from_millis(200),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = client.connect(&format!("ws://{address}/")).await.unwrap();
    let received = time::timeout(Duration::from_secs(2), session.receive())
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
    let (pong_seen_tx, pong_seen_rx) = oneshot::channel();
    let (continue_tx, continue_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket.send(Message::Text("one".into())).await.unwrap();
        websocket.send(Message::Text("two".into())).await.unwrap();
        websocket.send(Message::Ping(b"ping".to_vec().into())).await.unwrap();
        time::timeout(Duration::from_secs(2), async {
            loop {
                if let Message::Pong(payload) = peer_message(&mut websocket).await.expect("peer read before Pong") {
                    assert_eq!(payload.as_ref(), b"ping");
                    break;
                }
            }
        })
        .await
        .expect("reader should flush Pong under queue backpressure");
        let _ = pong_seen_tx.send(());

        time::timeout(Duration::from_secs(2), continue_rx)
            .await
            .expect("test should allow queue to become full")
            .expect("test should release peer");
        time::timeout(Duration::from_secs(2), websocket.send(Message::Text("three".into())))
            .await
            .expect("sending third message should be bounded")
            .unwrap();

        let close = time::timeout(Duration::from_secs(2), async {
            loop {
                if let Message::Close(frame) = peer_message(&mut websocket).await.expect("peer read before Close") {
                    break frame;
                }
            }
        })
        .await
        .expect("driver should send a Close instead of resetting the TCP socket");
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
            no_progress_timeout: Duration::from_millis(100),
            close_timeout: Duration::from_secs(1),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = client.connect(&format!("ws://{address}/")).await.unwrap();
    time::timeout(Duration::from_secs(2), pong_seen_rx)
        .await
        .expect("server should observe Pong")
        .expect("server should signal Pong");
    let first = time::timeout(Duration::from_secs(2), session.receive())
        .await
        .expect("first queued message should drain");
    assert!(matches!(first, Some(Ok(WsMessage::Text(text))) if text == "one"));
    continue_tx.send(()).expect("peer should wait before third message");
    peer.await.unwrap();
    assert!(matches!(
        time::timeout(Duration::from_secs(2), session.receive())
            .await
            .expect("second queued message should drain"),
        Some(Ok(WsMessage::Text(text))) if text == "two"
    ));
    assert!(matches!(
        time::timeout(Duration::from_secs(2), session.receive())
            .await
            .expect("slow consumer terminal should arrive"),
        Some(Err(error)) if error.kind() == WsErrorKind::Timeout
    ));
}

#[tokio::test]
async fn test_cancelled_close_can_be_retried_to_stable_termination() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (close_seen_tx, close_seen_rx) = oneshot::channel();
    let (confirm_tx, confirm_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        peer_close(&mut websocket)
            .await
            .expect("peer should observe client Close");
        let _ = close_seen_tx.send(());
        time::timeout(DEADLINE, confirm_rx)
            .await
            .expect("confirmation release deadline")
            .expect("test should release peer confirmation");
        time::timeout(DEADLINE, websocket.flush())
            .await
            .expect("peer Close flush deadline")
            .expect("peer should flush the Close confirmation");
    });

    let client = WsClient::builder().build().unwrap();
    let mut session = time::timeout(Duration::from_secs(2), client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();

    {
        let close = session.close();
        tokio::pin!(close);
        time::timeout(DEADLINE, async {
            tokio::select! {
                seen = close_seen_rx => {
                    seen.expect("peer should report Close observation");
                },
                result = &mut close => {
                    panic!("close should wait for peer confirmation: {result:?}")
                }
            }
        })
        .await
        .expect("peer Close observation deadline");
    }
    assert!(session.termination().is_none());
    confirm_tx.send(()).expect("peer is waiting for confirmation release");

    time::timeout(Duration::from_secs(2), session.close())
        .await
        .expect("retry should observe the same driver completion")
        .unwrap();
    peer_join(peer).await;
    let termination = session.termination().expect("driver termination should be stable");
    assert!(matches!(termination, WsTermination::Closed { .. }));
    time::timeout(Duration::from_secs(1), session.close())
        .await
        .expect("repeated close should be immediate")
        .unwrap();
    assert_eq!(session.termination(), Some(termination));
}

#[tokio::test]
async fn test_extra_inbound_data_reports_backpressure_and_closes_with_1013() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket.send(Message::Text("one".into())).await.unwrap();
        websocket.send(Message::Text("two".into())).await.unwrap();
        websocket.send(Message::Text("tri".into())).await.unwrap();

        let close = time::timeout(Duration::from_secs(2), async {
            loop {
                match peer_message(&mut websocket)
                    .await
                    .expect("peer read before overload Close")
                {
                    Message::Close(Some(frame)) => break frame,
                    Message::Close(None) => panic!("expected overload Close with a code"),
                    _ => {}
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
            no_progress_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(1),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = time::timeout(Duration::from_secs(2), client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();

    peer.await.unwrap();
    let first = time::timeout(Duration::from_secs(2), session.receive())
        .await
        .expect("queued message should remain receivable");
    assert!(
        matches!(first, Some(Ok(WsMessage::Text(ref text))) if text == "one"),
        "unexpected first receive: {first:?}"
    );
    assert!(matches!(
        session.receive().await,
        Some(Err(error)) if error.kind() == WsErrorKind::InboundBackpressure
    ));
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
    let (pong_seen_tx, pong_seen_rx) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket.send(Message::Text("one".into())).await.unwrap();
        websocket.send(Message::Text("two".into())).await.unwrap();
        websocket.send(Message::Ping(b"ping".to_vec().into())).await.unwrap();

        time::timeout(Duration::from_secs(2), async {
            loop {
                if let Message::Pong(payload) = peer_message(&mut websocket).await.expect("peer read before Pong") {
                    assert_eq!(payload.as_ref(), b"ping");
                    break;
                }
            }
        })
        .await
        .expect("Ping must be processed while enqueue is blocked");
        let _ = pong_seen_tx.send(());

        let _close = time::timeout(Duration::from_secs(2), async {
            loop {
                if let Message::Close(frame) = peer_message(&mut websocket).await.expect("peer read before Close") {
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
            close_timeout: Duration::from_secs(1),
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = time::timeout(Duration::from_secs(2), client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();

    time::timeout(Duration::from_secs(2), pong_seen_rx)
        .await
        .expect("server should observe Pong")
        .unwrap();
    time::timeout(Duration::from_secs(2), session.close())
        .await
        .expect("close must finish while inbound enqueue is blocked")
        .unwrap();
    peer.await.unwrap();
    assert!(matches!(
        session.receive().await,
        Some(Ok(WsMessage::Text(text))) if text == "one"
    ));
    assert!(session.receive().await.is_none());
}
