// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::WsClient;
use crate::client::handshake::connect_socket;
use crate::error::WsError;
use crate::error::WsErrorKind;
use crate::error::WsSendError;
use crate::error::WsTermination;
use crate::message::WsMessage;
use crate::options::WsSessionLimits;
use crate::session::driver::CloseRequest;
use crate::session::queue::BoundedQueue;

mod driver;
mod queue;

struct Owner {
    inbound: BoundedQueue,
    outbound: BoundedQueue,
    close: watch::Sender<Option<CloseRequest>>,
    terminal: Arc<Mutex<Option<WsTermination>>>,
    terminal_changed: Arc<Notify>,
    close_timeout: Duration,
    close_result: Mutex<Option<Option<WsErrorKind>>>,
    close_gate: AsyncMutex<()>,
    task: AsyncMutex<Option<JoinHandle<()>>>,
    abort: tokio::task::AbortHandle,
}

impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.close.send(Some(CloseRequest::Local));
        self.inbound.close();
        self.outbound.close();
    }
}

/// A connected WebSocket session with one receiving owner and cloneable
/// senders.
pub struct WsSession {
    owner: Arc<Owner>,
    error_delivered: bool,
    response_status_code: u16,
    selected_subprotocol: Option<String>,
}

/// A cloneable handle for bounded asynchronous sends.
#[derive(Clone)]
pub struct WsSender {
    owner: Arc<Owner>,
}

impl WsClient {
    /// Opens a WebSocket using the client's default request headers.
    pub async fn connect(&self, url: &str) -> Result<WsSession, WsError> {
        self.connect_with(url, http::HeaderMap::new()).await
    }

    /// Opens a WebSocket with per-connection request headers.
    pub async fn connect_with(&self, url: &str, headers: http::HeaderMap) -> Result<WsSession, WsError> {
        let connected = connect_socket(url, &self.options, &headers).await?;
        let response_status_code = connected.status_code;
        let selected_subprotocol = connected.selected_subprotocol;
        let WsSessionLimits {
            inbound_messages,
            inbound_bytes,
            outbound_messages,
            outbound_bytes,
            max_frame_bytes,
            max_message_bytes,
            no_progress_timeout,
            close_timeout,
            ..
        } = self.options.limits.clone();
        let inbound = BoundedQueue::new(inbound_messages, inbound_bytes);
        let outbound = BoundedQueue::new(outbound_messages, outbound_bytes);
        let terminal = Arc::new(Mutex::new(None));
        let terminal_changed = Arc::new(Notify::new());
        let (close, close_rx) = watch::channel(None);
        let task = tokio::spawn(driver::run(
            connected.stream,
            inbound.clone(),
            outbound.clone(),
            close_rx,
            close.clone(),
            terminal.clone(),
            terminal_changed.clone(),
            no_progress_timeout,
            close_timeout,
            max_frame_bytes,
            max_message_bytes,
        ));
        let abort = task.abort_handle();
        Ok(WsSession {
            owner: Arc::new(Owner {
                inbound,
                outbound,
                close,
                terminal,
                terminal_changed,
                close_timeout,
                close_result: Mutex::new(None),
                close_gate: AsyncMutex::new(()),
                task: AsyncMutex::new(Some(task)),
                abort,
            }),
            error_delivered: false,
            response_status_code,
            selected_subprotocol,
        })
    }
}

impl WsSession {
    /// Returns the successful opening handshake's HTTP status code.
    pub fn response_status_code(&self) -> u16 {
        self.response_status_code
    }

    /// Returns the subprotocol selected by the server, if one was negotiated.
    pub fn selected_subprotocol(&self) -> Option<&str> {
        self.selected_subprotocol.as_deref()
    }

    /// Receives the next application message or the single terminal error.
    pub async fn receive(&mut self) -> Option<Result<WsMessage, WsError>> {
        loop {
            if let Some(envelope) = self.owner.inbound.try_pop() {
                return Some(Ok(envelope.into_item()));
            }
            let terminal = *self.owner.terminal.lock().expect("terminal mutex poisoned");
            if terminal.is_some() {
                if self.error_delivered {
                    return None;
                }
                self.error_delivered = true;
                return match terminal {
                    Some(WsTermination::Error { kind }) => Some(Err(WsError::new(kind))),
                    _ => None,
                };
            }
            let terminal_changed = self.owner.terminal_changed.notified();
            tokio::pin!(terminal_changed);
            terminal_changed.as_mut().enable();
            tokio::select! {
                biased;
                envelope = self.owner.inbound.pop() => {
                    if let Some(envelope) = envelope {
                        return Some(Ok(envelope.into_item()));
                    }
                }
                _ = &mut terminal_changed => {}
            }
        }
    }

    /// Returns a cloneable sender sharing this session's outbound budget.
    pub fn sender(&self) -> WsSender {
        WsSender {
            owner: self.owner.clone(),
        }
    }

    /// Enqueues a message immediately when both outbound limits permit it.
    pub fn try_send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.try_push(message)
    }

    /// Waits until both outbound limits permit enqueueing the message.
    pub async fn send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.push(message).await
    }

    /// Performs a bounded close handshake; repeated calls are harmless.
    pub async fn close(&mut self) -> Result<(), WsError> {
        let _gate = self.owner.close_gate.lock().await;
        if let Some(result) = *self.owner.close_result.lock().expect("close result mutex poisoned") {
            return result.map_or(Ok(()), |kind| Err(WsError::new(kind)));
        }
        self.owner.outbound.close();
        let _ = self.owner.close.send(Some(CloseRequest::Local));
        let deadline = tokio::time::Instant::now() + self.owner.close_timeout;
        let mut result = loop {
            if let Some(termination) = *self.owner.terminal.lock().expect("terminal mutex poisoned") {
                break termination_error(termination);
            }

            let terminal_changed = self.owner.terminal_changed.notified();
            tokio::pin!(terminal_changed);
            terminal_changed.as_mut().enable();
            if let Some(termination) = *self.owner.terminal.lock().expect("terminal mutex poisoned") {
                break termination_error(termination);
            }

            match tokio::time::timeout_at(deadline, &mut terminal_changed).await {
                Ok(()) => {}
                Err(_) => {
                    self.owner.abort.abort();
                    let mut terminal = self.owner.terminal.lock().expect("terminal mutex poisoned");
                    if terminal.is_none() {
                        *terminal = Some(WsTermination::Error {
                            kind: WsErrorKind::Timeout,
                        });
                    }
                    self.owner.terminal_changed.notify_waiters();
                    break Some(WsErrorKind::Timeout);
                }
            }
        };

        let mut task = self.owner.task.lock().await;
        if let Some(task) = task.as_mut() {
            match tokio::time::timeout_at(deadline, task).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    if result.is_none() {
                        result = Some(WsErrorKind::Internal);
                        *self.owner.terminal.lock().expect("terminal mutex poisoned") = Some(WsTermination::Error {
                            kind: WsErrorKind::Internal,
                        });
                    }
                }
                Err(_) => {
                    self.owner.abort.abort();
                    result = Some(WsErrorKind::Timeout);
                    self.set_terminal_error_if_empty(WsErrorKind::Timeout);
                }
            }
        }
        *self.owner.close_result.lock().expect("close result mutex poisoned") = Some(result);
        result.map_or(Ok(()), |kind| Err(WsError::new(kind)))
    }

    fn set_terminal_error_if_empty(&self, kind: WsErrorKind) {
        let mut terminal = self.owner.terminal.lock().expect("terminal mutex poisoned");
        if terminal.is_none() {
            *terminal = Some(WsTermination::Error { kind });
            self.owner.terminal_changed.notify_waiters();
        }
    }

    /// Returns the immutable terminal snapshot, once the driver has stopped.
    pub fn termination(&self) -> Option<WsTermination> {
        *self.owner.terminal.lock().expect("terminal mutex poisoned")
    }
}

fn termination_error(termination: WsTermination) -> Option<WsErrorKind> {
    match termination {
        WsTermination::Error { kind } => Some(kind),
        WsTermination::Normal { .. } => None,
    }
}

impl WsSender {
    /// Enqueues a message immediately when both outbound limits permit it.
    pub fn try_send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.try_push(message)
    }

    /// Waits until both outbound limits permit enqueueing the message.
    pub async fn send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.push(message).await
    }
}

#[cfg(test)]
mod tests {
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
    use crate::session::queue::BoundedQueue;

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
    async fn explicit_peer_close_remains_a_normal_termination() {
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
        assert!(matches!(session.termination(), Some(WsTermination::Normal { .. })));
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
                max_frame_bytes: 4,
                max_message_bytes: 8,
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
                max_frame_bytes: 4,
                max_message_bytes: 8,
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
                max_frame_bytes: 8,
                max_message_bytes: 8,
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
        assert!(matches!(termination, WsTermination::Normal { .. }));
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
                max_frame_bytes: 4,
                max_message_bytes: 4,
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
                max_frame_bytes: 4,
                max_message_bytes: 4,
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
}
