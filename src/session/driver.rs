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

use futures_util::Sink;
use futures_util::SinkExt;
use futures_util::Stream;
use futures_util::StreamExt;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use crate::error::WsErrorKind;
use crate::error::WsTermination;
use crate::message::WsMessage;
use crate::session::queue::BoundedQueue;

type WsSocket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Inbound capacity thresholds used to classify tungstenite read errors.
#[derive(Clone, Copy, Default)]
struct CapacityLimits {
    max_inbound_frame_bytes: usize,
    max_inbound_message_bytes: usize,
}

/// A lifecycle request sent from the session or reader to the writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CloseRequest {
    Local,
    SlowConsumer,
}

/// Runs the socket lifecycle while transferring each queue and signal to its
/// reader or writer loop. The driver publishes a terminal state only after
/// their outcomes have been combined; a reader exit waits a bounded time for
/// the writer before publication.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    socket: WsSocket,
    inbound: BoundedQueue,
    outbound: BoundedQueue,
    close: watch::Receiver<Option<CloseRequest>>,
    close_tx: watch::Sender<Option<CloseRequest>>,
    terminal: Arc<Mutex<Option<WsTermination>>>,
    terminal_changed: Arc<Notify>,
    no_progress_timeout: Duration,
    close_timeout: Duration,
    max_inbound_frame_bytes: usize,
    max_inbound_message_bytes: usize,
) {
    let (sink, stream) = socket.split();
    run_parts(
        sink,
        stream,
        inbound,
        outbound,
        close,
        close_tx,
        terminal,
        terminal_changed,
        no_progress_timeout,
        close_timeout,
        max_inbound_frame_bytes,
        max_inbound_message_bytes,
    )
    .await;
}

/// Runs the split reader/writer lifecycle for a socket sink and stream.
#[allow(clippy::too_many_arguments)]
async fn run_parts<S, St>(
    sink: S,
    stream: St,
    inbound: BoundedQueue,
    outbound: BoundedQueue,
    close: watch::Receiver<Option<CloseRequest>>,
    close_tx: watch::Sender<Option<CloseRequest>>,
    terminal: Arc<Mutex<Option<WsTermination>>>,
    terminal_changed: Arc<Notify>,
    no_progress_timeout: Duration,
    close_timeout: Duration,
    max_inbound_frame_bytes: usize,
    max_inbound_message_bytes: usize,
) where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin + Send + 'static,
    St: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin + Send + 'static,
{
    let ping = Arc::new(Notify::new());
    // `None` means no peer Close was observed; `Some(None)` means the peer
    // sent Close without a status code; `Some(Some(code))` retains its code.
    let (peer_closed, peer_closed_rx) = watch::channel(None);
    let _peer_closed_keepalive = peer_closed.clone();
    let peer_closed_snapshot = peer_closed_rx.clone();
    let writer = write_loop(
        sink,
        outbound.clone(),
        close.clone(),
        peer_closed_rx,
        ping.clone(),
        no_progress_timeout,
        close_timeout,
        CapacityLimits {
            max_inbound_frame_bytes,
            max_inbound_message_bytes,
        },
    );
    let reader = read_loop(
        stream,
        inbound,
        close.clone(),
        close_tx,
        peer_closed,
        ping,
        no_progress_timeout,
        close_timeout,
        CapacityLimits {
            max_inbound_frame_bytes,
            max_inbound_message_bytes,
        },
    );
    let mut writer = tokio::spawn(writer);
    let mut reader = tokio::spawn(reader);
    let termination = tokio::select! {
        biased;
        result = &mut reader => {
            let read_result = result.unwrap_or(ReadResult::Error(WsErrorKind::Internal));
            complete_after_reader(
                read_result,
                peer_closed_snapshot.clone(),
                &mut writer,
                close_timeout,
            ).await
        }
        result = &mut writer => {
            reader.abort();
            let write_result = result.unwrap_or(Some(WsErrorKind::Internal));
            finish_after_writer(write_result, &peer_closed_snapshot)
        }
    };
    outbound.close();
    let mut state = terminal.lock().expect("terminal mutex poisoned");
    if state.is_none() {
        *state = Some(termination);
    }
    drop(state);
    terminal_changed.notify_waiters();
}

/// Outcome observed by the reader before the writer's final result is known.
#[derive(Clone, Copy)]
enum ReadResult {
    /// The peer sent a Close frame with the given raw status code, if any.
    Closed(Option<u16>),
    /// The local side requested closure and the reader stopped.
    LocalClose,
    /// The reader encountered a protocol, transport, capacity, or timeout
    /// error.
    Error(WsErrorKind),
}

/// Combines reader and writer outcomes into the single public session state.
///
/// Reader failures take precedence because they are observed first. A peer
/// Close or a local close is successful only when the writer also succeeds.
fn resolve_termination(
    read: ReadResult,
    write: Option<WsErrorKind>,
    peer_closed: Option<Option<u16>>,
) -> WsTermination {
    match (read, write) {
        (ReadResult::Error(kind), _) => WsTermination::Error { kind },
        (ReadResult::Closed(code), None) => WsTermination::Closed { code },
        (_, Some(kind)) => WsTermination::Error { kind },
        (ReadResult::LocalClose, None) => match peer_closed {
            Some(code) => WsTermination::Closed { code },
            None => WsTermination::Error {
                kind: WsErrorKind::Internal,
            },
        },
    }
}

/// Resolves a writer-first exit against the peer Close snapshot observed so
/// far.
fn finish_after_writer(
    write: Option<WsErrorKind>,
    peer_closed: &watch::Receiver<Option<Option<u16>>>,
) -> WsTermination {
    let peer_closed = { *peer_closed.borrow() };
    resolve_termination(ReadResult::LocalClose, write, peer_closed)
}

/// Waits at most `close_timeout` for the writer after the reader has stopped.
///
/// On expiry the writer is aborted and the timeout participates in the normal
/// termination precedence rules. A task panic or cancellation is classified
/// as `Internal`, since it is not an underlying socket I/O failure.
async fn complete_after_reader(
    read: ReadResult,
    peer_closed: watch::Receiver<Option<Option<u16>>>,
    writer: &mut tokio::task::JoinHandle<Option<WsErrorKind>>,
    close_timeout: Duration,
) -> WsTermination {
    let write = match tokio::time::timeout(close_timeout, &mut *writer).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Some(WsErrorKind::Internal),
        Err(_) => {
            writer.abort();
            Some(WsErrorKind::Timeout)
        }
    };
    let peer_closed_snapshot = { *peer_closed.borrow() };
    resolve_termination(read, write, peer_closed_snapshot)
}

/// Reads peer messages and control frames, coordinating close requests and
/// reporting the reader's outcome to the lifecycle driver.
#[allow(clippy::too_many_arguments)]
async fn read_loop<St>(
    mut stream: St,
    inbound: BoundedQueue,
    mut close: watch::Receiver<Option<CloseRequest>>,
    close_tx: watch::Sender<Option<CloseRequest>>,
    peer_closed: watch::Sender<Option<Option<u16>>>,
    ping: Arc<Notify>,
    no_progress_timeout: Duration,
    close_timeout: Duration,
    capacity: CapacityLimits,
) -> ReadResult
where
    St: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let mut close_deadline = None;
    loop {
        tokio::select! {
            changed = close.changed() => {
                if changed.is_err() { return ReadResult::LocalClose; }
                if close.borrow().is_some() && close_deadline.is_none() {
                    close_deadline = Some(tokio::time::Instant::now() + close_timeout);
                }
            }
            _ = async {
                if let Some(deadline) = close_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => return ReadResult::Error(WsErrorKind::Timeout),
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let result = enqueue_while_polling_control(
                            &mut stream,
                            inbound.clone(),
                            WsMessage::Text(text.to_string()),
                            &mut close,
                            &ping,
                            no_progress_timeout,
                            capacity,
                        ).await;
                        match result {
                            EnqueueResult::Queued => {},
                            EnqueueResult::Closed => return ReadResult::LocalClose,
                            EnqueueResult::LocalClose => {
                                close_deadline = Some(tokio::time::Instant::now() + close_timeout);
                                continue;
                            }
                            EnqueueResult::PeerClosed(code) => {
                                let _ = peer_closed.send(Some(code));
                                return ReadResult::Closed(code);
                            }
                            EnqueueResult::SlowConsumer => {
                                let _ = close_tx.send(Some(CloseRequest::SlowConsumer));
                                return ReadResult::Error(WsErrorKind::Timeout);
                            }
                            EnqueueResult::Error(kind) => {
                                let _ = close_tx.send(Some(CloseRequest::Local));
                                return ReadResult::Error(kind);
                            }
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        let result = enqueue_while_polling_control(
                            &mut stream,
                            inbound.clone(),
                            WsMessage::Binary(bytes),
                            &mut close,
                            &ping,
                            no_progress_timeout,
                            capacity,
                        ).await;
                        match result {
                            EnqueueResult::Queued => {},
                            EnqueueResult::Closed => return ReadResult::LocalClose,
                            EnqueueResult::LocalClose => {
                                close_deadline = Some(tokio::time::Instant::now() + close_timeout);
                                continue;
                            }
                            EnqueueResult::PeerClosed(code) => {
                                let _ = peer_closed.send(Some(code));
                                return ReadResult::Closed(code);
                            }
                            EnqueueResult::SlowConsumer => {
                                let _ = close_tx.send(Some(CloseRequest::SlowConsumer));
                                return ReadResult::Error(WsErrorKind::Timeout);
                            }
                            EnqueueResult::Error(kind) => {
                                let _ = close_tx.send(Some(CloseRequest::Local));
                                return ReadResult::Error(kind);
                            }
                        }
                    }
                    Some(Ok(Message::Ping(_))) => { ping.notify_one(); }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        let code = frame.map(|frame| u16::from(frame.code));
                        let _ = peer_closed.send(Some(code));
                        return ReadResult::Closed(code);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        let _ = close_tx.send(Some(CloseRequest::Local));
                        return ReadResult::Error(map_error(&error, capacity));
                    }
                    None => {
                        let _ = close_tx.send(Some(CloseRequest::Local));
                        return ReadResult::Error(WsErrorKind::Protocol);
                    }
                }
            }
        }
    }
}

/// Result of waiting to enqueue one inbound message while servicing controls.
///
/// The reader retains at most the message currently waiting for queue capacity.
/// It continues polling control frames during that wait; additional application
/// data or expiry of the no-progress deadline takes the slow-consumer path.
#[derive(Debug)]
enum EnqueueResult {
    /// The waiting message was admitted to the bounded inbound queue.
    Queued,
    /// The inbound queue closed before it could admit the message.
    Closed,
    /// A local close request stopped the wait.
    LocalClose,
    /// The peer sent Close while the reader was waiting for capacity.
    PeerClosed(Option<u16>),
    /// More application data arrived, or the no-progress deadline expired.
    SlowConsumer,
    /// Reading failed; the category distinguishes transport, protocol, and
    /// capacity failures.
    Error(WsErrorKind),
}

/// Waits to enqueue one inbound message while continuing to poll control
/// frames.
///
/// Only the message passed to this function may wait outside the bounded queue.
/// If capacity becomes available it is queued; another text or binary message
/// arriving first, or expiry of `no_progress_timeout`, returns
/// [`EnqueueResult::SlowConsumer`]. Ping, Pong, peer Close, local close, and
/// read errors retain their distinct outcomes so the caller can preserve
/// control-frame liveness and classify failures. The slow-consumer caller asks
/// the writer to send Close code `1013`; a write failure may prevent the peer
/// from receiving it.
async fn enqueue_while_polling_control<St>(
    stream: &mut St,
    inbound: BoundedQueue,
    message: WsMessage,
    close: &mut watch::Receiver<Option<CloseRequest>>,
    ping: &Notify,
    no_progress_timeout: Duration,
    capacity: CapacityLimits,
) -> EnqueueResult
where
    St: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let push = inbound.push(message);
    tokio::pin!(push);
    let deadline = tokio::time::sleep(no_progress_timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            result = &mut push => return match result {
                //
                Ok(()) => EnqueueResult::Queued,
                Err(_) => EnqueueResult::Closed,
            },
            changed = close.changed() => {
                if changed.is_err() || matches!(*close.borrow(), Some(CloseRequest::Local)) {
                    return EnqueueResult::LocalClose;
                }
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Ping(_))) => { ping.notify_one(); }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        return EnqueueResult::PeerClosed(frame.map(|frame| u16::from(frame.code)));
                    }
                    // At most one message is retained outside the bounded queue. If more
                    // application data arrives while it is full, terminate with 1013.
                    Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                        return EnqueueResult::SlowConsumer;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return EnqueueResult::Error(map_error(&error, capacity)),
                    None => return EnqueueResult::Error(WsErrorKind::Protocol),
                }
            }
            _ = &mut deadline => return EnqueueResult::SlowConsumer,
        }
    }
}

/// Writes queued messages and control frames, then waits for peer confirmation
/// when performing a local close handshake.
#[allow(clippy::too_many_arguments)]
async fn write_loop<S>(
    mut sink: S,
    outbound: BoundedQueue,
    mut close: watch::Receiver<Option<CloseRequest>>,
    mut peer_closed: watch::Receiver<Option<Option<u16>>>,
    ping: Arc<Notify>,
    no_progress_timeout: Duration,
    close_timeout: Duration,
    capacity: CapacityLimits,
) -> Option<WsErrorKind>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    loop {
        tokio::select! {
            biased;
            changed = peer_closed.changed() => {
                if changed.is_err() || peer_closed.borrow().is_some() {
                    return timeout_flush(&mut sink, close_timeout, capacity).await;
                }
            }
            changed = close.changed() => {
                if changed.is_err() { return Some(WsErrorKind::Internal); }
                let request = *close.borrow();
                match request {
                    Some(CloseRequest::Local) => {
                        return close_sink_and_wait(
                            &mut sink,
                            &mut peer_closed,
                            close_timeout,
                            capacity,
                        )
                        .await;
                    }
                    Some(CloseRequest::SlowConsumer) => {
                        return send_slow_consumer_close(
                            &mut sink,
                            close_timeout,
                            capacity,
                        )
                        .await;
                    }
                    None => {}
                }
            }
            _ = ping.notified() => {
                if let Some(kind) = timeout_flush(&mut sink, no_progress_timeout, capacity).await {
                    return Some(kind);
                }
            }
            outgoing = outbound.pop() => {
                let Some(envelope) = outgoing else {
                    // Queue closure without a close request or peer Close is
                    // an impossible lifecycle transition.
                    return Some(WsErrorKind::Internal);
                };
                match send_envelope(
                    &mut sink,
                    envelope,
                    &mut close,
                    &mut peer_closed,
                    &ping,
                    no_progress_timeout,
                    close_timeout,
                    capacity,
                ).await {
                    SendResult::Sent => {},
                    SendResult::Exit(kind) => return kind,
                }
            }
        }
    }
}

enum SendResult {
    Sent,
    Exit(Option<WsErrorKind>),
}

/// Sends one dequeued message while observing close and peer-control signals.
/// Dropping the envelope releases its reserved queue capacity.
#[allow(clippy::too_many_arguments)]
async fn send_envelope<S>(
    sink: &mut S,
    envelope: crate::session::queue::Envelope,
    close: &mut watch::Receiver<Option<CloseRequest>>,
    peer_closed: &mut watch::Receiver<Option<Option<u16>>>,
    ping: &Notify,
    no_progress_timeout: Duration,
    close_timeout: Duration,
    capacity: CapacityLimits,
) -> SendResult
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let crate::session::queue::Envelope {
        item,
        _message_permit,
        _byte_permit,
    } = envelope;
    let mut message = Some(match item {
        WsMessage::Text(text) => Message::Text(text.into()),
        WsMessage::Binary(bytes) => Message::Binary(bytes),
    });
    let mut started = false;
    let deadline = tokio::time::Instant::now() + no_progress_timeout;

    loop {
        let event = tokio::select! {
            biased;
            changed = peer_closed.changed() => {
                if changed.is_err() || peer_closed.borrow().is_some() {
                    SendEvent::PeerClosed
                } else {
                    continue;
                }
            }
            changed = close.changed() => {
                if changed.is_err() {
                    SendEvent::Close(None)
                } else {
                    SendEvent::Close(*close.borrow())
                }
            }
            _ = ping.notified() => SendEvent::Ping,
            result = tokio::time::timeout_at(deadline, std::future::poll_fn(|context| {
                if started { sink.poll_flush_unpin(context) } else { sink.poll_ready_unpin(context) }
            })) => SendEvent::Progress(result),
        };

        match event {
            SendEvent::PeerClosed => {
                return SendResult::Exit(timeout_flush(sink, close_timeout, capacity).await);
            }
            SendEvent::Close(Some(CloseRequest::Local)) | SendEvent::Close(None) => {
                return SendResult::Exit(close_sink_and_wait(sink, peer_closed, close_timeout, capacity).await);
            }
            SendEvent::Close(Some(CloseRequest::SlowConsumer)) => {
                return SendResult::Exit(send_slow_consumer_close(sink, close_timeout, capacity).await);
            }
            SendEvent::Ping => {
                if let Some(kind) = timeout_flush_until(sink, deadline, capacity).await {
                    return SendResult::Exit(Some(kind));
                }
                if started {
                    return SendResult::Sent;
                }
            }
            SendEvent::Progress(Err(_)) => return SendResult::Exit(Some(WsErrorKind::Timeout)),
            SendEvent::Progress(Ok(Err(error))) => {
                return SendResult::Exit(Some(map_error(&error, capacity)));
            }
            SendEvent::Progress(Ok(Ok(()))) if started => return SendResult::Sent,
            SendEvent::Progress(Ok(Ok(()))) => {
                if let Err(error) = sink.start_send_unpin(message.take().expect("message not yet sent")) {
                    return SendResult::Exit(Some(map_error(&error, capacity)));
                }
                started = true;
            }
        }
    }
}

enum SendEvent {
    Progress(Result<Result<(), tokio_tungstenite::tungstenite::Error>, tokio::time::error::Elapsed>),
    Ping,
    Close(Option<CloseRequest>),
    PeerClosed,
}

/// Flushes pending control output before `timeout`, classifying I/O and expiry.
async fn timeout_flush<S>(sink: &mut S, timeout: Duration, capacity: CapacityLimits) -> Option<WsErrorKind>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout(timeout, sink.flush()).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(map_error(&error, capacity)),
        Err(_) => Some(WsErrorKind::Timeout),
    }
}

/// Flushes pending output, then sends a `1013` close within one shared
/// deadline.
async fn send_slow_consumer_close<S>(sink: &mut S, timeout: Duration, capacity: CapacityLimits) -> Option<WsErrorKind>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let deadline = tokio::time::Instant::now() + timeout;
    if let Some(kind) = timeout_flush_until(sink, deadline, capacity).await {
        return Some(kind);
    }
    let close = Message::Close(Some(CloseFrame {
        code: CloseCode::Again,
        reason: "queue full".into(),
    }));
    match tokio::time::timeout_at(deadline, sink.send(close)).await {
        Ok(Ok(())) => Some(WsErrorKind::Timeout),
        Ok(Err(error)) => Some(map_error(&error, capacity)),
        Err(_) => Some(WsErrorKind::Timeout),
    }
}

/// Sends the local Close frame and waits for peer confirmation by one deadline.
async fn close_sink_and_wait<S>(
    sink: &mut S,
    peer_closed: &mut watch::Receiver<Option<Option<u16>>>,
    timeout: Duration,
    capacity: CapacityLimits,
) -> Option<WsErrorKind>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let deadline = tokio::time::Instant::now() + timeout;
    match tokio::time::timeout_at(deadline, sink.close()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Some(map_error(&error, capacity)),
        Err(_) => return Some(WsErrorKind::Timeout),
    }

    loop {
        if peer_closed.borrow().is_some() {
            return None;
        }
        match tokio::time::timeout_at(deadline, peer_closed.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return Some(WsErrorKind::Timeout),
        }
    }
}

/// Flushes pending output before an existing operation deadline.
async fn timeout_flush_until<S>(
    sink: &mut S,
    deadline: tokio::time::Instant,
    capacity: CapacityLimits,
) -> Option<WsErrorKind>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout_at(deadline, sink.flush()).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(map_error(&error, capacity)),
        Err(_) => Some(WsErrorKind::Timeout),
    }
}

/// Maps a tungstenite failure to its stable, non-sensitive public category.
fn map_error(error: &tokio_tungstenite::tungstenite::Error, limits: CapacityLimits) -> WsErrorKind {
    use tokio_tungstenite::tungstenite::Error as TungsteniteError;
    match error {
        // Tungstenite reports only max_size, not which check produced the error.
        // Equal configured limits are indistinguishable, so the frame limit wins that tie.
        TungsteniteError::Capacity(tokio_tungstenite::tungstenite::error::CapacityError::MessageTooLong {
            max_size,
            ..
        }) => match (
            *max_size == limits.max_inbound_frame_bytes,
            *max_size == limits.max_inbound_message_bytes,
        ) {
            (true, false) | (true, true) => WsErrorKind::FrameTooLarge,
            (false, true) | (false, false) => WsErrorKind::MessageTooLarge,
        },
        TungsteniteError::Capacity(_) => WsErrorKind::Protocol,
        TungsteniteError::Protocol(_) => WsErrorKind::Protocol,
        TungsteniteError::ConnectionClosed | TungsteniteError::AlreadyClosed => WsErrorKind::Protocol,
        TungsteniteError::Io(source) if source.kind() == std::io::ErrorKind::TimedOut => WsErrorKind::Timeout,
        _ => WsErrorKind::Transport,
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use futures_util::Sink;
    use tokio::sync::Notify;
    use tokio::sync::watch;
    use tokio_tungstenite::tungstenite::Message;

    use super::CapacityLimits;
    use super::CloseRequest;
    use super::ReadResult;
    use super::SendResult;
    use super::complete_after_reader;
    use super::enqueue_while_polling_control;
    use super::finish_after_writer;
    use super::resolve_termination;
    use super::run_parts;
    use super::send_envelope;
    use super::timeout_flush;
    use super::write_loop;
    use crate::error::WsErrorKind;
    use crate::error::WsTermination;
    use crate::message::WsMessage;
    use crate::session::Owner;
    use crate::session::WsSession;
    use crate::session::queue::BoundedQueue;

    #[tokio::test]
    async fn extra_inbound_message_triggers_slow_consumer_before_deadline() {
        let inbound = BoundedQueue::new(1, 8);
        inbound
            .try_push(WsMessage::Text("full".into()))
            .expect("the single queue slot is available");
        let mut stream = futures_util::stream::iter([Ok(Message::Text("extra".into()))]);
        let (close_tx, mut close_rx) = watch::channel(None);
        let ping = Notify::new();

        let result = enqueue_while_polling_control(
            &mut stream,
            inbound,
            WsMessage::Text("waiting".into()),
            &mut close_rx,
            &ping,
            Duration::from_secs(30),
            CapacityLimits::default(),
        )
        .await;

        assert!(matches!(result, super::EnqueueResult::SlowConsumer));
        drop(close_tx);
    }

    #[tokio::test]
    async fn peer_close_flush_failure_reaches_session_api() {
        let inbound = BoundedQueue::new(1, 16);
        let outbound = BoundedQueue::new(1, 16);
        let (close, close_rx) = watch::channel(None);
        let (close_tx, _close_keepalive) = watch::channel(None);
        let terminal = Arc::new(Mutex::new(None));
        let terminal_changed = Arc::new(Notify::new());
        let state = Arc::new(Mutex::new(SinkState {
            flush: FlushBehavior::Error,
            ..SinkState::default()
        }));
        let sink = ControlledSink {
            state,
            poll_ready_seen: Arc::new(Notify::new()),
            close_seen: Arc::new(Notify::new()),
        };
        let stream = futures_util::stream::iter([Ok(Message::Close(None))]);
        let driver = tokio::spawn(run_parts(
            sink,
            stream,
            inbound.clone(),
            outbound.clone(),
            close_rx,
            close_tx,
            terminal.clone(),
            terminal_changed.clone(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            16,
            16,
        ));
        let abort = driver.abort_handle();
        let owner = Arc::new(Owner {
            inbound,
            outbound,
            close,
            terminal,
            terminal_changed,
            close_timeout: Duration::from_secs(1),
            close_result: Mutex::new(None),
            close_gate: tokio::sync::Mutex::new(()),
            task: tokio::sync::Mutex::new(Some(driver)),
            abort,
        });
        let mut session = WsSession {
            owner,
            error_delivered: false,
            response_status_code: 101,
            selected_subprotocol: None,
        };

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), session.receive())
                .await
                .expect("driver should publish the flush error"),
            Some(Err(error)) if error.kind() == WsErrorKind::Transport
        ));
        assert_eq!(
            session.termination(),
            Some(WsTermination::Error {
                kind: WsErrorKind::Transport
            })
        );
        assert!(session.receive().await.is_none());
        assert_eq!(
            session
                .close()
                .await
                .expect_err("failed close flush must reach close()")
                .kind(),
            WsErrorKind::Transport
        );
    }

    #[tokio::test]
    async fn closed_outbound_without_close_request_is_internal_error() {
        let outbound = BoundedQueue::new(1, 16);
        outbound.close();
        let (_close_tx, close) = watch::channel(None);
        let (_peer_closed_tx, peer_closed) = watch::channel(None);
        let state = Arc::new(Mutex::new(SinkState::default()));
        let sink = ControlledSink {
            state,
            poll_ready_seen: Arc::new(Notify::new()),
            close_seen: Arc::new(Notify::new()),
        };

        let result = write_loop(
            sink,
            outbound,
            close,
            peer_closed,
            Arc::new(Notify::new()),
            Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityLimits::default(),
        )
        .await;

        assert_eq!(result, Some(WsErrorKind::Internal));
    }

    #[tokio::test]
    async fn closed_close_signal_without_request_is_internal_error() {
        let outbound = BoundedQueue::new(1, 16);
        let (close_tx, close) = watch::channel(None);
        drop(close_tx);
        let (_peer_closed_tx, peer_closed) = watch::channel(None);
        let sink = ControlledSink {
            state: Arc::new(Mutex::new(SinkState::default())),
            poll_ready_seen: Arc::new(Notify::new()),
            close_seen: Arc::new(Notify::new()),
        };

        let result = write_loop(
            sink,
            outbound,
            close,
            peer_closed,
            Arc::new(Notify::new()),
            Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityLimits::default(),
        )
        .await;

        assert_eq!(result, Some(WsErrorKind::Internal));
    }

    #[tokio::test]
    async fn peer_close_waits_for_writer_result_before_selecting_termination() {
        let mut writer = tokio::spawn(async { Some(WsErrorKind::Transport) });
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                &mut writer,
                Duration::from_secs(1)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Transport
            }
        );
    }

    #[tokio::test]
    async fn complete_after_reader_uses_close_snapshot_updated_during_writer_wait() {
        let (peer_closed_tx, peer_closed) = watch::channel(None);
        let (writer_release_tx, writer_release_rx) = tokio::sync::oneshot::channel();
        let (writer_started_tx, writer_started_rx) = tokio::sync::oneshot::channel();
        let mut writer = tokio::spawn(async move {
            writer_started_tx.send(()).expect("completion is waiting");
            let _ = writer_release_rx.await;
            None
        });
        let completion = tokio::spawn(async move {
            complete_after_reader(ReadResult::LocalClose, peer_closed, &mut writer, Duration::from_secs(1)).await
        });

        writer_started_rx.await.expect("writer is being awaited");
        peer_closed_tx.send(Some(Some(1009))).expect("reader remains active");
        writer_release_tx.send(()).expect("writer remains active");

        assert_eq!(
            completion.await.expect("completion task exits"),
            WsTermination::Closed { code: Some(1009) }
        );
    }

    #[test]
    fn writer_first_uses_peer_close_snapshot() {
        let (peer_closed_tx, peer_observer) = watch::channel(None);
        peer_closed_tx
            .send(Some(Some(1009)))
            .expect("writer observes peer Close");
        assert_eq!(
            finish_after_writer(None, &peer_observer),
            WsTermination::Closed { code: Some(1009) }
        );

        let (_peer_closed_tx, peer_observer) = watch::channel(None);
        assert_eq!(
            finish_after_writer(None, &peer_observer),
            WsTermination::Error {
                kind: WsErrorKind::Internal
            }
        );
    }

    #[tokio::test]
    async fn peer_close_aborts_writer_when_close_wait_expires() {
        let mut writer = tokio::spawn(std::future::pending::<Option<WsErrorKind>>());
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                &mut writer,
                Duration::from_millis(10)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Timeout
            }
        );
        let join_error = writer.await.expect_err("timed-out writer should be canceled");
        assert!(join_error.is_cancelled(), "writer cancellation should be observable");
    }

    #[tokio::test]
    async fn writer_join_failure_is_internal_error() {
        let mut writer = tokio::spawn(std::future::pending::<Option<WsErrorKind>>());
        writer.abort();
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                &mut writer,
                Duration::from_secs(1)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Internal
            }
        );
    }

    #[test]
    fn termination_resolution_preserves_close_codes_and_error_precedence() {
        for code in [Some(1000), Some(1009), None] {
            assert_eq!(
                resolve_termination(ReadResult::Closed(code), None, Some(code)),
                WsTermination::Closed { code }
            );
        }
        assert_eq!(
            resolve_termination(
                ReadResult::Closed(Some(1000)),
                Some(WsErrorKind::Timeout),
                Some(Some(1000))
            ),
            WsTermination::Error {
                kind: WsErrorKind::Timeout
            }
        );
        assert_eq!(
            resolve_termination(ReadResult::LocalClose, None, None),
            WsTermination::Error {
                kind: WsErrorKind::Internal
            }
        );
        for code in [Some(1000), Some(1009), None] {
            assert_eq!(
                resolve_termination(ReadResult::LocalClose, None, Some(code)),
                WsTermination::Closed { code }
            );
        }
        assert_eq!(
            resolve_termination(ReadResult::LocalClose, Some(WsErrorKind::Transport), None),
            WsTermination::Error {
                kind: WsErrorKind::Transport
            }
        );
        assert_eq!(
            resolve_termination(
                ReadResult::Error(WsErrorKind::Protocol),
                Some(WsErrorKind::Timeout),
                None
            ),
            WsTermination::Error {
                kind: WsErrorKind::Protocol
            }
        );
    }

    #[derive(Default)]
    struct SinkState {
        events: Vec<&'static str>,
        ready: bool,
        flush: FlushBehavior,
    }

    #[derive(Clone, Copy, Default)]
    enum FlushBehavior {
        #[default]
        Success,
        Error,
        Pending,
    }

    struct ControlledSink {
        state: Arc<Mutex<SinkState>>,
        poll_ready_seen: Arc<Notify>,
        close_seen: Arc<Notify>,
    }

    impl Sink<Message> for ControlledSink {
        type Error = tokio_tungstenite::tungstenite::Error;

        fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().expect("sink state poisoned");
            state.events.push("poll_ready");
            self.poll_ready_seen.notify_one();
            if state.ready {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            self.state
                .lock()
                .expect("sink state poisoned")
                .events
                .push("start_send");
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().expect("sink state poisoned");
            state.events.push("flush");
            state.ready = true;
            match state.flush {
                FlushBehavior::Success => Poll::Ready(Ok(())),
                FlushBehavior::Error => Poll::Ready(Err(tokio_tungstenite::tungstenite::Error::Io(
                    std::io::Error::other("controlled flush failure"),
                ))),
                FlushBehavior::Pending => Poll::Pending,
            }
        }

        fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().expect("sink state poisoned");
            state.events.push("close");
            self.close_seen.notify_one();
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn controlled_flush_failure_is_transport_error() {
        let state = Arc::new(Mutex::new(SinkState {
            flush: FlushBehavior::Error,
            ..SinkState::default()
        }));
        let mut sink = ControlledSink {
            state,
            poll_ready_seen: Arc::new(Notify::new()),
            close_seen: Arc::new(Notify::new()),
        };
        let mut writer =
            tokio::spawn(
                async move { timeout_flush(&mut sink, Duration::from_secs(1), CapacityLimits::default()).await },
            );
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                &mut writer,
                Duration::from_secs(1)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Transport
            }
        );
    }

    #[tokio::test]
    async fn controlled_pending_flush_is_timeout() {
        let state = Arc::new(Mutex::new(SinkState {
            flush: FlushBehavior::Pending,
            ..SinkState::default()
        }));
        let mut sink = ControlledSink {
            state,
            poll_ready_seen: Arc::new(Notify::new()),
            close_seen: Arc::new(Notify::new()),
        };
        let mut writer = tokio::spawn(async move {
            timeout_flush(&mut sink, Duration::from_millis(10), CapacityLimits::default()).await
        });
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                &mut writer,
                Duration::from_secs(1)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Timeout
            }
        );
    }

    #[tokio::test]
    async fn ping_flush_interrupts_pending_business_poll_ready() {
        let queue = BoundedQueue::new(1, 16);
        queue
            .try_push(WsMessage::Text("payload".to_owned()))
            .expect("queue has capacity");
        let envelope = queue.pop().await.expect("queued envelope");

        let state = Arc::new(Mutex::new(SinkState::default()));
        let poll_ready_seen = Arc::new(Notify::new());
        let close_seen = Arc::new(Notify::new());
        let mut sink = ControlledSink {
            state: state.clone(),
            poll_ready_seen: poll_ready_seen.clone(),
            close_seen,
        };
        let ping = Notify::new();
        let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
        let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
        let send = send_envelope(
            &mut sink,
            envelope,
            &mut close,
            &mut peer_closed,
            &ping,
            Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityLimits::default(),
        );
        tokio::pin!(send);

        tokio::select! {
            _ = poll_ready_seen.notified() => {},
            _ = &mut send => panic!("send finished before pending poll_ready was interrupted"),
        }
        ping.notify_one();
        assert!(matches!(send.await, SendResult::Sent));

        let events = &state.lock().expect("sink state poisoned").events;
        let control_flush = events
            .iter()
            .position(|event| *event == "flush")
            .expect("Ping should trigger a flush");
        let business_send = events
            .iter()
            .position(|event| *event == "start_send")
            .expect("business message should eventually be sent");
        assert!(control_flush < business_send, "events: {events:?}");
    }

    #[tokio::test]
    async fn local_close_during_pending_business_write_waits_for_peer_ack() {
        let queue = BoundedQueue::new(1, 16);
        queue
            .try_push(WsMessage::Text("payload".to_owned()))
            .expect("queue has capacity");
        let envelope = queue.pop().await.expect("queued envelope");

        let state = Arc::new(Mutex::new(SinkState::default()));
        let poll_ready_seen = Arc::new(Notify::new());
        let close_seen = Arc::new(Notify::new());
        let mut sink = ControlledSink {
            state: state.clone(),
            poll_ready_seen: poll_ready_seen.clone(),
            close_seen: close_seen.clone(),
        };
        let ping = Notify::new();
        let (close_tx, mut close) = watch::channel(None::<CloseRequest>);
        let (peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
        let send = send_envelope(
            &mut sink,
            envelope,
            &mut close,
            &mut peer_closed,
            &ping,
            Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityLimits::default(),
        );
        tokio::pin!(send);

        tokio::select! {
            _ = poll_ready_seen.notified() => {},
            _ = &mut send => panic!("business write should be pending before close"),
        }
        close_tx.send(Some(CloseRequest::Local)).expect("close receiver exists");
        tokio::select! {
            _ = close_seen.notified() => {},
            _ = &mut send => panic!("successful Close must wait for peer confirmation"),
        }
        peer_tx.send(Some(None)).expect("peer-close receiver exists");
        assert!(matches!(send.await, SendResult::Exit(None)));

        let events = &state.lock().expect("sink state poisoned").events;
        assert!(!events.contains(&"start_send"), "events: {events:?}");
    }
}
