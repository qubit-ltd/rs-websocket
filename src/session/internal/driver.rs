// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::Sink;
use futures_util::SinkExt;
use futures_util::Stream;
use futures_util::StreamExt;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::error::CapacityError;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use super::bounded_queue::BoundedQueue;
use super::capacity_limits::CapacityLimits;
use super::close_request::CloseRequest;
use super::enqueue_result::EnqueueResult;
use super::envelope::Envelope;
use super::read_result::ReadResult;
use super::send_event::SendEvent;
use super::send_result::SendResult;
use crate::error::WsErrorKind;
use crate::error::WsTermination;
use crate::options::WsSessionLimits;
use crate::ws_message::WsMessage;

/// Represents the concrete WebSocket stream handled by the session driver.
type WsSocket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Owns the queues and lifecycle signals shared by the socket driver.
///
/// The limits travel with the session context so the socket entry points do
/// not rely on positional arguments to keep independent budgets aligned.
pub(in crate::session) struct DriverContext {
    /// Inbound application queue shared with the receiving session.
    pub(in crate::session) inbound: BoundedQueue,
    /// Outbound application queue shared with session senders.
    pub(in crate::session) outbound: BoundedQueue,
    /// Receives the first local or slow-consumer close request.
    pub(in crate::session) close: watch::Receiver<Option<CloseRequest>>,
    /// Publishes close requests for the reader and writer loops.
    pub(in crate::session) close_tx: watch::Sender<Option<CloseRequest>>,
    /// Stores the single terminal result selected by the driver.
    pub(in crate::session) terminal: Arc<Mutex<Option<WsTermination>>>,
    /// Wakes session operations waiting for terminal publication.
    pub(in crate::session) terminal_changed: Arc<Notify>,
    /// Queue, frame, and lifecycle time limits applied to this connection.
    pub(in crate::session) limits: WsSessionLimits,
}

/// Runs the socket lifecycle while transferring each queue and signal to its
/// reader or writer loop. The driver publishes a terminal state only after
/// their outcomes have been combined; a reader exit waits a bounded time for
/// the writer before publication.
pub(in crate::session) async fn run(socket: WsSocket, context: DriverContext) {
    let (sink, stream) = socket.split();
    run_parts(sink, stream, context).await;
}

/// Runs the split reader/writer lifecycle for a socket sink and stream.
///
/// The context is consumed and its queue handles are cloned only for the
/// reader and writer loops that need shared access. Terminal state is
/// published after both loop outcomes are reconciled according to
/// [`resolve_termination`].
async fn run_parts<S, St>(sink: S, stream: St, context: DriverContext)
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
    St: Stream<Item = Result<Message, TungsteniteError>> + Unpin,
{
    let DriverContext {
        inbound,
        outbound,
        close,
        close_tx,
        terminal,
        terminal_changed,
        limits,
    } = context;
    let WsSessionLimits {
        no_progress_timeout,
        close_timeout,
        max_inbound_frame_bytes,
        max_inbound_message_bytes,
        ..
    } = limits;
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
    let termination = {
        let mut writer = Box::pin(
            AssertUnwindSafe(writer)
                .catch_unwind()
                .map(|result| result.map_err(|_| ())),
        );
        let mut reader = Box::pin(
            AssertUnwindSafe(reader)
                .catch_unwind()
                .map(|result| result.map_err(|_| ())),
        );
        tokio::select! {
            biased;
            result = &mut reader => {
                let read_result = result.unwrap_or(ReadResult::Error(WsErrorKind::Internal));
                complete_after_reader(
                    read_result,
                    peer_closed_snapshot.clone(),
                    writer.as_mut(),
                    close_timeout,
                ).await
            }
            result = &mut writer => {
                drop(reader);
                let write_result = result.unwrap_or(Some(WsErrorKind::Internal));
                finish_after_writer(write_result, &peer_closed_snapshot)
            }
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
/// On expiry the writer future is dropped and timeout participates in the
/// normal termination precedence rules. A poll panic is classified as
/// `Internal`, since it is not an underlying socket I/O failure.
async fn complete_after_reader(
    read: ReadResult,
    peer_closed: watch::Receiver<Option<Option<u16>>>,
    writer: Pin<&mut (impl Future<Output = Result<Option<WsErrorKind>, ()>> + ?Sized)>,
    close_timeout: Duration,
) -> WsTermination {
    let write = match tokio::time::timeout(close_timeout, writer).await {
        Ok(Ok(result)) => result,
        Ok(Err(())) => Some(WsErrorKind::Internal),
        Err(_) => Some(WsErrorKind::Timeout),
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
    St: Stream<Item = Result<Message, TungsteniteError>> + Unpin,
{
    let mut close_deadline = None;
    loop {
        if close.borrow().is_some() && close_deadline.is_none() {
            close_deadline = Some(tokio::time::Instant::now() + close_timeout);
        }
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
                    Some(Ok(message @ (Message::Text(_) | Message::Binary(_)))) => {
                        if close.borrow().is_some() {
                            continue;
                        }
                        let message = match message {
                            Message::Text(text) => WsMessage::Text(text.to_string()),
                            Message::Binary(bytes) => WsMessage::Binary(bytes),
                            _ => unreachable!("the match arm accepts application messages only"),
                        };
                        let result = enqueue_while_polling_control(
                            &mut stream,
                            inbound.clone(),
                            message,
                            &mut close,
                            &ping,
                            no_progress_timeout,
                            capacity,
                        ).await;
                        match result {
                            EnqueueResult::Queued => {},
                            EnqueueResult::Closed if close.borrow().is_some() => {
                                if close_deadline.is_none() {
                                    close_deadline = Some(tokio::time::Instant::now() + close_timeout);
                                }
                                continue;
                            }
                            EnqueueResult::Closed => return ReadResult::Error(WsErrorKind::Internal),
                            EnqueueResult::LocalClose => {
                                if close_deadline.is_none() {
                                    close_deadline = Some(tokio::time::Instant::now() + close_timeout);
                                }
                                continue;
                            }
                            EnqueueResult::PeerClosed(code) => {
                                let _ = peer_closed.send(Some(code));
                                return ReadResult::Closed(code);
                            }
                            EnqueueResult::Overflow => {
                                let _ = close_tx.send(Some(CloseRequest::SlowConsumer));
                                return ReadResult::Error(WsErrorKind::InboundBackpressure);
                            }
                            EnqueueResult::Stalled => {
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

/// Waits to enqueue one inbound message while continuing to poll control
/// frames.
///
/// Only the message passed to this function may wait outside the bounded queue.
/// If capacity becomes available it is queued; another text or binary message
/// arriving first returns [`EnqueueResult::Overflow`], while expiry of
/// `no_progress_timeout` returns [`EnqueueResult::Stalled`]. Ping, Pong, peer
/// Close, local close, and read errors retain their distinct outcomes so the
/// caller can preserve control-frame liveness and classify failures. The
/// slow-consumer caller asks the writer to send Close code `1013`; a write
/// failure may prevent the peer from receiving it.
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
    St: Stream<Item = Result<Message, TungsteniteError>> + Unpin,
{
    let push = inbound.push(message);
    tokio::pin!(push);
    let deadline = tokio::time::sleep(no_progress_timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            result = &mut push => return match result {
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
                        return EnqueueResult::Overflow;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return EnqueueResult::Error(map_error(&error, capacity)),
                    None => return EnqueueResult::Error(WsErrorKind::Protocol),
                }
            }
            _ = &mut deadline => return EnqueueResult::Stalled,
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
    S: Sink<Message, Error = TungsteniteError> + Unpin,
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

/// Sends one dequeued message while observing close and peer-control signals.
/// Dropping the envelope releases its reserved queue capacity.
#[allow(clippy::too_many_arguments)]
async fn send_envelope<S>(
    sink: &mut S,
    envelope: Envelope,
    close: &mut watch::Receiver<Option<CloseRequest>>,
    peer_closed: &mut watch::Receiver<Option<Option<u16>>>,
    ping: &Notify,
    no_progress_timeout: Duration,
    close_timeout: Duration,
    capacity: CapacityLimits,
) -> SendResult
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    let Envelope { item, _reservation } = envelope;
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

/// Flushes pending control output before `timeout`, classifying I/O and expiry.
async fn timeout_flush<S>(sink: &mut S, timeout: Duration, capacity: CapacityLimits) -> Option<WsErrorKind>
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
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
    S: Sink<Message, Error = TungsteniteError> + Unpin,
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
    S: Sink<Message, Error = TungsteniteError> + Unpin,
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
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    match tokio::time::timeout_at(deadline, sink.flush()).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(map_error(&error, capacity)),
        Err(_) => Some(WsErrorKind::Timeout),
    }
}

/// Maps a tungstenite failure to its stable, non-sensitive public category.
fn map_error(error: &TungsteniteError, limits: CapacityLimits) -> WsErrorKind {
    match error {
        // Tungstenite reports only max_size, not which check produced the error.
        // Equal configured limits are indistinguishable, so the frame limit wins that tie.
        TungsteniteError::Capacity(CapacityError::MessageTooLong { max_size, .. }) => match (
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
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use futures_util::Sink;
    use futures_util::Stream;
    use tokio::sync::Notify;
    use tokio::sync::watch;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    use super::BoundedQueue;
    use super::CapacityLimits;
    use super::CloseRequest;
    use super::DriverContext;
    use super::ReadResult;
    use super::SendResult;
    use super::TungsteniteError;
    use super::complete_after_reader;
    use super::enqueue_while_polling_control;
    use super::finish_after_writer;
    use super::read_loop;
    use super::resolve_termination;
    use super::run_parts;
    use super::send_envelope;
    use super::timeout_flush;
    use super::write_loop;
    use crate::error::WsErrorKind;
    use crate::error::WsTermination;
    use crate::options::WsSessionLimits;
    use crate::session::WsSession;
    use crate::session::internal::Owner;
    use crate::ws_message::WsMessage;

    #[tokio::test]
    async fn test_reader_ignores_application_data_after_local_close_but_observes_peer_close() {
        let inbound = BoundedQueue::new(1, 8);
        inbound.close();
        let (close_tx, close_rx) = watch::channel(Some(CloseRequest::Local));
        let (peer_closed, _) = watch::channel(None);
        let stream = futures_util::stream::iter([Ok(Message::Text("discarded".into())), Ok(Message::Close(None))]);

        let result = read_loop(
            stream,
            inbound,
            close_rx,
            close_tx.clone(),
            peer_closed,
            Arc::new(Notify::new()),
            Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityLimits::default(),
        )
        .await;

        assert!(matches!(result, ReadResult::Closed(None)));
        drop(close_tx);
    }

    #[tokio::test]
    async fn test_reader_queues_text_and_binary_through_one_application_path() {
        let inbound = BoundedQueue::new(2, 8);
        let (close_tx, close_rx) = watch::channel(None);
        let (peer_closed, _) = watch::channel(None);
        let stream = futures_util::stream::iter([
            Ok(Message::Text("text".into())),
            Ok(Message::Binary(bytes::Bytes::from_static(b"bin"))),
            Ok(Message::Close(Some(
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: CloseCode::Normal,
                    reason: "done".into(),
                },
            ))),
        ]);

        let result = read_loop(
            stream,
            inbound.clone(),
            close_rx,
            close_tx.clone(),
            peer_closed,
            Arc::new(Notify::new()),
            Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityLimits::default(),
        )
        .await;

        assert!(matches!(result, ReadResult::Closed(Some(1000))));
        assert_eq!(
            inbound.try_pop().expect("queued text").into_item(),
            WsMessage::Text("text".to_owned())
        );
        assert_eq!(
            inbound.try_pop().expect("queued binary").into_item(),
            WsMessage::Binary(bytes::Bytes::from_static(b"bin"))
        );
        drop(close_tx);
    }

    #[tokio::test]
    async fn test_reader_reports_unexpected_inbound_queue_close_as_internal_error() {
        let inbound = BoundedQueue::new(1, 8);
        inbound.close();
        let (close_tx, close_rx) = watch::channel(None);
        let (peer_closed, _) = watch::channel(None);
        let stream = futures_util::stream::iter([Ok(Message::Text("unexpected".into()))]);

        let result = read_loop(
            stream,
            inbound,
            close_rx,
            close_tx.clone(),
            peer_closed,
            Arc::new(Notify::new()),
            Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityLimits::default(),
        )
        .await;

        assert!(matches!(result, ReadResult::Error(WsErrorKind::Internal)));
        drop(close_tx);
    }

    #[tokio::test]
    async fn test_extra_inbound_message_reports_overflow_before_deadline() {
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

        assert!(matches!(result, super::EnqueueResult::Overflow));
        drop(close_tx);
    }

    #[tokio::test]
    async fn test_inbound_enqueue_reports_stalled_when_no_progress_deadline_expires() {
        let inbound = BoundedQueue::new(1, 8);
        inbound
            .try_push(WsMessage::Text("full".into()))
            .expect("the single queue slot is available");
        let mut stream = futures_util::stream::pending();
        let (close_tx, mut close_rx) = watch::channel(None);
        let ping = Notify::new();

        let result = enqueue_while_polling_control(
            &mut stream,
            inbound,
            WsMessage::Text("waiting".into()),
            &mut close_rx,
            &ping,
            Duration::from_millis(10),
            CapacityLimits::default(),
        )
        .await;

        assert!(matches!(result, super::EnqueueResult::Stalled));
        drop(close_tx);
    }

    #[tokio::test]
    async fn test_inbound_enqueue_queues_waiting_message_after_capacity_recovers() {
        let inbound = BoundedQueue::new(1, 8);
        inbound
            .try_push(WsMessage::Text("full".into()))
            .expect("the single queue slot is available");
        let mut stream = futures_util::stream::pending();
        let (close_tx, mut close_rx) = watch::channel(None);
        let ping = Arc::new(Notify::new());
        let queue_for_enqueue = inbound.clone();
        let enqueue = tokio::spawn(async move {
            enqueue_while_polling_control(
                &mut stream,
                queue_for_enqueue,
                WsMessage::Text("waiting".into()),
                &mut close_rx,
                &ping,
                Duration::from_secs(1),
                CapacityLimits::default(),
            )
            .await
        });

        assert_eq!(
            inbound.try_pop().expect("the original message is queued").into_item(),
            WsMessage::Text("full".into())
        );
        assert!(matches!(
            enqueue.await.expect("enqueue task exits"),
            super::EnqueueResult::Queued
        ));
        assert_eq!(
            inbound
                .try_pop()
                .expect("the waiting message is now queued")
                .into_item(),
            WsMessage::Text("waiting".into())
        );
        drop(close_tx);
    }

    #[tokio::test]
    async fn test_peer_close_flush_failure_reaches_session_api() {
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
            DriverContext {
                inbound: inbound.clone(),
                outbound: outbound.clone(),
                close: close_rx,
                close_tx,
                terminal: terminal.clone(),
                terminal_changed: terminal_changed.clone(),
                limits: WsSessionLimits {
                    max_inbound_frame_bytes: 16,
                    max_inbound_message_bytes: 16,
                    no_progress_timeout: Duration::from_secs(1),
                    close_timeout: Duration::from_secs(1),
                    ..WsSessionLimits::default()
                },
            },
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
    async fn test_closed_outbound_without_close_request_is_internal_error() {
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

    struct DropProbeStream(Arc<AtomicBool>);

    impl Stream for DropProbeStream {
        type Item = Result<Message, TungsteniteError>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl Drop for DropProbeStream {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct PanickingStream;

    impl Stream for PanickingStream {
        type Item = Result<Message, TungsteniteError>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            panic!("controlled reader poll panic");
        }
    }

    struct DropProbeSink {
        terminal: Arc<Mutex<Option<WsTermination>>>,
        dropped: Arc<AtomicBool>,
    }

    impl Sink<Message> for DropProbeSink {
        type Error = TungsteniteError;

        fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            unreachable!("the drop probe sink never becomes ready")
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    impl Drop for DropProbeSink {
        fn drop(&mut self) {
            assert!(
                self.terminal.lock().expect("terminal mutex poisoned").is_none(),
                "writer future must be dropped before terminal publication"
            );
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    struct TerminalDropProbeStream {
        terminal: Arc<Mutex<Option<WsTermination>>>,
        dropped: Arc<AtomicBool>,
    }

    impl Stream for TerminalDropProbeStream {
        type Item = Result<Message, TungsteniteError>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl Drop for TerminalDropProbeStream {
        fn drop(&mut self) {
            assert!(
                self.terminal.lock().expect("terminal mutex poisoned").is_none(),
                "reader future must be dropped before terminal publication"
            );
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn test_driver_drops_reader_before_publishing_writer_first_terminal() {
        let inbound = BoundedQueue::new(1, 16);
        let outbound = BoundedQueue::new(1, 16);
        outbound.close();
        let (_close_tx, close_rx) = watch::channel(None);
        let (close_tx, _close_keepalive) = watch::channel(None);
        let terminal = Arc::new(Mutex::new(None));
        let terminal_changed = Arc::new(Notify::new());
        let reader_dropped = Arc::new(AtomicBool::new(false));
        let sink = ControlledSink {
            state: Arc::new(Mutex::new(SinkState::default())),
            poll_ready_seen: Arc::new(Notify::new()),
            close_seen: Arc::new(Notify::new()),
        };

        run_parts(
            sink,
            DropProbeStream(reader_dropped.clone()),
            DriverContext {
                inbound,
                outbound,
                close: close_rx,
                close_tx,
                terminal: terminal.clone(),
                terminal_changed,
                limits: WsSessionLimits {
                    close_timeout: Duration::from_millis(10),
                    ..WsSessionLimits::default()
                },
            },
        )
        .await;

        assert_eq!(
            *terminal.lock().expect("terminal mutex poisoned"),
            Some(WsTermination::Error {
                kind: WsErrorKind::Internal
            })
        );
        assert!(reader_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_reader_poll_panic_is_published_as_internal_error() {
        let inbound = BoundedQueue::new(1, 16);
        let outbound = BoundedQueue::new(1, 16);
        let (_close_tx, close_rx) = watch::channel(None);
        let (close_tx, _close_keepalive) = watch::channel(None);
        let terminal = Arc::new(Mutex::new(None));
        let writer_dropped = Arc::new(AtomicBool::new(false));

        run_parts(
            DropProbeSink {
                terminal: terminal.clone(),
                dropped: writer_dropped.clone(),
            },
            PanickingStream,
            DriverContext {
                inbound,
                outbound,
                close: close_rx,
                close_tx,
                terminal: terminal.clone(),
                terminal_changed: Arc::new(Notify::new()),
                limits: WsSessionLimits {
                    close_timeout: Duration::from_millis(10),
                    ..WsSessionLimits::default()
                },
            },
        )
        .await;

        assert_eq!(
            *terminal.lock().expect("terminal mutex poisoned"),
            Some(WsTermination::Error {
                kind: WsErrorKind::Internal
            })
        );
        assert!(writer_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_writer_poll_panic_is_published_as_internal_error() {
        let inbound = BoundedQueue::new(1, 16);
        let outbound = BoundedQueue::new(1, 16);
        outbound
            .try_push(WsMessage::Text("outbound".to_owned()))
            .expect("outbound queue has capacity");
        let (_close_tx, close_rx) = watch::channel(None);
        let (close_tx, _close_keepalive) = watch::channel(None);
        let terminal = Arc::new(Mutex::new(None));
        let reader_dropped = Arc::new(AtomicBool::new(false));

        run_parts(
            ControlledSink {
                state: Arc::new(Mutex::new(SinkState {
                    panic_on_ready: true,
                    ..SinkState::default()
                })),
                poll_ready_seen: Arc::new(Notify::new()),
                close_seen: Arc::new(Notify::new()),
            },
            TerminalDropProbeStream {
                terminal: terminal.clone(),
                dropped: reader_dropped.clone(),
            },
            DriverContext {
                inbound,
                outbound,
                close: close_rx,
                close_tx,
                terminal: terminal.clone(),
                terminal_changed: Arc::new(Notify::new()),
                limits: WsSessionLimits {
                    close_timeout: Duration::from_millis(10),
                    ..WsSessionLimits::default()
                },
            },
        )
        .await;

        assert_eq!(
            *terminal.lock().expect("terminal mutex poisoned"),
            Some(WsTermination::Error {
                kind: WsErrorKind::Internal
            })
        );
        assert!(reader_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_reader_first_timeout_drops_writer_before_terminal_publication() {
        let inbound = BoundedQueue::new(1, 16);
        let outbound = BoundedQueue::new(1, 16);
        let (_close_tx, close_rx) = watch::channel(None);
        let (close_tx, _close_keepalive) = watch::channel(None);
        let terminal = Arc::new(Mutex::new(None));
        let writer_dropped = Arc::new(AtomicBool::new(false));

        run_parts(
            DropProbeSink {
                terminal: terminal.clone(),
                dropped: writer_dropped.clone(),
            },
            futures_util::stream::iter([Ok(Message::Close(None))]),
            DriverContext {
                inbound,
                outbound,
                close: close_rx,
                close_tx,
                terminal: terminal.clone(),
                terminal_changed: Arc::new(Notify::new()),
                limits: WsSessionLimits {
                    close_timeout: Duration::from_millis(10),
                    ..WsSessionLimits::default()
                },
            },
        )
        .await;

        assert_eq!(
            *terminal.lock().expect("terminal mutex poisoned"),
            Some(WsTermination::Error {
                kind: WsErrorKind::Timeout
            })
        );
        assert!(writer_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_closed_close_signal_without_request_is_internal_error() {
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
    async fn test_peer_close_waits_for_writer_result_before_selecting_termination() {
        let mut writer = Box::pin(async { Ok(Some(WsErrorKind::Transport)) });
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                writer.as_mut(),
                Duration::from_secs(1)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Transport
            }
        );
    }

    #[tokio::test]
    async fn test_complete_after_reader_uses_close_snapshot_updated_during_writer_wait() {
        let (peer_closed_tx, peer_closed) = watch::channel(None);
        let (writer_release_tx, writer_release_rx) = tokio::sync::oneshot::channel();
        let (writer_started_tx, writer_started_rx) = tokio::sync::oneshot::channel();
        let mut writer = Box::pin(async move {
            writer_started_tx.send(()).expect("completion is waiting");
            let _ = writer_release_rx.await;
            Ok(None)
        });
        let completion = tokio::spawn(async move {
            complete_after_reader(
                ReadResult::LocalClose,
                peer_closed,
                writer.as_mut(),
                Duration::from_secs(1),
            )
            .await
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
    fn test_writer_first_uses_peer_close_snapshot() {
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
    async fn test_peer_close_drops_writer_when_close_wait_expires() {
        let mut writer = Box::pin(std::future::pending::<Result<Option<WsErrorKind>, ()>>());
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                writer.as_mut(),
                Duration::from_millis(10)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Timeout
            }
        );
    }

    #[tokio::test]
    async fn test_writer_panic_is_internal_error() {
        let mut writer = Box::pin(async { Err(()) });
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                writer.as_mut(),
                Duration::from_secs(1)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Internal
            }
        );
    }

    #[test]
    fn test_termination_resolution_preserves_close_codes_and_error_precedence() {
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
        panic_on_ready: bool,
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
        type Error = TungsteniteError;

        fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().expect("sink state poisoned");
            state.events.push("poll_ready");
            self.poll_ready_seen.notify_one();
            assert!(!state.panic_on_ready, "controlled writer poll panic");
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
                FlushBehavior::Error => Poll::Ready(Err(TungsteniteError::Io(std::io::Error::other(
                    "controlled flush failure",
                )))),
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
    async fn test_controlled_flush_failure_is_transport_error() {
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
            Box::pin(
                async move { Ok(timeout_flush(&mut sink, Duration::from_secs(1), CapacityLimits::default()).await) },
            );
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                writer.as_mut(),
                Duration::from_secs(1)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Transport
            }
        );
    }

    #[tokio::test]
    async fn test_controlled_pending_flush_is_timeout() {
        let state = Arc::new(Mutex::new(SinkState {
            flush: FlushBehavior::Pending,
            ..SinkState::default()
        }));
        let mut sink = ControlledSink {
            state,
            poll_ready_seen: Arc::new(Notify::new()),
            close_seen: Arc::new(Notify::new()),
        };
        let mut writer = Box::pin(async move {
            Ok(timeout_flush(&mut sink, Duration::from_millis(10), CapacityLimits::default()).await)
        });
        assert_eq!(
            complete_after_reader(
                ReadResult::Closed(Some(1000)),
                watch::channel(None).1,
                writer.as_mut(),
                Duration::from_secs(1)
            )
            .await,
            WsTermination::Error {
                kind: WsErrorKind::Timeout
            }
        );
    }

    #[tokio::test]
    async fn test_ping_flush_interrupts_pending_business_poll_ready() {
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
    async fn test_local_close_during_pending_business_write_waits_for_peer_ack() {
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
