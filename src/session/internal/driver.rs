// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::io::ErrorKind;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::Sink;
use futures_util::SinkExt;
use futures_util::Stream;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::error::CapacityError;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use super::bounded_queue::BoundedQueue;
use super::capacity_limits::CapacityLimits;
use super::close_request::CloseRequest;
use super::driver_context::DriverContext;
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
type WsSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
            close_deadline = match checked_deadline(close_timeout) {
                Ok(deadline) => Some(deadline),
                Err(kind) => return ReadResult::Error(kind),
            };
        }
        tokio::select! {
            changed = close.changed() => {
                if changed.is_err() { return ReadResult::LocalClose; }
                if close.borrow().is_some() && close_deadline.is_none() {
                    close_deadline = match checked_deadline(close_timeout) {
                        Ok(deadline) => Some(deadline),
                        Err(kind) => return ReadResult::Error(kind),
                    };
                }
            }
            _ = async {
                if let Some(deadline) = close_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    pending::<()>().await;
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
                                    close_deadline = match checked_deadline(close_timeout) {
                                        Ok(deadline) => Some(deadline),
                                        Err(kind) => return ReadResult::Error(kind),
                                    };
                                }
                                continue;
                            }
                            EnqueueResult::Closed => return ReadResult::Error(WsErrorKind::Internal),
                            EnqueueResult::LocalClose => {
                                if close_deadline.is_none() {
                                    close_deadline = match checked_deadline(close_timeout) {
                                        Ok(deadline) => Some(deadline),
                                        Err(kind) => return ReadResult::Error(kind),
                                    };
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
    let deadline_at = match checked_deadline(no_progress_timeout) {
        Ok(deadline) => deadline,
        Err(kind) => return EnqueueResult::Error(kind),
    };
    let deadline = tokio::time::sleep_until(deadline_at);
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
            _ = &mut deadline => return EnqueueResult::Stalled,
            incoming = stream.next() => {
                if Instant::now() >= deadline_at {
                    return EnqueueResult::Stalled;
                }
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
    let deadline = match checked_deadline(no_progress_timeout) {
        Ok(deadline) => deadline,
        Err(kind) => return SendResult::Exit(Some(kind)),
    };

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
            result = tokio::time::timeout_at(deadline, poll_fn(|context| {
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
    let deadline = match checked_deadline(timeout) {
        Ok(deadline) => deadline,
        Err(kind) => return Some(kind),
    };
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
    let deadline = match checked_deadline(timeout) {
        Ok(deadline) => deadline,
        Err(kind) => return Some(kind),
    };
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

/// Creates a deadline relative to the current Tokio clock.
///
/// # Parameters
///
/// - `timeout`: Duration to add to the current runtime instant.
///
/// # Returns
///
/// The instant at which the operation deadline expires.
///
/// # Errors
///
/// Returns `InvalidConfig` if adding `timeout` overflows the runtime clock.
/// This function does not panic on overflow.
fn checked_deadline(timeout: Duration) -> Result<Instant, WsErrorKind> {
    Instant::now().checked_add(timeout).ok_or(WsErrorKind::InvalidConfig)
}

/// Flushes pending output before an existing operation deadline.
async fn timeout_flush_until<S>(sink: &mut S, deadline: Instant, capacity: CapacityLimits) -> Option<WsErrorKind>
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
        TungsteniteError::Protocol(_) | TungsteniteError::Utf8(_) => WsErrorKind::Protocol,
        TungsteniteError::ConnectionClosed | TungsteniteError::AlreadyClosed => WsErrorKind::Protocol,
        TungsteniteError::Io(source) if source.kind() == ErrorKind::TimedOut => WsErrorKind::Timeout,
        _ => WsErrorKind::Transport,
    }
}

#[cfg(test)]
mod tests;
