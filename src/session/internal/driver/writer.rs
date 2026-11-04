// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::poll_fn;
use std::sync::Arc;
use std::time::Duration;

use futures_util::Sink;
use futures_util::SinkExt;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::super::bounded_queue::BoundedQueue;
use super::super::capacity_limits::CapacityLimits;
use super::super::close_request::CloseRequest;
use super::super::envelope::Envelope;
use super::super::ping_flush_event::PingFlushEvent;
use super::super::ping_flush_result::PingFlushResult;
use super::super::send_event::SendEvent;
use super::super::send_result::SendResult;
use super::super::session_failure::SessionFailure;
use super::super::writer_work::WriterWork;
use super::close::CloseAction;
use super::close::checked_deadline;
use super::close::finish_writer;
use super::close::timeout_flush_until;
use crate::error::WsErrorKind;
use crate::ws_message::WsMessage;

/// Writes queued messages and control frames, then waits for peer confirmation
/// when performing a local close handshake.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_loop<S>(
    mut sink: S,
    outbound: BoundedQueue,
    mut close: watch::Receiver<Option<CloseRequest>>,
    mut peer_closed: watch::Receiver<Option<Option<u16>>>,
    ping: Arc<Notify>,
    no_progress_timeout: Duration,
    close_timeout: Duration,
    capacity: CapacityLimits,
) -> Option<SessionFailure>
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    let mut prefer_ping = true;
    loop {
        tokio::select! {
            biased;
            changed = peer_closed.changed() => {
                if changed.is_err() {
                    return finish_writer(&mut sink, CloseAction::PeerWatchEnded, &mut peer_closed, close_timeout, capacity).await;
                }
                if peer_closed.borrow().is_some() {
                    return finish_writer(&mut sink, CloseAction::PeerClosed, &mut peer_closed, close_timeout, capacity).await;
                }
            }
            changed = close.changed() => {
                if changed.is_err() { return Some(WsErrorKind::Internal.into()); }
                let request = *close.borrow();
                match request {
                    Some(CloseRequest::Local) => {
                        return finish_writer(
                            &mut sink,
                            CloseAction::Local,
                            &mut peer_closed,
                            close_timeout,
                            capacity,
                        )
                        .await;
                    }
                    Some(CloseRequest::SlowConsumer) => {
                        return finish_writer(
                            &mut sink,
                            CloseAction::SlowConsumer,
                            &mut peer_closed,
                            close_timeout,
                            capacity,
                        )
                        .await;
                    }
                    None => {}
                }
            }
            work = select_writer_work(&outbound, &ping, prefer_ping) => {
                match work {
                    WriterWork::Ping => {
                        let deadline = match checked_deadline(no_progress_timeout) {
                            Ok(deadline) => deadline,
                            Err(kind) => return Some(kind.into()),
                        };
                        match ping_flush_until(
                            &mut sink,
                            deadline,
                            &mut close,
                            &mut peer_closed,
                            capacity,
                        )
                        .await
                        {
                            PingFlushResult::Flushed { error: Some(kind) } => return Some(kind),
                            PingFlushResult::Flushed { error: None } => {
                                prefer_ping = false;
                            }
                            PingFlushResult::CloseChanged { request: None } => {}
                            PingFlushResult::PeerClosed => {
                                if peer_closed.borrow().is_none() {
                                    return finish_writer(&mut sink, CloseAction::PeerWatchEnded, &mut peer_closed, close_timeout, capacity).await;
                                }
                                return finish_writer(
                                    &mut sink,
                                    CloseAction::PeerClosed,
                                    &mut peer_closed,
                                    close_timeout,
                                    capacity,
                                )
                                .await;
                            }
                            PingFlushResult::CloseChanged {
                                request: Some(CloseRequest::Local),
                            } => {
                                return finish_writer(
                                    &mut sink,
                                    CloseAction::Local,
                                    &mut peer_closed,
                                    close_timeout,
                                    capacity,
                                )
                                .await;
                            }
                            PingFlushResult::CloseChanged {
                                request: Some(CloseRequest::SlowConsumer),
                            } => {
                                return finish_writer(
                                    &mut sink,
                                    CloseAction::SlowConsumer,
                                    &mut peer_closed,
                                    close_timeout,
                                    capacity,
                                )
                                .await;
                            }
                            PingFlushResult::CloseWatchClosed => {
                                return Some(WsErrorKind::Internal.into());
                            }
                        }
                    }
                    WriterWork::Outgoing(outgoing) => {
                        let Some(envelope) = outgoing else {
                            // Queue closure without a close request or peer Close is
                            // an impossible lifecycle transition.
                            return Some(WsErrorKind::Internal.into());
                        };
                        prefer_ping = true;
                        match send_envelope(
                            &mut sink,
                            envelope,
                            &mut close,
                            &mut peer_closed,
                            &ping,
                            no_progress_timeout,
                            close_timeout,
                            capacity,
                        )
                        .await
                        {
                            SendResult::Sent => {}
                            SendResult::Exit(kind) => return kind,
                        }
                    }
                }
            }
        }
    }
}

/// Selects Ping or outbound work with alternating priority when both are
/// ready. Lifecycle watches remain in the caller's higher-priority select.
pub(super) async fn select_writer_work(outbound: &BoundedQueue, ping: &Notify, prefer_ping: bool) -> WriterWork {
    if prefer_ping {
        tokio::select! {
            biased;
            _ = ping.notified() => WriterWork::Ping,
            outgoing = outbound.pop() => WriterWork::Outgoing(outgoing),
        }
    } else {
        tokio::select! {
            biased;
            outgoing = outbound.pop() => WriterWork::Outgoing(outgoing),
            _ = ping.notified() => WriterWork::Ping,
        }
    }
}

/// Sends one dequeued message while observing close and peer-control signals.
/// Dropping the envelope releases its reserved queue capacity.
#[allow(clippy::too_many_arguments)]
pub(super) async fn send_envelope<S>(
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
    let mut prefer_progress = true;
    let deadline = match checked_deadline(no_progress_timeout) {
        Ok(deadline) => deadline,
        Err(kind) => return SendResult::Exit(Some(kind.into())),
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
            event = select_send_progress(sink, ping, deadline, started, prefer_progress) => event,
        };

        match event {
            SendEvent::PeerClosed => {
                if peer_closed.borrow().is_some() {
                    return SendResult::Exit(
                        finish_writer(sink, CloseAction::PeerClosed, peer_closed, close_timeout, capacity).await,
                    );
                }
                return SendResult::Exit(
                    finish_writer(sink, CloseAction::PeerWatchEnded, peer_closed, close_timeout, capacity).await,
                );
            }
            SendEvent::Close(Some(CloseRequest::Local)) | SendEvent::Close(None) => {
                return SendResult::Exit(
                    finish_writer(sink, CloseAction::Local, peer_closed, close_timeout, capacity).await,
                );
            }
            SendEvent::Close(Some(CloseRequest::SlowConsumer)) => {
                return SendResult::Exit(
                    finish_writer(sink, CloseAction::SlowConsumer, peer_closed, close_timeout, capacity).await,
                );
            }
            SendEvent::Ping => match ping_flush_until(sink, deadline, close, peer_closed, capacity).await {
                PingFlushResult::Flushed { error: Some(kind) } => {
                    return SendResult::Exit(Some(kind));
                }
                PingFlushResult::Flushed { error: None } => {
                    prefer_progress = true;
                    if started {
                        return SendResult::Sent;
                    }
                }
                PingFlushResult::PeerClosed => {
                    if peer_closed.borrow().is_none() {
                        return SendResult::Exit(
                            finish_writer(sink, CloseAction::PeerWatchEnded, peer_closed, close_timeout, capacity)
                                .await,
                        );
                    }
                    return SendResult::Exit(
                        finish_writer(sink, CloseAction::PeerClosed, peer_closed, close_timeout, capacity).await,
                    );
                }
                PingFlushResult::CloseChanged {
                    request: Some(CloseRequest::Local),
                }
                | PingFlushResult::CloseChanged { request: None }
                | PingFlushResult::CloseWatchClosed => {
                    return SendResult::Exit(
                        finish_writer(sink, CloseAction::Local, peer_closed, close_timeout, capacity).await,
                    );
                }
                PingFlushResult::CloseChanged {
                    request: Some(CloseRequest::SlowConsumer),
                } => {
                    return SendResult::Exit(
                        finish_writer(sink, CloseAction::SlowConsumer, peer_closed, close_timeout, capacity).await,
                    );
                }
            },
            SendEvent::Progress(Err(_)) => {
                return SendResult::Exit(Some(WsErrorKind::Timeout.into()));
            }
            SendEvent::Progress(Ok(Err(error))) => {
                return SendResult::Exit(Some(SessionFailure::from_tungstenite(&error, capacity)));
            }
            SendEvent::Progress(Ok(Ok(()))) if started => return SendResult::Sent,
            SendEvent::Progress(Ok(Ok(()))) => {
                if let Err(error) = sink.start_send_unpin(message.take().expect("message not yet sent")) {
                    return SendResult::Exit(Some(SessionFailure::from_tungstenite(&error, capacity)));
                }
                started = true;
                prefer_progress = false;
            }
        }
    }
}

/// Chooses the next sink progress or Ping poll, alternating only their order.
pub(super) async fn select_send_progress<S>(
    sink: &mut S,
    ping: &Notify,
    deadline: Instant,
    started: bool,
    prefer_progress: bool,
) -> SendEvent
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    if prefer_progress {
        tokio::select! {
            biased;
            result = tokio::time::timeout_at(deadline, poll_fn(|context| {
                if started { sink.poll_flush_unpin(context) } else { sink.poll_ready_unpin(context) }
            })) => SendEvent::Progress(result),
            _ = ping.notified() => SendEvent::Ping,
        }
    } else {
        tokio::select! {
            biased;
            _ = ping.notified() => SendEvent::Ping,
            result = tokio::time::timeout_at(deadline, poll_fn(|context| {
                if started { sink.poll_flush_unpin(context) } else { sink.poll_ready_unpin(context) }
            })) => SendEvent::Progress(result),
        }
    }
}

/// Flushes Ping output until its existing deadline while keeping close signals
/// live.
pub(super) async fn ping_flush_until<S>(
    sink: &mut S,
    deadline: Instant,
    close: &mut watch::Receiver<Option<CloseRequest>>,
    peer_closed: &mut watch::Receiver<Option<Option<u16>>>,
    capacity: CapacityLimits,
) -> PingFlushResult
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    loop {
        let event = tokio::select! {
            biased;
            changed = peer_closed.changed() => {
                if changed.is_err() || peer_closed.borrow().is_some() {
                    PingFlushEvent::PeerClosed
                } else {
                    PingFlushEvent::PeerUpdated
                }
            }
            changed = close.changed() => {
                if changed.is_err() {
                    PingFlushEvent::CloseWatchClosed
                } else {
                    PingFlushEvent::CloseChanged {
                        request: *close.borrow(),
                    }
                }
            }
            result = timeout_flush_until(sink, deadline, capacity) => {
                PingFlushEvent::Flushed { error: result }
            }
        };
        match event {
            PingFlushEvent::PeerClosed => return PingFlushResult::PeerClosed,
            PingFlushEvent::PeerUpdated => continue,
            PingFlushEvent::CloseChanged { request } => {
                return PingFlushResult::CloseChanged { request };
            }
            PingFlushEvent::CloseWatchClosed => return PingFlushResult::CloseWatchClosed,
            PingFlushEvent::Flushed { error } => return PingFlushResult::Flushed { error },
        }
    }
}
