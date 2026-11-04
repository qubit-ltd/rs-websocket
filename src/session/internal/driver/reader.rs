// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use futures_util::Stream;
use futures_util::StreamExt;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::super::bounded_queue::BoundedQueue;
use super::super::capacity_limits::CapacityLimits;
use super::super::close_request::CloseRequest;
use super::super::enqueue_result::EnqueueResult;
use super::super::read_result::ReadResult;
use super::super::session_failure::SessionFailure;
use super::close::checked_deadline;
use crate::error::WsErrorKind;
use crate::ws_message::WsMessage;

const CONTROL_FRAME_POLL_BUDGET: usize = 64;

/// Reads peer messages and control frames, coordinating close requests and
/// reporting the reader's outcome to the lifecycle driver.
#[allow(clippy::too_many_arguments)]
pub(super) async fn read_loop<St>(
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
    let mut frames_since_yield = 0;
    loop {
        if close.borrow().is_some() && close_deadline.is_none() {
            close_deadline = match checked_deadline(close_timeout) {
                Ok(deadline) => Some(deadline),
                Err(kind) => return ReadResult::Error(kind.into()),
            };
        }
        tokio::select! {
            changed = close.changed() => {
                if changed.is_err() { return ReadResult::LocalClose; }
                if close.borrow().is_some() && close_deadline.is_none() {
                    close_deadline = match checked_deadline(close_timeout) {
                        Ok(deadline) => Some(deadline),
                        Err(kind) => return ReadResult::Error(kind.into()),
                    };
                }
            }
            _ = async {
                if let Some(deadline) = close_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    pending::<()>().await;
                }
            } => return ReadResult::Error(WsErrorKind::Timeout.into()),
            incoming = stream.next() => {
                frames_since_yield += 1;
                if frames_since_yield >= CONTROL_FRAME_POLL_BUDGET {
                    frames_since_yield = 0;
                    tokio::task::yield_now().await;
                }
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
                                        Err(kind) => return ReadResult::Error(kind.into()),
                                    };
                                }
                                continue;
                            }
                            EnqueueResult::Closed => return ReadResult::Error(WsErrorKind::Internal.into()),
                            EnqueueResult::LocalClose => {
                                if close_deadline.is_none() {
                                    close_deadline = match checked_deadline(close_timeout) {
                                        Ok(deadline) => Some(deadline),
                                        Err(kind) => return ReadResult::Error(kind.into()),
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
                                return ReadResult::Error(WsErrorKind::InboundBackpressure.into());
                            }
                            EnqueueResult::Stalled => {
                                let _ = close_tx.send(Some(CloseRequest::SlowConsumer));
                                return ReadResult::Error(WsErrorKind::Timeout.into());
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
                        return ReadResult::Error(SessionFailure::from_tungstenite(&error, capacity));
                    }
                    None => {
                        let _ = close_tx.send(Some(CloseRequest::Local));
                        return ReadResult::Error(WsErrorKind::Protocol.into());
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
pub(super) async fn enqueue_while_polling_control<St>(
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
        Err(kind) => return EnqueueResult::Error(kind.into()),
    };
    let deadline = tokio::time::sleep_until(deadline_at);
    tokio::pin!(deadline);
    let mut frames_since_yield = 0;
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
                frames_since_yield += 1;
                if frames_since_yield >= CONTROL_FRAME_POLL_BUDGET {
                    frames_since_yield = 0;
                    tokio::task::yield_now().await;
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
                    Some(Err(error)) => return EnqueueResult::Error(SessionFailure::from_tungstenite(&error, capacity)),
                    None => return EnqueueResult::Error(WsErrorKind::Protocol.into()),
                }
            }
        }
    }
}
