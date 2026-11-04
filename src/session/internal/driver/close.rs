// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use futures_util::Sink;
use futures_util::SinkExt;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use super::super::capacity_limits::CapacityLimits;
use super::super::session_failure::SessionFailure;
use crate::error::WsErrorKind;

/// Close action selected from the peer and local lifecycle signals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CloseAction {
    /// A peer Close frame was observed; flush the pending acknowledgement.
    PeerClosed,
    /// The peer-close watch ended without a Close snapshot; preserve flush-only
    /// behavior.
    PeerWatchEnded,
    /// Perform the local Close handshake.
    Local,
    /// Attempt the best-effort `1013` slow-consumer Close.
    SlowConsumer,
}

/// Performs the writer-side close action selected by the driver.
///
/// `PeerClosed` flushes the pending acknowledgement after a peer Close was
/// observed. `PeerWatchEnded` retains the flush-only path when the watch ends
/// without a Close snapshot; this does not prove that a peer Close was
/// observed. `Local` sends a Close frame and waits for peer confirmation
/// within the deadline. `SlowConsumer` attempts a best-effort `1013` Close.
///
/// `None` means this writer-side action did not report a failure; it does not
/// by itself mean the whole close handshake succeeded. `Some(SessionFailure)`
/// preserves the failure classification and safe diagnostic for this action.
/// Even when the `SlowConsumer` action successfully sends its `1013` Close, it
/// reports `Timeout`; the driver determines the final session outcome by
/// combining the reader and writer results.
pub(super) async fn finish_writer<S>(
    sink: &mut S,
    action: CloseAction,
    peer_closed: &mut watch::Receiver<Option<Option<u16>>>,
    timeout: Duration,
    capacity: CapacityLimits,
) -> Option<SessionFailure>
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    match action {
        CloseAction::PeerClosed | CloseAction::PeerWatchEnded => timeout_flush(sink, timeout, capacity).await,
        CloseAction::Local => close_sink_and_wait(sink, peer_closed, timeout, capacity).await,
        CloseAction::SlowConsumer => send_slow_consumer_close(sink, timeout, capacity).await,
    }
}

/// Flushes pending control output before `timeout`, classifying I/O and expiry.
pub(super) async fn timeout_flush<S>(
    sink: &mut S,
    timeout: Duration,
    capacity: CapacityLimits,
) -> Option<SessionFailure>
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    match tokio::time::timeout(timeout, sink.flush()).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(SessionFailure::from_tungstenite(&error, capacity)),
        Err(_) => Some(WsErrorKind::Timeout.into()),
    }
}

/// Flushes pending output, then sends a `1013` close within one shared
/// deadline.
pub(super) async fn send_slow_consumer_close<S>(
    sink: &mut S,
    timeout: Duration,
    capacity: CapacityLimits,
) -> Option<SessionFailure>
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    let deadline = match checked_deadline(timeout) {
        Ok(deadline) => deadline,
        Err(kind) => return Some(kind.into()),
    };
    if let Some(kind) = timeout_flush_until(sink, deadline, capacity).await {
        return Some(kind);
    }
    let close = Message::Close(Some(CloseFrame {
        code: CloseCode::Again,
        reason: "queue full".into(),
    }));
    match tokio::time::timeout_at(deadline, sink.send(close)).await {
        Ok(Ok(())) => Some(WsErrorKind::Timeout.into()),
        Ok(Err(error)) => Some(SessionFailure::from_tungstenite(&error, capacity)),
        Err(_) => Some(WsErrorKind::Timeout.into()),
    }
}

/// Sends the local Close frame and waits for peer confirmation by one deadline.
pub(super) async fn close_sink_and_wait<S>(
    sink: &mut S,
    peer_closed: &mut watch::Receiver<Option<Option<u16>>>,
    timeout: Duration,
    capacity: CapacityLimits,
) -> Option<SessionFailure>
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    let deadline = match checked_deadline(timeout) {
        Ok(deadline) => deadline,
        Err(kind) => return Some(kind.into()),
    };
    match tokio::time::timeout_at(deadline, sink.close()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Some(SessionFailure::from_tungstenite(&error, capacity)),
        Err(_) => return Some(WsErrorKind::Timeout.into()),
    }

    loop {
        if peer_closed.borrow().is_some() {
            return None;
        }
        match tokio::time::timeout_at(deadline, peer_closed.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return Some(WsErrorKind::Timeout.into()),
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
pub(super) fn checked_deadline(timeout: Duration) -> Result<Instant, WsErrorKind> {
    Instant::now().checked_add(timeout).ok_or(WsErrorKind::InvalidConfig)
}

/// Flushes pending output before an existing operation deadline.
pub(super) async fn timeout_flush_until<S>(
    sink: &mut S,
    deadline: Instant,
    capacity: CapacityLimits,
) -> Option<SessionFailure>
where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    match tokio::time::timeout_at(deadline, sink.flush()).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(SessionFailure::from_tungstenite(&error, capacity)),
        Err(_) => Some(WsErrorKind::Timeout.into()),
    }
}
