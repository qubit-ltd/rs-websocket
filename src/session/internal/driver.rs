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
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::Sink;
use futures_util::Stream;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::capacity_limits::CapacityLimits;
use super::driver_context::DriverContext;
use super::read_result::ReadResult;
use super::session_failure::SessionFailure;
use super::terminal_state::TerminalState;
use crate::error::WsErrorKind;
use crate::error::WsTermination;
use crate::options::WsSessionLimits;

mod close;
mod reader;
mod writer;

#[cfg(test)]
use close::finish_writer;
#[cfg(test)]
use close::timeout_flush;
#[cfg(test)]
use reader::enqueue_while_polling_control;
use reader::read_loop;
#[cfg(test)]
use writer::ping_flush_until;
#[cfg(test)]
use writer::send_envelope;
use writer::write_loop;

#[cfg(test)]
use super::bounded_queue::BoundedQueue;
#[cfg(test)]
use super::close_action::CloseAction;
#[cfg(test)]
use super::close_request::CloseRequest;
#[cfg(test)]
use super::enqueue_result::EnqueueResult;
#[cfg(test)]
use super::envelope::Envelope;
#[cfg(test)]
use super::ping_flush_result;
#[cfg(test)]
use super::send_result::SendResult;

/// Represents the concrete WebSocket stream handled by the session driver.
type WsSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Runs the socket lifecycle while transferring each queue and signal to its
/// reader or writer loop. The driver publishes a terminal state only after
/// their outcomes have been combined; a reader exit waits a bounded time for
/// the writer before publication.
///
/// # Parameters
///
/// - `socket`: Connected WebSocket stream whose lifecycle the driver manages.
/// - `context`: Queues, lifecycle signals, and configured session limits.
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
///
/// # Type Parameters
///
/// - `S`: Unpinned sink type that writes WebSocket messages.
/// - `St`: Unpinned stream type that yields WebSocket messages or errors.
///
/// # Parameters
///
/// - `sink`: Split WebSocket sink transferred to the writer loop.
/// - `stream`: Split WebSocket stream transferred to the reader loop.
/// - `context`: Queues, lifecycle signals, and limits shared with the loops.
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
                let read_result = result.unwrap_or(ReadResult::Error(SessionFailure::new(WsErrorKind::Internal)));
                outbound.close();
                complete_after_reader(
                    read_result,
                    peer_closed_snapshot.clone(),
                    writer.as_mut(),
                    close_timeout,
                ).await
            }
            result = &mut writer => {
                drop(reader);
                let write_result = result.unwrap_or(Some(SessionFailure::new(WsErrorKind::Internal)));
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
///
/// # Parameters
///
/// - `read`: Reader outcome observed before writer completion.
/// - `write`: Writer failure, if the writer did not complete successfully.
/// - `peer_closed`: Latest peer Close snapshot; the outer `Option`
///   distinguishes no observed Close from a Close without a status code.
fn resolve_termination(
    read: ReadResult,
    write: Option<SessionFailure>,
    peer_closed: Option<Option<u16>>,
) -> TerminalState {
    match (read, write) {
        (ReadResult::Error(failure), _) => TerminalState::failure(failure),
        (ReadResult::Closed(code), None) => TerminalState::new(WsTermination::Closed { code }),
        (_, Some(failure)) => TerminalState::failure(failure),
        (ReadResult::LocalClose, None) => match peer_closed {
            Some(code) => TerminalState::new(WsTermination::Closed { code }),
            None => TerminalState::error(WsErrorKind::Internal),
        },
    }
}

/// Resolves a writer-first exit against the peer Close snapshot observed so
/// far.
///
/// # Parameters
///
/// - `write`: Writer failure, if any.
/// - `peer_closed`: Watch receiver holding the latest peer Close snapshot.
fn finish_after_writer(
    write: Option<SessionFailure>,
    peer_closed: &watch::Receiver<Option<Option<u16>>>,
) -> TerminalState {
    let peer_closed = { *peer_closed.borrow() };
    resolve_termination(ReadResult::LocalClose, write, peer_closed)
}

/// Waits at most `close_timeout` for the writer after the reader has stopped.
///
/// On expiry the writer future is dropped and timeout participates in the
/// normal termination precedence rules. A poll panic is classified as
/// `Internal`, since it is not an underlying socket I/O failure.
///
/// # Parameters
///
/// - `read`: Reader outcome to combine with the writer result.
/// - `peer_closed`: Watch receiver used to snapshot the peer's Close status.
/// - `writer`: Pinned writer future; it is dropped if the deadline expires.
/// - `close_timeout`: Maximum time allowed for the writer to finish.
async fn complete_after_reader(
    read: ReadResult,
    peer_closed: watch::Receiver<Option<Option<u16>>>,
    writer: Pin<&mut (impl Future<Output = Result<Option<SessionFailure>, ()>> + ?Sized)>,
    close_timeout: Duration,
) -> TerminalState {
    let write = match tokio::time::timeout(close_timeout, writer).await {
        Ok(Ok(result)) => result,
        Ok(Err(())) => Some(SessionFailure::new(WsErrorKind::Internal)),
        Err(_) => Some(SessionFailure::new(WsErrorKind::Timeout)),
    };
    let peer_closed_snapshot = { *peer_closed.borrow() };
    resolve_termination(read, write, peer_closed_snapshot)
}

#[cfg(test)]
mod driver_tests;
