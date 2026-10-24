// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::error::WsSendError;
use crate::session::internal::Owner;
use crate::ws_message::WsMessage;

/// A cloneable handle for bounded asynchronous sends.
///
/// Each clone shares the session's outbound count and byte budgets. Successful
/// enqueueing only transfers a message into that queue; it does not confirm
/// delivery to the peer. [`Self::try_send`] returns
/// [`WsSendError::Backpressure`] when admitted messages leave insufficient
/// capacity. [`Self::send`] waits until both budgets are available and does
/// not reserve either while waiting. Admitted messages keep their reservations
/// while the driver writes them. Concurrent waiters have no strict FIFO or
/// starvation guarantee, and each waiting future retains its payload.
/// Dropping the receiving [`crate::WsSession`] closes the outbound queue, after
/// which sends return [`WsSendError::Closed`], including for oversized
/// messages. [`WsSendError::MessageTooLarge`] applies only while the queue is
/// open.
///
/// # Examples
///
/// ```no_run
/// use qubit_websocket::{WsClient, WsMessage};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let client = WsClient::builder().build()?;
///     let session = client.connect("ws://127.0.0.1:9000/events").await?;
///     let sender = session.sender();
///     match sender.send(WsMessage::Text("subscribe".into())).await {
///         Ok(()) => println!("message queued"),
///         Err(error) => println!("message was not queued: {error:?}"),
///     }
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct WsSender {
    /// Shared session owner that keeps the outbound queue and its limits alive.
    pub(super) owner: Arc<Owner>,
}

impl WsSender {
    /// Enqueues a message immediately when both outbound limits permit it.
    ///
    /// # Parameters
    ///
    /// - `message`: application message to place in the outbound queue.
    ///
    /// # Returns
    ///
    /// `Ok(())` means the message was queued, not delivered.
    ///
    /// # Errors
    ///
    /// Returns [`WsSendError::Closed`] after session shutdown. While the
    /// session is open, returns [`WsSendError::MessageTooLarge`] if the payload
    /// exceeds the byte limit, or [`WsSendError::Backpressure`] if admitted
    /// messages leave insufficient message-count or payload-byte capacity.
    /// Waiting sends do not reserve either budget, and admitted messages keep
    /// their reservations while the driver writes them.
    #[inline]
    pub fn try_send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.try_push(message)
    }

    /// Waits until both outbound limits permit enqueueing the message.
    ///
    /// # Parameters
    ///
    /// - `message`: application message to place in the outbound queue.
    ///
    /// # Returns
    ///
    /// `Ok(())` means the message was queued, not delivered. Cancelling this
    /// future drops its pending capacity wait and does not enqueue the message.
    /// Waiting sends reserve neither budget; concurrent waiters have no strict
    /// FIFO or starvation guarantee, and this future retains its payload.
    ///
    /// # Errors
    ///
    /// Returns [`WsSendError::Closed`] if the session is shut down before the
    /// message is queued, or [`WsSendError::MessageTooLarge`] if the open
    /// session's payload exceeds the byte limit.
    #[inline]
    pub async fn send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.push(message).await
    }
}
