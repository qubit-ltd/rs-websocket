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
/// [`WsSendError::Backpressure`] when either budget is full. [`Self::send`]
/// waits for capacity and its future may be cancelled without enqueueing.
/// Dropping the receiving [`crate::WsSession`] closes the outbound queue, after
/// which sends return [`WsSendError::Closed`].
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
    pub(super) owner: Arc<Owner>,
}

impl WsSender {
    /// Enqueues a message immediately when both outbound limits permit it.
    ///
    /// Returns [`WsSendError::Backpressure`] if either the message-count or
    /// payload-byte limit is full, and [`WsSendError::Closed`] after session
    /// shutdown. Success means queued, not delivered.
    pub fn try_send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.try_push(message)
    }

    /// Waits until both outbound limits permit enqueueing the message.
    ///
    /// Returns [`WsSendError::Closed`] if the session shuts down before the
    /// message is queued. Cancelling this future drops its pending capacity
    /// wait and does not enqueue the message. Success means queued, not
    /// delivered.
    pub async fn send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.push(message).await
    }
}
