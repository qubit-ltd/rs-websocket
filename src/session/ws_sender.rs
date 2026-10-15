// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::error::WsSendError;
use crate::message::WsMessage;
use crate::session::internal::Owner;

/// A cloneable handle for bounded asynchronous sends.
///
/// Each clone shares the session's outbound count and byte budgets. Successful
/// enqueueing only transfers a message into that queue; it does not confirm
/// delivery to the peer. Dropping the receiving [`crate::WsSession`] closes the
/// outbound queue, after which sends return [`WsSendError::Closed`].
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
    pub fn try_send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.try_push(message)
    }

    /// Waits until both outbound limits permit enqueueing the message.
    pub async fn send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.push(message).await
    }
}
