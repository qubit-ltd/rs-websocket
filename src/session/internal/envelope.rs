// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use tokio::sync::OwnedSemaphorePermit;

use crate::ws_message::WsMessage;

/// An application message that keeps its queue capacity reserved until dropped.
///
/// The message-count and payload-byte permits travel with the envelope, so
/// capacity remains charged while the message is queued or being written.
pub(crate) struct Envelope {
    /// Application message retained by the queue or active writer.
    pub(super) item: WsMessage,
    /// Queue-count capacity held for this message.
    pub(super) _message_permit: OwnedSemaphorePermit,
    /// Payload-byte capacity held for a nonempty message.
    pub(super) _byte_permit: Option<OwnedSemaphorePermit>,
}

impl Envelope {
    /// Returns the message and releases its queue permits.
    pub(crate) fn into_item(self) -> WsMessage {
        self.item
    }
}
