// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::queue_inner::Reservation;
use crate::ws_message::WsMessage;

/// An application message that keeps its queue capacity reserved until dropped.
pub(crate) struct Envelope {
    /// Application message retained by the queue or active writer.
    pub(super) item: WsMessage,
    /// Atomic count and byte reservation held for this message.
    pub(super) _reservation: Reservation,
}

impl Envelope {
    /// Returns the message and releases its queue reservation.
    #[inline]
    pub(crate) fn into_item(self) -> WsMessage {
        self.item
    }
}
