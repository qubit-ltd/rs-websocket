// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Inbound size thresholds used to classify tungstenite read errors.
///
/// The frame limit applies to an individual frame; the message limit applies
/// after fragments have been assembled. These thresholds do not configure the
/// bounded application queue.
#[derive(Clone, Copy, Default)]
pub(super) struct CapacityLimits {
    /// Maximum accepted payload size for one inbound frame.
    pub(super) max_inbound_frame_bytes: usize,
    /// Maximum accepted payload size after inbound fragments are assembled.
    pub(super) max_inbound_message_bytes: usize,
}
