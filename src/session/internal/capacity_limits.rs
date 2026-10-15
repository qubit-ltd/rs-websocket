// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Inbound capacity thresholds used to classify tungstenite read errors.
#[derive(Clone, Copy, Default)]
pub(super) struct CapacityLimits {
    /// Maximum accepted payload size for one inbound frame.
    pub(super) max_inbound_frame_bytes: usize,
    /// Maximum accepted payload size after inbound fragments are assembled.
    pub(super) max_inbound_message_bytes: usize,
}
