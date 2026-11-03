// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::envelope::Envelope;

/// Result of selecting control-output flushing or outbound queue work.
pub(super) enum WriterWork {
    /// Flush pending automatic control output; this does not send an active
    /// Ping.
    Ping,
    /// Carries the next outbound envelope, or reports a closed, empty queue.
    Outgoing(
        /// Some retains the message capacity reservation through writing;
        /// None leaves queue-closure handling to the lifecycle driver.
        Option<Envelope>,
    ),
}
