// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod ws_error;
mod ws_error_kind;
mod ws_handshake_failure;
mod ws_send_error;
mod ws_termination;

pub use ws_error::WsError;
pub use ws_error_kind::WsErrorKind;
pub use ws_handshake_failure::WsHandshakeFailure;
pub use ws_send_error::WsSendError;
pub use ws_termination::WsTermination;
