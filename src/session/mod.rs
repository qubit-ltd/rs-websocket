// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod internal;
mod ws_sender;
mod ws_session;

pub use ws_sender::WsSender;
pub use ws_session::WsSession;

#[cfg(test)]
#[path = "../tests/session/mod.rs"]
mod tests;
