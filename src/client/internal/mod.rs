// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod connected_socket;
mod handshake;

pub(crate) use connected_socket::ConnectedSocket;
pub(crate) use handshake::connect_socket;
