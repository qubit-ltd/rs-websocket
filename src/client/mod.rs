// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

pub(crate) mod internal;
mod ws_client;
mod ws_client_builder;

#[cfg(feature = "config")]
mod ws_client_builder_config;

pub use ws_client::WsClient;
pub use ws_client_builder::WsClientBuilder;
