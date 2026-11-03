// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use rustls::ClientConfig;
use rustls::crypto::CryptoProvider;

/// One successfully constructed config, associated with the provider allocation
/// used to build it. Provider identity is compared with `Arc::ptr_eq`.
pub(super) struct CachedConfig {
    /// Keeps the provider allocation alive and records the config's provider
    /// identity.
    pub(super) provider: Arc<CryptoProvider>,
    /// Config built for `provider` and this cache's fixed trust-root policy.
    pub(super) config: Arc<ClientConfig>,
}
