// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

use rustls::RootCertStore;

/// Selects the trusted certificate authorities used for secure WebSocket
/// connections.
///
/// `Default` trusts the standard WebPKI roots, `Additional` combines those
/// roots with caller-supplied authorities, and `Exclusive` trusts only the
/// supplied authorities. Certificate and hostname checks remain enabled.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsTrustRoots;
///
/// let roots = WsTrustRoots::Default;
/// assert!(matches!(roots, WsTrustRoots::Default));
/// ```
#[derive(Clone)]
pub enum WsTrustRoots {
    /// Use the standard WebPKI server roots.
    Default,
    /// Use the standard WebPKI roots plus the supplied roots.
    Additional(RootCertStore),
    /// Use only the supplied roots.
    Exclusive(RootCertStore),
}

impl fmt::Debug for WsTrustRoots {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("Default"),
            Self::Additional(roots) => f.debug_struct("Additional").field("root_count", &roots.len()).finish(),
            Self::Exclusive(roots) => f.debug_struct("Exclusive").field("root_count", &roots.len()).finish(),
        }
    }
}
