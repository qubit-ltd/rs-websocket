// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::session_failure::SessionFailure;
use crate::error::WsErrorKind;
use crate::error::WsTermination;

/// A terminal outcome and its safe diagnostic, published atomically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::session) struct TerminalState {
    /// Public terminal snapshot.
    pub(in crate::session) termination: WsTermination,
    /// Safe diagnostic belonging to the winning failure, if any.
    pub(in crate::session) io_error_kind: Option<std::io::ErrorKind>,
}

impl TerminalState {
    /// Builds the terminal state for a failure.
    pub(in crate::session) const fn failure(failure: SessionFailure) -> Self {
        Self {
            termination: WsTermination::Error { kind: failure.kind },
            io_error_kind: failure.io_error_kind,
        }
    }

    /// Builds a terminal state with no I/O diagnostic.
    pub(in crate::session) const fn new(termination: WsTermination) -> Self {
        Self {
            termination,
            io_error_kind: None,
        }
    }

    /// Returns the failure that belongs to this state, if it is an error.
    pub(in crate::session) const fn failure_value(self) -> Option<SessionFailure> {
        match self.termination {
            WsTermination::Error { kind } => Some(SessionFailure {
                kind,
                io_error_kind: self.io_error_kind,
            }),
            WsTermination::Closed { .. } => None,
        }
    }

    /// Creates a category-only terminal failure.
    pub(in crate::session) const fn error(kind: WsErrorKind) -> Self {
        Self::failure(SessionFailure::new(kind))
    }
}

impl PartialEq<WsTermination> for TerminalState {
    fn eq(&self, other: &WsTermination) -> bool {
        self.termination == *other
    }
}
