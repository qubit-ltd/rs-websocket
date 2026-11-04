// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io::ErrorKind;

use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::error::CapacityError;

use super::capacity_limits::CapacityLimits;
use crate::error::WsError;
use crate::error::WsErrorKind;

/// A session failure category paired with safe direct-I/O diagnostic data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::session) struct SessionFailure {
    /// Stable public classification.
    pub(in crate::session) kind: WsErrorKind,
    /// Kind of the direct runtime I/O error, without its potentially sensitive
    /// text.
    pub(in crate::session) io_error_kind: Option<ErrorKind>,
}

impl SessionFailure {
    /// Creates a non-I/O failure.
    pub(in crate::session) const fn new(kind: WsErrorKind) -> Self {
        Self {
            kind,
            io_error_kind: None,
        }
    }

    /// Classifies a tungstenite error while retaining only a direct I/O kind.
    pub(super) fn from_tungstenite(error: &TungsteniteError, limits: CapacityLimits) -> Self {
        let kind = match error {
            TungsteniteError::Capacity(CapacityError::MessageTooLong { max_size, .. }) => match (
                *max_size == limits.max_inbound_frame_bytes,
                *max_size == limits.max_inbound_message_bytes,
            ) {
                (true, false) | (true, true) => WsErrorKind::FrameTooLarge,
                (false, true) | (false, false) => WsErrorKind::MessageTooLarge,
            },
            TungsteniteError::Capacity(_) => WsErrorKind::Protocol,
            TungsteniteError::Protocol(_) | TungsteniteError::Utf8(_) => WsErrorKind::Protocol,
            TungsteniteError::ConnectionClosed | TungsteniteError::AlreadyClosed => WsErrorKind::Protocol,
            TungsteniteError::Io(source) if source.kind() == ErrorKind::TimedOut => WsErrorKind::Timeout,
            TungsteniteError::Io(_) => WsErrorKind::Transport,
            _ => WsErrorKind::Transport,
        };
        let io_error_kind = match error {
            TungsteniteError::Io(source) => Some(source.kind()),
            _ => None,
        };
        Self { kind, io_error_kind }
    }

    /// Creates the public error without retaining runtime error text.
    pub(in crate::session) fn into_error(self) -> WsError {
        WsError::with_io_error_kind(self.kind, self.io_error_kind)
    }
}

impl From<WsErrorKind> for SessionFailure {
    fn from(kind: WsErrorKind) -> Self {
        Self::new(kind)
    }
}

impl PartialEq<WsErrorKind> for SessionFailure {
    fn eq(&self, other: &WsErrorKind) -> bool {
        self.kind == *other
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use tokio_tungstenite::tungstenite::Error as TungsteniteError;

    use super::SessionFailure;
    use crate::error::WsError;
    use crate::error::WsErrorKind;
    use crate::session::internal::capacity_limits::CapacityLimits;

    #[test]
    fn direct_io_errors_keep_only_the_safe_kind() {
        for kind in [ErrorKind::ConnectionReset, ErrorKind::BrokenPipe, ErrorKind::TimedOut] {
            let error = TungsteniteError::Io(std::io::Error::new(kind, "secret-test-payload"));
            let failure = SessionFailure::from_tungstenite(&error, CapacityLimits::default());
            assert_eq!(
                failure.kind,
                if kind == ErrorKind::TimedOut {
                    WsErrorKind::Timeout
                } else {
                    WsErrorKind::Transport
                }
            );
            assert_eq!(failure.io_error_kind, Some(kind));

            let public = failure.into_error();
            assert_eq!(public.io_error_kind(), Some(kind));
            assert!(!format!("{public}").contains("secret-test-payload"));
            assert!(!format!("{public:?}").contains("secret-test-payload"));
            assert!(!format!("{public:?}").contains("io_error_kind"));
        }
    }

    #[test]
    fn non_io_errors_have_no_io_kind() {
        let error = TungsteniteError::ConnectionClosed;
        let failure = SessionFailure::from_tungstenite(&error, CapacityLimits::default());
        assert_eq!(failure.kind, WsErrorKind::Protocol);
        assert_eq!(failure.io_error_kind, None);
        assert_eq!(SessionFailure::new(WsErrorKind::Timeout).io_error_kind, None);
        assert_eq!(WsError::new(WsErrorKind::Transport).io_error_kind(), None);
    }
}
