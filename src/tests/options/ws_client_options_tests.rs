// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::options::WsClientOptions;

#[test]
fn test_checked_write_buffer_limit_reserves_data_and_control_frames() {
    assert_eq!(WsClientOptions::checked_write_buffer_limit(1), Some(146));
    assert_eq!(WsClientOptions::checked_write_buffer_limit(usize::MAX), None);
}
