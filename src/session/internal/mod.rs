// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod bounded_queue;
mod capacity_limits;
mod close_request;
mod driver;
mod enqueue_result;
mod envelope;
mod owner;
mod queue_inner;
mod read_result;
mod send_event;
mod send_result;

pub(super) use bounded_queue::BoundedQueue;
pub(super) use close_request::CloseRequest;
pub(super) use driver::run;
pub(super) use owner::Owner;
