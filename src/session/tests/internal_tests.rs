// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::poll_fn;
use futures_util::poll;
use tokio::spawn;
use tokio::sync::Barrier;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::time::timeout;

use crate::error::WsSendError;
use crate::session::internal::BoundedQueue;
use crate::ws_message::WsMessage;

fn text(payload: &str) -> WsMessage {
    WsMessage::Text(payload.to_owned())
}

fn waiter_sizes(k: usize, order: &str) -> Vec<usize> {
    fn shuffle(values: &mut [usize]) {
        let mut state = 0x98470bb0_u64;
        for index in (1..values.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            values.swap(index, (state as usize) % (index + 1));
        }
    }

    let mut values = (1..=k).collect::<Vec<_>>();
    match order {
        "ascending" => {}
        "descending" => values.reverse(),
        "seeded_shuffle" => shuffle(&mut values),
        "blocked_prefix" => {
            shuffle(&mut values);
            values.insert(0, k + 1);
        }
        _ => panic!("unknown waiter order: {order}"),
    }
    values
}
#[derive(Default)]
struct WakeCounter {
    calls: AtomicUsize,
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_recoverable_backpressure_returns_original_text() {
    let queue = BoundedQueue::new(1, 64);
    queue.try_push(text("occupied")).expect("initial message fits");
    let mut value = String::with_capacity(32);
    value.push_str("secret-command");
    let pointer = value.as_ptr();
    let capacity = value.capacity();

    let failure = queue
        .try_push_recoverable(WsMessage::Text(value))
        .expect_err("full queue returns the unsent message");

    assert_eq!(failure.kind(), WsSendError::Backpressure);
    assert!(!format!("{failure:?}").contains("secret-command"));
    assert!(!format!("{failure}").contains("secret-command"));
    let (kind, message) = failure.into_parts();
    assert_eq!(kind, WsSendError::Backpressure);
    let WsMessage::Text(text) = message else {
        panic!("text expected")
    };
    assert_eq!(text, "secret-command");
    assert_eq!(text.as_ptr(), pointer);
    assert_eq!(text.capacity(), capacity);
    assert_eq!(queue.usage(), (1, 8));
}

#[test]
fn test_batch_release_selects_distinct_waiters_before_repoll() {
    let k = 129;
    let queue = BoundedQueue::new(k, k);
    for _ in 0..k {
        queue.try_push(text("o")).unwrap();
    }
    let counters = (0..k).map(|_| Arc::new(WakeCounter::default())).collect::<Vec<_>>();
    let wakers = counters.iter().cloned().map(Waker::from).collect::<Vec<_>>();
    let mut contexts = wakers.iter().map(Context::from_waker).collect::<Vec<_>>();
    let mut futures = (0..k).map(|_| Box::pin(queue.push(text("w")))).collect::<Vec<_>>();
    for (future, context) in futures.iter_mut().zip(&mut contexts) {
        assert!(future.as_mut().poll(context).is_pending());
    }
    assert!(counters.iter().all(|counter| counter.calls.load(Ordering::SeqCst) == 0));

    for released in 0..64 {
        drop(queue.try_pop().unwrap());
        for (id, counter) in counters.iter().enumerate() {
            assert_eq!(
                counter.calls.load(Ordering::SeqCst),
                usize::from(id <= released),
                "wake count for waiter {id} after release {released}"
            );
        }
    }
    for index in 0..64 {
        assert_eq!(futures[index].as_mut().poll(&mut contexts[index]), Poll::Ready(Ok(())));
    }
    drop(futures);
    while let Some(envelope) = queue.try_pop() {
        drop(envelope);
    }
    assert_eq!(queue.usage(), (0, 0));
}

#[test]
fn test_recoverable_rejections_preserve_text_and_binary_messages() {
    let full = BoundedQueue::new(1, 4);
    full.try_push(text("full")).expect("initial message fits");
    let binary = Bytes::from_static(b"data");
    let binary_pointer = binary.as_ptr();
    let failure = full
        .try_push_recoverable(WsMessage::Binary(binary))
        .expect_err("full queue rejects binary message");
    assert_eq!(failure.kind(), WsSendError::Backpressure);
    assert!(!format!("{failure:?}").contains("data"));
    assert!(!format!("{failure}").contains("data"));
    let recovered = failure.into_message();
    let WsMessage::Binary(bytes) = recovered else {
        panic!("binary message expected");
    };
    assert_eq!(bytes.as_ref(), b"data");
    assert_eq!(bytes.as_ptr(), binary_pointer);

    for message in [text("xx"), WsMessage::Binary(Bytes::from_static(b"xx"))] {
        let queue = BoundedQueue::new(1, 1);
        let failure = queue
            .try_push_recoverable(message)
            .expect_err("oversized message rejected");
        assert_eq!(failure.kind(), WsSendError::MessageTooLarge);
        assert_eq!(failure.message().payload_len(), 2);
        assert_eq!(queue.usage(), (0, 0));
    }

    let closed = BoundedQueue::new(1, 1);
    closed.close();
    let failure = closed
        .try_push_recoverable(text("too large"))
        .expect_err("closed takes priority over oversized");
    assert_eq!(failure.kind(), WsSendError::Closed);
    assert_eq!(failure.message(), &text("too large"));
}

#[test]
fn test_capacity_index_selects_oldest_fitting_waiter_across_orders() {
    const K: usize = 129;
    for order in ["descending", "seeded_shuffle"] {
        let queue = BoundedQueue::new(K + 2, K + 1);
        queue.try_push(text(&"a".repeat(64))).unwrap();
        queue.try_push(text(&"b".repeat(66))).unwrap();
        let mut sizes = waiter_sizes(K, order);
        sizes.insert(0, K + 1);
        let counters = sizes
            .iter()
            .map(|_| Arc::new(WakeCounter::default()))
            .collect::<Vec<_>>();
        let waiters = counters.iter().cloned().map(Waker::from).collect::<Vec<_>>();
        let mut contexts = waiters.iter().map(Context::from_waker).collect::<Vec<_>>();
        let mut futures = sizes
            .iter()
            .map(|&size| Box::pin(queue.push(text(&"w".repeat(size)))))
            .collect::<Vec<_>>();
        for (future, context) in futures.iter_mut().zip(&mut contexts) {
            assert!(future.as_mut().poll(context).is_pending());
        }
        let expected = sizes.iter().position(|&size| size <= 64).unwrap();
        let before = counters
            .iter()
            .map(|counter| counter.calls.load(Ordering::SeqCst))
            .collect::<Vec<_>>();
        drop(queue.try_pop().expect("release 64-byte placeholder"));
        for (index, counter) in counters.iter().enumerate() {
            let after = counter.calls.load(Ordering::SeqCst);
            assert_eq!(
                after > before[index],
                index == expected,
                "order={order}, registration={index}, expected={expected}"
            );
        }
        assert_eq!(
            futures[expected].as_mut().poll(&mut contexts[expected]),
            Poll::Ready(Ok(()))
        );
        drop(futures);
        drop(queue.try_pop().unwrap());
        drop(queue.try_pop().unwrap());
        assert_eq!(queue.usage(), (0, 0));
    }
}

#[test]
fn test_indexed_waiter_zero_bytes_progress_when_no_byte_capacity_is_free() {
    const NONZERO_WAITERS: usize = 129;
    let queue = BoundedQueue::new(2, 1);
    queue.try_push(text("")).unwrap();
    queue.try_push(text("x")).unwrap();

    let counters = (0..=NONZERO_WAITERS)
        .map(|_| Arc::new(WakeCounter::default()))
        .collect::<Vec<_>>();
    let wakers = counters.iter().cloned().map(Waker::from).collect::<Vec<_>>();
    let mut contexts = wakers.iter().map(Context::from_waker).collect::<Vec<_>>();
    let mut futures = (0..NONZERO_WAITERS)
        .map(|_| Box::pin(queue.push(text("w"))))
        .chain(std::iter::once(Box::pin(queue.push(text("")))))
        .collect::<Vec<_>>();
    for (future, context) in futures.iter_mut().zip(&mut contexts) {
        assert!(future.as_mut().poll(context).is_pending());
    }
    assert_eq!(queue.usage(), (2, 1));

    drop(queue.try_pop().expect("free a count slot without bytes"));
    for (index, counter) in counters.iter().enumerate() {
        assert_eq!(
            counter.calls.load(Ordering::SeqCst) > 0,
            index == NONZERO_WAITERS,
            "only the zero-byte waiter at the end of registration order is selected"
        );
    }
    assert_eq!(
        futures[NONZERO_WAITERS].as_mut().poll(&mut contexts[NONZERO_WAITERS]),
        Poll::Ready(Ok(()))
    );
    drop(futures);
    drop(queue.try_pop().unwrap());
    drop(queue.try_pop().unwrap());
    assert_eq!(queue.usage(), (0, 0));
}
#[tokio::test]
async fn test_recoverable_wait_returns_message_on_close_and_cancellation_releases_budget() {
    let queue = BoundedQueue::new(1, 64);
    queue.try_push(text("occupied")).expect("initial message fits");
    let oversized = queue
        .push_recoverable(text(&"x".repeat(65)))
        .await
        .expect_err("oversized waiting send fails immediately");
    assert_eq!(oversized.kind(), WsSendError::MessageTooLarge);
    assert_eq!(oversized.message().payload_len(), 65);
    assert_eq!(queue.usage(), (1, 8));
    let mut value = String::with_capacity(32);
    value.push_str("waiting-command");
    let pointer = value.as_ptr();
    let capacity = value.capacity();
    let waiting_queue = queue.clone();
    let waiting = spawn(async move { waiting_queue.push_recoverable(WsMessage::Text(value)).await });
    yield_now().await;
    queue.close();
    let failure = timeout(Duration::from_secs(1), waiting)
        .await
        .expect("close wakes recoverable waiter")
        .expect("waiter task joins")
        .expect_err("closed queue rejects waiting message");
    assert_eq!(failure.kind(), WsSendError::Closed);
    let WsMessage::Text(recovered) = failure.into_message() else {
        panic!("text message expected");
    };
    assert_eq!(recovered, "waiting-command");
    assert_eq!(recovered.as_ptr(), pointer);
    assert_eq!(recovered.capacity(), capacity);
    assert_eq!(queue.usage(), (0, 0));

    let cancelled = BoundedQueue::new(1, 1);
    cancelled.try_push(text("x")).expect("fill queue");
    let waiting_queue = cancelled.clone();
    let waiter = spawn(async move { waiting_queue.push_recoverable(text("y")).await });
    yield_now().await;
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(cancelled.usage(), (1, 1));
    drop(cancelled.try_pop().expect("release full queue"));
    cancelled
        .try_push(text("z"))
        .expect("cancelled recoverable wait leaked no queue budget");
}

#[tokio::test]
async fn test_registration_zero_length_progress_with_exhausted_bytes() {
    let queue = BoundedQueue::new(3, 1);
    queue.try_push(text("x")).unwrap();
    let pending = queue.push(text("y"));
    tokio::pin!(pending);
    assert!(futures_util::poll!(pending.as_mut()).is_pending());
    queue.push(text("")).await.unwrap();
    assert_eq!(queue.usage(), (2, 1));
    drop(queue.try_pop().unwrap());
    tokio::time::timeout(Duration::from_secs(2), pending.as_mut())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queue.try_pop().unwrap().into_item(), text(""));
    assert_eq!(queue.try_pop().unwrap().into_item(), text("y"));
    assert_eq!(queue.usage(), (0, 0));
}

#[test]
fn test_capacity_waiters_wake_one_that_fits_mixed_byte_budget() {
    let queue = BoundedQueue::new(2, 130);
    queue.try_push(text("h")).unwrap();
    queue.try_push(text(&"f".repeat(128))).unwrap();
    let large_count = Arc::new(WakeCounter::default());
    let small_count = Arc::new(WakeCounter::default());
    let large_waker = Waker::from(large_count.clone());
    let small_waker = Waker::from(small_count.clone());
    let mut large_context = Context::from_waker(&large_waker);
    let mut small_context = Context::from_waker(&small_waker);
    let mut larger = Box::pin(queue.push(text(&"l".repeat(65))));
    let mut smaller = Box::pin(queue.push(text("s")));
    assert!(larger.as_mut().poll(&mut large_context).is_pending());
    assert!(smaller.as_mut().poll(&mut small_context).is_pending());
    let before_small = small_count.calls.load(Ordering::SeqCst);
    drop(queue.try_pop().unwrap());
    assert!(small_count.calls.load(Ordering::SeqCst) > before_small);
    assert_eq!(smaller.as_mut().poll(&mut small_context), Poll::Ready(Ok(())));
    assert!(larger.as_mut().poll(&mut large_context).is_pending());
    let before_large = large_count.calls.load(Ordering::SeqCst);
    drop(queue.try_pop().unwrap());
    assert!(large_count.calls.load(Ordering::SeqCst) > before_large);
    assert_eq!(larger.as_mut().poll(&mut large_context), Poll::Ready(Ok(())));
    assert_eq!(queue.usage(), (2, 66));
    drop(queue.try_pop().unwrap());
    drop(queue.try_pop().unwrap());
    assert_eq!(queue.usage(), (0, 0));
}

#[test]
fn test_indexed_waiter_reset_and_cancellation_handoff_preserve_progress() {
    const WAITERS: usize = 129;

    let queue = BoundedQueue::new(WAITERS + 2, 128);
    queue.try_push(text(&"a".repeat(64))).unwrap();
    queue.try_push(text(&"b".repeat(64))).unwrap();

    let first_counter = Arc::new(WakeCounter::default());
    let second_counter = Arc::new(WakeCounter::default());
    let first_waker = Waker::from(first_counter.clone());
    let second_waker = Waker::from(second_counter.clone());
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);
    let mut first = Box::pin(queue.push(text(&"f".repeat(63))));
    let mut second = Box::pin(queue.push(text(&"s".repeat(63))));
    assert!(first.as_mut().poll(&mut first_context).is_pending());
    assert!(second.as_mut().poll(&mut second_context).is_pending());

    let other_waker = Waker::from(Arc::new(WakeCounter::default()));
    let mut other_context = Context::from_waker(&other_waker);
    let mut other_waiters = (2..WAITERS)
        .map(|_| Box::pin(queue.push(text(&"w".repeat(63)))))
        .collect::<Vec<_>>();
    for waiter in &mut other_waiters {
        assert!(waiter.as_mut().poll(&mut other_context).is_pending());
    }

    let first_wakes = first_counter.calls.load(Ordering::SeqCst);
    drop(queue.try_pop().expect("release first reservation"));
    assert!(first_counter.calls.load(Ordering::SeqCst) > first_wakes);

    // Steal the released bytes before the selected waiter retries. Its poll
    // resets its selection marker and must put it back in the active index.
    queue.try_push(text(&"c".repeat(64))).unwrap();
    assert!(first.as_mut().poll(&mut first_context).is_pending());
    assert_eq!(queue.usage(), (2, 128));

    let first_wakes = first_counter.calls.load(Ordering::SeqCst);
    drop(queue.try_pop().expect("release the remaining placeholder"));
    assert!(first_counter.calls.load(Ordering::SeqCst) > first_wakes);

    // Cancelling the selected waiter hands the same capacity opportunity to
    // the next eligible waiter even while the large-waiter index is active.
    let second_wakes = second_counter.calls.load(Ordering::SeqCst);
    drop(first);
    assert!(second_counter.calls.load(Ordering::SeqCst) > second_wakes);
    assert_eq!(second.as_mut().poll(&mut second_context), Poll::Ready(Ok(())));
    drop(second);
    drop(other_waiters);
    while let Some(item) = queue.try_pop() {
        drop(item);
    }
    assert_eq!(queue.usage(), (0, 0));
}

#[test]
fn test_capacity_index_skips_unfit_prefix_and_selects_first_fitting_waiter() {
    const UNFIT_WAITERS: usize = 129;

    let queue = BoundedQueue::new(UNFIT_WAITERS + 4, 128);
    queue.try_push(text(&"a".repeat(64))).unwrap();
    queue.try_push(text(&"b".repeat(64))).unwrap();
    let other_waker = Waker::from(Arc::new(WakeCounter::default()));
    let mut other_context = Context::from_waker(&other_waker);
    let mut unfit_waiters = (0..UNFIT_WAITERS)
        .map(|_| Box::pin(queue.push(text(&"x".repeat(65)))))
        .collect::<Vec<_>>();
    for waiter in &mut unfit_waiters {
        assert!(waiter.as_mut().poll(&mut other_context).is_pending());
    }

    let first_counter = Arc::new(WakeCounter::default());
    let second_counter = Arc::new(WakeCounter::default());
    let first_waker = Waker::from(first_counter.clone());
    let second_waker = Waker::from(second_counter.clone());
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);
    let mut first_fit = Box::pin(queue.push(text(&"f".repeat(64))));
    let mut second_fit = Box::pin(queue.push(text(&"s".repeat(64))));
    assert!(first_fit.as_mut().poll(&mut first_context).is_pending());
    assert!(second_fit.as_mut().poll(&mut second_context).is_pending());

    let first_wakes = first_counter.calls.load(Ordering::SeqCst);
    drop(queue.try_pop().expect("release one byte reservation"));
    assert!(first_counter.calls.load(Ordering::SeqCst) > first_wakes);
    assert_eq!(first_fit.as_mut().poll(&mut first_context), Poll::Ready(Ok(())));

    drop(first_fit);
    let second_wakes = second_counter.calls.load(Ordering::SeqCst);
    drop(queue.try_pop().expect("release first admitted message"));
    assert!(second_counter.calls.load(Ordering::SeqCst) > second_wakes);
    assert_eq!(second_fit.as_mut().poll(&mut second_context), Poll::Ready(Ok(())));

    drop(second_fit);
    drop(unfit_waiters);
    while let Some(item) = queue.try_pop() {
        drop(item);
    }
    assert_eq!(queue.usage(), (0, 0));
}

#[test]
fn test_capacity_index_fast_path_selects_oldest_fitting_waiter() {
    const WAITERS: usize = 129;

    let queue = BoundedQueue::new(1, 128);
    queue.try_push(text(&"o".repeat(128))).unwrap();
    let counters = (0..WAITERS)
        .map(|_| Arc::new(WakeCounter::default()))
        .collect::<Vec<_>>();
    let wakers = counters.iter().cloned().map(Waker::from).collect::<Vec<_>>();
    let mut futures = (0..WAITERS)
        .map(|index| {
            let size = if index == 0 { 1 } else { 2 + (index % 62) };
            Box::pin(queue.push(text(&"w".repeat(size))))
        })
        .collect::<Vec<_>>();
    for (future, waker) in futures.iter_mut().zip(&wakers) {
        let mut context = Context::from_waker(waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
    }
    drop(queue.try_pop().expect("release all payload capacity"));
    assert!(counters[0].calls.load(Ordering::SeqCst) > 0);
    assert!(
        counters[1..]
            .iter()
            .all(|counter| counter.calls.load(Ordering::SeqCst) == 0)
    );
    let mut first_context = Context::from_waker(&wakers[0]);
    assert_eq!(futures[0].as_mut().poll(&mut first_context), Poll::Ready(Ok(())));

    drop(futures);
    while let Some(item) = queue.try_pop() {
        drop(item);
    }
    assert_eq!(queue.usage(), (0, 0));
}

#[test]
fn test_capacity_index_close_broadcasts_to_all_registered_waiters() {
    const WAITERS: usize = 129;

    let queue = BoundedQueue::new(1, WAITERS + 1);
    queue.try_push(text("x")).unwrap();
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(counter.clone());
    let mut context = Context::from_waker(&waker);
    let mut waiters = (0..WAITERS)
        .map(|_| Box::pin(queue.push(text("y"))))
        .collect::<Vec<_>>();
    for waiter in &mut waiters {
        assert!(waiter.as_mut().poll(&mut context).is_pending());
    }

    let wakes_before_close = counter.calls.load(Ordering::SeqCst);
    queue.close();
    assert!(counter.calls.load(Ordering::SeqCst) >= wakes_before_close + WAITERS);
    for waiter in &mut waiters {
        assert_eq!(
            waiter.as_mut().poll(&mut context),
            Poll::Ready(Err(WsSendError::Closed))
        );
    }
    drop(waiters);
    assert_eq!(queue.usage(), (0, 0));
}

#[tokio::test]
async fn test_cancelling_selected_waiter_passes_wake_to_next_waiter() {
    let queue = BoundedQueue::new(1, 1);
    queue.try_push(text("x")).expect("fill queue");
    let mut first = Box::pin(queue.push(text("a")));
    let mut second = Box::pin(queue.push(text("b")));
    assert!(poll!(first.as_mut()).is_pending());
    assert!(poll!(second.as_mut()).is_pending());

    drop(queue.try_pop().expect("release capacity"));
    drop(first);
    assert!(
        poll!(second.as_mut()).is_ready(),
        "cancellation transfers the only available wake"
    );
    assert_eq!(queue.usage(), (1, 1));
}

#[tokio::test]
async fn test_capacity_release_racing_with_waiter_registration_makes_progress() {
    for _ in 0..32 {
        let queue = BoundedQueue::new(1, 1);
        queue.try_push(text("x")).expect("fill queue");
        let waiting_queue = queue.clone();
        let waiting = spawn(async move { waiting_queue.push(text("y")).await });
        drop(queue.try_pop().expect("release near registration"));
        timeout(Duration::from_secs(1), waiting)
            .await
            .expect("release/register race does not lose progress")
            .expect("waiter joins")
            .expect("waiter is admitted");
        drop(queue.try_pop().expect("release admitted message"));
    }
}

#[test]
fn test_close_wakes_all_registered_capacity_waiters() {
    let queue = BoundedQueue::new(1, 1);
    queue.try_push(text("x")).unwrap();
    let counters: Vec<_> = (0..8).map(|_| Arc::new(WakeCounter::default())).collect();
    let wakers: Vec<_> = counters.iter().cloned().map(Waker::from).collect();
    let mut futures: Vec<_> = (0..8).map(|_| Box::pin(queue.push(text("y")))).collect();
    for (future, waker) in futures.iter_mut().zip(&wakers) {
        let mut context = Context::from_waker(waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
    }
    let before: Vec<_> = counters
        .iter()
        .map(|counter| counter.calls.load(Ordering::SeqCst))
        .collect();
    queue.close();
    for (index, (future, waker)) in futures.iter_mut().zip(&wakers).enumerate() {
        assert!(counters[index].calls.load(Ordering::SeqCst) > before[index]);
        let mut context = Context::from_waker(waker);
        assert_eq!(
            future.as_mut().poll(&mut context),
            Poll::Ready(Err(WsSendError::Closed))
        );
    }
    assert_eq!(queue.usage(), (0, 0));
}

#[tokio::test]
async fn test_closed_queue_takes_priority_over_oversized_payload() {
    let queue = BoundedQueue::new(1, 1);
    assert_eq!(queue.try_push(text("xx")), Err(WsSendError::MessageTooLarge));
    assert_eq!(queue.push(text("xx")).await, Err(WsSendError::MessageTooLarge));
    queue.close();
    assert_eq!(queue.try_push(text("xx")), Err(WsSendError::Closed));
    assert_eq!(queue.push(text("xx")).await, Err(WsSendError::Closed));
}

#[tokio::test]
async fn test_enforces_message_and_byte_limits_and_releases_permits_on_pop() {
    let queue = BoundedQueue::new(2, 5);
    queue.try_push(text("ab")).expect("2 bytes fit");
    queue.try_push(text("xyz")).expect("3 more bytes fit");
    assert_eq!(queue.try_push(text("")), Err(WsSendError::Backpressure));

    assert_eq!(queue.try_pop().expect("first item").into_item(), text("ab"));
    queue.try_push(text("q")).expect("pop releases count and byte permits");
    assert_eq!(queue.try_push(text("123456")), Err(WsSendError::MessageTooLarge));

    let empty_queue = BoundedQueue::new(1, 1);
    empty_queue.try_push(text("")).expect("empty text uses a count permit");
    assert_eq!(empty_queue.try_push(text("")), Err(WsSendError::Backpressure));
}

#[tokio::test]
async fn test_async_push_waits_for_capacity_and_drop_cancels_without_leaking_permits() {
    let queue = BoundedQueue::new(1, 2);
    queue.try_push(text("ok")).expect("initial item fits");

    let waiting_queue = queue.clone();
    let waiting = spawn(async move { waiting_queue.push(text("x")).await });
    yield_now().await;
    assert_eq!(queue.usage(), (1, 2));

    drop(queue.pop().await.expect("pop releases capacity"));
    timeout(Duration::from_secs(1), waiting)
        .await
        .expect("push wakes after capacity is available")
        .expect("push task joins")
        .expect("push succeeds");
    assert_eq!(queue.usage(), (1, 1));

    let cancelled_queue = BoundedQueue::new(1, 1);
    cancelled_queue.try_push(text("a")).expect("fill queue");
    let pending_queue = cancelled_queue.clone();
    let pending = spawn(async move { pending_queue.push(text("b")).await });
    yield_now().await;
    pending.abort();
    let _ = pending.await;
    drop(cancelled_queue.pop().await.expect("release occupied slot"));
    cancelled_queue
        .try_push(text("b"))
        .expect("cancelled waiter leaked no permit");

    let partial_queue = BoundedQueue::new(2, 1);
    partial_queue.try_push(text("a")).expect("occupy byte permit");
    let pending_queue = partial_queue.clone();
    let pending = spawn(async move { pending_queue.push(text("b")).await });
    yield_now().await;
    assert_eq!(partial_queue.usage(), (1, 1));
    pending.abort();
    let _ = pending.await;
    assert_eq!(partial_queue.usage(), (1, 1));
    partial_queue
        .try_push(text(""))
        .expect("cancellation while waiting for bytes releases the count permit");
}

#[tokio::test]
async fn test_waiting_push_does_not_reserve_only_one_capacity_budget() {
    let queue = BoundedQueue::new(2, 1);
    queue.try_push(text("a")).expect("occupy byte budget");

    let mut waiting = Box::pin(queue.push(text("b")));
    assert!(poll!(waiting.as_mut()).is_pending());

    assert_eq!(queue.usage(), (1, 1));
    queue.try_push(text("")).expect("empty payload uses only a count slot");
    assert_eq!(queue.usage(), (2, 1));

    drop(queue.try_pop().expect("pop releases the occupied byte slot"));
    timeout(Duration::from_secs(1), waiting.as_mut())
        .await
        .expect("waiting push wakes after both budgets become available")
        .expect("waiting message is enqueued");

    assert_eq!(
        queue.try_pop().expect("empty message remains queued").into_item(),
        text("")
    );
    assert_eq!(
        queue.try_pop().expect("waiting message is queued").into_item(),
        text("b")
    );
}

#[tokio::test]
async fn test_different_sized_waiters_are_notified_when_capacity_changes() {
    let queue = BoundedQueue::new(2, 2);
    queue.try_push(text("")).expect("occupy one count slot");
    queue.try_push(text("a")).expect("occupy the remaining byte capacity");

    let big_queue = queue.clone();
    let (big_pending_tx, big_pending_rx) = oneshot::channel();
    let big = spawn(async move {
        let mut pending_signal = Some(big_pending_tx);
        let mut push = Box::pin(big_queue.push(text("bb")));
        poll_fn(|context| {
            let result = push.as_mut().poll(context);
            if result.is_pending()
                && let Some(signal) = pending_signal.take()
            {
                let _ = signal.send(());
            }
            result
        })
        .await
    });
    timeout(Duration::from_secs(1), big_pending_rx)
        .await
        .expect("big push reaches its wait state")
        .expect("big push signals its pending poll");

    let small_queue = queue.clone();
    let (small_pending_tx, small_pending_rx) = oneshot::channel();
    let small = spawn(async move {
        let mut pending_signal = Some(small_pending_tx);
        let mut push = Box::pin(small_queue.push(text("")));
        poll_fn(|context| {
            let result = push.as_mut().poll(context);
            if result.is_pending()
                && let Some(signal) = pending_signal.take()
            {
                let _ = signal.send(());
            }
            result
        })
        .await
    });
    timeout(Duration::from_secs(1), small_pending_rx)
        .await
        .expect("small push reaches its wait state")
        .expect("small push signals its pending poll");

    drop(queue.try_pop().expect("release the first count slot"));
    timeout(Duration::from_secs(1), small)
        .await
        .expect("small waiter is notified when a count slot is released")
        .expect("small push task joins")
        .expect("small message is enqueued");
    assert!(!big.is_finished(), "big waiter still lacks byte capacity");
    assert_eq!(queue.usage(), (2, 1));

    drop(queue.try_pop().expect("release the occupied byte capacity"));
    timeout(Duration::from_secs(1), big)
        .await
        .expect("big waiter is notified when byte capacity is released")
        .expect("big push task joins")
        .expect("big message is enqueued");
    assert_eq!(queue.usage(), (2, 2));
}

#[tokio::test]
async fn test_multiple_waiters_retry_after_capacity_is_released() {
    let queue = BoundedQueue::new(2, 4);
    queue.try_push(text("a")).expect("initial item fits");

    let first_queue = queue.clone();
    let first = spawn(async move { first_queue.push(text("bb")).await });
    let second_queue = queue.clone();
    let second = spawn(async move { second_queue.push(text("cc")).await });
    yield_now().await;

    drop(queue.pop().await.expect("release initial capacity"));
    drop(
        timeout(Duration::from_secs(1), queue.pop())
            .await
            .expect("a waiter enqueues after capacity is released")
            .expect("first waiting item is queued"),
    );
    drop(
        timeout(Duration::from_secs(1), queue.pop())
            .await
            .expect("the other waiter retries after capacity is released")
            .expect("second waiting item is queued"),
    );
    timeout(Duration::from_secs(1), first)
        .await
        .expect("first push task terminates")
        .expect("first push task joins")
        .expect("first push succeeds");
    timeout(Duration::from_secs(1), second)
        .await
        .expect("second push task terminates")
        .expect("second push task joins")
        .expect("second push succeeds");
}

#[tokio::test]
async fn test_close_wakes_all_blocked_pushers() {
    let queue = BoundedQueue::new(1, 1);
    queue.try_push(text("a")).expect("fill queue");
    let mut waiters = (0..3)
        .map(|index| {
            let queue = queue.clone();
            let (pending_tx, pending_rx) = oneshot::channel();
            let waiter = spawn(async move {
                let mut pending_signal = Some(pending_tx);
                let mut push = Box::pin(queue.push(text(&index.to_string())));
                poll_fn(|context| {
                    let result = push.as_mut().poll(context);
                    if result.is_pending()
                        && let Some(signal) = pending_signal.take()
                    {
                        let _ = signal.send(());
                    }
                    result
                })
                .await
            });
            (waiter, pending_rx)
        })
        .collect::<Vec<_>>();

    for (_, pending_rx) in &mut waiters {
        timeout(Duration::from_secs(1), pending_rx)
            .await
            .expect("each push reaches its wait state")
            .expect("each push signals its pending poll");
    }

    queue.close();
    for (waiter, _) in waiters {
        assert_eq!(
            timeout(Duration::from_secs(1), waiter)
                .await
                .expect("close wakes every blocked pusher")
                .expect("push task joins"),
            Err(WsSendError::Closed)
        );
    }
    assert_eq!(queue.usage(), (0, 0));
}

#[tokio::test]
async fn test_close_before_reservation_drop_releases_capacity() {
    let queue = BoundedQueue::new(1, 1);
    queue.try_push(text("a")).expect("fill queue");
    let envelope = queue.try_pop().expect("pop keeps reservation alive");
    queue.close();
    assert_eq!(queue.usage(), (1, 1));
    drop(envelope);
    assert_eq!(queue.usage(), (0, 0));
    assert_eq!(queue.try_push(text("b")), Err(WsSendError::Closed));
}

#[tokio::test]
async fn test_reservation_drop_before_close_releases_capacity() {
    let queue = BoundedQueue::new(1, 1);
    queue.try_push(text("a")).expect("fill queue");
    let envelope = queue.try_pop().expect("pop keeps reservation alive");
    drop(envelope);
    assert_eq!(queue.usage(), (0, 0));
    queue.close();
    assert_eq!(queue.usage(), (0, 0));
    assert_eq!(queue.try_push(text("b")), Err(WsSendError::Closed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_close_and_reservation_drop_can_race_without_leaking_capacity() {
    const ROUNDS: usize = 32;
    const DEADLINE: Duration = Duration::from_secs(1);

    for round in 0..ROUNDS {
        let queue = BoundedQueue::new(1, 1);
        queue.try_push(text("a")).expect("fill queue");
        let envelope = queue.try_pop().expect("pop keeps reservation alive");

        let mut waiters = (0..2)
            .map(|index| {
                let queue = queue.clone();
                let (pending_tx, pending_rx) = oneshot::channel();
                let waiter = spawn(async move {
                    let mut pending_signal = Some(pending_tx);
                    let mut push = Box::pin(queue.push(text(&index.to_string())));
                    poll_fn(|context| {
                        let result = push.as_mut().poll(context);
                        if result.is_pending()
                            && let Some(signal) = pending_signal.take()
                        {
                            let _ = signal.send(());
                        }
                        result
                    })
                    .await
                });
                (waiter, pending_rx)
            })
            .collect::<Vec<_>>();

        for (index, (_, pending_rx)) in waiters.iter_mut().enumerate() {
            timeout(DEADLINE, pending_rx)
                .await
                .unwrap_or_else(|_| panic!("round {round}: waiter {index} pending handshake timed out"))
                .unwrap_or_else(|_| panic!("round {round}: waiter {index} pending handshake dropped"));
        }

        let barrier = Arc::new(Barrier::new(3));
        let close_queue = queue.clone();
        let close_barrier = barrier.clone();
        let closing = spawn(async move {
            close_barrier.wait().await;
            close_queue.close();
        });
        let drop_barrier = barrier.clone();
        let dropping = spawn(async move {
            drop_barrier.wait().await;
            drop(envelope);
        });

        timeout(DEADLINE, barrier.wait())
            .await
            .unwrap_or_else(|_| panic!("round {round}: race start handshake timed out"));
        timeout(DEADLINE, closing)
            .await
            .unwrap_or_else(|_| panic!("round {round}: close task timed out"))
            .unwrap_or_else(|_| panic!("round {round}: close task panicked"));
        timeout(DEADLINE, dropping)
            .await
            .unwrap_or_else(|_| panic!("round {round}: reservation drop task timed out"))
            .unwrap_or_else(|_| panic!("round {round}: reservation drop task panicked"));

        for (index, (waiter, _)) in waiters.into_iter().enumerate() {
            match timeout(DEADLINE, waiter)
                .await
                .unwrap_or_else(|_| panic!("round {round}: waiter {index} did not finish"))
                .unwrap_or_else(|_| panic!("round {round}: waiter {index} panicked"))
            {
                Ok(()) | Err(WsSendError::Closed) => {}
                Err(error) => panic!("round {round}: waiter {index} returned {error:?}"),
            }
        }

        assert_eq!(queue.usage(), (0, 0), "round {round}: capacity leaked");
        assert_eq!(
            queue.try_push(text("z")),
            Err(WsSendError::Closed),
            "round {round}: close did not take effect"
        );
    }
}

#[test]
fn test_popped_envelope_keeps_capacity_until_dropped() {
    let queue = BoundedQueue::new(1, 2);
    queue.try_push(text("a")).expect("item fits");

    let envelope = queue.try_pop().expect("pop keeps reservation alive");
    assert_eq!(queue.usage(), (1, 1));
    assert_eq!(envelope.into_item(), text("a"));
    assert_eq!(queue.usage(), (0, 0));
}

#[tokio::test]
async fn test_close_releases_queued_items_and_wakes_blocked_pushers() {
    let queue = BoundedQueue::new(1, 2);
    queue.try_push(text("a")).expect("fill queue");

    let waiting_queue = queue.clone();
    let waiting = spawn(async move { waiting_queue.push(text("b")).await });
    yield_now().await;
    queue.close();

    assert_eq!(
        timeout(Duration::from_secs(1), waiting)
            .await
            .expect("closed queue wakes blocked pushers")
            .expect("push task joins"),
        Err(WsSendError::Closed)
    );
    assert_eq!(queue.usage(), (0, 0));
    assert_eq!(queue.try_push(text("c")), Err(WsSendError::Closed));
    assert!(
        timeout(Duration::from_secs(1), queue.pop())
            .await
            .expect("closed queue pop resolves")
            .is_none()
    );
}

#[tokio::test]
async fn test_repeated_close_and_waiter_cancellation_always_terminate() {
    for iteration in 0..32 {
        let queue = BoundedQueue::new(1, 1);
        queue.try_push(text("x")).expect("fill queue");

        let waiting_queue = queue.clone();
        let waiting = spawn(async move { waiting_queue.push(text("y")).await });
        yield_now().await;

        if iteration % 2 == 0 {
            waiting.abort();
            timeout(Duration::from_secs(1), waiting)
                .await
                .expect("cancelled waiter terminates")
                .expect_err("aborted waiter reports cancellation");
            queue.close();
        } else {
            queue.close();
            assert_eq!(
                timeout(Duration::from_secs(1), waiting)
                    .await
                    .expect("close wakes waiter")
                    .expect("push task joins"),
                Err(WsSendError::Closed)
            );
        }

        assert_eq!(queue.usage(), (0, 0));
        assert!(
            timeout(Duration::from_secs(1), queue.pop())
                .await
                .expect("closed queue pop resolves")
                .is_none()
        );
    }
}

#[test]
fn test_queue_reports_usage_and_capacity_errors() {
    let count_limited = BoundedQueue::new(1, 2);
    count_limited
        .try_push(WsMessage::Text("a".to_owned()))
        .expect("first item fits the count and byte limits");
    assert_eq!(count_limited.usage(), (1, 1));
    assert_eq!(
        count_limited.try_push(WsMessage::Text("b".to_owned())),
        Err(WsSendError::Backpressure)
    );
    count_limited.close();
    assert_eq!(count_limited.usage(), (0, 0));
    assert_eq!(
        count_limited.try_push(WsMessage::Text("c".to_owned())),
        Err(WsSendError::Closed)
    );

    let byte_limited = BoundedQueue::new(2, 1);
    byte_limited
        .try_push(WsMessage::Text("x".to_owned()))
        .expect("first item fits the byte limit");
    assert_eq!(
        byte_limited.try_push(WsMessage::Text("y".to_owned())),
        Err(WsSendError::Backpressure)
    );
}

// Contains manual queue performance measurements.
mod performance_tests;
// Contains manual queue contention measurements.
mod contention_tests;
