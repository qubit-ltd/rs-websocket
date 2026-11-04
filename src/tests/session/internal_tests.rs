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
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures_util::future::poll_fn;
use futures_util::poll;
use tokio::spawn;
use tokio::sync::Barrier;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::time::timeout;

use crate::error::WsSendError;
use crate::session::internal::BoundedQueue;
use crate::ws_message::WsMessage;

fn text(payload: &str) -> WsMessage {
    WsMessage::Text(payload.to_owned())
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
async fn test_capacity_waiters_wake_one_that_fits_mixed_byte_budget() {
    let queue = BoundedQueue::new(2, 130);
    queue.try_push(text("h")).expect("fill one byte first");
    queue.try_push(text(&"f".repeat(128))).expect("fill remaining budget");
    let too_large_for_remaining = queue.clone();
    let larger = spawn(async move { too_large_for_remaining.push(text(&"l".repeat(65))).await });
    yield_now().await;
    let fits = queue.clone();
    let smaller = spawn(async move { fits.push(text("s")).await });
    yield_now().await;

    drop(queue.try_pop().expect("release one-byte message"));
    timeout(Duration::from_secs(1), smaller)
        .await
        .expect("one-byte waiter fits and is woken")
        .expect("small waiter joins")
        .expect("small waiter is admitted");
    assert!(!larger.is_finished(), "non-fitting waiter remains asleep");
    drop(queue.try_pop().expect("release 128-byte message"));
    timeout(Duration::from_secs(1), larger)
        .await
        .expect("larger waiter eventually fits")
        .expect("large waiter joins")
        .expect("large waiter is admitted");
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

#[tokio::test]
async fn test_close_wakes_all_registered_capacity_waiters() {
    let queue = BoundedQueue::new(1, 1);
    queue.try_push(text("x")).expect("fill queue");
    let mut waiters = Vec::new();
    for _ in 0..8 {
        let waiting_queue = queue.clone();
        waiters.push(spawn(async move { waiting_queue.push(text("y")).await }));
    }
    yield_now().await;
    queue.close();
    for waiter in waiters {
        let result = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("close wakes every registered waiter")
            .expect("waiter joins");
        assert_eq!(result.expect_err("closed queue rejects waiter"), WsSendError::Closed);
    }
}

#[tokio::test]
#[ignore = "manual contention probe; run with --ignored --nocapture"]
async fn wake_probe() {
    async fn wait_until(counter: &AtomicUsize, expected: usize, changed: &Notify) {
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if counter.load(Ordering::Acquire) >= expected {
                return;
            }
            notified.await;
        }
    }

    fn percentile(values: &[u128], percentile: usize) -> u128 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
        sorted[index]
    }

    for waiter_count in [1, 16, 128] {
        for mixed in [false, true] {
            for byte_limited in [false, true] {
                let configuration_started = Instant::now();
                let mut elapsed_micros = Vec::new();
                let mut poll_totals = Vec::new();
                for round in 0..5 {
                    let max_messages = if byte_limited { waiter_count + 1 } else { 1 };
                    let max_bytes = if byte_limited { 128 } else { (waiter_count + 1) * 128 };
                    let queue = BoundedQueue::new(max_messages, max_bytes);
                    queue
                        .try_push(text(&"f".repeat(128)))
                        .expect("probe starts with occupied capacity");
                    let polls = Arc::new(AtomicUsize::new(0));
                    let registered = Arc::new(AtomicUsize::new(0));
                    let accepted = Arc::new(AtomicUsize::new(0));
                    let poll_changed = Arc::new(Notify::new());
                    let registration_changed = Arc::new(Notify::new());
                    let accepted_changed = Arc::new(Notify::new());
                    let mut tasks = Vec::with_capacity(waiter_count);

                    for waiter in 0..waiter_count {
                        let queue = queue.clone();
                        let payload = if mixed && waiter % 2 == 0 { 65 } else { 128 };
                        let message = text(&"w".repeat(payload));
                        let polls = polls.clone();
                        let registered = registered.clone();
                        let accepted = accepted.clone();
                        let poll_changed = poll_changed.clone();
                        let registration_changed = registration_changed.clone();
                        let accepted_changed = accepted_changed.clone();
                        tasks.push(spawn(async move {
                            let mut pending = Box::pin(queue.push(message));
                            let mut saw_pending = false;
                            let result = poll_fn(|context| {
                                polls.fetch_add(1, Ordering::AcqRel);
                                poll_changed.notify_waiters();
                                let result = pending.as_mut().poll(context);
                                if result.is_pending() && !saw_pending {
                                    saw_pending = true;
                                    registered.fetch_add(1, Ordering::AcqRel);
                                    registration_changed.notify_waiters();
                                }
                                result
                            })
                            .await;
                            result.expect("queue stays open during measurement");
                            accepted.fetch_add(1, Ordering::AcqRel);
                            accepted_changed.notify_waiters();
                        }));
                    }

                    wait_until(&registered, waiter_count, &registration_changed).await;
                    let mut round_latencies = Vec::with_capacity(waiter_count);
                    for release in 1..=waiter_count {
                        let before = Instant::now();
                        drop(queue.try_pop().expect("one occupied message per release"));
                        wait_until(&accepted, release, &accepted_changed).await;
                        round_latencies.push(before.elapsed().as_micros());
                        // The selective candidate should poll only the waiter
                        // selected for this released capacity.
                        let polls_after_release = waiter_count + release;
                        wait_until(&polls, polls_after_release, &poll_changed).await;
                    }
                    for task in tasks {
                        task.await.expect("waiter task joins");
                    }
                    poll_totals.push(polls.load(Ordering::Acquire));
                    elapsed_micros.extend(round_latencies.iter().copied());
                    assert_eq!(accepted.load(Ordering::Acquire), waiter_count);
                    drop(queue.try_pop().expect("drop last admitted message"));
                    assert_eq!(queue.usage(), (0, 0));
                    eprintln!(
                        "wake_probe raw waiter_count={waiter_count} mixed={mixed} byte_limited={byte_limited} round={round} polls={} accept_us={round_latencies:?}",
                        poll_totals[round]
                    );
                }
                eprintln!(
                    "wake_probe summary waiter_count={waiter_count} mixed={mixed} byte_limited={byte_limited} rounds=5 polls={poll_totals:?} accept_p50_us={} accept_p95_us={} accept_p99_us={} elapsed_ms={}",
                    percentile(&elapsed_micros, 50),
                    percentile(&elapsed_micros, 95),
                    percentile(&elapsed_micros, 99),
                    configuration_started.elapsed().as_millis()
                );
            }
        }
    }
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
