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

#[tokio::test]
#[ignore = "manual notified-prefix batch-release selector cost probe"]
async fn test_manual_batch_release_notified_prefix_probe() {
    fn percentile(values: &[u128], percentile: usize) -> u128 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        sorted[(sorted.len() * percentile).div_ceil(100).saturating_sub(1)]
    }

    fn sample(k: usize, batch: usize) -> u128 {
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

        let started = Instant::now();
        for _ in 0..batch {
            drop(queue.try_pop().unwrap());
        }
        let elapsed = started.elapsed().as_nanos();

        drop(futures);
        while let Some(envelope) = queue.try_pop() {
            drop(envelope);
        }
        assert_eq!(queue.usage(), (0, 0));
        elapsed
    }

    let selector_variant =
        std::env::var("BATCH_RELEASE_SELECTOR_VARIANT").unwrap_or_else(|_| "unmodified_selector".to_owned());
    eprintln!(
        "manual_batch_release_probe profile=release runtime=current_thread rustc={} rounds=3 warmups=3 samples=30 sizes=16,128,129,1024,4096 batches=1,min(16,K),K timing=reservation_release_only source={selector_variant}",
        option_env!("RUSTC_VERSION").unwrap_or("see evidence/toolchain.txt")
    );
    for round in 0..3 {
        for k in [16, 128, 129, 1_024, 4_096] {
            for batch in [1, 16.min(k), k] {
                for _ in 0..3 {
                    std::hint::black_box(sample(k, batch));
                }
                let values = (0..30).map(|_| sample(k, batch)).collect::<Vec<_>>();
                let total_p50 = percentile(&values, 50);
                let total_p95 = percentile(&values, 95);
                let per_release = values.iter().map(|value| value / batch as u128).collect::<Vec<_>>();
                eprintln!(
                    "round={round} k={k} batch={batch} raw_ns={values:?} total_p50_ns={total_p50} total_p95_ns={total_p95} per_release_p50_ns={} per_release_p95_ns={}",
                    percentile(&per_release, 50),
                    percentile(&per_release, 95)
                );
            }
        }
    }
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
#[ignore = "manual capacity-release selector cost probe; run with --ignored --nocapture"]
async fn test_capacity_release_after_unfit_prefix_probe() {
    fn percentile(values: &[u128], percentile: usize) -> u128 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
        sorted[index]
    }

    let irrelevant_waker = Waker::from(Arc::new(WakeCounter::default()));
    let mut irrelevant_context = Context::from_waker(&irrelevant_waker);
    eprintln!(
        "capacity_release_probe environment profile=release runtime=current_thread rustc=1.94.0 capacity_bytes=128 timing=reservation_release_and_manual_accept_poll rounds=7 sizes=1,16,128,1024,4096,16384"
    );

    for waiter_count in [1, 16, 128, 1_024, 4_096, 16_384] {
        let mut release_ns_samples = Vec::with_capacity(7);
        let mut acceptance_ns_samples = Vec::with_capacity(7);
        let mut cancellation_ns_samples = Vec::with_capacity(7);
        let mut close_ns_samples = Vec::with_capacity(7);

        // The unsuitable 65-byte waiters precede one 63-byte waiter. Two
        // 64-byte reservations fill the byte budget; dropping one must scan
        // the prefix and wake the only request that now fits.
        for round in 0..7 {
            let queue = BoundedQueue::new(waiter_count + 2, 128);
            queue.try_push(text(&"a".repeat(64))).unwrap();
            queue.try_push(text(&"b".repeat(64))).unwrap();
            let mut futures = (0..waiter_count)
                .map(|_| Box::pin(queue.push(text(&"x".repeat(65)))))
                .collect::<Vec<_>>();
            let mut unsuitable_polls = 0;
            for future in &mut futures {
                assert!(future.as_mut().poll(&mut irrelevant_context).is_pending());
                unsuitable_polls += 1;
            }
            let selected_counter = Arc::new(WakeCounter::default());
            let selected_waker = Waker::from(selected_counter.clone());
            let mut selected_context = Context::from_waker(&selected_waker);
            let mut selected = Box::pin(queue.push(text(&"s".repeat(63))));
            assert!(selected.as_mut().poll(&mut selected_context).is_pending());
            assert_eq!(queue.usage(), (2, 128));

            let started = Instant::now();
            drop(queue.try_pop().expect("release one 64-byte reservation"));
            let release_ns = started.elapsed().as_nanos();
            assert!(
                selected_counter.calls.load(Ordering::Acquire) > 0,
                "the fitting waiter behind {unsuitable_polls} unsuitable waiters is selected"
            );
            let accepted_started = Instant::now();
            assert_eq!(selected.as_mut().poll(&mut selected_context), Poll::Ready(Ok(())));
            let acceptance_ns = accepted_started.elapsed().as_nanos();
            assert_eq!(queue.usage(), (2, 127));
            release_ns_samples.push(release_ns);
            acceptance_ns_samples.push(acceptance_ns);

            // Cancel a selected waiter and measure the handoff to the next
            // fitting waiter, independently from release selection.
            let handoff_queue = BoundedQueue::new(4, 128);
            handoff_queue.try_push(text(&"a".repeat(64))).unwrap();
            handoff_queue.try_push(text(&"b".repeat(64))).unwrap();
            let mut first = Box::pin(handoff_queue.push(text(&"f".repeat(63))));
            let second_counter = Arc::new(WakeCounter::default());
            let second_waker = Waker::from(second_counter.clone());
            let mut second_context = Context::from_waker(&second_waker);
            let mut second = Box::pin(handoff_queue.push(text(&"g".repeat(63))));
            assert!(first.as_mut().poll(&mut irrelevant_context).is_pending());
            assert!(second.as_mut().poll(&mut second_context).is_pending());
            drop(handoff_queue.try_pop().unwrap());
            let cancel_started = Instant::now();
            drop(first);
            let cancellation_ns = cancel_started.elapsed().as_nanos();
            assert!(second_counter.calls.load(Ordering::Acquire) > 0);
            assert_eq!(second.as_mut().poll(&mut second_context), Poll::Ready(Ok(())));
            cancellation_ns_samples.push(cancellation_ns);
            drop(second);
            drop(handoff_queue.try_pop().unwrap());
            drop(handoff_queue.try_pop().unwrap());
            assert_eq!(handoff_queue.usage(), (0, 0));

            // Close is a separate control: it broadcasts rather than running
            // capacity selection. Every registered waiter must observe it.
            let close_queue = BoundedQueue::new(1, waiter_count + 1);
            close_queue.try_push(text("z")).unwrap();
            let mut close_waiters = (0..waiter_count)
                .map(|_| Box::pin(close_queue.push(text("w"))))
                .collect::<Vec<_>>();
            for future in &mut close_waiters {
                assert!(future.as_mut().poll(&mut irrelevant_context).is_pending());
            }
            let close_started = Instant::now();
            close_queue.close();
            let close_ns = close_started.elapsed().as_nanos();
            close_ns_samples.push(close_ns);
            for future in &mut close_waiters {
                assert_eq!(
                    future.as_mut().poll(&mut irrelevant_context),
                    Poll::Ready(Err(WsSendError::Closed))
                );
            }
            drop(close_waiters);
            assert_eq!(close_queue.usage(), (0, 0));

            // Count-limited/equal-size and zero-byte controls are correctness
            // checks: they verify the selector and fast-path cases remain
            // distinct from the incompatible-byte-prefix measurement.
            if round == 0 {
                let equal_queue = BoundedQueue::new(waiter_count + 2, 128);
                equal_queue.try_push(text(&"a".repeat(64))).unwrap();
                equal_queue.try_push(text(&"b".repeat(64))).unwrap();
                let mut equal_waiters = (0..waiter_count)
                    .map(|_| Box::pin(equal_queue.push(text(&"e".repeat(63)))))
                    .collect::<Vec<_>>();
                for future in &mut equal_waiters {
                    assert!(future.as_mut().poll(&mut irrelevant_context).is_pending());
                }
                drop(equal_queue.try_pop().unwrap());
                assert_eq!(
                    equal_waiters[0].as_mut().poll(&mut irrelevant_context),
                    Poll::Ready(Ok(()))
                );
                drop(equal_waiters);
                while let Some(item) = equal_queue.try_pop() {
                    drop(item);
                }
                assert_eq!(equal_queue.usage(), (0, 0));

                let count_queue = BoundedQueue::new(2, waiter_count + 128);
                count_queue.try_push(text(&"a".repeat(64))).unwrap();
                count_queue.try_push(text(&"b".repeat(64))).unwrap();
                let mut equal = Box::pin(count_queue.push(text("e")));
                assert!(equal.as_mut().poll(&mut irrelevant_context).is_pending());
                drop(count_queue.try_pop().unwrap());
                assert_eq!(equal.as_mut().poll(&mut irrelevant_context), Poll::Ready(Ok(())));
                drop(equal);
                drop(count_queue.try_pop().unwrap());
                drop(count_queue.try_pop().unwrap());
                assert_eq!(count_queue.usage(), (0, 0));

                let zero_queue = BoundedQueue::new(3, 128);
                zero_queue.try_push(text(&"a".repeat(64))).unwrap();
                zero_queue.try_push(text(&"b".repeat(64))).unwrap();
                zero_queue.push(text("")).await.unwrap();
                assert_eq!(zero_queue.usage(), (3, 128));
                while let Some(item) = zero_queue.try_pop() {
                    drop(item);
                }
                assert_eq!(zero_queue.usage(), (0, 0));
            }

            drop(selected);
            drop(futures);
            while let Some(item) = queue.try_pop() {
                drop(item);
            }
            assert_eq!(queue.usage(), (0, 0));
        }
        eprintln!(
            "capacity_release_probe raw waiters={waiter_count} release_ns={release_ns_samples:?} acceptance_ns={acceptance_ns_samples:?} cancellation_ns={cancellation_ns_samples:?} close_ns={close_ns_samples:?}"
        );
        eprintln!(
            "capacity_release_probe summary waiters={waiter_count} samples=7 release_p50_ns={} release_p95_ns={} acceptance_p50_ns={} acceptance_p95_ns={} cancellation_p50_ns={} cancellation_p95_ns={} close_p50_ns={} close_p95_ns={}",
            percentile(&release_ns_samples, 50),
            percentile(&release_ns_samples, 95),
            percentile(&acceptance_ns_samples, 50),
            percentile(&acceptance_ns_samples, 95),
            percentile(&cancellation_ns_samples, 50),
            percentile(&cancellation_ns_samples, 95),
            percentile(&close_ns_samples, 50),
            percentile(&close_ns_samples, 95),
        );
    }

    // A queue can see a contention spike and later return to a small waiter
    // set. Measure registration after that transition to catch index upkeep
    // that would otherwise persist after the queue is quiet again.
    let mut post_peak_registration_ns = Vec::with_capacity(7);
    let mut threshold_deactivation_ns = Vec::with_capacity(7);
    for _ in 0..7 {
        let queue = BoundedQueue::new(16_385, 128);
        queue.try_push(text(&"a".repeat(64))).unwrap();
        queue.try_push(text(&"b".repeat(64))).unwrap();
        let mut peak_waiters = (0..16_384).map(|_| Box::pin(queue.push(text("w")))).collect::<Vec<_>>();
        for future in &mut peak_waiters {
            assert!(future.as_mut().poll(&mut irrelevant_context).is_pending());
        }
        drop(peak_waiters.drain(..16_255));
        assert_eq!(peak_waiters.len(), 129);
        let started = Instant::now();
        drop(peak_waiters.pop().expect("cancel waiter crossing threshold"));
        threshold_deactivation_ns.push(started.elapsed().as_nanos());
        drop(peak_waiters.drain(..112));
        assert_eq!(peak_waiters.len(), 16);
        let started = Instant::now();
        let mut quiet_waiters = (0..16).map(|_| Box::pin(queue.push(text("q")))).collect::<Vec<_>>();
        for future in &mut quiet_waiters {
            assert!(future.as_mut().poll(&mut irrelevant_context).is_pending());
        }
        post_peak_registration_ns.push(started.elapsed().as_nanos());
        drop(quiet_waiters);
        drop(peak_waiters);
        drop(queue.try_pop().unwrap());
        drop(queue.try_pop().unwrap());
        assert_eq!(queue.usage(), (0, 0));
    }
    eprintln!(
        "capacity_release_probe summary peak_then_recede_waiters=16384->16 samples=7 threshold_cancel_p50_ns={} threshold_cancel_p95_ns={} threshold_cancel_raw={threshold_deactivation_ns:?} registration_16_p50_ns={} registration_16_p95_ns={} registration_raw={post_peak_registration_ns:?}",
        percentile(&threshold_deactivation_ns, 50),
        percentile(&threshold_deactivation_ns, 95),
        percentile(&post_peak_registration_ns, 50),
        percentile(&post_peak_registration_ns, 95),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual waiter threshold churn performance probe; run with --ignored --nocapture"]
async fn test_manual_waiter_threshold_churn_probe() {
    fn percentile(values: &[u128], percentile: usize) -> u128 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
        sorted[index]
    }

    const BATCHES: usize = 30;
    const WARMUP_BATCHES: usize = 3;
    const CHURN_PER_BATCH: usize = 64;
    const ROUNDS: usize = 3;
    const WAITER_COUNTS: [usize; 6] = [16, 64, 128, 129, 256, 1_024];
    let waker = Waker::from(Arc::new(WakeCounter::default()));
    let mut context = Context::from_waker(&waker);
    eprintln!(
        "threshold_churn environment profile=release runtime=tokio_multi_thread workers=2 rustc=record_separately batches={BATCHES} rounds={ROUNDS} churn_per_batch={CHURN_PER_BATCH} sizes=16,64,128,129,256,1024 timing=first_poll_cancel_and_complete_cycle units=ns"
    );

    // Interleave sizes in every round so a thermal or clock shift does not
    // turn into a size-specific result. Each batch constructs its baseline
    // pending set before starting the timer.
    for round in 0..ROUNDS {
        for scenario in 0..WAITER_COUNTS.len() {
            let waiter_count = WAITER_COUNTS[(scenario + round) % WAITER_COUNTS.len()];
            let mut first_poll = Vec::with_capacity(BATCHES);
            let mut cancel = Vec::with_capacity(BATCHES);
            let mut cycle = Vec::with_capacity(BATCHES);
            for batch in 0..(BATCHES + WARMUP_BATCHES) {
                let queue = BoundedQueue::new(waiter_count + CHURN_PER_BATCH + 1, 1);
                queue.try_push(text("o")).unwrap();
                let mut baseline = (0..waiter_count)
                    .map(|_| Box::pin(queue.push(text("w"))))
                    .collect::<Vec<_>>();
                for pending in &mut baseline {
                    assert!(pending.as_mut().poll(&mut context).is_pending());
                }
                assert_eq!(queue.usage(), (1, 1));

                let mut first_poll_batch = 0u128;
                let mut cancel_batch = 0u128;
                let started = Instant::now();
                for _ in 0..CHURN_PER_BATCH {
                    let mut newcomer = Box::pin(queue.push(text("n")));
                    let poll_started = Instant::now();
                    assert!(newcomer.as_mut().poll(&mut context).is_pending());
                    first_poll_batch += poll_started.elapsed().as_nanos();
                    let cancel_started = Instant::now();
                    drop(newcomer);
                    cancel_batch += cancel_started.elapsed().as_nanos();
                }
                let cycle_ns = started.elapsed().as_nanos();
                if batch >= WARMUP_BATCHES {
                    cycle.push(cycle_ns);
                    first_poll.push(first_poll_batch / CHURN_PER_BATCH as u128);
                    cancel.push(cancel_batch / CHURN_PER_BATCH as u128);
                }
                assert_eq!(queue.usage(), (1, 1));
                drop(baseline);
                drop(queue.try_pop().unwrap());
                assert_eq!(queue.usage(), (0, 0));
                if batch >= WARMUP_BATCHES {
                    eprintln!(
                        "threshold_churn raw round={round} batch={} waiters={waiter_count} first_poll_avg_ns={} cancel_avg_ns={} cycle_ns={cycle_ns}",
                        batch - WARMUP_BATCHES,
                        first_poll.last().unwrap(),
                        cancel.last().unwrap()
                    );
                }
            }
            eprintln!(
                "threshold_churn summary round={round} waiters={waiter_count} batches={BATCHES} first_poll_p50_ns={} cancel_p50_ns={} cycle_p50_ns={} cycle_p95_ns={} cycle_samples_ns={cycle:?}",
                percentile(&first_poll, 50),
                percentile(&cancel, 50),
                percentile(&cycle, 50),
                percentile(&cycle, 95)
            );
            tokio::task::yield_now().await;
        }
    }

    // Contention supplement: a second Tokio worker repeatedly reserves and
    // releases a distinct queue while this worker performs the same churn.
    let running = Arc::new(AtomicUsize::new(1));
    let operations = Arc::new(AtomicUsize::new(0));
    let competitor_running = running.clone();
    let competitor_operations = operations.clone();
    let competitor_queue = BoundedQueue::new(1, 1);
    let competitor = tokio::spawn(async move {
        while competitor_running.load(Ordering::Relaxed) != 0 {
            if competitor_queue.try_push(text("c")).is_ok() {
                drop(competitor_queue.try_pop().unwrap());
                competitor_operations.fetch_add(1, Ordering::Relaxed);
            }
            yield_now().await;
        }
    });
    yield_now().await;
    let mut contention = Vec::with_capacity(BATCHES);
    for batch in 0..(BATCHES + WARMUP_BATCHES) {
        let queue = BoundedQueue::new(130, 1);
        queue.try_push(text("o")).unwrap();
        let mut baseline = (0..128).map(|_| Box::pin(queue.push(text("w")))).collect::<Vec<_>>();
        for pending in &mut baseline {
            assert!(pending.as_mut().poll(&mut context).is_pending());
        }
        let started = Instant::now();
        for _ in 0..CHURN_PER_BATCH {
            let mut newcomer = Box::pin(queue.push(text("n")));
            assert!(newcomer.as_mut().poll(&mut context).is_pending());
            drop(newcomer);
        }
        let elapsed_ns = started.elapsed().as_nanos();
        if batch >= WARMUP_BATCHES {
            contention.push(elapsed_ns);
        }
        drop(baseline);
        drop(queue.try_pop().unwrap());
    }
    running.store(0, Ordering::Relaxed);
    competitor.await.unwrap();
    eprintln!(
        "threshold_churn contention workers=2 waiters=128 batches={BATCHES} cycle_p50_ns={} cycle_p95_ns={} cycles_ns={contention:?} competitor_operations={}",
        percentile(&contention, 50),
        percentile(&contention, 95),
        operations.load(Ordering::Relaxed)
    );
}

#[tokio::test]
#[ignore = "manual waiter payload diversity performance probe; run with --ignored --nocapture"]
async fn test_manual_waiter_payload_diversity_probe() {
    fn percentile(values: &[u128], percentile: usize) -> u128 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
        sorted[index]
    }

    let small = std::env::var_os("WAITER_ORDER_PROBE_SMALL").is_some();
    let batches = if small { 2 } else { 30 };
    let warmup_batches = if small { 1 } else { 3 };
    let rounds = if small { 1 } else { 3 };
    let sizes = if small {
        vec![1usize, 16]
    } else {
        vec![1usize, 16, 128, 1_024, 4_096]
    };
    let orders = ["ascending", "descending", "seeded_shuffle", "blocked_prefix"];
    let ratios = [("zero", 0usize), ("quarter", 1usize), ("all", 2usize)];
    let selector_variant = std::env::var("WAITER_SELECTOR_VARIANT").unwrap_or_else(|_| "unspecified".to_owned());
    eprintln!(
        "payload_diversity environment selector={selector_variant} profile=release runtime=current_thread rustc=record_separately seed=0x98470bb0 warmup_batches={warmup_batches} measured_batches={batches} rounds={rounds} small={small} orders=ascending,descending,seeded_shuffle,blocked_prefix eligible_target=0,25,100 timing=reservation_release_only units=ns"
    );

    // Rotate K, order, and target across rounds to limit drift. Sequence and
    // payload construction is outside the timed release operation.
    for round in 0..rounds {
        for scenario in 0..sizes.len() {
            let k = sizes[(scenario + round) % sizes.len()];
            for order_scenario in 0..orders.len() {
                let order = orders[(order_scenario + round) % orders.len()];
                let prefix = order == "blocked_prefix";
                let registration_sizes = waiter_sizes(k, order);
                for ratio_scenario in 0..ratios.len() {
                    let (ratio_name, ratio) = ratios[(ratio_scenario + round) % ratios.len()];
                    let free_bytes = match ratio {
                        0 => 0,
                        1 => (k / 4).max(1),
                        _ => k,
                    };
                    let max_bytes = if prefix { k + 1 } else { k };
                    let released_bytes = if free_bytes == 0 { 0 } else { free_bytes };
                    let mut samples = Vec::with_capacity(batches);
                    let mut eligible_counts = Vec::with_capacity(batches);
                    for batch in 0..(batches + warmup_batches) {
                        let queue = BoundedQueue::new(2, max_bytes);
                        if released_bytes == 0 {
                            queue.try_push(text("")).unwrap();
                            queue.try_push(text(&"f".repeat(max_bytes))).unwrap();
                        } else {
                            queue.try_push(text(&"r".repeat(released_bytes))).unwrap();
                            queue.try_push(text(&"f".repeat(max_bytes - released_bytes))).unwrap();
                        }
                        let counters = registration_sizes
                            .iter()
                            .map(|_| Arc::new(WakeCounter::default()))
                            .collect::<Vec<_>>();
                        let waiters = counters.iter().cloned().map(Waker::from).collect::<Vec<_>>();
                        let payloads = registration_sizes
                            .iter()
                            .map(|&n| text(&"w".repeat(n)))
                            .collect::<Vec<_>>();
                        let mut futures = payloads
                            .into_iter()
                            .map(|payload| Box::pin(queue.push(payload)))
                            .collect::<Vec<_>>();
                        for (future, waiter) in futures.iter_mut().zip(&waiters) {
                            let mut waiter_context = Context::from_waker(waiter);
                            assert!(future.as_mut().poll(&mut waiter_context).is_pending());
                        }
                        assert_eq!(queue.usage(), (2, max_bytes));
                        let eligible = registration_sizes.iter().filter(|&&bytes| bytes <= free_bytes).count();
                        let expected = registration_sizes.iter().position(|&bytes| bytes <= free_bytes);
                        let started = Instant::now();
                        drop(queue.try_pop().expect("release selected reservation"));
                        let release_ns = started.elapsed().as_nanos();
                        eligible_counts.push(eligible);
                        if let Some(expected) = expected {
                            assert!(
                                counters[expected].calls.load(Ordering::Acquire) > 0,
                                "first fitting registration at {expected} ({} bytes) is woken; order={order} target={ratio_name}",
                                registration_sizes[expected]
                            );
                            assert!(
                                counters
                                    .iter()
                                    .enumerate()
                                    .all(|(index, counter)| index == expected
                                        || counter.calls.load(Ordering::Acquire) == 0),
                                "only first fitting registration {expected} is selected; order={order} target={ratio_name}"
                            );
                        } else {
                            assert!(
                                counters
                                    .iter()
                                    .all(|counter| counter.calls.load(Ordering::Acquire) == 0),
                                "no ineligible waiter is reported as selected"
                            );
                        }
                        drop(futures);
                        drop(queue.try_pop().unwrap());
                        assert_eq!(queue.usage(), (0, 0));
                        if batch >= warmup_batches {
                            samples.push(release_ns);
                            eprintln!(
                                "payload_diversity raw round={round} batch={} K={k} order={order} prefix={prefix} target={ratio_name} free_bytes={free_bytes} eligible={eligible}/{} expected_position={expected:?} release_ns={release_ns}",
                                batch - warmup_batches,
                                registration_sizes.len()
                            );
                        }
                    }
                    eprintln!(
                        "payload_diversity summary round={round} K={k} order={order} prefix={prefix} target={ratio_name} free_bytes={free_bytes} batches={batches} actual_eligible={}/{} release_p50_ns={} release_p95_ns={} samples_ns={samples:?}",
                        eligible_counts[0],
                        registration_sizes.len(),
                        percentile(&samples, 50),
                        percentile(&samples, 95)
                    );
                    tokio::task::yield_now().await;
                }
            }
        }
    }
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual registration-order contention probe; run with --ignored --nocapture"]
async fn test_manual_waiter_registration_order_contention_probe() {
    let current_queue = Arc::new(std::sync::Mutex::new(None::<BoundedQueue>));
    let competitor_queue = current_queue.clone();
    let attempts = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::new(AtomicUsize::new(0));
    let backpressure = Arc::new(AtomicUsize::new(0));
    let competitor_attempts = attempts.clone();
    let competitor_accepted = accepted.clone();
    let competitor_backpressure = backpressure.clone();
    let start = Arc::new(Barrier::new(2));
    let competitor_start = start.clone();
    let stop = Arc::new(AtomicUsize::new(0));
    let competitor_stop = stop.clone();
    let competitor = tokio::spawn(async move {
        competitor_start.wait().await;
        while competitor_stop.load(Ordering::Relaxed) == 0 {
            let queue = competitor_queue.lock().unwrap().clone();
            if let Some(queue) = queue {
                competitor_attempts.fetch_add(1, Ordering::Relaxed);
                match queue.try_push(text("c")) {
                    Ok(()) => {
                        drop(queue.try_pop().unwrap());
                        competitor_accepted.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(WsSendError::Backpressure) => {
                        competitor_backpressure.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {}
                }
            }
            yield_now().await;
        }
    });
    eprintln!(
        "contention_probe environment profile=release runtime=multi_thread workers=2 seed=0x98470bb0 K=1024,4096 order=blocked_prefix targets=quarter,all timing=reservation_release_only competitor=try_push_1_byte_then_pop"
    );
    start.wait().await;
    for round in 0..3 {
        for k in [1_024usize, 4_096] {
            for (target, free_bytes) in [("quarter", k / 4), ("all", k)] {
                let queue = BoundedQueue::new(2, k + 1);
                queue.try_push(text(&"r".repeat(free_bytes))).unwrap();
                queue.try_push(text(&"f".repeat(k + 1 - free_bytes))).unwrap();
                let sizes = waiter_sizes(k, "blocked_prefix");
                let wakers = sizes
                    .iter()
                    .map(|_| Waker::from(Arc::new(WakeCounter::default())))
                    .collect::<Vec<_>>();
                let mut contexts = wakers.iter().map(Context::from_waker).collect::<Vec<_>>();
                let mut futures = sizes
                    .iter()
                    .map(|&size| Box::pin(queue.push(text(&"w".repeat(size)))))
                    .collect::<Vec<_>>();
                for (future, context) in futures.iter_mut().zip(&mut contexts) {
                    assert!(future.as_mut().poll(context).is_pending());
                }
                *current_queue.lock().unwrap() = Some(queue.clone());
                let attempts_before = attempts.load(Ordering::Relaxed);
                let accepted_before = accepted.load(Ordering::Relaxed);
                let backpressure_before = backpressure.load(Ordering::Relaxed);
                let started = Instant::now();
                drop(queue.try_pop().expect("release first reservation"));
                let release_ns = started.elapsed().as_nanos();
                tokio::task::yield_now().await;
                *current_queue.lock().unwrap() = None;
                let competitor_attempts = attempts.load(Ordering::Relaxed) - attempts_before;
                let competitor_accepted = accepted.load(Ordering::Relaxed) - accepted_before;
                let competitor_backpressure = backpressure.load(Ordering::Relaxed) - backpressure_before;
                drop(futures);
                while queue.try_pop().is_some() {}
                assert_eq!(queue.usage(), (0, 0));
                eprintln!(
                    "contention_probe raw K={k} order=blocked_prefix target={target} free_bytes={free_bytes} round={round} release_ns={release_ns} competitor_attempts={competitor_attempts} competitor_accepted={competitor_accepted} competitor_backpressure={competitor_backpressure}"
                );
            }
        }
    }
    stop.store(1, Ordering::Relaxed);
    competitor.await.unwrap();
    eprintln!(
        "contention_probe summary competitor_total_attempts={} competitor_total_accepted={} competitor_total_backpressure={}",
        attempts.load(Ordering::Relaxed),
        accepted.load(Ordering::Relaxed),
        backpressure.load(Ordering::Relaxed)
    );
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
#[ignore = "manual registration cost probe; run with --ignored --nocapture"]
async fn test_manual_waiter_registration_cost_probe() {
    fn percentile(values: &[u128], percentile: usize) -> u128 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
        sorted[index]
    }

    let waker = Waker::from(Arc::new(WakeCounter::default()));
    let mut context = Context::from_waker(&waker);
    eprintln!(
        "registration_probe environment profile=release runtime=current_thread rustc=1.94.0 payload=alternating_65_63 budget_bytes=128 timing=first_poll_to_pending rounds=7"
    );
    for waiter_count in [128, 1_024, 4_096, 16_384] {
        let warmup_queue = BoundedQueue::new(waiter_count + 1, 128);
        warmup_queue.try_push(text(&"o".repeat(128))).unwrap();
        let warmup_payloads: Vec<_> = (0..waiter_count)
            .map(|index| text(&"w".repeat(if index % 2 == 0 { 65 } else { 63 })))
            .collect();
        let mut warmup_futures: Vec<_> = warmup_payloads
            .into_iter()
            .map(|payload| Box::pin(warmup_queue.push(payload)))
            .collect();
        for future in &mut warmup_futures {
            assert!(future.as_mut().poll(&mut context).is_pending());
        }
        drop(warmup_futures);
        drop(warmup_queue.try_pop().unwrap());
        assert_eq!(warmup_queue.usage(), (0, 0));
        eprintln!("registration_probe warmup waiter_count={waiter_count} scenario=byte_limited");

        let mut rounds = Vec::with_capacity(7);
        for round in 0..7 {
            let queue = BoundedQueue::new(waiter_count + 1, 128);
            queue.try_push(text(&"o".repeat(128))).unwrap();
            let payloads: Vec<_> = (0..waiter_count)
                .map(|index| text(&"w".repeat(if index % 2 == 0 { 65 } else { 63 })))
                .collect();
            let mut futures: Vec<_> = payloads
                .into_iter()
                .map(|payload| Box::pin(queue.push(payload)))
                .collect();
            let started = Instant::now();
            for future in &mut futures {
                assert!(future.as_mut().poll(&mut context).is_pending());
            }
            let elapsed = started.elapsed().as_nanos();
            rounds.push(elapsed);
            assert_eq!(queue.usage(), (1, 128));
            drop(futures);
            drop(queue.try_pop().expect("remove occupied message"));
            assert_eq!(queue.usage(), (0, 0));
            eprintln!(
                "registration_probe raw waiter_count={waiter_count} scenario=byte_limited round={round} ns={elapsed} ns_per_registration={}",
                elapsed / waiter_count as u128
            );
        }
        eprintln!(
            "registration_probe summary waiter_count={waiter_count} scenario=byte_limited rounds_ns={rounds:?} p50_ns={} p95_ns={} p50_ns_per_registration={} p95_ns_per_registration={} samples=7",
            percentile(&rounds, 50),
            percentile(&rounds, 95),
            percentile(&rounds, 50) / waiter_count as u128,
            percentile(&rounds, 95) / waiter_count as u128
        );
    }

    // Count-limited control: the selector exits before scanning any waiters.
    for waiter_count in [128, 1_024, 4_096, 16_384] {
        let mut rounds = Vec::with_capacity(7);
        for round in 0..7 {
            let queue = BoundedQueue::new(1, waiter_count * 128 + 128);
            queue.try_push(text("occupied")).unwrap();
            let payloads: Vec<_> = (0..waiter_count).map(|_| text("w")).collect();
            let mut futures: Vec<_> = payloads
                .into_iter()
                .map(|payload| Box::pin(queue.push(payload)))
                .collect();
            let started = Instant::now();
            for future in &mut futures {
                assert!(future.as_mut().poll(&mut context).is_pending());
            }
            let elapsed = started.elapsed().as_nanos();
            rounds.push(elapsed);
            assert_eq!(queue.usage(), (1, 8));
            eprintln!(
                "registration_probe raw waiter_count={waiter_count} scenario=count_limited round={round} ns={elapsed}"
            );
            drop(futures);
            drop(queue.try_pop().unwrap());
            assert_eq!(queue.usage(), (0, 0));
        }
        eprintln!(
            "registration_probe summary waiter_count={waiter_count} scenario=count_limited rounds_ns={rounds:?} p50_ns={} p95_ns={} samples=7",
            percentile(&rounds, 50),
            percentile(&rounds, 95)
        );
    }

    // Zero-byte messages use the no-wait enqueue path even when byte capacity
    // is exhausted; this control measures that path, not waiter registration.
    for waiter_count in [128, 1_024, 4_096, 16_384] {
        let mut rounds = Vec::with_capacity(7);
        for round in 0..7 {
            let queue = BoundedQueue::new(waiter_count + 1, 128);
            queue.try_push(text(&"o".repeat(128))).unwrap();
            let payloads: Vec<_> = (0..waiter_count).map(|_| text("")).collect();
            let mut futures: Vec<_> = payloads
                .into_iter()
                .map(|payload| Box::pin(queue.push(payload)))
                .collect();
            let started = Instant::now();
            for future in &mut futures {
                assert_eq!(future.as_mut().poll(&mut context), Poll::Ready(Ok(())));
            }
            let elapsed = started.elapsed().as_nanos();
            rounds.push(elapsed);
            assert_eq!(queue.usage(), (waiter_count + 1, 128));
            drop(futures);
            assert_eq!(queue.try_pop().unwrap().into_item(), text(&"o".repeat(128)));
            for _ in 0..waiter_count {
                assert_eq!(queue.try_pop().unwrap().into_item(), text(""));
            }
            assert_eq!(queue.usage(), (0, 0));
            eprintln!(
                "registration_probe raw waiter_count={waiter_count} scenario=zero_byte_fast_path round={round} ns={elapsed}"
            );
        }
        eprintln!(
            "registration_probe summary waiter_count={waiter_count} scenario=zero_byte_fast_path rounds_ns={rounds:?} p50_ns={} p95_ns={} samples=7",
            percentile(&rounds, 50),
            percentile(&rounds, 95)
        );
    }

    // Measure whether a separate OS thread continues to run while one current-
    // thread executor performs the synchronous registration workload.
    let running = Arc::new(AtomicUsize::new(1));
    let ticks = Arc::new(AtomicUsize::new(0));
    let thread_running = running.clone();
    let thread_ticks = ticks.clone();
    let heartbeat = std::thread::spawn(move || {
        while thread_running.load(Ordering::Relaxed) != 0 {
            thread_ticks.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_micros(100));
        }
    });
    while ticks.load(Ordering::Relaxed) == 0 {
        std::thread::yield_now();
    }
    let queue = BoundedQueue::new(4_097, 128);
    queue.try_push(text(&"o".repeat(128))).unwrap();
    let payloads: Vec<_> = (0..4_096)
        .map(|index| text(&"w".repeat(if index % 2 == 0 { 65 } else { 63 })))
        .collect();
    let mut futures: Vec<_> = payloads
        .into_iter()
        .map(|payload| Box::pin(queue.push(payload)))
        .collect();
    let ticks_before = ticks.load(Ordering::Relaxed);
    let started = Instant::now();
    for future in &mut futures {
        assert!(future.as_mut().poll(&mut context).is_pending());
    }
    let elapsed = started.elapsed().as_nanos();
    let heartbeat_ticks = ticks.load(Ordering::Relaxed) - ticks_before;
    eprintln!(
        "registration_probe heartbeat scenario=current_thread waiter_count=4096 ns={elapsed} heartbeat_ticks={heartbeat_ticks} interval_us=100"
    );
    assert_eq!(queue.usage(), (1, 128));
    drop(futures);
    drop(queue.try_pop().unwrap());
    assert_eq!(queue.usage(), (0, 0));
    running.store(0, Ordering::Relaxed);
    heartbeat.join().unwrap();

    // A bounded progress check after release ensures the probe's setup models
    // the normal wake-and-retry path as well as Pending registration.
    let queue = BoundedQueue::new(2, 1);
    queue.try_push(text("x")).unwrap();
    let mut pending = Box::pin(queue.push(text("y")));
    assert!(pending.as_mut().poll(&mut context).is_pending());
    assert_eq!(queue.usage(), (1, 1));
    drop(queue.try_pop().unwrap());
    tokio::time::timeout(Duration::from_secs(2), pending.as_mut())
        .await
        .unwrap()
        .unwrap();
    drop(queue.try_pop().unwrap());
    assert_eq!(queue.usage(), (0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual multi-thread capacity-release probe; run with --ignored --nocapture"]
async fn test_manual_multi_thread_capacity_release_contention_probe() {
    async fn wait_for_registration(
        queue: BoundedQueue,
        message: WsMessage,
        registered: tokio::sync::mpsc::UnboundedSender<()>,
    ) -> Result<(), WsSendError> {
        let mut push = Box::pin(queue.push(message));
        let mut registered = Some(registered);
        futures_util::future::poll_fn(|context| {
            let result = push.as_mut().poll(context);
            if result.is_pending()
                && let Some(registered) = registered.take()
            {
                registered.send(()).expect("registration receiver remains open");
            }
            result
        })
        .await
    }

    fn percentile(values: &[u128], percentile: usize) -> u128 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
        sorted[index]
    }

    let waiter_count = 16_384;
    let mut release_ns_samples = Vec::with_capacity(7);
    let mut acceptance_ns_samples = Vec::with_capacity(7);
    for round in 0..7 {
        let queue = BoundedQueue::new(waiter_count + 2, 128);
        queue.try_push(text(&"a".repeat(64))).unwrap();
        queue.try_push(text(&"b".repeat(64))).unwrap();
        let (registered_tx, mut registered_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut waiters = Vec::with_capacity(waiter_count);
        for _ in 0..waiter_count {
            let queue = queue.clone();
            let registered_tx = registered_tx.clone();
            waiters.push(tokio::spawn(wait_for_registration(
                queue,
                text(&"x".repeat(65)),
                registered_tx,
            )));
        }
        for _ in 0..waiter_count {
            timeout(Duration::from_secs(10), registered_rx.recv())
                .await
                .expect("waiter registration deadline")
                .expect("registration signal remains available");
        }

        let queue_for_fit = queue.clone();
        let fitting_waiter = tokio::spawn(wait_for_registration(
            queue_for_fit,
            text(&"s".repeat(63)),
            registered_tx.clone(),
        ));
        timeout(Duration::from_secs(10), registered_rx.recv())
            .await
            .expect("fitting waiter registration deadline")
            .expect("fitting waiter registration signal remains available");

        let started = Instant::now();
        drop(queue.try_pop().expect("release one 64-byte reservation"));
        release_ns_samples.push(started.elapsed().as_nanos());
        let accepted_started = Instant::now();
        timeout(Duration::from_secs(10), fitting_waiter)
            .await
            .expect("fitting waiter admission deadline")
            .expect("fitting waiter task joins")
            .expect("fitting waiter is admitted");
        acceptance_ns_samples.push(accepted_started.elapsed().as_nanos());

        queue.close();
        for waiter in waiters {
            assert_eq!(
                timeout(Duration::from_secs(10), waiter)
                    .await
                    .expect("unfit waiter close deadline")
                    .expect("unfit waiter task joins"),
                Err(WsSendError::Closed)
            );
        }
        assert_eq!(queue.usage(), (0, 0));
        eprintln!(
            "multi_thread_capacity_release raw waiters={waiter_count} round={round} release_ns={} acceptance_ns={}",
            release_ns_samples[round], acceptance_ns_samples[round]
        );
    }
    eprintln!(
        "multi_thread_capacity_release summary waiters={waiter_count} rounds=7 release_p50_ns={} release_p95_ns={} accepted_p50={} accepted_p95={}",
        percentile(&release_ns_samples, 50),
        percentile(&release_ns_samples, 95),
        percentile(&acceptance_ns_samples, 50),
        percentile(&acceptance_ns_samples, 95),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual two-worker registration supplement; run with --ignored --nocapture"]
async fn test_manual_multi_thread_waiter_registration_cost_probe() {
    let waker = Waker::from(Arc::new(WakeCounter::default()));
    let mut context = Context::from_waker(&waker);
    let running = Arc::new(AtomicUsize::new(1));
    let ticks = Arc::new(AtomicUsize::new(0));
    let heartbeat_running = running.clone();
    let heartbeat_ticks = ticks.clone();
    let heartbeat = tokio::spawn(async move {
        while heartbeat_running.load(Ordering::Relaxed) != 0 {
            heartbeat_ticks.fetch_add(1, Ordering::Relaxed);
            yield_now().await;
        }
    });
    yield_now().await;
    for waiter_count in [128, 1_024, 4_096, 16_384] {
        let queue = BoundedQueue::new(waiter_count + 1, 128);
        queue.try_push(text(&"o".repeat(128))).unwrap();
        let payloads: Vec<_> = (0..waiter_count)
            .map(|index| text(&"w".repeat(if index % 2 == 0 { 65 } else { 63 })))
            .collect();
        let mut futures: Vec<_> = payloads
            .into_iter()
            .map(|payload| Box::pin(queue.push(payload)))
            .collect();
        let ticks_before = ticks.load(Ordering::Relaxed);
        let started = Instant::now();
        for future in &mut futures {
            assert!(future.as_mut().poll(&mut context).is_pending());
        }
        let elapsed = started.elapsed().as_nanos();
        let heartbeat_ticks = ticks.load(Ordering::Relaxed) - ticks_before;
        eprintln!(
            "registration_probe_multithread raw waiter_count={waiter_count} ns={elapsed} heartbeat_ticks={heartbeat_ticks}"
        );
        assert_eq!(queue.usage(), (1, 128));
        drop(futures);
        drop(queue.try_pop().unwrap());
        assert_eq!(queue.usage(), (0, 0));
    }
    running.store(0, Ordering::Relaxed);
    heartbeat.await.unwrap();
}

#[tokio::test]
#[ignore = "manual contention probe; run with --ignored --nocapture"]
async fn test_manual_capacity_wake_progress_probe() {
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
#[ignore = "manual queue cancellation and close probe; run with --ignored --nocapture"]
async fn test_manual_queue_follow_up_progress_probe() {
    fn percentile(values: &[u128], percentile: usize) -> u128 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
        sorted[index]
    }

    async fn heartbeat_baseline() -> Vec<u128> {
        let stop = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(AtomicUsize::new(0));
        let samples = Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_stop = stop.clone();
        let task_seen = seen.clone();
        let task_samples = samples.clone();
        let task = spawn(async move {
            let mut previous = Instant::now();
            while task_stop.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let now = Instant::now();
                task_samples
                    .lock()
                    .unwrap()
                    .push(now.duration_since(previous).as_nanos());
                previous = now;
                task_seen.fetch_add(1, Ordering::Release);
            }
        });
        while seen.load(Ordering::Acquire) < 20 {
            yield_now().await;
        }
        stop.store(1, Ordering::Release);
        task.await.expect("heartbeat task joins");
        samples.lock().unwrap().clone()
    }

    async fn measured_drop<T>(items: Vec<T>) -> (u128, u128, bool) {
        let epoch = Instant::now();
        let last_heartbeat_ns = Arc::new(AtomicUsize::new(0));
        let stop_heartbeat = Arc::new(AtomicUsize::new(0));
        let heartbeat_value = last_heartbeat_ns.clone();
        let heartbeat_stop = stop_heartbeat.clone();
        let heartbeat = spawn(async move {
            while heartbeat_stop.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                heartbeat_value.store(epoch.elapsed().as_nanos() as usize, Ordering::Release);
            }
        });
        while last_heartbeat_ns.load(Ordering::Acquire) == 0 {
            yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        let measuring = Arc::new(AtomicUsize::new(1));
        let max_gap_ns = Arc::new(AtomicUsize::new(0));
        let watchdog_timeout = Arc::new(AtomicUsize::new(0));
        let watcher_measuring = measuring.clone();
        let watcher_max_gap = max_gap_ns.clone();
        let watcher_timeout = watchdog_timeout.clone();
        let watcher_heartbeat = last_heartbeat_ns.clone();
        let measurement_epoch = Instant::now();
        let watcher = std::thread::spawn(move || {
            while watcher_measuring.load(Ordering::Acquire) != 0 {
                let now_ns = measurement_epoch.elapsed().as_nanos() as usize;
                let heartbeat_ns = watcher_heartbeat.load(Ordering::Acquire);
                let epoch_ns = epoch.elapsed().as_nanos() as usize;
                let gap_ns = epoch_ns.saturating_sub(heartbeat_ns);
                watcher_max_gap.fetch_max(gap_ns, Ordering::AcqRel);
                if now_ns > 30_000_000_000 {
                    watcher_timeout.store(1, Ordering::Release);
                    break;
                }
                std::thread::yield_now();
            }
        });
        let started = Instant::now();
        drop(items);
        let elapsed = started.elapsed().as_nanos();
        measuring.store(0, Ordering::Release);
        watcher.join().expect("independent cancellation watchdog joins");
        stop_heartbeat.store(1, Ordering::Release);
        heartbeat.await.expect("heartbeat task joins");
        (
            elapsed,
            max_gap_ns.load(Ordering::Acquire) as u128,
            watchdog_timeout.load(Ordering::Acquire) != 0,
        )
    }

    let heartbeat_idle = heartbeat_baseline().await;
    eprintln!(
        "queue_followup_probe environment profile=release rustc=1.94.0 runtime=current_thread heartbeat_idle_ns={heartbeat_idle:?}"
    );

    for payload_kind in ["text", "bytes"] {
        let mut payload = if payload_kind == "text" {
            text("baseline")
        } else {
            WsMessage::Binary(Bytes::from_static(b"baseline"))
        };
        let queue = BoundedQueue::new(1, 32);
        for _ in 0..10_000 {
            queue.push(payload).await.unwrap();
            payload = queue.try_pop().unwrap().into_item();
        }
        let mut rounds = Vec::with_capacity(7);
        for round in 0..7 {
            let started = Instant::now();
            for _ in 0..100_000 {
                queue.push(payload).await.unwrap();
                payload = queue.try_pop().unwrap().into_item();
            }
            let elapsed = started.elapsed();
            rounds.push(elapsed.as_nanos() / 100_000);
            eprintln!(
                "queue_followup_probe normal payload={payload_kind} round={round} iterations=100000 ns_per_op={} registrations=not_observable_from_this_module payload_reused=true",
                rounds[round]
            );
        }
        eprintln!(
            "queue_followup_probe normal_summary payload={payload_kind} rounds_ns_per_op={rounds:?} p50={} p95={}",
            percentile(&rounds, 50),
            percentile(&rounds, 95)
        );
    }

    for waiter_count in [128, 1024, 4096, 16384] {
        for mixed in [false, true] {
            for round in 0..7 {
                let max_bytes = if mixed { 128 } else { waiter_count + 1 };
                let queue = BoundedQueue::new(if mixed { waiter_count + 1 } else { 1 }, max_bytes);
                queue.try_push(text(&"f".repeat(if mixed { 128 } else { 1 }))).unwrap();
                let mut futures = Vec::with_capacity(waiter_count);
                for index in 0..waiter_count {
                    let message = match (mixed, index % 2) {
                        (true, 0) => text(&"m".repeat(65)),
                        (true, _) => WsMessage::Binary(Bytes::from(vec![b'b'; 63])),
                        (false, _) => text("w"),
                    };
                    futures.push(Box::pin(queue.push(message)));
                }
                for future in &mut futures {
                    assert!(
                        poll!(future.as_mut()).is_pending(),
                        "all waiters register before timing"
                    );
                }
                let (cancel_ns, heartbeat_max_gap_ns, watchdog_timeout) = measured_drop(futures).await;
                let after_cancel = queue.usage();
                drop(queue.try_pop().unwrap());
                let after_release = queue.usage();
                assert_eq!(after_release, (0, 0));
                eprintln!(
                    "queue_followup_probe cancel waiters={waiter_count} mixed={mixed} round={round} ns={cancel_ns} ns_per_waiter={} heartbeat_max_gap_ns={heartbeat_max_gap_ns} watchdog_timeout={watchdog_timeout} budget_after_cancel={after_cancel:?} budget_after_release={after_release:?}",
                    cancel_ns / waiter_count as u128
                );
            }
        }

        for round in 0..7 {
            let queue = BoundedQueue::new(1, waiter_count + 1);
            queue.try_push(text("f")).unwrap();
            let mut futures = (0..waiter_count)
                .map(|_| Box::pin(queue.push(text("w"))))
                .collect::<Vec<_>>();
            for future in &mut futures {
                assert!(poll!(future.as_mut()).is_pending());
            }
            let started = Instant::now();
            queue.close();
            let close_ns = started.elapsed().as_nanos();
            let drain_started = Instant::now();
            let mut closed = 0;
            for future in &mut futures {
                assert_eq!(poll!(future.as_mut()), Poll::Ready(Err(WsSendError::Closed)));
                closed += 1;
            }
            let drain_ns = drain_started.elapsed().as_nanos();
            drop(futures);
            assert_eq!(closed, waiter_count);
            assert_eq!(queue.usage(), (0, 0));
            eprintln!(
                "queue_followup_probe close waiters={waiter_count} round={round} close_ns={close_ns} drain_ns={drain_ns} closed={closed} residual_budget={:?}",
                queue.usage()
            );
        }

        for round in 0..7 {
            let queue = BoundedQueue::new(2, 2);
            queue.try_push(text("f")).unwrap();
            queue.try_push(text("g")).unwrap();
            let mut futures = (0..waiter_count)
                .map(|_| Box::pin(queue.push(text("w"))))
                .collect::<Vec<_>>();
            for future in &mut futures {
                assert!(poll!(future.as_mut()).is_pending());
            }
            let cancelled = futures.len() / 2;
            futures.drain(..cancelled);
            drop(queue.try_pop().unwrap());
            let mut progressed = 0;
            for future in &mut futures {
                if poll!(future.as_mut()).is_ready() {
                    progressed += 1;
                }
            }
            assert_eq!(progressed, 1, "remaining waiter progresses after partial cancellation");
            drop(futures);
            drop(queue.try_pop().unwrap());
            drop(queue.try_pop().unwrap());
            assert_eq!(queue.usage(), (0, 0));
            eprintln!(
                "queue_followup_probe partial_cancel waiters={waiter_count} round={round} cancelled={cancelled} progressed={progressed} residual_budget={:?}",
                queue.usage()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual multi-thread heartbeat supplement for queue_followup_probe"]
async fn test_manual_multi_thread_queue_follow_up_progress_probe() {
    for waiter_count in [128, 1024, 4096, 16384] {
        let mut rounds = Vec::with_capacity(7);
        for round in 0..7 {
            let queue = BoundedQueue::new(1, waiter_count + 1);
            queue.try_push(text("f")).unwrap();
            let mut futures = (0..waiter_count)
                .map(|_| Box::pin(queue.push(text("w"))))
                .collect::<Vec<_>>();
            for future in &mut futures {
                assert!(poll!(future.as_mut()).is_pending());
            }

            let epoch = Instant::now();
            let last_tick = Arc::new(AtomicUsize::new(0));
            let max_tick_gap_ns = Arc::new(AtomicUsize::new(0));
            let stop = Arc::new(AtomicUsize::new(0));
            let task_tick = last_tick.clone();
            let task_max_gap = max_tick_gap_ns.clone();
            let task_stop = stop.clone();
            let heartbeat = spawn(async move {
                let mut previous = Instant::now();
                while task_stop.load(Ordering::Acquire) == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    let now = Instant::now();
                    task_max_gap.fetch_max(now.duration_since(previous).as_nanos() as usize, Ordering::AcqRel);
                    previous = now;
                    task_tick.store(epoch.elapsed().as_nanos() as usize, Ordering::Release);
                }
            });
            while last_tick.load(Ordering::Acquire) == 0 {
                yield_now().await;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            let started = Instant::now();
            drop(futures);
            let cancel_ns = started.elapsed().as_nanos();
            yield_now().await;
            yield_now().await;
            stop.store(1, Ordering::Release);
            heartbeat.await.unwrap();
            drop(queue.try_pop().unwrap());
            assert_eq!(queue.usage(), (0, 0));
            let max_gap = max_tick_gap_ns.load(Ordering::Acquire);
            rounds.push(cancel_ns);
            eprintln!(
                "queue_followup_probe_multithread waiters={waiter_count} round={round} cancel_ns={cancel_ns} heartbeat_max_gap_ns={max_gap}"
            );
        }
        let mut sorted = rounds.clone();
        sorted.sort_unstable();
        eprintln!(
            "queue_followup_probe_multithread summary waiters={waiter_count} rounds_ns={rounds:?} p50_ns={} p95_ns={}",
            sorted[3], sorted[6]
        );
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
