// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::{text, waiter_sizes, WakeCounter};
use bytes::Bytes;
use futures_util::poll;
use crate::error::WsSendError;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;
use tokio::spawn;
use tokio::task::yield_now;
use crate::session::internal::BoundedQueue;
use crate::ws_message::WsMessage;

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
