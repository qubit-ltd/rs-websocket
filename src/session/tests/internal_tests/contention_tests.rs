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
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use futures_util::future::poll_fn;
use futures_util::poll;
use tokio::spawn;
use tokio::sync::Barrier;
use tokio::sync::Notify;
use tokio::task::yield_now;
use tokio::time::timeout;

use super::WakeCounter;
use super::text;
use super::waiter_sizes;
use crate::error::WsSendError;
use crate::session::internal::BoundedQueue;
use crate::ws_message::WsMessage;

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
