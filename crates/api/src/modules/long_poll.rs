// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Long-poll detector, off by default (`[metrics].long-poll-threshold-ms`); needs a `--cfg tokio_unstable` build.
//! Logs every task poll longer than the threshold with the task's spawn location, to name what blocks a worker.

use std::time::Duration;

/// Installs the poll hooks on `builder`. Does nothing in a build without `tokio_unstable`.
pub fn install(builder: &mut tokio::runtime::Builder, threshold: Duration) {
    #[cfg(tokio_unstable)]
    hooks::install(builder, threshold);
    #[cfg(not(tokio_unstable))]
    let _ = (builder, threshold);
}

/// Logs at startup whether the detector runs, and the number of runtime workers.
pub fn log_status(threshold_ms: Option<u64>) {
    let Some(threshold_ms) = threshold_ms else {
        return;
    };
    let workers = tokio::runtime::Handle::current().metrics().num_workers();
    if cfg!(tokio_unstable) {
        tracing::info!(target: "long_poll", threshold_ms, workers, "long-poll detector enabled");
    } else {
        tracing::warn!(
            target: "long_poll",
            threshold_ms,
            workers,
            "long-poll-threshold-ms is set, but this build lacks --cfg tokio_unstable; the detector is off"
        );
    }
}

#[cfg(tokio_unstable)]
mod hooks {
    use std::{
        cell::Cell,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, Instant},
    };

    /// Task polls started on any worker. Its change over one long poll shows whether other workers kept running.
    static POLLS: AtomicU64 = AtomicU64::new(0);

    /// Polls at or above the threshold, for tests.
    pub(super) static LONG_POLLS: AtomicU64 = AtomicU64::new(0);

    thread_local! {
        /// Start time and `POLLS` value of the poll running on this worker thread.
        static POLL_START: Cell<Option<(Instant, u64)>> = const { Cell::new(None) };
    }

    pub(super) fn install(builder: &mut tokio::runtime::Builder, threshold: Duration) {
        builder
            .on_before_task_poll(|_| {
                let polls = POLLS.fetch_add(1, Ordering::Relaxed);
                POLL_START.set(Some((Instant::now(), polls)));
            })
            .on_after_task_poll(move |meta| {
                let Some((start, polls)) = POLL_START.take() else {
                    return;
                };
                let poll = start.elapsed();
                if poll < threshold {
                    return;
                }
                LONG_POLLS.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    target: "long_poll",
                    task_id = %meta.id(),
                    spawned_at = %meta.spawned_at(),
                    poll_us = poll.as_micros() as i64,
                    other_polls = POLLS.load(Ordering::Relaxed).saturating_sub(polls + 1) as i64,
                    "task poll ran long"
                );
            });
    }
}

#[cfg(all(test, tokio_unstable))]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// A task that blocks its worker for 200 ms is reported once; short tasks are not.
    #[test]
    fn reports_a_blocking_poll() {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2).enable_all();
        install(&mut builder, Duration::from_millis(100));
        let runtime = builder.build().unwrap();
        let before = hooks::LONG_POLLS.load(Ordering::Relaxed);
        runtime.block_on(async {
            tokio::spawn(async { tokio::task::yield_now().await })
                .await
                .unwrap();
            tokio::spawn(async { std::thread::sleep(Duration::from_millis(200)) })
                .await
                .unwrap();
        });
        assert_eq!(hooks::LONG_POLLS.load(Ordering::Relaxed) - before, 1);
    }
}
