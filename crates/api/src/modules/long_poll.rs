// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Long-poll detector, off by default (`[metrics].long-poll-threshold-ms`); needs a `--cfg tokio_unstable` build.
//! Records every task poll longer than the threshold with the task's spawn location, to name what blocks a worker.

use std::time::Duration;

use lazy_static::lazy_static;
use prometheus::{HistogramOpts, HistogramVec, Registry};

lazy_static! {
    /// Task polls at or above the threshold, in microseconds, by the task's spawn location.
    pub static ref CLOUDBREAK_API_LONG_POLL_US: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "cloudbreak_api_long_poll_us",
            "Tokio task polls at or above the long-poll threshold, in microseconds, labelled by spawn location"
        )
        .buckets(vec![
            5_000.0, 10_000.0, 20_000.0, 50_000.0, 100_000.0, 250_000.0, 500_000.0, 1_000_000.0,
            5_000_000.0,
        ]),
        &["spawned_at"],
    )
    .unwrap();
}

/// Installs the poll hooks on `builder`. Does nothing in a build without `tokio_unstable`.
pub fn install(builder: &mut tokio::runtime::Builder, threshold: Duration) {
    #[cfg(tokio_unstable)]
    hooks::install(builder, threshold);
    #[cfg(not(tokio_unstable))]
    let _ = (builder, threshold);
}

/// Registers the histogram and logs whether the detector runs. Call once from `setup_metrics`.
pub fn register(registry: &Registry, threshold_ms: Option<u64>) {
    let Some(threshold_ms) = threshold_ms else {
        return;
    };
    registry
        .register(Box::new(CLOUDBREAK_API_LONG_POLL_US.clone()))
        .expect("long poll histogram can't be registered");
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

    use super::CLOUDBREAK_API_LONG_POLL_US;

    /// Task polls started on any worker. Its change over one long poll shows whether other workers kept running.
    static POLLS: AtomicU64 = AtomicU64::new(0);

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
                let spawned_at = meta.spawned_at();
                let location = format!("{}:{}", spawned_at.file(), spawned_at.line());
                CLOUDBREAK_API_LONG_POLL_US
                    .with_label_values(&[location.as_str()])
                    .observe(poll.as_micros() as f64);
                tracing::warn!(
                    target: "long_poll",
                    task_id = %meta.id(),
                    spawned_at = %location,
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
    use prometheus::core::Collector;

    /// A task that blocks its worker for 200 ms is recorded once, under its spawn location here.
    #[test]
    fn records_a_blocking_poll() {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2).enable_all();
        install(&mut builder, Duration::from_millis(100));
        let runtime = builder.build().unwrap();
        let before = long_polls_spawned_in_this_file();
        runtime.block_on(async {
            tokio::spawn(async { tokio::task::yield_now().await })
                .await
                .unwrap();
            tokio::spawn(async { std::thread::sleep(Duration::from_millis(200)) })
                .await
                .unwrap();
        });
        assert_eq!(long_polls_spawned_in_this_file() - before, 1);
    }

    fn long_polls_spawned_in_this_file() -> u64 {
        CLOUDBREAK_API_LONG_POLL_US
            .collect()
            .iter()
            .flat_map(|family| family.get_metric())
            .filter(|metric| metric.get_label().iter().any(|l| l.get_value().starts_with(file!())))
            .map(|metric| metric.get_histogram().get_sample_count())
            .sum()
    }
}
