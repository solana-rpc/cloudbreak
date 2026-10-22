// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Runtime lag probe, off by default (`[metrics].runtime-lag-probe-enabled`). A few tasks sleep 10 ms in a
//! loop and record how late they wake up; a late wake-up means a Tokio worker was too busy to poll ready tasks.

use std::time::{Duration, Instant};

use lazy_static::lazy_static;
use prometheus::{Histogram, HistogramOpts, Registry};

/// Sleep between two wake-ups of one probe task.
const INTERVAL: Duration = Duration::from_millis(10);

/// Probe tasks. Several tasks spread over more worker threads than one would.
const PROBES: usize = 4;

/// Lag at or above this is also logged (`runtime_lag` target), to line it up with traces by time.
const LOG_THRESHOLD: Duration = Duration::from_millis(50);

lazy_static! {
    /// Extra delay after a probe's 10 ms sleep before the task ran again, in microseconds.
    /// Includes the timer's 1 ms granularity, so values under ~1,000 µs are normal.
    pub static ref CLOUDBREAK_API_RUNTIME_LAG_US: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "cloudbreak_api_runtime_lag_us",
            "Extra delay in microseconds after a 10 ms sleep before the runtime lag probe task ran again."
        )
        .buckets(vec![
            250.0, 500.0, 1_000.0, 2_000.0, 5_000.0, 10_000.0, 25_000.0, 50_000.0, 100_000.0,
            250_000.0, 500_000.0, 1_000_000.0, 2_000_000.0, 5_000_000.0, 10_000_000.0,
        ])
    )
    .unwrap();
}

/// Registers the histogram and starts the probe tasks when `enabled`; otherwise does nothing.
/// Call once from `setup_metrics`, inside the Tokio runtime.
pub fn register(registry: &Registry, enabled: bool) {
    if !enabled {
        return;
    }
    registry
        .register(Box::new(CLOUDBREAK_API_RUNTIME_LAG_US.clone()))
        .expect("runtime lag histogram can't be registered");
    for probe in 0..PROBES {
        tokio::spawn(probe_loop(probe));
    }
}

async fn probe_loop(probe: usize) {
    loop {
        let start = Instant::now();
        tokio::time::sleep(INTERVAL).await;
        let lag = start.elapsed().saturating_sub(INTERVAL);
        CLOUDBREAK_API_RUNTIME_LAG_US.observe(lag.as_micros() as f64);
        if lag >= LOG_THRESHOLD {
            tracing::warn!(
                target: "runtime_lag",
                probe,
                lag_us = lag.as_micros() as u64,
                "runtime lag probe woke up late"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A task that blocks its worker thread delays a probe on the same single-threaded runtime.
    #[tokio::test(flavor = "current_thread")]
    async fn blocked_worker_shows_up_as_lag() {
        let probe = tokio::spawn(async {
            let start = Instant::now();
            tokio::time::sleep(INTERVAL).await;
            start.elapsed().saturating_sub(INTERVAL)
        });
        tokio::task::yield_now().await;
        // Block the only worker for 200 ms without yielding, like a long CPU-bound poll.
        std::thread::sleep(Duration::from_millis(200));
        let lag = probe.await.unwrap();
        assert!(lag >= Duration::from_millis(150), "lag {lag:?}");
    }
}
