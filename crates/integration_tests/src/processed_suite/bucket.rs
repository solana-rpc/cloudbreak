// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Token bucket that paces every request to one source, shared by all fronts.
//!
//! It is a virtual scheduling bucket: each caller reserves the next free send time and sleeps
//! until it. A reservation never lands earlier than one burst before now, so an idle bucket
//! allows at most `burst` requests at once and the long-run rate never exceeds `rate`.

use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// Seconds of rate an idle bucket may spend at once.
const BURST_SECS: f64 = 0.2;

pub struct TokenBucket {
    interval: Duration,
    burst: Duration,
    next: Mutex<Option<Instant>>,
}

impl TokenBucket {
    /// `rate` is requests per second, at least 0.1.
    pub fn new(rate: f64) -> Self {
        let rate = rate.max(0.1);
        let interval = Duration::from_secs_f64(1.0 / rate);
        let burst = Duration::from_secs_f64((rate * BURST_SECS).max(1.0) / rate);
        Self {
            interval,
            burst,
            next: Mutex::new(None),
        }
    }

    /// Send time reserved for a caller arriving at `now`.
    pub fn reserve(&self, now: Instant) -> Instant {
        let mut next = self.next.lock().expect("bucket lock");
        let earliest = now.checked_sub(self.burst).unwrap_or(now);
        let at = next.map_or(earliest, |n| n.max(earliest));
        *next = Some(at + self.interval);
        at.max(now)
    }

    /// Waits for one token.
    pub async fn take(&self) {
        let at = self.reserve(Instant::now());
        tokio::time::sleep_until(at).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_rate_and_burst() {
        let bucket = TokenBucket::new(10.0);
        let start = Instant::now() + Duration::from_secs(10);
        let at: Vec<Duration> = (0..12).map(|_| bucket.reserve(start) - start).collect();
        // Two tokens of burst, then one every 100 ms.
        assert_eq!(at[0], Duration::ZERO);
        assert_eq!(at[1], Duration::ZERO);
        assert_eq!(at[2], Duration::from_millis(0));
        assert!(at[3] > Duration::ZERO);
        let tail = at[11] - at[3];
        assert_eq!(tail, Duration::from_millis(800));

        // After a long idle gap the burst is capped again.
        let later = start + Duration::from_secs(60);
        let idle: Vec<Duration> = (0..4).map(|_| bucket.reserve(later) - later).collect();
        assert_eq!(idle[..3], [Duration::ZERO; 3]);
        assert_eq!(idle[3], Duration::from_millis(100));
    }
}
