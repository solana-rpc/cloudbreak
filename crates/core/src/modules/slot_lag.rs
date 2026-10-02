// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Measures when the API learns a slot's commitment from the gRPC feed and from the Postgres
//! slot notifications. A slot both sides announced observes `cloudbreak_slot_lag_ms`. A slot
//! only one side announced within [`UNMATCHED_AFTER`] counts in
//! `cloudbreak_slot_lag_unmatched_total`. Metrics only: nothing reads it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::metrics::{SLOT_LAG_LAST_SLOT, SLOT_LAG_MS, SLOT_LAG_SLOTS, SLOT_LAG_UNMATCHED_TOTAL};

/// How long a slot waits for the other side before it counts as unmatched.
pub const UNMATCHED_AFTER: Duration = Duration::from_secs(60);
/// Bound on pending slots per commitment, so a dead side cannot grow memory.
const MAX_PENDING: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotCommitment {
    Confirmed,
    Finalized,
}

impl SlotCommitment {
    fn label(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotSource {
    Grpc,
    Postgres,
}

impl SlotSource {
    fn label(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::Postgres => "postgres",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Cheap-clone handle shared by the gRPC feed and the slot syncronizer.
#[derive(Clone, Default)]
pub struct SlotLag(Arc<Mutex<[Pending; 2]>>);

#[derive(Default)]
struct Pending {
    slots: BTreeMap<u64, Seen>,
    /// Highest slot per source, indexed by [`SlotSource::index`].
    last: [Option<u64>; 2],
}

struct Seen {
    at: [Option<Instant>; 2],
    first_seen: Instant,
}

/// A matched slot: the lag in milliseconds and the side that announced it first.
#[derive(Debug, PartialEq)]
struct Matched {
    lag_ms: f64,
    first: SlotSource,
}

impl SlotLag {
    /// Records that `source` announced `slot` at `commitment` at time `at`.
    pub fn record(&self, source: SlotSource, commitment: SlotCommitment, slot: u64, at: Instant) {
        let mut pending = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let pending = &mut pending[commitment.index()];
        let commitment_label = commitment.label();

        let last = pending.last[source.index()].get_or_insert(slot);
        *last = (*last).max(slot);
        SLOT_LAG_LAST_SLOT
            .with_label_values(&[commitment_label, source.label()])
            .set(*last as i64);
        if let [Some(grpc), Some(postgres)] = pending.last {
            SLOT_LAG_SLOTS
                .with_label_values(&[commitment_label])
                .set(grpc as i64 - postgres as i64);
        }

        if let Some(matched) = pending.on_announce(source, slot, at) {
            SLOT_LAG_MS
                .with_label_values(&[commitment_label, matched.first.label()])
                .observe(matched.lag_ms);
        }
        for seen_by in pending.expire(at) {
            SLOT_LAG_UNMATCHED_TOTAL
                .with_label_values(&[commitment_label, seen_by.label()])
                .inc();
        }
    }
}

impl Pending {
    /// Stores the first announcement per side. Returns the match once both sides announced.
    fn on_announce(&mut self, source: SlotSource, slot: u64, at: Instant) -> Option<Matched> {
        let seen = self.slots.entry(slot).or_insert(Seen {
            at: [None; 2],
            first_seen: at,
        });
        seen.at[source.index()].get_or_insert(at);
        let [Some(grpc), Some(postgres)] = seen.at else {
            return None;
        };
        self.slots.remove(&slot);
        Some(if postgres >= grpc {
            Matched {
                lag_ms: (postgres - grpc).as_secs_f64() * 1_000.0,
                first: SlotSource::Grpc,
            }
        } else {
            Matched {
                lag_ms: (grpc - postgres).as_secs_f64() * 1_000.0,
                first: SlotSource::Postgres,
            }
        })
    }

    /// Drops slots older than [`UNMATCHED_AFTER`], and the lowest slots past [`MAX_PENDING`].
    /// Returns the side that saw each dropped slot.
    fn expire(&mut self, now: Instant) -> Vec<SlotSource> {
        let mut dropped = Vec::new();
        self.slots.retain(|_, seen| {
            if now.saturating_duration_since(seen.first_seen) < UNMATCHED_AFTER {
                return true;
            }
            dropped.push(seen.seen_by());
            false
        });
        while self.slots.len() > MAX_PENDING {
            if let Some((_, seen)) = self.slots.pop_first() {
                dropped.push(seen.seen_by());
            }
        }
        dropped
    }
}

impl Seen {
    fn seen_by(&self) -> SlotSource {
        if self.at[SlotSource::Grpc.index()].is_some() {
            SlotSource::Grpc
        } else {
            SlotSource::Postgres
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_reports_the_side_that_was_first() {
        let start = Instant::now();
        let mut pending = Pending::default();
        assert_eq!(pending.on_announce(SlotSource::Grpc, 10, start), None);
        let matched = pending
            .on_announce(SlotSource::Postgres, 10, start + Duration::from_millis(40))
            .unwrap();
        assert_eq!(matched.first, SlotSource::Grpc);
        assert!((matched.lag_ms - 40.0).abs() < 1e-6);
        assert!(pending.slots.is_empty());

        pending.on_announce(SlotSource::Postgres, 11, start);
        let matched = pending
            .on_announce(SlotSource::Grpc, 11, start + Duration::from_millis(5))
            .unwrap();
        assert_eq!(matched.first, SlotSource::Postgres);
        assert!((matched.lag_ms - 5.0).abs() < 1e-6);
    }

    #[test]
    fn a_repeated_announcement_keeps_the_first_time() {
        let start = Instant::now();
        let mut pending = Pending::default();
        pending.on_announce(SlotSource::Grpc, 10, start);
        pending.on_announce(SlotSource::Grpc, 10, start + Duration::from_millis(30));
        let matched = pending
            .on_announce(SlotSource::Postgres, 10, start + Duration::from_millis(50))
            .unwrap();
        assert!((matched.lag_ms - 50.0).abs() < 1e-6);
    }

    #[test]
    fn unmatched_slots_expire_by_the_side_that_saw_them() {
        let start = Instant::now();
        let mut pending = Pending::default();
        pending.on_announce(SlotSource::Grpc, 10, start);
        pending.on_announce(SlotSource::Postgres, 11, start);
        assert!(pending.expire(start + Duration::from_secs(1)).is_empty());

        let dropped = pending.expire(start + UNMATCHED_AFTER);
        assert_eq!(dropped, vec![SlotSource::Grpc, SlotSource::Postgres]);
        assert!(pending.slots.is_empty());
    }

    #[test]
    fn pending_slots_are_bounded() {
        let start = Instant::now();
        let mut pending = Pending::default();
        for slot in 0..(MAX_PENDING as u64 + 3) {
            pending.on_announce(SlotSource::Grpc, slot, start);
        }
        assert_eq!(pending.expire(start).len(), 3);
        assert_eq!(pending.slots.len(), MAX_PENDING);
        assert_eq!(
            pending.slots.first_key_value().map(|(slot, _)| *slot),
            Some(3)
        );
    }

    #[test]
    fn record_tracks_the_highest_slot_per_source() {
        let lag = SlotLag::default();
        let now = Instant::now();
        lag.record(SlotSource::Grpc, SlotCommitment::Confirmed, 12, now);
        lag.record(SlotSource::Grpc, SlotCommitment::Confirmed, 11, now);
        lag.record(SlotSource::Postgres, SlotCommitment::Confirmed, 10, now);
        let pending = lag.0.lock().unwrap();
        assert_eq!(
            pending[SlotCommitment::Confirmed.index()].last,
            [Some(12), Some(10)]
        );
        assert_eq!(
            pending[SlotCommitment::Finalized.index()].last,
            [None, None]
        );
    }
}
