// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Deferred finalize-slot cleanup.
//!
//! The finalize worker writes the finalized marker, hands the slot's cleanup keys to this queue,
//! and returns. A background drainer runs the DELETEs, so no slot's marker waits on the previous
//! slot's deletes.
//!
//! | File | Role |
//! |---|---|
//! | `mod.rs` | The handle. |
//! | `pending.rs` | The pending map, the merge rule, the keys a slot contributes. |
//! | `drain.rs` | The spawned drainer. |
//! | `persist.rs` | The statements and the two-table ordering rule. |
//!
//! # The cutoff rule
//!
//! Each key carries one exclusive cutoff, merged by max, and the drain deletes its rows below
//! that slot. An open key at finalized slot `s` takes `s`, so the row written at `s` survives. A
//! closed or owner-moved key takes `s + 1`, so its mask at `s` goes with the rows it shadows.
//! Every cutoff comes from a slot at which the key was written or masked, which is why max is
//! safe in any arrival order, including a repaired slot finalizing below the frontier.
//!
//! # Always on
//!
//! There is no enable flag. A node without cleanup fills its disk and slows every latest-version
//! read. `cleanup-interval-slots` is the only knob. This deviates from
//! `docs/feature-guideline.md` rules 2 and 5 deliberately.
//!
//! # Where this lives
//!
//! Rule 1 puts a feature in `crates/core` so the API can share a read path. This one has no
//! reader and no API surface, and its dependencies are all in `crates/index`. Deliberate.
//!
//! # Restart
//!
//! The queue is in memory and a restart drops it. Nothing incorrect becomes visible: every lost
//! key describes superseded rows or a mask, which the read paths already hide, and the rows clear
//! on the key's next touch under a higher cutoff.

pub mod drain;
pub mod pending;
pub mod persist;

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

pub use drain::spawn_cleanup_drainer;
pub use pending::{CleanupKey, keys_for_slot};

use crate::metrics;
use pending::{Pending, Taken};

struct Shared {
    pending: Mutex<Pending>,
    /// Wakes the drainer. The finalize worker never blocks on it.
    work_available: Notify,
}

/// Cheap-clone handle over the pending map, held by the finalize worker and the drainer.
#[derive(Clone)]
pub struct CleanupHandle {
    shared: Arc<Shared>,
}

impl CleanupHandle {
    /// `interval_slots` is clamped to at least 1, so a zero in config drains every slot.
    pub fn new(interval_slots: u64) -> Self {
        Self {
            shared: Arc::new(Shared {
                pending: Mutex::new(Pending::new(interval_slots.max(1))),
                work_available: Notify::new(),
            }),
        }
    }

    /// Hands one finalized slot's keys to the queue. Never blocks.
    pub fn enqueue(&self, slot: u64, items: &[(CleanupKey, u64)]) {
        let wake = {
            let mut pending = self.lock();
            pending.enqueue(slot, items);
            pending.should_drain()
        };
        if wake {
            self.shared.work_available.notify_one();
        }
        self.publish_lag();
    }

    pub async fn wait_for_work(&self) {
        self.shared.work_available.notified().await;
    }

    pub fn take_all(&self) -> Taken {
        self.lock().take_all()
    }

    pub fn finish(&self) {
        self.lock().finish();
    }

    /// Returns a failed drain's keys to the queue.
    pub fn reinsert(&self, taken: Taken) {
        self.lock().reinsert(taken);
    }

    pub fn note_drain(&self) {
        self.lock().note_drain();
    }

    pub fn is_quiescent(&self) -> bool {
        self.lock().is_quiescent()
    }

    pub fn publish_lag(&self) {
        let lag = self.lock().lag_slots();
        metrics::CLEANUP_LAG_SLOTS.set(lag as i64);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Pending> {
        self.shared.pending.lock().expect("Failed to lock cleanup")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pending::tests::routed;

    #[test]
    fn a_zero_interval_drains_every_slot() {
        let handle = CleanupHandle::new(0);
        handle.enqueue(100, &[(routed(1, 1), 100)]);
        assert!(!handle.is_quiescent());
    }
}
