// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Retention, the caps and the dropper thread.
//!
//! [`BlockStore::prune`] runs after every event. It drops blocks and poison
//! entries at or below the anchor, blocks proven off the anchor, and linked
//! blocks off the root line. Blocks that cannot link yet stay, so the view warms
//! up after start and re-links after a gap. Then the caps evict the lowest
//! slots, first while the stored span exceeds `max-overlay-slots`, then while
//! live bytes minus pending drop bytes exceed `max-memory-mb`.
//!
//! Every removed block becomes an [`Evicted`] that the caller sends to the
//! dropper. Its bytes count as pending only when the store and at most the
//! published view hold it, because only then does releasing it free memory. A
//! block pinned by a request adds nothing, so pinned excess can evict the whole
//! store.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

use super::ingest::SlotBlock;
use super::store::{BlockStore, ChainMemo, Link, Links, block_id};
use crate::metrics;

/// A block removed from the store. Dropping it releases the block and clears its pending bytes.
pub(crate) struct Evicted {
    block: Option<Arc<SlotBlock>>,
    pending_bytes: usize,
    pending_drop_bytes: Arc<AtomicUsize>,
}

impl Drop for Evicted {
    fn drop(&mut self) {
        drop(self.block.take());
        self.pending_drop_bytes
            .fetch_sub(self.pending_bytes, Ordering::Relaxed);
    }
}

impl BlockStore {
    /// Applies retention and the caps. Returns links valid for the blocks left.
    pub(crate) fn prune(&mut self) -> Links {
        let mut links = None;
        if let Some(a) = self.anchor_slot() {
            let at_or_below: Vec<u64> = self.by_slot.range(..=a).map(|(s, _)| *s).collect();
            for slot in at_or_below {
                self.evict_slot(slot, "anchor");
            }
            self.dead.retain(|slot| *slot > a);
            self.created.retain(|slot| *slot > a);

            // Removing off-root and fork blocks leaves the links of the rest unchanged.
            let current = self.links();
            let line = self.root_line(&current);
            let mut memo = ChainMemo::new();
            let removed: Vec<(u64, usize, &str)> = self
                .blocks()
                .filter_map(|b| {
                    let reason = match current.get(&block_id(b)) {
                        Some(Link::OffRoot) => "off_root",
                        Some(Link::Linked)
                            if line.as_ref().is_some_and(|line| {
                                !self.chain_status(b, Some(line), a, &mut memo).1
                            }) =>
                        {
                            "fork"
                        }
                        _ => return None,
                    };
                    Some((b.slot, block_id(b), reason))
                })
                .collect();
            for (slot, id, reason) in removed {
                self.evict_id(slot, id, reason);
            }
            links = Some(current);
        }

        let capped = self.enforce_caps();
        match links {
            Some(links) if !capped => links,
            _ => self.links(),
        }
    }

    /// Evicts the lowest slots over the span and memory caps. True when anything went.
    fn enforce_caps(&mut self) -> bool {
        let mut evicted_any = false;
        while let (Some(lowest), Some(highest)) = (
            self.by_slot.keys().next().copied(),
            self.by_slot.keys().next_back().copied(),
        ) {
            if highest - lowest < self.limits.max_overlay_slots {
                break;
            }
            self.evict_slot(lowest, "overlay_slots");
            evicted_any = true;
        }

        while self
            .live_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(self.pending_drop_bytes.load(Ordering::Relaxed))
            > self.limits.max_memory_bytes
        {
            let Some(lowest) = self.by_slot.keys().next().copied() else {
                break;
            };
            self.evict_slot(lowest, "memory_cap");
            evicted_any = true;
        }
        evicted_any
    }

    fn evict_slot(&mut self, slot: u64, reason: &str) {
        for block in self.by_slot.remove(&slot).unwrap_or_default() {
            self.push_evicted(block, Some(reason));
        }
    }

    fn evict_id(&mut self, slot: u64, id: usize, reason: &str) {
        let Some(blocks) = self.by_slot.get_mut(&slot) else {
            return;
        };
        let Some(position) = blocks.iter().position(|b| block_id(b) == id) else {
            return;
        };
        let block = blocks.remove(position);
        if blocks.is_empty() {
            self.by_slot.remove(&slot);
        }
        self.push_evicted(block, Some(reason));
    }

    /// Queues a block removed from the store. `reason` labels the eviction metric.
    pub(super) fn push_evicted(&mut self, block: Arc<SlotBlock>, reason: Option<&str>) {
        if let Some(reason) = reason {
            metrics::PROCESSED_EVICTIONS_TOTAL
                .with_label_values(&[reason])
                .inc();
        }
        let pending_bytes = if self.is_freeable(&block) {
            block.bytes
        } else {
            0
        };
        self.pending_drop_bytes
            .fetch_add(pending_bytes, Ordering::Relaxed);
        self.evicted.push(Evicted {
            block: Some(block),
            pending_bytes,
            pending_drop_bytes: self.pending_drop_bytes.clone(),
        });
    }

    /// True when releasing `block` frees it: only this store and at most the
    /// published view hold it.
    fn is_freeable(&self, block: &Arc<SlotBlock>) -> bool {
        let mut holders = 1;
        if let Some(view) = &self.last_view
            && view.chain.iter().any(|b| Arc::ptr_eq(b, block))
        {
            // The published copy and this store hold the view. More means a request does.
            if Arc::strong_count(view) > 2 {
                return false;
            }
            holders += 1;
        }
        Arc::strong_count(block) == holders
    }
}

/// Starts `processed-dropper`, which frees evicted blocks off the writer.
/// Returns `None` when the thread cannot start, and blocks then drop in place.
pub(super) fn spawn_dropper() -> Option<mpsc::Sender<Evicted>> {
    let (tx, rx) = mpsc::channel::<Evicted>();
    match std::thread::Builder::new()
        .name("processed-dropper".to_string())
        .spawn(move || {
            for evicted in rx {
                drop(evicted);
            }
        }) {
        Ok(_) => Some(tx),
        Err(e) => {
            tracing::error!("Failed to start processed-dropper thread: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::DegradeReason;
    use super::super::ingest::ENTRY_OVERHEAD_BYTES;
    use super::super::ingest::tests::{account_info, update};
    use super::super::store::StoreLimits;
    use super::super::store::tests::{TEST_LIMITS, TestChain, anchor_at};
    use super::*;
    use solana_pubkey::Pubkey;

    const PER_BLOCK: usize = ENTRY_OVERHEAD_BYTES + 8;

    #[test]
    fn anchor_advance_drops_blocks_and_poison_at_or_below() {
        let mut chain = TestChain::new();
        chain.linear(101, 105);
        chain.store.on_dead(102);
        chain.store.set_anchor(anchor_at(100));
        chain.store.prune();
        assert_eq!(chain.store.block_count(), 5);
        chain.store.set_anchor(anchor_at(103));
        chain.store.prune();
        assert_eq!(chain.store.block_count(), 2);
        assert!(chain.store.dead.is_empty());
        assert!(chain.store.created.iter().all(|s| *s > 103));
        assert_eq!(chain.store.take_evicted().len(), 3);
    }

    #[test]
    fn overlay_slot_cap_evicts_lowest() {
        let mut chain = TestChain::with_limits(StoreLimits {
            max_overlay_slots: 3,
            max_servable_depth: 3,
            ..TEST_LIMITS
        });
        chain.linear(101, 106);
        chain.store.prune();
        assert_eq!(
            chain.store.by_slot.keys().copied().collect::<Vec<_>>(),
            vec![104, 105, 106]
        );
        chain.store.set_anchor(anchor_at(102));
        assert_eq!(chain.head(), Err(DegradeReason::Unlinked));
        assert_eq!(chain.store.block_count(), 3);
        chain.store.set_anchor(anchor_at(103));
        assert_eq!(chain.head(), Ok(106));
    }

    #[test]
    fn memory_cap_counts_pinned_blocks_and_rejects_oversized() {
        let mut chain = TestChain::with_limits(StoreLimits {
            max_memory_bytes: 4_000,
            ..TEST_LIMITS
        });
        chain.store.on_created_bank(101, Some(100));
        let oversized = account_info(
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            vec![0; 5_000],
        );
        chain.raw(update(101, "h101", 100, "h100", vec![oversized]));
        assert_eq!(chain.store.block_count(), 0);
        assert!(chain.pending() > 0);
        drop(chain.store.take_evicted());
        assert_eq!(chain.live(), 0);
        assert_eq!(chain.pending(), 0);

        for slot in 101..=103 {
            chain.block_with(slot, slot - 1, vec![(Pubkey::new_unique(), 1)]);
        }
        chain.store.set_anchor(anchor_at(100));
        let pinned = chain.event().unwrap();
        let held = chain.live();
        assert_eq!(held, 3 * PER_BLOCK);

        // Pinned blocks free nothing when evicted, so the whole store goes.
        chain.live_bytes.fetch_add(4_000, Ordering::Relaxed);
        chain.store.prune();
        assert_eq!(chain.store.block_count(), 0);
        assert_eq!(chain.pending(), 0);
        assert_eq!(chain.store.take_evicted().len(), 3);
        assert_eq!(chain.live(), held + 4_000);
        chain.live_bytes.fetch_sub(4_000, Ordering::Relaxed);
        assert_eq!(pinned.slot, 103);
        drop(pinned);
        assert_eq!(chain.live(), 0);
        assert_eq!(
            chain.store.select_view().unwrap_err(),
            DegradeReason::HeadNotAhead
        );
    }

    #[test]
    fn memory_cap_does_not_count_pending_bytes_twice() {
        let mut chain = TestChain::with_limits(StoreLimits {
            max_memory_bytes: 2 * PER_BLOCK + PER_BLOCK / 2,
            ..TEST_LIMITS
        });
        for slot in 101..=103 {
            chain.block_with(slot, slot - 1, vec![(Pubkey::new_unique(), 1)]);
        }
        chain.store.prune();
        assert_eq!(chain.store.block_count(), 2);
        assert_eq!(chain.pending(), PER_BLOCK);

        chain.store.prune();
        assert_eq!(chain.store.block_count(), 2);
        drop(chain.store.take_evicted());
        assert_eq!(chain.pending(), 0);
        assert_eq!(chain.live(), 2 * PER_BLOCK);
    }

    #[test]
    fn blocks_held_only_by_the_published_view_are_freeable() {
        let mut chain = TestChain::new();
        for slot in 101..=103 {
            chain.block_with(slot, slot - 1, vec![(Pubkey::new_unique(), 1)]);
        }
        chain.store.set_anchor(anchor_at(100));
        let links = chain.store.prune();
        let published = chain.store.select_view_with(&links).map(Arc::new);
        chain.store.observe_publish(&published);

        chain.store.set_anchor(anchor_at(101));
        chain.store.prune();
        assert_eq!(chain.pending(), PER_BLOCK);

        let request = published.as_ref().unwrap().clone();
        chain.store.set_anchor(anchor_at(102));
        chain.store.prune();
        assert_eq!(chain.pending(), PER_BLOCK);

        drop(chain.store.take_evicted());
        assert_eq!(chain.pending(), 0);
        // The view still pins 101 and 102.
        assert_eq!(chain.live(), 3 * PER_BLOCK);
        drop(request);
        drop(published);
        chain.store.last_view = None;
        assert_eq!(chain.live(), PER_BLOCK);
    }

    #[test]
    fn dropper_frees_blocks_and_pending_bytes() {
        let tx = spawn_dropper().unwrap();
        let mut chain = TestChain::new();
        chain.block_with(101, 100, vec![(Pubkey::new_unique(), 1)]);
        assert!(chain.live() > 0);
        chain.store.set_anchor(anchor_at(101));
        chain.store.prune();
        assert_eq!(chain.pending(), PER_BLOCK);
        for evicted in chain.store.take_evicted() {
            tx.send(evicted).unwrap();
        }
        drop(tx);
        for _ in 0..200 {
            if chain.live() == 0 && chain.pending() == 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(chain.live(), 0);
        assert_eq!(chain.pending(), 0);
    }
}
