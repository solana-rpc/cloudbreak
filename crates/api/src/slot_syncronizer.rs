// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::db_query;
use sea_orm::DatabaseConnection;
use solana_commitment_config::CommitmentLevel;
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};
use cloudbreak_core::ApiConfig;
use cloudbreak_core::modules::processed::Anchor;

/// Data structure to store the confirmed and finalized slots from the slot data
///  syncronizer background task
#[derive(Clone, Default, Debug)]
pub struct SlotSyncronizerData {
    pub confirmed_slot: SlotData,
    pub finalized_slot: SlotData,
    /// Denormalised service health (mirrors the `slots.health` column). Defaults
    /// to `false` (unhealthy) until the first successful sync populates it.
    pub healthy: bool,
}

impl SlotSyncronizerData {
    pub fn is_healthy(&self) -> bool {
        self.healthy
    }

    pub fn get_slot_for_commitment(&self, commitment: CommitmentLevel) -> u64 {
        match commitment {
            CommitmentLevel::Finalized => self.finalized_slot.slot,
            CommitmentLevel::Confirmed => self.confirmed_slot.slot,
            CommitmentLevel::Processed => self.confirmed_slot.slot,
        }
    }

    pub fn get_block_time_for_commitment(&self, commitment: CommitmentLevel) -> i64 {
        match commitment {
            CommitmentLevel::Finalized => self.finalized_slot.block_time,
            CommitmentLevel::Confirmed => self.confirmed_slot.block_time,
            CommitmentLevel::Processed => self.confirmed_slot.block_time,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct SlotData {
    pub slot: u64,
    pub block_time: i64,
}

/// Floor for the blockhash read timeout when the poll interval is very short.
const MIN_BLOCKHASH_READ_TIMEOUT: Duration = Duration::from_millis(100);

/// With `[processed-accounts]` enabled, every successful poll also publishes the
/// processed [`Anchor`] on `anchor_tx`.
pub fn start_slot_syncronizer(
    db: DatabaseConnection,
    config: &ApiConfig,
    anchor_tx: watch::Sender<Option<Anchor>>,
) -> Option<(JoinHandle<()>, Arc<RwLock<SlotSyncronizerData>>)> {
    if !config.slot_syncronizer.enabled {
        return None;
    }

    let slot_syncronizer_data = Arc::new(RwLock::new(SlotSyncronizerData::default()));
    let delay = Duration::from_millis(config.slot_syncronizer.interval_ms);
    let anchor_tx = config.processed_accounts_enabled().then_some(anchor_tx);

    let slot_data_clone = slot_syncronizer_data.clone();
    let join_handle = tokio::spawn(async move {
        let mut last_time_sync = Instant::now();
        loop {
            tokio::time::sleep(delay).await;
            tracing::debug!(target: "slot_syncronizer", "Slot syncronizer: last time sync: {:?}", last_time_sync.elapsed().as_secs_f32());
            let query_start_time = Instant::now();
            let mut polled = None;

            if let Some(db_slot_data) = db_query::get_slot_data(&db).await {
                let mut cached_slot_data =
                    slot_data_clone.write().expect("Failed to lock slot data");

                if db_slot_data.confirmed_slot.slot - cached_slot_data.confirmed_slot.slot > 1
                    || db_slot_data.finalized_slot.slot - cached_slot_data.finalized_slot.slot > 1
                {
                    tracing::warn!(
                      target: "slot_syncronizer",
                        "Slot syncronizer slot mismatch: finalized (cached: {} - db: {}) - confirmed (cached: {} - db: {}) (last sync {:?} secs ago)",
                        cached_slot_data.finalized_slot.slot,
                        db_slot_data.finalized_slot.slot,
                        cached_slot_data.confirmed_slot.slot,
                        db_slot_data.confirmed_slot.slot,
                        last_time_sync.elapsed().as_secs_f32()
                    );
                }

                *cached_slot_data = db_slot_data;

                tracing::debug!(
                  target: "slot_syncronizer",
                    "Slot syncronizer: confirmed slot: {}, finalized slot: {} - query took {:?}",
                    cached_slot_data.confirmed_slot.slot,
                    cached_slot_data.finalized_slot.slot,
                    query_start_time.elapsed().as_secs_f32()
                );

                last_time_sync = Instant::now();
                polled = Some(cached_slot_data.clone());
            }

            // Published after the block so the slot cache lock is released first.
            if let (Some(anchor_tx), Some(slots)) = (&anchor_tx, polled) {
                let blockhash = read_blockhash(&db, slots.confirmed_slot.slot, delay);
                publish_anchor(anchor_tx, &slots, blockhash).await;
            }
        }
    });

    Some((join_handle, slot_syncronizer_data))
}

/// Reads the blockhash of `slot`, bounded by the poll interval so a slow read
/// delays the next poll by at most one interval.
async fn read_blockhash(db: &DatabaseConnection, slot: u64, delay: Duration) -> Option<String> {
    let read_timeout = delay.max(MIN_BLOCKHASH_READ_TIMEOUT);
    match tokio::time::timeout(read_timeout, db_query::get_blockhash_at_slot(db, slot)).await {
        Ok(Ok(blockhash)) => blockhash,
        Ok(Err(e)) => {
            tracing::warn!(target: "slot_syncronizer", "Confirmed blockhash read failed for slot {slot}: {e}");
            None
        }
        Err(_elapsed) => {
            tracing::warn!(target: "slot_syncronizer", "Confirmed blockhash read for slot {slot} timed out after {read_timeout:?}");
            None
        }
    }
}

/// Publishes the anchor for one successful poll. An unhealthy poll resends the
/// previous anchor as unhealthy before `blockhash` is awaited.
async fn publish_anchor(
    anchor_tx: &watch::Sender<Option<Anchor>>,
    slots: &SlotSyncronizerData,
    blockhash: impl Future<Output = Option<String>>,
) {
    let polled_at = std::time::Instant::now();
    if !slots.healthy {
        let previous = anchor_tx.borrow().clone();
        if let Some(kept) = next_anchor(previous, slots, None, polled_at) {
            anchor_tx.send_replace(Some(kept));
        }
    }
    let blockhash = blockhash.await;
    let previous = anchor_tx.borrow().clone();
    if let Some(next) = next_anchor(previous, slots, blockhash, polled_at) {
        anchor_tx.send_replace(Some(next));
    }
}

/// The anchor for one poll. Without a blockhash it keeps the previous slot, hash
/// and finalized slot, and there is no anchor before the first hash.
fn next_anchor(
    previous: Option<Anchor>,
    slots: &SlotSyncronizerData,
    blockhash: Option<String>,
    polled_at: std::time::Instant,
) -> Option<Anchor> {
    match blockhash {
        Some(confirmed_blockhash) => Some(Anchor {
            confirmed_slot: slots.confirmed_slot.slot,
            confirmed_blockhash,
            finalized_slot: slots.finalized_slot.slot,
            healthy: slots.healthy,
            polled_at,
        }),
        None => previous.map(|previous| Anchor {
            healthy: slots.healthy,
            polled_at,
            ..previous
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slots(confirmed: u64, finalized: u64, healthy: bool) -> SlotSyncronizerData {
        SlotSyncronizerData {
            confirmed_slot: SlotData {
                slot: confirmed,
                block_time: 0,
            },
            finalized_slot: SlotData {
                slot: finalized,
                block_time: 0,
            },
            healthy,
        }
    }

    fn assert_triple(anchor: &Anchor, confirmed: u64, hash: &str, finalized: u64) {
        assert_eq!(anchor.confirmed_slot, confirmed);
        assert_eq!(anchor.confirmed_blockhash, hash);
        assert_eq!(anchor.finalized_slot, finalized);
    }

    #[test]
    fn no_anchor_before_the_first_hash() {
        let now = std::time::Instant::now();
        assert_eq!(next_anchor(None, &slots(100, 68, true), None, now), None);
    }

    #[test]
    fn hash_row_publishes_the_polled_triple() {
        let now = std::time::Instant::now();
        let anchor = next_anchor(None, &slots(100, 68, true), Some("h100".into()), now).unwrap();
        assert_triple(&anchor, 100, "h100", 68);
        assert!(anchor.healthy);
        assert_eq!(anchor.polled_at, now);
    }

    #[test]
    fn missing_hash_row_keeps_the_previous_triple_with_a_new_poll_time() {
        let first = std::time::Instant::now();
        let previous = next_anchor(None, &slots(100, 68, true), Some("h100".into()), first);
        let later = first + Duration::from_millis(200);
        let anchor = next_anchor(previous, &slots(101, 69, false), None, later).unwrap();
        assert_triple(&anchor, 100, "h100", 68);
        assert!(!anchor.healthy);
        assert_eq!(anchor.polled_at, later);
    }

    #[tokio::test]
    async fn unhealthy_poll_publishes_before_the_hash_read() {
        let first = std::time::Instant::now();
        let initial = next_anchor(None, &slots(100, 68, true), Some("h100".into()), first);
        let (anchor_tx, anchor_rx) = watch::channel(initial);

        let read = async {
            let during = anchor_rx.borrow().clone().unwrap();
            assert_triple(&during, 100, "h100", 68);
            assert!(!during.healthy);
            None
        };
        publish_anchor(&anchor_tx, &slots(101, 69, false), read).await;
        assert!(!anchor_rx.borrow().as_ref().unwrap().healthy);

        let read = async {
            // A healthy poll publishes nothing before the read.
            assert!(!anchor_rx.borrow().as_ref().unwrap().healthy);
            Some("h101".to_string())
        };
        publish_anchor(&anchor_tx, &slots(101, 69, true), read).await;
        let anchor = anchor_rx.borrow().clone().unwrap();
        assert_triple(&anchor, 101, "h101", 69);
        assert!(anchor.healthy);
    }
}
