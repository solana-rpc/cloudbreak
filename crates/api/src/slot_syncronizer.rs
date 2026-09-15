// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::db_query;
use cloudbreak_core::ApiConfig;
use cloudbreak_core::modules::processed::Anchor;
use sea_orm::DatabaseConnection;
use solana_commitment_config::CommitmentLevel;
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

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
/// confirmed slot and its blockhash as the processed [`Anchor`] on `anchor_tx`.
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
            let mut confirmed = None;

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
                confirmed = Some(cached_slot_data.confirmed_slot.slot);
            }

            // Published after the block so the slot cache lock is released first.
            if let (Some(anchor_tx), Some(confirmed_slot)) = (&anchor_tx, confirmed)
                && let Some(confirmed_blockhash) = read_blockhash(&db, confirmed_slot, delay).await
            {
                publish_anchor(
                    anchor_tx,
                    Anchor {
                        confirmed_slot,
                        confirmed_blockhash,
                    },
                );
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

/// Publishes the anchor when it differs from the last one published.
fn publish_anchor(anchor_tx: &watch::Sender<Option<Anchor>>, anchor: Anchor) {
    anchor_tx.send_if_modified(|current| {
        if current.as_ref() == Some(&anchor) {
            return false;
        }
        *current = Some(anchor);
        true
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_is_published_once_per_change() {
        let (anchor_tx, mut anchor_rx) = watch::channel(None);
        let anchor = Anchor {
            confirmed_slot: 100,
            confirmed_blockhash: "h100".to_string(),
        };
        publish_anchor(&anchor_tx, anchor.clone());
        assert!(anchor_rx.has_changed().unwrap());
        assert_eq!(anchor_rx.borrow_and_update().as_ref(), Some(&anchor));

        publish_anchor(&anchor_tx, anchor.clone());
        assert!(!anchor_rx.has_changed().unwrap());

        publish_anchor(
            &anchor_tx,
            Anchor {
                confirmed_slot: 101,
                ..anchor
            },
        );
        assert!(anchor_rx.has_changed().unwrap());
        assert_eq!(
            anchor_rx
                .borrow_and_update()
                .as_ref()
                .unwrap()
                .confirmed_slot,
            101
        );
    }
}
