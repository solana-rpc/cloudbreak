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
use tokio::{task::JoinHandle, time::Instant};
use cloudbreak_core::ApiConfig;

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

pub fn start_slot_syncronizer(
    db: DatabaseConnection,
    config: &ApiConfig,
) -> Option<(JoinHandle<()>, Arc<RwLock<SlotSyncronizerData>>)> {
    if !config.slot_syncronizer.enabled {
        return None;
    }

    let slot_syncronizer_data = Arc::new(RwLock::new(SlotSyncronizerData::default()));
    let delay = Duration::from_millis(config.slot_syncronizer.interval_ms);

    let slot_data_clone = slot_syncronizer_data.clone();
    let join_handle = tokio::spawn(async move {
        let mut last_time_sync = Instant::now();
        loop {
            tokio::time::sleep(delay).await;
            tracing::debug!(target: "slot_syncronizer", "Slot syncronizer: last time sync: {:?}", last_time_sync.elapsed().as_secs_f32());
            let query_start_time = Instant::now();

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
            }
        }
    });

    Some((join_handle, slot_syncronizer_data))
}
