// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::Result;
use cloudbreak_core::{HashCheckerConfig, IndexConfig, SnapshotConfigOnIndexer};
use cloudbreak_snapshot::lt_hash::compute_filtered_snapshot_lt_hash;
use cloudbreak_snapshot::sidecar::{self, AccountFileData, SnapshotType, download_snapshot_file};
use sea_orm::DatabaseConnection;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use yellowstone_grpc_proto::geyser::{SlotStatus, SubscribeUpdate, subscribe_update::UpdateOneof};

use crate::indexer::IndexerState;
use crate::modules::lt_hash::compute_db_lt_hash;

const SLOT_MS: u64 = 400;
const FINALIZE_DRAIN_POLL_MS: u64 = 500;
const FINALIZE_DRAIN_FINAL_WAIT_MS: u64 = 2_000;

#[derive(Clone)]
pub struct HashCheckerState {
    cfg: HashCheckerConfig,
    snapshot_cfg: SnapshotConfigOnIndexer,
    target_slot: Arc<AtomicU64>,
    covered_slot: Arc<AtomicU64>,
    buffer: Arc<Mutex<Vec<SubscribeUpdate>>>,
    buffering: Arc<AtomicBool>,
    grpc_cancel: Arc<AtomicBool>,
    snapshot_files: Arc<Mutex<Option<SnapshotFile>>>,
}

pub struct SnapshotFile {
    pub files: Vec<AccountFileData>,
    pub metadata_path: PathBuf,
}

impl HashCheckerState {
    pub fn new(
        cfg: HashCheckerConfig,
        snapshot_cfg: SnapshotConfigOnIndexer,
        grpc_cancel: Arc<AtomicBool>,
    ) -> Self {
        if cfg.time_limit.is_none() && cfg.slot_limit.is_none() {
            panic!("hash-checker config requires either time-limit or slot-limit");
        }
        Self {
            target_slot: Arc::new(AtomicU64::new(cfg.slot_limit.unwrap_or(0))),
            covered_slot: Arc::new(AtomicU64::new(0)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            buffering: Arc::new(AtomicBool::new(false)),
            grpc_cancel,
            snapshot_files: Arc::new(Mutex::new(None)),
            cfg,
            snapshot_cfg,
        }
    }

    pub fn set_target_from_first_block(&self, first_block_slot: u64) {
        if self.target_slot.load(Ordering::SeqCst) != 0 {
            return;
        }
        let Some(time_limit) = self.cfg.time_limit else {
            return;
        };
        let slots = (time_limit.as_millis() as u64) / SLOT_MS;
        let target = first_block_slot + slots;
        if self
            .target_slot
            .compare_exchange(0, target, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            tracing::info!(
                "hash-checker target_slot set to {} (first_block={}, time_limit={:?})",
                target,
                first_block_slot,
                time_limit
            );
        }
    }

    pub fn on_finalized_slot(&self, slot: u64) {
        let target = self.target_slot.load(Ordering::SeqCst);
        if target == 0 || slot < target {
            return;
        }
        if !self.buffering.swap(true, Ordering::SeqCst) {
            tracing::info!("hash-checker buffering phase started at slot {}", slot);
        }
        let covered = self.covered_slot.load(Ordering::SeqCst);
        if covered != 0 && slot >= covered {
            self.grpc_cancel.store(true, Ordering::SeqCst);
        }
    }

    pub fn is_buffering(&self) -> bool {
        self.buffering.load(Ordering::SeqCst)
    }

    pub fn push(&self, update: SubscribeUpdate) {
        self.buffer
            .lock()
            .expect("Failed to lock hash-checker buffer")
            .push(update);
    }

    pub fn should_break(&self) -> bool {
        self.grpc_cancel.load(Ordering::SeqCst)
    }

    pub fn spawn_orchestrator(&self) {
        let state = self.clone();
        tokio::spawn(async move {
            if let Err(e) = run_orchestrator(state).await {
                tracing::error!("hash-checker orchestrator failed: {:?}", e);
                std::process::exit(1);
            }
        });
    }
}

pub fn note_update(state: &HashCheckerState, update: &SubscribeUpdate) {
    match &update.update_oneof {
        Some(UpdateOneof::Block(block)) => {
            state.set_target_from_first_block(block.slot);
        }
        Some(UpdateOneof::Slot(slot_update)) => {
            if let Ok(SlotStatus::SlotFinalized) = SlotStatus::try_from(slot_update.status) {
                state.on_finalized_slot(slot_update.slot);
            }
        }
        _ => {}
    }
}

async fn run_orchestrator(state: HashCheckerState) -> Result<()> {
    loop {
        if state.buffering.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let target_slot = state.target_slot.load(Ordering::SeqCst);
    let tracker_endpoint = state.snapshot_cfg.tracker_endpoint.endpoint.clone();

    let snapshot_pair =
        sidecar::get_snapshot_data(&tracker_endpoint, Some(target_slot), false, true).await?;

    let incremental_snapshot_data = snapshot_pair
        .incremental_snapshot
        .clone()
        .ok_or_else(|| anyhow::anyhow!("No incremental snapshot available"))?;

    tracing::info!(
        "hash-checker: snapshots will cover slot {} (snapshot_pair={:?})",
        incremental_snapshot_data.slot,
        snapshot_pair
    );
    state
        .covered_slot
        .store(incremental_snapshot_data.slot, Ordering::SeqCst);

    let full_base_dir = sidecar::snapshot_base_dir(snapshot_pair.full_snapshot.slot);
    let inc_base_dir = sidecar::snapshot_base_dir(incremental_snapshot_data.slot);

    let full = download_snapshot_file(
        &snapshot_pair.downloading_endpoint,
        snapshot_pair.full_snapshot.clone(),
        SnapshotType::Full,
        &full_base_dir,
    );
    let inc = download_snapshot_file(
        &snapshot_pair.downloading_endpoint,
        incremental_snapshot_data.clone(),
        SnapshotType::Incremental,
        &inc_base_dir,
    );

    match tokio::join!(full, inc) {
        (Ok(_), Ok(_)) => (),
        _ => return Err(anyhow::anyhow!("Failed to download snapshots")),
    };

    let full_path = full_base_dir.join(&snapshot_pair.full_snapshot.file_name);
    let inc_path = inc_base_dir.join(&incremental_snapshot_data.file_name);

    let mut snapshot_files = sidecar::unpack_compressed_snapshot(
        full_path,
        &full_base_dir,
        snapshot_pair.full_snapshot.slot,
    )?
    .account_files;
    snapshot_files.extend(
        sidecar::unpack_compressed_snapshot(
            inc_path,
            &inc_base_dir,
            incremental_snapshot_data.slot,
        )?
        .account_files,
    );

    let slot = incremental_snapshot_data.slot;

    let metadata_path = PathBuf::from(format!(
        "./snapshot_{slot}/uncompressed_snapshot/snapshots/{slot}/{slot}"
    ));

    *state
        .snapshot_files
        .lock()
        .expect("Failed to lock snapshot_files") = Some(SnapshotFile {
        files: snapshot_files,
        metadata_path,
    });

    Ok(())
}

pub async fn finalize_and_compare(
    state: HashCheckerState,
    db: DatabaseConnection,
    config: IndexConfig,
    indexer_state: IndexerState,
) -> Result<bool> {
    let covered = state.covered_slot.load(Ordering::SeqCst);
    if covered == 0 {
        return Err(anyhow::anyhow!(
            "hash-checker finalize called before covered slot was known"
        ));
    }

    let buffered: Vec<SubscribeUpdate> = std::mem::take(
        &mut *state
            .buffer
            .lock()
            .expect("Failed to lock hash-checker buffer"),
    );
    tracing::info!(
        "hash-checker replaying {} buffered updates up to slot {}",
        buffered.len(),
        covered
    );

    for update in buffered {
        let over = match &update.update_oneof {
            Some(UpdateOneof::Block(b)) => b.slot > covered,
            Some(UpdateOneof::Slot(s)) => s.slot > covered,
            _ => false,
        };
        if over {
            continue;
        }
        crate::indexer::process_update(update, &indexer_state, &db, &config).await;
    }

    loop {
        let size = *indexer_state
            .finalize_slot_buffer_size
            .lock()
            .expect("Failed to lock finalize_slot_buffer_size");
        if size == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(FINALIZE_DRAIN_POLL_MS)).await;
    }
    tokio::time::sleep(Duration::from_millis(FINALIZE_DRAIN_FINAL_WAIT_MS)).await;

    let SnapshotFile {
        files,
        metadata_path,
    } = loop {
        if let Some(pair) = state
            .snapshot_files
            .lock()
            .expect("Failed to lock snapshot_files")
            .take()
        {
            break pair;
        }
        tracing::info!("hash-checker: waiting for snapshots to finish downloading");
        tokio::time::sleep(Duration::from_secs(1)).await;
    };

    tracing::info!("hash-checker: computing DB LtHash at slot {}", covered);
    let (db_hash, db_count) = compute_db_lt_hash(&db, covered, &config.programs).await?;

    tracing::info!("hash-checker: computing snapshot LtHash");
    let (snap_hash, snap_count) =
        compute_filtered_snapshot_lt_hash(&files, &metadata_path, &config.programs)?;

    let db_cs = db_hash.checksum();
    let sn_cs = snap_hash.checksum();
    let matches = db_cs.0 == sn_cs.0;

    tracing::info!(
        "hash-checker db: {} accounts, checksum={}",
        db_count,
        hex::encode(db_cs.0)
    );
    tracing::info!(
        "hash-checker snapshot: {} accounts, checksum={}",
        snap_count,
        hex::encode(sn_cs.0)
    );

    if matches {
        tracing::info!("hash-checker MATCH — indexer data is consistent with on-chain state");
    } else {
        tracing::error!("hash-checker MISMATCH — indexer data differs from on-chain state");
    }

    Ok(matches)
}
