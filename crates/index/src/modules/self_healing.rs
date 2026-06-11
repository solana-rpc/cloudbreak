// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::{IndexConfig, SnapshotConfig};
use cloudbreak_snapshot::sidecar::SnapshotType;
use sea_orm::DatabaseConnection;
use std::{
    collections::{BTreeSet, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;

use crate::{
    indexer::IndexerState,
    metrics,
    modules::{finalize_slot::SlotFinalizer, snapshot::SnapshotProcessingState},
};

/// Tracks slot continuity on the gRPC block stream and repairs confirmed gaps out of snapshots.
///
/// Gaps are confirmed purely from the block chain (`parent_slot` / `parent_blockhash`) without any
/// RPC call: if a newly received block does not build directly on the last block we have, then at
/// least one real block was missed and the whole range is repaired (empty slots in the middle are
/// resolved for free, since the snapshot only contains slots that actually had account data).
#[derive(Clone)]
pub struct SelfHealingState {
    pub last_slot_received: Arc<Mutex<u64>>,
    /// Slots pending repair from a snapshot.
    pub gaps_list: Arc<Mutex<Vec<u64>>>,
    /// For each confirmed gap, the last live slot we received before it (`gap_start - 1`). After
    /// the gap is repaired we seed an ancestor walk from these slots, because the repaired slots
    /// carry no chain data and therefore can not bridge the walk back to them (see
    /// [`SlotFinalizer::enqueue_gap_boundary`]).
    pub gap_boundaries: Arc<Mutex<BTreeSet<u64>>>,
    pub finalizer: SlotFinalizer,
}

impl SelfHealingState {
    pub fn new(_config: &IndexConfig, finalizer: SlotFinalizer) -> Self {
        Self {
            last_slot_received: Arc::new(Mutex::new(0)),
            gaps_list: Arc::new(Mutex::new(Vec::new())),
            gap_boundaries: Arc::new(Mutex::new(BTreeSet::new())),
            finalizer,
        }
    }

    fn remove_slot_from_gaps_list(&self, slot: u64) {
        self.gaps_list
            .lock()
            .expect("Failed to lock gaps_list")
            .retain(|s| *s != slot);
    }

    /// Checks whether `slot` continues the chain from the last block we received.
    ///
    /// Using the block's parent pointer:
    /// - If the block builds directly on the last received slot (matching hash), any slots in
    ///   between were skipped/empty: nothing to do.
    /// - Otherwise at least one real block was missed: the whole range is queued for repair, the
    ///   service is marked unhealthy, and finalization is paused until the gap is filled.
    pub async fn check_slot_gap(&self, slot: u64, parent_slot: u64, parent_blockhash: &str) {
        let last_slot_received = *self
            .last_slot_received
            .lock()
            .expect("Failed to lock last_slot_received");

        if slot <= last_slot_received {
            tracing::warn!(
                "Out of order slot received: {} - previous slot received: {}",
                slot,
                last_slot_received
            );

            // If we had added the slot to the gaps list, remove it
            self.remove_slot_from_gaps_list(slot);

            return;
        }

        if last_slot_received != 0 && slot > last_slot_received + 1 {
            // The block builds directly on the last slot we have (and the hash matches) if all the
            // slots in between were empty/skipped. Anything else means a real block was missed.
            let builds_on_last = parent_slot == last_slot_received
                && self
                    .finalizer
                    .block_hash(parent_slot)
                    .map(|hash| hash == parent_blockhash)
                    .unwrap_or(false);

            if builds_on_last {
                tracing::debug!(
                    target: "self_healing_empty_slots",
                    "Skipped empty slots between {} and {} (block builds directly on {})",
                    last_slot_received,
                    slot,
                    parent_slot
                );
            } else {
                let new_gap_slots = ((last_slot_received + 1)..slot).collect::<Vec<_>>();

                tracing::error!(
                    target: "self_healing",
                    "Confirmed slot gap: last received {} - new slot {} - parent_new_slot {} - queuing {} slots for repair",
                    last_slot_received,
                    slot,
                    parent_slot,
                    new_gap_slots.len()
                );

                self.gaps_list
                    .lock()
                    .expect("Failed to lock gaps_list")
                    .extend(new_gap_slots);

                // Remember the last live slot before the gap so we can seed an ancestor walk from
                // it once the gap is repaired (the repaired slots can not carry the walk back to it).
                self.gap_boundaries
                    .lock()
                    .expect("Failed to lock gap_boundaries")
                    .insert(last_slot_received);

                // Pause finalization and mark the service unhealthy immediately. `fill_gaps` resumes
                // it once every slot in the gap has been repaired. NOTE: a gap discovered during
                // startup pauses the very worker that completes startup; that is intentionally
                // unsupported and `fill_gaps` fails fast in that case.
                self.finalizer.pause().await;
            }
        }

        *self
            .last_slot_received
            .lock()
            .expect("Failed to lock last_slot_received") = slot;
    }

    /// Starts a separate task that periodically repairs confirmed slot gaps out of incremental snapshots.
    ///
    /// Processes only one snapshot at a time, downloading an incremental snapshot that covers the
    /// newest slot in the gaps list and processing only the slots that are in the gaps list. The
    /// repaired slots are recorded and enqueued for finalization; once the gaps list drains,
    /// finalization is resumed (which marks the service healthy again).
    pub async fn fill_gaps(
        self,
        db: DatabaseConnection,
        config: IndexConfig,
        indexer_state: IndexerState,
    ) -> JoinHandle<Result<(), anyhow::Error>> {
        tokio::spawn(async move {
            let _guard = metrics::TokioTaskCounterGuard::new("self_healing_fill_gaps");

            loop {
                // TODO: Make the gap filling interval configurable
                tokio::time::sleep(Duration::from_secs(30)).await;

                let mut confirmed_gaps_list = self
                    .gaps_list
                    .lock()
                    .expect("Failed to lock gaps_list")
                    .clone();

                // Snapshot the gap boundaries that belong to the gaps existing at the start of this
                // iteration. New gaps detected mid-fill (and their boundaries) are excluded so we
                // never seed a boundary that sits above a gap not yet repaired in this iteration.
                let gap_boundaries_snapshot = self
                    .gap_boundaries
                    .lock()
                    .expect("Failed to lock gap_boundaries")
                    .clone();

                if confirmed_gaps_list.is_empty() {
                    // No gaps pending: make sure finalization is resumed (no-op if it was not paused).
                    self.finalizer.resume().await;
                    continue;
                }

                // We have confirmed gaps to repair. Finalization was already paused when the gap was
                // discovered (in `check_slot_gap`). A confirmed gap is only expected after startup has
                // finished: during startup the (now paused) finalizer worker is what completes startup,
                // so a gap there can never be repaired. Fail fast instead of stalling forever.
                let is_startup_finished = *indexer_state
                    .snapshot_processing_state
                    .lock()
                    .expect("Failed to lock snapshot_processing_state")
                    == SnapshotProcessingState::FinishedAndCleanedUp;
                if !is_startup_finished {
                    tracing::error!(
                        "Confirmed slot gap detected before startup finished; cannot repair gaps during startup"
                    );
                    panic!(
                        "Confirmed slot gap detected before startup finished; cannot repair gaps during startup"
                    );
                }

                let start_time = tokio::time::Instant::now();
                tracing::info!("Starting to fill gaps: {:?}", confirmed_gaps_list);

                confirmed_gaps_list.sort_unstable();
                let newest_slot_in_gaps_list =
                    *confirmed_gaps_list.last().expect("No slots in gaps list");

                let snapshot_config = config.snapshot.as_ref().unwrap();
                let snapshot_config = SnapshotConfig {
                    accounts_file_concurency: snapshot_config.accounts_file_concurency,
                    database: config.database.clone(),
                    tracker_endpoint: snapshot_config.tracker_endpoint.clone(),
                    metrics: config.metrics.clone(),
                    programs: config.programs.clone(),
                    pg_indexes: snapshot_config.pg_indexes.clone(),
                };

                let (handle, mut rx) = match download_and_process_snapshot_for_gap_filling(
                    Some(newest_slot_in_gaps_list),
                    snapshot_config,
                    confirmed_gaps_list.clone(),
                )
                .await
                {
                    Ok((handle, rx)) => (handle, rx),
                    Err(e) => {
                        tracing::warn!(
                            "Snapshot is not available for gap filling yet, waiting for next iteration (error: {:?})",
                            e
                        );
                        continue;
                    }
                };

                // Finalization is already paused (since the gap was discovered in `check_slot_gap`)
                let mut repaired_slots: HashSet<u64> = HashSet::new();
                while let Some(update_block) = rx.recv().await {
                    let slot = update_block.slot;

                    // Write the repaired block's accounts and record it in the finalizer map.
                    crate::modules::save_block::save_block(
                        update_block,
                        &db,
                        config.clone(),
                        indexer_state.clone(),
                    )
                    .await;

                    // Snapshot data is already finalized, so enqueue the repaired slot directly
                    // (bypassing the back-pressure bound). It will be finalized in order on resume.
                    self.finalizer.enqueue_unbounded(slot);
                    self.remove_slot_from_gaps_list(slot);
                    repaired_slots.insert(slot);
                }

                handle.await??;

                // Every gap slot the snapshot did NOT emit a block for had no account data: it is an
                // empty/skipped slot with nothing to repair. Crucially these slots are not part of
                // the chain the ancestor walk follows (they have no blockhash), so they are expected
                // to be absent.
                let empty_slots: Vec<u64> = confirmed_gaps_list
                    .iter()
                    .copied()
                    .filter(|slot| !repaired_slots.contains(slot))
                    .collect();
                if !empty_slots.is_empty() {
                    tracing::debug!(
                        target: "self_healing_empty_slots",
                        "Gap fill from snapshot: {} of {} gap slot(s) had no accounts (empty slots, nothing to repair, not in the ancestor chain): {:?}",
                        empty_slots.len(),
                        confirmed_gaps_list.len(),
                        empty_slots
                    );
                    for slot in &empty_slots {
                        self.remove_slot_from_gaps_list(*slot);
                    }
                }

                // Seed an ancestor walk from the slot just before each repaired gap. These slots
                // are smaller than the repaired/live slots, so on resume the worker processes them
                // first, walking down through any pre-gap slots whose finalized notifications were
                // missed (the repaired slots can not bridge the walk back to them). Walks here only
                // descend into already-clean/repaired territory, so ordering is preserved.
                if !gap_boundaries_snapshot.is_empty() {
                    for boundary_slot in &gap_boundaries_snapshot {
                        self.enqueue_gap_boundary(*boundary_slot);
                    }
                    let mut gap_boundaries = self
                        .gap_boundaries
                        .lock()
                        .expect("Failed to lock gap_boundaries");
                    for boundary_slot in &gap_boundaries_snapshot {
                        gap_boundaries.remove(boundary_slot);
                    }
                }

                let elapsed = start_time.elapsed().as_secs_f64();
                tracing::info!(
                    "Finished filling gaps: {:?} - in {} seconds",
                    confirmed_gaps_list,
                    elapsed
                );

                // If every gap has been repaired, resume finalization
                let gaps_remaining = !self
                    .gaps_list
                    .lock()
                    .expect("Failed to lock gaps_list")
                    .is_empty();
                if !gaps_remaining {
                    self.finalizer.resume().await;
                }
            }
        })
    }

    /// Enqueues the live slot immediately preceding a repaired gap (`gap_start - 1`) so the worker
    /// runs an ancestor walk from it.
    ///
    /// Repaired slots carry no chain data (empty `blockhash`/`parent_blockhash`), so they can not
    /// bridge the ancestor walk back across the gap to this slot and its ancestors. Without this
    /// seed, finalized notifications that were missed for the slots just below a (large) gap would
    /// never be applied, leaving those slots stuck in the map. The slot is provably finalized (it is
    /// an ancestor of the finalized snapshot data), and finalizing it is idempotent if it was
    /// already finalized. Bypasses the back-pressure bound.
    ///
    /// # When this is needed
    ///
    /// The normal ancestor walk only catches a missed finalized notification if some descendant we
    /// *do* receive a notification for can walk back to it through unbroken chain data. A repaired
    /// gap breaks that chain, and the failure only bites when the gap is large enough (≈ the
    /// confirmed→finalized lag, ~32 slots) that the missed-notification window reaches the slot just
    /// below the gap.
    ///
    /// ### Example:
    /// A 40-slot disconnect. After reconnect the confirmed tip is `100` and the network
    /// finalized frontier is `68` (~32 behind), and the gap is the 40 missed slots `60..=99`
    /// (boundary slot `59` = `gap_start - 1`):
    ///
    /// - Each finalized notification trails confirmed by ~32, so while we were missing confirmed
    ///   blocks `60..=99` we also dropped the finalized notifications emitted in that window: those
    ///   for slots `28..=67`.
    /// - Slots `28..=59` are pre-gap live slots (present in the map with real chain data); `60..=67`
    ///   fall inside the gap and are repaired from the snapshot.
    /// - After reconnect the finalized notifications resume at `68`, but `68..=99` are repaired gap
    ///   slots with no chain data, so their walks stop immediately and never reach `59`. The post-gap
    ///   live slots (`100+`) walk back only as far as `99` (repaired) and stop too.
    /// - So nothing descends into `28..=59`; without intervention they stay stuck in the map.
    ///
    /// Seeding a walk from `59` finalizes `59 → 58 → … → 28`, stopping at the last already-finalized
    /// slot (`27`). For a *small* gap (e.g. `60..=62`) the boundary's own notification arrives
    /// normally after reconnect (its missed-notification window, `28..=30`, never reaches `59`), so
    /// by fill time slot `59` is already finalized and this seed is a harmless no-op.
    fn enqueue_gap_boundary(&self, slot: u64) {
        tracing::info!(target: "finalizer", "Enqueuing gap boundary slot {}", slot);
        self.finalizer.enqueue_unbounded(slot);
    }
}

async fn download_and_process_snapshot_for_gap_filling(
    received_slot: Option<u64>,
    config: SnapshotConfig,
    gaps_list: Vec<u64>,
) -> Result<
    (
        JoinHandle<Result<(), anyhow::Error>>,
        Receiver<SubscribeUpdateBlock>,
    ),
    anyhow::Error,
> {
    let snapshot_pair_future = cloudbreak_snapshot::sidecar::get_snapshot_data(
        &config.tracker_endpoint.endpoint,
        received_slot,
        true,
        true,
    );

    let snapshot_pair =
        tokio::time::timeout(Duration::from_secs(60), snapshot_pair_future).await??;

    let (tx, rx) = tokio::sync::mpsc::channel::<SubscribeUpdateBlock>(100);

    // We passed the force_returned_incremental flag to true, so we know that the snapshot pair contains an incremental snapshot
    let incremental_snapshot_data = snapshot_pair
        .incremental_snapshot
        .ok_or_else(|| anyhow::anyhow!("No incremental snapshot available"))?;

    // Use a timestamped directory so concurrent/sequential gap fills for the same slot never
    // collide on disk.
    let base_dir: PathBuf =
        cloudbreak_snapshot::sidecar::snapshot_base_dir_timestamped(incremental_snapshot_data.slot);

    let handle = tokio::spawn(async move {
        cloudbreak_snapshot::sidecar::download_snapshot_file(
            &snapshot_pair.downloading_endpoint,
            incremental_snapshot_data.clone(),
            SnapshotType::Incremental,
            &base_dir,
        )
        .await?;

        cloudbreak_snapshot::process_downloaded_snapshot_with_gap_filling(
            incremental_snapshot_data.slot,
            incremental_snapshot_data.file_name,
            base_dir,
            config,
            gaps_list,
            tx,
        )
        .await?;

        Ok(())
    });

    Ok((handle, rx))
}
