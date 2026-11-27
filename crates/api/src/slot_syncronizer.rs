// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Keeps the confirmed and finalized slots and the service health in memory. The `slots_notify`
//! trigger sends every real change of a `slots` row on [`SLOTS_CHANNEL`]; this task applies it.
//! A lost connection, a bad payload or [`IDLE_TIMEOUT`] without a notification panics.
//! With `[processed-accounts]` enabled it also publishes the processed [`Anchor`]. With
//! `[mint-cache]` it applies each confirmed slot's mint changes before publishing the slot.

use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cloudbreak_core::ApiConfig;
use cloudbreak_core::modules::mint_cache::MintCache;
use cloudbreak_core::modules::processed::Anchor;
use sea_orm::DatabaseConnection;
use sea_orm::sqlx::postgres::PgListener;
use serde::Deserialize;
use solana_commitment_config::CommitmentLevel;
use tokio::{sync::watch, task::JoinHandle};

use crate::metrics;

/// The channel the `slots_notify` trigger sends on.
pub const SLOTS_CHANNEL: &str = "cloudbreak_slots";
/// `slots.commitment` values, as the indexer writes them.
const COMMITMENT_CONFIRMED: i32 = 1;
const COMMITMENT_FINALIZED: i32 = 2;
/// Slots move every ~200 ms, so this much silence means the listener or the indexer is stuck.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

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

/// One `slots` row, from a notification payload.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SlotRow {
    pub commitment: i32,
    pub slot: i64,
    pub block_time: i64,
    pub health: bool,
    /// Set on the confirmed row only.
    pub blockhash: Option<String>,
    /// Postgres wall clock in unix microseconds when the trigger ran.
    pub written_at_us: i64,
}

/// Starts the slot syncronizer, or returns `None` when `[slot-syncronizer]` is disabled.
/// The task only ends by panicking; the caller must treat that as fatal.
pub fn start_slot_syncronizer(
    db: DatabaseConnection,
    config: &ApiConfig,
    anchor_tx: watch::Sender<Option<Anchor>>,
    mint_cache: MintCache,
) -> Option<(JoinHandle<()>, Arc<RwLock<SlotSyncronizerData>>)> {
    if !config.slot_syncronizer.enabled {
        return None;
    }

    let data = Arc::new(RwLock::new(SlotSyncronizerData::default()));
    let anchor_tx = config.processed_accounts_enabled().then_some(anchor_tx);
    let handle = tokio::spawn(run(db, data.clone(), anchor_tx, mint_cache));
    spawn_dedicated_listener(config.database.url.clone());

    Some((handle, data))
}

/// Records the notify delay as `stage="received_dedicated"` from a second listener on its own OS
/// thread, runtime and connection. Against `received` it separates API scheduling from Postgres.
fn spawn_dedicated_listener(url: String) {
    let spawned = std::thread::Builder::new()
        .name("slot-notify-probe".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(e) => {
                    tracing::error!(target: "slot_syncronizer", "notify probe runtime: {e}");
                    return;
                }
            };
            runtime.block_on(async move {
                let mut listener = match PgListener::connect(&url).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        tracing::error!(target: "slot_syncronizer", "notify probe connect: {e}");
                        return;
                    }
                };
                if let Err(e) = listener.listen(SLOTS_CHANNEL).await {
                    tracing::error!(target: "slot_syncronizer", "notify probe LISTEN: {e}");
                    return;
                }
                // `recv` reconnects by itself; a payload it cannot parse is skipped.
                loop {
                    match listener.recv().await {
                        Ok(notification) => {
                            let payload = notification.payload();
                            if let Ok(row) = serde_json::from_str::<SlotRow>(payload) {
                                record_notify_delay(&row, "received_dedicated");
                            }
                        }
                        Err(e) => {
                            tracing::warn!(target: "slot_syncronizer", "notify probe: {e}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
        });
    if let Err(e) = spawned {
        tracing::error!(target: "slot_syncronizer", "notify probe thread: {e}");
    }
}

/// Listens on [`SLOTS_CHANNEL`] and applies each notification. Until the first one arrives the
/// slots are 0 and the node reads as unhealthy.
async fn run(
    db: DatabaseConnection,
    data: Arc<RwLock<SlotSyncronizerData>>,
    anchor_tx: Option<watch::Sender<Option<Anchor>>>,
    mint_cache: MintCache,
) {
    let mut listener = PgListener::connect_with(db.get_postgres_connection_pool())
        .await
        .expect("slot syncronizer: failed to connect the listener");
    listener
        .listen(SLOTS_CHANNEL)
        .await
        .expect("slot syncronizer: LISTEN failed");
    tracing::info!(target: "slot_syncronizer", "Listening on {SLOTS_CHANNEL}");

    loop {
        // `Ok(None)` means the connection dropped and notifications were lost.
        let notification = tokio::time::timeout(IDLE_TIMEOUT, listener.try_recv())
            .await
            .unwrap_or_else(|_| {
                panic!("slot syncronizer: no slot notification for {IDLE_TIMEOUT:?}")
            })
            .expect("slot syncronizer: listener error")
            .expect("slot syncronizer: listener connection lost");
        let row: SlotRow = serde_json::from_str(notification.payload()).unwrap_or_else(|e| {
            panic!(
                "slot syncronizer: bad {SLOTS_CHANNEL} payload {:?}: {e}",
                notification.payload()
            )
        });
        record_notify_delay(&row, "received");
        // The cache must hold a slot's mint changes before a request can pick that slot.
        if mint_cache.is_enabled() && confirmed_advance(&data, &row) {
            mint_cache
                .api()
                .on_confirmed(row.slot as u64, row.blockhash.as_deref(), row.health);
        }
        apply(&data, anchor_tx.as_ref(), &row);
        record_notify_delay(&row, "applied");
    }
}

/// Records the delay from the trigger's `written_at_us` to now. A negative delay is only counted.
fn record_notify_delay(row: &SlotRow, stage: &str) {
    let commitment = match row.commitment {
        COMMITMENT_CONFIRMED => "confirmed",
        COMMITMENT_FINALIZED => "finalized",
        _ => "other",
    };
    match notify_delay_us(row.written_at_us, SystemTime::now()) {
        Some(delay_us) => metrics::CLOUDBREAK_API_SLOT_NOTIFY_DELAY_US
            .with_label_values(&[commitment, stage])
            .observe(delay_us as f64),
        None => metrics::CLOUDBREAK_API_SLOT_NOTIFY_NEGATIVE_DELAY_TOTAL
            .with_label_values(&[commitment, stage])
            .inc(),
    }
}

/// Microseconds from `written_at_us` to `now`, or `None` when `now` is earlier.
fn notify_delay_us(written_at_us: i64, now: SystemTime) -> Option<u64> {
    let now_us = i64::try_from(now.duration_since(UNIX_EPOCH).ok()?.as_micros()).ok()?;
    u64::try_from(now_us.checked_sub(written_at_us)?).ok()
}

fn confirmed_advance(data: &RwLock<SlotSyncronizerData>, row: &SlotRow) -> bool {
    row.commitment == COMMITMENT_CONFIRMED
        && row.slot as u64 > data.read().expect("Failed to lock slot data").confirmed_slot.slot
}

/// Applies one row. Slots never move backwards. A confirmed advance publishes the anchor.
fn apply(
    data: &RwLock<SlotSyncronizerData>,
    anchor_tx: Option<&watch::Sender<Option<Anchor>>>,
    row: &SlotRow,
) {
    let slot = row.slot as u64;
    let advanced = {
        let mut data = data.write().expect("Failed to lock slot data");
        data.healthy = row.health;
        let current = match row.commitment {
            COMMITMENT_CONFIRMED => &mut data.confirmed_slot,
            COMMITMENT_FINALIZED => &mut data.finalized_slot,
            other => panic!("slot syncronizer: unknown commitment {other}"),
        };
        let advanced = slot > current.slot;
        if advanced {
            *current = SlotData {
                slot,
                block_time: row.block_time,
            };
        }
        advanced
    };

    if advanced
        && row.commitment == COMMITMENT_CONFIRMED
        && let Some(anchor_tx) = anchor_tx
    {
        let confirmed_blockhash = row
            .blockhash
            .clone()
            .unwrap_or_else(|| panic!("slot syncronizer: confirmed slot {slot} has no blockhash"));
        anchor_tx.send_replace(Some(Anchor {
            confirmed_slot: slot,
            confirmed_blockhash,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(commitment: i32, slot: i64, health: bool, blockhash: Option<&str>) -> SlotRow {
        SlotRow {
            commitment,
            slot,
            block_time: slot * 10,
            health,
            blockhash: blockhash.map(str::to_string),
            written_at_us: slot * 1_000,
        }
    }

    fn data() -> RwLock<SlotSyncronizerData> {
        RwLock::new(SlotSyncronizerData::default())
    }

    #[test]
    fn payload_parses_with_and_without_blockhash_and_needs_written_at_us() {
        let row: SlotRow = serde_json::from_str(
            r#"{"commitment":1,"slot":100,"block_time":1000,"health":true,"blockhash":"abc","written_at_us":100000}"#,
        )
        .unwrap();
        assert_eq!(row, self::row(1, 100, true, Some("abc")));
        let row: SlotRow = serde_json::from_str(
            r#"{"commitment":2,"slot":90,"block_time":900,"health":false,"blockhash":null,"written_at_us":90000}"#,
        )
        .unwrap();
        assert_eq!(row, self::row(2, 90, false, None));
        assert!(
            serde_json::from_str::<SlotRow>(
                r#"{"commitment":1,"slot":100,"block_time":1000,"health":true,"blockhash":"abc"}"#,
            )
            .is_err()
        );
    }

    #[test]
    fn notify_delay_is_microseconds_and_none_when_now_is_earlier() {
        let now = UNIX_EPOCH + Duration::from_micros(1_795_800_000_123_456);
        assert_eq!(notify_delay_us(1_795_800_000_123_000, now), Some(456));
        assert_eq!(notify_delay_us(1_795_800_000_123_456, now), Some(0));
        assert_eq!(notify_delay_us(1_795_800_000_123_457, now), None);
    }

    #[test]
    fn slots_only_move_forward_and_health_follows_the_latest_row() {
        let data = data();
        apply(&data, None, &row(1, 100, true, Some("h100")));
        apply(&data, None, &row(1, 99, false, Some("h99")));
        apply(&data, None, &row(2, 80, false, None));
        let data = data.read().unwrap().clone();
        assert_eq!(data.confirmed_slot.slot, 100);
        assert_eq!(data.confirmed_slot.block_time, 1000);
        assert_eq!(data.finalized_slot.slot, 80);
        assert!(!data.healthy);
    }

    #[test]
    fn a_confirmed_advance_publishes_the_anchor() {
        let data = data();
        let (anchor_tx, mut anchor_rx) = watch::channel(None);
        apply(&data, Some(&anchor_tx), &row(1, 100, true, Some("h100")));
        assert_eq!(
            anchor_rx.borrow_and_update().clone(),
            Some(Anchor {
                confirmed_slot: 100,
                confirmed_blockhash: "h100".to_string()
            })
        );

        apply(&data, Some(&anchor_tx), &row(1, 99, true, Some("h99")));
        apply(&data, Some(&anchor_tx), &row(2, 101, true, None));
        assert!(!anchor_rx.has_changed().unwrap());
    }

    #[test]
    #[should_panic(expected = "has no blockhash")]
    fn a_confirmed_advance_without_blockhash_panics_when_the_anchor_is_needed() {
        let (anchor_tx, _anchor_rx) = watch::channel(None);
        apply(&data(), Some(&anchor_tx), &row(1, 100, true, None));
    }

    #[test]
    #[should_panic(expected = "unknown commitment")]
    fn an_unknown_commitment_panics() {
        apply(&data(), None, &row(0, 100, true, None));
    }
}
