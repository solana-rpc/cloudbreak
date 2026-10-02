// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Keeps the confirmed and finalized slots and the service health in memory. Postgres pushes
//! every real change of a `slots` row on [`SLOTS_CHANNEL`]. A read of `slots` every
//! `interval_ms` is the safety net and counts the notifications it finds missing. With
//! `[processed-accounts]` enabled it also publishes the processed [`Anchor`].

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use cloudbreak_core::ApiConfig;
use cloudbreak_core::modules::processed::Anchor;
use cloudbreak_core::modules::slot_lag::{SlotCommitment, SlotLag, SlotSource};
use sea_orm::DatabaseConnection;
use sea_orm::sqlx::postgres::PgListener;
use serde::Deserialize;
use solana_commitment_config::CommitmentLevel;
use tokio::{sync::watch, task::JoinHandle};

use crate::{db_query, metrics};

/// The channel the `slots_notify` trigger sends on.
pub const SLOTS_CHANNEL: &str = "cloudbreak_slots";
/// `slots.commitment` values, as the indexer writes them.
const COMMITMENT_CONFIRMED: i32 = 1;
const COMMITMENT_FINALIZED: i32 = 2;
const LISTENER_RETRY_BACKOFF: Duration = Duration::from_secs(1);
/// Slots move every ~400 ms, so this much silence means the listener connection is dead.
const LISTENER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a slot the poll found waits for its notification before it counts as missed.
const NOTIFICATION_GRACE: Duration = Duration::from_secs(2);
/// Recently notified slots kept per commitment for the missed-notification check.
const NOTIFIED_SLOTS_KEPT: usize = 1024;
/// Floor for the blockhash read timeout when the poll interval is very short.
const MIN_BLOCKHASH_READ_TIMEOUT: Duration = Duration::from_millis(100);

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

/// One `slots` row, from a notification payload or a read.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SlotRow {
    pub commitment: i32,
    pub slot: i64,
    pub block_time: i64,
    pub health: bool,
    pub blockhash: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Notify,
    Poll,
    /// The read after the listener (re)connects. Notifications sent before it were never coming.
    Resync,
}

impl Source {
    fn label(self) -> &'static str {
        match self {
            Self::Notify => "notify",
            Self::Poll => "poll",
            Self::Resync => "resync",
        }
    }
}

/// A confirmed slot that moved forward, for the processed anchor.
#[derive(Debug, PartialEq, Eq)]
struct ConfirmedAdvance {
    slot: u64,
    blockhash: Option<String>,
}

/// State shared by the listener and the poll. Never held across an await.
struct Syncer {
    data: Arc<RwLock<SlotSyncronizerData>>,
    /// Recently notified slots, indexed like [`commitment_index`].
    notified: [VecDeque<u64>; 2],
    /// Slots the poll applied before their notification: (commitment index, slot, deadline).
    pending_checks: Vec<(usize, u64, Instant)>,
    /// Notifications applied so far. The poll applies health only when this did not move.
    notifications: u64,
    slot_lag: Option<SlotLag>,
}

fn commitment_index(commitment: i32) -> Option<usize> {
    match commitment {
        COMMITMENT_CONFIRMED => Some(0),
        COMMITMENT_FINALIZED => Some(1),
        _ => None,
    }
}

fn commitment_label(index: usize) -> &'static str {
    ["confirmed", "finalized"][index]
}

impl Syncer {
    fn new(data: Arc<RwLock<SlotSyncronizerData>>, slot_lag: Option<SlotLag>) -> Self {
        Self {
            data,
            notified: Default::default(),
            pending_checks: Vec::new(),
            notifications: 0,
            slot_lag,
        }
    }

    /// Applies one row. Slots never move backwards. Returns the confirmed advance, if any.
    fn apply(
        &mut self,
        row: &SlotRow,
        source: Source,
        apply_health: bool,
        now: Instant,
    ) -> Option<ConfirmedAdvance> {
        let index = commitment_index(row.commitment)?;
        let slot = row.slot as u64;

        if source == Source::Notify {
            self.notifications += 1;
            let notified = &mut self.notified[index];
            notified.push_back(slot);
            if notified.len() > NOTIFIED_SLOTS_KEPT {
                notified.pop_front();
            }
            if let Some(slot_lag) = &self.slot_lag {
                let commitment = [SlotCommitment::Confirmed, SlotCommitment::Finalized][index];
                slot_lag.record(SlotSource::Postgres, commitment, slot, now);
            }
        }

        let mut data = self.data.write().expect("Failed to lock slot data");
        if apply_health {
            data.healthy = row.health;
        }
        let current = match index {
            0 => &mut data.confirmed_slot,
            _ => &mut data.finalized_slot,
        };
        if slot <= current.slot {
            return None;
        }
        *current = SlotData {
            slot,
            block_time: row.block_time,
        };
        drop(data);

        metrics::CLOUDBREAK_API_SLOT_SYNC_UPDATES_TOTAL
            .with_label_values(&[commitment_label(index), source.label()])
            .inc();
        if source == Source::Poll {
            self.pending_checks
                .push((index, slot, now + NOTIFICATION_GRACE));
        }
        (index == 0).then(|| ConfirmedAdvance {
            slot,
            blockhash: row.blockhash.clone(),
        })
    }

    /// Counts the poll-found slots whose notification has not arrived within the grace period.
    fn check_missed(&mut self, now: Instant) -> Vec<(usize, u64)> {
        let mut missed = Vec::new();
        self.pending_checks.retain(|&(index, slot, deadline)| {
            if now < deadline {
                return true;
            }
            if !self.notified[index].contains(&slot) {
                missed.push((index, slot));
            }
            false
        });
        missed
    }
}

/// Starts the slot syncronizer, or returns `None` when `[slot-syncronizer]` is disabled.
/// With `slot_lag` set, every notification is reported to it.
pub fn start_slot_syncronizer(
    db: DatabaseConnection,
    config: &ApiConfig,
    anchor_tx: watch::Sender<Option<Anchor>>,
    slot_lag: Option<SlotLag>,
) -> Option<(JoinHandle<()>, Arc<RwLock<SlotSyncronizerData>>)> {
    if !config.slot_syncronizer.enabled {
        return None;
    }

    let slot_syncronizer_data = Arc::new(RwLock::new(SlotSyncronizerData::default()));
    let syncer = Arc::new(Mutex::new(Syncer::new(
        slot_syncronizer_data.clone(),
        slot_lag,
    )));
    let anchor = Arc::new(AnchorPublisher {
        db: db.clone(),
        anchor_tx: config.processed_accounts_enabled().then_some(anchor_tx),
        read_timeout: Duration::from_millis(config.slot_syncronizer.interval_ms)
            .max(MIN_BLOCKHASH_READ_TIMEOUT),
    });
    let interval = Duration::from_millis(config.slot_syncronizer.interval_ms);

    let listener = tokio::spawn(run_listener(db.clone(), syncer.clone(), anchor.clone()));
    let poll = tokio::spawn(run_poll(db, syncer, anchor, interval));
    let join_handle = tokio::spawn(async move {
        tokio::select! {
            result = listener => tracing::error!(target: "slot_syncronizer", "Slot listener stopped: {result:?}"),
            result = poll => tracing::error!(target: "slot_syncronizer", "Slot poll stopped: {result:?}"),
        }
    });

    Some((join_handle, slot_syncronizer_data))
}

/// Listens on [`SLOTS_CHANNEL`]. Reads `slots` after every (re)connect, then applies each
/// notification. Reconnects when the connection drops or stays silent too long.
async fn run_listener(
    db: DatabaseConnection,
    syncer: Arc<Mutex<Syncer>>,
    anchor: Arc<AnchorPublisher>,
) {
    loop {
        let mut listener = match PgListener::connect_with(db.get_postgres_connection_pool()).await {
            Ok(listener) => listener,
            Err(e) => {
                tracing::warn!(target: "slot_syncronizer", "Slot listener connect failed: {e}");
                tokio::time::sleep(LISTENER_RETRY_BACKOFF).await;
                continue;
            }
        };
        if let Err(e) = listener.listen(SLOTS_CHANNEL).await {
            tracing::warn!(target: "slot_syncronizer", "LISTEN {SLOTS_CHANNEL} failed: {e}");
            tokio::time::sleep(LISTENER_RETRY_BACKOFF).await;
            continue;
        }
        tracing::info!(target: "slot_syncronizer", "Listening on {SLOTS_CHANNEL}");
        read_and_apply(&db, &syncer, &anchor, Source::Resync).await;

        loop {
            match tokio::time::timeout(LISTENER_IDLE_TIMEOUT, listener.try_recv()).await {
                Ok(Ok(Some(notification))) => {
                    let row = match serde_json::from_str::<SlotRow>(notification.payload()) {
                        Ok(row) => row,
                        Err(e) => {
                            tracing::error!(target: "slot_syncronizer", "Bad {SLOTS_CHANNEL} payload {:?}: {e}", notification.payload());
                            continue;
                        }
                    };
                    let advance = lock(&syncer).apply(&row, Source::Notify, true, Instant::now());
                    if let Some(advance) = advance {
                        anchor.publish(advance).await;
                    }
                }
                // The listener already reconnected and listens again; notifications in between are lost.
                Ok(Ok(None)) => {
                    metrics::CLOUDBREAK_API_SLOT_SYNC_LISTENER_RECONNECTS_TOTAL.inc();
                    tracing::warn!(target: "slot_syncronizer", "Slot listener connection lost, reconnected");
                    read_and_apply(&db, &syncer, &anchor, Source::Resync).await;
                }
                Ok(Err(e)) => {
                    metrics::CLOUDBREAK_API_SLOT_SYNC_LISTENER_RECONNECTS_TOTAL.inc();
                    tracing::warn!(target: "slot_syncronizer", "Slot listener error, reconnecting: {e}");
                    tokio::time::sleep(LISTENER_RETRY_BACKOFF).await;
                    break;
                }
                Err(_elapsed) => {
                    metrics::CLOUDBREAK_API_SLOT_SYNC_LISTENER_RECONNECTS_TOTAL.inc();
                    tracing::warn!(target: "slot_syncronizer", "No slot notification for {LISTENER_IDLE_TIMEOUT:?}, reconnecting");
                    break;
                }
            }
        }
    }
}

/// Reads `slots` every `interval`, applies what the notifications missed and counts it.
async fn run_poll(
    db: DatabaseConnection,
    syncer: Arc<Mutex<Syncer>>,
    anchor: Arc<AnchorPublisher>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        read_and_apply(&db, &syncer, &anchor, Source::Poll).await;
        for (index, slot) in lock(&syncer).check_missed(Instant::now()) {
            metrics::CLOUDBREAK_API_SLOT_SYNC_MISSED_NOTIFICATIONS_TOTAL
                .with_label_values(&[commitment_label(index)])
                .inc();
            tracing::warn!(target: "slot_syncronizer", "No notification for {} slot {slot} within {NOTIFICATION_GRACE:?} of the poll finding it", commitment_label(index));
        }
    }
}

/// Reads every `slots` row and applies it. Health applies only if no notification arrived
/// during the read, so a slow read never overwrites a newer health value.
async fn read_and_apply(
    db: &DatabaseConnection,
    syncer: &Mutex<Syncer>,
    anchor: &AnchorPublisher,
    source: Source,
) {
    let notifications_before = lock(syncer).notifications;
    let rows = match db_query::get_slot_rows(db).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(target: "slot_syncronizer", "Slots read failed: {e}");
            return;
        }
    };
    let advance = {
        let mut syncer = lock(syncer);
        let apply_health = syncer.notifications == notifications_before;
        let now = Instant::now();
        let mut advance = None;
        for row in &rows {
            advance = syncer.apply(row, source, apply_health, now).or(advance);
        }
        advance
    };
    if let Some(advance) = advance {
        anchor.publish(advance).await;
    }
}

fn lock(syncer: &Mutex<Syncer>) -> std::sync::MutexGuard<'_, Syncer> {
    syncer.lock().unwrap_or_else(|e| e.into_inner())
}

/// Publishes the processed anchor. A row without a blockhash (indexer not migrated yet)
/// falls back to `recent_blockhashes`.
struct AnchorPublisher {
    db: DatabaseConnection,
    anchor_tx: Option<watch::Sender<Option<Anchor>>>,
    read_timeout: Duration,
}

impl AnchorPublisher {
    async fn publish(&self, advance: ConfirmedAdvance) {
        let Some(anchor_tx) = &self.anchor_tx else {
            return;
        };
        let blockhash = match advance.blockhash {
            Some(blockhash) => Some(blockhash),
            None => self.read_blockhash(advance.slot).await,
        };
        if let Some(confirmed_blockhash) = blockhash {
            publish_anchor(
                anchor_tx,
                Anchor {
                    confirmed_slot: advance.slot,
                    confirmed_blockhash,
                },
            );
        }
    }

    /// Reads the blockhash of `slot` from `recent_blockhashes`, bounded by `read_timeout`.
    async fn read_blockhash(&self, slot: u64) -> Option<String> {
        match tokio::time::timeout(
            self.read_timeout,
            db_query::get_blockhash_at_slot(&self.db, slot),
        )
        .await
        {
            Ok(Ok(blockhash)) => blockhash,
            Ok(Err(e)) => {
                tracing::warn!(target: "slot_syncronizer", "Confirmed blockhash read failed for slot {slot}: {e}");
                None
            }
            Err(_elapsed) => {
                tracing::warn!(target: "slot_syncronizer", "Confirmed blockhash read for slot {slot} timed out after {:?}", self.read_timeout);
                None
            }
        }
    }
}

/// Publishes the anchor when it differs from the last one published, and never moves it back.
fn publish_anchor(anchor_tx: &watch::Sender<Option<Anchor>>, anchor: Anchor) {
    anchor_tx.send_if_modified(|current| {
        if current
            .as_ref()
            .is_some_and(|current| current.confirmed_slot >= anchor.confirmed_slot)
        {
            return false;
        }
        *current = Some(anchor);
        true
    });
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
        }
    }

    fn syncer() -> Syncer {
        Syncer::new(Arc::new(RwLock::new(SlotSyncronizerData::default())), None)
    }

    #[test]
    fn payload_parses_with_and_without_blockhash() {
        let row: SlotRow = serde_json::from_str(
            r#"{"commitment":1,"slot":100,"block_time":5,"health":true,"blockhash":"abc"}"#,
        )
        .unwrap();
        assert_eq!(row, self::row(1, 100, true, Some("abc")).with_block_time(5));
        let row: SlotRow = serde_json::from_str(
            r#"{"commitment":2,"slot":90,"block_time":5,"health":false,"blockhash":null}"#,
        )
        .unwrap();
        assert_eq!(row.blockhash, None);
    }

    impl SlotRow {
        fn with_block_time(mut self, block_time: i64) -> Self {
            self.block_time = block_time;
            self
        }
    }

    #[test]
    fn slots_only_move_forward_and_confirmed_advances_carry_the_blockhash() {
        let mut syncer = syncer();
        let now = Instant::now();
        let advance = syncer.apply(&row(1, 100, true, Some("h100")), Source::Notify, true, now);
        assert_eq!(
            advance,
            Some(ConfirmedAdvance {
                slot: 100,
                blockhash: Some("h100".to_string())
            })
        );
        assert_eq!(
            syncer.apply(&row(1, 99, true, Some("h99")), Source::Notify, true, now),
            None
        );
        assert_eq!(
            syncer.apply(&row(2, 80, true, None), Source::Notify, true, now),
            None
        );
        let data = syncer.data.read().unwrap().clone();
        assert_eq!(data.confirmed_slot.slot, 100);
        assert_eq!(data.confirmed_slot.block_time, 1000);
        assert_eq!(data.finalized_slot.slot, 80);
        assert!(data.healthy);
    }

    #[test]
    fn unknown_commitment_is_ignored() {
        let mut syncer = syncer();
        assert_eq!(
            syncer.apply(
                &row(0, 100, true, None),
                Source::Notify,
                true,
                Instant::now()
            ),
            None
        );
        assert_eq!(syncer.notifications, 0);
    }

    #[test]
    fn health_from_a_read_applies_only_when_asked() {
        let mut syncer = syncer();
        let now = Instant::now();
        syncer.apply(&row(1, 100, true, None), Source::Notify, true, now);
        syncer.apply(&row(1, 100, false, None), Source::Poll, false, now);
        assert!(syncer.data.read().unwrap().healthy);
        syncer.apply(&row(1, 100, false, None), Source::Poll, true, now);
        assert!(!syncer.data.read().unwrap().healthy);
    }

    #[test]
    fn a_poll_found_slot_counts_as_missed_only_without_its_notification() {
        let mut syncer = syncer();
        let now = Instant::now();
        syncer.apply(&row(1, 100, true, None), Source::Poll, true, now);
        syncer.apply(&row(1, 101, true, None), Source::Poll, true, now);
        // 100 is notified late, inside the grace period. 101 never is.
        syncer.apply(&row(1, 100, true, None), Source::Notify, true, now);
        assert!(syncer.check_missed(now).is_empty());
        assert_eq!(
            syncer.check_missed(now + NOTIFICATION_GRACE),
            vec![(0, 101)]
        );
        assert!(syncer.pending_checks.is_empty());
    }

    #[test]
    fn a_resync_never_counts_as_missed() {
        let mut syncer = syncer();
        let now = Instant::now();
        syncer.apply(&row(1, 100, true, None), Source::Resync, true, now);
        assert!(syncer.check_missed(now + NOTIFICATION_GRACE).is_empty());
    }

    #[test]
    fn anchor_is_published_once_per_change_and_never_moves_back() {
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
                confirmed_slot: 99,
                confirmed_blockhash: "h99".to_string(),
            },
        );
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
