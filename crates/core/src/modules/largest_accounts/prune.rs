//! Finalize-time pruning of redundant largest_accounts generations.

use crate::IndexConfig;
use crate::metrics;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::timeout;

/// Deletes redundant `largest_accounts` generations older than the newest one at
/// or below the finalized `slot`, per mint. The newest generation is always kept.
pub async fn prune_largest_accounts(db: &DatabaseConnection, slot: u64, config: &IndexConfig) {
    let query_timeout = Duration::from_secs(config.database.finalize_slot_queries_timeout);

    let query = db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "DELETE FROM largest_accounts la USING ( \
            SELECT mint, MAX(slot) AS keep_slot FROM largest_accounts \
            WHERE slot <= $1 GROUP BY mint \
         ) keep WHERE la.mint = keep.mint AND la.slot < keep.keep_slot",
        [Value::BigInt(Some(slot as i64))],
    ));

    let result = timeout(query_timeout, query).await.unwrap_or_else(|elapsed| {
        tracing::error!("prune_largest_accounts timeout ERROR: {}", elapsed);
        Err(sea_orm::DbErr::RecordNotInserted)
    });

    if let Err(e) = result {
        metrics::LARGEST_ACCOUNTS_DB_ERRORS.inc();
        tracing::error!(
            "prune_largest_accounts: failed to prune for slot {}: {}",
            slot,
            e
        );
    }
}

/// Long-lived task that prunes stale `largest_accounts` generations at most once every
/// `prune-interval-slots` finalized slots, off the finalization critical path.
pub fn spawn_largest_accounts_pruner(
    db: DatabaseConnection,
    config: IndexConfig,
    mut finalized_slot_rx: watch::Receiver<u64>,
) {
    let Some(prune_interval_slots) = config.largest_accounts_prune_interval_slots() else {
        return;
    };

    tokio::spawn(async move {
        let _guard = metrics::TokioTaskCounterGuard::new("largest_accounts_pruner");

        let mut last_pruned_slot = 0u64;
        while finalized_slot_rx.changed().await.is_ok() {
            let finalized_slot = *finalized_slot_rx.borrow_and_update();
            if finalized_slot.saturating_sub(last_pruned_slot) >= prune_interval_slots {
                prune_largest_accounts(&db, finalized_slot, &config).await;
                last_pruned_slot = finalized_slot;
            }
        }
    });
}
