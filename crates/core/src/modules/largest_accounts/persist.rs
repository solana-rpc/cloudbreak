//! Write path for the largest_accounts table: record upserts, stale/cleared
//! deletes, and the config-driven tracker construction.

use super::{MintRecord, encode_record};
use super::tracker::{BlockOutcome, LargestAccountsTracker, PendingLargestAccount};
use crate::metrics;
use crate::IndexConfig;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr, Statement, TransactionTrait,
};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

pub async fn persist_records(
    db: &DatabaseConnection,
    records: &[MintRecord],
) -> Result<(), DbErr> {
    if records.is_empty() {
        return Ok(());
    }
    let txn = db.begin().await?;
    for record in records {
        txn.execute(Statement::from_string(
            DatabaseBackend::Postgres,
            format!(
                "INSERT INTO largest_accounts (mint, slot, record) \
                 VALUES ('\\x{}'::bytea, {}, '\\x{}'::bytea) \
                 ON CONFLICT (mint, slot) DO UPDATE \
                 SET record = EXCLUDED.record, updated_on = now()",
                hex::encode(record.mint.as_ref()),
                record.slot,
                hex::encode(encode_record(&record.rows)),
            ),
        ))
        .await?;
    }
    txn.commit().await
}

pub async fn delete_mint_rows(db: &DatabaseConnection, mint: &Pubkey) -> Result<(), DbErr> {
    db.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        format!(
            "DELETE FROM largest_accounts WHERE mint = '\\x{}'::bytea",
            hex::encode(mint.as_ref())
        ),
    ))
    .await?;
    Ok(())
}

pub async fn clear_largest_accounts(db: &DatabaseConnection) -> Result<(), DbErr> {
    db.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        "DELETE FROM largest_accounts".to_string(),
    ))
    .await?;
    Ok(())
}

impl LargestAccountsTracker {
    /// Builds the tracker from the two config sections independently:
    /// `[largest-accounts]` enables the SOL/class sentinel tops (and validates
    /// its prerequisites), `[token-largest-accounts]` enables the per-mint token
    /// tops. Clears stale `largest_accounts` rows when either is enabled.
    pub async fn from_config(db: &DatabaseConnection, config: &IndexConfig) -> Self {
        let sol_k = config
            .largest_accounts
            .as_ref()
            .filter(|largest_config| largest_config.enabled)
            .map(|largest_config| {
                if !config.programs.supports_simulation() {
                    panic!("largest-accounts requires an empty [programs] filter");
                }
                if config.snapshot.is_none() {
                    panic!("largest-accounts requires the [snapshot] section");
                }
                if !config.accounts_owner_map_enabled {
                    panic!(
                        "largest-accounts requires accounts-owner-map-enabled (needed for the circulating/non-circulating filter)"
                    );
                }
                if largest_config.accounts_per_mint < super::PERSISTED_TOP_N {
                    panic!(
                        "largest-accounts accounts-per-mint must be at least {}",
                        super::PERSISTED_TOP_N
                    );
                }
                largest_config.accounts_per_mint
            });

        let token = config
            .token_largest_accounts
            .as_ref()
            .filter(|token_config| token_config.enabled)
            .map(|token_config| {
                if token_config.tracked_mints.is_empty() {
                    panic!("token-largest-accounts requires a non-empty tracked-mints");
                }
                if config.snapshot.is_none() {
                    panic!("token-largest-accounts requires the [snapshot] section");
                }
                if token_config.accounts_per_mint < super::PERSISTED_TOP_N {
                    panic!(
                        "token-largest-accounts accounts-per-mint must be at least {}",
                        super::PERSISTED_TOP_N
                    );
                }
                // The mint-metadata lookup and the reservoir read token accounts,
                // so the token programs must be in the indexed set.
                let include = &config.programs.include;
                let exclude = &config.programs.exclude;
                let include_ok = include.is_empty()
                    || (include.iter().any(|p| p.0 == super::TOKEN_PROGRAM_ID)
                        && include.iter().any(|p| p.0 == super::TOKEN_2022_PROGRAM_ID));
                let exclude_ok = !exclude
                    .iter()
                    .any(|p| p.0 == super::TOKEN_PROGRAM_ID || p.0 == super::TOKEN_2022_PROGRAM_ID);
                if !include_ok || !exclude_ok {
                    panic!(
                        "token-largest-accounts requires the token programs to be indexed: \
                         [programs] must be unfiltered, or include both {} and {}, and exclude neither",
                        super::TOKEN_PROGRAM_ID,
                        super::TOKEN_2022_PROGRAM_ID
                    );
                }
                (
                    token_config.tracked_mints.iter().map(|mint| mint.0).collect(),
                    token_config.accounts_per_mint,
                )
            });

        let tracker = Self::new(sol_k, token);

        if tracker.is_enabled() {
            clear_largest_accounts(db)
                .await
                .expect("Failed to clear largest_accounts table");
        }

        tracker
    }

    /// Finish bootstrap and persist the resulting records. Mints that fail to
    /// persist are marked stale. No-op when the tracker is disabled.
    pub async fn finish_bootstrap_and_persist(&self, db: &DatabaseConnection) {
        if !self.is_enabled() {
            return;
        }

        let outcome = self.finish_bootstrap();
        for mint in &outcome.newly_stale {
            tracing::error!(
                target: "largest_accounts",
                "largest accounts bootstrap left mint {} unsound, marking stale",
                mint
            );
        }

        if outcome.records.is_empty() {
            return;
        }

        match persist_records(db, &outcome.records).await {
            Ok(()) => {
                tracing::info!(
                    target: "largest_accounts",
                    "largest accounts bootstrap persisted {} mint record(s)",
                    outcome.records.len()
                );
            }
            Err(e) => {
                tracing::error!(
                    target: "largest_accounts",
                    "failed to persist largest accounts bootstrap records, marking {} mint(s) stale: {:?}",
                    outcome.records.len(),
                    e
                );
                for record in &outcome.records {
                    self.mark_mint_stale(&record.mint);
                }
            }
        }
    }

    /// Applies a block's pending accounts to the tracker and persists the outcome.
    /// No-op when the tracker is disabled.
    pub async fn commit_block(
        &self,
        slot: u64,
        pending: HashMap<Pubkey, PendingLargestAccount>,
        db: &DatabaseConnection,
        config: &IndexConfig,
    ) {
        if !self.is_enabled() {
            return;
        }
        let outcome = self.apply_block(slot, pending);
        persist_largest_outcome(self, outcome, slot, db, config).await;
    }
}

/// Persists a block/recompute outcome: deletes stale/cleared mints, writes new
/// records, and marks mints stale on persistence failure.
pub(crate) async fn persist_largest_outcome(
    largest_accounts: &LargestAccountsTracker,
    outcome: BlockOutcome,
    slot: u64,
    db: &DatabaseConnection,
    config: &IndexConfig,
) {
    let query_timeout = Duration::from_secs(config.database.save_block_queries_timeout);

    for mint in &outcome.newly_stale {
        tracing::error!(
            target: "largest_accounts",
            "largest accounts reservoir unsound for mint {} at slot {}, marking stale",
            mint,
            slot
        );
    }

    for mint in outcome.newly_stale.iter().chain(outcome.cleared.iter()) {
        let delete = timeout(query_timeout, delete_mint_rows(db, mint)).await;
        if !matches!(delete, Ok(Ok(()))) {
            metrics::LARGEST_ACCOUNTS_DB_ERRORS.inc();
            tracing::error!(
                target: "largest_accounts",
                "failed to delete largest_accounts rows for mint {}: {:?}",
                mint,
                delete
            );
        }
    }

    if !outcome.records.is_empty() {
        let persist = timeout(query_timeout, persist_records(db, &outcome.records)).await;
        if !matches!(persist, Ok(Ok(()))) {
            metrics::LARGEST_ACCOUNTS_DB_ERRORS.inc();
            tracing::error!(
                target: "largest_accounts",
                "failed to persist largest_accounts records for slot {}, marking {} mint(s) stale: {:?}",
                slot,
                outcome.records.len(),
                persist
            );
            for record in &outcome.records {
                largest_accounts.mark_mint_stale(&record.mint);
                let delete = timeout(query_timeout, delete_mint_rows(db, &record.mint)).await;
                if !matches!(delete, Ok(Ok(()))) {
                    metrics::LARGEST_ACCOUNTS_DB_ERRORS.inc();
                }
            }
        }
    }

    metrics::LARGEST_ACCOUNTS_STALE_MINTS.set(largest_accounts.stale_count() as i64);
}
