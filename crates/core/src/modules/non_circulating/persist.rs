// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The write path: `from_config` and the member-list upsert the API reads for
//! `nonCirculatingAccounts`. The list is written once at the bootstrap flip and
//! then only on a block that changed membership.

use super::{NonCirculatingTracker, StakeBlock};
use crate::IndexConfig;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value, sea_query::ArrayType,
};
use solana_pubkey::Pubkey;
use std::time::Duration;
use tokio::time::timeout;

/// Timeout for the member-list upsert at the bootstrap flip.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(60);

impl NonCirculatingTracker {
    /// Enabled when `[largest-accounts]` or `[supply]` is on, since both
    /// classify accounts. Clears a prior run's member list so a stale list is
    /// never served before the flip.
    pub async fn from_config(db: &DatabaseConnection, config: &IndexConfig) -> Self {
        let largest_accounts_enabled = config
            .largest_accounts
            .as_ref()
            .is_some_and(|section| section.enabled);
        if !largest_accounts_enabled && !config.supply_enabled() {
            return Self::default();
        }
        if config.snapshot.is_none() {
            panic!("non-circulating membership requires the [snapshot] section for its seed");
        }
        if let Err(e) = db
            .execute_unprepared("DELETE FROM non_circulating_accounts WHERE id = 1")
            .await
        {
            tracing::error!(target: "non_circulating", "failed to clear the prior member list: {:?}", e);
        }
        Self::new()
    }

    /// Flips Live and persists the initial member list. No-op when disabled.
    pub async fn finish_bootstrap_and_persist(&self, db: &DatabaseConnection) {
        if self.finish_bootstrap() {
            persist_members(db, self, BOOTSTRAP_TIMEOUT).await;
        }
    }

    /// Re-persists the member list after a block that changed it.
    pub async fn persist_block(
        &self,
        block: &StakeBlock,
        db: &DatabaseConnection,
        config: &IndexConfig,
    ) {
        if block.members_changed {
            let query_timeout = Duration::from_secs(config.database.save_block_queries_timeout);
            persist_members(db, self, query_timeout).await;
        }
    }
}

async fn persist_members(
    db: &DatabaseConnection,
    tracker: &NonCirculatingTracker,
    query_timeout: Duration,
) {
    let (slot, members) = tracker.members();
    let accounts = Value::Array(
        ArrayType::Bytes,
        Some(Box::new(
            members
                .iter()
                .map(|pubkey| Value::Bytes(Some(Box::new(pubkey.to_bytes().to_vec()))))
                .collect(),
        )),
    );
    let upsert = db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "INSERT INTO non_circulating_accounts (id, slot, accounts, updated_at) \
         VALUES (1, $1, $2, now()) \
         ON CONFLICT (id) DO UPDATE SET \
            slot = EXCLUDED.slot, \
            accounts = EXCLUDED.accounts, \
            updated_at = now()",
        [Value::from(slot as i64), accounts],
    ));
    let result = timeout(query_timeout, upsert).await;
    if !matches!(result, Ok(Ok(_))) {
        tracing::error!(
            target: "non_circulating",
            "failed to persist {} non-circulating members at slot {}: {:?}",
            members.len(),
            slot,
            result
        );
    }
}

/// Decodes a persisted member list.
pub(super) fn decode_members(rows: Vec<Vec<u8>>) -> Vec<Pubkey> {
    rows.into_iter()
        .filter_map(|bytes| Pubkey::try_from(bytes.as_slice()).ok())
        .collect()
}
