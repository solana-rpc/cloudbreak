// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The one read path the API shares: the persisted member list. An absent row
//! means the indexer has not flipped Live, and the API serves nothing.

use super::persist::decode_members;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use solana_pubkey::Pubkey;

pub async fn load_members(db: &DatabaseConnection) -> Result<Option<Vec<Pubkey>>, anyhow::Error> {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT accounts FROM non_circulating_accounts WHERE id = 1".to_string(),
        ))
        .await?;
    row.map(|row| Ok(decode_members(row.try_get("", "accounts")?)))
        .transpose()
}
