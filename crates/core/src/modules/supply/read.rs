// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The one read path shared by the API. The indexer writes the ring and the
//! member list. The API only reads them through [`load_latest_supply`]. No
//! feature-enablement state lives in the DB: an absent ring means not served.

use crate::modules::non_circulating::read::load_members;
use crate::modules::supply::tracker::SUPPLY_RING_SLOTS;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use solana_pubkey::Pubkey;

/// The ring rows and the non-circulating member list, as the API serves them.
#[derive(Debug, Clone, Default)]
pub struct SupplySnapshot {
    pub rows: Vec<SupplyRow>,
    pub non_circulating_accounts: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
pub struct SupplyRow {
    pub slot: u64,
    pub total: u64,
    pub non_circulating: Option<u64>,
}

fn decimal_to_u64(value: Decimal, column: &str) -> Result<u64, anyhow::Error> {
    value
        .to_u64()
        .ok_or_else(|| anyhow::anyhow!("supply.{} {} does not fit in u64", column, value))
}

/// Reads the retained ring rows (oldest first) and the member list. Returns
/// `None` while the ring is empty, so the API degrades to node-unhealthy rather
/// than serving a partial total.
pub async fn load_latest_supply(
    db: &DatabaseConnection,
) -> Result<Option<SupplySnapshot>, anyhow::Error> {
    let supply_rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT slot, total, non_circulating_lamports FROM supply ORDER BY slot DESC LIMIT $1",
            [(SUPPLY_RING_SLOTS as i64).into()],
        ))
        .await?;
    if supply_rows.is_empty() {
        return Ok(None);
    }
    let mut rows = Vec::with_capacity(supply_rows.len());
    for row in supply_rows {
        let slot: i64 = row.try_get("", "slot")?;
        let total = decimal_to_u64(row.try_get("", "total")?, "total")?;
        let non_circulating: Option<Decimal> = row.try_get("", "non_circulating_lamports")?;
        let non_circulating = non_circulating
            .map(|lamports| decimal_to_u64(lamports, "non_circulating_lamports"))
            .transpose()?;
        rows.push(SupplyRow {
            slot: slot as u64,
            total,
            non_circulating,
        });
    }
    rows.reverse();

    let non_circulating_accounts = load_members(db)
        .await?
        .map(|members| members.iter().map(Pubkey::to_string).collect());

    Ok(Some(SupplySnapshot {
        rows,
        non_circulating_accounts,
    }))
}
