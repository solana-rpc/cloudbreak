// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The `mints` table: row encoding, the reads, the guarded upsert and `last_db_slot`. Every
//! query runs through `sqlx` with binds, so bytea columns arrive in binary format.

use std::time::Duration;

use sea_orm::sqlx::{self, PgPool, Row, postgres::PgRow};
use solana_pubkey::Pubkey;
use spl_token_2022_interface::extension::interest_bearing_mint::InterestBearingConfig;
use spl_token_2022_interface::extension::scaled_ui_amount::ScaledUiAmountConfig;

use crate::utils::token::{MintConfigs, MintState};

/// Rows per upsert statement. Each column is one array bind, so the bind count stays at 7.
pub(super) const UPSERT_CHUNK: usize = 50_000;
/// Rows per page of the API startup load: each page is one short query.
const LOAD_PAGE: i64 = 100_000;

/// One `mints` row. `state` is `None` for a tombstone.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MintRow {
    pub pubkey: Pubkey,
    pub slot: u64,
    pub state: Option<MintState>,
}

impl MintRow {
    /// A tombstone counts as mutable: only a mutable mint can close.
    fn mutable(&self) -> bool {
        self.state.as_ref().is_none_or(|state| !state.immutable)
    }

    fn configs(&self) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let configs = self
            .state
            .as_ref()
            .and_then(|state| state.configs.as_deref());
        (
            configs
                .and_then(|c| c.interest_bearing.as_ref())
                .map(|c| bytemuck::bytes_of(c).to_vec()),
            configs
                .and_then(|c| c.scaled_ui_amount.as_ref())
                .map(|c| bytemuck::bytes_of(c).to_vec()),
        )
    }

    /// The rows changed after `slot`: the API reload after a gap.
    pub async fn read_after(pool: &PgPool, slot: u64) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query(
            "SELECT pubkey, slot, decimals, mutable, closed, interest_bearing_config, \
             scaled_ui_amount_config FROM mints WHERE slot > $1",
        )
        .bind(slot as i64)
        .fetch_all(pool)
        .await?
        .iter()
        .map(Self::from_pg_row)
        .collect()
    }

    /// The next page of rows after `pubkey`, in key order: the API startup load reads the whole
    /// table page by page. An empty page is the end.
    pub async fn read_page(pool: &PgPool, after: &Pubkey) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query(
            "SELECT pubkey, slot, decimals, mutable, closed, interest_bearing_config, \
             scaled_ui_amount_config FROM mints WHERE pubkey > $1 ORDER BY pubkey LIMIT $2",
        )
        .bind(after.to_bytes().to_vec())
        .bind(LOAD_PAGE)
        .fetch_all(pool)
        .await?
        .iter()
        .map(Self::from_pg_row)
        .collect()
    }

    fn from_pg_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let pubkey = Pubkey::new_from_array(row.try_get::<[u8; 32], _>("pubkey")?);
        let slot = row.try_get::<i64, _>("slot")? as u64;
        let decimals: Option<i16> = row.try_get("decimals")?;
        let closed: bool = row.try_get("closed")?;
        let state = match decimals {
            Some(decimals) if !closed => {
                let interest_bearing = row
                    .try_get::<Option<Vec<u8>>, _>("interest_bearing_config")?
                    .and_then(|b| {
                        bytemuck::try_pod_read_unaligned::<InterestBearingConfig>(&b).ok()
                    });
                let scaled_ui_amount = row
                    .try_get::<Option<Vec<u8>>, _>("scaled_ui_amount_config")?
                    .and_then(|b| {
                        bytemuck::try_pod_read_unaligned::<ScaledUiAmountConfig>(&b).ok()
                    });
                let configs =
                    (interest_bearing.is_some() || scaled_ui_amount.is_some()).then(|| {
                        Box::new(MintConfigs {
                            interest_bearing,
                            scaled_ui_amount,
                        })
                    });
                Some(MintState {
                    decimals: decimals as u8,
                    configs,
                    immutable: !row.try_get::<bool, _>("mutable")?,
                })
            }
            _ => None,
        };
        Ok(Self {
            pubkey,
            slot,
            state,
        })
    }
}

/// The `mints` columns of some rows, one array per column, in the upsert's bind order.
#[derive(Default)]
struct Columns {
    pubkeys: Vec<Vec<u8>>,
    slots: Vec<i64>,
    decimals: Vec<Option<i16>>,
    mutable: Vec<bool>,
    closed: Vec<bool>,
    interest_bearing: Vec<Option<Vec<u8>>>,
    scaled_ui_amount: Vec<Option<Vec<u8>>>,
}

impl Columns {
    fn of(rows: &[MintRow]) -> Self {
        let mut columns = Self::default();
        for row in rows {
            let (interest_bearing, scaled_ui_amount) = row.configs();
            columns.pubkeys.push(row.pubkey.to_bytes().to_vec());
            columns.slots.push(row.slot as i64);
            columns
                .decimals
                .push(row.state.as_ref().map(|s| i16::from(s.decimals)));
            columns.mutable.push(row.mutable());
            columns.closed.push(row.state.is_none());
            columns.interest_bearing.push(interest_bearing);
            columns.scaled_ui_amount.push(scaled_ui_amount);
        }
        columns
    }
}

/// Upserts `rows`, one per pubkey, in chunks. A row never overwrites a newer one. `timeout`
/// bounds each chunk.
pub(crate) async fn upsert_rows(
    pool: &PgPool,
    rows: &[MintRow],
    timeout: Option<Duration>,
) -> anyhow::Result<()> {
    for chunk in rows.chunks(UPSERT_CHUNK) {
        let columns = Columns::of(chunk);
        let query = sqlx::query(
            "INSERT INTO mints (pubkey, slot, decimals, mutable, closed, \
             interest_bearing_config, scaled_ui_amount_config) \
             SELECT * FROM unnest($1::bytea[], $2::int8[], $3::int2[], $4::bool[], $5::bool[], \
             $6::bytea[], $7::bytea[]) \
             ON CONFLICT (pubkey) DO UPDATE SET slot = EXCLUDED.slot, decimals = EXCLUDED.decimals, \
             mutable = EXCLUDED.mutable, closed = EXCLUDED.closed, \
             interest_bearing_config = EXCLUDED.interest_bearing_config, \
             scaled_ui_amount_config = EXCLUDED.scaled_ui_amount_config \
             WHERE mints.slot <= EXCLUDED.slot",
        )
            .bind(columns.pubkeys)
            .bind(columns.slots)
            .bind(columns.decimals)
            .bind(columns.mutable)
            .bind(columns.closed)
            .bind(columns.interest_bearing)
            .bind(columns.scaled_ui_amount)
            .execute(pool);
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, query).await??,
            None => query.await?,
        };
    }
    Ok(())
}

/// The slot `mints` is complete through, or `None` before the indexer bootstrap wrote it.
pub(crate) async fn read_last_db_slot(pool: &PgPool) -> Result<Option<u64>, sqlx::Error> {
    let slot: Option<Option<i64>> =
        sqlx::query_scalar("SELECT mints_saved_slot FROM environment_info WHERE id = 1")
            .fetch_optional(pool)
            .await?;
    Ok(slot.flatten().map(|slot| slot as u64))
}

pub(crate) async fn write_last_db_slot(pool: &PgPool, slot: u64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO environment_info (id, mints_saved_slot) VALUES (1, $1) \
         ON CONFLICT (id) DO UPDATE SET mints_saved_slot = EXCLUDED.mints_saved_slot",
    )
    .bind(slot as i64)
    .execute(pool)
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_encode_live_rows_and_tombstones() {
        let pubkey = Pubkey::new_from_array([1; 32]);
        let live = MintRow {
            pubkey,
            slot: 7,
            state: Some(MintState {
                decimals: 6,
                configs: None,
                immutable: true,
            }),
        };
        let tombstone = MintRow {
            pubkey,
            slot: 8,
            state: None,
        };
        let columns = Columns::of(&[live, tombstone]);
        assert_eq!(columns.slots, vec![7, 8]);
        assert_eq!(columns.decimals, vec![Some(6), None]);
        // A tombstone counts as mutable: only a mutable mint can close.
        assert_eq!(columns.mutable, vec![false, true]);
        assert_eq!(columns.closed, vec![false, true]);
        assert_eq!(columns.interest_bearing, vec![None, None]);
    }
}
