// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Processed commitment for getAccountInfo, getMultipleAccounts, getBalance,
//! getTokenAccountBalance, getTokenSupply and getSlot. [`ProcessedBlocks`] and its
//! account rules are documented in `cloudbreak_core::modules::processed`.
//!
//! [`read`] gives each request a [`Read`]. A processed request
//! carries the latest [`ProcessedBlocks`] while the node is healthy, the head is
//! not below the cached confirmed slot and the cached finalized slot is not above
//! the anchor. Otherwise it reads as `Confirmed`, even with
//! `processed-commitment = "reject"`. Every other request resolves through
//! [`resolve_commitment`].
//!
//! [`read_accounts`] serves getAccountInfo, getMultipleAccounts,
//! getTokenAccountBalance and getTokenSupply at every commitment with the
//! method's own SQL. With processed blocks, keys written in them answer from
//! memory and only the unknown keys go to that SQL, bounded at the anchor. The
//! context slot and block time come from the head block. A jsonParsed token
//! account resolves its mint through the blocks first:
//!
//! | Mint in the blocks | Account from the blocks | Account from Postgres |
//! |---|---|---|
//! | `Live` | mint data from the blocks | mint data from the blocks, overriding the joined mint |
//! | `Closed` | no mint data | no mint data |
//! | `Unknown` | one `getMultipleAccounts.sql` read at the anchor | the joined mint |
//!
//! getBalance keeps `getBalance.sql` and looks its key up in the blocks itself,
//! with the same anchor bound for an unknown key. getSlot answers the head slot
//! from the blocks, and otherwise reads the `slots` row at the commitment.
//!
//! The in-memory lookup runs in a `processed_read` span with the store size in
//! `stored_blocks` and `stored_bytes`.

use std::collections::HashMap;
use std::sync::Arc;

use cloudbreak_core::modules::processed::{LiveAccount, ProcessedAccount, ProcessedBlocks};
use rust_decimal::prelude::ToPrimitive;
use sea_orm::sqlx::{self, Row, postgres::PgRow};
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_pubkey::Pubkey;
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::{is_token_program, resolve_commitment};
use crate::slot_syncronizer::SlotSyncronizerData;
use crate::{db_query, metrics};

/// How one request reads: at a commitment, or through processed blocks.
pub(crate) struct Read {
    pub(crate) commitment: CommitmentLevel,
    /// Set for a processed request the blocks can serve. `commitment` is then `Confirmed`.
    pub(crate) blocks: Option<Arc<ProcessedBlocks>>,
}

impl Read {
    /// The context slot and block time: the head block, or the cached slot at the commitment.
    pub(crate) async fn slot_and_block_time(
        &self,
        state: &CloudbreakRpcState,
    ) -> Result<(u64, i64), RpcError> {
        match &self.blocks {
            Some(blocks) => Ok((blocks.slot, blocks.block_time)),
            None => state.latest_slot_and_block_time(self.commitment).await,
        }
    }

    /// The Postgres slot bound: the blocks anchor, or the request slot.
    fn sql_slot_bound(&self, latest_slot: u64) -> u64 {
        self.blocks
            .as_ref()
            .map_or(latest_slot, |blocks| blocks.anchor_slot)
    }
}

/// Resolves the request commitment. `method` is the metric label.
pub(crate) fn read(
    state: &CloudbreakRpcState,
    commitment: Option<CommitmentConfig>,
    method: &str,
) -> Result<Read, RpcError> {
    let commitment = commitment.map(|config| config.commitment);
    if commitment != Some(CommitmentLevel::Processed) || !state.processed.is_enabled() {
        let commitment = commitment
            .map(|commitment| resolve_commitment(commitment, state.processed_commitment))
            .transpose()?
            .unwrap_or(CommitmentLevel::Finalized);
        return Ok(Read {
            commitment,
            blocks: None,
        });
    }
    // Blocks first, so the cached slots are at least as new as the blocks.
    let blocks = state.processed.blocks();
    let slots = state
        .slot_syncronizer_data
        .as_ref()
        .map_or_else(Default::default, |data| {
            data.read()
                .expect("Failed to read slot syncronizer data")
                .clone()
        });
    let reason = match &blocks {
        Some(blocks) => fallback_reason(blocks.slot, blocks.anchor_slot, &slots),
        None => Some("no_blocks"),
    };
    let blocks = match reason {
        None => {
            count_route(method, "view", "none");
            blocks
        }
        Some(reason) => {
            count_route(method, "degraded", reason);
            None
        }
    };
    Ok(Read {
        commitment: CommitmentLevel::Confirmed,
        blocks,
    })
}

/// Why blocks with this head and anchor take the confirmed path.
fn fallback_reason(
    head: u64,
    anchor_slot: u64,
    slots: &SlotSyncronizerData,
) -> Option<&'static str> {
    if !slots.healthy {
        Some("unhealthy")
    } else if head < slots.confirmed_slot.slot {
        Some("head_behind")
    } else if slots.finalized_slot.slot > anchor_slot {
        Some("finalized_above_anchor")
    } else {
        None
    }
}

fn count_route(method: &str, route: &str, reason: &str) {
    metrics::CLOUDBREAK_API_PROCESSED_REQUESTS_TOTAL
        .with_label_values(&[method, route, reason])
        .inc();
}

/// One live account, from the blocks or a Postgres row.
#[derive(Debug, Clone)]
pub(crate) struct Account {
    pub(crate) lamports: u64,
    pub(crate) owner: Pubkey,
    pub(crate) executable: bool,
    pub(crate) rent_epoch: u64,
    pub(crate) data: Arc<Vec<u8>>,
    /// The joined `mint_data` column, or the mint resolved through the blocks.
    mint_data: Option<Arc<Vec<u8>>>,
}

impl Account {
    fn from_live(account: &LiveAccount) -> Self {
        Self {
            lamports: account.lamports,
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: account.data.clone(),
            mint_data: None,
        }
    }

    fn from_row(row: &PgRow, with_mint: bool) -> Result<Self, RpcError> {
        let mint_data = if with_mint {
            row.try_get::<Vec<u8>, _>("mint_data").ok().map(Arc::new)
        } else {
            None
        };
        Ok(Self {
            lamports: row.get::<i64, _>("lamports") as u64,
            owner: pubkey_column(row, "owner")?,
            executable: row.get("executable"),
            rent_epoch: row
                .get::<rust_decimal::Decimal, _>("rent_epoch")
                .to_u64()
                .unwrap_or(0),
            data: Arc::new(row.get("data")),
            mint_data,
        })
    }

    /// The mint data of a jsonParsed token account. Empty when the mint was not found.
    pub(crate) fn mint_data(&self) -> &[u8] {
        self.mint_data.as_deref().map_or(&[], Vec::as_slice)
    }

    /// The mint of a token-program account with at least 32 bytes of data.
    fn token_mint(&self) -> Option<Pubkey> {
        if !is_token_program(&self.owner) || self.data.len() < 32 {
            return None;
        }
        Pubkey::try_from(&self.data[..32]).ok()
    }
}

/// Reads `keys` in order, `None` for an absent or closed key. Without blocks
/// every key reads Postgres with `sql_template` at `latest_slot`. With blocks,
/// a live key comes from memory and an unknown key reads Postgres at the anchor.
/// `$1` in the template takes one bytea literal, or an array when the template
/// unnests it. `what` names the method in logs.
pub(crate) async fn read_accounts(
    state: &CloudbreakRpcState,
    read: &Read,
    keys: &[Pubkey],
    sql_template: &str,
    latest_slot: u64,
    with_mint: bool,
    what: &str,
) -> Result<Vec<Option<Account>>, RpcError> {
    let in_blocks: Vec<ProcessedAccount<'_>> = match &read.blocks {
        Some(blocks) => blocks
            .read_span(what)
            .in_scope(|| keys.iter().map(|key| blocks.get_account(key)).collect()),
        None => vec![ProcessedAccount::Unknown; keys.len()],
    };
    let unknown: Vec<Pubkey> = keys
        .iter()
        .zip(&in_blocks)
        .filter(|(_, from_blocks)| matches!(from_blocks, ProcessedAccount::Unknown))
        .map(|(key, _)| *key)
        .collect();

    let mut found = HashMap::with_capacity(unknown.len());
    if !unknown.is_empty() {
        let bound = read.sql_slot_bound(latest_slot);
        for row in fetch_accounts(state, sql_template, &unknown, bound, what).await? {
            found.insert(
                pubkey_column(&row, "pubkey")?,
                Account::from_row(&row, with_mint)?,
            );
        }
    }

    let mut accounts = in_key_order(keys, &in_blocks, found);
    if let Some(blocks) = &read.blocks
        && with_mint
    {
        resolve_mints(state, blocks, &mut accounts, &in_blocks).await?;
    }
    Ok(accounts)
}

/// Pairs each key with its account from the blocks, from Postgres, or `None`.
fn in_key_order(
    keys: &[Pubkey],
    in_blocks: &[ProcessedAccount<'_>],
    found: HashMap<Pubkey, Account>,
) -> Vec<Option<Account>> {
    keys.iter()
        .zip(in_blocks)
        .map(|(key, from_blocks)| match from_blocks {
            ProcessedAccount::Live(account) => Some(Account::from_live(account)),
            ProcessedAccount::Closed => None,
            ProcessedAccount::Unknown => found.get(key).cloned(),
        })
        .collect()
}

async fn resolve_mints(
    state: &CloudbreakRpcState,
    blocks: &ProcessedBlocks,
    accounts: &mut [Option<Account>],
    in_blocks: &[ProcessedAccount<'_>],
) -> Result<(), RpcError> {
    let mut queried: Vec<(usize, Pubkey)> = Vec::new();
    for (index, (account, from_blocks)) in accounts.iter_mut().zip(in_blocks).enumerate() {
        let Some(account) = account else {
            continue;
        };
        let Some(mint) = account.token_mint() else {
            continue;
        };
        // A mint in the blocks overrides the joined one. An account from the blocks
        // whose mint is unknown reads the mint at the anchor.
        let account_from_postgres = matches!(from_blocks, ProcessedAccount::Unknown);
        match blocks.get_account(&mint) {
            ProcessedAccount::Live(mint) => account.mint_data = Some(mint.data.clone()),
            ProcessedAccount::Closed => account.mint_data = None,
            ProcessedAccount::Unknown if account_from_postgres => {}
            ProcessedAccount::Unknown => queried.push((index, mint)),
        }
    }
    if queried.is_empty() {
        return Ok(());
    }

    let mut mints: Vec<Pubkey> = queried.iter().map(|(_, mint)| *mint).collect();
    mints.sort_unstable();
    mints.dedup();
    let sql_template = include_str!("../db/getMultipleAccounts.sql");
    let rows = fetch_accounts(
        state,
        sql_template,
        &mints,
        blocks.anchor_slot,
        "processed mint",
    )
    .await?;
    let mut found = HashMap::with_capacity(rows.len());
    for row in &rows {
        let data = Arc::new(row.get::<Vec<u8>, _>("data"));
        found.insert(pubkey_column(row, "pubkey")?, data);
    }
    for (index, mint) in queried {
        if let Some(account) = &mut accounts[index] {
            account.mint_data = found.get(&mint).cloned();
        }
    }
    Ok(())
}

/// Runs `sql_template` for `keys` bounded at `slot` under the API query timeout.
async fn fetch_accounts(
    state: &CloudbreakRpcState,
    sql_template: &str,
    keys: &[Pubkey],
    slot: u64,
    what: &str,
) -> Result<Vec<PgRow>, RpcError> {
    let keys_literal = if sql_template.contains("unnest($1)") {
        bytea_array_literal(keys)
    } else {
        bytea_literal(&keys[0])
    };
    let sql = sql_template.replace("$1", &keys_literal);
    let sql = sql.replace("$2", &slot.to_string());
    let sql = db_query::add_trace_traceparent_to_query(&sql);

    tracing::debug!(target: "account_sql", "## {what} sql: {}", sql);

    let pool = state.database.get_postgres_connection_pool();
    timeout(state.queries_timeout, async {
        let span = tracing::info_span!("account_db", method = what);
        sqlx::raw_sql(&sql).fetch_all(pool).instrument(span).await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("{what} query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })
}

fn pubkey_column(row: &PgRow, column: &str) -> Result<Pubkey, RpcError> {
    let bytes: Vec<u8> = row.try_get(column).map_err(|e| {
        tracing::error!("missing {column} column returned by DB: {e}");
        RpcError::InternalError
    })?;
    Pubkey::try_from(bytes.as_slice()).map_err(|_| {
        tracing::error!("invalid {column} bytes returned by DB");
        RpcError::InternalError
    })
}

fn bytea_literal(pubkey: &Pubkey) -> String {
    format!("'\\x{}'::bytea", hex::encode(pubkey.as_ref()))
}

fn bytea_array_literal(pubkeys: &[Pubkey]) -> String {
    let literals: Vec<String> = pubkeys.iter().map(bytea_literal).collect();
    format!("ARRAY[{}]", literals.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::methods::LEGACY_TOKEN_PROGRAM_ID;
    use crate::slot_syncronizer::SlotData;

    fn slots(confirmed: u64, finalized: u64, healthy: bool) -> SlotSyncronizerData {
        SlotSyncronizerData {
            confirmed_slot: SlotData {
                slot: confirmed,
                block_time: 0,
            },
            finalized_slot: SlotData {
                slot: finalized,
                block_time: 0,
            },
            healthy,
        }
    }

    #[test]
    fn fallback_checks_health_head_and_finalized_against_the_cached_slots() {
        assert_eq!(
            fallback_reason(103, 100, &slots(100, 68, false)),
            Some("unhealthy")
        );
        assert_eq!(
            fallback_reason(99, 98, &slots(100, 68, true)),
            Some("head_behind")
        );
        assert_eq!(
            fallback_reason(103, 100, &slots(100, 101, true)),
            Some("finalized_above_anchor")
        );
        assert_eq!(fallback_reason(100, 100, &slots(100, 100, true)), None);
        assert_eq!(fallback_reason(103, 100, &slots(101, 68, true)), None);
    }

    fn live(owner: Pubkey, lamports: u64, data: Vec<u8>) -> LiveAccount {
        LiveAccount {
            lamports,
            owner,
            executable: false,
            rent_epoch: u64::MAX,
            data: Arc::new(data),
        }
    }

    #[test]
    fn in_key_order_keeps_order_across_live_closed_and_unknown() {
        let mut keys: Vec<Pubkey> = (0..5).map(|_| Pubkey::new_unique()).collect();
        // A repeated key answers at every position.
        keys[4] = keys[2];
        let live_in_blocks = live(Pubkey::new_unique(), 7, vec![]);
        let in_blocks = [
            ProcessedAccount::Live(&live_in_blocks),
            ProcessedAccount::Closed,
            ProcessedAccount::Unknown,
            ProcessedAccount::Unknown,
            ProcessedAccount::Unknown,
        ];
        let mut found = HashMap::new();
        found.insert(
            keys[2],
            Account {
                lamports: 9,
                owner: Pubkey::new_unique(),
                executable: false,
                rent_epoch: 0,
                data: Arc::new(vec![]),
                mint_data: None,
            },
        );

        let accounts = in_key_order(&keys, &in_blocks, found);
        let lamports: Vec<Option<u64>> = accounts
            .iter()
            .map(|account| account.as_ref().map(|account| account.lamports))
            .collect();
        assert_eq!(lamports, vec![Some(7), None, Some(9), None, Some(9)]);
    }

    #[test]
    fn token_mint_needs_a_token_owner_and_32_bytes_of_data() {
        let mint = Pubkey::new_unique();
        let mut data = vec![0u8; 165];
        data[..32].copy_from_slice(mint.as_ref());

        let account = Account::from_live(&live(LEGACY_TOKEN_PROGRAM_ID, 1, data.clone()));
        assert_eq!(account.token_mint(), Some(mint));
        assert!(account.mint_data().is_empty());

        let short = Account::from_live(&live(LEGACY_TOKEN_PROGRAM_ID, 1, data[..10].to_vec()));
        assert_eq!(short.token_mint(), None);

        let other = Account::from_live(&live(Pubkey::new_unique(), 1, data));
        assert_eq!(other.token_mint(), None);
    }
}
