// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! getBalance and getTokenAccountBalance on a view.

use std::sync::Arc;

use cloudbreak_core::AccountSelectorConfig;
use cloudbreak_core::modules::processed::{LiveAccount, Lookup, ProcessedView};
use sea_orm::sqlx::Row;
use solana_account_decoder::parse_token::token_amount_to_ui_amount_v3;
use solana_account_decoder_client_types::token::UiTokenAmount;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::Response as RpcResponse;
use spl_token_2022_interface::extension::StateWithExtensions;
use spl_token_2022_interface::state::Account as TokenAccount;

use super::accounts::{MintSource, mint_data_at_slot, mint_source};
use super::{
    bind_query, bytea_literal, check_min_context_slot, fetch_rows, owner_excluded, pubkey_column,
    record_lookups, response,
};
use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::is_token_program;
use crate::methods::token::parse_additional_mint_data;

pub(crate) async fn get_balance(
    state: &CloudbreakRpcState,
    view: &ProcessedView,
    pubkey: &Pubkey,
    min_context_slot: Option<u64>,
) -> Result<RpcResponse<u64>, RpcError> {
    check_min_context_slot(view, min_context_slot)?;
    let lookup = view.lookup(pubkey);
    record_lookups("getBalance", &[lookup]);
    let lamports = match balance_from_lookup(pubkey, lookup)? {
        Some(lamports) => lamports,
        None => {
            let row = balance_at_slot(state, pubkey, view.anchor_slot).await?;
            balance_from_row(&state.indexer_filter, pubkey, row)?
        }
    };
    Ok(response(view, lamports))
}

/// The balance a view answers for one key, or `None` for a miss.
fn balance_from_lookup(pubkey: &Pubkey, lookup: Lookup<'_>) -> Result<Option<u64>, RpcError> {
    match lookup {
        Lookup::Live(account) => Ok(Some(account.lamports)),
        Lookup::Closed => Ok(Some(0)),
        Lookup::Excluded(owner) => Err(owner_excluded(pubkey, &owner)),
        Lookup::Miss => Ok(None),
    }
}

/// Applies the owner check to the `(owner, lamports)` of a miss. No row is a zero balance.
fn balance_from_row(
    filter: &AccountSelectorConfig,
    pubkey: &Pubkey,
    row: Option<(Pubkey, u64)>,
) -> Result<u64, RpcError> {
    match row {
        None => Ok(0),
        Some((owner, _)) if !filter.is_program_selected(&owner) => {
            Err(owner_excluded(pubkey, &owner))
        }
        Some((_, lamports)) => Ok(lamports),
    }
}

/// Newest live `(owner, lamports)` at `slot <= slot`, read with `getBalanceAtSlot.sql`.
async fn balance_at_slot(
    state: &CloudbreakRpcState,
    pubkey: &Pubkey,
    slot: u64,
) -> Result<Option<(Pubkey, u64)>, RpcError> {
    let template = include_str!("../../db/getBalanceAtSlot.sql");
    let query = bind_query(template, &bytea_literal(pubkey), slot);
    tracing::debug!(target: "get_balance_sql", "## sql: {}", query);
    let span = tracing::info_span!("get_balance_db");
    let rows = fetch_rows(state, &query, span, "getBalance").await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let lamports = row.get::<i64, _>("lamports") as u64;
    Ok(Some((pubkey_column(row, "owner")?, lamports)))
}

pub(crate) async fn get_token_account_balance(
    state: &CloudbreakRpcState,
    view: &ProcessedView,
    pubkey: &Pubkey,
) -> Result<RpcResponse<UiTokenAmount>, RpcError> {
    let lookup = view.lookup(pubkey);
    record_lookups("getTokenAccountBalance", &[lookup]);
    let (amount, mint, mint_data) = match lookup {
        Lookup::Closed => {
            return Err(RpcError::AccountNotFound {
                pubkey: pubkey.to_string(),
            });
        }
        Lookup::Excluded(owner) => return Err(owner_excluded(pubkey, &owner)),
        Lookup::Live(account) => {
            let (amount, mint) = token_balance_from_live(pubkey, account)?;
            let mint_data = match mint_source(view.lookup(&mint), false) {
                MintSource::Overlay(data) => Some(data),
                MintSource::Query => mint_data_at_slot(state, &[mint], view.anchor_slot)
                    .await?
                    .remove(&mint),
                MintSource::Empty | MintSource::Joined => None,
            };
            (amount, mint, mint_data.unwrap_or_default())
        }
        Lookup::Miss => {
            let (amount, mint, joined) =
                token_balance_at_slot(state, pubkey, view.anchor_slot).await?;
            let mint_data = match mint_source(view.lookup(&mint), true) {
                MintSource::Overlay(data) => data,
                MintSource::Empty => Arc::default(),
                MintSource::Joined | MintSource::Query => Arc::new(joined),
            };
            (amount, mint, mint_data)
        }
    };

    // Empty mint data keeps the WSOL native_mint short-circuit.
    let additional_mint_data = parse_additional_mint_data(&mint, &mint_data, view.block_time);
    let additional_data = additional_mint_data
        .as_ref()
        .and_then(|d| d.spl_token_additional_data.as_ref())
        .ok_or_else(|| RpcError::MintDataNotFound {
            mint: mint.to_string(),
        })?;
    Ok(response(
        view,
        token_amount_to_ui_amount_v3(amount, additional_data),
    ))
}

/// Amount and mint of an in-memory token account. A non-token owner or data
/// that does not unpack as a token account gives `NotATokenAccount`, as in Agave.
fn token_balance_from_live(
    pubkey: &Pubkey,
    account: &LiveAccount,
) -> Result<(u64, Pubkey), RpcError> {
    let not_a_token_account = || RpcError::NotATokenAccount {
        pubkey: pubkey.to_string(),
    };
    if !is_token_program(&account.owner) {
        return Err(not_a_token_account());
    }
    let token = StateWithExtensions::<TokenAccount>::unpack(&account.data)
        .map_err(|_| not_a_token_account())?;
    Ok((token.base.amount, token.base.mint))
}

/// Reads a token account at `slot <= slot` with `getTokenAccountBalance.sql` and
/// returns its amount, mint and joined mint data, empty when no live mint joined.
async fn token_balance_at_slot(
    state: &CloudbreakRpcState,
    pubkey: &Pubkey,
    slot: u64,
) -> Result<(u64, Pubkey, Vec<u8>), RpcError> {
    let template = include_str!("../../db/getTokenAccountBalance.sql");
    let query = bind_query(template, &bytea_literal(pubkey), slot);
    tracing::debug!(target: "get_token_account_balance_sql", "## sql: {}", query);
    let span = tracing::info_span!("get_token_account_balance_db");
    let rows = fetch_rows(state, &query, span, "getTokenAccountBalance").await?;

    let Some(row) = rows.first() else {
        return Err(RpcError::AccountNotFound {
            pubkey: pubkey.to_string(),
        });
    };
    let owner = pubkey_column(row, "owner")?;
    if !state.indexer_filter.is_program_selected(&owner) {
        return Err(owner_excluded(pubkey, &owner));
    }
    if !is_token_program(&owner) {
        return Err(RpcError::NotATokenAccount {
            pubkey: pubkey.to_string(),
        });
    }

    // The SQL returns the 8 amount bytes for token-owned accounts.
    let amount_bytes: Vec<u8> = row.get("amount");
    let amount = <[u8; 8]>::try_from(amount_bytes.as_slice())
        .map(u64::from_le_bytes)
        .map_err(|_| {
            tracing::error!(
                "getTokenAccountBalance: unexpected amount length {} for pubkey {pubkey}",
                amount_bytes.len()
            );
            RpcError::InternalError
        })?;
    let mint = pubkey_column(row, "token_mint")?;
    let mint_data: Vec<u8> = row.try_get("mint_data").ok().unwrap_or_default();
    Ok((amount, mint, mint_data))
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{live, mint_data, token_account_data};
    use super::*;
    use crate::methods::{LEGACY_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};
    use cloudbreak_core::PubkeyDef;

    #[test]
    fn token_balance_unpacks_like_agave() {
        let pubkey = Pubkey::new_unique();
        let mint = Pubkey::new_unique();

        let valid = live(LEGACY_TOKEN_PROGRAM_ID, 1, token_account_data(&mint, 42));
        assert_eq!(
            token_balance_from_live(&pubkey, &valid).unwrap(),
            (42, mint)
        );

        let not_a_token_account = |account: LiveAccount| {
            matches!(
                token_balance_from_live(&pubkey, &account),
                Err(RpcError::NotATokenAccount { .. })
            )
        };
        assert!(not_a_token_account(live(
            LEGACY_TOKEN_PROGRAM_ID,
            1,
            vec![]
        )));
        assert!(not_a_token_account(live(
            LEGACY_TOKEN_PROGRAM_ID,
            1,
            vec![1; 64]
        )));
        assert!(not_a_token_account(live(
            TOKEN_2022_PROGRAM_ID,
            1,
            mint_data(6)
        )));
        assert!(not_a_token_account(live(
            Pubkey::new_unique(),
            1,
            token_account_data(&mint, 42)
        )));
    }

    #[test]
    fn balance_maps_each_lookup_and_row() {
        let selected = Pubkey::new_unique();
        let pubkey = Pubkey::new_unique();
        let account = live(selected, 7, vec![]);
        let from_lookup = |lookup| balance_from_lookup(&pubkey, lookup);
        assert_eq!(from_lookup(Lookup::Live(&account)).unwrap(), Some(7));
        assert_eq!(from_lookup(Lookup::Closed).unwrap(), Some(0));
        assert_eq!(from_lookup(Lookup::Miss).unwrap(), None);
        assert!(matches!(
            from_lookup(Lookup::Excluded(selected)),
            Err(RpcError::AccountOwnerExcluded { .. })
        ));

        let filter = AccountSelectorConfig {
            include: vec![PubkeyDef(selected)],
            exclude: vec![],
        };
        assert_eq!(balance_from_row(&filter, &pubkey, None).unwrap(), 0);
        assert_eq!(
            balance_from_row(&filter, &pubkey, Some((selected, 9))).unwrap(),
            9
        );
        assert!(matches!(
            balance_from_row(&filter, &pubkey, Some((Pubkey::new_unique(), 9))),
            Err(RpcError::AccountOwnerExcluded { .. })
        ));
    }
}
