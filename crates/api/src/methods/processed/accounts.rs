// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! getAccountInfo and getMultipleAccounts on a view, mint resolution and
//! account encoding.

use std::collections::HashMap;
use std::sync::Arc;

use cloudbreak_core::AccountSelectorConfig;
use cloudbreak_core::modules::processed::{LiveAccount, Lookup, ProcessedView};
use rust_decimal::prelude::ToPrimitive;
use sea_orm::sqlx::Row;
use sea_orm::sqlx::postgres::PgRow;
use solana_account::AccountSharedData;
use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig, encode_ui_account};
use solana_account_decoder_client_types::UiAccount;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::Response as RpcResponse;

use super::{
    bind_query, bytea_array_literal, bytea_literal, check_min_context_slot, fetch_rows,
    owner_excluded, pubkey_column, record_lookups, response,
};
use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::is_token_program;
use crate::methods::token::{check_account_data_len_for_encoding, parse_additional_mint_data};

pub(crate) async fn get_account_info(
    state: &CloudbreakRpcState,
    view: &ProcessedView,
    pubkey: &Pubkey,
    config: &RpcAccountInfoConfig,
) -> Result<RpcResponse<Option<UiAccount>>, RpcError> {
    check_min_context_slot(view, config.min_context_slot)?;
    let encoding = config.encoding.unwrap_or(UiAccountEncoding::Binary);
    let keys = std::slice::from_ref(pubkey);
    let reads = read_accounts(state, view, Method::AccountInfo, keys, encoding).await?;
    let value = match reads.into_iter().next() {
        Some(KeyRead::Account { account, mint_data }) => Some(encode_account(
            pubkey,
            account,
            mint_data.as_deref().map(Vec::as_slice),
            encoding,
            config.data_slice,
            view.block_time,
        )?),
        Some(KeyRead::Excluded(owner)) => return Err(owner_excluded(pubkey, &owner)),
        Some(KeyRead::Absent) | None => None,
    };
    Ok(response(view, value))
}

pub(crate) async fn get_multiple_accounts(
    state: &CloudbreakRpcState,
    view: &ProcessedView,
    pubkeys: &[Pubkey],
    config: &RpcAccountInfoConfig,
) -> Result<RpcResponse<Vec<Option<UiAccount>>>, RpcError> {
    check_min_context_slot(view, config.min_context_slot)?;
    let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64);
    let reads = read_accounts(state, view, Method::MultipleAccounts, pubkeys, encoding).await?;
    let mut value = Vec::with_capacity(pubkeys.len());
    for (pubkey, read) in pubkeys.iter().zip(reads) {
        value.push(match read {
            KeyRead::Account { account, mint_data } => Some(encode_account(
                pubkey,
                account,
                mint_data.as_deref().map(Vec::as_slice),
                encoding,
                config.data_slice,
                view.block_time,
            )?),
            KeyRead::Excluded(owner) => {
                tracing::error!(
                    target: "gma_indexer_filter",
                    pubkey = %pubkey,
                    owner = %owner,
                    "getMultipleAccounts: skipping account because owner is excluded by the current indexer filter"
                );
                None
            }
            KeyRead::Absent => None,
        });
    }
    Ok(response(view, value))
}

/// The calling method. It picks the metric label and the SQL that reads misses.
#[derive(Debug, Clone, Copy)]
enum Method {
    AccountInfo,
    MultipleAccounts,
}

/// Account fields ready for encoding, from the overlay or a Postgres row.
#[derive(Debug, Clone)]
struct AccountParts {
    lamports: u64,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
    data: Arc<Vec<u8>>,
}

impl AccountParts {
    fn from_live(account: &LiveAccount) -> Self {
        Self {
            lamports: account.lamports,
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: account.data.clone(),
        }
    }

    fn from_row(row: &PgRow, owner: Pubkey) -> Self {
        Self {
            lamports: row.get::<i64, _>("lamports") as u64,
            owner,
            executable: row.get("executable"),
            rent_epoch: row
                .get::<rust_decimal::Decimal, _>("rent_epoch")
                .to_u64()
                .unwrap_or(0),
            data: Arc::new(row.get("data")),
        }
    }

    /// The mint of a token-program account with at least 32 bytes of data.
    fn token_mint(&self) -> Option<Pubkey> {
        if !is_token_program(&self.owner) || self.data.len() < 32 {
            return None;
        }
        Pubkey::try_from(&self.data[..32]).ok()
    }
}

/// The state of one requested key.
#[derive(Debug)]
enum KeyRead {
    /// `mint_data` is set only for jsonParsed token accounts whose mint was found.
    Account {
        account: AccountParts,
        mint_data: Option<Arc<Vec<u8>>>,
    },
    /// Never written, or closed.
    Absent,
    /// Owner outside the program filter.
    Excluded(Pubkey),
}

/// Reads the requested keys in order. Misses read Postgres at the anchor, and
/// jsonParsed token accounts resolve their mint through the view.
async fn read_accounts(
    state: &CloudbreakRpcState,
    view: &ProcessedView,
    method: Method,
    keys: &[Pubkey],
    encoding: UiAccountEncoding,
) -> Result<Vec<KeyRead>, RpcError> {
    let with_mint = encoding == UiAccountEncoding::JsonParsed;
    let lookups: Vec<Lookup<'_>> = keys.iter().map(|key| view.lookup(key)).collect();
    let label = match method {
        Method::AccountInfo => "gAI",
        Method::MultipleAccounts => "getMultipleAccounts",
    };
    record_lookups(label, &lookups);

    let misses: Vec<Pubkey> = keys
        .iter()
        .zip(&lookups)
        .filter(|(_, lookup)| matches!(lookup, Lookup::Miss))
        .map(|(key, _)| *key)
        .collect();
    let rows = if misses.is_empty() {
        Vec::new()
    } else {
        fetch_misses(state, method, &misses, view.anchor_slot, with_mint).await?
    };
    let mut row_by_pubkey: HashMap<Pubkey, &PgRow> = HashMap::with_capacity(rows.len());
    for row in &rows {
        row_by_pubkey.insert(pubkey_column(row, "pubkey")?, row);
    }

    let mut reads = Vec::with_capacity(keys.len());
    for (key, lookup) in keys.iter().zip(&lookups) {
        reads.push(match *lookup {
            Lookup::Live(account) => KeyRead::Account {
                account: AccountParts::from_live(account),
                mint_data: None,
            },
            Lookup::Closed => KeyRead::Absent,
            Lookup::Excluded(owner) => KeyRead::Excluded(owner),
            Lookup::Miss => match row_by_pubkey.get(key) {
                Some(row) => key_read_from_row(&state.indexer_filter, row, with_mint)?,
                None => KeyRead::Absent,
            },
        });
    }
    if with_mint {
        resolve_mints(state, view, &mut reads, &lookups).await?;
    }
    Ok(reads)
}

fn key_read_from_row(
    filter: &AccountSelectorConfig,
    row: &PgRow,
    with_mint: bool,
) -> Result<KeyRead, RpcError> {
    let owner = pubkey_column(row, "owner")?;
    if !filter.is_program_selected(&owner) {
        return Ok(KeyRead::Excluded(owner));
    }
    let mint_data = if with_mint {
        row.try_get::<Vec<u8>, _>("mint_data").ok().map(Arc::new)
    } else {
        None
    };
    Ok(KeyRead::Account {
        account: AccountParts::from_row(row, owner),
        mint_data,
    })
}

/// Reads the misses at `slot` with the method's own SQL.
async fn fetch_misses(
    state: &CloudbreakRpcState,
    method: Method,
    misses: &[Pubkey],
    slot: u64,
    with_mint: bool,
) -> Result<Vec<PgRow>, RpcError> {
    match method {
        Method::AccountInfo => {
            let template = if with_mint {
                include_str!("../../db/getAccountInfoWithMintData.sql")
            } else {
                include_str!("../../db/getAccountInfo.sql")
            };
            let query = bind_query(template, &bytea_literal(&misses[0]), slot);
            tracing::debug!(target: "gai_sql", "## sql: {}", query);
            let span = tracing::info_span!("gai_db");
            fetch_rows(state, &query, span, "getAccountInfo").await
        }
        Method::MultipleAccounts => {
            let template = if with_mint {
                include_str!("../../db/getMultipleAccountsWithMintData.sql")
            } else {
                include_str!("../../db/getMultipleAccounts.sql")
            };
            let query = bind_query(template, &bytea_array_literal(misses), slot);
            tracing::debug!(target: "gma_sql", "## sql: {}", query);
            let span = tracing::info_span!("gma_db");
            fetch_rows(state, &query, span, "getMultipleAccounts").await
        }
    }
}

/// Where a token account's mint data comes from on a view.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum MintSource {
    /// The mint is live in the overlay.
    Overlay(Arc<Vec<u8>>),
    /// The mint is closed or excluded in the overlay.
    Empty,
    /// Keep the mint joined by the Postgres row.
    Joined,
    /// The account came from the overlay and the mint missed: read it at the anchor.
    Query,
}

/// Resolves a mint through the view before any Postgres data.
pub(super) fn mint_source(mint_lookup: Lookup<'_>, account_from_postgres: bool) -> MintSource {
    match mint_lookup {
        Lookup::Live(mint) => MintSource::Overlay(mint.data.clone()),
        Lookup::Closed | Lookup::Excluded(_) => MintSource::Empty,
        Lookup::Miss if account_from_postgres => MintSource::Joined,
        Lookup::Miss => MintSource::Query,
    }
}

async fn resolve_mints(
    state: &CloudbreakRpcState,
    view: &ProcessedView,
    reads: &mut [KeyRead],
    lookups: &[Lookup<'_>],
) -> Result<(), RpcError> {
    let mut queried: Vec<(usize, Pubkey)> = Vec::new();
    for (index, (read, lookup)) in reads.iter_mut().zip(lookups).enumerate() {
        let KeyRead::Account { account, mint_data } = read else {
            continue;
        };
        let Some(mint) = account.token_mint() else {
            continue;
        };
        let from_postgres = matches!(lookup, Lookup::Miss);
        match mint_source(view.lookup(&mint), from_postgres) {
            MintSource::Overlay(data) => *mint_data = Some(data),
            MintSource::Empty => *mint_data = None,
            MintSource::Joined => {}
            MintSource::Query => queried.push((index, mint)),
        }
    }
    if queried.is_empty() {
        return Ok(());
    }

    let mut mints: Vec<Pubkey> = queried.iter().map(|(_, mint)| *mint).collect();
    mints.sort_unstable();
    mints.dedup();
    let found = mint_data_at_slot(state, &mints, view.anchor_slot).await?;
    for (index, mint) in queried {
        if let KeyRead::Account { mint_data, .. } = &mut reads[index] {
            *mint_data = found.get(&mint).cloned();
        }
    }
    Ok(())
}

/// Live mint data at `slot <= slot`, keyed by mint, read with `getMultipleAccounts.sql`.
pub(super) async fn mint_data_at_slot(
    state: &CloudbreakRpcState,
    mints: &[Pubkey],
    slot: u64,
) -> Result<HashMap<Pubkey, Arc<Vec<u8>>>, RpcError> {
    let query = bind_query(
        include_str!("../../db/getMultipleAccounts.sql"),
        &bytea_array_literal(mints),
        slot,
    );
    let span = tracing::info_span!("processed_mint_db");
    let rows = fetch_rows(state, &query, span, "processed mint").await?;
    let mut found = HashMap::with_capacity(rows.len());
    for row in &rows {
        let data = Arc::new(row.get::<Vec<u8>, _>("data"));
        found.insert(pubkey_column(row, "pubkey")?, data);
    }
    Ok(found)
}

/// Encodes one account. jsonParsed token accounts use `mint_data`, or empty mint data.
fn encode_account(
    pubkey: &Pubkey,
    account: AccountParts,
    mint_data: Option<&[u8]>,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
    block_time: i64,
) -> Result<UiAccount, RpcError> {
    let additional_mint_data = if encoding == UiAccountEncoding::JsonParsed {
        account.token_mint().and_then(|mint| {
            parse_additional_mint_data(&mint, mint_data.unwrap_or_default(), block_time)
        })
    } else {
        None
    };

    check_account_data_len_for_encoding(encoding, data_slice, account.data.len(), pubkey)?;

    let account_shared_data = AccountSharedData::create_from_existing_shared_data(
        account.lamports,
        account.data,
        account.owner,
        account.executable,
        account.rent_epoch,
    );

    // encode_ui_account takes `space` from the full data before slicing, as Agave does.
    Ok(encode_ui_account(
        pubkey,
        &account_shared_data,
        encoding,
        additional_mint_data,
        data_slice,
    ))
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{live, mint_data, token_account_data};
    use super::*;
    use crate::methods::{LEGACY_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};
    use solana_account_decoder_client_types::UiAccountData;

    #[test]
    fn overlay_mint_overrides_joined_mint() {
        let mint = live(TOKEN_2022_PROGRAM_ID, 1, mint_data(6));
        for from_postgres in [true, false] {
            assert_eq!(
                mint_source(Lookup::Live(&mint), from_postgres),
                MintSource::Overlay(mint.data.clone())
            );
            assert_eq!(
                mint_source(Lookup::Closed, from_postgres),
                MintSource::Empty
            );
            assert_eq!(
                mint_source(Lookup::Excluded(Pubkey::new_unique()), from_postgres),
                MintSource::Empty
            );
        }
        assert_eq!(mint_source(Lookup::Miss, true), MintSource::Joined);
        assert_eq!(mint_source(Lookup::Miss, false), MintSource::Query);
    }

    fn parsed_decimals(account: UiAccount) -> Option<u64> {
        let UiAccountData::Json(parsed) = account.data else {
            return None;
        };
        parsed.parsed["info"]["tokenAmount"]["decimals"].as_u64()
    }

    #[test]
    fn encode_account_uses_the_resolved_mint_data() {
        let mint = Pubkey::new_unique();
        let pubkey = Pubkey::new_unique();
        let account = AccountParts::from_live(&live(
            LEGACY_TOKEN_PROGRAM_ID,
            2_039_280,
            token_account_data(&mint, 5_000),
        ));
        let encode = |mint_data: Option<&[u8]>| {
            encode_account(
                &pubkey,
                account.clone(),
                mint_data,
                UiAccountEncoding::JsonParsed,
                None,
                0,
            )
            .unwrap()
        };
        assert_eq!(parsed_decimals(encode(Some(&mint_data(6)))), Some(6));
        assert_eq!(parsed_decimals(encode(Some(&mint_data(9)))), Some(9));
        // No mint data cannot be parsed as a token account, as on the Postgres path.
        assert_eq!(parsed_decimals(encode(None)), None);
    }
}
