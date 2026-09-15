// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Processed commitment for getAccountInfo and getMultipleAccounts.
//!
//! Each method calls [`route`] first. With `[processed-accounts]` enabled, a
//! processed request takes one [`ProcessedView`] ([`Route::View`]) and the method
//! returns early into this module. When no view is published, the node is
//! unhealthy, the view head is below the cached confirmed slot, or the cached
//! finalized slot is above the view anchor, the request takes the confirmed
//! path ([`Route::Db`]) and answers exactly as a confirmed request would, even
//! with `processed-commitment = "reject"`. Every other request resolves through
//! [`resolve_commitment`].
//!
//! On a view, a key written in the chain is answered from memory. A miss reads
//! Postgres at `slot <= view.anchor_slot`. `Lookup::Closed` never falls back to
//! Postgres and answers null. A Postgres row keeps the confirmed owner check.
//! `context.slot` and the block time come from the head block. A jsonParsed
//! token account resolves its mint through the view first:
//!
//! | Mint lookup | Account from the view | Account from Postgres |
//! |---|---|---|
//! | `Live` | view mint data | view mint data, overriding the joined mint |
//! | `Closed` | no mint data | no mint data |
//! | `Miss` | one `getMultipleAccounts.sql` read at the anchor | the joined mint |

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use cloudbreak_core::AccountSelectorConfig;
use cloudbreak_core::modules::processed::{LiveAccount, Lookup, ProcessedView};
use rust_decimal::prelude::ToPrimitive;
use sea_orm::sqlx::{self, Row, postgres::PgRow};
use solana_account::AccountSharedData;
use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig, encode_ui_account};
use solana_account_decoder_client_types::UiAccount;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::token::{check_account_data_len_for_encoding, parse_additional_mint_data};
use crate::methods::{is_token_program, resolve_commitment};
use crate::slot_syncronizer::SlotSyncronizerData;
use crate::{db_query, metrics};

/// Where one request is served from.
pub(crate) enum Route {
    /// The processed view, with Postgres misses bounded at its anchor.
    View(Arc<ProcessedView>),
    /// Postgres at this commitment.
    Db(CommitmentLevel),
}

/// Resolves the request commitment. `method` is the metric label.
pub(crate) fn route(
    state: &CloudbreakRpcState,
    commitment: Option<CommitmentConfig>,
    method: &str,
) -> Result<Route, RpcError> {
    let commitment = commitment.map(|config| config.commitment);
    if commitment != Some(CommitmentLevel::Processed) || !state.processed.is_enabled() {
        let level = commitment
            .map(|commitment| resolve_commitment(commitment, state.processed_commitment))
            .transpose()?
            .unwrap_or(CommitmentLevel::Finalized);
        return Ok(Route::Db(level));
    }
    let slots = state
        .slot_syncronizer_data
        .as_ref()
        .map_or_else(Default::default, |data| {
            data.read()
                .expect("Failed to read slot syncronizer data")
                .clone()
        });
    Ok(match servable(state.processed.view(), &slots) {
        Ok(view) => {
            count_route(method, "view", "none");
            Route::View(view)
        }
        Err(reason) => {
            count_route(method, "degraded", reason);
            Route::Db(CommitmentLevel::Confirmed)
        }
    })
}

/// The view a processed request serves, or why it takes the confirmed path.
fn servable(
    view: Option<Arc<ProcessedView>>,
    slots: &SlotSyncronizerData,
) -> Result<Arc<ProcessedView>, &'static str> {
    let view = view.ok_or("no_view")?;
    match fallback_reason(view.slot, view.anchor_slot, slots) {
        Some(reason) => Err(reason),
        None => Ok(view),
    }
}

/// Why a view with this head and anchor takes the confirmed path.
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
        Some(KeyRead::Excluded(owner)) => {
            return Err(RpcError::AccountOwnerExcluded {
                pubkey: pubkey.to_string(),
                owner: owner.to_string(),
            });
        }
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

/// The calling method. It picks the SQL that reads misses.
#[derive(Debug, Clone, Copy)]
enum Method {
    AccountInfo,
    MultipleAccounts,
}

/// Account fields ready for encoding, from the view or a Postgres row.
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
    /// A Postgres row whose owner is outside the program filter.
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
                include_str!("../db/getAccountInfoWithMintData.sql")
            } else {
                include_str!("../db/getAccountInfo.sql")
            };
            let query = bind_query(template, &bytea_literal(&misses[0]), slot);
            tracing::debug!(target: "gai_sql", "## sql: {}", query);
            let span = tracing::info_span!("gai_db");
            fetch_rows(state, &query, span, "getAccountInfo").await
        }
        Method::MultipleAccounts => {
            let template = if with_mint {
                include_str!("../db/getMultipleAccountsWithMintData.sql")
            } else {
                include_str!("../db/getMultipleAccounts.sql")
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
enum MintSource {
    /// The mint is live in the view.
    View(Arc<Vec<u8>>),
    /// The mint is closed in the view.
    Empty,
    /// Keep the mint joined by the Postgres row.
    Joined,
    /// The account came from the view and the mint missed: read it at the anchor.
    Query,
}

/// Resolves a mint through the view before any Postgres data.
fn mint_source(mint_lookup: Lookup<'_>, account_from_postgres: bool) -> MintSource {
    match mint_lookup {
        Lookup::Live(mint) => MintSource::View(mint.data.clone()),
        Lookup::Closed => MintSource::Empty,
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
            MintSource::View(data) => *mint_data = Some(data),
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
    let query = bind_query(
        include_str!("../db/getMultipleAccounts.sql"),
        &bytea_array_literal(&mints),
        view.anchor_slot,
    );
    let span = tracing::info_span!("processed_mint_db");
    let rows = fetch_rows(state, &query, span, "processed mint").await?;
    let mut found = HashMap::with_capacity(rows.len());
    for row in &rows {
        let data = Arc::new(row.get::<Vec<u8>, _>("data"));
        found.insert(pubkey_column(row, "pubkey")?, data);
    }
    for (index, mint) in queried {
        if let KeyRead::Account { mint_data, .. } = &mut reads[index] {
            *mint_data = found.get(&mint).cloned();
        }
    }
    Ok(())
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

fn check_min_context_slot(
    view: &ProcessedView,
    min_context_slot: Option<u64>,
) -> Result<(), RpcError> {
    match min_context_slot {
        Some(min_context_slot) if view.slot < min_context_slot => {
            Err(RpcError::RpcSlotBehindMinContextSlot {
                rpc_slot: view.slot,
            })
        }
        _ => Ok(()),
    }
}

fn response<T>(view: &ProcessedView, value: T) -> RpcResponse<T> {
    RpcResponse {
        context: RpcResponseContext {
            slot: view.slot,
            api_version: None,
        },
        value,
    }
}

fn pubkey_column(row: &PgRow, column: &str) -> Result<Pubkey, RpcError> {
    let bytes: Vec<u8> = row.try_get(column).map_err(|e| {
        tracing::error!("processed read: missing {column} column: {e}");
        RpcError::InternalError
    })?;
    Pubkey::try_from(bytes.as_slice()).map_err(|_| RpcError::InternalError)
}

fn bytea_literal(pubkey: &Pubkey) -> String {
    format!("'\\x{}'::bytea", hex::encode(pubkey.as_ref()))
}

fn bytea_array_literal(pubkeys: &[Pubkey]) -> String {
    let literals: Vec<String> = pubkeys.iter().map(bytea_literal).collect();
    format!("ARRAY[{}]", literals.join(", "))
}

/// Substitutes `$1` with a key literal and `$2` with the bound, then adds the traceparent.
fn bind_query(template: &str, keys_literal: &str, bound: impl Display) -> String {
    let query = template
        .replace("$1", keys_literal)
        .replace("$2", &bound.to_string());
    db_query::add_trace_traceparent_to_query(&query)
}

/// Runs `query` under the API query timeout. `what` names the query in the timeout log.
async fn fetch_rows(
    state: &CloudbreakRpcState,
    query: &str,
    span: tracing::Span,
    what: &str,
) -> Result<Vec<PgRow>, RpcError> {
    let pool = state.database.get_postgres_connection_pool();
    timeout(
        state.queries_timeout,
        sqlx::raw_sql(query).fetch_all(pool).instrument(span),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::methods::{LEGACY_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};
    use crate::slot_syncronizer::SlotData;
    use solana_account_decoder_client_types::UiAccountData;

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
        assert_eq!(servable(None, &slots(100, 68, true)).err(), Some("no_view"));
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

    /// An initialized SPL mint with no authorities.
    fn mint_data(decimals: u8) -> Vec<u8> {
        let mut data = vec![0u8; 82];
        data[44] = decimals;
        data[45] = 1;
        data
    }

    /// An initialized SPL token account with no optional fields.
    fn token_account_data(mint: &Pubkey, amount: u64) -> Vec<u8> {
        let mut data = vec![0u8; 165];
        data[..32].copy_from_slice(mint.as_ref());
        data[32..64].copy_from_slice(Pubkey::new_unique().as_ref());
        data[64..72].copy_from_slice(&amount.to_le_bytes());
        data[108] = 1;
        data
    }

    #[test]
    fn view_mint_overrides_joined_mint() {
        let mint = live(TOKEN_2022_PROGRAM_ID, 1, mint_data(6));
        for from_postgres in [true, false] {
            assert_eq!(
                mint_source(Lookup::Live(&mint), from_postgres),
                MintSource::View(mint.data.clone())
            );
            assert_eq!(
                mint_source(Lookup::Closed, from_postgres),
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
