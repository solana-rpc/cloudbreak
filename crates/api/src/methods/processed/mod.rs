// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Processed commitment for getAccountInfo, getMultipleAccounts, getBalance and
//! getTokenAccountBalance.
//!
//! Each method calls [`route`] first. With `[processed-accounts]` enabled, a
//! processed request takes one [`ProcessedView`] ([`Route::View`]) and the method
//! returns early into this module. When the core module reports a degrade reason,
//! or the view head is below the cached confirmed slot, the request takes the
//! confirmed path ([`Route::Db`]), even with `processed-commitment = "reject"`.
//! Every other request resolves through [`resolve_commitment`].
//!
//! On a view, a key written in the chain is answered from memory. A miss reads
//! Postgres at `slot <= view.anchor_slot`. `Lookup::Closed` never falls back to
//! Postgres, and `Lookup::Excluded` answers like the owner check on a Postgres
//! row. `context.slot` and the block time come from the head block. A jsonParsed
//! token account resolves its mint through the view first:
//!
//! | Mint lookup | Account from the overlay | Account from Postgres |
//! |---|---|---|
//! | `Live` | overlay mint data | overlay mint data, overriding the joined mint |
//! | `Closed` or `Excluded` | no mint data | no mint data |
//! | `Miss` | one `getMultipleAccounts.sql` read at the anchor | the joined mint |
//!
//! # Layout
//!
//! - `mod.rs`: [`Route`], [`route`], lookup metrics and the shared SQL helpers.
//! - `accounts.rs`: getAccountInfo and getMultipleAccounts, mint resolution and
//!   account encoding.
//! - `balance.rs`: getBalance and getTokenAccountBalance.

mod accounts;
mod balance;

pub(crate) use accounts::{get_account_info, get_multiple_accounts};
pub(crate) use balance::{get_balance, get_token_account_balance};

use std::fmt::Display;
use std::sync::Arc;

use cloudbreak_core::ProcessedCommitmentBehavior;
use cloudbreak_core::modules::processed::{
    DegradeReason, Lookup, ProcessedAccounts, ProcessedView,
};
use sea_orm::sqlx::{self, Row, postgres::PgRow};
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::resolve_commitment;
use crate::{db_query, metrics};

/// Where one request is served from.
#[derive(Debug)]
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
    let cached_confirmed = if commitment == Some(CommitmentLevel::Processed) {
        state.slot_syncronizer_data.as_ref().map(|data| {
            data.read()
                .expect("Failed to read slot syncronizer data")
                .confirmed_slot
                .slot
        })
    } else {
        None
    };
    route_with(
        &state.processed,
        state.processed_commitment,
        commitment,
        cached_confirmed,
        method,
    )
}

fn route_with(
    processed: &ProcessedAccounts,
    behavior: ProcessedCommitmentBehavior,
    commitment: Option<CommitmentLevel>,
    cached_confirmed: Option<u64>,
    method: &str,
) -> Result<Route, RpcError> {
    if commitment == Some(CommitmentLevel::Processed) && processed.is_enabled() {
        let view = processed.view().and_then(|view| {
            head_not_behind_confirmed(view.slot, cached_confirmed)?;
            Ok(view)
        });
        let route = match view {
            Ok(view) => {
                count_route(method, "view", "none");
                Route::View(view)
            }
            Err(reason) => {
                count_route(method, "degraded", reason.as_str());
                Route::Db(CommitmentLevel::Confirmed)
            }
        };
        return Ok(route);
    }
    let level = commitment
        .map(|commitment| resolve_commitment(commitment, behavior))
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);
    Ok(Route::Db(level))
}

/// A head below the cached confirmed slot would answer older than confirmed.
fn head_not_behind_confirmed(
    head_slot: u64,
    cached_confirmed: Option<u64>,
) -> Result<(), DegradeReason> {
    match cached_confirmed {
        Some(confirmed) if head_slot < confirmed => Err(DegradeReason::HeadNotAhead),
        _ => Ok(()),
    }
}

fn count_route(method: &str, route: &str, reason: &str) {
    metrics::CLOUDBREAK_API_PROCESSED_REQUESTS_TOTAL
        .with_label_values(&[method, route, reason])
        .inc();
}

/// Counts the requested keys of one request by lookup source.
fn record_lookups(method: &str, lookups: &[Lookup<'_>]) {
    let mut counts = [("live", 0), ("closed", 0), ("excluded", 0), ("postgres", 0)];
    for lookup in lookups {
        let source = match lookup {
            Lookup::Live(_) => 0,
            Lookup::Closed => 1,
            Lookup::Excluded(_) => 2,
            Lookup::Miss => 3,
        };
        counts[source].1 += 1;
    }
    for (source, count) in counts {
        if count > 0 {
            metrics::CLOUDBREAK_API_PROCESSED_LOOKUPS_TOTAL
                .with_label_values(&[method, source])
                .inc_by(count);
        }
    }
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

fn owner_excluded(pubkey: &Pubkey, owner: &Pubkey) -> RpcError {
    RpcError::AccountOwnerExcluded {
        pubkey: pubkey.to_string(),
        owner: owner.to_string(),
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
mod fixtures {
    use cloudbreak_core::modules::processed::LiveAccount;
    use solana_pubkey::Pubkey;
    use std::sync::Arc;

    pub(super) fn live(owner: Pubkey, lamports: u64, data: Vec<u8>) -> LiveAccount {
        LiveAccount {
            lamports,
            owner,
            executable: false,
            rent_epoch: u64::MAX,
            data: Arc::new(data),
        }
    }

    /// An initialized SPL mint with no authorities.
    pub(super) fn mint_data(decimals: u8) -> Vec<u8> {
        let mut data = vec![0u8; 82];
        data[44] = decimals;
        data[45] = 1;
        data
    }

    /// An initialized SPL token account with no optional fields.
    pub(super) fn token_account_data(mint: &Pubkey, amount: u64) -> Vec<u8> {
        let mut data = vec![0u8; 165];
        data[..32].copy_from_slice(mint.as_ref());
        data[32..64].copy_from_slice(Pubkey::new_unique().as_ref());
        data[64..72].copy_from_slice(&amount.to_le_bytes());
        data[108] = 1;
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudbreak_core::AccountSelectorConfig;

    fn enabled_handle() -> ProcessedAccounts {
        let config = serde_json::from_value(serde_json::json!({
            "enabled": true,
            "endpoint": "http://grpc:10000",
        }))
        .unwrap();
        ProcessedAccounts::from_config(Some(&config), Arc::new(AccountSelectorConfig::default()))
            .unwrap()
    }

    fn level(route: Result<Route, RpcError>) -> Result<CommitmentLevel, String> {
        match route {
            Ok(Route::Db(level)) => Ok(level),
            Ok(Route::View(_)) => Err("view".to_string()),
            Err(e) => Err(e.to_error_code().to_string()),
        }
    }

    #[test]
    fn disabled_handle_routes_like_resolve_commitment() {
        let disabled = ProcessedAccounts::default();
        let behaviors = [
            ProcessedCommitmentBehavior::Reject,
            ProcessedCommitmentBehavior::UseConfirmed,
        ];
        let commitments = [
            None,
            Some(CommitmentLevel::Processed),
            Some(CommitmentLevel::Confirmed),
            Some(CommitmentLevel::Finalized),
        ];
        for behavior in behaviors {
            for commitment in commitments {
                for cached_confirmed in [None, Some(100)] {
                    let expected = commitment
                        .map(|c| resolve_commitment(c, behavior))
                        .transpose()
                        .map(|c| c.unwrap_or(CommitmentLevel::Finalized))
                        .map_err(|e| e.to_error_code().to_string());
                    let routed = level(route_with(
                        &disabled,
                        behavior,
                        commitment,
                        cached_confirmed,
                        "gAI",
                    ));
                    assert_eq!(routed, expected, "{behavior:?} {commitment:?}");
                }
            }
        }
    }

    #[test]
    fn degraded_view_serves_confirmed_even_with_reject() {
        let handle = enabled_handle();
        assert_eq!(handle.view().unwrap_err(), DegradeReason::NotWarm);
        let reject = ProcessedCommitmentBehavior::Reject;
        let processed = Some(CommitmentLevel::Processed);
        assert_eq!(
            level(route_with(&handle, reject, processed, Some(100), "gAI")),
            Ok(CommitmentLevel::Confirmed)
        );
        // Other commitments keep resolve_commitment on an enabled handle.
        for commitment in [None, Some(CommitmentLevel::Confirmed)] {
            let expected = commitment.unwrap_or(CommitmentLevel::Finalized);
            assert_eq!(
                level(route_with(&handle, reject, commitment, Some(100), "gAI")),
                Ok(expected)
            );
        }
    }

    #[test]
    fn head_below_cached_confirmed_degrades() {
        assert_eq!(
            head_not_behind_confirmed(99, Some(100)),
            Err(DegradeReason::HeadNotAhead)
        );
        assert_eq!(head_not_behind_confirmed(100, Some(100)), Ok(()));
        assert_eq!(head_not_behind_confirmed(103, Some(100)), Ok(()));
        assert_eq!(head_not_behind_confirmed(5, None), Ok(()));
    }
}
