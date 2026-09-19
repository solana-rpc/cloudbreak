// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::modules::processed::ProcessedAccount;
use sea_orm::sqlx::Row;
use sea_orm::sqlx::{self};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcContextConfig;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::processed;
use crate::{db_query, metrics};

#[tracing::instrument(name = "get_balance_rpc", skip_all, fields(pubkey = %pubkey))]
pub async fn get_balance(
    state: &CloudbreakRpcState,
    pubkey: String,
    config: Option<RpcContextConfig>,
) -> Result<RpcResponse<u64>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getBalance");

    let config = config.unwrap_or_default();

    let pubkey: Pubkey = pubkey
        .parse()
        .map_err(|e| RpcError::PubkeyValidationError(format!("{e:?}")))?;

    let read = processed::read(state, config.commitment, "getBalance")?;

    // A key written in the blocks answers from memory at the head slot.
    if let Some(blocks) = &read.blocks {
        let span = blocks.read_span("getBalance");
        let account = span.in_scope(|| blocks.get_account(&pubkey));
        let lamports = match account {
            ProcessedAccount::Live(account) => Some(account.lamports),
            ProcessedAccount::Closed => Some(0),
            ProcessedAccount::Unknown => None,
        };
        if let Some(lamports) = lamports {
            if let Some(min_context_slot) = config.min_context_slot
                && blocks.slot < min_context_slot
            {
                return Err(RpcError::MinContextSlotNotReached { context_slot: blocks.slot });
            }
            return Ok(RpcResponse {
                context: RpcResponseContext {
                    slot: blocks.slot,
                    api_version: None,
                },
                value: lamports,
            });
        }
    }

    // The slot bound is the blocks anchor, or the slot at the commitment.
    let slot_bound = match &read.blocks {
        Some(blocks) => blocks.anchor_slot.to_string(),
        None => format!(
            "(SELECT slot FROM slots WHERE commitment = {})",
            read.commitment as i32
        ),
    };

    let sql_template = include_str!("../db/getBalance.sql");
    let pubkey_hex = format!("'\\x{}'::bytea", hex::encode(pubkey.as_ref()));
    let sql = sql_template.replace("$1", &pubkey_hex);
    let sql = sql.replace("$2", &slot_bound);
    let sql = db_query::add_trace_traceparent_to_query(&sql);

    tracing::debug!(target: "get_balance_sql", "## sql: {}", sql);

    let pool = state.database.get_postgres_connection_pool();
    let rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("get_balance_db");
        sqlx::raw_sql(&sql).fetch_all(pool).instrument(span).await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getBalance query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    let row = rows.first().ok_or_else(|| {
        tracing::error!(
            "getBalance: slots table missing entry for commitment {:?}",
            read.commitment
        );
        RpcError::InternalError
    })?;

    // With blocks the context slot is the head slot.
    let context_slot = match &read.blocks {
        Some(blocks) => blocks.slot,
        None => row.get::<i64, _>("context_slot") as u64,
    };

    if let Some(min_context_slot) = config.min_context_slot
        && context_slot < min_context_slot
    {
        return Err(RpcError::MinContextSlotNotReached { context_slot });
    }

    // `owner` and `lamports` are nullable when the LEFT JOIN finds no
    // matching account row (account not found or closed at all relevant slots).
    let owner_bytes: Option<Vec<u8>> = row.try_get("owner").ok();
    let lamports: Option<i64> = row.try_get("lamports").ok();

    let (Some(owner_bytes), Some(lamports)) = (owner_bytes, lamports) else {
        return Ok(RpcResponse {
            context: RpcResponseContext {
                slot: context_slot,
                api_version: None,
            },
            value: 0,
        });
    };

    let owner = Pubkey::try_from(owner_bytes.as_slice()).map_err(|_| RpcError::InternalError)?;

    if !state.indexer_filter.is_program_selected(&owner) {
        return Err(RpcError::AccountOwnerExcluded {
            pubkey: pubkey.to_string(),
            owner: owner.to_string(),
        });
    }

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: context_slot,
            api_version: None,
        },
        value: lamports as u64,
    })
}
