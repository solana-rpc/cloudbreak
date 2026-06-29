// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_entity::slots;
use sea_orm::EntityTrait;
use sea_orm::sqlx::Row;
use sea_orm::sqlx::{self};
use solana_account_decoder::parse_token::token_amount_to_ui_amount_v3;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{
    Response as RpcResponse, RpcResponseContext, RpcTokenAccountBalance,
};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::token::parse_additional_mint_data;
use crate::methods::{is_token_program, resolve_commitment};
use crate::{db_query, metrics};

#[tracing::instrument(name = "get_token_largest_accounts_rpc", skip_all, fields(pubkey = %mint))]
pub async fn get_token_largest_accounts(
    state: &CloudbreakRpcState,
    mint: String,
    commitment: Option<CommitmentConfig>,
) -> Result<RpcResponse<Vec<RpcTokenAccountBalance>>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getTokenLargestAccounts");

    let pubkey: Pubkey = mint
        .parse()
        .map_err(|_| RpcError::PubkeyValidationError(mint.clone()))?;

    let commitment = commitment
        .map(|commitment_config| {
            resolve_commitment(commitment_config.commitment, state.processed_commitment)
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (latest_slot, block_time): (u64, i64) = match &state.slot_syncronizer_data {
        Some(data) => {
            let data = data.read().expect("Failed to read slot syncronizer data");
            (
                data.get_slot_for_commitment(commitment),
                data.get_block_time_for_commitment(commitment),
            )
        }
        None => {
            let slot_model = slots::Entity::find_by_id(commitment as i32)
                .one(&state.database)
                .instrument(tracing::info_span!("slot_db"))
                .await?;

            let model = slot_model.ok_or(RpcError::InternalError)?;
            (model.slot as u64, model.block_time)
        }
    };

    let mint_sql = include_str!("../db/getAccountInfo.sql");
    let mint_hex = format!("'\\x{}'::bytea", hex::encode(pubkey.as_ref()));
    let mint_sql = mint_sql.replace("$1", &mint_hex);
    let mint_sql = mint_sql.replace("$2", &latest_slot.to_string());
    let mint_sql = db_query::add_trace_traceparent_to_query(&mint_sql);

    tracing::debug!(target: "get_token_mint_sql", "## sql: {}", mint_sql);

    let pool = state.database.get_postgres_connection_pool();
    let mint_rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("get_token_mint_db");
        sqlx::raw_sql(&mint_sql)
            .fetch_all(pool)
            .instrument(span)
            .await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getTokenLargestAccounts mint lookup timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    let Some(mint_row) = mint_rows.first() else {
        return Err(RpcError::AccountNotFound {
            pubkey: pubkey.to_string(),
        });
    };
    let owner_bytes: Vec<u8> = mint_row.get("owner");
    let owner = Pubkey::try_from(owner_bytes.as_slice()).map_err(|_| RpcError::InternalError)?;
    if !state.indexer_filter.is_program_selected(&owner) {
        return Err(RpcError::AccountOwnerExcluded {
            pubkey: pubkey.to_string(),
            owner: owner.to_string(),
        });
    }
    if !is_token_program(&owner) {
        return Err(RpcError::NotATokenAccount {
            pubkey: pubkey.to_string(),
        });
    }
    let data: Vec<u8> = mint_row.get("data");
    let additional_mint_data = parse_additional_mint_data(&pubkey, &data, block_time);
    let additional_data = additional_mint_data
        .as_ref()
        .and_then(|d| d.spl_token_additional_data.as_ref())
        .ok_or_else(|| RpcError::MintDataNotFound {
            mint: pubkey.to_string(),
        })?;

    let sql_template = include_str!("../db/getTokenLargestAccounts.sql");
    let sql = sql_template.replace("$1", &mint_hex);
    let sql = sql.replace("$2", &latest_slot.to_string());
    let sql = db_query::add_trace_traceparent_to_query(&sql);

    tracing::debug!(target: "get_token_largest_accounts_sql", "## sql: {}", sql);

    let holder_rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("get_token_largest_accounts_db");
        sqlx::raw_sql(&sql).fetch_all(pool).instrument(span).await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getTokenLargestAccounts holder query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    let mut value = Vec::with_capacity(holder_rows.len());
    for row in &holder_rows {
        let address_bytes: Vec<u8> = row.get("pubkey");
        let address = Pubkey::try_from(address_bytes.as_slice())
            .map_err(|_| RpcError::InternalError)?
            .to_string();

        let amount_bytes: Vec<u8> = row.get("amount");
        let amount_array: [u8; 8] = amount_bytes
            .as_slice()
            .try_into()
            .map_err(|_| RpcError::InternalError)?;
        let amount = u64::from_le_bytes(amount_array);

        value.push(RpcTokenAccountBalance {
            address,
            amount: token_amount_to_ui_amount_v3(amount, additional_data),
        });
    }

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: latest_slot,
            api_version: None,
        },
        value,
    })
}
