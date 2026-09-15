// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::sqlx::Row;
use sea_orm::sqlx::{self};
use solana_account_decoder::parse_token::token_amount_to_ui_amount_v3;
use solana_account_decoder_client_types::token::UiTokenAmount;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::token::parse_additional_mint_data;
use crate::methods::{is_token_program, resolve_commitment};
use crate::{db_query, metrics};

#[tracing::instrument(name = "get_token_account_balance_rpc", skip_all, fields(pubkey = %pubkey))]
pub async fn get_token_account_balance(
    state: &CloudbreakRpcState,
    pubkey: String,
    commitment: Option<CommitmentConfig>,
) -> Result<RpcResponse<UiTokenAmount>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getTokenAccountBalance");

    let pubkey: Pubkey = pubkey
        .parse()
        .map_err(|_| RpcError::PubkeyValidationError(pubkey.clone()))?;

    let commitment = commitment
        .map(|commitment_config| {
            resolve_commitment(commitment_config.commitment, state.processed_commitment)
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (latest_slot, block_time) = state.latest_slot_and_block_time(commitment).await?;

    let sql_template = include_str!("../db/getTokenAccountBalance.sql");
    let pubkey_hex = format!("'\\x{}'::bytea", hex::encode(pubkey.as_ref()));
    let sql = sql_template.replace("$1", &pubkey_hex);
    let sql = sql.replace("$2", &latest_slot.to_string());
    let sql = db_query::add_trace_traceparent_to_query(&sql);

    tracing::debug!(target: "get_token_account_balance_sql", "## sql: {}", sql);

    let pool = state.database.get_postgres_connection_pool();
    let rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("get_token_account_balance_db");
        sqlx::raw_sql(&sql).fetch_all(pool).instrument(span).await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getTokenAccountBalance query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    let Some(row) = rows.first() else {
        // Account not in DB (or its latest version was closed)
        return Err(RpcError::AccountNotFound {
            pubkey: pubkey.to_string(),
        });
    };

    let owner_bytes: Vec<u8> = row.get("owner");
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

    // Amount: u64 LE at bytes 64..72 of the token account data. The SQL guarantees
    // exactly 8 bytes for token-owned accounts (and 8 zero bytes for anything else,
    // which we've already rejected above).
    let amount_bytes: Vec<u8> = row.get("amount");
    let amount_array: [u8; 8] = amount_bytes.as_slice().try_into().map_err(|_| {
        tracing::error!(
            "getTokenAccountBalance: unexpected amount length {} for pubkey {}",
            amount_bytes.len(),
            pubkey
        );
        RpcError::InternalError
    })?;
    let amount = u64::from_le_bytes(amount_array);

    // Mint pubkey from the generated token_mint column (bytes 0..32 of data).
    let mint_pubkey_bytes: Vec<u8> = row.try_get("token_mint").map_err(|e| {
        tracing::error!(
            "getTokenAccountBalance: missing token_mint for pubkey {}: {}",
            pubkey,
            e
        );
        RpcError::InternalError
    })?;
    let mint_pubkey =
        Pubkey::try_from(mint_pubkey_bytes.as_slice()).map_err(|_| RpcError::InternalError)?;

    // Pass mint_data (or empty) unconditionally so the WSOL native_mint short-circuit
    // can hardcode decimals=9 even when the mint account itself isn't in our DB —
    // same trick we use in gAI / gTABO.
    let mint_data: Vec<u8> = row.try_get("mint_data").ok().unwrap_or_default();
    let additional_mint_data = parse_additional_mint_data(&mint_pubkey, &mint_data, block_time);

    let additional_data = additional_mint_data
        .as_ref()
        .and_then(|d| d.spl_token_additional_data.as_ref())
        .ok_or_else(|| RpcError::MintDataNotFound {
            mint: mint_pubkey.to_string(),
        })?;

    let ui_token_amount = token_amount_to_ui_amount_v3(amount, additional_data);

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: latest_slot,
            api_version: None,
        },
        value: ui_token_amount,
    })
}
