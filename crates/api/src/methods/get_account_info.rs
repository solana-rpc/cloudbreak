// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::sync::Arc;

use rust_decimal::prelude::ToPrimitive;
use sea_orm::sqlx::Row;
use sea_orm::sqlx::{self};
use solana_account::AccountSharedData;
use solana_account_decoder::parse_account_data::AccountAdditionalDataV3;
use solana_account_decoder::{UiAccountEncoding, encode_ui_account};
use solana_account_decoder_client_types::UiAccount;
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::token::{check_account_data_len_for_encoding, parse_additional_mint_data};
use crate::methods::{is_token_program, resolve_commitment};
use crate::{db_query, metrics};

#[tracing::instrument(name = "gai_rpc", skip_all, fields(pubkey = %pubkey))]
pub async fn get_account_info(
    state: &CloudbreakRpcState,
    pubkey: String,
    config: Option<RpcAccountInfoConfig>,
) -> Result<RpcResponse<Option<UiAccount>>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("gai");

    let config = config.unwrap_or_default();

    let pubkey: Pubkey = pubkey
        .parse()
        .map_err(|_| RpcError::PubkeyValidationError(pubkey.clone()))?;

    let commitment = config
        .commitment
        .map(|commitment_config| {
            resolve_commitment(commitment_config.commitment, state.processed_commitment)
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (latest_slot, block_time) = state.latest_slot_and_block_time(commitment).await?;

    if let Some(min_context_slot) = config.min_context_slot
        && latest_slot < min_context_slot
    {
        return Err(RpcError::RpcSlotBehindMinContextSlot {
            rpc_slot: latest_slot,
        });
    }

    let encoding = config.encoding.unwrap_or(UiAccountEncoding::Binary);
    let data_slice = config.data_slice;
    let with_mint = encoding == UiAccountEncoding::JsonParsed;

    // Pick which SQL to run depending on whether jsonParsed needs the mint JOIN.
    let sql_template = if with_mint {
        include_str!("../db/getAccountInfoWithMintData.sql")
    } else {
        include_str!("../db/getAccountInfo.sql")
    };

    let pubkey_hex = format!("'\\x{}'::bytea", hex::encode(pubkey.as_ref()));
    let sql = sql_template.replace("$1", &pubkey_hex);
    let sql = sql.replace("$2", &latest_slot.to_string());
    let sql = db_query::add_trace_traceparent_to_query(&sql);

    tracing::debug!(target: "gai_sql", "## sql: {}", sql);

    let pool = state.database.get_postgres_connection_pool();
    let rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("gai_db");
        sqlx::raw_sql(&sql).fetch_all(pool).instrument(span).await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getAccountInfo query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    let Some(row) = rows.first() else {
        // No row for this pubkey (or its only versions had lamports = 0). Account not found.
        return Ok(RpcResponse {
            context: RpcResponseContext {
                slot: latest_slot,
                api_version: None,
            },
            value: None,
        });
    };

    let owner_bytes: Vec<u8> = row.get("owner");
    let owner = Pubkey::try_from(owner_bytes.as_slice()).map_err(|_| RpcError::InternalError)?;

    // Post-query indexer-filter check: if this owner is excluded by the current indexer filter error.
    if !state.indexer_filter.is_program_selected(&owner) {
        return Err(RpcError::AccountOwnerExcluded {
            pubkey: pubkey.to_string(),
            owner: owner.to_string(),
        });
    }

    let lamports = row.get::<i64, _>("lamports") as u64;
    let executable: bool = row.get("executable");
    let rent_epoch = row
        .get::<rust_decimal::Decimal, _>("rent_epoch")
        .to_u64()
        .unwrap_or(0);
    let data: Vec<u8> = row.get("data");

    // For jsonParsed encoding we may have fetched the mint's data in the same SQL roundtrip.
    //
    // We pass the mint pubkey to parse_additional_mint_data unconditionally (with empty
    // mint_data if the join didn't return a row): that way the function's native_mint
    // short-circuit can still hardcode decimals for WSOL.
    let additional_mint_data: Option<AccountAdditionalDataV3> =
        if with_mint && is_token_program(&owner) {
            if let Some(mint_pubkey) = get_token_mint_from_data(&data) {
                let mint_data: Vec<u8> = row.try_get("mint_data").ok().unwrap_or_default();
                parse_additional_mint_data(&mint_pubkey, &mint_data, block_time)
            } else {
                None
            }
        } else {
            None
        };

    let account_shared_data = AccountSharedData::create_from_existing_shared_data(
        lamports,
        Arc::new(data.clone()),
        owner,
        executable,
        rent_epoch,
    );

    check_account_data_len_for_encoding(encoding, data_slice, data.len(), &pubkey)?;

    // encode_ui_account computes `space = data.len()` BEFORE applying dataSlice, so we pass
    // the full data and let it slice — keeps `space` honest, matching Agave.
    let ui_account = encode_ui_account(
        &pubkey,
        &account_shared_data,
        encoding,
        additional_mint_data,
        data_slice,
    );

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: latest_slot,
            api_version: None,
        },
        value: Some(ui_account),
    })
}

/// Extracts the mint pubkey (bytes 0..32) from a token account's raw data.
/// Returns None if the data is shorter than 32 bytes.
fn get_token_mint_from_data(data: &[u8]) -> Option<Pubkey> {
    if data.len() < 32 {
        return None;
    }
    Pubkey::try_from(&data[..32]).ok()
}
