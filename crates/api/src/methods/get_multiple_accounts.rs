// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::HashMap;
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

#[tracing::instrument(name = "gma_rpc", skip_all, fields(num_pubkeys = pubkeys.len()))]
pub async fn get_multiple_accounts(
    state: &CloudbreakRpcState,
    pubkeys: Vec<String>,
    config: Option<RpcAccountInfoConfig>,
) -> Result<RpcResponse<Vec<Option<UiAccount>>>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("gma");

    let max_multiple_accounts = state.max_multiple_accounts;
    if pubkeys.len() > max_multiple_accounts {
        return Err(RpcError::InvalidParamsWithMessage(format!(
            "Too many inputs provided; max {max_multiple_accounts}"
        )));
    }

    let config = config.unwrap_or_default();

    // Validate all pubkeys up-front. Any failure fails the whole call
    let parsed_pubkeys: Vec<Pubkey> = pubkeys
        .iter()
        .map(|pk| {
            pk.parse::<Pubkey>()
                .map_err(|_| RpcError::PubkeyValidationError(pk.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;

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

    // Short-circuit for an empty input list, return `value: []` without touching the DB.
    if parsed_pubkeys.is_empty() {
        return Ok(RpcResponse {
            context: RpcResponseContext {
                slot: latest_slot,
                api_version: None,
            },
            value: vec![],
        });
    }

    let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64);
    let data_slice = config.data_slice;
    let with_mint = encoding == UiAccountEncoding::JsonParsed;

    let sql_template = if with_mint {
        include_str!("../db/getMultipleAccountsWithMintData.sql")
    } else {
        include_str!("../db/getMultipleAccounts.sql")
    };

    // Build the bytea[] array literal
    let mut array_literal = String::with_capacity(parsed_pubkeys.len() * 95 + 32);
    array_literal.push_str("ARRAY[");
    for (i, pk) in parsed_pubkeys.iter().enumerate() {
        if i > 0 {
            array_literal.push_str(", ");
        }
        array_literal.push_str(&format!("'\\x{}'::bytea", hex::encode(pk.as_ref())));
    }
    array_literal.push(']');

    let sql = sql_template.replace("$1", &array_literal);
    let sql = sql.replace("$2", &latest_slot.to_string());
    let sql = db_query::add_trace_traceparent_to_query(&sql);

    tracing::debug!(target: "gma_sql", "## sql: {}", sql);

    let pool = state.database.get_postgres_connection_pool();
    let rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("gma_db");
        sqlx::raw_sql(&sql).fetch_all(pool).instrument(span).await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getMultipleAccounts query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    // Build a (pubkey -> row) lookup. The SQL only returns rows for input pubkeys
    // that exist AND are live (lamports > 0).
    let mut row_by_pubkey: HashMap<Pubkey, &_> = HashMap::with_capacity(rows.len());
    for row in &rows {
        let pubkey_bytes: Vec<u8> = row.get("pubkey");
        let row_pubkey = Pubkey::try_from(pubkey_bytes.as_slice()).map_err(|_| {
            tracing::error!("getMultipleAccounts: invalid pubkey bytes returned by DB");
            RpcError::InternalError
        })?;
        row_by_pubkey.insert(row_pubkey, row);
    }

    let mut result: Vec<Option<UiAccount>> = Vec::with_capacity(parsed_pubkeys.len());

    for pubkey in &parsed_pubkeys {
        // Missing in the map = account doesn't exist (or its latest version is closed).
        let Some(&row) = row_by_pubkey.get(pubkey) else {
            result.push(None);
            continue;
        };

        let owner_bytes: Vec<u8> = row.get("owner");
        let owner =
            Pubkey::try_from(owner_bytes.as_slice()).map_err(|_| RpcError::InternalError)?;

        // Per-position indexer-filter check: if the owner is excluded, we return None at that position and log a tracing error.
        if !state.indexer_filter.is_program_selected(&owner) {
            tracing::error!(
                target: "gma_indexer_filter",
                pubkey = %pubkey,
                owner = %owner,
                "getMultipleAccounts: skipping account because owner is excluded by the current indexer filter"
            );
            result.push(None);
            continue;
        }

        let lamports = row.get::<i64, _>("lamports") as u64;
        let executable: bool = row.get("executable");
        let rent_epoch = row
            .get::<rust_decimal::Decimal, _>("rent_epoch")
            .to_u64()
            .unwrap_or(0);
        let data: Vec<u8> = row.get("data");

        let additional_mint_data: Option<AccountAdditionalDataV3> =
            if with_mint && is_token_program(&owner) {
                if data.len() >= 32 {
                    let mint_pubkey =
                        Pubkey::try_from(&data[..32]).map_err(|_| RpcError::InternalError)?;
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

        check_account_data_len_for_encoding(encoding, data_slice, data.len(), pubkey)?;

        let ui_account = encode_ui_account(
            pubkey,
            &account_shared_data,
            encoding,
            additional_mint_data,
            data_slice,
        );

        result.push(Some(ui_account));
    }

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: latest_slot,
            api_version: None,
        },
        value: result,
    })
}
