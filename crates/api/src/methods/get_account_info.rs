// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use solana_account::AccountSharedData;
use solana_account_decoder::parse_account_data::AccountAdditionalDataV3;
use solana_account_decoder::{UiAccountEncoding, encode_ui_account};
use solana_account_decoder_client_types::UiAccount;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::token::{check_account_data_len_for_encoding, parse_additional_mint_data};
use crate::methods::{is_token_program, processed};
use crate::metrics;

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

    let read = processed::account_read(state, config.commitment, "gAI")?;

    let (latest_slot, block_time) = read.slot_and_block_time(state).await?;

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

    let accounts = processed::read_accounts(
        state,
        &read,
        std::slice::from_ref(&pubkey),
        sql_template,
        latest_slot,
        with_mint,
        "getAccountInfo",
    )
    .await?;

    let Some(account) = accounts.into_iter().next().flatten() else {
        // No account for this pubkey (absent, or its newest version is closed). Account not found.
        return Ok(RpcResponse {
            context: RpcResponseContext {
                slot: latest_slot,
                api_version: None,
            },
            value: None,
        });
    };

    let owner = account.owner;

    // Post-query indexer-filter check: if this owner is excluded by the current indexer filter error.
    if !state.indexer_filter.is_program_selected(&owner) {
        return Err(RpcError::AccountOwnerExcluded {
            pubkey: pubkey.to_string(),
            owner: owner.to_string(),
        });
    }

    // For jsonParsed encoding the mint's data came with the account read.
    //
    // We pass the mint pubkey to parse_additional_mint_data unconditionally (with empty
    // mint_data if the mint was not found): that way the function's native_mint
    // short-circuit can still hardcode decimals for WSOL.
    let additional_mint_data: Option<AccountAdditionalDataV3> =
        if with_mint && is_token_program(&owner) {
            if let Some(mint_pubkey) = get_token_mint_from_data(&account.data) {
                parse_additional_mint_data(&mint_pubkey, account.mint_data(), block_time)
            } else {
                None
            }
        } else {
            None
        };

    let account_shared_data = AccountSharedData::create_from_existing_shared_data(
        account.lamports,
        account.data.clone(),
        owner,
        account.executable,
        account.rent_epoch,
    );

    check_account_data_len_for_encoding(encoding, data_slice, account.data.len(), &pubkey)?;

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
