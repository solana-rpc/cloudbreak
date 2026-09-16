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

    let read = processed::account_read(state, config.commitment, "getMultipleAccounts")?;

    let (latest_slot, block_time) = read.slot_and_block_time(state).await?;

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

    // One entry per input pubkey, in order. None = the account does not exist
    // (or its latest version is closed).
    let accounts = processed::read_accounts(
        state,
        &read,
        &parsed_pubkeys,
        sql_template,
        latest_slot,
        with_mint,
        "getMultipleAccounts",
    )
    .await?;

    let mut result: Vec<Option<UiAccount>> = Vec::with_capacity(parsed_pubkeys.len());

    for (pubkey, account) in parsed_pubkeys.iter().zip(accounts) {
        let Some(account) = account else {
            result.push(None);
            continue;
        };

        let owner = account.owner;

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

        let additional_mint_data: Option<AccountAdditionalDataV3> =
            if with_mint && is_token_program(&owner) {
                if account.data.len() >= 32 {
                    let mint_pubkey = Pubkey::try_from(&account.data[..32])
                        .map_err(|_| RpcError::InternalError)?;
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

        check_account_data_len_for_encoding(encoding, data_slice, account.data.len(), pubkey)?;

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
