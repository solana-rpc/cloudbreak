// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use solana_account_decoder::parse_token::token_amount_to_ui_amount_v3;
use solana_account_decoder_client_types::token::UiTokenAmount;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use spl_token_2022::extension::StateWithExtensions;
use spl_token_2022::state::Mint;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::token::parse_additional_mint_data;
use crate::methods::{is_token_program, processed};
use crate::metrics;

#[tracing::instrument(name = "get_token_supply_rpc", skip_all, fields(pubkey = %mint))]
pub async fn get_token_supply(
    state: &CloudbreakRpcState,
    mint: String,
    commitment: Option<CommitmentConfig>,
) -> Result<RpcResponse<UiTokenAmount>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getTokenSupply");

    let pubkey: Pubkey = mint
        .parse()
        .map_err(|_| RpcError::PubkeyValidationError(mint.clone()))?;

    let read = processed::read(state, commitment, "getTokenSupply")?;

    let (latest_slot, block_time) = read.slot_and_block_time(state).await?;

    let sql_template = include_str!("../db/getAccountInfo.sql");
    let accounts = processed::read_accounts(
        state,
        &read,
        std::slice::from_ref(&pubkey),
        sql_template,
        latest_slot,
        false,
        "getTokenSupply",
    )
    .await?;

    let Some(account) = accounts.into_iter().next().flatten() else {
        // Account not in DB (or its latest version was closed)
        return Err(RpcError::AccountNotFound {
            pubkey: pubkey.to_string(),
        });
    };

    let owner = account.owner;

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

    let data: &[u8] = &account.data;

    let mint_state =
        StateWithExtensions::<Mint>::unpack(data).map_err(|_| RpcError::MintDataNotFound {
            mint: pubkey.to_string(),
        })?;

    let supply = mint_state.base.supply;
    let additional_mint_data = parse_additional_mint_data(&pubkey, data, block_time);
    let additional_data = additional_mint_data
        .as_ref()
        .and_then(|d| d.spl_token_additional_data.as_ref())
        .ok_or_else(|| RpcError::MintDataNotFound {
            mint: pubkey.to_string(),
        })?;

    let ui_token_amount = token_amount_to_ui_amount_v3(supply, additional_data);

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: latest_slot,
            api_version: None,
        },
        value: ui_token_amount,
    })
}
