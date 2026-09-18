// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use solana_account_decoder::parse_token::token_amount_to_ui_amount_v3;
use solana_account_decoder_client_types::token::UiTokenAmount;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::token::parse_additional_mint_data;
use crate::methods::{is_token_program, processed};
use crate::metrics;

#[tracing::instrument(name = "get_token_account_balance_rpc", skip_all, fields(pubkey = %pubkey))]
pub async fn get_token_account_balance(
    state: &CloudbreakRpcState,
    pubkey: String,
    commitment: Option<CommitmentConfig>,
) -> Result<RpcResponse<UiTokenAmount>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getTokenAccountBalance");

    let pubkey: Pubkey = pubkey
        .parse()
        .map_err(|e| RpcError::PubkeyValidationError(format!("{e:?}")))?;

    let read = processed::read(state, commitment, "getTokenAccountBalance")?;

    let (latest_slot, block_time) = read.slot_and_block_time(state).await?;

    // The mint JOIN supplies the mint data the balance needs.
    let sql_template = include_str!("../db/getAccountInfoWithMintData.sql");
    let accounts = processed::read_accounts(
        state,
        &read,
        std::slice::from_ref(&pubkey),
        sql_template,
        latest_slot,
        true,
        "getTokenAccountBalance",
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

    // Amount: u64 LE at bytes 64..72 of the token account data. Shorter data
    // yields fewer bytes, as SUBSTRING does, and fails the conversion below.
    let amount_bytes = sql_substring(&account.data, 64, 8);
    let amount_array: [u8; 8] = amount_bytes.try_into().map_err(|_| {
        tracing::error!(
            "getTokenAccountBalance: unexpected amount length {} for pubkey {}",
            amount_bytes.len(),
            pubkey
        );
        RpcError::InternalError
    })?;
    let amount = u64::from_le_bytes(amount_array);

    // Mint pubkey from bytes 0..32 of the token account data.
    let mint_pubkey = Pubkey::try_from(sql_substring(&account.data, 0, 32)).map_err(|_| {
        tracing::error!(
            "getTokenAccountBalance: invalid token mint for pubkey {}",
            pubkey
        );
        RpcError::InternalError
    })?;

    // Empty mint data means the mint row is missing or closed. WSOL needs none:
    // parse_additional_mint_data hardcodes its decimals.
    let mint_data = account.mint_data();
    if mint_data.is_empty() && mint_pubkey != spl_token_interface::native_mint::id() {
        return Err(RpcError::MintDataNotFound {
            mint: mint_pubkey.to_string(),
        });
    }
    let additional_mint_data = parse_additional_mint_data(&mint_pubkey, mint_data, block_time);

    let additional_data = additional_mint_data
        .as_ref()
        .and_then(|d| d.spl_token_additional_data.as_ref())
        .ok_or_else(|| RpcError::TokenMintCouldNotBeUnpacked {
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

/// The bytes `SUBSTRING(data FROM start + 1 FOR len)` returns: shorter when data ends early.
fn sql_substring(data: &[u8], start: usize, len: usize) -> &[u8] {
    let end = (start + len).min(data.len());
    &data[start.min(end)..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_substring_matches_postgres_on_short_data() {
        let data = [1u8, 2, 3, 4, 5];
        assert_eq!(sql_substring(&data, 1, 2), &[2, 3]);
        assert_eq!(sql_substring(&data, 3, 8), &[4, 5]);
        assert!(sql_substring(&data, 64, 8).is_empty());
    }
}
