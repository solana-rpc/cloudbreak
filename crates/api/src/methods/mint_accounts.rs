// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::{
    error::RpcError,
    http::CloudbreakRpcState,
    methods::{LEGACY_TOKEN_PROGRAM_ID, program::GpaStreamingResponse},
};
use cloudbreak_core::modules::rpc_filter_type::{Memcmp, RpcFilterType, RpcProgramAccountsConfig};
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTokenAccountsByMintConfig {
    #[serde(flatten)]
    pub account_config: RpcAccountInfoConfig,
    pub program_id: Option<String>,
}

#[tracing::instrument(name = "getTokenAccountsByMint", skip_all, fields(mint = %mint))]
pub async fn get_token_accounts_by_mint(
    state: &CloudbreakRpcState,
    mint: String,
    config: Option<GetTokenAccountsByMintConfig>,
) -> Result<GpaStreamingResponse, RpcError> {
    let mint_pubkey = mint
        .parse::<Pubkey>()
        .map_err(|_| RpcError::InvalidParams)?;

    let config = config.unwrap_or_default();
    let program = config
        .program_id
        .unwrap_or_else(|| LEGACY_TOKEN_PROGRAM_ID.to_string());

    let gpa_config = RpcProgramAccountsConfig {
        filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            0,
            mint_pubkey.to_bytes().to_vec(),
        ))]),
        account_config: config.account_config,
        with_context: Some(true),
        sort_results: None,
    };

    super::program::get_program_accounts(state, program, Some(gpa_config)).await
}
