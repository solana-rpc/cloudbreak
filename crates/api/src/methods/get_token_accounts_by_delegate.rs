// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! getTokenAccountsByDelegate: a gPA on the token program with Agave's delegate filters. The
//! response always has a context. With `jsonParsed` each account carries its own mint data.

use cloudbreak_core::modules::rpc_filter_type::{Memcmp, RpcFilterType, RpcProgramAccountsConfig};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::get_program_accounts::{GpaStreamingResponse, get_program_accounts};
use crate::methods::get_token_accounts_by_owner::{TokenAccountsFilter, read_mint};
use crate::metrics;
use crate::modules::mint_resolver::MintResolver;
use crate::utils::token::{
    TOKEN_ACCOUNT_DELEGATE_OFFSET, TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET, TOKEN_ACCOUNT_MINT_OFFSET,
    is_token_program,
};

#[tracing::instrument(name = "gtabd_rpc", skip_all, fields(token_program = tracing::field::Empty))]
pub async fn get_token_accounts_by_delegate(
    state: &CloudbreakRpcState,
    delegate: String,
    filter: TokenAccountsFilter,
    config: Option<RpcAccountInfoConfig>,
) -> Result<GpaStreamingResponse, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("gtabd");
    let delegate = delegate
        .parse::<Pubkey>()
        .map_err(|e| RpcError::PubkeyValidationError(format!("{e:?}")))?;
    let account_config = config.unwrap_or_default();

    // Add the delegate filter and token account state filter.
    let mut filters = vec![
        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET,
            vec![1, 0, 0, 0],
        )),
        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            TOKEN_ACCOUNT_DELEGATE_OFFSET,
            delegate.to_bytes().to_vec(),
        )),
        RpcFilterType::TokenAccountState,
    ];
    let (program, mint) = match filter {
        TokenAccountsFilter::ProgramId(program) => {
            if !is_token_program(&program) {
                return Err(RpcError::UnrecognizedTokenProgramId {
                    program_id: program.to_string(),
                });
            }
            (program, None)
        }
        TokenAccountsFilter::Mint(mint) => {
            let (program, data) = read_mint(state, &mint, &account_config).await?;
            filters.push(RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                TOKEN_ACCOUNT_MINT_OFFSET,
                mint.to_bytes().to_vec(),
            )));
            (program, data.map(|data| (mint, data)))
        }
    };
    let program_id = program.to_string();
    tracing::Span::current().record("token_program", &program_id);

    let mut mint_resolver = MintResolver::new(state, &program_id, account_config.encoding);
    if let Some((mint, data)) = mint {
        mint_resolver = mint_resolver.with_mint(mint, program, data);
    }

    let gpa_config = RpcProgramAccountsConfig {
        filters: Some(filters),
        account_config,
        with_context: Some(true),
        sort_results: None,
    };
    get_program_accounts(state, program_id, Some(gpa_config), "gtabd", mint_resolver).await
}
