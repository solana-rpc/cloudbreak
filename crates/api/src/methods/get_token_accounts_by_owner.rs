// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! getTokenAccountsByOwner: a gPA on the token program with Agave's owner filters. The
//! response always has a context. With `jsonParsed` each account carries its own mint data.

use std::fmt;

use cloudbreak_core::modules::rpc_filter_type::{Memcmp, RpcFilterType, RpcProgramAccountsConfig};
use sea_orm::sqlx::{self, Row};
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use spl_token_2022::extension::StateWithExtensions;
use spl_token_2022::state::Mint;
use tokio::time::timeout;
use tracing::Instrument;

use crate::db_query::bytea_literal;
use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::get_program_accounts::{GpaStreamingResponse, get_program_accounts};
use crate::methods::resolve_commitment;
use crate::metrics;
use crate::modules::mint_resolver::MintResolver;
use crate::utils::token::{
    LEGACY_TOKEN_PROGRAM_ID, TOKEN_ACCOUNT_MINT_OFFSET, TOKEN_ACCOUNT_OWNER_OFFSET,
    is_token_program,
};

#[tracing::instrument(name = "gtabo_rpc", skip_all, fields(token_program = tracing::field::Empty))]
pub async fn get_token_accounts_by_owner(
    state: &CloudbreakRpcState,
    owner: String,
    filter: TokenAccountsFilter,
    config: Option<RpcAccountInfoConfig>,
) -> Result<GpaStreamingResponse, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("gtabo");
    let owner = owner
        .parse::<Pubkey>()
        .map_err(|e| RpcError::PubkeyValidationError(format!("{e:?}")))?;
    let account_config = config.unwrap_or_default();

    // Add the owner filter and token account state filter.
    let mut filters = vec![
        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            TOKEN_ACCOUNT_OWNER_OFFSET,
            owner.to_bytes().to_vec(),
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
    get_program_accounts(state, program_id, Some(gpa_config), "gtabo", mint_resolver).await
}

/// The `{mint}` / `{programId}` filter of getTokenAccountsByOwner and getTokenAccountsByDelegate.
#[derive(Deserialize)]
pub enum TokenAccountsFilter {
    #[serde(rename = "mint", deserialize_with = "deserialize_pubkey")]
    Mint(Pubkey),
    #[serde(rename = "programId", deserialize_with = "deserialize_pubkey")]
    ProgramId(Pubkey),
}

fn deserialize_pubkey<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
where
    D: Deserializer<'de>,
{
    struct PubkeyVisitor;

    impl<'de> Visitor<'de> for PubkeyVisitor {
        type Value = Pubkey;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string representing a Pubkey")
        }

        fn visit_str<E>(self, value: &str) -> Result<Pubkey, E>
        where
            E: de::Error,
        {
            value.parse::<Pubkey>().map_err(de::Error::custom)
        }
    }

    deserializer.deserialize_str(PubkeyVisitor)
}

/// The token program that owns `mint`, and the mint data, read at the request slot, with Agave's
/// `{mint}` errors. The native mint is Tokenkeg and has no data to read.
pub(crate) async fn read_mint(
    state: &CloudbreakRpcState,
    mint: &Pubkey,
    config: &RpcAccountInfoConfig,
) -> Result<(Pubkey, Option<Vec<u8>>), RpcError> {
    if *mint == spl_token_interface::native_mint::id() {
        return Ok((LEGACY_TOKEN_PROGRAM_ID, None));
    }

    let commitment = config
        .commitment
        .map(|c| resolve_commitment(c.commitment, state.processed_commitment))
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);
    let (slot, _) = state.latest_slot_and_block_time(commitment).await?;

    let sql = include_str!("../db/getMintData.sql")
        .replace("$1", &bytea_literal(mint))
        .replace("$2", &slot.to_string());
    let pool = state.database.get_postgres_connection_pool();
    let rows = timeout(
        state.queries_timeout,
        sqlx::raw_sql(&sql)
            .fetch_all(pool)
            .instrument(tracing::info_span!("mint_db")),
    )
    .await
    .map_err(|_elapsed| {
        tracing::error!("Mint query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {e}");
        RpcError::InternalError
    })?;

    // The query only reads token-program rows, so a missing or closed row also covers a mint
    // owned by another program (Agave: "not a Token mint").
    let Some(row) = rows.first().filter(|row| row.get::<i64, _>("lamports") > 0) else {
        return Err(RpcError::MintDataNotFound {
            mint: mint.to_string(),
        });
    };
    let data: Vec<u8> = row.get("data");
    if StateWithExtensions::<Mint>::unpack(&data).is_err() {
        return Err(RpcError::TokenMintCouldNotBeUnpacked {
            mint: mint.to_string(),
        });
    }
    Ok((
        Pubkey::new_from_array(row.get::<[u8; 32], _>("owner")),
        Some(data),
    ))
}
