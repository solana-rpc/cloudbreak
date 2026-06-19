// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::error::RpcError;
use crate::http::{CloudbreakApiResponse, CloudbreakRpcState};
use crate::methods::program::{self, GpaResponse};
use crate::methods::{LEGACY_TOKEN_PROGRAM_ID, SqlDataSliceFilter, is_token_program};
use crate::metrics::GpaMetricsData;
use crate::{db_query, metrics};

use futures::StreamExt;
use rust_decimal::prelude::ToPrimitive;
use sea_orm::sqlx::postgres::PgRow;
use sea_orm::sqlx::{self, Row};
use sea_orm::{DatabaseConnection, EntityTrait};
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

use solana_account::AccountSharedData;
use solana_account_decoder::parse_account_data::{
    AccountAdditionalDataV3, SplTokenAdditionalDataV2,
};
use solana_account_decoder::{
    MAX_BASE58_BYTES, UiAccountData, UiAccountEncoding, UiDataSliceConfig, encode_ui_account,
};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use cloudbreak_core::modules::rpc_filter_type::{RpcFilterType, RpcProgramAccountsConfig};
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::{
    Response as RpcResponse, RpcKeyedAccount, RpcResponseContext,
};
use spl_token_2022::extension::{BaseStateWithExtensions, StateWithExtensions};
use spl_token_2022::state::Mint;
use spl_token_2022_interface::extension::interest_bearing_mint::InterestBearingConfig;
use spl_token_2022_interface::extension::scaled_ui_amount::ScaledUiAmountConfig;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, timeout};
use tracing::Instrument;
use cloudbreak_entity::slots;

#[derive(Clone, Copy)]
pub enum TokenQueryType {
    GetTokenAccountsByOwner,
    GetTokenAccountsByDelegate,
}

impl TokenQueryType {
    pub fn get_sql_filter(&self, owner_or_delegate: &Pubkey, table: &str) -> String {
        match self {
            TokenQueryType::GetTokenAccountsByOwner => {
                format!(
                    " AND {}.token_owner = '\\x{}'::bytea",
                    table,
                    hex::encode(owner_or_delegate.as_ref())
                )
            }
            TokenQueryType::GetTokenAccountsByDelegate => {
                format!(
                    " AND SUBSTRING({table}.data FROM 73 FOR 1) = '\\x01'::bytea AND SUBSTRING({table}.data FROM 77 FOR 32) = '\\x{}'::bytea",
                    hex::encode(owner_or_delegate.as_ref())
                )
            }
        }
    }

    pub fn get_label(&self) -> &str {
        match self {
            TokenQueryType::GetTokenAccountsByOwner => "gtabo",
            TokenQueryType::GetTokenAccountsByDelegate => "gtabd",
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn generate_filters_for_table(
    filters: &TokenAccountsFilter,
    owner_or_delegate: &Pubkey,
    table: &str,
    slot: u64,
    db: Option<&DatabaseConnection>,
    queries_timeout: Duration,
    query_type: TokenQueryType,
    additional_filters: Option<Vec<RpcFilterType>>,
) -> Result<(String, Pubkey, Option<Vec<u8>>), RpcError> {
    let mut sql_filters = query_type.get_sql_filter(owner_or_delegate, table);

    if let Some(additional_filters) = additional_filters {
        for filter in additional_filters {
            filter
                .verify()
                .map_err(|e| RpcError::InvalidParamsWithMessage(format!("Invalid param: {e}")))?;

            let filter_str = SqlDataSliceFilter::new(&filter, table, true).to_string();
            if let Some(filter_str) = filter_str {
                sql_filters.push_str(&format!(" AND {}", filter_str));
            }
        }
    }

    let (mint_data, token_pogram_pubkey) = match filters {
        TokenAccountsFilter::Mint(mint) => {
            let mint_hex = format!("'\\x{}'::bytea", hex::encode(mint.as_ref()));

            sql_filters.push_str(&format!(" AND {}.token_mint = {}", table, mint_hex));

            if let Some(db) = db {
                let mint_span = tracing::info_span!("mint_db", wall_time = tracing::field::Empty);
                let start_time = Instant::now();

                if mint == &spl_token_interface::native_mint::id() {
                    return Ok((sql_filters, LEGACY_TOKEN_PROGRAM_ID, Some(Vec::new())));
                }

                let sql = include_str!("../db/getMintData.sql").replace("$1", &mint_hex);
                let sql = sql.replace("$2", slot.to_string().as_str());
                let pool = db.get_postgres_connection_pool();

                let rows = timeout(queries_timeout, async {
                    sqlx::raw_sql(&sql).fetch_all(pool).await.map_err(|e| {
                        tracing::error!("Database query error: {}", e);
                        RpcError::InternalError
                    })
                })
                .await
                .unwrap_or_else(|elapsed| {
                    tracing::error!("Database query error: {}", elapsed);
                    Err(RpcError::InternalError)
                })?;

                let row = rows.first().ok_or_else(|| {
                    tracing::error!("No row found for mint: {} at slot: {}", mint, slot);
                    RpcError::InternalError
                })?;

                let mint_data: Vec<u8> = row.get::<Vec<u8>, _>("data");
                let token_program_pubkey: [u8; 32] = row.get::<[u8; 32], _>("owner");

                mint_span.record("wall_time", start_time.elapsed().as_millis() as i64);

                tracing::debug!(
                    target: "gtabo_mint_data",
                    "Mint filter query duration: {:?} microseconds - mint data length: {}",
                    start_time.elapsed().as_micros(),
                    mint_data.len()
                );

                (
                    Some(mint_data),
                    Pubkey::new_from_array(token_program_pubkey),
                )
            } else {
                (None, Pubkey::default())
            }
        }
        TokenAccountsFilter::ProgramId(program_id) => {
            if !is_token_program(program_id) {
                return Err(RpcError::InvalidParams);
            }

            (None, *program_id)
        }
    };

    Ok((sql_filters, token_pogram_pubkey, mint_data))
}

/// The `additional_filters` are used for gPA calls that may contain addtional filters when parsed for using this function.
#[tracing::instrument(
    name = "gtabo_rpc",
    skip_all,
    fields(query_type = tracing::field::Empty, token_program = tracing::field::Empty, mint_filter = tracing::field::Empty)
)]
pub async fn get_token_accounts_by_owner_or_delegate(
    state: &CloudbreakRpcState,
    owner_or_delegate: String,
    filter: TokenAccountsFilter,
    config: Option<RpcAccountInfoConfig>,
    query_type: TokenQueryType,
    additional_filters: Option<Vec<RpcFilterType>>,
) -> Result<GpaResponse, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("gtabo");
    let db = &state.database;

    let owner_or_delegate = owner_or_delegate
        .parse::<solana_pubkey::Pubkey>()
        .map_err(|_| RpcError::InvalidParams)?;

    let commitment = config
        .as_ref()
        .and_then(|config| config.commitment)
        .map(|commitment_config| {
            crate::methods::resolve_commitment(
                commitment_config.commitment,
                state.processed_commitment,
            )
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    // If the slot syncronizer is enabled, use the cached slot data, otherwise query the database
    let (latest_slot, block_time) = match &state.slot_syncronizer_data {
        Some(data) => {
            let data = data.read().expect("Failed to read slot syncronizer data");

            (
                data.get_slot_for_commitment(commitment),
                data.get_block_time_for_commitment(commitment),
            )
        }
        None => {
            let slot_model = slots::Entity::find_by_id(commitment as i32)
                .one(&state.database)
                .instrument(tracing::info_span!("slot_db"))
                .await?;

            let model = slot_model.ok_or(RpcError::InternalError)?;

            (model.slot as u64, model.block_time)
        }
    };

    let (accounts_filters, program, filter_mint_data) = generate_filters_for_table(
        &filter,
        &owner_or_delegate,
        "accounts",
        latest_slot,
        Some(db),
        state.queries_timeout,
        query_type,
        additional_filters.clone(),
    )
    .await?;

    if !state.indexer_filter.is_program_selected(&program) {
        return Err(RpcError::KeyExcludedFromSecondaryIndex {
            key: program.to_string(),
        });
    }

    let (snapshot_filters, _, _) = generate_filters_for_table(
        &filter,
        &owner_or_delegate,
        "snapshot_accounts",
        latest_slot,
        None, // Only query mint data once
        state.queries_timeout,
        query_type,
        additional_filters,
    )
    .await?;

    tracing::Span::current().record("query_type", query_type.get_label());
    tracing::Span::current().record("token_program", program.to_string());
    if filter_mint_data.is_some() {
        tracing::Span::current().record("mint_filter", "true");
    }

    // Load sql and add dynamic filters to it
    let sql = if let Some(encoding) = config.as_ref().map(|config| config.encoding) {
        // If we already have the mint data, we don't need to join any additional mint data
        if encoding == Some(UiAccountEncoding::JsonParsed) && filter_mint_data.is_none() {
            include_str!("../db/getTokenAccountsByOwner.sql")
        } else {
            include_str!("../db/getProgramAccounts.sql")
        }
    } else {
        include_str!("../db/getProgramAccounts.sql")
    };

    let sql = sql.replace("-- {accounts_filters}", &accounts_filters);
    let sql = sql.replace("-- {snapshot_filters}", &snapshot_filters);
    let sql = sql.replace("$2", latest_slot.to_string().as_str());

    let program_bytes = program.as_ref().to_vec();
    let sql = sql.replace(
        "$1",
        &format!("'\\x{}'::bytea", hex::encode(&program_bytes)),
    );

    let sql = db_query::add_trace_traceparent_to_query(&sql);

    tracing::debug!(target: "gpa_sql", "## sql: {}", sql);
    let pool = db.get_postgres_connection_pool();

    let mut rows = sqlx::raw_sql(&sql).fetch(pool);

    let encoding = config
        .as_ref()
        .and_then(|config| config.encoding)
        .unwrap_or(UiAccountEncoding::Binary);
    let data_slice = config.as_ref().and_then(|config| config.data_slice);
    let should_filter = encoding == UiAccountEncoding::JsonParsed;

    let mut response_bytes = 0;
    let mut accounts_count = 0;
    let mut db_query_total_ms = Duration::from_millis(0);
    let mut encoding_total_ms = Duration::from_millis(0);
    let mut db_first_row_time = Duration::from_millis(0);

    // TODO: Do more than 1 batch
    let mut batch = Vec::new();

    timeout(state.queries_timeout, async {
        let db_span = tracing::info_span!("gpa_db", wall_time = tracing::field::Empty);
        let db_execution_span =
            tracing::info_span!("gpa_db_execution", wall_time = tracing::field::Empty);

        let mut first_loop_iteration = true;

        loop {
            let before = Instant::now();

            // Note:The 1st await will contain the time consumed by the database query
            let next_row = if first_loop_iteration {
                first_loop_iteration = false;
                let first_row = rows
                    .next()
                    .instrument(db_execution_span.clone())
                    .instrument(db_span.clone())
                    .await;

                db_first_row_time = before.elapsed();
                db_execution_span.record("wall_time", db_first_row_time.as_millis() as i64);

                first_row
            } else {
                rows.next().instrument(db_span.clone()).await
            };

            db_query_total_ms += before.elapsed();

            let Some(row) = next_row else { break };

            let row = row.map_err(|e| {
                tracing::error!("Database query error: {}", e);
                RpcError::InternalError
            })?;

            accounts_count += 1;

            batch.push(row);
        }

        db_span.record("wall_time", db_query_total_ms.as_millis() as i64);

        Ok::<_, RpcError>(())
    })
    .await
    .unwrap_or_else(|elapsed| {
        tracing::error!("Database query error: {}", elapsed);
        Err(RpcError::InternalError)
    })?;

    let encode_start_time = Instant::now();

    // Will measure the total time taken for `encode_ui_account()`
    let encode_span = tracing::info_span!(
        "gpa_encode",
        bytes = 0i32,
        accounts = 0i32,
        wall_time = tracing::field::Empty,
        encoding = program::encoding_to_string(&encoding)
    );
    let encode_span2 = tracing::info_span!("gpa_encode2");

    let accounts = tokio::task::spawn_blocking(move || {
        let mut accounts = Vec::new();
        for row in batch {
            let account = proces_token_row(
                row,
                encoding,
                data_slice,
                &filter_mint_data,
                block_time,
                &mut response_bytes,
            )?;

            // Only keep accounts that could be parsed as JSON
            if should_filter && !matches!(account.account.data, UiAccountData::Json(_)) {
                continue;
            }

            encode_span2.in_scope(|| {
                accounts.push(account);
            });
        }

        encode_span.record("bytes", response_bytes as i32);
        encode_span.record("accounts", accounts_count);
        encode_span.record("wall_time", encode_start_time.elapsed().as_millis() as i64);

        Ok::<_, RpcError>(accounts)
    })
    .await
    .map_err(|_| RpcError::InternalError)??;

    encoding_total_ms += encode_start_time.elapsed();

    let context = RpcResponseContext::new(latest_slot);

    let metrics_data = GpaMetricsData::new(query_type.get_label().to_string());
    metrics_data.set_db_metrics(
        db_query_total_ms.as_millis() as f64,
        db_first_row_time.as_millis() as f64,
    );
    metrics_data.set_encode_metrics(encoding_total_ms.as_millis() as f64);

    Ok(GpaResponse {
        response: CloudbreakApiResponse::ResponseWithContext(RpcResponse {
            context,
            value: accounts,
        }),
        metrics_data: Some(metrics_data),
    })
}

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

pub fn parse_additional_mint_data(
    mint_pubkey: &Pubkey,
    mint_data: &[u8],
    block_time: i64,
) -> Option<AccountAdditionalDataV3> {
    if mint_pubkey == &spl_token_interface::native_mint::id() {
        return Some(AccountAdditionalDataV3 {
            spl_token_additional_data: Some(SplTokenAdditionalDataV2::with_decimals(
                spl_token_interface::native_mint::DECIMALS,
            )),
        });
    }

    let mint = StateWithExtensions::<Mint>::unpack(mint_data).ok()?;
    let decimals = mint.base.decimals;
    let interest_bearing_config = mint.get_extension::<InterestBearingConfig>().cloned().ok();
    let scaled_ui_amount_config = mint.get_extension::<ScaledUiAmountConfig>().cloned().ok();

    Some(AccountAdditionalDataV3 {
        spl_token_additional_data: Some(SplTokenAdditionalDataV2 {
            decimals,
            interest_bearing_config: interest_bearing_config.map(|i| (i, block_time)),
            scaled_ui_amount_config: scaled_ui_amount_config.map(|s| (s, block_time)),
        }),
    })
}

pub struct TokenQueryInputRequest {
    pub owner_or_delegate: String,
    pub filter: TokenAccountsFilter,
    pub config: Option<RpcAccountInfoConfig>,
    pub query_type: TokenQueryType,
    pub additional_filters: Option<Vec<RpcFilterType>>,
}

/// If the gPA can be parsed into a gTABO or gTABD, return the request parameters for the gTABO or gTABD.
pub fn try_parse_gpa_into_gtabo(
    program: Pubkey,
    config: RpcProgramAccountsConfig,
) -> Result<TokenQueryInputRequest, RpcError> {
    if !is_token_program(&program) {
        return Err(RpcError::InternalError);
    }

    let filters = config.filters.as_ref().ok_or(RpcError::InvalidParams)?;

    let mut owner_or_delegate = None;
    let mut query_type = None;
    let mut additional_filters = Vec::new();

    for filter in filters {
        match filter {
            RpcFilterType::Memcmp(memcmp) => {
                let offset = memcmp.offset();
                let bytes = memcmp.bytes().ok_or(RpcError::InvalidParams)?;

                if owner_or_delegate.is_some() {
                    additional_filters.push(filter.clone());
                    continue;
                }

                // Try to convert the memcmp into a token owner or delegate filter
                if offset == 32 && bytes.len() == 32 {
                    owner_or_delegate = Some(
                        Pubkey::try_from(bytes.as_slice())
                            .map_err(|_| RpcError::InvalidParams)?
                            .to_string(),
                    );
                    query_type = Some(TokenQueryType::GetTokenAccountsByOwner);
                } else if offset == 76 && bytes.len() == 32 {
                    owner_or_delegate = Some(
                        Pubkey::try_from(bytes.as_slice())
                            .map_err(|_| RpcError::InvalidParams)?
                            .to_string(),
                    );
                    query_type = Some(TokenQueryType::GetTokenAccountsByDelegate);
                } else {
                    additional_filters.push(filter.clone());
                }
            }
            _ => {
                additional_filters.push(filter.clone());
            }
        }
    }

    let additional_filters = if additional_filters.is_empty() {
        None
    } else {
        Some(additional_filters)
    };

    Ok(TokenQueryInputRequest {
        owner_or_delegate: owner_or_delegate.ok_or(RpcError::InvalidParams)?,
        filter: TokenAccountsFilter::ProgramId(program),
        config: Some(config.account_config),
        query_type: query_type.ok_or(RpcError::InvalidParams)?,
        additional_filters,
    })
}

fn proces_token_row(
    row: PgRow,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
    filter_mint_data: &Option<Vec<u8>>,
    block_time: i64,
    response_bytes: &mut u64,
) -> Result<RpcKeyedAccount, RpcError> {
    let pubkey = Pubkey::new_from_array(row.get(0));
    let owner = Pubkey::new_from_array(row.get(1));
    let lamports = row.get::<i64, _>(2);
    let executable = row.get(4);
    let rent_epoch = row.get::<rust_decimal::Decimal, _>(5);
    let data: Vec<u8> = row.get(6);

    *response_bytes += data.len() as u64;

    check_account_data_len_for_encoding(encoding, data_slice, data.len(), &pubkey)?;

    let row_mint_data: Option<Vec<u8>> = row.try_get("mint_data").ok();
    let mint_pubkey: Pubkey = Pubkey::new_from_array(row.get("token_mint"));

    // If we got a mint filter, use the already retrieved mint data (for the jsonParsed encoding)
    let additional_mint_data = if let Some(mint_data) = &filter_mint_data {
        if row_mint_data.is_some() {
            tracing::error!(
                "Mint data found in both filter and row for pubkey: {}",
                mint_pubkey
            );
        }

        parse_additional_mint_data(&mint_pubkey, mint_data, block_time)
    } else {
        // Pass the mint pubkey unconditionally (with empty data if the JOIN missed) so
        // parse_additional_mint_data's native_mint short-circuit can still hardcode
        // decimals for WSOL.
        parse_additional_mint_data(
            &mint_pubkey,
            row_mint_data.as_deref().unwrap_or(&[]),
            block_time,
        )
    };

    let account_shared_data = AccountSharedData::create_from_existing_shared_data(
        lamports as u64,
        Arc::new(data),
        owner,
        executable,
        rent_epoch.to_u64().unwrap_or(0),
    );

    let maybe_encoded_account = encode_ui_account(
        &pubkey,
        &account_shared_data,
        encoding,
        additional_mint_data,
        data_slice,
    );

    Ok(RpcKeyedAccount {
        pubkey: pubkey.to_string(),
        account: maybe_encoded_account,
    })
}

pub fn check_account_data_len_for_encoding(
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
    account_data_length: usize,
    pubkey: &Pubkey,
) -> Result<(), RpcError> {
    if (encoding == UiAccountEncoding::Binary || encoding == UiAccountEncoding::Base58)
        && data_slice
            .map(|s| core::cmp::min(s.length, account_data_length.saturating_sub(s.offset)))
            .unwrap_or(account_data_length)
            > MAX_BASE58_BYTES
    {
        let message = format!(
            "Encoded binary (base 58) data should be less than {MAX_BASE58_BYTES} bytes, please \
         use Base64 encoding. pubkey: {pubkey}"
        );

        return Err(RpcError::InvalidParamsWithMessage(message));
    }

    Ok(())
}
