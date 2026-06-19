// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::error::RpcError;
use crate::http::{CloudbreakApiResponse, CloudbreakRpcState};
use crate::methods::token::{
    self, check_account_data_len_for_encoding, get_token_accounts_by_owner_or_delegate,
    try_parse_gpa_into_gtabo,
};
use crate::methods::{SqlDataSliceFilter, is_token_program, mint};
use crate::metrics::GpaMetricsData;
use crate::modules::cache::{GpaProcessor, KeyedRpcAccount, MaybeJsonAccount};
use crate::{db_query, metrics};
use async_stream::try_stream;
use cloudbreak_core::modules::rpc_filter_type::{
    RpcProgramAccountsConfig, account_matches_value_cmps, has_value_cmp,
};
use cloudbreak_entity::slots;
use futures::{Stream, StreamExt};
use rust_decimal::prelude::ToPrimitive;
use sea_orm::EntityTrait;
use sea_orm::sqlx::postgres::PgRow;
use sea_orm::sqlx::{self, Row};
use solana_account::AccountSharedData;
use solana_account_decoder::parse_account_data::AccountAdditionalDataV3;
use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig, encode_ui_account};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::RpcKeyedAccount;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::{Instant, timeout};
use tracing::Instrument;

pub const MAX_BASE58_BYTES: usize = 128;

pub struct GpaResponse {
    pub response: CloudbreakApiResponse<Vec<RpcKeyedAccount>>,
    pub metrics_data: Option<GpaMetricsData>,
}

pub struct GpaStreamingResponse {
    pub accounts_stream: EncodedAccountBatchStream,
    pub metrics_data: Option<GpaMetricsData>,
    /// All context slot data will be added to json using this on [`gpa_streaming_response_body`]
    pub context_slot: Option<u64>,
    pub gpa_processor: GpaProcessor,
    pub program: Pubkey,
    pub encoding: UiAccountEncoding,
}

#[tracing::instrument(name = "gpa_rpc", skip_all, fields(program = %program))]
pub async fn get_program_accounts(
    state: &CloudbreakRpcState,
    program: String,
    config: Option<RpcProgramAccountsConfig>,
) -> Result<GpaStreamingResponse, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("gpa");

    let config = config.unwrap_or_default();

    let program = program
        .parse::<solana_pubkey::Pubkey>()
        .map_err(|_| RpcError::InvalidParams)?;

    let encoding = config
        .account_config
        .encoding
        .unwrap_or(UiAccountEncoding::Binary);

    if !state.indexer_filter.is_program_selected(&program) {
        return Err(RpcError::KeyExcludedFromSecondaryIndex {
            key: program.to_string(),
        });
    }

    let is_token_program = is_token_program(&program);
    let mut mint_filter_label = "";
    let mut additional_mint_data = None;

    let commitment = config
        .account_config
        .commitment
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

    let context_slot = if let Some(with_context) = config.with_context {
        if with_context {
            if let Some(min_context_slot) = config.account_config.min_context_slot
                && latest_slot < min_context_slot
            {
                return Err(RpcError::RpcSlotBehindMinContextSlot {
                    rpc_slot: latest_slot,
                });
            }

            Some(latest_slot)
        } else {
            None
        }
    } else {
        None
    };

    if is_token_program {
        // There is only support gPA token programs queries that can be parsed into a gTABO or gTABD
        match try_parse_gpa_into_gtabo(program, config.clone()) {
            Ok(gtabo_query_result) => {
                let response = get_token_accounts_by_owner_or_delegate(
                    state,
                    gtabo_query_result.owner_or_delegate,
                    gtabo_query_result.filter,
                    gtabo_query_result.config,
                    gtabo_query_result.query_type,
                    gtabo_query_result.additional_filters,
                )
                .await?;

                // Extract accounts from gTABO and only add context slot if needed
                let encoded_accounts = match response.response {
                    CloudbreakApiResponse::ResponseWithContext(rpc_response) => rpc_response.value,
                    CloudbreakApiResponse::Response(accounts) => accounts,
                };

                // Token-program path stays buffered; we wrap the resulting Vec in a
                // `stream::iter` so the caller doesn't need to special-case it.
                return Ok(GpaStreamingResponse {
                    context_slot,
                    accounts_stream: Box::pin(futures::stream::once(async move {
                        Ok(encoded_accounts
                            .into_iter()
                            .map(|account| {
                                MaybeJsonAccount::Fresh(KeyedRpcAccount {
                                    // Pubkey is unused in Standard mode
                                    pubkey: Pubkey::default(),
                                    account,
                                })
                            })
                            .collect())
                    })),
                    metrics_data: response.metrics_data,
                    gpa_processor: GpaProcessor::Standard,
                    program,
                    encoding,
                });
            }
            Err(_) => {
                // If there is a valid tokenmint filter, get the mint data from the database if jsonParsed encoding is used
                if let Some(mint_pubkey) =
                    mint::check_filters_are_valid_for_token_query(program, config.clone())?
                {
                    mint_filter_label = "_mint";
                    let mint_pubkey_key = Pubkey::try_from(mint_pubkey.as_slice())
                        .map_err(|_| RpcError::InvalidParams)?;

                    if config.account_config.encoding == Some(UiAccountEncoding::JsonParsed) {
                        additional_mint_data = mint::get_mint(
                            program,
                            mint_pubkey,
                            latest_slot,
                            &state.database,
                            state.queries_timeout,
                        )
                        .await
                        .and_then(|mint_data| {
                            token::parse_additional_mint_data(
                                &mint_pubkey_key,
                                &mint_data,
                                block_time,
                            )
                        });
                    }
                }
            }
        }
    }

    // Buffer query for batch submission to remote query tracker (if enabled)
    if let Some(client) = &state.query_tracker_client {
        client.buffer_query(program, Some(config.clone()));
    }

    if let Some(ref filters) = config.filters {
        for filter in filters {
            filter
                .verify()
                .map_err(|e| RpcError::InvalidParamsWithMessage(format!("Invalid param: {e}")))?;
        }
    }

    // Build filter clauses for both tables
    let accounts_filters = if let Some(ref filters) = config.filters {
        filters
            .iter()
            .filter_map(|f| SqlDataSliceFilter::new(f, "accounts", is_token_program).to_string())
            .map(|filter| format!("AND {}", filter))
            .collect::<Vec<_>>()
            .join("\n                    ")
    } else {
        String::new()
    };

    let snapshot_filters = if let Some(ref filters) = config.filters {
        filters
            .iter()
            .filter_map(|f| {
                SqlDataSliceFilter::new(f, "snapshot_accounts", is_token_program).to_string()
            })
            .map(|filter| format!("AND {}", filter))
            .collect::<Vec<_>>()
            .join("\n                    ")
    } else {
        String::new()
    };

    let metrics_data = GpaMetricsData::new(format!("gpa{mint_filter_label}"));

    let mut request_processor = state
        .gpa_processor
        .for_request(config.filters.as_deref().unwrap_or(&[]));

    // Database query
    let rx = gpa_db_query(
        GpaDbQueryInput {
            program,
            config: config.clone(),
            state: state.clone(),
            latest_slot,
            accounts_filters,
            snapshot_filters,
            metrics_data: metrics_data.clone(),
        },
        &mut request_processor,
    );

    // Encoding phase
    let encoded_accounts_stream = gpa_encoding_stream(GpaEncodingInput {
        config,
        additional_mint_data,
        metrics_data: metrics_data.clone(),
        gpa_processor: request_processor.clone(),
        rx,
    });

    Ok(GpaStreamingResponse {
        context_slot,
        accounts_stream: encoded_accounts_stream,
        metrics_data: Some(metrics_data),
        gpa_processor: request_processor,
        program,
        encoding,
    })
}

pub struct GpaDbQueryInput {
    pub program: Pubkey,
    pub config: RpcProgramAccountsConfig,
    pub state: CloudbreakRpcState,
    pub latest_slot: u64,
    pub accounts_filters: String,
    pub snapshot_filters: String,
    pub metrics_data: GpaMetricsData,
}

fn gpa_db_query(
    input: GpaDbQueryInput,
    gpa_processor: &mut GpaProcessor,
) -> UnboundedReceiver<Result<Vec<PgRow>, RpcError>> {
    // `load_sql` mutates the caller-owned processor
    let sql = gpa_processor.load_sql(&input);

    let program_bytes = input.program.as_ref().to_vec();
    let sql = sql.replace(
        "$1",
        &format!("'\\x{}'::bytea", hex::encode(&program_bytes)),
    );

    tracing::debug!(target: "gpa_sql", "## sql: {}", sql);

    let pool = input.state.database.get_postgres_connection_pool().clone();

    let sql = db_query::add_trace_traceparent_to_query(&sql);

    let queries_timeout = input.state.queries_timeout;
    let gpa_stream_batch_size = input.state.gpa_stream_batch_size;

    let (tx, rx) = mpsc::unbounded_channel::<Result<Vec<PgRow>, RpcError>>();

    let metrics_data_clone = input.metrics_data.clone();
    let parent_span = tracing::Span::current();
    tokio::spawn(async move {
        let db_query = async {
            let mut db_query_total_ms = Duration::from_millis(0);
            let mut db_first_row_time = Duration::from_millis(0);

            let db_span = tracing::info_span!(
                parent: &parent_span,
                "gpa_db",
                wall_time = tracing::field::Empty
            );
            let db_execution_span = tracing::info_span!(
                parent: &parent_span,
                "gpa_db_execution",
                wall_time = tracing::field::Empty
            );
            let mut first_loop_iteration = true;

            let mut rows = sqlx::raw_sql(&sql).fetch(&pool);
            let mut batch: Vec<PgRow> = Vec::with_capacity(gpa_stream_batch_size);

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

                let row = match row {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!("Database query error: {}", e);
                        let _ = tx.send(Err(RpcError::InternalError));
                        return;
                    }
                };

                batch.push(row);

                if batch.len() >= gpa_stream_batch_size {
                    let to_send =
                        std::mem::replace(&mut batch, Vec::with_capacity(gpa_stream_batch_size));
                    if tx.send(Ok(to_send)).is_err() {
                        // Consumer side dropped
                        return;
                    }
                }
            }

            if !batch.is_empty() {
                let _ = tx.send(Ok(batch));
            }

            // Record db metrics
            metrics_data_clone.set_db_metrics(
                db_query_total_ms.as_millis() as f64,
                db_first_row_time.as_millis() as f64,
            );
            db_span.record("wall_time", db_query_total_ms.as_millis() as i64);
        };

        if timeout(queries_timeout, db_query).await.is_err() {
            tracing::error!("Database streaming query timed out");
            let _ = tx.send(Err(RpcError::InternalError));
        }
    });

    rx
}

pub struct GpaEncodingInput {
    pub config: RpcProgramAccountsConfig,
    pub additional_mint_data: Option<AccountAdditionalDataV3>,
    pub metrics_data: GpaMetricsData,
    pub gpa_processor: GpaProcessor,
    pub rx: UnboundedReceiver<Result<Vec<PgRow>, RpcError>>,
}

/// Stream items are *batches* of encoded accounts (not individual accounts).
pub type EncodedAccountBatchStream =
    Pin<Box<dyn Stream<Item = Result<Vec<MaybeJsonAccount>, RpcError>> + Send>>;

/// Process the received batches of rows coming from `rx` (`gpa_db_query`) and streams
/// batches of encoded `MaybeJsonAccount`
pub fn gpa_encoding_stream(input: GpaEncodingInput) -> EncodedAccountBatchStream {
    let GpaEncodingInput {
        config,
        additional_mint_data,
        metrics_data,
        gpa_processor,
        mut rx,
    } = input;

    let encoding = config
        .account_config
        .encoding
        .unwrap_or(UiAccountEncoding::Binary);
    let data_slice = config.account_config.data_slice;

    // ValueCmp filters are applied here as an in-memory post-filter over the streamed rows.
    // Captured once; the per-row pass is skipped entirely when no ValueCmp filters are present.
    // Note: a request with ValueCmp filters always uses the `Standard` (non-cached) processor
    let filters = config.filters.clone().unwrap_or_default();
    let apply_value_cmp = has_value_cmp(&filters);

    // Accounts encoding step
    let stream = try_stream! {
        let encode_span = tracing::info_span!(
            "gpa_encode",
            bytes = 0i32,
            accounts = 0i32,
            wall_time = tracing::field::Empty,
            encoding = encoding_to_string(&encoding)
        );
        let mut encoding_total_ms = Duration::from_millis(0);
        let mut accounts_count = 0;
        let mut response_bytes = 0u64;

        while let Some(batch_result) = rx.recv().await {
            let encode_iteration_start_time = Instant::now();
            let batch = batch_result?;

            // Apply the ValueCmp post-filter before encoding so non-matching
            // accounts are never encoded or counted.
            let batch = if apply_value_cmp {
                batch
                    .into_iter()
                    .filter(|row| account_matches_value_cmps(&filters, &row.get::<Vec<u8>, _>(6)))
                    .collect::<Vec<_>>()
            } else {
                batch
            };

            let encode_span_clone = encode_span.clone();
            let gpa_processor = gpa_processor.clone();
            let (encoded_batch, local_bytes) = tokio::task::spawn_blocking(move || {
                let mut local_bytes = 0u64;
                batch
                    .into_iter()
                    .map(|row| {
                        gpa_processor.process_row(
                            row,
                            encoding,
                            data_slice,
                            &mut local_bytes,
                            &encode_span_clone,
                            additional_mint_data,
                        )
                    })
                    .collect::<Result<Vec<MaybeJsonAccount>, RpcError>>()
                    .map(|accounts| (accounts, local_bytes))
            })
            .await
            .map_err(|e| {
                tracing::error!("spawn_blocking join error: {}", e);
                RpcError::InternalError
            })??;

            encoding_total_ms += encode_iteration_start_time.elapsed();
            accounts_count += encoded_batch.len();
            response_bytes += local_bytes;

            yield encoded_batch;
        }

        // Record encode metrics
        metrics_data.set_encode_metrics(encoding_total_ms.as_millis() as f64);
        encode_span.record("bytes", response_bytes as i32);
        encode_span.record("accounts", accounts_count);
        encode_span.record("wall_time", encoding_total_ms.as_millis() as i64);
    };

    Box::pin(stream)
}

pub fn encoding_to_string(encoding: &UiAccountEncoding) -> &'static str {
    match encoding {
        UiAccountEncoding::Binary => "binary",
        UiAccountEncoding::Base58 => "base58",
        UiAccountEncoding::Base64 => "base64",
        UiAccountEncoding::JsonParsed => "jsonParsed",
        UiAccountEncoding::Base64Zstd => "base64Zstd",
    }
}

pub fn load_sql(input: &GpaDbQueryInput) -> String {
    let sql = include_str!("../db/getProgramAccounts.sql");
    let sql = sql.replace("-- {accounts_filters}", &input.accounts_filters);
    let sql = sql.replace("-- {snapshot_filters}", &input.snapshot_filters);

    sql.replace("$2", input.latest_slot.to_string().as_str())
}

pub fn process_row(
    row: PgRow,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
    response_bytes: &mut u64,
    encode_span: &tracing::Span,
    additional_mint_data: Option<AccountAdditionalDataV3>,
) -> Result<KeyedRpcAccount, RpcError> {
    encode_span.in_scope(|| {
        let pubkey = Pubkey::new_from_array(row.get(0));
        let owner = Pubkey::new_from_array(row.get(1));
        let lamports = row.get::<i64, _>(2);
        let executable = row.get(4);
        let rent_epoch = row.get::<rust_decimal::Decimal, _>(5);
        let data: Vec<u8> = row.get(6);

        *response_bytes += data.len() as u64;

        let account_shared_data = AccountSharedData::create_from_existing_shared_data(
            lamports as u64,
            Arc::new(data.clone()),
            owner,
            executable,
            rent_epoch.to_u64().unwrap_or(0),
        );

        check_account_data_len_for_encoding(encoding, data_slice, data.len(), &pubkey)?;

        let maybe_encoded_account = encode_ui_account(
            &pubkey,
            &account_shared_data,
            encoding,
            additional_mint_data,
            data_slice,
        );

        Ok(KeyedRpcAccount {
            pubkey,
            account: RpcKeyedAccount {
                pubkey: pubkey.to_string(),
                account: maybe_encoded_account,
            },
        })
    })
}
