// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::num::NonZeroUsize;
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
    RpcFilterType, RpcProgramAccountsConfig, account_matches_value_cmps, has_value_cmp,
};
use futures::{Stream, StreamExt};
use rust_decimal::prelude::ToPrimitive;
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
        .map_err(|e| RpcError::PubkeyValidationError(format!("{e:?}")))?;

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

    let (latest_slot, block_time) = state.latest_slot_and_block_time(commitment).await?;

    if let Some(min_context_slot) = config.account_config.min_context_slot
        && latest_slot < min_context_slot
    {
        return Err(RpcError::MinContextSlotNotReached { context_slot: latest_slot });
    }

    let context_slot = config
        .with_context
        .unwrap_or(false)
        .then_some(latest_slot);

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

    if let Some(ref filters) = config.filters {
        for filter in filters {
            filter.verify().map_err(|e| {
                RpcError::InvalidParamsWithMessage(format!(
                    "Invalid param: {}",
                    e.invalid_param_text()
                ))
            })?;
        }
    }

    // Build filter clauses for both tables
    let (accounts_filters, snapshot_filters, filter_params) =
        bound_filters(config.filters.as_deref().unwrap_or(&[]), is_token_program);

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
            filter_params,
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
    /// Memcmp bytes bound to `$1..$n` in the filters.
    pub filter_params: Vec<Vec<u8>>,
    pub metrics_data: GpaMetricsData,
}

/// gPA SQL text and the slot values bound after the filter parameters, in placeholder order.
pub struct GpaSql {
    pub sql: String,
    pub slot_params: Vec<i64>,
}

/// Renders the filters of both tables. Memcmp bytes become `$1..$n`, shared by both tables;
/// the values that select a query-tracker index (dataSize, offsets, lengths) stay literal.
fn bound_filters(
    filters: &[RpcFilterType],
    is_token_program: bool,
) -> (String, String, Vec<Vec<u8>>) {
    let mut accounts_filters = Vec::new();
    let mut snapshot_filters = Vec::new();
    let mut params = Vec::new();

    for filter in filters {
        let param = params.len() + 1;
        let Some((accounts_filter, value)) =
            SqlDataSliceFilter::new(filter, "accounts", is_token_program).to_bound_sql(param)
        else {
            continue;
        };
        if let Some((snapshot_filter, _)) =
            SqlDataSliceFilter::new(filter, "snapshot_accounts", is_token_program)
                .to_bound_sql(param)
        {
            snapshot_filters.push(format!("AND {}", snapshot_filter));
        }
        accounts_filters.push(format!("AND {}", accounts_filter));
        params.extend(value);
    }

    (
        accounts_filters.join("\n                    "),
        snapshot_filters.join("\n                    "),
        params,
    )
}

/// Writes the template's `$1` as the literal program, so each program prunes to its partition
/// at plan time, and renumbers `$2`, `$3`, ... to `$first_slot_param`, `$first_slot_param + 1`, ...
pub fn render_template_params(sql: &str, program: &Pubkey, first_slot_param: usize) -> String {
    let mut out = String::with_capacity(sql.len() + 64);
    let mut rest = sql;

    while let Some(pos) = rest.find('$') {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + 1..];
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();

        match after[..digits].parse::<usize>() {
            Ok(1) => out.push_str(&format!("'\\x{}'::bytea", hex::encode(program.as_ref()))),
            Ok(n) if n >= 2 => out.push_str(&format!("${}", first_slot_param + n - 2)),
            _ => out.push_str(&rest[pos..pos + 1 + digits]),
        }
        rest = &after[digits..];
    }
    out.push_str(rest);

    out
}

fn gpa_db_query(
    input: GpaDbQueryInput,
    gpa_processor: &mut GpaProcessor,
) -> UnboundedReceiver<Result<Vec<PgRow>, RpcError>> {
    // `load_sql` mutates the caller-owned processor
    let GpaSql { sql, slot_params } = gpa_processor.load_sql(&input);

    tracing::debug!(target: "gpa_sql", "## sql: {}", sql);

    let pool = input.state.database.get_postgres_connection_pool().clone();

    // A traced SQL text is unique, so it runs as an unnamed statement and stays out of the statement cache.
    let traceparent = db_query::traceparent_comment();
    let persistent = traceparent.is_none();
    let sql = match traceparent {
        Some(comment) => format!("{comment} {sql}"),
        None => sql,
    };

    let queries_timeout = input.state.queries_timeout;
    // `None` sends every row as one batch, so DB failures surface before the response starts.
    let gpa_stream_batch_size = input.state.gpa_stream_batch_size.map(NonZeroUsize::get);

    let (tx, rx) = mpsc::unbounded_channel::<Result<Vec<PgRow>, RpcError>>();

    let metrics_data_clone = input.metrics_data.clone();
    let parent_span = tracing::Span::current();
    let fetch = async move {
        let db_query = async {
            let mut db_query_total_time = Duration::from_millis(0);
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

            let mut query = sqlx::query(&sql).persistent(persistent);
            for value in &input.filter_params {
                query = query.bind(value.as_slice());
            }
            for slot in &slot_params {
                query = query.bind(*slot);
            }
            let mut rows = query.fetch(&pool);
            let mut batch: Vec<PgRow> = Vec::with_capacity(gpa_stream_batch_size.unwrap_or(0));

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

                db_query_total_time += before.elapsed();

                let Some(row) = next_row else { break };

                let row = match row {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!("Database query error: {}", e);
                        // Record the failed query too: a pattern that errors out
                        // mid-stream is still real demand for an index.
                        if let Some(client) = &input.state.query_tracker_client {
                            client.buffer_query(
                                input.program,
                                Some(input.config.clone()),
                                db_query_total_time.as_micros() as u64,
                                true,
                            );
                        }
                        let _ = tx.send(Err(RpcError::InternalError));
                        return;
                    }
                };

                batch.push(row);

                if let Some(batch_size) = gpa_stream_batch_size
                    && batch.len() >= batch_size
                {
                    let to_send = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                    if tx.send(Ok(to_send)).is_err() {
                        // Consumer side dropped
                        return;
                    }
                }
            }

            if !batch.is_empty() {
                let _ = tx.send(Ok(batch));
            }

            if let Some(client) = &input.state.query_tracker_client {
                client.buffer_query(
                    input.program,
                    Some(input.config.clone()),
                    db_query_total_time.as_micros() as u64,
                    false,
                );
            }

            // Record db metrics
            metrics_data_clone.set_db_metrics(
                db_query_total_time.as_millis() as f64,
                db_first_row_time.as_millis() as f64,
            );
            db_span.record("wall_time", db_query_total_time.as_millis() as i64);
        };

        if timeout(queries_timeout, db_query).await.is_err() {
            tracing::error!("Database streaming query timed out");
            // A timed-out query is strong demand for an index (it currently
            // cannot be served); record it with the timeout budget as the cost
            // estimate so it can be prioritized for creation.
            if let Some(client) = &input.state.query_tracker_client {
                client.buffer_query(
                    input.program,
                    Some(input.config.clone()),
                    queries_timeout.as_micros() as u64,
                    true,
                );
            }
            let _ = tx.send(Err(RpcError::InternalError));
        }
    };

    // The row loop does not yield while rows keep arriving; on a blocking thread it cannot hold an async worker.
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || runtime.block_on(fetch));

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

pub fn load_sql(input: &GpaDbQueryInput) -> GpaSql {
    let sql = include_str!("../db/getProgramAccounts.sql");
    let sql = render_template_params(sql, &input.program, input.filter_params.len() + 1);
    let sql = sql.replace("-- {accounts_filters}", &input.accounts_filters);
    let sql = sql.replace("-- {snapshot_filters}", &input.snapshot_filters);

    GpaSql {
        sql,
        slot_params: vec![input.latest_slot as i64],
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use cloudbreak_core::modules::rpc_filter_type::Memcmp;

    fn memcmp(offset: usize, bytes: &[u8]) -> RpcFilterType {
        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(offset, bytes.to_vec()))
    }

    #[test]
    fn memcmp_bytes_are_bound_and_index_shape_stays_literal() {
        let filters = vec![
            RpcFilterType::DataSize(165),
            memcmp(8, &[1, 2, 3]),
            memcmp(40, &[9]),
        ];

        let (accounts, snapshot, params) = bound_filters(&filters, false);

        assert_eq!(
            accounts,
            "AND length(accounts.data) = 165\n                    \
             AND SUBSTRING(accounts.data FROM 9 FOR 3) = $1\n                    \
             AND SUBSTRING(accounts.data FROM 41 FOR 1) = $2"
        );
        assert_eq!(
            snapshot,
            accounts.replace("accounts.", "snapshot_accounts.")
        );
        assert_eq!(params, vec![vec![1, 2, 3], vec![9]]);
    }

    #[test]
    fn token_owner_and_mint_values_are_bound() {
        let filters = vec![memcmp(0, &[7; 32]), memcmp(32, &[8; 32])];

        let (accounts, _, params) = bound_filters(&filters, true);

        assert_eq!(
            accounts,
            "AND accounts.token_mint = $1\n                    AND accounts.token_owner = $2"
        );
        assert_eq!(params, vec![vec![7; 32], vec![8; 32]]);
    }

    #[test]
    fn template_program_is_literal_and_slots_follow_filter_params() {
        let program = Pubkey::new_from_array([0xab; 32]);
        let program_sql = format!("'\\x{}'::bytea", "ab".repeat(32));

        let sql = render_template_params(
            "owner = $1 AND slot <= $2 AND cached <= $3 -- {accounts_filters}",
            &program,
            3,
        );

        assert_eq!(
            sql,
            format!(
                "owner = {program_sql} AND slot <= $3 AND cached <= $4 -- {{accounts_filters}}"
            )
        );
    }

    #[test]
    fn rendered_templates_use_every_placeholder_once_in_order() {
        let program = Pubkey::new_unique();
        let filters = vec![memcmp(8, &[1, 2, 3]), RpcFilterType::DataSize(10)];
        let (accounts, snapshot, params) = bound_filters(&filters, false);

        for (template, slot_params) in [
            (include_str!("../db/getProgramAccounts.sql"), 1),
            (include_str!("../modules/cache/gpa_with_cache.sql"), 2),
        ] {
            let sql = render_template_params(template, &program, params.len() + 1)
                .replace("-- {accounts_filters}", &accounts)
                .replace("-- {snapshot_filters}", &snapshot);

            let used: std::collections::BTreeSet<usize> = sql
                .split('$')
                .skip(1)
                .map(|s| {
                    let digits = s.bytes().take_while(u8::is_ascii_digit).count();
                    s[..digits].parse().expect("placeholder number")
                })
                .collect();

            let expected = (1..=params.len() + slot_params).collect();
            assert_eq!(used, expected);
            assert!(sql.contains(&hex::encode(program.as_ref())));
        }
    }
}
