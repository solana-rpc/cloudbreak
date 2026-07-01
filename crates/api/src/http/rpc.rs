// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use bytes::Bytes;
use cloudbreak_core::modules::rpc_filter_type::RpcProgramAccountsConfig;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use hyper::{Request, StatusCode};
use serde::Serialize;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcContextConfig};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::time::Instant;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::http::server::{HttpHandlerResponse, ResponseBody};
use crate::http::streaming::gpa_streaming_response_body;
use crate::http::{
    JsonRpcRequest, JsonRpcResponse, RpcRequestPayload, extract_param, make_error_response,
};
use crate::methods::slot::RpcGetSlotConfig;
use crate::methods::token::{
    TokenAccountsFilter, TokenQueryType, get_token_accounts_by_owner_or_delegate,
};
use crate::{db_query, methods, metrics};

pub async fn handle_rpc_request(
    req: Request<Incoming>,
    state: Arc<CloudbreakRpcState>,
    subscription_id: &str,
) -> HttpHandlerResponse {
    let body = match http_body_util::BodyExt::collect(req.into_body()).await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return make_error_response(
                serde_json::Value::Null,
                -32700,
                format!("Parse error: {}", e),
            );
        }
    };

    let payload: RpcRequestPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return make_error_response(
                serde_json::Value::Null,
                -32700,
                format!("Parse error: {}", e),
            );
        }
    };

    match payload {
        RpcRequestPayload::Single(req) => {
            process_single_request(req, &state, subscription_id, false).await
        }
        RpcRequestPayload::Batch(requests) => process_batch(requests, state, subscription_id).await,
    }
}

async fn process_batch(
    requests: Vec<JsonRpcRequest>,
    state: Arc<CloudbreakRpcState>,
    subscription_id: &str,
) -> HttpHandlerResponse {
    let batch_size = requests.len();
    metrics::CLOUDBREAK_API_BATCH_REQUESTS
        .with_label_values(&[metrics::batch_size_bucket(batch_size)])
        .inc();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        state.batch_handling_max_concurrency,
    ));
    let subscription_id = subscription_id.to_string();
    let mut handles = Vec::with_capacity(batch_size);

    for req in requests {
        let state = state.clone();
        let sub_id = subscription_id.clone();
        let sem = semaphore.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            process_single_request(req, &state, &sub_id, true).await
        }));
    }

    let mut all_results = Vec::with_capacity(batch_size);
    for handle in handles {
        all_results.push(handle.await.unwrap());
    }

    let responses: Vec<serde_json::Value> = all_results
        .into_iter()
        .filter_map(|r| match r.body {
            ResponseBody::Buffered(bytes) => serde_json::from_slice(&bytes).ok(),
            ResponseBody::Streaming(_) => {
                // Unreachable in practice because we pass `in_batch = true`
                tracing::error!("Streaming body encountered inside batch context; dropping entry");
                None
            }
        })
        .collect();
    let body = serde_json::to_vec(&responses).unwrap_or_default();

    HttpHandlerResponse {
        status: StatusCode::OK,
        body: ResponseBody::Buffered(body),
    }
}

/// Note: `in_batch` param, will make the streamed response to be buffered
/// into a `Vec<u8>` and returned as a `ResponseBody::Buffered(Vec<u8>)`.
async fn process_single_request(
    rpc_request: JsonRpcRequest,
    state: &Arc<CloudbreakRpcState>,
    subscription_id: &str,
    in_batch: bool,
) -> HttpHandlerResponse {
    let id = rpc_request.id.clone();
    let method = rpc_request.method.as_str();

    let response_bytes: Vec<u8> = match method {
        "getHealth" => {
            let healthy = db_query::get_service_health(&state.database).await;

            let result = if !healthy {
                Err(RpcError::InternalError)
            } else {
                Ok(serde_json::Value::String("ok".to_string()))
            };

            json_serialize_response(id, result).await
        }
        "getSlot" => {
            let config: Option<RpcGetSlotConfig> =
                extract_param(&rpc_request.params, 0).ok().flatten();
            let slot = methods::slot::get_slot(state, config).await;

            json_serialize_response(id, slot).await
        }
        "getVersion" => {
            let version = methods::version::get_version(state).await;

            json_serialize_response(id, version).await
        }
        "getGenesisHash" => {
            let hash = methods::genesis::get_genesis_hash(state).await;

            json_serialize_response(id, hash).await
        }
        "getVoteAccounts" => {
            let config: Option<methods::vote_accounts::GetVoteAccountsConfig> =
                extract_param(&rpc_request.params, 0).ok().flatten();
            let result = methods::vote_accounts::get_vote_accounts(state, config).await;
            json_serialize_response(id, result).await
        }
        "getAccountInfo" => {
            let start_time = Instant::now();

            let pubkey: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcAccountInfoConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result = methods::get_account_info::get_account_info(state, pubkey, config).await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(target: "api_request_errors_count", "getAccountInfo error: {:?}", result.as_ref().unwrap_err());
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["gAI", status_label])
                .inc();

            let json_response = json_serialize_response(id, result).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&["gAI", metrics::bytes_bucket(json_response.len() as u64)])
                .observe(start_time.elapsed().as_millis() as f64);

            json_response
        }
        "getBalance" => {
            let start_time = Instant::now();

            let pubkey: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcContextConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result = methods::get_balance::get_balance(state, pubkey, config).await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(target: "api_request_errors_count", "getBalance error: {:?}", result.as_ref().unwrap_err());
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getBalance", status_label])
                .inc();

            let json_response = json_serialize_response(id, result).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getBalance",
                    metrics::bytes_bucket(json_response.len() as u64),
                ])
                .observe(start_time.elapsed().as_millis() as f64);

            json_response
        }
        "getMultipleAccounts" => {
            let start_time = Instant::now();

            let pubkeys: Vec<String> = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcAccountInfoConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result =
                methods::get_multiple_accounts::get_multiple_accounts(state, pubkeys, config).await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(
                    target: "api_request_errors_count",
                    "getMultipleAccounts error: {:?}",
                    result.as_ref().unwrap_err()
                );
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getMultipleAccounts", status_label])
                .inc();

            let json_response = json_serialize_response(id, result).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getMultipleAccounts",
                    metrics::bytes_bucket(json_response.len() as u64),
                ])
                .observe(start_time.elapsed().as_millis() as f64);

            json_response
        }
        "getProgramAccounts" => {
            let gpa_global_start_time = Instant::now();

            let program: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcProgramAccountsConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let gpa_response = match methods::program::get_program_accounts(state, program, config)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "api_request_errors_count", "getProgramAccounts error: {:?}", e);
                    metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                        .with_label_values(&["gPA", "error"])
                        .inc();
                    return make_error_response(
                        id,
                        e.to_numeric_code(),
                        e.to_error_code().to_string(),
                    );
                }
            };

            let body = match gpa_streaming_response_body(
                id.clone(),
                gpa_response,
                gpa_global_start_time,
                subscription_id.to_string(),
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "api_request_errors_count", "getProgramAccounts error: {:?}", e);
                    metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                        .with_label_values(&["gPA", "error"])
                        .inc();
                    return make_error_response(
                        id,
                        e.to_numeric_code(),
                        e.to_error_code().to_string(),
                    );
                }
            };

            if in_batch {
                // Await and collect the streaming body into a `Vec<u8>`
                gpa_streamed_to_buffered(body, id).await
            } else {
                return HttpHandlerResponse {
                    status: StatusCode::OK,
                    body: ResponseBody::Streaming(body),
                };
            }
        }
        "getTokenAccountsByMint" => {
            let gpa_global_start_time = Instant::now();

            let mint: String = match extract_param(&rpc_request.params, 0) {
                Ok(m) => m,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<methods::mint_accounts::GetTokenAccountsByMintConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let gpa_response = match methods::mint_accounts::get_token_accounts_by_mint(
                state, mint, config,
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "api_request_errors_count", "getTokenAccountsByMint error: {:?}", e);
                    metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                        .with_label_values(&["gTABM", "error"])
                        .inc();
                    return make_error_response(
                        id,
                        e.to_numeric_code(),
                        e.to_error_code().to_string(),
                    );
                }
            };

            let body = match gpa_streaming_response_body(
                id.clone(),
                gpa_response,
                gpa_global_start_time,
                subscription_id.to_string(),
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "api_request_errors_count", "getTokenAccountsByMint error: {:?}", e);
                    metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                        .with_label_values(&["gTABM", "error"])
                        .inc();
                    return make_error_response(
                        id,
                        e.to_numeric_code(),
                        e.to_error_code().to_string(),
                    );
                }
            };

            if in_batch {
                gpa_streamed_to_buffered(body, id).await
            } else {
                return HttpHandlerResponse {
                    status: StatusCode::OK,
                    body: ResponseBody::Streaming(body),
                };
            }
        }
        "getTokenAccountBalance" => {
            let start_time = Instant::now();

            let pubkey: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            // Agave: only a CommitmentConfig is accepted here (no minContextSlot).
            let commitment: Option<CommitmentConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result = methods::get_token_account_balance::get_token_account_balance(
                state, pubkey, commitment,
            )
            .await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(
                    target: "api_request_errors_count",
                    "getTokenAccountBalance error: {:?}",
                    result.as_ref().unwrap_err()
                );
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getTokenAccountBalance", status_label])
                .inc();

            let json_response = json_serialize_response(id, result).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getTokenAccountBalance",
                    metrics::bytes_bucket(json_response.len() as u64),
                ])
                .observe(start_time.elapsed().as_millis() as f64);

            json_response
        }
        "getTokenSupply" => {
            let start_time = Instant::now();

            let pubkey: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };

            let commitment: Option<CommitmentConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result =
                methods::get_token_supply::get_token_supply(state, pubkey, commitment).await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(
                    target: "api_request_errors_count",
                    "getTokenSupply error: {:?}",
                    result.as_ref().unwrap_err()
                );
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getTokenSupply", status_label])
                .inc();

            let json_response = json_serialize_response(id, result).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getTokenSupply",
                    metrics::bytes_bucket(json_response.len() as u64),
                ])
                .observe(start_time.elapsed().as_millis() as f64);

            json_response
        }
        "getTokenAccountsByOwner" => {
            let owner: String = match extract_param(&rpc_request.params, 0) {
                Ok(o) => o,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let filter: TokenAccountsFilter = match extract_param(&rpc_request.params, 1) {
                Ok(f) => f,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcAccountInfoConfig> =
                extract_param(&rpc_request.params, 2).ok().flatten();

            let start_time = Instant::now();
            let query_type = TokenQueryType::GetTokenAccountsByOwner;

            let result = get_token_accounts_by_owner_or_delegate(
                state, owner, filter, config, query_type, None,
            )
            .await;
            let (response, metrics_data) = match result {
                Ok(result) => (Ok(result.response), result.metrics_data),
                Err(e) => (Err(e), None),
            };

            let json_start_time = Instant::now();

            let json_response = json_serialize_response(id, response).await;
            let response_size = json_response.len() as u64;

            if let Some(metrics_data) = metrics_data {
                metrics_data.record_metrics(
                    json_start_time.elapsed().as_millis() as f64,
                    start_time.elapsed().as_millis() as f64,
                    response_size,
                    0,
                    0.0,
                    subscription_id.to_string(),
                );
            } else {
                tracing::error!(target: "api_request_errors_count", "getTokenAccountsByOwner error: no metrics data");
                metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                    .with_label_values(&["gTABO", "error"])
                    .inc();
            }

            json_response
        }
        "getTokenAccountsByDelegate" => {
            let delegate: String = match extract_param(&rpc_request.params, 0) {
                Ok(d) => d,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let filter: TokenAccountsFilter = match extract_param(&rpc_request.params, 1) {
                Ok(f) => f,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcAccountInfoConfig> =
                extract_param(&rpc_request.params, 2).ok().flatten();

            let start_time = Instant::now();
            let query_type = TokenQueryType::GetTokenAccountsByDelegate;

            let result = get_token_accounts_by_owner_or_delegate(
                state, delegate, filter, config, query_type, None,
            )
            .await;
            let (response, metrics_data) = match result {
                Ok(result) => (Ok(result.response), result.metrics_data),
                Err(e) => (Err(e), None),
            };
            let json_start_time = Instant::now();

            let json_response = json_serialize_response(id, response).await;
            let response_size = json_response.len() as u64;

            if let Some(metrics_data) = metrics_data {
                metrics_data.record_metrics(
                    json_start_time.elapsed().as_millis() as f64,
                    start_time.elapsed().as_millis() as f64,
                    response_size,
                    0,
                    0.0,
                    subscription_id.to_string(),
                );
            } else {
                tracing::error!(target: "api_request_errors_count", "getTokenAccountsByDelegate error: no metrics data");
                metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                    .with_label_values(&["gTABD", "error"])
                    .inc();
            }

            json_response
        }
        _ => {
            return make_error_response(id, -32601, format!("Method not found: {}", method));
        }
    };

    HttpHandlerResponse {
        status: StatusCode::OK,
        body: ResponseBody::Buffered(response_bytes),
    }
}

#[tracing::instrument(name = "json_encoding", skip_all)]
async fn json_serialize_response<T: Serialize + Send + 'static>(
    id: serde_json::Value,
    result: Result<T, RpcError>,
) -> Vec<u8> {
    let res = match result {
        Ok(value) => tokio::task::spawn_blocking(move || {
            let response = JsonRpcResponse::success(id, value);
            serde_json::to_vec(&response)
        })
        .await
        .unwrap_or_else(|_| {
            tracing::error!("Failed to join handle for json_serialize_response");
            Ok(vec![])
        }),
        Err(e) => {
            let response = JsonRpcResponse::<()>::error(
                id,
                e.to_numeric_code(),
                e.to_error_code().to_string(),
            );
            serde_json::to_vec(&response)
        }
    };

    res.unwrap_or_else(|_| {
        tracing::error!("Failed to json_serialize_response");
        vec![]
    })
}

async fn gpa_streamed_to_buffered(
    body: UnsyncBoxBody<Bytes, Infallible>,
    id: serde_json::Value,
) -> Vec<u8> {
    let collected = http_body_util::BodyExt::collect(body)
        .await
        .expect("streaming body error type is Infallible");

    let bytes = collected.to_bytes().to_vec();

    // If the body was truncated mid-stream, the bytes won't be valid JSON
    let valid = serde_json::from_slice::<serde_json::Value>(&bytes).is_ok();

    if valid {
        bytes
    } else {
        tracing::error!(target: "api_request_errors_count", "getProgramAccounts streaming body was truncated mid-flight;");
        metrics::CLOUDBREAK_API_REQUESTS_TOTAL
            .with_label_values(&["gPA", "error"])
            .inc();
        let err_response = JsonRpcResponse::<()>::error(
            id,
            RpcError::InternalError.to_numeric_code(),
            RpcError::InternalError.to_error_code().to_string(),
        );
        serde_json::to_vec(&err_response).unwrap_or_default()
    }
}
