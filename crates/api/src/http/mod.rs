// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::http::server::HttpHandlerResponse;
use crate::http::server::ResponseBody;
use crate::modules::cache::GpaProcessor;
use crate::modules::supply_cache::SharedSupplySnapshot;
use crate::modules::vote_accounts_cache::SharedStakesSnapshot;
use crate::error::RpcError;
use crate::query_tracker_client::QueryTrackerClient;
use crate::slot_syncronizer::SlotSyncronizerData;
use agave_feature_set::FeatureSet;
use hyper::StatusCode;
use sea_orm::{DatabaseConnection, EntityTrait};
use serde::{Deserialize, Serialize};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::Response as RpcResponse;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::Instrument;
use cloudbreak_core::{AccountSelectorConfig, ProcessedCommitmentBehavior, UnhealthyResponseBehavior};
use cloudbreak_entity::slots;

#[derive(Clone)]
pub struct CachedFeatureSet {
    pub set: Arc<FeatureSet>,
    pub built_at: Instant,
}

pub mod operational_endpoints;
pub mod rpc;
pub mod server;
pub mod streaming;

#[derive(Clone)]
pub struct CloudbreakRpcState {
    pub database: DatabaseConnection,
    pub query_tracker_client: Option<QueryTrackerClient>,
    pub queries_timeout: Duration,
    pub slot_syncronizer_data: Option<Arc<RwLock<SlotSyncronizerData>>>,
    pub indexer_filter: Arc<AccountSelectorConfig>,
    pub batch_handling_max_concurrency: usize,
    pub gpa_stream_batch_size: usize,
    pub request_timeout: Duration,
    pub processed_commitment: ProcessedCommitmentBehavior,
    pub unhealthy_response: UnhealthyResponseBehavior,
    pub gpa_processor: GpaProcessor,
    pub genesis_hash: String,
    pub vote_accounts_supported: bool,
    pub stakes_cache: SharedStakesSnapshot,
    pub max_multiple_accounts: usize,
    pub simulation_supported: bool,
    pub supply_supported: bool,
    pub supply_cache: SharedSupplySnapshot,
    pub feature_set_cache: Arc<RwLock<Option<CachedFeatureSet>>>,
    pub largest_accounts_mints: Option<Arc<HashSet<Pubkey>>>,
}

impl CloudbreakRpcState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database: DatabaseConnection,
        queries_timeout: Duration,
        slot_syncronizer_data: Option<Arc<RwLock<SlotSyncronizerData>>>,
        client: Option<QueryTrackerClient>,
        indexer_filter: Arc<AccountSelectorConfig>,
        batch_handling_max_concurrency: usize,
        gpa_stream_batch_size: usize,
        request_timeout: Duration,
        processed_commitment: ProcessedCommitmentBehavior,
        unhealthy_response: UnhealthyResponseBehavior,
        gpa_processor: GpaProcessor,
        genesis_hash: String,
        vote_accounts_supported: bool,
        stakes_cache: SharedStakesSnapshot,
        max_multiple_accounts: usize,
        simulation_supported: bool,
        largest_accounts_mints: Option<Arc<HashSet<Pubkey>>>,
        supply_supported: bool,
        supply_cache: SharedSupplySnapshot,
    ) -> Self {
        Self {
            database,
            queries_timeout,
            slot_syncronizer_data,
            query_tracker_client: client,
            indexer_filter,
            batch_handling_max_concurrency,
            gpa_stream_batch_size,
            request_timeout,
            processed_commitment,
            unhealthy_response,
            gpa_processor,
            genesis_hash,
            vote_accounts_supported,
            stakes_cache,
            max_multiple_accounts,
            simulation_supported,
            supply_supported,
            supply_cache,
            feature_set_cache: Arc::new(RwLock::new(None)),
            largest_accounts_mints,
        }
    }

    /// Builds a [`RpcError::NodeUnhealthy`], baking in whether it should be
    /// surfaced as an HTTP `503` (per the `unhealthy-response` config) so the
    /// response layer can decide the HTTP status purely from the error.
    pub fn node_unhealthy(&self) -> RpcError {
        RpcError::NodeUnhealthy {
            service_unavailable: self.unhealthy_response == UnhealthyResponseBehavior::HttpUnavailable,
        }
    }

    /// Resolves the latest slot and its block time for the given commitment.
    ///
    /// When the slot syncronizer is enabled the cached in-memory value is used,
    /// otherwise the `slots` table is queried directly. If the service is marked
    /// unhealthy (via the denormalised `slots.health` flag) this returns
    /// [`RpcError::NodeUnhealthy`] instead of a slot.
    pub async fn latest_slot_and_block_time(
        &self,
        commitment: CommitmentLevel,
    ) -> Result<(u64, i64), RpcError> {
        match &self.slot_syncronizer_data {
            Some(data) => {
                let data = data.read().expect("Failed to read slot syncronizer data");

                if !data.is_healthy() {
                    return Err(self.node_unhealthy());
                }

                Ok((
                    data.get_slot_for_commitment(commitment),
                    data.get_block_time_for_commitment(commitment),
                ))
            }
            None => {
                let slot_model = slots::Entity::find_by_id(commitment as i32)
                    .one(&self.database)
                    .instrument(tracing::info_span!("slot_db"))
                    .await?;

                let model = slot_model.ok_or(RpcError::InternalError)?;

                if !model.health {
                    return Err(self.node_unhealthy());
                }

                Ok((model.slot as u64, model.block_time))
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum CloudbreakApiResponse<T> {
    Response(T),
    ResponseWithContext(RpcResponse<T>),
}

// ============================================================================
// JSON-RPC Types
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum RpcRequestPayload {
    Single(JsonRpcRequest),
    Batch(Vec<JsonRpcRequest>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
    pub id: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse<T> {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl<T: Serialize> JsonRpcResponse<T> {
    pub fn success(id: serde_json::Value, result: T) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: serde_json::Value, code: i32, message: String) -> JsonRpcResponse<()> {
        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }
}

fn extract_param<T: serde::de::DeserializeOwned>(
    params: &serde_json::Value,
    index: usize,
) -> Result<T, String> {
    match params {
        serde_json::Value::Array(arr) => arr
            .get(index)
            .ok_or_else(|| format!("Missing parameter at index {}", index))
            .and_then(|v| {
                serde_json::from_value(v.clone()).map_err(|e| format!("Invalid parameter: {}", e))
            }),
        serde_json::Value::Null => Err(format!("Missing parameter at index {}", index)),
        _ => Err("Parameters must be an array".to_string()),
    }
}

fn make_error_response(id: serde_json::Value, code: i32, message: String) -> HttpHandlerResponse {
    make_error_response_with_status(id, code, message, StatusCode::OK)
}

fn make_error_response_with_status(
    id: serde_json::Value,
    code: i32,
    message: String,
    status: StatusCode,
) -> HttpHandlerResponse {
    let response = JsonRpcResponse::<()>::error(id, code, message);
    HttpHandlerResponse {
        status,
        body: ResponseBody::Buffered(serde_json::to_vec(&response).unwrap()),
    }
}

/// Maps an [`RpcError`] to the HTTP status the response should carry. Almost
/// everything is a `200 OK` JSON-RPC error; the sole exception is an unhealthy
/// node when `unhealthy-response = "http-unavailable"`, which is baked into the
/// error at its source (see [`CloudbreakRpcState::node_unhealthy`]) and surfaced
/// here as HTTP `503 Service Unavailable`.
pub fn http_status_for_error(err: &RpcError) -> StatusCode {
    match err {
        RpcError::NodeUnhealthy {
            service_unavailable: true,
        } => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::OK,
    }
}
