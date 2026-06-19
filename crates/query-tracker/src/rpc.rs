// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use jsonrpsee::proc_macros::rpc;
use serde::{Deserialize, Serialize};
use cloudbreak_core::modules::rpc_filter_type::RpcProgramAccountsConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTrackerStatus {
    pub healthy: bool,
    pub tracked_queries: usize,
    pub queue_size: usize,
    pub index_creation_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryBatchEntry {
    pub program: String,
    pub config: Option<RpcProgramAccountsConfig>,
    pub count: u32,
}

#[rpc(server, client)]
pub trait QueryTrackerRpc {
    #[method(name = "trackQuery")]
    async fn track_query(
        &self,
        program: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> Result<(), jsonrpsee_types::ErrorObject<'static>>;

    #[method(name = "trackQueries")]
    async fn track_queries(
        &self,
        queries: Vec<QueryBatchEntry>,
    ) -> Result<(), jsonrpsee_types::ErrorObject<'static>>;

    #[method(name = "getStatus")]
    async fn get_status(&self) -> Result<QueryTrackerStatus, jsonrpsee_types::ErrorObject<'static>>;

    #[method(name = "getQueueSize")]
    async fn get_queue_size(&self) -> Result<u32, jsonrpsee_types::ErrorObject<'static>>;

    #[method(name = "getHealth")]
    async fn get_health(&self) -> Result<String, jsonrpsee_types::ErrorObject<'static>>;
}
