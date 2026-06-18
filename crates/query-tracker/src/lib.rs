// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::{QueryTrackerServiceConfig, TryLoadConfig};
use jsonrpsee::server::{Server, ServerConfig};
use sea_orm::{ConnectOptions, Database};
use std::str::FromStr;
use tracing::info;

pub mod error;
pub mod index_listener;
pub mod metrics;
pub mod rpc;
pub mod tracker;

use crate::index_listener::index_listener;
use crate::rpc::{QueryBatchEntry, QueryTrackerRpcServer, QueryTrackerStatus};
use crate::tracker::{
    get_queue_size, get_tracked_query_count, init_query_tracker, query_counts_reset_task,
    track_program_accounts_query,
};

pub use error::{QueryTrackerError, QueryTrackerResult};

pub struct QueryTrackerRpcImpl {
    index_creation_enabled: bool,
}

impl QueryTrackerRpcImpl {
    pub fn new(index_creation_enabled: bool) -> Self {
        Self {
            index_creation_enabled,
        }
    }
}

#[jsonrpsee::core::async_trait]
impl rpc::QueryTrackerRpcServer for QueryTrackerRpcImpl {
    async fn track_query(
        &self,
        program: String,
        config: Option<cloudbreak_core::modules::rpc_filter_type::RpcProgramAccountsConfig>,
    ) -> Result<(), jsonrpsee_types::ErrorObject<'static>> {
        let pubkey = solana_pubkey::Pubkey::from_str(&program).map_err(|e| {
            jsonrpsee_types::ErrorObject::owned(
                -32602,
                format!("Invalid pubkey: {}", e),
                None::<()>,
            )
        })?;

        track_program_accounts_query(pubkey, config.as_ref(), 1, 0);

        Ok(())
    }

    async fn track_queries(
        &self,
        queries: Vec<QueryBatchEntry>,
    ) -> Result<(), jsonrpsee_types::ErrorObject<'static>> {
        for entry in queries {
            let pubkey = solana_pubkey::Pubkey::from_str(&entry.program).map_err(|e| {
                jsonrpsee_types::ErrorObject::owned(
                    -32602,
                    format!("Invalid pubkey: {}", e),
                    None::<()>,
                )
            })?;

            track_program_accounts_query(pubkey, entry.config.as_ref(), entry.count, entry.total_cost_us);
        }

        Ok(())
    }

    async fn get_status(
        &self,
    ) -> Result<QueryTrackerStatus, jsonrpsee_types::ErrorObject<'static>> {
        Ok(QueryTrackerStatus {
            healthy: true,
            tracked_queries: get_tracked_query_count(),
            queue_size: get_queue_size(),
            index_creation_enabled: self.index_creation_enabled,
        })
    }

    async fn get_queue_size(&self) -> Result<u32, jsonrpsee_types::ErrorObject<'static>> {
        Ok(get_queue_size() as u32)
    }

    async fn get_health(&self) -> Result<String, jsonrpsee_types::ErrorObject<'static>> {
        Ok("ok".to_string())
    }
}

pub async fn run(config_path: &str) -> cloudbreak_core::Result<()> {
    let config = QueryTrackerServiceConfig::try_load(config_path)?;

    let server_addr = config.server_addr();
    let metrics_addr = config.metrics_addr();

    tokio::spawn(async move {
        metrics::serve_metrics(metrics_addr).await;
    });

    let database = Database::connect(ConnectOptions::from(config.database.clone())).await?;

    let query_tracker_config = config.query_tracker.clone();

    info!(
        "Query tracker service initialized (threshold: {}, auto-create indexes: {}, delay: {}, reset interval: {})",
        query_tracker_config.index_generation_threshold,
        query_tracker_config.create_database_indexes,
        humantime::format_duration(query_tracker_config.index_creation_delay),
        humantime::format_duration(query_tracker_config.query_counts_reset_interval)
    );

    init_query_tracker(
        query_tracker_config.index_generation_threshold,
        query_tracker_config.cost_eligibility_threshold_us,
        query_tracker_config.cost_weighting,
    );

    let reset_interval = query_tracker_config.query_counts_reset_interval;
    let index_creation_enabled = query_tracker_config.create_database_indexes;

    let listener_db = database.clone();
    tokio::spawn(async move {
        index_listener(listener_db, query_tracker_config).await;
    });

    tokio::spawn(async move {
        query_counts_reset_task(reset_interval).await;
    });

    let server = Server::builder()
        .set_config(
            ServerConfig::builder()
                .max_connections(config.server.max_connections)
                .max_request_body_size(u32::MAX)
                .max_response_body_size(u32::MAX)
                .build(),
        )
        .build(server_addr)
        .await?;

    let rpc = QueryTrackerRpcImpl::new(index_creation_enabled);

    info!("Query Tracker service is starting...");

    let server_handle = server.start(rpc.into_rpc());

    info!(
        "Query Tracker service is running at http://{}. Press Ctrl+C to stop.",
        server_addr
    );

    tokio::signal::ctrl_c().await?;

    info!("Shutdown signal received. Stopping Query Tracker service...");

    server_handle.stop()?;
    server_handle.stopped().await;

    info!("Query Tracker service has been stopped.");

    Ok(())
}
