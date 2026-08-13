// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use futures::future;
use sea_orm::{ConnectOptions, Database};
use std::sync::Arc;
use std::time::Duration;
use cloudbreak_core::{ApiConfig, EnvironmentInfo, TryLoadConfig};

use crate::{
    http::{CloudbreakRpcState, HeaderKeys},
    metrics::setup_metrics,
    modules::{cache::GpaProcessor, vote_accounts_cache},
    query_tracker_client::QueryTrackerClient,
};
use std::sync::RwLock;
use tracing::info;

pub mod db_query;
pub mod error;
mod http;
pub mod methods;
pub mod metrics;
mod modules;
pub mod query_tracker_client;
mod slot_syncronizer;

pub async fn run(config: &str) -> cloudbreak_core::Result<()> {
    let config = ApiConfig::try_load(config)?;

    setup_metrics(&config)?;

    let mut database_connect_options = ConnectOptions::from(config.database.clone());
    let index_timeout = config.database.api_queries_timeout * 1000;
    database_connect_options.map_sqlx_postgres_opts(move |pg_opts| {
        pg_opts.options([("statement_timeout", index_timeout.to_string())])
    });

    let database = Database::connect(database_connect_options)
        .await
        .expect("Failed to create index listener database connection");

    let query_tracker_client: Option<QueryTrackerClient> = match &config.query_tracker_client {
        Some(qt_config) => {
            info!(
                "Connecting to remote Query Tracker service at {}",
                &qt_config.endpoint
            );
            Some(QueryTrackerClient::new(
                &qt_config.endpoint,
                qt_config.timeout,
                qt_config.flush_interval,
                qt_config.max_buffered_identities,
                qt_config.max_batch_size,
            ))
        }
        None => {
            info!(
                "Query tracker client disabled (no [query-tracker-client] config section); query patterns will not be tracked."
            );
            None
        }
    };

    let header_keys = HeaderKeys {
        subscription_id: config.metrics.subscription_id_key.clone(),
        request_id: config.metrics.request_id_key.clone(),
        client_ip: config.metrics.client_ip_key.clone(),
    };

    let (mut slot_syncronizer_handle, slot_syncronizer_data) =
        match slot_syncronizer::start_slot_syncronizer(database.clone(), &config) {
            Some((handle, data)) => (future::Either::Left(handle), Some(data)),
            None => (future::Either::Right(future::pending()), None),
        };

    let queries_timeout = Duration::from_secs(config.database.api_queries_timeout);

    let indexer_filter = Arc::new(
        EnvironmentInfo::load_filters(&database)
            .await
            .expect("Failed to load indexer filter"),
    );
    let batch_handling_max_concurrency = config.server.batch_handling_max_concurrency;
    let gpa_stream_batch_size = config.server.gpa_stream_batch_size;
    let request_timeout = config.server.request_timeout;
    let max_multiple_accounts = config.server.max_multiple_accounts;

    // Setup optional module cache
    let gpa_processor = GpaProcessor::new(config.gpa_cache.clone());

    let simulation_supported = indexer_filter.supports_simulation();
    info!("simulateTransaction: supported: {}", simulation_supported);

    let vote_accounts_supported = indexer_filter.supports_vote_accounts();
    let stakes_cache: vote_accounts_cache::SharedStakesSnapshot = Arc::new(RwLock::new(Arc::new(
        vote_accounts_cache::StakesSnapshot::empty(),
    )));
    if vote_accounts_supported {
        match vote_accounts_cache::load_latest_stakes(&database).await {
            Ok(Some(snapshot)) => {
                info!(
                    "Loaded initial stakes snapshot: epoch {} ({} voters)",
                    snapshot.epoch,
                    snapshot.voters.len()
                );
                *stakes_cache.write().unwrap() = Arc::new(snapshot);
            }
            Ok(None) => {
                tracing::warn!(
                    "epoch_stakes table is empty at startup; getVoteAccounts will fail until \
                     the indexer processes a snapshot"
                );
            }
            Err(e) => {
                tracing::error!("Failed to load initial stakes snapshot: {:?}", e);
            }
        }
        vote_accounts_cache::spawn_poll_task(database.clone(), stakes_cache.clone());
        info!("getVoteAccounts: supported=true (Vote+Stake programs are indexed)");
    } else {
        info!(
            "getVoteAccounts: supported=false (Vote and/or Stake programs are not in the \
             indexer's filter set)"
        );
    }

    let state = CloudbreakRpcState::new(
        database,
        queries_timeout,
        slot_syncronizer_data,
        query_tracker_client,
        indexer_filter,
        batch_handling_max_concurrency,
        gpa_stream_batch_size,
        request_timeout,
        config.processed_commitment,
        config.unhealthy_response,
        gpa_processor,
        config.genesis_hash.clone(),
        vote_accounts_supported,
        stakes_cache,
        max_multiple_accounts,
        simulation_supported,
    );

    info!("Server is starting...");

    let server = http::server::HttpServer::new(state, header_keys);

    tokio::select! {
        result = server.run(config.server_addr()) => { match result {
            Ok(_) => {
                info!("Server has been stopped OK.");
            }
            Err(e) => {
                tracing::error!("Error running server: {:?}", e);
            }
        } }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received. Stopping server...");
        }
        result = &mut slot_syncronizer_handle => {
            tracing::error!("Slot synchronizer stopped unexpectedly: {:?}", result);
        }
    }

    Ok(())
}
