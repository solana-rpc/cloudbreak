// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

pub mod json_file;
pub mod mismatch_files;
pub mod victoria_logs;

use std::collections::HashSet;
use std::time::Duration;

use crate::{benchmark::RequestType, config::SourceConfig};
use anyhow::Result;
use serde_json::Value as JsonValue;
use tokio::sync::watch;

/// A request to benchmark plus an optional estimated response size in bytes.
///
/// `est_bytes` is populated only by the VictoriaLogs source (from its `bytes`
/// field) and drives the bandwidth cap. Other sources leave it `None`, so those
/// requests run uncapped.
#[derive(Clone, Debug)]
pub struct BenchRequest {
    pub body: JsonValue,
    pub est_bytes: Option<u64>,
}

fn retain_unseen(
    requests: Vec<victoria_logs::LoggedRequest>,
    seen: &mut HashSet<String>,
    replay_once: bool,
) -> Vec<BenchRequest> {
    let to_bench = |r: victoria_logs::LoggedRequest| BenchRequest {
        body: r.body,
        est_bytes: r.bytes,
    };
    if !replay_once {
        return requests.into_iter().map(to_bench).collect();
    }
    requests
        .into_iter()
        .filter(|r| match &r.req_id {
            Some(id) => seen.insert(id.clone()),
            None => true,
        })
        .map(to_bench)
        .collect()
}

/// Loads the requests from the source and returns a watch::Receiver that will be updated with new requests depending on the source type
pub async fn load_requests_from_source(
    source: &SourceConfig,
    request_type: RequestType,
) -> Result<watch::Receiver<Vec<BenchRequest>>, anyhow::Error> {
    match source {
        SourceConfig::JsonFile { path } => {
            let rx = json_file::load_requests(path)?;

            Ok(rx)
        }
        SourceConfig::VictoriaLogs {
            url,
            minutes,
            window_seconds,
            limit,
            min_request_size,
            max_request_size,
            encoding,
            pool_dedicated,
            inject_context: _,
            poll_interval_secs,
            replay_once,
        } => {
            let client = reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .build()?;

            tracing::info!(
                target: "bench_source",
                "Fetching initial requests from VictoriaLogs"
            );

            let replay_once = *replay_once;
            let mut seen: HashSet<String> = HashSet::new();

            let initial_requests = victoria_logs::get_requests(
                &client,
                url,
                request_type,
                *min_request_size,
                *max_request_size,
                encoding.as_deref(),
                *minutes,
                *window_seconds,
                *limit,
                pool_dedicated.as_deref(),
            )
            .await?;
            let initial_requests = retain_unseen(initial_requests, &mut seen, replay_once);

            tracing::info!(
                target: "bench_source",
                "Fetched {} requests from VictoriaLogs",
                initial_requests.len()
            );

            let (tx, rx) = watch::channel(initial_requests);

            let url = url.clone();
            let min_request_size = *min_request_size;
            let max_request_size = *max_request_size;
            let encoding = encoding.clone();
            let pool_dedicated = pool_dedicated.clone();
            let minutes = *minutes;
            let window_seconds = *window_seconds;
            let limit = *limit;
            let poll_interval_secs = *poll_interval_secs;

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(poll_interval_secs)).await;
                    match victoria_logs::get_requests(
                        &client,
                        &url,
                        request_type,
                        min_request_size,
                        max_request_size,
                        encoding.as_deref(),
                        minutes,
                        window_seconds,
                        limit,
                        pool_dedicated.as_deref(),
                    )
                    .await
                    {
                        Ok(new_requests) => {
                            let new_requests = retain_unseen(new_requests, &mut seen, replay_once);
                            if replay_once && new_requests.is_empty() {
                                continue;
                            }
                            tracing::info!(
                                target: "bench_source",
                                "Refreshed {} requests from VictoriaLogs",
                                new_requests.len()
                            );
                            if tx.send(new_requests).is_err() {
                                break; // all receivers dropped, stop fetching
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                target: "bench_source",
                                "Failed to refresh requests: {}", e
                            );
                        }
                    }
                }
            });

            Ok(rx)
        }
        SourceConfig::MismatchDir {
            path,
            inject_context,
        } => {
            let rx = mismatch_files::load_requests(path, *inject_context)?;
            Ok(rx)
        }
    }
}
