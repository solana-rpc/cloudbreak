// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use reqwest::Client;
use serde::Serialize;
use cloudbreak_core::modules::rpc_filter_type::RpcProgramAccountsConfig;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info, warn};

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Serialize)]
struct QueryKey<'a> {
    program: &'a str,
    config: Option<&'a RpcProgramAccountsConfig>,
}

struct BufferedQuery {
    program: Pubkey,
    config: Option<RpcProgramAccountsConfig>,
    count: u32,
}

#[derive(Serialize)]
struct QueryBatchEntry {
    program: String,
    config: Option<RpcProgramAccountsConfig>,
    count: u32,
}

#[derive(Serialize)]
struct JsonRpcRequest<T: Serialize> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: T,
}

pub struct QueryTrackerClient {
    buffer: Arc<Mutex<HashMap<String, BufferedQuery>>>,
}

impl QueryTrackerClient {
    pub fn new(
        endpoint: &str,
        timeout: Option<Duration>,
        flush_interval: Option<Duration>,
    ) -> Self {
        if endpoint.is_empty() || endpoint.eq("http://") {
            panic!(
                "Query tracker client is enabled but no valid endpoint is configured. Queries will not be tracked."
            );
        }

        let client = Self {
            buffer: Arc::new(Mutex::new(HashMap::new())),
        };

        client.spawn_flush_task(
            endpoint.to_string(),
            timeout.unwrap_or(Duration::from_secs(5)),
            flush_interval.unwrap_or(DEFAULT_FLUSH_INTERVAL),
        );

        client
    }

    /// Buffer a query locally for later batch submission. This is a cheap
    /// in-memory operation with no network calls.
    pub fn buffer_query(&self, program: Pubkey, config: Option<RpcProgramAccountsConfig>) {
        let key = serialize_query_key(&program, config.as_ref());

        match self.buffer.lock() {
            Ok(mut buf) => {
                let entry = buf.entry(key).or_insert_with(|| BufferedQuery {
                    program,
                    config,
                    count: 0,
                });
                entry.count += 1;
            }
            Err(e) => {
                warn!("Failed to acquire query buffer lock: {}", e);
            }
        }
    }

    fn spawn_flush_task(&self, endpoint: String, timeout: Duration, interval: Duration) {
        let buffer = self.buffer.clone();

        let http_client = Client::builder()
            .timeout(timeout)
            .build()
            .expect("Failed to build HTTP client for query tracker");

        info!(
            "Query tracker flush task started (interval: {})",
            humantime::format_duration(interval)
        );

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                let entries: HashMap<String, BufferedQuery> = {
                    match buffer.lock() {
                        Ok(mut buf) => buf.drain().collect(),
                        Err(e) => {
                            warn!("Failed to acquire query buffer lock for flush: {}", e);
                            continue;
                        }
                    }
                };

                if entries.is_empty() {
                    continue;
                }

                let batch_len = entries.len();
                let batch: Vec<QueryBatchEntry> = entries
                    .into_values()
                    .map(|bq| QueryBatchEntry {
                        program: bq.program.to_string(),
                        config: bq.config,
                        count: bq.count,
                    })
                    .collect();

                let request = JsonRpcRequest {
                    jsonrpc: "2.0",
                    id: 1,
                    method: "trackQueries",
                    params: (batch,),
                };

                match http_client.post(&endpoint).json(&request).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            debug!("Flushed {} tracked queries to {}", batch_len, endpoint);
                        } else {
                            warn!(
                                "Query tracker returned status {} when flushing to {}",
                                resp.status(),
                                endpoint
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Failed to flush tracked queries to {}: {}", endpoint, e);
                    }
                }
            }
        });
    }
}

impl Clone for QueryTrackerClient {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer.clone(),
        }
    }
}

fn serialize_query_key(program: &Pubkey, config: Option<&RpcProgramAccountsConfig>) -> String {
    let key_struct = QueryKey {
        program: &program.to_string(),
        config,
    };

    serde_json::to_string(&key_struct).unwrap_or_else(|_| program.to_string())
}
