// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::modules::rpc_filter_type::{RpcFilterType, RpcProgramAccountsConfig};
use serde::Serialize;
use solana_pubkey::Pubkey;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::Duration;
use tracing::{info, warn};

lazy_static::lazy_static! {
    static ref PROGRAM_ACCOUNTS_QUERY_COUNTS: Mutex<HashMap<String, u32>> = Mutex::new(HashMap::new());
    static ref PRIORITY_QUEUE: Mutex<BinaryHeap<PrioritizedQuery>> = Mutex::new(BinaryHeap::new());
    static ref QUEUE_CONDVAR: Condvar = Condvar::new();
}

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const MAX_PRIORITY_QUEUE_SIZE: usize = 1000;
const MAX_TRACKED_QUERIES: usize = 10_000;

static INDEX_GENERATION_THRESHOLD: OnceLock<u32> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct PrioritizedQuery {
    pub count: u32,
    pub key: String,
    pub program: Pubkey,
    pub config: Option<RpcProgramAccountsConfig>,
}

impl PartialEq for PrioritizedQuery {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && self.key == other.key
    }
}

impl Eq for PrioritizedQuery {}

impl PartialOrd for PrioritizedQuery {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritizedQuery {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.count.cmp(&other.count) {
            Ordering::Equal => self.key.cmp(&other.key),
            other => other,
        }
    }
}

pub fn init_query_tracker(threshold: u32) {
    let _ = INDEX_GENERATION_THRESHOLD.set(threshold);
}

#[derive(Serialize)]
struct QueryKey<'a> {
    program: &'a str,
    config: Option<&'a RpcProgramAccountsConfig>,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedFilter {
    offsets_and_lengths: Vec<(usize, usize)>,
    datasize: Option<u64>,
}

pub fn track_program_accounts_query(
    program: Pubkey,
    config: Option<&RpcProgramAccountsConfig>,
    increment: u32,
) {
    let key_struct = QueryKey {
        program: &program.to_string(),
        config,
    };

    let key = match serde_json::to_string(&key_struct) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to serialize query key for tracking: {}", e);
            return;
        }
    };

    let threshold = INDEX_GENERATION_THRESHOLD.get().copied().unwrap_or(10);

    match PROGRAM_ACCOUNTS_QUERY_COUNTS.lock() {
        Ok(mut counts) => {
            if counts.len() >= MAX_TRACKED_QUERIES && !counts.contains_key(&key) {
                warn!(
                    "Query tracker has reached maximum capacity ({} unique queries). Clearing old entries.",
                    MAX_TRACKED_QUERIES
                );
                counts.clear();
            }

            let key_for_queue = key.clone();
            let count = counts.entry(key).or_insert(0);
            *count += increment;

            if *count >= threshold {
                match PRIORITY_QUEUE.lock() {
                    Ok(mut queue) => {
                        if queue.len() >= MAX_PRIORITY_QUEUE_SIZE {
                            let key_exists = queue.iter().any(|q| q.key == key_for_queue);
                            if !key_exists {
                                let mut temp_vec: Vec<_> = queue.drain().collect();
                                temp_vec.sort_by(|a, b| b.cmp(a));
                                if temp_vec.len() >= MAX_PRIORITY_QUEUE_SIZE {
                                    temp_vec.pop();
                                    warn!(
                                        "Priority queue at capacity; removed lowest-priority query"
                                    );
                                }
                                for item in temp_vec {
                                    queue.push(item);
                                }
                            }
                        }

                        let mut temp_vec: Vec<_> = queue.drain().collect();
                        let mut found = false;

                        for item in temp_vec.iter_mut() {
                            if item.key == key_for_queue {
                                item.count = *count;
                                found = true;
                                tracing::debug!(
                                    target: "query_tracker_tracker",
                                    "Updated priority queue entry for program '{}' (count: {})",
                                    program, count
                                );
                                break;
                            }
                        }

                        if !found && *count < threshold + increment {
                            temp_vec.push(PrioritizedQuery {
                                count: *count,
                                key: key_for_queue,
                                program,
                                config: config.cloned(),
                            });
                            tracing::debug!(
                                target: "query_tracker_tracker",
                                "Added query for program '{}' to priority queue (count: {})",
                                program, count
                            );
                        }

                        for item in temp_vec {
                            queue.push(item);
                        }

                        drop(queue);
                        QUEUE_CONDVAR.notify_one();
                    }
                    Err(e) => {
                        warn!("Failed to acquire priority queue lock: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            warn!("Failed to acquire query tracker lock: {}", e);
        }
    }
}

fn parse_gpa_config(
    program: &str,
    config: Option<&RpcProgramAccountsConfig>,
) -> Option<ParsedFilter> {
    if program == TOKEN_PROGRAM_ID {
        return None;
    }

    let config = config?;
    let filters = config.filters.as_ref()?;

    let mut offsets_and_lengths = Vec::new();
    let mut datasize = None;

    for filter in filters {
        match filter {
            RpcFilterType::DataSize(size) => {
                datasize = Some(*size);
            }
            RpcFilterType::Memcmp(memcmp) => {
                let offset = memcmp.offset();
                if let Some(bytes_vec) = memcmp.bytes() {
                    let length = bytes_vec.len();
                    offsets_and_lengths.push((offset, length));
                } else {
                    tracing::warn!("Failed to extract bytes from memcmp filter");
                }
            }
            RpcFilterType::TokenAccountState => {}
            // ValueCmp is a value comparison, not an equality/substring match,
            // so it does not map onto index generation; ignore it here.
            RpcFilterType::ValueCmp(_) => {}
        }
    }

    if offsets_and_lengths.is_empty() && datasize.is_none() {
        return None;
    }

    offsets_and_lengths.sort_by_key(|(offset, _)| *offset);

    Some(ParsedFilter {
        offsets_and_lengths,
        datasize,
    })
}

fn generate_create_index(
    program: Pubkey,
    parsed: &ParsedFilter,
    table_name: &str,
) -> (String, String) {
    let program_prefix = &program.to_string()[0..6];

    let mut index_name = program_prefix.to_string();

    for (offset, length) in &parsed.offsets_and_lengths {
        index_name.push_str(&format!("_o{}l{}", offset, length));
    }

    if let Some(size) = parsed.datasize {
        index_name.push_str(&format!("_d{}", size));
    }

    let mut columns = Vec::new();

    for (offset, length) in &parsed.offsets_and_lengths {
        columns.push(format!("substring(data, {}, {})", offset + 1, length));
    }

    columns.push("slot".to_string());

    let columns_str = columns.join(", ");

    let mut where_clause = format!("owner = '\\x{}'::bytea", hex::encode(program.to_bytes()));

    if let Some(size) = parsed.datasize {
        where_clause.push_str(&format!(" AND length(data) = {}", size));
    }

    let sql_query = format!(
        "CREATE INDEX idx_{table_name}_{index_name} ON {table_name} ({columns_str}) WHERE {where_clause}"
    );

    (sql_query, index_name)
}

fn format_rpc_example(program: &str, config: Option<&RpcProgramAccountsConfig>) -> String {
    let rpc_call = serde_json::json!({
        "id": 0,
        "jsonrpc": "2.0",
        "method": "getProgramAccounts",
        "params": [program, config]
    });

    serde_json::to_string(&rpc_call).unwrap_or_else(|_| "{}".to_string())
}

pub fn pop_highest_priority_query() -> Option<PrioritizedQuery> {
    match PRIORITY_QUEUE.lock() {
        Ok(mut queue) => {
            if queue.is_empty() {
                None
            } else {
                queue.pop()
            }
        }
        Err(e) => {
            warn!("Failed to acquire priority queue lock for pop: {}", e);
            None
        }
    }
}

pub fn wait_for_queue_items(timeout_ms: u64) -> bool {
    match PRIORITY_QUEUE.lock() {
        Ok(guard) => {
            let timeout = std::time::Duration::from_millis(timeout_ms);
            let result = QUEUE_CONDVAR
                .wait_timeout_while(guard, timeout, |queue| queue.is_empty())
                .unwrap();
            !result.1.timed_out() || !result.0.is_empty()
        }
        Err(e) => {
            warn!("Failed to acquire priority queue lock for wait: {}", e);
            false
        }
    }
}

pub fn generate_sql_for_query(
    program: Pubkey,
    config: Option<&RpcProgramAccountsConfig>,
    table_name: &str,
) -> Option<(String, String, String)> {
    if let Some(parsed) = parse_gpa_config(&program.to_string(), config) {
        let (sql_query, index_name) = generate_create_index(program, &parsed, table_name);
        let rpc_example = format_rpc_example(&program.to_string(), config);
        Some((sql_query, index_name, rpc_example))
    } else {
        None
    }
}

pub fn clear_query_counts() {
    match PROGRAM_ACCOUNTS_QUERY_COUNTS.lock() {
        Ok(mut counts) => {
            let previous_len = counts.len();
            counts.clear();
            info!(
                target: "query_tracker_tracker",
                "Cleared PROGRAM_ACCOUNTS_QUERY_COUNTS ({} entries removed)",
                previous_len
            );
        }
        Err(e) => {
            warn!("Failed to acquire lock for clearing query counts: {}", e);
        }
    }
}

pub fn get_tracked_query_count() -> usize {
    match PROGRAM_ACCOUNTS_QUERY_COUNTS.lock() {
        Ok(counts) => counts.len(),
        Err(_) => 0,
    }
}

pub fn get_queue_size() -> usize {
    match PRIORITY_QUEUE.lock() {
        Ok(queue) => queue.len(),
        Err(_) => 0,
    }
}

#[tracing::instrument(name = "query_counts_reset_task", skip_all)]
pub async fn query_counts_reset_task(reset_interval: Duration) {
    info!(
        target: "query_tracker_reset",
        "Query counts reset task started with interval {:?} ({:.2} hours)",
        reset_interval,
        reset_interval.as_secs_f64() / 3600.0
    );

    loop {
        tokio::time::sleep(reset_interval).await;
        info!(
            target: "query_tracker_reset",
            "Periodic reset triggered after {:?}",
            reset_interval
        );
        clear_query_counts();
    }
}

pub async fn read_indexer_metrics(metrics_url: &str) -> Option<u64> {
    let client = reqwest::Client::new();
    let response = client.get(metrics_url).send().await.ok()?;
    let body = response.text().await.ok()?;

    for line in body.lines() {
        if line.starts_with("cloudbreak_finalize_slot_handler_queue_size") {
            let value = line
                .split_whitespace()
                .last()
                .and_then(|v| v.parse::<u64>().ok());
            tracing::debug!(target: "query_tracker_metrics", "##### value: {:?}", value);
            return value;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use cloudbreak_core::modules::rpc_filter_type::{Memcmp, MemcmpEncodedBytes};

    #[test]
    fn test_parse_memcmp_bytes_encoding() {
        let bytes = vec![143, 245, 200, 17, 74, 214, 196, 135];
        let memcmp = Memcmp::new(0, MemcmpEncodedBytes::Bytes(bytes.clone()));

        assert_eq!(memcmp.offset(), 0);
        assert_eq!(memcmp.bytes().as_ref().map(|b| b.to_vec()), Some(bytes));
    }

    #[test]
    fn test_parse_memcmp_with_offset() {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new(
                0,
                MemcmpEncodedBytes::Bytes(vec![143, 245, 200, 17, 74, 214, 196, 135]),
            ))]),
            ..Default::default()
        };

        let parsed = parse_gpa_config(
            "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c",
            Some(&config),
        );
        assert!(parsed.is_some());

        let parsed = parsed.unwrap();
        assert_eq!(parsed.offsets_and_lengths.len(), 1);
        assert_eq!(parsed.offsets_and_lengths[0], (0, 8));
        assert_eq!(parsed.datasize, None);
    }

    #[test]
    fn test_generate_index_memcmp_only() {
        let parsed = ParsedFilter {
            offsets_and_lengths: vec![(0, 8)],
            datasize: None,
        };

        let sql = generate_create_index(
            Pubkey::from_str("2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c").unwrap(),
            &parsed,
            "accounts",
        );
        assert!(sql.0.contains("idx_accounts_2wT8Yq_o0l8"));
        assert!(sql.0.contains("substring(data, 1, 8)"));
    }

    #[test]
    fn test_generate_index_datasize_only() {
        let parsed = ParsedFilter {
            offsets_and_lengths: vec![],
            datasize: Some(752),
        };

        let sql = generate_create_index(
            Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
            &parsed,
            "accounts",
        );
        assert!(sql.0.contains("idx_accounts_675kPX_d752"));
        assert!(sql.0.contains("length(data) = 752"));
    }

    #[test]
    fn test_skip_token_program() {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(165)]),
            ..Default::default()
        };

        let parsed = parse_gpa_config(TOKEN_PROGRAM_ID, Some(&config));
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_no_filters() {
        let config = RpcProgramAccountsConfig {
            filters: None,
            ..Default::default()
        };

        let parsed = parse_gpa_config("SomeProgram11111111111111111111111111111", Some(&config));
        assert!(parsed.is_none());
    }
}
