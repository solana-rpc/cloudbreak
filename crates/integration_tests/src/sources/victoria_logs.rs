// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use serde_json::Value as JsonValue;

use crate::benchmark::RequestType;
use crate::utils;

pub struct LoggedRequest {
    pub req_id: Option<String>,
    pub body: JsonValue,
    /// Logged response size in bytes (the VictoriaLogs `bytes` field), used as
    /// the bandwidth-cap estimate. `None` when the field is absent/unparseable.
    pub bytes: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
pub fn get_body_query(
    request_type: RequestType,
    min_filter: &str,
    max_filter: &str,
    encoding: Option<&str>,
    minutes: Option<u32>,
    window_seconds: Option<u32>,
    limit: u32,
    pool_dedicated: Option<&str>,
) -> String {
    let encoding_filter = encoding
        .map(|e| format!(r#" AND body:~`"encoding":\s*"{}"`"#, e))
        .unwrap_or_default();

    // `pool_dedicated` filter, configurable per source. When unset, no
    // `pool_dedicated` constraint is applied. `simulateTransaction` keeps its
    // historical `foundation` default when the config leaves it unset.
    let pool_filter = pool_dedicated
        .map(|p| format!(" AND pool_dedicated:~\"{}\"", p))
        .unwrap_or_default();
    let simulate_pool_filter = pool_dedicated
        .or(Some("foundation"))
        .map(|p| format!(" AND pool_dedicated:~\"{}\"", p))
        .unwrap_or_default();

    let time_filter = match window_seconds {
        Some(s) => format!("_time:>now-{s}s AND "),
        None => minutes
            .map(|m| format!("_time:>now-{m}m AND _time:<=now-{}m AND ", m - 1))
            .unwrap_or_default(),
    };

    match request_type {
        RequestType::Gtabo => format!(
            "query={time_filter}rpc_call:=\"getTokenAccountsByOwner\" {encoding_filter}{pool_filter} {min_filter} {max_filter} | limit {limit}"
        ),
        RequestType::Gtabd => format!(
            "query={time_filter}rpc_call:=\"getTokenAccountsByDelegate\" {encoding_filter}{pool_filter} {min_filter} {max_filter} | limit {limit}"
        ),
        RequestType::Gpa => format!(
            "query={time_filter}rpc_call:=\"getProgramAccounts\" {pool_filter} AND NOT body:~\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA|TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb\" {encoding_filter} {min_filter} {max_filter} | limit {limit}"
        ),
        RequestType::GpaTokenOwner => format!(
            "query={time_filter}rpc_call:=\"getProgramAccounts\" {encoding_filter} AND b_end:~\"tokenowner\" AND body:~\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA|TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb\" AND body:~`memcmp.*offset.:\\s*32` | limit {limit}"
        ),
        RequestType::GpaTokenMint => format!(
            "query={time_filter}rpc_call:=\"getProgramAccounts\" {encoding_filter} AND b_end:~\"liquid-tokenmint\" AND body:~\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA|TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb\" AND body:~`memcmp.*offset.:\\s*0` | limit {limit}"
        ),
        RequestType::GetAccountInfo => format!(
            "query={time_filter}rpc_call:=\"getAccountInfo\"{pool_filter} | limit {limit}"
        ),
        RequestType::GetMultipleAccounts => format!(
            "query={time_filter}rpc_call:=\"getMultipleAccounts\"{pool_filter} | limit {limit}"
        ),
        RequestType::GetBalance => format!(
            "query={time_filter}rpc_call:=\"getBalance\"{pool_filter} | limit {limit}"
        ),
        RequestType::GetTokenAccountBalance => format!(
            "query={time_filter}rpc_call:=\"getTokenAccountBalance\"{pool_filter} | limit {limit}"
        ),
        RequestType::GetTokenSupply => format!(
            "query={time_filter}rpc_call:=\"getTokenSupply\" AND pool_dedicated:~\"liquid\" | limit {limit}"
        ),
        RequestType::SimulateTransaction => format!(
            "query={time_filter}rpc_call:=\"simulateTransaction\"{simulate_pool_filter} AND body:* | limit {limit}"
        ),
        RequestType::GetSupply => format!(
            "query={time_filter}rpc_call:=\"getSupply\" AND body:* | limit {limit}"
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn get_requests(
    client: &reqwest::Client,
    url: &str,
    request_type: RequestType,
    min_request_size: Option<u64>,
    max_request_size: Option<u64>,
    encoding: Option<&str>,
    minutes: Option<u32>,
    window_seconds: Option<u32>,
    limit: u32,
    pool_dedicated: Option<&str>,
) -> Result<Vec<LoggedRequest>, anyhow::Error> {
    let min_filter = if let Some(min_request_size) = min_request_size {
        format!(" AND bytes:>{}", min_request_size)
    } else {
        "".to_string()
    };

    let max_filter = if let Some(max_request_size) = max_request_size {
        format!(" AND bytes:<{}", max_request_size)
    } else {
        "".to_string()
    };

    let body_query = get_body_query(
        request_type,
        &min_filter,
        &max_filter,
        encoding,
        minutes,
        window_seconds,
        limit,
        pool_dedicated,
    );

    let response = client
        .post(url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body_query)
        .send()
        .await?;

    let data = response.text().await?;

    // Parse NDJSON (Newline Delimited JSON)
    let logs: Vec<LoggedRequest> = data
        .trim()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| match serde_json::from_str(line) {
            Ok(parsed) => Some(parsed),
            Err(e) => {
                eprintln!("Failed to parse line: {}", e);
                eprintln!("Raw data: {}", line);
                None
            }
        })
        .filter_map(|log: JsonValue| {
            if log["body"].is_null() {
                return None;
            }

            let req_id = log["req_id"].as_str().map(String::from);
            // VictoriaLogs may emit `bytes` as a JSON number or a string.
            let bytes = log["bytes"]
                .as_u64()
                .or_else(|| log["bytes"].as_str().and_then(|s| s.parse::<u64>().ok()));
            let mut body: JsonValue = serde_json::from_str(log["body"].as_str().unwrap()).ok()?;

            if request_type == RequestType::GpaTokenMint
                || request_type == RequestType::GpaTokenOwner
            {
                let is_tokenkeg = body
                    .get("params")
                    .and_then(|p| p.get(0))
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| s == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

                if is_tokenkeg
                    && let Some(filters) = body
                        .get_mut("params")
                        .and_then(|p| p.get_mut(1))
                        .and_then(|opts| opts.get_mut("filters"))
                        .and_then(|f| f.as_array_mut())
                {
                    let has_data_size = filters.iter().any(|f| f.get("dataSize").is_some());
                    if !has_data_size {
                        filters.push(serde_json::json!({"dataSize": 165}));
                    }
                }
            }

            // Exclude batched requests
            if body.is_array() {
                return None;
            }

            // Post-fetch encoding filter: the VictoriaLogs regex can produce false
            // positives when "encoding" appears in a memcmp filter rather than as
            // the response encoding parameter. Verify using the actual parsed field.
            //
            // Methods without an encoding field (`getBalance`, `getTokenAccountBalance`,
            // `getTokenSupply`) report `"none"` from `extract_encoding_from_request` —
            // for those the encoding filter is treated as a no-op (the request passes
            // through).
            if let Some(target_encoding) = encoding {
                let actual_encoding = utils::extract_encoding_from_request(&body, request_type);
                if actual_encoding != "none" {
                    let matches = target_encoding.split('|').any(|e| e == actual_encoding);
                    if !matches {
                        return None;
                    }
                }
            }

            Some(LoggedRequest {
                req_id,
                body,
                bytes,
            })
        })
        .collect();

    Ok(logs)
}
