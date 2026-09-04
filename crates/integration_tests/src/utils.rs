// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::{
    benchmark::RequestType,
    config::{PrintConfig, RpcEndpoint},
    db_check::DbProbeResult,
    response_comparison::{CompareResponsesResult, IterationCapture, ReponseComparison},
};
use anyhow::{Context, Result};
use chrono::SecondsFormat;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub async fn send_rpc_request(
    client: &reqwest::Client,
    endpoint: &RpcEndpoint,
    request: &JsonValue,
    meter: Option<&std::sync::Arc<crate::bandwidth::BwMeter>>,
) -> Result<(JsonValue, u128)> {
    let start = Instant::now();

    let response = client
        .post(&endpoint.url)
        .header("x-subscription-id", "test-value")
        .json(request)
        .send()
        .await
        .with_context(|| format!("Failed to connect to {}", endpoint.name))?;

    // Read the raw body first so we can account the true received wire size
    // cheaply: `reqwest::Response::json()` already buffers the full body into
    // `bytes()` internally, so doing it ourselves adds no extra copy — we just
    // get `.len()` for free and parse from the slice. The re-serialized JSON
    // length used for the size buckets is computed by the caller as before.
    let body = response
        .bytes()
        .await
        .with_context(|| format!("Failed to read response from {}", endpoint.name))?;
    let duration = start.elapsed().as_millis();

    if let Some(meter) = meter {
        meter.record(body.len() as u64);
    }

    let json = serde_json::from_slice(&body)
        .with_context(|| format!("Failed to parse response from {}", endpoint.name))?;

    Ok((json, duration))
}

/// Extract accounts array - handles both with and without context
pub fn get_accounts(response: &JsonValue) -> Option<&Vec<JsonValue>> {
    response
        .get("result")
        .and_then(|r| r.get("value").or(Some(r)))
        .and_then(|v| v.as_array())
}

/// Check if a response is an error response
pub fn is_error_response(response: &JsonValue) -> bool {
    response.get("error").is_some()
}

/// Extract slot from response
pub fn get_slot(response: &JsonValue) -> Option<u64> {
    response
        .get("result")
        .and_then(|r| r.get("context"))
        .and_then(|c| c.get("slot"))
        .and_then(|s| s.as_u64())
}

pub struct CompareContextResult {
    pub context_matches: bool,
    /// `endpoint1 slot - endpoint2 slot`
    /// Some(positive) means endpoint1 is ahead
    /// Some(0) means there is context but slots are the same
    pub slots_behind: Option<i64>,
}

/// Will compare of both responses have or not context (independently of the context slot value)
pub fn compare_context(response1: &JsonValue, response2: &JsonValue) -> CompareContextResult {
    let context1 = response1.get("result").and_then(|r| r.get("context"));
    let context2 = response2.get("result").and_then(|r| r.get("context"));

    let context_matches = context1.is_some() == context2.is_some();

    let slots_behind = match (get_slot(response1), get_slot(response2)) {
        (Some(s1), Some(s2)) => Some(s1 as i64 - s2 as i64),
        _ => None,
    };

    CompareContextResult {
        context_matches,
        slots_behind,
    }
}

/// Extracts the commitment level from the request.
/// If not present, returns the default ("finalized") which matches the API default.
pub fn extract_commitment_from_request(request: &JsonValue, request_type: RequestType) -> String {
    let opts_index = match request_type {
        RequestType::Gpa
        | RequestType::GpaTokenOwner
        | RequestType::GpaTokenMint
        | RequestType::GetAccountInfo
        | RequestType::GetMultipleAccounts
        | RequestType::GetBalance
        | RequestType::GetTokenAccountBalance
        | RequestType::GetTokenSupply
        | RequestType::GetTokenLargestAccounts
        | RequestType::SimulateTransaction => 1,
        RequestType::Gtabo | RequestType::Gtabd => 2,
    };
    request
        .get("params")
        .and_then(|p| p.get(opts_index))
        .and_then(|o| o.get("commitment"))
        .and_then(|c| c.as_str())
        .unwrap_or("finalized")
        .to_string()
}

/// Extracts the encoding from the request based on the request type.
///
/// Returns:
/// - For methods that carry an `encoding` field in their config object
///   (`getProgramAccounts`, `getTokenAccountsBy{Owner,Delegate}`, `getAccountInfo`,
///   `getMultipleAccounts`): the request's `encoding` if present, otherwise the
///   per-method Agave default.
/// - For methods that don't carry an encoding at all (`getBalance`,
///   `getTokenAccountBalance`, `getTokenSupply`, `getTokenLargestAccounts`):
///   the sentinel string `"none"`.
///
/// Agave defaults followed here:
/// - `getProgramAccounts` → `base58`
/// - `getTokenAccountsBy{Owner,Delegate}` → `jsonParsed`
/// - `getAccountInfo` → `binary` (deprecated base58 plain-string)
/// - `getMultipleAccounts` → `base64`
pub fn extract_encoding_from_request(request: &JsonValue, request_type: RequestType) -> String {
    // getBalance / getTokenAccountBalance / getTokenSupply / getTokenLargestAccounts
    // have no encoding concept.
    if matches!(
        request_type,
        RequestType::GetBalance
            | RequestType::GetTokenAccountBalance
            | RequestType::GetTokenSupply
            | RequestType::GetTokenLargestAccounts
    ) {
        return "none".to_string();
    }

    let opts_index = match request_type {
        RequestType::Gpa
        | RequestType::GpaTokenOwner
        | RequestType::GpaTokenMint
        | RequestType::GetAccountInfo
        | RequestType::GetMultipleAccounts
        | RequestType::SimulateTransaction => 1,
        RequestType::Gtabo | RequestType::Gtabd => 2,
        RequestType::GetBalance
        | RequestType::GetTokenAccountBalance
        | RequestType::GetTokenSupply
        | RequestType::GetTokenLargestAccounts => {
            unreachable!()
        }
    };
    let encoding = request
        .get("params")
        .and_then(|p| p.get(opts_index))
        .and_then(|o| o.get("encoding"))
        .and_then(|e| e.as_str())
        .map(String::from);

    match encoding {
        Some(e) => e,
        None => match request_type {
            RequestType::Gpa | RequestType::GpaTokenOwner | RequestType::GpaTokenMint => {
                "base58".to_string()
            }
            RequestType::Gtabo | RequestType::Gtabd => "jsonParsed".to_string(),
            RequestType::GetAccountInfo => "binary".to_string(),
            RequestType::GetMultipleAccounts => "base64".to_string(),
            RequestType::SimulateTransaction => "base58".to_string(),
            RequestType::GetBalance
            | RequestType::GetTokenAccountBalance
            | RequestType::GetTokenSupply
            | RequestType::GetTokenLargestAccounts => unreachable!(),
        },
    }
}

/// How a single account differs between the two endpoints' responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffKind {
    /// Both endpoints returned the account, with different contents.
    DataMismatch,
    /// Only rpc1 returned the account.
    OnlyRpc1,
    /// Only rpc2 returned the account.
    OnlyRpc2,
}

/// One differing account, with whichever side(s) returned it.
#[derive(Debug, Clone)]
pub struct AccountDiff {
    pub pubkey: String,
    pub kind: DiffKind,
    pub account1: Option<JsonValue>,
    pub account2: Option<JsonValue>,
}

/// Diffs two `{pubkey, account}`-array responses by pubkey.
///
/// Returns `None` when either response is not such an array — the caller has
/// nothing per-account to report. An empty `Vec` means the two agree.
pub fn diff_accounts(response1: &JsonValue, response2: &JsonValue) -> Option<Vec<AccountDiff>> {
    let index = |response: &JsonValue| -> Option<HashMap<String, JsonValue>> {
        Some(
            get_accounts(response)?
                .iter()
                .filter_map(|item| {
                    let pubkey = item.get("pubkey")?.as_str()?.to_string();
                    Some((pubkey, item.get("account")?.clone()))
                })
                .collect(),
        )
    };

    let map1 = index(response1)?;
    let map2 = index(response2)?;

    let mut diffs = Vec::new();
    for (pubkey, account1) in &map1 {
        match map2.get(pubkey) {
            Some(account2) if account1 != account2 => diffs.push(AccountDiff {
                pubkey: pubkey.clone(),
                kind: DiffKind::DataMismatch,
                account1: Some(account1.clone()),
                account2: Some(account2.clone()),
            }),
            None => diffs.push(AccountDiff {
                pubkey: pubkey.clone(),
                kind: DiffKind::OnlyRpc1,
                account1: Some(account1.clone()),
                account2: None,
            }),
            _ => {}
        }
    }
    for (pubkey, account2) in &map2 {
        if !map1.contains_key(pubkey) {
            diffs.push(AccountDiff {
                pubkey: pubkey.clone(),
                kind: DiffKind::OnlyRpc2,
                account1: None,
                account2: Some(account2.clone()),
            });
        }
    }

    Some(diffs)
}

/// Borrowed view of one retry attempt suitable for saving into a mismatch file.
pub struct SavedRetry<'a> {
    pub response_comparison: &'a ReponseComparison,
    pub context_matches: bool,
    /// `retry_in_place.retry_after_ms` value when the retry was pre-scheduled;
    /// `None` for on-mismatch retries (fired right after the original mismatch
    /// verdict was known).
    pub fire_after_ms: Option<u64>,
    /// All per-iteration captures from the retry comparison pass. `Some` only
    /// when `comparison.save_compensation_iterations = true`.
    pub iterations: Option<&'a [IterationCapture]>,
}

/// Formats a `SystemTime` as an ISO-8601 / RFC-3339 string with millisecond
/// precision and `Z` suffix, e.g. `2026-05-27T17:34:44.044Z`. Falls back to a
/// plain epoch-ms string for timestamps that can't be represented in chrono.
fn format_fired_at(t: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Renders a `DbProbeResult` into the JSON shape stored inside the
/// `db_probe` field of an iteration entry. `slots_table` is keyed by
/// commitment name (`Processed` / `Confirmed` / `Finalized`) for readability;
/// `accounts` is a list of `{table, slot, lamports}` rows in newest-first
/// order.
fn db_probe_to_json(probe: &DbProbeResult) -> JsonValue {
    let mut slots_obj = serde_json::Map::new();
    for row in &probe.slots {
        let label = match row.commitment {
            0 => "Processed",
            1 => "Confirmed",
            2 => "Finalized",
            other => {
                slots_obj.insert(format!("Unknown({other})"), JsonValue::from(row.slot));
                continue;
            }
        };
        slots_obj.insert(label.to_string(), JsonValue::from(row.slot));
    }
    let accounts: Vec<JsonValue> = probe
        .accounts
        .iter()
        .map(|row| {
            serde_json::json!({
                "table": row.table,
                "slot": row.slot,
                "lamports": row.lamports,
            })
        })
        .collect();
    serde_json::json!({
        "probed_at": format_fired_at(probe.probed_at),
        "slots_table": JsonValue::Object(slots_obj),
        "accounts": accounts,
    })
}

/// Renders one `IterationCapture` into the JSON object shape used inside the
/// `iterations` array of a mismatch / rescued file.
fn iteration_to_json(
    capture: &IterationCapture,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
) -> JsonValue {
    let (resp1, dur1, size1, slot1) = match &capture.rpc1 {
        Some((j, d)) => (
            j.clone(),
            JsonValue::from(*d as u64),
            JsonValue::from(j.to_string().len()),
            get_slot(j).map(JsonValue::from).unwrap_or(JsonValue::Null),
        ),
        None => (
            JsonValue::Null,
            JsonValue::Null,
            JsonValue::Null,
            JsonValue::Null,
        ),
    };
    let (resp2, dur2, size2, slot2) = match &capture.rpc2 {
        Some((j, d)) => (
            j.clone(),
            JsonValue::from(*d as u64),
            JsonValue::from(j.to_string().len()),
            get_slot(j).map(JsonValue::from).unwrap_or(JsonValue::Null),
        ),
        None => (
            JsonValue::Null,
            JsonValue::Null,
            JsonValue::Null,
            JsonValue::Null,
        ),
    };
    let db_probe_json = capture
        .db_probe
        .as_ref()
        .map(db_probe_to_json)
        .unwrap_or(JsonValue::Null);
    serde_json::json!({
        "phase": capture.phase,
        "fired_at": format_fired_at(capture.fired_at),
        "sizes": {
            rpc1.name.clone(): size1,
            rpc2.name.clone(): size2,
        },
        "durations_ms": {
            rpc1.name.clone(): dur1,
            rpc2.name.clone(): dur2,
        },
        "slots": {
            rpc1.name.clone(): slot1,
            rpc2.name.clone(): slot2,
        },
        "responses": {
            rpc1.name.clone(): resp1,
            rpc2.name.clone(): resp2,
        },
        "db_probe": db_probe_json,
    })
}

/// Mismatch save variant that bundles the original *and* the retry into a
/// single JSON file. Falls back to the legacy shape when `retry` is `None`.
///
/// `kind` becomes the filename prefix: `mismatch` for "verdict still wrong
/// after retry", `rescued` for "retry flipped the verdict to OK". The on-disk
/// schema is identical so both flavors are replayable through the
/// `mismatch_dir` source.
///
/// When `original_iterations` / `retry.iterations` are `Some`, each block gets
/// an `iterations: [...]` array next to its existing fields (final picked
/// pair). Each entry has its own `fired_at` (ISO-8601 with ms), `phase`,
/// `responses`, `slots`, `durations_ms`, `sizes`.
#[allow(clippy::too_many_arguments)]
pub fn save_responses_diff_to_file_with_retry(
    request: &JsonValue,
    kind: &str,
    program_id: &str,
    mismatch_output_dir: &str,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
    original: &ReponseComparison,
    original_context_matches: bool,
    original_iterations: Option<&[IterationCapture]>,
    retry: Option<SavedRetry<'_>>,
    geyser_probe: Option<JsonValue>,
) -> Result<()> {
    let mut original_block = serde_json::json!({
        "context_matches": original_context_matches,
        "sizes": {
            rpc1.name.clone(): original.response1.to_string().len(),
            rpc2.name.clone(): original.response2.to_string().len(),
        },
        "durations_ms": {
            rpc1.name.clone(): original.duration1,
            rpc2.name.clone(): original.duration2,
        },
        "slots": {
            rpc1.name.clone(): get_slot(&original.response1),
            rpc2.name.clone(): get_slot(&original.response2),
        },
        "responses": {
            rpc1.name.clone(): original.response1.clone(),
            rpc2.name.clone(): original.response2.clone(),
        },
    });
    if let Some(probe) = geyser_probe
        && let Some(obj) = original_block.as_object_mut()
    {
        obj.insert("geyser_probe".to_string(), probe);
    }
    if let Some(iters) = original_iterations
        && let Some(obj) = original_block.as_object_mut()
    {
        let arr: Vec<JsonValue> = iters
            .iter()
            .map(|c| iteration_to_json(c, rpc1, rpc2))
            .collect();
        obj.insert("iterations".to_string(), JsonValue::Array(arr));
    }

    let combined = if let Some(retry) = retry {
        let mut retry_block = serde_json::json!({
            "fire_after_ms": retry.fire_after_ms,
            "context_matches": retry.context_matches,
            "sizes": {
                rpc1.name.clone(): retry.response_comparison.response1.to_string().len(),
                rpc2.name.clone(): retry.response_comparison.response2.to_string().len(),
            },
            "durations_ms": {
                rpc1.name.clone(): retry.response_comparison.duration1,
                rpc2.name.clone(): retry.response_comparison.duration2,
            },
            "slots": {
                rpc1.name.clone(): get_slot(&retry.response_comparison.response1),
                rpc2.name.clone(): get_slot(&retry.response_comparison.response2),
            },
            "responses": {
                rpc1.name.clone(): retry.response_comparison.response1.clone(),
                rpc2.name.clone(): retry.response_comparison.response2.clone(),
            },
        });
        if let Some(iters) = retry.iterations
            && let Some(obj) = retry_block.as_object_mut()
        {
            let arr: Vec<JsonValue> = iters
                .iter()
                .map(|c| iteration_to_json(c, rpc1, rpc2))
                .collect();
            obj.insert("iterations".to_string(), JsonValue::Array(arr));
        }
        serde_json::json!({
            "request": request,
            "original": original_block,
            "retry": retry_block,
        })
    } else {
        serde_json::json!({
            "request": request,
            "original": original_block,
        })
    };

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    std::fs::create_dir_all(mismatch_output_dir)?;
    std::fs::write(
        format!("{mismatch_output_dir}/{kind}_{program_id}_{timestamp}.json"),
        serde_json::to_string_pretty(&combined)?,
    )?;

    Ok(())
}

pub fn bytes_bucket(bytes: u64) -> &'static str {
    match bytes {
        0..=1_000 => "0-1KB",
        1_001..=10_000 => "1-10KB",
        10_001..=100_000 => "10-100KB",
        100_001..=1_000_000 => "100KB-1MB",
        1_000_001..=10_000_000 => "1MB-10MB",
        10_000_001..=50_000_000 => "10MB-50MB",
        50_000_001..=100_000_000 => "50MB-100MB",
        100_000_001..=200_000_000 => "100MB-200MB",
        200_000_001..=500_000_000 => "200MB-500MB",
        _ => "500MB+",
    }
}

/// All config thresholds must be met to print the request result
pub fn print_request_result(
    request: &JsonValue,
    duration: u128,
    json: &JsonValue,
    rpc: &RpcEndpoint,
    encoding: &str,
    print_config: &PrintConfig,
) {
    let param_id = request["params"][0].as_str().unwrap_or("unknown");
    let size = json.to_string().len();

    if is_error_response(json) {
        tracing::info!(
            target: "bench_request",
            "💥 {} {} [{}] {}ms - {}",
            rpc.name, param_id, encoding, duration, json["error"]
        );
        return;
    }

    let account_count = get_accounts(json).map(|a| a.len()).unwrap_or(0);

    if (size as u64) < print_config.min_request_bytes
        || duration < print_config.min_request_duration_ms as u128
        || (account_count as u64) < print_config.min_request_account_count
    {
        return;
    }

    let slot = get_slot(json)
        .map(|s| format!(" slot:{s}"))
        .unwrap_or_default();

    let speed = match duration {
        0..=100 => "⚡⚡⚡⚡",
        101..=500 => "⚡⚡⚡",
        501..=2000 => "⚡⚡",
        2001..=5000 => "⚡",
        _ => "🐢",
    };

    tracing::info!(
        target: "bench_request",
        "{speed} {} [{}] {:.2}KB | {}ms | {} accounts{slot}",
        rpc.name,
        encoding,
        size as f64 / 1024.0,
        duration,
        account_count,
    );
}

/// Borrowed view of one retry attempt suitable for the per-request comparison
/// log line.
pub struct RetryPrintInfo<'a> {
    pub response_comparison: &'a ReponseComparison,
    pub compare_result: &'a CompareResponsesResult,
    pub internal_retries: u32,
}

/// Print-comparison variant that knows about retry-in-place. Delegates to the
/// legacy `print_compare_responses_result` when `retry` is `None` so the
/// existing log format is preserved bit-for-bit for runs without the feature.
#[allow(clippy::too_many_arguments)]
pub fn print_compare_responses_result_with_retry(
    original_result: &CompareResponsesResult,
    original_internal_retries: u32,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
    request: &JsonValue,
    original_comparison: &ReponseComparison,
    retry: Option<RetryPrintInfo<'_>>,
    original_elapsed_ms: u128,
    recovered_by_retry: bool,
    encoding: &str,
    commitment: &str,
    print_config: &PrintConfig,
) {
    let Some(retry) = retry else {
        print_compare_responses_result(
            original_result,
            original_internal_retries,
            rpc1,
            rpc2,
            request,
            original_comparison,
            encoding,
            commitment,
            print_config,
        );
        return;
    };

    let param_id = request["params"][0].as_str().unwrap_or("unknown");
    let orig_size1 = original_comparison.response1.to_string().len();
    let orig_size2 = original_comparison.response2.to_string().len();
    let d1 = original_comparison.duration1;
    let d2 = original_comparison.duration2;

    let rpc1_info = format!(
        "{} ({:.2}KB {}ms)",
        rpc1.name,
        orig_size1 as f64 / 1024.0,
        d1
    );
    let rpc2_info = format!(
        "{} ({:.2}KB {}ms)",
        rpc2.name,
        orig_size2 as f64 / 1024.0,
        d2
    );

    let original_matched = original_result.matches;
    let retry_matched = retry.compare_result.matches;
    let final_matched = original_matched || retry_matched;

    // Slot delta between the original and the retry for the endpoint under
    // test (rpc1). The whole point of retry_after_ms is to surface how many
    // slots advanced between the two reads.
    let orig_slot1 = get_slot(&original_comparison.response1);
    let retry_slot1 = get_slot(&retry.response_comparison.response1);
    let slot_delta_str = match (orig_slot1, retry_slot1) {
        (Some(o), Some(r)) => {
            let d = r as i64 - o as i64;
            let sign = if d >= 0 { "+" } else { "" };
            format!("{sign}{d} slots")
        }
        _ => "?? slots".to_string(),
    };

    let retry_d1 = retry.response_comparison.duration1;
    let retry_d2 = retry.response_comparison.duration2;
    let retry_info = format!(
        "🔁 retry({} ms after start, {}ms+{}ms, {})",
        original_elapsed_ms, retry_d1, retry_d2, slot_delta_str
    );

    let slot_info = match original_result.context_matches.slots_behind {
        Some(diff) if diff != 0 => format!(" 🔀 {diff} slots diff"),
        Some(_) => " (slot diff: 0)".to_string(),
        None => String::new(),
    };

    let total_retries = original_internal_retries + retry.internal_retries;
    let internal_retry_info = if total_retries > 0 {
        format!(" 🔄({total_retries} retries)")
    } else {
        String::new()
    };

    if final_matched && recovered_by_retry {
        tracing::info!(
            target: "bench_compare::rescued",
            "✅ rescued {param_id} [{encoding}] [{commitment}] {rpc1_info} | {rpc2_info}{slot_info}{internal_retry_info} {retry_info}",
        );
    } else if final_matched {
        // Original matched; retry was either a no-op (just timing) or also
        // matched. Same category as a plain match; retry's slot delta is still
        // informative for temporal-drift observation.
        tracing::info!(
            target: "bench_compare::match",
            "✅ {param_id} [{encoding}] [{commitment}] {rpc1_info} | {rpc2_info}{slot_info}{internal_retry_info} {retry_info}",
        );
    } else {
        tracing::info!(
            target: "bench_compare::mismatch",
            "❌ retry-also-failed {param_id} [{encoding}] [{commitment}] {rpc1_info} | {rpc2_info}{slot_info}{internal_retry_info} {retry_info}",
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub fn print_compare_responses_result(
    compare_responses_result: &CompareResponsesResult,
    retries: u32,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
    request: &JsonValue,
    response_comparison: &ReponseComparison,
    encoding: &str,
    commitment: &str,
    _print_config: &PrintConfig, // Not used yet
) {
    let param_id = request["params"][0].as_str().unwrap_or("unknown");
    let size1 = response_comparison.response1.to_string().len();
    let size2 = response_comparison.response2.to_string().len();
    let d1 = response_comparison.duration1;
    let d2 = response_comparison.duration2;

    let error1 = is_error_response(&response_comparison.response1);
    let error2 = is_error_response(&response_comparison.response2);

    let rpc1_info = if error1 {
        format!("💥 {} ({}ms)", rpc1.name, d1)
    } else {
        format!("{} ({:.2}KB {}ms)", rpc1.name, size1 as f64 / 1024.0, d1)
    };

    let rpc2_info = if error2 {
        format!("💥 {} ({}ms)", rpc2.name, d2)
    } else {
        format!("{} ({:.2}KB {}ms)", rpc2.name, size2 as f64 / 1024.0, d2)
    };

    if error1 && error2 {
        let icon = "💥💥";
        tracing::info!(
            target: "bench_compare::error",
            "{icon} {param_id} [{encoding}] [{commitment}] {rpc1_info} | {rpc2_info}",
        );
        return;
    }
    if error1 || error2 {
        let icon = "💥";
        tracing::info!(
            target: "bench_compare::error",
            "{icon} {param_id} [{encoding}] [{commitment}] {rpc1_info} | {rpc2_info}",
        );
        return;
    }

    let (match_icon, no_ctx_label) = if compare_responses_result.matches {
        ("✅", "")
    } else if compare_responses_result.is_no_context_mismatch() {
        ("⚠️", " (no context — possible slot lag)")
    } else {
        ("❌", "")
    };

    // rpc1 faster → ⚡, rpc1 slower → 🐢
    let ratio = if d2 > 0 { d1 as f64 / d2 as f64 } else { 1.0 };
    let speed = match ratio {
        r if r <= 0.25 => "⚡⚡⚡⚡",
        r if r <= 0.50 => "⚡⚡⚡",
        r if r <= 0.75 => "⚡⚡",
        r if r <= 0.95 => "⚡",
        r if r <= 1.05 => "", // roughly equal
        r if r <= 1.33 => "🐢",
        r if r <= 2.00 => "🐢🐢",
        r if r <= 4.00 => "🐢🐢🐢",
        _ => "🐢🐢🐢🐢",
    };

    let slot_info = match compare_responses_result.context_matches.slots_behind {
        Some(diff) if diff != 0 => {
            format!(" 🔀 {} slots diff", diff)
        }
        Some(_) => " (slot diff: 0)".to_string(),
        None => String::new(),
    };

    let retry_info = if retries > 0 {
        format!(" 🔄({retries} retries)")
    } else {
        String::new()
    };

    if compare_responses_result.matches {
        tracing::info!(
            target: "bench_compare::match",
            "{match_icon}{speed} {param_id} [{encoding}] [{commitment}] {rpc1_info} | {rpc2_info}{slot_info}{retry_info}",
        );
    } else if compare_responses_result.is_no_context_mismatch() {
        tracing::info!(
            target: "bench_compare::no_context_mismatch",
            "{match_icon}{speed} {param_id} [{encoding}] [{commitment}] {rpc1_info} | {rpc2_info}{slot_info}{retry_info}{no_ctx_label}",
        );
    } else {
        tracing::info!(
            target: "bench_compare::mismatch",
            "{match_icon}{speed} {param_id} [{encoding}] [{commitment}] {rpc1_info} | {rpc2_info}{slot_info}{retry_info}{no_ctx_label}",
        );
    }
}

/// Sets `withContext: true` in the params object of a request.
/// For GPA the options object is at index 1, for GTABO/GTABD at index 2.
/// If the last param element is already an object, it adds the field.
/// If it's not an object (or params is empty), it appends a new `{"withContext": true}`.
pub fn inject_with_context(request: &mut JsonValue) {
    let params = match request.get_mut("params").and_then(|p| p.as_array_mut()) {
        Some(p) => p,
        None => return,
    };

    if let Some(last) = params.last_mut()
        && let Some(obj) = last.as_object_mut()
    {
        obj.insert("withContext".to_string(), serde_json::Value::Bool(true));
        return;
    }

    params.push(serde_json::json!({"withContext": true}));
}

/// Returns true if the request already has `withContext: true` set.
pub fn has_with_context(request: &JsonValue) -> bool {
    request
        .get("params")
        .and_then(|p| p.as_array())
        .and_then(|arr| arr.last())
        .and_then(|last| last.get("withContext"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Last sample emission timestamp (nanos since epoch). Shared across all
/// concurrent `process_request` tasks so we throttle to at most one sample
/// per `sample_every_secs` window without per-task locking.
static LAST_SAMPLE_NANOS: AtomicI64 = AtomicI64::new(0);

/// Attempt to atomically claim the next sample slot. Returns `true` for at most
/// one caller per `interval` window; all other callers in the same window get
/// `false`. Lock-free via `compare_exchange`, so it adds no contention to the
/// hot path.
fn try_claim_sample_slot(interval: Duration) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    let interval_nanos = interval.as_nanos() as i64;

    let last = LAST_SAMPLE_NANOS.load(Ordering::Relaxed);
    if now - last < interval_nanos {
        return false;
    }
    LAST_SAMPLE_NANOS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

/// Pretty-prints a `(request, response1, response2)` tuple via the
/// `bench_sample` tracing target — but only once per `sample_every_secs`
/// across all concurrent benchmark tasks. No-op if `sample_every_secs` is unset.
///
/// Useful as a periodic sanity check that the comparator is operating on the
/// data you expect (encoding, slot, account shape) without flooding the logs.
pub fn maybe_print_sample(
    print_config: &PrintConfig,
    request: &JsonValue,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
    response_comparison: &ReponseComparison,
    compare_responses_result: &CompareResponsesResult,
) {
    let Some(secs) = print_config.sample_every_secs else {
        return;
    };
    if secs == 0 {
        return;
    }
    if !try_claim_sample_slot(Duration::from_secs(secs)) {
        return;
    }

    let match_icon = if compare_responses_result.matches {
        "✅"
    } else if compare_responses_result.is_no_context_mismatch() {
        "⚠️"
    } else {
        "❌"
    };

    let request_pretty =
        serde_json::to_string_pretty(request).unwrap_or_else(|_| request.to_string());
    let response1_pretty = serde_json::to_string_pretty(&response_comparison.response1)
        .unwrap_or_else(|_| response_comparison.response1.to_string());
    let response2_pretty = serde_json::to_string_pretty(&response_comparison.response2)
        .unwrap_or_else(|_| response_comparison.response2.to_string());

    tracing::info!(
        target: "bench_sample",
        "📋 SAMPLE {match_icon}\n--- request ---\n{request_pretty}\n--- {} response ({}ms) ---\n{response1_pretty}\n--- {} response ({}ms) ---\n{response2_pretty}",
        rpc1.name,
        response_comparison.duration1,
        rpc2.name,
        response_comparison.duration2,
    );
}
