// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::Result;
use base64::Engine as _;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};

use crate::benchmark::RequestType;
use crate::config::{ComparisonConfig, RpcEndpoint};
use crate::db_check::{DbProbeCtx, DbProbeResult, probe_get_balance_state};
use crate::utils::{self, CompareContextResult};

/// One concrete rpc1+rpc2 send-pair captured during slot compensation /
/// no-context retry. Pushed into a shared `Vec<IterationCapture>` from both
/// `run_comparison` (the seed pair before slot compensation starts) and the
/// inner loop of `compare_with_slot_compensation` (each subsequent retry).
///
/// Both halves of a pair are fired in the same `tokio::join!`, so `fired_at`
/// is captured **once** immediately before the join (i.e. it is the moment
/// when both requests left the client). Per-endpoint round-trip latencies
/// are still distinct and live in the `(_, duration)` tuples.
///
/// A `None` on either side means that endpoint errored out for this
/// iteration (network failure, timeout, etc.).
///
/// `db_probe` is populated when the per-iteration `getBalance` DB probe is
/// enabled (`comparison.save_db_probe_iterations`). The probe fires as the
/// third arm of the same `tokio::join!`, so it shares wall-clock time with
/// the rpc1/rpc2 requests. `None` means probing was disabled for this
/// iteration (request type isn't `getBalance`, or the flag is off, or the
/// query failed — see the one-shot WARN in `db_check`).
#[derive(Clone)]
pub struct IterationCapture {
    /// `"initial"` for the first slot-compensation pass; `"with_context_retry"`
    /// for the second pass triggered by an `inject_context = true` no-context
    /// mismatch.
    pub phase: &'static str,
    /// Wall-clock moment when the pair was fired (captured right before
    /// `tokio::join!`). Serialized as ISO-8601 with ms precision at save time.
    pub fired_at: SystemTime,
    pub rpc1: Option<(JsonValue, u128)>,
    pub rpc2: Option<(JsonValue, u128)>,
    pub db_probe: Option<DbProbeResult>,
}

pub struct ReponseComparison {
    pub response1: JsonValue,
    pub response2: JsonValue,
    pub duration1: u128,
    pub duration2: u128,
}

pub struct CompareResponsesResult {
    pub matches: bool,
    pub context_matches: CompareContextResult,
}

impl CompareResponsesResult {
    pub fn false_with_context(compare_context_result: CompareContextResult) -> Self {
        Self {
            matches: false,
            context_matches: compare_context_result,
        }
    }

    pub fn new_with_matching_context(
        matches: bool,
        compare_context_result: CompareContextResult,
    ) -> Self {
        Self {
            matches,
            context_matches: compare_context_result,
        }
    }

    /// Both responses lack context, so the mismatch may be caused by slot lag
    /// that we cannot verify or compensate for.
    pub fn is_no_context_mismatch(&self) -> bool {
        !self.matches
            && self.context_matches.context_matches
            && self.context_matches.slots_behind.is_none()
    }
}

fn decompress_b64_zstd(b64: &str) -> Option<Vec<u8>> {
    let compressed = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    zstd::decode_all(std::io::Cursor::new(compressed)).ok()
}

/// Compares two account JSON objects. For zstd-compressed encodings, decompresses
/// the data before comparison to avoid false mismatches from non-deterministic compression.
fn accounts_equal(account1: &JsonValue, account2: &JsonValue, encoding: &str) -> bool {
    if account1 == account2 {
        return true;
    }
    if !encoding.contains("zstd") {
        return false;
    }

    for field in &["lamports", "owner", "executable", "rentEpoch"] {
        if account1.get(field) != account2.get(field) {
            return false;
        }
    }

    let d1 = account1.get("data").and_then(|d| d.as_array());
    let d2 = account2.get("data").and_then(|d| d.as_array());

    match (d1, d2) {
        (Some(arr1), Some(arr2)) => {
            if arr1.get(1) != arr2.get(1) {
                return false;
            }
            let raw1 = arr1
                .first()
                .and_then(|v| v.as_str())
                .and_then(decompress_b64_zstd);
            let raw2 = arr2
                .first()
                .and_then(|v| v.as_str())
                .and_then(decompress_b64_zstd);
            match (raw1, raw2) {
                (Some(r1), Some(r2)) => r1 == r2,
                _ => false,
            }
        }
        _ => false,
    }
}

/// Compares two RPC responses, dispatching on `request_type` to handle the
/// per-method response shape:
///
/// - `Gpa` / `GpaTokenOwner` / `GpaTokenMint` — `result` or `result.value` is an
///   array of `{pubkey, account}` items, compared as an unordered pubkey-keyed set.
/// - `Gtabo` / `Gtabd` — same shape as GPA (always under `result.value`).
/// - `GetMultipleAccounts` — `result.value` is a positional array of
///   `UiAccount | null`, compared position-by-position (order is meaningful).
/// - `GetAccountInfo` — `result.value` is a single `UiAccount | null`,
///   compared directly.
/// - `GetBalance` — `result.value` is a `u64`, compared directly.
/// - `GetTokenAccountBalance` — `result.value` is a `UiTokenAmount` object,
///   compared directly.
/// - `GetTokenSupply` — `result.value` is a `UiTokenAmount` object,
///   compared directly.
/// - `GetTokenLargestAccounts` — `result.value` is an array of
///   `RpcTokenAccountBalance` objects ordered deterministically by both
///   implementations (amount descending, pubkey-descending tie-break),
///   compared directly.
///
/// For zstd-compressed encodings, account data is decompressed before
/// comparison to handle non-deterministic compression across implementations.
pub fn compare_responses(
    response1: &JsonValue,
    response2: &JsonValue,
    encoding: &str,
    request_type: RequestType,
) -> CompareResponsesResult {
    // If both responses are errors, treat as a match regardless of error message
    if response1.get("error").is_some() && response2.get("error").is_some() {
        return CompareResponsesResult::new_with_matching_context(
            true,
            utils::compare_context(response1, response2),
        );
    }

    let compare_context_result = utils::compare_context(response1, response2);

    if !compare_context_result.context_matches {
        return CompareResponsesResult::false_with_context(compare_context_result);
    }

    let matches = match request_type {
        RequestType::Gpa
        | RequestType::GpaTokenOwner
        | RequestType::GpaTokenMint
        | RequestType::Gtabo
        | RequestType::Gtabd => compare_account_array_responses(response1, response2, encoding),
        RequestType::GetMultipleAccounts => {
            compare_multiple_accounts_responses(response1, response2, encoding)
        }
        RequestType::GetAccountInfo => compare_value_with_account(response1, response2, encoding),
        RequestType::GetBalance
        | RequestType::GetTokenAccountBalance
        | RequestType::GetTokenSupply
        | RequestType::GetTokenLargestAccounts => compare_value_direct(response1, response2),
        RequestType::SimulateTransaction => compare_simulate_responses(response1, response2),
        RequestType::GetSupply => compare_supply_responses(response1, response2),
    };

    CompareResponsesResult::new_with_matching_context(matches, compare_context_result)
}

/// Compares two `result.value: u64 | object | array` responses by direct JSON
/// equality. Used for `getBalance` (u64), `getTokenAccountBalance` /
/// `getTokenSupply` (UiTokenAmount), and `getTokenLargestAccounts` (array of
/// `RpcTokenAccountBalance`, deterministically ordered by amount descending
/// with a pubkey-descending tie-break in both implementations).
fn compare_value_direct(response1: &JsonValue, response2: &JsonValue) -> bool {
    let v1 = response1.get("result").and_then(|r| r.get("value"));
    let v2 = response2.get("result").and_then(|r| r.get("value"));
    v1 == v2
}

static NULL_VALUE: JsonValue = JsonValue::Null;

fn compare_supply_responses(response1: &JsonValue, response2: &JsonValue) -> bool {
    let v1 = response1.get("result").and_then(|r| r.get("value"));
    let v2 = response2.get("result").and_then(|r| r.get("value"));

    let (v1, v2) = match (v1, v2) {
        (Some(v1), Some(v2)) => (v1, v2),
        _ => return v1 == v2,
    };

    if ["total", "circulating", "nonCirculating"]
        .iter()
        .any(|key| field(v1, key) != field(v2, key))
    {
        return false;
    }

    let accounts_set = |value: &JsonValue| -> Option<HashSet<String>> {
        field(value, "nonCirculatingAccounts")
            .as_array()
            .map(|array| {
                array
                    .iter()
                    .filter_map(|a| a.as_str().map(String::from))
                    .collect()
            })
    };
    accounts_set(v1) == accounts_set(v2)
}

fn field<'a>(value: &'a JsonValue, key: &str) -> &'a JsonValue {
    value.get(key).unwrap_or(&NULL_VALUE)
}

const SIMULATE_EXACT_FIELDS: [&str; 9] = [
    "err",
    "logs",
    "unitsConsumed",
    "loadedAccountsDataSize",
    "returnData",
    "fee",
    "loadedAddresses",
    "innerInstructions",
    "accounts",
];

fn token_balance_map(balances: &JsonValue) -> Option<HashMap<String, &JsonValue>> {
    let array = balances.as_array()?;
    Some(
        array
            .iter()
            .map(|b| {
                let key = format!(
                    "{}:{}",
                    field(b, "accountIndex"),
                    field(b, "mint"),
                );
                (key, b)
            })
            .collect(),
    )
}

fn compare_token_balances(value1: &JsonValue, value2: &JsonValue, key: &str) -> bool {
    let b1 = field(value1, key);
    let b2 = field(value2, key);

    match (token_balance_map(b1), token_balance_map(b2)) {
        (Some(m1), Some(m2)) => m1 == m2,
        (None, None) => b1 == b2,
        _ => false,
    }
}

fn compare_simulate_responses(response1: &JsonValue, response2: &JsonValue) -> bool {
    let v1 = response1.get("result").and_then(|r| r.get("value"));
    let v2 = response2.get("result").and_then(|r| r.get("value"));

    let (v1, v2) = match (v1, v2) {
        (Some(v1), Some(v2)) => (v1, v2),
        _ => return v1 == v2,
    };

    if SIMULATE_EXACT_FIELDS
        .iter()
        .any(|key| field(v1, key) != field(v2, key))
    {
        return false;
    }

    if field(v1, "replacementBlockhash").is_null() != field(v2, "replacementBlockhash").is_null() {
        return false;
    }

    if field(v1, "preBalances") != field(v2, "preBalances")
        || field(v1, "postBalances") != field(v2, "postBalances")
    {
        return false;
    }

    compare_token_balances(v1, v2, "preTokenBalances")
        && compare_token_balances(v1, v2, "postTokenBalances")
}

/// Compares two `result.value: UiAccount | null` responses (`getAccountInfo`).
/// Handles zstd-encoded data so non-deterministic compression isn't flagged.
fn compare_value_with_account(
    response1: &JsonValue,
    response2: &JsonValue,
    encoding: &str,
) -> bool {
    let v1 = response1.get("result").and_then(|r| r.get("value"));
    let v2 = response2.get("result").and_then(|r| r.get("value"));
    match (v1, v2) {
        (Some(a), Some(b)) => account_or_null_equal(a, b, encoding),
        // If `value` is missing in one but not the other, mismatch.
        (None, None) => true,
        _ => false,
    }
}

/// Compares two `result.value: Vec<UiAccount | null>` responses
/// (`getMultipleAccounts`). Position-by-position comparison — order matters.
fn compare_multiple_accounts_responses(
    response1: &JsonValue,
    response2: &JsonValue,
    encoding: &str,
) -> bool {
    let arr1 = response1
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|v| v.as_array());
    let arr2 = response2
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|v| v.as_array());

    let (arr1, arr2) = match (arr1, arr2) {
        (Some(a), Some(b)) => (a, b),
        _ => return false,
    };

    if arr1.len() != arr2.len() {
        return false;
    }

    arr1.iter()
        .zip(arr2.iter())
        .all(|(a, b)| account_or_null_equal(a, b, encoding))
}

/// Compares two values that are each either a `UiAccount` object or `null`.
fn account_or_null_equal(a: &JsonValue, b: &JsonValue, encoding: &str) -> bool {
    match (a.is_null(), b.is_null()) {
        (true, true) => true,
        (false, false) => accounts_equal(a, b, encoding),
        _ => false,
    }
}

/// Compares two GPA-shaped responses (array of `{pubkey, account}` items)
/// as an unordered pubkey-keyed set. Used for gPA / gTABO / gTABD.
fn compare_account_array_responses(
    response1: &JsonValue,
    response2: &JsonValue,
    encoding: &str,
) -> bool {
    let accounts1 = match utils::get_accounts(response1) {
        Some(a) => a,
        // Fall back to whole-response JSON equality if not an array (e.g. error shape).
        None => return response1 == response2,
    };
    let accounts2 = match utils::get_accounts(response2) {
        Some(a) => a,
        None => return false,
    };

    if accounts1.len() != accounts2.len() {
        return false;
    }

    let map2: HashMap<&str, &JsonValue> = accounts2
        .iter()
        .filter_map(|item| {
            let pubkey = item.get("pubkey")?.as_str()?;
            let account = item.get("account")?;
            Some((pubkey, account))
        })
        .collect();

    for item in accounts1 {
        let pubkey = match item.get("pubkey").and_then(|p| p.as_str()) {
            Some(p) => p,
            None => return false,
        };
        let account1 = match item.get("account") {
            Some(a) => a,
            None => return false,
        };

        match map2.get(pubkey) {
            Some(account2) => {
                if !accounts_equal(account1, account2, encoding) {
                    return false;
                }
            }
            None => return false,
        }
    }

    let map1: HashMap<&str, &JsonValue> = accounts1
        .iter()
        .filter_map(|item| {
            let pubkey = item.get("pubkey")?.as_str()?;
            let account = item.get("account")?;
            Some((pubkey, account))
        })
        .collect();

    for item in accounts2 {
        let pubkey = match item.get("pubkey").and_then(|p| p.as_str()) {
            Some(p) => p,
            None => return false,
        };
        if !map1.contains_key(pubkey) {
            return false;
        }
    }

    true
}

struct SlottedResponse {
    response: JsonValue,
    duration: u128,
    slot: u64,
}

/// Concurrently fires the rpc1+rpc2 send pair and, when `db_probe_ctx` is
/// `Some`, a per-iteration `getBalance`-shaped DB probe — all inside the same
/// `tokio::join!` so they share an effective wall-clock instant. Used by both
/// `run_comparison` (for its initial / with-context-retry seeds) and the
/// retry loop of `compare_with_slot_compensation`.
pub async fn join_pair_with_probe(
    client: &reqwest::Client,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
    request: &JsonValue,
    db_probe_ctx: Option<&DbProbeCtx>,
    bw_meter: &std::sync::Arc<crate::bandwidth::BwMeter>,
) -> (
    Result<(JsonValue, u128)>,
    Result<(JsonValue, u128)>,
    Option<DbProbeResult>,
) {
    // Only rpc1 (the endpoint under test) is metered for the Gbit/s limit and
    // the average throughput display; rpc2 is the comparison arm.
    match db_probe_ctx {
        Some(ctx) => {
            let (r1, r2, probe) = tokio::join!(
                utils::send_rpc_request(client, rpc1, request, Some(bw_meter)),
                utils::send_rpc_request(client, rpc2, request, None),
                probe_get_balance_state(ctx),
            );
            (r1, r2, probe)
        }
        None => {
            let (r1, r2) = tokio::join!(
                utils::send_rpc_request(client, rpc1, request, Some(bw_meter)),
                utils::send_rpc_request(client, rpc2, request, None),
            );
            (r1, r2, None)
        }
    }
}

/// Fires concurrent requests to both endpoints at regular intervals, collects all
/// responses, then finds the first pair (one from each) that share the same slot.
/// This avoids the ping-pong problem where sequential retries on the behind endpoint
/// can never converge because the chain keeps advancing.
///
/// When `iterations` is `Some(...)`, every retry round appends one
/// `IterationCapture` to the vec — useful for post-mortem reconstruction of
/// what slot compensation actually did. The seed (initial) pair is not pushed
/// here: it is the caller's responsibility (see `run_comparison`) so the
/// initial `fired_at` reflects the *real* wall-clock moment the join started.
#[allow(clippy::collapsible_if, clippy::too_many_arguments)]
pub async fn compare_with_slot_compensation(
    client: &reqwest::Client,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
    request: &JsonValue,
    response_comparison: &mut ReponseComparison,
    comparison_config: &ComparisonConfig,
    encoding: &str,
    request_type: RequestType,
    mut iterations: Option<&mut Vec<IterationCapture>>,
    iteration_phase: &'static str,
    db_probe_ctx: Option<&DbProbeCtx>,
    bw_meter: &std::sync::Arc<crate::bandwidth::BwMeter>,
) -> Result<(CompareResponsesResult, u32)> {
    let enable_slot_compensation = comparison_config.enable_slot_compensation;
    let max_retries = comparison_config.slot_compensation_max_retries;
    let retry_interval = Duration::from_millis(comparison_config.slot_compensation_interval_ms);

    let mut compare_responses_result = compare_responses(
        &response_comparison.response1,
        &response_comparison.response2,
        encoding,
        request_type,
    );

    if enable_slot_compensation && !compare_responses_result.matches {
        if let (Some(s1), Some(s2)) = (
            utils::get_slot(&response_comparison.response1),
            utils::get_slot(&response_comparison.response2),
        ) {
            if s1 != s2 {
                let mut rpc1_responses: Vec<SlottedResponse> = vec![SlottedResponse {
                    response: response_comparison.response1.clone(),
                    duration: response_comparison.duration1,
                    slot: s1,
                }];
                let mut rpc2_responses: Vec<SlottedResponse> = vec![SlottedResponse {
                    response: response_comparison.response2.clone(),
                    duration: response_comparison.duration2,
                    slot: s2,
                }];

                let mut retries = 0u32;
                let mut matched = false;

                while retries < max_retries {
                    tokio::time::sleep(retry_interval).await;
                    retries += 1;

                    let fired_at = SystemTime::now();
                    let (r1, r2, db_probe) = join_pair_with_probe(
                        client,
                        rpc1,
                        rpc2,
                        request,
                        db_probe_ctx,
                        bw_meter,
                    )
                    .await;

                    if let Some(it) = iterations.as_deref_mut() {
                        it.push(IterationCapture {
                            phase: iteration_phase,
                            fired_at,
                            rpc1: r1
                                .as_ref()
                                .ok()
                                .map(|(j, d)| (j.clone(), *d)),
                            rpc2: r2
                                .as_ref()
                                .ok()
                                .map(|(j, d)| (j.clone(), *d)),
                            db_probe,
                        });
                    }

                    if let Ok((resp1, dur1)) = r1 {
                        if let Some(slot) = utils::get_slot(&resp1) {
                            rpc1_responses.push(SlottedResponse {
                                response: resp1,
                                duration: dur1,
                                slot,
                            });
                        }
                    }
                    if let Ok((resp2, dur2)) = r2 {
                        if let Some(slot) = utils::get_slot(&resp2) {
                            rpc2_responses.push(SlottedResponse {
                                response: resp2,
                                duration: dur2,
                                slot,
                            });
                        }
                    }

                    // Check if any pair shares the same slot (earliest match wins)
                    if let Some((r1_match, r2_match)) =
                        find_slot_match(&rpc1_responses, &rpc2_responses)
                    {
                        response_comparison.response1 = r1_match.response;
                        response_comparison.duration1 = r1_match.duration;
                        response_comparison.response2 = r2_match.response;
                        response_comparison.duration2 = r2_match.duration;
                        matched = true;
                        break;
                    }
                }

                if !matched {
                    // No slot match found — use the latest responses from each endpoint
                    if let Some(last1) = rpc1_responses.pop() {
                        response_comparison.response1 = last1.response;
                        response_comparison.duration1 = last1.duration;
                    }
                    if let Some(last2) = rpc2_responses.pop() {
                        response_comparison.response2 = last2.response;
                        response_comparison.duration2 = last2.duration;
                    }
                }

                let final_result = compare_responses(
                    &response_comparison.response1,
                    &response_comparison.response2,
                    encoding,
                    request_type,
                );
                compare_responses_result.matches = final_result.matches;

                return Ok((compare_responses_result, retries));
            }
        }
    }
    Ok((compare_responses_result, 0))
}

/// Finds the first pair of responses (one from each endpoint) that share the same slot.
/// Prioritizes earlier slots to get the closest-in-time comparison.
fn find_slot_match(
    rpc1_responses: &[SlottedResponse],
    rpc2_responses: &[SlottedResponse],
) -> Option<(SlottedResponse, SlottedResponse)> {
    let rpc2_by_slot: HashMap<u64, usize> = rpc2_responses
        .iter()
        .enumerate()
        .map(|(i, r)| (r.slot, i))
        .collect();

    for r1 in rpc1_responses {
        if let Some(&idx) = rpc2_by_slot.get(&r1.slot) {
            return Some((
                SlottedResponse {
                    response: r1.response.clone(),
                    duration: r1.duration,
                    slot: r1.slot,
                },
                SlottedResponse {
                    response: rpc2_responses[idx].response.clone(),
                    duration: rpc2_responses[idx].duration,
                    slot: rpc2_responses[idx].slot,
                },
            ));
        }
    }
    None
}
