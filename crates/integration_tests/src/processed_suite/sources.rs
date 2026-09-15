// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Timed JSON-RPC calls per source and the comparable account shapes shared by the fronts.
//!
//! Every call first holds a permit from the source's semaphore, so `--concurrency` caps in-flight
//! requests per source across all fronts. It then takes a token from the source's bucket, so
//! `--rps` caps the total request rate per source across all fronts. The reply latency starts
//! after both, and the wait for them is reported apart. Error strings never carry the request URL.

use super::bucket::TokenBucket;
use crate::utils::get_slot;
use serde_json::{Value as JsonValue, json};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Semaphore;
use tokio::time::Instant;

/// Owner-excluded error of the Postgres read path.
const EXCLUDED_CODE: i64 = -32010;
const COMPARED_FIELDS: [&str; 6] = [
    "lamports",
    "owner",
    "executable",
    "rentEpoch",
    "space",
    "data",
];

/// Include mode, then the program list, from environment_info.
pub type OwnerFilter = (bool, HashSet<String>);

#[derive(Clone)]
pub struct Source {
    pub name: String,
    url: String,
    permits: Arc<Semaphore>,
    bucket: Arc<TokenBucket>,
    /// Requests sent so far, for the effective rate in the report.
    pub calls: Arc<AtomicU64>,
}

pub struct Reply {
    /// Parsed body, or a transport or HTTP error without a JSON-RPC body.
    pub body: Result<JsonValue, String>,
    /// From send to the last body byte.
    pub ms: f64,
    /// Local wait for the permit and the token before the send.
    pub queued_ms: f64,
    pub received: Instant,
    pub timed_out: bool,
    pub bytes: usize,
}

impl Reply {
    pub fn json(&self) -> Option<&JsonValue> {
        self.body.as_ref().ok()
    }

    pub fn slot(&self) -> Option<u64> {
        self.json().and_then(get_slot)
    }

    pub fn rpc_error(&self) -> Option<i64> {
        self.json()?
            .get("error")
            .map(|e| e["code"].as_i64().unwrap_or(0))
    }

    pub fn values(&self) -> Option<&Vec<JsonValue>> {
        self.json()?["result"]["value"].as_array()
    }
}

impl Source {
    pub fn new(name: &str, url: &str, concurrency: usize, rps: f64) -> Self {
        Self {
            name: name.to_string(),
            url: url.to_string(),
            permits: Arc::new(Semaphore::new(concurrency.max(1))),
            bucket: Arc::new(TokenBucket::new(rps)),
            calls: Arc::default(),
        }
    }

    pub async fn call(&self, client: &reqwest::Client, method: &str, params: JsonValue) -> Reply {
        self.call_hook(client, method, params, || ()).await.0
    }

    /// Like `call`, and runs `at_send` right before the send, after the permit and the token.
    pub async fn call_hook<T>(
        &self,
        client: &reqwest::Client,
        method: &str,
        params: JsonValue,
        at_send: impl FnOnce() -> T,
    ) -> (Reply, T) {
        let entered = Instant::now();
        let _permit = self.permits.acquire().await.expect("never closed");
        self.bucket.take().await;
        let hooked = at_send();
        self.calls.fetch_add(1, Ordering::Relaxed);
        let request = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let sent = Instant::now();
        let request = client
            .post(&self.url)
            .header("x-subscription-id", "test-value")
            .json(&request);
        let (mut timed_out, mut bytes) = (false, 0);
        let mut failed = |e: reqwest::Error| {
            timed_out = e.is_timeout();
            Err(e.without_url().to_string())
        };
        let body = match request.send().await {
            Ok(response) => {
                let status = response.status();
                match response.bytes().await {
                    Ok(raw) => {
                        bytes = raw.len();
                        parse_body(&raw, status)
                    }
                    Err(e) => failed(e),
                }
            }
            Err(e) => failed(e),
        };
        let received = Instant::now();
        let reply = Reply {
            body,
            ms: (received - sent).as_secs_f64() * 1000.0,
            queued_ms: (sent - entered).as_secs_f64() * 1000.0,
            received,
            timed_out,
            bytes,
        };
        (reply, hooked)
    }

    /// Takes one token without a request, for calls that do not go through `call`.
    pub async fn pace(&self) {
        self.bucket.take().await;
    }

    pub async fn slot_at(&self, client: &reqwest::Client, commitment: &str) -> Option<u64> {
        let reply = self
            .call(client, "getSlot", json!([{"commitment": commitment}]))
            .await;
        reply.json()?["result"].as_u64()
    }

    /// Confirmed `context.slot` from getMultipleAccounts, the slot cache a fallback read uses.
    pub async fn cached_confirmed(&self, client: &reqwest::Client, key: &str) -> Option<u64> {
        let config = json!({"commitment": "confirmed", "encoding": "base64",
            "dataSlice": {"offset": 0, "length": 0}});
        let reply = self
            .call(client, "getMultipleAccounts", json!([[key], config]))
            .await;
        reply.slot()
    }

    /// Exclusion of `key` from a confirmed getAccountInfo, see [`probe_verdict`].
    pub async fn probe_excluded(
        &self,
        client: &reqwest::Client,
        key: &str,
        owner: &str,
    ) -> Option<bool> {
        let config = json!({"commitment": "confirmed", "encoding": "base64",
            "dataSlice": {"offset": 0, "length": 0}});
        let reply = self
            .call(client, "getAccountInfo", json!([key, config]))
            .await;
        probe_verdict(&reply.body, owner)
    }

    /// Confirmed block slots in `[lo, hi]`.
    pub async fn blocks(&self, client: &reqwest::Client, lo: u64, hi: u64) -> Option<Vec<u64>> {
        let params = json!([lo, hi, {"commitment": "confirmed"}]);
        let reply = self.call(client, "getBlocks", params).await;
        let slots = reply.json()?["result"].as_array()?;
        Some(slots.iter().filter_map(JsonValue::as_u64).collect())
    }
}

/// The Postgres path answers -32010 for an excluded owner, which proves the exclusion. An account
/// served with `owner` proves the owner is included. Anything else says nothing.
pub fn probe_verdict(body: &Result<JsonValue, String>, owner: &str) -> Option<bool> {
    let json = body.as_ref().ok()?;
    if json["error"]["code"] == EXCLUDED_CODE {
        return Some(true);
    }
    let served = json["result"]["value"]["owner"].as_str();
    (served == Some(owner)).then_some(false)
}

/// A JSON-RPC error body is kept whatever the HTTP status, anything else non-2xx is an error.
fn parse_body(raw: &[u8], status: reqwest::StatusCode) -> Result<JsonValue, String> {
    match serde_json::from_slice::<JsonValue>(raw) {
        Ok(json) if status.is_success() || json.get("error").is_some() => Ok(json),
        Ok(_) => Err(format!("HTTP {status}")),
        Err(e) if status.is_success() => Err(format!("invalid JSON: {e}")),
        Err(_) => Err(format!("HTTP {status}")),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum KeyCheck {
    Match,
    Mismatch(String),
    /// Null for an owner proven excluded.
    Excluded,
    /// Null for a live account while exclusion is not proven either way. Probe it.
    ExcludedUnknown,
}

pub fn owner_excluded(filter: &OwnerFilter, owner: &str) -> bool {
    filter.1.contains(owner) != filter.0
}

/// `excluded` is Some when the filter or a probe answered for the owner.
fn null_verdict(excluded: Option<bool>, owner: &str) -> KeyCheck {
    match excluded {
        None => KeyCheck::ExcludedUnknown,
        Some(true) => KeyCheck::Excluded,
        Some(false) => KeyCheck::Mismatch(format!("null, want an account owned by {owner}")),
    }
}

/// Settles an `ExcludedUnknown` with a probe result. Other checks pass through.
pub fn settle_probe(check: KeyCheck, probe: Option<bool>, owner: &str) -> KeyCheck {
    match (check, probe) {
        (KeyCheck::ExcludedUnknown, Some(true)) => KeyCheck::Excluded,
        (KeyCheck::ExcludedUnknown, Some(false)) => KeyCheck::Mismatch(format!(
            "null for a live account owned by {owner}, confirmed getAccountInfo serves that owner"
        )),
        (check, _) => check,
    }
}

/// Account fields that differ between two base64 account values.
pub fn diff_fields(a: &JsonValue, b: &JsonValue) -> Vec<&'static str> {
    COMPARED_FIELDS
        .into_iter()
        .filter(|f| a.get(f) != b.get(f))
        .collect()
}

/// Cloudbreak value against a reference value read at the same slot.
pub fn compare_account(
    cb: &JsonValue,
    reference: &JsonValue,
    filter: Option<&OwnerFilter>,
) -> KeyCheck {
    let excluded =
        |v: &JsonValue| filter.map(|f| owner_excluded(f, v["owner"].as_str().unwrap_or_default()));
    match (cb.is_null(), reference.is_null()) {
        (true, true) => KeyCheck::Match,
        (false, true) => KeyCheck::Mismatch("account served, reference is null".to_string()),
        (true, false) => null_verdict(
            excluded(reference),
            reference["owner"].as_str().unwrap_or_default(),
        ),
        (false, false) if excluded(cb) == Some(true) => {
            KeyCheck::Mismatch(format!("served excluded owner {}", cb["owner"]))
        }
        (false, false) => match diff_fields(cb, reference) {
            diff if diff.is_empty() => KeyCheck::Match,
            diff => KeyCheck::Mismatch(format!("fields differ: {}", diff.join(", "))),
        },
    }
}

/// A served value against the watcher's `(lamports, owner)`. Zero lamports must read as null.
pub fn check_chain(got: &JsonValue, want: (u64, &str), excluded: Option<bool>) -> KeyCheck {
    let (lamports, owner) = want;
    if got.is_null() {
        return match lamports {
            0 => KeyCheck::Match,
            _ => null_verdict(excluded, owner),
        };
    }
    let (got_lamports, got_owner) = (got["lamports"].as_u64(), got["owner"].as_str());
    if lamports > 0 && got_lamports == Some(lamports) && got_owner == Some(owner) {
        return match excluded {
            Some(true) => KeyCheck::Mismatch(format!("served excluded owner {owner}")),
            _ => KeyCheck::Match,
        };
    }
    let got = format!("{} {}", got["lamports"], got["owner"]);
    KeyCheck::Mismatch(format!("lamports and owner {got}, want {lamports} {owner}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: &str = "11111111111111111111111111111111";

    #[test]
    fn chain_checks_with_exclusion() {
        let account = json!({"lamports": 5, "owner": OWNER});
        let null = JsonValue::Null;
        assert_eq!(check_chain(&account, (5, OWNER), None), KeyCheck::Match);
        assert_eq!(
            check_chain(&account, (5, OWNER), Some(false)),
            KeyCheck::Match
        );
        let served = check_chain(&account, (5, OWNER), Some(true));
        assert!(matches!(served, KeyCheck::Mismatch(_)));
        assert_eq!(check_chain(&null, (0, OWNER), None), KeyCheck::Match);
        assert_eq!(
            check_chain(&null, (5, OWNER), None),
            KeyCheck::ExcludedUnknown
        );
        assert_eq!(
            check_chain(&null, (5, OWNER), Some(true)),
            KeyCheck::Excluded
        );
        let wrong_null = check_chain(&null, (5, OWNER), Some(false));
        assert!(matches!(wrong_null, KeyCheck::Mismatch(_)));

        let filter = (false, HashSet::from([OWNER.to_string()]));
        let reference = json!({"lamports": 5, "owner": OWNER, "executable": false});
        let cb_served = compare_account(&reference, &reference, Some(&filter));
        assert!(matches!(cb_served, KeyCheck::Mismatch(_)));
        assert_eq!(
            compare_account(&reference, &reference, None),
            KeyCheck::Match
        );
    }

    #[test]
    fn probe_settles_unknown_nulls() {
        let excluded = Ok(json!({"error": {"code": -32010, "message": "excluded"}}));
        let account = |owner: &str| {
            Ok(
                json!({"result": {"context": {"slot": 1}, "value": {"lamports": 1, "owner": owner}}}),
            )
        };
        let null = Ok(json!({"result": {"context": {"slot": 1}, "value": null}}));
        let other = Ok(json!({"error": {"code": -32603, "message": "internal"}}));
        assert_eq!(probe_verdict(&excluded, OWNER), Some(true));
        assert_eq!(probe_verdict(&account(OWNER), OWNER), Some(false));
        assert_eq!(probe_verdict(&account("other"), OWNER), None);
        assert_eq!(probe_verdict(&null, OWNER), None);
        assert_eq!(probe_verdict(&other, OWNER), None);
        assert_eq!(probe_verdict(&Err("timeout".to_string()), OWNER), None);

        let unknown = KeyCheck::ExcludedUnknown;
        assert_eq!(
            settle_probe(unknown.clone(), Some(true), OWNER),
            KeyCheck::Excluded
        );
        let wrong = settle_probe(unknown.clone(), Some(false), OWNER);
        assert!(matches!(wrong, KeyCheck::Mismatch(_)));
        assert_eq!(settle_probe(unknown.clone(), None, OWNER), unknown);
        assert_eq!(
            settle_probe(KeyCheck::Match, Some(false), OWNER),
            KeyCheck::Match
        );
    }
}
