// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Streams a `getProgramAccounts` response and reduces it as it arrives.
//!
//! A multi-gigabyte gPA response cannot be buffered and parsed into a
//! `serde_json::Value` — the `Value` alone runs several times the wire size.
//! This module parses the body incrementally and keeps only a digest per
//! account, so peak memory tracks the account *count*, not the response size.
//!
//! Two properties matter beyond memory:
//!
//! 1. **Truncation is an error, never a short list.** The API truncates its
//!    body on purpose when the account stream fails mid-response (see
//!    `cloudbreak_api::http::streaming`), so a partial body is invalid JSON. The
//!    parse runs to `Deserializer::end()`, which turns that into a hard error
//!    instead of a response that looks complete but is missing accounts.
//! 2. **The context slot arrives first.** The envelope opens with
//!    `{"jsonrpc":"2.0","result":{"context":{"slot":N},"value":[`, so both
//!    endpoints publish their slot to a shared [`SlotGate`] within the first
//!    few hundred bytes. If the slots disagree, both bodies abort immediately
//!    rather than transferring gigabytes that will be discarded.
//!
//! See `crates/integration_tests/README.md` for how the subcommand uses this.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use base64::Engine;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use clmm_liquidity_check::check::Collected;

use crate::config::RpcEndpoint;

/// Sentinel for "this side has not reported a slot yet".
const SLOT_UNSET: u64 = u64::MAX;
/// Sentinel for "this side responded without `result.context`".
const SLOT_NONE: u64 = u64::MAX - 1;

/// How many accounts to fold between checks of the abort flag.
///
/// The gate is also checked once before the first element, which catches the
/// common case: both requests start together, so by the time the array opens
/// the peer's slot is usually already known. This periodic check only covers
/// the side that started parsing before its peer reported, and 512 accounts of
/// tick arrays is a few MB — small next to an 8 GB body.
const ABORT_CHECK_INTERVAL: usize = 512;

/// How many bytes between transfer-progress lines.
const PROGRESS_LOG_BYTES: u64 = 1 << 30;

/// What one account reduces to. 88 bytes, versus kilobytes of JSON.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountDigest {
    pub lamports: u64,
    pub owner: Arc<str>,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data_len: u32,
    pub data_hash: [u8; 32],
}

/// A fully received and reduced gPA response.
pub struct GpaSnapshot {
    pub endpoint: String,
    pub context_slot: Option<u64>,
    pub accounts: HashMap<String, AccountDigest>,
    /// Bytes read off the wire.
    pub wire_bytes: u64,
    pub duration_ms: u128,
    /// Populated when this endpoint was asked to collect for the CLMM
    /// invariant check.
    pub clmm: Option<Collected>,
}

/// Coordinates the two in-flight responses so a slot disagreement kills both
/// transfers in the header instead of after gigabytes.
#[derive(Debug)]
pub struct SlotGate {
    slots: [AtomicU64; 2],
    aborted: AtomicBool,
    /// Endpoint names, indexed by side, for the progress lines.
    names: [String; 2],
    started: Instant,
}

impl SlotGate {
    pub fn new(names: [String; 2]) -> Self {
        Self {
            slots: [AtomicU64::new(SLOT_UNSET), AtomicU64::new(SLOT_UNSET)],
            aborted: AtomicBool::new(false),
            names,
            started: Instant::now(),
        }
    }

    /// Records this side's context slot and aborts both sides when the other
    /// side has already reported a different one. A response without context
    /// never aborts — there is nothing to compare.
    fn publish(&self, side: usize, slot: Option<u64>) {
        let encoded = slot.unwrap_or(SLOT_NONE);
        self.slots[side].store(encoded, Ordering::SeqCst);
        let elapsed = self.started.elapsed().as_secs_f64();

        match slot {
            Some(slot) => tracing::info!(
                target: "bench_streaming",
                "{} reported context slot {} (+{:.1}s)", self.names[side], slot, elapsed,
            ),
            None => tracing::info!(
                target: "bench_streaming",
                "{} responded without result.context (+{:.1}s) — slot gating is off",
                self.names[side], elapsed,
            ),
        }

        let other = self.slots[1 - side].load(Ordering::SeqCst);
        if other == SLOT_UNSET || other == SLOT_NONE || encoded == SLOT_NONE {
            return;
        }

        // Only the second side to report gets here, so this decides the round.
        if other != encoded {
            self.aborted.store(true, Ordering::SeqCst);
        } else {
            tracing::info!(
                target: "bench_streaming",
                "✓ both endpoints at slot {} (+{:.1}s) — streaming account data",
                encoded, elapsed,
            );
        }
    }

    fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
    }

    /// Cancels both transfers. Used when one endpoint returns a JSON-RPC error:
    /// there is nothing left to compare against, so the peer should stop
    /// downloading rather than finish a body no one will read.
    fn abort(&self) {
        self.aborted.store(true, Ordering::SeqCst);
    }

    /// The slots both sides reported, once known.
    pub fn slots(&self) -> (Option<u64>, Option<u64>) {
        let decode = |raw: u64| match raw {
            SLOT_UNSET | SLOT_NONE => None,
            slot => Some(slot),
        };
        (
            decode(self.slots[0].load(Ordering::SeqCst)),
            decode(self.slots[1].load(Ordering::SeqCst)),
        )
    }
}

/// Distinguishes "we cancelled this transfer on purpose" from a real failure.
#[derive(Debug)]
pub enum FetchError {
    /// The other endpoint reported a different context slot.
    SlotMismatch,
    /// The endpoint returned a JSON-RPC error object.
    Rpc(String),
    Other(anyhow::Error),
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FetchError::SlotMismatch => {
                write!(f, "aborted: peer reported a different context slot")
            }
            FetchError::Rpc(e) => write!(f, "endpoint returned an error: {e}"),
            FetchError::Other(e) => write!(f, "{e:#}"),
        }
    }
}

/// Marker planted in the serde error chain so an abort is not mistaken for
/// malformed JSON.
const ABORT_MARKER: &str = "__gpa_slot_gate_abort__";
/// Marker for a JSON-RPC `error` object, which is a valid response, not a
/// parse failure.
const RPC_ERROR_MARKER: &str = "__gpa_rpc_error__:";

/// Sends the request and reduces the streamed response.
///
/// `side` is this endpoint's index in the shared [`SlotGate`] (0 for rpc1,
/// 1 for rpc2).
pub async fn fetch_snapshot(
    client: &reqwest::Client,
    endpoint: &RpcEndpoint,
    request: &serde_json::Value,
    gate: Arc<SlotGate>,
    side: usize,
    collect_clmm: bool,
) -> Result<GpaSnapshot, FetchError> {
    let start = Instant::now();

    let response = client
        .post(&endpoint.url)
        .header("x-subscription-id", "test-value")
        .json(request)
        .send()
        .await
        .with_context(|| format!("Failed to connect to {}", endpoint.name))
        .map_err(FetchError::Other)?;

    let status = response.status();
    if !status.is_success() {
        return Err(FetchError::Other(anyhow::anyhow!(
            "{} returned HTTP {}",
            endpoint.name,
            status
        )));
    }

    // Count wire bytes as they pass, then hand the same bytes to a blocking
    // reader so `serde_json` can pull from them without buffering the body.
    let wire_bytes = Arc::new(AtomicU64::new(0));
    let counter = wire_bytes.clone();
    // A multi-gigabyte body takes minutes; without this the run looks hung
    // between the slot lines and the final summary.
    let progress_name = endpoint.name.clone();
    let mut next_mark = PROGRESS_LOG_BYTES;
    let byte_stream = futures_util::TryStreamExt::map_ok(
        futures_util::TryStreamExt::map_err(response.bytes_stream(), std::io::Error::other),
        move |chunk| {
            let total =
                counter.fetch_add(chunk.len() as u64, Ordering::Relaxed) + chunk.len() as u64;
            if total >= next_mark {
                while next_mark <= total {
                    next_mark += PROGRESS_LOG_BYTES;
                }
                tracing::info!(
                    target: "bench_streaming",
                    "{}: {:.2} GB received", progress_name, total as f64 / 1e9,
                );
            }
            chunk
        },
    );
    let reader = tokio_util::io::StreamReader::new(byte_stream);

    let name = endpoint.name.clone();
    let parsed = tokio::task::spawn_blocking(move || {
        let reader = tokio_util::io::SyncIoBridge::new(reader);
        let reader = std::io::BufReader::with_capacity(1 << 20, reader);
        let mut de = serde_json::Deserializer::from_reader(reader);
        let snapshot = EnvelopeSeed {
            gate,
            side,
            clmm: collect_clmm.then(Collected::default),
        }
        .deserialize(&mut de)?;
        // Refuse a body that stops early. The API truncates on purpose when its
        // account stream fails, so this is the check that keeps a partial
        // response from reading as a complete one.
        de.end()?;
        Ok::<_, serde_json::Error>(snapshot)
    })
    .await
    .map_err(|e| FetchError::Other(anyhow::anyhow!("parse task panicked: {e}")))?;

    let parsed = match parsed {
        Ok(parsed) => parsed,
        Err(e) => {
            let message = e.to_string();
            if message.contains(ABORT_MARKER) {
                return Err(FetchError::SlotMismatch);
            }
            if let Some(index) = message.find(RPC_ERROR_MARKER) {
                let rest = &message[index + RPC_ERROR_MARKER.len()..];
                let end = rest.find(" at line").unwrap_or(rest.len());
                return Err(FetchError::Rpc(rest[..end].to_string()));
            }
            let received = wire_bytes.load(Ordering::Relaxed);
            return Err(FetchError::Other(anyhow::Error::new(e).context(format!(
                "Failed to parse the streamed response from {name} after {received} bytes \
                 ({:.2} GB). A clean EOF with incomplete JSON means the endpoint truncated its \
                 own body mid-response — look for `Account stream errored mid-response` in its logs",
                received as f64 / 1e9,
            ))));
        }
    };

    Ok(GpaSnapshot {
        endpoint: endpoint.name.clone(),
        context_slot: parsed.context_slot,
        accounts: parsed.accounts,
        wire_bytes: wire_bytes.load(Ordering::Relaxed),
        duration_ms: start.elapsed().as_millis(),
        clmm: parsed.clmm,
    })
}

struct ParsedEnvelope {
    context_slot: Option<u64>,
    accounts: HashMap<String, AccountDigest>,
    clmm: Option<Collected>,
}

impl fmt::Debug for ParsedEnvelope {
    // `Collected` has no Debug, and printing 2M accounts would be useless
    // anyway, so summarise.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParsedEnvelope")
            .field("context_slot", &self.context_slot)
            .field("accounts", &self.accounts.len())
            .field(
                "clmm_accounts",
                &self.clmm.as_ref().map(|c| c.account_count),
            )
            .finish()
    }
}

/// Deserializes the JSON-RPC envelope, publishing the context slot to the gate
/// as soon as it is seen.
struct EnvelopeSeed {
    gate: Arc<SlotGate>,
    side: usize,
    clmm: Option<Collected>,
}

impl<'de> DeserializeSeed<'de> for EnvelopeSeed {
    type Value = ParsedEnvelope;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for EnvelopeSeed {
    type Value = ParsedEnvelope;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON-RPC response object")
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut parsed = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "result" => {
                    parsed = Some(map.next_value_seed(ResultSeed {
                        gate: self.gate.clone(),
                        side: self.side,
                        clmm: self.clmm.take(),
                    })?);
                }
                "error" => {
                    let value: serde_json::Value = map.next_value()?;
                    // Stop the peer too — with one side erroring there is
                    // nothing to compare, and its body may be gigabytes.
                    self.gate.abort();
                    return Err(de::Error::custom(format!("{RPC_ERROR_MARKER}{value}")));
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        parsed.ok_or_else(|| de::Error::custom("response has neither `result` nor `error`"))
    }
}

/// `result` is either `{"context":{...},"value":[...]}` or a bare array when
/// the request omitted `withContext`.
struct ResultSeed {
    gate: Arc<SlotGate>,
    side: usize,
    clmm: Option<Collected>,
}

impl<'de> DeserializeSeed<'de> for ResultSeed {
    type Value = ParsedEnvelope;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ResultSeed {
    type Value = ParsedEnvelope;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a gPA result: an account array or a context-wrapped one")
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, seq: A) -> Result<Self::Value, A::Error> {
        // No context, so nothing to gate on. Tell the peer so it does not wait.
        self.gate.publish(self.side, None);
        let accounts = AccountsSeed {
            gate: &self.gate,
            clmm: self.clmm.as_mut(),
        }
        .visit_seq(seq)?;
        Ok(ParsedEnvelope {
            context_slot: None,
            accounts,
            clmm: self.clmm,
        })
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut context_slot = None;
        let mut accounts = None;
        let mut published = false;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "context" => {
                    context_slot = map.next_value::<ResponseContext>()?.slot;
                    // The envelope puts `context` before `value`, so this fires
                    // in the first bytes — before the body is worth cancelling.
                    self.gate.publish(self.side, context_slot);
                    published = true;
                }
                "value" => {
                    if !published {
                        self.gate.publish(self.side, context_slot);
                        published = true;
                    }
                    accounts = Some(map.next_value_seed(AccountsSeed {
                        gate: &self.gate,
                        clmm: self.clmm.as_mut(),
                    })?);
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok(ParsedEnvelope {
            context_slot,
            accounts: accounts.unwrap_or_default(),
            clmm: self.clmm,
        })
    }
}

#[derive(serde::Deserialize)]
struct ResponseContext {
    #[serde(default)]
    slot: Option<u64>,
}

/// Folds the account array into the digest map, dropping each account's bytes
/// as soon as they are hashed.
struct AccountsSeed<'a> {
    gate: &'a SlotGate,
    /// When present, every account is also pushed into the third-party CLMM
    /// invariant collector as it is decoded. Only the endpoint under test
    /// collects; the oracle does not.
    clmm: Option<&'a mut Collected>,
}

impl<'de> DeserializeSeed<'de> for AccountsSeed<'_> {
    type Value = HashMap<String, AccountDigest>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for AccountsSeed<'_> {
    type Value = HashMap<String, AccountDigest>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an array of {pubkey, account} entries")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        // Both requests are fired together, so the peer's slot is normally on
        // record by the time the array opens. Checking here means a mismatched
        // round costs the header, not a slice of the body.
        if self.gate.is_aborted() {
            return Err(de::Error::custom(ABORT_MARKER));
        }

        let mut accounts: HashMap<String, AccountDigest> =
            HashMap::with_capacity(seq.size_hint().unwrap_or(1024));
        // Almost every account shares one owner, so interning turns a 44-byte
        // string per account into a pointer.
        let mut owners: HashMap<String, Arc<str>> = HashMap::new();
        let mut since_check = 0usize;

        let mut clmm = self.clmm;

        while let Some(keyed) = seq.next_element_seed(KeyedAccountSeed {
            owners: &mut owners,
            clmm: clmm.as_deref_mut(),
        })? {
            accounts.insert(keyed.0, keyed.1);

            since_check += 1;
            if since_check >= ABORT_CHECK_INTERVAL {
                since_check = 0;
                if self.gate.is_aborted() {
                    return Err(de::Error::custom(ABORT_MARKER));
                }
            }
        }

        Ok(accounts)
    }
}

struct KeyedAccountSeed<'a> {
    owners: &'a mut HashMap<String, Arc<str>>,
    clmm: Option<&'a mut Collected>,
}

impl<'de> DeserializeSeed<'de> for KeyedAccountSeed<'_> {
    type Value = (String, AccountDigest);

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for KeyedAccountSeed<'_> {
    type Value = (String, AccountDigest);

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a {pubkey, account} entry")
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut pubkey = None;
        let mut account = None;
        let mut raw = None;

        // The two implementations order these keys differently — cloudbreak
        // emits `pubkey` first, Agave emits `account` first — so the decoded
        // bytes are held until both halves are known, then handed over.
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "pubkey" => pubkey = Some(map.next_value::<String>()?),
                "account" => {
                    let (digest, bytes) = map.next_value_seed(AccountSeed {
                        owners: self.owners,
                        keep_raw: self.clmm.is_some(),
                    })?;
                    account = Some(digest);
                    raw = bytes;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        match (pubkey, account) {
            (Some(pubkey), Some(account)) => {
                if let (Some(collected), Some(bytes)) = (self.clmm.as_deref_mut(), raw) {
                    // Exactly the call the standalone binary makes, so the
                    // check sees the same input either way.
                    collected.push_account(&pubkey, &bytes);
                }
                Ok((pubkey, account))
            }
            _ => Err(de::Error::custom("entry is missing `pubkey` or `account`")),
        }
    }
}

struct AccountSeed<'a> {
    owners: &'a mut HashMap<String, Arc<str>>,
    keep_raw: bool,
}

impl<'de> DeserializeSeed<'de> for AccountSeed<'_> {
    type Value = (AccountDigest, Option<Vec<u8>>);

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for AccountSeed<'_> {
    type Value = (AccountDigest, Option<Vec<u8>>);

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a UiAccount object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut lamports = 0u64;
        let mut owner: Option<Arc<str>> = None;
        let mut executable = false;
        let mut rent_epoch = 0u64;
        let mut data: Option<DecodedData> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "lamports" => lamports = map.next_value()?,
                "executable" => executable = map.next_value()?,
                "rentEpoch" => rent_epoch = map.next_value()?,
                "owner" => {
                    let raw: String = map.next_value()?;
                    owner = Some(match self.owners.get(&raw) {
                        Some(interned) => interned.clone(),
                        None => {
                            let interned: Arc<str> = Arc::from(raw.as_str());
                            self.owners.insert(raw, interned.clone());
                            interned
                        }
                    });
                }
                "data" => {
                    data = Some(map.next_value_seed(DecodedDataSeed {
                        keep_raw: self.keep_raw,
                    })?)
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        let data = data.ok_or_else(|| de::Error::custom("account is missing `data`"))?;

        Ok((
            AccountDigest {
                lamports,
                owner: owner.unwrap_or_else(|| Arc::from("")),
                executable,
                rent_epoch,
                data_len: data.len,
                data_hash: data.hash,
            },
            data.raw,
        ))
    }
}

/// Account data reduced to a hash the moment it is read, so the bytes never
/// outlive one account.
///
/// `raw` is populated only when the CLMM invariant check is collecting from
/// this endpoint. It holds one account at a time — the decoder hands it
/// straight to `Collected::push_account` and drops it — so peak memory still
/// tracks the account count, not the response size.
struct DecodedData {
    len: u32,
    hash: [u8; 32],
    raw: Option<Vec<u8>>,
}

/// Seed carrying whether the decoded bytes must be kept for the CLMM check.
struct DecodedDataSeed {
    keep_raw: bool,
}

impl<'de> DeserializeSeed<'de> for DecodedDataSeed {
    type Value = DecodedData;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<DecodedData, D::Error> {
        deserializer.deserialize_any(DecodedDataVisitor {
            keep_raw: self.keep_raw,
        })
    }
}

struct DecodedDataVisitor {
    keep_raw: bool,
}

impl<'de> Visitor<'de> for DecodedDataVisitor {
    type Value = DecodedData;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("account data as [encoded, encoding] or a parsed object")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let encoded: String = seq
            .next_element()?
            .ok_or_else(|| de::Error::custom("data array is empty"))?;
        let encoding: String = seq.next_element()?.unwrap_or_else(|| "base64".to_string());
        while seq.next_element::<IgnoredAny>()?.is_some() {}

        // Decode before hashing: zstd output is not byte-stable across
        // implementations, so hashing the encoded form would report false
        // mismatches.
        let raw = match encoding.as_str() {
            "base64" => base64::engine::general_purpose::STANDARD
                .decode(&encoded)
                .map_err(de::Error::custom)?,
            "base58" => bs58::decode(&encoded)
                .into_vec()
                .map_err(de::Error::custom)?,
            "base64+zstd" => {
                let compressed = base64::engine::general_purpose::STANDARD
                    .decode(&encoded)
                    .map_err(de::Error::custom)?;
                zstd::decode_all(std::io::Cursor::new(compressed)).map_err(de::Error::custom)?
            }
            other => return Err(de::Error::custom(format!("unsupported encoding `{other}`"))),
        };

        Ok(DecodedData {
            len: raw.len() as u32,
            hash: *blake3::hash(&raw).as_bytes(),
            raw: self.keep_raw.then_some(raw),
        })
    }

    /// `jsonParsed` has no byte form, so the parsed object itself is hashed.
    /// That is comparable between endpoints but not against Geyser.
    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        let value = serde_json::Value::deserialize(de::value::MapAccessDeserializer::new(map))
            .map_err(de::Error::custom)?;
        let rendered = value.to_string();
        Ok(DecodedData {
            len: rendered.len() as u32,
            hash: *blake3::hash(rendered.as_bytes()).as_bytes(),
            raw: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses a complete document the way `fetch_snapshot` does, including the
    /// trailing `end()` that rejects a short body.
    fn parse(body: &str, gate: Arc<SlotGate>, side: usize) -> Result<ParsedEnvelope, String> {
        parse_with_clmm(body, gate, side, false)
    }

    fn parse_with_clmm(
        body: &str,
        gate: Arc<SlotGate>,
        side: usize,
        collect: bool,
    ) -> Result<ParsedEnvelope, String> {
        let mut de = serde_json::Deserializer::from_reader(body.as_bytes());
        let parsed = EnvelopeSeed {
            gate,
            side,
            clmm: collect.then(Collected::default),
        }
        .deserialize(&mut de)
        .map_err(|e| e.to_string())?;
        de.end().map_err(|e| e.to_string())?;
        Ok(parsed)
    }

    fn names() -> [String; 2] {
        ["rpc1".to_string(), "rpc2".to_string()]
    }

    fn account(pubkey: &str, lamports: u64, data_b64: &str) -> String {
        format!(
            r#"{{"pubkey":"{pubkey}","account":{{"lamports":{lamports},"owner":"CAMM","executable":false,"rentEpoch":0,"space":3,"data":["{data_b64}","base64"]}}}}"#
        )
    }

    fn envelope(slot: u64, accounts: &[String]) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","result":{{"context":{{"apiVersion":"2.1.0","slot":{slot}}},"value":[{}]}},"id":"R"}}"#,
            accounts.join(",")
        )
    }

    #[test]
    fn reduces_accounts_to_digests() {
        let body = envelope(100, &[account("A", 5, "AQID"), account("B", 7, "BAUG")]);
        let parsed = parse(&body, Arc::new(SlotGate::new(names())), 0).expect("parses");

        assert_eq!(parsed.context_slot, Some(100));
        assert_eq!(parsed.accounts.len(), 2);
        let a = &parsed.accounts["A"];
        assert_eq!(a.lamports, 5);
        assert_eq!(&*a.owner, "CAMM");
        assert_eq!(a.data_len, 3);
        // The digest is over the decoded bytes, not the base64 text.
        assert_eq!(a.data_hash, *blake3::hash(&[1u8, 2, 3]).as_bytes());
    }

    /// The API truncates its body deliberately when the account stream fails
    /// mid-response. That must surface as an error, never as a short account
    /// list — otherwise every missing account reads as a real diff.
    #[test]
    fn truncated_body_is_an_error_not_a_short_list() {
        let full = envelope(100, &[account("A", 5, "AQID"), account("B", 7, "BAUG")]);

        // Cut at three points: mid-account, after a complete account but before
        // the closing `]},"id":...}`, and one byte short of the end.
        for cut in [full.len() / 2, full.rfind(']').unwrap(), full.len() - 1] {
            let err = parse(&full[..cut], Arc::new(SlotGate::new(names())), 0)
                .expect_err("a truncated body must not parse");
            assert!(
                err.contains("EOF") || err.contains("eof"),
                "cut at {cut} gave an unexpected error: {err}",
            );
        }

        // The intact body still parses, so the check is not simply always-fail.
        assert!(parse(&full, Arc::new(SlotGate::new(names())), 0).is_ok());
    }

    /// The context slot precedes the accounts in the envelope, so it reaches
    /// the gate before the body is worth cancelling.
    #[test]
    fn context_slot_is_published_before_the_accounts() {
        let gate = Arc::new(SlotGate::new(names()));
        // Side 1 already reported a different slot.
        gate.publish(1, Some(999));

        let body = envelope(100, &[account("A", 5, "AQID")]);
        let err = parse(&body, gate.clone(), 0).expect_err("must abort");
        assert!(err.contains(ABORT_MARKER), "unexpected error: {err}");
        assert!(gate.is_aborted());
        assert_eq!(gate.slots(), (Some(100), Some(999)));
    }

    /// Matching slots must not abort.
    #[test]
    fn equal_slots_do_not_abort() {
        let gate = Arc::new(SlotGate::new(names()));
        gate.publish(1, Some(100));
        let body = envelope(100, &[account("A", 5, "AQID")]);
        assert!(parse(&body, gate.clone(), 0).is_ok());
        assert!(!gate.is_aborted());
    }

    /// A response without `result.context` has no slot to gate on, and must
    /// not abort the peer.
    #[test]
    fn missing_context_never_aborts() {
        let gate = Arc::new(SlotGate::new(names()));
        gate.publish(1, Some(100));

        let body = format!(
            r#"{{"jsonrpc":"2.0","result":[{}],"id":"R"}}"#,
            account("A", 5, "AQID")
        );
        let parsed = parse(&body, gate.clone(), 0).expect("parses");
        assert_eq!(parsed.context_slot, None);
        assert_eq!(parsed.accounts.len(), 1);
        assert!(!gate.is_aborted());
    }

    /// A JSON-RPC error object is a valid response, not a parse failure — this
    /// is how Agave's scan limit arrives.
    #[test]
    fn rpc_error_is_reported_as_such() {
        let body = r#"{"jsonrpc":"2.0","error":{"code":-32012,"message":"scan aborted: The accumulated scan results exceeded the limit"},"id":"R"}"#;
        let err = parse(body, Arc::new(SlotGate::new(names())), 0).expect_err("must fail");
        assert!(err.contains(RPC_ERROR_MARKER), "unexpected error: {err}");
        assert!(err.contains("-32012"));
    }

    /// One endpoint erroring must stop the other: with nothing to compare
    /// against, finishing a multi-gigabyte body is pure waste.
    #[test]
    fn rpc_error_aborts_the_peer() {
        let gate = Arc::new(SlotGate::new(names()));
        let body = r#"{"jsonrpc":"2.0","error":{"code":-32012,"message":"scan aborted"},"id":"R"}"#;
        parse(body, gate.clone(), 1).expect_err("must fail");
        assert!(gate.is_aborted(), "the peer's transfer should be cancelled");
    }

    /// zstd output is not byte-stable, so data must be decompressed before it
    /// is hashed or two healthy endpoints would look like they disagree.
    #[test]
    fn zstd_is_decompressed_before_hashing() {
        let raw = b"the same account bytes";
        let compressed = zstd::encode_all(std::io::Cursor::new(raw), 3).expect("compresses");
        let b64 = base64::engine::general_purpose::STANDARD.encode(&compressed);
        let body = format!(
            r#"{{"jsonrpc":"2.0","result":{{"context":{{"slot":1}},"value":[{{"pubkey":"A","account":{{"lamports":1,"owner":"CAMM","executable":false,"rentEpoch":0,"data":["{b64}","base64+zstd"]}}}}]}},"id":1}}"#
        );

        let parsed = parse(&body, Arc::new(SlotGate::new(names())), 0).expect("parses");
        let digest = &parsed.accounts["A"];
        assert_eq!(digest.data_len, raw.len() as u32);
        assert_eq!(digest.data_hash, *blake3::hash(raw).as_bytes());
    }

    /// The CLMM check must see exactly the bytes the standalone binary would.
    /// It classifies accounts by data length, so a pool state (1544 bytes) has
    /// to arrive intact and be counted.
    #[test]
    fn clmm_collector_receives_the_decoded_bytes() {
        use base64::Engine;

        // A 1544-byte account is a CLMM pool state; 100 bytes is ignored.
        let pool = base64::engine::general_purpose::STANDARD.encode(vec![7u8; 1544]);
        let other = base64::engine::general_purpose::STANDARD.encode(vec![9u8; 100]);
        // `check.rs` decodes the pubkey and requires a real 32-byte key, so a
        // placeholder string would be silently skipped.
        let pool_key = bs58::encode([1u8; 32]).into_string();
        let other_key = bs58::encode([2u8; 32]).into_string();
        let body = envelope(
            5,
            &[account(&pool_key, 1, &pool), account(&other_key, 2, &other)],
        );

        let parsed =
            parse_with_clmm(&body, Arc::new(SlotGate::new(names())), 0, true).expect("parses");
        let collected = parsed.clmm.expect("collector present");

        // Both accounts reach push_account; only the pool is decoded further.
        assert_eq!(collected.account_count, 2);
        assert_eq!(collected.pools.len(), 1);
        assert_eq!(collected.positions.len(), 0);
    }

    /// Without the collector, no account bytes are retained — the digest path
    /// must stay allocation-free per account.
    #[test]
    fn raw_bytes_are_dropped_when_the_check_is_off() {
        let body = envelope(5, &[account("A", 1, "AQID")]);
        let parsed = parse(&body, Arc::new(SlotGate::new(names())), 0).expect("parses");
        assert!(parsed.clmm.is_none());
        // The digest is still complete.
        assert_eq!(
            parsed.accounts["A"].data_hash,
            *blake3::hash(&[1u8, 2, 3]).as_bytes()
        );
    }

    /// Owners repeat across every account, so they are interned rather than
    /// stored per account.
    #[test]
    fn owners_are_interned() {
        let body = envelope(
            1,
            &[
                account("A", 1, "AQID"),
                account("B", 2, "AQID"),
                account("C", 3, "AQID"),
            ],
        );
        let parsed = parse(&body, Arc::new(SlotGate::new(names())), 0).expect("parses");
        let a = parsed.accounts["A"].owner.clone();
        let b = parsed.accounts["B"].owner.clone();
        assert!(Arc::ptr_eq(&a, &b), "identical owners must share one Arc");
    }
}
