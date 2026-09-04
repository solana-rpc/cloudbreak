// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Cross-checks a `getProgramAccounts` mismatch against the raw Yellowstone
//! Geyser stream the indexer consumes.
//!
//! A background task mirrors the indexer's subscription (whole `Block` at
//! `Confirmed`, see `cloudbreak_index::modules::grpc`) and keeps a bounded
//! per-pubkey ring of account writes. When the benchmark finds a differing
//! account it asks this module which Geyser event each endpoint's bytes match,
//! which turns "the two RPCs disagree" into "cloudbreak never applied the
//! write at slot N". `getBlock` then confirms the write against the ledger.
//!
//! See `crates/integration_tests/README.md` for the verdict taxonomy.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use futures::StreamExt;
use serde_json::Value as JsonValue;
use solana_pubkey::Pubkey;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots,
    subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

use crate::config::{GeyserCheckConfig, RpcEndpoint};
use crate::utils::{self, AccountDiff, DiffKind};

/// One account write seen on the Geyser stream. Account data is stored as a
/// blake3 hash so a busy program's ring stays a few tens of KB per pubkey.
#[derive(Clone, Debug)]
pub struct AccountEvent {
    pub slot: u64,
    pub write_version: u64,
    pub lamports: u64,
    pub data_hash: [u8; 32],
    pub data_len: u32,
    /// `None` while the account is owned by the tracked program, which is every
    /// event but the rare one that reassigns it. Storing the owner outright
    /// would repeat the same 44-byte string on every write.
    pub owner: Option<Box<str>>,
    /// Kept raw. Base58 encoding every signature on ingest would cost more than
    /// the whole rest of the hot path, and only mismatches ever read it.
    pub txn_signature: Option<[u8; 64]>,
}

impl AccountEvent {
    /// The account's owner at this write. `program` is the tracked program,
    /// which is what a `None` owner means.
    fn owner<'a>(&'a self, program: &'a str) -> &'a str {
        self.owner.as_deref().unwrap_or(program)
    }

    fn signature(&self) -> Option<String> {
        self.txn_signature
            .as_ref()
            .map(|sig| bs58::encode(sig).into_string())
    }

    fn to_json(&self, program: &str) -> JsonValue {
        serde_json::json!({
            "slot": self.slot,
            "write_version": self.write_version,
            "lamports": self.lamports,
            "owner": self.owner(program),
            "data_len": self.data_len,
            "data_hash": hex::encode(&self.data_hash[..8]),
            "txn_signature": self.signature(),
        })
    }
}

#[derive(Default)]
struct Inner {
    /// pubkey (base58) → ring of writes, oldest first. Its key set is also the
    /// tracking set: once an account appears here it is followed forever, so a
    /// close (`lamports == 0`, owner → system) or a reassignment away from the
    /// program still lands as one more write instead of being filtered out.
    accounts: HashMap<String, VecDeque<AccountEvent>>,
    first_slot: Option<u64>,
    last_slot: u64,
    blocks_seen: u64,
    /// How many times one pubkey appeared more than once inside a single block
    /// message. Expected to stay 0. If it does not, two rows share a
    /// `(pubkey, slot)` in the `accounts` table, and `getProgramAccounts.sql`
    /// picks between them arbitrarily — it orders by `slot DESC` with no
    /// `write_version` tie-break, unlike `lt_hash.rs`.
    duplicate_writes: u64,
}

/// How many blocks between running-total log lines. At ~2.5 blocks/s this is
/// roughly every 3 minutes, so a 7-minute comparison round produces two.
const STATS_EVERY_BLOCKS: u64 = 500;

pub struct GeyserHistory {
    inner: RwLock<Inner>,
    program: String,
    history_size: usize,
}

impl GeyserHistory {
    fn new(program: String, history_size: usize) -> Self {
        Self {
            inner: RwLock::new(Inner::default()),
            program,
            history_size,
        }
    }

    /// Slot of the first block we observed. `None` until the stream delivers
    /// one — every verdict before that is `NoHistory`.
    pub fn first_slot(&self) -> Option<u64> {
        self.inner.read().expect("history poisoned").first_slot
    }

    pub fn stats(&self) -> (u64, u64, usize, u64) {
        let inner = self.inner.read().expect("history poisoned");
        (
            inner.last_slot,
            inner.blocks_seen,
            inner.accounts.len(),
            inner.duplicate_writes,
        )
    }

    fn events(&self, pubkey: &str) -> Vec<AccountEvent> {
        self.inner
            .read()
            .expect("history poisoned")
            .accounts
            .get(pubkey)
            .map(|ring| ring.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn ingest_block(
        &self,
        slot: u64,
        accounts: Vec<yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo>,
    ) {
        let mut inner = self.inner.write().expect("history poisoned");
        inner.first_slot.get_or_insert(slot);
        inner.last_slot = inner.last_slot.max(slot);
        inner.blocks_seen += 1;

        // pubkey -> the write_version last seen for it in this block.
        let mut seen_this_block: HashMap<String, u64> = HashMap::new();
        let mut duplicates: Vec<(String, u64, u64)> = Vec::new();

        for account in accounts {
            let Ok(pubkey) = Pubkey::try_from(account.pubkey.as_slice()) else {
                continue;
            };
            let Ok(owner) = Pubkey::try_from(account.owner.as_slice()) else {
                continue;
            };
            let pubkey = pubkey.to_string();
            let owner = owner.to_string();

            // Mirror the indexer: an account counts once it is owned by the
            // program, and keeps counting afterwards even when it is closed or
            // reassigned to another owner.
            let is_program_owned = owner == self.program;
            if !is_program_owned && !inner.accounts.contains_key(&pubkey) {
                continue;
            }

            if let Some(previous) = seen_this_block.insert(pubkey.clone(), account.write_version) {
                duplicates.push((pubkey.clone(), previous, account.write_version));
                inner.duplicate_writes += 1;
            }

            let event = AccountEvent {
                slot,
                write_version: account.write_version,
                lamports: account.lamports,
                data_hash: *blake3::hash(&account.data).as_bytes(),
                data_len: account.data.len() as u32,
                owner: (!is_program_owned).then(|| owner.into_boxed_str()),
                txn_signature: account
                    .txn_signature
                    .as_deref()
                    .and_then(|sig| sig.try_into().ok()),
            };

            let history_size = self.history_size;
            let ring = inner.accounts.entry(pubkey).or_default();
            if ring.len() >= history_size {
                ring.pop_front();
            }
            ring.push_back(event);
        }

        // One line per affected block rather than per account, with the
        // write_versions that a tie-break would have had to choose between.
        if !duplicates.is_empty() {
            let sample: Vec<String> = duplicates
                .iter()
                .take(5)
                .map(|(pubkey, first, second)| {
                    format!("{pubkey} (write_version {first} then {second})")
                })
                .collect();
            tracing::warn!(
                target: "bench_geyser_check",
                "⚠ slot {}: {} pubkey(s) written more than once in one block — getProgramAccounts.sql \
                 breaks (pubkey, slot) ties arbitrarily: {}",
                slot,
                duplicates.len(),
                sample.join(", "),
            );
        }

        // Surface the running totals without waiting for a mismatch, so the
        // duplicate-write question gets answered on any run.
        if inner.blocks_seen.is_multiple_of(STATS_EVERY_BLOCKS) {
            tracing::info!(
                target: "bench_geyser_check",
                "geyser: slot {} | {} blocks | {} tracked pubkeys | {} duplicate writes",
                inner.last_slot,
                inner.blocks_seen,
                inner.accounts.len(),
                inner.duplicate_writes,
            );
        }
    }
}

/// Subscribes to Geyser in the background and returns the history the
/// subscriber fills. Reconnects forever; the benchmark keeps running with a
/// stale history if the stream drops, and says so in the verdicts.
pub fn spawn_subscriber(config: &GeyserCheckConfig) -> Arc<GeyserHistory> {
    // rustls 0.23 needs a process-level crypto provider before the first TLS
    // handshake, the same install the indexer does. An `Err` here only means
    // something already installed one, so it is ignored.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let history = Arc::new(GeyserHistory::new(
        config.program.clone(),
        config.history_size,
    ));
    let task_history = history.clone();
    let config = config.clone();

    tokio::spawn(async move {
        loop {
            if let Err(e) = run_subscription(&config, &task_history).await {
                tracing::error!(target: "bench_geyser_check", "Geyser subscription failed: {:#}", e);
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    history
}

async fn run_subscription(config: &GeyserCheckConfig, history: &GeyserHistory) -> Result<()> {
    let timeout = Duration::from_secs(config.timeout_secs);

    let mut client = GeyserGrpcClient::build_from_shared(config.endpoint.clone())
        .context("Failed to build GeyserGrpcClient")?
        .x_token(Some(config.x_token.clone().unwrap_or_default()))
        .context("Failed to set x-token")?
        .max_decoding_message_size(usize::MAX)
        .accept_compressed(CompressionEncoding::Zstd)
        .connect_timeout(timeout)
        .timeout(timeout)
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .context("Failed to set tls config")?
        .tcp_keepalive(Some(Duration::from_secs(10)))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(10))
        .connect()
        .await
        .context("Failed to connect to Yellowstone GRPC")?;

    // Byte-for-byte the indexer's subscription: whole blocks with accounts at
    // Confirmed. Keeping it identical is the point — a filter difference here
    // would make every verdict meaningless.
    let request = SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::from([(
            "accounts_slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(false),
            },
        )]),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::from([(
            "accounts_blocks".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: vec![],
                include_transactions: Some(false),
                include_accounts: Some(true),
                include_entries: Some(false),
            },
        )]),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    };

    let (_tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .context("Failed to subscribe to Yellowstone GRPC")?;

    tracing::info!(
        target: "bench_geyser_check",
        "Subscribed to Geyser at {} tracking owner {}",
        config.endpoint,
        config.program,
    );

    while let Some(update) = stream.next().await {
        let update = update.context("Geyser stream error")?;
        if let Some(UpdateOneof::Block(block)) = update.update_oneof {
            history.ingest_block(block.slot, block.accounts);
        }
    }

    anyhow::bail!("Geyser stream ended")
}

/// What an RPC response says about one account, reduced to something
/// comparable with an `AccountEvent`.
#[derive(Clone, Debug)]
struct Fingerprint {
    lamports: u64,
    owner: String,
    /// `None` when the response encoding carries no raw bytes (`jsonParsed`),
    /// in which case only lamports and owner are compared.
    data_hash: Option<[u8; 32]>,
}

impl Fingerprint {
    fn matches(&self, event: &AccountEvent, program: &str) -> bool {
        if self.lamports != event.lamports || self.owner != event.owner(program) {
            return false;
        }
        match self.data_hash {
            Some(hash) => hash == event.data_hash,
            None => true,
        }
    }
}

/// Decodes the `data` field of a `UiAccount` back to raw bytes. Returns `None`
/// for `jsonParsed`, which has no byte representation to hash.
fn decode_account_data(data: &JsonValue) -> Option<Vec<u8>> {
    let arr = data.as_array()?;
    let raw = arr.first()?.as_str()?;
    match arr.get(1)?.as_str()? {
        "base64" => base64::engine::general_purpose::STANDARD.decode(raw).ok(),
        "base58" => bs58::decode(raw).into_vec().ok(),
        "base64+zstd" => {
            let compressed = base64::engine::general_purpose::STANDARD.decode(raw).ok()?;
            zstd::decode_all(std::io::Cursor::new(compressed)).ok()
        }
        _ => None,
    }
}

fn fingerprint(account: &JsonValue) -> Option<Fingerprint> {
    // The streaming comparison never keeps account bytes, so it supplies the
    // blake3 digest directly under `dataHash`.
    let precomputed = account
        .get("dataHash")
        .and_then(|h| h.as_str())
        .and_then(|hex| hex::decode(hex).ok())
        .and_then(|bytes| <[u8; 32]>::try_from(bytes.as_slice()).ok());

    Some(Fingerprint {
        lamports: account.get("lamports")?.as_u64()?,
        owner: account.get("owner")?.as_str()?.to_string(),
        data_hash: precomputed.or_else(|| {
            account
                .get("data")
                .and_then(decode_account_data)
                .map(|bytes| *blake3::hash(&bytes).as_bytes())
        }),
    })
}

/// Per-account verdict. The ordering here is the triage order: everything above
/// `Rpc1Lagging` is a cloudbreak bug, everything below is expected slot lag or
/// insufficient evidence.
#[derive(Debug)]
enum Verdict {
    /// Geyser delivered a write at a slot at or below cloudbreak's own context
    /// slot, and cloudbreak still serves the pre-write state. A real miss.
    MissedUpdate { applied_slot: u64, missed_slot: u64 },
    /// Cloudbreak returns an account whose latest Geyser write closed it.
    ClosedAccountLeak { closed_at: u64 },
    /// Cloudbreak returns an account whose latest Geyser write reassigned it
    /// away from the tracked program.
    OwnerChangeLeak { new_owner: String, at_slot: u64 },
    /// Cloudbreak's bytes match no write Geyser ever delivered.
    Rpc1Unknown,
    /// Agave's bytes match no write Geyser ever delivered.
    Rpc2Unknown,
    /// Neither side matches any observed write.
    BothUnknown,
    /// Cloudbreak is behind, but its context slot is below the newer write, so
    /// the lag is legitimate.
    Rpc1Lagging { rpc1_slot: u64, rpc2_slot: u64 },
    /// Agave is the one behind.
    Rpc2Lagging { rpc1_slot: u64, rpc2_slot: u64 },
    /// Geyser never saw this account, or saw it only before we subscribed.
    NoHistory,
}

impl Verdict {
    fn label(&self) -> &'static str {
        match self {
            Verdict::MissedUpdate { .. } => "MissedUpdate",
            Verdict::ClosedAccountLeak { .. } => "ClosedAccountLeak",
            Verdict::OwnerChangeLeak { .. } => "OwnerChangeLeak",
            Verdict::Rpc1Unknown => "Rpc1Unknown",
            Verdict::Rpc2Unknown => "Rpc2Unknown",
            Verdict::BothUnknown => "BothUnknown",
            Verdict::Rpc1Lagging { .. } => "Rpc1Lagging",
            Verdict::Rpc2Lagging { .. } => "Rpc2Lagging",
            Verdict::NoHistory => "NoHistory",
        }
    }

    /// True for the verdicts that indicate a cloudbreak defect rather than
    /// expected lag or missing evidence.
    fn is_suspect(&self) -> bool {
        matches!(
            self,
            Verdict::MissedUpdate { .. }
                | Verdict::ClosedAccountLeak { .. }
                | Verdict::OwnerChangeLeak { .. }
                | Verdict::Rpc1Unknown
                | Verdict::BothUnknown
        )
    }

    fn detail(&self) -> String {
        match self {
            Verdict::MissedUpdate {
                applied_slot,
                missed_slot,
            } => format!("serving slot {applied_slot}, never applied slot {missed_slot}"),
            Verdict::ClosedAccountLeak { closed_at } => format!("closed at slot {closed_at}"),
            Verdict::OwnerChangeLeak { new_owner, at_slot } => {
                format!("reassigned to {new_owner} at slot {at_slot}")
            }
            Verdict::Rpc1Unknown => "cloudbreak bytes match no observed write".to_string(),
            Verdict::Rpc2Unknown => "agave bytes match no observed write".to_string(),
            Verdict::BothUnknown => "neither side matches an observed write".to_string(),
            Verdict::Rpc1Lagging {
                rpc1_slot,
                rpc2_slot,
            }
            | Verdict::Rpc2Lagging {
                rpc1_slot,
                rpc2_slot,
            } => format!("rpc1 at slot {rpc1_slot}, rpc2 at slot {rpc2_slot}"),
            Verdict::NoHistory => "no Geyser events since subscribe".to_string(),
        }
    }
}

fn classify(
    diff: &AccountDiff,
    events: &[AccountEvent],
    program: &str,
    slot1: Option<u64>,
) -> Verdict {
    let Some(latest) = events.last() else {
        return Verdict::NoHistory;
    };

    match diff.kind {
        // Only cloudbreak returns it. The interesting case is an account whose
        // last write removed it from the program's set.
        DiffKind::OnlyRpc1 => {
            if latest.lamports == 0 {
                return Verdict::ClosedAccountLeak {
                    closed_at: latest.slot,
                };
            }
            if let Some(new_owner) = latest.owner.as_deref() {
                return Verdict::OwnerChangeLeak {
                    new_owner: new_owner.to_string(),
                    at_slot: latest.slot,
                };
            }
            Verdict::Rpc2Unknown
        }
        // Only agave returns it: cloudbreak never inserted a write it was sent.
        DiffKind::OnlyRpc2 => match slot1 {
            Some(slot1) if latest.slot <= slot1 => Verdict::MissedUpdate {
                applied_slot: 0,
                missed_slot: latest.slot,
            },
            _ => Verdict::Rpc1Lagging {
                rpc1_slot: slot1.unwrap_or(0),
                rpc2_slot: latest.slot,
            },
        },
        DiffKind::DataMismatch => {
            let fp1 = diff.account1.as_ref().and_then(fingerprint);
            let fp2 = diff.account2.as_ref().and_then(fingerprint);
            let m1 = fp1.and_then(|fp| events.iter().rposition(|e| fp.matches(e, program)));
            let m2 = fp2.and_then(|fp| events.iter().rposition(|e| fp.matches(e, program)));

            match (m1, m2) {
                (None, None) => Verdict::BothUnknown,
                (None, Some(_)) => Verdict::Rpc1Unknown,
                (Some(_), None) => Verdict::Rpc2Unknown,
                (Some(i1), Some(i2)) if i1 < i2 => {
                    // cloudbreak is behind. It is only a bug if a write it
                    // should already have applied sits at or below its own
                    // context slot.
                    let missed = slot1.and_then(|slot1| {
                        events[i1 + 1..=i2]
                            .iter()
                            .find(|e| e.slot <= slot1)
                            .map(|e| e.slot)
                    });
                    match missed {
                        Some(missed_slot) => Verdict::MissedUpdate {
                            applied_slot: events[i1].slot,
                            missed_slot,
                        },
                        None => Verdict::Rpc1Lagging {
                            rpc1_slot: events[i1].slot,
                            rpc2_slot: events[i2].slot,
                        },
                    }
                }
                (Some(i1), Some(i2)) => Verdict::Rpc2Lagging {
                    rpc1_slot: events[i1].slot,
                    rpc2_slot: events[i2].slot,
                },
            }
        }
    }
}

/// Caps concurrent `getBlock` calls across every in-flight request.
///
/// A mainnet block is several MB of JSON and parses to a multiple of that as a
/// `serde_json::Value`. `benchmark.max_in_flight` allows hundreds of concurrent
/// requests, so without this an unlucky run of mismatches would hold hundreds of
/// parsed blocks at once. The reduced result is a few hundred bytes, so only the
/// parse needs bounding.
static BLOCK_FETCH_LIMIT: LazyLock<tokio::sync::Semaphore> =
    LazyLock::new(|| tokio::sync::Semaphore::new(2));

/// One transaction's write to a pubkey, as read back from `getBlock`.
#[derive(Clone, Debug)]
struct BlockWrite {
    signature: String,
    post_balance: u64,
    failed: bool,
}

/// Fetches one block with `transactionDetails: "accounts"` and reduces it to
/// the writes touching `wanted`, discarding the rest of the (multi-MB) payload.
async fn fetch_block_writes(
    client: &reqwest::Client,
    rpc_url: &str,
    slot: u64,
    wanted: &HashSet<String>,
) -> Result<Option<HashMap<String, Vec<BlockWrite>>>> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [slot, {
            "encoding": "jsonParsed",
            "transactionDetails": "accounts",
            "rewards": false,
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0,
        }],
    });

    let response: JsonValue = {
        let _permit = BLOCK_FETCH_LIMIT
            .acquire()
            .await
            .expect("block fetch semaphore is never closed");
        client
            .post(rpc_url)
            .json(&request)
            .send()
            .await
            .context("getBlock request failed")?
            .json()
            .await
            .context("getBlock response parse failed")?
    };

    if let Some(error) = response.get("error") {
        anyhow::bail!("getBlock RPC error for slot {}: {}", slot, error);
    }
    Ok(parse_block_writes(&response, wanted))
}

/// Reduces a `transactionDetails: "accounts"` block to the writes touching
/// `wanted`. Returns `None` when the slot produced no block.
///
/// `accountKeys` is index-aligned with `postBalances` and already contains the
/// addresses loaded from lookup tables, so the balance is read positionally.
fn parse_block_writes(
    response: &JsonValue,
    wanted: &HashSet<String>,
) -> Option<HashMap<String, Vec<BlockWrite>>> {
    let transactions = response.get("result")?.get("transactions")?.as_array()?;

    let mut out: HashMap<String, Vec<BlockWrite>> = HashMap::new();
    for tx in transactions {
        let Some(keys) = tx
            .pointer("/transaction/accountKeys")
            .and_then(|k| k.as_array())
        else {
            continue;
        };
        let failed = tx.pointer("/meta/err").is_some_and(|e| !e.is_null());
        let post_balances = tx.pointer("/meta/postBalances").and_then(|b| b.as_array());
        let signature = tx
            .pointer("/transaction/signatures/0")
            .and_then(|s| s.as_str())
            .unwrap_or_default()
            .to_string();

        for (index, key) in keys.iter().enumerate() {
            let Some(pubkey) = key.get("pubkey").and_then(|p| p.as_str()) else {
                continue;
            };
            if !wanted.contains(pubkey) || key.get("writable") != Some(&JsonValue::Bool(true)) {
                continue;
            }
            let post_balance = post_balances
                .and_then(|b| b.get(index))
                .and_then(|b| b.as_u64())
                .unwrap_or_default();
            out.entry(pubkey.to_string()).or_default().push(BlockWrite {
                signature: signature.clone(),
                post_balance,
                failed,
            });
        }
    }

    Some(out)
}

/// Ledger cross-check for the Geyser events of one pubkey at one slot.
fn verify_slot(
    events: &[AccountEvent],
    slot: u64,
    block: Option<&HashMap<String, Vec<BlockWrite>>>,
    pubkey: &str,
) -> JsonValue {
    let Some(block) = block else {
        return serde_json::json!({ "slot": slot, "block": "not produced" });
    };

    let empty = Vec::new();
    let writes = block.get(pubkey).unwrap_or(&empty);
    let geyser_sigs: HashSet<String> = events
        .iter()
        .filter(|e| e.slot == slot)
        .filter_map(AccountEvent::signature)
        .collect();

    // Writes the ledger attributes to this account that Geyser never reported.
    let missing_in_geyser: Vec<&str> = writes
        .iter()
        .filter(|w| !w.failed && !geyser_sigs.contains(&w.signature))
        .map(|w| w.signature.as_str())
        .collect();

    // Last successful write in the block decides the end-of-slot balance.
    let final_post = writes
        .iter()
        .rev()
        .find(|w| !w.failed)
        .map(|w| w.post_balance);
    let geyser_lamports = events.iter().rfind(|e| e.slot == slot).map(|e| e.lamports);

    serde_json::json!({
        "slot": slot,
        "ledger_writers": writes.iter().map(|w| w.signature.as_str()).collect::<Vec<_>>(),
        "geyser_signatures": geyser_sigs.iter().collect::<Vec<_>>(),
        "writes_missing_from_geyser": missing_in_geyser,
        "ledger_post_balance": final_post,
        "geyser_lamports": geyser_lamports,
        "lamports_match": match (final_post, geyser_lamports) {
            (Some(a), Some(b)) => Some(a == b),
            _ => None,
        },
    })
}

/// Classifies every differing account against the Geyser history, optionally
/// confirming the decisive slots against the ledger with `getBlock`. Returns
/// the report embedded as `geyser_probe` in the saved mismatch file, or `None`
/// when the two responses do not differ per-account.
pub async fn check_differing_accounts(
    client: &reqwest::Client,
    response_comparison: &crate::response_comparison::ReponseComparison,
    config: &GeyserCheckConfig,
    history: &GeyserHistory,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
) -> Result<Option<JsonValue>> {
    let Some(diffs) = utils::diff_accounts(
        &response_comparison.response1,
        &response_comparison.response2,
    ) else {
        return Ok(None);
    };

    let slot1 = utils::get_slot(&response_comparison.response1);
    let slot2 = utils::get_slot(&response_comparison.response2);

    check_account_diffs(
        client, &diffs, config, history, slot1, slot2, &rpc1.name, &rpc2.name,
    )
    .await
}

/// Classifies an already-computed diff list. Callers that never materialize the
/// responses — the streaming gPA comparison — enter here.
#[allow(clippy::too_many_arguments)]
pub async fn check_account_diffs(
    client: &reqwest::Client,
    diffs: &[AccountDiff],
    config: &GeyserCheckConfig,
    history: &GeyserHistory,
    slot1: Option<u64>,
    slot2: Option<u64>,
    rpc1_name: &str,
    rpc2_name: &str,
) -> Result<Option<JsonValue>> {
    if diffs.is_empty() {
        return Ok(None);
    }

    let (last_slot, blocks_seen, tracked, multi_writes) = history.stats();

    tracing::info!(
        target: "bench_geyser_check",
        "🛰 {} differing accounts | {} slot: {:?} | {} slot: {:?} | geyser slot: {} ({} blocks, {} tracked pubkeys)",
        diffs.len(),
        rpc1_name,
        slot1,
        rpc2_name,
        slot2,
        last_slot,
        blocks_seen,
        tracked,
    );
    if multi_writes > 0 {
        tracing::warn!(
            target: "bench_geyser_check",
            "{} duplicate writes seen: a pubkey appeared more than once in one block, so \
             getProgramAccounts.sql picks between same-slot rows arbitrarily",
            multi_writes,
        );
    }

    // One shared set of pubkeys, so a block fetched for one account also
    // answers for every other differing account in the same slot.
    let wanted: HashSet<String> = diffs.iter().map(|d| d.pubkey.clone()).collect();
    let mut block_cache: HashMap<u64, Option<HashMap<String, Vec<BlockWrite>>>> = HashMap::new();
    let mut blocks_fetched = 0usize;

    let mut reports = Vec::with_capacity(diffs.len());

    for diff in diffs {
        let events = history.events(&diff.pubkey);
        let verdict = classify(diff, &events, &config.program, slot1);

        tracing::info!(
            target: "bench_geyser_check",
            "  {} | {:?} | {} | {} | {} geyser events",
            diff.pubkey,
            diff.kind,
            verdict.label(),
            verdict.detail(),
            events.len(),
        );

        let mut report = serde_json::json!({
            "pubkey": diff.pubkey,
            "diff_kind": format!("{:?}", diff.kind),
            "verdict": verdict.label(),
            "detail": verdict.detail(),
            "suspect": verdict.is_suspect(),
            "events": events.iter().rev().take(10).map(|e| e.to_json(&config.program)).collect::<Vec<_>>(),
        });

        // Only pay for getBlock on the accounts that look wrong, and only for
        // the slots the verdict actually turned on.
        if config.verify_with_get_block
            && verdict.is_suspect()
            && let Some(rpc_url) = config.rpc_url.as_deref()
        {
            let mut candidates: Vec<u64> = match verdict {
                Verdict::MissedUpdate {
                    applied_slot,
                    missed_slot,
                } => vec![missed_slot, applied_slot],
                _ => events.last().map(|e| vec![e.slot]).unwrap_or_default(),
            };
            candidates.retain(|s| *s != 0);
            candidates.sort_unstable();
            candidates.dedup();

            // Fetch first, verify second: a block pulled for one account also
            // answers for every other differing account in the same slot.
            for slot in &candidates {
                if block_cache.contains_key(slot) {
                    continue;
                }
                if blocks_fetched >= config.max_blocks_per_mismatch {
                    break;
                }
                blocks_fetched += 1;
                match fetch_block_writes(client, rpc_url, *slot, &wanted).await {
                    Ok(block) => {
                        block_cache.insert(*slot, block);
                    }
                    Err(e) => tracing::error!(
                        target: "bench_geyser_check",
                        "    └─ getBlock({}) failed: {:#}", slot, e,
                    ),
                }
            }

            let mut verifications = Vec::new();
            for slot in candidates {
                let Some(block) = block_cache.get(&slot) else {
                    continue;
                };
                let verification = verify_slot(&events, slot, block.as_ref(), &diff.pubkey);
                tracing::info!(target: "bench_geyser_check", "    └─ {}", verification);
                verifications.push(verification);
            }

            if let Some(obj) = report.as_object_mut() {
                obj.insert("get_block".to_string(), JsonValue::Array(verifications));
            }
        }

        reports.push(report);
    }

    Ok(Some(serde_json::json!({
        "program": config.program,
        "geyser_last_slot": last_slot,
        "geyser_first_slot": history.first_slot(),
        "geyser_blocks_seen": blocks_seen,
        "tracked_pubkeys": tracked,
        "duplicate_writes": multi_writes,
        "accounts": reports,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pads a short label into a 64-byte signature so tests can name them.
    fn sig(label: &str) -> [u8; 64] {
        let mut out = [0u8; 64];
        out[..label.len()].copy_from_slice(label.as_bytes());
        out
    }

    /// `postBalances` is index-aligned with `accountKeys`, and `accountKeys`
    /// already contains the addresses loaded from lookup tables. The whole
    /// ledger cross-check rests on that, so pin it with real mainnet shapes:
    /// the writable lookup-table key below sits at index 14 of a 25-key list.
    #[test]
    fn reads_balances_positionally_including_lookup_table_keys() {
        let response = serde_json::json!({
            "result": {
                "transactions": [
                    {
                        "version": 0,
                        "transaction": {
                            "signatures": ["bjubsiQYFNk9ufqfkbTpK1zQjEAPTV9pNCXGTu83kGDa"],
                            "accountKeys": [
                                { "pubkey": "Payer1111111111111111111111111111111111111", "signer": true, "writable": true, "source": "transaction" },
                                { "pubkey": "ReadOnly11111111111111111111111111111111111", "signer": false, "writable": false, "source": "transaction" },
                                { "pubkey": "DKyUs1xXMDy8Z11zNsLnUg3dy9HZf6hYZidB6WodcaGy", "signer": false, "writable": true, "source": "lookupTable" }
                            ]
                        },
                        "meta": { "err": null, "postBalances": [1, 2, 332331539811u64] }
                    },
                    {
                        "version": 0,
                        "transaction": {
                            "signatures": ["5u7rg59skueTQP7Y95UvNndL993PL6RC3qUHkt3jLBCa"],
                            "accountKeys": [
                                { "pubkey": "DKyUs1xXMDy8Z11zNsLnUg3dy9HZf6hYZidB6WodcaGy", "signer": false, "writable": true, "source": "transaction" }
                            ]
                        },
                        "meta": { "err": { "InstructionError": [4, { "Custom": 111 }] }, "postBalances": [999] }
                    }
                ]
            }
        });

        let wanted = HashSet::from([
            "DKyUs1xXMDy8Z11zNsLnUg3dy9HZf6hYZidB6WodcaGy".to_string(),
            "ReadOnly11111111111111111111111111111111111".to_string(),
        ]);
        let writes = parse_block_writes(&response, &wanted).expect("block parses");

        // The read-only appearance is not a write, so it must not show up.
        assert!(!writes.contains_key("ReadOnly11111111111111111111111111111111111"));

        let writes = &writes["DKyUs1xXMDy8Z11zNsLnUg3dy9HZf6hYZidB6WodcaGy"];
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].post_balance, 332331539811);
        assert!(!writes[0].failed);
        assert!(writes[1].failed);
    }

    /// A skipped slot returns `result: null`, which is the fork signal.
    #[test]
    fn missing_block_is_none() {
        let response = serde_json::json!({ "result": JsonValue::Null });
        assert!(parse_block_writes(&response, &HashSet::new()).is_none());
    }

    /// Only the writes Geyser never reported are listed, and failed
    /// transactions are not counted as missing.
    #[test]
    fn verify_slot_reports_writes_geyser_missed() {
        let pubkey = "DKyUs1xXMDy8Z11zNsLnUg3dy9HZf6hYZidB6WodcaGy";
        let events = vec![AccountEvent {
            slot: 100,
            write_version: 1,
            lamports: 500,
            data_hash: [0u8; 32],
            data_len: 0,
            owner: None,
            txn_signature: Some(sig("seen")),
        }];
        let block = HashMap::from([(
            pubkey.to_string(),
            vec![
                BlockWrite {
                    signature: bs58::encode(sig("seen")).into_string(),
                    post_balance: 400,
                    failed: false,
                },
                BlockWrite {
                    signature: "unseen".to_string(),
                    post_balance: 500,
                    failed: false,
                },
                BlockWrite {
                    signature: "failed".to_string(),
                    post_balance: 0,
                    failed: true,
                },
            ],
        )]);

        let report = verify_slot(&events, 100, Some(&block), pubkey);
        assert_eq!(
            report["writes_missing_from_geyser"],
            serde_json::json!(["unseen"])
        );
        // Last successful write decides the end-of-slot balance.
        assert_eq!(report["ledger_post_balance"], serde_json::json!(500));
        assert_eq!(report["lamports_match"], serde_json::json!(true));
    }

    /// A write at or below cloudbreak's own context slot that it never applied
    /// is a bug; the same shape with the write above its context slot is just
    /// lag. Telling those two apart is the point of the check.
    #[test]
    fn missed_update_is_distinguished_from_lag() {
        let event = |slot: u64, lamports: u64| AccountEvent {
            slot,
            write_version: 0,
            lamports,
            data_hash: *blake3::hash(b"").as_bytes(),
            data_len: 0,
            owner: None,
            txn_signature: None,
        };
        let events = vec![event(100, 10), event(200, 20)];
        let account = |lamports: u64| serde_json::json!({ "lamports": lamports, "owner": "CAMM", "data": ["", "base64"] });
        let diff = AccountDiff {
            pubkey: "P".to_string(),
            kind: DiffKind::DataMismatch,
            account1: Some(account(10)),
            account2: Some(account(20)),
        };

        // Both writes carry empty data, so lamports decide which one each side
        // matches: rpc1 is on the slot-100 write, rpc2 on the slot-200 one.
        assert!(matches!(
            classify(&diff, &events, "CAMM", Some(250)),
            Verdict::MissedUpdate {
                applied_slot: 100,
                missed_slot: 200
            }
        ));
        assert!(matches!(
            classify(&diff, &events, "CAMM", Some(150)),
            Verdict::Rpc1Lagging { .. }
        ));
    }

    /// An account cloudbreak still returns after Geyser closed it.
    #[test]
    fn closed_account_leak() {
        let events = vec![AccountEvent {
            slot: 300,
            write_version: 0,
            lamports: 0,
            data_hash: [0u8; 32],
            data_len: 0,
            owner: Some("11111111111111111111111111111111".into()),
            txn_signature: None,
        }];
        let diff = AccountDiff {
            pubkey: "P".to_string(),
            kind: DiffKind::OnlyRpc1,
            account1: Some(serde_json::json!({ "lamports": 5, "owner": "CAMM" })),
            account2: None,
        };
        assert!(matches!(
            classify(&diff, &events, "CAMM", Some(400)),
            Verdict::ClosedAccountLeak { closed_at: 300 }
        ));
    }

    /// A reassignment away from the program is one more write in the same ring,
    /// and the foreign owner is the only case that stores an owner at all.
    #[test]
    fn owner_change_leak() {
        let events = vec![AccountEvent {
            slot: 300,
            write_version: 0,
            lamports: 2039280,
            data_hash: [0u8; 32],
            data_len: 165,
            owner: Some("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into()),
            txn_signature: None,
        }];
        let diff = AccountDiff {
            pubkey: "P".to_string(),
            kind: DiffKind::OnlyRpc1,
            account1: Some(serde_json::json!({ "lamports": 2039280, "owner": "CAMM" })),
            account2: None,
        };
        let verdict = classify(&diff, &events, "CAMM", Some(400));
        assert!(matches!(
            verdict,
            Verdict::OwnerChangeLeak { at_slot: 300, .. }
        ));
        assert!(verdict.is_suspect());
        // A program-owned write reports the program, without storing it.
        assert_eq!(
            events[0].owner("CAMM"),
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        );
    }

    /// A pubkey written twice in one block is the case where
    /// `getProgramAccounts.sql` has to break a `(pubkey, slot)` tie it has no
    /// rule for. Both writes are kept, and the counter records the event.
    #[test]
    fn duplicate_writes_in_one_block_are_counted() {
        let account = |pubkey: u8, write_version: u64| {
            yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo {
                pubkey: vec![pubkey; 32],
                lamports: 1,
                owner: solana_pubkey::Pubkey::new_unique().to_bytes().to_vec(),
                executable: false,
                rent_epoch: 0,
                data: vec![],
                write_version,
                txn_signature: None,
            }
        };

        // Owner must match the tracked program for the account to be kept.
        let program = solana_pubkey::Pubkey::new_unique();
        let history = GeyserHistory::new(program.to_string(), 500);
        let owned = |pubkey: u8, write_version: u64| {
            let mut a = account(pubkey, write_version);
            a.owner = program.to_bytes().to_vec();
            a
        };

        history.ingest_block(10, vec![owned(1, 100), owned(2, 101), owned(1, 102)]);

        let (_, _, tracked, duplicates) = history.stats();
        assert_eq!(tracked, 2, "two distinct pubkeys");
        assert_eq!(duplicates, 1, "pubkey 1 was written twice in one block");

        // Both writes are retained, in arrival order, so the ring can still
        // show which version each endpoint is serving.
        let key = solana_pubkey::Pubkey::from([1u8; 32]).to_string();
        let events = history.events(&key);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].write_version, 100);
        assert_eq!(events[1].write_version, 102);
    }

    /// The ordinary case must not trip the counter.
    #[test]
    fn one_write_per_pubkey_is_not_a_duplicate() {
        let program = solana_pubkey::Pubkey::new_unique();
        let history = GeyserHistory::new(program.to_string(), 500);
        let owned = |pubkey: u8| yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo {
            pubkey: vec![pubkey; 32],
            lamports: 1,
            owner: program.to_bytes().to_vec(),
            executable: false,
            rent_epoch: 0,
            data: vec![],
            write_version: 1,
            txn_signature: None,
        };

        history.ingest_block(10, vec![owned(1), owned(2), owned(3)]);
        history.ingest_block(11, vec![owned(1), owned(2)]);

        let (_, blocks, tracked, duplicates) = history.stats();
        assert_eq!(blocks, 2);
        assert_eq!(tracked, 3);
        assert_eq!(
            duplicates, 0,
            "the same pubkey in different blocks is normal"
        );
    }

    /// No events at all must never be reported as a miss.
    #[test]
    fn no_history_is_not_a_finding() {
        let diff = AccountDiff {
            pubkey: "P".to_string(),
            kind: DiffKind::DataMismatch,
            account1: None,
            account2: None,
        };
        let verdict = classify(&diff, &[], "CAMM", Some(400));
        assert!(matches!(verdict, Verdict::NoHistory));
        assert!(!verdict.is_suspect());
    }
}
