// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Compares one `getProgramAccounts` request between two endpoints when the
//! response is too large to hold in memory.
//!
//! Both endpoints are streamed concurrently and reduced to per-account digests
//! (see [`crate::streaming_gpa`]). The two sides share a slot gate: whichever
//! reports its `result.context.slot` first parks the value, and if the other
//! disagrees both transfers abort in the header and the round restarts. That
//! is this command's form of slot compensation — retrying costs a few hundred
//! bytes instead of two multi-gigabyte transfers.
//!
//! Differing accounts are handed to the Geyser cross-check when
//! `[geyser_check]` is configured. Responses are never written to disk; the
//! report holds digests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::Value as JsonValue;

use crate::config::{GeyserCheckConfig, RpcEndpoint, StreamingConfig};
use crate::streaming_gpa::{self, AccountDigest, FetchError, GpaSnapshot, SlotGate};
use crate::utils::DiffKind;

#[derive(Parser, Debug)]
pub struct Args {
    /// Path to the TOML config file
    #[arg(short, long, default_value = "cloudbreak.integration_tests.toml")]
    pub config: String,
    /// JSON file holding the request. An array takes its first entry.
    #[arg(
        short,
        long,
        default_value = "crates/integration_tests/raydium_gpa.json"
    )]
    pub request: String,
    /// How many comparison rounds to run. 0 runs until interrupted.
    #[arg(long, default_value_t = 1)]
    pub rounds: u32,
    /// Attempts per round to land both endpoints on the same context slot.
    #[arg(long, default_value_t = 5)]
    pub slot_attempts: u32,
    /// Seconds to wait between slot-alignment attempts inside one round. An
    /// aborted attempt still costs the endpoints a full accounts scan, so
    /// retrying immediately hammers them.
    #[arg(long, default_value_t = 30)]
    pub attempt_delay_secs: u64,
    /// Base seconds to wait after a failed round. Doubles on each consecutive
    /// failure up to `--retry-delay-max-secs`, and resets after any round that
    /// completes.
    #[arg(long, default_value_t = 30)]
    pub retry_delay_secs: u64,
    /// Ceiling for the round-failure backoff.
    #[arg(long, default_value_t = 300)]
    pub retry_delay_max_secs: u64,
    /// Skip the CLMM liquidity invariant check on rpc1. It runs on every round
    /// where the slots matched, over the same accounts already streamed.
    #[arg(long)]
    pub no_clmm_check: bool,
    /// Directory for the per-round report.
    #[arg(
        long,
        default_value = "crates/integration_tests/compare_responses_results/streaming"
    )]
    pub output_dir: String,
}

pub async fn run(args: &Args) -> Result<()> {
    let config: StreamingConfig = toml::from_str(
        &std::fs::read_to_string(&args.config)
            .with_context(|| format!("Failed to read config `{}`", args.config))?,
    )
    .with_context(|| format!("Failed to parse config `{}`", args.config))?;

    crate::logging::init_tracing(crate::logging::directives_for_print_config(
        &config.print_config,
    ));

    let rpc2 = config
        .rpc2
        .as_ref()
        .context("[rpc2] is required — there is nothing to compare against without it")?;

    let request = load_request(&args.request)?;
    warn_on_missing_context(&request);

    // No global timeout: a multi-gigabyte body legitimately runs for minutes,
    // and the slot gate — not a clock — is what cancels a doomed transfer.
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .read_timeout(Duration::from_secs(120))
        .build()?;

    let geyser = config
        .geyser_check
        .as_ref()
        .map(crate::geyser_check::spawn_subscriber);

    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mut tally = Tally {
        rpc1_name: Some(config.rpc1.name.clone()),
        ..Tally::default()
    };
    let mut round = 0u32;
    let mut consecutive_failures = 0u32;

    loop {
        round += 1;
        let round_started = Instant::now();

        let outcome = compare_once(
            &client,
            &config.rpc1,
            rpc2,
            &request,
            args.slot_attempts,
            config.geyser_check.as_ref(),
            geyser.as_deref(),
            &args.output_dir,
            args.attempt_delay_secs,
            !args.no_clmm_check,
        )
        .await;

        // A slot disagreement still proves both endpoints are alive and
        // serving, so it does not escalate the backoff.
        if outcome.endpoints_responded() {
            consecutive_failures = 0;
        } else {
            consecutive_failures += 1;
        }
        if let RoundOutcome::EndpointError { message } = &outcome {
            tracing::error!(
                target: "bench_streaming",
                "Round {round} failed ({consecutive_failures} in a row): {message}",
            );
        }

        tally.record(round, &outcome, round_started.elapsed().as_secs_f64());
        tally.log();
        if let Err(e) = tally.write(&args.output_dir, &started_at) {
            tracing::error!(target: "bench_streaming", "Failed to write summary.json: {e:#}");
        }

        if args.rounds != 0 && round >= args.rounds {
            break;
        }

        // Back off only when an endpoint actually failed. Doubling per
        // consecutive failure keeps a dead endpoint from being polled all
        // night; any round that completes resets it.
        if consecutive_failures > 0 && args.retry_delay_secs > 0 {
            let delay = args
                .retry_delay_secs
                .saturating_mul(1u64 << (consecutive_failures - 1).min(16))
                .min(args.retry_delay_max_secs.max(args.retry_delay_secs));
            tracing::info!(
                target: "bench_streaming",
                "Waiting {delay}s before round {}", round + 1,
            );
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
    }

    Ok(())
}

fn load_request(path: &str) -> Result<JsonValue> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read request file `{path}`"))?;
    let value: JsonValue =
        serde_json::from_str(&content).with_context(|| format!("`{path}` is not valid JSON"))?;

    match value {
        JsonValue::Array(mut items) if !items.is_empty() => Ok(items.remove(0)),
        JsonValue::Array(_) => anyhow::bail!("`{path}` is an empty array"),
        object => Ok(object),
    }
}

/// Without `withContext: true` there is no slot to gate on, so a mismatch
/// cannot be told from slot lag and every round transfers in full.
fn warn_on_missing_context(request: &JsonValue) {
    let has_context = request
        .pointer("/params/1/withContext")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !has_context {
        tracing::warn!(
            target: "bench_streaming",
            "Request has no `withContext: true`: slot gating and compensation are both disabled, \
             and every round will transfer both responses in full",
        );
    }
}

/// How one round ended. The point of separating these is that they mean very
/// different things: a slot-alignment failure says nothing about correctness,
/// while a mismatch is the finding the whole exercise is looking for.
#[derive(Debug)]
enum RoundOutcome {
    /// Both endpoints agreed on every account at the same slot.
    Matched {
        slot: Option<u64>,
        accounts: usize,
        clmm: Option<JsonValue>,
    },
    /// Real differences. The report path is recorded so it can be found later.
    Mismatched {
        slot: Option<u64>,
        diffs: usize,
        report: Option<String>,
        clmm: Option<JsonValue>,
    },
    /// Ran out of attempts without both endpoints landing on the same slot.
    /// Not a correctness signal.
    SlotExhausted { attempts: u32 },
    /// One endpoint errored, timed out, or truncated its body.
    EndpointError { message: String },
}

impl RoundOutcome {
    fn label(&self) -> &'static str {
        match self {
            RoundOutcome::Matched { .. } => "matched",
            RoundOutcome::Mismatched { .. } => "mismatched",
            RoundOutcome::SlotExhausted { .. } => "slot_exhausted",
            RoundOutcome::EndpointError { .. } => "endpoint_error",
        }
    }

    /// Whether the endpoints answered. Used to reset the failure backoff:
    /// a slot disagreement still proves both are alive and serving.
    fn endpoints_responded(&self) -> bool {
        !matches!(self, RoundOutcome::EndpointError { .. })
    }
}

/// Running totals, rewritten to `summary.json` after every round so a run left
/// overnight can be read at a glance instead of grepped out of the log.
#[derive(Default)]
struct Tally {
    rounds: u32,
    matched: u32,
    mismatched: u32,
    slot_exhausted: u32,
    endpoint_error: u32,
    /// CLMM verdicts, split by which endpoint failed. That split is the point:
    /// one endpoint failing is an implementation bug, both failing points at
    /// Raydium or at the invariant itself.
    clmm_clean: u32,
    clmm_rpc1_only: u32,
    clmm_rpc2_only: u32,
    clmm_both: u32,
    history: Vec<JsonValue>,
    reports: Vec<String>,
    /// Needed to attribute a single-endpoint CLMM failure to the right side.
    rpc1_name: Option<String>,
}

impl Tally {
    fn record(&mut self, round: u32, outcome: &RoundOutcome, elapsed_secs: f64) {
        self.rounds += 1;
        match outcome {
            RoundOutcome::Matched { .. } => self.matched += 1,
            RoundOutcome::Mismatched { report, .. } => {
                self.mismatched += 1;
                if let Some(path) = report {
                    self.reports.push(path.clone());
                }
            }
            RoundOutcome::SlotExhausted { .. } => self.slot_exhausted += 1,
            RoundOutcome::EndpointError { .. } => self.endpoint_error += 1,
        }

        // The CLMM verdict is orthogonal to whether the endpoints agreed, so it
        // is counted separately. `clmm` is keyed by endpoint name, in the order
        // rpc1 then rpc2.
        let clmm = match outcome {
            RoundOutcome::Matched { clmm, .. } | RoundOutcome::Mismatched { clmm, .. } => {
                clmm.as_ref().and_then(|c| c.as_object())
            }
            _ => None,
        };
        if let Some(clmm) = clmm {
            let mut failed = Vec::new();
            for (endpoint, verdict) in clmm {
                let count = verdict.get("count").and_then(|c| c.as_u64()).unwrap_or(0);
                if count > 0 {
                    failed.push(endpoint.clone());
                    if let Some(path) = verdict.get("report").and_then(|p| p.as_str()) {
                        self.reports.push(path.to_string());
                    }
                }
            }
            match failed.len() {
                0 => self.clmm_clean += 1,
                n if n >= clmm.len() => self.clmm_both += 1,
                // Exactly one side failed. Which one decides whether this is a
                // cloudbreak bug or an oracle problem.
                _ => {
                    if failed.iter().any(|e| Some(e) == self.rpc1_name.as_ref()) {
                        self.clmm_rpc1_only += 1;
                    } else {
                        self.clmm_rpc2_only += 1;
                    }
                }
            }
        }

        let detail = match outcome {
            RoundOutcome::Matched {
                slot,
                accounts,
                clmm,
            } => serde_json::json!({ "slot": slot, "accounts": accounts, "clmm": clmm }),
            RoundOutcome::Mismatched {
                slot,
                diffs,
                report,
                clmm,
            } => serde_json::json!({
                "slot": slot, "diffs": diffs, "report": report, "clmm": clmm,
            }),
            RoundOutcome::SlotExhausted { attempts } => serde_json::json!({ "attempts": attempts }),
            RoundOutcome::EndpointError { message } => serde_json::json!({ "error": message }),
        };

        self.history.push(serde_json::json!({
            "round": round,
            "at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "outcome": outcome.label(),
            "elapsed_secs": (elapsed_secs * 10.0).round() / 10.0,
            "detail": detail,
        }));
    }

    fn log(&self) {
        tracing::info!(
            target: "bench_streaming",
            "📊 totals: {} rounds | {} matched | {} MISMATCHED | {} slot-exhausted | {} endpoint-error \
             || clmm: {} clean | {} rpc1-only | {} rpc2-only | {} both",
            self.rounds,
            self.matched,
            self.mismatched,
            self.slot_exhausted,
            self.endpoint_error,
            self.clmm_clean,
            self.clmm_rpc1_only,
            self.clmm_rpc2_only,
            self.clmm_both,
        );
    }

    fn write(&self, output_dir: &str, started: &str) -> Result<()> {
        let summary = serde_json::json!({
            "started_at": started,
            "updated_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "totals": {
                "rounds": self.rounds,
                "matched": self.matched,
                "mismatched": self.mismatched,
                "slot_exhausted": self.slot_exhausted,
                "endpoint_error": self.endpoint_error,
                "clmm_clean": self.clmm_clean,
                "clmm_rpc1_only": self.clmm_rpc1_only,
                "clmm_rpc2_only": self.clmm_rpc2_only,
                "clmm_both": self.clmm_both,
            },
            "mismatch_reports": self.reports,
            "rounds": self.history,
        });
        std::fs::create_dir_all(output_dir)?;
        std::fs::write(
            format!("{output_dir}/summary.json"),
            serde_json::to_string_pretty(&summary)?,
        )?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn compare_once(
    client: &reqwest::Client,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
    request: &JsonValue,
    slot_attempts: u32,
    geyser_config: Option<&GeyserCheckConfig>,
    geyser_history: Option<&crate::geyser_check::GeyserHistory>,
    output_dir: &str,
    attempt_delay_secs: u64,
    clmm_check: bool,
) -> RoundOutcome {
    let started = Instant::now();
    let mut attempt = 0u32;

    let (snap1, snap2) = loop {
        attempt += 1;
        let gate = Arc::new(SlotGate::new([rpc1.name.clone(), rpc2.name.clone()]));

        tracing::info!(
            target: "bench_streaming",
            "Attempt {attempt}/{slot_attempts}: streaming {} and {} concurrently",
            rpc1.name,
            rpc2.name,
        );

        let (r1, r2) = tokio::join!(
            streaming_gpa::fetch_snapshot(client, rpc1, request, gate.clone(), 0, clmm_check),
            streaming_gpa::fetch_snapshot(client, rpc2, request, gate.clone(), 1, clmm_check),
        );

        match (r1, r2) {
            (Ok(s1), Ok(s2)) => break (s1, s2),
            // A JSON-RPC error is permanent for this request, so report it
            // rather than retrying. It also aborts the peer, which would
            // otherwise surface as a slot mismatch and be retried forever.
            (Err(FetchError::Rpc(e)), _) => {
                return RoundOutcome::EndpointError {
                    message: format!("{} returned an error: {e}", rpc1.name),
                };
            }
            (_, Err(FetchError::Rpc(e))) => {
                return RoundOutcome::EndpointError {
                    message: format!("{} returned an error: {e}", rpc2.name),
                };
            }
            (Err(FetchError::SlotMismatch), _) | (_, Err(FetchError::SlotMismatch)) => {
                let (a, b) = gate.slots();
                // The gap says whether these are 1-slot straddles, which a
                // tolerance would absorb, or real divergence.
                let gap = match (a, b) {
                    (Some(a), Some(b)) => (a as i64 - b as i64).to_string(),
                    _ => "unknown".to_string(),
                };
                tracing::info!(
                    target: "bench_streaming",
                    "Attempt {attempt} aborted early on slot mismatch: {} = {:?}, {} = {:?} (gap {} slots) — retrying",
                    rpc1.name, a, rpc2.name, b, gap,
                );
                if attempt >= slot_attempts {
                    tracing::warn!(
                        target: "bench_streaming",
                        "Round gave up after {slot_attempts} attempts without matching slots. \
                         This says nothing about correctness — it is counted separately.",
                    );
                    return RoundOutcome::SlotExhausted { attempts: attempt };
                }
                // An aborted attempt still cost both endpoints a full accounts
                // scan, so pause before asking for another one.
                if attempt_delay_secs > 0 {
                    tracing::info!(
                        target: "bench_streaming",
                        "Waiting {attempt_delay_secs}s before attempt {}", attempt + 1,
                    );
                    tokio::time::sleep(Duration::from_secs(attempt_delay_secs)).await;
                }
                continue;
            }
            (Err(e), _) => {
                return RoundOutcome::EndpointError {
                    message: format!("{} failed: {e}", rpc1.name),
                };
            }
            (_, Err(e)) => {
                return RoundOutcome::EndpointError {
                    message: format!("{} failed: {e}", rpc2.name),
                };
            }
        }
    };

    report_snapshot(&snap1);
    report_snapshot(&snap2);

    // Both endpoints agreed on a slot, so each response is a coherent snapshot
    // — the precondition the invariant check needs. Running it on both is what
    // makes a violation attributable: a failure on one endpoint points at that
    // implementation, the same failure on both points at Raydium or at the
    // invariant itself.
    let clmm1 = run_clmm_check(&snap1, output_dir);
    let clmm2 = run_clmm_check(&snap2, output_dir);
    let clmm_summary = match (&clmm1, &clmm2) {
        (None, None) => None,
        _ => {
            let mut obj = serde_json::Map::new();
            for (snapshot, verdict) in [(&snap1, &clmm1), (&snap2, &clmm2)] {
                if let Some(verdict) = verdict {
                    obj.insert(snapshot.endpoint.clone(), verdict.to_json());
                }
            }
            Some(JsonValue::Object(obj))
        }
    };

    let diffs = diff_snapshots(&snap1, &snap2);
    let elapsed = started.elapsed();

    if diffs.is_empty() {
        tracing::info!(
            target: "bench_streaming",
            "✅ {} and {} agree on {} accounts at slot {:?} ({:.1}s)",
            snap1.endpoint,
            snap2.endpoint,
            snap1.accounts.len(),
            snap1.context_slot,
            elapsed.as_secs_f64(),
        );
        return RoundOutcome::Matched {
            slot: snap1.context_slot,
            accounts: snap1.accounts.len(),
            clmm: clmm_summary,
        };
    }

    tracing::info!(
        target: "bench_streaming",
        "❌ {} differing accounts between {} ({} accounts) and {} ({} accounts) at slot {:?} ({:.1}s)",
        diffs.len(),
        snap1.endpoint,
        snap1.accounts.len(),
        snap2.endpoint,
        snap2.accounts.len(),
        snap1.context_slot,
        elapsed.as_secs_f64(),
    );

    let geyser_probe = match (geyser_config, geyser_history) {
        (Some(cfg), Some(history)) => {
            let account_diffs: Vec<crate::utils::AccountDiff> =
                diffs.iter().map(DigestDiff::to_account_diff).collect();
            crate::geyser_check::check_account_diffs(
                client,
                &account_diffs,
                cfg,
                history,
                snap1.context_slot,
                snap2.context_slot,
                &snap1.endpoint,
                &snap2.endpoint,
            )
            .await
            .unwrap_or_else(|e| {
                tracing::error!(target: "bench_geyser_check", "Geyser check failed: {e:#}");
                None
            })
        }
        _ => None,
    };

    let report = match save_report(output_dir, request, &snap1, &snap2, &diffs, geyser_probe) {
        Ok(path) => Some(path),
        Err(e) => {
            tracing::error!(target: "bench_streaming", "Failed to write mismatch report: {e:#}");
            None
        }
    };

    RoundOutcome::Mismatched {
        slot: snap1.context_slot,
        diffs: diffs.len(),
        report,
        clmm: clmm_summary,
    }
}

/// One endpoint's CLMM verdict for a round.
struct ClmmVerdict {
    errors: usize,
    report: Option<String>,
}

impl ClmmVerdict {
    fn to_json(&self) -> JsonValue {
        serde_json::json!({
            "result": if self.errors == 0 { "clean" } else { "errors" },
            "count": self.errors,
            "report": self.report,
        })
    }
}

/// Runs the third-party invariant check over one endpoint's collected accounts.
/// Returns `None` when that endpoint was not collecting.
fn run_clmm_check(snapshot: &GpaSnapshot, output_dir: &str) -> Option<ClmmVerdict> {
    let collected = snapshot.clmm.as_ref()?;

    let started = Instant::now();
    let errors = clmm_liquidity_check::check::check_clmm_liquidity(collected);
    let elapsed = started.elapsed().as_secs_f64();

    if errors.is_empty() {
        tracing::info!(
            target: "bench_streaming",
            "🧮 CLMM invariants hold on {} ({} accounts, {:.1}s)",
            snapshot.endpoint, collected.account_count, elapsed,
        );
        return Some(ClmmVerdict {
            errors: 0,
            report: None,
        });
    }

    tracing::error!(
        target: "bench_streaming",
        "🧮 check clmm liquidity error, count {} on {} at slot {:?}",
        errors.len(), snapshot.endpoint, snapshot.context_slot,
    );
    for line in errors.iter().take(20) {
        tracing::error!(target: "bench_streaming", "   {line}");
    }

    let report = save_clmm_errors(output_dir, snapshot, &errors).unwrap_or_else(|e| {
        tracing::error!(target: "bench_streaming", "Failed to write CLMM errors: {e:#}");
        None
    });
    Some(ClmmVerdict {
        errors: errors.len(),
        report,
    })
}

/// Writes the CLMM invariant violations for one round. These lines are the
/// finding itself, and there can be many, so they get their own file.
fn save_clmm_errors(
    output_dir: &str,
    snapshot: &GpaSnapshot,
    errors: &[String],
) -> Result<Option<String>> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::fs::create_dir_all(output_dir)?;
    let endpoint = snapshot
        .endpoint
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
    let path = format!("{output_dir}/clmm_errors_{endpoint}_{timestamp}.json");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&serde_json::json!({
            "endpoint": snapshot.endpoint,
            "slot": snapshot.context_slot,
            "accounts": snapshot.accounts.len(),
            "error_count": errors.len(),
            "errors": errors,
        }))?,
    )?;
    tracing::error!(target: "bench_streaming", "CLMM errors written to {path}");
    Ok(Some(path))
}

fn report_snapshot(snapshot: &GpaSnapshot) {
    tracing::info!(
        target: "bench_streaming",
        "{}: {} accounts | slot {:?} | {:.2} GB | {:.1}s",
        snapshot.endpoint,
        snapshot.accounts.len(),
        snapshot.context_slot,
        snapshot.wire_bytes as f64 / 1e9,
        snapshot.duration_ms as f64 / 1000.0,
    );
}

/// One differing account, holding digests rather than response JSON.
pub struct DigestDiff {
    pub pubkey: String,
    pub kind: DiffKind,
    pub digest1: Option<AccountDigest>,
    pub digest2: Option<AccountDigest>,
}

impl DigestDiff {
    /// Renders the digests back into the `{lamports, owner, data}` shape the
    /// Geyser check fingerprints. `data` carries the precomputed blake3 hash,
    /// which is what the comparison actually uses.
    fn to_account_diff(&self) -> crate::utils::AccountDiff {
        crate::utils::AccountDiff {
            pubkey: self.pubkey.clone(),
            kind: self.kind,
            account1: self.digest1.as_ref().map(digest_to_json),
            account2: self.digest2.as_ref().map(digest_to_json),
        }
    }

    fn to_json(&self) -> JsonValue {
        serde_json::json!({
            "pubkey": self.pubkey,
            "diff_kind": format!("{:?}", self.kind),
            "rpc1": self.digest1.as_ref().map(digest_to_json),
            "rpc2": self.digest2.as_ref().map(digest_to_json),
        })
    }
}

fn digest_to_json(digest: &AccountDigest) -> JsonValue {
    serde_json::json!({
        "lamports": digest.lamports,
        "owner": digest.owner.as_ref(),
        "executable": digest.executable,
        "rentEpoch": digest.rent_epoch,
        "dataLen": digest.data_len,
        "dataHash": hex::encode(digest.data_hash),
    })
}

pub fn diff_snapshots(snap1: &GpaSnapshot, snap2: &GpaSnapshot) -> Vec<DigestDiff> {
    let mut diffs = Vec::new();

    for (pubkey, digest1) in &snap1.accounts {
        match snap2.accounts.get(pubkey) {
            Some(digest2) if digest1 != digest2 => diffs.push(DigestDiff {
                pubkey: pubkey.clone(),
                kind: DiffKind::DataMismatch,
                digest1: Some(digest1.clone()),
                digest2: Some(digest2.clone()),
            }),
            None => diffs.push(DigestDiff {
                pubkey: pubkey.clone(),
                kind: DiffKind::OnlyRpc1,
                digest1: Some(digest1.clone()),
                digest2: None,
            }),
            _ => {}
        }
    }
    for (pubkey, digest2) in &snap2.accounts {
        if !snap1.accounts.contains_key(pubkey) {
            diffs.push(DigestDiff {
                pubkey: pubkey.clone(),
                kind: DiffKind::OnlyRpc2,
                digest1: None,
                digest2: Some(digest2.clone()),
            });
        }
    }

    diffs
}

fn save_report(
    output_dir: &str,
    request: &JsonValue,
    snap1: &GpaSnapshot,
    snap2: &GpaSnapshot,
    diffs: &[DigestDiff],
    geyser_probe: Option<JsonValue>,
) -> Result<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for diff in diffs {
        *counts.entry(format!("{:?}", diff.kind)).or_default() += 1;
    }

    let report = serde_json::json!({
        "request": request,
        "endpoints": {
            snap1.endpoint.clone(): {
                "accounts": snap1.accounts.len(),
                "context_slot": snap1.context_slot,
                "wire_bytes": snap1.wire_bytes,
                "duration_ms": snap1.duration_ms,
            },
            snap2.endpoint.clone(): {
                "accounts": snap2.accounts.len(),
                "context_slot": snap2.context_slot,
                "wire_bytes": snap2.wire_bytes,
                "duration_ms": snap2.duration_ms,
            },
        },
        "diff_counts": counts,
        "diffs": diffs.iter().map(DigestDiff::to_json).collect::<Vec<_>>(),
        "geyser_probe": geyser_probe,
    });

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::fs::create_dir_all(output_dir)?;
    let path = format!("{output_dir}/streaming_mismatch_{timestamp}.json");
    std::fs::write(&path, serde_json::to_string_pretty(&report)?)?;

    tracing::info!(target: "bench_streaming", "Report written to {path}");
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tally_with(outcomes: Vec<RoundOutcome>) -> Tally {
        let mut tally = Tally {
            rpc1_name: Some("cb".to_string()),
            ..Tally::default()
        };
        for (i, outcome) in outcomes.iter().enumerate() {
            tally.record(i as u32 + 1, outcome, 1.0);
        }
        tally
    }

    /// The four outcomes must stay separated. A slot-alignment failure says
    /// nothing about correctness, so folding it in with real mismatches would
    /// make an overnight run unreadable.
    #[test]
    fn counts_each_outcome_separately() {
        let tally = tally_with(vec![
            RoundOutcome::Matched {
                slot: Some(1),
                accounts: 10,
                clmm: Some(serde_json::json!({
                    "cb": {"result": "clean", "count": 0},
                    "agave": {"result": "clean", "count": 0},
                })),
            },
            RoundOutcome::Matched {
                slot: Some(2),
                accounts: 10,
                clmm: Some(serde_json::json!({
                    "cb": {"result": "errors", "count": 4, "report": "/tmp/clmm_a.json"},
                    "agave": {"result": "clean", "count": 0},
                })),
            },
            RoundOutcome::SlotExhausted { attempts: 5 },
            RoundOutcome::Mismatched {
                slot: Some(3),
                diffs: 7,
                report: Some("/tmp/report_a.json".to_string()),
                clmm: None,
            },
            RoundOutcome::EndpointError {
                message: "connection refused".to_string(),
            },
        ]);

        assert_eq!(tally.rounds, 5);
        assert_eq!(tally.matched, 2);
        assert_eq!(tally.slot_exhausted, 1);
        assert_eq!(tally.mismatched, 1);
        assert_eq!(tally.endpoint_error, 1);
        // The CLMM verdict is orthogonal to endpoint agreement: round 2 matched
        // *and* violated an invariant, and both facts are recorded. Attribution
        // matters — only rpc1 failed, so it is an implementation problem there,
        // not a Raydium or invariant problem.
        assert_eq!(tally.clmm_clean, 1);
        assert_eq!(tally.clmm_rpc1_only, 1);
        assert_eq!(tally.clmm_rpc2_only, 0);
        assert_eq!(tally.clmm_both, 0);
        // Report paths are collected so the files are findable without grepping.
        assert_eq!(
            tally.reports,
            vec![
                "/tmp/clmm_a.json".to_string(),
                "/tmp/report_a.json".to_string()
            ]
        );
        assert_eq!(tally.history.len(), 5);
    }

    /// A failure on both endpoints is not an implementation bug — it points at
    /// Raydium or at the invariant. Keeping that separate from a one-sided
    /// failure is the reason the check runs on both.
    #[test]
    fn clmm_failures_are_attributed_to_an_endpoint() {
        let both = tally_with(vec![RoundOutcome::Matched {
            slot: Some(1),
            accounts: 10,
            clmm: Some(serde_json::json!({
                "cb": {"result": "errors", "count": 2},
                "agave": {"result": "errors", "count": 2},
            })),
        }]);
        assert_eq!(both.clmm_both, 1);
        assert_eq!(both.clmm_rpc1_only, 0);
        assert_eq!(both.clmm_rpc2_only, 0);

        let oracle = tally_with(vec![RoundOutcome::Matched {
            slot: Some(1),
            accounts: 10,
            clmm: Some(serde_json::json!({
                "cb": {"result": "clean", "count": 0},
                "agave": {"result": "errors", "count": 9},
            })),
        }]);
        assert_eq!(oracle.clmm_rpc2_only, 1);
        assert_eq!(oracle.clmm_rpc1_only, 0);
        assert_eq!(oracle.clmm_both, 0);
    }

    /// Only a genuine endpoint failure escalates the backoff. A slot
    /// disagreement proves both endpoints are alive and serving.
    #[test]
    fn only_endpoint_errors_count_as_unresponsive() {
        assert!(RoundOutcome::SlotExhausted { attempts: 5 }.endpoints_responded());
        assert!(
            RoundOutcome::Mismatched {
                slot: Some(1),
                diffs: 1,
                report: None,
                clmm: None,
            }
            .endpoints_responded()
        );
        assert!(
            !RoundOutcome::EndpointError {
                message: "boom".to_string()
            }
            .endpoints_responded()
        );
    }

    /// The summary must be readable without the log, and must survive being
    /// rewritten after every round.
    #[test]
    fn summary_is_written_and_reloadable() {
        let dir = std::env::temp_dir().join(format!("gpa_summary_{}", std::process::id()));
        let dir = dir.to_string_lossy().to_string();
        let tally = tally_with(vec![
            RoundOutcome::Matched {
                slot: Some(444),
                accounts: 2_292_875,
                clmm: Some(serde_json::json!({
                    "cb": {"result": "clean", "count": 0},
                    "agave": {"result": "clean", "count": 0},
                })),
            },
            RoundOutcome::Mismatched {
                slot: Some(445),
                diffs: 3,
                report: Some("/tmp/r.json".to_string()),
                clmm: None,
            },
        ]);
        tally.write(&dir, "2026-09-04T00:00:00Z").expect("writes");

        let written: JsonValue =
            serde_json::from_str(&std::fs::read_to_string(format!("{dir}/summary.json")).unwrap())
                .expect("valid json");
        assert_eq!(written["totals"]["rounds"], 2);
        assert_eq!(written["totals"]["matched"], 1);
        assert_eq!(written["totals"]["mismatched"], 1);
        assert_eq!(written["mismatch_reports"][0], "/tmp/r.json");
        assert_eq!(written["totals"]["clmm_clean"], 1);
        assert_eq!(
            written["rounds"][0]["detail"]["clmm"]["cb"]["result"],
            "clean"
        );
        assert_eq!(
            written["rounds"][0]["detail"]["clmm"]["agave"]["result"],
            "clean"
        );
        assert_eq!(written["rounds"][1]["outcome"], "mismatched");
        assert_eq!(written["rounds"][1]["detail"]["diffs"], 3);

        std::fs::remove_dir_all(&dir).ok();
    }
}
