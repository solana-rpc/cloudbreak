// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::{
    config::{
        BenchmarkArgs, BenchmarkConfig, ComparisonConfig, Config, DbCheckConfig, PrintConfig,
        RetryInPlaceConfig, RpcEndpoint,
    },
    db_check::{self, DbProbeCtx},
    response_comparison, sources, utils,
};
use anyhow::Result;
use clap::ValueEnum;
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, ValueEnum, PartialEq, Copy)]
pub enum RequestType {
    Gpa,
    Gtabo,
    Gtabd,
    GpaTokenOwner,
    GpaTokenMint,
    GetAccountInfo,
    GetMultipleAccounts,
    GetBalance,
    GetTokenAccountBalance,
}

pub async fn run(args: &BenchmarkArgs) -> Result<()> {
    let config_content = std::fs::read_to_string(&args.config)?;
    let config: Config = toml::from_str(&config_content)?;
    let request_type = args.request_type;

    let Config {
        benchmark,
        rpc1,
        rpc2,
        source,
        comparison,
        db_check,
        print_config,
        retry_in_place,
    } = config;

    // Install the tracing subscriber with TOML-derived per-target overrides
    // appended after `RUST_LOG`. This must happen before any tracing event
    // is emitted by the benchmark setup below.
    crate::logging::init_tracing(crate::logging::directives_for_print_config(&print_config));

    let BenchmarkConfig {
        target_rps,
        max_in_flight,
        duration_secs,
        timeout_secs,
    } = benchmark;

    let retry_with_context = source.retry_with_context();
    let requests_rx = sources::load_requests_from_source(&source, request_type).await?;

    let client = reqwest::Client::builder()
        // .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(timeout_secs))
        .build()?;

    // Optional per-iteration DB probe pool — only built when both the flag is
    // on AND the request type is `getBalance` (the only shape the probe knows
    // how to interpret). We validate `[db_check]` is configured up front so a
    // misconfiguration fails before the benchmark loop even starts.
    let db_probe_pool = if comparison
        .as_ref()
        .map(|c| c.save_db_probe_iterations)
        .unwrap_or(false)
    {
        if request_type != RequestType::GetBalance {
            anyhow::bail!(
                "comparison.save_db_probe_iterations is currently supported only for \
                 request_type = GetBalance (got {:?}). Disable the flag or run with \
                 --request-type get-balance.",
                request_type,
            );
        }
        Some(db_check::build_db_probe_pool(&db_check).await?)
    } else {
        None
    };

    // New: constant-rate spawner with result collection
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let semaphore = Arc::new(Semaphore::new(max_in_flight)); // e.g., 100
    let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / target_rps));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let dropped_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let dropped_count_spawner = dropped_count.clone();
    let rpc1_name = rpc1.name.clone();
    let retry_in_place_enabled = retry_in_place.enabled;

    // Spawner loop (runs for `duration` seconds)
    tokio::spawn(async move {
        let deadline = Instant::now() + Duration::from_secs(duration_secs);
        let mut idx: usize = 0;

        // Will make the requests loop infinitely until the deadline is reached
        while Instant::now() < deadline {
            ticker.tick().await;
            let permit = semaphore.clone().try_acquire_owned();

            let request = {
                let requests = requests_rx.borrow();
                if requests.is_empty() {
                    continue;
                }
                let req = requests[idx % requests.len()].clone();
                idx = idx.wrapping_add(1);

                req
            };

            match permit {
                Ok(permit) => {
                    let tx = tx.clone();
                    let client = client.clone();
                    let rpc1 = rpc1.clone();
                    let rpc2 = rpc2.clone();
                    let comparison = comparison.clone();
                    let db_check = db_check.clone();
                    let print_config = print_config.clone();
                    let retry_in_place = retry_in_place.clone();
                    let db_probe_pool = db_probe_pool.clone();

                    tokio::spawn(async move {
                        if let Err(e) = process_request(
                            &client,
                            &rpc1,
                            &rpc2,
                            &request,
                            &comparison,
                            &db_check,
                            tx,
                            request_type,
                            &print_config,
                            retry_with_context,
                            &retry_in_place,
                            db_probe_pool.as_ref(),
                        )
                        .await
                        {
                            tracing::error!("process_request Error: {}", e);
                        }

                        drop(permit);
                    });
                }
                Err(_) => {
                    dropped_count_spawner.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    });

    let mut stats = BenchStats::new(rpc1_name);
    while let Some(result) = rx.recv().await {
        stats.record(result);
    }

    let dropped = dropped_count.load(std::sync::atomic::Ordering::Relaxed);
    stats.print_summary(dropped, retry_in_place_enabled);

    if stats.total_mismatches > 0 {
        anyhow::bail!(
            "{} mismatch(es) detected (excluding {} no-context mismatches)",
            stats.total_mismatches,
            stats.total_no_context_mismatches,
        );
    }

    Ok(())
}

pub struct BenchResult {
    duration: u128,
    size: usize,
    encoding: String,
    rpc_name: String,
    correct_response: Option<bool>,
    slots_behind: Option<i64>,
    no_context_mismatch: bool,
    /// Set on the BenchResult that carries the final verdict whenever the
    /// original request mismatched but the retry rescued it. Used purely for
    /// surfacing the rescue count in the summary; doesn't affect the
    /// match/mismatch tallies (those reflect the final verdict).
    recovered_by_retry: bool,
}

/// Result of one full comparison pass (rpc1 + rpc2 + slot compensation +
/// no-context retry). Used identically for the original request and any
/// retry-in-place re-run.
struct ComparisonOutcome {
    response_comparison: response_comparison::ReponseComparison,
    compare_result: response_comparison::CompareResponsesResult,
    /// Total internal retries spent on slot compensation + no-context retry
    /// (for the per-request log line; orthogonal to retry-in-place).
    internal_retries: u32,
    /// When `comparison.save_compensation_iterations = true`, this holds one
    /// `IterationCapture` per rpc1+rpc2 send that happened during this pass
    /// (the seed send plus every slot-compensation retry, plus the second-pass
    /// seed and its retries when `inject_context` triggered). `None` when the
    /// flag is off — we don't waste memory on the per-iteration clones.
    iterations: Option<Vec<response_comparison::IterationCapture>>,
}

/// Runs one full comparison pass: send to rpc1+rpc2, run slot compensation,
/// optionally re-run with `withContext:true` injected if the initial result
/// was a no-context mismatch. Pure: doesn't touch shared state.
#[allow(clippy::too_many_arguments)]
async fn run_comparison(
    client: &reqwest::Client,
    rpc1: &RpcEndpoint,
    rpc2: &RpcEndpoint,
    request: &JsonValue,
    comparison_config: &ComparisonConfig,
    request_type: RequestType,
    encoding: &str,
    retry_with_context: bool,
    db_probe_ctx: Option<&DbProbeCtx>,
) -> Result<ComparisonOutcome> {
    let mut iterations: Option<Vec<response_comparison::IterationCapture>> = comparison_config
        .save_compensation_iterations
        .then(Vec::new);

    let initial_fired_at = SystemTime::now();
    let (r1, r2, initial_probe) =
        response_comparison::join_pair_with_probe(client, rpc1, rpc2, request, db_probe_ctx).await;
    let (json1, duration1) = r1?;
    let (json2, duration2) = r2?;

    if let Some(it) = iterations.as_mut() {
        it.push(response_comparison::IterationCapture {
            phase: "initial",
            fired_at: initial_fired_at,
            rpc1: Some((json1.clone(), duration1)),
            rpc2: Some((json2.clone(), duration2)),
            db_probe: initial_probe,
        });
    }

    let mut response_comparison = response_comparison::ReponseComparison {
        response1: json1,
        response2: json2,
        duration1,
        duration2,
    };

    let (mut compare_result, retries) = response_comparison::compare_with_slot_compensation(
        client,
        rpc1,
        rpc2,
        request,
        &mut response_comparison,
        comparison_config,
        encoding,
        request_type,
        iterations.as_mut(),
        "initial",
        db_probe_ctx,
    )
    .await?;

    let mut context_retry_retries = 0u32;
    if retry_with_context
        && compare_result.is_no_context_mismatch()
        && !utils::has_with_context(request)
    {
        let mut ctx_request = request.clone();
        utils::inject_with_context(&mut ctx_request);

        let ctx_fired_at = SystemTime::now();
        let (ctx_r1, ctx_r2, ctx_probe) = response_comparison::join_pair_with_probe(
            client,
            rpc1,
            rpc2,
            &ctx_request,
            db_probe_ctx,
        )
        .await;
        let (ctx_json1, ctx_dur1) = ctx_r1?;
        let (ctx_json2, ctx_dur2) = ctx_r2?;

        if let Some(it) = iterations.as_mut() {
            it.push(response_comparison::IterationCapture {
                phase: "with_context_retry",
                fired_at: ctx_fired_at,
                rpc1: Some((ctx_json1.clone(), ctx_dur1)),
                rpc2: Some((ctx_json2.clone(), ctx_dur2)),
                db_probe: ctx_probe,
            });
        }

        response_comparison = response_comparison::ReponseComparison {
            response1: ctx_json1,
            response2: ctx_json2,
            duration1: ctx_dur1,
            duration2: ctx_dur2,
        };

        let (ctx_result, ctx_retries) = response_comparison::compare_with_slot_compensation(
            client,
            rpc1,
            rpc2,
            &ctx_request,
            &mut response_comparison,
            comparison_config,
            encoding,
            request_type,
            iterations.as_mut(),
            "with_context_retry",
            db_probe_ctx,
        )
        .await?;

        compare_result = ctx_result;
        context_retry_retries = ctx_retries;
    }

    Ok(ComparisonOutcome {
        response_comparison,
        compare_result,
        internal_retries: retries + context_retry_retries,
        iterations,
    })
}

#[allow(clippy::too_many_arguments)]
async fn process_request(
    client: &reqwest::Client,
    rpc1: &RpcEndpoint,
    rpc2: &Option<RpcEndpoint>,
    request: &JsonValue,
    comparison_config: &Option<ComparisonConfig>,
    db_check_config: &Option<DbCheckConfig>,
    results_tx: tokio::sync::mpsc::UnboundedSender<BenchResult>,
    request_type: RequestType,
    print_config: &PrintConfig,
    retry_with_context: bool,
    retry_in_place: &RetryInPlaceConfig,
    db_probe_pool: Option<&Arc<sea_orm::DatabaseConnection>>,
) -> Result<()> {
    let encoding = utils::extract_encoding_from_request(request, request_type);
    let commitment = utils::extract_commitment_from_request(request, request_type);

    // Build the per-iteration DB probe context once per request. We pre-parse
    // the base58 pubkey here so every iteration can reuse the bytea form
    // without re-touching the request JSON. If parsing fails (malformed
    // request, etc.) we silently disable the probe for this request rather
    // than fail the whole comparison.
    let db_probe_ctx: Option<DbProbeCtx> = db_probe_pool.and_then(|pool| {
        let pubkey_b58 = request.get("params")?.as_array()?.first()?.as_str()?;
        let pubkey_bytes = bs58::decode(pubkey_b58).into_vec().ok()?;
        Some(DbProbeCtx {
            db: pool.clone(),
            pubkey_bytes,
        })
    });
    let db_probe_ctx_ref = db_probe_ctx.as_ref();

    // Comparison-disabled path keeps the legacy one-sided behavior (just rpc1).
    let Some(comparison_config) = comparison_config else {
        let (json, duration) = utils::send_rpc_request(client, rpc1, request).await?;
        utils::print_request_result(request, duration, &json, rpc1, &encoding, print_config);
        if let Err(e) = results_tx.send(BenchResult {
            duration,
            size: json.to_string().len(),
            encoding,
            rpc_name: rpc1.name.clone(),
            correct_response: None,
            slots_behind: None,
            no_context_mismatch: false,
            recovered_by_retry: false,
        }) {
            tracing::error!("results_tx.send Error: {}", e);
        }
        return Ok(());
    };

    let rpc2 = rpc2
        .as_ref()
        .expect("rpc2 is not present with the comparison config set");

    if rand::random::<f64>() > comparison_config.ratio {
        return Ok(());
    }

    // Pre-schedule the retry if `retry_after_ms` is configured. The retry must
    // fire exactly that many ms after the *original* was sent, regardless of
    // the original's latency — for small values the retry is in flight before
    // the original returns. We compute the fire-time relative to `now` (which
    // becomes the original's start time on the very next line).
    let scheduled_retry_handle = if retry_in_place.enabled
        && let Some(retry_after_ms) = retry_in_place.retry_after_ms
    {
        let fire_at = tokio::time::Instant::now() + Duration::from_millis(retry_after_ms);
        let client = client.clone();
        let rpc1 = rpc1.clone();
        let rpc2 = rpc2.clone();
        let request = request.clone();
        let comparison_config = comparison_config.clone();
        let encoding = encoding.clone();
        let db_probe_ctx = db_probe_ctx.clone();
        Some(tokio::spawn(async move {
            tokio::time::sleep_until(fire_at).await;
            run_comparison(
                &client,
                &rpc1,
                &rpc2,
                &request,
                &comparison_config,
                request_type,
                &encoding,
                retry_with_context,
                db_probe_ctx.as_ref(),
            )
            .await
        }))
    } else {
        None
    };

    let original_start = Instant::now();
    let original = run_comparison(
        client,
        rpc1,
        rpc2,
        request,
        comparison_config,
        request_type,
        &encoding,
        retry_with_context,
        db_probe_ctx_ref,
    )
    .await?;
    let original_elapsed_ms = original_start.elapsed().as_millis();

    utils::print_request_result(
        request,
        original.response_comparison.duration1,
        &original.response_comparison.response1,
        rpc1,
        &encoding,
        print_config,
    );

    // Latency-only BenchResults for the original (rpc1+rpc2). Verdict is
    // intentionally deferred until after we know the retry outcome (if any).
    if let Err(e) = results_tx.send(BenchResult {
        duration: original.response_comparison.duration1,
        size: original.response_comparison.response1.to_string().len(),
        encoding: encoding.clone(),
        rpc_name: rpc1.name.clone(),
        correct_response: None,
        slots_behind: None,
        no_context_mismatch: false,
        recovered_by_retry: false,
    }) {
        tracing::error!("results_tx.send Error: {}", e);
    }

    // Decide whether to actually wait for / fire a retry. Three cases:
    //  1. retry_in_place disabled → no retry at all.
    //  2. scheduled retry was pre-fired → always await it (its traffic is real
    //     and we want its result regardless of original's verdict).
    //  3. on-mismatch only, and the original matched → no retry needed.
    //  4. on-mismatch only, and the original mismatched → fire one synchronously.
    let retry: Option<ComparisonOutcome> = if !retry_in_place.enabled {
        None
    } else if let Some(handle) = scheduled_retry_handle {
        match handle.await {
            Ok(Ok(outcome)) => Some(outcome),
            Ok(Err(e)) => {
                tracing::error!("scheduled retry comparison failed: {}", e);
                None
            }
            Err(e) => {
                tracing::error!("scheduled retry task panicked: {}", e);
                None
            }
        }
    } else if !original.compare_result.matches {
        match run_comparison(
            client,
            rpc1,
            rpc2,
            request,
            comparison_config,
            request_type,
            &encoding,
            retry_with_context,
            db_probe_ctx_ref,
        )
        .await
        {
            Ok(outcome) => Some(outcome),
            Err(e) => {
                tracing::error!("on-mismatch retry comparison failed: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Latency-only BenchResults for the retry's rpc1+rpc2 traffic.
    if let Some(ref r) = retry {
        if let Err(e) = results_tx.send(BenchResult {
            duration: r.response_comparison.duration1,
            size: r.response_comparison.response1.to_string().len(),
            encoding: encoding.clone(),
            rpc_name: rpc1.name.clone(),
            correct_response: None,
            slots_behind: None,
            no_context_mismatch: false,
            recovered_by_retry: false,
        }) {
            tracing::error!("results_tx.send Error: {}", e);
        }
        if let Err(e) = results_tx.send(BenchResult {
            duration: r.response_comparison.duration2,
            size: r.response_comparison.response2.to_string().len(),
            encoding: encoding.clone(),
            rpc_name: rpc2.name.clone(),
            correct_response: None,
            slots_behind: None,
            no_context_mismatch: false,
            recovered_by_retry: false,
        }) {
            tracing::error!("results_tx.send Error: {}", e);
        }
    }

    let original_matched = original.compare_result.matches;
    let retry_matched = retry.as_ref().map(|r| r.compare_result.matches);
    let final_matched = original_matched || retry_matched == Some(true);
    let recovered_by_retry = !original_matched && retry_matched == Some(true);

    // For final-verdict reporting (mismatch save / db_check / sample logging)
    // we use the retry's response_comparison when it exists, since it is the
    // most recent picture of the discrepancy. For matched outcomes we use the
    // original — there's nothing interesting in the retry case to surface.
    let verdict_outcome = if final_matched {
        &original
    } else {
        retry.as_ref().unwrap_or(&original)
    };

    if let Err(e) = results_tx.send(BenchResult {
        duration: verdict_outcome.response_comparison.duration2,
        size: verdict_outcome
            .response_comparison
            .response2
            .to_string()
            .len(),
        encoding: encoding.clone(),
        rpc_name: rpc2.name.clone(),
        correct_response: Some(final_matched),
        slots_behind: verdict_outcome.compare_result.context_matches.slots_behind,
        no_context_mismatch: verdict_outcome.compare_result.is_no_context_mismatch(),
        recovered_by_retry,
    }) {
        tracing::error!("results_tx.send Error: {}", e);
    }

    utils::maybe_print_sample(
        print_config,
        request,
        rpc1,
        rpc2,
        &verdict_outcome.response_comparison,
        &verdict_outcome.compare_result,
    );

    if !final_matched
        && let Some(db_cfg) = db_check_config
        && let Err(e) = db_check::check_differing_accounts(
            client,
            &verdict_outcome.response_comparison,
            db_cfg,
            rpc1,
            rpc2,
        )
        .await
    {
        tracing::error!(target: "bench_db_check", "DB check failed: {}", e);
    }

    if !final_matched && comparison_config.save_mismatches {
        utils::save_responses_diff_to_file_with_retry(
            request,
            "mismatch",
            &rpc1.name,
            &comparison_config.mismatch_output_dir,
            rpc1,
            rpc2,
            &original.response_comparison,
            original.compare_result.context_matches.context_matches,
            original.iterations.as_deref(),
            retry.as_ref().map(|r| utils::SavedRetry {
                response_comparison: &r.response_comparison,
                context_matches: r.compare_result.context_matches.context_matches,
                fire_after_ms: retry_in_place.retry_after_ms,
                iterations: r.iterations.as_deref(),
            }),
        )?;
    }

    if recovered_by_retry && retry_in_place.save_rescued {
        utils::save_responses_diff_to_file_with_retry(
            request,
            "rescued",
            &rpc1.name,
            &comparison_config.mismatch_output_dir,
            rpc1,
            rpc2,
            &original.response_comparison,
            original.compare_result.context_matches.context_matches,
            original.iterations.as_deref(),
            retry.as_ref().map(|r| utils::SavedRetry {
                response_comparison: &r.response_comparison,
                context_matches: r.compare_result.context_matches.context_matches,
                fire_after_ms: retry_in_place.retry_after_ms,
                iterations: r.iterations.as_deref(),
            }),
        )?;
    }

    utils::print_compare_responses_result_with_retry(
        &original.compare_result,
        original.internal_retries,
        rpc1,
        rpc2,
        request,
        &original.response_comparison,
        retry.as_ref().map(|r| utils::RetryPrintInfo {
            response_comparison: &r.response_comparison,
            compare_result: &r.compare_result,
            internal_retries: r.internal_retries,
        }),
        original_elapsed_ms,
        recovered_by_retry,
        &encoding,
        &commitment,
        print_config,
    );

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BucketKey {
    rpc_name: String,
    size_category: String,
    encoding: String,
}

struct BenchStats {
    buckets: HashMap<BucketKey, Vec<u128>>,
    rpc1_name: String,
    total_requests: u64,
    total_mismatches: u64,
    total_no_context_mismatches: u64,
    total_matches: u64,
    /// Count of comparisons where the original mismatched but the retry-in-place
    /// rescued the request. Contributes to `total_matches` (i.e. the final
    /// verdict), not in addition to it.
    total_recovered_by_retry: u64,
    start_time: Instant,
    total_with_context: u64,
    total_compared: u64,
    slot_diffs: Vec<i64>,
}

impl BenchStats {
    fn new(rpc1_name: String) -> Self {
        Self {
            buckets: HashMap::new(),
            rpc1_name,
            total_requests: 0,
            total_mismatches: 0,
            total_no_context_mismatches: 0,
            total_matches: 0,
            total_recovered_by_retry: 0,
            start_time: Instant::now(),
            total_with_context: 0,
            total_compared: 0,
            slot_diffs: Vec::new(),
        }
    }

    fn record(&mut self, result: BenchResult) {
        self.total_requests += 1;

        match result.correct_response {
            Some(true) => {
                self.total_matches += 1;
                if result.recovered_by_retry {
                    self.total_recovered_by_retry += 1;
                }
            }
            Some(false) => {
                if result.no_context_mismatch {
                    self.total_no_context_mismatches += 1;
                } else {
                    self.total_mismatches += 1;
                }
            }
            None => {}
        }

        if result.correct_response.is_some() {
            self.total_compared += 1;
            if let Some(diff) = result.slots_behind {
                self.total_with_context += 1;
                if diff != 0 {
                    self.slot_diffs.push(diff);
                }
            }
        }

        let key = BucketKey {
            rpc_name: result.rpc_name,
            size_category: utils::bytes_bucket(result.size as u64).to_string(),
            encoding: result.encoding,
        };

        self.buckets.entry(key).or_default().push(result.duration);
    }

    fn print_summary(&mut self, dropped: u64, retry_in_place_enabled: bool) {
        let elapsed = self.start_time.elapsed();

        println!("\n{}", "=".repeat(90));
        println!("BENCHMARK SUMMARY");
        println!("{}", "=".repeat(90));
        println!(
            "Duration: {:.1}s | Total requests: {} | Effective RPS: {:.1}",
            elapsed.as_secs_f64(),
            self.total_requests,
            self.total_requests as f64 / elapsed.as_secs_f64(),
        );

        if dropped > 0 {
            println!(
                "Dropped requests (backpressure): {} ({:.1}% of attempted)",
                dropped,
                dropped as f64 / (self.total_requests + dropped) as f64 * 100.0,
            );
        }

        if self.total_matches + self.total_mismatches + self.total_no_context_mismatches > 0 {
            println!(
                "Comparisons: {} matches, {} mismatches, {} no-context mismatches (possible slot lag)",
                self.total_matches, self.total_mismatches, self.total_no_context_mismatches,
            );
            if self.total_recovered_by_retry > 0 {
                let denom = self.total_matches + self.total_mismatches;
                let pct = if denom > 0 {
                    self.total_recovered_by_retry as f64 / denom as f64 * 100.0
                } else {
                    0.0
                };
                println!(
                    "Recovered by retry: {} (would have been mismatches without retry_in_place) ({:.2}% of compared)",
                    self.total_recovered_by_retry, pct,
                );
            }
        }

        #[derive(Clone)]
        struct BucketStats {
            count: usize,
            avg: f64,
            p50: u128,
            p90: u128,
            p99: u128,
        }

        let mut computed: HashMap<BucketKey, BucketStats> = HashMap::new();
        for (key, durations) in &mut self.buckets {
            durations.sort_unstable();
            let count = durations.len();
            computed.insert(
                key.clone(),
                BucketStats {
                    count,
                    avg: durations.iter().sum::<u128>() as f64 / count as f64,
                    p50: percentile(durations, 50.0),
                    p90: percentile(durations, 90.0),
                    p99: percentile(durations, 99.0),
                },
            );
        }

        let rpc_names: Vec<String> = {
            let mut names: Vec<_> = computed.keys().map(|k| k.rpc_name.clone()).collect();
            names.sort_by(|a, _b| {
                if *a == self.rpc1_name {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            });
            names.dedup();
            names
        };

        let category_keys: Vec<(String, String)> = {
            let mut cats: Vec<_> = computed
                .keys()
                .map(|k| (k.size_category.clone(), k.encoding.clone()))
                .collect();
            cats.sort_by(|a, b| {
                a.1.cmp(&b.1)
                    .then(size_category_ord(&a.0).cmp(&size_category_ord(&b.0)))
            });
            cats.dedup();
            cats
        };

        let has_comparison = rpc_names.len() == 2;
        let w = if has_comparison { 140 } else { 90 };

        if has_comparison {
            let name1 = &rpc_names[0];
            let name2 = &rpc_names[1];
            println!(
                "\n{:<15} {:<15}  {:>8} {:>8} {:>8} {:>8} {:>8}  {:>8} {:>8} {:>8} {:>8} {:>8}  {:>8}",
                "",
                "",
                &format!("-- {} ", name1),
                "",
                "",
                "",
                "",
                &format!("-- {} ", name2),
                "",
                "",
                "",
                "",
                "",
            );
            println!(
                "{:<15} {:<15}  {:>8} {:>8} {:>8} {:>8} {:>8}  {:>8} {:>8} {:>8} {:>8} {:>8}  {:>8}",
                "Size",
                "Encoding",
                "N",
                "Avg",
                "P50",
                "P90",
                "P99",
                "N",
                "Avg",
                "P50",
                "P90",
                "P99",
                "P50 diff"
            );
            println!("{}", "-".repeat(w));

            for (size, enc) in &category_keys {
                let key1 = BucketKey {
                    rpc_name: name1.clone(),
                    size_category: size.clone(),
                    encoding: enc.clone(),
                };
                let key2 = BucketKey {
                    rpc_name: name2.clone(),
                    size_category: size.clone(),
                    encoding: enc.clone(),
                };
                let s1 = computed.get(&key1);
                let s2 = computed.get(&key2);

                let fmt_col = |s: Option<&BucketStats>| -> String {
                    match s {
                        Some(st) => format!(
                            "{:>8} {:>8.1} {:>8} {:>8} {:>8}",
                            st.count, st.avg, st.p50, st.p90, st.p99
                        ),
                        None => format!("{:>8} {:>8} {:>8} {:>8} {:>8}", "-", "-", "-", "-", "-"),
                    }
                };

                let diff_str = match (s1, s2) {
                    (Some(a), Some(b)) if b.p50 > 0 => {
                        let pct = (a.p50 as f64 - b.p50 as f64) / b.p50 as f64 * 100.0;
                        let sign = if pct < 0.0 { "" } else { "+" };
                        format!("{sign}{pct:.0}%")
                    }
                    _ => "-".to_string(),
                };

                println!(
                    "{:<15} {:<15}  {}  {}  {:>8}",
                    size,
                    enc,
                    fmt_col(s1),
                    fmt_col(s2),
                    diff_str,
                );
            }
        } else {
            let name = &rpc_names[0];
            println!(
                "\n{:<15} {:<15} {:>6} {:>10} {:>8} {:>8} {:>8}",
                "Size", "Encoding", "Count", "Avg(ms)", "P50", "P90", "P99",
            );
            println!("{}", "-".repeat(w));

            for (size, enc) in &category_keys {
                let key = BucketKey {
                    rpc_name: name.clone(),
                    size_category: size.clone(),
                    encoding: enc.clone(),
                };
                if let Some(st) = computed.get(&key) {
                    println!(
                        "{:<15} {:<15} {:>6} {:>10.1} {:>8} {:>8} {:>8}",
                        size, enc, st.count, st.avg, st.p50, st.p90, st.p99,
                    );
                }
            }
        }

        println!("{}", "=".repeat(w));

        // Slot comparison data
        if self.total_compared > 0 {
            let pct_with_context =
                self.total_with_context as f64 / self.total_compared as f64 * 100.0;
            println!(
                "Context slot: {}/{} compared requests have context ({:.1}%)",
                self.total_with_context, self.total_compared, pct_with_context,
            );
            if !self.slot_diffs.is_empty() {
                self.slot_diffs.sort_unstable();
                let count = self.slot_diffs.len();
                let avg = self.slot_diffs.iter().sum::<i64>() as f64 / count as f64;
                let min = self.slot_diffs[0];
                let max = self.slot_diffs[count - 1];
                println!("\nSlot difference (rpc1 - rpc2, non-zero only):");
                println!(
                    "  (uses the 1st iteration slot value, the slot compensation doesn't affect the result) \n"
                );
                println!(
                    "  Count: {} | Avg: {:.1} | Min: {} | Max: {}",
                    count, avg, min, max,
                );
                // Histogram buckets
                println!("  Distribution:");
                let mut counts: Vec<(String, u64)> = Vec::new();

                // < -4
                let far_behind = self.slot_diffs.iter().filter(|&&d| d < -4).count() as u64;
                counts.push(("rpc1 behind by >4".to_string(), far_behind));

                // -4 through +4, one by one
                for i in (-4..=4).rev() {
                    let c = self.slot_diffs.iter().filter(|&&d| d == i).count() as u64;
                    let label = match i {
                        d if d < 0 => format!("rpc1 behind by  {}", d.abs()),
                        d => format!("rpc1 ahead by   {}", d),
                    };
                    counts.push((label, c));
                }

                // > +4
                let far_ahead = self.slot_diffs.iter().filter(|&&d| d > 4).count() as u64;
                counts.push(("rpc1 ahead by  >4".to_string(), far_ahead));

                for (label, c) in &counts {
                    if *c > 0 {
                        println!(
                            "    {:<20} {:>6} ({:.1}%)",
                            label,
                            c,
                            *c as f64 / count as f64 * 100.0
                        );
                    }
                }
            } else if self.total_with_context > 0 {
                println!("Slot difference: all compared requests had matching slots");
            }
        }

        if retry_in_place_enabled {
            println!(
                "\nNote (retry_in_place): in `\u{1f501} retry(..., +N slots)` annotations, \
                 `+N slots` is `retry_rpc1_slot - original_rpc1_slot` — the difference between \
                 the original and the retry's rpc1 `context.slot` after slot compensation. \
                 Positive means the retry observed a later slot."
            );
        }
    }
}

fn size_category_ord(cat: &str) -> u8 {
    match cat {
        "0-1KB" => 0,
        "1-10KB" => 1,
        "10-100KB" => 2,
        "100KB-1MB" => 3,
        "1MB-10MB" => 4,
        "10MB-50MB" => 5,
        "50MB-100MB" => 6,
        "100MB-200MB" => 7,
        "200MB-500MB" => 8,
        "500MB+" => 9,
        _ => 10,
    }
}

fn percentile(sorted: &[u128], pct: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
