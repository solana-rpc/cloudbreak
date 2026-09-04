// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use clap::Parser;
use serde::Deserialize;

use crate::benchmark::RequestType;

#[derive(Parser, Debug)]
pub struct BenchmarkArgs {
    /// Path to the TOML config file
    #[arg(short, long, default_value = "cloudbreak.integration_tests.toml")]
    pub config: String,
    /// Request type to benchmark
    #[arg(value_enum)]
    pub request_type: RequestType,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RpcEndpoint {
    pub url: String,
    pub name: String,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub rpc1: RpcEndpoint,
    pub rpc2: Option<RpcEndpoint>,
    pub benchmark: BenchmarkConfig,
    pub source: SourceConfig,
    #[serde(default)]
    pub comparison: Option<ComparisonConfig>,
    #[serde(default)]
    pub db_check: Option<DbCheckConfig>,
    #[serde(default)]
    pub geyser_check: Option<GeyserCheckConfig>,
    pub print_config: PrintConfig,
    #[serde(default)]
    pub retry_in_place: RetryInPlaceConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DbCheckConfig {
    pub db_url: String,
    /// RPC endpoint to query for last transaction signatures (typically the Agave validator)
    pub rpc_url: Option<String>,
    /// If true (and rpc_url is set), calls getSignaturesForAddress for each differing
    /// account to find the slot of its last transaction — the Agave source of truth.
    #[serde(default)]
    pub get_last_signature: bool,
}

/// Cross-checks mismatched accounts against the Yellowstone Geyser stream the
/// indexer consumes. The subscription mirrors the indexer's exactly (whole
/// blocks at Confirmed); only accounts owned by `program` are retained.
#[derive(Deserialize, Debug, Clone)]
pub struct GeyserCheckConfig {
    /// Must be the same Yellowstone endpoint the indexer behind rpc1 consumes,
    /// otherwise the comparison proves nothing.
    pub endpoint: String,
    #[serde(default)]
    pub x_token: Option<String>,
    /// Owner program whose accounts are tracked, base58.
    pub program: String,
    #[serde(default = "defaults::geyser_history_size")]
    pub history_size: usize,
    #[serde(default = "defaults::geyser_timeout_secs")]
    pub timeout_secs: u64,
    /// Agave RPC used for the `getBlock` ledger cross-check. Omit to classify
    /// from the Geyser history alone.
    #[serde(default)]
    pub rpc_url: Option<String>,
    #[serde(default)]
    pub verify_with_get_block: bool,
    /// Upper bound on `getBlock` calls per mismatch. Blocks are several MB, so
    /// this caps both bandwidth and the load on the oracle node.
    #[serde(default = "defaults::geyser_max_blocks_per_mismatch")]
    pub max_blocks_per_mismatch: usize,
}

#[derive(Deserialize, Debug)]
pub struct BenchmarkConfig {
    pub target_rps: f64,

    #[serde(default = "defaults::max_in_flight")]
    pub max_in_flight: usize,

    #[serde(default)]
    pub duration_secs: u64,

    #[serde(default = "defaults::timeout_secs")]
    pub timeout_secs: u64,

    #[serde(default)]
    pub start_on_first_request: bool,

    /// Optional bandwidth cap in Gbit/s, enforced against *actual received*
    /// rpc1 response bytes. Acts as an upper bound together with `target_rps`:
    /// if byte throughput hits this limit below `target_rps`, effective RPS is
    /// reduced so the average stays under the cap. Omit/0 to disable.
    #[serde(default)]
    pub target_gbits: Option<f64>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "json_file")]
    JsonFile { path: String },

    #[serde(rename = "victoria_logs")]
    VictoriaLogs {
        url: String,
        /// Time window in minutes. If omitted, no time constraint is applied.
        minutes: Option<u32>,
        window_seconds: Option<u32>,
        #[serde(default = "defaults::vlogs_limit")]
        limit: u32,
        min_request_size: Option<u64>,
        max_request_size: Option<u64>,
        encoding: Option<String>,
        /// Value for the `pool_dedicated` VictoriaLogs filter (e.g. `"liquid"`).
        /// When omitted, no `pool_dedicated` constraint is added to the query.
        #[serde(default)]
        pool_dedicated: Option<String>,
        /// If true, when a no-context mismatch is detected, re-sends the request
        /// with `withContext: true` injected and runs slot compensation before
        /// deciding whether it's a real mismatch.
        #[serde(default)]
        inject_context: bool,
        #[serde(default = "defaults::poll_interval_secs")]
        poll_interval_secs: u64,
        #[serde(default)]
        replay_once: bool,
    },

    #[serde(rename = "mismatch_dir")]
    MismatchDir {
        #[serde(default = "defaults::mismatch_output_dir")]
        path: String,
        /// If true, injects `withContext: true` into each request's params.
        /// Useful for GPA requests that were originally sent without context,
        /// so that slot compensation can work on the re-run.
        #[serde(default)]
        inject_context: bool,
    },
}

impl SourceConfig {
    /// Whether to retry no-context mismatches by injecting `withContext: true`
    /// and re-comparing with slot compensation.
    pub fn retry_with_context(&self) -> bool {
        match self {
            SourceConfig::VictoriaLogs { inject_context, .. } => *inject_context,
            _ => false,
        }
    }

    pub fn replay_once(&self) -> bool {
        match self {
            SourceConfig::VictoriaLogs { replay_once, .. } => *replay_once,
            _ => false,
        }
    }
}

/// The subset `compare-gpa-streaming` reads. Declared separately from `Config`
/// so a deployment does not have to carry `[benchmark]` and `[source]` sections
/// that the streaming comparison never looks at. A full `Config` file still
/// parses as one, since unknown sections are ignored.
#[derive(Deserialize, Debug)]
pub struct StreamingConfig {
    pub rpc1: RpcEndpoint,
    pub rpc2: Option<RpcEndpoint>,
    #[serde(default)]
    pub geyser_check: Option<GeyserCheckConfig>,
    #[serde(default)]
    pub print_config: PrintConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ComparisonConfig {
    /// Fraction of requests also sent to rpc2 for correctness checking (0.0 to 1.0)
    #[serde(default = "defaults::compare_ratio")]
    pub ratio: f64,

    #[serde(default = "defaults::mismatch_output_dir")]
    pub mismatch_output_dir: String,

    pub enable_slot_compensation: bool,

    #[serde(default = "defaults::slot_compensation_max_retries")]
    pub slot_compensation_max_retries: u32,

    #[serde(default = "defaults::slot_compensation_interval_ms")]
    pub slot_compensation_interval_ms: u64,

    #[serde(default = "defaults::save_mismatches")]
    pub save_mismatches: bool,

    /// When `true`, every saved `mismatch_*.json` / `rescued_*.json` file
    /// includes an `iterations: [...]` array on each block (original & retry)
    /// capturing every single rpc1+rpc2 send that happened during slot
    /// compensation (and the no-context retry, if it ran). Each entry has its
    /// own ISO-8601-with-ms `fired_at` timestamp, plus the per-endpoint
    /// response/slot/duration/size and a `phase` tag (`"initial"` /
    /// `"with_context_retry"`). The pre-existing top-level `responses` etc.
    /// fields are preserved (final picked pair); the iterations array is
    /// additive. Useful for ruling out slot-compensation as the cause of an
    /// apparent rescue or mismatch.
    #[serde(default)]
    pub save_compensation_iterations: bool,

    /// When `true` AND `[db_check]` is configured AND the benchmark request
    /// type is `getBalance`, every per-iteration capture additionally fires a
    /// SQL probe against the Cloudbreak DB *concurrently* with the rpc1+rpc2
    /// send (third arm of the `tokio::join!`), recording:
    ///   - the contents of the `slots` table (Processed / Confirmed /
    ///     Finalized rows), and
    ///   - the top-N newest `(slot, lamports)` rows for the queried pubkey
    ///     from both `accounts` and `snapshot_accounts`.
    ///
    /// Useful for confirming whether a `getBalance` mismatch is caused by an
    /// indexer slot-vs-account write race (i.e. `slots.finalized = N` but the
    /// `accounts` table lacks a row at `slot = N` for the queried pubkey).
    /// Requires `comparison.save_compensation_iterations = true` (the probe
    /// piggybacks on each iteration entry's JSON).
    #[serde(default)]
    pub save_db_probe_iterations: bool,
}

mod defaults {
    pub fn max_in_flight() -> usize {
        100
    }
    pub fn geyser_history_size() -> usize {
        500
    }
    pub fn geyser_timeout_secs() -> u64 {
        30
    }
    pub fn geyser_max_blocks_per_mismatch() -> usize {
        6
    }
    pub fn timeout_secs() -> u64 {
        30
    }
    pub fn vlogs_limit() -> u32 {
        1000
    }
    pub fn poll_interval_secs() -> u64 {
        60
    }
    pub fn compare_ratio() -> f64 {
        1.0
    }
    pub fn mismatch_output_dir() -> String {
        "crates/integration_tests/compare_responses_results".into()
    }
    pub fn slot_compensation_max_retries() -> u32 {
        50
    }
    pub fn slot_compensation_interval_ms() -> u64 {
        100
    }
    pub fn save_mismatches() -> bool {
        true
    }
}

/// Re-run a request once and only count it as a mismatch if the retry also
/// mismatches. The retry goes through the full comparison pipeline (rpc1+rpc2,
/// slot compensation, no-context retry) so a "rescue" is a real same-slot match,
/// not just a temporal coincidence.
///
/// Two modes:
/// - `enabled = true, retry_after_ms = None` — retry-on-mismatch only. The
///   extra request only fires if the original comparison failed.
/// - `enabled = true, retry_after_ms = Some(N)` — scheduled retry. Every
///   request is automatically re-fired exactly `N` ms after the original was
///   *sent* (not after the response arrived — for short `N` the retry will be
///   in flight before the original's response returns). The retry result is
///   then used to rescue any original mismatch.
///
/// ⚠️  Setting `retry_after_ms` effectively **doubles** the request volume on
/// both endpoints. Use a smaller `target_rps` accordingly.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct RetryInPlaceConfig {
    /// Master switch. When `false` the feature is fully off regardless of
    /// `retry_after_ms`.
    #[serde(default)]
    pub enabled: bool,

    /// When set, schedules a retry for every request fired at exactly
    /// `original_send_time + retry_after_ms`. Probes how much temporal
    /// inconsistency remains between two consecutive reads.
    #[serde(default)]
    pub retry_after_ms: Option<u64>,

    /// When `true`, every request whose verdict flipped from mismatch → match
    /// thanks to the retry (i.e. `recovered_by_retry`) is written as a
    /// `rescued_<program_id>_<timestamp>.json` file in
    /// `comparison.mismatch_output_dir`. The on-disk schema is identical to a
    /// mismatch save (both the `original` and `retry` blocks are included),
    /// so the file can be replayed via the `mismatch_dir` source. Independent
    /// of `comparison.save_mismatches`.
    #[serde(default)]
    pub save_rescued: bool,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct PrintConfig {
    pub min_request_bytes: u64,
    pub min_request_duration_ms: u64,
    pub min_request_account_count: u64,
    /// If set, every ~N seconds one full (request, response1, response2) tuple is
    /// dumped via the `bench_sample` tracing target so you can eyeball what's
    /// actually being compared during a run. Logging is throttled atomically
    /// across all concurrent request tasks. Only fires when comparison is enabled.
    #[serde(default)]
    pub sample_every_secs: Option<u64>,

    // ---- per-event log toggles (all default false / opt-in) ----
    //
    // Each flag controls whether a specific class of `bench_compare` /
    // `bench_request` event is allowed to reach the tracing subscriber. When
    // `false`, an `<target>=off` directive is injected into the `EnvFilter`
    // *after* the directives parsed from `RUST_LOG`, so the TOML flag wins
    // (longer target prefix → more specific match in `EnvFilter`).
    //
    // When `true`, no directive is added and the event fires at its natural
    // `INFO` level — visible at default `RUST_LOG`, but a `RUST_LOG=info,
    // bench_compare=off` style override is still honored as a backstop.
    /// `bench_compare::match` — ✅ both endpoints matched on the first try.
    #[serde(default)]
    pub log_matches: bool,
    /// `bench_compare::mismatch` — ❌ post-compensation mismatch (covers both
    /// "plain" mismatches and the `retry-also-failed` case when
    /// `[retry_in_place]` is on).
    #[serde(default)]
    pub log_mismatches: bool,
    /// `bench_compare::rescued` — ✅ `[retry_in_place]` flipped the verdict
    /// (original mismatched, retry matched).
    #[serde(default)]
    pub log_rescues: bool,
    /// `bench_compare::no_context_mismatch` — ⚠️ mismatch where both responses
    /// lacked `result.context` so slot lag can't be verified or compensated for.
    #[serde(default)]
    pub log_no_context_mismatches: bool,
    /// `bench_geyser_check` — 🛰 per-account verdicts from the Geyser
    /// cross-check. Only fires when `[geyser_check]` is configured.
    #[serde(default)]
    pub log_geyser_check: bool,
    /// `bench_compare::error` — 💥 / 💥💥 one or both endpoints returned an
    /// error in the comparison.
    #[serde(default)]
    pub log_compare_errors: bool,
    /// `bench_request` — per-rpc1 line (also subject to the `min_*` thresholds).
    #[serde(default)]
    pub log_individual_requests: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The versioned example config must keep parsing. `cloudbreak.*.toml` is
    /// gitignored, so this is the only config CI can check.
    #[test]
    fn example_config_parses() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../example.cloudbreak.integration_tests.toml"
        );
        let content = std::fs::read_to_string(path).expect("example config is readable");
        toml::from_str::<Config>(&content).expect("example config parses");
    }

    /// A deployment only needs the sections the streaming comparison reads —
    /// no `[benchmark]`, no `[source]`.
    #[test]
    fn streaming_config_needs_only_its_own_sections() {
        let config: StreamingConfig = toml::from_str(
            r#"
            [rpc1]
            url = "http://cb:8899"
            name = "Cloudbreak"
            [rpc2]
            url = "http://agave:8899"
            name = "Agave"
            "#,
        )
        .expect("minimal streaming config parses");

        assert_eq!(config.rpc1.name, "Cloudbreak");
        assert_eq!(config.rpc2.expect("rpc2").name, "Agave");
        assert!(config.geyser_check.is_none());
    }

    /// A full benchmark config still works, so one file can drive both.
    #[test]
    fn streaming_config_ignores_the_benchmark_sections() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../example.cloudbreak.integration_tests.toml"
        );
        let content = std::fs::read_to_string(path).expect("example config is readable");
        toml::from_str::<StreamingConfig>(&content).expect("full config parses as streaming");
    }

    /// Every field but `endpoint` and `program` is optional.
    #[test]
    fn geyser_check_defaults() {
        let geyser: GeyserCheckConfig = toml::from_str(
            r#"
            endpoint = "http://localhost:10000"
            program = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
            "#,
        )
        .expect("minimal geyser_check parses");

        assert_eq!(geyser.history_size, 500);
        assert_eq!(geyser.timeout_secs, 30);
        assert_eq!(geyser.max_blocks_per_mismatch, 6);
        assert!(geyser.rpc_url.is_none());
        assert!(!geyser.verify_with_get_block);
    }
}
