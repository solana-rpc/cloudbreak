// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Command line of `processed-suite`, the front selection and the redacted inputs for the report.

use clap::Parser;
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

/// Long `--help` text. The subcommand variant repeats it, so its doc comment does not replace it.
pub const LONG_ABOUT: &str = "\
Correctness and speed suite for processed commitment on one cloudbreak API instance. same-node \
checks processed reads against Postgres once their slot is confirmed. cross-source compares \
cloudbreak with Agave references at equal context slots, with confirmed as a control group. \
convergence checks that a processed value holds once confirmed passes its slot. forks bursts \
requests around fork and dead-slot events from the Yellowstone watcher and checks each answer \
against its own chain. read-after-write measures how soon a block's lamports are visible on each \
source. latency runs a gentle open-loop load per source and reports freshness. metrics diffs the \
API's processed metrics. A front whose inputs are missing is skipped. The run stops at \
--duration or on Ctrl-C and still prints the report. The exit code is non-zero when a \
correctness rule fails.";

#[derive(Parser)]
#[command(name = "processed-suite", long_about = LONG_ABOUT)]
pub struct Args {
    /// Cloudbreak RPC URL under test, pinned to one API instance
    #[arg(long)]
    pub cloudbreak: String,
    /// Prometheus metrics URL of the same API instance, enables the metrics front
    #[arg(long)]
    pub cloudbreak_metrics: Option<String>,
    /// Agave reference as name=url, repeatable. The first one is the getBlocks oracle
    #[arg(long = "reference", value_parser = parse_reference)]
    pub references: Vec<(String, String)>,
    /// Yellowstone gRPC endpoint for the watcher
    #[arg(long)]
    pub grpc_endpoint: Option<String>,
    /// Yellowstone x-token, never printed
    #[arg(long, env = "PROCESSED_SUITE_X_TOKEN", hide_env_values = true)]
    pub grpc_x_token: Option<String>,
    /// Postgres URL of the database the API reads, enables the same-node front
    #[arg(long)]
    pub db_url: Option<String>,
    /// Extra keys, one base58 pubkey per line
    #[arg(long)]
    pub pubkeys_file: Option<PathBuf>,
    /// Comma list of same-node,cross-source,convergence,forks,read-after-write,latency,metrics
    #[arg(long, default_value = "all", value_parser = parse_fronts)]
    pub fronts: FrontSet,
    /// Run time of the continuous fronts
    #[arg(long, default_value = "10m", value_parser = humantime::parse_duration)]
    pub duration: Duration,
    /// Total requests per second per source, all fronts combined. The latency load uses half
    #[arg(long, default_value_t = 20.0)]
    pub rps: f64,
    /// In-flight request cap per source
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,
    /// Also write the report as JSON
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// Max processed cross-source mismatches on the canonical chain
    #[arg(long, default_value_t = 0)]
    pub max_cross_mismatch: u64,
    /// Max percentage of samples served on an abandoned slot
    #[arg(long, default_value_t = 5.0)]
    pub max_abandoned_served_pct: f64,
    /// Min percentage of processed reads above the source's confirmed slot
    #[arg(long, default_value_t = 50.0)]
    pub min_fresh_pct: f64,
    /// Fail when a cloudbreak latency row has a p99 above this
    #[arg(long)]
    pub max_p99_ms: Option<f64>,
    /// Burst length after a fork or dead slot event
    #[arg(long, default_value = "3s", value_parser = humantime::parse_duration)]
    pub fork_burst: Duration,
    /// Sample one in this many blocks for read-after-write
    #[arg(long, default_value_t = 5)]
    pub raw_every: u64,
    /// Keys per sampled block for read-after-write
    #[arg(long, default_value_t = 20)]
    pub raw_keys: usize,
    /// Keys per getMultipleAccounts sample
    #[arg(long, default_value_t = 20)]
    pub keys_per_sample: usize,
    /// Block tree retention by time
    #[arg(long, default_value = "10m", value_parser = humantime::parse_duration)]
    pub watcher_window: Duration,
    /// Block tree retention by slots below the highest slot seen
    #[arg(long, default_value_t = 1500)]
    pub watcher_max_slots: u64,
    /// HTTP timeout, and the same-node wait for confirmed to reach S, in seconds
    #[arg(long, default_value_t = 30)]
    pub timeout: u64,
}

impl Args {
    /// Inputs for the JSON report. URLs keep only scheme, host and port, and no secret is kept.
    pub fn inputs(&self) -> JsonValue {
        let redact = |url: &Option<String>| url.as_deref().map(redact_url);
        let references: Vec<JsonValue> = (self.references.iter())
            .map(|(name, url)| json!({"name": name, "url": redact_url(url)}))
            .collect();
        let fronts: Vec<&str> = self.fronts.0.iter().map(|f| f.name()).collect();
        json!({
            "cloudbreak": redact_url(&self.cloudbreak),
            "cloudbreak_metrics": redact(&self.cloudbreak_metrics),
            "references": references,
            "grpc_endpoint": redact(&self.grpc_endpoint),
            "grpc_x_token_set": self.grpc_x_token.is_some(),
            "db_url": redact(&self.db_url),
            "pubkeys_file": self.pubkeys_file,
            "fronts": fronts,
            "duration_secs": self.duration.as_secs_f64(),
            "rps": self.rps,
            "concurrency": self.concurrency,
            "fork_burst_secs": self.fork_burst.as_secs_f64(),
            "raw_every": self.raw_every,
            "raw_keys": self.raw_keys,
            "keys_per_sample": self.keys_per_sample,
            "watcher_window_secs": self.watcher_window.as_secs_f64(),
            "watcher_max_slots": self.watcher_max_slots,
            "timeout_secs": self.timeout,
        })
    }
}

/// Scheme, host and port of a URL. User info, path and query can all carry credentials.
pub fn redact_url(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").unwrap_or(("", url));
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    match scheme {
        "" => host.to_string(),
        scheme => format!("{scheme}://{host}"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Front {
    SameNode,
    CrossSource,
    Convergence,
    Forks,
    ReadAfterWrite,
    Latency,
    Metrics,
}

impl Front {
    pub const ALL: [Front; 7] = [
        Front::SameNode,
        Front::CrossSource,
        Front::Convergence,
        Front::Forks,
        Front::ReadAfterWrite,
        Front::Latency,
        Front::Metrics,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Front::SameNode => "same-node",
            Front::CrossSource => "cross-source",
            Front::Convergence => "convergence",
            Front::Forks => "forks",
            Front::ReadAfterWrite => "read-after-write",
            Front::Latency => "latency",
            Front::Metrics => "metrics",
        }
    }
}

#[derive(Clone)]
pub struct FrontSet(pub BTreeSet<Front>);

fn parse_fronts(text: &str) -> Result<FrontSet, String> {
    if text.trim() == "all" {
        return Ok(FrontSet(Front::ALL.into()));
    }
    let find = |name: &str| Front::ALL.into_iter().find(|f| f.name() == name.trim());
    let fronts = text
        .split(',')
        .map(|n| find(n).ok_or(format!("unknown front {n}")));
    fronts.collect::<Result<_, _>>().map(FrontSet)
}

fn parse_reference(text: &str) -> Result<(String, String), String> {
    let (name, url) = text.split_once('=').ok_or("expected name=url")?;
    Ok((name.to_string(), url.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_credentials_from_urls() {
        let db = "postgres://reader:secret@db.example:5432/cloudbreak?sslmode=require";
        assert_eq!(redact_url(db), "postgres://db.example:5432");
        let rpc = "https://mainnet.rpcpool.com/0000-token-0000?api-key=x";
        assert_eq!(redact_url(rpc), "https://mainnet.rpcpool.com");
        assert_eq!(redact_url("host:8899"), "host:8899");

        let args = Args::parse_from([
            "processed-suite",
            "--cloudbreak",
            "http://user:pw@cb:26722/abc",
            "--db-url",
            db,
            "--grpc-x-token",
            "tok",
        ]);
        let text = args.inputs().to_string();
        assert!(!text.contains("secret") && !text.contains("tok\"") && !text.contains("pw"));
        assert!(text.contains("\"grpc_x_token_set\":true"));
    }
}
