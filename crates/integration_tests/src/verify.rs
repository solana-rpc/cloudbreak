// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `verify` runs one method against one endpoint and writes the result
//! document the shared verification runner collects. It is a thin wrapper around
//! `benchmark`: the comparison is the assertion.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use serde_json::Value as JsonValue;

use crate::{
    benchmark::{self, RequestType},
    config::Config,
    verification::{self, RESULT_MARKER, Thresholds},
};

/// Policy, compiled in. Holds no endpoint.
const PROFILE: &str = include_str!("../verify-profile.toml");

/// Request sets, compiled in. A fixed set makes a gate run reproducible.
/// Separate from the benchmark fixtures, whose requests are deliberately heavy
/// for load testing and often return nothing or time out.
const GPA_FIXTURE: &str = include_str!("../gpa_verify_requests.json");
const GTABO_FIXTURE: &str = include_str!("../gtabo_verify_requests.json");

#[derive(Parser, Debug)]
#[command(name = "verify")]
#[command(about = "\
Run one method against an endpoint, compare every response against a reference \
endpoint, and write the verification result document.")]
pub struct Args {
    /// Method to exercise.
    #[arg(long, value_enum)]
    pub method: RequestType,

    /// The endpoint under test.
    #[arg(long)]
    pub endpoint: String,

    /// The endpoint treated as the source of truth.
    #[arg(long)]
    pub reference_endpoint: String,

    /// Where to write the result document.
    #[arg(long)]
    pub json_file: PathBuf,

    /// Read requests from this file instead of the built-in fixture.
    #[arg(long)]
    pub source_file: Option<PathBuf>,

    /// Read requests from VictoriaLogs instead of the built-in fixture.
    #[arg(long)]
    pub source_url: Option<String>,

    /// Override the built-in profile. For local use.
    #[arg(long)]
    pub profile: Option<PathBuf>,

    /// Name for the endpoint under test, in logs and in the result document.
    #[arg(long, default_value = "cloudbreak")]
    pub endpoint_name: String,
}

pub async fn run(args: &Args) -> Result<()> {
    let started = chrono::Utc::now();
    let start = std::time::Instant::now();

    let profile_text = match &args.profile {
        Some(path) => std::fs::read_to_string(path)
            .with_context(|| format!("failed to read profile {}", path.display()))?,
        None => PROFILE.to_string(),
    };
    let (config, thresholds) = build_config(&profile_text, args)?;

    // Provenance first, so the document names what it tested even if the run
    // then fails.
    let versions = fetch_versions(args).await;

    let outcome = benchmark::run_with_config(config, args.method).await?;

    let doc = verification::from_run(
        &outcome,
        &args.endpoint,
        versions,
        started,
        start.elapsed().as_millis() as u64,
        &thresholds,
    );

    doc.write(&args.json_file)?;
    // The job strips this line and its poststop task logs the stamped copy.
    println!("{RESULT_MARKER} {}", doc.to_json_line()?);

    std::process::exit(doc.exit_code());
}

/// Merges `[method.<name>]` over `[default]`, then adds the endpoints and the
/// request source from the command line.
fn build_config(profile_text: &str, args: &Args) -> Result<(Config, Thresholds)> {
    let profile: toml::Value =
        toml::from_str(profile_text).context("failed to parse the verify profile")?;

    let mut merged = profile
        .get("default")
        .cloned()
        .ok_or_else(|| anyhow!("the verify profile has no [default] table"))?;

    if let Some(over) = profile.get("method").and_then(|m| m.get(method_slug(args.method).as_str())) {
        merge(&mut merged, over);
    }

    let table = merged
        .as_table_mut()
        .ok_or_else(|| anyhow!("[default] is not a table"))?;

    // Thresholds belong to `verify`, not to the benchmark config.
    let thresholds = Thresholds {
        min_samples: take_u64(table, "min_samples")?.unwrap_or(100),
        error_rate_critical: take_f64(table, "error_rate_critical")?.unwrap_or(0.01),
    };

    // Everything left at the top level is `[benchmark]`.
    let mut root = toml::map::Map::new();
    let source = std::mem::take(
        table
            .remove("source")
            .unwrap_or_else(|| toml::Value::Table(toml::map::Map::new()))
            .as_table_mut()
            .ok_or_else(|| anyhow!("[source] is not a table"))?,
    );
    for key in ["comparison", "retry_in_place", "print_config"] {
        if let Some(v) = table.remove(key) {
            root.insert(key.to_string(), v);
        }
    }
    root.insert("benchmark".into(), toml::Value::Table(table.clone()));
    root.insert("rpc1".into(), endpoint_table(&args.endpoint, &args.endpoint_name));
    root.insert(
        "rpc2".into(),
        endpoint_table(&args.reference_endpoint, "reference"),
    );
    root.insert("source".into(), resolve_source(args, source)?);

    let config: Config = toml::Value::Table(root)
        .try_into()
        .context("the verify profile did not produce a usable config")?;
    Ok((config, thresholds))
}

/// `--source-url` wins, then `--source-file`, then the built-in fixture.
fn resolve_source(args: &Args, mut source: toml::map::Map<String, toml::Value>) -> Result<toml::Value> {
    if let Some(url) = &args.source_url {
        source.insert("type".into(), "victoria_logs".into());
        source.insert("url".into(), url.clone().into());
        source.entry("minutes".to_string()).or_insert(5.into());
        source.entry("limit".to_string()).or_insert(1000.into());
        source.entry("inject_context".to_string()).or_insert(true.into());
        return Ok(toml::Value::Table(source));
    }

    let path = match &args.source_file {
        Some(path) => path.clone(),
        None => write_fixture(args)?,
    };

    let mut table = toml::map::Map::new();
    table.insert("type".into(), "json_file".into());
    table.insert("path".into(), path.to_string_lossy().to_string().into());
    Ok(toml::Value::Table(table))
}

/// The fixture is compiled in, and `json_file` reads a path, so write it out
/// beside the result document, which is always writable.
fn write_fixture(args: &Args) -> Result<PathBuf> {
    let body = match args.method {
        RequestType::Gpa => GPA_FIXTURE,
        RequestType::Gtabo => GTABO_FIXTURE,
        other => bail!(
            "no built-in fixture for `{}`. Pass --source-file with a request set, or --source-url \
             to read from VictoriaLogs.",
            method_slug(other),
        ),
    };

    let dir = args
        .json_file
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create {}", dir.display()))?;

    let path = dir.join(format!("verify-fixture-{}.json", method_slug(args.method)));
    std::fs::write(&path, body)
        .with_context(|| format!("failed to write the fixture to {}", path.display()))?;
    Ok(path)
}

/// Reads `getVersion` from both endpoints. Provenance must never fail the run,
/// so an endpoint that does not answer is simply left out.
async fn fetch_versions(args: &Args) -> BTreeMap<String, String> {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(_) => return BTreeMap::new(),
    };

    let mut versions = BTreeMap::new();
    for (key, url) in [
        (args.endpoint_name.as_str(), &args.endpoint),
        ("reference", &args.reference_endpoint),
    ] {
        if let Some(version) = get_version(&client, url).await {
            versions.insert(key.to_string(), version);
        }
    }
    versions
}

async fn get_version(client: &reqwest::Client, url: &str) -> Option<String> {
    let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "getVersion"});
    let response: JsonValue = client.post(url).json(&body).send().await.ok()?.json().await.ok()?;
    Some(
        response
            .get("result")?
            .get("solana-core")?
            .as_str()?
            .to_string(),
    )
}

fn endpoint_table(url: &str, name: &str) -> toml::Value {
    let mut table = toml::map::Map::new();
    table.insert("url".into(), url.into());
    table.insert("name".into(), name.into());
    toml::Value::Table(table)
}

/// The name clap shows for the method, used as the profile's `[method.*]` key.
fn method_slug(method: RequestType) -> String {
    method
        .to_possible_value()
        .map(|v| v.get_name().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn take_u64(table: &mut toml::map::Map<String, toml::Value>, key: &str) -> Result<Option<u64>> {
    match table.remove(key) {
        None => Ok(None),
        Some(v) => v
            .as_integer()
            .and_then(|i| u64::try_from(i).ok())
            .map(Some)
            .ok_or_else(|| anyhow!("{key} must be a non-negative integer")),
    }
}

fn take_f64(table: &mut toml::map::Map<String, toml::Value>, key: &str) -> Result<Option<f64>> {
    match table.remove(key) {
        None => Ok(None),
        Some(v) => v
            .as_float()
            .or_else(|| v.as_integer().map(|i| i as f64))
            .map(Some)
            .ok_or_else(|| anyhow!("{key} must be a number")),
    }
}

/// Recursive table merge. A scalar in the override replaces the base value.
fn merge(base: &mut toml::Value, over: &toml::Value) {
    match (base, over) {
        (toml::Value::Table(base), toml::Value::Table(over)) => {
            for (key, value) in over {
                match base.get_mut(key) {
                    Some(existing) => merge(existing, value),
                    None => {
                        base.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base, over) => *base = over.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(method: RequestType) -> Args {
        Args {
            method,
            endpoint: "http://under-test:8899".into(),
            reference_endpoint: "http://reference:8899".into(),
            json_file: PathBuf::from("/tmp/acc372-test/result.json"),
            source_file: Some(PathBuf::from("/tmp/requests.json")),
            source_url: None,
            profile: None,
            endpoint_name: "cloudbreak".into(),
        }
    }

    #[test]
    fn the_shipped_profile_builds_a_config() {
        let (config, thresholds) = build_config(PROFILE, &args(RequestType::Gpa)).unwrap();
        assert_eq!(config.rpc1.url, "http://under-test:8899");
        assert_eq!(config.rpc2.as_ref().unwrap().url, "http://reference:8899");
        assert!(config.retry_in_place.enabled, "retry-in-place must be on");
        assert_eq!(
            config.retry_in_place.retry_after_ms,
            Some(200),
            "every request is re-fired at a fixed interval, not on mismatch"
        );
        assert!(config.comparison.as_ref().unwrap().enable_slot_compensation);
        assert_eq!(thresholds.min_samples, 50);
        assert_eq!(thresholds.error_rate_critical, 0.01);

        // A run below the floor always reports UNKNOWN, so the rate and the
        // duration have to produce more verdicts than that.
        let verdicts = config.benchmark.target_rps * config.benchmark.duration_secs as f64;
        assert!(
            verdicts > thresholds.min_samples as f64,
            "{} rps for {}s is {verdicts} verdicts, at or below the floor of {}",
            config.benchmark.target_rps,
            config.benchmark.duration_secs,
            thresholds.min_samples,
        );
    }

    #[test]
    fn the_profile_publishes_no_endpoint() {
        // The repo keeps internal endpoints out of git. Targets arrive as flags.
        for needle in ["http://", "https://", ".ts.net", ".rpcpool"] {
            assert!(
                !PROFILE.contains(needle),
                "the profile must not carry `{needle}`"
            );
        }
    }

    #[test]
    fn per_method_overrides_merge_over_default() {
        let (config, _) = build_config(PROFILE, &args(RequestType::SimulateTransaction)).unwrap();
        // The override only touches [source], so the defaults survive.
        assert_eq!(config.benchmark.target_rps, 3.0);
        assert!(config.retry_in_place.enabled);
    }

    #[test]
    fn source_url_wins_over_a_fixture() {
        let mut a = args(RequestType::Gpa);
        a.source_url = Some("http://logs.example/select/logsql/query".into());
        let (config, _) = build_config(PROFILE, &a).unwrap();
        assert!(matches!(
            config.source,
            crate::config::SourceConfig::VictoriaLogs { .. }
        ));
    }

    #[test]
    fn a_method_with_no_fixture_says_so() {
        let mut a = args(RequestType::GetBalance);
        a.source_file = None;
        let err = build_config(PROFILE, &a).unwrap_err().to_string();
        assert!(err.contains("no built-in fixture"), "{err}");
        assert!(err.contains("--source-url"), "{err}");
    }

    #[test]
    fn method_slugs_match_the_cli() {
        assert_eq!(method_slug(RequestType::Gpa), "gpa");
        assert_eq!(method_slug(RequestType::Gtabo), "gtabo");
        assert_eq!(
            method_slug(RequestType::SimulateTransaction),
            "simulate-transaction"
        );
    }

    #[test]
    fn merge_replaces_scalars_and_recurses_tables() {
        let mut base: toml::Value = toml::from_str("a = 1\n[t]\nx = 1\ny = 2").unwrap();
        let over: toml::Value = toml::from_str("a = 2\n[t]\ny = 9\nz = 3").unwrap();
        merge(&mut base, &over);
        assert_eq!(base["a"].as_integer(), Some(2));
        assert_eq!(base["t"]["x"].as_integer(), Some(1));
        assert_eq!(base["t"]["y"].as_integer(), Some(9));
        assert_eq!(base["t"]["z"].as_integer(), Some(3));
    }

    #[test]
    fn the_shipped_fixtures_parse_and_name_their_method() {
        for (body, method) in [(GPA_FIXTURE, "getProgramAccounts"), (GTABO_FIXTURE, "getTokenAccountsByOwner")] {
            let requests: Vec<JsonValue> = serde_json::from_str(body).unwrap();
            assert!(!requests.is_empty());
            for request in &requests {
                assert_eq!(request["method"].as_str(), Some(method));
            }
        }
    }
}
