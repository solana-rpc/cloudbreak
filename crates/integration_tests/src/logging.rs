// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Tracing setup for the `integration_tests` binary.
//!
//! The base filter is read from `RUST_LOG` (via `EnvFilter::from_default_env`)
//! and topped with a default `INFO` level + `sqlx=error`. Each subcommand is
//! free to append additional directives — most importantly, the `benchmark`
//! command derives a set of per-target directives from `[print_config]` so the
//! TOML acts as a higher-precedence layer on top of `RUST_LOG`.
//!
//! Precedence is determined by `EnvFilter`'s longest-target-prefix-wins rule:
//! a directive targeting `bench_compare::match` will override a less specific
//! `bench_compare=info` from `RUST_LOG` for events whose target is exactly
//! `bench_compare::match`.
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::Directive;

use crate::config::PrintConfig;

/// Initializes `tracing_subscriber` with the standard base filter
/// (`RUST_LOG` → `INFO` default → `sqlx=error`) plus any extra directives
/// derived from the active subcommand's TOML.
///
/// Must be called once, before any tracing events are emitted.
pub fn init_tracing(extra_directives: Vec<Directive>) {
    let mut filter = EnvFilter::from_default_env()
        .add_directive(tracing::Level::INFO.into())
        .add_directive("sqlx=error".parse().expect("hardcoded directive parses"));

    for d in extra_directives {
        filter = filter.add_directive(d);
    }

    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Returns the `EnvFilter` directives that translate a `[print_config]`
/// section into log-level overrides.
///
/// Only emits directives for flags that are `false`, since `false` means
/// "suppress this category". `true` flags are no-ops — they fall through to
/// the natural `INFO` level of the call site, which is visible at default
/// `RUST_LOG`.
pub fn directives_for_print_config(cfg: &PrintConfig) -> Vec<Directive> {
    let mut out = Vec::new();
    if !cfg.log_matches {
        out.push(parse("bench_compare::match=off"));
    }
    if !cfg.log_mismatches {
        out.push(parse("bench_compare::mismatch=off"));
    }
    if !cfg.log_rescues {
        out.push(parse("bench_compare::rescued=off"));
    }
    if !cfg.log_no_context_mismatches {
        out.push(parse("bench_compare::no_context_mismatch=off"));
    }
    if !cfg.log_compare_errors {
        out.push(parse("bench_compare::error=off"));
    }
    if !cfg.log_geyser_check {
        out.push(parse("bench_geyser_check=off"));
    }
    if !cfg.log_individual_requests {
        out.push(parse("bench_request=off"));
    }
    out
}

fn parse(s: &str) -> Directive {
    s.parse()
        .unwrap_or_else(|e| panic!("invalid hardcoded directive `{s}`: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All of the `<target>=off` strings we hand to `EnvFilter` at runtime
    /// must parse. The targets contain `match` (a Rust keyword as a path
    /// segment) and `::` separators — this guards against an `EnvFilter`
    /// grammar surprise.
    #[test]
    fn all_print_config_directives_parse() {
        let cfg = PrintConfig {
            min_request_bytes: 0,
            min_request_duration_ms: 0,
            min_request_account_count: 0,
            sample_every_secs: None,
            log_matches: false,
            log_mismatches: false,
            log_rescues: false,
            log_no_context_mismatches: false,
            log_compare_errors: false,
            log_geyser_check: false,
            log_individual_requests: false,
        };
        let directives = directives_for_print_config(&cfg);
        assert_eq!(directives.len(), 7);
    }
}
