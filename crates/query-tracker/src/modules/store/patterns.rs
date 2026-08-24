// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `index_patterns` row type and (de)serialization helpers.
//!
//! [`PatternRow`] is the in-memory view of a row, decoupled from the raw SQL so
//! the rest of the tracker never touches column names or JSON encoding.

use crate::error::QueryTrackerError;
use cloudbreak_core::modules::index_identity::IndexIdentity;
use solana_pubkey::Pubkey;

/// Lifecycle status of a pattern. Stored as text for legibility in the DB.
pub mod status {
    /// Demand seen, not yet built (eligible for creation).
    pub const CANDIDATE: &str = "candidate";
    /// Physical index pair exists.
    pub const CREATED: &str = "created";
    /// Previously created, dropped by eviction (demand may resurrect it).
    pub const EVICTED: &str = "evicted";
    /// Dropped by the latency regression guard because the index made the
    /// pattern slower. Sticky: demand alone does not resurrect it — only fresh
    /// without-index evidence that the index would now help (see
    /// `Store::promote_recovered_rejections`).
    pub const REJECTED: &str = "rejected";
}

/// Latest verdict of the optional `EXPLAIN` sampling pass: which physical tables
/// the planner would actually use the index on. Stored as text for legibility
/// (like [`status`]); the column is `NULL` until the pass runs (or when
/// `explain-enabled` is off). See `crate::stats::explain`.
pub mod explain_state {
    /// Planner would not use the index on either table.
    pub const NONE: &str = "none";
    /// Planner would use it on `accounts` only.
    pub const ACCOUNTS: &str = "accounts_table";
    /// Planner would use it on `snapshot_accounts` only.
    pub const SNAPSHOT: &str = "snapshot_accounts_table";
    /// Planner would use it on both tables.
    pub const BOTH: &str = "both";
}

/// In-memory view of an `index_patterns` row (only the columns consumers need).
///
/// Each field maps 1:1 to a column defined in the `index_patterns` migration
/// (`crates/migration/src/m20260711_000000_create_index_patterns_table.rs`);
/// see that file for the full schema, including columns the tracker persists but
/// does not read back into this struct.
#[derive(Debug, Clone)]
pub struct PatternRow {
    /// Primary key: deterministic hash of the [`IndexIdentity`] this row
    /// represents (`program` + `offsets_lengths` + `datasize`).
    pub pattern_id: String,
    /// Raw 32-byte program (owner) pubkey the index filters on.
    pub program: Vec<u8>,
    /// Human-readable index label used in logs, metrics and debug endpoints.
    pub human_name: String,
    /// `memcmp` filters as `(offset, length)` pairs; the index columns.
    pub offsets_lengths: Vec<(u64, u64)>,
    /// Optional `dataSize` filter (account data length), if part of the identity.
    pub datasize: Option<i64>,
    /// One representative request (`RpcProgramAccountsConfig` JSON) that maps to
    /// this identity, captured on first sight and kept. Its real memcmp values
    /// let the `EXPLAIN` probe use a realistic constant instead of zeros, and it
    /// doubles as a human-readable example on the debug endpoints. `None` for
    /// legacy rows or requests that carried no config.
    pub example_request: Option<serde_json::Value>,
    /// Demand signal: cumulative API request count observed for this pattern.
    pub demand_count: i64,
    /// Snapshot of `demand_count` taken when the index was created; the baseline
    /// for measuring demand accrued *since* creation (comparable to `idx_scan`).
    pub demand_at_create: i64,
    /// Demand signal: cumulative query cost in microseconds (drives cost-aware
    /// prioritization).
    pub total_cost_us: i64,
    /// Demand signal: cumulative count of failed/timed-out queries for this
    /// pattern.
    pub failed_count: i64,
    /// Latency signal: cost (µs) accrued while the index existed, and the number
    /// of requests it covers. `cost_with_index_us / cost_with_index_count` is the
    /// average served-with-index latency.
    pub cost_with_index_us: i64,
    /// Requests counted into [`Self::cost_with_index_us`].
    pub cost_with_index_count: i64,
    /// Latency signal: cost (µs) accrued while no index existed, and the number
    /// of requests it covers. `cost_without_index_us / cost_without_index_count`
    /// is the average served-without-index latency (the baseline to beat).
    pub cost_without_index_us: i64,
    /// Requests counted into [`Self::cost_without_index_us`].
    pub cost_without_index_count: i64,
    /// HyperLogLog estimate of distinct filter *values* served (query variety).
    pub variety_estimate: i64,
    /// Lifecycle status: one of [`status::CANDIDATE`], [`status::CREATED`],
    /// [`status::EVICTED`], [`status::REJECTED`].
    pub status: String,
    /// Supply signal: last observed Postgres `idx_scan` for the physical index.
    pub last_idx_scan: i64,
    /// On-disk size of the physical index in bytes (supply-side sizing signal).
    pub index_bytes: i64,
    /// Last computed discrepancy verdict between demand and supply, if any.
    pub discrepancy_state: Option<String>,
    /// Last computed demand/supply divergence ratio backing `discrepancy_state`.
    pub discrepancy_ratio: Option<f64>,
    /// Latest `EXPLAIN` sampling verdict — which physical tables the planner
    /// would use this index on. One of the [`explain_state`] constants, or
    /// `None` until the (optional) explain pass has run for this pattern.
    pub explain_state: Option<String>,
    /// Unix epoch seconds at which the index was created (`created_at`), or
    /// `None` for patterns that have never been built (`candidate`, `evicted`,
    /// `rejected`). Used to order the `/debug/created` view by recency.
    pub created_at_epoch: Option<f64>,
}

impl PatternRow {
    /// Reconstruct the [`IndexIdentity`] this row represents.
    ///
    /// Returns an error if the row whose stored `program`bytes are not a valid pubkey
    pub fn identity(&self) -> Result<IndexIdentity, QueryTrackerError> {
        let program = Pubkey::try_from(self.program.as_slice()).map_err(|e| {
            QueryTrackerError::Internal(format!(
                "pattern {} ({}) has invalid program bytes ({} bytes): {e:?}",
                self.pattern_id,
                self.human_name,
                self.program.len(),
            ))
        })?;
        Ok(IndexIdentity::from_parts(
            program,
            self.offsets_lengths.clone(),
            self.datasize.map(|d| d as u64),
        ))
    }
}

/// Encode `(offset, length)` pairs as a JSON array of two-element arrays for the
/// `offsets_lengths` JSONB column.
pub fn offsets_to_json(offsets_lengths: &[(u64, u64)]) -> serde_json::Value {
    serde_json::Value::Array(
        offsets_lengths
            .iter()
            .map(|(o, l)| serde_json::json!([o, l]))
            .collect(),
    )
}

/// Decode the `offsets_lengths` JSONB column back into pairs. Malformed entries
/// are skipped rather than failing the whole read.
pub fn offsets_from_json(value: &serde_json::Value) -> Vec<(u64, u64)> {
    let Some(arr) = value.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|pair| {
            let p = pair.as_array()?;
            let o = p.first()?.as_u64()?;
            let l = p.get(1)?.as_u64()?;
            Some((o, l))
        })
        .collect()
}
