// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The HTTP contract between the API (`query_tracker_client`) and the query
//! tracker service. Kept here, in `core`, so both sides share exactly one set
//! of typed request/response structs instead of duplicating wire shapes.
//!
//! There is a single ingest endpoint — `POST /track` — that always takes a
//! batch. A single query is just a batch of length one.
//!
//! Values that do not define an index (commitment, encoding, filter *values*,
//! ...) are intentionally not modelled here beyond what [`TrackObservation`]
//! carries: the client sends a representative `config` (so the tracker can
//! reconstruct the `IndexIdentity`) plus the aggregated demand and a bounded
//! set of `value_fingerprints` used only for variety estimation.

use crate::modules::rpc_filter_type::RpcProgramAccountsConfig;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::collections::HashSet;

/// Path of the ingest endpoint on the tracker's main HTTP server.
pub const TRACK_PATH: &str = "/track";

/// Cap on distinct value fingerprints carried per identity per flush interval.
/// Bounds the client buffer and the `/track` payload; the tracker's HLL keeps
/// accumulating variety across intervals, so this only limits how much *new*
/// variety a single interval can contribute, not the total ever counted.
pub const MAX_FINGERPRINTS_PER_IDENTITY: usize = 256;

/// One aggregated observation for a single `IndexIdentity`, covering all the
/// requests the API saw for that identity during a flush interval.
///
/// This is both the wire type (`POST /track`) and the API client's in-memory
/// accumulator: the client keys a buffer by identity, folds each observed query
/// into the matching entry (bumping counters and inserting the value
/// fingerprint), and flushes the entries as-is. `new`/`merge` support that
/// accumulation; everything not part of the identity (commitment, encoding,
/// data slice, filter *values*) is intentionally collapsed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackObservation {
    /// Program id (base58). Combined with `config` this reconstructs the
    /// `IndexIdentity` on the tracker side.
    pub program: String,
    /// A representative request config for this identity. Only its
    /// index-defining shape (memcmp offsets/lengths, datasize) is used.
    #[serde(default)]
    pub config: Option<RpcProgramAccountsConfig>,
    /// Number of requests observed for this identity.
    pub count: u32,
    /// Sum of observed DB cost in microseconds across those requests. For
    /// failed/timed-out requests the client contributes the timeout budget as a
    /// cost estimate.
    #[serde(default)]
    pub total_cost_us: u64,
    /// How many of `count` failed or timed out (a demand signal for patterns
    /// that currently cannot be served without an index).
    #[serde(default)]
    pub failed_count: u32,
    /// Bounded set of distinct fingerprints of the memcmp *values* seen this
    /// interval (see `IndexIdentity::value_fingerprint`). Fed into the tracker's
    /// per-identity variety sketch; may be empty (e.g. datasize-only indexes).
    /// A `HashSet` so repeated values dedup in place and the
    /// [`MAX_FINGERPRINTS_PER_IDENTITY`] cap bounds *distinct* values.
    #[serde(default)]
    pub value_fingerprints: HashSet<u64>,
}

impl TrackObservation {
    /// A fresh, empty observation for `program`/`config`. Counters start at zero
    /// and are accumulated by the caller (the API client's `buffer_query`).
    pub fn new(program: Pubkey, config: Option<RpcProgramAccountsConfig>) -> Self {
        Self {
            program: program.to_string(),
            config,
            count: 0,
            total_cost_us: 0,
            failed_count: 0,
            value_fingerprints: HashSet::new(),
        }
    }

    /// Fold another observation for the same identity into this one: sum the
    /// counters (saturating) and union the fingerprints up to the cap. Used to
    /// re-merge a batch back into the buffer after a failed flush.
    pub fn merge(&mut self, other: TrackObservation) {
        self.count = self.count.saturating_add(other.count);
        self.total_cost_us = self.total_cost_us.saturating_add(other.total_cost_us);
        self.failed_count = self.failed_count.saturating_add(other.failed_count);
        for fp in other.value_fingerprints {
            if self.value_fingerprints.len() >= MAX_FINGERPRINTS_PER_IDENTITY {
                break;
            }
            self.value_fingerprints.insert(fp);
        }
    }
}

/// A batch of observations. `POST /track` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackBatch {
    pub observations: Vec<TrackObservation>,
}

/// `POST /track` response: how many observations were applied vs. skipped
/// (not indexable / unparsable program).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackResponse {
    pub accepted: usize,
    pub skipped: usize,
}
