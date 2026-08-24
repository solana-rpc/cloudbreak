// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Variety tracking — how many *distinct* filter values a single index serves.
//!
//! ## What part of the process this covers
//!
//! An [`IndexIdentity`](cloudbreak_core::modules::index_identity::IndexIdentity)
//! deliberately collapses everything that does not define an index: commitment,
//! encoding, data slice, and crucially the memcmp *values*. So one index answers
//! many different queries that share the same `(program, offsets/lengths,
//! datasize)` shape but pass different bytes. This module keeps a cheap estimate
//! of *how many distinct value-sets* an index is serving, so the tracker can
//! tell "one query hammered a million times" apart from "a million distinct
//! queries collapsing onto one index" — useful for prioritization and eviction.
//!
//! ## What variety is (and isn't) preserved
//!
//! We track the number of distinct **memcmp value fingerprints** per identity
//! (see `IndexIdentity::value_fingerprint`). We do **not** keep the values
//! themselves, nor per-value counts, nor variety across the non-indexing
//! dimensions (commitment/encoding/etc.) — those are aggregated away entirely.
//!
//! ## Implementation
//!
//! Keeping the exact set of values would be unbounded, so we use a HyperLogLog
//! sketch: a fixed-ish-size approximate distinct count. The estimator itself
//! (bias correction, sparse↔dense representation) is delegated to the
//! [`hyperloglogplus`] crate (HyperLogLog++); this module is only a thin,
//! documented wrapper that persists to / restores from the
//! `index_patterns.variety_hll` column.
//!
//! Inputs are already-hashed, uniformly distributed `u64` fingerprints, so we
//! feed their bits straight into the sketch via an identity hasher instead of
//! hashing again — see [`IdentityBuildHasher`].

use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use serde::{Deserialize, Serialize};
use std::hash::{BuildHasher, Hasher};

/// log2 of the number of registers. `P = 12` gives a standard error of
/// ~1.04/sqrt(2^P) ≈ 1.6%. HLL++ packs registers into 6 bits and uses a sparse
/// representation at low cardinality, so a low-variety index costs far less than
/// the dense worst case.
const PRECISION: u8 = 12;

/// Deterministic, zero-sized, serde-able [`BuildHasher`].
///
/// The fingerprints we insert are already blake3-derived, uniformly distributed
/// `u64`s (`IndexIdentity::value_fingerprint`), so there is nothing to gain from
/// hashing them again: this hasher just returns the `u64` it was given. It must
/// be deterministic (and serializable) because the sketch is stored in the DB
/// and reloaded across restarts — a randomly-seeded hasher would hash the same
/// value differently after a restart and silently inflate the estimate.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct IdentityBuildHasher;

impl BuildHasher for IdentityBuildHasher {
    type Hasher = IdentityHasher;
    fn build_hasher(&self) -> IdentityHasher {
        IdentityHasher(0)
    }
}

#[derive(Default)]
pub struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write_u64(&mut self, i: u64) {
        // `u64::hash()` calls exactly this, so our fingerprint passes through
        // untouched.
        self.0 = i;
    }
    fn write(&mut self, bytes: &[u8]) {
        // Fallback for any non-`u64` input; not exercised on our hot path.
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ b as u64;
        }
    }
}

/// Per-index distinct-value estimator. Thin wrapper over [`HyperLogLogPlus`].
pub struct VarietySketch {
    hll: HyperLogLogPlus<u64, IdentityBuildHasher>,
}

impl Default for VarietySketch {
    fn default() -> Self {
        Self::new()
    }
}

impl VarietySketch {
    pub fn new() -> Self {
        Self {
            hll: HyperLogLogPlus::new(PRECISION, IdentityBuildHasher)
                .expect("PRECISION is within the valid range"),
        }
    }

    /// Rebuild a sketch from its stored bytes. Returns a fresh empty sketch if
    /// the blob is absent or cannot be deserialized (e.g. a format change), so a
    /// corrupt/legacy blob degrades gracefully rather than erroring.
    pub fn from_bytes(bytes: Option<&[u8]>) -> Self {
        bytes
            .and_then(|b| serde_json::from_slice(b).ok())
            .map(|hll| Self { hll })
            .unwrap_or_default()
    }

    /// Serialized form for storage.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.hll).unwrap_or_default()
    }

    /// Fold one fingerprint into the sketch.
    pub fn insert(&mut self, hash: u64) {
        self.hll.insert(&hash);
    }

    /// Fold in every fingerprint yielded by `hashes`.
    pub fn insert_many(&mut self, hashes: impl IntoIterator<Item = u64>) {
        for h in hashes {
            self.hll.insert(&h);
        }
    }

    /// Approximate number of distinct fingerprints observed so far. Takes
    /// `&mut self` because the crate's `count()` may fold its sparse buffer.
    pub fn estimate(&mut self) -> u64 {
        self.hll.count().round() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_zero() {
        assert_eq!(VarietySketch::new().estimate(), 0);
    }

    #[test]
    fn counts_distinct_within_error() {
        // Feed 10_000 well-spread fingerprints; expect ~10k within a few %.
        let mut s = VarietySketch::new();
        for i in 0..10_000u64 {
            // splitmix64 to spread the bits like real blake3 output would.
            let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            s.insert(z);
        }
        let est = s.estimate();
        let err = (est as f64 - 10_000.0).abs() / 10_000.0;
        assert!(err < 0.10, "estimate {est} too far from 10000 (err {err})");
    }

    #[test]
    fn duplicates_do_not_inflate() {
        let mut s = VarietySketch::new();
        for _ in 0..5_000 {
            s.insert(0xDEAD_BEEF_CAFE_1234);
        }
        assert_eq!(s.estimate(), 1);
    }

    #[test]
    fn roundtrips_through_bytes() {
        let mut s = VarietySketch::new();
        s.insert_many([1, 2, 3, 4, 5]);
        let before = s.estimate();
        let mut restored = VarietySketch::from_bytes(Some(&s.to_bytes()));
        assert_eq!(before, restored.estimate());
        // Garbage / missing blob degrades to empty.
        assert_eq!(
            VarietySketch::from_bytes(Some(&[0xFF, 0x00, 0x11])).estimate(),
            0
        );
        assert_eq!(VarietySketch::from_bytes(None).estimate(), 0);
    }
}
