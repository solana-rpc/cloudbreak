// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Index identity: the canonical unit the query tracker reasons about.
//!
//! A `getProgramAccounts` request carries many dimensions (commitment, encoding,
//! data slice, filter *values*, ...), but only a few of them determine which
//! database index would serve it: the program, the memcmp `(offset, length)`
//! columns, and an optional `datasize` predicate. [`IndexIdentity`] captures
//! exactly that subset so that:
//!
//! - demand is aggregated per index we would actually build (not per raw query),
//! - the API client and the tracker service agree on the same key, and
//! - index names are derived deterministically and can never exceed Postgres'
//!   identifier limit (see [`IndexIdentity::pattern_id`]).
//!
//! Everything not part of the identity (commitment, encoding, data slice,
//! `withContext`, and the memcmp *values*) is intentionally collapsed. The
//! distinct memcmp *values* served by one identity are tracked separately and
//! approximately (see the query tracker's `variety` module); the fingerprint for
//! that is produced by [`IndexIdentity::value_fingerprint`].

use crate::modules::rpc_filter_type::{RpcFilterType, RpcProgramAccountsConfig};
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;

/// SPL Token program — never auto-indexed (served by specialized token paths).
pub const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Postgres identifiers are limited to `NAMEDATALEN - 1 = 63` bytes; longer
/// names are silently truncated, which would let two distinct patterns collide.
/// We keep generated names well under this by using a fixed-width hash token.
pub const PG_MAX_IDENTIFIER_LEN: usize = 63;

/// Width (hex chars) of the hash token embedded in an index name. 16 hex chars
/// = 64 bits of the pattern hash, giving negligible collision probability for
/// the modest number of distinct index shapes we track.
const PATTERN_ID_HEX_LEN: usize = 16;

/// The index-defining subset of a GPA request's filters.
///
/// `offsets_lengths` are the memcmp `(offset, length)` columns (sorted and
/// deduplicated for a canonical form); `datasize` is the optional
/// `length(data) = N` partial-index predicate. Filter *values* are excluded on
/// purpose — they are what varies *across* queries that share one index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ParsedIndexFilter {
    pub offsets_lengths: Vec<(u64, u64)>,
    pub datasize: Option<u64>,
}

/// A logical auto-index identity: `program` + the index-defining filter shape.
///
/// This is the unit demand is aggregated by and the unit a physical index *pair*
/// (`accounts` + `snapshot_accounts`) is built for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexIdentity {
    pub program: Pubkey,
    pub filter: ParsedIndexFilter,
}

impl IndexIdentity {
    /// Parse a request into an identity, or `None` if it is not indexable
    /// (token program, no usable filters, or unparseable memcmp bytes).
    ///
    /// This is the single source of truth for "what would we index"; both the
    /// API client and the tracker call it so their keys always agree.
    pub fn parse(program: Pubkey, config: Option<&RpcProgramAccountsConfig>) -> Option<Self> {
        if program.to_string() == TOKEN_PROGRAM_ID {
            return None;
        }

        let filters = config?.filters.as_ref()?;

        let mut offsets_lengths: Vec<(u64, u64)> = Vec::new();
        let mut datasize: Option<u64> = None;

        for filter in filters {
            match filter {
                RpcFilterType::DataSize(size) => datasize = Some(*size),
                RpcFilterType::Memcmp(memcmp) => {
                    let bytes = memcmp.bytes()?;
                    offsets_lengths.push((memcmp.offset() as u64, bytes.len() as u64));
                }
                // Not index-defining: state marker and value comparisons.
                RpcFilterType::TokenAccountState | RpcFilterType::ValueCmp(_) => {}
            }
        }

        if offsets_lengths.is_empty() && datasize.is_none() {
            return None;
        }

        offsets_lengths.sort_unstable();
        offsets_lengths.dedup();

        Some(Self {
            program,
            filter: ParsedIndexFilter {
                offsets_lengths,
                datasize,
            },
        })
    }

    /// Reconstruct an identity from its stored parts (e.g. a database row).
    /// `offsets_lengths` is assumed already canonical (sorted/deduped), as it is
    /// when produced by [`IndexIdentity::parse`] before storage.
    pub fn from_parts(
        program: Pubkey,
        offsets_lengths: Vec<(u64, u64)>,
        datasize: Option<u64>,
    ) -> Self {
        Self {
            program,
            filter: ParsedIndexFilter {
                offsets_lengths,
                datasize,
            },
        }
    }

    /// Stable, deterministic short id derived from the identity (program +
    /// filter shape). Used both as the DB primary key and as the hash token in
    /// the physical index names. Deterministic across processes and restarts
    /// (blake3, not the std hasher), so the same pattern always maps to the same
    /// row and the same index.
    pub fn pattern_id(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.program.as_ref());
        for (offset, length) in &self.filter.offsets_lengths {
            hasher.update(&offset.to_le_bytes());
            hasher.update(&length.to_le_bytes());
        }
        match self.filter.datasize {
            Some(size) => {
                hasher.update(&[1]);
                hasher.update(&size.to_le_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        };
        let hex = hasher.finalize().to_hex();
        hex[..PATTERN_ID_HEX_LEN].to_string()
    }

    /// Physical index name on `table` for this identity, e.g.
    /// `idx_snapshot_accounts_<pattern_id>`. Guaranteed `<= 63` bytes:
    /// `idx_snapshot_accounts_` (22) + 16 = 38.
    pub fn pg_index_name(&self, table: &str) -> String {
        let name = format!("idx_{table}_{}", self.pattern_id());
        debug_assert!(name.len() <= PG_MAX_IDENTIFIER_LEN);
        name
    }

    /// Human-readable description used in logs, metrics labels and the debug
    /// endpoint. Not used as a database identifier, so length is unconstrained.
    pub fn human_name(&self) -> String {
        let mut parts = Vec::with_capacity(self.filter.offsets_lengths.len() + 1);
        parts.push(self.program.to_string());
        for (offset, length) in &self.filter.offsets_lengths {
            parts.push(format!("o{offset}l{length}"));
        }
        if let Some(size) = self.filter.datasize {
            parts.push(format!("d{size}"));
        }
        parts.join(" ")
    }

    /// `CREATE INDEX` statement for one side of the pair. Mirrors the columns
    /// and partial predicate the GPA SQL relies on.
    pub fn create_index_sql(&self, table: &str) -> String {
        let mut columns: Vec<String> = self
            .filter
            .offsets_lengths
            .iter()
            .map(|(offset, length)| format!("substring(data, {}, {})", offset + 1, length))
            .collect();
        columns.push("slot".to_string());

        let mut where_clause = format!(
            "owner = '\\x{}'::bytea",
            hex::encode(self.program.to_bytes())
        );
        if let Some(size) = self.filter.datasize {
            where_clause.push_str(&format!(" AND length(data) = {size}"));
        }

        format!(
            "CREATE INDEX IF NOT EXISTS {name} ON {table} ({columns}) WHERE {where_clause}",
            name = self.pg_index_name(table),
            columns = columns.join(", "),
        )
    }

    /// Fingerprint of the *values* of this request's index-relevant memcmp
    /// filters, for approximate distinct-value (variety) tracking. Two requests
    /// with the same identity but different memcmp bytes produce different
    /// fingerprints; identical bytes produce the same one. Returns `None` when
    /// there are no memcmp values to fingerprint (e.g. datasize-only indexes).
    pub fn value_fingerprint(config: Option<&RpcProgramAccountsConfig>) -> Option<u64> {
        let filters = config?.filters.as_ref()?;
        let mut hasher = blake3::Hasher::new();
        let mut any = false;
        // Sort by offset so fingerprint is order-independent.
        let mut memcmps: Vec<(u64, Vec<u8>)> = filters
            .iter()
            .filter_map(|f| match f {
                RpcFilterType::Memcmp(m) => m.bytes().map(|b| (m.offset() as u64, b.to_vec())),
                _ => None,
            })
            .collect();
        memcmps.sort_by_key(|(offset, _)| *offset);
        for (offset, bytes) in memcmps {
            any = true;
            hasher.update(&offset.to_le_bytes());
            hasher.update(&bytes);
        }
        if !any {
            return None;
        }
        let hash = hasher.finalize();
        Some(u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::rpc_filter_type::{Memcmp, MemcmpEncodedBytes};
    use std::str::FromStr;

    fn program() -> Pubkey {
        Pubkey::from_str("2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c").unwrap()
    }

    fn cfg(filters: Vec<RpcFilterType>) -> RpcProgramAccountsConfig {
        RpcProgramAccountsConfig {
            filters: Some(filters),
            ..Default::default()
        }
    }

    #[test]
    fn token_program_is_not_indexable() {
        let id = IndexIdentity::parse(
            Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap(),
            Some(&cfg(vec![RpcFilterType::DataSize(165)])),
        );
        assert!(id.is_none());
    }

    #[test]
    fn no_filters_is_not_indexable() {
        assert!(IndexIdentity::parse(program(), Some(&cfg(vec![]))).is_none());
        assert!(IndexIdentity::parse(program(), None).is_none());
    }

    #[test]
    fn identity_is_value_independent() {
        let a = IndexIdentity::parse(
            program(),
            Some(&cfg(vec![RpcFilterType::Memcmp(Memcmp::new(
                0,
                MemcmpEncodedBytes::Bytes(vec![1, 2, 3, 4, 5, 6, 7, 8]),
            ))])),
        )
        .unwrap();
        let b = IndexIdentity::parse(
            program(),
            Some(&cfg(vec![RpcFilterType::Memcmp(Memcmp::new(
                0,
                MemcmpEncodedBytes::Bytes(vec![9, 9, 9, 9, 9, 9, 9, 9]),
            ))])),
        )
        .unwrap();
        // Same offset/length, different values => same identity, same id.
        assert_eq!(a, b);
        assert_eq!(a.pattern_id(), b.pattern_id());
        // But different value fingerprints (that is the variety we track).
        let fa = IndexIdentity::value_fingerprint(Some(&cfg(vec![RpcFilterType::Memcmp(
            Memcmp::new(0, MemcmpEncodedBytes::Bytes(vec![1, 2, 3, 4, 5, 6, 7, 8])),
        )])));
        let fb = IndexIdentity::value_fingerprint(Some(&cfg(vec![RpcFilterType::Memcmp(
            Memcmp::new(0, MemcmpEncodedBytes::Bytes(vec![9, 9, 9, 9, 9, 9, 9, 9])),
        )])));
        assert_ne!(fa, fb);
    }

    #[test]
    fn index_name_within_pg_limit() {
        let id = IndexIdentity::parse(
            program(),
            Some(&cfg(vec![
                RpcFilterType::Memcmp(Memcmp::new(
                    123_456,
                    MemcmpEncodedBytes::Bytes(vec![0; 128]),
                )),
                RpcFilterType::Memcmp(Memcmp::new(
                    999_999,
                    MemcmpEncodedBytes::Bytes(vec![0; 128]),
                )),
                RpcFilterType::DataSize(10_485_760),
            ])),
        )
        .unwrap();
        assert!(id.pg_index_name("snapshot_accounts").len() <= PG_MAX_IDENTIFIER_LEN);
    }

    #[test]
    fn create_sql_shape() {
        let id = IndexIdentity::parse(
            program(),
            Some(&cfg(vec![RpcFilterType::Memcmp(Memcmp::new(
                0,
                MemcmpEncodedBytes::Bytes(vec![1, 2, 3, 4, 5, 6, 7, 8]),
            ))])),
        )
        .unwrap();
        let sql = id.create_index_sql("accounts");
        assert!(sql.contains("CREATE INDEX IF NOT EXISTS idx_accounts_"));
        assert!(sql.contains("substring(data, 1, 8)"));
        assert!(sql.contains("owner = "));
    }
}
