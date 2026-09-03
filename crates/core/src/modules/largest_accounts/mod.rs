//! Top-N largest-accounts tracking backing getLargestAccounts (GLA) and
//! getTokenLargestAccounts (GTLA).
//!
//! The indexer maintains an in-memory top-N holder set per tracked mint (the
//! `[token-largest-accounts]` section) and three sentinel "mints" for
//! native-SOL, circulating, and non-circulating (the `[largest-accounts]`
//! section). When a top-N changes, its top 20 rows are written as one packed
//! bytea record per `(mint, slot)` row in the `largest_accounts` table. The API
//! serves the newest record at or below the request's commitment slot.
//!
//! # Layout
//!
//! - `mod.rs`: public surface, the sentinel mint constants, [`PERSISTED_TOP_N`],
//!   token program ids, [`MintRecord`], and the packed bytea codec.
//! - `tracker.rs`: in-memory state (per-mint tops with the eviction reservoir
//!   and the `dropped_floor`/stale soundness bookkeeping), the snapshot seed
//!   path, `apply_block`, and the class-sentinel reseed.
//! - `persist.rs`: write path (record upserts, stale and cleared-mint deletes,
//!   `from_config`, outcome persistence).
//! - `read.rs`: read path shared with the API ([`fetch_record`]).
//! - `prune.rs`: finalize-time pruner for redundant record generations, off the
//!   finalization critical path.
//!
//! # Runtime model
//!
//! Both features ship in every binary and are gated at runtime by config. Two
//! independent sections enable two independent domains in one tracker, each on
//! only when its section is present with `enabled = true`.
//!
//! - `[largest-accounts]` (GLA) enables the three sentinel tops. It requires an
//!   empty `[programs]` filter and the `[snapshot]` section.
//! - `[token-largest-accounts]` (GTLA) enables one top per mint in
//!   `tracked-mints`. It requires the token programs (Tokenkeg / Token-2022)
//!   indexed, meaning an unfiltered `[programs]` or an include-list with both,
//!   plus the `[snapshot]` section. It does not need the non-circulating tracker
//!   or the GLA sentinels.
//!
//! The tracker is seeded from the startup snapshot pass and goes live when
//! snapshot processing finishes. When neither section is enabled the handle is a
//! no-op ([`LargestAccountsTracker::default()`]), so the indexer hooks cost
//! nothing. The API enables each method from the same two sections in its own
//! config and routes GTLA on record presence: a persisted record means the mint
//! is tracked, and no record returns a fast error with no fallback table scan.
//! There is no shared enablement state in the database.

mod persist;
mod prune;
mod read;
mod tracker;

pub(crate) use persist::persist_largest_outcome;
pub use prune::{prune_largest_accounts, spawn_largest_accounts_pruner};
pub use read::fetch_record;
pub use tracker::{BlockOutcome, LargestAccountsSeed, LargestAccountsTracker, PendingLargestAccount};

use solana_pubkey::Pubkey;

/// A snapshot of one mint's persisted top-N at a slot: the payload for a single
/// `largest_accounts` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintRecord {
    pub mint: Pubkey,
    pub slot: u64,
    pub rows: Vec<(Pubkey, u64)>,
}

/// Bytes per entry in a packed record: a 32-byte pubkey followed by a
/// little-endian `u64` amount.
pub const RECORD_ENTRY_SIZE: usize = 40;

pub fn encode_record(rows: &[(Pubkey, u64)]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(rows.len() * RECORD_ENTRY_SIZE);
    for (pubkey, amount) in rows {
        bytes.extend_from_slice(pubkey.as_ref());
        bytes.extend_from_slice(&amount.to_le_bytes());
    }
    bytes
}

pub fn decode_record(bytes: &[u8]) -> Option<Vec<(Pubkey, u64)>> {
    if !bytes.len().is_multiple_of(RECORD_ENTRY_SIZE) {
        return None;
    }
    let mut rows = Vec::with_capacity(bytes.len() / RECORD_ENTRY_SIZE);
    for chunk in bytes.chunks_exact(RECORD_ENTRY_SIZE) {
        let pubkey = Pubkey::try_from(&chunk[..32]).ok()?;
        let amount = u64::from_le_bytes(chunk[32..].try_into().ok()?);
        rows.push((pubkey, amount));
    }
    Some(rows)
}

/// Number of rows persisted (and served) per record. The in-memory top can be
/// larger (`accounts-per-mint`); the surplus acts as an eviction reservoir.
pub const PERSISTED_TOP_N: usize = 20;

/// Sentinel mint keying the native-SOL top-N that serves plain `getLargestAccounts`.
pub const SOL_SENTINEL_MINT: Pubkey = Pubkey::new_from_array([0u8; 32]);
/// Sentinel mint keying the circulating-accounts top-N behind the
/// `getLargestAccounts` `filter: circulating` option.
pub const CIRCULATING_SENTINEL_MINT: Pubkey = Pubkey::new_from_array([1u8; 32]);
/// Sentinel mint keying the non-circulating-accounts top-N behind the
/// `getLargestAccounts` `filter: nonCirculating` option.
pub const NON_CIRCULATING_SENTINEL_MINT: Pubkey = Pubkey::new_from_array([2u8; 32]);

fn is_sentinel(mint: &Pubkey) -> bool {
    *mint == SOL_SENTINEL_MINT
        || *mint == CIRCULATING_SENTINEL_MINT
        || *mint == NON_CIRCULATING_SENTINEL_MINT
}

pub const TOKEN_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub const TOKEN_2022_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
