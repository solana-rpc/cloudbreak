//! Top-N largest-accounts tracking backing getLargestAccounts and
//! getTokenLargestAccounts.
//!
//! The indexer maintains a top-N holder set per tracked mint (the
//! token-largest-accounts config section) and three sentinel "mints" for
//! native-SOL, circulating, and non-circulating (the largest-accounts
//! section). When a top-N changes, the persisted slice is written as one
//! packed bytea record per (mint, slot) row in the largest_accounts table.
//! The API reads the newest record at or below the commitment slot.
//!
//! See README.md in this directory for the module layout and runtime model.

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
