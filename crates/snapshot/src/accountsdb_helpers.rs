// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use solana_accounts_db::blockhash_queue::BlockhashQueue;
use solana_program::clock::{Epoch, Slot, UnixTimestamp};
use solana_pubkey::Pubkey;
use solana_runtime::stake_history::StakeHistory;
use solana_serde::default_on_eof;
use solana_vote::vote_account::VoteAccounts;

pub const MAX_STREAM_SIZE: u64 = 32 * 1024 * 1024 * 1024;

/// Mirror of solana-runtime's `stakes::DeserializableStakes`, which is `pub(crate)`
/// upstream — `Stakes<T>` itself only derives `Serialize` from 4.0 onward. The bincode
/// layout is identical to `Stakes<T>`.
#[derive(Clone, Debug, Deserialize)]
pub struct DeserializableStakes<T> {
    pub vote_accounts: VoteAccounts,
    pub stake_delegations: Vec<(Pubkey, T)>,
    pub unused: u64,
    pub epoch: Epoch,
    pub stake_history: StakeHistory,
}

/// Mirror of solana-runtime's `epoch_stakes::DeserializableEpochStakes` (`pub(crate)`).
#[derive(Clone, Debug, Deserialize)]
pub struct DeserializableEpochStakes {
    pub vote_accounts: VoteAccounts,
    pub stake_delegations: Vec<(Pubkey, solana_stake_interface::state::Stake)>,
    pub unused: u64,
    pub epoch: Epoch,
    pub stake_history: StakeHistory,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeVoteAccounts {
    pub vote_accounts: Vec<Pubkey>,
    pub total_stake: u64,
}

/// Mirror of solana-runtime's `epoch_stakes::DeserializableVersionedEpochStakes`
/// (`pub(crate)`); `VersionedEpochStakes` only derives `Serialize` from 4.0 onward.
#[derive(Clone, Debug, Deserialize)]
pub enum DeserializableVersionedEpochStakes {
    Current {
        stakes: DeserializableEpochStakes,
        total_stake: u64,
        node_id_to_vote_accounts: HashMap<Pubkey, NodeVoteAccounts>,
        epoch_authorized_voters: HashMap<Pubkey, Pubkey>,
    },
}

impl DeserializableVersionedEpochStakes {
    pub fn vote_accounts(&self) -> &VoteAccounts {
        let Self::Current { stakes, .. } = self;
        &stakes.vote_accounts
    }
}

// Serializable version of AccountStorageEntry for snapshot format
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct SerializableAccountStorageEntry {
    pub id: usize, // SerializedAccountsFileId
    pub accounts_current_len: usize,
}

#[derive(Clone, Deserialize, Debug)]
#[allow(dead_code)]
#[allow(deprecated)]
pub struct DeserializableVersionedBank {
    pub blockhash_queue: BlockhashQueue,
    // Agave dropped the `AncestorsForSerialization` alias in 4.0; it was always this map.
    pub ancestors: HashMap<Slot, usize>,
    pub hash: solana_hash::Hash,
    pub parent_hash: solana_hash::Hash,
    pub parent_slot: Slot,
    pub hard_forks: solana_hard_forks::HardForks,
    pub transaction_count: u64,
    pub tick_height: u64,
    pub signature_count: u64,
    pub capitalization: u64,
    pub max_tick_height: u64,
    pub hashes_per_tick: Option<u64>,
    pub ticks_per_slot: u64,
    pub ns_per_slot: u128,
    pub genesis_creation_time: UnixTimestamp,
    pub slots_per_year: f64,
    pub accounts_data_len: u64,
    pub slot: Slot,
    pub epoch: Epoch,
    pub block_height: u64,
    pub collector_id: Pubkey,
    pub collector_fees: u64,
    pub _fee_calculator: solana_fee_calculator::FeeCalculator,
    pub fee_rate_governor: solana_fee_calculator::FeeRateGovernor,
    pub collected_rent: u64,
    pub rent_collector: solana_runtime::rent_collector::RentCollector,
    pub epoch_schedule: solana_epoch_schedule::EpochSchedule,
    pub inflation: solana_inflation::Inflation,
    pub stakes: DeserializableStakes<solana_stake_interface::state::Delegation>,
    #[allow(dead_code)]
    pub unused_accounts: UnusedAccounts,
    pub unused_epoch_stakes: HashMap<Epoch, ()>,
    pub is_delta: bool,
}

/// Obsolete (always `None`) — mirror of solana-runtime's
/// `serde_snapshot::ObsoleteIncrementalSnapshotPersistence`, present only so the
/// extra-fields stream deserializes at the correct offsets.
#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub struct ObsoleteIncrementalSnapshotPersistence {
    pub full_slot: u64,
    pub full_hash: [u8; 32],
    pub full_capitalization: u64,
    pub incremental_hash: [u8; 32],
    pub incremental_capitalization: u64,
}

/// Mirror of solana-runtime's `serde_snapshot::ExtraFieldsToDeserialize`.
/// Serialized at the END of the snapshot manifest, AFTER the bank struct and
/// `AccountsDbFields`. This is where the real `versioned_epoch_stakes` live
/// (the source for `epochVoteAccount`)
#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub struct ExtraFields {
    #[serde(deserialize_with = "default_on_eof")]
    pub lamports_per_signature: u64,
    #[serde(deserialize_with = "default_on_eof")]
    pub obsolete_incremental_snapshot_persistence: Option<ObsoleteIncrementalSnapshotPersistence>,
    #[serde(deserialize_with = "default_on_eof")]
    pub obsolete_epoch_accounts_hash: Option<[u8; 32]>,
    #[serde(deserialize_with = "default_on_eof")]
    pub versioned_epoch_stakes: HashMap<u64, DeserializableVersionedEpochStakes>,
}

#[derive(Default, Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct UnusedAccounts {
    unused1: HashSet<Pubkey>,
    unused2: HashSet<Pubkey>,
    unused3: HashMap<Pubkey, u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct AccountsDbFields<T>(
    /// Careful! This contains entries for all historical slots with accounts, not only the slots
    ///  for the snapshot (even if it's incremental)
    pub HashMap<Slot, Vec<T>>,
    pub u64, // obsolete, formerly write_version
    pub Slot,
    #[allow(private_interfaces)] pub BankHashInfo,
    /// all slots that were roots within the last epoch
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<Slot>,
    /// slots that were roots within the last epoch for which we care about the hash value
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<(Slot, solana_hash::Hash)>,
);

/// Needed for snapshot deserialization but values are not used
#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
struct BankHashInfo {
    _obsolete_accounts_delta_hash: [u8; 32],
    _obsolete_accounts_hash: [u8; 32],
    _obsolete_stats: ObsoleteBankHashStats,
}

/// Matches layout of solana_runtime::bank::BankHashStats (5 x u64) - it was throwing warnings but works for the purpose of deserialization this way
#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
struct ObsoleteBankHashStats {
    _num_updated_accounts: u64,
    _num_removed_accounts: u64,
    _num_lamports_stored: u64,
    _total_data_len: u64,
    _num_executable_accounts: u64,
}
