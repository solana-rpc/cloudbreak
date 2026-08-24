use std::collections::HashMap;

use solana_pubkey::Pubkey;

use crate::accountsdb_helpers::{
    DeserializableVersionedBank, DeserializableVersionedEpochStakes,
};

/// Per-snapshot stake data extracted from `DeserializableVersionedBank`.
/// This is what Agave's `getVoteAccounts` uses for the two non-account-derived
/// fields (`activatedStake`, `epochVoteAccount`)
#[derive(Debug, Clone)]
pub struct SnapshotStakeData {
    pub epoch: u64,
    pub voters: Vec<VoterStakeRow>,
}

#[derive(Debug, Clone)]
pub struct VoterStakeRow {
    pub vote_pubkey: Pubkey,
    pub node_pubkey: Pubkey,
    pub activated_stake: u64,
    pub in_epoch_set: bool,
}

/// Extracts per-voter activated stake + epoch-set membership from the snapshot.
#[allow(deprecated)]
pub fn extract_stake_data(
    bank: &DeserializableVersionedBank,
    versioned_epoch_stakes: &HashMap<u64, DeserializableVersionedEpochStakes>,
) -> SnapshotStakeData {
    let epoch = bank.epoch;

    let epoch_set: std::collections::HashSet<Pubkey> = versioned_epoch_stakes
        .get(&epoch)
        .map(|epoch_stakes| {
            epoch_stakes
                .vote_accounts()
                .iter()
                .map(|(pubkey, _)| *pubkey)
                .collect()
        })
        .unwrap_or_default();

    let vote_accounts = &bank.stakes.vote_accounts;
    let mut voters = Vec::with_capacity(vote_accounts.len());
    for (vote_pubkey, stake) in vote_accounts.delegated_stakes() {
        let Some(vote_account) = vote_accounts.get(vote_pubkey) else {
            continue;
        };
        voters.push(VoterStakeRow {
            vote_pubkey: *vote_pubkey,
            node_pubkey: *vote_account.node_pubkey(),
            activated_stake: stake,
            in_epoch_set: epoch_set.contains(vote_pubkey),
        });
    }

    SnapshotStakeData { epoch, voters }
}
