// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Builds a [`SlotBlock`] from one `SubscribeUpdateBlock`.
//!
//! Each pubkey is inserted once with no per-block dedup. The plugin seals at
//! most one entry per pubkey per block (see the upstream invariants in `mod.rs`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use solana_pubkey::Pubkey;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

use super::{AccountEntry, LiveAccount};
use crate::config::AccountSelectorConfig;

/// One sealed processed block, keyed in the store by slot.
pub(crate) struct SlotBlock {
    pub slot: u64,
    pub blockhash: String,
    pub parent_slot: u64,
    pub parent_blockhash: String,
    pub block_time: Option<i64>,
    pub received_at: Instant,
    pub accounts: HashMap<Pubkey, AccountEntry>,
    /// Estimated heap bytes: account data plus the map table.
    pub heap_bytes: usize,
}

impl SlotBlock {
    /// Classifies every account. A zero-lamport write and a write whose owner
    /// is outside the program filter are both closes.
    pub(crate) fn from_update(
        block: SubscribeUpdateBlock,
        program_filter: &AccountSelectorConfig,
        received_at: Instant,
    ) -> Self {
        let mut accounts = HashMap::with_capacity(block.accounts.len());
        let mut skipped = 0usize;
        let mut data_bytes = 0usize;

        for account in block.accounts {
            let (Ok(pubkey), Ok(owner)) = (
                Pubkey::try_from(account.pubkey.as_slice()),
                Pubkey::try_from(account.owner.as_slice()),
            ) else {
                skipped += 1;
                continue;
            };
            let entry = if account.lamports == 0 || !program_filter.is_program_selected(&owner) {
                AccountEntry::Closed
            } else {
                data_bytes += account.data.len();
                AccountEntry::Live(LiveAccount {
                    lamports: account.lamports,
                    owner,
                    executable: account.executable,
                    rent_epoch: account.rent_epoch,
                    data: Arc::new(account.data),
                })
            };
            accounts.insert(pubkey, entry);
        }

        if skipped > 0 {
            tracing::warn!(
                slot = block.slot,
                skipped,
                "processed block has accounts with a malformed pubkey or owner"
            );
        }

        Self {
            slot: block.slot,
            blockhash: block.blockhash,
            parent_slot: block.parent_slot,
            parent_blockhash: block.parent_blockhash,
            block_time: block.block_time.map(|t| t.timestamp),
            received_at,
            heap_bytes: data_bytes + accounts.capacity() * size_of::<(Pubkey, AccountEntry)>(),
            accounts,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::PubkeyDef;
    use yellowstone_grpc_proto::prelude::{SubscribeUpdateAccountInfo, UnixTimestamp};

    pub(crate) fn account_info(
        pubkey: Pubkey,
        owner: Pubkey,
        lamports: u64,
        data: Vec<u8>,
    ) -> SubscribeUpdateAccountInfo {
        SubscribeUpdateAccountInfo {
            pubkey: pubkey.to_bytes().to_vec(),
            lamports,
            owner: owner.to_bytes().to_vec(),
            executable: false,
            rent_epoch: u64::MAX,
            data,
            write_version: 0,
            txn_signature: None,
        }
    }

    pub(crate) fn update(
        slot: u64,
        hash: &str,
        parent_slot: u64,
        parent_hash: &str,
        accounts: Vec<SubscribeUpdateAccountInfo>,
    ) -> SubscribeUpdateBlock {
        SubscribeUpdateBlock {
            slot,
            blockhash: hash.to_string(),
            parent_slot,
            parent_blockhash: parent_hash.to_string(),
            block_time: Some(UnixTimestamp {
                timestamp: 1_700_000_000 + slot as i64,
            }),
            accounts,
            ..Default::default()
        }
    }

    fn build(
        accounts: Vec<SubscribeUpdateAccountInfo>,
        filter: &AccountSelectorConfig,
    ) -> SlotBlock {
        SlotBlock::from_update(update(10, "h10", 9, "h9", accounts), filter, Instant::now())
    }

    #[test]
    fn classifies_live_and_treats_excluded_as_closed() {
        let included = Pubkey::new_unique();
        let excluded = Pubkey::new_unique();
        let filter = AccountSelectorConfig {
            include: vec![PubkeyDef(included)],
            exclude: vec![],
        };
        let live_key = Pubkey::new_unique();
        let closed_key = Pubkey::new_unique();
        let excluded_key = Pubkey::new_unique();
        let block = build(
            vec![
                account_info(live_key, included, 5, vec![1, 2, 3]),
                account_info(closed_key, included, 0, vec![9; 100]),
                account_info(excluded_key, excluded, 7, vec![9; 100]),
            ],
            &filter,
        );
        match &block.accounts[&live_key] {
            AccountEntry::Live(account) => {
                assert_eq!(account.lamports, 5);
                assert_eq!(account.owner, included);
                assert_eq!(*account.data, vec![1, 2, 3]);
            }
            other => panic!("expected live, got {other:?}"),
        }
        assert_eq!(block.accounts[&closed_key], AccountEntry::Closed);
        assert_eq!(block.accounts[&excluded_key], AccountEntry::Closed);
        assert!(block.heap_bytes > 3);
        assert_eq!(block.block_time, Some(1_700_000_010));
    }

    #[test]
    fn skips_malformed_pubkeys() {
        let owner = Pubkey::new_unique();
        let mut bad = account_info(Pubkey::new_unique(), owner, 1, vec![]);
        bad.pubkey.truncate(31);
        let mut bad_owner = account_info(Pubkey::new_unique(), owner, 1, vec![]);
        bad_owner.owner.push(0);
        let good = Pubkey::new_unique();
        let block = build(
            vec![bad, bad_owner, account_info(good, owner, 1, vec![])],
            &AccountSelectorConfig::default(),
        );
        assert_eq!(block.accounts.len(), 1);
        assert!(block.accounts.contains_key(&good));
    }
}
