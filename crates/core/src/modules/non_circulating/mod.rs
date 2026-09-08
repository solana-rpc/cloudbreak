// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Real-time non-circulating membership from an in-memory stake map.
//!
//! # What it does
//!
//! Agave's non-circulating set is the pinned account list plus every stake
//! account whose lockup is in force or whose withdrawer is a listed authority.
//! This module keeps one 40-byte record per stake account (and per pinned
//! account) in memory, evaluates membership against the Clock sysvar as each
//! block arrives, and keeps a running sum of the members' lamports. Lockup
//! expiry has no on-chain event, so members whose lockup can expire sit in one
//! of two ordered sets keyed by their expiry, and the head of each set is
//! popped as the clock passes it.
//!
//! The map is also the previous-balance source for stake pubkeys in the
//! getSupply delta. It reports the summed stake delta and the pubkeys it
//! handled per block, and the supply hot cache skips those pubkeys.
//!
//! # Layout
//!
//! - `mod.rs`: the handle re-export, the [`StakeEntry`] record and its flags,
//!   the per-block [`StakeBlock`] outcome, and the debug views.
//! - `tracker.rs`: the state behind one mutex, `apply_block`, the snapshot seed,
//!   and the bootstrap flip.
//! - `persist.rs`: `from_config`, the member-list upsert, and the two persist
//!   entry points.
//! - `read.rs`: the member-list read the API shares.
//! - `lists.rs`: Agave's pinned account and withdraw-authority lists.
//!
//! # Runtime model
//!
//! Enabled when `[largest-accounts]` or `[supply]` is on. The snapshot pass
//! seeds every stake and pinned account and the Clock sysvar through
//! `seed_account`. `finish_bootstrap` evaluates every entry once, builds the
//! expiry sets and the running sum, and flips Live. Before Live the tracker
//! serves nothing: `is_non_circulating` is false and `total` is `None`.
//!
//! Per block, `apply_block` advances the clock from the block's Clock sysvar,
//! pops the expiries the new clock reaches, then folds every account: a
//! stake-owned or pinned account is parsed and replaces its entry, and any
//! other write to a pubkey already in the map turns the entry into a tombstone.
//! A membership flip moves the pubkey between the sets and the running sum.
//! GLA reads the post-block membership through `is_non_circulating` and gets
//! the expiries the block did not write as `StakeBlock::expired`.
//!
//! # Newest slot wins
//!
//! Every entry carries the slot of its last write. A write at or below it is
//! ignored, so a replayed or repaired block never moves an entry backwards. The
//! clock only advances. A tombstone keeps its stamp for the same reason, and
//! every later write to that pubkey refreshes it.
//!
//! # Ownership of a pubkey in the supply delta
//!
//! The map owns a pubkey while its entry is stake-owned. It reports the delta of
//! every write to an owned entry in `StakeBlock::delta` and the pubkey in
//! `StakeBlock::handled`, including a write that closes the account, and
//! including a stale write, which contributes zero. A stake-owned write to a
//! pubkey the map does not own inserts it with no delta and leaves the pubkey
//! to the supply tracker for that block, which resolves the previous balance
//! through its own cache and miss read and then drops it from the cache. So a
//! pubkey is never a balance source in both places.
//!
//! # Membership and Agave
//!
//! Agave evaluates the lockup against the bank clock at the requested
//! commitment. This tracker evaluates against the confirmed block's clock and
//! the API serves the persisted row, so an account whose lockup expires inside
//! the confirmed-to-finalized window is classified differently by the two for
//! up to about 32 slots. The clock arrives with the Clock sysvar in the block.
//! A block without it leaves expiries to the next block that carries one.
//!
//! # Footprint
//!
//! The record is 40 bytes with natural alignment, 73 bytes per hashmap bucket
//! with the 32-byte key [est]. The map is pre-sized to
//! [`STAKE_ACCOUNTS_CAPACITY`], which rounds to 2^21 buckets, about 153 MB
//! [est] against the 1.435M stake accounts measured live. The two expiry sets
//! hold about 3,134 members [measured], under 0.4 MB. Tombstones stay in the
//! map after a close so their stamp keeps guarding repaired blocks, and are
//! purged at bootstrap.

pub mod lists;
mod persist;
pub mod read;
mod tracker;

pub use tracker::NonCirculatingTracker;

use serde::Serialize;
use solana_program::clock::Clock;
use solana_pubkey::Pubkey;
use std::collections::HashSet;

/// Pre-sized item capacity of the stake map: 1.435M stake accounts measured on
/// mainnet plus headroom, rounding to 2^21 buckets.
pub const STAKE_ACCOUNTS_CAPACITY: usize = 1_500_000;

pub const CLOCK_SYSVAR_ID: Pubkey =
    Pubkey::from_str_const("SysvarC1ock11111111111111111111111111111111");

/// One stake or pinned account as the map holds it. `flags` carries the
/// ownership, the parse result, the two list memberships, the evaluated
/// membership, and which expiry set the entry sits in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StakeEntry {
    pub lamports: u64,
    pub lockup_unix_timestamp: i64,
    pub lockup_epoch: u64,
    pub slot: u64,
    pub flags: u8,
}

impl StakeEntry {
    /// The last write's owner is the Stake program: the map owns the pubkey.
    pub const STAKE_OWNED: u8 = 1 << 0;
    /// The data parsed as `Initialized` or `Stake`, so the lockup fields are set.
    pub const LOCKUP: u8 = 1 << 1;
    /// The withdrawer is in `WITHDRAW_AUTHORITY`.
    pub const LISTED_WITHDRAWER: u8 = 1 << 2;
    /// The pubkey is in `NON_CIRCULATING_ACCOUNTS`.
    pub const PINNED: u8 = 1 << 3;
    /// Currently non-circulating.
    pub const MEMBER: u8 = 1 << 4;
    pub(crate) const IN_BY_EPOCH: u8 = 1 << 5;
    pub(crate) const IN_BY_TIMESTAMP: u8 = 1 << 6;

    pub fn has(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    pub fn stake_owned(&self) -> bool {
        self.has(Self::STAKE_OWNED)
    }

    pub fn member(&self) -> bool {
        self.has(Self::MEMBER)
    }

    /// Agave's rule: a pinned account, or a stake account whose lockup is in
    /// force or whose withdrawer is listed. The custodian is not consulted.
    pub fn evaluate(&self, clock: &BlockClock) -> bool {
        self.has(Self::PINNED)
            || (self.stake_owned()
                && self.has(Self::LOCKUP)
                && (self.has(Self::LISTED_WITHDRAWER)
                    || self.lockup_unix_timestamp > clock.unix_timestamp
                    || self.lockup_epoch > clock.epoch))
    }
}

/// What one block did to the stake map, consumed by the supply tracker and GLA.
#[derive(Debug, Default)]
pub struct StakeBlock {
    /// The summed lamport delta over every pubkey in `handled`.
    pub delta: i128,
    /// The pubkeys whose previous balance came from the map. The supply
    /// tracker skips them.
    pub handled: HashSet<Pubkey>,
    /// Members whose lockup the block's clock expired, with their lamports.
    /// They were not written in the block, so GLA moves them itself.
    pub expired: Vec<(Pubkey, u64)>,
    /// True when any account joined or left, so the member list is re-persisted.
    pub members_changed: bool,
}

/// One member's balance at a slot, as GLA's bootstrap class seed reads it.
#[derive(Clone, Debug)]
pub struct NonCirculatingBalance {
    pub pubkey: Pubkey,
    pub slot: u64,
    pub lamports: u64,
}

/// An O(1) copy of the tracker state for the debug endpoint, plus the heads of
/// the two expiry sets bounded by the request's limit.
#[derive(Serialize)]
pub struct NonCirculatingSummary {
    pub status: &'static str,
    pub slot: u64,
    pub clock: Option<BlockClock>,
    pub members: usize,
    pub non_circulating_lamports: u64,
    pub stake_accounts: usize,
    pub by_epoch: usize,
    pub by_timestamp: usize,
    pub epoch_heads: Vec<(u64, String)>,
    pub timestamp_heads: Vec<(i64, String)>,
}

/// The three Clock sysvar fields membership depends on, as the last block set
/// them. The slot orders clocks so one never regresses.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct BlockClock {
    pub slot: u64,
    pub epoch: u64,
    pub unix_timestamp: i64,
}

impl From<Clock> for BlockClock {
    fn from(clock: Clock) -> Self {
        Self {
            slot: clock.slot,
            epoch: clock.epoch,
            unix_timestamp: clock.unix_timestamp,
        }
    }
}

/// One map entry with its flags unpacked, for the debug endpoint.
#[derive(Serialize)]
pub struct StakeEntryView {
    pub lamports: u64,
    pub lockup_unix_timestamp: i64,
    pub lockup_epoch: u64,
    pub slot: u64,
    pub stake_owned: bool,
    pub lockup: bool,
    pub listed_withdrawer: bool,
    pub pinned: bool,
    pub member: bool,
}

impl From<StakeEntry> for StakeEntryView {
    fn from(entry: StakeEntry) -> Self {
        Self {
            lamports: entry.lamports,
            lockup_unix_timestamp: entry.lockup_unix_timestamp,
            lockup_epoch: entry.lockup_epoch,
            slot: entry.slot,
            stake_owned: entry.stake_owned(),
            lockup: entry.has(StakeEntry::LOCKUP),
            listed_withdrawer: entry.has(StakeEntry::LISTED_WITHDRAWER),
            pinned: entry.has(StakeEntry::PINNED),
            member: entry.member(),
        }
    }
}
