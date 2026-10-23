// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The mint maps shared by the indexer copy and the API cache. Immutable mints keep only their
//! decimals; mutable mints keep the slot of their last change; tombstones keep the close slot.

use std::collections::HashMap;

use solana_pubkey::Pubkey;

use super::MintChange;
use super::db::MintRow;
use crate::utils::token::MintState;

/// A mutable mint and the slot of its last change.
#[derive(Debug, Clone, PartialEq)]
pub struct MutableMint {
    pub slot: u64,
    pub state: MintState,
}

/// How a change altered the store. Labels the per-block metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeKind {
    Create,
    Update,
    Close,
    Reinit,
}

impl ChangeKind {
    pub const ALL: [ChangeKind; 4] = [Self::Create, Self::Update, Self::Close, Self::Reinit];

    pub fn label(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Close => "close",
            Self::Reinit => "reinit",
        }
    }
}

/// What the store holds for one key.
#[derive(Debug, Clone, PartialEq)]
pub enum StoredMint<'a> {
    Immutable {
        decimals: u8,
    },
    Mutable(&'a MutableMint),
    /// A closed mint, closed at `slot`.
    Tombstone {
        slot: u64,
    },
    Missing,
}

/// Fits 2^27 buckets: up to ~117M entries without a resize.
const EXPECTED_MINTS: usize = 100_000_000;

#[derive(Debug, Default)]
pub struct MintStore {
    immutable: HashMap<Pubkey, u8>,
    mutable: HashMap<Pubkey, MutableMint>,
    /// Closed mints, with their close slot.
    tombstones: HashMap<Pubkey, u64>,
}

impl MintStore {
    /// Pre-sizes the immutable map for [`EXPECTED_MINTS`], so it never rehashes while it grows.
    pub fn presized() -> Self {
        Self {
            immutable: HashMap::with_capacity(EXPECTED_MINTS),
            ..Self::default()
        }
    }

    pub fn get(&self, pubkey: &Pubkey) -> StoredMint<'_> {
        if let Some(entry) = self.mutable.get(pubkey) {
            return StoredMint::Mutable(entry);
        }
        if let Some(slot) = self.tombstones.get(pubkey) {
            return StoredMint::Tombstone { slot: *slot };
        }
        match self.immutable.get(pubkey) {
            Some(decimals) => StoredMint::Immutable {
                decimals: *decimals,
            },
            None => StoredMint::Missing,
        }
    }

    /// The `mints` row of `pubkey`'s current state. An immutable entry has no slot; it takes
    /// `slot`, the slot of the write that created it.
    pub(super) fn row(&self, pubkey: Pubkey, slot: u64) -> Option<MintRow> {
        match self.get(&pubkey) {
            StoredMint::Immutable { decimals } => Some(MintRow {
                pubkey,
                slot,
                state: Some(MintState {
                    decimals,
                    configs: None,
                    immutable: true,
                }),
            }),
            StoredMint::Mutable(entry) => Some(MintRow {
                pubkey,
                slot: entry.slot,
                state: Some(entry.state.clone()),
            }),
            StoredMint::Tombstone { slot } => Some(MintRow {
                pubkey,
                slot,
                state: None,
            }),
            StoredMint::Missing => None,
        }
    }

    /// Applies one write at `slot`. The higher slot wins per key; an immutable mint never
    /// changes. Returns how the store changed, or `None` when it did not.
    pub fn apply(&mut self, pubkey: Pubkey, slot: u64, change: MintChange) -> Option<ChangeKind> {
        match change {
            MintChange::Live(state) => self.apply_live(pubkey, slot, state),
            MintChange::Closed => self.close(pubkey, slot, true),
        }
    }

    /// Applies one `mints` table row. Unlike a block write, a tombstone row is stored even for
    /// a key the store has not seen.
    pub fn apply_row(
        &mut self,
        pubkey: Pubkey,
        slot: u64,
        state: Option<MintState>,
    ) -> Option<ChangeKind> {
        match state {
            Some(state) => self.apply_live(pubkey, slot, state),
            None => self.close(pubkey, slot, false),
        }
    }

    fn apply_live(&mut self, pubkey: Pubkey, slot: u64, state: MintState) -> Option<ChangeKind> {
        if self.immutable.contains_key(&pubkey) {
            return None;
        }
        let kind = match (self.tombstones.get(&pubkey), self.mutable.get(&pubkey)) {
            (Some(closed), _) if *closed > slot => return None,
            (Some(_), _) => ChangeKind::Reinit,
            (None, Some(entry)) if entry.slot > slot || entry.state == state => return None,
            (None, Some(_)) => ChangeKind::Update,
            (None, None) => ChangeKind::Create,
        };
        self.tombstones.remove(&pubkey);
        if state.immutable {
            self.mutable.remove(&pubkey);
            self.immutable.insert(pubkey, state.decimals);
        } else {
            self.mutable.insert(pubkey, MutableMint { slot, state });
        }
        Some(kind)
    }

    /// `known_only`: a block close of an unknown key is ignored, as most zero-lamport writes are
    /// not mints. An immutable mint cannot close on chain, so a close of one is an older write of
    /// the address (from before the mint existed) and is ignored.
    fn close(&mut self, pubkey: Pubkey, slot: u64, known_only: bool) -> Option<ChangeKind> {
        if self.immutable.contains_key(&pubkey) || self.tombstones.contains_key(&pubkey) {
            return None;
        }
        match self.mutable.get(&pubkey) {
            Some(entry) if entry.slot > slot => return None,
            Some(_) => {}
            None if known_only => return None,
            None => {}
        }
        self.mutable.remove(&pubkey);
        self.tombstones.insert(pubkey, slot);
        Some(ChangeKind::Close)
    }

    pub fn immutable_len(&self) -> usize {
        self.immutable.len()
    }

    pub fn mutable_len(&self) -> usize {
        self.mutable.len()
    }

    pub fn tombstones(&self) -> usize {
        self.tombstones.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(decimals: u8, immutable: bool) -> MintState {
        MintState {
            decimals,
            configs: None,
            immutable,
        }
    }

    fn live(decimals: u8, immutable: bool) -> MintChange {
        MintChange::Live(state(decimals, immutable))
    }

    #[test]
    fn immutable_mints_are_created_once_and_never_change() {
        let mut store = MintStore::default();
        let key = Pubkey::new_unique();
        assert_eq!(
            store.apply(key, 10, live(6, true)),
            Some(ChangeKind::Create)
        );
        assert_eq!(store.apply(key, 11, live(9, true)), None);
        assert_eq!(store.get(&key), StoredMint::Immutable { decimals: 6 });
        assert_eq!(store.immutable_len(), 1);
    }

    #[test]
    fn mutable_mints_follow_the_higher_slot() {
        let mut store = MintStore::default();
        let key = Pubkey::new_unique();
        assert_eq!(
            store.apply(key, 10, live(6, false)),
            Some(ChangeKind::Create)
        );
        // Supply-only writes parse to the same state.
        assert_eq!(store.apply(key, 11, live(6, false)), None);
        assert_eq!(
            store.apply(key, 12, live(7, false)),
            Some(ChangeKind::Update)
        );
        // An older write loses.
        assert_eq!(store.apply(key, 11, live(8, false)), None);
        let StoredMint::Mutable(entry) = store.get(&key) else {
            panic!("expected a mutable entry");
        };
        assert_eq!((entry.slot, entry.state.decimals), (12, 7));
        assert_eq!(store.mutable_len(), 1);
    }

    #[test]
    fn close_and_reinit_keep_a_tombstone_in_between() {
        let mut store = MintStore::default();
        let key = Pubkey::new_unique();
        store.apply(key, 10, live(6, false));
        assert_eq!(
            store.apply(key, 20, MintChange::Closed),
            Some(ChangeKind::Close)
        );
        assert_eq!(store.apply(key, 21, MintChange::Closed), None);
        // An older live write cannot bring the closed mint back.
        assert_eq!(store.apply(key, 15, live(6, false)), None);
        assert_eq!(store.get(&key), StoredMint::Tombstone { slot: 20 });
        assert_eq!((store.mutable_len(), store.tombstones()), (0, 1));

        // A re-init as an immutable mint moves the key to the immutable map.
        assert_eq!(
            store.apply(key, 30, live(2, true)),
            Some(ChangeKind::Reinit)
        );
        assert_eq!(store.get(&key), StoredMint::Immutable { decimals: 2 });
        assert_eq!((store.mutable_len(), store.tombstones()), (0, 0));
    }

    #[test]
    fn a_close_never_removes_an_immutable_mint() {
        let mut store = MintStore::default();
        let key = Pubkey::new_unique();
        store.apply(key, 30, live(6, true));
        assert_eq!(store.apply(key, 10, MintChange::Closed), None);
        assert_eq!(store.get(&key), StoredMint::Immutable { decimals: 6 });
    }

    #[test]
    fn closes_of_unknown_keys_are_ignored() {
        let mut store = MintStore::default();
        assert_eq!(
            store.apply(Pubkey::new_unique(), 10, MintChange::Closed),
            None
        );
        assert_eq!(store.tombstones(), 0);
    }

    #[test]
    fn a_tombstone_row_is_stored_for_an_unknown_key() {
        let mut store = MintStore::default();
        let key = Pubkey::new_unique();
        assert_eq!(store.apply_row(key, 20, None), Some(ChangeKind::Close));
        assert_eq!(store.apply_row(key, 10, Some(state(6, false))), None);
        assert_eq!(store.tombstones(), 1);
        assert_eq!(
            store.apply_row(key, 30, Some(state(6, false))),
            Some(ChangeKind::Reinit)
        );
    }

    #[test]
    fn a_close_seen_before_the_create_wins_when_its_slot_is_higher() {
        // Startup merge: the buffer applied a close, then the table load brings an older row.
        let mut store = MintStore::default();
        let key = Pubkey::new_unique();
        store.apply(key, 10, live(6, false));
        store.apply(key, 20, MintChange::Closed);
        assert_eq!(store.apply(key, 10, live(6, false)), None);
        assert_eq!(store.tombstones(), 1);
    }
}
