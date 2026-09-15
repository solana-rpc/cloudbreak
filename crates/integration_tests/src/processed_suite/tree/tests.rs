// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Unit tests for the block tree, and helpers other modules' tests reuse.

use super::*;

pub fn key(n: u8) -> Pubkey {
    Pubkey::new_from_array([n; 32])
}

/// Owner id of `key(200)` in a tree built by [`tree`].
pub const OWNER_ID: u32 = 3;

/// A block with hash `h{slot}{tag}` whose parent is `h{parent}{parent_tag}`.
pub fn block(slot: u64, tag: &str, parent: (u64, &str), touched: &[(u8, u64)]) -> Block {
    let mut touched: Vec<Touch> = touched
        .iter()
        .map(|(k, l)| Touch {
            key: key(*k),
            lamports: *l,
            owner: OWNER_ID,
        })
        .collect();
    touched.sort_by_key(|t| t.key);
    Block {
        slot,
        blockhash: format!("h{slot}{tag}"),
        parent_slot: parent.0,
        parent_blockhash: format!("h{}{}", parent.0, parent.1),
        received_at: Instant::now(),
        touched: touched.into(),
    }
}

pub fn tree(blocks: Vec<Block>) -> BlockTree {
    let mut tree = BlockTree::default();
    assert_eq!(tree.intern(key(200)), OWNER_ID);
    blocks.into_iter().for_each(|b| _ = tree.insert(b));
    tree
}

fn forks(events: &[Event]) -> Vec<u64> {
    let forks = events.iter().filter_map(|e| match e {
        Event::Fork { fork_point } => Some(*fork_point),
        _ => None,
    });
    forks.collect()
}

fn chain(slots: &[(u64, &str, (u64, &str))]) -> Vec<Block> {
    slots
        .iter()
        .map(|(s, t, p)| block(*s, t, *p, &[]))
        .collect()
}

#[test]
fn ancestry_requires_blockhash_match() {
    let t = tree(chain(&[
        (10, "", (9, "")),
        (11, "", (10, "")),
        (12, "", (11, "x")),
    ]));
    assert!(t.is_ancestor(10, 11) && t.is_ancestor(11, 11));
    assert!(!t.is_ancestor(11, 12) && !t.is_ancestor(10, 12));
    assert!(!t.is_ancestor(12, 11));
}

#[test]
fn fork_event_on_second_child() {
    let mut t = tree(chain(&[(10, "", (9, "")), (11, "", (10, ""))]));
    let events = t.insert(block(12, "b", (10, ""), &[]));
    assert_eq!(forks(&events), [10]);
    assert!(t.insert(block(12, "b", (10, ""), &[])).is_empty());
}

#[test]
fn fork_event_once_per_fork_point() {
    let mut t = tree(chain(&[
        (10, "", (9, "")),
        (11, "", (10, "")),
        (12, "", (11, "")),
        (13, "", (12, "")),
        (14, "", (13, "")),
    ]));
    assert_eq!(forks(&t.insert(block(12, "b", (11, ""), &[]))), [11]);
    assert!(forks(&t.insert(block(15, "b", (12, "b"), &[]))).is_empty());
    // Each new block of the lower tip is off the tip, but the fork point is known.
    assert!(forks(&t.insert(block(15, "", (14, ""), &[]))).is_empty());
    assert!(forks(&t.insert(block(16, "", (15, ""), &[]))).is_empty());
    assert!(forks(&t.insert(block(17, "b", (15, "b"), &[]))).is_empty());
    assert!(forks(&t.insert(block(17, "", (16, ""), &[]))).is_empty());
    assert_eq!(forks(&t.insert(block(18, "c", (16, ""), &[]))), [16]);
}

#[test]
fn dead_descendant() {
    let mut t = tree(chain(&[
        (10, "", (9, "")),
        (11, "", (10, "")),
        (12, "", (11, "")),
        (13, "b", (10, "")),
    ]));
    t.mark_dead(11);
    assert!(t.dead_chain(11) && t.dead_chain(12));
    assert!(!t.dead_chain(10) && !t.dead_chain(13));
    assert_eq!(t.canonical(11), Some(false));
}

#[test]
fn dead_slot_repaired_by_confirmation() {
    let mut t = tree(chain(&[
        (10, "", (9, "")),
        (11, "", (10, "")),
        (12, "", (11, "")),
        (13, "", (12, "")),
    ]));
    t.mark_dead(11);
    assert!(t.dead_chain(12));
    t.confirm(13);
    assert_eq!(t.canonical(11), Some(true));
    assert!(!t.dead_chain(11) && !t.dead_chain(12) && !t.dead_chain(13));

    let mut direct = tree(chain(&[(10, "", (9, "")), (11, "", (10, ""))]));
    direct.mark_dead(10);
    direct.confirm(10);
    assert_eq!(direct.canonical(10), Some(true));
    assert!(!direct.dead_chain(11));
}

#[test]
fn restarted_slot_reads_dead_until_confirmed() {
    let mut t = tree(chain(&[
        (10, "", (9, "")),
        (11, "", (10, "")),
        (12, "", (11, "")),
    ]));
    assert!(!t.on_created_bank(11));
    assert!(!t.dead_chain(12));
    assert!(t.on_created_bank(11));
    assert!(!t.on_created_bank(11));
    assert!(t.dead_chain(11) && t.dead_chain(12));
    t.new_session();
    assert!(!t.on_created_bank(12));
    t.confirm(12);
    assert!(!t.dead_chain(11) && !t.dead_chain(12));
}

#[test]
fn canonical_states() {
    let mut t = tree(chain(&[
        (10, "", (9, "")),
        (11, "", (10, "")),
        (12, "b", (10, "")),
        (13, "", (11, "")),
    ]));
    assert_eq!((t.canonical(11), t.canonical(12)), (None, None));
    t.confirm(13);
    assert_eq!(t.canonical(10), Some(true));
    assert_eq!(t.canonical(11), Some(true));
    assert_eq!(t.canonical(12), Some(false));
    assert_eq!(t.canonical(13), Some(true));
    assert_eq!(t.canonical(14), None);
}

#[test]
fn lamports_at_along_chain_and_across_fork() {
    let t = tree(vec![
        block(10, "", (9, ""), &[(1, 100), (2, 7)]),
        block(11, "", (10, ""), &[(1, 110)]),
        block(12, "", (11, ""), &[(3, 5)]),
        block(13, "b", (10, ""), &[(1, 90)]),
    ]);
    let owner = key(200);
    assert_eq!(t.lamports_at(&key(1), 12), Some((110, owner)));
    assert_eq!(t.lamports_at(&key(1), 10), Some((100, owner)));
    assert_eq!(t.lamports_at(&key(1), 13), Some((90, owner)));
    assert_eq!(t.lamports_at(&key(2), 12), Some((7, owner)));
    assert_eq!(t.lamports_at(&key(9), 12), None);
    assert_eq!(t.write_between(&key(1), 10, 12), Some(Some((110, owner))));
    assert_eq!(t.write_between(&key(2), 10, 12), Some(None));
    assert_eq!(t.write_between(&key(9), 8, 12), None);
    assert_eq!(t.write_between(&key(9), 9, 12), Some(None));
}

#[test]
fn duplicate_slot_leaves_keys_uncovered() {
    let t = tree(vec![
        block(10, "", (9, ""), &[(1, 100)]),
        block(11, "", (10, ""), &[(1, 110)]),
        block(11, "d", (10, ""), &[(1, 120)]),
        block(12, "", (11, ""), &[(2, 5)]),
    ]);
    assert_eq!(t.lamports_at(&key(1), 11), None);
    assert_eq!(t.lamports_at(&key(1), 12), None);
    assert_eq!(t.lamports_at(&key(2), 12), Some((5, key(200))));
    assert_eq!(t.write_between(&key(1), 10, 12), None);
    assert_eq!(t.lamports_at(&key(1), 10), Some((100, key(200))));
}

#[test]
fn prune_bounds_blocks_and_slot_sets() {
    let blocks = (10..40).map(|s| block(s, "", (s - 1, ""), &[])).collect();
    let mut t = tree(blocks);
    (0..40).for_each(|s| _ = t.confirm(s));
    t.prune(Instant::now(), Duration::from_secs(600), 10);
    assert_eq!((t.len(), t.lowest_slot()), (11, Some(29)));
    assert_eq!(t.confirmed.first(), Some(&29));
    let mut slots_only = BlockTree::default();
    (0..1000).for_each(|s| _ = slots_only.confirm(s));
    slots_only.prune(Instant::now(), Duration::from_secs(600), 10);
    assert_eq!(slots_only.confirmed.len(), 11);
}
