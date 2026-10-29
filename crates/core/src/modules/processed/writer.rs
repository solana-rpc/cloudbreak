// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The single writer. It owns the block store without a lock. After every block or anchor
//! change it prunes, selects the latest chained blocks and publishes them.

use std::sync::Arc;
use std::time::Instant;

use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;

use super::ingest::SlotBlock;
use super::store::BlockStore;
use super::{Anchor, Shared};

/// A null-object handle (README "Optional modules"): the default is the disabled writer. It is the
/// only owner of the store, so it holds `Option<Inner>` without `Arc` and is not `Clone`.
#[derive(Default)]
pub struct ProcessedWriter(Option<Inner>);

struct Inner {
    shared: Arc<Shared>,
    store: BlockStore,
}

impl ProcessedWriter {
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self(Some(Inner {
            shared,
            store: BlockStore::new(),
        }))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// A processed block from the gRPC feed.
    pub fn on_block(&mut self, block: SubscribeUpdateBlock) {
        let Some(inner) = &mut self.0 else {
            return;
        };
        let block = SlotBlock::from_update(block, &inner.shared.program_filter, Instant::now());
        inner.store.on_block(block);
        inner.publish_latest();
    }

    /// The Postgres confirmed slot and blockhash from the slot syncronizer.
    pub fn set_anchor(&mut self, anchor: Anchor) {
        let Some(inner) = &mut self.0 else {
            return;
        };
        inner.store.set_anchor(anchor);
        inner.publish_latest();
    }
}

impl Inner {
    fn publish_latest(&mut self) {
        self.store.prune();
        self.shared
            .set_latest(self.store.latest_chained_blocks().map(Arc::new));
    }
}
