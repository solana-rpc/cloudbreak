// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Bounds how many full scans of big mints run at once. Each such scan reads every holder of the
//! mint through shared_buffers, so a fixed number of slots caps the buffer churn they cause.

use std::collections::HashSet;
use std::sync::Arc;

use cloudbreak_core::BigMintScansConfig;
use cloudbreak_core::modules::rpc_filter_type::RpcFilterType;
use cloudbreak_core::utils::token::{
    TOKEN_ACCOUNT_DELEGATE_OFFSET, TOKEN_ACCOUNT_MINT_OFFSET, TOKEN_ACCOUNT_OWNER_OFFSET,
    is_token_program,
};
use solana_pubkey::Pubkey;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The `[big-mint-scans]` handle. `None` inside means the section is off and nothing waits.
#[derive(Clone, Debug, Default)]
pub struct BigMintScans(Option<Arc<Inner>>);

#[derive(Debug)]
struct Inner {
    mints: HashSet<Pubkey>,
    slots: Arc<Semaphore>,
    max_parallel_workers: Option<u32>,
    bitmap_scan: bool,
}

impl BigMintScans {
    pub fn from_config(config: Option<&BigMintScansConfig>) -> Self {
        let Some(config) = config.filter(|config| config.enabled) else {
            return Self(None);
        };
        Self(Some(Arc::new(Inner {
            mints: config.mints.iter().map(|mint| mint.0).collect(),
            slots: Arc::new(Semaphore::new(config.max_concurrent.max(1))),
            max_parallel_workers: config.max_parallel_workers,
            bitmap_scan: config.bitmap_scan,
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// True for a token gPA on a listed mint without a 32-byte owner or delegate memcmp, the
    /// filters an index serves without reading every holder of the mint.
    pub fn is_big_scan(&self, program: &Pubkey, filters: &[RpcFilterType]) -> bool {
        let Some(inner) = &self.0 else {
            return false;
        };
        if !is_token_program(program) {
            return false;
        }
        let key_at = |offset: usize| {
            filters.iter().find_map(|filter| match filter {
                RpcFilterType::Memcmp(memcmp) if memcmp.offset() == offset => {
                    memcmp.bytes().filter(|bytes| bytes.len() == 32)
                }
                _ => None,
            })
        };
        let Some(mint) = key_at(TOKEN_ACCOUNT_MINT_OFFSET) else {
            return false;
        };
        let Ok(mint) = Pubkey::try_from(mint.as_slice()) else {
            return false;
        };
        inner.mints.contains(&mint)
            && key_at(TOKEN_ACCOUNT_OWNER_OFFSET).is_none()
            && key_at(TOKEN_ACCOUNT_DELEGATE_OFFSET).is_none()
    }

    /// Waits for a free slot. The permit frees it on drop.
    pub async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        let inner = self.0.as_ref()?;
        inner.slots.clone().acquire_owned().await.ok()
    }

    /// `set_config` arguments for the transaction that runs a big scan.
    pub fn planner_settings(&self) -> Vec<(&'static str, String)> {
        let Some(inner) = &self.0 else {
            return Vec::new();
        };
        let mut settings = Vec::new();
        if let Some(workers) = inner.max_parallel_workers {
            settings.push(("max_parallel_workers_per_gather", workers.to_string()));
        }
        if inner.bitmap_scan {
            settings.push(("enable_indexscan", "off".to_string()));
        }
        settings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudbreak_core::PubkeyDef;
    use cloudbreak_core::modules::rpc_filter_type::Memcmp;
    use cloudbreak_core::utils::token::LEGACY_TOKEN_PROGRAM_ID;

    const MINT: Pubkey = Pubkey::new_from_array([7; 32]);

    fn scans() -> BigMintScans {
        BigMintScans::from_config(Some(&BigMintScansConfig {
            enabled: true,
            mints: vec![PubkeyDef(MINT)],
            max_concurrent: 2,
            max_parallel_workers: Some(0),
            bitmap_scan: true,
        }))
    }

    fn memcmp(offset: usize, bytes: &[u8]) -> RpcFilterType {
        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(offset, bytes.to_vec()))
    }

    #[test]
    fn a_listed_mint_without_owner_or_delegate_is_a_big_scan() {
        let scans = scans();
        let program = LEGACY_TOKEN_PROGRAM_ID;
        let mint = memcmp(0, MINT.as_ref());
        assert!(scans.is_big_scan(&program, &[RpcFilterType::DataSize(165), mint.clone()]));
        // An owner prefix still reads every holder of the mint.
        assert!(scans.is_big_scan(&program, &[mint.clone(), memcmp(32, &[1, 2])]));
        assert!(!scans.is_big_scan(&program, &[mint.clone(), memcmp(32, &[1; 32])]));
        assert!(!scans.is_big_scan(&program, &[mint.clone(), memcmp(76, &[1; 32])]));
        assert!(!scans.is_big_scan(&program, &[memcmp(0, &[8; 32])]));
        assert!(!scans.is_big_scan(&Pubkey::new_unique(), &[mint]));
    }

    #[test]
    fn a_disabled_section_never_waits_or_changes_the_planner() {
        let scans = BigMintScans::from_config(None);
        let mint = memcmp(0, MINT.as_ref());
        assert!(!scans.is_big_scan(&LEGACY_TOKEN_PROGRAM_ID, &[mint]));
        assert!(scans.planner_settings().is_empty());
    }

    #[test]
    fn planner_settings_follow_the_config() {
        assert_eq!(
            scans().planner_settings(),
            vec![
                ("max_parallel_workers_per_gather", "0".to_string()),
                ("enable_indexscan", "off".to_string())
            ]
        );
    }
}
