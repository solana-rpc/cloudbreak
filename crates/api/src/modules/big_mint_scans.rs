// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Bounds how many full scans of big mints run at once

use std::collections::HashSet;
use std::sync::Arc;

use crate::metrics::InFlightRequestGuard;
use cloudbreak_core::BigMintScansConfig;
use cloudbreak_core::modules::rpc_filter_type::RpcFilterType;
use cloudbreak_core::utils::token::{
    TOKEN_ACCOUNT_DELEGATE_OFFSET, TOKEN_ACCOUNT_MINT_OFFSET, TOKEN_ACCOUNT_OWNER_OFFSET,
    is_token_program,
};
use sea_orm::sqlx::postgres::{PgConnection, PgPool};
use sea_orm::sqlx::{self, Postgres, Transaction};
use solana_pubkey::Pubkey;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The `[big-mint-scans]` handle. `None` inside means the section is off and nothing waits.
#[derive(Clone, Debug, Default)]
pub struct BigMintScans(Option<Arc<Inner>>);

#[derive(Debug)]
struct Inner {
    mints: HashSet<Pubkey>,
    current_parallel: Arc<Semaphore>,
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
            current_parallel: Arc::new(Semaphore::new(config.max_concurrent.max(1))),
            max_parallel_workers: config.max_parallel_workers,
            bitmap_scan: config.bitmap_scan,
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// True for a token gPA on a listed mint without an owner memcmp (32 bytes or a prefix) or a
    /// 32-byte delegate memcmp, the filters an index serves without reading every holder.
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
        // `SqlFilters` turns these into `token_owner` equality or a `token_owner` range.
        let has_owner = filters.iter().any(|filter| {
            matches!(filter, RpcFilterType::Memcmp(memcmp)
                if memcmp.offset() == TOKEN_ACCOUNT_OWNER_OFFSET
                    && memcmp.bytes().is_some_and(|bytes| !bytes.is_empty() && bytes.len() <= 32))
        });
        inner.mints.contains(&mint) && !has_owner && key_at(TOKEN_ACCOUNT_DELEGATE_OFFSET).is_none()
    }

    /// Starts the DB phase of a request. For a big scan it waits for a permit, then opens the
    /// transaction that carries the planner settings. `None` for every other request.
    pub async fn begin(
        &self,
        pool: &PgPool,
        program: &Pubkey,
        filters: &[RpcFilterType],
    ) -> Result<Option<BigMintScan>, sqlx::Error> {
        if !self.is_big_scan(program, filters) {
            return Ok(None);
        }
        let _permit = {
            let _waiting = InFlightRequestGuard::new("big_mint_scan_waiting");
            self.acquire().await
        };
        let _running = InFlightRequestGuard::new("big_mint_scan_running");
        let transaction = begin_with_settings(pool, self.planner_settings()).await?;
        Ok(Some(BigMintScan {
            transaction,
            _running,
            _permit,
        }))
    }

    /// Waits for a `current_parallel` permit. The permit goes back on drop.
    async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        let inner = self.0.as_ref()?;
        inner.current_parallel.clone().acquire_owned().await.ok()
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

/// A running big scan: its permit, its gauge and the transaction that scopes the planner settings.
/// Nothing commits: the query only reads and the drop rolls back. See `[big-mint-scans]` in README.
pub struct BigMintScan {
    transaction: Transaction<'static, Postgres>,
    _running: InFlightRequestGuard,
    _permit: Option<OwnedSemaphorePermit>,
}

impl BigMintScan {
    /// The connection of the transaction that carries the planner settings.
    pub fn connection(&mut self) -> &mut PgConnection {
        &mut self.transaction
    }
}

/// Opens a transaction and applies `settings` to it only (`set_config(.., true)` = `SET LOCAL`).
/// They end with the transaction, so no other request on the pooled connection runs with them.
async fn begin_with_settings(
    pool: &PgPool,
    settings: Vec<(&'static str, String)>,
) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    for (name, value) in settings {
        sqlx::query("SELECT set_config($1, $2, true)")
            .bind(name)
            .bind(value)
            .execute(&mut *transaction)
            .await?;
    }
    Ok(transaction)
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
        // An owner filter, exact or prefix, reads its slice of the owner index.
        assert!(!scans.is_big_scan(&program, &[mint.clone(), memcmp(32, &[1; 32])]));
        assert!(!scans.is_big_scan(&program, &[mint.clone(), memcmp(32, &[1])]));
        assert!(!scans.is_big_scan(&program, &[mint.clone(), memcmp(32, &[1; 31])]));
        // Longer values and other offsets stay byte comparisons over every holder.
        assert!(scans.is_big_scan(&program, &[mint.clone(), memcmp(32, &[1; 40])]));
        assert!(scans.is_big_scan(&program, &[mint.clone(), memcmp(33, &[1, 2])]));
        assert!(scans.is_big_scan(&program, &[mint.clone(), memcmp(76, &[1, 2])]));
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
