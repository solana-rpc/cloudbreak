// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Mint data for `jsonParsed` token accounts in a gPA,
//! each account is encoded with its own mint, read by pubkey only, and an account that does not
//! parse is left out. The caller builds the resolver; it remembers every mint it resolves, so a
//! mint costs one lookup per request: the mint cache (`[mint-cache]`) when it can answer,
//! otherwise one Postgres query per batch. It also records whether every account used an immutable
//! mint and parsed, so the response only changes when its accounts change but it can't be affected
//! by changes to the mint account.
//! Queries are marked as volatile inside [`MintResolver::get_mint_data`] on mutable or missing mints,
//! or inside [`MintResolver::should_drop`] in case a new account is dropped because of failed JSON parsing.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use sea_orm::sqlx::{self, Row, postgres::PgRow};
use solana_account_decoder::UiAccountData;
use solana_account_decoder::UiAccountEncoding;
use solana_account_decoder::parse_account_data::AccountAdditionalDataV3;
use solana_account_decoder::parse_token::get_token_account_mint;
use solana_pubkey::Pubkey;
use tokio::time::timeout;
use tracing::Instrument;

use crate::db_query::{self, bytea_array_literal};
use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::modules::cache::MaybeJsonAccount;
use cloudbreak_core::utils::token::{MintState, is_token_program};

/// The data column of the gPA SQL rows.
const DATA_COLUMN: usize = 6;

/// Why the response can change while its accounts do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Volatility {
    /// An account uses a mint whose parsed fields can change, or a missing mint.
    MutableMint,
    /// An account did not parse and was left out of the response.
    DroppedAccount,
}

impl Volatility {
    pub fn label(self) -> &'static str {
        match self {
            Self::MutableMint => "mutable_mint",
            Self::DroppedAccount => "dropped_account",
        }
    }
}

/// Cheap-clone handle. The disabled resolver (any request that is not a token-program
/// `jsonParsed` gPA) does nothing.
#[derive(Clone, Default)]
pub struct MintResolver(Option<Arc<Inner>>);

struct Inner {
    state: CloudbreakRpcState,
    /// The gPA slot and block time, set once by the gPA.
    at: OnceLock<(u64, i64)>,
    /// Mints found so far. A mint that does not exist at the slot has no entry.
    mints: Mutex<HashMap<Pubkey, MintState>>,
    /// `None` while every account used an immutable mint and parsed.
    volatility: Mutex<Option<Volatility>>,
}

impl std::fmt::Debug for MintResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MintResolver")
            .field("enabled", &self.0.is_some())
            .finish()
    }
}

impl MintResolver {
    /// Enabled for a `jsonParsed` gPA on a token program, disabled otherwise.
    pub fn new(
        state: &CloudbreakRpcState,
        program: &str,
        encoding: Option<UiAccountEncoding>,
    ) -> Self {
        let token_program = program
            .parse::<Pubkey>()
            .is_ok_and(|p| is_token_program(&p));
        if !token_program || encoding != Some(UiAccountEncoding::JsonParsed) {
            return Self::default();
        }
        Self(Some(Arc::new(Inner {
            state: state.clone(),
            at: OnceLock::new(),
            mints: Mutex::new(HashMap::new()),
            volatility: Mutex::new(None),
        })))
    }

    /// Adds a mint the caller already read, so the gPA never looks it up.
    pub fn with_mint(self, mint: Pubkey, owner: Pubkey, data: Vec<u8>) -> Self {
        if let Some(inner) = &self.0
            && let Some(state) = MintState::from_mint(&mint, &data)
        {
            inner
                .mints
                .lock()
                .expect("mint resolver mutex poisoned")
                .insert(mint, state.classify(&owner));
        }
        self
    }

    /// The gPA slot: batch lookups read mints at it, configs are evaluated at its block time.
    pub fn set_slot(&self, slot: u64, block_time: i64) {
        if let Some(inner) = &self.0 {
            let _ = inner.at.set((slot, block_time));
        }
    }

    /// Resolves the mints of the batch's fresh token accounts that are not resolved yet: from
    /// the mint cache when it can answer, otherwise in one Postgres query by pubkey only.
    pub async fn resolve_mints(&self, rows: &[PgRow]) -> Result<(), RpcError> {
        let Some(inner) = &self.0 else {
            return Ok(());
        };
        let native_mint = spl_token_interface::native_mint::id();
        let new_mint_keys: Vec<Pubkey> = {
            let mints = inner.mints.lock().expect("mint resolver mutex poisoned");
            rows.iter()
                .filter_map(get_mint_from_token_account)
                .filter(|mint| *mint != native_mint && !mints.contains_key(mint))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        };

        // Use the same slot the gPA is operating at for consistency.
        let (slot, _) = inner.slot_and_block_time();

        let new_mint_keys = inner
            .state
            .mint_cache
            .api()
            .resolve(new_mint_keys, slot, &inner.mints);
        if new_mint_keys.is_empty() {
            return Ok(());
        }

        let sql = include_str!("../db/getMultipleAccounts.sql")
            .replace("$1", &bytea_array_literal(&new_mint_keys))
            .replace("$2", &slot.to_string());
        let sql = db_query::add_trace_traceparent_to_query(&sql);
        let pool = inner.state.database.get_postgres_connection_pool();
        let span = tracing::info_span!("mint_db", mints = new_mint_keys.len());
        let rows = timeout(
            inner.state.queries_timeout,
            sqlx::raw_sql(&sql).fetch_all(pool).instrument(span),
        )
        .await
        .map_err(|_elapsed| {
            tracing::error!("Mint lookup timed out");
            RpcError::InternalError
        })?
        .map_err(|e| {
            tracing::error!("Mint lookup query error: {e}");
            RpcError::InternalError
        })?;

        let mut mints = inner.mints.lock().expect("mint resolver mutex poisoned");
        for row in rows {
            // `raw_sql` returns text-format columns, which sqlx cannot decode into `&[u8]`.
            let (Ok(pubkey), Ok(owner), Ok(data)) = (
                row.try_get::<[u8; 32], _>("pubkey"),
                row.try_get::<[u8; 32], _>("owner"),
                row.try_get::<Vec<u8>, _>("data"),
            ) else {
                tracing::error!("Mint lookup returned a row without pubkey, owner or data");
                return Err(RpcError::InternalError);
            };
            let pubkey = Pubkey::new_from_array(pubkey);
            if let Some(state) = MintState::from_mint(&pubkey, &data) {
                mints.insert(pubkey, state.classify(&Pubkey::new_from_array(owner)));
            }
        }
        Ok(())
    }

    /// The mint data to encode a fresh token account with. `None` for a cache-hit row, an
    /// account that is not a token account, and a missing or unparsable mint (as in Agave, the
    /// account then has no decimals). A mutable or missing mint marks the complete query response volatile.
    pub fn get_mint_data(&self, row: &PgRow) -> Option<AccountAdditionalDataV3> {
        let inner = self.0.as_ref()?;
        let mint = get_mint_from_token_account(row)?;
        let (_, block_time) = inner.slot_and_block_time();
        if mint == spl_token_interface::native_mint::id() {
            return MintState::from_mint(&mint, &[]).map(|state| state.additional_data(block_time));
        }
        let mints = inner.mints.lock().expect("mint resolver mutex poisoned");
        let additional = mints.get(&mint).map(|state| {
            if !state.immutable {
                inner.mark_as_volatile(Volatility::MutableMint);
            }
            state.additional_data(block_time)
        });
        if additional.is_none() {
            inner.mark_as_volatile(Volatility::MutableMint);
        }
        additional
    }

    /// True when account is not correctly parsed as `jsonParsed`, which means there was an issue
    /// with the additional mint data.
    pub fn should_drop(&self, account: &MaybeJsonAccount) -> bool {
        // `inner` being Some already means the request is jsonParsed.
        let Some(inner) = &self.0 else {
            return false;
        };
        let dropped = match account {
            MaybeJsonAccount::Fresh(keyed) => {
                // Correctly parsed as `jsonParsed`
                !matches!(keyed.account.account.data, UiAccountData::Json(_))
            }
            MaybeJsonAccount::Cached { .. } => false, // If it's cached it already means it was correctly parsed as `jsonParsed`
        };

        if dropped {
            inner.mark_as_volatile(Volatility::DroppedAccount);
        }
        dropped
    }

    /// `None` when every `jsonParsed` token account used an immutable mint and parsed, so the
    /// response only changes when its accounts change. Otherwise the first reason it may not.
    pub fn volatility(&self) -> Option<Volatility> {
        *self
            .0
            .as_ref()?
            .volatility
            .lock()
            .expect("mint resolver mutex poisoned")
    }
}

impl Inner {
    /// Saved with [`MintResolver::set_slot`] using the values the gPA uses for the main query.
    fn slot_and_block_time(&self) -> (u64, i64) {
        *self
            .at
            .get()
            .expect("gPA sets the mint resolver slot first")
    }

    /// The first reason wins.
    fn mark_as_volatile(&self, reason: Volatility) {
        self.volatility
            .lock()
            .expect("mint resolver mutex poisoned")
            .get_or_insert(reason);
    }
}

/// It parses the Token account data, returning the mint `Pubkey` if available.
fn get_mint_from_token_account(row: &PgRow) -> Option<Pubkey> {
    // gPA rows are binary, so the data is read in place from the row buffer.
    let data: Option<&[u8]> = row.try_get(DATA_COLUMN).ok().flatten();
    get_token_account_mint(data?)
}
