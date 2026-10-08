// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Translates gPA filters into SQL predicates on `accounts` and `snapshot_accounts`.

use cloudbreak_core::modules::rpc_filter_type::{Memcmp, RpcFilterType};
use solana_pubkey::Pubkey;

use crate::db_query::bytea_literal;
use crate::error::RpcError;
use crate::utils::token::{
    TOKEN_ACCOUNT_DELEGATE_OFFSET, TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET, TOKEN_ACCOUNT_MINT_OFFSET,
    TOKEN_ACCOUNT_OWNER_OFFSET, is_token_program,
};

/// The SQL filter clauses of one gPA, for both tables, and its metric label.
#[derive(Debug, PartialEq, Eq)]
pub struct SqlFilters {
    pub accounts: String,
    pub snapshot: String,
    pub label: String,
}

/// The indexed key a token-program gPA filters on. The first one present, in this order, names
/// the label suffix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenKey {
    Mint,
    Delegate,
    Owner,
}

impl SqlFilters {
    /// Verifies the filters and translates them to SQL.
    ///
    /// On a token program the query must filter on an indexed key: a 32-byte memcmp on the mint,
    /// the delegate or the owner. Those memcmps and the delegate tag become the token index
    /// expressions. A delegate filter without a tag gets the tag (known difference, see the
    /// README). A `gpa` label gets the key as suffix (`gpa_mint`, `gpa_delegate`, `gpa_token`);
    /// any other label is kept.
    pub fn build(
        program: &Pubkey,
        filters: &[RpcFilterType],
        label: &str,
    ) -> Result<Self, RpcError> {
        for filter in filters {
            filter.verify().map_err(|e| {
                RpcError::InvalidParamsWithMessage(format!(
                    "Invalid param: {}",
                    e.invalid_param_text()
                ))
            })?;
        }

        let is_token_program = is_token_program(program);
        let mut label = label.to_string();
        let mut add_delegate_tag = false;
        if is_token_program {
            let key = is_valid_token_query(filters)?;
            if label == "gpa" {
                label.push_str(match key {
                    TokenKey::Mint => "_mint",
                    TokenKey::Delegate => "_delegate",
                    TokenKey::Owner => "_token",
                });
            }
            add_delegate_tag = has_memcmp(filters, TOKEN_ACCOUNT_DELEGATE_OFFSET, Some(32))
                && !has_memcmp(filters, TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET, None);
        }

        let delegate_tag = RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET,
            vec![1, 0, 0, 0],
        ));
        let clauses = |table: &str| {
            filters
                .iter()
                .chain(add_delegate_tag.then_some(&delegate_tag))
                .filter_map(|filter| filter_sql(filter, table, is_token_program))
                .map(|clause| format!("AND {clause}"))
                .collect::<Vec<_>>()
                .join("\n                    ")
        };

        Ok(Self {
            accounts: clauses("accounts"),
            snapshot: clauses("snapshot_accounts"),
            label,
        })
    }
}

/// The indexed key of a token-program query, or `InvalidParams` without one.
fn is_valid_token_query(filters: &[RpcFilterType]) -> Result<TokenKey, RpcError> {
    let mut keys = Vec::new();
    for filter in filters {
        if let RpcFilterType::Memcmp(memcmp) = filter {
            let bytes = memcmp.bytes().ok_or(RpcError::InvalidParams)?;
            if bytes.len() != 32 {
                continue;
            }
            keys.extend(match memcmp.offset() {
                TOKEN_ACCOUNT_MINT_OFFSET => Some(TokenKey::Mint),
                TOKEN_ACCOUNT_DELEGATE_OFFSET => Some(TokenKey::Delegate),
                TOKEN_ACCOUNT_OWNER_OFFSET => Some(TokenKey::Owner),
                _ => None,
            });
        }
    }
    [TokenKey::Mint, TokenKey::Delegate, TokenKey::Owner]
        .into_iter()
        .find(|key| keys.contains(key))
        .ok_or(RpcError::InvalidParams)
}

/// True when a memcmp sits at `offset`, of `len` bytes when given.
fn has_memcmp(filters: &[RpcFilterType], offset: usize, len: Option<usize>) -> bool {
    filters.iter().any(|filter| {
        matches!(filter, RpcFilterType::Memcmp(memcmp)
            if memcmp.offset() == offset
                && memcmp.bytes().is_some_and(|b| len.is_none_or(|len| b.len() == len)))
    })
}

/// One filter as a SQL predicate on `table`. For a token program, owner, mint and delegate-tag
/// memcmps become the exact expressions the token indexes are built on.
fn filter_sql(filter: &RpcFilterType, table: &str, token_program: bool) -> Option<String> {
    match filter {
        RpcFilterType::DataSize(size) => Some(format!("length({table}.data) = {size}")),
        RpcFilterType::Memcmp(memcmp) => {
            let bytes = memcmp.bytes()?.to_vec();
            let offset = memcmp.offset();

            if token_program {
                if offset == TOKEN_ACCOUNT_OWNER_OFFSET && bytes.len() == 32 {
                    return Some(format!("{table}.token_owner = {}", bytea_literal(bytes)));
                } else if offset == TOKEN_ACCOUNT_MINT_OFFSET && bytes.len() == 32 {
                    return Some(format!("{table}.token_mint = {}", bytea_literal(bytes)));
                } else if offset == TOKEN_ACCOUNT_DELEGATE_TAG_OFFSET && bytes == [1, 0, 0, 0] {
                    // The first byte alone is the predicate of the token delegate partial index.
                    return Some(format!(
                        "SUBSTRING({table}.data FROM 73 FOR 1) = '\\x01'::bytea AND SUBSTRING({table}.data FROM 74 FOR 3) = '\\x000000'::bytea"
                    ));
                }
            }

            Some(format!(
                "SUBSTRING({table}.data FROM {} FOR {}) = E'\\\\x{}'::bytea",
                offset + 1,
                bytes.len(),
                hex::encode(bytes)
            ))
        }
        // Agave's `spl_token_2022_interface::state::Account::valid_account_data`: byte 108 is the
        // account state (0 = uninitialized), a 355-byte account is a multisig.
        RpcFilterType::TokenAccountState => Some(format!(
            "(SUBSTRING({table}.data FROM 109 FOR 1) <> '\\x00'::bytea AND (length({table}.data) = 165 OR (length({table}.data) > 165 AND length({table}.data) <> 355 AND SUBSTRING({table}.data FROM 166 FOR 1) = '\\x02'::bytea)))"
        )),
        RpcFilterType::ValueCmp(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::token::LEGACY_TOKEN_PROGRAM_ID;

    fn sql(filter: RpcFilterType, token_program: bool) -> String {
        filter_sql(&filter, "accounts", token_program).unwrap()
    }

    fn memcmp(offset: usize, bytes: &[u8]) -> RpcFilterType {
        RpcFilterType::Memcmp(Memcmp::new_raw_bytes(offset, bytes.to_vec()))
    }

    #[test]
    fn token_owner_and_mint_memcmps_use_the_indexed_columns() {
        let key = [7u8; 32];
        let hex = hex::encode(key);
        assert_eq!(
            sql(memcmp(32, &key), true),
            format!("accounts.token_owner = '\\x{hex}'::bytea")
        );
        assert_eq!(
            sql(memcmp(0, &key), true),
            format!("accounts.token_mint = '\\x{hex}'::bytea")
        );
    }

    #[test]
    fn delegate_tag_memcmp_matches_the_delegate_index_predicate() {
        assert_eq!(
            sql(memcmp(72, &[1, 0, 0, 0]), true),
            "SUBSTRING(accounts.data FROM 73 FOR 1) = '\\x01'::bytea AND SUBSTRING(accounts.data FROM 74 FOR 3) = '\\x000000'::bytea"
        );
        // Any other shape at 72 stays a plain byte comparison.
        assert_eq!(
            sql(memcmp(72, &[1]), true),
            "SUBSTRING(accounts.data FROM 73 FOR 1) = E'\\\\x01'::bytea"
        );
    }

    #[test]
    fn delegate_memcmp_uses_the_delegate_index_expression() {
        let key = [9u8; 32];
        assert_eq!(
            sql(memcmp(76, &key), true),
            format!(
                "SUBSTRING(accounts.data FROM 77 FOR 32) = E'\\\\x{}'::bytea",
                hex::encode(key)
            )
        );
    }

    #[test]
    fn non_token_programs_keep_plain_byte_comparisons() {
        let key = [7u8; 32];
        let hex = hex::encode(key);
        assert_eq!(
            sql(memcmp(32, &key), false),
            format!("SUBSTRING(accounts.data FROM 33 FOR 32) = E'\\\\x{hex}'::bytea")
        );
        assert_eq!(
            sql(memcmp(72, &[1, 0, 0, 0]), false),
            "SUBSTRING(accounts.data FROM 73 FOR 4) = E'\\\\x01000000'::bytea"
        );
    }

    #[test]
    fn token_account_state_matches_agave_valid_account_data() {
        assert_eq!(
            sql(RpcFilterType::TokenAccountState, true),
            "(SUBSTRING(accounts.data FROM 109 FOR 1) <> '\\x00'::bytea AND (length(accounts.data) = 165 OR (length(accounts.data) > 165 AND length(accounts.data) <> 355 AND SUBSTRING(accounts.data FROM 166 FOR 1) = '\\x02'::bytea)))"
        );
    }

    fn label(filters: Vec<RpcFilterType>, label: &str) -> Result<String, RpcError> {
        SqlFilters::build(&LEGACY_TOKEN_PROGRAM_ID, &filters, label).map(|sql| sql.label)
    }

    #[test]
    fn a_gpa_label_gets_the_token_key_as_suffix() {
        let key = vec![7; 32];
        assert_eq!(
            label(vec![memcmp(0, &key), memcmp(32, &key)], "gpa").unwrap(),
            "gpa_mint"
        );
        assert_eq!(
            label(vec![memcmp(76, &key), memcmp(32, &key)], "gpa").unwrap(),
            "gpa_delegate"
        );
        assert_eq!(label(vec![memcmp(32, &key)], "gpa").unwrap(), "gpa_token");
        assert_eq!(label(vec![memcmp(0, &key)], "gtabm").unwrap(), "gtabm");
    }

    #[test]
    fn a_token_query_without_an_indexed_key_is_rejected() {
        for filters in [
            vec![],
            vec![memcmp(64, &[7; 32])],
            vec![memcmp(32, &[7; 8])],
            vec![RpcFilterType::TokenAccountState],
        ] {
            assert!(matches!(
                label(filters, "gpa"),
                Err(RpcError::InvalidParams)
            ));
        }
    }

    #[test]
    fn a_delegate_filter_without_tag_gets_the_tag_in_sql() {
        let delegate = memcmp(76, &[9; 32]);
        let tagged = SqlFilters::build(
            &LEGACY_TOKEN_PROGRAM_ID,
            std::slice::from_ref(&delegate),
            "gpa",
        )
        .unwrap();
        assert!(
            tagged
                .accounts
                .contains("SUBSTRING(accounts.data FROM 73 FOR 1) = '\\x01'::bytea")
        );
        assert!(
            tagged
                .snapshot
                .contains("SUBSTRING(snapshot_accounts.data FROM 73 FOR 1) = '\\x01'::bytea")
        );

        let own_tag = SqlFilters::build(
            &LEGACY_TOKEN_PROGRAM_ID,
            &[delegate, memcmp(72, &[1])],
            "gpa",
        )
        .unwrap();
        assert_eq!(own_tag.accounts.matches("FROM 73 FOR 1").count(), 1);
    }

    #[test]
    fn other_programs_keep_their_filters_and_label() {
        let sql = SqlFilters::build(&Pubkey::new_unique(), &[memcmp(32, &[7; 32])], "gpa").unwrap();
        assert_eq!(sql.label, "gpa");
        assert_eq!(
            sql.accounts,
            format!(
                "AND SUBSTRING(accounts.data FROM 33 FOR 32) = E'\\\\x{}'::bytea",
                hex::encode([7; 32])
            )
        );
        assert!(
            SqlFilters::build(&Pubkey::new_unique(), &[], "gpa")
                .unwrap()
                .accounts
                .is_empty()
        );
    }
}
