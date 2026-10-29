// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm_migration::prelude::*;
use solana_pubkey::{pubkey, Pubkey};

#[derive(DeriveMigrationName)]
pub struct Migration;

const TOKEN_KEG_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const TOKEN_EXTENSIONS_ID: Pubkey = pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        let cfg = crate::migration_config();
        let indexes = &cfg.pg_indexes;

        let create_accounts_table_sql =
            crate::build_create_table_sql("accounts", &cfg.pg_owner_partitions);

        connection
            .execute_unprepared(&create_accounts_table_sql)
            .await?;

        if indexes.idx_accounts_pubkey {
            manager
                .create_index(
                    Index::create()
                        .name("idx_accounts_pubkey")
                        .table(Account::Table)
                        .index_type(IndexType::Hash)
                        .col(Account::Pubkey)
                        .to_owned(),
                )
                .await?;
        }

        if indexes.idx_accounts_token_mint {
            manager
                .create_index(
                    Index::create()
                        .name("idx_accounts_token_mint")
                        .table(Account::Table)
                        .col(Account::TokenMint)
                        .cond_where(
                            Condition::any()
                                .add(Expr::col(Account::Owner).eq(TOKEN_KEG_ID.as_ref()))
                                .add(Expr::col(Account::Owner).eq(TOKEN_EXTENSIONS_ID.as_ref())),
                        )
                        .to_owned(),
                )
                .await?;
        }

        if indexes.idx_accounts_token_owner {
            // The program is in the key: an owner's accounts in the other token program are not read.
            manager
                .create_index(
                    Index::create()
                        .name("idx_accounts_token_owner")
                        .table(Account::Table)
                        .col(Account::TokenOwner)
                        .col(Account::Owner)
                        .cond_where(
                            Condition::any()
                                .add(Expr::col(Account::Owner).eq(TOKEN_KEG_ID.as_ref()))
                                .add(Expr::col(Account::Owner).eq(TOKEN_EXTENSIONS_ID.as_ref())),
                        )
                        .to_owned(),
                )
                .await?;
        }

        if indexes.idx_accounts_pubkey_slot {
            // Used for the insertClosedAccount query (for looking for the latest version of the account)
            connection
                .execute_unprepared(
                    r#"
                    CREATE INDEX idx_accounts_pubkey_slot ON accounts (pubkey, slot DESC);
                "#,
                )
                .await?;
        }

        if indexes.idx_accounts_token_delegate {
            connection.execute_unprepared(
                    r#"
                        CREATE INDEX idx_accounts_token_delegate
                        ON accounts (SUBSTRING(data FROM 77 FOR 32))
                        WHERE (owner = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea
                            OR owner = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea)
                        AND SUBSTRING(data FROM 73 FOR 1) = '\x01'::bytea;
                    "#
                )
                .await?;
        }

        connection
            .execute_unprepared(
                r#"
                DO $$
                BEGIN
                    CREATE EXTENSION IF NOT EXISTS pg_tracing;
                EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'pg_tracing extension not available (optional, skipping): %', SQLERRM;
                END
                $$;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_pubkey")
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_slot")
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_token_mint")
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_token_owner")
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_pubkey_slot")
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_token_delegate")
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(Account::Table).if_exists().to_owned())
            .await?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Iden)]
enum Account {
    #[iden = "accounts"]
    Table,
    Pubkey,
    Owner,
    Lamports,
    Slot,
    Executable,
    RentEpoch,
    Data,
    WriteVersion,
    UpdatedOn,
    TxnSignature,
    TokenMint,
    TokenOwner,
}
