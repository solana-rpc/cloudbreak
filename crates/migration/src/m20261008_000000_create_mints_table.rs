// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The `mints` table of the mint cache: one row per mint, upserted by the indexer. Unlogged like
//! the account tables. A closed mint stays as a tombstone row (`closed = true`). See "Mint cache"
//! in the README.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE UNLOGGED TABLE IF NOT EXISTS mints (
                    pubkey                  BYTEA    PRIMARY KEY,
                    slot                    BIGINT   NOT NULL,
                    decimals                SMALLINT,
                    mutable                 BOOLEAN  NOT NULL,
                    closed                  BOOLEAN  NOT NULL DEFAULT false,
                    interest_bearing_config BYTEA,
                    scaled_ui_amount_config BYTEA
                );
                CREATE INDEX IF NOT EXISTS mints_slot_idx ON mints (slot);
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS mints;")
            .await?;

        Ok(())
    }
}
