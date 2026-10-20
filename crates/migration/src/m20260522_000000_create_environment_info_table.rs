// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();

        connection
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS environment_info (
                    id INTEGER PRIMARY KEY,
                    mode TEXT NOT NULL DEFAULT 'exclude' CHECK (mode IN ('include', 'exclude')),
                    programs TEXT NOT NULL DEFAULT '',
                    solana_version TEXT,
                    -- Mint cache: the slot the `mints` table is complete through.
                    mints_saved_slot BIGINT
                );
                "#,
            )
            .await?;

        connection
            .execute_unprepared("DROP TABLE IF EXISTS indexer_filters;")
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS environment_info;")
            .await?;

        Ok(())
    }
}
