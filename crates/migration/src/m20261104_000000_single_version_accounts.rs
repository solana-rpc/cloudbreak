// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Keeps one row per account in `accounts`: the primary key loses `slot`.
//! Existing rows are reduced to the newest version per pubkey first, and closed-account masks go.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        let cfg = crate::migration_config();
        let primary_key = if cfg.pg_owner_partitions.is_owner_partitioned() {
            "(owner, pubkey)"
        } else {
            "(pubkey)"
        };

        connection
            .execute_unprepared(
                r#"
                DELETE FROM accounts a
                USING (
                    SELECT tableoid, ctid
                    FROM (
                        SELECT
                            tableoid,
                            ctid,
                            lamports,
                            row_number() OVER (
                                PARTITION BY pubkey ORDER BY slot DESC, (lamports > 0) DESC
                            ) AS version_rank
                        FROM accounts
                    ) AS ranked
                    WHERE version_rank > 1 OR lamports = 0
                ) AS superseded
                WHERE a.tableoid = superseded.tableoid AND a.ctid = superseded.ctid;
                "#,
            )
            .await?;

        connection
            .execute_unprepared(&format!(
                "ALTER TABLE accounts DROP CONSTRAINT accounts_pkey, ADD PRIMARY KEY {primary_key};"
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        let cfg = crate::migration_config();
        let primary_key = if cfg.pg_owner_partitions.is_owner_partitioned() {
            "(owner, pubkey, slot)"
        } else {
            "(pubkey, slot)"
        };

        connection
            .execute_unprepared(&format!(
                "ALTER TABLE accounts DROP CONSTRAINT accounts_pkey, ADD PRIMARY KEY {primary_key};"
            ))
            .await?;

        Ok(())
    }
}
