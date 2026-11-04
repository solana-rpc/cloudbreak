// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Creates `accounts_stage`: the indexer writes a block's rows here in parallel, then moves
//! them into `accounts` in the block transaction. `data` uses the compression of `accounts`.

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
                CREATE UNLOGGED TABLE IF NOT EXISTS accounts_stage (
                    slot BIGINT NOT NULL,
                    pubkey BYTEA NOT NULL,
                    owner BYTEA NOT NULL,
                    lamports BIGINT NOT NULL,
                    executable BOOLEAN NOT NULL,
                    rent_epoch NUMERIC(20, 0) NOT NULL,
                    data BYTEA NOT NULL,
                    write_version BIGINT NOT NULL,
                    txn_signature BYTEA
                ) WITH (
                    autovacuum_vacuum_scale_factor = 0,
                    autovacuum_vacuum_threshold = 5000,
                    autovacuum_vacuum_cost_delay = 0,
                    toast.autovacuum_vacuum_scale_factor = 0,
                    toast.autovacuum_vacuum_threshold = 5000,
                    toast.autovacuum_vacuum_cost_delay = 0
                );
                CREATE INDEX IF NOT EXISTS accounts_stage_slot_idx ON accounts_stage (slot);

                DO $$
                DECLARE method "char";
                BEGIN
                    SELECT a.attcompression INTO method
                    FROM pg_attribute a
                    WHERE a.attrelid = 'accounts'::regclass AND a.attname = 'data';
                    IF method = 'l' THEN
                        ALTER TABLE accounts_stage ALTER COLUMN data SET COMPRESSION lz4;
                    ELSIF method = 'p' THEN
                        ALTER TABLE accounts_stage ALTER COLUMN data SET COMPRESSION pglz;
                    END IF;
                END $$;
                "#,
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS accounts_stage;")
            .await?;
        Ok(())
    }
}
