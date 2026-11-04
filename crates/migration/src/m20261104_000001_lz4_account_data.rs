// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Compresses large account data with lz4 instead of pglz: on `accounts`, `snapshot_accounts`
//! and every partition, and as the database default so later sessions and temp tables use it too.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn set_compression(method: &str) -> String {
    format!(
        r#"
        DO $$
        DECLARE r record;
        BEGIN
            EXECUTE format('ALTER DATABASE %I SET default_toast_compression = %L', current_database(), '{method}');
            FOR r IN
                SELECT relid FROM pg_partition_tree('accounts')
                UNION ALL
                SELECT relid FROM pg_partition_tree('snapshot_accounts')
            LOOP
                EXECUTE format('ALTER TABLE %s ALTER COLUMN data SET COMPRESSION {method}', r.relid::regclass);
            END LOOP;
        END $$;
        "#
    )
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(&set_compression("lz4"))
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(&set_compression("pglz"))
            .await?;
        Ok(())
    }
}
