// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Compresses large account data with lz4 instead of pglz: on `accounts`, `snapshot_accounts`
//! and every partition, and as the database default so later sessions and new tables use it too.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

/// `database_setting` is the `ALTER DATABASE` action and `column_method` the `SET COMPRESSION`
/// method. `ALTER TABLE` on a partitioned parent does not reach its partitions, so each one is set.
fn set_compression(database_setting: &str, column_method: &str) -> String {
    format!(
        r#"
        DO $$
        DECLARE r record;
        BEGIN
            EXECUTE format('ALTER DATABASE %I {database_setting}', current_database());
            FOR r IN
                SELECT relid FROM pg_partition_tree('accounts')
                UNION ALL
                SELECT relid FROM pg_partition_tree('snapshot_accounts')
            LOOP
                EXECUTE format('ALTER TABLE %s ALTER COLUMN data SET COMPRESSION {column_method}', r.relid::regclass);
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
            .execute_unprepared(&set_compression(
                "SET default_toast_compression = lz4",
                "lz4",
            ))
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(&set_compression(
                "RESET default_toast_compression",
                "DEFAULT",
            ))
            .await?;
        Ok(())
    }
}
