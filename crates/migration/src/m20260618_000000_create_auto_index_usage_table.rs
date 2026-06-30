// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(AutoIndexUsage::Table)
                    .if_not_exists()
                    .col(text(AutoIndexUsage::IndexName).primary_key())
                    .col(big_integer(AutoIndexUsage::LastIdxScan).not_null())
                    .col(
                        timestamp_with_time_zone(AutoIndexUsage::LastSeenUsed)
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(AutoIndexUsage::CreatedAt)
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(AutoIndexUsage::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum AutoIndexUsage {
    Table,
    IndexName,
    LastIdxScan,
    LastSeenUsed,
    CreatedAt,
}
