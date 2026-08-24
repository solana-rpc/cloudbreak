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
        manager
            .create_table(
                Table::create()
                    .table(Slot::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Slot::Slot).big_integer().not_null())
                    .col(ColumnDef::new(Slot::Commitment).integer().not_null())
                    .col(ColumnDef::new(Slot::BlockTime).big_integer().not_null())
                    .col(
                        ColumnDef::new(Slot::Health)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .primary_key(Index::create().col(Slot::Commitment))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Slot::Table).if_exists().to_owned())
            .await?;

        Ok(())
    }
}

#[derive(Iden)]
pub enum Slot {
    #[iden = "slots"]
    Table,
    #[allow(clippy::enum_variant_names)]
    Slot,
    Commitment,
    BlockTime,
    Health,
}
