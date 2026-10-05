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
                    .col(ColumnDef::new(Slot::Blockhash).text().null())
                    .primary_key(Index::create().col(Slot::Commitment))
                    .to_owned(),
            )
            .await?;

        // Sends every real change of a row on `cloudbreak_slots` for the API slot syncronizer.
        // A no-op update (such as a health write that changes nothing) sends nothing.
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION notify_slots_change() RETURNS trigger AS $$
                BEGIN
                    IF TG_OP = 'UPDATE' AND OLD IS NOT DISTINCT FROM NEW THEN
                        RETURN NEW;
                    END IF;
                    PERFORM pg_notify(
                        'cloudbreak_slots',
                        json_build_object(
                            'commitment', NEW.commitment,
                            'slot', NEW.slot,
                            'block_time', NEW.block_time,
                            'health', NEW.health,
                            'blockhash', NEW.blockhash
                        )::text
                    );
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS slots_notify ON slots;
                CREATE TRIGGER slots_notify
                    AFTER INSERT OR UPDATE ON slots
                    FOR EACH ROW EXECUTE FUNCTION notify_slots_change();
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Slot::Table).if_exists().to_owned())
            .await?;

        manager
            .get_connection()
            .execute_unprepared("DROP FUNCTION IF EXISTS notify_slots_change();")
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
    Blockhash,
}
