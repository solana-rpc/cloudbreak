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
                CREATE TABLE IF NOT EXISTS largest_accounts (
                    mint       BYTEA       NOT NULL,
                    slot       BIGINT      NOT NULL,
                    record     BYTEA       NOT NULL,
                    updated_on TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (mint, slot)
                );
                ALTER TABLE environment_info
                    ADD COLUMN IF NOT EXISTS largest_accounts_mints TEXT;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TABLE IF EXISTS largest_accounts;
                ALTER TABLE environment_info DROP COLUMN IF EXISTS largest_accounts_mints;
                "#,
            )
            .await?;

        Ok(())
    }
}
