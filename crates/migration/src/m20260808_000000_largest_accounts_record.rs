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
                DROP TABLE IF EXISTS largest_accounts;
                CREATE TABLE largest_accounts (
                    mint       BYTEA       NOT NULL,
                    slot       BIGINT      NOT NULL,
                    record     BYTEA       NOT NULL,
                    updated_on TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (mint, slot)
                );
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
                CREATE TABLE IF NOT EXISTS largest_accounts (
                    mint       BYTEA          NOT NULL,
                    pubkey     BYTEA          NOT NULL,
                    amount     NUMERIC(20, 0) NOT NULL,
                    slot       BIGINT         NOT NULL,
                    updated_on TIMESTAMPTZ    NOT NULL DEFAULT now(),
                    PRIMARY KEY (mint, pubkey, slot)
                );
                CREATE INDEX IF NOT EXISTS idx_largest_accounts_mint_slot
                    ON largest_accounts (mint, slot);
                "#,
            )
            .await?;

        Ok(())
    }
}
