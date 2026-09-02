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
                CREATE TABLE IF NOT EXISTS supply (
                    slot                     BIGINT         PRIMARY KEY,
                    total                    NUMERIC(20, 0) NOT NULL,
                    non_circulating_lamports NUMERIC(20, 0),
                    updated_at               TIMESTAMPTZ    NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS non_circulating_accounts (
                    id         BIGINT      PRIMARY KEY DEFAULT 1,
                    slot       BIGINT      NOT NULL,
                    accounts   BYTEA[]     NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
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
                "DROP TABLE IF EXISTS supply; DROP TABLE IF EXISTS non_circulating_accounts;",
            )
            .await?;

        Ok(())
    }
}
