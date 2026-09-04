use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

/// Partial index on stake-owned rows of the `accounts` table for the getSupply
/// non-circulating stake scan. With owner partitioning off, the scan is otherwise
/// a full ~1.15B-row table scan every 10 minutes. Gated by
/// `pg-indexes.idx-accounts-stake-owner`; only the supply node sets it.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !crate::migration_config().pg_indexes.idx_accounts_stake_owner {
            return Ok(());
        }

        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX IF NOT EXISTS idx_accounts_stake_owner ON accounts (pubkey, slot DESC)
                WHERE owner = '\x06a1d8179137542a983437bdfe2a7ab2557f535c8a78722b68a49dc000000000'::bytea;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS idx_accounts_stake_owner;")
            .await?;

        Ok(())
    }
}
