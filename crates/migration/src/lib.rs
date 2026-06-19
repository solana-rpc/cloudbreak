// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

pub use sea_orm_migration::prelude::*;
use std::sync::OnceLock;
pub use cloudbreak_core::{
    MigrationConfig, MigrationPgIndexesConfig, PgOwnerPartitionsConfig, TryLoadConfig,
};

mod m20220101_000001_create_slots_table;
pub(crate) mod m20250414_201255_create_accounts_table;
mod m20250714_055019_add_bs58_functions;
mod m20251008_080739_create_snapshot_accounts_table;
mod m20251021_222145_create_service_health_table;
mod m20260325_000000_drop_temp_tables;
mod m20260414_000000_create_indexer_filters_table;
mod m20260522_000000_create_environment_info_table;
mod m20260528_000000_create_epoch_stakes_table;
mod m20260618_000000_create_auto_index_usage_table;

pub struct Migrator;

pub const CLOUDBREAK_MIGRATION_CONFIG_ENV: &str = "CLOUDBREAK_MIGRATION_CONFIG";

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20220101_000001_create_slots_table::Migration),
            Box::new(m20250414_201255_create_accounts_table::Migration),
            Box::new(m20250714_055019_add_bs58_functions::Migration),
            Box::new(m20251008_080739_create_snapshot_accounts_table::Migration),
            Box::new(m20251021_222145_create_service_health_table::Migration),
            Box::new(m20260325_000000_drop_temp_tables::Migration),
            Box::new(m20260414_000000_create_indexer_filters_table::Migration),
            Box::new(m20260522_000000_create_environment_info_table::Migration),
            Box::new(m20260528_000000_create_epoch_stakes_table::Migration),
            Box::new(m20260618_000000_create_auto_index_usage_table::Migration),
        ]
    }
}

/// Cached migration config. Loaded once per process from the TOML file pointed at by
/// `CLOUDBREAK_MIGRATION_CONFIG`. Each migration that needs config just calls
/// `migration_config()`.
static MIGRATION_CONFIG: OnceLock<MigrationConfig> = OnceLock::new();

pub fn migration_config() -> &'static MigrationConfig {
    MIGRATION_CONFIG.get_or_init(|| {
        let path = std::env::var(CLOUDBREAK_MIGRATION_CONFIG_ENV).unwrap_or_else(|_| {
            panic!("{CLOUDBREAK_MIGRATION_CONFIG_ENV} must point to a TOML migration config file")
        });

        MigrationConfig::try_load(&path)
            .unwrap_or_else(|err| panic!("failed to load migration config from {path}: {err}"))
    })
}

/// Build the block that creates `table_name` with the requested partitioning shape.
///
/// Handles the four (hash, list) combinations:
/// - (false, false) → plain UNLOGGED table, PK is `(pubkey, slot)`.
/// - (true,  false) → `PARTITION BY HASH (owner)` with `hash_partition_count` buckets, PK is `(owner, pubkey, slot)`.
/// - (false, true ) → `PARTITION BY LIST (owner)` with per-program partitions and a plain (non-partitioned) `_default` catch-all.
/// - (true,  true ) → `PARTITION BY LIST (owner)` whose `_default` is further `PARTITION BY HASH (owner)`.
pub fn build_create_table_sql(table_name: &str, cfg: &PgOwnerPartitionsConfig) -> String {
    let columns = columns_sql();
    let primary_key = if cfg.is_owner_partitioned() {
        "PRIMARY KEY (owner, pubkey, slot)"
    } else {
        "PRIMARY KEY (pubkey, slot)"
    };

    match (cfg.hash_partitions, cfg.list_partitions) {
        (false, false) => format!(
            r#"
            CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                {columns},
                {primary_key}
            );
            "#
        ),
        (true, false) => {
            let hash_partitions =
                hash_partition_block(table_name, table_name, cfg.hash_partition_count);
            format!(
                r#"
                CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                    {columns},
                    {primary_key}
                ) PARTITION BY HASH (owner);

                {hash_partitions}
                "#
            )
        }
        (false, true) => {
            let list_partitions =
                list_partition_block(table_name, &cfg.programs_for_list_partition);
            format!(
                r#"
                CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                    {columns},
                    {primary_key}
                ) PARTITION BY LIST (owner);

                {list_partitions}

                CREATE UNLOGGED TABLE {table_name}_default PARTITION OF {table_name} DEFAULT;
                "#
            )
        }
        (true, true) => {
            let list_partitions =
                list_partition_block(table_name, &cfg.programs_for_list_partition);
            let default_table = format!("{table_name}_default");
            let hash_partitions =
                hash_partition_block(table_name, &default_table, cfg.hash_partition_count);
            format!(
                r#"
                CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                    {columns},
                    {primary_key}
                ) PARTITION BY LIST (owner);

                {list_partitions}

                CREATE UNLOGGED TABLE {default_table} PARTITION OF {table_name} DEFAULT
                    PARTITION BY HASH (owner);

                {hash_partitions}
                "#
            )
        }
    }
}

fn columns_sql() -> &'static str {
    r#"pubkey BYTEA NOT NULL,
            owner BYTEA NOT NULL,
            lamports BIGINT NOT NULL,
            slot BIGINT NOT NULL,
            executable BOOLEAN NOT NULL,
            rent_epoch NUMERIC(20, 0) NOT NULL,
            data BYTEA NOT NULL,
            write_version BIGINT NOT NULL,
            updated_on TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            txn_signature BYTEA,
            token_mint BYTEA GENERATED ALWAYS AS (SUBSTRING(data FROM 1 FOR 32)) STORED,
            token_owner BYTEA GENERATED ALWAYS AS (SUBSTRING(data FROM 33 FOR 32)) STORED"#
}

fn list_partition_block(
    table_name: &str,
    programs: &[cloudbreak_core::PubkeyDef],
) -> String {
    programs
        .iter()
        .map(|program| {
            let pk = program.0;
            format!(
                r#"CREATE UNLOGGED TABLE {table_name}_{program_name} PARTITION OF {table_name} FOR VALUES IN ('\x{program_hex}');"#,
                program_name = pk,
                program_hex = hex::encode(pk.to_bytes()),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn hash_partition_block(table_name: &str, parent_table: &str, num_partitions: u32) -> String {
    format!(
        r#"
        DO $$
        DECLARE
            num_partitions INTEGER := {num_partitions};
        BEGIN
            FOR i IN 0..(num_partitions - 1) LOOP
                EXECUTE format(
                    'CREATE UNLOGGED TABLE {table_name}_p%1$s PARTITION OF {parent_table} FOR VALUES WITH (MODULUS {num_partitions}, REMAINDER %1$s)',
                    i
                );
            END LOOP;
        END $$;
        "#
    )
}
