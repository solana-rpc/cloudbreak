# Migration CLI

The `cloudbreak-migration` binary wraps `sea-orm-migration` with project-specific configuration for **table partitioning** and **index creation**. Both are driven by a TOML config file pointed at by an environment variable; the database URL is supplied via the standard `sea-orm-cli` flag or environment variable.

## Quick Start

```sh
# 1. Copy the example config to the repo root and tweak if needed
cp example.cloudbreak.migration.toml cloudbreak.migration.toml

# 2. Point the migration binary at the config and your Postgres
export CLOUDBREAK_MIGRATION_CONFIG=./cloudbreak.migration.toml
export DATABASE_URL="postgres://cloudbreak:cloudbreak@localhost:5432/cloudbreak"

# 3. Apply all pending migrations
cargo run -p cloudbreak-migration
```

The default `example.cloudbreak.migration.toml` enables HASH partitioning on `owner` with 10 partitions, the standard composite `(pubkey, slot DESC)` B-tree, and the three SPL Token field indexes — sensible defaults for most installations.

## Configuration

The migration CLI reads two inputs from the environment:

| Variable                     | Required | Description                                                                                                                                                                                                 |
| ---------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CLOUDBREAK_MIGRATION_CONFIG` | **yes**  | Filesystem path to the TOML config file (see below). The binary panics on startup if this is unset or the file cannot be parsed. The config is read once and cached for the duration of the CLI invocation. |
| `DATABASE_URL`               | no\*     | Postgres connection string. Standard `sea-orm-cli` convention; can also be passed explicitly via the `-u <url>` flag (the flag takes precedence). \*One of `DATABASE_URL` or `-u <url>` must be set.        |

### TOML schema

An annotated example lives at the repo root in [`example.cloudbreak.migration.toml`](../../example.cloudbreak.migration.toml). Two sections are supported:

#### `[pg-owner-partitions]`

Controls the partitioning shape of the `accounts` and `snapshot_accounts` tables. The combination of `hash-partitions` and `list-partitions` selects one of four schemas:

| `hash-partitions` | `list-partitions` | Resulting schema                                                                                                                                           |
| ----------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `false`           | `false`           | Plain (non-partitioned) table. **PRIMARY KEY (pubkey, slot)**.                                                                                             |
| `true`            | `false`           | `PARTITION BY HASH (owner)` with `hash-partition-count` partitions. **PRIMARY KEY (owner, pubkey, slot)**.                                                 |
| `false`           | `true`            | `PARTITION BY LIST (owner)` with one dedicated partition per pubkey in `programs-for-list-partition`, plus a plain (non-partitioned) `_default` catch-all. |
| `true`            | `true`            | `PARTITION BY LIST (owner)` whose `_default` is itself `PARTITION BY HASH (owner)` with `hash-partition-count` sub-partitions.                             |

Whenever any owner-partitioning is enabled, `owner` is the first column of the primary key (so partition pruning works).

| Key                           | Type          | Default | Description                                                                                                        |
| ----------------------------- | ------------- | ------- | ------------------------------------------------------------------------------------------------------------------ |
| `hash-partitions`             | `bool`        | `true`  | Enable HASH partitioning on `owner`.                                                                               |
| `hash-partition-count`        | `u32`         | `10`    | Number of HASH partitions. Only used when `hash-partitions = true`.                                                |
| `list-partitions`             | `bool`        | `false` | Enable LIST partitioning on `owner`. Each entry in `programs-for-list-partition` gets its own dedicated partition. |
| `programs-for-list-partition` | `Vec<Pubkey>` | `[]`    | Base58-encoded program pubkeys that get dedicated LIST partitions. Ignored when `list-partitions = false`.         |

#### `[pg-indexes]`

Per-index toggles for the migration-time indexes on the `accounts` table. Each key is the exact PostgreSQL index name; set to `true` to create the index, `false` to skip it.

| Key                           | Type   | Default | Description                                                                                                                                                                       |
| ----------------------------- | ------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `idx_accounts_pubkey`         | `bool` | `false` | `USING HASH` index on `pubkey`. Opt-in because the composite B-tree `idx_accounts_pubkey_slot` covers the same access pattern and additionally supports ordering and range scans. |
| `idx_accounts_pubkey_slot`    | `bool` | `true`  | Composite B-tree `(pubkey, slot DESC)`. Drives latest-version-per-pubkey lookups for `getAccountInfo` / `getBalance` / `getTokenAccountBalance` / `getMultipleAccounts`.          |
| `idx_accounts_token_mint`     | `bool` | `true`  | Index on the generated `token_mint` column. Used by `getTokenAccountsByOwner` and friends when filtered by a specific mint.                                                       |
| `idx_accounts_token_owner`    | `bool` | `true`  | Index on the SPL Token owner field (bytes 32..64 of `data`) and the program, so one program's query skips the other's rows.                                                       |
| `idx_accounts_token_delegate` | `bool` | `true`  | Index on the SPL Token delegate field.                                                                                                                                            |

> Indexes for the `snapshot_accounts` table are created at snapshot-ingest time by the `snapshot` crate (not by migrations), so they live in `cloudbreak.snapshot.toml` (or under `[snapshot.pg-indexes]` of `cloudbreak.index.toml`) — not here. See [`example.cloudbreak.snapshot.toml`](../../example.cloudbreak.snapshot.toml).

## Choosing a partitioning shape

Rough guidance for picking values in `[pg-owner-partitions]`:

- **Small / single-program installations** (a few hundred million rows, one or two programs being indexed) — the **plain non-partitioned** shape (`hash-partitions = false`, `list-partitions = false`) is usually simplest. PK is `(pubkey, slot)`, which is exactly the lookup shape the API uses, so latest-version queries hit the PK B-tree directly.
- **General-purpose installations indexing many programs** — the **HASH-only** shape is the default and what most installations should use. `hash-partition-count = 10` is a good starting point; raise it (e.g. 32 or 64) if individual partitions grow large enough that `CLUSTER`, `VACUUM`, and per-partition maintenance start hurting. Don't pick a partition count larger than your number of distinct owners — partitions can only be as well-balanced as the underlying hash distribution allows.
- **Installations with a small number of very large programs you want isolated** (e.g. SPL Token, your own program) — **LIST + HASH** (both `true`): list those program pubkeys in `programs-for-list-partition` so each one gets its own dedicated partition (great for targeted `CLUSTER` / `REINDEX` / dropping a single program's data), while everything else fans out across `hash-partition-count` partitions under `_default`.
- **LIST without HASH** is mostly useful if you want LIST partitions plus a single un-partitioned catch-all table (no fan-out for non-listed owners). Rare; choose only if you have a specific reason.

The `dbtools` CLI ships `partition-sizes`, `distinct-owners-count`, and `get-biggest-programs` commands that are useful to size things — see [`crates/dbtools/README.md`](../dbtools/README.md).

## Changing configuration after the initial migration

The partitioning shape and the set of indexes are baked in at table creation time. Migrations are append-only and do **not** repartition or recreate indexes on a subsequent run, so editing `cloudbreak.migration.toml` after the fact does **not** retroactively change an existing database.

To apply config changes to an existing database you have two options:

1. **Drop and recreate everything** (destructive — wipes all account data):

   ```sh
   cargo run -p cloudbreak-migration -- fresh
   ```

   Use this for a local dev setup, or for production if you're going to re-bootstrap from a snapshot anyway.

2. **Apply the change by hand** (non-destructive but manual): edit the TOML so it matches what you want the _next_ fresh database to look like, then run the equivalent DDL directly against Postgres for the existing one (e.g. `CREATE INDEX CONCURRENTLY ...` to enable a new index without downtime, or attach/detach partitions if you're reshaping the partition tree).

In both cases, also remember to update `[snapshot.pg-indexes]` in your snapshot/indexer config if the change involves the `snapshot_accounts` table.

## Running

Always set both `CLOUDBREAK_MIGRATION_CONFIG` and the database URL.

```sh
export CLOUDBREAK_MIGRATION_CONFIG=./cloudbreak.migration.toml
export DATABASE_URL="postgres://cloudbreak:cloudbreak@localhost:5432/cloudbreak"

# Apply all pending migrations (the default subcommand is `up`)
cargo run -p cloudbreak-migration

# Or pass the URL explicitly with -u (takes precedence over DATABASE_URL):
cargo run -p cloudbreak-migration -- up -u "postgres://..."
```

## CLI Reference

The migration binary still uses the `sea-orm-cli` subcommand parser, so any flag accepted by `sea-orm-migration` works (e.g. `-u <url>`, `-s <schema>`, `-n <count>`).

- Generate a new migration file
  ```sh
  cargo run -p cloudbreak-migration -- generate MIGRATION_NAME
  ```
- Apply all pending migrations
  ```sh
  cargo run -p cloudbreak-migration
  ```
  ```sh
  cargo run -p cloudbreak-migration -- up
  ```
- Apply first 10 pending migrations
  ```sh
  cargo run -p cloudbreak-migration -- up -n 10
  ```
- Rollback last applied migration
  ```sh
  cargo run -p cloudbreak-migration -- down
  ```
- Rollback last 10 applied migrations
  ```sh
  cargo run -p cloudbreak-migration -- down -n 10
  ```
- Drop all tables from the database, then reapply all migrations
  ```sh
  cargo run -p cloudbreak-migration -- fresh
  ```
- Rollback all applied migrations, then reapply all migrations
  ```sh
  cargo run -p cloudbreak-migration -- refresh
  ```
- Rollback all applied migrations
  ```sh
  cargo run -p cloudbreak-migration -- reset
  ```
- Check the status of all migrations
  ```sh
  cargo run -p cloudbreak-migration -- status
  ```

## Troubleshooting

- **`CLOUDBREAK_MIGRATION_CONFIG must point to a TOML migration config file`** — the binary couldn't find the env var. Export it before running (`export CLOUDBREAK_MIGRATION_CONFIG=./cloudbreak.migration.toml`) or pass it inline (`CLOUDBREAK_MIGRATION_CONFIG=./cloudbreak.migration.toml cargo run -p cloudbreak-migration`).
- **`failed to load migration config from <path>: ...`** — the file path resolved but the TOML didn't parse against the schema. Compare against [`example.cloudbreak.migration.toml`](../../example.cloudbreak.migration.toml). Common causes: typo in a key name (keys are kebab-case, not snake_case), invalid base58 pubkey in `programs-for-list-partition`, wrong type (e.g. string where bool is expected).
- **`pg_tracing` warning during migrations** — informational only. The migration tries to load the optional `pg_tracing` Postgres extension; if it's not installed the migration skips it and continues. The stock Docker Postgres image does not include it, so this warning is expected in local dev.
- **Changing the TOML doesn't affect an existing database** — by design. See [Changing configuration after the initial migration](#changing-configuration-after-the-initial-migration).
