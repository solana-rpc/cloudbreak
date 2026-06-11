# Cloudbreak

Cloudbreak is a Postgres-backed Solana RPC implementation focused on serving `getProgramAccounts`-style endpoints. It maintains a filtered segment of Solana account state by program owner, sourced from Yellowstone gRPC streams and optional full/incremental snapshots.

## Supported RPC Methods

The API server exposes the following JSON-RPC methods:

| Method                       | Description                                                                                                                                                |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getProgramAccounts`         | Returns all accounts owned by a program. Supports `memcmp`, `dataSize`, and `dataSlice` filters.                                                            |
| `getTokenAccountsByOwner`    | Returns SPL token accounts owned by a specific wallet.                                                                                                     |
| `getTokenAccountsByDelegate` | Returns SPL token accounts delegated to a specific address.                                                                                                |
| `getTokenAccountsByMint`     | Returns all token accounts for a given mint. Generic implementation backed by `getProgramAccounts` + a `memcmp(offset=0, mint)` filter; the SPL Token program is used by default, override with `programId` in the config (e.g. for Token-2022). Streamed response shape, same as `getProgramAccounts`. |
| `getAccountInfo`             | Returns the latest version of a single account. Supports `base58`, `base64`, `base64+zstd`, and `jsonParsed` encodings, `dataSlice`, and `minContextSlot`. |
| `getMultipleAccounts`        | Batched `getAccountInfo` for up to `[server].max-multiple-accounts` pubkeys per request (default `100`). Returns `null` per position for missing or indexer-filter-excluded accounts.                   |
| `getBalance`                 | Returns the lamport balance of an account. Returns `0` for missing or closed accounts (Agave-compatible).                                                  |
| `getTokenAccountBalance`     | Returns the `UiTokenAmount` of an SPL Token / Token-2022 account, including mint-aware decimals and UI amounts. WSOL native mint is recognised explicitly. |
| `getSlot`                    | Returns the current slot at the requested commitment level.                                                                                                |
| `getHealth`                  | Returns the health status of the service.                                                                                                                  |
| `getVersion`                 | Returns the cluster version, Agave-compatible (`{"solana-core": "<string>"}`). See note below for the composite-string format Cloudbreak uses.              |
| `getGenesisHash`             | Returns the cluster genesis hash as a base58 string.                                                                                                       |

Only **confirmed** and **finalized** commitment levels are fully supported. By default, requests with `processed` commitment return an error. This can be overridden via the `processed-commitment` configuration option (see [API Configuration](#api-server-cloudbreakapitoml)).

> **Note on `getVersion`.** The `solana-core` field returned by Cloudbreak is a *composite* string of the form `"<upstream-solana-core>-cloudbreak<cloudbreak-version>"` (e.g. `"2.0.21-cloudbreak0.1.0"`). The upstream half is the `solana-core` version reported by the gRPC source the indexer is subscribed to (persisted to the `environment_info` table on indexer startup); the suffix is Cloudbreak's own crate version. This lets clients see *both* what cluster they're effectively talking to and which Cloudbreak build is serving them. If the indexer has never written an upstream version, the prefix falls back to `"unknown"`. The response is cached in-process for 10 minutes.

## Roadmap

### Planned RPC Methods

The following methods are planned for future releases:

- `getVoteAccounts`

### Processed Commitment Level

Full native support for the `processed` commitment level is planned as an **optional plugin**, allowing operators to enable it when low-latency reads of unconfirmed state are needed. In the meantime, operators can set `processed-commitment = "use-confirmed"` in the API config to respond with `confirmed` data instead of rejecting `processed` requests.

### Paginated Responses

Support for paginated responses to queries is planned, enabling clients to efficiently iterate over large result sets without loading all accounts into a single response.

## Architecture Overview

```
                                ┌────────────────┐
                                │  Solana Node   │
                                │  (Yellowstone  │
                                │   gRPC plugin) │
                                └───────┬────────┘
                                        │
                                        ▼
┌──────────────┐            ┌───────────────────┐           ┌───────────────────┐
│   Cluster    │───────────▶│     Indexer       │──────────▶│    PostgreSQL     │
│   Tracker    │            │  (gRPC consumer)  │           │  (accounts/slots) │
└──────┬───────┘            └───────────────────┘           └────────┬──────────┘
       │ scrapes                                                     │
       ▼                                                             ▼
┌──────────────┐                                            ┌───────────────────┐
│  Snapshot    │                                            │   Query Tracker   │◀──┐
│  sources     │                                            │   (auto-indexing) │   │
│  (RPC nodes  │                                            └───────────────────┘   │
│   / sidecars)│                                                                    │
└──────────────┘                                            ┌───────────────────┐   │
                                                            │    API Server     │───┘
                                                            │    (JSON-RPC)     │
                                                            └───────────────────┘
```

The **cluster tracker** is a [Blockdaemon `solcluster tracker`](https://github.com/Blockdaemon/solana-cluster) instance that scrapes one or more **snapshot sources** — either Solana RPC nodes that serve snapshot archives or `solcluster sidecar` agents running next to validators — and exposes a unified `/v1/snapshots` API listing the available full and incremental snapshot pairs. The indexer asks the tracker which snapshots cover a given slot and downloads the archives directly from the source the tracker reports. A tracker is provided in `docker-compose.yaml` for local development; see [Cluster Tracker](#cluster-tracker).

## Project Structure

### Crates

| Crate                       | Package                               | Description                                                                                                                                                                                                                                                                                                                                                |
| --------------------------- | ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/command/`           | `cloudbreak`               | Main binary. Dispatches to `api`, `index`, `snapshot`, and `query-tracker` subcommands. Initializes OpenTelemetry tracing. Uses jemalloc as the global allocator on non-MSVC targets.                                                                                                                                                                      |
| `crates/api/`               | `cloudbreak-api`           | HTTP JSON-RPC server (Hyper). Serves `getProgramAccounts`, `getTokenAccountsByOwner`, `getTokenAccountsByDelegate`, `getTokenAccountsByMint`, `getAccountInfo`, `getMultipleAccounts`, `getBalance`, `getTokenAccountBalance`, `getSlot`, `getHealth`, `getVersion`, and `getGenesisHash`. Streams `getProgramAccounts` and `getTokenAccountsByMint` responses as chunked JSON-RPC body frames, applies per-request `statement_timeout` and a total request timeout, supports batch requests with bounded concurrency, optionally caches results in memory (`[gpa-cache]`), and reports query usage to the query tracker.                                                                   |
| `crates/index/`             | `cloudbreak-index`         | Live indexer. Subscribes to Yellowstone gRPC, applies program filters, writes account/slot state to Postgres. Includes finalize-slot pipeline, self-healing for slot gaps. Uses the `snapshot` crate to optionally load a full snapshot on startup for fast bootstrapping.                                                                                 |
| `crates/snapshot/`          | `cloudbreak-snapshot`      | Batch loads Solana full and incremental snapshots. Queries a cluster tracker (`/v1/snapshots`) to find a covering snapshot pair, downloads the archives from the source reported by the tracker, unpacks them, reads Solana `AccountsFile` entries, bulk-upserts into Postgres, handles deduplication, optional partition clustering, and index creation. |
| `crates/query-tracker/`     | `cloudbreak-query-tracker` | JSON-RPC sidecar service (jsonrpsee). Counts `getProgramAccounts`-shaped queries, maintains a priority queue, and optionally drives automatic `CREATE INDEX` on Postgres based on query frequency. Includes periodic query count resets.                                                                                                                   |
| `crates/core/`              | `cloudbreak-core`          | Shared library. Contains all configuration struct definitions (TOML deserialization), database connection helpers, the `AccountOwnerMap` abstraction, and the `AccountSelectorConfig` (include/exclude program filter logic synced to DB).                                                                                                                 |
| `crates/entity/`            | `cloudbreak-entity`        | SeaORM entity definitions for Postgres tables: `accounts`, `snapshot_accounts`, `slots`, `service_health`. Also defines the `CommitmentLevel` enum and account conversion types. (The `environment_info` table is accessed via raw SQL in `cloudbreak-core`, not as a SeaORM entity.)                                                                       |
| `crates/migration/`         | `cloudbreak-migration`     | Schema evolution via `sea-orm-migration`. Defines `accounts`/`snapshot_accounts` DDL helpers with TOML-configurable owner partitioning (none / HASH / LIST / LIST+HASH) and per-index toggles, plus bs58 encode/decode PL/pgSQL functions, the `service_health` table, and the `environment_info` table (program filter + Solana version). See [`crates/migration/README.md`](crates/migration/README.md).                                                                                                                |
| `crates/dbtools/`           | `cloudbreak-dbtools`       | Standalone operator CLI for running analytics queries and toggling Postgres configuration across one or more servers. Does not depend on other cloudbreak crates. See [`crates/dbtools/README.md`](crates/dbtools/README.md).                                                                                                                               |
| `crates/integration_tests/` | `integration_tests`                   | Black-box benchmarking and correctness testing tool. Constant-rate load generation, dual-endpoint comparison with slot compensation, VictoriaLogs integration, and optional DB forensics. See [`crates/integration_tests/README.md`](crates/integration_tests/README.md).                                                                                  |

## Getting Started

### What You Need

Everything for local development is included in this repository **except** the following external data sources that you must provide:

1. **A Yellowstone gRPC endpoint** — for streaming live account and slot updates from a Solana validator. This is the primary data source for the indexer.
2. **(Optional) Snapshot sources for the cluster tracker** — by default, the tracker shipped in `docker-compose.yaml` is pre-configured (via `tracker-config.yml`) to scrape the public Solana mainnet endpoint `https://api.mainnet.solana.com`, so **snapshots work out of the box** with no extra setup. The only caveat is that the public endpoint can be rate-limited and may slow down or refuse downloads under load. For production or heavy local use, point `tracker-config.yml` at your own `solcluster sidecar` instances (or other Solana RPC nodes that expose snapshot archives) or either use your own valid tracker endpoint — see [Cluster Tracker](#cluster-tracker).

Everything else (PostgreSQL, Prometheus, Grafana, Tempo, the cluster tracker — including a working default snapshot source) is provided via Docker Compose, and the example config files are pre-configured for local development.

### Quick Start

> **Heads up:** the very first `cargo build` (or first `cargo run`) of this workspace can take some time depending on your machine — there are a lot of crates to compile from scratch. The terminal will look idle for long stretches; that's normal. Subsequent builds are incremental and finish in seconds.

```sh
# 1. Start all infrastructure (Postgres, Prometheus, Grafana, Tempo, cluster tracker)
docker compose up -d

# 2. Run database migrations (point the binary at the example migration config)
export DATABASE_URL="postgres://cloudbreak:cloudbreak@localhost:5432/cloudbreak"
cp example.cloudbreak.migration.toml cloudbreak.migration.toml
export CLOUDBREAK_MIGRATION_CONFIG=./cloudbreak.migration.toml
cargo run -p cloudbreak-migration

# 3. Copy example configs and set your gRPC + (optionally) tracker endpoint
cp example.cloudbreak.index.toml cloudbreak.index.toml
cp example.cloudbreak.api.toml cloudbreak.api.toml
cp example.cloudbreak.query-tracker.toml cloudbreak.query-tracker.toml
# Edit cloudbreak.index.toml → set [grpc] endpoint and x-token
#   (the default [snapshot.tracker_endpoint] already points at the local Compose tracker,
#    which by default scrapes https://api.mainnet.solana.com — works out of the box, rate-limited)
# (Optional) Edit tracker-config.yml → swap the public endpoint for your own sidecars/RPC nodes

# 4. Start the services (in separate terminals)
cargo run -p cloudbreak -- -c ./cloudbreak.index.toml index
cargo run -p cloudbreak -- -c ./cloudbreak.api.toml api
cargo run -p cloudbreak -- -c ./cloudbreak.query-tracker.toml query-tracker
```

The API will be available at `http://localhost:4000`, Grafana at `http://localhost:3000` (admin/admin), and Prometheus at `http://localhost:9090`.

### Step-by-Step Setup

#### 1. Start Infrastructure

The `docker-compose.yaml` starts everything you need for local development:

```sh
docker compose up -d
```

> **First-run note:** The `solana-tracker` service is built from a Git context (`rpcpool/solana-cluster#triton`), so the **first** `docker compose up` will clone the repo and pull the Go toolchain to build the image. This can add several minutes on top of the normal Compose start. Subsequent runs reuse the cached image. See [Troubleshooting: `solana-tracker` build fails](#solana-tracker-build-fails) if you ever need to force a rebuild.

This creates:

| Service         | Port                                     | Details                                                                                           |
| --------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------- |
| PostgreSQL      | 5432                                     | User/password/database: `cloudbreak` / `cloudbreak` / `cloudbreak`                                   |
| Prometheus      | 9090                                     | Pre-configured to scrape cloudbreak services                                                       |
| Pushgateway     | 9091                                     | Push-based metrics                                                                                |
| Tempo           | 4317 (OTLP gRPC), 4318 (OTLP HTTP), 3200 | Distributed tracing backend                                                                       |
| Grafana         | 3000                                     | admin/admin, Tempo + Prometheus datasources pre-configured                                        |
| Cluster Tracker | 8458 (public API), 8457 (metrics)        | `solcluster tracker` from `rpcpool/solana-cluster#triton`. Configured via `./tracker-config.yml`. |

The database connection URL for all configs is: `postgres://cloudbreak:cloudbreak@localhost:5432/cloudbreak`

#### 2. Run Database Migrations

The migration CLI requires a TOML config (partitioning shape + per-index toggles) pointed at by `CLOUDBREAK_MIGRATION_CONFIG`, plus a database URL via `DATABASE_URL` or the `-u <url>` flag:

```sh
cp example.cloudbreak.migration.toml cloudbreak.migration.toml
export CLOUDBREAK_MIGRATION_CONFIG=./cloudbreak.migration.toml
export DATABASE_URL="postgres://cloudbreak:cloudbreak@localhost:5432/cloudbreak"
cargo run -p cloudbreak-migration
```

> **First build can take a while.** This is the **first** `cargo run` in the workspace, so Cargo will compile every dependency from scratch. The same applies to the first `cargo run` of the indexer / API / query-tracker in [Step 4](#4-start-the-services) (each binary triggers another compile of its own dependency graph the first time). All subsequent runs are incremental.

Migrations create the `accounts` and `snapshot_accounts` tables (with the partitioning shape and indexes defined in `cloudbreak.migration.toml`), PL/pgSQL helper functions, and the `service_health` and `environment_info` tables.

> **Note:** The migrations attempt to load the `pg_tracing` Postgres extension. This is optional and will be skipped with a warning if the extension is not installed (the stock Docker Postgres image does not include it). Everything works without it. The compose file uses PostgreSQL 16, which is the version compatible with `pg_tracing` if you want to install it later.

**Other migration commands:**

```sh
cargo run -p cloudbreak-migration -- status    # Check migration status
cargo run -p cloudbreak-migration -- up        # Apply all pending
cargo run -p cloudbreak-migration -- up -n 1   # Apply next N
cargo run -p cloudbreak-migration -- down      # Rollback last
cargo run -p cloudbreak-migration -- fresh     # Drop all, reapply
cargo run -p cloudbreak-migration -- refresh   # Rollback all, reapply
```

See [`crates/migration/README.md`](crates/migration/README.md) for the full config reference (partitioning shapes, per-index toggles, env vars, and CLI flags).

##### Table Partitioning & Indexes

The `accounts` and `snapshot_accounts` tables can be partitioned by `owner` (HASH, LIST, both, or not at all), and each migration-time index on `accounts` can be individually toggled. Both are controlled by the TOML file pointed at by `CLOUDBREAK_MIGRATION_CONFIG`. The default `example.cloudbreak.migration.toml` enables HASH partitioning with 10 partitions plus the standard composite + token indexes — sensible defaults for most setups.

See [`crates/migration/README.md`](crates/migration/README.md) for the four supported partitioning shapes, the full set of index toggles, and worked examples.

#### 3. Configure the Services

Copy the example configs:

```sh
cp example.cloudbreak.index.toml cloudbreak.index.toml
cp example.cloudbreak.api.toml cloudbreak.api.toml
cp example.cloudbreak.query-tracker.toml cloudbreak.query-tracker.toml
```

The example configs are pre-configured for local development — database URL, metrics ports, and all defaults are already set. **The only values you need to fill in are in `cloudbreak.index.toml`:**

| Setting    | Section                       | What to set                                                                                                                                                         |
| ---------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint` | `[grpc]`                      | Your Yellowstone gRPC endpoint URL                                                                                                                                  |
| `x-token`  | `[grpc]`                      | Your authentication token (if required)                                                                                                                             |
| `endpoint` | `[snapshot.tracker_endpoint]` | Cluster tracker URL. Defaults to `http://localhost:8458` (the Compose tracker). Remove the `[snapshot]` section entirely if you don't want snapshot bootstrap/heal. |
| `include`  | `[programs]`                  | Base58 pubkeys of programs you want to index                                                                                                                        |

#### 4. Start the Services

> **Startup order matters — start the services in this order, each in its own terminal:**
>
> 1. **Indexer first.** It populates the `environment_info` table that the API reads on startup. The API panics on launch if this row does not exist (see [Troubleshooting: API returns "environment_info row not found"](#api-returns-environment_info-row-not-found)). Wait until the indexer has finished its own startup (you'll see it begin consuming gRPC blocks in the logs) before moving on.
> 2. **API server.** Once the indexer is up and the `environment_info` row is populated, start the API.
> 3. **Query tracker** *(optional)*. Only needed if you want automatic DB index creation; safe to skip for a basic dev loop.

Run each service in a separate terminal:

```sh
# 1. Start the indexer (streams account updates from gRPC into Postgres)
cargo run -p cloudbreak -- --config ./cloudbreak.index.toml index

# 2. Start the API server (serves JSON-RPC queries from Postgres)
cargo run -p cloudbreak -- --config ./cloudbreak.api.toml api

# 3. (Optional) Start the query tracker — enables automatic DB index creation
cargo run -p cloudbreak -- --config ./cloudbreak.query-tracker.toml query-tracker
```

**First startup notes:**

- If `[snapshot]` is configured, the indexer will download a full Solana snapshot on first start. This can be **very large** (100+ GB for mainnet) and take significant time. With the default `tracker-config.yml` (which uses the public, rate-limited `https://api.mainnet.solana.com` endpoint), the download can also be throttled, so allow extra time or point `tracker-config.yml` at your own snapshot source — see [Cluster Tracker](#cluster-tracker). For a lighter local setup, either remove the `[snapshot]` section to skip snapshot loading entirely (the indexer will begin from live gRPC data only), or index a small program like `Stake11111111111111111111111111111111111111`.
- **The correct, healthy steady state requires the snapshot to be loaded.** Until the indexer finishes snapshot processing, the database holds only the partial data streamed in live from gRPC, and the `service_health` row stays unhealthy — so `getHealth` will return an error. This is expected: `getSlot` and `getProgramAccounts` work immediately as data flows in, but `getHealth` is intentionally the last thing to clear. **Running without `[snapshot]` is only meant for a quick smoke test of the full setup, or for iterating on a code change that doesn't need a complete dataset** — in that mode `getHealth` will *never* clear, by design. See [Troubleshooting: `getHealth` returns `INTERNAL_ERROR`](#gethealth-returns-internal_error) for details.
- The API example config has `[tracing] enabled = true`, which sends traces to the Tempo instance from Docker Compose. If you're not running the compose stack, set `enabled = false` or remove the `[tracing]` section to avoid connection errors in logs.

#### Manual PostgreSQL Setup (Alternative)

If you prefer not to use Docker, set up Postgres manually:

```sql
CREATE USER cloudbreak WITH PASSWORD 'cloudbreak';
CREATE DATABASE cloudbreak OWNER cloudbreak;
```

Then follow steps 2-4 above. The `pg_tracing` extension is optional and will be skipped if not installed.

## Cluster Tracker

For snapshot bootstrap and self-healing, the indexer queries a **cluster tracker** (`solcluster tracker` from [Blockdaemon's solana-cluster](https://github.com/Blockdaemon/solana-cluster), built from the [`rpcpool/solana-cluster` `triton` branch](https://github.com/rpcpool/solana-cluster/tree/triton)) on its `/v1/snapshots` endpoint. The tracker periodically scrapes one or more snapshot sources (Solana RPC nodes or `solcluster sidecar` instances running next to validators), aggregates the available snapshot archives, and tells the indexer which source to download each archive from.

For local development you have two options:

1. **Use the tracker provided by Docker Compose** (the default). The `solana-tracker` service in `docker-compose.yaml` builds a container directly from the `rpcpool/solana-cluster#triton` Git context, runs it locally, and exposes:
   - `:8458` — public API (`/v1/snapshots`, `/v1/snapshot/<filename>`). The indexer talks to this port.
   - `:8457` — internal/metrics endpoint. The Compose healthcheck uses this.
   - Config mounted from `./tracker-config.yml`.

   This is what the example configs point at out of the box and requires no extra setup.
2. **Point the indexer at any tracker endpoint you already have access to.** Simply set `[snapshot.tracker_endpoint].endpoint` in `cloudbreak.index.toml` (and `[tracker_endpoint].endpoint` in `cloudbreak.snapshot.toml` if you use the standalone snapshot subcommand) to the URL of an existing `solcluster tracker` instance, and remove or ignore the `solana-tracker` Compose service.

### `tracker-config.yml`

The config file controls how often the tracker scrapes its targets and which targets it scrapes. The default file shipped in the repo is:

```yaml
scrape_interval: 30s

target_groups:
  - group: mainnet
    http_targets:
      targets:
        - https://api.mainnet.solana.com
```

| Key                                    | Description                                                                                                                        |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `scrape_interval`                      | How often the tracker polls each target for snapshot metadata.                                                                     |
| `target_groups[].group`                | A label for the group (free-form).                                                                                                 |
| `target_groups[].http_targets.targets` | List of HTTP base URLs to scrape. Each target can be either a `solcluster sidecar` instance or a regular Solana RPC node that serves snapshot archives.                          |

> **Default works out of the box.** The `triton` tracker fork can scrape regular Solana RPC nodes that expose snapshot archives at the standard paths (`/snapshot.tar.bz2`, `/incremental-snapshot-<base>-<slot>-<hash>.tar.zst`), not just dedicated `solcluster sidecar` agents. The default target `https://api.mainnet.solana.com` is a real, working snapshot source — the indexer will be able to bootstrap from it without any further configuration. The trade-off is that it's a public, **rate-limited** endpoint: downloads can be slow, throttled, or temporarily refused under load. For production or heavy local use, replace the `targets:` list with your own `solcluster sidecar` instances (typically `http://<validator-host>:13080`) or your own RPC nodes that serve snapshots.

After changing `tracker-config.yml`, restart the service:

```sh
docker compose restart solana-tracker
```

The compose file also publishes the internal port, so you can hot-reload via `POST http://localhost:8457/reload` instead of restarting.

### Pointing the Indexer/Snapshot Crate at the Tracker

In `cloudbreak.index.toml`:

```toml
[snapshot]
accounts-file-concurency = 32

[snapshot.tracker_endpoint]
endpoint = "http://localhost:8458"
```

In `cloudbreak.snapshot.toml` (only when running the standalone `snapshot` subcommand):

```toml
[tracker_endpoint]
endpoint = "http://localhost:8458"
```

If you want to run a tracker outside Compose, point `endpoint` at it.

## Configuration Reference

### Indexer (`cloudbreak.index.toml`)

#### Top-level

| Key                          | Type    | Default | Description                                                                                                           |
| ---------------------------- | ------- | ------- | --------------------------------------------------------------------------------------------------------------------- |
| `finalize-slot-buffer-size`  | `usize` | `1000`  | Buffer size for queuing finalize-slot events.                                                                         |
| `accounts-owner-map-enabled` | `bool`  | `false` | Enables in-memory account-to-owner map for closed-account handling and owner-change tracking. Increases memory usage. |

#### `[snapshot]` (optional)

When this section is present, the indexer downloads and processes snapshots on startup for fast bootstrapping.

| Key                        | Type    | Default | Description                                                     |
| -------------------------- | ------- | ------- | --------------------------------------------------------------- |
| `accounts-file-concurency` | `usize` | (none)  | Max number of `AccountsFile` entries to process simultaneously. |

#### `[snapshot.tracker_endpoint]` (optional)

| Key        | Type     | Description                                                                                                                                                                                                             |
| ---------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint` | `String` | URL of the cluster tracker HTTP API (e.g. `http://localhost:8458`). The tracker is provided by Docker Compose for local dev — see [Cluster Tracker](#cluster-tracker) for how to point it at your own snapshot sources. |

If the `[snapshot]` section is omitted entirely, the indexer skips snapshot processing.

#### `[database]`

Shared across all services. Controls the SeaORM/SQLx connection pool and query timeouts.

| Key                              | Type       | Default        | Description                                                                          |
| -------------------------------- | ---------- | -------------- | ------------------------------------------------------------------------------------ |
| `url`                            | `String`   | **required**   | Postgres connection string.                                                          |
| `max-connections`                | `u32`      | (SQLx default) | Maximum pool connections.                                                            |
| `min-connections`                | `u32`      | (SQLx default) | Minimum pool connections.                                                            |
| `connect-timeout`                | `Duration` | (SQLx default) | Connection establishment timeout (e.g. `"30s"`).                                     |
| `idle-timeout`                   | `Duration` | (SQLx default) | How long idle connections are kept alive (e.g. `"10m"`).                             |
| `acquire-timeout`                | `Duration` | (SQLx default) | Max wait time to acquire a connection from the pool (e.g. `"60s"`).                  |
| `max-lifetime`                   | `Duration` | (SQLx default) | Max total lifetime of a connection (e.g. `"30m"`).                                   |
| `sqlx-logging`                   | `bool`     | (SQLx default) | Enable SQLx statement logging.                                                       |
| `schema-search-path`             | `String`   | (none)         | Postgres `search_path` (e.g. `"public"`).                                            |
| `test-before-acquire`            | `bool`     | (none)         | Ping connection before returning from pool.                                          |
| `connect-lazy`                   | `bool`     | (none)         | Defer connection until first use.                                                    |
| `partition-clustering-threshold` | `u64`      | (none)         | Partition size in bytes above which `CLUSTER` is skipped during snapshot processing. |
| `save-block-queries-timeout`     | `u64`      | `30`           | Timeout in seconds for save-block queries.                                           |
| `finalize-slot-queries-timeout`  | `u64`      | `300`          | Timeout in seconds for finalize-slot queries.                                        |
| `api-queries-timeout`            | `u64`      | `10`           | Timeout in seconds for API-facing queries.                                           |
| `server-side-timeout`            | `u64`      | `300000`       | Server-side statement timeout in milliseconds.                                       |
| `max-db-errors-threshold`        | `f64`      | `100`          | Number of accumulated DB errors before the process exits.                            |

#### `[grpc]`

| Key                    | Type     | Default           | Description                                                                          |
| ---------------------- | -------- | ----------------- | ------------------------------------------------------------------------------------ |
| `endpoint`             | `String` | **required**      | Yellowstone gRPC server URL.                                                         |
| `x-token`              | `String` | (none)            | Authentication token for the gRPC connection.                                        |
| `timeout`              | `u64`    | **required**      | Connection timeout in seconds.                                                       |
| `worker-count`         | `usize`  | (none)            | Number of workers handling subscription events simultaneously.                       |
| `channel-size`         | `usize`  | `1000`            | Buffer size for queuing subscription events from gRPC.                               |
| `chunk-size`           | `usize`  | `1000`            | Chunk size for subscription events.                                                  |
| `max-chunk-bytes-data` | `usize`  | `2097152` (2 MiB) | Max bytes per data chunk.                                                            |
| `max-grpc-errors`      | `usize`  | **required**      | Max gRPC errors before attempting reconnection (always reconnects on stream `None`). |

#### `[programs]`

Controls which program-owned accounts are indexed. The filter is synced to the `environment_info` table in Postgres so the API can read it.

| Key       | Type          | Default | Description                                                                             |
| --------- | ------------- | ------- | --------------------------------------------------------------------------------------- |
| `include` | `Vec<String>` | `[]`    | Base58-encoded program pubkeys to index. If non-empty, only these programs are indexed. |
| `exclude` | `Vec<String>` | `[]`    | Base58-encoded program pubkeys to exclude. Only used when `include` is empty.           |

Use either `include` or `exclude`, not both. If `include` is non-empty, `exclude` is ignored.

#### `[metrics]`

| Key                   | Type     | Default               | Description                             |
| --------------------- | -------- | --------------------- | --------------------------------------- |
| `host`                | `String` | `"0.0.0.0"`           | Prometheus metrics server bind address. |
| `port`                | `u16`    | `8875`                | Prometheus metrics server port.         |
| `subscription-id-key` | `String` | `"x-subscription-id"` | HTTP header name for subscription ID.   |

### API Server (`cloudbreak.api.toml`)

#### `[server]`

| Key                              | Type       | Default     | Description                                                                                                                                                                                              |
| -------------------------------- | ---------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                           | `String`   | `"0.0.0.0"` | Bind address for the JSON-RPC server.                                                                                                                                                                    |
| `port`                           | `u16`      | `4000`      | Listen port.                                                                                                                                                                                             |
| `max-connections`                | `u32`      | `100`       | Maximum concurrent HTTP connections.                                                                                                                                                                     |
| `batch-handling-max-concurrency` | `usize`    | `5`         | Maximum concurrent requests within a single batch JSON-RPC call.                                                                                                                                         |
| `gpa-stream-batch-size`          | `usize`    | `1000`      | Number of accounts grouped per batch in the streaming `getProgramAccounts` pipeline (DB fetch → encoding). See [Streamed Responses](#streamed-responses).                                                |
| `request-timeout`                | `Duration` | `"60s"`     | Total per-request wall-clock budget (handler + body transport). When exceeded, the response stream is truncated and the request is counted under the `timeout` status. See [Streamed Responses](#streamed-responses). |
| `max-multiple-accounts`          | `usize`    | `100`       | Maximum number of pubkeys accepted per `getMultipleAccounts` request. Requests exceeding this limit are rejected with an `InvalidParams` error.                                                          |

#### Streamed Responses

`getProgramAccounts` responses are emitted incrementally as a `Transfer-Encoding: chunked` JSON-RPC body. This keeps peak memory bounded and lets clients start parsing accounts before the server has finished fetching them.

Pipeline:

1. **DB fetch** — Postgres rows are streamed by `sqlx` and grouped into batches of `gpa-stream-batch-size` accounts.
2. **Unbounded channel** — completed batches are pushed onto an unbounded `tokio::mpsc` channel so the database connection can close as fast as possible (no head-of-line blocking from slow encoding/clients).
3. **Encoding** — each batch is decoded into `UiAccount`s (per-batch `spawn_blocking` to keep the runtime responsive).
4. **JSON serialization** — encoded accounts are serialized into a 64 KB pre-allocated `BytesMut`. Whenever the buffer reaches 32 KB, the filled portion is frozen into a `Bytes` chunk and yielded as a single HTTP body frame. Accounts larger than the chunk threshold cause the buffer to grow naturally and are flushed as a single oversized frame.

The first batch is always peeked synchronously before the response status line is committed — so SQL errors, parameter errors, and similar early failures still surface as a proper JSON-RPC error response, not a truncated `200`. Errors that happen mid-stream truncate the body intentionally; clients see a JSON parse error rather than a silently-truncated valid document.

`request-timeout` is enforced by a `TrackedBody` wrapping the response body. On expiry the body is truncated, the `cloudbreak_api_request_duration_ms` `http_with_transport` observation is **not** recorded, and a `cloudbreak_api_requests_total{method="http",status="timeout"}` counter is incremented instead.

#### `[gpa-cache]` (optional)

Optional in-memory cache for `getProgramAccounts` responses. Omitting this section disables the module entirely.

| Key                   | Type    | Default      | Description                                                                                                                                          |
| --------------------- | ------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max-total-bytes`     | `usize` | **required** | Maximum cache size in bytes. When inserting a new query would exceed this, the oldest cached queries (by slot) are evicted until enough room exists. |
| `min-bytes-per-query` | `usize` | **required** | Minimum serialized size of a query for it to be cached at all. Smaller queries are skipped so they don't dilute room available for heavier ones.     |

The cache is keyed by `(program, sorted filters, encoding, data_slice, commitment)` and holds the **pre-serialized JSON bytes** of each account, ref-counted via `bytes::Bytes`. Concretely:

- **Postgres only sends what's missing.** When a cached entry exists for a query, the SQL returns full row data only for accounts whose slot is newer than the cached slot; for everything else it returns just `(pubkey, slot)` to confirm the cached version is still valid. Less data over the wire, less server-side decoding.
- **Cache hits are zero-copy and zero-allocation.** Cached account bytes are appended into the outgoing response buffer by ref-counting an existing `Bytes` slice — no clone, no re-encode, no extra allocation.
- **No JSON re-serialization.** Cached entries store the exact JSON fragment that already left the encoder once; on subsequent serves the same bytes are emitted verbatim.

Eviction is age-ordered by slot (oldest cached query goes first). Cache state is observable at runtime via [`/debug/modules/gpa_cache`](#api-server-default-4000).

#### `[database]`

Same as the indexer `[database]` section. For the API, `api-queries-timeout` and `server-side-timeout` are the most relevant timeout settings.

#### `[metrics]`

Same as indexer `[metrics]`.

#### `[query-tracker-client]`

Connects the API to the query tracker service for reporting GPA query patterns.

| Key              | Type       | Default      | Description                                                              |
| ---------------- | ---------- | ------------ | ------------------------------------------------------------------------ |
| `endpoint`       | `String`   | **required** | Query tracker RPC endpoint (e.g. `http://localhost:4001`).               |
| `timeout`        | `Duration` | (none)       | Request timeout (e.g. `"5s"`).                                           |
| `flush-interval` | `Duration` | (none)       | How often batched query counts are flushed to the tracker (e.g. `"1m"`). |

#### `[slot-syncronizer]` (optional)

Periodically syncs the latest slot from the database for consistency checks. Enabled by default; omit this section or set `enabled = false` to disable.

| Key           | Type   | Default | Description                          |
| ------------- | ------ | ------- | ------------------------------------ |
| `enabled`     | `bool` | `true`  | Enable/disable slot synchronization. |
| `interval_ms` | `u64`  | `200`   | Sync interval in milliseconds.       |

#### `processed-commitment` (top-level, optional)

Controls how the API handles requests that specify the `processed` commitment level. This is a top-level key (not inside any section).

| Value             | Description                                                                  |
| ----------------- | ---------------------------------------------------------------------------- |
| `"reject"`        | **(default)** Return an error when a client requests `processed` commitment. |
| `"use-confirmed"` | Silently respond with `confirmed` data instead of rejecting the request.     |

Example:

```toml
processed-commitment = "use-confirmed"
```

#### `[tracing]` (optional)

OpenTelemetry tracing configuration. If this section is omitted, OTel is disabled and only `tracing-subscriber` is used.

| Key               | Type          | Default                   | Description                                                          |
| ----------------- | ------------- | ------------------------- | -------------------------------------------------------------------- |
| `enabled`         | `bool`        | (none)                    | Enable OTel export.                                                  |
| `endpoint`        | `String`      | `"http://localhost:4317"` | OTLP gRPC collector endpoint.                                        |
| `sample-ratio`    | `f64`         | (none)                    | Trace sampling ratio (0.0 to 1.0).                                   |
| `export-interval` | `u64`         | (none)                    | Export interval in seconds.                                          |
| `max-batch-size`  | `u64`         | (none)                    | Max spans per export batch.                                          |
| `max-queue-size`  | `u64`         | (none)                    | Max queued spans before dropping.                                    |
| `track-idle-time` | `bool`        | (none)                    | Include idle time in spans.                                          |
| `span-filter`     | `Vec<String>` | (none)                    | Span names to export. See `example.cloudbreak.api.toml` for the current full list (covers gPA, gTABO/gTABD, JSON encoding, HTTP transport, mint lookups, and the cache finalize span). |

### Query Tracker Service (`cloudbreak.query-tracker.toml`)

#### `[server]`

| Key               | Type     | Default     | Description                 |
| ----------------- | -------- | ----------- | --------------------------- |
| `host`            | `String` | `"0.0.0.0"` | Bind address.               |
| `port`            | `u16`    | `4001`      | Listen port for JSON-RPC.   |
| `max-connections` | `u32`    | `100`       | Max concurrent connections. |

#### `[database]`

Same as other services.

#### `[metrics]`

| Key    | Type     | Default     | Description                                              |
| ------ | -------- | ----------- | -------------------------------------------------------- |
| `host` | `String` | `"0.0.0.0"` | Metrics bind address.                                    |
| `port` | `u16`    | `8876`      | Metrics port (note: different default from API/indexer). |

#### `[query-tracker]`

Controls the automatic database index creation behavior.

| Key                           | Type          | Default      | Description                                                                                                                               |
| ----------------------------- | ------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `create-database-indexes`     | `bool`        | `false`      | Enable automatic `CREATE INDEX` based on query frequency.                                                                                 |
| `index-generation-threshold`  | `u32`         | `10`         | Number of times a unique query must be seen before it becomes eligible for indexing.                                                      |
| `index-creation-delay`        | `Duration`    | `"10s"`      | Delay between `CREATE INDEX` operations to avoid overloading the database.                                                                |
| `query-counts-reset-interval` | `Duration`    | `"24h"`      | Interval at which the query count queue is cleared.                                                                                       |
| `included-programs`           | `Vec<String>` | `[]`         | Only create indexes for these program pubkeys. Empty means all programs are eligible.                                                     |
| `excluded-programs`           | `Vec<String>` | `[]`         | Never create indexes for these program pubkeys.                                                                                           |
| `indexer-metrics`             | `String`      | **required** | `host:port` of the indexer's Prometheus metrics endpoint (e.g. `"localhost:8875"`). Used to check indexer health before creating indexes. |
| `indexer-metrics-threshold`   | `u64`         | `5`          | The `cloudbreak_finalize_slot_handler_queue_size` metric threshold; index creation is paused when the indexer queue exceeds this value.    |

### Snapshot Processor (`cloudbreak.snapshot.toml`)

> **Note:** Snapshot processing is already built into the normal indexer operation (when `[snapshot]` is present in the indexer config). The standalone `snapshot` subcommand exists for special cases where you need to run snapshot ingestion independently from the indexer — for example, to bulk-load account state into an existing database without starting a gRPC subscription. In most setups you do not need to run this separately.

#### Top-level

| Key                        | Type    | Description                                                     |
| -------------------------- | ------- | --------------------------------------------------------------- |
| `accounts-file-concurency` | `usize` | Max number of `AccountsFile` entries to process simultaneously. |

#### `[tracker_endpoint]`

| Key        | Type     | Description                                                                                           |
| ---------- | -------- | ----------------------------------------------------------------------------------------------------- |
| `endpoint` | `String` | Cluster tracker HTTP API URL (e.g. `http://localhost:8458`). See [Cluster Tracker](#cluster-tracker). |

#### `[database]`

Same as other services. The `partition-clustering-threshold` is particularly relevant here (skips `CLUSTER` on partitions larger than this size in bytes).

#### `[programs]`

Same include/exclude program filter as the indexer.

#### `[metrics]`

Same as other services.

### Environment Variables

| Variable                     | Where         | Description                                                                                                                                                                  |
| ---------------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RUST_LOG`                   | All binaries  | Tracing filter directive. Default: `info,sqlx=error`. Supports per-target filtering (e.g. `debug,hyper=warn`).                                                                |
| `DATABASE_URL`               | Migration CLI | Standard SeaORM/SQLx convention for the migration tool. Can also be supplied via the `-u <url>` CLI flag (which takes precedence).                                            |
| `CLOUDBREAK_MIGRATION_CONFIG` | Migration CLI | **Required.** Filesystem path to the migration TOML config (partitioning shape + per-index toggles). See [`crates/migration/README.md`](crates/migration/README.md) for the schema. |

## Automatic Database Index Creation

The query tracker enables automatic creation of database indexes based on observed query patterns. This is how it works:

1. The API server counts unique `getProgramAccounts`-shaped queries and reports them to the query tracker service.
2. When a query pattern exceeds `index-generation-threshold` within a reset interval, it becomes eligible for indexing.
3. A background task in the query tracker pops the highest-count query from the priority queue and runs `CREATE INDEX` on the database.
4. Only one index is created at a time, with a configurable delay (`index-creation-delay`) between operations to avoid overloading the database.
5. Before creating an index, the query tracker checks the indexer's `cloudbreak_finalize_slot_handler_queue_size` metric. If the queue size exceeds `indexer-metrics-threshold`, index creation is paused until the indexer catches up.
6. The query count queue is cleared every `query-counts-reset-interval` to avoid stale patterns driving index creation.
7. Use `included-programs` and `excluded-programs` to control which programs are eligible for automatic indexing.

## Self-Healing (Slot Gap Detection and Repair)

The indexer includes a self-healing mechanism that automatically detects and repairs gaps in the slot stream. This is critical for maintaining data consistency when the gRPC stream drops slots due to network issues, reconnections, or transient failures.

### How It Works

1. **Gap detection (no RPC):** As each block arrives from the gRPC stream, the indexer checks that it builds directly on the last block it received, using the block's own `parent_slot` / `parent_blockhash` against the in-memory blocks map. If it does, any slots in between were simply empty/skipped by the validator and are ignored. If it does not, at least one real block was missed: the whole intervening range is queued for repair in `gaps_list`. This replaces the previous `getBlocksWithLimit` RPC confirmation — gaps are confirmed purely from the chain.

2. **Pause + mark unhealthy:** The instant a gap is confirmed, finalization is **paused** and the service is flagged **unhealthy** via the `service_health` table. Live finalized notifications keep buffering (bounded by `finalize-slot-buffer-size`, then back-pressuring the stream) but are not applied until the gap is repaired, preserving in-order finalization.

3. **Gap filling via incremental snapshots:** Every 30 s a background task processes confirmed gaps by asking the cluster tracker for an incremental snapshot pair covering the newest missing slot, downloading it (into a timestamped directory), and processing **only the gap slots**. Repaired accounts are written to the database and enqueued for finalization directly (snapshot data is already finalized). Gap slots that have **no accounts in the snapshot** are empty/skipped slots: they are logged (target `self_healing_empty_slots`) and dropped from the list. If the tracker has no covering pair yet, the task retries on the next tick.

4. **Missed finalized notifications:** A reconnect can also drop finalized notifications for slots just *below* a large gap. When finalizing a live slot the finalizer walks its ancestor chain (hash-checked) to finalize any ancestors whose notification was missed; additionally the slot just before each repaired gap (`gap_start - 1`) is seeded so its ancestors are caught even though repaired slots carry no chain data to bridge the walk.

5. **Recovery:** Once `gaps_list` drains, finalization is **resumed**, which restores the service to healthy. Out-of-order slots that arrive late (e.g. gRPC buffering) are removed from the gaps list when received.

> A confirmed gap is only expected after startup has finished. During startup the (now-paused) finalizer worker is what completes startup, so a gap there can never be repaired; the fill task fails fast (panics) rather than stalling forever.

### Monitoring

Use the indexer's [operational debug endpoints](#indexer-default-metrics-port-8875) to inspect gap and finalizer state:

- `/debug/modules/self_healing` — chain tips, still-missing slots grouped into gaps (with boundary, length, and distance behind confirmed), and summary counts.
- `/debug/modules/finalizer` — the confirmed-but-not-finalized blocks map, the ordered pending queue, and pause state.

The `[snapshot]` section (with `[snapshot.tracker_endpoint]`) must be present in the indexer config for gap filling to work, since it relies on the cluster tracker to discover and download incremental snapshots.

## Program Filter Configuration

Programs are configured in two different places, each serving a different purpose. Understanding how they relate helps avoid confusion:

| Config                                        | Location                                              | Purpose                                                                                                                                                                                                                                                                              |
| --------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **`[programs]`** section                      | Indexer config (`cloudbreak.index.toml`)               | Controls **which accounts are indexed**. Only accounts owned by included programs (or all except excluded programs) are written to the database. This filter is synced to the `environment_info` table so the API knows which programs are available.                                 |
| **`included-programs` / `excluded-programs`** | Query tracker config (`cloudbreak.query-tracker.toml`) | Controls **which programs are eligible for automatic index creation**. This is a subset concern — it only makes sense to auto-index programs that are already being indexed. Use `excluded-programs` to skip programs like SPL Token that have too many accounts for useful indexes. |

These two are independent but related: the indexer `[programs]` determines what data exists, and the query tracker programs control which of that data gets automatic database indexes.

> **Note (storage layout):** If you want certain programs to get dedicated LIST partitions in the `accounts` / `snapshot_accounts` tables (a schema-level storage optimization, separate from the indexing filter above), configure `[pg-owner-partitions].programs-for-list-partition` in the migration TOML. This must be set before running migrations; changing it later requires re-running migrations (e.g. `fresh`). See [`crates/migration/README.md`](crates/migration/README.md).

## Optional Modules

### Snapshot on Indexer Startup

When the `[snapshot]` section (with `[snapshot.tracker_endpoint]`) is present in the indexer config, the indexer queries the cluster tracker, downloads the latest covering snapshot pair from the source the tracker reports, and processes the archives before beginning gRPC streaming. This provides fast bootstrapping of account state. Omit the entire `[snapshot]` section to skip this.

### Account Owner Map

Set `accounts-owner-map-enabled = true` in the indexer config to maintain an in-memory map of account pubkey to owner + slot. This is used for tracking owner changes and closed-account handling. It increases memory usage but improves correctness for owner-change scenarios.

### Slot Synchronizer

The API server's `[slot-syncronizer]` section controls periodic slot fetching from the database. Enabled by default (200ms interval). Disable with `enabled = false` if not needed.

### Query Tracker Integration

The API can operate with or without the query tracker. To disable it, remove the `[query-tracker-client]` section from the API config. Without it, no query patterns are tracked and automatic index creation is unavailable.

### GPA Cache

Set `[gpa-cache]` in the API config to enable an in-memory cache for `getProgramAccounts` responses. The cache stores pre-serialized JSON bytes (ref-counted, zero-copy on hits) and changes the SQL to skip re-sending the body of already-cached accounts. See [`[gpa-cache]` (optional)](#gpa-cache-optional) for the config keys and a benefits breakdown. Omit the section to disable.

### OpenTelemetry Tracing

Add a `[tracing]` section to any service config to enable OTLP trace export. When omitted, only standard `tracing-subscriber` logging is used.

## HTTP Endpoints

Each service exposes HTTP endpoints on its metrics port (or server port for the API). Below is a breakdown per service.

### API Server (default `:4000`)

| Endpoint                           | Method | Description                                                                                                                                                                                                                                                                                                                                                                |
| ---------------------------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/`                                | POST   | JSON-RPC endpoint for all RPC methods (`getProgramAccounts`, `getTokenAccountsByOwner`, etc.). Supports batch requests.                                                                                                                                                                                                                                                    |
| `/metrics`                         | GET    | Prometheus metrics.                                                                                                                                                                                                                                                                                                                                                        |
| `/debug/log_filter`                | GET    | Returns the current `tracing` log filter.                                                                                                                                                                                                                                                                                                                                  |
| `/debug/log_filter?filter=<value>` | GET    | Dynamically updates the log filter at runtime without restarting the service. Example: `curl "http://localhost:4000/debug/log_filter?filter=debug,hyper=warn"`                                                                                                                                                                                                             |
| `/debug/modules/gpa_cache`         | GET    | Inspects the live GPA cache. Supports `?detail=summary\|queries\|full`, `?program=<pubkey>`, `?with_pubkeys=true`, `?min_size_bytes`, `?max_size_bytes`, `?limit`. Returns `{"enabled": false}` when `[gpa-cache]` is not configured. See the rustdoc on `gpa_cache_handler` in `crates/api/src/http/operational_endpoints.rs` for the full query-parameter and response-body reference. |

### Indexer (default metrics port `:8875`)

| Endpoint                      | Method | Description                                                                                                                                                                                                                            |
| ----------------------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/metrics`                    | GET    | Prometheus metrics (block processing, chunk processing, slot finalization, gRPC stats, etc.).                                                                                                                                          |
| `/debug/modules/finalizer`    | GET    | Inspects the live slot finalizer (confirmed-but-not-finalized blocks map, ordered pending queue, pause state). Returns JSON with the DB chain tips (`confirmed`/`finalized` + lag). Supports `?detail=summary\|slots\|full`, `?kind=all\|live\|repaired`, `?min_slot`, `?max_slot`, `?limit`, `?with_pubkeys=true`. See the rustdoc on `handle` in `crates/index/src/operational_endpoints/finalizer.rs` for the full reference.                       |
| `/debug/modules/self_healing` | GET    | Inspects self-healing gap state. Returns JSON with the DB chain tips and the still-missing slots grouped into gaps (each with `boundary_slot`, `start`/`end`, `len`, and `slots_behind_confirmed`). Supports `?detail=summary\|slots\|full` (`summary` returns only the `stats` counts, omitting the gap list), plus `?min_slot`, `?max_slot`, `?limit` to filter the gap view. See the rustdoc on `handle` in `crates/index/src/operational_endpoints/self_healing.rs`.                                                                                                                   |
| `/debug/accounts_owner_map`   | GET    | Returns debug info about the in-memory account-to-owner map. Only populated when `accounts-owner-map-enabled = true` in the indexer config.                                                                                            |

### Query Tracker (default metrics port `:8876`)

| Endpoint   | Method | Description                                 |
| ---------- | ------ | ------------------------------------------- |
| `/metrics` | GET    | Prometheus metrics.                         |
| `/health`  | GET    | Returns `200 OK` if the service is running. |

## Metrics Reference

All metrics are emitted in the Prometheus text exposition format on each service's `/metrics` endpoint (see [HTTP Endpoints](#http-endpoints)). Metric names are stable and intended to be referenced from Grafana panels and alert rules. Labels are described per-metric below; when label values are enumerated, they are the complete set currently emitted by the code.

### API Server (`cloudbreak-api`)

| Metric                                              | Type              | Labels                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| --------------------------------------------------- | ----------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cloudbreak_api_requests_total`                      | Counter           | `method`, `status`     | Count of RPC method invocations grouped by outcome. `method` ∈ {`gPA`, `gTABO`, `gTABD`, `gTABM`, `gAI`, `getBalance`, `getMultipleAccounts`, `getTokenAccountBalance`, `http`}: `gPA` = `getProgramAccounts`, `gTABO` = `getTokenAccountsByOwner`, `gTABD` = `getTokenAccountsByDelegate`, `gTABM` = `getTokenAccountsByMint`, `gAI` = `getAccountInfo`, `http` is connection-level. `status` ∈ {`success`, `error`, `timeout`}: `error` is incremented on RPC-level failures (bad params, DB failure, stream-mid-error); `timeout` is incremented on `http` when the total `request-timeout` fires. The point-lookup methods (`gAI`, `getBalance`, `getMultipleAccounts`, `getTokenAccountBalance`) emit both `success` and `error`. The streaming methods (`gPA`, `gTABO`, `gTABD`, `gTABM`) emit `error` only — for their total throughput use `cloudbreak_api_request_duration_ms` instead. Note: `getSlot`, `getHealth`, `getVersion`, and `getGenesisHash` are not currently surfaced under this counter.                                                                                                                                                                          |
| `cloudbreak_api_request_duration_ms`                 | Histogram         | `method`, `bytes`      | Per-stage request latency in milliseconds. `bytes` is the response-size bucket (`0-1KB`, `1-10KB`, `10-100KB`, `100KB-1MB`, `1MB-10MB`, `10MB-50MB`, `50MB-100MB`, `100MB-200MB`, `200MB-500MB`, `500MB+`). `method` values: `gpa` / `gpa_mint` (total in-handler time for `getProgramAccounts`, with `_mint` suffix when a token-mint filter is applied), `gpa_db` (Postgres query time), `gpa_db_first_row_time` (time-to-first-row), `gpa_encode` (account-encoding time), `gpa_json` (JSON serialization time); analogous `gtabo*` / `gtabd*` for the token-account methods; `gAI` / `getBalance` / `getMultipleAccounts` / `getTokenAccountBalance` (single observation per request — total handler + serialization time for the point-lookup methods); `http_with_transport` (end-to-end including body transport, label `bytes` reflects response size); `http_connection` (per-TCP-connection lifetime, label `bytes="0"`). |
| `cloudbreak_api_requests_by_subscription_id`         | Counter           | `subscription_id_key`  | Per-client request counter, attributed via the HTTP header configured by `[metrics].subscription-id-key` (default `x-subscription-id`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `cloudbreak_api_data_fetched_by_subscription_id`     | Counter           | `subscription_id_key`  | Per-client cumulative response size in bytes (JSON-encoded payload).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `cloudbreak_api_inflight_requests`                   | Gauge             | `method`               | Currently in-flight requests per stage. Emitted values: `http_connection` (live TCP connections), `http` (active HTTP requests), `gpa`, `gtabo`, `gai`, `gma`, `getBalance`, `getTokenAccountBalance`. The special label `max` is set once at startup to `[server].max-connections`; plot against the live gauges to visualize saturation.                                                                                                                                                                                                                                                                                                                                                                                    |
| `cloudbreak_api_batch_requests_total`                | Counter           | `batch_size`           | Counter incremented once per batch JSON-RPC request, bucketed by batch size: `1-5`, `6-10`, `11-20`, `21-50`, `51-100`, `100+`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |

### Indexer (`cloudbreak-index`)

| Metric                                              | Type              | Labels        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| --------------------------------------------------- | ----------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cloudbreak_block_processing`                        | Histogram         | `origin`      | Total time in **seconds** to process a full gRPC block (parse + per-chunk insert). `origin` = `block`. Buckets: 10 ms – 10 s.                                                                                                                                                                                                                                                                                                                                                                                |
| `cloudbreak_chunk_processing`                        | Histogram         | `origin`      | Per-chunk DB insert latency in **seconds**. `origin` = `block`. Buckets: 10 ms – 10 s.                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `cloudbreak_block_size`                              | Histogram         | `origin`      | Block size in bytes. `origin` = `block`. Buckets: 100 KB – 30 MB.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `cloudbreak_chunk_size`                              | Histogram         | `origin`      | Chunk size in bytes. `origin` = `chunk`. Buckets: 50 KB – 2 MB.                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `cloudbreak_finalize_slot`                           | Histogram         | `origin`      | Finalize-slot stage latency in **seconds**. `origin` values: `total` (full finalize cycle including all sub-tasks), `cleanup_closed_accounts`, `cleanup_accounts_batch`, `cleanup_snapshot_accounts_batch`, `cleanup_snapshot_closed_accounts`, `cleanup_startup_snapshot_accounts_batch`. Buckets: 5 ms – 10 s.                                                                                                                                                                                              |
| `cloudbreak_finalize_slot_deleted_accounts`          | Histogram         | —             | Number of older account versions deleted per `cleanup_accounts` call. Useful to spot slots that churn an unusually large number of rows.                                                                                                                                                                                                                                                                                                                                                                     |
| `cloudbreak_new_accounts_in_slot`                    | Histogram         | `origin`      | Accounts seen in a slot. `origin` = `new_accounts_in_slot` (accounts net-new to the database after finalize-time deduplication) or `block_accounts_total` (raw count of account updates in the block, including overwrites). The ratio gives a sense of write churn per slot.                                                                                                                                                                                                                                |
| `cloudbreak_grpc_buffer_channel_size`                | Histogram         | `origin`      | gRPC ingestion buffer depth sampled per push. `origin` = `grpc_buffer_channel_size`. Buckets: 1 – 10 000. Rising values indicate the indexer is falling behind the stream.                                                                                                                                                                                                                                                                                                                                   |
| `cloudbreak_grpc_buffer_channel_size_sender`         | IntGauge          | —             | Latest gRPC buffer channel depth (last sample, gauge form for easy alerting).                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cloudbreak_grpc_timeout_errors`                     | Counter           | —             | gRPC stream timeout errors.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `cloudbreak_grpc_errors`                             | Counter           | —             | gRPC errors other than timeouts (transport, decoding, etc.).                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `cloudbreak_grpc_total_updates_received`             | Counter           | —             | Total messages received from the gRPC subscription. Use the rate as the input-side throughput.                                                                                                                                                                                                                                                                                                                                                                                                               |
| `cloudbreak_grpc_gap_errors`                         | Counter           | —             | Slot gaps detected on gRPC reconnection. Each increment corresponds to a missing-slot range observed at reconnect.                                                                                                                                                                                                                                                                                                                                                                                           |
| `cloudbreak_db_errors`                               | Counter           | —             | All database errors (insert / cleanup / read) hit by the indexer. When the cumulative count exceeds `database.max-db-errors-threshold` (default `100`), the indexer process exits.                                                                                                                                                                                                                                                                                                                           |
| `cloudbreak_closed_accounts_per_slot`                | Histogram         | —             | Number of accounts marked closed per slot. Buckets: 1 – 10 000.                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `cloudbreak_insert_closed_accounts_per_slot_ms`      | Histogram         | —             | Latency in **milliseconds** of inserting closed-account markers for one slot. Buckets: 0.1 ms – 10 s.                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cloudbreak_current_tokio_tasks`                     | IntGauge          | `task_type`   | Current count of spawned Tokio tasks per category. `task_type` values: `grpc`, `finalize_slot_internal`, `self_healing`, `self_healing_fill_gaps`, `snapshot_processing`, `insert_accounts_chunk`, `insert_closed_accounts`, `startup_snapshot_accounts_cleanup`, `metrics_server`. Counter is guarded by `TokioTaskCounterGuard` so panicking tasks still decrement.                                                                                                                                         |
| `cloudbreak_finalize_slot_handler_queue_size`        | IntGauge          | —             | Pending items in the finalize-slot handler channel. The query tracker reads this via the configured `indexer-metrics` endpoint to pause `CREATE INDEX` when the indexer is behind (`indexer-metrics-threshold`).                                                                                                                                                                                                                                                                                             |

### Snapshot (`cloudbreak-snapshot`)

Snapshot metrics are emitted whenever snapshot processing runs — either embedded in the indexer at startup (when `[snapshot]` is configured) or via the standalone `snapshot` subcommand. They piggyback on the metrics endpoint of the calling service.

| Metric                                                                  | Type      | Labels  | Description                                                                                                                                                                                                                                                                                            |
| ----------------------------------------------------------------------- | --------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cloudbreak_db_snapshot_errors`                                          | Counter   | —       | Database error counter for snapshot-time inserts and dedup queries.                                                                                                                                                                                                                                    |
| `cloudbreak_snapshot_batch_insert_time`                                  | Histogram | —       | Latency in **seconds** of a single batched `AccountsFile` insert. Uses Prometheus default buckets.                                                                                                                                                                                                     |
| `cloudbreak_processed_snapshot_items`                                    | Gauge     | `type`  | Snapshot ingestion progress. `type` ∈ {`accounts_files_total` (count of completed `AccountsFile` entries — incremented as a counter despite the gauge type), `accounts_files_percentage` (0 – 100), `accounts_total` (cumulative account count written from the snapshot)}.                            |
| `cloudbreak_snapshot_accounts_buffer_size`                               | Gauge     | `type`  | Snapshot-time intermediate buffer depths. `type` = `cleanup_duplicated` (size of the dedup-stage receive channel).                                                                                                                                                                                     |
| `cloudbreak_snapshot_clean_up_duplicated_accounts_batch_time`            | Histogram | —       | Latency in **seconds** of de-duplicating a batch of overlapping snapshot accounts against the live `accounts` table. Uses Prometheus default buckets.                                                                                                                                                  |
| `cloudbreak_snapshot_clean_up_duplicated_accounts_batch_rows_affected`   | Counter   | `type`  | Registered but not currently observed in code; reserved for surfacing the row count touched by each dedup batch.                                                                                                                                                                                       |

### Query Tracker (`cloudbreak-query-tracker`)

The query-tracker service exposes `/metrics` for protocol compatibility but does not currently register any service-specific metrics. The endpoint returns an empty Prometheus payload. Use `/health` for liveness probes.

## dbtools CLI

The `dbtools` binary is a standalone operator CLI for running analytics and configuration commands against one or more Postgres servers.

**Config file** (`cloudbreak.dbtools.toml`):

```toml
[[servers]]
name = "production"
database_url = "postgres://user:pass@host/cloudbreak"

[[servers]]
name = "staging"
database_url = "postgres://user:pass@host2/cloudbreak"
```

### Analytics Commands

```sh
cargo run -p cloudbreak-dbtools -- analytics <command>
```

| Command                 | Description                                                                             |
| ----------------------- | --------------------------------------------------------------------------------------- |
| `get-biggest-programs`  | Top program owners by account count across partitions.                                  |
| `table-size`            | Database and table sizes (heap, indexes, total) for `accounts` and `snapshot_accounts`. |
| `indexes-sizes`         | Aggregated index sizes across the partition tree.                                       |
| `partition-sizes`       | Recursive partition hierarchy with sizes.                                               |
| `distinct-owners-count` | Per-partition `COUNT(DISTINCT owner)` with totals.                                      |
| `mint-accounts-count`   | SPL Token and Token-2022 mint account counts.                                           |
| `indexes-count`         | Total number of indexes on `accounts` and `snapshot_accounts`.                          |
| `slow-queries`          | Currently running queries exceeding 1 minute (from `pg_stat_activity`).                 |
| `get-delegates`         | Sample delegated token accounts; outputs benchmark request bodies.                      |
| `accounts-count`        | Total row counts for `accounts` and `snapshot_accounts`.                                |

See [`crates/dbtools/README.md`](crates/dbtools/README.md) for more details.

## Integration Tests

The `integration_tests` crate is a standalone binary for benchmarking and correctness testing of any Solana RPC endpoint (not just Cloudbreak). It supports constant-rate load generation, dual-endpoint comparison with slot compensation, multiple request sources (static files, VictoriaLogs, mismatch replays), and optional database forensics.

### Quick Start

```sh
# Copy and edit the example config
cp example.cloudbreak.integration_tests.toml cloudbreak.integration_tests.toml

# Run a benchmark
cargo run --bin integration_tests -- benchmark gpa
cargo run --bin integration_tests -- benchmark gtabo
cargo run --bin integration_tests -- benchmark gtabd
```

### Commands

| Command            | Description                                                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `benchmark <type>` | Main command. Load test with optional dual-endpoint comparison. Types: `gpa`, `gtabo`, `gtabd`, `gpa-token-owner`, `gpa-token-mint`. |
| `compare`          | (Legacy) Full pubkey set comparison between two endpoints with transaction history checks.                                           |
| `get-slot`         | (Legacy) Polls `getSlot` on rpc1 every 100ms.                                                                                        |

See [`crates/integration_tests/README.md`](crates/integration_tests/README.md) for the full documentation including configuration reference, comparison semantics, slot compensation, logging targets, and output format.

## Development

### Building

```sh
cargo build
cargo build --release
```

### Logging

All services use `tracing` with `RUST_LOG` environment variable control:

```sh
# Default level
RUST_LOG=info cargo run -p cloudbreak -- --config ./cloudbreak.index.toml index

# Debug with SQLx silenced
RUST_LOG=debug,sqlx=error cargo run -p cloudbreak -- --config ./cloudbreak.api.toml api

# Target-specific filtering
RUST_LOG=cloudbreak_api=debug,hyper=warn cargo run -p cloudbreak -- --config ./cloudbreak.api.toml api
```

### Database Access

All crates use [SeaORM](https://www.sea-ql.org/SeaORM/) for database access with the `sqlx-postgres` + `runtime-tokio-rustls` backend.

### Observability Stack

See the [infrastructure table](#1-start-infrastructure) in Getting Started for all ports and services provided by `docker compose up`. Prometheus scrape targets are configured in `prometheus-config.yaml`.

## Troubleshooting

### Migration shows `pg_tracing` warning

The accounts table migration attempts to load the `pg_tracing` Postgres extension. If the extension is not installed (e.g. stock Docker Postgres image), a warning is printed but the migration continues successfully. The extension is optional and only needed for detailed query plan tracing via the dbtools `pg-tracing-high-detail` command.

### Postgres container crashes with "database files are incompatible"

This happens when the Docker volume contains data from a different Postgres major version (e.g. data initialized by PG17 but the compose file uses PG16). Fix by removing the volume and starting fresh:

```sh
docker compose down -v
docker compose up -d
```

### Indexer shows unhealthy status

The `service_health` table is set to unhealthy when confirmed slot gaps are detected. Check the indexer's `/debug/modules/self_healing` endpoint to see which slots are missing:

```sh
curl http://localhost:8875/debug/modules/self_healing
```

If the `[snapshot]` section is configured, the self-healing mechanism will automatically attempt to fill gaps via incremental snapshots fetched through the cluster tracker. If it's not configured (or the tracker has no source producing usable snapshots), gaps cannot be repaired automatically and require manual intervention (e.g. re-running a snapshot or restarting the indexer).

### `getHealth` returns `INTERNAL_ERROR`

The `service_health` row is only flipped to healthy **after the indexer finishes snapshot processing**. The flag is never set from live gRPC streaming alone, by design — gRPC catch-up cannot produce a complete account state on its own, so the API has no way to know the dataset is correct until a snapshot has been ingested.

This produces two distinct situations:

- **`[snapshot]` is configured and the indexer is still loading it.** `getHealth` will return an error for the entire duration of snapshot processing (can be hours for mainnet) and clear automatically once it completes. This is expected. `getSlot` and `getProgramAccounts` work normally during this window — only `getHealth` is gated.
- **`[snapshot]` is _not_ configured (no-snapshot / smoke-test mode).** `getHealth` will return `INTERNAL_ERROR` **permanently**, because the only code path that sets the health flag is snapshot completion. This is also expected: the no-snapshot mode is only meant for verifying the full setup wires up correctly, or for iterating on a code change that doesn't require a complete dataset. Add a `[snapshot]` section to the indexer config if you want `getHealth` to eventually clear.

If you've configured `[snapshot]` and `getHealth` still isn't clearing after the snapshot finished downloading, check the indexer logs for snapshot-processing errors and the `service_health` row directly:

```sh
psql "$DATABASE_URL" -c 'SELECT * FROM service_health;'
```

### Indexer falling behind (growing gRPC buffer)

If the indexer can't keep up with the gRPC stream, the buffer channel fills up. Monitor the `cloudbreak_grpc_buffer_channel_size` Prometheus metric. Common causes:

- Database writes are too slow — check `slow-queries` via dbtools or increase `max-connections`.
- Snapshot processing is running (expected during startup) — wait for it to finish.
- Too many programs selected — narrow the `[programs].include` list.

### Query tracker pauses index creation

The query tracker checks the indexer's `cloudbreak_finalize_slot_handler_queue_size` metric before creating indexes. If the queue exceeds `indexer-metrics-threshold` (default `5`), index creation is paused to avoid overloading the database while the indexer is catching up. This is expected behavior — index creation resumes automatically once the queue drains.

### API returns "environment_info row not found"

The API loads the program filter from the `environment_info` table, which is populated by the indexer on startup. This error means the indexer has never run against this database. Start the indexer at least once before starting the API.

### Changing indexed programs

If you change the `[programs]` filter in the indexer config, the `environment_info` table is updated on the next indexer startup. However, existing account data for previously indexed (now excluded) programs remains in the database. To clean it up, you can either:

- Run `cargo run -p cloudbreak-migration -- fresh` to drop and recreate all tables (destructive).
- Manually delete rows for the unwanted programs.

### High memory usage with account owner map

When `accounts-owner-map-enabled = true`, the indexer maintains an in-memory map of every tracked account's pubkey to its owner and slot. For large program sets, this can consume significant memory. If memory is a concern and you don't need owner-change tracking, set `accounts-owner-map-enabled = false`.

### Indexer logs `No incremental snapshot available` / `Snapshot is not available for gap filling yet`

The cluster tracker hasn't reported a snapshot pair that covers the slot the indexer asked for. Common causes:

- The targets in `tracker-config.yml` are unreachable, rate-limited (common with the default `https://api.mainnet.solana.com`), or haven't produced an incremental snapshot covering that slot yet.
- The slot you asked for is newer than any snapshot the tracker currently knows about. The indexer will retry on the next iteration; this is informational, not fatal.
- You replaced the default with custom sidecars but the tracker hasn't completed its first scrape yet (controlled by `scrape_interval` in `tracker-config.yml`).

Inspect what the tracker is exposing with:

```sh
curl http://localhost:8458/v1/snapshots | jq .
```

If that response is empty or missing `files` entries, the issue is upstream of cloudbreak — fix `tracker-config.yml` and reload.

### `solana-tracker` build fails

The `solana-tracker` Compose service uses a Git build context (`https://github.com/rpcpool/solana-cluster.git#triton`). The first `docker compose up` needs network access to clone the repo and pull the Go toolchain. After upstream commits to the `triton` branch, Compose will reuse the cached image — to pick up new commits run:

```sh
docker compose build --no-cache --pull solana-tracker
docker compose up -d solana-tracker
```

## Correctness Testing

> **How we validate correctness.** Cloudbreak intentionally does **not** ship synthetic fixtures or a `seed` subcommand for testing query results. The chosen approach is to validate against **real Solana state**, because the only ground truth we trust is what a Solana validator actually sees. There are two supported, end-to-end ways to do this locally:
>
> 1. **API-vs-Agave comparison (response correctness).** Start the indexer with `[snapshot]` configured, start the API, and run the [`integration_tests`](crates/integration_tests/README.md) benchmark in dual-endpoint mode against a reference Agave RPC node pointed at the same cluster. The harness compensates for slot skew between the two endpoints and reports per-request mismatches, so any divergence between Cloudbreak's responses and a stock Agave node is surfaced directly.
> 2. **Hash-checker (database-vs-snapshot correctness).** Start only the indexer (with `[snapshot]` configured) and enable the `[hash-checker]` section described below. The indexer streams gRPC until a target slot, then re-fetches a covering snapshot pair and lattice-hashes its own DB against the snapshot. A match is a cryptographic guarantee that every indexed account (address, data, lamports, owner, executable) is identical to what the validator wrote.
>
> Both paths require a real upstream — a Yellowstone gRPC endpoint and a working cluster-tracker source — and both require `[snapshot]` to be configured on the indexer. Running without `[snapshot]` is for smoke-testing the setup only and cannot be used to assert correctness.

The indexer can run an automated correctness test by adding a `[hash-checker]` section to `cloudbreak.index.toml`. When enabled, the indexer runs normally until a target slot is reached, then asks the cluster tracker for the next covering snapshot pair, downloads the full + incremental archives from the source reported by the tracker, replays buffered updates up to that snapshot slot, computes a lattice hash over both the database and the snapshot, and exits with `0` on match or `1` on mismatch.

A lattice hash is calculated with all accounts addresses, data, lamports, owner and executable. If the two hashes match, it's sure that the indexed data is correct.

### Configuration

Requires the `[snapshot]` section (with `[snapshot.tracker_endpoint]`) to be present. Set one of `time-limit` or `slot-limit`:

```toml
[hash-checker]
# Either:
time-limit = "6h"
# Or:
slot-limit = 405775018
```

- `time-limit`: target slot is computed as `first_grpc_slot + time_limit / 400ms`.
- `slot-limit`: target slot is used directly.

### Running

Just run the indexer normally:

```sh
cargo run -p cloudbreak -- --config ./cloudbreak.index.toml index
```

The process stays up until the target slot is finalized and the next covering snapshot pair is available from the tracker, then prints the comparison result and exits.

### Debugging a mismatch with `snapshot-diff`

When the hash-check fails, `snapshot-diff` shows which pubkeys differ between the indexer DB and the snapshot, instead of just a hash mismatch. It scans the snapshot into per-prefix temp files, then compares them against the DB filtered by `slot <= target_slot` and the configured program selector.

```
cargo run -p cloudbreak -- --config ./cloudbreak.index.toml snapshot-diff \
    --target-slot 416520592 \
    --full-snapshot /path/to/snapshot-...tar.zst \
    --incremental-snapshot /path/to/incremental-...tar.zst \
    --dump-mismatches ./diff.dump
```

Optional flags: `--prefix 0a1b` to scan only a single 16-bit prefix, `--keep-tmp` to retain `./snapshot_diff_tmp/` after exit.

Output ends with four counters:

```
match: <pubkeys identical in both>
mismatch: <pubkeys present in both but with different state>
only_in_snapshot: <pubkeys in snapshot but missing/older in DB>
only_in_db: <pubkeys in DB but missing in snapshot — typical leak symptom>
```

When `--dump-mismatches` is set, every diverging pubkey is appended to that file (uncapped); stdout shows only the first 50 of each category as a sample.

## License

Copyright 2025-2026 Triton One Limited. All rights reserved.

Licensed under the GNU Affero General Public License v3.0 only. See `LICENSE`.
