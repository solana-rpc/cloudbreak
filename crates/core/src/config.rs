// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// use crate::AccountSelect;
use anyhow::Result;
use sea_orm::{ConnectOptions, ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, de};
use solana_pubkey::Pubkey;
use std::borrow::Cow;
use std::fs;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use toml::from_str;

pub const DEFAULT_API_PROM_METRICS_COLLECTOR_PORT: u16 = 8875;
pub const DEFAULT_OTLP_COLLECTOR_PORT: u16 = 4318;
pub const DEFAULT_API_SERVER_PORT: u16 = 4000;

pub trait TryLoadConfig: Sized + DeserializeOwned {
    fn try_load(path: &str) -> Result<Self> {
        let config = fs::read_to_string(path)?;

        Ok(from_str(&config)?)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct GrpcConfig {
    /// The endpoint of the Yellowstone server.
    pub endpoint: String,
    /// The token to use for authentication.
    #[serde(rename = "x-token")]
    pub x_token: Option<String>,
    /// The timeout for the connection.
    pub timeout: u64,
    /// The count of workers handling subscription events simultaneously
    #[serde(rename = "worker-count")]
    pub jobs: Option<usize>,
    /// The buffer size for queuing subscription events
    #[serde(
        rename = "channel-size",
        default = "GrpcConfig::default_sources_channel_size"
    )]
    pub sources_channel_size: usize,
    /// The chunk size for the subscription events
    #[serde(rename = "chunk-size", default = "GrpcConfig::default_chunk_size")]
    pub chunk_size: usize,
    /// The max chunk bytes data for the subscription events
    #[serde(
        rename = "max-chunk-bytes-data",
        default = "GrpcConfig::default_max_chunk_bytes_data"
    )]
    pub max_chunk_bytes_data: usize,
    /// The max number of grpc errors before trying to reconnect
    ///  (it will always reconnect on a single stream `None`)
    #[serde(rename = "max-grpc-errors")]
    pub max_grpc_errors: usize,
}

impl GrpcConfig {
    pub fn rpc_url(&self) -> String {
        format!(
            "{}/{}:8899",
            self.endpoint,
            self.x_token.clone().unwrap_or_default()
        )
    }

    const fn default_sources_channel_size() -> usize {
        1_000
    }
    const fn default_chunk_size() -> usize {
        1000
    }
    const fn default_max_chunk_bytes_data() -> usize {
        2 * 1024 * 1024
    }
}

#[derive(Debug, Clone)]
pub struct PubkeyDef(pub Pubkey);

impl<'de> Deserialize<'de> for PubkeyDef {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer).map_err(de::Error::custom)?;
        let pubkey = Pubkey::from_str(&s).map_err(serde::de::Error::custom)?;

        Ok(PubkeyDef(pubkey))
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct AccountSelectorConfig {
    #[serde(default)]
    pub include: Vec<PubkeyDef>,
    #[serde(default)]
    pub exclude: Vec<PubkeyDef>,
}

impl AccountSelectorConfig {
    pub fn is_program_selected(&self, program: &Pubkey) -> bool {
        if self.include.is_empty() {
            !self.exclude.iter().any(|p| &p.0 == program)
        } else {
            self.include.iter().any(|p| &p.0 == program)
        }
    }

    /// `getVoteAccounts` requires both the Vote and Stake programs to be indexed.
    pub fn supports_vote_accounts(&self) -> bool {
        self.is_program_selected(&VOTE_PROGRAM_ID) && self.is_program_selected(&STAKE_PROGRAM_ID)
    }
}

pub const VOTE_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("Vote111111111111111111111111111111111111111");
pub const STAKE_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("Stake11111111111111111111111111111111111111");

pub struct EnvironmentInfo;

impl EnvironmentInfo {
    pub async fn upsert_filters(
        db: &DatabaseConnection,
        filters: &AccountSelectorConfig,
    ) -> Result<()> {
        let (mode, programs) = if filters.include.is_empty() {
            ("exclude", &filters.exclude)
        } else {
            ("include", &filters.include)
        };
        let programs_csv = programs
            .iter()
            .map(|p| p.0.to_string())
            .collect::<Vec<_>>()
            .join(",");

        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO environment_info (id, mode, programs) VALUES (1, $1, $2) \
             ON CONFLICT (id) DO UPDATE SET mode = EXCLUDED.mode, programs = EXCLUDED.programs",
            [mode.into(), programs_csv.into()],
        ))
        .await?;

        Ok(())
    }

    pub async fn load_filters(db: &DatabaseConnection) -> Result<AccountSelectorConfig> {
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT mode, programs FROM environment_info WHERE id = 1".to_string(),
            ))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("environment_info row not found; has the indexer run?")
            })?;

        let mode: String = row.try_get("", "mode")?;
        let programs: String = row.try_get("", "programs")?;
        let programs = programs
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| Pubkey::from_str(s).map(PubkeyDef))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(match mode.as_str() {
            "include" => AccountSelectorConfig {
                include: programs,
                exclude: Vec::new(),
            },
            "exclude" => AccountSelectorConfig {
                include: Vec::new(),
                exclude: programs,
            },
            other => anyhow::bail!("Invalid filter mode: {}", other),
        })
    }

    pub async fn upsert_grpc_version(db: &DatabaseConnection, version: &str) -> Result<()> {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO environment_info (id, solana_version) VALUES (1, $1) \
             ON CONFLICT (id) DO UPDATE SET solana_version = EXCLUDED.solana_version",
            [version.into()],
        ))
        .await?;

        Ok(())
    }

    pub async fn load_grpc_version(db: &DatabaseConnection) -> Result<Option<String>> {
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT solana_version FROM environment_info WHERE id = 1".to_string(),
            ))
            .await?;

        match row {
            Some(row) => Ok(row.try_get("", "solana_version")?),
            None => Ok(None),
        }
    }
}

#[derive(Deserialize, Default, Debug, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(rename = "max-connections")]
    pub max_connections: Option<u32>,
    #[serde(rename = "min-connections")]
    pub min_connections: Option<u32>,
    #[serde(
        rename = "connect-timeout",
        default,
        deserialize_with = "deserialize_duration"
    )]
    pub connect_timeout: Option<Duration>,
    #[serde(
        rename = "idle-timeout",
        default,
        deserialize_with = "deserialize_duration"
    )]
    pub idle_timeout: Option<Duration>,
    #[serde(
        rename = "acquire-timeout",
        default,
        deserialize_with = "deserialize_duration"
    )]
    pub acquire_timeout: Option<Duration>,
    #[serde(
        rename = "max-lifetime",
        default,
        deserialize_with = "deserialize_duration"
    )]
    pub max_lifetime: Option<Duration>,
    #[serde(rename = "sqlx-logging")]
    pub sqlx_logging: Option<bool>,
    #[serde(rename = "sqlcipher-key")]
    pub sqlcipher_key: Option<Cow<'static, str>>,
    #[serde(rename = "schema-search-path")]
    pub schema_search_path: Option<String>,
    #[serde(rename = "test-before-acquire")]
    pub test_before_acquire: Option<bool>,
    #[serde(rename = "connect-lazy")]
    pub connect_lazy: Option<bool>,
    /// The threshold for the partition clustering (above this size in bytes the partition
    /// won't be clustered, to avoid overloading the DB)
    #[serde(rename = "partition-clustering-threshold")]
    pub partition_clustering_threshold: Option<u64>,
    #[serde(
        rename = "save-block-queries-timeout",
        default = "DatabaseConfig::default_save_block_queries_timeout"
    )]
    pub save_block_queries_timeout: u64,
    #[serde(
        rename = "finalize-slot-queries-timeout",
        default = "DatabaseConfig::default_finalize_slot_queries_timeout"
    )]
    pub finalize_slot_queries_timeout: u64,
    #[serde(
        rename = "api-queries-timeout",
        default = "DatabaseConfig::default_api_queries_timeout"
    )]
    pub api_queries_timeout: u64,
    #[serde(
        rename = "server-side-timeout",
        default = "DatabaseConfig::default_server_side_timeout_ms"
    )]
    pub server_side_timeout: u64,
    /// The threshold for the number of DB errors before exiting the process
    #[serde(
        rename = "max-db-errors-threshold",
        default = "DatabaseConfig::default_max_db_errors_threshold"
    )]
    pub max_db_errors_threshold: Option<f64>,
}

impl DatabaseConfig {
    const fn default_save_block_queries_timeout() -> u64 {
        30
    }
    const fn default_finalize_slot_queries_timeout() -> u64 {
        300
    }
    const fn default_api_queries_timeout() -> u64 {
        10
    }

    const fn default_server_side_timeout_ms() -> u64 {
        300_000
    }

    const fn default_max_db_errors_threshold() -> Option<f64> {
        Some(100.0)
    }
}

impl From<DatabaseConfig> for ConnectOptions {
    fn from(config: DatabaseConfig) -> Self {
        let mut options = ConnectOptions::new(config.url);

        if let Some(max_conn) = config.max_connections {
            options.max_connections(max_conn);
        }
        if let Some(min_conn) = config.min_connections {
            options.min_connections(min_conn);
        }
        if let Some(timeout) = config.connect_timeout {
            options.connect_timeout(timeout);
        }
        if let Some(idle) = config.idle_timeout {
            options.idle_timeout(idle);
        }
        if let Some(acquire) = config.acquire_timeout {
            options.acquire_timeout(acquire);
        }
        if let Some(lifetime) = config.max_lifetime {
            options.max_lifetime(lifetime);
        }
        if let Some(sqlx_logging) = config.sqlx_logging {
            options.sqlx_logging(sqlx_logging);
        }
        if let Some(key) = config.sqlcipher_key {
            options.sqlcipher_key(key);
        }
        if let Some(path) = config.schema_search_path {
            options.set_schema_search_path(path);
        }
        if let Some(test_before_acquire) = config.test_before_acquire {
            options.test_before_acquire(test_before_acquire);
        }
        if let Some(connect_lazy) = config.connect_lazy {
            options.connect_lazy(connect_lazy);
        }

        options
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct TrackerConfig {
    pub endpoint: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct IndexConfig {
    /// If `Some`, the indexer will also download and process the snapshots
    pub snapshot: Option<SnapshotConfigOnIndexer>,
    pub database: DatabaseConfig,
    pub grpc: GrpcConfig,
    pub metrics: MetricsConfig,
    pub programs: AccountSelectorConfig,
    #[serde(
        rename = "finalize-slot-buffer-size",
        default = "IndexConfig::default_finalize_slot_buffer_size"
    )]
    pub finalize_slot_buffer_size: usize,
    #[serde(rename = "hash-checker")]
    pub hash_checker: Option<HashCheckerConfig>,
    #[serde(default)]
    #[serde(rename = "accounts-owner-map-enabled")]
    pub accounts_owner_map_enabled: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct HashCheckerConfig {
    #[serde(
        rename = "time-limit",
        default,
        deserialize_with = "deserialize_duration"
    )]
    pub time_limit: Option<Duration>,
    #[serde(rename = "slot-limit")]
    pub slot_limit: Option<u64>,
}

impl IndexConfig {
    pub fn get_prom_metrics_collector_endpoint(&self) -> SocketAddr {
        SocketAddr::from_str(&format!(
            "{}:{}",
            self.metrics.host.as_ref().map_or("0.0.0.0", |v| v),
            self.metrics
                .port
                .unwrap_or(DEFAULT_API_PROM_METRICS_COLLECTOR_PORT)
        ))
        .expect("error getting prom metrics collector endpoint")
    }

    fn default_finalize_slot_buffer_size() -> usize {
        1000
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct SnapshotConfigOnIndexer {
    pub tracker_endpoint: TrackerConfig,
    /// The maximum number of `AccountsFile` to process simultaneously
    #[serde(rename = "accounts-file-concurency")]
    pub accounts_file_concurency: Option<usize>,
    /// Enable/disable flags for the indexes created on `snapshot_accounts` after ingest.
    /// Mirrors `SnapshotConfig.pg_indexes` so a snapshot triggered by the indexer (self-healing,
    /// startup processing) uses the same per-index toggles as a stand-alone snapshot run.
    #[serde(rename = "pg-indexes", default)]
    pub pg_indexes: SnapshotPgIndexesConfig,
}

impl TryLoadConfig for IndexConfig {}

#[derive(Deserialize, Debug, Clone)]
pub struct SnapshotConfig {
    /// The maximum number of `AccountsFile` to process simultaneously
    #[serde(rename = "accounts-file-concurency")]
    pub accounts_file_concurency: Option<usize>,
    pub database: DatabaseConfig,
    pub tracker_endpoint: TrackerConfig,
    pub metrics: MetricsConfig,
    pub programs: AccountSelectorConfig,
    /// Enable/disable flags for indexes created on `snapshot_accounts` after ingest.
    #[serde(rename = "pg-indexes", default)]
    pub pg_indexes: SnapshotPgIndexesConfig,
}

impl TryLoadConfig for SnapshotConfig {}

#[derive(Deserialize, Debug)]
pub struct ServerConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    #[serde(
        rename = "max-connections",
        default = "ServerConfig::default_max_connections"
    )]
    pub max_connections: u32,
    #[serde(
        rename = "batch-handling-max-concurrency",
        default = "ServerConfig::default_batch_handling_max_concurrency"
    )]
    pub batch_handling_max_concurrency: usize,
    #[serde(
        rename = "gpa-stream-batch-size",
        default = "ServerConfig::default_gpa_stream_batch_size"
    )]
    pub gpa_stream_batch_size: usize,
    #[serde(
        rename = "request-timeout",
        default = "ServerConfig::default_request_timeout",
        deserialize_with = "deserialize_duration_required"
    )]
    pub request_timeout: Duration,
    #[serde(
        rename = "max-multiple-accounts",
        default = "ServerConfig::default_max_multiple_accounts"
    )]
    pub max_multiple_accounts: usize,
}

impl ServerConfig {
    pub fn default_max_connections() -> u32 {
        100
    }

    pub fn default_gpa_stream_batch_size() -> usize {
        1000
    }

    pub fn default_request_timeout() -> Duration {
        Duration::from_secs(60)
    }

    pub fn default_batch_handling_max_concurrency() -> usize {
        5
    }

    pub const fn default_max_multiple_accounts() -> usize {
        100
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct MetricsConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    #[serde(
        rename = "subscription-id-key",
        default = "MetricsConfig::default_subscription_id_key"
    )]
    pub subscription_id_key: String,
}

impl MetricsConfig {
    fn default_subscription_id_key() -> String {
        "x-subscription-id".to_string()
    }
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ProcessedCommitmentBehavior {
    #[default]
    Reject,
    UseConfirmed,
}

#[derive(Deserialize, Debug)]
pub struct ApiConfig {
    pub database: DatabaseConfig,
    pub server: ServerConfig,
    pub metrics: MetricsConfig,
    /// Optional. When omitted, the query tracker integration is disabled: the
    /// API runs without reporting query patterns and automatic index creation
    /// is unavailable.
    #[serde(rename = "query-tracker-client", default)]
    pub query_tracker_client: Option<QueryTrackerClientConfig>,
    #[serde(
        rename = "slot-syncronizer",
        default = "SlotSyncronizerConfig::default_interval"
    )]
    pub slot_syncronizer: SlotSyncronizerConfig,
    #[serde(rename = "processed-commitment", default)]
    pub processed_commitment: ProcessedCommitmentBehavior,
    #[serde(rename = "gpa-cache")]
    pub gpa_cache: Option<GpaCacheConfig>,
    #[serde(rename = "genesis-hash", default = "ApiConfig::default_genesis_hash")]
    pub genesis_hash: String,
}

/// Config for the `cache` optional module for the API.
#[derive(Deserialize, Debug, Clone)]
#[serde(try_from = "GpaCacheConfigRaw")]
pub struct GpaCacheConfig {
    /// Max total size of the cache in bytes.
    pub max_total_bytes: usize,
    /// Used to avoid small queries for which the cache is not worth it.
    /// And for avoid cleaning up more relevant queries.
    pub min_bytes_per_query: usize,
    /// Optional upper bound (in bytes) on the size of a query that is eligible
    /// for eviction. Queries larger than this are kept in the cache and never
    /// evicted by cleanup (they remain until replaced by a newer version of the
    /// same query). `None` (key omitted) means every cached query is evictable.
    pub max_bytes_query_cleanup: Option<usize>,
    /// Max fraction of `max_total_bytes` that pinned (non-evictable) queries may
    /// collectively occupy. When pinned usage exceeds this cap, the oldest
    /// pinned queries are evicted via the normal cleanup process until usage is
    /// back under the cap. Required when `max_bytes_query_cleanup` is set, and
    /// ignored otherwise (no query is pinned without `max_bytes_query_cleanup`).
    pub max_pinned_bytes_ratio: Option<f64>,
}

/// Raw, file-facing shape of [`GpaCacheConfig`]. Deserialized first so we can
/// run cross-field validation in `TryFrom` before exposing the typed config.
#[derive(Deserialize, Debug, Clone)]
struct GpaCacheConfigRaw {
    #[serde(rename = "max-total-bytes")]
    max_total_bytes: usize,
    #[serde(rename = "min-bytes-per-query")]
    min_bytes_per_query: usize,
    #[serde(rename = "max-bytes-query-cleanup", default)]
    max_bytes_query_cleanup: Option<usize>,
    #[serde(rename = "max-pinned-bytes-ratio", default)]
    max_pinned_bytes_ratio: Option<f64>,
}

impl TryFrom<GpaCacheConfigRaw> for GpaCacheConfig {
    type Error = String;

    fn try_from(raw: GpaCacheConfigRaw) -> std::result::Result<Self, Self::Error> {
        if raw.max_bytes_query_cleanup.is_some() && raw.max_pinned_bytes_ratio.is_none() {
            return Err(
                "`max-pinned-bytes-ratio` is required when `max-bytes-query-cleanup` is set"
                    .to_string(),
            );
        }

        if let Some(ratio) = raw.max_pinned_bytes_ratio
            && !(ratio > 0.0 && ratio <= 1.0)
        {
            return Err(format!(
                "`max-pinned-bytes-ratio` must be greater than 0.0 and at most 1.0, got {ratio}"
            ));
        }

        Ok(Self {
            max_total_bytes: raw.max_total_bytes,
            min_bytes_per_query: raw.min_bytes_per_query,
            max_bytes_query_cleanup: raw.max_bytes_query_cleanup,
            max_pinned_bytes_ratio: raw.max_pinned_bytes_ratio,
        })
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct SlotSyncronizerConfig {
    pub enabled: bool,
    pub interval_ms: u64,
}

impl SlotSyncronizerConfig {
    pub fn default_interval() -> Self {
        Self {
            enabled: true,
            interval_ms: 200,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct QueryTrackerConfig {
    #[serde(
        rename = "create-database-indexes",
        default = "QueryTrackerConfig::default_create_database_indexes"
    )]
    pub create_database_indexes: bool,
    #[serde(
        rename = "index-generation-threshold",
        default = "QueryTrackerConfig::default_index_generation_threshold"
    )]
    pub index_generation_threshold: u32,
    #[serde(
        rename = "index-creation-delay",
        default = "QueryTrackerConfig::default_index_creation_delay",
        deserialize_with = "deserialize_duration_required"
    )]
    pub index_creation_delay: Duration,
    #[serde(
        rename = "query-counts-reset-interval",
        default = "QueryTrackerConfig::default_query_counts_reset_interval",
        deserialize_with = "deserialize_duration_required"
    )]
    pub query_counts_reset_interval: Duration,
    /// Programs to include in the index creation, if empty, all programs will be included
    /// In general this is meant to be used either this or excluded-programs (not both), for fine-grained control
    /// This is different from what data is being saved in the database, which is controlled by the `index` cmd,
    /// but it's closely related (because it only makes sense to index programs that are being saved in the database)
    #[serde(rename = "included-programs", default)]
    pub included_programs: Vec<PubkeyDef>,
    /// Programs to exclude from the index creation
    #[serde(rename = "excluded-programs", default)]
    pub excluded_programs: Vec<PubkeyDef>,
    /// The URL of the indexer metrics
    #[serde(
        rename = "indexer-metrics",
        deserialize_with = "QueryTrackerConfig::deserialize_indexer_metrics"
    )]
    pub indexer_metrics: String,
    /// The threshold for the indexer metrics to trigger index creation,
    /// the used metric is `cloudbreak_finalize_slot_handler_queue_size`
    #[serde(
        rename = "indexer-metrics-threshold",
        default = "QueryTrackerConfig::default_indexer_metrics_threshold"
    )]
    pub indexer_metrics_threshold: u64,
    /// Optional cap on the total number of indexes on the `snapshot_accounts` table
    #[serde(rename = "max-auto-indexes", default)]
    pub max_auto_indexes: Option<usize>,
}

impl QueryTrackerConfig {
    const fn default_create_database_indexes() -> bool {
        false
    }

    const fn default_index_generation_threshold() -> u32 {
        10
    }

    fn default_index_creation_delay() -> Duration {
        Duration::from_secs(10) // 10 seconds
    }

    fn default_query_counts_reset_interval() -> Duration {
        Duration::from_secs(86400) // 24 hours
    }

    fn default_indexer_metrics_threshold() -> u64 {
        5
    }

    pub fn deserialize_indexer_metrics<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let indexer_metrics_host =
            String::deserialize(deserializer).expect("error deserializing indexer-metrics");
        if indexer_metrics_host.is_empty() {
            panic!("indexer-metrics cannot be an empty string");
        }

        Ok(format!("http://{indexer_metrics_host}/metrics"))
    }
}

impl Default for QueryTrackerConfig {
    fn default() -> Self {
        Self {
            create_database_indexes: Self::default_create_database_indexes(),
            index_generation_threshold: Self::default_index_generation_threshold(),
            index_creation_delay: Self::default_index_creation_delay(),
            query_counts_reset_interval: Self::default_query_counts_reset_interval(),
            included_programs: Vec::new(),
            excluded_programs: Vec::new(),
            indexer_metrics: String::default(),
            indexer_metrics_threshold: Self::default_indexer_metrics_threshold(),
            max_auto_indexes: None,
        }
    }
}

impl TryLoadConfig for ApiConfig {}

impl ApiConfig {
    /// Mainnet-beta genesis hash. Used as the default if `genesis-hash` is not set in config.
    fn default_genesis_hash() -> String {
        "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d".to_string()
    }

    pub fn server_addr(&self) -> SocketAddr {
        SocketAddr::from_str(&format!(
            "{}:{}",
            self.server.host.as_ref().map_or("0.0.0.0", |v| v),
            self.server.port.unwrap_or(DEFAULT_API_SERVER_PORT)
        ))
        .expect("error getting endpoint")
    }
}

#[derive(Deserialize, Debug)]
pub struct QueryTrackerServiceConfig {
    pub database: DatabaseConfig,
    pub server: ServerConfig,
    pub metrics: MetricsConfig,
    #[serde(rename = "query-tracker")]
    pub query_tracker: QueryTrackerConfig,
}

impl TryLoadConfig for QueryTrackerServiceConfig {}

impl QueryTrackerServiceConfig {
    pub fn server_addr(&self) -> SocketAddr {
        SocketAddr::from_str(&format!(
            "{}:{}",
            self.server.host.as_ref().map_or("0.0.0.0", |v| v),
            self.server
                .port
                .unwrap_or(DEFAULT_QUERY_TRACKER_SERVER_PORT)
        ))
        .expect("error getting endpoint")
    }

    pub fn metrics_addr(&self) -> SocketAddr {
        SocketAddr::from_str(&format!(
            "{}:{}",
            self.metrics.host.as_ref().map_or("0.0.0.0", |v| v),
            self.metrics
                .port
                .unwrap_or(DEFAULT_QUERY_TRACKER_METRICS_PORT)
        ))
        .expect("error getting metrics endpoint")
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct QueryTrackerClientConfig {
    pub endpoint: String,
    #[serde(default, deserialize_with = "deserialize_duration")]
    pub timeout: Option<Duration>,
    #[serde(
        rename = "flush-interval",
        default,
        deserialize_with = "deserialize_duration"
    )]
    pub flush_interval: Option<Duration>,
}

pub const DEFAULT_QUERY_TRACKER_SERVER_PORT: u16 = 4001;
pub const DEFAULT_QUERY_TRACKER_METRICS_PORT: u16 = 8876;

/// Configuration for owner-based partitioning of the `accounts` and `snapshot_accounts` tables.
///
/// Read by the migration crate at table creation time. The combination of `hash_partitions`
/// and `list_partitions` determines the partitioning strategy:
/// - both off: no partitioning, PK is `(pubkey, slot)`.
/// - hash only: `PARTITION BY HASH (owner)` with `hash_partition_count` buckets.
/// - list only: `PARTITION BY LIST (owner)` with one partition per program and a plain
///   (non-partitioned) `_default` table for everything else.
/// - both on: `PARTITION BY LIST (owner)` with `_default` further `PARTITION BY HASH (owner)`.
#[derive(Deserialize, Debug, Clone)]
pub struct PgOwnerPartitionsConfig {
    #[serde(
        rename = "hash-partitions",
        default = "PgOwnerPartitionsConfig::default_hash_partitions"
    )]
    pub hash_partitions: bool,
    #[serde(
        rename = "hash-partition-count",
        default = "PgOwnerPartitionsConfig::default_hash_partition_count"
    )]
    pub hash_partition_count: u32,
    #[serde(rename = "list-partitions", default)]
    pub list_partitions: bool,
    #[serde(rename = "programs-for-list-partition", default)]
    pub programs_for_list_partition: Vec<PubkeyDef>,
}

impl PgOwnerPartitionsConfig {
    const fn default_hash_partitions() -> bool {
        true
    }
    const fn default_hash_partition_count() -> u32 {
        10
    }

    /// True when the table is partitioned on `owner` (and therefore `owner` must be in the PK).
    pub fn is_owner_partitioned(&self) -> bool {
        self.hash_partitions || self.list_partitions
    }
}

impl Default for PgOwnerPartitionsConfig {
    fn default() -> Self {
        Self {
            hash_partitions: Self::default_hash_partitions(),
            hash_partition_count: Self::default_hash_partition_count(),
            list_partitions: false,
            programs_for_list_partition: Vec::new(),
        }
    }
}

/// Per-index enable/disable flags for the `accounts` table (created in migrations).
///
/// All flags default to true except `idx_accounts_pubkey`, which is a `USING HASH` index and
/// is opt-in.
#[derive(Deserialize, Debug, Clone)]
pub struct MigrationPgIndexesConfig {
    #[serde(default)]
    pub idx_accounts_pubkey: bool,
    #[serde(default = "default_true")]
    pub idx_accounts_pubkey_slot: bool,
    #[serde(default = "default_true")]
    pub idx_accounts_token_mint: bool,
    #[serde(default = "default_true")]
    pub idx_accounts_token_owner: bool,
    #[serde(default = "default_true")]
    pub idx_accounts_token_delegate: bool,
}

impl Default for MigrationPgIndexesConfig {
    fn default() -> Self {
        Self {
            idx_accounts_pubkey: false,
            idx_accounts_pubkey_slot: true,
            idx_accounts_token_mint: true,
            idx_accounts_token_owner: true,
            idx_accounts_token_delegate: true,
        }
    }
}

/// Per-index enable/disable flags for the `snapshot_accounts` table (created at runtime by the
/// snapshot crate, after ingest).
///
/// All flags default to true except `idx_snapshot_accounts_pubkey`, which is a `USING HASH`
/// index and is opt-in.
#[derive(Deserialize, Debug, Clone)]
pub struct SnapshotPgIndexesConfig {
    #[serde(default)]
    pub idx_snapshot_accounts_pubkey: bool,
    #[serde(default = "default_true")]
    pub idx_snapshot_accounts_pubkey_slot: bool,
    #[serde(default = "default_true")]
    pub idx_snapshot_accounts_token_mint: bool,
    #[serde(default = "default_true")]
    pub idx_snapshot_accounts_token_owner: bool,
    #[serde(default = "default_true")]
    pub idx_snapshot_accounts_token_delegate: bool,
}

impl Default for SnapshotPgIndexesConfig {
    fn default() -> Self {
        Self {
            idx_snapshot_accounts_pubkey: false,
            idx_snapshot_accounts_pubkey_slot: true,
            idx_snapshot_accounts_token_mint: true,
            idx_snapshot_accounts_token_owner: true,
            idx_snapshot_accounts_token_delegate: true,
        }
    }
}

const fn default_true() -> bool {
    true
}

/// Top-level migration config. Loaded from the TOML file pointed at by the
/// `CLOUDBREAK_MIGRATION_CONFIG` environment variable.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct MigrationConfig {
    #[serde(rename = "pg-owner-partitions", default)]
    pub pg_owner_partitions: PgOwnerPartitionsConfig,
    #[serde(rename = "pg-indexes", default)]
    pub pg_indexes: MigrationPgIndexesConfig,
}

impl TryLoadConfig for MigrationConfig {}

pub fn deserialize_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    if let Some(s) = s {
        humantime::parse_duration(&s)
            .map(Some)
            .map_err(serde::de::Error::custom)
    } else {
        Ok(None)
    }
}

pub fn deserialize_duration_required<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(|e| {
        serde::de::Error::custom(format!(
            "Invalid duration format: {}. Expected format like '24h', '1d', '30m', etc.",
            e
        ))
    })
}
