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
    /// How long to keep retrying (re)connection/subscription before giving up and panicking.
    #[serde(
        rename = "reconnect-give-up",
        default = "GrpcConfig::default_reconnect_give_up",
        deserialize_with = "deserialize_duration_required"
    )]
    pub reconnect_give_up: Duration,
    /// Delay between (re)connection/subscription attempts.
    #[serde(
        rename = "reconnect-backoff",
        default = "GrpcConfig::default_reconnect_backoff",
        deserialize_with = "deserialize_duration_required"
    )]
    pub reconnect_backoff: Duration,
    /// How long a reconnection keeps replaying from the last received slot (`from_slot`). Once we
    /// have been failing for longer than this, subscribe without `from_slot` (the server may not
    /// have it buffered).
    #[serde(
        rename = "reconnect-from-slot-retain",
        default = "GrpcConfig::default_reconnect_from_slot_retain",
        deserialize_with = "deserialize_duration_required"
    )]
    pub reconnect_from_slot_retain: Duration,
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
    const fn default_reconnect_give_up() -> Duration {
        Duration::from_secs(600)
    }
    const fn default_reconnect_backoff() -> Duration {
        Duration::from_secs(5)
    }
    const fn default_reconnect_from_slot_retain() -> Duration {
        Duration::from_secs(300)
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

    /// `simulateTransaction` requires a full unfiltered index
    pub fn supports_simulation(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty()
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
        Some(1.0)
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
    /// Maximum number of consecutive gap-filling iterations that may fail to fetch a covering
    /// snapshot pair from the tracker before the self-healing task gives up and fails the indexer.
    /// The counter resets as soon as a snapshot pair is fetched successfully.
    #[serde(
        rename = "gap-fill-max-snapshot-retries",
        default = "SnapshotConfigOnIndexer::default_gap_fill_max_snapshot_retries"
    )]
    pub gap_fill_max_snapshot_retries: u32,
}

impl SnapshotConfigOnIndexer {
    pub fn default_gap_fill_max_snapshot_retries() -> u32 {
        10
    }
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

    /// Enable the per-client-IP egress bandwidth metrics module (peak gauge +
    /// throughput histogram). Off by default.
    #[serde(rename = "client-ip-bandwidth-enabled", default)]
    pub client_ip_bandwidth_enabled: bool,

    /// Request header carrying the client IP used to group the per-client-IP
    /// bandwidth metrics. When unset — or when the header is absent on a
    /// request — all bandwidth folds into a single placeholder label.
    #[serde(rename = "client-ip-key", default)]
    pub client_ip_key: Option<String>,
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

/// How the API responds when the node is unhealthy (the `slots.health` flag is
/// unset / the slot syncronizer reports unhealthy).
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum UnhealthyResponseBehavior {
    /// Return a JSON-RPC error (`NODE_UNHEALTHY`) with HTTP `200 OK` (default).
    #[default]
    JsonRpcError,
    /// Return an HTTP `503 Service Unavailable` response instead.
    HttpUnavailable,
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
    /// How the API responds to requests while the node is unhealthy.
    #[serde(rename = "unhealthy-response", default)]
    pub unhealthy_response: UnhealthyResponseBehavior,
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

/// How creation candidates are ranked when the tracker decides which index to
/// build next. The score is computed on read from the stored demand columns, so
/// changing this takes effect immediately without any data migration. See the
/// query tracker's `prioritization` module for the exact `ORDER BY` mapping.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum PriorityMode {
    /// Rank by how many requests demanded the pattern (most frequent first).
    #[default]
    Frequency,
    /// Rank by accumulated DB cost in microseconds (heaviest total load first).
    Cost,
    /// Rank by average cost per request, so individually expensive but rarer
    /// patterns still rise to the top ("huge queries first").
    CostPerHit,
    /// Rank by average cost-per-hit scaled by a weighted blend of **windowed**
    /// activity. Unlike the other modes — which rank on lifetime totals — this
    /// one uses the per-window counts maintained by the score roll task, so it
    /// reflects *current* throughput and both creation and eviction rank by the
    /// same number.
    ///
    /// Score: `(avg_cost * gain) * (1 + demand_weight*demand +
    /// supply_weight*(supply/2) + failure_weight*failed)`, where `avg_cost =
    /// total_cost_us / demand_count`, each count is the value observed in the
    /// last window, and `gain = 1 + latency_weight * ln(latency_ratio)` scales
    /// `avg_cost` by how much faster the pattern is *with* the index than without
    /// (see `latency_weight`), where `latency_ratio = (without_index_compensation_factor
    /// × without) / with`. The `ln` is what keeps that scaling well-behaved:
    /// it grows slowly, so a wildly faster index (`latency_ratio` in the tens or
    /// hundreds) is compressed instead of dominating the ranking, while staying
    /// symmetric — `ln(1) = 0` is neutral and a harmful index (`ratio < 1`) gives
    /// a negative log that drags the score down. The `+ 1` baseline keeps the
    /// volume factor non-zero, so a zero-activity pattern just ranks by
    /// `avg_cost * gain` (no special-casing, and no ties at zero when evicting).
    ///
    /// The weights (and the measurement window) only make sense here, so they
    /// live inside the variant. In TOML this is a table while the other modes
    /// stay bare strings:
    /// `priority-mode = { weighted = { demand-weight = 1.0, rate-window = "1h" } }`.
    /// All weights default `0.0`, which collapses the score to plain average
    /// cost-per-hit (and needs no rate roll).
    Weighted {
        /// Weight on demand (request count in the window). Default `0.0`.
        #[serde(rename = "demand-weight", default)]
        demand_weight: f64,
        /// Weight on supply (`idx_scan` in the window, halved via
        /// `SCANS_PER_REQUEST` so it is comparable to demand). Only contributes
        /// for created indexes — candidates have no supply yet. Default `0.0`.
        #[serde(rename = "supply-weight", default)]
        supply_weight: f64,
        /// Weight on failed/timed-out requests in the window, to prioritize
        /// patterns that currently *cannot* be served without an index.
        /// Default `0.0`.
        #[serde(rename = "failure-weight", default)]
        failure_weight: f64,
        /// Weight on the measured latency **gain** from the index — the ratio of
        /// the (compensated) without-index average cost to the with-index
        /// average. Applied to `avg_cost` as `gain = 1 + latency_weight *
        /// ln(latency_ratio)`, so a clearly helpful index (ratio > 1) boosts the
        /// score while a harmful one (ratio < 1) drags it down; `ln` smooths
        /// extreme ratios. The without-index side is first scaled by
        /// `without-index-compensation-factor` (a top-level key, since the
        /// regression guard applies it too). Stays neutral (`gain = 1`) until the
        /// pattern has served requests both with and without the index (so a
        /// ratio can be formed), or when the weight is `0`. Default `0.0`.
        #[serde(rename = "latency-weight", default)]
        latency_weight: f64,
        /// Window over which the demand/supply/failure counts are measured. A
        /// background task snapshots the counters at this cadence and stores the
        /// per-window deltas the score reads. Default `1h`.
        #[serde(
            rename = "rate-window",
            default = "PriorityMode::default_rate_window",
            deserialize_with = "deserialize_duration_required"
        )]
        rate_window: Duration,
    },
}

impl PriorityMode {
    fn default_rate_window() -> Duration {
        Duration::from_secs(3600)
    }

    /// The measurement window when this mode needs one (only `Weighted`).
    pub fn rate_window(&self) -> Option<Duration> {
        match self {
            PriorityMode::Weighted { rate_window, .. } => Some(*rate_window),
            _ => None,
        }
    }

    /// Whether this mode needs the windowed score columns maintained. Only
    /// `Weighted` with at least one non-zero weight does; an all-zero `Weighted`
    /// collapses to plain average cost-per-hit and needs no rate roll.
    pub fn uses_windowed_rate(&self) -> bool {
        matches!(
            self,
            PriorityMode::Weighted { demand_weight, supply_weight, failure_weight, .. }
                if *demand_weight != 0.0 || *supply_weight != 0.0 || *failure_weight != 0.0
        )
    }
}

/// What the tracker does when a created index measures **slower** than the same
/// pattern was *without* it (a latency regression). An index becomes eligible
/// for the guard once it is older than `index-min-age-grace` — the same
/// lifetime gate idle eviction uses — by which point it has had time to gather
/// with-index latency to compare against its frozen without-index baseline.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum IndexRegressionGuard {
    /// Ignore latency regressions entirely (no logging, no action).
    #[default]
    Off,
    /// Log a warning and increment a metric, but keep the index in place.
    Warn,
    /// Drop the index pair and mark the pattern `rejected`, so it is not rebuilt
    /// until fresh without-index samples show the pattern is now *slower*
    /// without the index than it was with it.
    Evict,
}

/// Configuration for the query tracker service.
///
/// The tracker is persistence-first: all demand/supply state lives in the
/// `index_patterns` table, so every knob here only tunes *behavior* (what to
/// build, when to evict), never durability. Defaults reproduce the previous
/// hardcoded behavior so existing deployments need no config changes.
#[derive(Deserialize, Debug, Clone)]
pub struct QueryTrackerConfig {
    // ---- creation ---------------------------------------------------------
    /// Master switch for building database indexes. When false the creation
    /// loop is not spawned; demand is still recorded so a later flip has
    /// history to act on.
    #[serde(
        rename = "create-database-indexes",
        default = "QueryTrackerConfig::default_create_database_indexes"
    )]
    pub create_database_indexes: bool,
    /// Minimum demand count a pattern must reach before it becomes a creation
    /// candidate. Filters out one-off queries.
    #[serde(
        rename = "index-generation-threshold",
        default = "QueryTrackerConfig::default_index_generation_threshold"
    )]
    pub index_generation_threshold: u32,
    /// Poll interval of the creation loop: how often it wakes to pick the next
    /// highest-priority candidate.
    #[serde(
        rename = "index-creation-delay",
        default = "QueryTrackerConfig::default_index_creation_delay",
        deserialize_with = "deserialize_duration_required"
    )]
    pub index_creation_delay: Duration,

    // ---- prioritization ---------------------------------------------------
    /// How candidates are ranked. See [`PriorityMode`].
    #[serde(rename = "priority-mode", default)]
    pub priority_mode: PriorityMode,
    /// Only patterns whose average cost-per-hit reaches this many microseconds
    /// are eligible for creation. `None` disables the gate.
    #[serde(
        rename = "cost-eligibility-threshold-us",
        default = "QueryTrackerConfig::default_cost_eligibility_threshold_us"
    )]
    pub cost_eligibility_threshold_us: Option<u64>,

    // ---- program scoping --------------------------------------------------
    /// Programs to include in index creation; empty means "all". Use either
    /// this or `excluded-programs`, not both.
    #[serde(rename = "included-programs", default)]
    pub included_programs: Vec<PubkeyDef>,
    /// Programs to exclude from index creation.
    #[serde(rename = "excluded-programs", default)]
    pub excluded_programs: Vec<PubkeyDef>,

    // ---- backpressure -----------------------------------------------------
    /// URL of the indexer metrics endpoint used to gate DDL under ingest load.
    #[serde(
        rename = "indexer-metrics",
        deserialize_with = "QueryTrackerConfig::deserialize_indexer_metrics"
    )]
    pub indexer_metrics: String,
    /// If `cloudbreak_finalize_slot_handler_queue_size` exceeds this, CREATE and
    /// DROP INDEX are deferred (they take heavy locks on hot tables).
    #[serde(
        rename = "indexer-metrics-threshold",
        default = "QueryTrackerConfig::default_indexer_metrics_threshold"
    )]
    pub indexer_metrics_threshold: u64,
    /// Optional cap on the total number of indexes on the target table.
    #[serde(rename = "max-auto-indexes", default)]
    pub max_auto_indexes: Option<usize>,

    // ---- eviction ---------------------------------------------------------
    /// Master switch for usage-based eviction. Off by default; when off the
    /// eviction task is not spawned.
    #[serde(
        rename = "index-eviction-enabled",
        default = "QueryTrackerConfig::default_index_eviction_enabled"
    )]
    pub index_eviction_enabled: bool,
    /// When `true`, the eviction pass marks the whole node **unhealthy** (via the
    /// shared `service_health` / `slots.health` flag) for the duration of the idle
    /// trim loop, then healthy again once it finishes — so a load balancer reading
    /// that flag drains traffic away from the node while `DROP INDEX` holds heavy
    /// `ACCESS EXCLUSIVE` locks.
    ///
    /// Off by default. It writes the *same shared* health row the indexer uses, so
    /// only enable it where the query tracker is the authority that may flip node
    /// health: if another process on the node also toggles it, the two can clobber
    /// each other (e.g. this restores healthy while the indexer wanted unhealthy).
    #[serde(
        rename = "mark-unhealthy-for-eviction",
        default = "QueryTrackerConfig::default_mark_unhealthy_for_eviction"
    )]
    pub mark_unhealthy_for_eviction: bool,
    /// When `true`, an index must also be idle by **supply** (`last_seen_used` /
    /// `idx_scan` unchanged for `index-min-idle`) to be eviction-eligible, in
    /// addition to demand-idle. Off by default: eligibility is demand-idle +
    /// age-grace only.
    ///
    /// The supply signal is useful when you want a more conservative drop set
    /// (indexes Postgres is still scanning stay protected even with no tracked
    /// API demand). The cost is that the eviction-pass supply refresh bumps
    /// `last_seen_used` for every index whose `idx_scan` moved since the last
    /// pass, which can collapse the eviction-candidates list for up to
    /// `index-min-idle` afterward — painful on high-rotation pattern DBs, where
    /// the creation-time value guard may then see an empty eligible set and
    /// treat `None` as "build anyway." Turn this on only if you want that extra
    /// conservatism; the collapse can also be mitigated by running eviction
    /// passes more frequently (so each refresh covers a shorter window).
    ///
    /// Applies to the shared eligibility gate used by the idle trim, the
    /// creation-time value guard, and `/debug/created?filter=eviction_candidates`.
    #[serde(
        rename = "use-supply-for-eviction",
        default = "QueryTrackerConfig::default_use_supply_for_eviction"
    )]
    pub use_supply_for_eviction: bool,
    /// How often the eviction pass runs. Keep comfortably larger than the
    /// creation rate to avoid drop/rebuild churn. Default: 1h.
    #[serde(
        rename = "index-eviction-interval",
        default = "QueryTrackerConfig::default_index_eviction_interval",
        deserialize_with = "deserialize_duration_required"
    )]
    pub index_eviction_interval: Duration,
    /// Minimum age before an index is eligible for eviction, so a freshly built
    /// index is never dropped before it has had a chance to be used. Default: 1h.
    #[serde(
        rename = "index-min-age-grace",
        default = "QueryTrackerConfig::default_index_min_age_grace",
        deserialize_with = "deserialize_duration_required"
    )]
    pub index_min_age_grace: Duration,
    /// How long a pattern must be idle before it is droppable. Demand-idle
    /// (`last_demand_at`) is always required; supply-idle (`last_seen_used`) is
    /// required only when [`Self::use_supply_for_eviction`] is on. Default: 24h.
    #[serde(
        rename = "index-min-idle",
        default = "QueryTrackerConfig::default_index_min_idle",
        deserialize_with = "deserialize_duration_required"
    )]
    pub index_min_idle: Duration,
    /// Fraction (0.0–1.0) of `max-auto-indexes` fill above which eviction is
    /// allowed to run. Below it we prefer keeping possibly-useful indexes and
    /// simply let creation slow down. Requires `max-auto-indexes`. Default: 0.9.
    #[serde(
        rename = "eviction-fill-threshold",
        default = "QueryTrackerConfig::default_eviction_fill_threshold"
    )]
    pub eviction_fill_threshold: f64,
    /// Multiplier applied to a **creation candidate's** score in the creation-time
    /// value guard, when it is compared against the index it would displace. It
    /// tunes *stickiness* toward existing indexes.
    ///
    /// Both sides are scored by the same `priority-mode`, but a `created` index
    /// carries realized signal a fresh candidate cannot have yet — its
    /// with-index latency `gain` and its `idx_scan` supply — which structurally
    /// biases the comparison toward incumbents. This factor compensates for that:
    /// `> 1.0` favors building new indexes (less sticky, more churn), `< 1.0`
    /// favors keeping incumbents (stickier), and `1.0` (default) compares the two
    /// scores as-is. Only consulted while the table is in the buffer band
    /// (at/above the fill target); below the target, creation is unguarded.
    #[serde(
        rename = "value-guard-creation-bias",
        default = "QueryTrackerConfig::default_value_guard_creation_bias"
    )]
    pub value_guard_creation_bias: f64,
    /// `lock_timeout` applied to each DROP INDEX. If the lock cannot be taken in
    /// this window the drop is skipped (with a warning) and retried next pass.
    #[serde(
        rename = "drop-lock-timeout",
        default = "QueryTrackerConfig::default_drop_lock_timeout",
        deserialize_with = "deserialize_duration_required"
    )]
    pub drop_lock_timeout: Duration,
    /// Number of extra attempts for a DROP INDEX that fails on lock timeout
    /// within the same pass. Default: 1.
    #[serde(
        rename = "drop-retries",
        default = "QueryTrackerConfig::default_drop_retries"
    )]
    pub drop_retries: u32,

    // ---- latency regression guard ----------------------------------------
    /// What to do when a created index is measured *slower* than the same
    /// pattern was without it. `off` (default) ignores it; `warn` logs and bumps
    /// a metric; `evict` drops the pair and marks the pattern `rejected` so it is
    /// not rebuilt until fresh evidence shows the index would help again. See
    /// [`IndexRegressionGuard`]. The guard runs inside the eviction pass (so it
    /// requires `index-eviction-enabled`) but is independent of the fill
    /// threshold — a harmful index is dropped even when there is room.
    #[serde(rename = "index-regression-guard", default)]
    pub index_regression_guard: IndexRegressionGuard,
    /// How many times slower *with* the index than without before it counts as a
    /// regression (e.g. `1.2` = the with-index average must exceed the
    /// without-index average by 20%). Also the hysteresis a `rejected` pattern
    /// must clear — its recent without-index average must exceed the with-index
    /// average recorded at rejection by this factor — before it is retried.
    /// Default: 1.2.
    #[serde(
        rename = "index-regression-ratio",
        default = "QueryTrackerConfig::default_index_regression_ratio"
    )]
    pub index_regression_ratio: f64,
    /// How long a pattern that was `rejected` for a latency regression must stay
    /// rejected — accumulating fresh without-index samples — before it may be
    /// retried. When this has elapsed *and* its without-index average since
    /// rejection has climbed past `index-regression-ratio ×` the with-index
    /// average recorded at rejection, it returns to `candidate`. Guards against
    /// rebuild churn on an index we already know hurt. Default: 6h.
    ///
    /// (The regression *detection* side has no separate window: an index becomes
    /// eligible for the guard once it is older than `index-min-age-grace`, the
    /// same lifetime gate idle eviction uses — long enough to have gathered
    /// with-index latency to compare against its frozen without-index baseline.)
    #[serde(
        rename = "index-regression-retry-delay",
        default = "QueryTrackerConfig::default_index_regression_retry_delay",
        deserialize_with = "deserialize_duration_required"
    )]
    pub index_regression_retry_delay: Duration,
    /// Multiplier applied to the **without-index** average cost wherever it is
    /// compared against the with-index average — that is, in the `weighted`
    /// priority mode's latency gain (`ln((factor × without) / with)`) *and* in
    /// the latency regression guard's detection and recovery comparisons.
    ///
    /// It compensates for Postgres settings that keep the two sides from being
    /// strictly comparable: a without-index `getProgramAccounts` may fan out
    /// across several parallel workers, so its wall-clock time looks small even
    /// though it burns far more CPU/IO than a single-worker index scan. A
    /// `factor > 1` inflates that measured without-index cost to reflect the
    /// resources it actually consumed, so the index earns proportionally more
    /// credit (and is less likely to be judged a regression). `1.0` (default) is
    /// a no-op — the raw wall-clock averages are compared as-is.
    #[serde(
        rename = "without-index-compensation-factor",
        default = "QueryTrackerConfig::default_without_index_compensation_factor"
    )]
    pub without_index_compensation_factor: f64,

    // ---- discrepancy detection -------------------------------------------
    /// Emit a metric/log when demand (API) and supply (`idx_scan`) diverge,
    /// surfacing indexes Postgres is ignoring despite live demand.
    #[serde(
        rename = "discrepancy-enabled",
        default = "QueryTrackerConfig::default_discrepancy_enabled"
    )]
    pub discrepancy_enabled: bool,
    /// Relative gap (0.0–1.0) between normalized demand and supply beyond which
    /// a pattern is flagged as discrepant. Default: 0.10 (10%).
    #[serde(
        rename = "discrepancy-delta",
        default = "QueryTrackerConfig::default_discrepancy_delta"
    )]
    pub discrepancy_delta: f64,

    // ---- optional: EXPLAIN sampling --------------------------------------
    /// Opt-in third signal: periodically `EXPLAIN` (not ANALYZE) a synthetic
    /// probe of each created index to see whether the planner *would* use it
    /// right now — the only signal that cleanly tells "no traffic" apart from
    /// "planner refuses". Off by default.
    #[serde(
        rename = "explain-enabled",
        default = "QueryTrackerConfig::default_explain_enabled"
    )]
    pub explain_enabled: bool,
    /// How often the EXPLAIN sampling pass runs when enabled. Default: 6h.
    #[serde(
        rename = "explain-interval",
        default = "QueryTrackerConfig::default_explain_interval",
        deserialize_with = "deserialize_duration_required"
    )]
    pub explain_interval: Duration,
}

impl QueryTrackerConfig {
    fn default_index_min_age_grace() -> Duration {
        Duration::from_secs(3600)
    }

    fn default_index_eviction_interval() -> Duration {
        Duration::from_secs(3600)
    }

    const fn default_index_eviction_enabled() -> bool {
        false
    }

    const fn default_mark_unhealthy_for_eviction() -> bool {
        false
    }

    const fn default_use_supply_for_eviction() -> bool {
        false
    }

    fn default_index_min_idle() -> Duration {
        Duration::from_secs(3600 * 24)
    }

    const fn default_eviction_fill_threshold() -> f64 {
        0.9
    }

    const fn default_value_guard_creation_bias() -> f64 {
        1.0
    }

    fn default_drop_lock_timeout() -> Duration {
        Duration::from_secs(5)
    }

    const fn default_drop_retries() -> u32 {
        1
    }

    const fn default_index_regression_ratio() -> f64 {
        1.2
    }

    fn default_index_regression_retry_delay() -> Duration {
        Duration::from_secs(6 * 3600)
    }

    const fn default_without_index_compensation_factor() -> f64 {
        1.0
    }

    const fn default_discrepancy_enabled() -> bool {
        true
    }

    const fn default_discrepancy_delta() -> f64 {
        0.10
    }

    const fn default_explain_enabled() -> bool {
        false
    }

    fn default_explain_interval() -> Duration {
        Duration::from_secs(3600 * 6)
    }

    const fn default_cost_eligibility_threshold_us() -> Option<u64> {
        None
    }

    const fn default_create_database_indexes() -> bool {
        false
    }

    const fn default_index_generation_threshold() -> u32 {
        10
    }

    fn default_index_creation_delay() -> Duration {
        Duration::from_secs(10)
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
            priority_mode: PriorityMode::default(),
            cost_eligibility_threshold_us: Self::default_cost_eligibility_threshold_us(),
            included_programs: Vec::new(),
            excluded_programs: Vec::new(),
            indexer_metrics: String::default(),
            indexer_metrics_threshold: Self::default_indexer_metrics_threshold(),
            max_auto_indexes: None,
            index_eviction_enabled: Self::default_index_eviction_enabled(),
            mark_unhealthy_for_eviction: Self::default_mark_unhealthy_for_eviction(),
            use_supply_for_eviction: Self::default_use_supply_for_eviction(),
            index_eviction_interval: Self::default_index_eviction_interval(),
            index_min_age_grace: Self::default_index_min_age_grace(),
            index_min_idle: Self::default_index_min_idle(),
            eviction_fill_threshold: Self::default_eviction_fill_threshold(),
            value_guard_creation_bias: Self::default_value_guard_creation_bias(),
            drop_lock_timeout: Self::default_drop_lock_timeout(),
            drop_retries: Self::default_drop_retries(),
            index_regression_guard: IndexRegressionGuard::default(),
            index_regression_ratio: Self::default_index_regression_ratio(),
            index_regression_retry_delay: Self::default_index_regression_retry_delay(),
            without_index_compensation_factor: Self::default_without_index_compensation_factor(),
            discrepancy_enabled: Self::default_discrepancy_enabled(),
            discrepancy_delta: Self::default_discrepancy_delta(),
            explain_enabled: Self::default_explain_enabled(),
            explain_interval: Self::default_explain_interval(),
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
    #[serde(rename = "query-tracker")]
    pub query_tracker: QueryTrackerConfig,
}

impl TryLoadConfig for QueryTrackerServiceConfig {}

impl QueryTrackerServiceConfig {
    /// Address of the single HTTP server, which serves the functional endpoints
    /// (`/track`, `/debug/*`) and the operational endpoints (`/metrics`,
    /// `/health`) on the same port.
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
}

/// API-side client that buffers observed GPA patterns and flushes them to the
/// tracker over HTTP. Buffering is keyed by `IndexIdentity` (so many raw queries
/// collapse into one entry) and is bounded so a slow/unreachable tracker can
/// never grow memory without limit.
#[derive(Deserialize, Debug, Clone)]
pub struct QueryTrackerClientConfig {
    /// Base URL of the tracker's HTTP `/track` endpoint host, e.g.
    /// `http://query-tracker:4001`.
    pub endpoint: String,
    /// Per-request HTTP timeout when flushing a batch.
    #[serde(default, deserialize_with = "deserialize_duration")]
    pub timeout: Option<Duration>,
    /// How often the buffer is flushed to the tracker.
    #[serde(
        rename = "flush-interval",
        default,
        deserialize_with = "deserialize_duration"
    )]
    pub flush_interval: Option<Duration>,
    /// Max number of distinct identities held in the buffer. When full, new
    /// identities are dropped (with a counter) rather than growing unbounded;
    /// already-buffered identities keep aggregating. Default: 10_000.
    #[serde(
        rename = "max-buffered-identities",
        default = "QueryTrackerClientConfig::default_max_buffered_identities"
    )]
    pub max_buffered_identities: usize,
    /// Max identities sent per flush request; larger buffers are split across
    /// several requests so a single POST body stays bounded. Default: 1_000.
    #[serde(
        rename = "max-batch-size",
        default = "QueryTrackerClientConfig::default_max_batch_size"
    )]
    pub max_batch_size: usize,
}

impl QueryTrackerClientConfig {
    const fn default_max_buffered_identities() -> usize {
        10_000
    }

    const fn default_max_batch_size() -> usize {
        1_000
    }
}

pub const DEFAULT_QUERY_TRACKER_SERVER_PORT: u16 = 4001;

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

#[cfg(test)]
mod tests {
    use super::*;

    // The eviction feature drops database indexes, so its defaults are a safety contract: it must
    // be off unless explicitly enabled, with conservative windows. Lock these so a refactor can't
    // silently flip them.
    #[test]
    fn query_tracker_eviction_defaults_are_safe() {
        let c = QueryTrackerConfig::default();
        assert!(!c.index_eviction_enabled, "eviction must be off by default");
        assert!(
            !c.mark_unhealthy_for_eviction,
            "mark-unhealthy-for-eviction must be off by default"
        );
        assert!(
            !c.use_supply_for_eviction,
            "use-supply-for-eviction must be off by default"
        );
        assert_eq!(c.index_min_idle, Duration::from_secs(86400));
        assert_eq!(c.index_min_age_grace, Duration::from_secs(3600));
        assert_eq!(c.index_eviction_interval, Duration::from_secs(3600));
        // Neutral value guard by default: candidate and incumbent scores compared as-is.
        assert_eq!(c.value_guard_creation_bias, 1.0);
    }
}
