use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

pub use cloudbreak_core::modules::supply::{SupplySnapshot, load_latest_supply};
use sea_orm::DatabaseConnection;
use tokio::task::JoinHandle;

pub type SharedSupplySnapshot = Arc<RwLock<Arc<SupplySnapshot>>>;

const SUPPLY_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Polls the supply ring and the member list into the API cache. The query lives
/// in core `read.rs`, shared with the indexer; this task only refreshes the cache.
pub fn spawn_poll_task(db: DatabaseConnection, cache: SharedSupplySnapshot) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SUPPLY_POLL_INTERVAL).await;
            match load_latest_supply(&db).await {
                Ok(Some(snapshot)) => {
                    *cache.write().unwrap() = Arc::new(snapshot);
                }
                Ok(None) => {
                    tracing::debug!(target: "supply_cache", "supply table is empty; will retry");
                }
                Err(e) => {
                    tracing::error!(target: "supply_cache", "failed to load supply: {:?}", e);
                }
            }
        }
    })
}
