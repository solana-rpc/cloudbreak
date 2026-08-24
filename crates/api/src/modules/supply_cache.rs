use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use cloudbreak_core::modules::supply_tracker::SUPPLY_RING_SLOTS;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use solana_pubkey::Pubkey;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Default)]
pub struct SupplySnapshot {
    pub rows: Vec<SupplyRow>,
    pub non_circulating_accounts: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
pub struct SupplyRow {
    pub slot: u64,
    pub total: u64,
    pub non_circulating: Option<u64>,
}

pub type SharedSupplySnapshot = Arc<RwLock<Arc<SupplySnapshot>>>;

const SUPPLY_POLL_INTERVAL: Duration = Duration::from_secs(5);

fn decimal_to_u64(value: Decimal, column: &str) -> Result<u64, anyhow::Error> {
    value
        .to_u64()
        .ok_or_else(|| anyhow::anyhow!("supply.{} {} does not fit in u64", column, value))
}

pub async fn load_latest_supply(
    db: &DatabaseConnection,
) -> Result<Option<SupplySnapshot>, anyhow::Error> {
    let supply_rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT slot, total, non_circulating_lamports FROM supply ORDER BY slot DESC LIMIT $1",
            [(SUPPLY_RING_SLOTS as i64).into()],
        ))
        .await?;
    if supply_rows.is_empty() {
        return Ok(None);
    }
    let mut rows = Vec::with_capacity(supply_rows.len());
    for row in supply_rows {
        let slot: i64 = row.try_get("", "slot")?;
        let total = decimal_to_u64(row.try_get("", "total")?, "total")?;
        let non_circulating: Option<Decimal> = row.try_get("", "non_circulating_lamports")?;
        let non_circulating = non_circulating
            .map(|lamports| decimal_to_u64(lamports, "non_circulating_lamports"))
            .transpose()?;
        rows.push(SupplyRow {
            slot: slot as u64,
            total,
            non_circulating,
        });
    }
    rows.reverse();

    let nc_row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT accounts FROM non_circulating_accounts WHERE id = 1".to_string(),
        ))
        .await?;
    let non_circulating_accounts = match nc_row {
        Some(row) => {
            let accounts: Vec<Vec<u8>> = row.try_get("", "accounts")?;
            Some(
                accounts
                    .into_iter()
                    .filter_map(|bytes| Pubkey::try_from(bytes.as_slice()).ok())
                    .map(|p| p.to_string())
                    .collect(),
            )
        }
        None => None,
    };

    Ok(Some(SupplySnapshot {
        rows,
        non_circulating_accounts,
    }))
}

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
