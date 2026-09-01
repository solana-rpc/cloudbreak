// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Shared node-health writer.
//!
//! The authoritative `service_health` row (id = 1) and the denormalised
//! `slots.health` flag are written together in a single transaction so they can
//! never diverge. It lives in `core` so every service that flips node health
//! shares one atomic implementation instead of each reimplementing the upsert:
//! the indexer (startup / gap-fill transitions) and, optionally, the query
//! tracker (draining traffic while `DROP INDEX` holds heavy locks during
//! eviction).

use std::time::Duration;

use cloudbreak_entity::{service_health, slots};
use sea_orm::{
    ActiveValue::{NotSet, Set},
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr, EntityTrait, Statement,
    TransactionTrait,
    prelude::Expr,
    sea_query::OnConflict,
};
use tokio::time::timeout;

/// Upper bound on the whole health-write transaction, so a stuck write can never
/// wedge the caller indefinitely.
const HEALTH_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Atomically set the node's health flag: upsert the single `service_health`
/// row (id = 1) and denormalise the same value onto every `slots` row, in one
/// transaction bounded by [`HEALTH_WRITE_TIMEOUT`]. Returns the DB result so each
/// caller can log or count errors as it sees fit (a timeout maps to
/// [`DbErr::RecordNotInserted`]).
pub async fn update_service_health(db: &DatabaseConnection, healthy: bool) -> Result<(), DbErr> {
    timeout(HEALTH_WRITE_TIMEOUT, async {
        let txn = db.begin().await?;

        service_health::Entity::insert(service_health::ActiveModel {
            id: Set(1), // Always the one default record.
            healthy: Set(healthy),
            last_updated_at: NotSet,
        })
        .on_conflict(
            OnConflict::columns([service_health::Column::Id])
                .update_columns([
                    service_health::Column::Healthy,
                    service_health::Column::LastUpdatedAt,
                ])
                .to_owned(),
        )
        .exec_without_returning(&txn)
        .await?;

        // Denormalise the same health state onto every `slots` row so the API can
        // read the latest slot and the health flag in a single lookup.
        slots::Entity::update_many()
            .col_expr(slots::Column::Health, Expr::value(healthy))
            .exec(&txn)
            .await?;

        txn.commit().await
    })
    .await
    .unwrap_or_else(|elapsed| {
        tracing::error!(target: "service_health", "update_service_health timed out: {elapsed}");
        Err(DbErr::RecordNotInserted)
    })
}

/// Reads the node's health flag from the `service_health` row (id = 1). Returns
/// `false` when the row is missing or the query fails.
pub async fn is_healthy(db: &DatabaseConnection) -> bool {
    db.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT healthy FROM service_health WHERE id = 1".to_string(),
    ))
    .await
    .ok()
    .flatten()
    .and_then(|row| row.try_get::<bool>("", "healthy").ok())
    .unwrap_or(false)
}
