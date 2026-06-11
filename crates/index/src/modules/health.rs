// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::DatabaseConnection;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::db_queries;

/// Reasons that keep the service marked as unhealthy. The service is reported
/// healthy on the DB only when there are no active reasons left.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum HealthReason {
    /// Set while the startup snapshot is still being processed/cleaned up.
    Startup,
    /// Set while a gap fill is in progress (finalization paused).
    /// (Only can be one snapshot filling gaps at a time, which sets this)
    GapFill,
}

/// Tracks service health as a set of active "unhealthy" reasons instead of a
/// single boolean, so that independent subsystems (startup, gap filling) can not
/// clobber each other's health state.
#[derive(Clone)]
pub struct ServiceHealth {
    reasons: Arc<Mutex<HashSet<HealthReason>>>,
    db: DatabaseConnection,
}

impl ServiceHealth {
    /// Starts in the `Startup` unhealthy state, matching the previous behaviour where
    /// the service is only marked healthy once the startup snapshot is processed.
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            reasons: Arc::new(Mutex::new(HashSet::from([HealthReason::Startup]))),
            db,
        }
    }

    /// Adds an unhealthy reason. If it was not already present, marks the service unhealthy.
    pub async fn add_reason(&self, reason: HealthReason) {
        let newly_added = self
            .reasons
            .lock()
            .expect("Failed to lock health reasons")
            .insert(reason);

        if newly_added {
            tracing::warn!(target: "service_health", "Service marked UNHEALTHY (reason added: {:?})", reason);
            db_queries::update_service_health(&self.db, false).await;
        }
    }

    /// Removes an unhealthy reason. If it was present and no reasons remain, marks the service
    /// healthy. Calling this for an absent reason is a cheap no-op (no DB write).
    pub async fn remove_reason(&self, reason: HealthReason) {
        let (was_present, now_empty) = {
            let mut reasons = self.reasons.lock().expect("Failed to lock health reasons");
            let was_present = reasons.remove(&reason);
            (was_present, reasons.is_empty())
        };

        if was_present && now_empty {
            tracing::info!(target: "service_health", "Service marked HEALTHY (reason cleared: {:?})", reason);
            db_queries::update_service_health(&self.db, true).await;
        }
    }
}
