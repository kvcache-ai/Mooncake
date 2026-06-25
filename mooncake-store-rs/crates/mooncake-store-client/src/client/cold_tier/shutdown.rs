use super::super::{ColdTierShutdownMode, StoreClient};

impl StoreClient {
    pub(in super::super) fn flush_pending_offloads_before_drain(&self) {
        const MAX_ROUNDS: usize = 64;
        const BATCH_SIZE: usize = 32;
        for round in 0..MAX_ROUNDS {
            if self.storage_owner.pending_offloads.is_empty() {
                if round > 0 {
                    tracing::info!(
                        runtime = %self.lease.runtime,
                        rounds = round,
                        "flush_pending_offloads_before_drain: complete"
                    );
                }
                return;
            }
            match self
                .storage_owner
                .materialize_pending_offloads_bounded(BATCH_SIZE)
            {
                Ok(0) => {
                    tracing::info!(
                        runtime = %self.lease.runtime,
                        rounds = round,
                        remaining = self.storage_owner.pending_offloads.len(),
                        "flush_pending_offloads_before_drain: no progress, stopping"
                    );
                    return;
                }
                Ok(materialized) => {
                    tracing::debug!(
                        runtime = %self.lease.runtime,
                        round,
                        materialized,
                        "flush_pending_offloads_before_drain: round complete"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        runtime = %self.lease.runtime,
                        round,
                        %error,
                        "flush_pending_offloads_before_drain: error, stopping"
                    );
                    return;
                }
            }
        }
        tracing::warn!(
            runtime = %self.lease.runtime,
            remaining = self.storage_owner.pending_offloads.len(),
            "flush_pending_offloads_before_drain: reached max rounds"
        );
    }

    fn cold_backing_on_local_device(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> bool {
        self.storage_owner
            .cold_tier_devices
            .has_local_backend(&cold_backing.cold_tier_id)
    }

    pub(in super::super) fn cleanup_owned_routes_on_shutdown(&self) {
        let routes = match self.collect_routes_by_replica_owner(&self.lease.runtime) {
            Ok(routes) => routes,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    runtime = %self.lease.runtime,
                    "shutdown route cleanup: failed to list owned routes"
                );
                return;
            }
        };

        let mode = self.cold_tier_shutdown_mode;
        let mut cleaned = 0usize;
        for route in &routes {
            let mut next = route.clone();
            next.version = route.version.next();
            next.replicas
                .retain(|replica| replica.owner != self.lease.runtime);

            if mode == ColdTierShutdownMode::Restart
                && next.replicas.is_empty()
                && !route.cold_backing.as_ref().is_some_and(|cold_backing| {
                    cold_backing.state == mooncake_store_core::ColdBackingState::Materialized
                })
            {
                continue;
            }

            if mode == ColdTierShutdownMode::Decommission
                && route
                    .cold_backing
                    .as_ref()
                    .is_some_and(|cold_backing| self.cold_backing_on_local_device(cold_backing))
            {
                next.cold_backing = route.cold_backing.clone();
                if let Some(cold_backing) = next.cold_backing.as_mut() {
                    cold_backing.state = mooncake_store_core::ColdBackingState::PendingDelete;
                }
            }

            let should_delete = mode == ColdTierShutdownMode::Decommission
                && next.replicas.is_empty()
                && next.cold_backing.is_none();
            let cas_result = if should_delete {
                self.route_ops()
                    .delete_route(&route.key, Some(route.version))
            } else if next.replicas.len() < route.replicas.len()
                || next.cold_backing != route.cold_backing
            {
                self.route_ops()
                    .prune_route(&route.key, Some(route.version), &next)
            } else {
                continue;
            };

            match cas_result {
                Ok(cas) if cas.applied => {
                    cleaned = cleaned.saturating_add(1);
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        key = %route.key.0,
                        error = %error,
                        "shutdown route cleanup CAS failed"
                    );
                }
            }
        }

        if mode == ColdTierShutdownMode::Decommission {
            self.mark_local_cold_backings_pending_delete_on_decommission();
        }

        if cleaned > 0 {
            tracing::info!(
                runtime = %self.lease.runtime,
                cleaned,
                total = routes.len(),
                mode = ?mode,
                "shutdown route cleanup complete"
            );
        }
    }

    fn mark_local_cold_backings_pending_delete_on_decommission(&self) {
        const MAX_DECOMMISSION_COLD_BACKINGS: usize = 1024;

        let device_ids = self.storage_owner.cold_tier_devices.local_device_ids();
        for device_id in &device_ids {
            let routes = match self.metadata.as_ref().list_object_routes_by_cold_backing(
                &mooncake_store_core::ColdBackingRouteFilter {
                    device_id: Some(device_id.clone()),
                    limit: Some(MAX_DECOMMISSION_COLD_BACKINGS),
                    ..Default::default()
                },
            ) {
                Ok(routes) => routes,
                Err(error) => {
                    tracing::warn!(
                        device_id,
                        error = %error,
                        "decommission: failed to list cold routes"
                    );
                    continue;
                }
            };
            for route in &routes {
                let Some(cold_backing) = route.cold_backing.as_ref() else {
                    continue;
                };
                if cold_backing.state == mooncake_store_core::ColdBackingState::PendingDelete {
                    continue;
                }
                let mut next = route.clone();
                next.version = route.version.next();
                if let Some(next_backing) = next.cold_backing.as_mut() {
                    next_backing.state = mooncake_store_core::ColdBackingState::PendingDelete;
                }
                let Err(error) =
                    self.route_ops()
                        .prune_route(&route.key, Some(route.version), &next)
                else {
                    continue;
                };
                tracing::warn!(
                    key = %route.key.0,
                    device_id,
                    error = %error,
                    "decommission: failed to mark cold backing pending delete"
                );
            }
            if let Err(error) = self
                .storage_owner
                .garbage_collect_pending_delete_backings_for_device_bounded(
                    device_id,
                    routes.len().min(MAX_DECOMMISSION_COLD_BACKINGS),
                )
            {
                tracing::warn!(
                    device_id,
                    error = %error,
                    "decommission: failed to garbage collect pending-delete cold backings"
                );
            }
        }
    }
}
