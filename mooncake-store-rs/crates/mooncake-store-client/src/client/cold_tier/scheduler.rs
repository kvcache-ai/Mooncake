use super::super::{
    current_time_ms, ClientEpoch, ClientLease, ClientRuntimeId, LocalMemoryConfig, ObjectRoute,
    Result, RouteTrafficReport, SharedRestorePromotionQueue, StorageOwnerState,
};
use super::{AsyncOffloadHandle, AsyncRestorePromotionHandle};
use std::sync::{atomic::Ordering, Arc};

const INITIAL_COLD_BACKING_REPAIR_INTERVAL_MS: u64 = 1_000;

pub(in super::super) struct ColdTierHandle {
    active: bool,
    _async_offload: AsyncOffloadHandle,
    _async_restore_promotion: AsyncRestorePromotionHandle,
}

impl ColdTierHandle {
    pub(in super::super) fn spawn(
        runtime: &ClientRuntimeId,
        lease: &ClientLease,
        local_memory: &LocalMemoryConfig,
        storage_owner: Arc<StorageOwnerState>,
        restore_promotions: SharedRestorePromotionQueue,
    ) -> Result<Self> {
        // Cold tier repair (rebuild offload queue, restore cold-only routes,
        // materialize) is deferred entirely to background_tick / repair_owned_state.
        // This keeps the startup path non-blocking and error-tolerant — critical
        // during hot-upgrade and on CI where metadata services may not be
        // immediately reachable.
        Self::with_workers(
            runtime,
            lease,
            local_memory,
            storage_owner,
            restore_promotions,
        )
    }

    pub(in super::super) fn run_startup(
        &self,
        storage_owner: Arc<StorageOwnerState>,
        lease: &ClientLease,
    ) -> Result<()> {
        if !self.active || super::cold_tier_disabled() {
            return Ok(());
        }
        if !serves_storage(lease) {
            return Ok(());
        }
        // During startup only rebuild the offload queue. Materialization is
        // deferred to background_tick so that authority RPC calls
        // (query_authorities_with_merge) cannot stall startup — critical during
        // hot-upgrade when the predecessor is mid-drain. Cold-only routes are
        // restored on demand by the read path, not eagerly here.
        if !storage_owner.pending_offloads.is_materializing() {
            super::offload::rebuild_pending_offload_queue(storage_owner.as_ref())?;
        }
        Ok(())
    }

    pub(in super::super) fn repair_owned_state(storage_owner: &StorageOwnerState) -> Result<()> {
        if super::cold_tier_disabled() {
            return Ok(());
        }
        Self::drain_pending_offloads(storage_owner)?;
        Ok(())
    }

    pub(in super::super) fn repair_after_metadata_recovery(
        &self,
        storage_owner: &StorageOwnerState,
    ) -> Result<()> {
        if !self.active || super::cold_tier_disabled() {
            return Ok(());
        }
        if !storage_owner.pending_offloads.is_materializing() {
            super::offload::rebuild_pending_offload_queue(storage_owner)?;
        }
        Self::drain_pending_offloads(storage_owner)?;
        Ok(())
    }

    pub(in super::super) fn background_tick(storage_owner: &StorageOwnerState) {
        if super::cold_tier_disabled() {
            storage_owner.record_cold_tier_observability_snapshots();
            return;
        }
        let pending_offloads = Self::refresh_offload_pressure(storage_owner);
        if Self::is_idle_without_local_devices(storage_owner, pending_offloads) {
            storage_owner.record_cold_tier_observability_snapshots();
            return;
        }

        Self::run_cleanup_jobs(storage_owner);
        Self::run_owner_repair_jobs(storage_owner);
        Self::run_primary_offload_job(storage_owner);
    }

    pub(in super::super) fn disabled(restore_promotions: SharedRestorePromotionQueue) -> Self {
        Self {
            active: false,
            _async_offload: AsyncOffloadHandle::disabled(),
            _async_restore_promotion: AsyncRestorePromotionHandle::spawn(
                &ClientRuntimeId::new("disabled", ClientEpoch(0)),
                restore_promotions,
            ),
        }
    }

    pub(in super::super) fn on_route_published(
        &self,
        storage_owner: &StorageOwnerState,
        route: &ObjectRoute,
    ) -> Option<ObjectRoute> {
        if !self.active || super::cold_tier_disabled() {
            return None;
        }
        storage_owner.track_route(route);
        Self::publish_initial_cold_backing_for_tracked_route(storage_owner, route)
    }

    pub(in super::super) fn on_routes_tracked(
        storage_owner: &StorageOwnerState,
        routes: &[ObjectRoute],
    ) -> RouteTrafficReport {
        let report = storage_owner.track_routes(routes);
        if super::cold_tier_disabled() {
            return report;
        }
        for route in routes {
            Self::publish_initial_cold_backing_for_tracked_route(storage_owner, route);
        }
        report
    }

    pub(in super::super) fn kick_offload_for_allocator_eviction(storage_owner: &StorageOwnerState) {
        if super::cold_tier_disabled() {
            return;
        }
        if let Err(error) = storage_owner.materialize_pending_offloads_foreground_kick() {
            tracing::warn!(
                runtime = %storage_owner.runtime,
                error = %error,
                "failed to materialize pending offloads before allocator eviction"
            );
        }
    }

    fn publish_initial_cold_backing_for_tracked_route(
        storage_owner: &StorageOwnerState,
        route: &ObjectRoute,
    ) -> Option<ObjectRoute> {
        if super::cold_tier_disabled() {
            return None;
        }
        storage_owner.enqueue_pending_offload(route);
        match super::publish_initial_write_cold_backing(storage_owner, route) {
            Ok(updated) => updated,
            Err(error) => {
                tracing::warn!(
                    runtime = %storage_owner.runtime,
                    key = %route.key.0,
                    error = %error,
                    "owner-side initial cold backing publication failed"
                );
                None
            }
        }
    }

    fn refresh_offload_pressure(storage_owner: &StorageOwnerState) -> usize {
        let pending_offloads = storage_owner.pending_offloads.len();
        storage_owner
            .cold_tier_devices
            .pressure_refresh_depth(pending_offloads);
        pending_offloads
    }

    fn is_idle_without_local_devices(
        storage_owner: &StorageOwnerState,
        pending_offloads: usize,
    ) -> bool {
        !storage_owner.cold_tier_devices.has_any_persistent_backend() && pending_offloads == 0
    }

    fn run_cleanup_jobs(storage_owner: &StorageOwnerState) {
        if let Err(error) = storage_owner.free_cold_tier_until_low_watermark() {
            tracing::warn!(error = %error, "background cold tier watermark cleanup failed");
        }
        if let Err(error) = storage_owner.garbage_collect_pending_delete_backings() {
            tracing::warn!(error = %error, "background pending-delete cold backing gc failed");
        }
    }

    fn run_owner_repair_jobs(storage_owner: &StorageOwnerState) {
        if let Err(error) = Self::repair_initial_write_cold_backings(storage_owner) {
            tracing::warn!(error = %error, "background initial cold backing repair failed");
        }
    }

    fn run_primary_offload_job(storage_owner: &StorageOwnerState) {
        if let Err(error) = Self::repair_owned_state(storage_owner) {
            tracing::warn!(error = %error, "background cold tier owned-state repair failed");
        }
    }

    fn drain_pending_offloads(storage_owner: &StorageOwnerState) -> Result<usize> {
        storage_owner.materialize_pending_offloads()
    }

    fn repair_initial_write_cold_backings(storage_owner: &StorageOwnerState) -> Result<usize> {
        let now = current_time_ms();
        let last = storage_owner
            .initial_cold_backing_repair_at_ms
            .load(Ordering::Acquire);
        if now.saturating_sub(last) < INITIAL_COLD_BACKING_REPAIR_INTERVAL_MS {
            return Ok(0);
        }
        if storage_owner
            .initial_cold_backing_repair_at_ms
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(0);
        }
        super::offload::repair_initial_write_cold_backings(
            storage_owner,
            storage_owner.offload_priority.pending_scan_limit,
        )
    }

    fn with_workers(
        runtime: &ClientRuntimeId,
        lease: &ClientLease,
        local_memory: &LocalMemoryConfig,
        storage_owner: Arc<StorageOwnerState>,
        restore_promotions: SharedRestorePromotionQueue,
    ) -> Result<Self> {
        let async_offload = if super::cold_tier_disabled() {
            AsyncOffloadHandle::disabled()
        } else {
            AsyncOffloadHandle::spawn(runtime, lease, local_memory, storage_owner)?
        };
        Ok(Self {
            active: true,
            _async_offload: async_offload,
            _async_restore_promotion: AsyncRestorePromotionHandle::spawn(
                runtime,
                restore_promotions,
            ),
        })
    }

    pub(in super::super) fn shutdown(&mut self) {
        self._async_restore_promotion.shutdown();
        self._async_offload.shutdown();
    }
}

fn serves_storage(lease: &ClientLease) -> bool {
    lease
        .endpoints
        .labels
        .get("storage")
        .map(|value| value == "true")
        .unwrap_or(false)
}

impl Drop for ColdTierHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}
