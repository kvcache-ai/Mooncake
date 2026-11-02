use super::super::LocalAllocatorAdapter;
use super::{
    batch_read_from_cold_staged, execute_owner_cold_restore_promote_phase, read_from_cold_one_shot,
};
use crate::control_plane::{
    ColdReadResponse, ColdReadTarget, ColdReclaimResult, ColdTierControlService,
    ColdTierFreeResult, ColdTierProbeResult,
};
use mooncake_store_core::{ClientRuntimeId, ObjectKey, ObjectRoute, SegmentName, StoreError};
use std::sync::Arc;

impl ColdTierControlService for LocalAllocatorAdapter {
    fn trigger_offload(&self, max_tasks: usize) -> mooncake_store_core::Result<usize> {
        self.ensure_accepting_writes()?;
        self.storage_owner
            .materialize_pending_offloads_bounded(max_tasks)
    }

    fn manual_gc(
        &self,
        device_id: &str,
        max_backings: usize,
    ) -> mooncake_store_core::Result<usize> {
        self.ensure_accepting_writes()?;
        self.storage_owner
            .garbage_collect_pending_delete_backings_for_device_bounded(device_id, max_backings)
    }

    fn manual_free(
        &self,
        device_id: &str,
        max_victims: usize,
    ) -> mooncake_store_core::Result<ColdTierFreeResult> {
        self.ensure_accepting_writes()?;
        let result = self
            .storage_owner
            .manual_free_cold_tier_device_bounded(device_id, max_victims)?;
        let collected = if result.freed_backings > 0 {
            self.storage_owner
                .garbage_collect_pending_delete_backings_for_device_bounded(
                    device_id,
                    result.freed_backings,
                )?
        } else {
            0
        };
        let _ = self.storage_owner.compact_cold_tier_backends();
        Ok(ColdTierFreeResult {
            attempted_victims: result.attempted_victims,
            freed_backings: result.freed_backings,
            collected_backings: collected,
            skipped_backings: result.skipped_backings,
            reached_low_watermark: result.reached_low_watermark,
        })
    }

    fn probe_device(&self, device_id: &str) -> mooncake_store_core::Result<ColdTierProbeResult> {
        let device = self.storage_owner.probe_cold_tier_device(device_id)?;
        let schedulable = device.schedulable();
        Ok(ColdTierProbeResult {
            device_id: device.device_id,
            capacity_bytes: device.capacity_bytes,
            used_bytes: device.used_bytes,
            reserved_bytes: device.reserved_bytes,
            schedulable,
            state: format!("{:?}", device.state),
            last_error: device.last_error,
        })
    }

    fn read_from_cold(
        &self,
        namespace: &str,
        authority: &str,
        tenant: &str,
        key: &str,
        domain: &str,
        object_set: &str,
    ) -> ColdReadResponse {
        if authority != self.runtime.stable_id.0 {
            return ColdReadResponse {
                result: Err(StoreError::InvalidState(format!(
                    "cold tier read authority mismatch: request={authority} local={}",
                    self.runtime.stable_id.0
                ))),
                deferred_promote: None,
            };
        }
        if namespace != self.storage_owner.metadata.route_namespace() {
            return ColdReadResponse {
                result: Err(StoreError::InvalidState(format!(
                    "cold tier read namespace mismatch: request={namespace} local={}",
                    self.storage_owner.metadata.route_namespace()
                ))),
                deferred_promote: None,
            };
        }
        let (result, deferred_ctx) = read_from_cold_one_shot(
            self.storage_owner.as_ref(),
            &self.allocator,
            tenant,
            key,
            domain,
            object_set,
        );
        let deferred_promote = deferred_ctx.map(|ctx| {
            let storage_owner = Arc::clone(&self.storage_owner);
            let allocator = Arc::clone(&self.allocator);
            Box::new(move || {
                execute_owner_cold_restore_promote_phase(&storage_owner, &allocator, ctx);
            }) as Box<dyn FnOnce() + Send>
        });
        ColdReadResponse {
            result,
            deferred_promote,
        }
    }

    fn batch_read_from_cold(
        &self,
        namespace: &str,
        authority: &str,
        targets: Vec<ColdReadTarget>,
    ) -> Vec<ColdReadResponse> {
        if authority != self.runtime.stable_id.0 {
            return targets
                .into_iter()
                .map(|_| ColdReadResponse {
                    result: Err(StoreError::InvalidState(format!(
                        "cold tier read authority mismatch: request={authority} local={}",
                        self.runtime.stable_id.0
                    ))),
                    deferred_promote: None,
                })
                .collect();
        }
        if namespace != self.storage_owner.metadata.route_namespace() {
            return targets
                .into_iter()
                .map(|_| ColdReadResponse {
                    result: Err(StoreError::InvalidState(format!(
                        "cold tier read namespace mismatch: request={namespace} local={}",
                        self.storage_owner.metadata.route_namespace()
                    ))),
                    deferred_promote: None,
                })
                .collect();
        }
        batch_read_from_cold_staged(&self.storage_owner, &self.allocator, targets)
    }

    fn ack_cold_read_complete(&self, slots: &[(SegmentName, u64)]) {
        self.storage_owner.read_pin_registry.unpin_batch(slots);
        let mut pending = self.storage_owner.pending_staging_slots.lock();
        for (segment, offset) in slots {
            if !self
                .storage_owner
                .read_pin_registry
                .is_pinned(segment, *offset)
            {
                pending.remove(&(segment.clone(), *offset));
            }
        }
    }

    fn batch_reclaim_cold_backings(
        &self,
        namespace: &str,
        authority: &str,
        route_key: ObjectKey,
        cold_backings: Vec<mooncake_store_core::ColdBackingRoute>,
    ) -> Vec<mooncake_store_core::Result<ColdReclaimResult>> {
        if authority != self.runtime.stable_id.0 {
            return cold_backings
                .into_iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "cold tier reclaim authority mismatch: request={authority} local={}",
                        self.runtime.stable_id.0
                    )))
                })
                .collect();
        }
        if namespace != self.storage_owner.metadata.route_namespace() {
            return cold_backings
                .into_iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "cold tier reclaim namespace mismatch: request={namespace} local={}",
                        self.storage_owner.metadata.route_namespace()
                    )))
                })
                .collect();
        }
        cold_backings
            .into_iter()
            .map(|cold_backing| {
                self.storage_owner
                    .reclaim_cold_backing_for_route_delete(&route_key, &cold_backing)
            })
            .collect()
    }

    fn accept_nof_owner_snapshot(
        &self,
        target_id: String,
        from: ClientRuntimeId,
        to: ClientRuntimeId,
        routes: Vec<ObjectRoute>,
    ) -> mooncake_store_core::Result<usize> {
        self.storage_owner
            .accept_nof_owner_snapshot(target_id, from, to, routes)
    }

    fn manage_nof_backing(
        &self,
        target_id: String,
        action: crate::control_plane::ManagedNofRouteAction,
        route: ObjectRoute,
        length: u64,
        checksum: Option<u64>,
    ) -> mooncake_store_core::Result<ObjectRoute> {
        self.storage_owner
            .cold_tier_devices
            .nof_targets
            .manage_managed_route(&target_id, action, route, length, checksum)
    }

    fn pin_for_read(&self, slots: &[(SegmentName, u64)]) -> u64 {
        let pending = self.storage_owner.pending_staging_slots.lock();
        slots
            .iter()
            .filter(|(segment, offset)| {
                pending.contains_key(&(segment.clone(), *offset))
                    && self
                        .storage_owner
                        .read_pin_registry
                        .is_pinned(segment, *offset)
            })
            .count() as u64
    }
}
