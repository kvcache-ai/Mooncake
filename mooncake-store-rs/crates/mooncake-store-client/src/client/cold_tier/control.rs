use super::super::LocalAllocatorAdapter;
use super::{
    batch_read_from_cold_staged, execute_owner_cold_restore_promote_phase, read_from_cold_one_shot,
};
use crate::control_plane::{ColdReadResponse, ColdReadTarget, ColdTierControlService};
use mooncake_store_core::{SegmentName, StoreError};
use std::sync::Arc;

impl ColdTierControlService for LocalAllocatorAdapter {
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
