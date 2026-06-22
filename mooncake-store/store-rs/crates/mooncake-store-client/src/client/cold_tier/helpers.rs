use super::super::{
    validate_replica_checksum, ColdObjectWrite, LocalAllocatorState, ObjectRoute,
    PendingOffloadPayload, PersistentStorageBackend, ReplicaRoute, ReplicaTier, Result,
    SegmentName, StoreError, StoreState,
};
use parking_lot::Mutex;
use std::sync::Arc;

#[allow(dead_code)]
pub(in super::super) fn backend_store_cold_payload_batch(
    backend: &dyn PersistentStorageBackend,
    writes: &[ColdObjectWrite<'_>],
) -> Vec<Result<mooncake_store_core::ColdBackingRoute>> {
    backend.put_objects_batch(writes)
}

#[allow(dead_code)]
pub(in super::super) fn backend_remove_cold_payload(
    backend: &dyn PersistentStorageBackend,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
) -> Result<bool> {
    backend.delete_object(cold_backing)
}

#[allow(dead_code)]
pub(in super::super) fn backend_remove_cold_payload_batch(
    backend: &dyn PersistentStorageBackend,
    cold_backings: &[&mooncake_store_core::ColdBackingRoute],
) -> Vec<Result<bool>> {
    backend.delete_objects_batch(cold_backings)
}

#[allow(dead_code)]
pub(in super::super) fn backend_store_pending_source(
    backend: &dyn PersistentStorageBackend,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
    payload: &[u8],
) -> Result<()> {
    backend.put_pending_source(cold_backing, payload)
}

#[allow(dead_code)]
pub(in super::super) fn backend_load_pending_source(
    backend: &dyn PersistentStorageBackend,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
) -> Result<Option<Vec<u8>>> {
    backend.get_pending_source(cold_backing)
}

#[allow(dead_code)]
pub(in super::super) fn backend_remove_pending_source(
    backend: &dyn PersistentStorageBackend,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
) -> Result<bool> {
    backend.delete_pending_source(cold_backing)
}

#[allow(dead_code)]
pub(in super::super) fn same_cold_payload(
    left: &mooncake_store_core::ColdBackingRoute,
    right: &mooncake_store_core::ColdBackingRoute,
) -> bool {
    left.owner == right.owner
        && left.cold_tier_id == right.cold_tier_id
        && left.object_locator == right.object_locator
}

#[allow(dead_code)]
pub(in super::super) fn materialized_cold_backing(
    route: &ObjectRoute,
) -> Option<mooncake_store_core::ColdBackingRoute> {
    route
        .cold_backing
        .clone()
        .filter(|cold| cold.state == mooncake_store_core::ColdBackingState::Materialized)
}

#[allow(dead_code)]
pub(in super::super) fn cold_backing_placeholder(
    cold_backing: &mooncake_store_core::ColdBackingRoute,
) -> ReplicaRoute {
    ReplicaRoute {
        owner: cold_backing.owner.clone(),
        segment_name: SegmentName::new("__cold_backing__"),
        offset: None,
        segment_offset: 0,
        length: cold_backing.length,
        checksum: cold_backing.checksum,
        tier: ReplicaTier::File,
        priority: 0,
    }
}

pub(in super::super) fn read_local_hot_replica_payload(
    allocator: &Arc<Mutex<LocalAllocatorState>>,
    state: &Arc<Mutex<StoreState>>,
    backend: &dyn PersistentStorageBackend,
    route: &ObjectRoute,
    replica: &ReplicaRoute,
) -> Result<PendingOffloadPayload> {
    if let Some(announcement) = allocator.lock().announcement(&replica.segment_name) {
        if announcement.owner != replica.owner {
            return Err(StoreError::InvalidState(format!(
                "segment {} owner mismatch while materializing {}",
                replica.segment_name.0, route.key.0
            )));
        }
    }
    let payload = borrow_replica_payload_from_local_memory(state, replica).or_else(|_| {
        load_offload_source(backend, route)?
            .map(PendingOffloadPayload::Owned)
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "route {} has no source payload available for offload",
                    route.key.0
                ))
            })
    })?;
    if payload.len() != replica.length as usize {
        return Err(StoreError::InvalidState(format!(
            "route {} local payload length mismatch: expected {} actual {}",
            route.key.0,
            replica.length,
            payload.len()
        )));
    }
    validate_replica_checksum(replica, payload.as_slice())?;
    Ok(payload)
}

fn borrow_replica_payload_from_local_memory(
    state: &Arc<Mutex<StoreState>>,
    replica: &ReplicaRoute,
) -> Result<PendingOffloadPayload> {
    let addr = {
        let state = state.lock();
        state
            .memory_ref()?
            .storage_address(&replica.segment_name, replica.segment_offset as usize)?
    };
    Ok(PendingOffloadPayload::LocalHot {
        addr: addr.cast::<u8>(),
        len: replica.length as usize,
    })
}

fn load_offload_source(
    backend: &dyn PersistentStorageBackend,
    route: &ObjectRoute,
) -> Result<Option<Vec<u8>>> {
    let Some(cold_backing) = route.cold_backing.as_ref() else {
        return Ok(None);
    };
    if cold_backing.state != mooncake_store_core::ColdBackingState::PendingOffload {
        return Ok(None);
    }
    backend_load_pending_source(backend, cold_backing)
}
