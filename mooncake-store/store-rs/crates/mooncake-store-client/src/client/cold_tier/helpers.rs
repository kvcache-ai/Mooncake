use super::super::{
    validate_replica_checksum, ColdObjectPayload, ColdObjectPinnedRead, ColdObjectRead,
    ColdObjectWrite, ColdRestoreFlightKey, LocalAllocatorState, ObjectRoute, PendingOffloadPayload,
    PersistentStorageBackend, ReplicaRoute, ReplicaTier, ResolvedObject, Result, SegmentName,
    StoreError, StoreState,
};
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::warn;
use xxhash_rust::xxh3::xxh3_64;

pub(in super::super) fn payload_checksum(payload: &[u8]) -> u64 {
    // Keep checksum validation on the hot read/write path cheap enough for
    // large restore batches. The stored route field remains a stable u64.
    xxh3_64(payload)
}

#[allow(dead_code)]
pub(in super::super) fn backend_store_cold_payload_batch(
    backend: &dyn PersistentStorageBackend,
    writes: &[ColdObjectWrite<'_>],
) -> Vec<Result<mooncake_store_core::ColdBackingRoute>> {
    backend.put_objects_batch(writes)
}

pub(in super::super) fn backend_load_cold_payload_batch_into(
    backend: &dyn PersistentStorageBackend,
    reads: &mut [ColdObjectRead<'_, '_>],
) -> Vec<Result<Option<usize>>> {
    backend.get_objects_into_batch(reads)
}

#[allow(dead_code)]
pub(in super::super) fn backend_load_cold_payload_batch_pinned(
    backend: &dyn PersistentStorageBackend,
    reads: &[ColdObjectPinnedRead<'_>],
) -> Vec<Result<Option<ColdObjectPayload>>> {
    backend.get_objects_pinned_batch(reads)
}

pub(in super::super) fn cold_restore_flight_key(
    route: &ObjectRoute,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
) -> ColdRestoreFlightKey {
    ColdRestoreFlightKey {
        route_key: route.key.clone(),
        route_version: route.version,
        cold_tier_id: cold_backing.cold_tier_id.clone(),
        object_locator: cold_backing.object_locator.clone(),
        length: cold_backing.length,
        checksum: cold_backing.checksum,
    }
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

/// Like [`materialized_cold_backing`] but without cloning the backing record —
/// for hot-path gating that only needs to know whether materialized backing exists.
pub(in super::super) fn has_materialized_cold_backing(route: &ObjectRoute) -> bool {
    route
        .cold_backing
        .as_ref()
        .is_some_and(|cold| cold.state == mooncake_store_core::ColdBackingState::Materialized)
}

pub(in super::super) fn validate_cold_restore_payload(
    resolved: &ResolvedObject,
    payload: &[u8],
) -> Result<()> {
    if payload.len() != resolved.replica.length as usize {
        warn!(
            tenant = %resolved.tenant,
            key = %resolved.key,
            expected = resolved.replica.length,
            actual = payload.len(),
            "cold restore payload length mismatch"
        );
        return Err(StoreError::InvalidState(format!(
            "tenant={} key={} cold backing length mismatch: expected {} actual {}",
            resolved.tenant,
            resolved.key,
            resolved.replica.length,
            payload.len()
        )));
    }
    validate_replica_checksum(&resolved.replica, payload).map_err(|e| {
        warn!(
            tenant = %resolved.tenant,
            key = %resolved.key,
            error = %e,
            "cold restore payload checksum failed"
        );
        e
    })
}

#[allow(dead_code)]
pub(in super::super) fn validate_resolved_payload_checksum(
    resolved: &ResolvedObject,
    payload: &[u8],
) -> Result<()> {
    if let Some(cold_backing) = materialized_cold_backing(&resolved.route) {
        if payload.len() != cold_backing.length as usize {
            warn!(
                tenant = %resolved.tenant,
                key = %resolved.key,
                expected = cold_backing.length,
                actual = payload.len(),
                "resolved payload cold backing length mismatch"
            );
            return Err(StoreError::InvalidState(format!(
                "tenant={} key={} payload length mismatch: expected {} actual {}",
                resolved.tenant,
                resolved.key,
                cold_backing.length,
                payload.len()
            )));
        }
        return validate_replica_checksum(&cold_backing_placeholder(&cold_backing), payload)
            .map_err(|e| {
                warn!(
                    tenant = %resolved.tenant,
                    key = %resolved.key,
                    error = %e,
                    "resolved payload cold backing checksum failed"
                );
                e
            });
    }
    if payload.len() != resolved.replica.length as usize {
        warn!(
            tenant = %resolved.tenant,
            key = %resolved.key,
            expected = resolved.replica.length,
            actual = payload.len(),
            segment = %resolved.replica.segment_name.0,
            owner = %resolved.replica.owner,
            "resolved payload replica length mismatch"
        );
        return Err(StoreError::InvalidState(format!(
            "tenant={} key={} payload length mismatch: expected {} actual {}",
            resolved.tenant,
            resolved.key,
            resolved.replica.length,
            payload.len()
        )));
    }
    validate_replica_checksum(&resolved.replica, payload).map_err(|e| {
        warn!(
            tenant = %resolved.tenant,
            key = %resolved.key,
            segment = %resolved.replica.segment_name.0,
            owner = %resolved.replica.owner,
            error = %e,
            "resolved payload replica checksum failed"
        );
        e
    })
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

pub(in super::super) fn is_cold_backing_placeholder(replica: &ReplicaRoute) -> bool {
    replica.tier == ReplicaTier::File && replica.segment_name.0 == "__cold_backing__"
}

pub(in super::super) fn cold_only_read_placeholder(
    route: &ObjectRoute,
    tenant: &str,
    logical_key: &str,
) -> Result<ReplicaRoute> {
    let cold_backing = materialized_cold_backing(route).ok_or_else(|| {
        StoreError::NotFound(format!(
            "tenant={tenant} key={logical_key} has no readable replica owner"
        ))
    })?;
    Ok(cold_backing_placeholder(&cold_backing))
}

pub(in super::super) fn with_disjoint_restore_caller_buffers(
    buffers: &mut [&mut [u8]],
    read_requests: &[(usize, mooncake_store_core::ColdBackingRoute)],
    f: impl FnOnce(&mut [ColdObjectRead<'_, '_>]) -> Vec<Result<Option<usize>>>,
) -> Vec<(usize, Result<Option<usize>>)> {
    let indices = read_requests
        .iter()
        .map(|(index, _)| *index)
        .collect::<Vec<_>>();
    let cold_backings = read_requests
        .iter()
        .map(|(_, cold_backing)| cold_backing.clone())
        .collect::<Vec<_>>();
    let mut slots = buffers
        .iter_mut()
        .map(|buffer| Some(&mut **buffer))
        .collect::<Vec<_>>();
    let mut reads = indices
        .iter()
        .zip(cold_backings.iter())
        .map(|(index, cold_backing)| ColdObjectRead {
            cold_backing,
            dst: slots[*index]
                .take()
                .expect("each restore read index should be unique"),
        })
        .collect::<Vec<_>>();
    indices.into_iter().zip(f(&mut reads)).collect()
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
    let payload = copy_replica_payload_from_local_memory(state, replica).or_else(|_| {
        load_offload_source(backend, route)?
            .map(PendingOffloadPayload::new)
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

pub(in super::super) fn local_hot_replica_checksum(
    allocator: &Arc<Mutex<LocalAllocatorState>>,
    state: &Arc<Mutex<StoreState>>,
    route: &ObjectRoute,
    replica: &ReplicaRoute,
) -> Result<u64> {
    if let Some(announcement) = allocator.lock().announcement(&replica.segment_name) {
        if announcement.owner != replica.owner {
            return Err(StoreError::InvalidState(format!(
                "segment {} owner mismatch while checksumming {}",
                replica.segment_name.0, route.key.0
            )));
        }
    }
    let payload = copy_replica_payload_from_local_memory(state, replica)?;
    if payload.len() != replica.length as usize {
        return Err(StoreError::InvalidState(format!(
            "route {} local payload length mismatch while checksumming: expected {} actual {}",
            route.key.0,
            replica.length,
            payload.len()
        )));
    }
    Ok(payload_checksum(payload.as_slice()))
}

fn copy_replica_payload_from_local_memory(
    state: &Arc<Mutex<StoreState>>,
    replica: &ReplicaRoute,
) -> Result<PendingOffloadPayload> {
    let payload = {
        let state = state.lock();
        let addr = state
            .memory_ref()?
            .storage_address(&replica.segment_name, replica.segment_offset as usize)?;
        // Keep pending offload materialization independent from hot-slot reuse.
        // The route/allocator state may move on before the backend write runs.
        unsafe { std::slice::from_raw_parts(addr.cast::<u8>(), replica.length as usize).to_vec() }
    };
    Ok(PendingOffloadPayload::new(payload))
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
