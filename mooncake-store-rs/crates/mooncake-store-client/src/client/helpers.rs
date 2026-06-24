#[derive(Clone)]
struct ResolvedObject {
    tenant: String,
    key: String,
    route: ObjectRoute,
    replica: ReplicaRoute,
    fallback_replicas: VecDeque<ReplicaRoute>,
}

fn copy_into_region(allocation: RegionAllocation, value: &[u8]) {
    unsafe {
        ptr::copy_nonoverlapping(value.as_ptr(), allocation.addr.cast::<u8>(), value.len());
    }
}

fn record_success_metric(operation: &'static str, bytes_in: u64, bytes_out: u64) {
    let result: Result<()> = Ok(());
    OperationTracker::new(operation)
        .input_bytes(bytes_in)
        .finish(&result, bytes_out);
}

fn next_route_version(
    current: Option<&ObjectRoute>,
    route_ops: &RouteOperations,
    key: &ObjectKey,
) -> RouteVersion {
    route_ops.next_route_version(current, key)
}

fn next_route_versions(
    current: &[Option<ObjectRoute>],
    route_ops: &RouteOperations,
    keys: &[ObjectKey],
) -> Vec<RouteVersion> {
    route_ops.next_route_versions(current, keys)
}

fn payload_checksum(payload: &[u8]) -> u64 {
    // Keep checksum validation on the hot read/write path cheap enough for
    // large restore batches. The stored route field remains a stable u64.
    xxh3_64(payload)
}

#[allow(dead_code)]
fn backend_store_cold_payload_batch(
    backend: &dyn PersistentStorageBackend,
    writes: &[ColdObjectWrite<'_>],
) -> Vec<Result<mooncake_store_core::ColdBackingRoute>> {
    backend.put_objects_batch(writes)
}

fn backend_load_cold_payload_batch_into(
    backend: &dyn PersistentStorageBackend,
    reads: &mut [ColdObjectRead<'_, '_>],
) -> Vec<Result<Option<usize>>> {
    backend.get_objects_into_batch(reads)
}

#[allow(dead_code)]
fn backend_load_cold_payload_batch_pinned(
    backend: &dyn PersistentStorageBackend,
    reads: &[ColdObjectPinnedRead<'_>],
) -> Vec<Result<Option<ColdObjectPayload>>> {
    backend.get_objects_pinned_batch(reads)
}

fn cold_restore_flight_key(
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

fn validate_cold_restore_payload(resolved: &ResolvedObject, payload: &[u8]) -> Result<()> {
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
fn validate_resolved_payload_checksum(resolved: &ResolvedObject, payload: &[u8]) -> Result<()> {
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
        return validate_replica_checksum(&cold_backing_placeholder(&cold_backing), payload).map_err(|e| {
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

fn with_disjoint_restore_caller_buffers(
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

#[allow(dead_code)]
fn backend_remove_cold_payload(
    backend: &dyn PersistentStorageBackend,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
) -> Result<bool> {
    backend.delete_object(cold_backing)
}

#[allow(dead_code)]
fn backend_remove_cold_payload_batch(
    backend: &dyn PersistentStorageBackend,
    cold_backings: &[&mooncake_store_core::ColdBackingRoute],
) -> Vec<Result<bool>> {
    backend.delete_objects_batch(cold_backings)
}

#[allow(dead_code)]
fn backend_store_pending_source(
    backend: &dyn PersistentStorageBackend,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
    payload: &[u8],
) -> Result<()> {
    backend.put_pending_source(cold_backing, payload)
}

#[allow(dead_code)]
fn backend_load_pending_source(
    backend: &dyn PersistentStorageBackend,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
) -> Result<Option<Vec<u8>>> {
    backend.get_pending_source(cold_backing)
}

#[allow(dead_code)]
fn backend_remove_pending_source(
    backend: &dyn PersistentStorageBackend,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
) -> Result<bool> {
    backend.delete_pending_source(cold_backing)
}

#[allow(dead_code)]
fn cold_backing_placeholder(cold_backing: &mooncake_store_core::ColdBackingRoute) -> ReplicaRoute {
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

/// Resolve-stage target selection for cold-backed objects with multiple replicas.
///
/// Priority:
///   1. **Local target** — any target whose `cold_tier_id` has a local backend
///      gets absolute preference (avoids gRPC, staging pool, RDMA — direct local
///      SSD read).
///   2. **Remote targets** — if no local target exists, keep the primary unchanged.
///
/// Locality is determined by the `is_local` predicate (typically backed by
/// `ColdTierDeviceManager::has_local_backend`), which checks whether a local
/// storage backend is registered for the given cold_tier_id. This is
/// identity-independent — it works even when the owner's stable_id or epoch
/// has changed across restarts.
///
/// When a non-primary target is selected, rewrites `cold_backing` primary fields
/// (`owner`, `cold_tier_id`, `object_locator`) so that `cold_backing_placeholder()`
/// returns the correct owner for the local/remote partition in `execute_batch_get_into`.
fn select_cold_backing_target(
    cold_backing: &mut mooncake_store_core::ColdBackingRoute,
    is_local: impl Fn(&str) -> bool,
) {
    if cold_backing.replicas.is_empty() {
        return;
    }
    // Primary is already local — nothing to do.
    if is_local(&cold_backing.cold_tier_id) {
        return;
    }
    // Search replicas for a local target.
    let local_idx = cold_backing
        .replicas
        .iter()
        .position(|r| is_local(&r.cold_tier_id));
    let Some(idx) = local_idx else {
        return;
    };
    // Swap: promote the local replica to primary, demote the old primary to replicas.
    let local_replica = cold_backing.replicas.swap_remove(idx);
    let old_primary = mooncake_store_core::ColdBackingReplica {
        owner: std::mem::replace(&mut cold_backing.owner, local_replica.owner),
        cold_tier_id: std::mem::replace(&mut cold_backing.cold_tier_id, local_replica.cold_tier_id),
        object_locator: std::mem::replace(
            &mut cold_backing.object_locator,
            local_replica.object_locator,
        ),
    };
    cold_backing.replicas.push(old_primary);
}

#[allow(dead_code)]
fn same_cold_payload(
    left: &mooncake_store_core::ColdBackingRoute,
    right: &mooncake_store_core::ColdBackingRoute,
) -> bool {
    left.owner == right.owner
        && left.cold_tier_id == right.cold_tier_id
        && left.object_locator == right.object_locator
}

fn materialized_cold_backing(route: &ObjectRoute) -> Option<mooncake_store_core::ColdBackingRoute> {
    route
        .cold_backing
        .clone()
        .filter(|cold| cold.state == mooncake_store_core::ColdBackingState::Materialized)
}

#[allow(dead_code)]
fn read_local_hot_replica_payload(
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
        load_offload_source(backend, route)?.map(PendingOffloadPayload::Owned).ok_or_else(|| {
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

fn validate_replica_checksum(replica: &ReplicaRoute, payload: &[u8]) -> Result<()> {
    let Some(expected) = replica.checksum else {
        registry::record_checksum_validation("missing");
        return Ok(());
    };
    let actual = payload_checksum(payload);
    if actual == expected {
        registry::record_checksum_validation("ok");
        return Ok(());
    }
    registry::record_checksum_validation("mismatch");
    Err(StoreError::InvalidState(format!(
        "checksum mismatch for {}:{} expected={} actual={}",
        replica.owner, replica.segment_name.0, expected, actual
    )))
}

fn flatten_slices(buffers: &[&[u8]]) -> Vec<u8> {
    let total = buffers.iter().map(|buffer| buffer.len()).sum();
    let mut payload = Vec::with_capacity(total);
    for buffer in buffers {
        payload.extend_from_slice(buffer);
    }
    payload
}

fn scatter_into_buffers(payload: &[u8], buffers: &mut [&mut [u8]]) {
    let mut cursor = 0usize;
    for buffer in buffers {
        if cursor >= payload.len() {
            buffer.fill(0);
            continue;
        }
        let remaining = payload.len() - cursor;
        let to_copy = remaining.min(buffer.len());
        buffer[..to_copy].copy_from_slice(&payload[cursor..cursor + to_copy]);
        if to_copy < buffer.len() {
            buffer[to_copy..].fill(0);
        }
        cursor += to_copy;
    }
}

fn compatibility_matches(left: &ClientLease, right: &ClientLease) -> bool {
    left.compatibility.is_compatible_with(&right.compatibility)
}

fn should_mark_runtime_suspect_after_allocator_error(error: &StoreError) -> bool {
    matches!(error, StoreError::Transport(_) | StoreError::Unsupported(_))
}

fn control_bind_host(rpc_address: &str) -> String {
    if rpc_address.is_empty() {
        return "127.0.0.1".to_string();
    }
    rpc_address
        .rsplit_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty() && *host != "0.0.0.0" && *host != "::")
        .unwrap_or("127.0.0.1")
        .to_string()
}

fn align_up_u64(value: u64, alignment: u64) -> u64 {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should advance")
        .as_millis() as u64
}

impl StoreClient {
    fn route_ops(&self) -> RouteOperations {
        RouteOperations::new(self.route_directory.clone(), self.lease.clone())
    }

    pub fn wait_for_restore_promotions(&self) {
        crate::client::cold_tier::wait_for_restore_promotions(self);
    }

    pub fn local_memory_base_addr(&self) -> Result<*mut c_void> {
        self.ensure_local_memory()?;
        let segment = self.segment_name()?;
        let state = self.state.lock();
        state.memory_ref()?.storage_address(&segment, 0)
    }

    pub fn local_memory_registration_bytes(&self) -> u64 {
        let total = self
            .local_memory
            .storage_bytes
            .saturating_add(self.local_memory.scratch_bytes);
        u64::try_from(total).unwrap_or(u64::MAX)
    }
}

impl RouteHitReporter for StoreClient {
    fn report_route_hits(&self, routes: &[&ObjectRoute]) {
        self.report_route_hits_best_effort(routes.iter().copied());
    }
}
use xxhash_rust::xxh3::xxh3_64;
