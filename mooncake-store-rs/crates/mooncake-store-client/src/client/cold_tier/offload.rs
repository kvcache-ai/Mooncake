use super::super::{
    cold_tier_device_id, current_time_ms, refresh_cold_tier_device_cache, registry, CasResult,
    ColdObjectWrite, ColdTierCleanupGuard, ColdTierCleanupManager, ColdTierDeviceRecord,
    ColdTierOffloadManager, ColdTierOffloadMode, ColdTierOffloadPriorityConfig,
    ColdTierOffloadRunGuard, ColdTierPendingOffloadPolicy, FreeSchedulerState, ObjectKey,
    ObjectRoute, OperationTracker, PendingOffloadCasContext, PendingOffloadCasOutcome,
    PendingOffloadEntry, PendingOffloadKey, PendingOffloadMaterialization,
    PendingOffloadPrepareOutcome, PendingOffloadQueue, PendingOffloadQueueSnapshot,
    PendingOffloadReadyRoute, PersistentStorageBackend, Result, RouteCasRequest, RouteState,
    RouteVersion, StorageOwnerState, StoreError, DEFAULT_PENDING_OFFLOAD_RETRY_BASE_MS,
    DEFAULT_PENDING_OFFLOAD_RETRY_MAX_MS,
};
use super::{
    backend_remove_cold_payload, backend_remove_pending_source, backend_store_cold_payload_batch,
    backend_store_pending_source, local_hot_replica_checksum, materialized_cold_backing,
    pending_persistent_backing, persistent_backing_contains_target, persistent_backing_targets,
    read_local_hot_replica_payload, same_cold_payload,
};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(in super::super) fn enqueue_pending_offload(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
) {
    if super::cold_tier_disabled() {
        return;
    }
    let Some(cold_backing) = pending_persistent_backing(route) else {
        return;
    };
    if cold_backing.owner != storage_owner.runtime {
        return;
    }
    enqueue_pending_offload_with_length(storage_owner, route, cold_backing.length);
}

fn enqueue_pending_offload_with_length(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
    length: u64,
) {
    if storage_owner
        .pending_offloads
        .push(route.key.clone(), route.version, Some(length))
    {
        storage_owner.cold_tier_devices.pressure_increment_pending();
    }
}

pub(in super::super) fn publish_initial_write_cold_backing(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
) -> Result<Option<ObjectRoute>> {
    publish_initial_write_backing(storage_owner, route, true)
}

fn repair_initial_write_local_backing(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
) -> Result<Option<ObjectRoute>> {
    publish_initial_write_backing(storage_owner, route, false)
}

fn publish_initial_write_backing(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
    allow_nof: bool,
) -> Result<Option<ObjectRoute>> {
    if super::cold_tier_disabled() {
        return Ok(None);
    }
    if route.state != RouteState::Active || route.cold_backing.is_some() {
        storage_owner.sync_route(route);
        return Ok(None);
    }
    let Some(replica) = route
        .replicas
        .iter()
        .filter(|replica| replica.owner == storage_owner.runtime)
        .min_by_key(|replica| replica.priority)
    else {
        storage_owner.sync_route(route);
        return Ok(None);
    };
    let Some(checksum) = checksum_for_cold_backing(storage_owner, route, replica)? else {
        storage_owner.sync_route(route);
        return Ok(None);
    };
    let Some(backing) =
        storage_owner.backing_for_route(route, replica.length, checksum, allow_nof)?
    else {
        storage_owner.sync_route(route);
        return Ok(None);
    };
    if backing.is_request_local() {
        enqueue_pending_offload_with_length(storage_owner, route, backing.backing().length);
        storage_owner.sync_route(route);
        return Ok(None);
    }
    let mut next = route.clone();
    next.version = next.version.next();
    backing.publish_local(&mut next);
    let cas = storage_owner.route_ops.compare_and_swap_route(
        &route.key,
        Some(route.version),
        Some(&next),
    )?;
    if cas.applied {
        storage_owner.sync_route(&next);
        enqueue_pending_offload(storage_owner, &next);
        Ok(Some(next))
    } else {
        match cas.current.as_ref() {
            Some(current) => {
                storage_owner.sync_route(current);
                enqueue_pending_offload(storage_owner, current);
            }
            None => storage_owner.hot_replicas.remove_key(&route.key),
        }
        Ok(None)
    }
}

pub(in super::super) fn repair_initial_write_cold_backings(
    storage_owner: &StorageOwnerState,
    publish_limit: usize,
) -> Result<usize> {
    if super::cold_tier_disabled()
        || storage_owner.offload_mode != ColdTierOffloadMode::Passthrough
        || !storage_owner.cold_tier_devices.has_any_persistent_backend()
    {
        return Ok(0);
    }

    let mut attempted = 0usize;
    let mut published = 0usize;
    let publish_limit = publish_limit.max(1);
    let result = storage_owner.route_ops.visit_routes_by_replica_owner(
        &storage_owner.runtime,
        &mut |route| {
            if route.state != RouteState::Active || route.cold_backing.is_some() {
                storage_owner.sync_route(&route);
                return Ok(());
            }
            if attempted >= publish_limit {
                return Ok(());
            }
            attempted = attempted.saturating_add(1);
            match repair_initial_write_local_backing(storage_owner, &route) {
                Ok(Some(_)) => published = published.saturating_add(1),
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        runtime = %storage_owner.runtime,
                        key = %route.key.0,
                        error = %error,
                        "owner-side initial cold backing repair skipped route"
                    );
                }
            }
            Ok(())
        },
    );
    match result {
        Ok(()) | Err(StoreError::Unsupported(_)) => {}
        Err(error) => return Err(error),
    }
    Ok(published)
}

pub(in super::super) fn publish_pending_cold_backing_for_eviction(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
) -> Result<Option<ObjectRoute>> {
    if super::cold_tier_disabled() {
        return Ok(None);
    }
    if route.state != RouteState::Active || route.cold_backing.is_some() {
        storage_owner.sync_route(route);
        return Ok(None);
    }
    let Some(replica) = route
        .replicas
        .iter()
        .filter(|replica| replica.owner == storage_owner.runtime)
        .min_by_key(|replica| replica.priority)
    else {
        storage_owner.sync_route(route);
        return Ok(None);
    };
    let Some(checksum) = checksum_for_cold_backing(storage_owner, route, replica)? else {
        storage_owner.sync_route(route);
        return Ok(None);
    };
    let Some(backing) =
        storage_owner.pending_backing_for_route(route, replica.length, checksum, true)?
    else {
        storage_owner.sync_route(route);
        return Ok(None);
    };
    if backing.is_nof() {
        enqueue_pending_offload_with_length(storage_owner, route, backing.backing().length);
        storage_owner.sync_route(route);
        return Ok(Some(route.clone()));
    }
    let mut next = route.clone();
    next.version = next.version.next();
    backing.publish_local(&mut next);
    let cas = storage_owner.route_ops.compare_and_swap_route(
        &route.key,
        Some(route.version),
        Some(&next),
    )?;
    if cas.applied {
        storage_owner.sync_route(&next);
        enqueue_pending_offload(storage_owner, &next);
        Ok(Some(next))
    } else {
        match cas.current.as_ref() {
            Some(current) => storage_owner.sync_route(current),
            None => storage_owner.hot_replicas.remove_key(&route.key),
        }
        Ok(None)
    }
}

fn checksum_for_cold_backing(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
    replica: &mooncake_store_core::ReplicaRoute,
) -> Result<Option<u64>> {
    if let Some(checksum) = replica.checksum {
        return Ok(Some(checksum));
    }
    match local_hot_replica_checksum(
        &storage_owner.allocator,
        &storage_owner.state,
        route,
        replica,
    ) {
        Ok(checksum) => Ok(Some(checksum)),
        Err(error) => {
            tracing::warn!(
                runtime = %storage_owner.runtime,
                key = %route.key.0,
                route_version = route.version.0,
                segment = %replica.segment_name.0,
                error = %error,
                "cold_tier_checksum_from_owner_hot_replica_failed"
            );
            Ok(None)
        }
    }
}

pub(in super::super) fn rebuild_pending_offload_queue(
    storage_owner: &StorageOwnerState,
) -> Result<usize> {
    if super::cold_tier_disabled() {
        return Ok(0);
    }
    rebuild_pending_offload_queue_with(
        storage_owner,
        storage_owner.offload_priority,
        &storage_owner.pending_offloads,
    )
}

pub(in super::super) fn materialize_pending_offloads_bounded(
    storage_owner: &StorageOwnerState,
    max_tasks: usize,
) -> Result<usize> {
    if super::cold_tier_disabled() {
        return Ok(0);
    }
    let max_tasks = max_tasks.max(1);
    let tracker = OperationTracker::new("storage_owner_background_offload");
    let result = (|| {
        let Some(_run_guard) = storage_owner.pending_offloads.try_begin_materialization() else {
            registry::record_cold_tier_operation("offload", "admission_reject", "concurrent");
            return Ok(0);
        };
        ensure_pending_offload_work_available(storage_owner)?;
        drain_pending_offload_round(storage_owner, max_tasks)
    })();
    tracker.finish(&result, result.as_ref().copied().unwrap_or_default() as u64);
    match &result {
        Ok(materialized) => {
            if *materialized > 0 {
                tracing::info!(
                    runtime = %storage_owner.runtime,
                    max_tasks,
                    materialized = *materialized,
                    "cold tier pending offloads materialized"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                runtime = %storage_owner.runtime,
                max_tasks,
                error = %error,
                "cold tier pending offload materialization failed"
            );
        }
    }
    storage_owner.record_cold_tier_observability_snapshots();
    result
}

fn ensure_pending_offload_work_available(storage_owner: &StorageOwnerState) -> Result<()> {
    if !storage_owner.pending_offloads.is_empty() {
        return Ok(());
    }
    rebuild_pending_offload_queue(storage_owner)?;
    Ok(())
}

fn materialize_ready_pending_offload_groups(
    storage_owner: &StorageOwnerState,
    pending: &mut [PendingOffloadMaterialization],
) -> Result<usize> {
    pending.sort_by(|left, right| {
        left.cold_backing
            .cold_tier_id
            .cmp(&right.cold_backing.cold_tier_id)
    });
    let mut round_materialized = 0usize;
    let mut first_error = None;
    let mut start = 0usize;
    while start < pending.len() {
        let cold_tier_id = pending[start].cold_backing.cold_tier_id.clone();
        let mut end = start + 1;
        while end < pending.len() && pending[end].cold_backing.cold_tier_id == cold_tier_id {
            end += 1;
        }
        round_materialized = round_materialized.saturating_add(
            storage_owner.materialize_prepared_pending_offload_batch(
                &pending[start..end],
                &mut first_error,
            )?,
        );
        start = end;
    }
    if round_materialized == 0 {
        if let Some(error) = first_error {
            return Err(error);
        }
    }
    Ok(round_materialized)
}

fn drain_pending_offload_round(
    storage_owner: &StorageOwnerState,
    max_tasks: usize,
) -> Result<usize> {
    let mut refresh_replays_remaining = max_tasks;
    loop {
        let pop_tracker = OperationTracker::new("cold_offload_pop_ready");
        let entries = pop_ready_pending_offload_entries(storage_owner, max_tasks);
        pop_tracker.finish_with_result("ok", entries.len() as u64);
        if entries.is_empty() {
            return Ok(0);
        }
        let prepare_tracker = OperationTracker::new("cold_offload_prepare_batch");
        let pending = storage_owner.prepare_pending_offload_entries(entries);
        prepare_tracker.finish(
            &pending,
            pending
                .as_ref()
                .map(|pending| pending.len())
                .unwrap_or_default() as u64,
        );
        let mut pending = pending?;
        if pending.is_empty() {
            let snapshot = storage_owner
                .pending_offloads
                .observability_snapshot(Instant::now());
            if snapshot.ready > 0 && refresh_replays_remaining > 0 {
                refresh_replays_remaining = refresh_replays_remaining.saturating_sub(1);
                continue;
            }
            return Ok(0);
        }
        return materialize_ready_pending_offload_groups(storage_owner, &mut pending);
    }
}

fn pop_ready_pending_offload_entries(
    storage_owner: &StorageOwnerState,
    limit: usize,
) -> Vec<PendingOffloadEntry> {
    let mut entries = Vec::new();
    while entries.len() < limit {
        let Some(entry) = storage_owner.pending_offloads.pop_ready(Instant::now()) else {
            break;
        };
        entries.push(entry);
    }
    entries
}

pub(in super::super) fn prepare_pending_offload_entries(
    storage_owner: &StorageOwnerState,
    entries: Vec<PendingOffloadEntry>,
) -> Result<Vec<PendingOffloadMaterialization>> {
    if super::cold_tier_disabled() {
        for entry in entries {
            storage_owner.pending_offloads.retry(entry);
        }
        return Ok(Vec::new());
    }
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let routes = storage_owner.route_ops.load_routes(
        &entries
            .iter()
            .map(|entry| entry.key.route_key.clone())
            .collect::<Vec<_>>(),
    )?;
    let devices = if storage_owner.cold_tier_devices.has_any_local_backend() {
        let devices = match storage_owner.cold_tier_devices.devices.lock().snapshot() {
            Some(devices) => devices,
            None => refresh_cold_tier_device_cache(
                storage_owner.metadata.as_ref(),
                &storage_owner.cold_tier_devices.devices,
                "cold_tier_device_snapshot_offload_prepare",
            )?,
        };
        devices
            .into_iter()
            .map(|device| (device.device_id.clone(), device))
            .collect::<BTreeMap<_, _>>()
    } else {
        BTreeMap::new()
    };
    let mut prepared = Vec::new();
    for index in 0..entries.len() {
        let entry = entries[index].clone();
        let key = entry.key.clone();
        let route = routes.get(index).cloned().unwrap_or(None);
        match prepare_pending_offload_entry(storage_owner, entry.clone(), route.clone(), &devices) {
            Ok(PendingOffloadPrepareOutcome::Ready(materialization)) => {
                prepared.push(*materialization)
            }
            Ok(PendingOffloadPrepareOutcome::Retry(entry)) => {
                registry::record_cold_tier_operation("offload", "retry", "none");
                storage_owner.pending_offloads.retry(entry);
            }
            Ok(PendingOffloadPrepareOutcome::RetryAfter(entry, delay)) => {
                registry::record_cold_tier_operation("offload", "retry", "admission");
                storage_owner.pending_offloads.retry_after(entry, delay);
            }
            Ok(PendingOffloadPrepareOutcome::Refreshed(entry)) => {
                registry::record_cold_tier_operation("offload", "refreshed", "stale");
                storage_owner
                    .pending_offloads
                    .retry_after(entry, Duration::ZERO);
            }
            Ok(PendingOffloadPrepareOutcome::Skipped) => {
                registry::record_cold_tier_operation("offload", "skipped", "none");
                storage_owner.pending_offloads.complete(&key);
            }
            Err(error) => {
                storage_owner.pending_offloads.retry(entry);
                for materialization in &prepared {
                    storage_owner
                        .pending_offloads
                        .retry(materialization.entry.clone());
                }
                for remaining in &entries[index + 1..] {
                    storage_owner.pending_offloads.retry(remaining.clone());
                }
                return Err(error);
            }
        }
    }
    Ok(prepared)
}

pub(in super::super) fn prepare_pending_offload_entry(
    storage_owner: &StorageOwnerState,
    entry: PendingOffloadEntry,
    route: Option<ObjectRoute>,
    devices: &BTreeMap<String, ColdTierDeviceRecord>,
) -> Result<PendingOffloadPrepareOutcome> {
    if super::cold_tier_disabled() {
        return Ok(PendingOffloadPrepareOutcome::Retry(entry));
    }
    let Some(route) = route else {
        storage_owner.hot_replicas.remove_key(&entry.key.route_key);
        return Ok(PendingOffloadPrepareOutcome::Skipped);
    };
    let refreshed_pending_entry = route
        .replicas
        .iter()
        .filter(|replica| replica.owner == storage_owner.runtime)
        .min_by_key(|replica| replica.priority)
        .map(|replica| PendingOffloadEntry {
            key: entry.key.clone(),
            route_version: route.version,
            attempts: entry.attempts,
            not_before: Instant::now(),
            enqueued_at: entry.enqueued_at,
            length_bytes: Some(replica.length),
        });
    if route.state != RouteState::Active {
        if let Some(refreshed_entry) = refreshed_pending_entry.clone() {
            storage_owner.sync_route(&route);
            return Ok(PendingOffloadPrepareOutcome::Refreshed(refreshed_entry));
        }
        storage_owner.sync_route(&route);
        return Ok(PendingOffloadPrepareOutcome::Skipped);
    }
    if route.version != entry.route_version {
        if let Some(refreshed_entry) = refreshed_pending_entry {
            storage_owner.sync_route(&route);
            return Ok(PendingOffloadPrepareOutcome::Refreshed(refreshed_entry));
        }
        storage_owner.sync_route(&route);
        return Ok(PendingOffloadPrepareOutcome::Skipped);
    }
    let Some(replica) = route
        .replicas
        .iter()
        .filter(|replica| replica.owner == storage_owner.runtime)
        .min_by_key(|replica| replica.priority)
        .cloned()
    else {
        return Ok(PendingOffloadPrepareOutcome::Retry(entry));
    };
    let managed_nof = route.nof_backing.is_some();
    let (cold_backing, transient_nof) = match pending_persistent_backing(&route) {
        Some(backing) => (backing, false),
        None => {
            let Some(checksum) = checksum_for_cold_backing(storage_owner, &route, &replica)? else {
                storage_owner.sync_route(&route);
                return Ok(PendingOffloadPrepareOutcome::Skipped);
            };
            let Some(backing) = storage_owner
                .cold_tier_devices
                .nof_targets
                .pending_backing(&route, replica.length, checksum)?
            else {
                storage_owner.sync_route(&route);
                return Ok(PendingOffloadPrepareOutcome::Skipped);
            };
            (backing, true)
        }
    };
    if !transient_nof && cold_backing.owner != storage_owner.runtime {
        storage_owner.sync_route(&route);
        return Ok(PendingOffloadPrepareOutcome::Skipped);
    }
    let device_id = cold_tier_device_id(&cold_backing);
    let device = if transient_nof || managed_nof {
        None
    } else {
        let Some(device) = devices.get(device_id).cloned() else {
            clear_unavailable_pending_cold_backing(
                storage_owner,
                &route,
                &cold_backing,
                transient_nof,
                "missing_device",
                None,
            )?;
            return Ok(PendingOffloadPrepareOutcome::Skipped);
        };
        if storage_owner.cold_tier_device_crosses_critical(&device, cold_backing.length) {
            let mut update = mooncake_store_core::ColdTierDeviceUpdate::new(current_time_ms());
            update.expected_updated_at_ms = Some(device.updated_at_ms);
            update.state = Some(mooncake_store_core::ColdTierDeviceState::Full);
            let updated = storage_owner
                .metadata
                .as_ref()
                .update_cold_tier_device(device_id, update)?;
            storage_owner.cold_tier_devices.upsert(updated);
            return Ok(PendingOffloadPrepareOutcome::Retry(entry));
        }
        if !storage_owner
            .cold_tier_devices
            .accepts_offload(&device, cold_backing.length)
        {
            return Ok(PendingOffloadPrepareOutcome::Retry(entry));
        }
        Some(device)
    };
    let permit = match storage_owner
        .cold_tier_devices
        .try_acquire_offload(device_id)
    {
        Ok(permit) => Some(permit),
        Err(reason) => {
            registry::record_cold_tier_operation(
                "offload",
                "admission_reject",
                reason.metric_label(),
            );
            return Ok(PendingOffloadPrepareOutcome::RetryAfter(
                entry,
                Duration::from_millis(100),
            ));
        }
    };
    let backend = match storage_owner.cold_tier_devices.backend_for(&cold_backing) {
        Ok(backend) => backend,
        Err(error) => {
            clear_unavailable_pending_cold_backing(
                storage_owner,
                &route,
                &cold_backing,
                transient_nof,
                "missing_backend",
                Some(&error),
            )?;
            return Ok(PendingOffloadPrepareOutcome::Skipped);
        }
    };
    let payload = read_local_hot_replica_payload(
        &storage_owner.allocator,
        &storage_owner.state,
        backend.as_ref(),
        &route,
        &replica,
    )?;
    Ok(PendingOffloadPrepareOutcome::Ready(Box::new(
        PendingOffloadMaterialization {
            entry,
            route,
            cold_backing,
            transient_nof,
            managed_nof,
            payload,
            device,
            permit,
        },
    )))
}

pub(in super::super) fn clear_unavailable_pending_cold_backing(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
    transient_nof: bool,
    reason: &'static str,
    error: Option<&StoreError>,
) -> Result<()> {
    if transient_nof {
        tracing::warn!(
            runtime = %storage_owner.runtime,
            key = %route.key.0,
            route_version = route.version.0,
            target_id = %cold_backing.cold_tier_id,
            reason,
            error = %error.map(|e| e.to_string()).unwrap_or_default(),
            "request-local NoF offload target is unavailable"
        );
        storage_owner.sync_route(route);
        return Ok(());
    }
    let mut next = route.clone();
    next.version = next.version.next();
    if route.nof_backing.is_some() {
        next.nof_backing = None;
    } else {
        next.cold_backing = None;
    }
    let cas = storage_owner.route_ops.compare_and_swap_route(
        &route.key,
        Some(route.version),
        Some(&next),
    )?;
    if cas.applied {
        tracing::warn!(
            runtime = %storage_owner.runtime,
            key = %route.key.0,
            route_version = route.version.0,
            cold_tier_id = %cold_backing.cold_tier_id,
            object_locator = %cold_backing.object_locator,
            reason,
            error = %error.map(|e| e.to_string()).unwrap_or_default(),
            "cold tier pending offload unavailable; falling back to hot-only route"
        );
        storage_owner.sync_route(&next);
    } else if let Some(current) = cas.current.as_ref() {
        storage_owner.sync_route(current);
    } else {
        storage_owner.hot_replicas.remove_key(&route.key);
    }
    Ok(())
}

pub(in super::super) fn materialize_prepared_pending_offload_batch(
    storage_owner: &StorageOwnerState,
    pending: &[PendingOffloadMaterialization],
    first_error: &mut Option<StoreError>,
) -> Result<usize> {
    if super::cold_tier_disabled() {
        retry_pending_offload_batch(storage_owner, pending);
        return Ok(0);
    }
    if pending.is_empty() {
        return Ok(0);
    }
    if pending[0].transient_nof {
        return materialize_transient_nof_batch(storage_owner, pending, first_error);
    }
    let backend = match storage_owner
        .cold_tier_devices
        .backend_for(&pending[0].cold_backing)
    {
        Ok(backend) => backend,
        Err(error) => {
            retry_pending_offload_batch(storage_owner, pending);
            return Err(error);
        }
    };
    if let Err(error) = reserve_pending_offload_batch(storage_owner, pending) {
        retry_pending_offload_batch(storage_owner, pending);
        return Err(error);
    }
    if let Err(error) = store_pending_offload_sources(storage_owner, backend.as_ref(), pending) {
        retry_pending_offload_batch(storage_owner, pending);
        settle_pending_offload_batch(
            storage_owner,
            pending,
            0,
            sum_pending_offload_payload_bytes(pending)?,
        )?;
        return Err(error);
    }
    let (mut ready, reserved_release_bytes) =
        match write_pending_offload_payloads(storage_owner, backend.as_ref(), pending, first_error)
        {
            Ok(result) => result,
            Err(error) => {
                retry_pending_offload_batch(storage_owner, pending);
                settle_pending_offload_batch(
                    storage_owner,
                    pending,
                    0,
                    sum_pending_offload_payload_bytes(pending)?,
                )?;
                return Err(error);
            }
        };
    if ready.is_empty() {
        settle_pending_offload_batch(storage_owner, pending, 0, reserved_release_bytes)?;
        return Ok(0);
    }
    // Write to replica backends (best-effort — failures are logged but don't
    // block the primary offload).
    write_pending_offload_replicas(storage_owner, pending, &mut ready);
    let cas_tracker = OperationTracker::new("cold_offload_route_cas_batch");
    let cas_results = match cas_pending_offload_routes(storage_owner, pending, &ready) {
        Ok(results) => {
            cas_tracker.finish_with_result("ok", results.len() as u64);
            results
        }
        Err(error) => {
            cas_tracker.finish_with_result("error", 0);
            let reserved_release_bytes = rollback_ready_pending_offloads(
                storage_owner,
                backend.as_ref(),
                pending,
                &ready,
                reserved_release_bytes,
            )?;
            settle_pending_offload_batch(storage_owner, pending, 0, reserved_release_bytes)?;
            return Err(error);
        }
    };
    let outcome = apply_pending_offload_cas_results(
        storage_owner,
        backend.as_ref(),
        pending,
        &ready,
        cas_results,
        reserved_release_bytes,
        first_error,
    )?;
    for backing in &outcome.cleanup {
        remove_unpublished_persistent_backing(storage_owner, backing);
    }
    if let Err(error) = settle_pending_offload_batch(
        storage_owner,
        pending,
        outcome.used_add_bytes,
        outcome.reserved_release_bytes,
    ) {
        complete_pending_offload_batch(storage_owner, pending);
        return Err(error);
    }
    // Signal cold restore waiters that new Materialized entries are available for
    // zero-I/O eviction. This is the "proxy execution" notification: offload completed
    // its SSD write, now cold restore can evict the clean entry instantly.
    if outcome.materialized > 0 {
        storage_owner.eviction_ready_signal.signal();
        // Reduce pressure: offload completed, backlog shrinks → restore can ramp up.
        for _ in 0..outcome.materialized {
            storage_owner.cold_tier_devices.pressure_decrement_pending();
        }
    }
    Ok(outcome.materialized)
}

fn materialize_transient_nof_batch(
    storage_owner: &StorageOwnerState,
    pending: &[PendingOffloadMaterialization],
    first_error: &mut Option<StoreError>,
) -> Result<usize> {
    if !pending.iter().all(|entry| entry.transient_nof) {
        retry_pending_offload_batch(storage_owner, pending);
        return Err(StoreError::InvalidState(
            "NoF and local Cold Tier writes cannot share one backend batch".to_string(),
        ));
    }
    let mut target_writes =
        BTreeMap::<String, Vec<(usize, mooncake_store_core::ColdBackingRoute)>>::new();
    for (index, prepared) in pending.iter().enumerate() {
        for target in persistent_backing_targets(&prepared.cold_backing) {
            target_writes
                .entry(target.cold_tier_id.clone())
                .or_default()
                .push((index, target));
        }
    }
    let mut errors = (0..pending.len()).map(|_| None).collect::<Vec<_>>();
    for writes in target_writes.into_values() {
        let backend = storage_owner.cold_tier_devices.backend_for(&writes[0].1);
        let results = match backend {
            Ok(backend) => {
                let requests = writes
                    .iter()
                    .map(|(index, backing)| ColdObjectWrite {
                        route: Some(&pending[*index].route),
                        cold_backing: backing,
                        payload: pending[*index].payload.as_slice(),
                    })
                    .collect::<Vec<_>>();
                backend_store_cold_payload_batch(backend.as_ref(), &requests)
            }
            Err(error) => std::iter::repeat_with(|| Err(error.clone()))
                .take(writes.len())
                .collect(),
        };
        if results.len() != writes.len() {
            let error = StoreError::InvalidState(format!(
                "NoF backend returned {} results for {} writes",
                results.len(),
                writes.len()
            ));
            for (index, _) in writes {
                errors[index].get_or_insert_with(|| error.clone());
            }
            continue;
        }
        for ((index, _), result) in writes.into_iter().zip(results) {
            if let Err(error) = result {
                errors[index].get_or_insert(error);
            }
        }
    }
    let mut completed = 0usize;
    for (prepared, error) in pending.iter().zip(errors) {
        match error {
            None => {
                if let Some(permit) = prepared.permit.as_ref() {
                    permit.complete_ok();
                }
                storage_owner.pending_offloads.complete(&prepared.entry.key);
                storage_owner.cold_tier_devices.pressure_decrement_pending();
                completed = completed.saturating_add(1);
            }
            Some(error) => {
                if let Some(permit) = prepared.permit.as_ref() {
                    permit.complete_error();
                }
                if first_error.is_none() {
                    *first_error = Some(error);
                }
                storage_owner.pending_offloads.retry(prepared.entry.clone());
            }
        }
    }
    if completed > 0 {
        storage_owner.eviction_ready_signal.signal();
    }
    Ok(completed)
}

fn retry_pending_offload_batch(
    storage_owner: &StorageOwnerState,
    pending: &[PendingOffloadMaterialization],
) {
    for prepared in pending {
        storage_owner.pending_offloads.retry(prepared.entry.clone());
    }
}

fn complete_pending_offload_batch(
    storage_owner: &StorageOwnerState,
    pending: &[PendingOffloadMaterialization],
) {
    for prepared in pending {
        storage_owner.pending_offloads.complete(&prepared.entry.key);
    }
}

fn store_pending_offload_sources(
    _storage_owner: &StorageOwnerState,
    backend: &dyn PersistentStorageBackend,
    pending: &[PendingOffloadMaterialization],
) -> Result<()> {
    if backend.disable_pending_source() {
        return Ok(());
    }
    for prepared in pending {
        match backend_store_pending_source(
            backend,
            &prepared.cold_backing,
            prepared.payload.as_slice(),
        ) {
            Ok(()) => {}
            Err(error) if is_stale_pending_cold_backing_error(&error) => {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn write_pending_offload_payloads(
    storage_owner: &StorageOwnerState,
    backend: &dyn PersistentStorageBackend,
    pending: &[PendingOffloadMaterialization],
    first_error: &mut Option<StoreError>,
) -> Result<(Vec<PendingOffloadReadyRoute>, i64)> {
    let writes = pending
        .iter()
        .map(|prepared| ColdObjectWrite {
            route: Some(&prepared.route),
            cold_backing: &prepared.cold_backing,
            payload: prepared.payload.as_slice(),
        })
        .collect::<Vec<_>>();
    let write_tracker = OperationTracker::new("cold_offload_backend_write_batch").input_bytes(
        pending
            .iter()
            .map(|prepared| prepared.payload.len() as u64)
            .sum::<u64>(),
    );
    let write_results = backend_store_cold_payload_batch(backend, &writes);
    let write_result_summary = if write_results.iter().all(|result| result.is_ok()) {
        "ok"
    } else {
        "error"
    };
    write_tracker.finish_with_result(write_result_summary, write_results.len() as u64);
    let mut reserved_release_bytes = 0i64;
    let mut ready = Vec::new();
    for (index, (prepared, write_result)) in pending.iter().zip(write_results).enumerate() {
        match write_result {
            Ok(mut materialized_cold_backing) => {
                if let Some(permit) = prepared.permit.as_ref() {
                    permit.complete_ok();
                }
                materialized_cold_backing.replicas.clear();
                let mut next_route = prepared.route.clone();
                next_route.version = next_route.version.next();
                publish_materialized_backing(
                    &mut next_route,
                    &materialized_cold_backing,
                    prepared.transient_nof,
                    prepared.managed_nof,
                );
                ready.push(PendingOffloadReadyRoute {
                    index,
                    materialized_cold_backing,
                    next_route,
                });
            }
            Err(error) => {
                if let Some(permit) = prepared.permit.as_ref() {
                    permit.complete_error();
                }
                reserved_release_bytes = record_failed_pending_offload_write(
                    storage_owner,
                    prepared,
                    error,
                    reserved_release_bytes,
                    first_error,
                )?;
            }
        }
    }
    Ok((ready, reserved_release_bytes))
}

/// Best-effort write of payloads to replica backends.  Failures are logged
/// but do not affect the primary offload outcome — a missing replica simply
/// means fewer read targets for that object until the next offload cycle.
fn write_pending_offload_replicas(
    storage_owner: &StorageOwnerState,
    pending: &[PendingOffloadMaterialization],
    ready: &mut [PendingOffloadReadyRoute],
) {
    for ready_route in ready {
        let prepared = &pending[ready_route.index];
        if prepared.cold_backing.replicas.is_empty() {
            continue;
        }
        let mut materialized_replicas = Vec::new();
        for replica in &prepared.cold_backing.replicas {
            // Build a temporary ColdBackingRoute pointing to the replica's
            // owner + device so that backend_for() resolves the correct backend.
            let replica_backing = mooncake_store_core::ColdBackingRoute {
                owner: replica.owner.clone(),
                cold_tier_id: replica.cold_tier_id.clone(),
                object_locator: replica.object_locator.clone(),
                ..prepared.cold_backing.clone()
            };
            let replica_backend = match storage_owner
                .cold_tier_devices
                .backend_for(&replica_backing)
            {
                Ok(backend) => backend,
                Err(error) => {
                    tracing::warn!(
                        cold_tier_id = %replica.cold_tier_id,
                        error = %error,
                        key = %prepared.route.key.0,
                        "cold_offload_replica_backend_not_found"
                    );
                    continue;
                }
            };
            let writes = [ColdObjectWrite {
                route: Some(&prepared.route),
                cold_backing: &replica_backing,
                payload: prepared.payload.as_slice(),
            }];
            let results = backend_store_cold_payload_batch(replica_backend.as_ref(), &writes);
            for result in results {
                match result {
                    Ok(materialized) => {
                        materialized_replicas.push(mooncake_store_core::ColdBackingReplica {
                            owner: materialized.owner,
                            cold_tier_id: materialized.cold_tier_id,
                            object_locator: materialized.object_locator,
                        })
                    }
                    Err(error) => {
                        tracing::warn!(
                            cold_tier_id = %replica.cold_tier_id,
                            error = %error,
                            key = %prepared.route.key.0,
                            "cold_offload_replica_write_failed"
                        );
                    }
                }
            }
        }
        ready_route.materialized_cold_backing.replicas = materialized_replicas;
        publish_materialized_backing(
            &mut ready_route.next_route,
            &ready_route.materialized_cold_backing,
            prepared.transient_nof,
            prepared.managed_nof,
        );
    }
}

fn publish_materialized_backing(
    route: &mut ObjectRoute,
    backing: &mooncake_store_core::ColdBackingRoute,
    transient_nof: bool,
    managed_nof: bool,
) {
    debug_assert!(!transient_nof || !managed_nof);
    if managed_nof {
        route.cold_backing = None;
        route.nof_backing = Some(mooncake_store_core::NofBackingRoute::from_cold(backing));
    } else if !transient_nof {
        route.cold_backing = Some(backing.clone());
    }
}

fn cas_pending_offload_routes(
    storage_owner: &StorageOwnerState,
    pending: &[PendingOffloadMaterialization],
    ready: &[PendingOffloadReadyRoute],
) -> Result<Vec<Result<CasResult>>> {
    let cas_requests = ready
        .iter()
        .map(|ready| RouteCasRequest {
            key: pending[ready.index].route.key.clone(),
            expected: Some(pending[ready.index].route.version),
            next: Some(ready.next_route.clone()),
        })
        .collect::<Vec<_>>();
    storage_owner.route_ops.publish_routes(&cas_requests)
}

fn rollback_ready_pending_offloads(
    storage_owner: &StorageOwnerState,
    backend: &dyn PersistentStorageBackend,
    pending: &[PendingOffloadMaterialization],
    ready: &[PendingOffloadReadyRoute],
    mut reserved_release_bytes: i64,
) -> Result<i64> {
    for ready in ready {
        // A failed batch call has an unknown publication outcome. Provider-managed NoF
        // locators are idempotent and have no Mooncake capacity reservation, so retain them
        // for route confirmation/retry rather than risking an applied route pointing at data
        // we just deleted. Local Cold Tier keeps its established reservation rollback path.
        if !pending[ready.index].transient_nof {
            remove_unpublished_persistent_backing(storage_owner, &ready.materialized_cold_backing);
        }
        reserved_release_bytes = checked_add_cold_tier_bytes(
            reserved_release_bytes,
            pending_offload_payload_len_i64(&pending[ready.index])?,
            "cold tier reserved release exceeds accounting range",
        )?;
        let _ = backend_remove_pending_source(backend, &pending[ready.index].cold_backing);
        storage_owner
            .pending_offloads
            .retry(pending[ready.index].entry.clone());
    }
    Ok(reserved_release_bytes)
}

fn apply_pending_offload_cas_results(
    storage_owner: &StorageOwnerState,
    backend: &dyn PersistentStorageBackend,
    pending: &[PendingOffloadMaterialization],
    ready: &[PendingOffloadReadyRoute],
    cas_results: Vec<Result<CasResult>>,
    mut reserved_release_bytes: i64,
    first_error: &mut Option<StoreError>,
) -> Result<PendingOffloadCasOutcome> {
    let mut outcome = PendingOffloadCasOutcome {
        materialized: 0,
        used_add_bytes: 0,
        reserved_release_bytes,
        cleanup: Vec::new(),
    };
    let mut context = PendingOffloadCasContext {
        backend,
        pending,
        first_error,
        outcome: &mut outcome,
    };
    for (ready, cas_result) in ready.iter().zip(cas_results) {
        reserved_release_bytes = apply_pending_offload_cas_result(
            storage_owner,
            &mut context,
            ready,
            cas_result,
            reserved_release_bytes,
        )?;
    }
    outcome.reserved_release_bytes = reserved_release_bytes;
    Ok(outcome)
}

fn apply_pending_offload_cas_result(
    storage_owner: &StorageOwnerState,
    context: &mut PendingOffloadCasContext<'_>,
    ready: &PendingOffloadReadyRoute,
    cas_result: Result<CasResult>,
    reserved_release_bytes: i64,
) -> Result<i64> {
    let pending = &context.pending[ready.index];
    let length_bytes = pending_offload_payload_len_i64(pending)?;
    let reserved_release_bytes = checked_add_cold_tier_bytes(
        reserved_release_bytes,
        length_bytes,
        "cold tier reserved release exceeds accounting range",
    )?;
    let _ = backend_remove_pending_source(context.backend, &pending.cold_backing);
    match cas_result {
        Ok(cas) if cas.applied => {
            context.outcome.used_add_bytes = checked_add_cold_tier_bytes(
                context.outcome.used_add_bytes,
                length_bytes,
                "cold tier used-byte accounting exceeds range",
            )?;
            context.outcome.materialized = context.outcome.materialized.saturating_add(1);
            registry::record_cold_tier_operation("offload", "materialized", "none");
            storage_owner.sync_route(&ready.next_route);
            storage_owner.pending_offloads.complete(&pending.entry.key);
        }
        Ok(cas) => {
            let current_backing = cas.current.as_ref().and_then(materialized_cold_backing);
            if let Some(current) = current_backing
                .as_ref()
                .filter(|current| same_cold_payload(current, &ready.materialized_cold_backing))
            {
                context.outcome.cleanup.extend(
                    persistent_backing_targets(&ready.materialized_cold_backing)
                        .into_iter()
                        .filter(|target| !persistent_backing_contains_target(current, target)),
                );
                tracing::warn!(
                    route_key = %pending.route.key.0,
                    cold_tier_id = %ready.materialized_cold_backing.cold_tier_id,
                    object_locator = %ready.materialized_cold_backing.object_locator,
                    current_state = ?current_backing.as_ref().map(|current| current.state),
                    expected_version = pending.route.version.0,
                    current_version = ?cas.current.as_ref().map(|c| c.version.0),
                    "offload CAS conflict kept payload because current route already references it"
                );
            } else {
                context
                    .outcome
                    .cleanup
                    .push(ready.materialized_cold_backing.clone());
            }
            registry::record_cold_tier_operation("offload", "skipped", "route_conflict");
            if let Some(current) = cas.current.as_ref() {
                storage_owner.sync_route(current);
            }
            storage_owner.pending_offloads.complete(&pending.entry.key);
        }
        Err(error) => {
            // Per-item errors can also represent a lost CAS reply. See the batch-error path
            // above for why provider-managed NoF payloads are retained until retry confirms the
            // route; definite conflicts are handled by the preceding branch and cleaned up.
            if !pending.transient_nof {
                context
                    .outcome
                    .cleanup
                    .push(ready.materialized_cold_backing.clone());
            }
            registry::record_cold_tier_operation(
                "offload",
                "error",
                registry::cold_tier_error_kind(&error),
            );
            if context.first_error.is_none() {
                *context.first_error = Some(error);
            }
            storage_owner.pending_offloads.retry(pending.entry.clone());
        }
    }
    Ok(reserved_release_bytes)
}

pub(in super::super) fn remove_unpublished_persistent_backing(
    storage_owner: &StorageOwnerState,
    backing: &mooncake_store_core::ColdBackingRoute,
) {
    for target in persistent_backing_targets(backing) {
        let result = storage_owner
            .cold_tier_devices
            .backend_for(&target)
            .and_then(|backend| backend_remove_cold_payload(backend.as_ref(), &target));
        if let Err(error) = result {
            tracing::warn!(
                cold_tier_id = %target.cold_tier_id,
                object_locator = %target.object_locator,
                error = %error,
                "failed to remove unpublished persistent backing target"
            );
        }
    }
}

fn reserve_pending_offload_batch(
    storage_owner: &StorageOwnerState,
    pending: &[PendingOffloadMaterialization],
) -> Result<()> {
    if pending[0].transient_nof || pending[0].managed_nof {
        return Ok(());
    }
    let reserved_bytes = sum_pending_offload_payload_bytes(pending)?;
    storage_owner.apply_cold_tier_usage_delta(
        cold_tier_device_id(&pending[0].cold_backing),
        0,
        reserved_bytes,
    )
}

pub(crate) fn is_stale_pending_cold_backing_error(error: &StoreError) -> bool {
    let message = error.to_string();
    message.contains("payload checksum mismatch before encode")
        || message.contains("payload length mismatch before encode")
}

fn record_failed_pending_offload_write(
    storage_owner: &StorageOwnerState,
    prepared: &PendingOffloadMaterialization,
    error: StoreError,
    reserved_release_bytes: i64,
    first_error: &mut Option<StoreError>,
) -> Result<i64> {
    let reserved_release_bytes = checked_add_cold_tier_bytes(
        reserved_release_bytes,
        pending_offload_payload_len_i64(prepared)?,
        "cold tier reserved release exceeds accounting range",
    )?;
    if is_stale_pending_cold_backing_error(&error) {
        clear_unavailable_pending_cold_backing(
            storage_owner,
            &prepared.route,
            &prepared.cold_backing,
            prepared.transient_nof,
            "checksum_mismatch",
            Some(&error),
        )?;
        registry::record_cold_tier_operation("offload", "skipped", "checksum_mismatch");
        storage_owner.pending_offloads.complete(&prepared.entry.key);
        return Ok(reserved_release_bytes);
    }
    if let Some(device) = prepared.device.as_ref() {
        let mut update = mooncake_store_core::ColdTierDeviceUpdate::new(current_time_ms());
        update.failure_count = Some(device.failure_count.saturating_add(1));
        update.last_error = Some(Some(error.to_string()));
        let _ = storage_owner
            .metadata
            .as_ref()
            .update_cold_tier_device(cold_tier_device_id(&prepared.cold_backing), update);
    }
    registry::record_cold_tier_operation(
        "offload",
        "error",
        registry::cold_tier_error_kind(&error),
    );
    if first_error.is_none() {
        *first_error = Some(error);
    }
    storage_owner.pending_offloads.retry(prepared.entry.clone());
    Ok(reserved_release_bytes)
}

fn settle_pending_offload_batch(
    storage_owner: &StorageOwnerState,
    pending: &[PendingOffloadMaterialization],
    used_add_bytes: i64,
    reserved_release_bytes: i64,
) -> Result<()> {
    if pending[0].transient_nof || pending[0].managed_nof {
        return Ok(());
    }
    storage_owner.apply_cold_tier_usage_delta(
        cold_tier_device_id(&pending[0].cold_backing),
        used_add_bytes,
        -reserved_release_bytes,
    )
}

pub(crate) fn checked_add_cold_tier_bytes(total: i64, delta: i64, message: &str) -> Result<i64> {
    total
        .checked_add(delta)
        .ok_or_else(|| StoreError::InvalidState(message.to_string()))
}

fn pending_offload_payload_len_i64(prepared: &PendingOffloadMaterialization) -> Result<i64> {
    i64::try_from(prepared.payload.len()).map_err(|_| {
        StoreError::InvalidState(format!(
            "cold tier payload {} length {} exceeds accounting range",
            prepared.cold_backing.object_locator,
            prepared.payload.len()
        ))
    })
}

fn sum_pending_offload_payload_bytes(pending: &[PendingOffloadMaterialization]) -> Result<i64> {
    pending.iter().try_fold(0i64, |total, prepared| {
        checked_add_cold_tier_bytes(
            total,
            pending_offload_payload_len_i64(prepared)?,
            "cold tier batch reservation exceeds accounting range",
        )
    })
}

fn rebuild_pending_offload_queue_with(
    storage_owner: &StorageOwnerState,
    offload_priority: ColdTierOffloadPriorityConfig,
    pending_offloads: &ColdTierOffloadManager,
) -> Result<usize> {
    // Query by device_id rather than replica-owner listing so we recover
    // pending offloads for local cold-tier devices after startup/metadata repair.
    let device_ids = storage_owner.cold_tier_devices.local_device_ids();
    let mut queue = PendingOffloadQueue::new(offload_priority);
    for device_id in device_ids {
        let routes = storage_owner
            .metadata
            .as_ref()
            .list_object_routes_by_cold_backing(&mooncake_store_core::ColdBackingRouteFilter {
                device_id: Some(device_id),
                state: Some(mooncake_store_core::ColdBackingState::PendingOffload),
                ..mooncake_store_core::ColdBackingRouteFilter::default()
            })?;
        for route in routes {
            let Some(cold_backing) = route.cold_backing.as_ref() else {
                continue;
            };
            if route.state == RouteState::Active
                && cold_backing.state == mooncake_store_core::ColdBackingState::PendingOffload
            {
                queue.push(route.key.clone(), route.version, Some(cold_backing.length));
            }
        }
    }
    let len = queue.len();
    pending_offloads.replace(queue);
    Ok(len)
}

// ---------------------------------------------------------------------------
// ColdTierOffloadManager / PendingOffloadQueue impl blocks
// ---------------------------------------------------------------------------

impl ColdTierOffloadManager {
    pub(in super::super) fn new(priority: ColdTierOffloadPriorityConfig) -> Self {
        Self {
            queue: Mutex::new(PendingOffloadQueue::new(priority)),
            materializing: AtomicBool::new(false),
        }
    }

    pub(in super::super) fn try_begin_materialization(
        &self,
    ) -> Option<ColdTierOffloadRunGuard<'_>> {
        self.materializing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| ColdTierOffloadRunGuard { manager: self })
    }

    pub(in super::super) fn push(
        &self,
        route_key: ObjectKey,
        route_version: RouteVersion,
        length_bytes: Option<u64>,
    ) -> bool {
        self.queue
            .lock()
            .push(route_key, route_version, length_bytes)
    }

    pub(in super::super) fn is_materializing(&self) -> bool {
        self.materializing.load(Ordering::Acquire)
    }

    pub(in super::super) fn replace(&self, queue: PendingOffloadQueue) {
        let mut guard = self.queue.lock();
        let prev_in_flight = std::mem::take(&mut guard.in_flight);
        *guard = queue;
        // Preserve in-flight keys: remove them from the new queue's entries
        // and restore them to in_flight so concurrent pop/claim won't hand
        // out a key that another thread is still processing.
        if !prev_in_flight.is_empty() {
            guard
                .entries
                .retain(|entry| !prev_in_flight.contains(&entry.key));
            for key in &prev_in_flight {
                guard.keys.remove(key);
            }
            guard.in_flight = prev_in_flight;
        }
    }

    pub(in super::super) fn observability_snapshot(
        &self,
        now: Instant,
    ) -> PendingOffloadQueueSnapshot {
        self.queue.lock().observability_snapshot(now)
    }

    pub(in super::super) fn pop_ready(&self, now: Instant) -> Option<PendingOffloadEntry> {
        self.queue.lock().pop_ready(now)
    }

    pub(in super::super) fn claim(
        &self,
        route_key: &ObjectKey,
        route_version: RouteVersion,
    ) -> Option<PendingOffloadEntry> {
        self.queue.lock().claim(route_key, route_version)
    }

    pub(in super::super) fn complete(&self, key: &PendingOffloadKey) {
        self.queue.lock().complete(key);
    }

    pub(in super::super) fn retry(&self, entry: PendingOffloadEntry) {
        self.queue.lock().retry(entry);
    }

    pub(in super::super) fn retry_after(&self, entry: PendingOffloadEntry, delay: Duration) {
        self.queue.lock().retry_after(entry, delay);
    }

    pub(in super::super) fn requeue_for_debug_evict_all(
        &self,
        route_key: ObjectKey,
        route_version: RouteVersion,
        length_bytes: Option<u64>,
    ) -> bool {
        self.queue
            .lock()
            .requeue_for_debug_evict_all(route_key, route_version, length_bytes)
    }

    pub(in super::super) fn discard_for_debug_evict_all(&self, route_key: &ObjectKey) {
        self.queue.lock().discard_for_debug_evict_all(route_key);
    }

    pub(in super::super) fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }

    pub(in super::super) fn len(&self) -> usize {
        self.queue.lock().len()
    }

    #[cfg(test)]
    pub(in super::super) fn clear(&self) {
        self.replace(PendingOffloadQueue::new(
            ColdTierOffloadPriorityConfig::default(),
        ));
    }
}

impl Drop for ColdTierOffloadRunGuard<'_> {
    fn drop(&mut self) {
        self.manager.materializing.store(false, Ordering::Release);
    }
}

impl ColdTierCleanupManager {
    pub(in super::super) fn try_start_device(
        &self,
        device_id: &str,
    ) -> Option<ColdTierCleanupGuard> {
        if !self.scheduler.lock().try_start(device_id) {
            return None;
        }
        Some(ColdTierCleanupGuard {
            scheduler: Arc::clone(&self.scheduler),
            device_id: device_id.to_string(),
        })
    }
}

impl Drop for ColdTierCleanupGuard {
    fn drop(&mut self) {
        self.scheduler.lock().finish(&self.device_id);
    }
}

impl FreeSchedulerState {
    fn try_start(&mut self, device_id: &str) -> bool {
        self.active_devices.insert(device_id.to_string())
    }

    fn finish(&mut self, device_id: &str) {
        self.active_devices.remove(device_id);
    }
}

impl Default for ColdTierOffloadManager {
    fn default() -> Self {
        Self::new(ColdTierOffloadPriorityConfig::default())
    }
}

impl Default for PendingOffloadQueue {
    fn default() -> Self {
        Self::new(ColdTierOffloadPriorityConfig::default())
    }
}

impl PendingOffloadQueue {
    pub(in super::super) fn new(priority: ColdTierOffloadPriorityConfig) -> Self {
        Self {
            entries: VecDeque::new(),
            keys: BTreeSet::new(),
            in_flight: BTreeSet::new(),
            priority,
        }
    }

    #[cfg(test)]
    pub(in super::super) fn set_entry_timing_for_test(
        &mut self,
        route_key: &ObjectKey,
        route_version: RouteVersion,
        not_before: Instant,
        enqueued_at: Instant,
    ) {
        let key = PendingOffloadKey {
            route_key: route_key.clone(),
        };
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("pending offload entry should exist");
        entry.route_version = route_version;
        entry.not_before = not_before;
        entry.enqueued_at = enqueued_at;
    }

    pub(in super::super) fn observability_snapshot(
        &self,
        now: Instant,
    ) -> PendingOffloadQueueSnapshot {
        let mut snapshot = PendingOffloadQueueSnapshot {
            total_pending: self.entries.len() + self.in_flight.len(),
            ..PendingOffloadQueueSnapshot::default()
        };
        for entry in &self.entries {
            if entry.not_before <= now {
                snapshot.ready = snapshot.ready.saturating_add(1);
            } else {
                snapshot.delayed = snapshot.delayed.saturating_add(1);
            }
            snapshot.max_attempts = snapshot.max_attempts.max(entry.attempts);
            snapshot.total_attempts = snapshot
                .total_attempts
                .saturating_add(entry.attempts as u64);
        }
        snapshot
    }

    pub(in super::super) fn push(
        &mut self,
        route_key: ObjectKey,
        route_version: RouteVersion,
        length_bytes: Option<u64>,
    ) -> bool {
        let key = PendingOffloadKey { route_key };
        if self.in_flight.contains(&key) || !self.keys.insert(key.clone()) {
            return false;
        }
        let now = Instant::now();
        self.entries.push_back(PendingOffloadEntry {
            key,
            route_version,
            attempts: 0,
            not_before: now,
            enqueued_at: now,
            length_bytes,
        });
        true
    }

    pub(in super::super) fn pop_ready(&mut self, now: Instant) -> Option<PendingOffloadEntry> {
        match self.priority.pending_policy {
            ColdTierPendingOffloadPolicy::Fifo => self.pop_ready_fifo(now),
            ColdTierPendingOffloadPolicy::SizeSmallFirst
            | ColdTierPendingOffloadPolicy::SizeLargeFirst => self.pop_ready_by_size(now),
        }
    }

    fn pop_ready_fifo(&mut self, now: Instant) -> Option<PendingOffloadEntry> {
        let len = self.entries.len();
        for _ in 0..len {
            let entry = self.entries.pop_front()?;
            if entry.not_before <= now {
                self.keys.remove(&entry.key);
                self.in_flight.insert(entry.key.clone());
                return Some(entry);
            }
            self.entries.push_back(entry);
        }
        None
    }

    fn pop_ready_by_size(&mut self, now: Instant) -> Option<PendingOffloadEntry> {
        let aging = Duration::from_millis(self.priority.pending_aging_ms.max(1));
        let mut oldest_aged: Option<(usize, Instant)> = None;
        let mut selected: Option<(usize, u64)> = None;
        let mut first_ready: Option<usize> = None;
        for (index, entry) in self
            .entries
            .iter()
            .enumerate()
            .take(self.priority.pending_scan_limit.max(1))
        {
            if entry.not_before > now {
                continue;
            }
            first_ready.get_or_insert(index);
            if now.saturating_duration_since(entry.enqueued_at) >= aging {
                match oldest_aged {
                    Some((_, oldest)) if oldest <= entry.enqueued_at => {}
                    _ => oldest_aged = Some((index, entry.enqueued_at)),
                }
                continue;
            }
            let Some(length) = entry.length_bytes else {
                continue;
            };
            match (self.priority.pending_policy, selected) {
                (_, None) => selected = Some((index, length)),
                (ColdTierPendingOffloadPolicy::SizeSmallFirst, Some((_, current)))
                    if length < current =>
                {
                    selected = Some((index, length));
                }
                (ColdTierPendingOffloadPolicy::SizeLargeFirst, Some((_, current)))
                    if length > current =>
                {
                    selected = Some((index, length));
                }
                _ => {}
            }
        }
        let index = oldest_aged
            .map(|(index, _)| index)
            .or_else(|| selected.map(|(index, _)| index))
            .or(first_ready)?;
        let entry = self.entries.remove(index)?;
        self.keys.remove(&entry.key);
        self.in_flight.insert(entry.key.clone());
        Some(entry)
    }

    pub(in super::super) fn claim(
        &mut self,
        route_key: &ObjectKey,
        route_version: RouteVersion,
    ) -> Option<PendingOffloadEntry> {
        let key = PendingOffloadKey {
            route_key: route_key.clone(),
        };
        if !self.in_flight.insert(key.clone()) {
            return None;
        }
        if let Some(index) = self.entries.iter().position(|entry| entry.key == key) {
            let entry = self.entries.remove(index)?;
            self.keys.remove(&key);
            Some(entry)
        } else {
            let now = Instant::now();
            Some(PendingOffloadEntry {
                key,
                route_version,
                attempts: 0,
                not_before: now,
                enqueued_at: now,
                length_bytes: None,
            })
        }
    }

    pub(in super::super) fn complete(&mut self, key: &PendingOffloadKey) {
        self.in_flight.remove(key);
    }

    pub(in super::super) fn retry(&mut self, mut entry: PendingOffloadEntry) {
        entry.attempts = entry.attempts.saturating_add(1);
        let shift = entry.attempts.saturating_sub(1).min(6);
        let delay_ms = DEFAULT_PENDING_OFFLOAD_RETRY_BASE_MS
            .saturating_mul(1u64 << shift)
            .min(DEFAULT_PENDING_OFFLOAD_RETRY_MAX_MS);
        self.retry_after(entry, Duration::from_millis(delay_ms));
    }

    pub(in super::super) fn retry_after(
        &mut self,
        mut entry: PendingOffloadEntry,
        delay: Duration,
    ) {
        self.in_flight.remove(&entry.key);
        let now = Instant::now();
        entry.not_before = now + delay;
        if delay.is_zero() {
            entry.enqueued_at = entry.enqueued_at.min(now);
            if self.keys.insert(entry.key.clone()) {
                self.entries.push_front(entry);
            }
        } else if self.keys.insert(entry.key.clone()) {
            self.entries.push_back(entry);
        }
    }

    pub(in super::super) fn requeue_for_debug_evict_all(
        &mut self,
        route_key: ObjectKey,
        route_version: RouteVersion,
        length_bytes: Option<u64>,
    ) -> bool {
        let key = PendingOffloadKey { route_key };
        let removed_in_flight = self.in_flight.remove(&key);
        if self.keys.contains(&key) {
            return removed_in_flight;
        }
        let now = Instant::now();
        self.keys.insert(key.clone());
        self.entries.push_front(PendingOffloadEntry {
            key,
            route_version,
            attempts: 0,
            not_before: now,
            enqueued_at: now,
            length_bytes,
        });
        true
    }

    pub(in super::super) fn discard_for_debug_evict_all(&mut self, route_key: &ObjectKey) {
        let key = PendingOffloadKey {
            route_key: route_key.clone(),
        };
        self.in_flight.remove(&key);
        self.keys.remove(&key);
        self.entries.retain(|entry| entry.key != key);
    }

    pub(in super::super) fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.in_flight.is_empty()
    }

    pub(in super::super) fn len(&self) -> usize {
        self.entries.len() + self.in_flight.len()
    }
}
