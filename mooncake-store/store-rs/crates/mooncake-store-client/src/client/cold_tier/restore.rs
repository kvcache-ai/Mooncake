use super::super::{
    backend_load_cold_payload_batch_into, cold_restore_flight_key, cold_tier_device_id,
    compatibility_matches, materialized_cold_backing, payload_checksum, registry,
    validate_cold_restore_payload, with_disjoint_restore_caller_buffers, AsyncEvictionHandle,
    AsyncReplicaTrackHandle, AsyncRouteHitReportHandle, ColdObjectRead, ColdRestoreFlightLeader,
    ColdRestoreFlightRegistration, ColdTierHandle, ColdTierRateLimitConfig, ControlPlaneHandle,
    LocalAllocatorState, MembershipSyncHandle, ObjectKey, ObjectRef, ObjectRoute, OperationTracker,
    ReplicaRoute, ReplicaTier, ReplicationPolicy, RequestDeadline, ResolvedObject,
    RestorePromotionKey, RestorePromotionPayload, RestorePromotionPushOutcome,
    RestorePromotionQueue, RestorePromotionTask, Result, RouteState, StorageOwnerState,
    StoreClient, StoreError, DEFAULT_REMOTE_COLD_RESTORE_RECHECK_DELAY,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    thread::sleep,
};
use tracing::{debug, info, warn};

pub(in super::super) fn enqueue_restore_promotion(
    client: &StoreClient,
    resolved: &ResolvedObject,
    payload: RestorePromotionPayload<'_>,
) {
    let payload = payload.into_arc();
    let payload_bytes = payload.len();
    let Some(cold_backing) = materialized_cold_backing(&resolved.route) else {
        debug!(
            runtime = %client.lease.runtime,
            tenant = %resolved.tenant,
            key = %resolved.key,
            route_key = %resolved.route.key.0,
            route_version = resolved.route.version.0,
            payload_bytes,
            reason = "no_materialized_cold_backing",
            "mooncake store skipped restore promotion enqueue"
        );
        return;
    };
    let key = RestorePromotionKey {
        route_key: resolved.route.key.clone(),
        route_version: resolved.route.version,
    };
    let object_id = match mooncake_store_core::route_logical_object_id(&resolved.route) {
        Ok(object_id) => object_id,
        Err(error) => {
            debug!(
                runtime = %client.lease.runtime,
                tenant = %resolved.tenant,
                key = %resolved.key,
                error = %error,
                "restore promotion skipped because route identity is unavailable"
            );
            return;
        }
    };
    let prefer_local = client.can_prefer_local_storage_for_write_mode()
        && cold_backing.owner == client.lease.runtime;
    let target_runtime = (!prefer_local).then(|| cold_backing.owner.clone());
    let target_segment = target_runtime.as_ref().and_then(|target_runtime| {
        client
            .metadata
            .get_client_lease(target_runtime)
            .ok()
            .flatten()
            .filter(|lease| compatibility_matches(&client.lease, lease))
            .and_then(|lease| lease.endpoints.segment_name.as_ref().cloned())
    });
    let mut policy = ReplicationPolicy::new().replica_count(1);
    if prefer_local {
        policy = policy.prefer_local(true);
    } else {
        policy = policy
            .preferred_storage_owners(vec![cold_backing.owner.storage_key()])
            .prefer_local(false);
        if let Some(segment_name) = target_segment.as_ref() {
            policy = policy.preferred_segment(segment_name.0.clone());
        }
    }
    let task = RestorePromotionTask {
        key,
        tenant: resolved.tenant.clone(),
        object_id,
        qos_tier: resolved.route.qos_tier.clone(),
        current: resolved.route.clone(),
        payload,
        policy,
        target_runtime,
        target_segment,
    };
    let outcome = client.restore_promotions.push(task);
    let result_label = match &outcome {
        RestorePromotionPushOutcome::Accepted => "accepted",
        RestorePromotionPushOutcome::Shutdown => "shutdown",
        RestorePromotionPushOutcome::Duplicate => "duplicate",
        RestorePromotionPushOutcome::InFlight => "in_flight_limit",
        RestorePromotionPushOutcome::Full => "queue_full",
    };
    info!(
        runtime = %client.lease.runtime,
        tenant = %resolved.tenant,
        key = %resolved.key,
        route_key = %resolved.route.key.0,
        route_version = resolved.route.version.0,
        payload_bytes,
        cold_tier_id = %cold_backing.cold_tier_id,
        owner = %cold_backing.owner,
        result = result_label,
        "mooncake store restore promotion enqueue"
    );
    match outcome {
        RestorePromotionPushOutcome::Accepted => {
            spawn_restore_promotion_worker_if_needed(client, resolved)
        }
        RestorePromotionPushOutcome::Shutdown => {
            registry::record_cold_tier_operation("restore_promote", "admission_reject", "shutdown")
        }
        RestorePromotionPushOutcome::Duplicate => {
            registry::record_cold_tier_operation("restore_promote", "admission_reject", "duplicate")
        }
        RestorePromotionPushOutcome::InFlight => registry::record_cold_tier_operation(
            "restore_promote",
            "admission_reject",
            "in_flight_limit",
        ),
        RestorePromotionPushOutcome::Full => registry::record_cold_tier_operation(
            "restore_promote",
            "admission_reject",
            "queue_full",
        ),
    }
}

pub(in super::super) fn wait_for_restore_promotions(client: &StoreClient) {
    client.restore_promotions.wait_for_worker_idle();
}

pub(in super::super) fn trigger_remote_owner_cold_restore(
    client: &StoreClient,
    entry: &mut ResolvedObject,
    request_deadline: RequestDeadline,
) -> Result<()> {
    let owner = entry.replica.owner.clone();
    let resolved_lease = match client.metadata.get_client_lease(&owner)? {
        Some(lease) => Some(lease),
        None => {
            // Owner incarnation gone — try finding the live incarnation of
            // the same physical store (same stable_id, new epoch).
            let live = client
                .metadata
                .get_live_runtime_by_stable_id(&owner.stable_id)?;
            if let Some(ref live_lease) = live {
                info!(
                    runtime = %client.lease.runtime,
                    tenant = %entry.tenant,
                    key = %entry.key,
                    stale_owner = %owner,
                    live_owner = %live_lease.runtime,
                    "cold restore resolved stale owner to live incarnation"
                );
            }
            live
        }
    };
    let owner_lease = resolved_lease
        .filter(|lease| compatibility_matches(&client.lease, lease))
        .ok_or_else(|| {
            debug!(
                runtime = %client.lease.runtime,
                tenant = %entry.tenant,
                key = %entry.key,
                storage_owner = %owner,
                reason = "owner_lease_unavailable",
                "mooncake store owner cold restore aborted before submit"
            );
            StoreError::NotFound(format!(
                "runtime {} is not available for cold restore of tenant={} key={}",
                owner, entry.tenant, entry.key
            ))
        })?;
    if !owner_lease.state.serves_reads() {
        debug!(
            runtime = %client.lease.runtime,
            tenant = %entry.tenant,
            key = %entry.key,
            storage_owner = %owner,
            owner_state = ?owner_lease.state,
            reason = "owner_not_readable",
            "mooncake store owner cold restore aborted before submit"
        );
        return Err(StoreError::InvalidState(format!(
            "runtime {} is not readable while {:?}",
            owner, owner_lease.state
        )));
    }
    let target_segment = owner_lease
        .endpoints
        .segment_name
        .as_ref()
        .ok_or_else(|| {
            debug!(
                runtime = %client.lease.runtime,
                tenant = %entry.tenant,
                key = %entry.key,
                storage_owner = %owner,
                reason = "owner_segment_name_missing",
                "mooncake store owner cold restore aborted before submit"
            );
            StoreError::InvalidState(format!(
                "runtime {} is missing segment_name for cold restore of tenant={} key={}",
                owner, entry.tenant, entry.key
            ))
        })?
        .0
        .clone();
    let scope = entry.route.namespace.clone().unwrap_or_else(|| {
        mooncake_store_core::NamespaceScope::with_defaults(Some(&entry.tenant), None, None)
    });
    info!(
        runtime = %client.lease.runtime,
        tenant = %entry.tenant,
        key = %entry.key,
        storage_owner = %owner,
        route_version = entry.route.version.0,
        route_key = %entry.route.key.0,
        logical_key = entry.route.logical_key.as_deref().unwrap_or(""),
        "submitting owner cold restore migration task"
    );
    let execution_id = client.control_client.submit_migration_task(
        &owner_lease,
        crate::control_plane::pb::SubmitMigrationTaskRequest {
            namespace: client.metadata.route_namespace(),
            authority: owner_lease.runtime.stable_id.0.clone(),
            tenant: scope.tenant,
            domain: scope.domain,
            object_set: scope.object_set,
            key: entry
                .route
                .logical_key
                .clone()
                .unwrap_or_else(|| entry.key.clone()),
            mode: crate::control_plane::pb::MigrationMode::Move as i32,
            source_segment: mooncake_store_core::COLD_TIER_MIGRATION_SOURCE_SEGMENT.to_string(),
            target_segments: vec![target_segment.clone()],
            task_executor: owner_lease.runtime.stable_id.0.clone(),
            max_retries: 1,
        },
    )?;
    info!(
        runtime = %client.lease.runtime,
        tenant = %entry.tenant,
        key = %entry.key,
        storage_owner = %owner,
        execution_id = %execution_id,
        target_segment = %target_segment,
        "owner cold restore migration task submitted"
    );
    let poll_start = std::time::Instant::now();
    const MAX_POLL_ITERATIONS: usize = 100;
    for _poll_iteration in 0..MAX_POLL_ITERATIONS {
        let status = client
            .control_client
            .get_migration_execution_status_detail(
                &owner_lease,
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: client.metadata.route_namespace(),
                    authority: owner_lease.runtime.stable_id.0.clone(),
                    execution_id: execution_id.clone(),
                },
            )?;
        debug!(
            runtime = %client.lease.runtime,
            tenant = %entry.tenant,
            key = %entry.key,
            storage_owner = %owner,
            execution_id = %execution_id,
            state = status.state,
            last_error = %status.last_error,
            poll_elapsed_ms = poll_start.elapsed().as_millis() as u64,
            "owner cold restore polled execution state"
        );
        match crate::control_plane::pb::MigrationExecutionState::try_from(status.state) {
            Ok(crate::control_plane::pb::MigrationExecutionState::Succeeded) => {
                if client.refresh_resolved_route_after_read_failure(entry)? {
                    info!(
                        runtime = %client.lease.runtime,
                        tenant = %entry.tenant,
                        key = %entry.key,
                        storage_owner = %owner,
                        execution_id = %execution_id,
                        refreshed_owner = %entry.replica.owner,
                        refreshed_segment = %entry.replica.segment_name.0,
                        poll_elapsed_ms = poll_start.elapsed().as_millis() as u64,
                        "owner cold restore succeeded and route refreshed"
                    );
                    return Ok(());
                }
                warn!(
                    runtime = %client.lease.runtime,
                    tenant = %entry.tenant,
                    key = %entry.key,
                    storage_owner = %owner,
                    execution_id = %execution_id,
                    refreshed_owner = %entry.replica.owner,
                    refreshed_segment = %entry.replica.segment_name.0,
                    poll_elapsed_ms = poll_start.elapsed().as_millis() as u64,
                    "owner cold restore succeeded but route not readable after refresh"
                );
                return Err(StoreError::Conflict(format!(
                    "tenant={} key={} owner {} completed cold restore but route was not updated",
                    entry.tenant, entry.key, owner
                )));
            }
            Ok(crate::control_plane::pb::MigrationExecutionState::Failed)
            | Ok(crate::control_plane::pb::MigrationExecutionState::Cancelled) => {
                let detail = if status.last_error.trim().is_empty() {
                    format!("state={}", status.state)
                } else {
                    status.last_error
                };
                warn!(
                    runtime = %client.lease.runtime,
                    tenant = %entry.tenant,
                    key = %entry.key,
                    storage_owner = %owner,
                    execution_id = %execution_id,
                    detail = %detail,
                    poll_elapsed_ms = poll_start.elapsed().as_millis() as u64,
                    "owner cold restore migration failed or cancelled"
                );
                return Err(StoreError::Transport(format!(
                    "tenant={} key={} owner {} cold restore failed: {}",
                    entry.tenant, entry.key, owner, detail
                )));
            }
            Ok(_) => {}
            Err(_) => {
                return Err(StoreError::Transport(format!(
                    "tenant={} key={} owner {} cold restore returned invalid state {}",
                    entry.tenant, entry.key, owner, status.state
                )));
            }
        }
        if request_deadline.remaining() <= DEFAULT_REMOTE_COLD_RESTORE_RECHECK_DELAY {
            break;
        }
        sleep(DEFAULT_REMOTE_COLD_RESTORE_RECHECK_DELAY);
    }
    warn!(
        runtime = %client.lease.runtime,
        tenant = %entry.tenant,
        key = %entry.key,
        storage_owner = %owner,
        execution_id,
        poll_elapsed_ms = poll_start.elapsed().as_millis() as u64,
        "owner cold restore did not complete before read deadline"
    );
    Err(StoreError::Transport(format!(
        "tenant={} key={} owner {} cold restore did not complete before read deadline",
        entry.tenant, entry.key, owner
    )))
}

pub(in super::super) fn execute_local_restore_batch_reads(
    client: &StoreClient,
    resolved: &[ResolvedObject],
    buffers: &mut [&mut [u8]],
    cold_indices: &[usize],
) -> Result<()> {
    if cold_indices.is_empty() {
        return Ok(());
    }
    info!(
        runtime = %client.lease.runtime,
        items = cold_indices.len(),
        cold_keys = ?cold_indices
            .iter()
            .map(|index| resolved[*index].key.clone())
            .collect::<Vec<_>>(),
        "mooncake store cold restore entering batch payload read"
    );
    let promotion_payloads =
        restore_batch_payloads_from_cold_backing(client, resolved, buffers, cold_indices).map_err(
            |e| {
                warn!(
                    runtime = %client.lease.runtime,
                    items = cold_indices.len(),
                    error = %e,
                    "restore_batch_payloads_from_cold_backing failed"
                );
                e
            },
        )?;
    info!(
        runtime = %client.lease.runtime,
        items = cold_indices.len(),
        returned_promotion_payloads = promotion_payloads.len(),
        cold_keys = ?cold_indices
            .iter()
            .map(|index| resolved[*index].key.clone())
            .collect::<Vec<_>>(),
        "mooncake store cold restore batch payload read returned"
    );
    let lengths = resolved
        .iter()
        .map(|entry| entry.replica.length as usize)
        .collect::<Vec<_>>();
    let mut shared_payloads = promotion_payloads.into_iter().peekable();
    for index in cold_indices.iter().copied() {
        let payload = match shared_payloads.next_if(|(payload_index, _)| *payload_index == index) {
            Some((_, payload)) => RestorePromotionPayload::Shared(payload),
            None => RestorePromotionPayload::Borrowed(&buffers[index][..lengths[index]]),
        };
        enqueue_restore_promotion(client, &resolved[index], payload);
    }
    Ok(())
}

pub(in super::super) fn restore_payload_from_cold_backing(
    client: &StoreClient,
    resolved: &ResolvedObject,
    buffer: &mut [u8],
) -> Result<Option<Arc<Vec<u8>>>> {
    if super::cold_paths_short_circuited() {
        return Err(StoreError::NotFound(
            "cold restore short-circuited for abort isolation".to_string(),
        ));
    }
    let total_tracker = OperationTracker::new("cold_restore_total");
    let result = restore_payload_from_cold_backing_singleflight_into(client, resolved, buffer);
    let metric_result = result.as_ref().map(|_| ()).map_err(Clone::clone);
    registry::record_cold_tier_operation_result("restore", &metric_result);
    total_tracker.finish(
        &metric_result,
        result
            .as_ref()
            .map(|payload| {
                payload
                    .as_ref()
                    .map(|payload| payload.len())
                    .unwrap_or(resolved.replica.length as usize)
            })
            .unwrap_or_default() as u64,
    );
    if let Err(error) = &result {
        warn!(
            runtime = %client.lease.runtime,
            tenant = %resolved.tenant,
            key = %resolved.key,
            error = %error,
            "cold tier payload restore failed"
        );
    }
    result
}

pub(in super::super) fn restore_batch_payloads_from_cold_backing(
    client: &StoreClient,
    resolved: &[ResolvedObject],
    buffers: &mut [&mut [u8]],
    indices: &[usize],
) -> Result<Vec<(usize, Arc<Vec<u8>>)>> {
    if super::cold_paths_short_circuited() {
        debug!(
            runtime = %client.lease.runtime,
            items = indices.len(),
            "cold restore batch short-circuited for abort isolation"
        );
        return Err(StoreError::NotFound(
            "cold restore batch short-circuited for abort isolation".to_string(),
        ));
    }
    let batch_tracker = OperationTracker::new("cold_restore_batch_total");
    let mut result =
        restore_batch_payloads_from_cold_backing_grouped(client, resolved, buffers, indices);
    if let Ok(payloads) = &mut result {
        debug!(
            runtime = %client.lease.runtime,
            requested_indices = indices.len(),
            returned_promotion_payloads = payloads.len(),
            returned_payload_bytes = payloads.iter().map(|(_, payload)| payload.len()).sum::<usize>(),
            "mooncake store cold restore batch returned payloads"
        );
        payloads.sort_by_key(|(index, _)| *index);
    }
    let metric_result = result.as_ref().map(|_| ()).map_err(Clone::clone);
    registry::record_cold_tier_operation_result("restore", &metric_result);
    batch_tracker.finish(
        &metric_result,
        indices
            .iter()
            .map(|index| resolved[*index].replica.length)
            .sum::<u64>(),
    );
    result
}

pub(in super::super) fn promote_owned_materialized_route_by_key(
    storage_owner: &StorageOwnerState,
    route_key: &ObjectKey,
) -> Result<Option<PromoteTimingBreakdown>> {
    let Some(route) = storage_owner.current_materialized_route(route_key)? else {
        return Ok(None);
    };
    let promote_result = promote_materialized_cold_backing(storage_owner, &route);
    // Emit restore_promote metric so Prometheus counters reflect migration-task restores
    // (previously only emitted in the async background promotion worker).
    match &promote_result {
        Ok(Some((_bytes, _timing))) => {
            registry::record_cold_tier_operation_result(
                "restore_promote",
                &Ok::<(), StoreError>(()),
            );
        }
        Ok(None) => {
            // Route race — promotion skipped, no metric.
        }
        Err(ref error) => {
            registry::record_cold_tier_operation_result::<()>(
                "restore_promote",
                &Err(error.clone()),
            );
        }
    }
    promote_result.map(|opt| opt.map(|(_bytes, timing)| timing))
}

fn spawn_restore_promotion_worker_if_needed(client: &StoreClient, resolved: &ResolvedObject) {
    let tasks = client.restore_promotions.take_ready_batch();
    if tasks.is_empty() {
        return;
    }
    let promoter = clone_restore_promotion_client(client);
    let queue = client.restore_promotions.clone();
    let tasks_for_abort = tasks.clone();
    let worker_queue = queue.clone();
    let run_tasks = move |mut tasks: Vec<RestorePromotionTask>| {
        loop {
            for task in tasks {
                let promote_result = execute_restore_promotion(&promoter, &task);
                registry::record_cold_tier_operation_result("restore_promote", &promote_result);
                match promote_result {
                    Ok(()) => {
                        debug!(
                            tenant = %task.tenant,
                            key = %task.object_id.logical_key,
                            route_key = %task.current.key.0,
                            route_version = task.current.version.0,
                            payload_bytes = task.payload.len(),
                            "cold tier restored payload promoted"
                        );
                    }
                    Err(error) => {
                        debug!(
                            tenant = %task.tenant,
                            key = %task.object_id.logical_key,
                            route_key = %task.current.key.0,
                            route_version = task.current.version.0,
                            payload_bytes = task.payload.len(),
                            error = %error,
                            "restore promotion failed after cold read"
                        );
                    }
                }
                worker_queue.complete(&task.key);
            }
            tasks = worker_queue.take_next_worker_batch();
            if tasks.is_empty() {
                break;
            }
        }
        worker_queue.finish_worker();
    };
    let spawn_result = std::thread::Builder::new()
        .name(format!(
            "mooncake-restore-promote-{}",
            client.lease.runtime.stable_id.0
        ))
        .spawn(move || run_tasks(tasks));
    if let Err(error) = spawn_result {
        queue.abort_worker_batch(tasks_for_abort);
        debug!(
            runtime = %client.lease.runtime,
            tenant = %resolved.tenant,
            key = %resolved.key,
            error = %error,
            "failed to spawn restore promotion worker"
        );
    }
}

fn clone_restore_promotion_client(client: &StoreClient) -> StoreClient {
    let queue = Arc::new(RestorePromotionQueue::from_rate_limits(
        ColdTierRateLimitConfig::default(),
    ));
    StoreClient {
        metadata: client.metadata.clone(),
        route_directory: client.route_directory.clone(),
        _control_plane: ControlPlaneHandle::detached(),
        control_client: client.control_client.clone(),
        allocator: client.allocator.clone(),
        storage_owner: client.storage_owner.clone(),
        lease: client.lease.clone(),
        lease_ttl_ms: client.lease_ttl_ms,
        live_client_cache: client.live_client_cache.clone(),
        suspect_runtime_cache: client.suspect_runtime_cache.clone(),
        restore_promotions: queue.clone(),
        membership_sync: MembershipSyncHandle::disabled(),
        _async_eviction: AsyncEvictionHandle::disabled(),
        async_replica_tracking: AsyncReplicaTrackHandle::disabled(),
        async_route_hit_reporting: AsyncRouteHitReportHandle::disabled(),
        cold_tier: ColdTierHandle::disabled(queue.clone()),
        cold_restore_flights: client.cold_restore_flights.clone(),
        default_tenant: client.default_tenant.clone(),
        local_memory: client.local_memory.clone(),
        transport: client.transport.clone(),
        transport_factory: client.transport_factory.clone(),
        write_mode: client.write_mode.clone(),
        route_control: client.route_control,
        route_topk: client.route_topk,
        transfer_stall_timeout: client.transfer_stall_timeout,
        request_timeout_override: client.request_timeout_override,
        lifecycle_state: client.lifecycle_state.clone(),
        route_write_gate: client.route_write_gate.clone(),
        startup_activation_pending: AtomicBool::new(false),
        heartbeat_repair_pending: AtomicUsize::new(0),
        tenant_quota_reservation_counter: client.tenant_quota_reservation_counter.clone(),
        namespace_quota: client.namespace_quota.clone(),
        execution_fairness: client.execution_fairness.clone(),
        bandwidth_shaping: client.bandwidth_shaping.clone(),
        placement_policy: client.placement_policy.clone(),
        state: client.state.clone(),
        deferred_cold_tier_reconciles: parking_lot::Mutex::new(Vec::new()),
        owns_cold_tier_lifecycle: false,
        cold_tier_shutdown_mode: super::super::ColdTierShutdownMode::Restart,
    }
}

fn execute_restore_promotion(client: &StoreClient, task: &RestorePromotionTask) -> Result<()> {
    client.ensure_local_memory()?;
    client.flush_due_reclaims()?;

    let tenant = task.object_id.scope.tenant.as_str();
    let key = task.object_id.logical_key.as_str();
    let mut object_ref = ObjectRef::new(key).tenant(tenant);
    if task.object_id.scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
        object_ref = object_ref.domain(task.object_id.scope.domain.as_str());
    }
    if task.object_id.scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
        object_ref = object_ref.object_set(task.object_id.scope.object_set.as_str());
    }
    if let Some(qos_tier) = task.qos_tier.as_deref() {
        if qos_tier != mooncake_store_core::DEFAULT_QOS_TIER {
            object_ref = object_ref.qos_tier(qos_tier);
        }
    }

    let policy = client.resolve_replication_policy(Some(&task.policy))?;
    let reserve_tracker =
        OperationTracker::new("restore_promote_reserve").input_bytes(task.payload.len() as u64);
    let reserve_result = if let (Some(target_runtime), Some(target_segment)) =
        (task.target_runtime.as_ref(), task.target_segment.as_ref())
    {
        client
            .reserve_candidate(
                &super::super::ReplicaPlacementTarget::Segment {
                    storage_runtime: target_runtime.clone(),
                    segment_name: target_segment.clone(),
                },
                task.payload.len(),
            )
            .map(|(target, reservation)| (vec![target], vec![reservation]))
    } else {
        client.reserve_replica_targets(&object_ref, task.payload.len(), &policy)
    };
    reserve_tracker.finish(&reserve_result, 0);
    let (targets, reservations) = reserve_result?;

    let write_tracker =
        OperationTracker::new("restore_promote_write").input_bytes(task.payload.len() as u64);
    let write_result = client.write_reserved_replicas(&targets, &reservations, &task.payload, None);
    write_tracker.finish(&write_result, task.payload.len() as u64);
    if let Err(error) = write_result {
        client.best_effort_release_reserved_allocations(
            &targets,
            &reservations,
            "restore_promote_write_failed",
        );
        client.note_remote_write_failure(&targets, &error, "restore_promote_write_failed");
        return Err(error);
    }

    let checksum = payload_checksum(&task.payload);
    let mut next = task.current.clone();
    next.version = task.current.version.next();
    next.state = RouteState::Active;
    next.replicas = targets
        .iter()
        .enumerate()
        .map(|(priority, target)| ReplicaRoute {
            owner: target.storage_runtime.clone(),
            segment_name: target.segment_name.clone(),
            offset: None,
            segment_offset: reservations[priority].offset_bytes,
            length: task.payload.len() as u64,
            checksum: Some(checksum),
            tier: ReplicaTier::Dram,
            priority: priority as u16,
        })
        .collect();
    next.cold_backing = task.current.cold_backing.clone();
    mooncake_store_core::apply_route_identity(&mut next, &task.object_id);
    next.qos_tier = task.qos_tier.clone();

    let cas_tracker = OperationTracker::new("restore_promote_route_cas");
    let cas_result = client.route_directory.compare_and_swap_object_route(
        &client.lease,
        &task.current.key,
        Some(task.current.version),
        Some(&next),
    );
    cas_tracker.finish(&cas_result, 0);
    let cas = match cas_result {
        Ok(cas) => cas,
        Err(error) => {
            client.best_effort_release_reserved_allocations(
                &targets,
                &reservations,
                "restore_promote_route_cas_error",
            );
            return Err(error);
        }
    };
    if !cas.applied {
        client.best_effort_release_reserved_allocations(
            &targets,
            &reservations,
            "restore_promote_route_cas_conflict",
        );
        return Err(StoreError::Conflict(format!(
            "restore promotion route update lost race for tenant={tenant} key={key}"
        )));
    }

    client.storage_owner.track_route(&next);
    client.track_remote_storage_owners_best_effort(std::slice::from_ref(&next));
    Ok(())
}

fn restore_payload_from_cold_backing_singleflight_into(
    client: &StoreClient,
    resolved: &ResolvedObject,
    buffer: &mut [u8],
) -> Result<Option<Arc<Vec<u8>>>> {
    let route_tracker = OperationTracker::new("cold_restore_route_check");
    let cold_backing = materialized_cold_backing(&resolved.route).ok_or_else(|| {
        StoreError::NotFound(format!(
            "tenant={} key={} has no materialized cold backing",
            resolved.tenant, resolved.key
        ))
    });
    route_tracker.finish(&cold_backing, 0);
    let cold_backing = cold_backing?;
    let key = cold_restore_flight_key(&resolved.route, &cold_backing);
    let singleflight_tracker = OperationTracker::new("cold_restore_singleflight_begin");
    let registration = client.cold_restore_flights.begin(key);
    singleflight_tracker.finish_with_result(
        match &registration {
            ColdRestoreFlightRegistration::Leader(_) => "leader",
            ColdRestoreFlightRegistration::Waiter(_) => "waiter",
            ColdRestoreFlightRegistration::Rejected => "rejected",
        },
        0,
    );
    match registration {
        ColdRestoreFlightRegistration::Leader(leader) => {
            debug!(
                runtime = %client.lease.runtime,
                tenant = %resolved.tenant,
                key = %resolved.key,
                route_key = %resolved.route.key.0,
                route_version = resolved.route.version.0,
                singleflight_role = "leader",
                "mooncake store cold restore singleflight entered"
            );
            let result = (|| {
                let admission_tracker = OperationTracker::new("cold_restore_admission");
                let permit_result = client
                    .storage_owner
                    .cold_tier_devices
                    .try_acquire_restore(cold_tier_device_id(&cold_backing));
                admission_tracker.finish_with_result(
                    if permit_result.is_ok() {
                        "ok"
                    } else {
                        "reject"
                    },
                    0,
                );
                let permit = match permit_result {
                    Ok(permit) => permit,
                    Err(reason) => {
                        debug!(
                            runtime = %client.lease.runtime,
                            tenant = %resolved.tenant,
                            key = %resolved.key,
                            reason = reason.metric_label(),
                            "singleflight leader admission rejected"
                        );
                        registry::record_cold_tier_operation(
                            "restore",
                            "admission_reject",
                            reason.metric_label(),
                        );
                        return Err(StoreError::Transport(format!(
                            "cold tier restore backpressure: {}",
                            reason.metric_label()
                        )));
                    }
                };
                let backend_resolve_tracker = OperationTracker::new("cold_restore_backend_resolve");
                let backend_result = client
                    .storage_owner
                    .cold_tier_devices
                    .backend_for_with_refresh(
                        client.metadata.as_ref(),
                        &cold_backing,
                        "cold_restore_backend_resolve_refresh",
                    );
                backend_resolve_tracker.finish(&backend_result, 0);
                let backend = backend_result?;
                let backend_read_tracker =
                    OperationTracker::new("cold_restore_backend_read_into_caller");
                let read_result = backend.get_object_into(&cold_backing, buffer);
                backend_read_tracker.finish(
                    &read_result,
                    read_result
                        .as_ref()
                        .ok()
                        .and_then(|length| *length)
                        .unwrap_or_default() as u64,
                );
                let length = match read_result {
                    Ok(Some(length)) => {
                        debug!(
                            runtime = %client.lease.runtime,
                            tenant = %resolved.tenant,
                            key = %resolved.key,
                            bytes = length,
                            backend_read = "some",
                            "mooncake store cold restore backend read returned payload"
                        );
                        permit.complete_ok();
                        length
                    }
                    Ok(None) => {
                        warn!(
                            runtime = %client.lease.runtime,
                            tenant = %resolved.tenant,
                            key = %resolved.key,
                            backend_read = "none",
                            "cold restore backend read returned no payload"
                        );
                        permit.complete_error();
                        return Err(StoreError::NotFound(format!(
                            "tenant={} key={} cold backing payload is unavailable",
                            resolved.tenant, resolved.key
                        )));
                    }
                    Err(error) => {
                        warn!(
                            runtime = %client.lease.runtime,
                            tenant = %resolved.tenant,
                            key = %resolved.key,
                            backend_read = "error",
                            error = %error,
                            "cold restore backend read error"
                        );
                        permit.complete_error();
                        return Err(error);
                    }
                };
                let validation_tracker = OperationTracker::new("cold_restore_validate");
                let validation_result = validate_cold_restore_payload(resolved, &buffer[..length]);
                validation_tracker.finish(&validation_result, length as u64);
                validation_result?;
                Ok(Arc::new(buffer[..length].to_vec()))
            })();
            leader.finish(result).map(Some)
        }
        ColdRestoreFlightRegistration::Waiter(flight) => {
            debug!(
                runtime = %client.lease.runtime,
                tenant = %resolved.tenant,
                key = %resolved.key,
                route_key = %resolved.route.key.0,
                route_version = resolved.route.version.0,
                singleflight_role = "waiter",
                "mooncake store cold restore singleflight entered"
            );
            let wait_tracker = OperationTracker::new("cold_restore_singleflight_wait");
            let result = client.cold_restore_flights.wait(&flight);
            wait_tracker.finish(
                &result,
                result
                    .as_ref()
                    .map(|payload| payload.len())
                    .unwrap_or_default() as u64,
            );
            let payload = result?;
            let copy_tracker = OperationTracker::new("cold_restore_copy_to_caller");
            buffer[..payload.len()].copy_from_slice(&payload);
            copy_tracker.finish_with_result("ok", payload.len() as u64);
            Ok(Some(payload))
        }
        ColdRestoreFlightRegistration::Rejected => {
            debug!(
                runtime = %client.lease.runtime,
                tenant = %resolved.tenant,
                key = %resolved.key,
                route_key = %resolved.route.key.0,
                route_version = resolved.route.version.0,
                "singleflight rejected (too many distinct flights)"
            );
            registry::record_cold_tier_operation(
                "restore",
                "admission_reject",
                "distinct_flight_limit",
            );
            Err(StoreError::Transport(
                "cold tier restore backpressure: too many distinct restore flights".to_string(),
            ))
        }
    }
}

fn restore_batch_payloads_from_cold_backing_grouped(
    client: &StoreClient,
    resolved: &[ResolvedObject],
    buffers: &mut [&mut [u8]],
    indices: &[usize],
) -> Result<Vec<(usize, Arc<Vec<u8>>)>> {
    let mut leaders = Vec::new();
    let mut promotion_payloads = Vec::new();
    for index in indices {
        let Some(cold_backing) = materialized_cold_backing(&resolved[*index].route) else {
            restore_payload_from_cold_backing(client, &resolved[*index], buffers[*index])?;
            continue;
        };
        let key = cold_restore_flight_key(&resolved[*index].route, &cold_backing);
        let begin_tracker = OperationTracker::new("cold_restore_batch_singleflight_begin");
        let registration = client.cold_restore_flights.begin(key);
        begin_tracker.finish_with_result(
            match &registration {
                ColdRestoreFlightRegistration::Leader(_) => "leader",
                ColdRestoreFlightRegistration::Waiter(_) => "waiter",
                ColdRestoreFlightRegistration::Rejected => "rejected",
            },
            0,
        );
        match registration {
            ColdRestoreFlightRegistration::Leader(leader) => {
                leaders.push((*index, cold_backing, leader));
            }
            ColdRestoreFlightRegistration::Waiter(flight) => {
                debug!(
                    runtime = %client.lease.runtime,
                    tenant = %resolved[*index].tenant,
                    key = %resolved[*index].key,
                    route_key = %resolved[*index].route.key.0,
                    route_version = resolved[*index].route.version.0,
                    singleflight_role = "waiter",
                    "mooncake store cold restore batch singleflight entered"
                );
                let wait_tracker = OperationTracker::new("cold_restore_batch_singleflight_wait");
                let wait_result = client.cold_restore_flights.wait(&flight);
                wait_tracker.finish(
                    &wait_result,
                    wait_result
                        .as_ref()
                        .map(|payload| payload.len())
                        .unwrap_or_default() as u64,
                );
                let payload = wait_result?;
                buffers[*index][..payload.len()].copy_from_slice(&payload);
                promotion_payloads.push((*index, payload));
            }
            ColdRestoreFlightRegistration::Rejected => {
                debug!(
                    runtime = %client.lease.runtime,
                    tenant = %resolved[*index].tenant,
                    key = %resolved[*index].key,
                    route_key = %resolved[*index].route.key.0,
                    route_version = resolved[*index].route.version.0,
                    "batch singleflight rejected (too many distinct flights)"
                );
                registry::record_cold_tier_operation(
                    "restore",
                    "admission_reject",
                    "distinct_flight_limit",
                );
                return Err(StoreError::Transport(
                    "cold tier restore backpressure: too many distinct restore flights".to_string(),
                ));
            }
        }
    }
    if leaders.is_empty() {
        return Ok(promotion_payloads);
    }
    // Per-IO target selection: for multi-replica objects, rewrite each
    // leader's routing fields to the selected target before grouping.
    // Three-layer LoadBalance: (inflight, batch_ios, global_accum).
    //
    // Filter to local-only targets before selection: the local cold read
    // path must only choose among local devices.  Remote targets would
    // appear to have zero inflight and win unfairly, but cannot be read
    // via local SSD — they require the remote gRPC path.
    {
        let is_local = |id: &str| client.storage_owner.cold_tier_devices.has_local_backend(id);
        let mut selector = super::target_selection::ColdTierTargetSelector::new(
            client.storage_owner.cold_tier_devices.admission(),
        );
        for (_, cold_backing, _) in &mut leaders {
            if !cold_backing.replicas.is_empty() {
                // Ensure primary is local (swap with a local replica if needed),
                // then drop all remote replicas so select_target only sees
                // local devices.
                super::super::select_cold_backing_target(cold_backing, |id| is_local(id));
                cold_backing.replicas.retain(|r| is_local(&r.cold_tier_id));
                let result = selector.select_target(cold_backing);
                if result.cold_tier_id != cold_backing.cold_tier_id
                    || result.owner != cold_backing.owner
                {
                    cold_backing.owner = result.owner;
                    cold_backing.cold_tier_id = result.cold_tier_id;
                    cold_backing.object_locator = result.object_locator;
                }
            }
        }
    }
    leaders.sort_by(|left, right| cold_tier_device_id(&left.1).cmp(cold_tier_device_id(&right.1)));
    while !leaders.is_empty() {
        let device_id = cold_tier_device_id(&leaders[0].1).to_string();
        let mut end = 1;
        while end < leaders.len() && cold_tier_device_id(&leaders[end].1) == device_id {
            end += 1;
        }
        let group = leaders.drain(..end).collect::<Vec<_>>();
        promotion_payloads.extend(restore_batch_leaders_from_same_cold_tier(
            client, resolved, buffers, group,
        )?);
    }
    Ok(promotion_payloads)
}

fn restore_batch_leaders_from_same_cold_tier(
    client: &StoreClient,
    resolved: &[ResolvedObject],
    buffers: &mut [&mut [u8]],
    leaders: Vec<(
        usize,
        mooncake_store_core::ColdBackingRoute,
        ColdRestoreFlightLeader,
    )>,
) -> Result<Vec<(usize, Arc<Vec<u8>>)>> {
    let Some((_, first_cold_backing, _)) = leaders.first() else {
        return Ok(Vec::new());
    };
    let first_cold_tier_id = first_cold_backing.cold_tier_id.clone();
    let backend_tracker = OperationTracker::new("cold_restore_batch_backend_resolve");
    let backend_result = client
        .storage_owner
        .cold_tier_devices
        .backend_for_with_refresh(
            client.metadata.as_ref(),
            first_cold_backing,
            "cold_restore_batch_backend_resolve_refresh",
        );
    backend_tracker.finish(&backend_result, 0);
    let backend = backend_result?;
    let mut permit_results = Vec::with_capacity(leaders.len());
    for (_, cold_backing, _) in &leaders {
        let admission_tracker = OperationTracker::new("cold_restore_batch_admission");
        let permit_result = client
            .storage_owner
            .cold_tier_devices
            .try_acquire_restore(cold_tier_device_id(cold_backing));
        admission_tracker.finish_with_result(
            if permit_result.is_ok() {
                "ok"
            } else {
                "reject"
            },
            0,
        );
        permit_results.push(permit_result);
    }
    let total_count = leaders.len();
    let readable_count = permit_results
        .iter()
        .filter(|result| result.is_ok())
        .count();
    info!(
        runtime = %client.lease.runtime,
        cold_tier_id = %first_cold_tier_id,
        leaders = total_count,
        admitted = readable_count,
        "mooncake store cold restore grouped leader batch prepared"
    );
    let read_requests = leaders
        .iter()
        .enumerate()
        .filter(|(offset, _)| permit_results[*offset].is_ok())
        .map(|(_, (index, cold_backing, _))| (*index, cold_backing.clone()))
        .collect::<Vec<_>>();
    let read_tracker = OperationTracker::new("cold_restore_batch_backend_read_into_caller");
    let read_results = with_disjoint_restore_caller_buffers(buffers, &read_requests, |reads| {
        backend_load_cold_payload_batch_into(backend.as_ref(), reads)
    });
    read_tracker.finish_with_result(
        if read_results.iter().all(|(_, result)| result.is_ok()) {
            "ok"
        } else {
            "error"
        },
        read_requests
            .iter()
            .map(|(index, _)| resolved[*index].replica.length)
            .sum::<u64>(),
    );
    let mut read_results = read_results.into_iter();
    let mut promotion_payloads = Vec::new();
    let mut first_error = None;
    for ((index, cold_backing, leader), permit_result) in leaders.into_iter().zip(permit_results) {
        let result = match permit_result {
            Ok(permit) => {
                let read_result = read_results
                    .next()
                    .ok_or_else(|| {
                        warn!(
                            runtime = %client.lease.runtime,
                            tenant = %resolved[index].tenant,
                            key = %resolved[index].key,
                            "cold restore batch result missing"
                        );
                        StoreError::InvalidState("cold restore batch result missing".to_string())
                    })
                    .and_then(|(read_index, result)| {
                        if read_index == index {
                            result
                        } else {
                            warn!(
                                runtime = %client.lease.runtime,
                                tenant = %resolved[index].tenant,
                                key = %resolved[index].key,
                                expected_index = index,
                                actual_index = read_index,
                                "cold restore batch result index mismatch"
                            );
                            Err(StoreError::InvalidState(format!(
                                "cold restore batch result index mismatch: expected {index} actual {read_index}"
                            )))
                        }
                    });
                match read_result {
                    Ok(Some(length)) => {
                        debug!(
                            runtime = %client.lease.runtime,
                            tenant = %resolved[index].tenant,
                            key = %resolved[index].key,
                            bytes = length,
                            backend_read = "some",
                            "mooncake store cold restore batch backend read returned payload"
                        );
                        permit.complete_ok();
                        validate_cold_restore_payload(&resolved[index], &buffers[index][..length])?;
                        Ok(Arc::new(buffers[index][..length].to_vec()))
                    }
                    Ok(None) => {
                        warn!(
                            runtime = %client.lease.runtime,
                            tenant = %resolved[index].tenant,
                            key = %resolved[index].key,
                            backend_read = "none",
                            "cold restore batch backend read returned no payload"
                        );
                        permit.complete_error();
                        Err(StoreError::NotFound(format!(
                            "tenant={} key={} cold backing payload is unavailable",
                            resolved[index].tenant, resolved[index].key
                        )))
                    }
                    Err(error) => {
                        warn!(
                            runtime = %client.lease.runtime,
                            tenant = %resolved[index].tenant,
                            key = %resolved[index].key,
                            backend_read = "error",
                            error = %error,
                            "cold restore batch backend read error"
                        );
                        permit.complete_error();
                        Err(error)
                    }
                }
            }
            Err(reason) => {
                registry::record_cold_tier_operation(
                    "restore",
                    "admission_reject",
                    reason.metric_label(),
                );
                Err(StoreError::Transport(format!(
                    "cold tier restore backpressure: {}",
                    reason.metric_label()
                )))
            }
        };
        let payload_result = leader.finish(result);
        match payload_result {
            Ok(payload) => {
                if payload.len() > buffers[index].len() {
                    return Err(StoreError::InvalidState(format!(
                        "cold tier restore payload ({} bytes) exceeds buffer ({} bytes)",
                        payload.len(),
                        buffers[index].len(),
                    )));
                }
                if buffers[index][..payload.len()] != payload[..] {
                    buffers[index][..payload.len()].copy_from_slice(&payload);
                }
                promotion_payloads.push((index, payload));
            }
            Err(error) => {
                warn!(
                    runtime = %client.lease.runtime,
                    cold_tier_id = %cold_backing.cold_tier_id,
                    object_locator = %cold_backing.object_locator,
                    error = %error,
                    "cold tier batch payload restore failed"
                );
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    debug!(
        runtime = %client.lease.runtime,
        cold_tier_id = %first_cold_tier_id,
        submitted = readable_count,
        total = total_count,
        "cold tier batch restore completed for one cold tier"
    );
    Ok(promotion_payloads)
}

fn owner_materialized_cold_backing<'a>(
    storage_owner: &StorageOwnerState,
    route: &'a ObjectRoute,
) -> Option<&'a mooncake_store_core::ColdBackingRoute> {
    route.cold_backing.as_ref().filter(|cold_backing| {
        route.state == RouteState::Active
            && route.replicas.is_empty()
            && cold_backing.owner == storage_owner.runtime
            && cold_backing.state == mooncake_store_core::ColdBackingState::Materialized
    })
}

fn route_restore_race_resolved(storage_owner: &StorageOwnerState, route: &ObjectRoute) -> bool {
    route
        .replicas
        .iter()
        .any(|replica| replica.owner == storage_owner.runtime)
        || owner_materialized_cold_backing(storage_owner, route).is_none()
}

fn release_owner_restore_reservation(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
    reservation: &mooncake_store_core::SegmentReservation,
    release_reason: &str,
) {
    if let Err(error) = storage_owner.allocator.lock().release(
        &storage_owner.runtime,
        &reservation.segment_name,
        reservation.offset_bytes,
        reservation.length_bytes,
    ) {
        warn!(
            runtime = %storage_owner.runtime,
            key = %route.key.0,
            segment = %reservation.segment_name.0,
            offset = reservation.offset_bytes,
            length = reservation.length_bytes,
            error = %error,
            release_reason,
            "failed to release unpublished cold restore reservation"
        );
    }
}

fn build_owner_restored_route(
    storage_owner: &StorageOwnerState,
    base: &ObjectRoute,
    object_id: &mooncake_store_core::LogicalObjectId,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
    reservation: &mooncake_store_core::SegmentReservation,
) -> ObjectRoute {
    // Compute the absolute RDMA target offset so that readers can use the
    // fast path in remote_replica_target_offset.  Without this the fallback
    // walks raw transport buffers and lands in the scratch region (all zeros)
    // instead of the storage region where the payload was promoted.
    let absolute_offset = storage_owner
        .state
        .lock()
        .memory_ref()
        .ok()
        .and_then(|memory| {
            memory
                .storage_address(&reservation.segment_name, reservation.offset_bytes as usize)
                .ok()
                .map(|addr| addr as u64)
        });
    let mut next = base.clone();
    next.version = base.version.next();
    next.replicas = vec![ReplicaRoute {
        owner: storage_owner.runtime.clone(),
        segment_name: reservation.segment_name.clone(),
        offset: absolute_offset,
        segment_offset: reservation.offset_bytes,
        length: cold_backing.length,
        checksum: cold_backing.checksum,
        tier: ReplicaTier::Dram,
        priority: 0,
    }];
    mooncake_store_core::apply_route_identity(&mut next, object_id);
    next
}

fn sync_current_route_by_key(
    storage_owner: &StorageOwnerState,
    route_key: &ObjectKey,
) -> Result<Option<ObjectRoute>> {
    let current = storage_owner.route_ops.load_route(route_key)?;
    if let Some(current) = current.as_ref() {
        storage_owner.sync_route(current);
    }
    Ok(current)
}

fn cas_owner_restored_route(
    storage_owner: &StorageOwnerState,
    current: &ObjectRoute,
    next: &ObjectRoute,
) -> Result<Option<ObjectRoute>> {
    let cas = storage_owner.route_ops.compare_and_swap_route(
        &current.key,
        Some(current.version),
        Some(next),
    );
    match cas {
        Ok(cas) if cas.applied => {
            storage_owner.track_route(next);
            Ok(None)
        }
        Ok(cas) => {
            if let Some(current) = cas.current.as_ref() {
                storage_owner.sync_route(current);
            }
            Ok(cas.current)
        }
        Err(StoreError::Conflict(_)) => sync_current_route_by_key(storage_owner, &current.key),
        Err(error) => Err(error),
    }
}

#[allow(dead_code)]
pub(in super::super) struct PromoteTimingBreakdown {
    pub admission_wait_us: u64,
    pub ssd_read_us: u64,
    pub memcpy_us: u64,
    pub route_update_us: u64,
    pub total_us: u64,
}

fn promote_materialized_cold_backing(
    storage_owner: &StorageOwnerState,
    route: &ObjectRoute,
) -> Result<Option<(u64, PromoteTimingBreakdown)>> {
    let total_start = std::time::Instant::now();
    let Some(cold_backing) = owner_materialized_cold_backing(storage_owner, route) else {
        storage_owner.sync_route(route);
        return Ok(None);
    };

    let object_id = mooncake_store_core::route_logical_object_id(route)?;

    // Reserve DRAM segment space.  Uses the same kick-offload + evict-one
    // loop as the PUT path — no separate admission gate needed.
    let t_reserve = std::time::Instant::now();
    let reservation = storage_owner.reserve_owner_restore_space(cold_backing.length)?;
    let admission_wait_us = t_reserve.elapsed().as_micros() as u64;
    let t_ssd = std::time::Instant::now();
    let ssd_read_result = (|| -> Result<()> {
        let addr = {
            let state = storage_owner.state.lock();
            state
                .memory_ref()?
                .storage_address(&reservation.segment_name, reservation.offset_bytes as usize)?
        };
        // SAFETY: `reservation` holds exclusive ownership of the byte range
        // [offset, offset+length) within the segment. The segment's backing memory is
        // pinned DRAM (hugepage mmap, 2MB aligned) that remains valid for the
        // StorageOwner's lifetime. The address is stable because the memory region
        // is never remapped while the reservation is live.
        let dst = unsafe {
            std::slice::from_raw_parts_mut(addr.cast::<u8>(), cold_backing.length as usize)
        };

        // Resolve backend and read SSD directly into segment memory.
        let backend = storage_owner.cold_tier_devices.backend_for_with_refresh(
            storage_owner.metadata.as_ref(),
            cold_backing,
            "cold_restore_owner_direct_to_segment",
        )?;
        let read_result = backend.get_object_into(cold_backing, dst)?;
        let bytes_read = read_result.ok_or_else(|| {
            StoreError::NotFound(format!(
                "route {} cold backing payload is unavailable (direct read)",
                route.key.0
            ))
        })?;
        if bytes_read != cold_backing.length as usize {
            return Err(StoreError::InvalidState(format!(
                "route {} cold backing length mismatch: expected {} actual {}",
                route.key.0, cold_backing.length, bytes_read
            )));
        }
        // Verify checksum in-place (data is already in segment).
        if let Some(expected) = cold_backing.checksum {
            let actual = payload_checksum(&dst[..bytes_read]);
            if actual != expected {
                return Err(StoreError::InvalidState(format!(
                    "checksum mismatch for cold backing {}:{} expected={} actual={}",
                    cold_backing.owner, cold_backing.object_locator, expected, actual
                )));
            }
        }
        Ok(())
    })();
    let ssd_read_elapsed = t_ssd.elapsed();
    let ssd_read_us = ssd_read_elapsed.as_micros() as u64;
    super::super::registry::record_cold_tier_ssd_read(
        if ssd_read_result.is_ok() {
            "ok"
        } else {
            "error"
        },
        ssd_read_elapsed,
    );
    let ssd_read_ms = ssd_read_elapsed.as_secs_f64() * 1000.0;
    if ssd_read_ms > 10.0 {
        warn!(
            runtime = %storage_owner.runtime,
            ssd_read_ms = format!("{ssd_read_ms:.3}"),
            object_locator = %cold_backing.object_locator,
            "ssd_read_slow: SSD restore read exceeded 10ms"
        );
    }

    if let Err(error) = ssd_read_result {
        release_owner_restore_reservation(storage_owner, route, &reservation, "ssd_read_error");
        return Err(error);
    }

    // Route CAS update (memcpy_us is 0 since we wrote directly to segment)
    let t_route = std::time::Instant::now();
    let route_result = (|| {
        let next = build_owner_restored_route(
            storage_owner,
            route,
            &object_id,
            cold_backing,
            &reservation,
        );
        let Some(current) = cas_owner_restored_route(storage_owner, route, &next)? else {
            return Ok(true);
        };
        if route_restore_race_resolved(storage_owner, &current)
            || current.cold_backing.as_ref() != Some(cold_backing)
        {
            return Ok(false);
        }
        let next = build_owner_restored_route(
            storage_owner,
            &current,
            &object_id,
            cold_backing,
            &reservation,
        );
        Ok(cas_owner_restored_route(storage_owner, &current, &next)?.is_none())
    })();
    let route_update_us = t_route.elapsed().as_micros() as u64;

    let timing = PromoteTimingBreakdown {
        admission_wait_us,
        ssd_read_us,
        memcpy_us: 0, // Direct-to-segment: no separate memcpy step
        route_update_us,
        total_us: total_start.elapsed().as_micros() as u64,
    };

    match route_result {
        Ok(true) => Ok(Some((cold_backing.length, timing))),
        Ok(false) => {
            release_owner_restore_reservation(storage_owner, route, &reservation, "route_race");
            Ok(None)
        }
        Err(error) => {
            release_owner_restore_reservation(storage_owner, route, &reservation, "error");
            Err(error)
        }
    }
}

// ---------------------------------------------------------------------------
// Batch ReadFromCold handler (owner-side batched cold read for remote readers)
// ---------------------------------------------------------------------------

/// Internal entry for batch cold read processing. One per target that needs
/// SSD I/O (after singleflight dedup).
struct BatchEntry {
    index: usize,
    route: ObjectRoute,
    cold_backing: mooncake_store_core::ColdBackingRoute,
    object_id: mooncake_store_core::LogicalObjectId,
    leader: Option<super::super::OwnerColdRestoreFlightLeader>,
}

/// Batch variant of `read_from_cold_one_shot`. Processes multiple cold read
/// targets using batched SSD I/O per device (sorted sequential reads) instead
/// of dispatching each target to an independent read pool thread.
///
/// Flow: resolve routes → singleflight dedup (non-blocking) → group by device
/// → batch staging alloc → batch SSD I/O → checksum → publish + pin.
pub(in super::super) fn batch_read_from_cold_staged(
    storage_owner: &Arc<StorageOwnerState>,
    allocator: &Arc<parking_lot::Mutex<LocalAllocatorState>>,
    targets: Vec<crate::control_plane::ColdReadTarget>,
) -> Vec<crate::control_plane::ColdReadResponse> {
    let started = std::time::Instant::now();
    let target_count = targets.len();

    // Result slots — one per input target, filled throughout the phases.
    let mut results: Vec<Option<crate::control_plane::ColdReadResponse>> =
        (0..target_count).map(|_| None).collect();

    // Phase 1: Resolve routes + extract cold backings.
    struct ResolvedTarget {
        index: usize,
        route_key: ObjectKey,
        route: ObjectRoute,
        cold_backing: mooncake_store_core::ColdBackingRoute,
        object_id: mooncake_store_core::LogicalObjectId,
    }

    let mut resolved_targets: Vec<ResolvedTarget> = Vec::with_capacity(target_count);

    for (index, target) in targets.iter().enumerate() {
        let scope = mooncake_store_core::NamespaceScope::with_defaults(
            Some(&target.tenant),
            if target.domain.is_empty() {
                None
            } else {
                Some(&target.domain)
            },
            if target.object_set.is_empty() {
                None
            } else {
                Some(&target.object_set)
            },
        );
        let object_id = mooncake_store_core::LogicalObjectId::new(scope, &target.key);
        let route_key = ObjectKey::from_logical_id(&object_id);

        let route = match storage_owner.current_materialized_route(&route_key) {
            Ok(Some(route)) => route,
            Ok(None) => {
                results[index] = Some(crate::control_plane::ColdReadResponse {
                    result: Err(StoreError::NotFound(format!(
                        "route_key={} has no materialized cold-only route to promote",
                        route_key.0
                    ))),
                    deferred_promote: None,
                });
                continue;
            }
            Err(e) => {
                results[index] = Some(crate::control_plane::ColdReadResponse {
                    result: Err(e),
                    deferred_promote: None,
                });
                continue;
            }
        };

        let cold_backing = match owner_materialized_cold_backing(storage_owner, &route) {
            Some(cb) => cb.clone(),
            None => {
                storage_owner.sync_route(&route);
                results[index] = Some(crate::control_plane::ColdReadResponse {
                    result: Err(StoreError::NotFound(format!(
                        "route_key={} cold backing not owned/materialized",
                        route_key.0
                    ))),
                    deferred_promote: None,
                });
                continue;
            }
        };

        resolved_targets.push(ResolvedTarget {
            index,
            route_key,
            route,
            cold_backing,
            object_id,
        });
    }

    let resolved_count = resolved_targets.len();

    // Phase 2: Singleflight dedup (non-blocking).
    // Leaders → batch SSD read. Waiters → try_wait, fallback to batch read.
    let mut batch_entries: Vec<BatchEntry> = Vec::with_capacity(resolved_count);

    for rt in resolved_targets {
        let role = storage_owner
            .owner_cold_restore_flights
            .acquire(&rt.route_key);
        match role {
            super::super::OwnerColdRestoreFlightRole::Leader(leader) => {
                batch_entries.push(BatchEntry {
                    index: rt.index,
                    route: rt.route,
                    cold_backing: rt.cold_backing,
                    object_id: rt.object_id,
                    leader: Some(leader),
                });
            }
            super::super::OwnerColdRestoreFlightRole::Waiter(flight) => {
                // Non-blocking: check if leader already published.
                match storage_owner.owner_cold_restore_flights.try_wait(&flight) {
                    Some(Ok(outcome)) => {
                        // Leader already done — reuse result, pin + build response.
                        storage_owner.read_pin_registry.pin(
                            &mooncake_store_core::SegmentName::new(&outcome.segment_name),
                            outcome.segment_offset,
                        );
                        results[rt.index] = Some(crate::control_plane::ColdReadResponse {
                            result: Ok(outcome_to_cold_read_result(&outcome)),
                            deferred_promote: None,
                        });
                    }
                    Some(Err(e)) => {
                        results[rt.index] = Some(crate::control_plane::ColdReadResponse {
                            result: Err(e),
                            deferred_promote: None,
                        });
                    }
                    None => {
                        // Leader still in-flight on another thread — don't block.
                        // Include in batch SSD read (marginal sequential I/O cost).
                        debug!(
                            runtime = %storage_owner.runtime,
                            route_key = %rt.route.key.0,
                            "batch_read_from_cold: waiter bypassing singleflight for batch I/O"
                        );
                        batch_entries.push(BatchEntry {
                            index: rt.index,
                            route: rt.route,
                            cold_backing: rt.cold_backing,
                            object_id: rt.object_id,
                            leader: None, // no leader token — won't publish
                        });
                    }
                }
            }
            super::super::OwnerColdRestoreFlightRole::Rejected => {
                batch_entries.push(BatchEntry {
                    index: rt.index,
                    route: rt.route,
                    cold_backing: rt.cold_backing,
                    object_id: rt.object_id,
                    leader: None,
                });
            }
        }
    }

    if batch_entries.is_empty() {
        use crate::observability::registry::{
            record_cold_restore_batch_duration, record_cold_restore_batch_items,
        };
        record_cold_restore_batch_duration("wall", started.elapsed());
        record_cold_restore_batch_items("resolved_no_ssd", target_count as u64);
        return results
            .into_iter()
            .map(|r| {
                r.unwrap_or_else(|| crate::control_plane::ColdReadResponse {
                    result: Err(StoreError::InvalidState("batch result missing".to_string())),
                    deferred_promote: None,
                })
            })
            .collect();
    }

    // Phase 3: Group by device_id (sort + split).
    let device_groups = split_into_device_groups(batch_entries);

    // Phase 4: Per-device batch SSD I/O.
    let staging_pool = match get_or_init_staging_pool(storage_owner) {
        Ok(pool) => pool,
        Err(e) => {
            warn!(
                runtime = %storage_owner.runtime,
                error = %e,
                "batch_read_from_cold: staging pool init failed"
            );
            for (_, group) in device_groups {
                for entry in group {
                    // Drop leaders → publish error to waiters via Drop.
                    results[entry.index] = Some(crate::control_plane::ColdReadResponse {
                        result: Err(StoreError::InvalidState(
                            "staging pool unavailable".to_string(),
                        )),
                        deferred_promote: None,
                    });
                }
            }
            return finalize_batch_results(results);
        }
    };

    // Parallel per-device SSD I/O — one scoped thread per device group.
    // std::thread::scope guarantees all spawned threads are joined before
    // the scope exits, preventing thread leaks in all code paths (including
    // child thread panics).
    let parallel_start = std::time::Instant::now();
    let all_group_results: Vec<Vec<(usize, crate::control_plane::ColdReadResponse)>> =
        std::thread::scope(|s| {
            let handles: Vec<_> = device_groups
                .into_iter()
                .map(|(device_id, group)| {
                    let so = &storage_owner;
                    let al = &allocator;
                    let sp = &staging_pool;
                    let batch_start = started;
                    s.spawn(move || {
                        process_device_group(so, al, sp, &device_id, group, batch_start)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_default())
                .collect()
        });

    // Merge per-device results into the top-level results array.
    for (index, response) in all_group_results.into_iter().flatten() {
        results[index] = Some(response);
    }

    // Record cold restore batch metrics
    use crate::observability::registry::{
        record_cold_restore_batch_duration, record_cold_restore_batch_items,
    };
    record_cold_restore_batch_duration("parallel", parallel_start.elapsed());
    record_cold_restore_batch_duration("wall", started.elapsed());
    record_cold_restore_batch_items("complete", target_count as u64);

    finalize_batch_results(results)
}

/// Sort batch entries by device_id and split into per-device groups.
fn split_into_device_groups(mut entries: Vec<BatchEntry>) -> Vec<(String, Vec<BatchEntry>)> {
    entries.sort_by(|a, b| {
        cold_tier_device_id(&a.cold_backing).cmp(cold_tier_device_id(&b.cold_backing))
    });
    let mut groups = Vec::new();
    while !entries.is_empty() {
        let device_id = cold_tier_device_id(&entries[0].cold_backing).to_string();
        let split_at = entries
            .iter()
            .position(|e| cold_tier_device_id(&e.cold_backing) != device_id)
            .unwrap_or(entries.len());
        groups.push((device_id, entries.drain(..split_at).collect()));
    }
    groups
}

/// Process one device group: backend resolve → staging alloc → batch SSD I/O
/// → checksum → publish + pin → build responses with deferred promote.
///
/// Returns `(target_index, ColdReadResponse)` pairs for every entry in the
/// group.  The caller merges these into the top-level results array.
fn process_device_group(
    storage_owner: &Arc<StorageOwnerState>,
    allocator: &Arc<parking_lot::Mutex<LocalAllocatorState>>,
    staging_pool: &std::sync::Arc<super::staging_pool::ColdRestoreStagingPool>,
    device_id: &str,
    mut group: Vec<BatchEntry>,
    batch_started: std::time::Instant,
) -> Vec<(usize, crate::control_plane::ColdReadResponse)> {
    let mut out: Vec<(usize, crate::control_plane::ColdReadResponse)> =
        Vec::with_capacity(group.len());

    // 4a. 1x backend resolve per device group.
    let backend = match storage_owner.cold_tier_devices.backend_for_with_refresh(
        storage_owner.metadata.as_ref(),
        &group[0].cold_backing,
        "batch_read_from_cold_backend_resolve",
    ) {
        Ok(b) => b,
        Err(e) => {
            warn!(
                runtime = %storage_owner.runtime,
                device_id,
                error = %e,
                "batch_read_from_cold: backend resolve failed for device group"
            );
            for entry in group {
                out.push((
                    entry.index,
                    cold_read_error(StoreError::NotFound(format!(
                        "backend resolve failed for device {device_id}: {e}"
                    ))),
                ));
            }
            return out;
        }
    };

    // 4b-c. Per-entry: admission + staging alloc.
    // Use Option<StagingSlot> parallel to `group` — None means failed alloc.
    let mut staged_slots: Vec<Option<super::staging_pool::StagingSlot>> =
        Vec::with_capacity(group.len());
    let mut restore_permits = Vec::with_capacity(group.len());

    for entry in group.iter() {
        restore_permits.push(
            storage_owner
                .cold_tier_devices
                .try_acquire_restore(cold_tier_device_id(&entry.cold_backing))
                .ok(),
        );

        let needed = entry.cold_backing.length as usize;
        let slot = staging_pool.try_allocate(needed).or_else(|| {
            let reclaimed =
                storage_owner.sweep_expired_staging_slots(std::time::Duration::from_secs(30));
            if reclaimed > 0 {
                if let Some(slot) = staging_pool.try_allocate(needed) {
                    return Some(slot);
                }
            }
            // Each entry independently waits for a slot — upper-layer retry
            // cost (gRPC + route resolve + singleflight) is much higher than
            // queuing here for a few seconds.
            staging_pool.allocate_blocking(needed, std::time::Duration::from_secs(5))
        });

        match slot {
            Some(slot) => staged_slots.push(Some(slot)),
            None => {
                staged_slots.push(None);
                out.push((
                    entry.index,
                    cold_read_error(StoreError::Backpressure(
                        "staging pool exhausted after blocking wait".to_string(),
                    )),
                ));
            }
        }
    }

    // Collect indices of entries that have staging slots.
    let staged_indices: Vec<usize> = staged_slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| slot.is_some())
        .map(|(i, _)| i)
        .collect();

    if staged_indices.is_empty() {
        return out;
    }

    // 4d-e. Build ColdObjectRead array → batch SSD I/O.
    let t_ssd = std::time::Instant::now();
    let read_results = {
        let mut cold_reads: Vec<ColdObjectRead<'_, '_>> = staged_indices
            .iter()
            .map(|&gi| {
                let entry = &group[gi];
                let slot = staged_slots[gi].as_ref().unwrap();
                let dst = unsafe {
                    std::slice::from_raw_parts_mut(slot.addr, entry.cold_backing.length as usize)
                };
                ColdObjectRead {
                    cold_backing: &entry.cold_backing,
                    dst,
                }
            })
            .collect();
        backend.get_objects_into_batch_profiled(&mut cold_reads, &mut |_, _| {})
        // cold_reads dropped here — no longer borrows staged_slots/group.
    };

    let ssd_read_us = t_ssd.elapsed().as_micros() as u64;
    drop(restore_permits);

    // 4f + Phase 5: validate checksum, publish to singleflight, pin, build response.
    // Track which group indices succeeded (for deferred promote pass below).
    let mut ok_group_indices: Vec<usize> = Vec::new();

    for (read_idx, &group_idx) in staged_indices.iter().enumerate() {
        let entry = &mut group[group_idx];
        let slot = staged_slots[group_idx].as_ref().unwrap();
        let read_result = &read_results[read_idx];

        let outcome = (|| -> Result<super::super::OwnerColdRestoreOutcome> {
            let opt_bytes = read_result.as_ref().map_err(|e| {
                StoreError::InvalidState(format!(
                    "batch SSD read error for {}: {e}",
                    entry.route.key.0
                ))
            })?;
            let bytes_read = *opt_bytes.as_ref().ok_or_else(|| {
                StoreError::NotFound(format!(
                    "route {} cold backing payload unavailable (batch staging)",
                    entry.route.key.0
                ))
            })?;
            if bytes_read != entry.cold_backing.length as usize {
                return Err(StoreError::InvalidState(format!(
                    "route {} cold backing length mismatch: expected {} actual {}",
                    entry.route.key.0, entry.cold_backing.length, bytes_read
                )));
            }
            let payload = unsafe { std::slice::from_raw_parts(slot.addr, bytes_read) };
            if let Some(expected) = entry.cold_backing.checksum {
                let actual = payload_checksum(payload);
                if actual != expected {
                    return Err(StoreError::InvalidState(format!(
                        "checksum mismatch for cold backing {}:{} expected={} actual={}",
                        entry.cold_backing.owner,
                        entry.cold_backing.object_locator,
                        expected,
                        actual
                    )));
                }
            }
            Ok(super::super::OwnerColdRestoreOutcome {
                segment_name: slot.segment_name.0.clone(),
                segment_offset: slot.offset,
                length: entry.cold_backing.length,
                checksum: entry.cold_backing.checksum,
                target_chunks: staging_pool.target_chunks().to_vec(),
                transport_endpoint: staging_pool.transport_endpoint().map(|s| s.to_string()),
                transport_segment_descriptor: staging_pool
                    .transport_segment_descriptor()
                    .map(|s| s.to_string()),
                timing: super::super::OwnerColdRestoreTiming {
                    admission_wait_us: 0,
                    ssd_read_us,
                    memcpy_us: 0,
                    route_update_us: 0,
                    total_us: batch_started.elapsed().as_micros() as u64,
                },
            })
        })();

        // Publish to singleflight waiters (if this entry is a leader).
        if let Some(mut leader) = entry.leader.take() {
            let _: Result<super::super::OwnerColdRestoreOutcome> =
                leader.publish_data_ready(outcome.clone());
        }

        match outcome {
            Ok(outcome_val) => {
                // Pin staging slot for RDMA protection.
                storage_owner.read_pin_registry.pin(
                    &mooncake_store_core::SegmentName::new(&outcome_val.segment_name),
                    outcome_val.segment_offset,
                );
                out.push((
                    entry.index,
                    crate::control_plane::ColdReadResponse {
                        result: Ok(outcome_to_cold_read_result(&outcome_val)),
                        deferred_promote: None, // filled below when staging_slot ownership transfers
                    },
                ));
                ok_group_indices.push(group_idx);
            }
            Err(e) => {
                out.push((entry.index, cold_read_error(e)));
            }
        }
    }

    // Transfer staging slot ownership into deferred promote closures.
    // Must be a separate pass because the loop above borrows `staged_slots` via `slot`.
    for &group_idx in &ok_group_indices {
        let entry = &group[group_idx];
        let staging_slot = staged_slots[group_idx].take().unwrap();
        let staging_ctx = OwnerColdRestoreStagingContext {
            route: entry.route.clone(),
            object_id: entry.object_id.clone(),
            cold_backing: entry.cold_backing.clone(),
            staging_slot,
            ssd_read_us,
            total_start: batch_started,
        };
        let so = Arc::clone(storage_owner);
        let al = Arc::clone(allocator);
        let deferred = Box::new(move || {
            execute_owner_cold_restore_promote_phase(&so, &al, staging_ctx);
        }) as Box<dyn FnOnce() + Send>;

        // Find the corresponding output entry and attach deferred promote.
        if let Some((_, resp)) = out.iter_mut().find(|(idx, _)| *idx == entry.index) {
            resp.deferred_promote = Some(deferred);
        }
        // For error entries: staging slot remains in staged_slots[gi] → dropped at end → returned to pool.
    }

    out
}

// Helper: convert OwnerColdRestoreOutcome to ColdReadResult.
fn outcome_to_cold_read_result(
    outcome: &super::super::OwnerColdRestoreOutcome,
) -> crate::control_plane::ColdReadResult {
    crate::control_plane::ColdReadResult {
        segment_name: outcome.segment_name.clone(),
        segment_offset: outcome.segment_offset,
        length: outcome.length,
        checksum: outcome.checksum,
        target_chunks: outcome.target_chunks.clone(),
        transport_endpoint: outcome.transport_endpoint.clone(),
        transport_segment_descriptor: outcome.transport_segment_descriptor.clone(),
        admission_wait_us: outcome.timing.admission_wait_us,
        ssd_read_us: outcome.timing.ssd_read_us,
        memcpy_us: outcome.timing.memcpy_us,
        route_update_us: outcome.timing.route_update_us,
        total_promote_us: outcome.timing.total_us,
    }
}

/// Convert `Option<ColdReadResponse>` slots to a flat `Vec<ColdReadResponse>`,
/// filling any `None` entries with an internal error.
fn finalize_batch_results(
    results: Vec<Option<crate::control_plane::ColdReadResponse>>,
) -> Vec<crate::control_plane::ColdReadResponse> {
    results
        .into_iter()
        .map(|r| {
            r.unwrap_or_else(|| {
                cold_read_error(StoreError::InvalidState(
                    "batch result slot was not filled".to_string(),
                ))
            })
        })
        .collect()
}

/// Shorthand for building a `ColdReadResponse` with an error and no deferred work.
fn cold_read_error(err: StoreError) -> crate::control_plane::ColdReadResponse {
    crate::control_plane::ColdReadResponse {
        result: Err(err),
        deferred_promote: None,
    }
}

// ---------------------------------------------------------------------------
// One-shot ReadFromCold handler (owner-side cold read for remote readers)
// ---------------------------------------------------------------------------

pub(in super::super) fn read_from_cold_one_shot(
    storage_owner: &StorageOwnerState,
    _allocator: &parking_lot::Mutex<LocalAllocatorState>,
    tenant: &str,
    key: &str,
    domain: &str,
    object_set: &str,
) -> (
    Result<crate::control_plane::ColdReadResult>,
    Option<OwnerColdRestoreStagingContext>,
) {
    let scope = mooncake_store_core::NamespaceScope::with_defaults(
        Some(tenant),
        if domain.is_empty() {
            None
        } else {
            Some(domain)
        },
        if object_set.is_empty() {
            None
        } else {
            Some(object_set)
        },
    );
    let object_id = mooncake_store_core::LogicalObjectId::new(scope, key);
    let route_key = ObjectKey::from_logical_id(&object_id);
    let started = std::time::Instant::now();
    debug!(
        runtime = %storage_owner.runtime,
        tenant = tenant,
        key = key,
        route_key = %route_key.0,
        "read_from_cold request received"
    );

    let mut deferred_staging_ctx: Option<OwnerColdRestoreStagingContext> = None;

    let result = (|| -> Result<crate::control_plane::ColdReadResult> {
        // Singleflight: dedup concurrent cold reads for the same key.
        // Leader reads SSD into staging buffer, publishes RDMA coords
        // (waiters unblock). Promote (DRAM eviction + memcpy + CAS) is
        // deferred — the caller runs it after sending the gRPC response.
        let role = storage_owner.owner_cold_restore_flights.acquire(&route_key);
        let outcome = match role {
            super::super::OwnerColdRestoreFlightRole::Leader(mut leader) => {
                let ssd_result =
                    execute_owner_cold_restore_ssd_phase_staging(storage_owner, &route_key);
                match ssd_result {
                    Ok((outcome, staging_ctx)) => {
                        // Publish RDMA coordinates immediately — waiters unblock here.
                        let published = leader.publish_data_ready(Ok(outcome))?;
                        // Defer promote — caller will run it after gRPC response.
                        deferred_staging_ctx = Some(staging_ctx);
                        published
                    }
                    Err(e) => {
                        // SSD read failed — notify waiters with error.
                        leader.publish_data_ready(Err(e))?
                    }
                }
            }
            super::super::OwnerColdRestoreFlightRole::Waiter(flight) => {
                debug!(
                    runtime = %storage_owner.runtime,
                    route_key = %route_key.0,
                    "read_from_cold joining existing flight (waiter)"
                );
                storage_owner
                    .owner_cold_restore_flights
                    .wait(&flight, std::time::Duration::from_secs(30))?
            }
            super::super::OwnerColdRestoreFlightRole::Rejected => {
                // Too many concurrent flights; fall through without dedup.
                debug!(
                    runtime = %storage_owner.runtime,
                    route_key = %route_key.0,
                    "read_from_cold flight map full, proceeding without dedup"
                );
                let (outcome, staging_ctx) =
                    execute_owner_cold_restore_ssd_phase_staging(storage_owner, &route_key)?;
                // Defer promote — caller will run it after gRPC response.
                deferred_staging_ctx = Some(staging_ctx);
                outcome
            }
        };

        // Pin the staging slot to protect it while the remote reader
        // performs its RDMA read.  Reader ACKs after RDMA → unpin + release.
        storage_owner.read_pin_registry.pin(
            &mooncake_store_core::SegmentName::new(&outcome.segment_name),
            outcome.segment_offset,
        );

        Ok(crate::control_plane::ColdReadResult {
            segment_name: outcome.segment_name,
            segment_offset: outcome.segment_offset,
            length: outcome.length,
            checksum: outcome.checksum,
            target_chunks: outcome.target_chunks,
            transport_endpoint: outcome.transport_endpoint,
            transport_segment_descriptor: outcome.transport_segment_descriptor,
            admission_wait_us: outcome.timing.admission_wait_us,
            ssd_read_us: outcome.timing.ssd_read_us,
            memcpy_us: outcome.timing.memcpy_us,
            route_update_us: outcome.timing.route_update_us,
            total_promote_us: outcome.timing.total_us,
        })
    })();
    match &result {
        Ok(reply) => {
            debug!(
                runtime = %storage_owner.runtime,
                tenant = tenant,
                key = key,
                route_key = %route_key.0,
                segment_name = %reply.segment_name,
                length = reply.length,
                elapsed_ms = started.elapsed().as_millis() as u64,
                admission_wait_us = reply.admission_wait_us,
                ssd_read_us = reply.ssd_read_us,
                "read_from_cold prepared staging RDMA replica"
            );
        }
        Err(error) => {
            warn!(
                runtime = %storage_owner.runtime,
                tenant = tenant,
                key = key,
                route_key = %route_key.0,
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "read_from_cold failed"
            );
            // On error, there's no staging context to defer.
            deferred_staging_ctx = None;
        }
    }
    (result, deferred_staging_ctx)
}

/// Execute the actual promote and build the `OwnerColdRestoreOutcome`.
/// Extracted so both Leader path and Rejected path can call it.
/// Context carried from SSD-read phase to CAS phase of owner cold restore.
pub(in super::super) struct OwnerColdRestoreCasContext {
    route: ObjectRoute,
    object_id: mooncake_store_core::LogicalObjectId,
    cold_backing: mooncake_store_core::ColdBackingRoute,
    reservation: mooncake_store_core::SegmentReservation,
    ssd_read_us: u64,
    total_start: std::time::Instant,
}

/// Context carried from staging SSD-read phase to promote+ACK phases.
pub(in super::super) struct OwnerColdRestoreStagingContext {
    route: ObjectRoute,
    object_id: mooncake_store_core::LogicalObjectId,
    cold_backing: mooncake_store_core::ColdBackingRoute,
    staging_slot: super::staging_pool::StagingSlot,
    ssd_read_us: u64,
    total_start: std::time::Instant,
}

/// Phase 2: route CAS update (not on waiter critical path).
/// On success the route gains a DRAM replica referencing the segment slot.
/// On failure the reservation is released (safe: RDMA readers have already
/// completed by the time CAS round-trips finish, ~2-4ms after publish).
pub(in super::super) fn execute_owner_cold_restore_cas_phase(
    storage_owner: &StorageOwnerState,
    ctx: OwnerColdRestoreCasContext,
) {
    let t_route = std::time::Instant::now();
    let route_result = (|| -> Result<bool> {
        let next = build_owner_restored_route(
            storage_owner,
            &ctx.route,
            &ctx.object_id,
            &ctx.cold_backing,
            &ctx.reservation,
        );
        let Some(current) = cas_owner_restored_route(storage_owner, &ctx.route, &next)? else {
            return Ok(true);
        };
        if route_restore_race_resolved(storage_owner, &current)
            || current.cold_backing.as_ref() != Some(&ctx.cold_backing)
        {
            return Ok(false);
        }
        let next = build_owner_restored_route(
            storage_owner,
            &current,
            &ctx.object_id,
            &ctx.cold_backing,
            &ctx.reservation,
        );
        Ok(cas_owner_restored_route(storage_owner, &current, &next)?.is_none())
    })();
    let route_update_us = t_route.elapsed().as_micros() as u64;
    let total_us = ctx.total_start.elapsed().as_micros() as u64;

    match route_result {
        Ok(true) => {
            debug!(
                runtime = %storage_owner.runtime,
                route_key = %ctx.route.key.0,
                ssd_read_us = ctx.ssd_read_us,
                route_update_us,
                total_us,
                "owner cold restore CAS succeeded"
            );
            registry::record_cold_tier_operation_result(
                "restore_promote",
                &Ok::<(), StoreError>(()),
            );
        }
        Ok(false) => {
            release_owner_restore_reservation(
                storage_owner,
                &ctx.route,
                &ctx.reservation,
                "cas_phase_race",
            );
            debug!(
                runtime = %storage_owner.runtime,
                route_key = %ctx.route.key.0,
                "owner cold restore CAS race — reservation released"
            );
        }
        Err(error) => {
            release_owner_restore_reservation(
                storage_owner,
                &ctx.route,
                &ctx.reservation,
                "cas_phase_error",
            );
            debug!(
                runtime = %storage_owner.runtime,
                route_key = %ctx.route.key.0,
                error = %error,
                "owner cold restore CAS failed — reservation released"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Staging-pool–based cold restore: SSD → staging buffer → RDMA passthrough
// ---------------------------------------------------------------------------

/// Eagerly initialize the staging pool during client startup.
/// Called from `ensure_local_memory` after transports are registered, so the
/// first cold restore batch does not pay the mmap + RDMA registration cost.
pub(in super::super) fn try_init_staging_pool(storage_owner: &StorageOwnerState) -> Result<()> {
    get_or_init_staging_pool(storage_owner).map(|_| ())
}

/// Lazily initialize the staging pool using a transport from existing segments.
fn get_or_init_staging_pool(
    storage_owner: &StorageOwnerState,
) -> Result<std::sync::Arc<super::staging_pool::ColdRestoreStagingPool>> {
    {
        let guard = storage_owner.staging_pool.lock();
        if let Some(pool) = guard.as_ref() {
            return Ok(std::sync::Arc::clone(pool));
        }
    }
    // Init outside the lock to avoid holding it during mmap + transport registration.
    let transport = {
        let state = storage_owner.state.lock();
        state
            .local_transports
            .values()
            .next()
            .cloned()
            .ok_or_else(|| {
                StoreError::InvalidState(
                    "no local transport available for staging pool init".to_string(),
                )
            })?
    };
    let pool = super::staging_pool::ColdRestoreStagingPool::new(
        super::staging_pool::StagingPoolInitParams {
            transport,
            total_bytes: storage_owner.staging_pool_bytes,
        },
    )?;
    let mut guard = storage_owner.staging_pool.lock();
    if let Some(existing) = guard.as_ref() {
        // Another thread won the race — use theirs.
        return Ok(std::sync::Arc::clone(existing));
    }
    *guard = Some(std::sync::Arc::clone(&pool));
    Ok(pool)
}

/// Phase 1 (staging variant): admission → staging slot → SSD read into staging.
/// Returns RDMA-ready outcome pointing to the staging buffer (NOT DRAM).
pub(in super::super) fn execute_owner_cold_restore_ssd_phase_staging(
    storage_owner: &StorageOwnerState,
    route_key: &ObjectKey,
) -> Result<(
    super::super::OwnerColdRestoreOutcome,
    OwnerColdRestoreStagingContext,
)> {
    let total_start = std::time::Instant::now();

    let Some(route) = storage_owner.current_materialized_route(route_key)? else {
        return Err(StoreError::NotFound(format!(
            "route_key={} has no materialized cold-only route to promote",
            route_key.0
        )));
    };
    let Some(cold_backing) = owner_materialized_cold_backing(storage_owner, &route) else {
        storage_owner.sync_route(&route);
        return Err(StoreError::NotFound(format!(
            "route_key={} cold backing not owned/materialized",
            route_key.0
        )));
    };
    let cold_backing = cold_backing.clone();
    let object_id = mooncake_store_core::route_logical_object_id(&route)?;

    // Track per-object concurrent I/O for singleflight correctness monitoring.
    // The guard decrements the count on drop (function return or early error).
    let _io_guard = storage_owner.cold_restore_io_tracker.begin_io(route_key);

    // SSD read admission — restore always admitted (track-only, no rate/concurrency
    // limit).  Natural backpressure comes from staging pool capacity (256 MiB) and
    // io_uring queue depth.  The previous fail-fast token-bucket approach rejected
    // ~24% of requests, causing expensive gRPC retry waves on the reader side.
    let device_id = cold_tier_device_id(&cold_backing);
    let restore_permit = storage_owner
        .cold_tier_devices
        .try_acquire_restore(device_id)
        .ok();

    // Allocate staging slot — 3FS pattern: try non-blocking, then wait.
    // Timeout prevents deadlock: read_from_cold and ack_cold_read_complete
    // share the same spawn_blocking pool; unbounded wait would starve ACKs.
    let staging_pool = get_or_init_staging_pool(storage_owner)?;
    let needed = cold_backing.length as usize;
    let staging_slot = staging_pool.try_allocate(needed).or_else(|| {
        // Pool exhausted — sweep expired entries before blocking.
        // Reclaims slots whose readers crashed without ACK.
        let util_before = staging_pool.utilization();
        let reclaimed = storage_owner
            .sweep_expired_staging_slots(std::time::Duration::from_secs(30));
        if reclaimed > 0 {
            // Retry fast path after sweep freed slots.
            if let Some(slot) = staging_pool.try_allocate(needed) {
                return Some(slot);
            }
        }
        let util = staging_pool.utilization();
        warn!(
            runtime = %storage_owner.runtime,
            route_key = %route_key.0,
            needed_bytes = needed,
            reclaimed,
            cursor = util.cursor,
            capacity = util.capacity,
            free_span_count = util.free_span_count,
            free_span_bytes = util.free_span_bytes,
            used_bytes = util.used_bytes,
            used_pct = format_args!("{:.1}", util.used_pct),
            pre_sweep_used_pct = format_args!("{:.1}", util_before.used_pct),
            "staging_pool_fallback_to_blocking: try_allocate failed, entering allocate_blocking(5s)"
        );
        staging_pool.allocate_blocking(needed, std::time::Duration::from_secs(5))
    }).ok_or_else(|| {
        if let Some(ref p) = restore_permit { p.complete_error(); }
        StoreError::Backpressure("staging pool exhausted".to_string())
    })?;

    let t_ssd = std::time::Instant::now();

    // SSD read directly into staging buffer, with sub-phase profiling.
    let mut backend_resolve_us: u64 = 0;
    let mut ssd_io_us: u64 = 0;
    let mut checksum_us: u64 = 0;
    let ssd_read_result = (|| -> Result<()> {
        let dst = unsafe {
            std::slice::from_raw_parts_mut(staging_slot.addr, cold_backing.length as usize)
        };
        let t_backend = std::time::Instant::now();
        let backend = storage_owner.cold_tier_devices.backend_for_with_refresh(
            storage_owner.metadata.as_ref(),
            &cold_backing,
            "cold_restore_owner_staging_ssd",
        )?;
        backend_resolve_us = t_backend.elapsed().as_micros() as u64;

        let t_io = std::time::Instant::now();
        let read_result = backend.get_object_into(&cold_backing, dst)?;
        ssd_io_us = t_io.elapsed().as_micros() as u64;

        let bytes_read = read_result.ok_or_else(|| {
            StoreError::NotFound(format!(
                "route {} cold backing payload is unavailable (staging_ssd)",
                route.key.0
            ))
        })?;
        if bytes_read != cold_backing.length as usize {
            return Err(StoreError::InvalidState(format!(
                "route {} cold backing length mismatch: expected {} actual {}",
                route.key.0, cold_backing.length, bytes_read
            )));
        }
        let t_cksum = std::time::Instant::now();
        if let Some(expected) = cold_backing.checksum {
            let actual = payload_checksum(&dst[..bytes_read]);
            if actual != expected {
                return Err(StoreError::InvalidState(format!(
                    "checksum mismatch for cold backing {}:{} expected={} actual={}",
                    cold_backing.owner, cold_backing.object_locator, expected, actual
                )));
            }
        }
        checksum_us = t_cksum.elapsed().as_micros() as u64;
        Ok(())
    })();
    let ssd_read_elapsed = t_ssd.elapsed();
    let ssd_read_us = ssd_read_elapsed.as_micros() as u64;
    super::super::registry::record_cold_tier_ssd_read(
        if ssd_read_result.is_ok() {
            "ok"
        } else {
            "error"
        },
        ssd_read_elapsed,
    );
    let ssd_read_ms = ssd_read_elapsed.as_secs_f64() * 1000.0;
    if ssd_read_ms > 10.0 {
        warn!(
            runtime = %storage_owner.runtime,
            route_key = %route_key.0,
            ssd_read_ms = format!("{ssd_read_ms:.3}"),
            backend_resolve_us,
            ssd_io_us,
            "ssd_read_slow: SSD staging restore read exceeded 10ms"
        );
    }

    if let Err(error) = ssd_read_result {
        warn!(
            runtime = %storage_owner.runtime,
            route_key = %route_key.0,
            error = %error,
            ssd_read_us,
            backend_resolve_us,
            ssd_io_us,
            "staging_ssd_read_failed: SSD read into staging failed"
        );
        if let Some(ref p) = restore_permit {
            p.complete_error();
        }
        // staging_slot Drop returns slot to pool automatically.
        return Err(error);
    }
    if let Some(ref p) = restore_permit {
        p.complete_ok();
    }

    // Build RDMA-ready outcome from staging pool metadata.
    let outcome = super::super::OwnerColdRestoreOutcome {
        segment_name: staging_slot.segment_name.0.clone(),
        segment_offset: staging_slot.offset,
        length: cold_backing.length,
        checksum: cold_backing.checksum,
        target_chunks: staging_pool.target_chunks().to_vec(),
        transport_endpoint: staging_pool.transport_endpoint().map(|s| s.to_string()),
        transport_segment_descriptor: staging_pool
            .transport_segment_descriptor()
            .map(|s| s.to_string()),
        timing: super::super::OwnerColdRestoreTiming {
            admission_wait_us: 0, // no DRAM reservation wait
            ssd_read_us,
            memcpy_us: 0,
            route_update_us: 0,
            total_us: total_start.elapsed().as_micros() as u64,
        },
    };

    let staging_ctx = OwnerColdRestoreStagingContext {
        route,
        object_id,
        cold_backing,
        staging_slot,
        ssd_read_us,
        total_start,
    };

    Ok((outcome, staging_ctx))
}

/// Best-effort DRAM promote after staging SSD read.
///
/// Handles the staging slot lifecycle internally:
///  - After memcpy succeeds, the staging buffer is no longer needed by
///    promote and is released immediately (stored in pending_staging_slots
///    for reader ACK, or dropped if the reader already ACKed).
///  - CAS route update proceeds without holding the staging buffer.
///  - If promote is skipped or memcpy fails, the staging buffer is also
///    released (reader may still be RDMA-reading from it, protected by pin).
///
/// Promote failure is normal — next read will re-do SSD.
pub(in super::super) fn execute_owner_cold_restore_promote_phase(
    storage_owner: &StorageOwnerState,
    allocator: &parking_lot::Mutex<LocalAllocatorState>,
    staging_ctx: OwnerColdRestoreStagingContext,
) {
    let staging_slot = staging_ctx.staging_slot;
    let length = staging_ctx.cold_backing.length;

    // Helper: store staging slot in pending map if readers are still
    // RDMA-reading from it, otherwise drop immediately to recycle the buffer.
    let release_staging = |slot: super::staging_pool::StagingSlot| {
        let key = (slot.segment_name.clone(), slot.offset);
        let mut pending = storage_owner.pending_staging_slots.lock();
        if storage_owner.read_pin_registry.is_pinned(&key.0, key.1) {
            pending.insert(key, (slot, std::time::Instant::now()));
        }
        // else: reader already ACKed → drop slot → recycle buffer.
        // Holding pending_staging_slots while checking the pin closes the race
        // with ACK: ACK unpins first, then takes the same pending lock.
    };

    // 1. Try fast-path: allocate from free pool (no eviction).
    let reservation = {
        let mut alloc = allocator.lock();
        match alloc.reserve_any(&storage_owner.runtime, length) {
            Ok(r) => {
                alloc.mark_pending_reservation(
                    &r,
                    super::super::pending_publish_deadline_ms(
                        r.length_bytes,
                        super::super::DEFAULT_TRANSFER_STALL_TIMEOUT,
                        None,
                    ),
                );
                Some(r)
            }
            Err(_) => None,
        }
    };
    // 2. If no free slot, try bounded clean eviction.
    //    Same scan budget as the normal eviction path (entries.len() * 2)
    //    but skips rebuild_clock — the rebuild is the main source of
    //    multi-second stalls under concurrency.
    let reservation = reservation.or_else(|| {
        match storage_owner.evict_one_clean_and_reserve_bounded(length, None) {
            Ok(Some(r)) => {
                allocator.lock().mark_pending_allocation(
                    &r.segment_name,
                    r.offset_bytes,
                    r.length_bytes,
                    super::super::pending_publish_deadline_ms(
                        r.length_bytes,
                        super::super::DEFAULT_TRANSFER_STALL_TIMEOUT,
                        None,
                    ),
                );
                Some(r)
            }
            _ => None,
        }
    });

    let Some(reservation) = reservation else {
        registry::record_cold_tier_operation("restore_promote", "skipped", "no_dram_slot");
        release_staging(staging_slot);
        return;
    };

    // 3. memcpy staging → DRAM.
    let memcpy_result = (|| -> Result<()> {
        let dram_addr = {
            let state = storage_owner.state.lock();
            state
                .memory_ref()?
                .storage_address(&reservation.segment_name, reservation.offset_bytes as usize)?
        };
        unsafe {
            std::ptr::copy_nonoverlapping(
                staging_slot.addr,
                dram_addr.cast::<u8>(),
                length as usize,
            );
        }
        Ok(())
    })();

    // 4. Release staging buffer immediately — data is in DRAM (or memcpy
    //    failed and we don't need it).  Reader RDMA is protected by pin.
    release_staging(staging_slot);

    if let Err(error) = memcpy_result {
        warn!(
            runtime = %storage_owner.runtime,
            route_key = %staging_ctx.route.key.0,
            error = %error,
            "promote memcpy failed — releasing reservation"
        );
        release_owner_restore_reservation(
            storage_owner,
            &staging_ctx.route,
            &reservation,
            "promote_memcpy_error",
        );
        return;
    }

    // 5. CAS route update — staging buffer already released.
    let cas_ctx = OwnerColdRestoreCasContext {
        route: staging_ctx.route,
        object_id: staging_ctx.object_id,
        cold_backing: staging_ctx.cold_backing,
        reservation,
        ssd_read_us: staging_ctx.ssd_read_us,
        total_start: staging_ctx.total_start,
    };
    execute_owner_cold_restore_cas_phase(storage_owner, cas_ctx);
}
