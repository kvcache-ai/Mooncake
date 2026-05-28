const MAX_TENANT_LOCAL_EVICTION_ATTEMPTS: usize = 8;
const PARALLEL_CHECKSUM_MIN_BYTES: usize = 8 * 1024 * 1024;
const MAX_PARALLEL_CHECKSUM_WORKERS: usize = 16;

struct PutObjectCurrentOptions<'a> {
    registered_source: Option<*mut c_void>,
    policy: Option<&'a ReplicationPolicy>,
    current: Option<&'a ObjectRoute>,
    reclaim_mode: ReclaimMode,
}

impl StoreClient {
    fn best_effort_release_reserved_allocations(
        &self,
        targets: &[ReplicaWriteTarget],
        reservations: &[mooncake_store_core::SegmentReservation],
        reason: &str,
    ) {
        if let Err(error) = self.release_reserved_allocations(targets, reservations) {
            warn!(
                runtime = %self.lease.runtime,
                error = %error,
                reason,
                "failed to release reserved allocations"
            );
        }
    }

    fn load_tenant_quota_policies(
        &self,
        tenants: &[String],
    ) -> Result<BTreeMap<String, Option<TenantQuotaPolicy>>> {
        let mut resolved = BTreeMap::new();
        let now = now_ms();
        let mut refresh_tenants = Vec::new();
        {
            let mut cache = self.live_client_cache.lock();
            for tenant in tenants {
                if let Some(quota) =
                    cache.tenant_quota_policy(tenant, now, DEFAULT_TENANT_POLICY_CACHE_TTL_MS)
                {
                    resolved.insert(tenant.clone(), quota);
                } else {
                    refresh_tenants.push(tenant.clone());
                }
            }
        }
        if refresh_tenants.is_empty() {
            return Ok(resolved);
        }
        let scopes = refresh_tenants
            .iter()
            .map(|tenant| TenantPolicyScope::new(tenant.as_str(), None::<String>, None::<String>))
            .collect::<Vec<_>>();
        let policies = self.metadata.get_tenant_policies(&scopes)?;
        let mut cache = self.live_client_cache.lock();
        for (tenant, policy) in refresh_tenants.into_iter().zip(policies) {
            let version = policy.as_ref().map(|policy| policy.version);
            let quota = policy.and_then(|policy| policy.spec.quota);
            cache.store_tenant_quota_policy(tenant.clone(), version, quota.clone(), now);
            resolved.insert(tenant, quota);
        }
        Ok(resolved)
    }

    fn tenant_quota_policy_for_object(
        &self,
        object_id: &LogicalObjectId,
    ) -> Result<Option<TenantQuotaPolicy>> {
        let tenant = object_id.scope.tenant.clone();
        if let Some(quota) = self
            .load_tenant_quota_policies(std::slice::from_ref(&tenant))?
            .remove(&tenant)
            .flatten()
        {
            return Ok(Some(quota));
        }
        Ok(self.namespace_quota.as_ref().map(|quota| TenantQuotaPolicy {
            max_bytes: quota.max_bytes,
            max_objects: quota.max_objects,
        }))
    }

    fn tenant_quota_scope(&self, object_id: &LogicalObjectId) -> TenantPolicyScope {
        TenantPolicyScope::new(object_id.scope.tenant.clone(), None::<String>, None::<String>)
    }

    fn route_committed_length(route: Option<&ObjectRoute>) -> u64 {
        route
            .and_then(|current| current.replicas.iter().min_by_key(|replica| replica.priority))
            .map(|replica| replica.length)
            .unwrap_or(0)
    }

    fn next_tenant_quota_reservation_id(&self, object_id: &LogicalObjectId) -> String {
        let next = self
            .tenant_quota_reservation_counter
            .fetch_add(1, Ordering::Relaxed);
        format!(
            "{}:{}:{}:{}",
            self.lease.runtime.stable_id.0,
            self.lease.runtime.epoch.0,
            object_id.scope.tenant,
            next
        )
    }

    fn reserve_tenant_quota_for_put(
        &self,
        object_id: &LogicalObjectId,
        scoped_key: &ObjectKey,
        current: Option<&ObjectRoute>,
        value_len: usize,
    ) -> Result<Option<TenantQuotaReservationRequest>> {
        let Some(limit) = self.tenant_quota_policy_for_object(object_id)? else {
            return Ok(None);
        };
        let current_object = self.metadata.get_tenant_object_accounting(scoped_key)?;
        let previous_len = current_object
            .as_ref()
            .map(|object| object.committed_length)
            .unwrap_or_else(|| Self::route_committed_length(current));
        let delta_bytes = value_len as i64 - previous_len as i64;
        let delta_objects = if previous_len == 0 { 1 } else { 0 };
        let created_at_ms = now_ms();
        let object_exceeds_byte_limit = limit
            .max_bytes
            .is_some_and(|max_bytes| delta_bytes.max(0) as u64 > max_bytes);
        let object_exceeds_object_limit = limit
            .max_objects
            .is_some_and(|max_objects| delta_objects.max(0) as usize > max_objects);
        let mut request = TenantQuotaReservationRequest {
            reservation_id: self.next_tenant_quota_reservation_id(object_id),
            scope: self.tenant_quota_scope(object_id),
            key: scoped_key.clone(),
            expected_object_version: current_object.as_ref().map(|object| object.version),
            delta_bytes,
            delta_objects,
            limit,
            expires_at_ms: created_at_ms.saturating_add(DEFAULT_TENANT_QUOTA_RESERVATION_TTL_MS),
            created_at_ms,
            writer_runtime: self.lease.runtime.clone(),
        };
        let mut eviction_candidates = None;
        for _ in 0..MAX_TENANT_LOCAL_EVICTION_ATTEMPTS {
            let reserve_result = self.metadata.reserve_tenant_quota(&request);
            registry::record_tenant_quota_reservation(match &reserve_result {
                Ok(_) => "ok",
                Err(StoreError::Conflict(_) | StoreError::QuotaExceeded { .. }) => "conflict",
                Err(_) => "error",
            });
            match reserve_result {
                Ok(_) => return Ok(Some(request)),
                Err(StoreError::QuotaExceeded { kind, message }) => {
                    if object_exceeds_byte_limit || object_exceeds_object_limit {
                        return Err(StoreError::QuotaExceeded { kind, message });
                    }
                    if !self.try_evict_one_object_in_tenant(
                        object_id,
                        scoped_key,
                        &mut eviction_candidates,
                    )? {
                        registry::record_tenant_local_eviction("miss");
                        return Err(StoreError::QuotaExceeded { kind, message });
                    }
                    registry::record_tenant_local_eviction("ok");
                    let refreshed = self.metadata.get_tenant_object_accounting(scoped_key)?;
                    request.expected_object_version =
                        refreshed.as_ref().map(|object| object.version);
                    continue;
                }
                Err(StoreError::Conflict(message))
                    if message.contains("tenant quota bytes exceeded")
                        || message.contains("tenant quota objects exceeded") =>
                {
                    if object_exceeds_byte_limit || object_exceeds_object_limit {
                        return Err(StoreError::Conflict(message));
                    }
                    if !self.try_evict_one_object_in_tenant(
                        object_id,
                        scoped_key,
                        &mut eviction_candidates,
                    )? {
                        registry::record_tenant_local_eviction("miss");
                        return Err(StoreError::Conflict(message));
                    }
                    registry::record_tenant_local_eviction("ok");
                    let refreshed = self.metadata.get_tenant_object_accounting(scoped_key)?;
                    request.expected_object_version =
                        refreshed.as_ref().map(|object| object.version);
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StoreError::Conflict(format!(
            "tenant-local quota eviction exhausted retries for {}",
            scoped_key.0
        )))
    }

    fn try_evict_one_object_in_tenant(
        &self,
        object_id: &LogicalObjectId,
        scoped_key: &ObjectKey,
        eviction_candidates: &mut Option<VecDeque<LogicalObjectId>>,
    ) -> Result<bool> {
        if eviction_candidates.is_none() {
            let scope = self.tenant_quota_scope(object_id);
            let candidates = self
                .metadata
                .list_tenant_eviction_candidates(&scope, MAX_TENANT_LOCAL_EVICTION_ATTEMPTS)?
                .into_iter()
                .filter(|candidate| {
                    candidate.key != *scoped_key
                        && candidate.state == TenantObjectAccountingState::Active
                })
                .filter_map(|candidate| {
                    mooncake_store_core::parse_legacy_scoped_key(&candidate.key)
                        .ok()
                        .filter(|victim| victim.scope.tenant == object_id.scope.tenant)
                })
                .collect();
            *eviction_candidates = Some(candidates);
        }
        let Some(candidates) = eviction_candidates.as_mut() else {
            return Ok(false);
        };
        while let Some(victim) = candidates.pop_front() {
            if self
                .remove_in_tenant(victim.scope.tenant.as_str(), victim.logical_key.as_str(), true)
                .is_ok()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn reserve_tenant_quota_for_delete(
        &self,
        object_id: &LogicalObjectId,
        scoped_key: &ObjectKey,
        route: &ObjectRoute,
    ) -> Result<Option<TenantQuotaReservationRequest>> {
        let Some(limit) = self.tenant_quota_policy_for_object(object_id)? else {
            return Ok(None);
        };
        let current_object = self.metadata.get_tenant_object_accounting(scoped_key)?;
        let previous_len = current_object
            .as_ref()
            .map(|object| object.committed_length)
            .unwrap_or_else(|| Self::route_committed_length(Some(route)));
        let created_at_ms = now_ms();
        let request = TenantQuotaReservationRequest {
            reservation_id: self.next_tenant_quota_reservation_id(object_id),
            scope: self.tenant_quota_scope(object_id),
            key: scoped_key.clone(),
            expected_object_version: current_object.as_ref().map(|object| object.version),
            delta_bytes: -(previous_len as i64),
            delta_objects: -1,
            limit,
            expires_at_ms: created_at_ms.saturating_add(DEFAULT_TENANT_QUOTA_RESERVATION_TTL_MS),
            created_at_ms,
            writer_runtime: self.lease.runtime.clone(),
        };
        let reserve_result = self.metadata.reserve_tenant_quota(&request);
        registry::record_tenant_quota_reservation(match &reserve_result {
            Ok(_) => "ok",
            Err(StoreError::Conflict(_) | StoreError::QuotaExceeded { .. }) => "conflict",
            Err(_) => "error",
        });
        reserve_result?;
        Ok(Some(request))
    }

    fn finalize_tenant_quota_put(
        &self,
        reservation: Option<&TenantQuotaReservationRequest>,
        route: &ObjectRoute,
        value_len: usize,
    ) -> Result<()> {
        let Some(reservation) = reservation else {
            return Ok(());
        };
        let finalize_result = self.metadata.finalize_tenant_quota(&TenantQuotaFinalizeRequest {
            reservation_id: reservation.reservation_id.clone(),
            expected_object_version: reservation.expected_object_version,
            committed_length: Some(value_len as u64),
            route_version: Some(route.version),
            state: TenantObjectAccountingState::Active,
            updated_at_ms: now_ms(),
            updated_by: self.lease.runtime.to_string(),
        });
        registry::record_tenant_quota_finalize(match &finalize_result {
            Ok(_) => "ok",
            Err(StoreError::Conflict(_)) => "conflict",
            Err(_) => "error",
        });
        finalize_result?;
        Ok(())
    }

    fn finalize_tenant_quota_delete(
        &self,
        reservation: Option<&TenantQuotaReservationRequest>,
    ) -> Result<()> {
        let Some(reservation) = reservation else {
            return Ok(());
        };
        let finalize_result = self.metadata.finalize_tenant_quota(&TenantQuotaFinalizeRequest {
            reservation_id: reservation.reservation_id.clone(),
            expected_object_version: reservation.expected_object_version,
            committed_length: None,
            route_version: None,
            state: TenantObjectAccountingState::Deleted,
            updated_at_ms: now_ms(),
            updated_by: self.lease.runtime.to_string(),
        });
        registry::record_tenant_quota_finalize(match &finalize_result {
            Ok(_) => "ok",
            Err(StoreError::Conflict(_)) => "conflict",
            Err(_) => "error",
        });
        finalize_result?;
        Ok(())
    }

    fn abort_tenant_quota_reservation(
        &self,
        reservation: Option<&TenantQuotaReservationRequest>,
        context: &str,
    ) -> Result<()> {
        let Some(reservation) = reservation else {
            return Ok(());
        };
        let abort_result = self.metadata.abort_tenant_quota(&reservation.reservation_id);
        registry::record_tenant_quota_abort(match &abort_result {
            Ok(_) => "ok",
            Err(StoreError::Conflict(_)) => "conflict",
            Err(_) => "error",
        });
        abort_result.map(|_| ()).map_err(|error| {
            StoreError::InvalidState(format!(
                "failed to abort tenant quota reservation {} after {context}: {error}",
                reservation.reservation_id
            ))
        })
    }

    fn put_object_with_policy_current(
        &self,
        object_id: &LogicalObjectId,
        qos_tier: Option<&str>,
        value: &[u8],
        options: PutObjectCurrentOptions<'_>,
    ) -> Result<ObjectRoute> {
        self.ensure_local_memory()?;
        self.flush_due_reclaims()?;
        let tenant = object_id.scope.tenant.as_str();
        let key = object_id.logical_key.as_str();
        let scoped_key = mooncake_store_core::ObjectKey::from_logical_id(object_id);
        let PutObjectCurrentOptions {
            registered_source,
            policy,
            current,
            reclaim_mode,
        } = options;
        let mut object_ref = ObjectRef::new(key).tenant(tenant);
        if object_id.scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
            object_ref = object_ref.domain(object_id.scope.domain.as_str());
        }
        if object_id.scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
            object_ref = object_ref.object_set(object_id.scope.object_set.as_str());
        }
        let qos_tier = qos_tier
            .or_else(|| current.and_then(|route| route.qos_tier.as_deref()))
            .unwrap_or(mooncake_store_core::DEFAULT_QOS_TIER);
        if qos_tier != mooncake_store_core::DEFAULT_QOS_TIER {
            object_ref = object_ref.qos_tier(qos_tier);
        }
        let policy = self.resolve_replication_policy(policy)?;
        let ranked_candidate_count = self
            .request_placement_planner()
            .ranked_candidates(self, &object_ref)?
            .len();
        let max_write_attempts = self.write_retry_limit(&policy, ranked_candidate_count);
        let mut current = current.cloned();
        let mut attempt = 0usize;
        loop {
            attempt = attempt.saturating_add(1);
            self.flush_due_reclaims()?;
            let quota_reservation = self.reserve_tenant_quota_for_put(
                object_id,
                &scoped_key,
                current.as_ref(),
                value.len(),
            )?;
            let reserve_tracker =
                OperationTracker::new("put_stage_reserve")
                    .attribute_str("mooncake.tenant", tenant)
                    .attribute_u64("mooncake.item_count", 1)
                    .attribute_u64("mooncake.replica_count", policy.replica_count as u64)
                    .input_bytes(value.len() as u64);
            let reserve_result = self.reserve_replica_targets(&object_ref, value.len(), &policy);
            reserve_tracker.finish(&reserve_result, 0);
            let (targets, reservations) = match reserve_result {
                Ok(reserved) => reserved,
                Err(error) => {
                    let _ = self.abort_tenant_quota_reservation(
                        quota_reservation.as_ref(),
                        "allocation_reserve_failed",
                    );
                    return Err(error);
                }
            };
            let remote_target_count = targets
                .iter()
                .filter(|target| target.storage_runtime != self.lease.runtime)
                .count();
            let local_target_count = targets.len().saturating_sub(remote_target_count);
            let write_tracker =
                OperationTracker::new("put_stage_write")
                    .attribute_str("mooncake.tenant", tenant)
                    .attribute_u64("mooncake.item_count", 1)
                    .attribute_u64("mooncake.replica_count", targets.len() as u64)
                    .attribute_u64("mooncake.local_target_count", local_target_count as u64)
                    .attribute_u64("mooncake.remote_target_count", remote_target_count as u64)
                    .input_bytes(value.len() as u64);
            let write_result =
                self.write_reserved_replicas(&targets, &reservations, value, registered_source);
            write_tracker.finish(&write_result, value.len() as u64);
            let offsets = match write_result {
                Ok(offsets) => offsets,
                Err(error) => {
                    self.best_effort_release_reserved_allocations(
                        &targets,
                        &reservations,
                        "put_stage_write_failed",
                    );
                    let _ = self.abort_tenant_quota_reservation(
                        quota_reservation.as_ref(),
                        "replica_write_failed",
                    );
                    self.note_remote_write_failure(&targets, &error, "remote_write_failed");
                    if attempt < max_write_attempts
                        && self.should_retry_put_after_write_failure(&error, &policy)
                    {
                        debug!(
                            runtime = %self.lease.runtime,
                            tenant,
                            key,
                            attempt,
                            max_write_attempts,
                            error = %error,
                            "retrying put after transient replica write failure"
                        );
                        continue;
                    }
                    return Err(error);
                }
            };
            let expected_version = current.as_ref().map(|route| route.version);
            let next_version = next_route_version(
                current.as_ref(),
                &self.route_ops(),
                &scoped_key,
            );
            let checksum = payload_checksum(value);
            let mut route = ObjectRoute {
                key: scoped_key.clone(),
                namespace: Some(mooncake_store_core::NamespaceScope::with_defaults(Some(tenant), None, None)),
                logical_key: Some(key.to_string()),
                canonical_key: None,
                sharing_scope: Some(tenant.to_string()),
                qos_tier: Some(mooncake_store_core::DEFAULT_QOS_TIER.to_string()),
                version: next_version,
                state: RouteState::Active,
                compatibility: self.lease.compatibility.clone(),
                replicas: targets
                    .iter()
                    .zip(offsets.iter())
                    .enumerate()
                    .map(|(priority, (target, offset))| ReplicaRoute {
                        owner: target.storage_runtime.clone(),
                        segment_name: target.segment_name.clone(),
                        offset: Some(*offset),
                        segment_offset: reservations[priority].offset_bytes,
                        length: value.len() as u64,
                        checksum: Some(checksum),
                        tier: ReplicaTier::Dram,
                        priority: priority as u16,
                    })
                    .collect(),
            };
            mooncake_store_core::apply_route_identity(&mut route, object_id);
            let cas_tracker = OperationTracker::new("put_stage_route_cas")
                .attribute_str("mooncake.tenant", tenant)
                .attribute_u64("mooncake.item_count", 1);
            let publish_started = Instant::now();
            let cas_result = self
                .route_ops()
                .publish_route(&route.key, expected_version, &route);
            registry::record_replication_publish(
                match &cas_result {
                    Ok(cas) if cas.applied => "ok",
                    Ok(_) => "conflict",
                    Err(StoreError::Conflict(_)) => "conflict",
                    Err(_) => "error",
                },
                publish_started.elapsed(),
            );
            cas_tracker.finish(&cas_result, 0);
            let cas = cas_result?;
            if !cas.applied {
                self.best_effort_release_reserved_allocations(
                    &targets,
                    &reservations,
                    "put_stage_route_cas_conflict",
                );
                let _ = self.abort_tenant_quota_reservation(
                    quota_reservation.as_ref(),
                    "route_compare_and_swap_conflict",
                );
                if attempt < max_write_attempts {
                    current = cas.current;
                    continue;
                }
                return Err(StoreError::Conflict(format!(
                    "route update lost race for tenant={tenant} key={key}"
                )));
            }
            route.qos_tier = Some(qos_tier.to_string());
            self.storage_owner.track_route(&route);
            self.track_remote_storage_owners_best_effort(std::slice::from_ref(&route));
            let finalize_result =
                self.finalize_tenant_quota_put(quota_reservation.as_ref(), &route, value.len());
            finalize_result?;
            log_route_publish_sample(&self.lease.runtime, &route, value.len(), "put");
            if let Some(previous) = current.as_ref() {
                if let Err(error) = self.reclaim_route(previous, reclaim_mode) {
                    warn!(
                        runtime = %self.lease.runtime,
                        tenant,
                        key,
                        error = %error,
                        "route overwrite reclaim failed after authoritative publish"
                    );
                }
            }
            return Ok(route);
        }
    }

    fn put_object(
        &self,
        object: &ObjectRef<'_>,
        value: &[u8],
        policy: Option<&ReplicationPolicy>,
    ) -> Result<ObjectRoute> {
        let tenant = object.tenant.unwrap_or(self.default_tenant());
        let object_id = LogicalObjectId::new(
            NamespaceScope::with_defaults(Some(tenant), object.domain, object.object_set),
            object.key,
        );
        let load_tracker = OperationTracker::new("put_stage_load_route")
            .attribute_str("mooncake.tenant", tenant)
            .attribute_u64("mooncake.item_count", 1);
        let current_result = self
            .route_ops()
            .load_route(&mooncake_store_core::ObjectKey::from_logical_id(&object_id));
        load_tracker.finish(&current_result, 0);
        let current = current_result?;
        self.put_object_with_policy_current(
            &object_id,
            object.qos_tier,
            value,
            PutObjectCurrentOptions {
                registered_source: None,
                policy,
                current: current.as_ref(),
                reclaim_mode: ReclaimMode::Scheduled,
            },
        )
    }

    fn put_object_from_registered(
        &self,
        object: &ObjectRef<'_>,
        buffer: *mut c_void,
        size: usize,
        policy: Option<&ReplicationPolicy>,
    ) -> Result<ObjectRoute> {
        let tenant = object.tenant.unwrap_or(self.default_tenant());
        let object_id = LogicalObjectId::new(
            NamespaceScope::with_defaults(Some(tenant), object.domain, object.object_set),
            object.key,
        );
        let load_tracker = OperationTracker::new("put_stage_load_route")
            .attribute_str("mooncake.tenant", tenant)
            .attribute_u64("mooncake.item_count", 1);
        let current_result = self
            .route_ops()
            .load_route(&mooncake_store_core::ObjectKey::from_logical_id(&object_id));
        load_tracker.finish(&current_result, 0);
        let current = current_result?;
        let value = unsafe { slice::from_raw_parts(buffer.cast::<u8>(), size) };
        self.put_object_with_policy_current(
            &object_id,
            object.qos_tier,
            value,
            PutObjectCurrentOptions {
                registered_source: Some(buffer),
                policy,
                current: current.as_ref(),
                reclaim_mode: ReclaimMode::Scheduled,
            },
        )
    }

    fn drain_segment_internal(
        &self,
        segment: &SegmentName,
        allow_last_active_primary: bool,
    ) -> Result<()> {
        self.ensure_local_memory()?;
        let primary = self.segment_name()?;
        let (active_count, current_state) = {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            let active_count = memory
                .storage_segments()
                .into_iter()
                .filter(|entry| entry.state == SegmentLifecycleState::Active)
                .count();
            let current_state = memory
                .storage_segments()
                .into_iter()
                .find(|entry| entry.segment_name == *segment)
                .map(|entry| entry.state)
                .ok_or_else(|| StoreError::NotFound(format!("segment {} not found", segment.0)))?;
            (active_count, current_state)
        };
        if current_state == SegmentLifecycleState::Draining {
            return Ok(());
        }
        if current_state != SegmentLifecycleState::Active {
            return Err(StoreError::InvalidState(format!(
                "segment {} is not active",
                segment.0
            )));
        }
        if !allow_last_active_primary && active_count <= 1 && *segment == primary {
            return Err(StoreError::InvalidState(
                "cannot drain the last active local segment".to_string(),
            ));
        }
        {
            let mut state = self.state.lock();
            state
                .memory_mut()?
                .update_storage_state(segment, SegmentLifecycleState::Draining)?;
        }
        self.allocator
            .lock()
            .update_state(segment, SegmentLifecycleState::Draining)?;
        self.metadata.update_segment_state(
            &self.lease.runtime,
            segment,
            SegmentLifecycleState::Draining,
        )
    }

    fn active_compatible_runtimes(&self, force_refresh: bool) -> Result<BTreeSet<ClientRuntimeId>> {
        Ok(self
            .available_compatible_live_clients(force_refresh)?
            .into_iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
            .map(|lease| lease.runtime)
            .collect())
    }

    fn active_compatible_runtimes_with_refresh_fallback(
        &self,
    ) -> Result<BTreeSet<ClientRuntimeId>> {
        let active = self.active_compatible_runtimes(false)?;
        if !active.is_empty() {
            return Ok(active);
        }
        self.active_compatible_runtimes(true)
    }

    fn collect_routes_by_replica_owner(&self, owner: &ClientRuntimeId) -> Result<Vec<ObjectRoute>> {
        self.route_ops().list_routes_by_replica_owner(owner)
    }

    fn route_object_id(&self, route: &ObjectRoute) -> Result<LogicalObjectId> {
        mooncake_store_core::route_logical_object_id(route)
    }

    fn migration_policy_for_route(
        &self,
        route: &ObjectRoute,
        source_owner: &ClientRuntimeId,
    ) -> Result<ReplicationPolicy> {
        let active = self.active_compatible_runtimes_with_refresh_fallback()?;
        let preferred = route
            .replicas
            .iter()
            .filter(|replica| replica.owner != *source_owner)
            .filter(|replica| active.contains(&replica.owner))
            .map(|replica| replica.owner.storage_key())
            .collect::<BTreeSet<_>>();
        Ok(ReplicationPolicy::new()
            .replica_count(route.replicas.len().max(1))
            .prefer_local(false)
            .with_soft_pin(true)
            .preferred_storage_owners(preferred))
    }

    fn migration_policy_for_successor_route(
        &self,
        route: &ObjectRoute,
        source_owner: &ClientRuntimeId,
        successor: &ClientRuntimeId,
    ) -> Result<ReplicationPolicy> {
        let active = self.active_compatible_runtimes_with_refresh_fallback()?;
        let mut preferred = Vec::new();
        let mut seen = BTreeSet::new();

        let successor_key = successor.storage_key();
        seen.insert(successor_key.clone());
        preferred.push(successor_key);

        for replica in route.replicas.iter().filter(|replica| {
            replica.owner != *source_owner
                && replica.owner != *successor
                && active.contains(&replica.owner)
        }) {
            let owner = replica.owner.storage_key();
            if seen.insert(owner.clone()) {
                preferred.push(owner);
            }
        }

        Ok(ReplicationPolicy::new()
            .replica_count(route.replicas.len().max(1))
            .prefer_local(false)
            .with_soft_pin(true)
            .preferred_storage_owners(preferred))
    }

    fn resolve_explicit_source_replica<'a>(
        current: &'a ObjectRoute,
        selector: &ReplicaReadSelector,
    ) -> Result<&'a ReplicaRoute> {
        match selector {
            ReplicaReadSelector::Segment(segment_name) => current
                .replicas
                .iter()
                .find(|replica| replica.segment_name == *segment_name)
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "source segment {} is not present in route {}",
                        segment_name.0, current.key.0
                    ))
                }),
            ReplicaReadSelector::OwnerAndSegment {
                owner,
                segment_name,
            } => current
                .replicas
                .iter()
                .find(|replica| replica.owner == *owner && replica.segment_name == *segment_name)
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "source replica {}:{} is not present in route {}",
                        owner, segment_name.0, current.key.0
                    ))
                }),
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn explicit_migration_policy(plan: &ExplicitMigrationPlan) -> Result<ReplicationPolicy> {
        if plan.target_segments.is_empty() {
            return Err(StoreError::InvalidState(
                "explicit migration requires at least one target segment".to_string(),
            ));
        }
        if !plan.all_or_nothing {
            return Err(StoreError::Unsupported(
                "partial-success explicit migration is not implemented".to_string(),
            ));
        }
        if matches!(plan.mode, ExplicitMigrationMode::Move) && plan.target_segments.len() != 1 {
            return Err(StoreError::InvalidState(
                "explicit move currently requires exactly one target segment".to_string(),
            ));
        }

        Ok(ReplicationPolicy::new()
            .replica_count(plan.target_segments.len())
            .prefer_local(false)
            .preferred_segments(
                plan.target_segments
                    .iter()
                    .map(|segment| segment.0.clone())
                    .collect::<Vec<_>>(),
            ))
    }

    fn read_payload_from_explicit_source(
        &self,
        object_id: &LogicalObjectId,
        route: &ObjectRoute,
        source: &ReplicaRoute,
    ) -> Result<Vec<u8>> {
        self.ensure_local_memory()?;
        let transport = self.transport()?;
        let mut payload = vec![0u8; source.length as usize];
        let resolved = ResolvedObject {
            tenant: object_id.scope.tenant.clone(),
            key: object_id.logical_key.clone(),
            route: route.clone(),
            replica: source.clone(),
            fallback_replicas: VecDeque::new(),
        };
        let deadline = self.request_deadline_for_transfer(source.length, 1);
        let length = source.length as usize;
        if !self.copy_local_replica_direct(transport, source, &mut payload, length)? {
            self.ensure_explicit_source_reachable(source)?;
            self.execute_remote_get_direct(transport, &resolved, &mut payload, length, deadline)?;
        }
        validate_replica_checksum(source, &payload[..length])?;
        Ok(payload)
    }

    fn ensure_explicit_source_reachable(&self, source: &ReplicaRoute) -> Result<()> {
        if source.owner == self.lease.runtime {
            return Ok(());
        }
        let lease = self
            .metadata
            .get_client_lease(&source.owner)?
            .filter(|lease| compatibility_matches(&self.lease, lease))
            .ok_or_else(|| {
                StoreError::NotFound(format!("runtime {} is not available", source.owner))
            })?;
        if !lease.state.serves_reads() {
            return Err(StoreError::InvalidState(format!(
                "runtime {} is not readable while {:?}",
                source.owner, lease.state
            )));
        }
        match self.control_client.probe_reachability(&lease) {
            crate::control_plane::ControlPlaneReachability::Reachable => Ok(()),
            crate::control_plane::ControlPlaneReachability::Unreachable => {
                self.mark_runtime_suspect(&source.owner, "explicit_source_control_unreachable");
                Err(StoreError::Transport(format!(
                    "explicit source runtime {} is unreachable before opening {}",
                    source.owner, source.segment_name.0
                )))
            }
            crate::control_plane::ControlPlaneReachability::Unknown(error) => Err(error),
        }
    }

    fn explicit_migration_object_ref<'a>(
        object_id: &'a LogicalObjectId,
        qos_tier: Option<&'a str>,
    ) -> ObjectRef<'a> {
        let mut object_ref = ObjectRef::new(object_id.logical_key.as_str())
            .tenant(object_id.scope.tenant.as_str());
        if object_id.scope.domain != mooncake_store_core::DEFAULT_DOMAIN {
            object_ref = object_ref.domain(object_id.scope.domain.as_str());
        }
        if object_id.scope.object_set != mooncake_store_core::DEFAULT_OBJECT_SET {
            object_ref = object_ref.object_set(object_id.scope.object_set.as_str());
        }
        if let Some(qos_tier) = qos_tier
            .filter(|tier| *tier != mooncake_store_core::DEFAULT_QOS_TIER)
        {
            object_ref = object_ref.qos_tier(qos_tier);
        }
        object_ref
    }

    fn execute_explicit_route_migration(
        &self,
        object_id: &LogicalObjectId,
        plan: &ExplicitMigrationPlan,
    ) -> Result<ObjectRoute> {
        self.ensure_local_memory()?;
        self.flush_due_reclaims()?;

        let current = self
            .route_ops()
            .load_route(&ObjectKey::from_logical_id(object_id))?
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "tenant={} key={} has no active route",
                    object_id.scope.tenant, object_id.logical_key
                ))
            })?;
        if current.state != RouteState::Active {
            return Err(StoreError::NotFound(format!(
                "tenant={} key={} is not active",
                object_id.scope.tenant, object_id.logical_key
            )));
        }

        let source = Self::resolve_explicit_source_replica(&current, &plan.source)?.clone();
        let payload = self.read_payload_from_explicit_source(object_id, &current, &source)?;
        let object_ref =
            Self::explicit_migration_object_ref(object_id, current.qos_tier.as_deref());
        let (targets, reservations) =
            self.reserve_explicit_migration_targets(&object_ref, payload.len(), plan)?;
        let offsets =
            match self.write_reserved_replicas(&targets, &reservations, &payload, None) {
                Ok(offsets) => offsets,
            Err(error) => {
                self.best_effort_release_reserved_allocations(
                    &targets,
                    &reservations,
                    "explicit_migration_write_failed",
                );
                self.note_remote_write_failure(&targets, &error, "explicit_migration_write_failed");
                return Err(error);
            }
        };

        let checksum = payload_checksum(&payload);
        let explicit_targets = targets
            .iter()
            .zip(offsets.iter())
            .enumerate()
            .map(|(priority, (target, offset))| ReplicaRoute {
                owner: target.storage_runtime.clone(),
                segment_name: target.segment_name.clone(),
                offset: Some(*offset),
                segment_offset: reservations[priority].offset_bytes,
                length: payload.len() as u64,
                checksum: Some(checksum),
                tier: ReplicaTier::Dram,
                priority: priority as u16,
            })
            .collect::<Vec<_>>();

        let next_route = match plan.mode {
            ExplicitMigrationMode::Copy => {
                Self::build_explicit_copy_route_delta(&current, &source.segment_name, explicit_targets)?
            }
            ExplicitMigrationMode::Move => {
                let target = explicit_targets
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        StoreError::InvalidState(
                            "explicit move requires exactly one written target".to_string(),
                        )
                    })?;
                Self::build_explicit_move_route_delta(&current, &source.segment_name, target)?
            }
        };

        let cas = match self
            .route_ops()
            .repair_route(&current.key, Some(current.version), &next_route)
        {
            Ok(cas) => cas,
            Err(error) => {
                self.best_effort_release_reserved_allocations(
                    &targets,
                    &reservations,
                    "explicit_migration_route_cas_error",
                );
                return Err(error);
            }
        };
        if !cas.applied {
            self.best_effort_release_reserved_allocations(
                &targets,
                &reservations,
                "explicit_migration_route_race",
            );
            return Err(StoreError::Conflict(format!(
                "explicit migration lost route race for tenant={} key={}",
                object_id.scope.tenant, object_id.logical_key
            )));
        }

        self.storage_owner.track_route(&next_route);
        self.track_remote_storage_owners_best_effort(std::slice::from_ref(&next_route));

        if matches!(plan.mode, ExplicitMigrationMode::Move) {
            let mut removed_source = current.clone();
            removed_source.replicas = vec![source];
            if let Err(error) = self.reclaim_route(&removed_source, ReclaimMode::Immediate) {
                warn!(
                    runtime = %self.lease.runtime,
                    tenant = %object_id.scope.tenant,
                    key = %object_id.logical_key,
                    error = %error,
                    "explicit move reclaim failed after authoritative publish"
                );
            }
        }

        Ok(next_route)
    }

    fn reserve_explicit_migration_targets(
        &self,
        object: &ObjectRef<'_>,
        length_bytes: usize,
        plan: &ExplicitMigrationPlan,
    ) -> Result<(
        Vec<ReplicaWriteTarget>,
        Vec<mooncake_store_core::SegmentReservation>,
    )> {
        let tenant = object.tenant.unwrap_or(self.default_tenant());
        let key = object.key;
        let mut targets = Vec::with_capacity(plan.target_segments.len());
        let mut reservations = Vec::with_capacity(plan.target_segments.len());

        for requested_segment in &plan.target_segments {
            let preferred = self.lookup_preferred_segment(requested_segment).map_err(|error| {
                StoreError::NotFound(format!(
                    "explicit migration target segment {} is unavailable: {error}",
                    requested_segment.0
                ))
            })?;
            if self.runtime_is_suspect(&preferred.owner) {
                self.best_effort_release_reserved_allocations(
                    &targets,
                    &reservations,
                    "explicit_migration_target_runtime_suspect",
                );
                return Err(StoreError::Transport(format!(
                    "explicit migration target segment {} belongs to suspect runtime {}",
                    requested_segment.0, preferred.owner
                )));
            }
            let is_local = preferred.owner == self.lease.runtime;
            let (target, reservation) = match self.reserve_specific_segment(
                &preferred.owner,
                &preferred.segment_name,
                length_bytes,
                is_local,
            ) {
                Ok(result) => result,
                Err(error) => {
                    self.best_effort_release_reserved_allocations(
                        &targets,
                        &reservations,
                        "explicit_migration_target_reserve_failed",
                    );
                    return Err(error);
                }
            };
            if target.storage_runtime != preferred.owner || target.segment_name != preferred.segment_name
            {
                self.best_effort_release_reserved_allocations(
                    &targets,
                    &reservations,
                    "explicit_migration_target_reserve_mismatch",
                );
                return Err(StoreError::Conflict(format!(
                    "explicit migration reserved target {} on {} instead of requested {} on {}",
                    target.segment_name.0,
                    target.storage_runtime,
                    preferred.segment_name.0,
                    preferred.owner
                )));
            }
            debug!(
                tenant,
                key,
                storage_runtime = %target.storage_runtime,
                segment = %target.segment_name.0,
                "reserved explicit migration target"
            );
            targets.push(target);
            reservations.push(reservation);
        }

        Ok((targets, reservations))
    }

    fn build_explicit_copy_route_delta(
        current: &ObjectRoute,
        source_segment: &SegmentName,
        targets: Vec<ReplicaRoute>,
    ) -> Result<ObjectRoute> {
        if !current
            .replicas
            .iter()
            .any(|replica| replica.segment_name == *source_segment)
        {
            return Err(StoreError::NotFound(format!(
                "source segment {} is not present in route {}",
                source_segment.0, current.key.0
            )));
        }
        Self::validate_explicit_route_targets(current, source_segment, &targets, false)?;
        let mut next = current.clone();
        next.version = current.version.next();
        next.replicas.extend(targets);
        Self::normalize_route_replica_priorities(&mut next.replicas);
        Ok(next)
    }

    fn build_explicit_move_route_delta(
        current: &ObjectRoute,
        source_segment: &SegmentName,
        target: ReplicaRoute,
    ) -> Result<ObjectRoute> {
        if target.segment_name == *source_segment {
            return Err(StoreError::Conflict(format!(
                "move target segment {} cannot be the same as source",
                source_segment.0
            )));
        }
        let source_index = current
            .replicas
            .iter()
            .position(|replica| replica.segment_name == *source_segment)
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "source segment {} is not present in route {}",
                    source_segment.0, current.key.0
                ))
            })?;
        Self::validate_explicit_route_targets(current, source_segment, std::slice::from_ref(&target), true)?;
        let mut next = current.clone();
        next.version = current.version.next();
        next.replicas.remove(source_index);
        next.replicas.push(target);
        Self::normalize_route_replica_priorities(&mut next.replicas);
        Ok(next)
    }

    fn validate_explicit_route_targets(
        current: &ObjectRoute,
        source_segment: &SegmentName,
        targets: &[ReplicaRoute],
        moving: bool,
    ) -> Result<()> {
        let mut seen = BTreeSet::new();
        for target in targets {
            if target.segment_name == *source_segment {
                return Err(StoreError::Conflict(format!(
                    "target segment {} conflicts with source segment",
                    source_segment.0
                )));
            }
            if !seen.insert(target.segment_name.clone()) {
                return Err(StoreError::Conflict(format!(
                    "duplicate target segment {} in explicit migration",
                    target.segment_name.0
                )));
            }
            let conflicts_with_current = current
                .replicas
                .iter()
                .any(|replica| replica.segment_name == target.segment_name);
            if conflicts_with_current {
                let message = if moving {
                    "move target segment already exists in route"
                } else {
                    "copy target segment already exists in route"
                };
                return Err(StoreError::Conflict(format!(
                    "{message}: {}",
                    target.segment_name.0
                )));
            }
        }
        Ok(())
    }

    fn normalize_route_replica_priorities(replicas: &mut [ReplicaRoute]) {
        for (priority, replica) in replicas.iter_mut().enumerate() {
            replica.priority = priority as u16;
        }
    }

    fn migrate_owned_route_via_writer(
        &self,
        writer: &StoreClient,
        route: &ObjectRoute,
    ) -> Result<bool> {
        let object_id = self.route_object_id(route)?;
        for _ in 0..4 {
            let Some(observed) = self.query_route_by_object_id(&object_id)? else {
                return Ok(false);
            };
            if observed.state != RouteState::Active {
                return Ok(false);
            }
            if !observed
                .replicas
                .iter()
                .any(|replica| replica.owner == self.lease.runtime)
            {
                return Ok(false);
            }

            let Some(confirmed) = self.query_route_by_object_id(&object_id)? else {
                return Ok(false);
            };
            if confirmed.version != observed.version {
                continue;
            }
            if !confirmed
                .replicas
                .iter()
                .any(|replica| replica.owner == self.lease.runtime)
            {
                return Ok(false);
            }

            let Some(source) =
                Self::matching_owned_replica(&observed, &confirmed, &self.lease.runtime).cloned()
            else {
                continue;
            };
            let payload = writer.read_payload_from_explicit_source(&object_id, &confirmed, &source)?;
            let policy = writer.migration_policy_for_route(&confirmed, &self.lease.runtime)?;
            match writer.put_object_with_policy_current(
                &object_id,
                confirmed.qos_tier.as_deref(),
                &payload,
                PutObjectCurrentOptions {
                    registered_source: None,
                    policy: Some(&policy),
                    current: Some(&confirmed),
                    reclaim_mode: ReclaimMode::Deferred,
                },
            ) {
                Ok(next) => {
                    writer.ensure_object_route_at_least(&next)?;
                    self.reclaim_route(&confirmed, ReclaimMode::Immediate)?;
                    registry::record_rebalance_route("migrate", "ok");
                    registry::record_rebalance_bytes(
                        "migrate",
                        observed
                            .replicas
                            .iter()
                            .filter(|replica| replica.owner == self.lease.runtime)
                            .map(|replica| replica.length)
                            .sum(),
                    );
                    return Ok(true);
                }
                Err(StoreError::Conflict(_)) => continue,
                Err(error) => {
                    registry::record_rebalance_route("migrate", "error");
                    return Err(error);
                }
            }
        }
        registry::record_rebalance_route("migrate", "conflict");
        Err(StoreError::Conflict(format!(
            "client shrink lost route update race for {}",
            route.key.0
        )))
    }

    fn migrate_owned_route(&self, route: &ObjectRoute) -> Result<bool> {
        self.migrate_owned_route_via_writer(self, route)
    }

    fn migrate_owned_route_to_runtime(
        &self,
        successor: &ClientRuntimeId,
        route: &ObjectRoute,
    ) -> Result<bool> {
        self.migrate_owned_route_to_runtime_via_writer(self, successor, route)
    }

    fn migrate_owned_route_to_runtime_via_writer(
        &self,
        writer: &StoreClient,
        successor: &ClientRuntimeId,
        route: &ObjectRoute,
    ) -> Result<bool> {
        let object_id = self.route_object_id(route)?;
        let tenant = object_id.scope.tenant.clone();
        let key = object_id.logical_key.clone();
        for _ in 0..4 {
            let Some(observed) = self.query_route_in_tenant(&tenant, &key)? else {
                return Ok(false);
            };
            if observed.state != RouteState::Active {
                return Ok(false);
            }
            if !observed
                .replicas
                .iter()
                .any(|replica| replica.owner == self.lease.runtime)
            {
                return Ok(false);
            }

            let Some(confirmed) = self.query_route_in_tenant(&tenant, &key)? else {
                return Ok(false);
            };
            if confirmed.version != observed.version {
                continue;
            }
            if !confirmed
                .replicas
                .iter()
                .any(|replica| replica.owner == self.lease.runtime)
            {
                return Ok(false);
            }

            let Some(source) =
                Self::matching_owned_replica(&observed, &confirmed, &self.lease.runtime).cloned()
            else {
                continue;
            };
            let payload = writer.read_payload_from_explicit_source(&object_id, &confirmed, &source)?;
            let policy = writer.migration_policy_for_successor_route(
                &confirmed,
                &self.lease.runtime,
                successor,
            )?;
            let qos_tier = confirmed.qos_tier.as_deref();
            let object_id = writer.route_object_id(&confirmed)?;
            match writer.put_object_with_policy_current(
                &object_id,
                qos_tier,
                &payload,
                PutObjectCurrentOptions {
                    registered_source: None,
                    policy: Some(&policy),
                    current: Some(&confirmed),
                    reclaim_mode: ReclaimMode::Deferred,
                },
            ) {
                Ok(next) => {
                    writer.ensure_object_route_at_least(&next)?;
                    self.reclaim_route(&confirmed, ReclaimMode::Immediate)?;
                    registry::record_rebalance_route("migrate", "ok");
                    registry::record_rebalance_bytes(
                        "migrate",
                        observed
                            .replicas
                            .iter()
                            .filter(|replica| replica.owner == self.lease.runtime)
                            .map(|replica| replica.length)
                            .sum(),
                    );
                    return Ok(true);
                }
                Err(StoreError::Conflict(_)) => continue,
                Err(error) => {
                    registry::record_rebalance_route("migrate", "error");
                    return Err(error);
                }
            }
        }
        registry::record_rebalance_route("migrate", "conflict");
        Err(StoreError::Conflict(format!(
            "client hot-upgrade lost route update race for {}",
            route.key.0
        )))
    }

    fn matching_owned_replica<'a>(
        observed: &ObjectRoute,
        confirmed: &'a ObjectRoute,
        owner: &ClientRuntimeId,
    ) -> Option<&'a ReplicaRoute> {
        observed
            .replicas
            .iter()
            .filter(|replica| replica.owner == *owner)
            .find_map(|observed| {
                confirmed.replicas.iter().find(|candidate| {
                    candidate.owner == observed.owner
                        && candidate.segment_name == observed.segment_name
                        && candidate.segment_offset == observed.segment_offset
                        && candidate.length == observed.length
                })
            })
    }

    fn current_owned_allocations(&self) -> Result<BTreeSet<AllocationSpan>> {
        let mut allocations = BTreeSet::new();
        for route in self.collect_routes_by_replica_owner(&self.lease.runtime)? {
            let object_id = self.route_object_id(&route)?;
            let tenant = object_id.scope.tenant;
            let key = object_id.logical_key;
            let Some(current) = self.query_route_in_tenant(&tenant, &key)? else {
                continue;
            };
            if current.state != RouteState::Active {
                continue;
            }
            for replica in current
                .replicas
                .iter()
                .filter(|replica| replica.owner == self.lease.runtime)
            {
                allocations.insert(AllocationSpan {
                    segment_name: replica.segment_name.clone(),
                    offset_bytes: replica.segment_offset,
                    length_bytes: replica.length,
                });
            }
        }
        Ok(allocations)
    }

    fn ensure_object_route_at_least(&self, route: &ObjectRoute) -> Result<()> {
        let route_ops = self.route_ops();
        let Some(current) = route_ops.load_route(&route.key)? else {
            let result = route_ops.publish_route(&route.key, None, route)?;
            if result.applied
                || result
                    .current
                    .as_ref()
                    .is_some_and(|current| current.version >= route.version)
            {
                return Ok(());
            }
            return Err(StoreError::Conflict(format!(
                "route {} update lost to older route version",
                route.key.0
            )));
        };
        if current.version >= route.version {
            return Ok(());
        }
        let result = route_ops.publish_route(&route.key, Some(current.version), route)?;
        if result.applied
            || result
                .current
                .as_ref()
                .is_some_and(|current| current.version >= route.version)
        {
            return Ok(());
        }
        Err(StoreError::Conflict(format!(
            "route {} update lost to older route version",
            route.key.0
        )))
    }

    fn release_stale_local_allocations(
        &self,
        live_allocations: &BTreeSet<AllocationSpan>,
    ) -> Result<usize> {
        let stale = self
            .allocator
            .lock()
            .stale_allocations(live_allocations, now_ms());
        if stale.is_empty() {
            return Ok(0);
        }
        let releases = stale
            .iter()
            .map(|allocation| AllocationReleaseRequest {
                storage_runtime: self.lease.runtime.clone(),
                segment_name: allocation.segment_name.clone(),
                offset_bytes: allocation.offset_bytes,
                length_bytes: allocation.length_bytes,
            })
            .collect::<Vec<_>>();
        self.release_segment_allocations_batch(&releases)?;
        Ok(stale.len())
    }

    fn retire_empty_draining_segments(&self) -> Result<()> {
        let segments = self.list_segments()?;
        for segment in segments
            .into_iter()
            .filter(|segment| segment.state == SegmentLifecycleState::Draining)
            .filter(|segment| segment.used_bytes == 0)
        {
            let _ = self.retire_segment(&segment.segment_name)?;
        }
        Ok(())
    }

    fn format_remaining_live_segments(segments: &[SegmentAnnouncement]) -> String {
        segments
            .iter()
            .map(|segment| format!("{}:{}", segment.segment_name.0, segment.used_bytes))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn remaining_live_segments(&self) -> Result<Vec<SegmentAnnouncement>> {
        Ok(self
            .list_segments()?
            .into_iter()
            .filter(|segment| segment.used_bytes != 0)
            .collect::<Vec<_>>())
    }

    fn next_pending_publish_deadline_ms(&self) -> Option<u64> {
        self.allocator.lock().next_pending_deadline_ms(now_ms())
    }

    fn wait_for_pending_publish_or_retry(&self) {
        let now = now_ms();
        let delay_ms = self
            .next_pending_publish_deadline_ms()
            .map(|deadline_ms| deadline_ms.saturating_sub(now).min(25))
            .unwrap_or(10);
        if delay_ms != 0 {
            std::thread::sleep(Duration::from_millis(delay_ms));
        }
    }

    fn evacuate_draining_routes_until_stable<F>(
        &self,
        remaining_error_prefix: &str,
        mut migrate_route: F,
    ) -> Result<usize>
    where
        F: FnMut(&ObjectRoute) -> Result<bool>,
    {
        self.ensure_local_memory()?;
        self.pin_draining_lease_for_evacuation()?;
        let segments = {
            let state = self.state.lock();
            state.memory_ref()?.storage_segments()
        };
        for segment in segments
            .iter()
            .filter(|segment| segment.state == SegmentLifecycleState::Active)
        {
            self.drain_segment_internal(&segment.segment_name, true)?;
        }
        self.flush_all_reclaims()?;

        let mut migrated = 0usize;
        loop {
            self.pin_draining_lease_for_evacuation()?;
            let routes = self.collect_routes_by_replica_owner(&self.lease.runtime)?;
            let had_routes = !routes.is_empty();
            for route in routes {
                if migrate_route(&route)? {
                    migrated = migrated.saturating_add(1);
                }
            }

            self.flush_all_reclaims()?;
            let live_allocations = self.current_owned_allocations()?;
            let _ = self.release_stale_local_allocations(&live_allocations)?;
            self.flush_all_reclaims()?;
            self.retire_empty_draining_segments()?;

            let remaining = self.remaining_live_segments()?;
            if remaining.is_empty() {
                return Ok(migrated);
            }

            if had_routes || self.next_pending_publish_deadline_ms().is_some() {
                self.wait_for_pending_publish_or_retry();
                continue;
            }

            return Err(StoreError::InvalidState(format!(
                "{remaining_error_prefix}: {}",
                Self::format_remaining_live_segments(&remaining)
            )));
        }
    }

    fn select_readable_replica(
        route: &ObjectRoute,
        local_runtime: &ClientRuntimeId,
        local_segments: &BTreeSet<SegmentName>,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) -> Option<ReplicaRoute> {
        route
            .replicas
            .iter()
            .filter(|replica| {
                Self::replica_is_readable(replica, local_runtime, local_segments, readable_runtimes)
            })
            .min_by_key(|replica| {
                (
                    !local_segments.contains(&replica.segment_name),
                    replica.priority,
                )
            })
            .cloned()
    }

    fn replica_is_readable(
        replica: &ReplicaRoute,
        local_runtime: &ClientRuntimeId,
        local_segments: &BTreeSet<SegmentName>,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) -> bool {
        if replica.owner == *local_runtime {
            return local_segments.contains(&replica.segment_name);
        }
        readable_runtimes.contains(&replica.owner)
    }

    fn filter_routes_to_readable(
        &self,
        routes: Vec<Option<ObjectRoute>>,
    ) -> Result<Vec<Option<ObjectRoute>>> {
        let mut routes = routes
            .into_iter()
            .map(|route| route.filter(|route| route.state == RouteState::Active))
            .collect::<Vec<_>>();

        let local_segments = self.local_storage_segments();
        let readable_runtimes = self.readable_runtime_set(false)?;

        let needs_refresh = routes
            .iter()
            .filter_map(Option::as_ref)
            .any(|route| {
                Self::select_readable_replica(
                    route,
                    &self.lease.runtime,
                    &local_segments,
                    &readable_runtimes,
                )
                .is_none()
            });
        let readable_runtimes = if needs_refresh {
            self.readable_runtime_set(true)?
        } else {
            readable_runtimes
        };

        for route in &mut routes {
            let Some(candidate) = route.as_ref() else {
                continue;
            };
            let Some(replica) = Self::select_readable_replica(
                candidate,
                &self.lease.runtime,
                &local_segments,
                &readable_runtimes,
            ) else {
                *route = None;
                continue;
            };
            if candidate
                .replicas
                .iter()
                .min_by_key(|candidate| candidate.priority)
                .is_some_and(|primary| primary.owner != replica.owner)
            {
                self.prune_unreadable_replicas_best_effort(candidate, &readable_runtimes);
            }
        }

        self.report_readable_route_hits_best_effort(
            routes.iter().filter_map(Option::as_ref),
            &local_segments,
            &readable_runtimes,
        );
        Ok(routes)
    }

    fn select_readable_replica_with_refresh(
        &self,
        route: &ObjectRoute,
    ) -> Result<Option<ReplicaRoute>> {
        let tracker =
            OperationTracker::new("readable_replica_select").scope("readable_replica_select");
        let result = (|| {
            if route.state != RouteState::Active {
                return Ok(None);
            }
            let local_segments = self.local_storage_segments();
            let readable_runtimes = self.readable_runtime_set(false)?;
            if let Some(replica) =
                Self::select_readable_replica(route, &self.lease.runtime, &local_segments, &readable_runtimes)
            {
                return Ok(Some(replica));
            }
            let refreshed_readable = self.readable_runtime_set(true)?;
            Ok(Self::select_readable_replica(
                route,
                &self.lease.runtime,
                &local_segments,
                &refreshed_readable,
            ))
        })();
        tracker.finish(
            &result,
            result
                .as_ref()
                .ok()
                .and_then(|replica| replica.as_ref())
                .map(|replica| replica.length)
                .unwrap_or(0),
        );
        result
    }

    fn local_storage_segments(&self) -> BTreeSet<SegmentName> {
        self.state
            .lock()
            .memory_ref()
            .ok()
            .map(|m| m.all_storage_segment_names())
            .unwrap_or_default()
    }

    fn prune_unreadable_replicas_best_effort(
        &self,
        route: &ObjectRoute,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) {
        let filtered = route
            .replicas
            .iter()
            .filter(|replica| readable_runtimes.contains(&replica.owner))
            .cloned()
            .collect::<Vec<_>>();
        if filtered.is_empty() || filtered.len() == route.replicas.len() {
            return;
        }

        let mut next = route.clone();
        next.version = route.version.next();
        next.replicas = filtered
            .into_iter()
            .enumerate()
            .map(|(priority, mut replica)| {
                replica.priority = priority as u16;
                replica
            })
            .collect::<Vec<_>>();

        match self
            .route_ops()
            .prune_route(&route.key, Some(route.version), &next)
        {
            Ok(cas) if cas.applied => debug!(
                runtime = %self.lease.runtime,
                key = %route.key.0,
                old_replicas = route.replicas.len(),
                new_replicas = next.replicas.len(),
                "pruned unreadable replica owners from stale route"
            ),
            Ok(_) => debug!(
                runtime = %self.lease.runtime,
                key = %route.key.0,
                "stale route prune lost compare-and-swap race"
            ),
            Err(error) => debug!(
                runtime = %self.lease.runtime,
                key = %route.key.0,
                error = %error,
                "stale route prune failed"
            ),
        }
    }

    fn has_same_replica(left: &ReplicaRoute, right: &ReplicaRoute) -> bool {
        left.owner == right.owner
            && left.segment_name == right.segment_name
            && left.segment_offset == right.segment_offset
            && left.length == right.length
    }

    fn should_retry_put_after_write_failure(
        &self,
        error: &StoreError,
        policy: &ResolvedReplicationPolicy,
    ) -> bool {
        policy.required_preferred_segments.is_empty()
            && matches!(
                error,
                StoreError::Transport(_) | StoreError::NotFound(_) | StoreError::InvalidState(_)
            )
    }

    fn note_remote_write_failure(
        &self,
        targets: &[ReplicaWriteTarget],
        error: &StoreError,
        context: &'static str,
    ) {
        if !matches!(
            error,
            StoreError::Transport(_) | StoreError::NotFound(_) | StoreError::InvalidState(_)
        ) {
            return;
        }
        let mut failed_runtimes = BTreeSet::new();
        {
            let mut state = self.state.lock();
            for target in targets {
                state.invalidate_remote_segment(&target.segment_name.0);
                if target.storage_runtime != self.lease.runtime {
                    failed_runtimes.insert(target.storage_runtime.clone());
                }
            }
        }
        for runtime in failed_runtimes {
            eprintln!(
                "[mooncake-store] marking remote write target suspect runtime={} context={} error={}",
                runtime, context, error
            );
            self.mark_runtime_suspect(&runtime, context);
        }
        let _ = refresh_live_client_cache(
            self.metadata.as_ref(),
            &self.live_client_cache,
            "live_client_snapshot_remote_write_failure",
        );
    }

    fn fallback_replicas_for_route(
        route: &ObjectRoute,
        selected: &ReplicaRoute,
        local_runtime: &ClientRuntimeId,
        local_segments: &BTreeSet<SegmentName>,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) -> VecDeque<ReplicaRoute> {
        let mut replicas = route
            .replicas
            .iter()
            .filter(|replica| !Self::has_same_replica(replica, selected))
            .filter(|replica| {
                Self::replica_is_readable(replica, local_runtime, local_segments, readable_runtimes)
            })
            .cloned()
            .collect::<Vec<_>>();
        replicas.sort_by_key(|replica| {
            (
                !local_segments.contains(&replica.segment_name),
                replica.priority,
            )
        });
        replicas.into()
    }

    fn try_advance_resolved_replica(
        &self,
        entry: &mut ResolvedObject,
        local_segments: &BTreeSet<SegmentName>,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) -> bool {
        while let Some(candidate) = entry.fallback_replicas.pop_front() {
            if !Self::replica_is_readable(
                &candidate,
                &self.lease.runtime,
                local_segments,
                readable_runtimes,
            ) {
                continue;
            }
            if candidate.length != entry.replica.length {
                debug!(
                    runtime = %self.lease.runtime,
                    tenant = %entry.tenant,
                    key = %entry.key,
                    current_length = entry.replica.length,
                    candidate_length = candidate.length,
                    "skipping fallback replica with mismatched length"
                );
                continue;
            }
            entry.replica = candidate;
            return true;
        }
        false
    }

    fn maybe_prune_route_after_failover(
        &self,
        entry: &ResolvedObject,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) {
        if entry
            .route
            .replicas
            .iter()
            .min_by_key(|candidate| candidate.priority)
            .is_some_and(|primary| primary.owner != entry.replica.owner)
        {
            self.prune_unreadable_replicas_best_effort(&entry.route, readable_runtimes);
        }
    }

    fn should_refresh_route_after_read_error(error: &StoreError) -> bool {
        match error {
            StoreError::Transport(_) | StoreError::NotFound(_) => true,
            StoreError::InvalidState(message) => message.contains("checksum mismatch"),
            _ => false,
        }
    }

    fn refresh_resolved_route_after_read_failure(
        &self,
        entry: &mut ResolvedObject,
    ) -> Result<bool> {
        let _ = refresh_live_client_cache(
            self.metadata.as_ref(),
            &self.live_client_cache,
            "live_client_snapshot_read_route_refresh",
        );
        let local_segments = self.local_storage_segments();
        let readable_runtimes = self.readable_runtime_set(true)?;
        let Some(route) = self.query_route_in_tenant(&entry.tenant, &entry.key)? else {
            return Ok(false);
        };
        if route.state != RouteState::Active {
            return Ok(false);
        }
        let Some(replica) = Self::select_readable_replica(
            &route,
            &self.lease.runtime,
            &local_segments,
            &readable_runtimes,
        ) else {
            return Ok(false);
        };
        let changed =
            route != entry.route || !Self::has_same_replica(&replica, &entry.replica);
        if !changed {
            return Ok(false);
        }
        let fallback_replicas = Self::fallback_replicas_for_route(
            &route,
            &replica,
            &self.lease.runtime,
            &local_segments,
            &readable_runtimes,
        );
        entry.route = route;
        entry.replica = replica;
        entry.fallback_replicas = fallback_replicas;
        self.maybe_prune_route_after_failover(entry, &readable_runtimes);
        Ok(true)
    }

    fn wait_for_route_refresh_retry(&self, request_deadline: RequestDeadline) -> bool {
        if request_deadline.has_expired() {
            return false;
        }
        let delay = request_deadline
            .remaining()
            .min(DEFAULT_ROUTE_REFRESH_RETRY_DELAY);
        if delay.is_zero() {
            return false;
        }
        sleep(delay);
        true
    }

    fn resolve_objects(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<ResolvedObject>> {
        let tracker = OperationTracker::new("route_lookup_many")
            .attribute_u64("mooncake.item_count", objects.len() as u64);
        let result = (|| {
            let scoped = objects
                .iter()
                .map(|object| {
                    let tenant = object.tenant.unwrap_or(self.default_tenant());
                    let object_id = LogicalObjectId::new(
                        NamespaceScope::with_defaults(Some(tenant), object.domain, object.object_set),
                        object.key,
                    );
                    (tenant.to_string(), mooncake_store_core::ObjectKey::from_logical_id(&object_id))
                })
                .collect::<Vec<_>>();
            let routes = self.route_ops().load_routes(
                &scoped
                    .iter()
                    .map(|(_, scoped)| scoped.clone())
                    .collect::<Vec<_>>(),
            )?;
            let local_segments = self.local_storage_segments();
            let readable_runtimes = self.readable_runtime_set(false)?;
            let mut resolved = Vec::with_capacity(objects.len());
            for (((tenant, _scoped), object), route) in scoped
                .into_iter()
                .zip(objects.iter())
                .zip(routes)
            {
                let route = route.ok_or_else(|| {
                    StoreError::NotFound(format!("tenant={tenant} key={}", object.key))
                })?;
                if route.state != RouteState::Active {
                    return Err(StoreError::NotFound(format!(
                        "tenant={tenant} key={} is not readable",
                        object.key
                    )));
                }
                let (replica, readable_for_route) = match Self::select_readable_replica(
                    &route,
                    &self.lease.runtime,
                    &local_segments,
                    &readable_runtimes,
                ) {
                    Some(replica) => (replica, readable_runtimes.clone()),
                    None => {
                        let refreshed_readable = self.readable_runtime_set(true)?;
                        let replica = Self::select_readable_replica(
                            &route,
                            &self.lease.runtime,
                            &local_segments,
                            &refreshed_readable,
                        )
                        .ok_or_else(|| {
                            StoreError::NotFound(format!(
                                "tenant={tenant} key={} has no readable replica owner",
                                object.key
                            ))
                        })?;
                        (replica, refreshed_readable)
                    }
                };
                if route
                    .replicas
                    .iter()
                    .min_by_key(|candidate| candidate.priority)
                    .is_some_and(|primary| primary.owner != replica.owner)
                {
                    self.prune_unreadable_replicas_best_effort(&route, &readable_for_route);
                }
                let fallback_replicas = Self::fallback_replicas_for_route(
                    &route,
                    &replica,
                    &self.lease.runtime,
                    &local_segments,
                    &readable_for_route,
                );
                resolved.push(ResolvedObject {
                    tenant: tenant.to_string(),
                    key: object.key.to_string(),
                    route,
                    replica,
                    fallback_replicas,
                });
            }
            Ok(resolved)
        })();
        if let Ok(resolved) = &result {
            debug!(
                runtime = %self.lease.runtime,
                items = resolved.len(),
                "resolved object routes"
            );
        }
        tracker.finish_with_result(
            match &result {
                Ok(_) => "ok",
                Err(StoreError::NotFound(_)) => "miss",
                Err(StoreError::Conflict(_)) => "conflict",
                Err(_) => "error",
            },
            0,
        );
        result
    }

    fn shaping_max_remote_batch_burst_items(&self) -> Option<usize> {
        self.bandwidth_shaping
            .as_ref()
            .and_then(|shaping| shaping.max_remote_batch_burst_items)
    }

    fn shaping_max_remote_batch_bytes(&self) -> Option<usize> {
        self.bandwidth_shaping
            .as_ref()
            .and_then(|shaping| shaping.max_remote_batch_bytes)
    }

    fn fairness_max_remote_batch_items_per_tenant(&self) -> Option<usize> {
        self.execution_fairness
            .as_ref()
            .and_then(|fairness| fairness.max_remote_batch_items_per_tenant)
    }

    fn shaping_max_inflight_bytes_per_batch(&self) -> Option<u64> {
        self.bandwidth_shaping
            .as_ref()
            .and_then(|shaping| shaping.max_inflight_bytes_per_batch)
    }

    fn remote_batch_chunk_limit(&self, bytes_per_item: usize, fallback_items: usize) -> usize {
        let burst_items = self
            .shaping_max_remote_batch_burst_items()
            .unwrap_or(fallback_items)
            .max(1);
        let byte_items = self
            .shaping_max_remote_batch_bytes()
            .map(|max_bytes| (max_bytes / bytes_per_item.max(1)).max(1))
            .unwrap_or(fallback_items)
            .max(1);
        burst_items.min(byte_items).min(fallback_items).max(1)
    }

    fn remote_write_chunk_limit(&self, bytes_per_item: usize, fallback_items: usize) -> usize {
        self.fairness_max_remote_batch_items_per_tenant()
            .unwrap_or(fallback_items)
            .max(1)
            .min(self.remote_batch_chunk_limit(bytes_per_item, fallback_items))
    }

    fn remote_batch_hints(
        &self,
        tenant: &str,
        _bytes: u64,
        mode: TransferPacingMode,
    ) -> TransferBatchHints {
        TransferBatchHints {
            pacing_group: Some(format!("tenant:{tenant}")),
            mode,
            max_inflight_bytes: self.shaping_max_inflight_bytes_per_batch(),
        }
    }

    fn fairness_slice_by_tenant<T, F>(&self, items: &[T], tenant_of: F) -> Vec<Vec<usize>>
    where
        F: Fn(&T) -> &str,
    {
        let Some(limit) = self.fairness_max_remote_batch_items_per_tenant() else {
            return vec![(0..items.len()).collect()];
        };
        let mut per_tenant = BTreeMap::<String, VecDeque<usize>>::new();
        for (index, item) in items.iter().enumerate() {
            per_tenant
                .entry(tenant_of(item).to_string())
                .or_default()
                .push_back(index);
        }
        let mut slices = Vec::new();
        while per_tenant.values().any(|queue| !queue.is_empty()) {
            let mut chunk = Vec::new();
            for queue in per_tenant.values_mut() {
                for _ in 0..limit {
                    let Some(index) = queue.pop_front() else {
                        break;
                    };
                    chunk.push(index);
                }
            }
            if chunk.is_empty() {
                break;
            }
            slices.push(chunk);
        }
        slices
    }

    fn fairness_slice_remote_indices(
        &self,
        resolved: &[ResolvedObject],
        remote_indices: &[usize],
    ) -> Vec<Vec<usize>> {
        let indexed = remote_indices
            .iter()
            .map(|index| resolved[*index].clone())
            .collect::<Vec<_>>();
        self.fairness_slice_by_tenant(&indexed, |entry| entry.tenant.as_str())
            .into_iter()
            .map(|slice| {
                slice
                    .into_iter()
                    .map(|position| remote_indices[position])
                    .collect()
            })
            .collect()
    }

    fn execute_batch_get_into(
        &self,
        resolved: &mut [ResolvedObject],
        buffers: &mut [&mut [u8]],
    ) -> Result<Vec<usize>> {
        self.ensure_local_memory()?;
        let transport = self.transport()?;
        if resolved.len() != buffers.len() {
            return Err(StoreError::InvalidState(
                "resolved routes and output buffers length mismatch".to_string(),
            ));
        }

        let lengths = resolved
            .iter()
            .map(|entry| entry.replica.length as usize)
            .collect::<Vec<_>>();
        for (index, buffer) in buffers.iter().enumerate() {
            let needed = lengths[index];
            if buffer.len() < needed {
                return Err(StoreError::Allocator(format!(
                    "buffer too small for tenant={} key={}: need {needed}, have {}",
                    resolved[index].tenant,
                    resolved[index].key,
                    buffer.len()
                )));
            }
        }
        let buffer_ptrs = buffers
            .iter_mut()
            .map(|buffer| buffer.as_mut_ptr().cast::<c_void>())
            .collect::<Vec<_>>();
        let local_open_segment_names = resolved
            .iter()
            .map(|entry| self.local_transport_open_segment_name(&entry.replica.segment_name))
            .collect::<Vec<_>>();

        // Phase 1: single lock — classify local/remote, gather cached metadata,
        // and pre-compute copy instructions for cache-hit segments.
        let (local_paths, mut copy_instructions, cache_miss_segments) = {
            let state = self.state.lock();
            let memory = state.memory_ref()?;

            let local_paths: Vec<bool> = resolved
                .iter()
                .map(|entry| {
                    entry.replica.owner == self.lease.runtime
                        && memory.has_storage_segment(&entry.replica.segment_name)
                })
                .collect();

            let mut segment_metadata: BTreeMap<
                SegmentName,
                (SegmentInfo, Vec<SegmentTargetChunk>),
            > = BTreeMap::new();
            let mut cache_miss_segments: Vec<SegmentName> = Vec::new();

            for ((entry, local), open_segment_name) in resolved
                .iter()
                .zip(local_paths.iter())
                .zip(local_open_segment_names.iter())
            {
                if !*local
                    || segment_metadata.contains_key(&entry.replica.segment_name)
                    || cache_miss_segments.contains(&entry.replica.segment_name)
                {
                    continue;
                }
                let info = state.cached_segment_info(open_segment_name);
                let chunks = state.cached_segment_target_chunks(
                    &entry.replica.owner,
                    &entry.replica.segment_name,
                );
                match (info, chunks) {
                    (Some(info), Some(chunks)) => {
                        segment_metadata
                            .insert(entry.replica.segment_name.clone(), (info, chunks));
                    }
                    _ => {
                        cache_miss_segments.push(entry.replica.segment_name.clone());
                    }
                }
            }

            // Compute copy instructions for all cache-hit local items.
            let max_reg = transport.max_registration_bytes();
            let mut copy_instructions: Vec<(PreparedCopy, *mut u8)> = Vec::new();
            for ((entry, buffer), local) in resolved
                .iter()
                .zip(buffers.iter_mut())
                .zip(local_paths.iter())
            {
                if !*local {
                    continue;
                }
                if let Some((info, chunks)) = segment_metadata.get(&entry.replica.segment_name) {
                    let target_offset =
                        Self::replica_storage_target_offset(info, chunks, &entry.replica)?;
                    let instructions = memory.prepare_storage_copy_instructions(
                        &entry.replica.segment_name,
                        info,
                        target_offset,
                        entry.replica.length as usize,
                        max_reg,
                    )?;
                    copy_instructions.push((instructions, buffer.as_mut_ptr()));
                }
            }

            (local_paths, copy_instructions, cache_miss_segments)
        };

        let local_items = local_paths.iter().filter(|local| **local).count();
        let local_bytes = local_paths
            .iter()
            .zip(lengths.iter())
            .filter_map(|(local, length)| (*local).then_some(*length as u64))
            .sum::<u64>();

        // Phase 2: handle cache misses — fall back to multi-lock path (rare in steady state).
        if !cache_miss_segments.is_empty() {
            let mut miss_segment_infos: BTreeMap<SegmentName, SegmentInfo> = BTreeMap::new();
            let mut miss_target_chunks: BTreeMap<SegmentName, Vec<SegmentTargetChunk>> =
                BTreeMap::new();

            for (entry, local) in resolved.iter().zip(local_paths.iter()) {
                if !*local || !cache_miss_segments.contains(&entry.replica.segment_name) {
                    continue;
                }
                if miss_segment_infos.contains_key(&entry.replica.segment_name) {
                    continue;
                }
                let open_segment_name =
                    self.local_transport_open_segment_name(&entry.replica.segment_name);
                let (_, info) = {
                    let mut state = self.state.lock();
                    state.open_segment_with_info(transport, &open_segment_name)?
                };
                miss_segment_infos.insert(entry.replica.segment_name.clone(), info);
                miss_target_chunks.insert(
                    entry.replica.segment_name.clone(),
                    self.replica_target_chunks(&entry.replica)?,
                );
            }

            // Compute copy instructions for miss items under one additional lock.
            let max_reg = transport.max_registration_bytes();
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            for ((entry, buffer), local) in resolved
                .iter()
                .zip(buffers.iter_mut())
                .zip(local_paths.iter())
            {
                if !*local || !cache_miss_segments.contains(&entry.replica.segment_name) {
                    continue;
                }
                let info = miss_segment_infos.get(&entry.replica.segment_name).ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "local segment info for {} is missing",
                        entry.replica.segment_name.0
                    ))
                })?;
                let chunks =
                    miss_target_chunks.get(&entry.replica.segment_name).ok_or_else(|| {
                        StoreError::InvalidState(format!(
                            "local segment target chunks for {} are missing",
                            entry.replica.segment_name.0
                        ))
                    })?;
                let target_offset =
                    Self::replica_storage_target_offset(info, chunks, &entry.replica)?;
                let instructions = memory.prepare_storage_copy_instructions(
                    &entry.replica.segment_name,
                    info,
                    target_offset,
                    entry.replica.length as usize,
                    max_reg,
                )?;
                copy_instructions.push((instructions, buffer.as_mut_ptr()));
            }
        }

        // Phase 3: execute all local copies without holding any lock.
        for (instructions, destination) in &copy_instructions {
            execute_copy_instructions(instructions, *destination);
        }
        if local_bytes != 0 {
            record_success_metric("get_local_copy", 0, local_bytes);
        }

        let remote_indices = local_paths
            .iter()
            .enumerate()
            .filter_map(|(index, local)| (!*local).then_some(index))
            .collect::<Vec<_>>();
        let mut remote_registered_buffers = vec![false; resolved.len()];
        {
            let state = self.state.lock();
            for index in &remote_indices {
                remote_registered_buffers[*index] =
                    state.buffer_is_registered(buffer_ptrs[*index], lengths[*index]);
            }
        }
        let remote_bytes = remote_indices
            .iter()
            .map(|index| lengths[*index] as u64)
            .sum::<u64>();
        let remote_attempts = remote_indices
            .iter()
            .map(|index| 1 + resolved[*index].fallback_replicas.len())
            .max()
            .unwrap_or(1);
        let request_deadline = self.request_deadline_for_transfer(remote_bytes, remote_attempts);
        if remote_indices.is_empty() {
            self.report_get_hits_best_effort(resolved);
            debug!(
                runtime = %self.lease.runtime,
                total_items = resolved.len(),
                local_items,
                local_bytes,
                "batch get completed via local copies only"
            );
            return Ok(lengths);
        }

        let mut remote_batch_chunks = 0usize;
        let mut remote_registered_batch_chunks = 0usize;
        let mut remote_direct_fallbacks = 0usize;
        let mut checked_remote_runtimes = BTreeSet::new();
        let fallback_local_segments = self.local_storage_segments();
        let fairness_slices = self.fairness_slice_remote_indices(resolved, &remote_indices);
        let fairness_rounds = fairness_slices.len();
        let shaping_chunk_limit = self.remote_batch_chunk_limit(
            remote_indices
                .iter()
                .map(|index| lengths[*index])
                .max()
                .unwrap_or(1),
            remote_indices.len(),
        );
        for fairness_slice in fairness_slices {
            let mut cursor = 0usize;
            while cursor < fairness_slice.len() {
                let capped_end = fairness_slice.len().min(cursor + shaping_chunk_limit);
                let mut direct_end = cursor;
                while direct_end < capped_end
                    && remote_registered_buffers[fairness_slice[direct_end]]
                {
                    direct_end += 1;
                }
                if direct_end > cursor {
                    let direct_chunk = &fairness_slice[cursor..direct_end];
                    match self.execute_remote_batch_get_direct_chunk(
                        transport,
                        resolved,
                        &buffer_ptrs,
                        direct_chunk,
                        request_deadline,
                    ) {
                        Ok(()) => {
                            remote_registered_batch_chunks += 1;
                        }
                        Err(_) => {
                            for index in direct_chunk {
                                let buffer = &mut *buffers[*index];
                                self.read_single_object_with_failover(
                                    transport,
                                    &mut resolved[*index],
                                    buffer,
                                    &mut checked_remote_runtimes,
                                    &fallback_local_segments,
                                    request_deadline,
                                )?;
                                remote_direct_fallbacks += 1;
                            }
                        }
                    }
                    cursor = direct_end;
                    continue;
                }
                let planning_window = &fairness_slice[cursor..capped_end];
                match self.plan_remote_get_chunk(planning_window, &lengths, 0)? {
                    Some((next, scratch)) => {
                        match self.execute_remote_batch_get_chunk(
                            transport,
                            resolved,
                            buffers,
                            &planning_window[..next],
                            &scratch,
                            request_deadline,
                        ) {
                            Ok(()) => {
                                remote_batch_chunks += 1;
                            }
                            Err(_) => {
                                for index in &planning_window[..next] {
                                    let buffer = &mut *buffers[*index];
                                    self.read_single_object_with_failover(
                                        transport,
                                        &mut resolved[*index],
                                        buffer,
                                        &mut checked_remote_runtimes,
                                        &fallback_local_segments,
                                        request_deadline,
                                    )?;
                                    remote_direct_fallbacks += 1;
                                }
                            }
                        }
                        cursor += next;
                    }
                    None => {
                        let index = fairness_slice[cursor];
                        let buffer = &mut *buffers[index];
                        self.read_single_object_with_failover(
                            transport,
                            &mut resolved[index],
                            buffer,
                            &mut checked_remote_runtimes,
                            &fallback_local_segments,
                            request_deadline,
                        )?;
                        remote_direct_fallbacks += 1;
                        cursor += 1;
                    }
                }
            }
        }

        debug!(
            runtime = %self.lease.runtime,
            total_items = resolved.len(),
            local_items,
            local_bytes,
            remote_items = remote_indices.len(),
            remote_bytes,
            fairness_rounds,
            remote_batch_chunks,
            remote_registered_batch_chunks,
            remote_direct_fallbacks,
            "batch get completed across local and remote paths"
        );
        let payloads = buffers
            .iter()
            .zip(lengths.iter())
            .map(|(buffer, length)| &buffer[..*length])
            .collect::<Vec<_>>();
        Self::validate_batch_replica_checksums(resolved, &payloads)?;
        self.report_get_hits_best_effort(resolved);
        Ok(lengths)
    }

    fn validate_batch_replica_checksums(
        resolved: &[ResolvedObject],
        payloads: &[&[u8]],
    ) -> Result<()> {
        if resolved.len() != payloads.len() {
            return Err(StoreError::InvalidState(
                "resolved routes and checksum payloads length mismatch".to_string(),
            ));
        }
        if resolved.len() <= 1 {
            for (entry, payload) in resolved.iter().zip(payloads.iter()) {
                validate_replica_checksum(&entry.replica, payload)?;
            }
            return Ok(());
        }

        let total_bytes = payloads.iter().map(|payload| payload.len()).sum::<usize>();
        if total_bytes < PARALLEL_CHECKSUM_MIN_BYTES {
            for (entry, payload) in resolved.iter().zip(payloads.iter()) {
                validate_replica_checksum(&entry.replica, payload)?;
            }
            return Ok(());
        }

        let workers = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1)
            .min(resolved.len())
            .min(MAX_PARALLEL_CHECKSUM_WORKERS);
        if workers <= 1 {
            for (entry, payload) in resolved.iter().zip(payloads.iter()) {
                validate_replica_checksum(&entry.replica, payload)?;
            }
            return Ok(());
        }

        let chunk_size = resolved.len().div_ceil(workers);
        let error = std::sync::Mutex::new(None);
        std::thread::scope(|scope| {
            for (entries, payloads) in resolved
                .chunks(chunk_size)
                .zip(payloads.chunks(chunk_size))
            {
                let error = &error;
                scope.spawn(move || {
                    for (entry, payload) in entries.iter().zip(payloads.iter()) {
                        if let Err(checksum_error) =
                            validate_replica_checksum(&entry.replica, payload)
                        {
                            let mut guard = error
                                .lock()
                                .expect("checksum validation error lock should succeed");
                            if guard.is_none() {
                                *guard = Some(checksum_error);
                            }
                            return;
                        }
                    }
                });
            }
        });
        if let Some(error) = error
            .into_inner()
            .expect("checksum validation error lock should succeed")
        {
            return Err(error);
        }
        Ok(())
    }

    fn report_get_hits_best_effort(&self, resolved: &[ResolvedObject]) {
        if resolved.is_empty() {
            return;
        }
        let mut local_hits = BTreeSet::new();
        let mut remote_hits = BTreeMap::<ClientRuntimeId, BTreeSet<ObjectKey>>::new();
        for entry in resolved {
            let key = self.scoped_key(&entry.tenant, &entry.key);
            if entry.replica.owner == self.lease.runtime {
                local_hits.insert(key);
            } else {
                remote_hits
                    .entry(entry.replica.owner.clone())
                    .or_default()
                    .insert(key);
            }
        }
        self.report_grouped_route_hits_best_effort(local_hits, remote_hits, true);
    }

    fn report_route_hits_best_effort<'a>(
        &self,
        routes: impl IntoIterator<Item = &'a ObjectRoute>,
    ) {
        let Ok(readable_runtimes) = self.readable_runtime_set(false) else {
            return;
        };
        let local_segments = self.local_storage_segments();
        self.report_readable_route_hits_best_effort(routes, &local_segments, &readable_runtimes);
    }

    fn report_readable_route_hits_best_effort<'a>(
        &self,
        routes: impl IntoIterator<Item = &'a ObjectRoute>,
        local_segments: &BTreeSet<SegmentName>,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) {
        let mut local_hits = BTreeSet::new();
        let mut remote_hits = BTreeMap::<ClientRuntimeId, BTreeSet<ObjectKey>>::new();
        for route in routes {
            if route.state != RouteState::Active {
                continue;
            }
            for replica in &route.replicas {
                if !Self::replica_is_readable(
                    replica,
                    &self.lease.runtime,
                    local_segments,
                    readable_runtimes,
                ) {
                    continue;
                }
                if replica.owner == self.lease.runtime {
                    local_hits.insert(route.key.clone());
                } else {
                    remote_hits
                        .entry(replica.owner.clone())
                        .or_default()
                        .insert(route.key.clone());
                }
            }
        }
        self.report_grouped_route_hits_best_effort(local_hits, remote_hits, false);
    }

    fn report_grouped_route_hits_best_effort(
        &self,
        local_hits: BTreeSet<ObjectKey>,
        remote_hits: BTreeMap<ClientRuntimeId, BTreeSet<ObjectKey>>,
        force_membership_refresh_on_miss: bool,
    ) {
        if local_hits.is_empty() && remote_hits.is_empty() {
            return;
        }
        if !local_hits.is_empty() {
            let local_hits = local_hits.into_iter().collect::<Vec<_>>();
            let _ = self.storage_owner.report_route_hits(&local_hits);
        }
        if remote_hits.is_empty() {
            return;
        }
        let leases = if force_membership_refresh_on_miss {
            self.lookup_runtime_leases(remote_hits.keys().cloned())
        } else {
            self.lookup_cached_runtime_leases_best_effort(remote_hits.keys().cloned())
        };
        let leases = match leases {
            Ok(leases) => leases,
            Err(error) => {
                debug!(
                    runtime = %self.lease.runtime,
                    error = %error,
                    targets = remote_hits.len(),
                    "failed to resolve storage-owner leases for hit reporting"
                );
                return;
            }
        };
        for (owner, keys) in remote_hits {
            let Some(lease) = leases.get(&owner) else {
                continue;
            };
            let keys = keys.into_iter().collect::<Vec<_>>();
            if let Err(error) = self.control_client.batch_report_route_hits(lease, &keys) {
                debug!(
                    runtime = %self.lease.runtime,
                    storage_owner = %owner,
                    error = %error,
                    items = keys.len(),
                    "storage-owner hit report failed"
                );
            }
        }
    }

    fn track_remote_storage_owners_best_effort(&self, routes: &[ObjectRoute]) {
        if routes.is_empty() {
            return;
        }
        let mut grouped = BTreeMap::<ClientRuntimeId, BTreeMap<String, ObjectRoute>>::new();
        for route in routes {
            let mut owners = BTreeSet::new();
            for replica in route
                .replicas
                .iter()
                .filter(|replica| replica.owner != self.lease.runtime)
            {
                if owners.insert(replica.owner.clone()) {
                    grouped
                        .entry(replica.owner.clone())
                        .or_default()
                        .insert(route.key.0.clone(), route.clone());
                }
            }
        }
        if grouped.is_empty() {
            return;
        }
        let leases = match self.lookup_runtime_leases(grouped.keys().cloned()) {
            Ok(leases) => leases,
            Err(error) => {
                debug!(
                    runtime = %self.lease.runtime,
                    error = %error,
                    targets = grouped.len(),
                    "failed to resolve storage-owner leases for route tracking"
                );
                return;
            }
        };
        for (owner, routes) in grouped {
            let Some(lease) = leases.get(&owner) else {
                continue;
            };
            let routes = routes.into_values().collect::<Vec<_>>();
            if let Err(error) = self
                .control_client
                .batch_track_replica_routes(lease, &routes)
            {
                debug!(
                    runtime = %self.lease.runtime,
                    storage_owner = %owner,
                    error = %error,
                    items = routes.len(),
                    "storage-owner route tracking failed"
                );
            }
        }
    }

    fn note_remote_read_failure(
        &self,
        entries: &[&ResolvedObject],
        error: &StoreError,
        context: &'static str,
        mark_runtime_suspect: bool,
    ) {
        if !matches!(
            error,
            StoreError::Transport(_) | StoreError::NotFound(_) | StoreError::InvalidState(_)
        ) {
            return;
        }
        let mut failed_segments_by_runtime = BTreeMap::<ClientRuntimeId, BTreeSet<String>>::new();
        {
            let mut state = self.state.lock();
            for entry in entries {
                state.invalidate_remote_segment(&entry.replica.segment_name.0);
                if entry.replica.owner != self.lease.runtime {
                    failed_segments_by_runtime
                        .entry(entry.replica.owner.clone())
                        .or_default()
                        .insert(entry.replica.segment_name.0.clone());
                }
            }
        }
        if mark_runtime_suspect {
            let transport = self.transport().ok();
            for (runtime, segments) in failed_segments_by_runtime {
                let should_mark = transport.as_ref().is_none_or(|transport| {
                    segments.iter().all(|segment_name| {
                        let open_segment_name = self
                            .remote_transport_open_segment_name(
                                &runtime,
                                &SegmentName::new(segment_name.clone()),
                            )
                            .unwrap_or_else(|_| segment_name.clone());
                        let mut state = self.state.lock();
                        state
                            .open_segment_with_info(*transport, &open_segment_name)
                            .is_err()
                    })
                });
                if should_mark {
                    self.mark_runtime_suspect(&runtime, context);
                }
            }
        }
        let _ = refresh_live_client_cache(
            self.metadata.as_ref(),
            &self.live_client_cache,
            "live_client_snapshot_remote_read_failure",
        );
    }

    fn remote_read_failure_marks_runtime_suspect(error: &StoreError) -> bool {
        matches!(
            error,
            StoreError::Transport(_) | StoreError::NotFound(_) | StoreError::InvalidState(_)
        )
    }

    fn plan_remote_get_chunk(
        &self,
        remote_indices: &[usize],
        lengths: &[usize],
        start: usize,
    ) -> Result<Option<(usize, ScratchReservation)>> {
        let mut chunk_lengths = Vec::new();
        let mut best = None;
        for end in start..remote_indices.len() {
            chunk_lengths.push(lengths[remote_indices[end]]);
            let planned = {
                let state = self.state.lock();
                state.memory_ref()?.plan_scratch(&chunk_lengths)
            };
            match planned {
                Ok(scratch) => best = Some((end + 1, scratch)),
                Err(StoreError::Allocator(_)) if best.is_some() => break,
                Err(StoreError::Allocator(_)) => {
                    debug!(
                        runtime = %self.lease.runtime,
                        remote_index = remote_indices[start],
                        requested_bytes = lengths[remote_indices[start]],
                        "remote get request exceeds scratch window; falling back to direct path"
                    );
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
        }
        if let Some((end, _)) = best.as_ref() {
            debug!(
                runtime = %self.lease.runtime,
                start,
                end = *end,
                items = end.saturating_sub(start),
                "planned remote batch get scratch window"
            );
        }
        Ok(best)
    }

    fn segment_info_covers_target(info: &SegmentInfo, target_offset: u64, length: u64) -> bool {
        let mut current = target_offset;
        let mut remaining = length;
        let mut buffers = info.buffers.iter().collect::<Vec<_>>();
        buffers.sort_by_key(|buffer| buffer.base);
        while remaining != 0 {
            let Some(buffer) = buffers.iter().find(|buffer| {
                let Some(buffer_end) = buffer.base.checked_add(buffer.length) else {
                    return false;
                };
                current >= buffer.base && current < buffer_end
            }) else {
                return false;
            };
            let Some(buffer_end) = buffer.base.checked_add(buffer.length) else {
                return false;
            };
            let available = buffer_end - current;
            if available >= remaining {
                return true;
            }
            remaining -= available;
            current = buffer_end;
        }
        true
    }

    fn segment_relative_target_offset(
        info: &SegmentInfo,
        segment_name: &SegmentName,
        segment_offset: u64,
        length: u64,
    ) -> Result<u64> {
        if info.buffers.is_empty() {
            return Err(StoreError::Transport(format!(
                "segment {} exposes no buffers",
                segment_name.0
            )));
        }
        let mut buffers = info.buffers.iter().collect::<Vec<_>>();
        buffers.sort_by_key(|buffer| buffer.base);
        let total_capacity: u64 = buffers
            .iter()
            .fold(0u64, |acc, buffer| acc.saturating_add(buffer.length));
        let mut remaining_offset = segment_offset;
        for buffer in &buffers {
            if remaining_offset >= buffer.length {
                remaining_offset -= buffer.length;
                continue;
            }
            let Some(target_offset) = buffer.base.checked_add(remaining_offset) else {
                break;
            };
            if Self::segment_info_covers_target(info, target_offset, length) {
                return Ok(target_offset);
            }
            break;
        }
        warn!(
            segment = %segment_name.0,
            segment_offset,
            length,
            total_capacity,
            num_buffers = buffers.len(),
            buffers = ?buffers.iter().map(|b| (b.base, b.length)).collect::<Vec<_>>(),
            "segment offset is outside segment — possible allocator/registration size mismatch \
             or stale segment info cache after segment extension"
        );
        Err(StoreError::Transport(format!(
            "segment offset {} length {} is outside segment {} \
             (total_capacity={}, num_buffers={})",
            segment_offset, length, segment_name.0, total_capacity, buffers.len()
        )))
    }

    fn storage_target_offset(
        target_chunks: &[SegmentTargetChunk],
        segment_name: &SegmentName,
        segment_offset: u64,
        length: u64,
    ) -> Result<u64> {
        let end = segment_offset
            .checked_add(length)
            .ok_or_else(|| StoreError::Transport("storage offset range overflow".to_string()))?;
        if target_chunks.is_empty() {
            return Err(StoreError::Transport(format!(
                "segment {} has no published storage target chunks",
                segment_name.0
            )));
        }

        let mut chunks = target_chunks.to_vec();
        chunks.sort_by_key(|chunk| chunk.logical_offset);
        let first = chunks
            .iter()
            .find(|chunk| {
                let chunk_end = chunk.logical_offset.saturating_add(chunk.length_bytes);
                segment_offset >= chunk.logical_offset && segment_offset < chunk_end
            })
            .ok_or_else(|| {
                StoreError::Transport(format!(
                    "storage offset {} length {} is outside segment {} target chunks {:?}",
                    segment_offset, length, segment_name.0, target_chunks
                ))
            })?;
        let first_delta = segment_offset
            .checked_sub(first.logical_offset)
            .ok_or_else(|| StoreError::Transport("storage target chunk underflow".to_string()))?;
        let target_offset = first.target_offset.checked_add(first_delta).ok_or_else(|| {
            StoreError::Transport("storage target offset overflow".to_string())
        })?;
        let mut cursor = segment_offset;
        while cursor < end {
            let chunk = chunks
                .iter()
                .find(|chunk| {
                    let chunk_end = chunk.logical_offset.saturating_add(chunk.length_bytes);
                    cursor >= chunk.logical_offset && cursor < chunk_end
                })
                .ok_or_else(|| {
                    StoreError::Transport(format!(
                        "storage range [{segment_offset}, {end}) crosses an unpublished target chunk for segment {}",
                        segment_name.0
                    ))
                })?;
            let chunk_delta = cursor.checked_sub(chunk.logical_offset).ok_or_else(|| {
                StoreError::Transport("storage target chunk underflow".to_string())
            })?;
            let expected = target_offset
                .checked_add(cursor.checked_sub(segment_offset).ok_or_else(|| {
                    StoreError::Transport("storage offset cursor underflow".to_string())
                })?)
                .ok_or_else(|| StoreError::Transport("storage target offset overflow".to_string()))?;
            let actual = chunk.target_offset.checked_add(chunk_delta).ok_or_else(|| {
                StoreError::Transport("storage target chunk offset overflow".to_string())
            })?;
            if actual != expected {
                return Err(StoreError::Transport(format!(
                    "segment {} target chunks are not contiguous at logical offset {}",
                    segment_name.0, cursor
                )));
            }
            let chunk_end = chunk
                .logical_offset
                .checked_add(chunk.length_bytes)
                .ok_or_else(|| StoreError::Transport("storage target chunk overflow".to_string()))?;
            cursor = chunk_end.min(end);
            if chunk.length_bytes == 0 {
                return Err(StoreError::Transport(format!(
                    "segment {} target chunk has zero length",
                    segment_name.0
                )));
            }
        }
        Ok(target_offset)
    }

    fn replica_storage_target_offset(
        info: &SegmentInfo,
        target_chunks: &[SegmentTargetChunk],
        replica: &ReplicaRoute,
    ) -> Result<u64> {
        if let Some(offset) = replica.offset {
            if Self::segment_info_covers_target(info, offset, replica.length) {
                return Ok(offset);
            }
        }
        if target_chunks.is_empty() {
            return Self::segment_relative_target_offset(
                info,
                &replica.segment_name,
                replica.segment_offset,
                replica.length,
            );
        }

        let resolved = Self::storage_target_offset(
            target_chunks,
            &replica.segment_name,
            replica.segment_offset,
            replica.length,
        );
        if let Ok(target_offset) = resolved {
            if Self::segment_info_covers_target(info, target_offset, replica.length) {
                return Ok(target_offset);
            }
            return Err(StoreError::Transport(format!(
                "segment offset {} length {} maps outside segment {} transport buffers (target_offset={} num_buffers={})",
                replica.segment_offset,
                replica.length,
                replica.segment_name.0,
                target_offset,
                info.buffers.len()
            )));
        }
        warn!(
            segment = %replica.segment_name.0,
            segment_offset = replica.segment_offset,
            length = replica.length,
            num_buffers = info.buffers.len(),
            buffers = ?info.buffers.iter().map(|buffer| (buffer.base, buffer.length)).collect::<Vec<_>>(),
            "replica segment offset is outside segment storage target map"
        );
        resolved
    }

    fn replica_target_chunks(&self, replica: &ReplicaRoute) -> Result<Vec<SegmentTargetChunk>> {
        {
            let state = self.state.lock();
            if let Some(target_chunks) =
                state.cached_segment_target_chunks(&replica.owner, &replica.segment_name)
            {
                return Ok(target_chunks);
            }
        }

        let segment = {
            let local = self.allocator.lock().announcement(&replica.segment_name);
            match local {
                Some(segment) if segment.owner == replica.owner => segment,
                _ => self
                    .metadata
                    .get_segment(&replica.owner, &replica.segment_name)?
                    .ok_or_else(|| {
                        StoreError::NotFound(format!(
                            "segment {} for runtime {} not found",
                            replica.segment_name.0, replica.owner
                        ))
                    })?,
            }
        };
        let target_chunks = segment.target_chunks.clone();
        self.state.lock().cache_segment_target_metadata(
            &replica.owner,
            &replica.segment_name,
            &target_chunks,
            segment.transport_endpoint.clone(),
            segment.transport_segment_descriptor.clone(),
        );
        Ok(target_chunks)
    }

    #[cfg(test)]
    fn remote_replica_target_offset(info: &SegmentInfo, replica: &ReplicaRoute) -> Result<u64> {
        if let Some(offset) = replica.offset {
            if Self::segment_info_covers_target(info, offset, replica.length) {
                return Ok(offset);
            }
        }
        Self::segment_relative_target_offset(
            info,
            &replica.segment_name,
            replica.segment_offset,
            replica.length,
        )
    }

    fn target_buffer_transfer_requests(
        opcode: Opcode,
        segment: u64,
        target_offset: u64,
        local_buffer: *mut c_void,
        length: u64,
        target_info: &SegmentInfo,
        max_registration_bytes: Option<usize>,
    ) -> Result<Vec<TransferRequest>> {
        let total_len = usize::try_from(length).map_err(|_| {
            StoreError::Transport(format!("transfer length {length} does not fit in usize"))
        })?;
        if total_len == 0 {
            return Ok(Vec::new());
        }

        let mut buffers = target_info.buffers.iter().collect::<Vec<_>>();
        buffers.sort_by_key(|buffer| buffer.base);
        let local_base = local_buffer as usize;
        let mut current_target = target_offset;
        let mut transferred = 0usize;
        let mut requests = Vec::new();

        while transferred < total_len {
            let Some(target_buffer) = buffers.iter().find(|buffer| {
                let Some(buffer_end) = buffer.base.checked_add(buffer.length) else {
                    return false;
                };
                current_target >= buffer.base && current_target < buffer_end
            }) else {
                return Err(StoreError::Transport(format!(
                    "target offset {} is outside remote segment buffers",
                    current_target
                )));
            };
            let buffer_end = target_buffer.base.checked_add(target_buffer.length).ok_or_else(|| {
                StoreError::Transport("target segment buffer end overflow".to_string())
            })?;
            let available = usize::try_from(buffer_end - current_target).map_err(|_| {
                StoreError::Transport("target segment buffer span does not fit in usize".to_string())
            })?;
            let target_chunk_len = available.min(total_len - transferred);
            let chunk_base = local_base.checked_add(transferred).ok_or_else(|| {
                StoreError::Transport("local transfer buffer pointer overflow".to_string())
            })? as *mut c_void;

            let mut chunk_transferred = 0u64;
            for (chunk_addr, chunk_len) in
                registration_chunks(chunk_base, target_chunk_len, max_registration_bytes)?
            {
                let chunk_len = u64::try_from(chunk_len).map_err(|_| {
                    StoreError::Transport("transfer chunk length does not fit in u64".to_string())
                })?;
                let chunk_target_offset =
                    current_target.checked_add(chunk_transferred).ok_or_else(|| {
                        StoreError::Transport("target transfer offset overflow".to_string())
                    })?;
                requests.push(TransferRequest {
                    opcode,
                    source: chunk_addr,
                    target_id: segment,
                    target_offset: chunk_target_offset,
                    length: chunk_len,
                });
                chunk_transferred = chunk_transferred.checked_add(chunk_len).ok_or_else(|| {
                    StoreError::Transport("transfer chunk accounting overflow".to_string())
                })?;
            }

            transferred = transferred.checked_add(target_chunk_len).ok_or_else(|| {
                StoreError::Transport("transfer accounting overflow".to_string())
            })?;
            current_target = current_target
                .checked_add(u64::try_from(target_chunk_len).map_err(|_| {
                    StoreError::Transport("target chunk length does not fit in u64".to_string())
                })?)
                .ok_or_else(|| StoreError::Transport("target offset overflow".to_string()))?;
        }

        Ok(requests)
    }

    fn ensure_remote_runtime_reachable(
        &self,
        runtime: &ClientRuntimeId,
        target: &str,
    ) -> Result<()> {
        if *runtime == self.lease.runtime {
            return Ok(());
        }

        let lease = self.lookup_runtime_lease(runtime)?;
        match self.control_client.probe_reachability(&lease) {
            crate::control_plane::ControlPlaneReachability::Reachable => Ok(()),
            crate::control_plane::ControlPlaneReachability::Unreachable => {
                self.mark_runtime_suspect(runtime, "remote_runtime_control_unreachable");
                Err(StoreError::Transport(format!(
                    "remote runtime {runtime} is unreachable before opening {target}"
                )))
            }
            crate::control_plane::ControlPlaneReachability::Unknown(error) => Err(error),
        }
    }

    fn ensure_remote_replica_reachable(&self, replica: &ReplicaRoute) -> Result<()> {
        self.ensure_remote_runtime_reachable(&replica.owner, &replica.segment_name.0)
    }

    fn ensure_remote_replica_reachable_once(
        &self,
        replica: &ReplicaRoute,
        checked_runtimes: &mut BTreeSet<ClientRuntimeId>,
    ) -> Result<()> {
        if replica.owner == self.lease.runtime {
            return Ok(());
        }
        if !checked_runtimes.insert(replica.owner.clone()) {
            return Ok(());
        }
        self.ensure_remote_replica_reachable(replica)
    }

    fn execute_remote_batch_get_chunk(
        &self,
        transport: &dyn StoreTransport,
        resolved: &[ResolvedObject],
        buffers: &mut [&mut [u8]],
        remote_indices: &[usize],
        scratch: &[RegionAllocation],
        request_deadline: RequestDeadline,
    ) -> Result<()> {
        let bytes_out = remote_indices
            .iter()
            .map(|index| resolved[*index].replica.length)
            .sum::<u64>();
        let tracker = OperationTracker::new("get_remote_batch_chunk")
            .attribute_u64("mooncake.item_count", remote_indices.len() as u64)
            .attribute_u64("mooncake.remote_target_count", remote_indices.len() as u64);
        let raw_result = (|| -> std::result::Result<(), (StoreError, bool)> {
            let target_chunks = remote_indices
                .iter()
                .map(|index| {
                    self.replica_target_chunks(&resolved[*index].replica)
                        .map_err(|error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let requests = {
                let mut batch = Vec::with_capacity(remote_indices.len());
                for (position, index) in remote_indices.iter().enumerate() {
                    let entry = &resolved[*index];
                    let open_segment_name = self
                        .replica_transport_open_segment_name(&entry.replica)
                        .map_err(|error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })?;
                    let (segment, info) = {
                        let mut state = self.state.lock();
                        state
                            .open_segment_with_info(transport, &open_segment_name)
                            .map_err(|error| {
                                (
                                    error.clone(),
                                    Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })?
                    };
                    let target_offset = Self::replica_storage_target_offset(
                        &info,
                        &target_chunks[position],
                        &entry.replica,
                    )
                    .map_err(|error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })?;
                    batch.extend(Self::target_buffer_transfer_requests(
                        Opcode::Read,
                        segment,
                        target_offset,
                        scratch[position].addr,
                        entry.replica.length,
                        &info,
                        transport.max_registration_bytes(),
                    )
                    .map_err(|error| {
                        (
                            error.clone(),
                            Self::remote_read_failure_marks_runtime_suspect(&error),
                        )
                    })?);
                }
                batch
            };
            let batch_id = transport.allocate_batch(requests.len()).map_err(|error| {
                (
                    error.clone(),
                    Self::remote_read_failure_marks_runtime_suspect(&error),
                )
            })?;
            let mode = if remote_indices.len() == 1 {
                TransferPacingMode::LatencySensitive
            } else {
                TransferPacingMode::ThroughputOptimized
            };
            let hints = self.remote_batch_hints(&resolved[remote_indices[0]].tenant, bytes_out, mode);
            let submit_result = transport.submit_with_hints(batch_id, &requests, &hints);
            if let Err(error) = submit_result {
                let _ = transport.free_batch(batch_id);
                return Err((
                    error.clone(),
                    Self::remote_read_failure_marks_runtime_suspect(&error),
                ));
            }
            let wait_result = wait_for_batch_completion_detailed(
                transport,
                batch_id,
                self.transfer_stall_timeout,
                request_deadline.instant(),
            )
            .map_err(|error| (StoreError::from(error.clone()), error.marks_runtime_suspect()));
            let free_result = transport.free_batch(batch_id);
            wait_result?;
            free_result.map_err(|error| {
                (
                    error.clone(),
                    Self::remote_read_failure_marks_runtime_suspect(&error),
                )
            })?;

            for (position, index) in remote_indices.iter().enumerate() {
                unsafe {
                    ptr::copy_nonoverlapping(
                        scratch[position].addr.cast::<u8>(),
                        buffers[*index].as_mut_ptr(),
                        resolved[*index].replica.length as usize,
                    );
                }
            }
            Ok(())
        })();
        let result = match raw_result {
            Ok(()) => Ok(()),
            Err((error, mark_runtime_suspect)) => {
                let failed = remote_indices
                    .iter()
                    .map(|index| &resolved[*index])
                    .collect::<Vec<_>>();
                self.note_remote_read_failure(
                    &failed,
                    &error,
                    "remote_batch_get_failed",
                    mark_runtime_suspect,
                );
                Err(error)
            }
        };
        debug!(
            runtime = %self.lease.runtime,
            items = remote_indices.len(),
            bytes_out,
            "executed remote batch get chunk"
        );
        tracker.finish(&result, bytes_out);
        registry::record_transport_operation(
            "read",
            "storage",
            if result.is_ok() { "ok" } else { "error" },
        );
        if result.is_ok() {
            registry::record_transport_bytes("read", "storage", bytes_out);
        }
        result
    }

    fn execute_remote_batch_get_direct_chunk(
        &self,
        transport: &dyn StoreTransport,
        resolved: &[ResolvedObject],
        buffer_ptrs: &[*mut c_void],
        remote_indices: &[usize],
        request_deadline: RequestDeadline,
    ) -> Result<()> {
        let bytes_out = remote_indices
            .iter()
            .map(|index| resolved[*index].replica.length)
            .sum::<u64>();
        let tracker = OperationTracker::new("get_remote_batch_direct")
            .attribute_u64("mooncake.item_count", remote_indices.len() as u64)
            .attribute_u64("mooncake.remote_target_count", remote_indices.len() as u64);
        let raw_result = (|| -> std::result::Result<(), (StoreError, bool)> {
            let target_chunks = remote_indices
                .iter()
                .map(|index| {
                    self.replica_target_chunks(&resolved[*index].replica)
                        .map_err(|error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let requests = {
                let mut batch = Vec::with_capacity(remote_indices.len());
                for (position, index) in remote_indices.iter().enumerate() {
                    let entry = &resolved[*index];
                    let open_segment_name = self
                        .replica_transport_open_segment_name(&entry.replica)
                        .map_err(|error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })?;
                    let (segment, info) = {
                        let mut state = self.state.lock();
                        state
                            .open_segment_with_info(transport, &open_segment_name)
                            .map_err(|error| {
                                (
                                    error.clone(),
                                    Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })?
                    };
                    let target_offset = Self::replica_storage_target_offset(
                        &info,
                        &target_chunks[position],
                        &entry.replica,
                    )
                    .map_err(|error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })?;
                    batch.extend(Self::target_buffer_transfer_requests(
                        Opcode::Read,
                        segment,
                        target_offset,
                        buffer_ptrs[*index],
                        entry.replica.length,
                        &info,
                        transport.max_registration_bytes(),
                    )
                    .map_err(|error| {
                        (
                            error.clone(),
                            Self::remote_read_failure_marks_runtime_suspect(&error),
                        )
                    })?);
                }
                batch
            };
            let batch_id = transport.allocate_batch(requests.len()).map_err(|error| {
                (
                    error.clone(),
                    Self::remote_read_failure_marks_runtime_suspect(&error),
                )
            })?;
            let mode = if remote_indices.len() == 1 {
                TransferPacingMode::LatencySensitive
            } else {
                TransferPacingMode::ThroughputOptimized
            };
            let hints = self.remote_batch_hints(&resolved[remote_indices[0]].tenant, bytes_out, mode);
            let submit_result = transport.submit_with_hints(batch_id, &requests, &hints);
            if let Err(error) = submit_result {
                let _ = transport.free_batch(batch_id);
                return Err((
                    error.clone(),
                    Self::remote_read_failure_marks_runtime_suspect(&error),
                ));
            }
            let wait_result = wait_for_batch_completion_detailed(
                transport,
                batch_id,
                self.transfer_stall_timeout,
                request_deadline.instant(),
            )
            .map_err(|error| (StoreError::from(error.clone()), error.marks_runtime_suspect()));
            let free_result = transport.free_batch(batch_id);
            wait_result?;
            free_result.map_err(|error| {
                (
                    error.clone(),
                    Self::remote_read_failure_marks_runtime_suspect(&error),
                )
            })
        })();
        let result = match raw_result {
            Ok(()) => Ok(()),
            Err((error, mark_runtime_suspect)) => {
                let failed = remote_indices
                    .iter()
                    .map(|index| &resolved[*index])
                    .collect::<Vec<_>>();
                self.note_remote_read_failure(
                    &failed,
                    &error,
                    "remote_batch_get_direct_failed",
                    mark_runtime_suspect,
                );
                Err(error)
            }
        };
        debug!(
            runtime = %self.lease.runtime,
            items = remote_indices.len(),
            bytes_out,
            "executed direct remote batch get chunk into registered buffers"
        );
        tracker.finish(&result, bytes_out);
        registry::record_transport_operation(
            "read",
            "storage",
            if result.is_ok() { "ok" } else { "error" },
        );
        if result.is_ok() {
            registry::record_transport_bytes("read", "storage", bytes_out);
        }
        result
    }

    fn execute_remote_get_direct(
        &self,
        transport: &dyn StoreTransport,
        resolved: &ResolvedObject,
        buffer: &mut [u8],
        length: usize,
        request_deadline: RequestDeadline,
    ) -> Result<()> {
        let tracker = OperationTracker::new("get_remote_direct")
            .attribute_str("mooncake.tenant", &resolved.tenant)
            .attribute_u64("mooncake.item_count", 1)
            .attribute_u64("mooncake.remote_target_count", 1);
        let raw_result = (|| -> std::result::Result<(), (StoreError, bool)> {
            let buffer_ptr = buffer.as_mut_ptr().cast::<c_void>();
            let buffer_len = buffer.len();
            let mut registered_here = false;
            {
                let mut state = self.state.lock();
                if !state.buffer_is_registered(buffer_ptr, length) {
                    state
                        .register_external_buffer(transport, buffer_ptr, buffer_len)
                        .map_err(|error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })?;
                    registered_here = true;
                }
            }
            let requests = {
                let target_chunks =
                    self.replica_target_chunks(&resolved.replica).map_err(|error| {
                        (
                            error.clone(),
                            Self::remote_read_failure_marks_runtime_suspect(&error),
                        )
                    })?;
                let open_segment_name = self
                    .replica_transport_open_segment_name(&resolved.replica)
                    .map_err(|error| {
                        (
                            error.clone(),
                            Self::remote_read_failure_marks_runtime_suspect(&error),
                        )
                    })?;
                let mut state = self.state.lock();
                let (segment, info) = state
                    .open_segment_with_info(transport, &open_segment_name)
                    .map_err(|error| {
                        (
                            error.clone(),
                            Self::remote_read_failure_marks_runtime_suspect(&error),
                        )
                    })?;
                let target_offset =
                    Self::replica_storage_target_offset(&info, &target_chunks, &resolved.replica)
                        .map_err(
                        |error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        },
                    )?;
                Self::target_buffer_transfer_requests(
                    Opcode::Read,
                    segment,
                    target_offset,
                    buffer_ptr,
                    resolved.replica.length,
                    &info,
                    transport.max_registration_bytes(),
                )
                .map_err(|error| {
                    (
                        error.clone(),
                        Self::remote_read_failure_marks_runtime_suspect(&error),
                    )
                })?
            };
            let batch_id = transport.allocate_batch(requests.len()).map_err(|error| {
                (
                    error.clone(),
                    Self::remote_read_failure_marks_runtime_suspect(&error),
                )
            })?;
            let hints = self.remote_batch_hints(
                &resolved.tenant,
                resolved.replica.length,
                TransferPacingMode::LatencySensitive,
            );
            let submit_result = transport.submit_with_hints(batch_id, &requests, &hints);
            if let Err(error) = submit_result {
                let _ = transport.free_batch(batch_id);
                if registered_here {
                    let _ = self
                        .state
                        .lock()
                        .unregister_external_buffer(transport, buffer_ptr, buffer_len);
                }
                return Err((
                    error.clone(),
                    Self::remote_read_failure_marks_runtime_suspect(&error),
                ));
            }
            let wait_result = wait_for_batch_completion_detailed(
                transport,
                batch_id,
                self.transfer_stall_timeout,
                request_deadline.instant(),
            )
            .map_err(|error| (StoreError::from(error.clone()), error.marks_runtime_suspect()));
            let free_result = transport.free_batch(batch_id);
            if registered_here {
                let _ = self
                    .state
                    .lock()
                    .unregister_external_buffer(transport, buffer_ptr, buffer_len);
            }
            wait_result?;
            free_result.map_err(|error| {
                (
                    error.clone(),
                    Self::remote_read_failure_marks_runtime_suspect(&error),
                )
            })
        })();
        let result = match raw_result {
            Ok(()) => Ok(()),
            Err((error, mark_runtime_suspect)) => {
                self.note_remote_read_failure(
                    &[resolved],
                    &error,
                    "remote_direct_get_failed",
                    mark_runtime_suspect,
                );
                Err(error)
            }
        };
        debug!(
            runtime = %self.lease.runtime,
            tenant = %resolved.tenant,
            key = %resolved.key,
            bytes_out = length,
            "executed remote direct get fallback"
        );
        tracker.finish(&result, length as u64);
        registry::record_transport_operation(
            "read",
            "storage",
            if result.is_ok() { "ok" } else { "error" },
        );
        if result.is_ok() {
            registry::record_transport_bytes("read", "storage", length as u64);
        }
        result
    }

    fn execute_selected_replica_direct(
        &self,
        transport: &dyn StoreTransport,
        resolved: &ResolvedObject,
        buffer: &mut [u8],
        checked_runtimes: &mut BTreeSet<ClientRuntimeId>,
        request_deadline: RequestDeadline,
    ) -> Result<()> {
        let length = resolved.replica.length as usize;
        if buffer.len() < length {
            return Err(StoreError::Allocator(format!(
                "buffer too small for tenant={} key={}: need {length}, have {}",
                resolved.tenant,
                resolved.key,
                buffer.len()
            )));
        }
        if !self.copy_local_replica_direct(transport, &resolved.replica, buffer, length)? {
            self.ensure_remote_replica_reachable_once(&resolved.replica, checked_runtimes)?;
            self.execute_remote_get_direct(transport, resolved, buffer, length, request_deadline)?;
        }
        validate_replica_checksum(&resolved.replica, &buffer[..length])
    }

    fn copy_local_replica_direct(
        &self,
        transport: &dyn StoreTransport,
        replica: &ReplicaRoute,
        buffer: &mut [u8],
        length: usize,
    ) -> Result<bool> {
        let local = {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            replica.owner == self.lease.runtime && memory.has_storage_segment(&replica.segment_name)
        };
        if !local {
            return Ok(false);
        }
        let open_segment_name = self.local_transport_open_segment_name(&replica.segment_name);
        let (_, target_info) = {
            let mut state = self.state.lock();
            state.open_segment_with_info(transport, &open_segment_name)?
        };
        let target_chunks = self.replica_target_chunks(replica)?;
        {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            memory.copy_storage_target_to(
                &replica.segment_name,
                &target_info,
                Self::replica_storage_target_offset(&target_info, &target_chunks, replica)?,
                buffer.as_mut_ptr(),
                length,
                transport.max_registration_bytes(),
            )?;
        };
        Ok(true)
    }

    fn execute_range_read(
        &self,
        resolved: &mut ResolvedObject,
        buffer: &mut [u8],
        src_offset: usize,
    ) -> Result<usize> {
        self.ensure_local_memory()?;
        let transport = self.transport()?;
        let size = buffer.len();

        if self.copy_local_replica_range(transport, &resolved.replica, buffer, src_offset, size)? {
            return Ok(size);
        }

        self.execute_remote_range_read(transport, resolved, buffer, src_offset, size)?;
        Ok(size)
    }

    fn copy_local_replica_range(
        &self,
        transport: &dyn StoreTransport,
        replica: &ReplicaRoute,
        buffer: &mut [u8],
        src_offset: usize,
        size: usize,
    ) -> Result<bool> {
        let local = {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            replica.owner == self.lease.runtime && memory.has_storage_segment(&replica.segment_name)
        };
        if !local {
            return Ok(false);
        }
        let open_segment_name = self.local_transport_open_segment_name(&replica.segment_name);
        let (_, target_info) = {
            let mut state = self.state.lock();
            state.open_segment_with_info(transport, &open_segment_name)?
        };
        let target_chunks = self.replica_target_chunks(replica)?;
        let base_target_offset =
            Self::replica_storage_target_offset(&target_info, &target_chunks, replica)?;
        let range_target_offset = base_target_offset
            .checked_add(src_offset as u64)
            .ok_or_else(|| {
                StoreError::Transport("range read target offset overflow".to_string())
            })?;
        {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            memory.copy_storage_target_to(
                &replica.segment_name,
                &target_info,
                range_target_offset,
                buffer.as_mut_ptr(),
                size,
                transport.max_registration_bytes(),
            )?;
        };
        Ok(true)
    }

    fn execute_remote_range_read(
        &self,
        transport: &dyn StoreTransport,
        resolved: &ResolvedObject,
        buffer: &mut [u8],
        src_offset: usize,
        size: usize,
    ) -> Result<()> {
        let buffer_ptr = buffer.as_mut_ptr().cast::<c_void>();
        let buffer_len = buffer.len();
        let mut registered_here = false;
        {
            let mut state = self.state.lock();
            if !state.buffer_is_registered(buffer_ptr, size) {
                state.register_external_buffer(transport, buffer_ptr, buffer_len)?;
                registered_here = true;
            }
        }
        let result = (|| -> Result<()> {
            let target_chunks = self.replica_target_chunks(&resolved.replica)?;
            let open_segment_name =
                self.replica_transport_open_segment_name(&resolved.replica)?;
            let (segment, info) = {
                let mut state = self.state.lock();
                state.open_segment_with_info(transport, &open_segment_name)?
            };
            let base_target_offset =
                Self::replica_storage_target_offset(&info, &target_chunks, &resolved.replica)?;
            let range_target_offset =
                base_target_offset.checked_add(src_offset as u64).ok_or_else(|| {
                    StoreError::Transport("range read remote target offset overflow".to_string())
                })?;
            let requests = Self::target_buffer_transfer_requests(
                Opcode::Read,
                segment,
                range_target_offset,
                buffer_ptr,
                size as u64,
                &info,
                transport.max_registration_bytes(),
            )?;
            let batch_id = transport.allocate_batch(requests.len())?;
            let hints = self.remote_batch_hints(
                &resolved.tenant,
                size as u64,
                TransferPacingMode::LatencySensitive,
            );
            if let Err(error) = transport.submit_with_hints(batch_id, &requests, &hints) {
                let _ = transport.free_batch(batch_id);
                return Err(error);
            }
            let request_deadline = self.request_deadline_for_transfer(size as u64, 1);
            let wait_result = wait_for_batch_completion_detailed(
                transport,
                batch_id,
                self.transfer_stall_timeout,
                request_deadline.instant(),
            )
            .map_err(StoreError::from);
            let _ = transport.free_batch(batch_id);
            wait_result
        })();
        if registered_here {
            let _ = self
                .state
                .lock()
                .unregister_external_buffer(transport, buffer_ptr, buffer_len);
        }
        result
    }

    fn read_single_object_with_failover(
        &self,
        transport: &dyn StoreTransport,
        resolved: &mut ResolvedObject,
        buffer: &mut [u8],
        checked_runtimes: &mut BTreeSet<ClientRuntimeId>,
        local_segments: &BTreeSet<SegmentName>,
        request_deadline: RequestDeadline,
    ) -> Result<()> {
        loop {
            match self.execute_selected_replica_direct(
                transport,
                resolved,
                buffer,
                checked_runtimes,
                request_deadline,
            ) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let failed_owner = resolved.replica.owner.clone();
                    let readable_runtimes = self.readable_runtime_set(true)?;
                    if !self.try_advance_resolved_replica(
                        resolved,
                        local_segments,
                        &readable_runtimes,
                    ) {
                        if Self::should_refresh_route_after_read_error(&error) {
                            if self.refresh_resolved_route_after_read_failure(resolved)? {
                                debug!(
                                    runtime = %self.lease.runtime,
                                    tenant = %resolved.tenant,
                                    key = %resolved.key,
                                    route_version = resolved.route.version.0,
                                    storage_owner = %resolved.replica.owner,
                                    segment = %resolved.replica.segment_name.0,
                                    "retrying get after route refresh"
                                );
                                continue;
                            }
                            if self.wait_for_route_refresh_retry(request_deadline) {
                                debug!(
                                    runtime = %self.lease.runtime,
                                    tenant = %resolved.tenant,
                                    key = %resolved.key,
                                    error = %error,
                                    "waiting for route refresh after transient read failure"
                                );
                                continue;
                            }
                        }
                        return Err(error);
                    }
                    self.mark_runtime_suspect(&failed_owner, "route_read_replica_failed");
                    let pruned_runtimes = self.readable_runtime_set(true)?;
                    self.maybe_prune_route_after_failover(resolved, &pruned_runtimes);
                    debug!(
                        runtime = %self.lease.runtime,
                        tenant = %resolved.tenant,
                        key = %resolved.key,
                        storage_owner = %resolved.replica.owner,
                        segment = %resolved.replica.segment_name.0,
                        "retrying get with fallback replica"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod runtime_io_tests {
    use super::*;
    use mooncake_transport::{SegmentBuffer, SegmentKind};

    fn memory_segment(buffers: &[(u64, u64)]) -> SegmentInfo {
        SegmentInfo {
            kind: SegmentKind::Memory,
            buffers: buffers
                .iter()
                .map(|(base, length)| SegmentBuffer {
                    base: *base,
                    length: *length,
                    location: "cpu:0".to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn segment_relative_target_offset_walks_chunked_buffers() {
        let info = memory_segment(&[(1000, 64), (1064, 64), (1128, 32)]);
        let segment = SegmentName::new("seg");

        assert_eq!(
            StoreClient::segment_relative_target_offset(&info, &segment, 80, 16)
                .expect("offset in second chunk should map"),
            1080
        );
        assert_eq!(
            StoreClient::segment_relative_target_offset(&info, &segment, 120, 24)
                .expect("contiguous chunks should cover the transfer"),
            1120
        );
    }

    #[test]
    fn segment_relative_target_offset_orders_chunked_buffers() {
        let info = memory_segment(&[(1064, 64), (1000, 64), (1128, 32)]);
        let segment = SegmentName::new("seg");

        assert_eq!(
            StoreClient::segment_relative_target_offset(&info, &segment, 80, 16)
                .expect("logical offset should use buffer address order"),
            1080
        );
    }

    #[test]
    fn segment_relative_target_offset_orders_tail_crossing_buffers() {
        let info = memory_segment(&[(1128, 64), (1000, 64), (1064, 64)]);
        let segment = SegmentName::new("seg");

        assert_eq!(
            StoreClient::segment_relative_target_offset(&info, &segment, 60, 8)
                .expect("logical range should cross into the next ordered buffer"),
            1060
        );
    }

    #[test]
    fn storage_target_offset_uses_explicit_chunks_when_scratch_matches_storage() {
        let segment = SegmentName::new("seg-storage-window");
        let chunks = vec![SegmentTargetChunk {
            logical_offset: 0,
            target_offset: 2000,
            length_bytes: 1024,
        }];

        let result = StoreClient::storage_target_offset(&chunks, &segment, 0, 64);
        assert_eq!(
            result.expect("storage mapping should use the published storage chunk"),
            2000
        );
    }

    #[test]
    fn storage_target_offset_allows_contiguous_split_chunks() {
        let segment = SegmentName::new("seg-split-storage");
        let chunks = vec![
            SegmentTargetChunk {
                logical_offset: 0,
                target_offset: 10_000,
                length_bytes: 64,
            },
            SegmentTargetChunk {
                logical_offset: 64,
                target_offset: 10_064,
                length_bytes: 64,
            },
        ];

        assert_eq!(
            StoreClient::storage_target_offset(&chunks, &segment, 60, 8)
                .expect("range may cross contiguous registration chunks"),
            10_060
        );
    }

    #[test]
    fn storage_target_offset_rejects_unpublished_range() {
        let segment = SegmentName::new("seg-storage-overrun");
        let chunks = vec![SegmentTargetChunk {
            logical_offset: 0,
            target_offset: 2000,
            length_bytes: 1024,
        }];

        let result = StoreClient::storage_target_offset(&chunks, &segment, 1000, 64);
        assert!(
            result.is_err(),
            "storage allocator offset must stay inside published target chunks"
        );
    }

    #[test]
    fn segment_relative_target_offset_rejects_holes() {
        let info = memory_segment(&[(1000, 64), (2000, 64)]);
        let segment = SegmentName::new("seg");

        assert!(
            StoreClient::segment_relative_target_offset(&info, &segment, 60, 8).is_err(),
            "logical ranges crossing non-contiguous buffers must not map"
        );
    }

    #[test]
    fn direct_buffer_transfer_requests_split_by_registration_limit() {
        let info = memory_segment(&[(900, 1024)]);
        let requests = StoreClient::target_buffer_transfer_requests(
            Opcode::Read,
            7,
            900,
            100usize as *mut c_void,
            100,
            &info,
            Some(64),
        )
        .expect("direct read planning should split requests");

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].source, 100usize as *mut c_void);
        assert_eq!(requests[0].target_id, 7);
        assert_eq!(requests[0].target_offset, 900);
        assert_eq!(requests[0].length, 64);
        assert_eq!(requests[1].source, 164usize as *mut c_void);
        assert_eq!(requests[1].target_id, 7);
        assert_eq!(requests[1].target_offset, 964);
        assert_eq!(requests[1].length, 36);
    }

    #[test]
    fn target_buffer_transfer_requests_split_at_remote_buffer_boundary() {
        let info = memory_segment(&[(0x1000, 0x1000), (0x2000, 0x1000)]);
        let requests = StoreClient::target_buffer_transfer_requests(
            Opcode::Write,
            7,
            0x1f80,
            0x8000usize as *mut c_void,
            0x100,
            &info,
            None,
        )
        .expect("remote target boundary should be split into valid slices");

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].source, 0x8000usize as *mut c_void);
        assert_eq!(requests[0].target_offset, 0x1f80);
        assert_eq!(requests[0].length, 0x80);
        assert_eq!(requests[1].source, 0x8080usize as *mut c_void);
        assert_eq!(requests[1].target_offset, 0x2000);
        assert_eq!(requests[1].length, 0x80);
    }

    // -----------------------------------------------------------------------
    // Segment offset boundary tests — reproduce and characterise the
    // "segment offset N length M is outside segment S" error observed in
    // production when sglang's KV cache backup thread calls batch_put_from
    // with a logical offset exceeding the segment's registered buffer size.
    // -----------------------------------------------------------------------

    #[test]
    fn segment_offset_exactly_at_boundary_is_rejected() {
        let four_gb: u64 = 4 * 1024 * 1024 * 1024;
        let info = memory_segment(&[(0x1_0000_0000, four_gb)]);
        let segment = SegmentName::new("seg-boundary");

        let result = StoreClient::segment_relative_target_offset(&info, &segment, four_gb, 1);
        assert!(
            result.is_err(),
            "offset equal to buffer length must be rejected"
        );
        let error_message = format!("{}", result.unwrap_err());
        assert!(
            error_message.contains("is outside segment"),
            "error should mention 'outside segment', got: {error_message}"
        );
    }

    #[test]
    fn segment_offset_exceeds_single_buffer_capacity() {
        // Reproduces the production scenario: segment has ~32 GB buffer but
        // the allocator handed out a ~50 GB offset (53686206464 bytes).
        let buffer_size: u64 = 32 * 1024 * 1024 * 1024;
        let info = memory_segment(&[(0x7f00_0000_0000, buffer_size)]);
        let segment = SegmentName::new(
            "sm-16--487fdbe0-1556-4ffb-a105-d6c5a7970cd5-segment-1777028012895-ext-1",
        );

        let overflowing_offset: u64 = 53_686_206_464; // ~50 GB
        let request_length: u64 = 1_540_096; // ~1.5 MB

        let result = StoreClient::segment_relative_target_offset(
            &info,
            &segment,
            overflowing_offset,
            request_length,
        );
        assert!(
            result.is_err(),
            "offset 50 GB into a 32 GB buffer must fail"
        );
        let error_message = format!("{}", result.unwrap_err());
        assert!(
            error_message.contains("is outside segment"),
            "error should contain 'outside segment', got: {error_message}"
        );
        assert!(
            error_message.contains("total_capacity="),
            "error should include total_capacity diagnostic, got: {error_message}"
        );
    }

    #[test]
    fn segment_offset_valid_within_large_buffer() {
        let buffer_size: u64 = 64 * 1024 * 1024 * 1024;
        let base: u64 = 0x7f00_0000_0000;
        let info = memory_segment(&[(base, buffer_size)]);
        let segment = SegmentName::new("seg-large");

        let offset: u64 = 53_686_206_464; // ~50 GB — fits in 64 GB
        let length: u64 = 1_540_096;

        let result = StoreClient::segment_relative_target_offset(&info, &segment, offset, length);
        assert!(
            result.is_ok(),
            "50 GB offset into 64 GB buffer should succeed, got: {:?}",
            result.unwrap_err()
        );
        assert_eq!(result.unwrap(), base + offset);
    }

    #[test]
    fn segment_offset_spans_across_buffer_tail_is_rejected() {
        let buffer_size: u64 = 1024 * 1024;
        let base: u64 = 0x1000_0000;
        let info = memory_segment(&[(base, buffer_size)]);
        let segment = SegmentName::new("seg-tail-overflow");

        let result = StoreClient::segment_relative_target_offset(
            &info,
            &segment,
            buffer_size - 1,
            2,
        );
        assert!(
            result.is_err(),
            "transfer crossing buffer tail must be rejected"
        );
    }

    #[test]
    fn segment_offset_multi_buffer_total_exceeded() {
        let sixteen_gb: u64 = 16 * 1024 * 1024 * 1024;
        let base1: u64 = 0x7f00_0000_0000;
        let base2: u64 = base1 + sixteen_gb;
        let info = memory_segment(&[(base1, sixteen_gb), (base2, sixteen_gb)]);
        let segment = SegmentName::new("seg-multi-overflow");

        let overflowing_offset: u64 = 53_686_206_464;
        let result = StoreClient::segment_relative_target_offset(
            &info,
            &segment,
            overflowing_offset,
            1_540_096,
        );
        assert!(
            result.is_err(),
            "offset exceeding multi-buffer total capacity must fail"
        );
    }

    #[test]
    fn segment_offset_multi_buffer_spans_correctly() {
        let thirty_two_gb: u64 = 32 * 1024 * 1024 * 1024;
        let base1: u64 = 0x7f00_0000_0000;
        let base2: u64 = base1 + thirty_two_gb;
        let info = memory_segment(&[(base1, thirty_two_gb), (base2, thirty_two_gb)]);
        let segment = SegmentName::new("seg-multi-valid");

        let offset: u64 = 53_686_206_464;
        let length: u64 = 1_540_096;

        let result =
            StoreClient::segment_relative_target_offset(&info, &segment, offset, length);
        assert!(
            result.is_ok(),
            "50 GB offset into 64 GB (2x32 GB) should succeed, got: {:?}",
            result.unwrap_err()
        );
        let expected_offset_in_second = offset - thirty_two_gb;
        assert_eq!(result.unwrap(), base2 + expected_offset_in_second);
    }

    #[test]
    fn segment_offset_empty_buffers_rejected() {
        let info = memory_segment(&[]);
        let segment = SegmentName::new("seg-empty");

        let result = StoreClient::segment_relative_target_offset(&info, &segment, 0, 1);
        assert!(result.is_err(), "empty buffer list must be rejected");
        let error_message = format!("{}", result.unwrap_err());
        assert!(
            error_message.contains("no buffers"),
            "error should mention 'no buffers', got: {error_message}"
        );
    }

    #[test]
    fn segment_offset_error_includes_capacity_diagnostic() {
        // Verify the improved error message includes total_capacity info.
        let buffer_size: u64 = 8 * 1024 * 1024 * 1024;
        let info = memory_segment(&[(0x1000, buffer_size)]);
        let segment = SegmentName::new("seg-diag");

        let result = StoreClient::segment_relative_target_offset(
            &info,
            &segment,
            buffer_size + 100,
            64,
        );
        assert!(result.is_err());
        let error_message = format!("{}", result.unwrap_err());
        assert!(
            error_message.contains(&format!("total_capacity={buffer_size}")),
            "error should include total_capacity={buffer_size}, got: {error_message}"
        );
        assert!(
            error_message.contains("num_buffers=1"),
            "error should include num_buffers=1, got: {error_message}"
        );
    }

    #[test]
    fn segment_info_covers_target_rejects_gap_between_buffers() {
        let info = memory_segment(&[(1000, 64), (2000, 64)]);

        assert!(
            !StoreClient::segment_info_covers_target(&info, 1060, 8),
            "address in gap between buffers should not be covered"
        );
        assert!(
            !StoreClient::segment_info_covers_target(&info, 1050, 20),
            "transfer spanning buffer boundary into gap should not be covered"
        );
    }

    #[test]
    fn segment_info_covers_target_accepts_contiguous_span() {
        let info = memory_segment(&[(1000, 64), (1064, 64)]);

        assert!(
            StoreClient::segment_info_covers_target(&info, 1000, 128),
            "full span across contiguous buffers should be covered"
        );
        assert!(
            StoreClient::segment_info_covers_target(&info, 1060, 8),
            "transfer crossing contiguous buffer boundary should be covered"
        );
    }

    #[test]
    fn segment_offset_u64_max_does_not_panic() {
        let info = memory_segment(&[(0, 1024)]);
        let segment = SegmentName::new("seg-u64max");

        let result = StoreClient::segment_relative_target_offset(&info, &segment, u64::MAX, 1);
        assert!(result.is_err(), "u64::MAX offset must be rejected");
    }

    #[test]
    fn segment_offset_addition_overflow_does_not_panic() {
        let info = memory_segment(&[(u64::MAX - 100, 200)]);
        let segment = SegmentName::new("seg-add-overflow");

        // Must not panic — either Ok or Err is acceptable.
        let _ = StoreClient::segment_relative_target_offset(&info, &segment, 50, 64);
    }

    // -----------------------------------------------------------------------
    // remote_replica_target_offset tests — this function is the entry point
    // for all read-path offset resolution and has two code paths:
    //   1. Fast path: replica.offset is present and already covered by buffers → return it
    //   2. Fallback: re-map via segment_relative_target_offset using
    //      replica.segment_offset
    // -----------------------------------------------------------------------

    fn make_replica(
        segment_name: &str,
        offset: u64,
        segment_offset: u64,
        length: u64,
    ) -> ReplicaRoute {
        ReplicaRoute {
            owner: ClientRuntimeId::new("test-runtime", ClientEpoch(0)),
            segment_name: SegmentName::new(segment_name),
            offset: Some(offset),
            segment_offset,
            length,
            checksum: None,
            tier: ReplicaTier::Dram,
            priority: 0,
        }
    }

    #[test]
    fn remote_replica_target_offset_fast_path_returns_absolute_offset() {
        let info = memory_segment(&[(1000, 128)]);
        let replica = make_replica("seg", 1024, 24, 32);

        let result = StoreClient::remote_replica_target_offset(&info, &replica);
        assert_eq!(
            result.expect("fast path should succeed when offset is within buffer"),
            1024,
            "fast path must return replica.offset directly"
        );
    }

    #[test]
    fn remote_replica_target_offset_fallback_remaps_via_segment_offset() {
        // replica.offset is NOT covered by any buffer, so the function
        // should fall back to segment_relative_target_offset using
        // replica.segment_offset.
        let info = memory_segment(&[(1000, 128)]);
        let replica = make_replica("seg", 9999, 24, 32);

        let result = StoreClient::remote_replica_target_offset(&info, &replica);
        assert_eq!(
            result.expect("fallback should remap using segment_offset"),
            1024,
            "fallback must compute base + segment_offset"
        );
    }

    #[test]
    fn remote_replica_target_offset_missing_offset_remaps_even_when_zero_is_valid() {
        let info = memory_segment(&[(0, 128)]);
        let mut replica = make_replica("seg", 0, 24, 32);
        replica.offset = None;

        let result = StoreClient::remote_replica_target_offset(&info, &replica);
        assert_eq!(
            result.expect("legacy route should remap using segment_offset"),
            24,
            "missing offset must not be treated as absolute target address 0"
        );
    }

    #[test]
    fn remote_replica_target_offset_fallback_fails_when_segment_offset_out_of_range() {
        let info = memory_segment(&[(1000, 64)]);
        let replica = make_replica("seg", 9999, 100, 16);

        let result = StoreClient::remote_replica_target_offset(&info, &replica);
        assert!(
            result.is_err(),
            "fallback must fail when segment_offset exceeds buffer capacity"
        );
    }

    #[test]
    fn remote_replica_target_offset_fast_path_with_multi_buffer() {
        let info = memory_segment(&[(1000, 64), (1064, 64)]);
        let replica = make_replica("seg", 1080, 80, 32);

        let result = StoreClient::remote_replica_target_offset(&info, &replica);
        assert_eq!(
            result.expect("fast path should work across contiguous buffers"),
            1080,
            "fast path should accept offset spanning contiguous buffers"
        );
    }

    #[test]
    fn remote_replica_target_offset_fast_path_rejects_gap_crossing() {
        // offset is within first buffer, but length crosses the gap
        let info = memory_segment(&[(1000, 64), (2000, 64)]);
        let replica = make_replica("seg", 1060, 60, 16);

        let result = StoreClient::remote_replica_target_offset(&info, &replica);
        // Fast path should reject because covers_target fails for gap
        // Fallback should also fail because logical mapping crosses gap
        assert!(
            result.is_err(),
            "both paths must reject transfer that crosses buffer gap"
        );
    }

    #[test]
    fn remote_replica_target_offset_prefers_fast_path_over_fallback() {
        // Both paths are valid; the function should use the fast path
        // (returning replica.offset) rather than recomputing.
        let info = memory_segment(&[(1000, 128)]);
        let replica = make_replica("seg", 1024, 24, 16);

        let result = StoreClient::remote_replica_target_offset(&info, &replica);
        assert_eq!(
            result.expect("should prefer fast path"),
            1024,
            "when both paths are valid, fast path (replica.offset) wins"
        );
    }

    // -----------------------------------------------------------------------
    // segment_info_covers_target — additional coverage
    // -----------------------------------------------------------------------

    #[test]
    fn segment_info_covers_target_with_unordered_buffers() {
        // Buffers are given in reverse order — the internal sort must handle this.
        let info = memory_segment(&[(1064, 64), (1000, 64)]);

        assert!(
            StoreClient::segment_info_covers_target(&info, 1000, 128),
            "covers_target must sort buffers internally and accept contiguous span"
        );
        assert!(
            StoreClient::segment_info_covers_target(&info, 1060, 8),
            "covers_target must handle cross-buffer boundary with unordered input"
        );
    }

    #[test]
    fn segment_info_covers_target_zero_length() {
        let info = memory_segment(&[(1000, 64)]);

        assert!(
            StoreClient::segment_info_covers_target(&info, 1000, 0),
            "zero-length transfer should be trivially covered"
        );
        assert!(
            StoreClient::segment_info_covers_target(&info, 9999, 0),
            "zero-length at arbitrary address should be covered (nothing to validate)"
        );
    }

    #[test]
    fn segment_info_covers_target_single_buffer_exact_fit() {
        let info = memory_segment(&[(1000, 64)]);

        assert!(
            StoreClient::segment_info_covers_target(&info, 1000, 64),
            "transfer filling entire buffer should be covered"
        );
        assert!(
            !StoreClient::segment_info_covers_target(&info, 1000, 65),
            "transfer exceeding buffer by 1 byte must not be covered"
        );
    }

    #[test]
    fn segment_info_covers_target_at_buffer_end_boundary() {
        let info = memory_segment(&[(1000, 64)]);

        assert!(
            StoreClient::segment_info_covers_target(&info, 1063, 1),
            "last byte of buffer should be covered"
        );
        assert!(
            !StoreClient::segment_info_covers_target(&info, 1064, 1),
            "first byte past buffer end must not be covered"
        );
    }

    #[test]
    fn segment_info_covers_target_overlapping_buffers() {
        // Two buffers with overlapping address ranges — a defensive scenario
        // where metadata is malformed. The function should still not panic.
        let info = memory_segment(&[(1000, 100), (1050, 100)]);

        // Address 1000..1100 is covered by first buffer,
        // 1050..1150 by second — overlap at 1050..1100.
        assert!(
            StoreClient::segment_info_covers_target(&info, 1000, 50),
            "first half should be covered by first buffer"
        );
        assert!(
            StoreClient::segment_info_covers_target(&info, 1100, 50),
            "second half should be covered by second buffer"
        );
        // Spanning the full range should work via contiguous iteration
        // (sorted: first ends at 1100, second starts at 1050 < 1100 → overlap)
        // The function's find() loop should handle this correctly.
        let _ = StoreClient::segment_info_covers_target(&info, 1000, 150);
    }

    #[test]
    fn segment_info_covers_target_three_buffer_full_span() {
        let info = memory_segment(&[(1000, 32), (1032, 32), (1064, 32)]);

        assert!(
            StoreClient::segment_info_covers_target(&info, 1000, 96),
            "transfer spanning all three contiguous buffers should be covered"
        );
        assert!(
            StoreClient::segment_info_covers_target(&info, 1030, 36),
            "transfer crossing two buffer boundaries should be covered"
        );
    }

    // -----------------------------------------------------------------------
    // segment_relative_target_offset — transfer length crossing gap
    // -----------------------------------------------------------------------

    #[test]
    fn segment_offset_transfer_length_crosses_non_contiguous_gap() {
        // Two non-contiguous buffers with a gap at [1064, 2000).
        // Logical offset 60 maps to base + 60 = 1060 in the first buffer.
        // The transfer length of 16 extends to 1076, which is in the gap.
        let info = memory_segment(&[(1000, 64), (2000, 64)]);
        let segment = SegmentName::new("seg-gap-cross");

        let result = StoreClient::segment_relative_target_offset(&info, &segment, 60, 16);
        assert!(
            result.is_err(),
            "transfer whose length crosses into a gap must be rejected"
        );
    }

    #[test]
    fn segment_offset_transfer_exactly_fills_first_buffer() {
        // Transfer starts near the end of the first buffer and exactly
        // fills it — no crossing needed.
        let info = memory_segment(&[(1000, 64), (2000, 64)]);
        let segment = SegmentName::new("seg-exact-fill");

        let result = StoreClient::segment_relative_target_offset(&info, &segment, 60, 4);
        assert_eq!(
            result.expect("transfer exactly filling first buffer should succeed"),
            1060
        );
    }

    // -----------------------------------------------------------------------
    // total_capacity saturating protection
    // -----------------------------------------------------------------------

    #[test]
    fn segment_offset_total_capacity_saturates_on_overflow() {
        // Two buffers whose lengths sum to more than u64::MAX.
        let info = memory_segment(&[(0, u64::MAX), (u64::MAX, u64::MAX)]);
        let segment = SegmentName::new("seg-saturate");

        let result = StoreClient::segment_relative_target_offset(&info, &segment, 0, 1);
        // Should not panic due to overflow; the result is either Ok or Err.
        // The important thing is that total_capacity in the error message
        // is u64::MAX (saturated), not 0 or a wrapped value.
        match result {
            Ok(_) => {} // acceptable if the buffer layout allows it
            Err(error) => {
                let message = format!("{error}");
                assert!(
                    !message.contains("total_capacity=0"),
                    "total_capacity must not wrap to 0, got: {message}"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // segment_relative_target_offset — single buffer start boundary
    // -----------------------------------------------------------------------

    #[test]
    fn segment_offset_zero_offset_zero_length() {
        let info = memory_segment(&[(5000, 128)]);
        let segment = SegmentName::new("seg-zero");

        // length=0 is a degenerate case; segment_info_covers_target
        // returns true for length=0, so this should succeed.
        let result = StoreClient::segment_relative_target_offset(&info, &segment, 0, 0);
        assert_eq!(
            result.expect("zero offset + zero length should map to buffer base"),
            5000
        );
    }

    #[test]
    fn segment_offset_at_start_with_exact_buffer_length() {
        let info = memory_segment(&[(5000, 128)]);
        let segment = SegmentName::new("seg-exact");

        let result = StoreClient::segment_relative_target_offset(&info, &segment, 0, 128);
        assert_eq!(
            result.expect("transfer exactly filling buffer from offset 0 should succeed"),
            5000
        );
    }

    #[test]
    fn segment_offset_at_start_exceeding_buffer_length() {
        let info = memory_segment(&[(5000, 128)]);
        let segment = SegmentName::new("seg-exceed");

        let result = StoreClient::segment_relative_target_offset(&info, &segment, 0, 129);
        assert!(
            result.is_err(),
            "transfer exceeding buffer length from offset 0 must fail"
        );
    }
}
