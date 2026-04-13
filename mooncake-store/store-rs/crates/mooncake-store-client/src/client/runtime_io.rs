impl StoreClient {
    fn enforce_namespace_quota(
        &self,
        object_id: &LogicalObjectId,
        value_len: usize,
        current: Option<&ObjectRoute>,
    ) -> Result<()> {
        let Some(quota) = self.namespace_quota.as_ref() else {
            return Ok(());
        };
        let mut live_bytes = 0u64;
        let mut live_objects = 0usize;
        for route in self.metadata.list_object_routes()? {
            if route.namespace.as_ref() != Some(&object_id.scope) || route.state != RouteState::Active {
                continue;
            }
            live_objects = live_objects.saturating_add(1);
            if let Some(replica) = route.replicas.iter().min_by_key(|replica| replica.priority) {
                live_bytes = live_bytes.saturating_add(replica.length);
            }
        }
        let replacing_existing = current.is_some();
        let current_bytes = current
            .and_then(|route| route.replicas.iter().min_by_key(|replica| replica.priority))
            .map(|replica| replica.length)
            .unwrap_or(0);
        let next_bytes = live_bytes
            .saturating_sub(current_bytes)
            .saturating_add(value_len as u64);
        let next_objects = if replacing_existing {
            live_objects
        } else {
            live_objects.saturating_add(1)
        };
        if let Some(max_objects) = quota.max_objects {
            if next_objects > max_objects {
                return Err(StoreError::Allocator(format!(
                    "namespace quota exceeded for {}/{}: objects {} > {}",
                    object_id.scope.tenant, object_id.scope.domain, next_objects, max_objects
                )));
            }
        }
        if let Some(max_bytes) = quota.max_bytes {
            if next_bytes > max_bytes {
                return Err(StoreError::Allocator(format!(
                    "namespace quota exceeded for {}/{}: bytes {} > {}",
                    object_id.scope.tenant, object_id.scope.domain, next_bytes, max_bytes
                )));
            }
        }
        Ok(())
    }

    fn put_scoped_with_policy_current(
        &self,
        tenant: &str,
        key: &str,
        value: &[u8],
        policy: Option<&ReplicationPolicy>,
        current: Option<&ObjectRoute>,
        reclaim_mode: ReclaimMode,
    ) -> Result<ObjectRoute> {
        self.ensure_local_memory()?;
        self.flush_due_reclaims()?;
        let object_id = mooncake_store_core::scoped_logical_object_id(tenant, key);
        let scoped_key = ObjectKey::from_logical_id(&object_id);
        self.enforce_namespace_quota(&object_id, value.len(), current)?;
        let policy = self.resolve_replication_policy(policy)?;
        let max_write_attempts = if policy.with_soft_pin {
            DEFAULT_PUT_WRITE_RETRY_LIMIT
        } else {
            1
        };
        let mut attempt = 0usize;
        loop {
            attempt = attempt.saturating_add(1);
            self.flush_due_reclaims()?;
            let reserve_tracker =
                OperationTracker::new("put_stage_reserve").input_bytes(value.len() as u64);
            let reserve_result = self.reserve_replica_targets(tenant, key, value.len(), &policy);
            reserve_tracker.finish(&reserve_result, 0);
            let (targets, reservations) = reserve_result?;
            let write_tracker =
                OperationTracker::new("put_stage_write").input_bytes(value.len() as u64);
            let write_result = self.write_reserved_replicas(&targets, &reservations, value);
            write_tracker.finish(&write_result, value.len() as u64);
            let offsets = match write_result {
                Ok(offsets) => offsets,
                Err(error) => {
                    let _ = self.release_reserved_allocations(&targets, &reservations);
                    self.note_remote_write_failure(
                        &targets,
                        &error,
                        "remote_write_failed",
                    );
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
            let expected_version = current.map(|route| route.version);
            let next_version = current
                .map(|route| route.version.next())
                .unwrap_or(RouteVersion(1));
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
                        offset: *offset,
                        segment_offset: reservations[priority].offset_bytes,
                        length: value.len() as u64,
                        checksum: Some(checksum),
                        tier: ReplicaTier::Dram,
                        priority: priority as u16,
                    })
                    .collect(),
            };
            mooncake_store_core::apply_route_identity(&mut route, &object_id);
            let cas_tracker = OperationTracker::new("put_stage_route_cas");
            let publish_started = Instant::now();
            let cas_result = self.route_directory.compare_and_swap_object_route(
                &self.lease,
                &route.key,
                expected_version,
                Some(&route),
            );
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
                let _ = self.release_reserved_allocations(&targets, &reservations);
                return Err(StoreError::Conflict(format!(
                    "route update lost race for tenant={tenant} key={key}"
                )));
            }
<<<<<<< HEAD
            self.storage_owner.track_route(&route);
            self.track_remote_storage_owners_best_effort(std::slice::from_ref(&route));
            if let Some(previous) = current {
                self.reclaim_route(previous, reclaim_mode)?;
            }
            return Ok(route);
=======
        };
        let expected_version = current.map(|route| route.version);
        let next_version = current
            .map(|route| route.version.next())
            .unwrap_or(RouteVersion(1));
        let checksum = payload_checksum(value);
        let mut route = ObjectRoute {
            key: scoped_key,
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
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
                    offset: *offset,
                    segment_offset: reservations[priority].offset_bytes,
                    length: value.len() as u64,
                    checksum: Some(checksum),
                    tier: ReplicaTier::Dram,
                    priority: priority as u16,
                })
                .collect(),
        };
        mooncake_store_core::apply_route_identity(&mut route, &object_id);
        let cas_tracker = OperationTracker::new("put_stage_route_cas");
        let publish_started = Instant::now();
        let cas_result = self.route_directory.compare_and_swap_object_route(
            &self.lease,
            &route.key,
            expected_version,
            Some(&route),
        );
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
            let _ = self.release_reserved_allocations(&targets, &reservations);
            return Err(StoreError::Conflict(format!(
                "route update lost race for tenant={tenant} key={key}"
            )));
>>>>>>> 215c90f ([Store-RS] centralize namespace identity routing)
        }
    }

    fn put_scoped_with_policy(
        &self,
        tenant: &str,
        key: &str,
        value: &[u8],
        policy: Option<&ReplicationPolicy>,
    ) -> Result<ObjectRoute> {
        let object_id = mooncake_store_core::scoped_logical_object_id(tenant, key);
        let load_tracker = OperationTracker::new("put_stage_load_route");
        let current_result = self
            .route_directory
            .get_object_route(&self.lease, &ObjectKey::from_logical_id(&object_id));
        load_tracker.finish(&current_result, 0);
        let current = current_result?;
        self.put_scoped_with_policy_current(
            tenant,
            key,
            value,
            policy,
            current.as_ref(),
            ReclaimMode::Scheduled,
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

    fn route_scan_authorities(&self) -> Result<Vec<ClientLease>> {
        Ok(self
            .available_compatible_live_clients(true)?
            .into_iter()
            .filter(|lease| {
                lease
                    .endpoints
                    .labels
                    .get("route")
                    .is_some_and(|value| value == "true")
            })
            .filter(|lease| lease.state == ClientLifecycleState::Active)
            .collect())
    }

    fn insert_latest_route(routes: &mut BTreeMap<LogicalObjectId, ObjectRoute>, route: ObjectRoute) {
        let Ok(object_id) = mooncake_store_core::route_logical_object_id(&route) else {
            return;
        };
        match routes.get(&object_id) {
            Some(current) if current.version >= route.version => {}
            _ => {
                routes.insert(object_id, route);
            }
        }
    }

    fn collect_routes_by_replica_owner(&self, owner: &ClientRuntimeId) -> Result<Vec<ObjectRoute>> {
        self.route_directory
            .list_routes_by_replica_owner(&self.lease, owner)
    }

    fn route_object_id(&self, route: &ObjectRoute) -> Result<LogicalObjectId> {
        mooncake_store_core::route_logical_object_id(route)
    }

    fn migration_policy_for_route(&self, route: &ObjectRoute) -> Result<ReplicationPolicy> {
        let active = self
            .available_compatible_live_clients(true)?
            .into_iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
            .map(|lease| lease.runtime)
            .collect::<BTreeSet<_>>();
        let preferred = route
            .replicas
            .iter()
            .filter(|replica| replica.owner != self.lease.runtime)
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
        successor: &ClientRuntimeId,
    ) -> Result<ReplicationPolicy> {
        let active = self
            .available_compatible_live_clients(true)?
            .into_iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
            .map(|lease| lease.runtime)
            .collect::<BTreeSet<_>>();
        let mut preferred = Vec::new();
        let mut seen = BTreeSet::new();

        let successor_key = successor.storage_key();
        seen.insert(successor_key.clone());
        preferred.push(successor_key);

        for replica in route.replicas.iter().filter(|replica| {
            replica.owner != self.lease.runtime
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

    fn migrate_owned_route_via_writer(
        &self,
        writer: &StoreClient,
        route: &ObjectRoute,
    ) -> Result<bool> {
        let object_id = self.route_object_id(route)?;
        let tenant = object_id.scope.tenant;
        let key = object_id.logical_key;
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

            let payload = writer.get_in_tenant(&tenant, &key)?;
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

            let policy = writer.migration_policy_for_route(&confirmed)?;
            match writer.put_scoped_with_policy_current(
                &tenant,
                &key,
                &payload,
                Some(&policy),
                Some(&confirmed),
                ReclaimMode::Deferred,
            ) {
                Ok(next) => {
                    writer.sync_route_to_live_authorities_excluding(
                        &next,
                        Some(&self.lease.runtime.stable_id),
                    )?;
                    writer.reclaim_route(&confirmed, ReclaimMode::Immediate)?;
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
        let tenant = object_id.scope.tenant;
        let key = object_id.logical_key;
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

            let payload = writer.get_in_tenant(&tenant, &key)?;
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

            let policy = writer.migration_policy_for_successor_route(&confirmed, successor)?;
            match writer.put_scoped_with_policy_current(
                &tenant,
                &key,
                &payload,
                Some(&policy),
                Some(&confirmed),
                ReclaimMode::Deferred,
            ) {
                Ok(next) => {
                    writer.sync_route_to_live_authorities_excluding(
                        &next,
                        Some(&self.lease.runtime.stable_id),
                    )?;
                    writer.reclaim_route(&confirmed, ReclaimMode::Immediate)?;
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

    fn replace_route_on_authority(
        &self,
        authority: &ClientLease,
        route: &ObjectRoute,
    ) -> Result<()> {
        let namespace = self.metadata.route_namespace();
        if authority.runtime.stable_id == self.lease.runtime.stable_id {
            return authority_replace(
                &namespace,
                &authority.runtime.stable_id,
                &route.key,
                Some(route),
            );
        }
        let request = RouteCasRequest {
            key: route.key.clone(),
            expected: None,
            next: Some(route.clone()),
        };
        let mut results = self.control_client.batch_replace_routes(
            authority,
            &namespace,
            &authority.runtime.stable_id,
            &[request],
        )?;
        results.pop().ok_or_else(|| {
            StoreError::Transport(
                "control plane replace route reply is missing batch item".to_string(),
            )
        })?
    }

    fn get_route_from_authority(
        &self,
        authority: &ClientLease,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        let namespace = self.metadata.route_namespace();
        if authority.runtime.stable_id == self.lease.runtime.stable_id {
            return authority_get(&namespace, &authority.runtime.stable_id, key);
        }
        let mut results = self.control_client.batch_get_routes(
            authority,
            &namespace,
            &authority.runtime.stable_id,
            std::slice::from_ref(key),
        )?;
        results.pop().ok_or_else(|| {
            StoreError::Transport(
                "control plane route get reply is missing batch item".to_string(),
            )
        })?
    }

    fn ensure_authority_route_not_stale(
        &self,
        authority: &ClientLease,
        route: &ObjectRoute,
    ) -> Result<()> {
        if self
            .get_route_from_authority(authority, &route.key)?
            .is_some_and(|current| current.version >= route.version)
        {
            return Ok(());
        }
        self.replace_route_on_authority(authority, route)
    }

    fn sync_route_to_live_authorities_excluding(
        &self,
        route: &ObjectRoute,
        excluded: Option<&ClientStableId>,
    ) -> Result<usize> {
        if self.route_control == RouteControlMode::MetadataOnly {
            return Ok(0);
        }
        let mut protected = 0usize;
        let mut last_error = None;
        for authority in self.route_scan_authorities()? {
            let is_excluded =
                excluded.is_some_and(|stable_id| authority.runtime.stable_id == *stable_id);
            match self.replace_route_on_authority(&authority, route) {
                Ok(()) => {
                    if !is_excluded {
                        protected = protected.saturating_add(1);
                    }
                }
                Err(error) => {
                    if authority.runtime != self.lease.runtime {
                        self.mark_runtime_suspect(
                            &authority.runtime,
                            "route_sync_authority_replace_failed",
                        );
                    }
                    warn!(
                        runtime = %self.lease.runtime,
                        authority = %authority.runtime,
                        key = %route.key.0,
                        error = %error,
                        "route authority sync failed during migration"
                    );
                    if !is_excluded {
                        last_error = Some(error);
                    }
                }
            }
        }
        if excluded.is_some() && protected == 0 {
            return Err(last_error.unwrap_or_else(|| {
                StoreError::InvalidState(format!(
                    "route {} has no live authority outside the draining runtime",
                    route.key.0
                ))
            }));
        }
        Ok(protected)
    }

    fn sync_local_authority_routes_to_live_authorities(&self) -> Result<usize> {
        if self.route_control == RouteControlMode::MetadataOnly {
            return Ok(0);
        }
        let routes = authority_list_routes(
            &self.metadata.route_namespace(),
            &self.lease.runtime.stable_id,
        )?;
        let mut synced = 0usize;
        for route in routes {
            let mut protected = 0usize;
            let mut last_error = None;
            for authority in self.route_scan_authorities()? {
                if authority.runtime.stable_id == self.lease.runtime.stable_id {
                    continue;
                }
                match self.ensure_authority_route_not_stale(&authority, &route) {
                    Ok(()) => protected = protected.saturating_add(1),
                    Err(error) => {
                        self.mark_runtime_suspect(
                            &authority.runtime,
                            "route_authority_shutdown_sync_failed",
                        );
                        warn!(
                            runtime = %self.lease.runtime,
                            authority = %authority.runtime,
                            key = %route.key.0,
                            error = %error,
                            "route authority shutdown sync failed"
                        );
                        last_error = Some(error);
                    }
                }
            }
            if protected == 0 {
                return Err(last_error.unwrap_or_else(|| {
                    StoreError::InvalidState(format!(
                        "local authority route {} has no live mirror",
                        route.key.0
                    ))
                }));
            }
            synced = synced.saturating_add(1);
        }
        Ok(synced)
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
                let _ = self.sync_local_authority_routes_to_live_authorities()?;
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
        &self,
        route: &ObjectRoute,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) -> Option<ReplicaRoute> {
        route
            .replicas
            .iter()
            .filter(|replica| {
                replica.owner == self.lease.runtime || readable_runtimes.contains(&replica.owner)
            })
            .min_by_key(|replica| replica.priority)
            .cloned()
    }

    fn prune_unreadable_replicas_best_effort(
        &self,
        route: &ObjectRoute,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) {
        let filtered = route
            .replicas
            .iter()
            .filter(|replica| {
                replica.owner == self.lease.runtime || readable_runtimes.contains(&replica.owner)
            })
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

        match self.route_directory.compare_and_swap_object_route(
            &self.lease,
            &route.key,
            Some(route.version),
            Some(&next),
        ) {
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
        policy.with_soft_pin
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
            self.mark_runtime_suspect(&runtime, context);
        }
        let _ = refresh_live_client_cache(
            self.metadata.as_ref(),
            &self.live_client_cache,
            "live_client_snapshot_remote_write_failure",
        );
    }

    fn fallback_replicas_for_route(
        &self,
        route: &ObjectRoute,
        selected: &ReplicaRoute,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) -> VecDeque<ReplicaRoute> {
        route.replicas
            .iter()
            .filter(|replica| !Self::has_same_replica(replica, selected))
            .filter(|replica| {
                replica.owner == self.lease.runtime || readable_runtimes.contains(&replica.owner)
            })
            .cloned()
            .collect()
    }

    fn try_advance_resolved_replica(
        &self,
        entry: &mut ResolvedObject,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) -> bool {
        while let Some(candidate) = entry.fallback_replicas.pop_front() {
            if candidate.owner != self.lease.runtime
                && !readable_runtimes.contains(&candidate.owner)
            {
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
        let readable_runtimes = self.readable_runtime_set(true)?;
        let Some(route) = self.query_route_in_tenant(&entry.tenant, &entry.key)? else {
            return Ok(false);
        };
        if route.state != RouteState::Active {
            return Ok(false);
        }
        let Some(replica) = self.select_readable_replica(&route, &readable_runtimes) else {
            return Ok(false);
        };
        let changed =
            route != entry.route || !Self::has_same_replica(&replica, &entry.replica);
        if !changed {
            return Ok(false);
        }
        let fallback_replicas =
            self.fallback_replicas_for_route(&route, &replica, &readable_runtimes);
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
        let tracker = OperationTracker::new("route_lookup_many");
        let result = (|| {
            let scoped = objects
                .iter()
                .map(|object| {
                    let tenant = object.tenant.unwrap_or(self.default_tenant());
                    (tenant.to_string(), self.scoped_key(tenant, object.key))
                })
                .collect::<Vec<_>>();
            let routes = self.route_directory.get_object_routes(
                &self.lease,
                &scoped
                    .iter()
                    .map(|(_, scoped)| scoped.clone())
                    .collect::<Vec<_>>(),
            )?;
            let readable_runtimes = self.readable_runtime_set(false)?;
            let mut resolved = Vec::with_capacity(objects.len());
            for (((tenant, _scoped), object), route) in scoped
                .into_iter()
                .zip(objects.iter())
                .zip(routes.into_iter())
            {
                let route = route.ok_or_else(|| {
                    StoreError::NotFound(format!("tenant={tenant} key={}", object.key))
                })?;
                if route.state != RouteState::Active {
                    return Err(StoreError::InvalidState(format!(
                        "tenant={tenant} key={} is not active",
                        object.key
                    )));
                }
                let (replica, readable_for_route) =
                    match self.select_readable_replica(&route, &readable_runtimes) {
                        Some(replica) => (replica, readable_runtimes.clone()),
                        None => {
                            let refreshed_readable = self.readable_runtime_set(true)?;
                            let replica = self
                                .select_readable_replica(&route, &refreshed_readable)
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
                let fallback_replicas =
                    self.fallback_replicas_for_route(&route, &replica, &readable_for_route);
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
            .and_then(|shaping| shaping.max_remote_batch_bytes)
            .map(|bytes| bytes as usize)
    }

    fn shaping_max_remote_batch_bytes(&self) -> Option<u64> {
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

        let local_paths = {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            resolved
                .iter()
                .map(|entry| {
                    entry.replica.owner == self.lease.runtime
                        && memory.has_storage_segment(&entry.replica.segment_name)
                })
                .collect::<Vec<_>>()
        };
        let local_items = local_paths.iter().filter(|local| **local).count();
        let local_bytes = local_paths
            .iter()
            .zip(lengths.iter())
            .filter_map(|(local, length)| (*local).then_some(*length as u64))
            .sum::<u64>();

        {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            for ((entry, buffer), local) in resolved
                .iter()
                .zip(buffers.iter_mut())
                .zip(local_paths.iter())
            {
                if !*local {
                    continue;
                }
                let source = memory.storage_address(
                    &entry.replica.segment_name,
                    entry.replica.segment_offset as usize,
                )?;
                unsafe {
                    ptr::copy_nonoverlapping(
                        source.cast::<u8>(),
                        buffer.as_mut_ptr(),
                        entry.replica.length as usize,
                    );
                }
            }
        }
        if local_bytes != 0 {
            record_success_metric("get_local_copy", 0, local_bytes);
        }

        let remote_indices = local_paths
            .iter()
            .enumerate()
            .filter_map(|(index, local)| (!*local).then_some(index))
            .collect::<Vec<_>>();
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
        let mut remote_direct_fallbacks = 0usize;
<<<<<<< HEAD
        while cursor < remote_indices.len() {
            match self.plan_remote_get_chunk(&remote_indices, &lengths, cursor)? {
                Some((next, scratch)) => {
                    match self.execute_remote_batch_get_chunk(
                        transport,
                        resolved,
                        buffers,
                        &remote_indices[cursor..next],
                        &scratch,
                        request_deadline,
                    ) {
                        Ok(()) => {
                            remote_batch_chunks += 1;
                        }
                        Err(_) => {
                            for index in &remote_indices[cursor..next] {
                                let buffer = &mut *buffers[*index];
                                self.read_single_object_with_failover(
                                    transport,
                                    &mut resolved[*index],
                                    buffer,
                                    request_deadline,
                                    "remote_batch_get_fallback",
                                )?;
                                remote_direct_fallbacks += 1;
=======
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
                let planning_window = &fairness_slice[cursor..capped_end];
                match self.plan_remote_get_chunk(planning_window, &lengths, 0)? {
                    Some((next, scratch)) => {
                        match self.execute_remote_batch_get_chunk(
                            transport,
                            resolved,
                            buffers,
                            &planning_window[..next],
                            &scratch,
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
                                        "remote_batch_get_fallback",
                                    )?;
                                    remote_direct_fallbacks += 1;
                                }
>>>>>>> 13606eb ([Store-RS] add runtime qos controls)
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
                            "remote_direct_get_fallback",
                        )?;
                        remote_direct_fallbacks += 1;
                        cursor += 1;
                    }
<<<<<<< HEAD
                    cursor = next;
                }
                None => {
                    let index = remote_indices[cursor];
                    let buffer = &mut *buffers[index];
                    self.read_single_object_with_failover(
                        transport,
                        &mut resolved[index],
                        buffer,
                        request_deadline,
                        "remote_direct_get_fallback",
                    )?;
                    remote_direct_fallbacks += 1;
                    cursor += 1;
=======
>>>>>>> 13606eb ([Store-RS] add runtime qos controls)
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
            remote_direct_fallbacks,
            "batch get completed across local and remote paths"
        );
        for (index, entry) in resolved.iter().enumerate() {
            validate_replica_checksum(&entry.replica, &buffers[index][..lengths[index]])?;
        }
        self.report_get_hits_best_effort(resolved);
        Ok(lengths)
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
        if !local_hits.is_empty() {
            let local_hits = local_hits.into_iter().collect::<Vec<_>>();
            let _ = self.storage_owner.report_route_hits(&local_hits);
        }
        if remote_hits.is_empty() {
            return;
        }
        let leases = match self.lookup_runtime_leases(remote_hits.keys().cloned()) {
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
        let mut failed_runtimes = BTreeSet::new();
        {
            let mut state = self.state.lock();
            for entry in entries {
                state.invalidate_remote_segment(&entry.replica.segment_name.0);
                if entry.replica.owner != self.lease.runtime {
                    failed_runtimes.insert(entry.replica.owner.clone());
                }
            }
        }
        if mark_runtime_suspect {
            for runtime in failed_runtimes {
                self.mark_runtime_suspect(&runtime, context);
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
    ) -> Result<Option<(usize, Vec<RegionAllocation>)>> {
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

    fn segment_info_contains_target(info: &SegmentInfo, target_offset: u64, length: u64) -> bool {
        info.buffers.iter().any(|buffer| {
            let Some(buffer_end) = buffer.base.checked_add(buffer.length) else {
                return false;
            };
            let Some(target_end) = target_offset.checked_add(length) else {
                return false;
            };
            target_offset >= buffer.base && target_end <= buffer_end
        })
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
        for buffer in &info.buffers {
            let Some(target_offset) = buffer.base.checked_add(segment_offset) else {
                continue;
            };
            if Self::segment_info_contains_target(info, target_offset, length) {
                return Ok(target_offset);
            }
        }
        Err(StoreError::Transport(format!(
            "segment offset {} length {} is outside segment {}",
            segment_offset, length, segment_name.0
        )))
    }

    fn remote_replica_target_offset(info: &SegmentInfo, replica: &ReplicaRoute) -> Result<u64> {
        if Self::segment_info_contains_target(info, replica.offset, replica.length) {
            return Ok(replica.offset);
        }
        Self::segment_relative_target_offset(
            info,
            &replica.segment_name,
            replica.segment_offset,
            replica.length,
        )
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
        let tracker = OperationTracker::new("get_remote_batch_chunk");
        let raw_result = (|| -> std::result::Result<(), (StoreError, bool)> {
            let requests = {
                let mut state = self.state.lock();
                let mut batch = Vec::with_capacity(remote_indices.len());
                let mut checked_runtimes = BTreeSet::new();
                for (position, index) in remote_indices.iter().enumerate() {
                    let entry = &resolved[*index];
                    if entry.replica.owner != self.lease.runtime
                        && checked_runtimes.insert(entry.replica.owner.clone())
                    {
                        drop(state);
                        self.ensure_remote_replica_reachable(&entry.replica)
                            .map_err(|error| (error, false))?;
                        state = self.state.lock();
                    }
                    let (segment, info) = state
                        .open_segment_with_info(transport, &entry.replica.segment_name.0)
                        .map_err(|error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })?;
                    let target_offset = Self::remote_replica_target_offset(&info, &entry.replica)
                        .map_err(|error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        })?;
                    batch.push(TransferRequest {
                        opcode: Opcode::Read,
                        source: scratch[position].addr,
                        target_id: segment,
                        target_offset,
                        length: entry.replica.length,
                    });
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
        let tracker = OperationTracker::new("get_remote_direct");
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
            let request = {
                let mut state = self.state.lock();
                let (segment, info) = state
                    .open_segment_with_info(transport, &resolved.replica.segment_name.0)
                    .map_err(|error| {
                        (
                            error.clone(),
                            Self::remote_read_failure_marks_runtime_suspect(&error),
                        )
                    })?;
                let target_offset =
                    Self::remote_replica_target_offset(&info, &resolved.replica).map_err(
                        |error| {
                            (
                                error.clone(),
                                Self::remote_read_failure_marks_runtime_suspect(&error),
                            )
                        },
                    )?;
                TransferRequest {
                    opcode: Opcode::Read,
                    source: buffer_ptr,
                    target_id: segment,
                    target_offset,
                    length: resolved.replica.length,
                }
            };
            let batch_id = transport.allocate_batch(1).map_err(|error| {
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
            let submit_result = transport.submit_with_hints(batch_id, &[request], &hints);
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
        let local = {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            resolved.replica.owner == self.lease.runtime
                && memory.has_storage_segment(&resolved.replica.segment_name)
        };
        if local {
            let source = {
                let state = self.state.lock();
                state.memory_ref()?.storage_address(
                    &resolved.replica.segment_name,
                    resolved.replica.segment_offset as usize,
                )?
            };
            unsafe {
                ptr::copy_nonoverlapping(source.cast::<u8>(), buffer.as_mut_ptr(), length);
            }
        } else {
            self.ensure_remote_replica_reachable(&resolved.replica)?;
            self.execute_remote_get_direct(transport, resolved, buffer, length, request_deadline)?;
        }
        validate_replica_checksum(&resolved.replica, &buffer[..length])
    }

    fn read_single_object_with_failover(
        &self,
        transport: &dyn StoreTransport,
        resolved: &mut ResolvedObject,
        buffer: &mut [u8],
        request_deadline: RequestDeadline,
        _context: &'static str,
    ) -> Result<()> {
        loop {
            match self.execute_selected_replica_direct(transport, resolved, buffer, request_deadline)
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let readable_runtimes = self.readable_runtime_set(true)?;
                    if !self.try_advance_resolved_replica(resolved, &readable_runtimes) {
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
                    self.maybe_prune_route_after_failover(resolved, &readable_runtimes);
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
