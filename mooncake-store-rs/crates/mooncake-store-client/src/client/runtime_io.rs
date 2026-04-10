impl StoreClient {
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
        let scoped_key = self.scoped_key(tenant, key);
        let policy = self.resolve_replication_policy(policy)?;
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
                return Err(error);
            }
        };
        let expected_version = current.map(|route| route.version);
        let next_version = current
            .map(|route| route.version.next())
            .unwrap_or(RouteVersion(1));
        let route = ObjectRoute {
            key: scoped_key,
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
                    checksum: None,
                    tier: ReplicaTier::Dram,
                    priority: priority as u16,
                })
                .collect(),
        };
        let cas_tracker = OperationTracker::new("put_stage_route_cas");
        let cas_result = self.route_directory.compare_and_swap_object_route(
            &self.lease,
            &route.key,
            expected_version,
            Some(&route),
        );
        cas_tracker.finish(&cas_result, 0);
        let cas = cas_result?;
        if !cas.applied {
            let _ = self.release_reserved_allocations(&targets, &reservations);
            return Err(StoreError::Conflict(format!(
                "route update lost race for tenant={tenant} key={key}"
            )));
        }
        self.storage_owner.track_route(&route);
        self.track_remote_storage_owners_best_effort(std::slice::from_ref(&route));
        if let Some(previous) = current {
            self.reclaim_route(previous, reclaim_mode)?;
        }
        Ok(route)
    }

    fn put_scoped_with_policy(
        &self,
        tenant: &str,
        key: &str,
        value: &[u8],
        policy: Option<&ReplicationPolicy>,
    ) -> Result<ObjectRoute> {
        let load_tracker = OperationTracker::new("put_stage_load_route");
        let current_result = self
            .route_directory
            .get_object_route(&self.lease, &self.scoped_key(tenant, key));
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
            .compatible_live_clients(true)?
            .into_iter()
            .filter(|lease| {
                lease
                    .endpoints
                    .labels
                    .get("route")
                    .is_some_and(|value| value == "true")
            })
            .filter(|lease| lease.state != ClientLifecycleState::Standby)
            .collect())
    }

    fn insert_latest_route(routes: &mut BTreeMap<String, ObjectRoute>, route: ObjectRoute) {
        match routes.get(&route.key.0) {
            Some(current) if current.version >= route.version => {}
            _ => {
                routes.insert(route.key.0.clone(), route);
            }
        }
    }

    fn collect_routes_by_replica_owner(&self, owner: &ClientRuntimeId) -> Result<Vec<ObjectRoute>> {
        let namespace = self.metadata.route_namespace();
        let mut routes = BTreeMap::new();
        for route in self.metadata.list_object_routes()? {
            Self::insert_latest_route(&mut routes, route);
        }
        for authority in self.route_scan_authorities()? {
            let scanned = if authority.runtime.stable_id == self.lease.runtime.stable_id {
                authority_list_routes_by_replica_owner(
                    &namespace,
                    &authority.runtime.stable_id,
                    owner,
                )
            } else {
                self.control_client.list_routes_by_replica_owner(
                    &authority,
                    &namespace,
                    &authority.runtime.stable_id,
                    owner,
                )
            };
            match scanned {
                Ok(found) => {
                    for route in found {
                        Self::insert_latest_route(&mut routes, route);
                    }
                }
                Err(error) => {
                    debug!(
                        authority = %authority.runtime,
                        owner = %owner,
                        error = %error,
                        "route-owner scan failed during shrink"
                    );
                }
            }
        }
        Ok(routes.into_values().collect())
    }

    fn split_scoped_route_key<'a>(&self, route: &'a ObjectRoute) -> Result<(&'a str, &'a str)> {
        route.key.0.split_once("::").ok_or_else(|| {
            StoreError::InvalidState(format!("route key {} is missing tenant scope", route.key.0))
        })
    }

    fn migration_policy_for_route(&self, route: &ObjectRoute) -> Result<ReplicationPolicy> {
        let active = self
            .compatible_live_clients(true)?
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
            .compatible_live_clients(true)?
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
        let (tenant, key) = self.split_scoped_route_key(route)?;
        for _ in 0..4 {
            let Some(observed) = self.query_route_in_tenant(tenant, key)? else {
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

            let payload = writer.get_in_tenant(tenant, key)?;
            let Some(confirmed) = self.query_route_in_tenant(tenant, key)? else {
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
                tenant,
                key,
                &payload,
                Some(&policy),
                Some(&confirmed),
                ReclaimMode::Immediate,
            ) {
                Ok(next) => {
                    writer.sync_route_to_live_authorities(&next)?;
                    return Ok(true);
                }
                Err(StoreError::Conflict(_)) => continue,
                Err(error) => return Err(error),
            }
        }
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
        let (tenant, key) = self.split_scoped_route_key(route)?;
        for _ in 0..4 {
            let Some(observed) = self.query_route_in_tenant(tenant, key)? else {
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

            let payload = writer.get_in_tenant(tenant, key)?;
            let Some(confirmed) = self.query_route_in_tenant(tenant, key)? else {
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
                tenant,
                key,
                &payload,
                Some(&policy),
                Some(&confirmed),
                ReclaimMode::Immediate,
            ) {
                Ok(next) => {
                    writer.sync_route_to_live_authorities(&next)?;
                    return Ok(true);
                }
                Err(StoreError::Conflict(_)) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(StoreError::Conflict(format!(
            "client hot-upgrade lost route update race for {}",
            route.key.0
        )))
    }

    fn current_owned_allocations(&self) -> Result<BTreeSet<AllocationSpan>> {
        let mut allocations = BTreeSet::new();
        for route in self.collect_routes_by_replica_owner(&self.lease.runtime)? {
            let (tenant, key) = self.split_scoped_route_key(&route)?;
            let Some(current) = self.query_route_in_tenant(tenant, key)? else {
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

    fn sync_route_to_live_authorities(&self, route: &ObjectRoute) -> Result<()> {
        for authority in self.route_scan_authorities()? {
            self.replace_route_on_authority(&authority, route)?;
        }
        Ok(())
    }

    fn release_stale_local_allocations(
        &self,
        live_allocations: &BTreeSet<AllocationSpan>,
    ) -> Result<usize> {
        let stale = self
            .allocator
            .lock()
            .allocations()
            .into_iter()
            .filter(|allocation| !live_allocations.contains(allocation))
            .collect::<Vec<_>>();
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
                let replica = route
                    .replicas
                    .iter()
                    .min_by_key(|replica| replica.priority)
                    .cloned()
                    .ok_or_else(|| {
                        StoreError::InvalidState(format!(
                            "tenant={tenant} key={} has no replica",
                            object.key
                        ))
                    })?;
                resolved.push(ResolvedObject {
                    tenant: tenant.to_string(),
                    key: object.key.to_string(),
                    replica,
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
        tracker.finish(&result, 0);
        result
    }

    fn execute_batch_get_into(
        &self,
        resolved: &[ResolvedObject],
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

        let mut cursor = 0usize;
        let mut remote_batch_chunks = 0usize;
        let mut remote_direct_fallbacks = 0usize;
        while cursor < remote_indices.len() {
            match self.plan_remote_get_chunk(&remote_indices, &lengths, cursor)? {
                Some((next, scratch)) => {
                    self.execute_remote_batch_get_chunk(
                        transport,
                        resolved,
                        buffers,
                        &remote_indices[cursor..next],
                        &scratch,
                    )?;
                    remote_batch_chunks += 1;
                    cursor = next;
                }
                None => {
                    let index = remote_indices[cursor];
                    self.execute_remote_get_direct(
                        transport,
                        &resolved[index],
                        buffers[index],
                        lengths[index],
                    )?;
                    remote_direct_fallbacks += 1;
                    cursor += 1;
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
            remote_batch_chunks,
            remote_direct_fallbacks,
            "batch get completed across local and remote paths"
        );
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
            if let Err(error) = self.control_client.batch_track_replica_routes(lease, &routes) {
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

    fn execute_remote_batch_get_chunk(
        &self,
        transport: &dyn StoreTransport,
        resolved: &[ResolvedObject],
        buffers: &mut [&mut [u8]],
        remote_indices: &[usize],
        scratch: &[RegionAllocation],
    ) -> Result<()> {
        let bytes_out = remote_indices
            .iter()
            .map(|index| resolved[*index].replica.length)
            .sum::<u64>();
        let tracker = OperationTracker::new("get_remote_batch_chunk");
        let result = (|| {
            let requests = {
                let mut state = self.state.lock();
                let mut batch = Vec::with_capacity(remote_indices.len());
                for (position, index) in remote_indices.iter().enumerate() {
                    let entry = &resolved[*index];
                    let segment = state.open_segment(transport, &entry.replica.segment_name.0)?;
                    batch.push(TransferRequest {
                        opcode: Opcode::Read,
                        source: scratch[position].addr,
                        target_id: segment,
                        target_offset: entry.replica.offset,
                        length: entry.replica.length,
                    });
                }
                batch
            };
            let batch_id = transport.allocate_batch(requests.len())?;
            let submit_result = transport.submit(batch_id, &requests);
            if let Err(error) = submit_result {
                let _ = transport.free_batch(batch_id);
                return Err(error);
            }
            let wait_result =
                wait_for_batch_completion(transport, batch_id, DEFAULT_TRANSFER_TIMEOUT);
            let free_result = transport.free_batch(batch_id);
            wait_result?;
            free_result?;

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
        debug!(
            runtime = %self.lease.runtime,
            items = remote_indices.len(),
            bytes_out,
            "executed remote batch get chunk"
        );
        tracker.finish(&result, bytes_out);
        result
    }

    fn execute_remote_get_direct(
        &self,
        transport: &dyn StoreTransport,
        resolved: &ResolvedObject,
        buffer: &mut [u8],
        length: usize,
    ) -> Result<()> {
        let tracker = OperationTracker::new("get_remote_direct");
        let result = (|| {
            let buffer_ptr = buffer.as_mut_ptr().cast::<c_void>();
            let buffer_len = buffer.len();
            let mut registered_here = false;
            {
                let mut state = self.state.lock();
                if !state.buffer_is_registered(buffer_ptr, length) {
                    state.register_external_buffer(transport, buffer_ptr, buffer_len)?;
                    registered_here = true;
                }
            }
            let request = {
                let mut state = self.state.lock();
                let segment = state.open_segment(transport, &resolved.replica.segment_name.0)?;
                TransferRequest {
                    opcode: Opcode::Read,
                    source: buffer_ptr,
                    target_id: segment,
                    target_offset: resolved.replica.offset,
                    length: resolved.replica.length,
                }
            };
            let batch_id = transport.allocate_batch(1)?;
            let submit_result = transport.submit(batch_id, &[request]);
            if let Err(error) = submit_result {
                let _ = transport.free_batch(batch_id);
                if registered_here {
                    let _ = self
                        .state
                        .lock()
                        .unregister_external_buffer(transport, buffer_ptr, buffer_len);
                }
                return Err(error);
            }
            let wait_result =
                wait_for_batch_completion(transport, batch_id, DEFAULT_TRANSFER_TIMEOUT);
            let free_result = transport.free_batch(batch_id);
            if registered_here {
                let _ = self
                    .state
                    .lock()
                    .unregister_external_buffer(transport, buffer_ptr, buffer_len);
            }
            wait_result?;
            free_result
        })();
        debug!(
            runtime = %self.lease.runtime,
            tenant = %resolved.tenant,
            key = %resolved.key,
            bytes_out = length,
            "executed remote direct get fallback"
        );
        tracker.finish(&result, length as u64);
        result
    }
}
