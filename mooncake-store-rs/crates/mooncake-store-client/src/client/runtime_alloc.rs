impl StoreClient {
    fn replica_write_target(
        &self,
        storage_runtime: &ClientRuntimeId,
        segment_name: &SegmentName,
    ) -> Result<ReplicaWriteTarget> {
        let segment = self.segment_announcement(storage_runtime, segment_name)?;
        Ok(ReplicaWriteTarget {
            storage_runtime: storage_runtime.clone(),
            segment_name: segment_name.clone(),
            target_chunks: segment.target_chunks,
        })
    }

    fn segment_announcement(
        &self,
        storage_runtime: &ClientRuntimeId,
        segment_name: &SegmentName,
    ) -> Result<SegmentAnnouncement> {
        if let Some(segment) = self.allocator.lock().announcement(segment_name) {
            if segment.owner == *storage_runtime {
                return Ok(segment);
            }
        }
        let segment = self
            .metadata
            .get_segment(storage_runtime, segment_name)?
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "segment {} for runtime {} not found",
                    segment_name.0, storage_runtime
                ))
            })?;
        if segment.owner == self.lease.runtime {
            self.allocator.lock().upsert(&segment);
        }
        Ok(segment)
    }

    fn reserve_specific_segment(
        &self,
        storage_runtime: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: usize,
        require_local_memory: bool,
    ) -> Result<(ReplicaWriteTarget, mooncake_store_core::SegmentReservation)> {
        let reservation = self.reserve_segment_allocation(
            storage_runtime,
            Some(segment_name),
            length_bytes as u64,
            require_local_memory,
        )?;
        let target = self.replica_write_target(storage_runtime, &reservation.segment_name)?;
        Ok((target, reservation))
    }

    fn reserve_storage_runtime_segments_batch(
        &self,
        requests: &[StorageRuntimeReservationRequest],
    ) -> Result<Vec<Result<(ReplicaWriteTarget, mooncake_store_core::SegmentReservation)>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let remote_leases = self
            .lookup_runtime_leases(
                requests
                    .iter()
                    .filter(|request| request.storage_runtime != self.lease.runtime)
                    .map(|request| request.storage_runtime.clone()),
            )
            .unwrap_or_default();
        let mut resolved = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<_>>();
        struct RemoteReservationGroup {
            lease: ClientLease,
            any_indices: Vec<usize>,
            specific_indices: Vec<usize>,
        }

        let mut remote_groups = BTreeMap::<ClientRuntimeId, RemoteReservationGroup>::new();
        for (index, request) in requests.iter().enumerate() {
            if request.storage_runtime == self.lease.runtime {
                resolved[index] = Some(
                    self.reserve_segment_allocation(
                        &request.storage_runtime,
                        request.segment_name.as_ref(),
                        request.length_bytes,
                        request.require_local_memory,
                    )
                    .and_then(|reservation| {
                        let target = self.replica_write_target(
                            &request.storage_runtime,
                            &reservation.segment_name,
                        )?;
                        Ok((target, reservation))
                    }),
                );
                continue;
            }
            let Some(lease) = remote_leases.get(&request.storage_runtime).cloned() else {
                resolved[index] = Some(Err(StoreError::NotFound(format!(
                    "runtime {} is not available",
                    request.storage_runtime
                ))));
                continue;
            };
            remote_groups
                .entry(request.storage_runtime.clone())
                .or_insert_with(|| RemoteReservationGroup {
                    lease,
                    any_indices: Vec::new(),
                    specific_indices: Vec::new(),
                });
            let group = remote_groups
                .get_mut(&request.storage_runtime)
                .expect("remote reservation group should exist");
            if request.segment_name.is_some() {
                group.specific_indices.push(index);
            } else {
                group.any_indices.push(index);
            }
        }

        for (storage_runtime, group) in remote_groups {
            if !group.any_indices.is_empty() {
                let lengths = group
                    .any_indices
                    .iter()
                    .map(|index| requests[*index].length_bytes)
                    .collect::<Vec<_>>();
                match self
                    .control_client
                    .batch_reserve_any(&group.lease, &storage_runtime, &lengths)
                {
                    Ok(results) => {
                        for ((index, _length_bytes), result) in group
                            .any_indices
                            .iter()
                            .copied()
                            .zip(lengths)
                            .zip(results)
                        {
                            let reservation = match result {
                                Ok(reservation) => reservation,
                                Err(error) => {
                                    if should_mark_runtime_suspect_after_allocator_error(&error) {
                                        self.mark_runtime_suspect(
                                            &storage_runtime,
                                            "allocator_batch_reserve_any_failed",
                                        );
                                    }
                                    resolved[index] = Some(Err(error.clone()));
                                    continue;
                                }
                            };
                            resolved[index] = Some(
                                self.replica_write_target(
                                    &storage_runtime,
                                    &reservation.segment_name,
                                )
                                .map(|target| (target, reservation)),
                            );
                        }
                    }
                    Err(error) => {
                        if should_mark_runtime_suspect_after_allocator_error(&error) {
                            self.mark_runtime_suspect(
                                &storage_runtime,
                                "allocator_batch_reserve_any_failed",
                            );
                        }
                        for index in group.any_indices {
                            resolved[index] = Some(Err(error.clone()));
                        }
                    }
                }
            }

            if !group.specific_indices.is_empty() {
                let ops = group
                    .specific_indices
                    .iter()
                    .map(|index| ReserveSpecificOp {
                        segment_name: requests[*index]
                            .segment_name
                            .clone()
                            .expect("specific reservation batch item must carry a segment name"),
                        length_bytes: requests[*index].length_bytes,
                    })
                    .collect::<Vec<_>>();
                match self
                    .control_client
                    .batch_reserve_specific(&group.lease, &storage_runtime, &ops)
                {
                    Ok(results) => {
                        for ((index, _request), result) in group
                            .specific_indices
                            .iter()
                            .copied()
                            .zip(ops.iter())
                            .zip(results)
                        {
                            let reservation = match result {
                                Ok(reservation) => reservation,
                                Err(error) => {
                                    if should_mark_runtime_suspect_after_allocator_error(&error) {
                                        self.mark_runtime_suspect(
                                            &storage_runtime,
                                            "allocator_batch_reserve_specific_failed",
                                        );
                                    }
                                    resolved[index] = Some(Err(error.clone()));
                                    continue;
                                }
                            };
                            resolved[index] = Some(
                                self.replica_write_target(
                                    &storage_runtime,
                                    &reservation.segment_name,
                                )
                                .map(|target| (target, reservation)),
                            );
                        }
                    }
                    Err(error) => {
                        if should_mark_runtime_suspect_after_allocator_error(&error) {
                            self.mark_runtime_suspect(
                                &storage_runtime,
                                "allocator_batch_reserve_specific_failed",
                            );
                        }
                        for index in group.specific_indices {
                            resolved[index] = Some(Err(error.clone()));
                        }
                    }
                }
            }
        }

        resolved
            .into_iter()
            .map(|entry| {
                entry.ok_or_else(|| {
                    StoreError::InvalidState(
                        "missing storage runtime reservation result from batch allocator"
                            .to_string(),
                    )
                })
            })
            .collect()
    }

    fn release_segment_allocations_batch(
        &self,
        requests: &[AllocationReleaseRequest],
    ) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        let remote_leases = self.lookup_runtime_leases(
            requests
                .iter()
                .filter(|request| request.storage_runtime != self.lease.runtime)
                .map(|request| request.storage_runtime.clone()),
        )?;
        let mut remote_groups = BTreeMap::<ClientRuntimeId, (ClientLease, Vec<usize>)>::new();
        for (index, request) in requests.iter().enumerate() {
            if request.storage_runtime == self.lease.runtime {
                self.allocator.lock().release(
                    &request.storage_runtime,
                    &request.segment_name,
                    request.offset_bytes,
                    request.length_bytes,
                )?;
                continue;
            }
            let lease = remote_leases
                .get(&request.storage_runtime)
                .cloned()
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "runtime {} is not available",
                        request.storage_runtime
                    ))
                })?;
            remote_groups
                .entry(request.storage_runtime.clone())
                .or_insert_with(|| (lease, Vec::new()))
                .1
                .push(index);
        }

        for (storage_runtime, (lease, indices)) in remote_groups {
            let ops = indices
                .iter()
                .map(|index| ReleaseOp {
                    segment_name: requests[*index].segment_name.clone(),
                    offset_bytes: requests[*index].offset_bytes,
                    length_bytes: requests[*index].length_bytes,
                })
                .collect::<Vec<_>>();
            match self
                .control_client
                .batch_release(&lease, &storage_runtime, &ops)
            {
                Ok(results) => {
                    for (index, result) in indices.iter().copied().zip(results) {
                        if let Err(error) = result {
                            if should_mark_runtime_suspect_after_allocator_error(&error) {
                                self.mark_runtime_suspect(
                                    &storage_runtime,
                                    "allocator_batch_release_failed",
                                );
                            }
                            debug!(
                                storage_runtime = %storage_runtime,
                                segment = %requests[index].segment_name.0,
                                offset_bytes = requests[index].offset_bytes,
                                length_bytes = requests[index].length_bytes,
                                error = %error,
                                "allocator batch release rpc failed"
                            );
                            return Err(error);
                        }
                    }
                }
                Err(error) => {
                    if should_mark_runtime_suspect_after_allocator_error(&error) {
                        self.mark_runtime_suspect(
                            &storage_runtime,
                            "allocator_batch_release_failed",
                        );
                    }
                    debug!(
                        storage_runtime = %storage_runtime,
                        error = %error,
                        items = indices.len(),
                        "allocator batch release rpc failed"
                    );
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    fn release_reserved_allocations(
        &self,
        targets: &[ReplicaWriteTarget],
        reservations: &[mooncake_store_core::SegmentReservation],
    ) -> Result<()> {
        if targets.len() != reservations.len() {
            return Err(StoreError::InvalidState(
                "targets and reservations length mismatch".to_string(),
            ));
        }
        let releases = targets
            .iter()
            .zip(reservations.iter())
            .map(|(target, reservation)| AllocationReleaseRequest {
                storage_runtime: target.storage_runtime.clone(),
                segment_name: target.segment_name.clone(),
                offset_bytes: reservation.offset_bytes,
                length_bytes: reservation.length_bytes,
            })
            .collect::<Vec<_>>();
        self.release_segment_allocations_batch(&releases)
    }

    fn flush_due_reclaims(&self) -> Result<()> {
        let due = {
            let mut state = self.state.lock();
            state.take_due_reclaims(now_ms())
        };
        let releases = due
            .into_iter()
            .map(|reclaim| AllocationReleaseRequest {
                storage_runtime: reclaim.storage_runtime,
                segment_name: reclaim.segment_name,
                offset_bytes: reclaim.offset_bytes,
                length_bytes: reclaim.length_bytes,
            })
            .collect::<Vec<_>>();
        self.release_segment_allocations_batch(&releases)
    }

    fn flush_all_reclaims(&self) -> Result<()> {
        let pending = {
            let mut state = self.state.lock();
            std::mem::take(&mut state.pending_reclaims)
        };
        let releases = pending
            .into_iter()
            .map(|reclaim| AllocationReleaseRequest {
                storage_runtime: reclaim.storage_runtime,
                segment_name: reclaim.segment_name,
                offset_bytes: reclaim.offset_bytes,
                length_bytes: reclaim.length_bytes,
            })
            .collect::<Vec<_>>();
        self.release_segment_allocations_batch(&releases)
    }

    fn schedule_route_reclaim(&self, route: &ObjectRoute) -> Result<()> {
        self.storage_owner.untrack_route(route);
        let grace_ms = self.local_memory.reclaim_grace_ms;
        if grace_ms == 0 {
            return self.release_route_allocations(route);
        }
        let object_id = mooncake_store_core::route_logical_object_id(route)?;
        let qos_tier = route
            .qos_tier
            .clone()
            .unwrap_or_else(|| mooncake_store_core::DEFAULT_QOS_TIER.to_string());
        let policy_rank = Self::reclaim_policy_rank(&qos_tier);
        let mut state = self.state.lock();
        let due_at_ms = now_ms().saturating_add(grace_ms);
        for replica in &route.replicas {
            state.pending_reclaims.push_back(PendingReclaim {
                due_at_ms,
                policy_rank,
                tenant: object_id.scope.tenant.clone(),
                qos_tier: qos_tier.clone(),
                storage_runtime: replica.owner.clone(),
                segment_name: replica.segment_name.clone(),
                offset_bytes: replica.segment_offset,
                length_bytes: replica.length,
            });
        }
        Ok(())
    }


    fn reclaim_policy_rank(qos_tier: &str) -> u8 {
        match qos_tier {
            "critical" => 3,
            "gold" => 2,
            "default" => 1,
            _ => 0,
        }
    }

    fn reclaim_route(&self, route: &ObjectRoute, mode: ReclaimMode) -> Result<()> {
        match mode {
            ReclaimMode::Scheduled => self.schedule_route_reclaim(route),
            ReclaimMode::Immediate => self.release_route_allocations(route),
            ReclaimMode::Deferred => Ok(()),
        }
    }

    fn transport(&self) -> Result<&dyn StoreTransport> {
        self.transport
            .as_deref()
            .ok_or_else(|| StoreError::Unsupported("transport is not configured".to_string()))
    }

    fn transport_factory(&self) -> Result<&dyn StoreTransportFactory> {
        self.transport_factory.as_deref().ok_or_else(|| {
            StoreError::Unsupported("transport factory is not configured".to_string())
        })
    }

    fn storage_segment_plans(
        &self,
        storage_bytes: usize,
    ) -> Result<Vec<crate::memory::LocalRegionPlan>> {
        let mut config = self.local_memory.clone();
        config.storage_bytes = storage_bytes;
        if !self.transport()?.supports_parallel_startup_registration() {
            config.numa_aware = false;
        }
        config.storage_region_plans(None)
    }

    fn ensure_local_memory(&self) -> Result<()> {
        if self.state.lock().memory.is_some() {
            return Ok(());
        }
        let transport = self.transport()?;
        let primary_segment = self.segment_name()?;
        let storage_segments = self.storage_segment_plans(self.local_memory.storage_bytes)?;
        let primary_segment_plan = storage_segments.first().cloned();
        let mut initial_config = self.local_memory.clone();
        initial_config.storage_bytes = primary_segment_plan
            .as_ref()
            .map(|plan| plan.capacity_bytes)
            .unwrap_or(0);
        if let Some(plan) = &primary_segment_plan {
            initial_config.location = plan.location.clone();
        }
        let mut memory = LocalMemoryState::register_startup_storage(
            transport,
            &primary_segment,
            &initial_config,
        )?;
        let mut initial_transports = Vec::new();
        if initial_config.storage_bytes > 0 {
            if let Some(transport) = self.transport.clone() {
                initial_transports.push((primary_segment.0.clone(), transport));
            }
        }

        struct StartupSegmentRegistration {
            segment_name: SegmentName,
            transport: Arc<dyn crate::transport::StoreTransport>,
            region: crate::memory::RegisteredRegion,
            state: SegmentLifecycleState,
            tags: Vec<String>,
        }

        struct StartupSegmentPlan {
            segment_name: SegmentName,
            transport: Arc<dyn crate::transport::StoreTransport>,
            capacity_bytes: usize,
            location: String,
            alignment: usize,
            hugepage_enabled: Option<bool>,
            hugepage_size_bytes: Option<usize>,
            state: SegmentLifecycleState,
            tags: Vec<String>,
        }

        let extra_names = {
            let mut state = self.state.lock();
            state.next_local_segment_id = state.next_local_segment_id.max(1);
            storage_segments
                .iter()
                .skip(1)
                .map(|_| state.next_segment_name(&primary_segment))
                .collect::<Vec<_>>()
        };
        let mut extra_segments = Vec::with_capacity(extra_names.len());
        for (plan, segment_name) in storage_segments
            .into_iter()
            .skip(1)
            .zip(extra_names)
        {
            let local_transport = self.transport_factory()?.create(&segment_name.0)?;
            extra_segments.push(StartupSegmentPlan {
                segment_name,
                transport: local_transport,
                capacity_bytes: plan.capacity_bytes,
                location: plan.location,
                alignment: self.local_memory.alignment,
                hugepage_enabled: self.local_memory.hugepage_enabled,
                hugepage_size_bytes: self.local_memory.hugepage_size_bytes,
                state: SegmentLifecycleState::Active,
                tags: self.local_memory.tags.clone(),
            });
        }

        let registrations = match std::thread::scope(|scope| -> Result<Vec<StartupSegmentRegistration>> {
            let mut handles = Vec::with_capacity(extra_segments.len());
            for plan in extra_segments {
                handles.push(scope.spawn(move || {
                    let region = crate::memory::RegisteredRegion::register_startup_storage(
                        plan.transport.as_ref(),
                        plan.capacity_bytes,
                        &plan.location,
                        plan.alignment,
                        mooncake_store_core::HugePageConfig::resolve(
                            plan.hugepage_enabled,
                            plan.hugepage_size_bytes,
                        )?,
                    )?;
                    Ok::<StartupSegmentRegistration, StoreError>(StartupSegmentRegistration {
                        segment_name: plan.segment_name,
                        transport: plan.transport,
                        region,
                        state: plan.state,
                        tags: plan.tags,
                    })
                }));
            }

            let mut completed = Vec::with_capacity(handles.len());
            for handle in handles {
                match handle.join() {
                    Ok(Ok(registration)) => completed.push(registration),
                    Ok(Err(error)) => {
                        for registration in completed {
                            let _ = registration.region.release(registration.transport.as_ref());
                        }
                        return Err(error);
                    }
                    Err(_) => {
                        for registration in completed {
                            let _ = registration.region.release(registration.transport.as_ref());
                        }
                        return Err(StoreError::Transport(
                            "startup storage registration worker panicked".to_string(),
                        ));
                    }
                }
            }
            Ok(completed)
        }) {
            Ok(registrations) => registrations,
            Err(error) => {
                if initial_config.storage_bytes > 0 {
                    let _ = memory.remove_storage_segment(transport, &primary_segment);
                }
                let _ = memory.release_scratch(transport);
                return Err(error);
            }
        };

        for registration in registrations {
            let segment_name = registration.segment_name.clone();
            memory.insert_registered_storage_segment(
                segment_name.clone(),
                registration.region,
                registration.state,
                registration.tags,
            )?;
            initial_transports.push((segment_name.0, registration.transport));
        }

        let segment_infos = memory.storage_segments();
        {
            let mut state = self.state.lock();
            for (segment_name, transport) in initial_transports {
                state.local_transports.insert(segment_name, transport);
            }
            state.memory = Some(memory);
        }
        self.publish_local_segments(&segment_infos, 0)?;
        Ok(())
    }

    fn scoped_key(&self, tenant: &str, key: &str) -> ObjectKey {
        mooncake_store_core::scoped_object_key(tenant, key)
    }

    fn publish_local_segment(&self, segment: &StorageExtentInfo, used_bytes: u64) -> Result<()> {
        debug!(
            runtime = %self.lease.runtime,
            segment = %segment.segment_name.0,
            capacity_bytes = segment.capacity_bytes,
            used_bytes,
            state = ?segment.state,
            "publishing local segment"
        );
        let announcement = segment.announcement(self.lease.runtime.clone(), used_bytes);
        self.allocator.lock().upsert(&announcement);
        self.metadata.publish_segment(&announcement)
    }

    fn publish_local_segments(&self, segments: &[StorageExtentInfo], used_bytes: u64) -> Result<()> {
        for segment in segments {
            self.publish_local_segment(segment, used_bytes)?;
        }
        Ok(())
    }

    fn reserve_storage_runtime_segment(
        &self,
        storage_runtime: &ClientRuntimeId,
        length_bytes: usize,
        require_local_memory: bool,
    ) -> Result<(ReplicaWriteTarget, mooncake_store_core::SegmentReservation)> {
        self.flush_due_reclaims()?;
        let reservation = self.reserve_segment_allocation(
            storage_runtime,
            None,
            length_bytes as u64,
            require_local_memory,
        )?;
        debug!(
            storage_runtime = %storage_runtime,
            segment = %reservation.segment_name.0,
            offset_bytes = reservation.offset_bytes,
            length_bytes = reservation.length_bytes,
            "reserved segment space"
        );
        let target = self.replica_write_target(storage_runtime, &reservation.segment_name)?;
        Ok((target, reservation))
    }

    fn release_route_allocations(&self, route: &ObjectRoute) -> Result<()> {
        self.storage_owner.untrack_route(route);
        let releases = route
            .replicas
            .iter()
            .map(|replica| {
                debug!(
                    owner = %replica.owner,
                    segment = %replica.segment_name.0,
                    offset_bytes = replica.segment_offset,
                    length_bytes = replica.length,
                    "releasing route allocation"
                );
                AllocationReleaseRequest {
                    storage_runtime: replica.owner.clone(),
                    segment_name: replica.segment_name.clone(),
                    offset_bytes: replica.segment_offset,
                    length_bytes: replica.length,
                }
            })
            .collect::<Vec<_>>();
        self.release_segment_allocations_batch(&releases)
    }
}
