impl StoreClient {
    fn replica_write_target(
        &self,
        storage_runtime: &ClientRuntimeId,
        segment_name: &SegmentName,
    ) -> Result<ReplicaWriteTarget> {
        let (target_chunks, transport_endpoint, _) =
            self.replica_write_target_metadata(storage_runtime, segment_name)?;
        Ok(ReplicaWriteTarget {
            storage_runtime: storage_runtime.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint,
            target_chunks,
        })
    }

    fn replica_write_target_metadata(
        &self,
        storage_runtime: &ClientRuntimeId,
        segment_name: &SegmentName,
    ) -> Result<(Vec<SegmentTargetChunk>, Option<String>, Option<String>)> {
        if let Some(segment) = self.allocator.lock().announcement(segment_name) {
            if segment.owner == *storage_runtime {
                return Ok((
                    segment.target_chunks,
                    segment.transport_endpoint,
                    segment.transport_segment_descriptor,
                ));
            }
        }
        {
            let state = self.state.lock();
            if let Some(metadata) =
                state.cached_segment_target_metadata(storage_runtime, segment_name)
            {
                return Ok((
                    metadata.target_chunks,
                    metadata.transport_endpoint,
                    metadata.transport_segment_descriptor,
                ));
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
        let target_chunks = segment.target_chunks.clone();
        let transport_endpoint = segment.transport_endpoint.clone();
        let transport_segment_descriptor = segment.transport_segment_descriptor.clone();
        if segment.owner == self.lease.runtime {
            self.allocator.lock().upsert(&segment);
        } else if !target_chunks.is_empty() || transport_segment_descriptor.is_some() {
            self.state.lock().cache_segment_target_metadata(
                storage_runtime,
                segment_name,
                &target_chunks,
                transport_endpoint.clone(),
                transport_segment_descriptor.clone(),
            );
        }
        Ok((
            target_chunks,
            transport_endpoint,
            transport_segment_descriptor,
        ))
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
        let mut remote_leases = BTreeMap::new();
        let mut remote_errors = BTreeMap::new();
        for runtime in requests
            .iter()
            .filter(|request| request.storage_runtime != self.lease.runtime)
            .map(|request| request.storage_runtime.clone())
            .collect::<BTreeSet<_>>()
        {
            match self.lookup_runtime_lease(&runtime) {
                Ok(lease) => {
                    remote_leases.insert(runtime, lease);
                }
                Err(error) => {
                    remote_errors.insert(runtime, error);
                }
            }
        }
        let mut resolved = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<_>>();
        struct RemoteReservationGroup {
            lease: ClientLease,
            any_indices: Vec<usize>,
            specific_indices: Vec<usize>,
        }

        struct RemoteReservationJob {
            storage_runtime: ClientRuntimeId,
            lease: ClientLease,
            any_indices: Vec<usize>,
            any_lengths: Vec<u64>,
            specific_indices: Vec<usize>,
            specific_ops: Vec<ReserveSpecificOp>,
        }

        struct RemoteReservationJobResult {
            storage_runtime: ClientRuntimeId,
            any_indices: Vec<usize>,
            any_results: Option<Result<Vec<Result<mooncake_store_core::SegmentReservation>>>>,
            specific_indices: Vec<usize>,
            specific_results: Option<Result<Vec<Result<mooncake_store_core::SegmentReservation>>>>,
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
            if let Some(error) = remote_errors.get(&request.storage_runtime) {
                resolved[index] = Some(Err(error.clone()));
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

        let control_client = self.control_client.clone();
        let remote_jobs = remote_groups
            .into_iter()
            .map(|(storage_runtime, group)| {
                let any_lengths = group
                    .any_indices
                    .iter()
                    .map(|index| requests[*index].length_bytes)
                    .collect::<Vec<_>>();
                let specific_ops = group
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
                RemoteReservationJob {
                    storage_runtime,
                    lease: group.lease,
                    any_indices: group.any_indices,
                    any_lengths,
                    specific_indices: group.specific_indices,
                    specific_ops,
                }
            })
            .collect::<Vec<_>>();
        let remote_results = run_bounded_parallel_jobs(remote_jobs, 8, move |job| {
            let any_results = if job.any_lengths.is_empty() {
                None
            } else {
                Some(control_client.batch_reserve_any(
                    &job.lease,
                    &job.storage_runtime,
                    &job.any_lengths,
                ))
            };
            let specific_results = if job.specific_ops.is_empty() {
                None
            } else {
                Some(control_client.batch_reserve_specific(
                    &job.lease,
                    &job.storage_runtime,
                    &job.specific_ops,
                ))
            };
            RemoteReservationJobResult {
                storage_runtime: job.storage_runtime,
                any_indices: job.any_indices,
                any_results,
                specific_indices: job.specific_indices,
                specific_results,
            }
        });

        for job in remote_results {
            if let Some(any_results) = job.any_results {
                match any_results {
                    Ok(results) => {
                        for (index, result) in job.any_indices.into_iter().zip(results) {
                            let reservation = match result {
                                Ok(reservation) => reservation,
                                Err(error) => {
                                    if should_mark_runtime_suspect_after_allocator_error(&error) {
                                        self.mark_runtime_suspect(
                                            &job.storage_runtime,
                                            "allocator_batch_reserve_any_failed",
                                        );
                                    }
                                    resolved[index] = Some(Err(error.clone()));
                                    continue;
                                }
                            };
                            resolved[index] = Some(
                                self.replica_write_target(
                                    &job.storage_runtime,
                                    &reservation.segment_name,
                                )
                                .map(|target| (target, reservation)),
                            );
                        }
                    }
                    Err(error) => {
                        if should_mark_runtime_suspect_after_allocator_error(&error) {
                            self.mark_runtime_suspect(
                                &job.storage_runtime,
                                "allocator_batch_reserve_any_failed",
                            );
                        }
                        for index in job.any_indices {
                            resolved[index] = Some(Err(error.clone()));
                        }
                    }
                }
            }

            if let Some(specific_results) = job.specific_results {
                match specific_results {
                    Ok(results) => {
                        for (index, result) in job.specific_indices.into_iter().zip(results) {
                            let reservation = match result {
                                Ok(reservation) => reservation,
                                Err(error) => {
                                    if should_mark_runtime_suspect_after_allocator_error(&error) {
                                        self.mark_runtime_suspect(
                                            &job.storage_runtime,
                                            "allocator_batch_reserve_specific_failed",
                                        );
                                    }
                                    resolved[index] = Some(Err(error.clone()));
                                    continue;
                                }
                            };
                            resolved[index] = Some(
                                self.replica_write_target(
                                    &job.storage_runtime,
                                    &reservation.segment_name,
                                )
                                .map(|target| (target, reservation)),
                            );
                        }
                    }
                    Err(error) => {
                        if should_mark_runtime_suspect_after_allocator_error(&error) {
                            self.mark_runtime_suspect(
                                &job.storage_runtime,
                                "allocator_batch_reserve_specific_failed",
                            );
                        }
                        for index in job.specific_indices {
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

    fn reclaim_unavailable_runtime_is_skippable(error: &StoreError) -> bool {
        matches!(
            error,
            StoreError::NotFound(message) | StoreError::InvalidState(message)
                if message.contains("runtime ") && message.contains(" is not available")
        )
    }

    fn duplicate_reclaim_release_is_skippable(error: &StoreError) -> bool {
        matches!(
            error,
            StoreError::Allocator(message)
                if message.contains("segment release missing live allocation")
        )
    }

    fn release_reclaim_allocations_best_effort(
        &self,
        requests: &[AllocationReleaseRequest],
        allow_duplicate_local_release: bool,
        context: &'static str,
    ) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }

        let mut remote_leases = BTreeMap::new();
        let mut remote_errors = BTreeMap::new();
        for runtime in requests
            .iter()
            .filter(|request| request.storage_runtime != self.lease.runtime)
            .map(|request| request.storage_runtime.clone())
            .collect::<BTreeSet<_>>()
        {
            match self.lookup_runtime_lease(&runtime) {
                Ok(lease) => {
                    remote_leases.insert(runtime, lease);
                }
                Err(error) => {
                    remote_errors.insert(runtime, error);
                }
            }
        }

        let mut remote_groups = BTreeMap::<ClientRuntimeId, (ClientLease, Vec<usize>)>::new();
        for (index, request) in requests.iter().enumerate() {
            if request.storage_runtime == self.lease.runtime {
                if let Err(error) = self.allocator.lock().release(
                    &request.storage_runtime,
                    &request.segment_name,
                    request.offset_bytes,
                    request.length_bytes,
                ) {
                    if allow_duplicate_local_release
                        && Self::duplicate_reclaim_release_is_skippable(&error)
                    {
                        crate::observability::registry::record_reclaim_release(
                            context,
                            "skipped_duplicate_local_release",
                        );
                        warn!(
                            runtime = %self.lease.runtime,
                            reclaim_runtime = %request.storage_runtime,
                            segment = %request.segment_name.0,
                            offset_bytes = request.offset_bytes,
                            length_bytes = request.length_bytes,
                            error = %error,
                            context,
                            "skipping duplicate local reclaim release"
                        );
                        continue;
                    }
                    return Err(error);
                }
                continue;
            }

            if let Some(error) = remote_errors.get(&request.storage_runtime) {
                if Self::reclaim_unavailable_runtime_is_skippable(error) {
                    crate::observability::registry::record_reclaim_release(
                        context,
                        "skipped_unavailable_runtime",
                    );
                    warn!(
                        runtime = %self.lease.runtime,
                        reclaim_runtime = %request.storage_runtime,
                        segment = %request.segment_name.0,
                        offset_bytes = request.offset_bytes,
                        length_bytes = request.length_bytes,
                        error = %error,
                        context,
                        "skipping reclaim for unavailable runtime"
                    );
                    continue;
                }
                return Err(error.clone());
            }

            let Some(lease) = remote_leases.get(&request.storage_runtime).cloned() else {
                crate::observability::registry::record_reclaim_release(
                    context,
                    "skipped_missing_runtime_lease",
                );
                warn!(
                    runtime = %self.lease.runtime,
                    reclaim_runtime = %request.storage_runtime,
                    segment = %request.segment_name.0,
                    offset_bytes = request.offset_bytes,
                    length_bytes = request.length_bytes,
                    context,
                    "skipping reclaim because runtime lease disappeared during lookup"
                );
                continue;
            };
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
                            if Self::reclaim_unavailable_runtime_is_skippable(&error) {
                                crate::observability::registry::record_reclaim_release(
                                    context,
                                    "skipped_unavailable_runtime",
                                );
                                warn!(
                                    runtime = %self.lease.runtime,
                                    reclaim_runtime = %storage_runtime,
                                    segment = %requests[index].segment_name.0,
                                    offset_bytes = requests[index].offset_bytes,
                                    length_bytes = requests[index].length_bytes,
                                    error = %error,
                                    context,
                                    "skipping reclaim release for unavailable runtime"
                                );
                                continue;
                            }
                            if Self::duplicate_reclaim_release_is_skippable(&error) {
                                crate::observability::registry::record_reclaim_release(
                                    context,
                                    "skipped_duplicate_remote_release",
                                );
                                warn!(
                                    runtime = %self.lease.runtime,
                                    reclaim_runtime = %storage_runtime,
                                    segment = %requests[index].segment_name.0,
                                    offset_bytes = requests[index].offset_bytes,
                                    length_bytes = requests[index].length_bytes,
                                    error = %error,
                                    context,
                                    "skipping duplicate remote reclaim release"
                                );
                                continue;
                            }
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
                                context,
                                "allocator batch reclaim release rpc failed"
                            );
                            crate::observability::registry::record_reclaim_release(
                                context,
                                "error",
                            );
                            return Err(error);
                        }
                    }
                }
                Err(error) => {
                    if Self::reclaim_unavailable_runtime_is_skippable(&error) {
                        crate::observability::registry::record_reclaim_release(
                            context,
                            "skipped_unavailable_runtime",
                        );
                        warn!(
                            runtime = %self.lease.runtime,
                            reclaim_runtime = %storage_runtime,
                            items = indices.len(),
                            error = %error,
                            context,
                            "skipping reclaim batch for unavailable runtime"
                        );
                        continue;
                    }
                    if Self::duplicate_reclaim_release_is_skippable(&error) {
                        crate::observability::registry::record_reclaim_release(
                            context,
                            "skipped_duplicate_remote_release",
                        );
                        warn!(
                            runtime = %self.lease.runtime,
                            reclaim_runtime = %storage_runtime,
                            items = indices.len(),
                            error = %error,
                            context,
                            "skipping duplicate remote reclaim batch release"
                        );
                        continue;
                    }
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
                        context,
                        "allocator batch reclaim release rpc failed"
                    );
                    crate::observability::registry::record_reclaim_release(context, "error");
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
        self.release_reclaim_allocations_best_effort(&releases, true, "flush_due_reclaims")
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
        self.release_reclaim_allocations_best_effort(&releases, true, "flush_all_reclaims")
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
                route_key: route.key.clone(),
                storage_runtime: replica.owner.clone(),
                segment_name: replica.segment_name.clone(),
                offset_bytes: replica.segment_offset,
                length_bytes: replica.length,
                cold_backing: route.cold_backing.clone(),
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
        self.refresh_lease_for_local_segment_publish()?;
        self.publish_local_segments(&segment_infos, 0)?;
        Ok(())
    }

    fn scoped_key(&self, tenant: &str, key: &str) -> ObjectKey {
        mooncake_store_core::scoped_object_key(tenant, key)
    }

    fn publish_local_segment(&self, segment: &StorageExtentInfo, used_bytes: u64) -> Result<()> {
        let transport_endpoint = self.local_transport_endpoint(&segment.segment_name);
        let transport_segment_descriptor =
            self.local_transport_segment_descriptor(&segment.segment_name);
        info!(
            runtime = %self.lease.runtime,
            segment = %segment.segment_name.0,
            transport_endpoint = transport_endpoint.as_deref().unwrap_or(""),
            capacity_bytes = segment.capacity_bytes,
            used_bytes,
            state = ?segment.state,
            "publishing local segment"
        );
        let mut announcement = segment.announcement(self.lease.runtime.clone(), used_bytes);
        announcement.transport_endpoint = transport_endpoint;
        announcement.transport_segment_descriptor = transport_segment_descriptor;
        self.allocator.lock().upsert(&announcement);
        self.metadata.publish_segment(&announcement)
    }

    fn local_transport_endpoint(&self, segment_name: &SegmentName) -> Option<String> {
        let transport = {
            let state = self.state.lock();
            state.local_transports.get(&segment_name.0).cloned()
        }?;
        transport
            .segment_name()
            .ok()
            .map(|endpoint| endpoint.trim().to_string())
            .filter(|endpoint| !endpoint.is_empty() && endpoint != &segment_name.0)
    }

    fn local_transport_segment_descriptor(&self, segment_name: &SegmentName) -> Option<String> {
        let transport = {
            let state = self.state.lock();
            state.local_transports.get(&segment_name.0).cloned()
        }
        .or_else(|| {
            let primary = self.segment_name().ok()?;
            (primary == *segment_name)
                .then(|| self.transport.clone())
                .flatten()
        })?;
        transport
            .local_segment_descriptor()
            .ok()
            .flatten()
            .map(|descriptor| descriptor.trim().to_string())
            .filter(|descriptor| !descriptor.is_empty())
    }

    fn refresh_lease_for_local_segment_publish(&self) -> Result<()> {
        let mut lease = self.lease.clone();
        lease.state = self.lifecycle_state();
        lease.expires_at_ms = lease
            .expires_at_ms
            .max(now_ms().saturating_add(
                self.lease_ttl_ms
                    .max(DEFAULT_SEGMENT_PUBLISH_LEASE_TTL_MS),
            ));
        self.metadata.upsert_client_lease(&lease)
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

fn run_bounded_parallel_jobs<Input, Output, F>(
    jobs: Vec<Input>,
    max_parallelism: usize,
    worker: F,
) -> Vec<Output>
where
    Input: Send,
    Output: Send,
    F: Fn(Input) -> Output + Sync,
{
    debug_assert!(
        max_parallelism > 0,
        "parallel job max_parallelism should be greater than zero"
    );
    let workers = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
        .min(jobs.len())
        .min(max_parallelism.max(1));
    if workers <= 1 || jobs.len() <= 1 {
        return jobs.into_iter().map(worker).collect();
    }

    let queue = std::sync::Mutex::new(std::collections::VecDeque::from(
        jobs.into_iter().enumerate().collect::<Vec<_>>(),
    ));
    let results = std::sync::Mutex::new(Vec::<(usize, Output)>::new());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let queue = &queue;
            let results = &results;
            let worker = &worker;
            scope.spawn(move || {
                let mut local_results = Vec::new();
                loop {
                    // Keep the queue lock scoped to pop_front only. Running worker(job)
                    // while holding the lock would let a panic poison later queue access.
                    let next_job = queue
                        .lock()
                        .expect("parallel job queue lock should succeed")
                        .pop_front();
                    let Some((index, job)) = next_job else {
                        break;
                    };
                    local_results.push((index, worker(job)));
                }
                if !local_results.is_empty() {
                    results
                        .lock()
                        .expect("parallel job results lock should succeed")
                        .extend(local_results);
                }
            });
        }
    });

    let mut results = results
        .into_inner()
        .expect("parallel job results lock should succeed");
    results.sort_by_key(|(index, _)| *index);
    results.into_iter().map(|(_, output)| output).collect()
}
