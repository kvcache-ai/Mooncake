impl StoreClient {
    fn segment_name(&self) -> Result<SegmentName> {
        self.lease
            .endpoints
            .segment_name
            .clone()
            .ok_or_else(|| StoreError::InvalidState("segment_name is not configured".to_string()))
    }

    fn write_reserved_replicas(
        &self,
        targets: &[ReplicaWriteTarget],
        reservations: &[mooncake_store_core::SegmentReservation],
        value: &[u8],
    ) -> Result<Vec<u64>> {
        if targets.len() != reservations.len() {
            return Err(StoreError::InvalidState(
                "targets and reservations length mismatch".to_string(),
            ));
        }
        let transport = self.transport()?;
        let mut absolute_offsets = vec![0u64; targets.len()];
        let mut remote_requests = Vec::new();
        let mut local_writes = 0usize;

        for (index, (target, reservation)) in targets.iter().zip(reservations.iter()).enumerate() {
            if target.storage_runtime == self.lease.runtime
                && self
                    .state
                    .lock()
                    .memory_ref()
                    .map(|memory| memory.has_storage_segment(&target.segment_name))
                    .unwrap_or(false)
            {
                let addr = {
                    let state = self.state.lock();
                    state
                        .memory_ref()?
                        .storage_address(&target.segment_name, reservation.offset_bytes as usize)?
                };
                unsafe {
                    ptr::copy_nonoverlapping(value.as_ptr(), addr.cast::<u8>(), value.len());
                }
                absolute_offsets[index] = addr as u64;
                local_writes += 1;
                continue;
            }

            let handle = {
                let mut state = self.state.lock();
                state.open_segment(transport, &target.segment_name.0)?
            };
            let info = transport.get_segment_info(handle)?;
            let buffer = info.buffers.first().ok_or_else(|| {
                StoreError::Transport(format!(
                    "segment {} exposes no buffers",
                    target.segment_name.0
                ))
            })?;
            let target_offset = buffer
                .base
                .checked_add(reservation.offset_bytes)
                .ok_or_else(|| {
                    StoreError::Transport("remote target offset overflow".to_string())
                })?;
            remote_requests.push((index, handle, target_offset));
            absolute_offsets[index] = target_offset;
        }
        if local_writes != 0 {
            record_success_metric("put_local_copy", (local_writes * value.len()) as u64, 0);
        }
        debug!(
            runtime = %self.lease.runtime,
            replicas = targets.len(),
            local_writes,
            remote_writes = remote_requests.len(),
            value_bytes = value.len(),
            "writing reserved replicas"
        );

        if !remote_requests.is_empty() {
            let tracker = OperationTracker::new("put_remote_batch_write")
                .input_bytes((remote_requests.len() * value.len()) as u64);
            let result = (|| {
                let scratch = {
                    let state = self.state.lock();
                    state.memory_ref()?.plan_scratch(&[value.len()])?
                };
                copy_into_region(scratch[0], value);
                let batch_id = transport.allocate_batch(remote_requests.len())?;
                let requests = remote_requests
                    .iter()
                    .map(|(_, handle, target_offset)| TransferRequest {
                        opcode: Opcode::Write,
                        source: scratch[0].addr,
                        target_id: *handle,
                        target_offset: *target_offset,
                        length: value.len() as u64,
                    })
                    .collect::<Vec<_>>();
                let submit_result = transport.submit(batch_id, &requests);
                if let Err(error) = submit_result {
                    let _ = transport.free_batch(batch_id);
                    return Err(error);
                }
                let wait_result =
                    wait_for_batch_completion(transport, batch_id, DEFAULT_TRANSFER_TIMEOUT);
                let free_result = transport.free_batch(batch_id);
                wait_result?;
                free_result
            })();
            tracker.finish(&result, 0);
            result?;
        }

        Ok(absolute_offsets)
    }

    fn batch_put_scoped_routed(
        &self,
        requests: &[PutRequest<'_>],
        policy: Option<&ReplicationPolicy>,
    ) -> Result<Vec<ObjectRoute>> {
        self.ensure_local_memory()?;
        self.flush_due_reclaims()?;
        self.validate_unique_requests(requests)?;
        let transport = self.transport()?;
        let WriteMode::Routed {
            planner,
            replica_count,
        } = &self.write_mode
        else {
            return Err(StoreError::InvalidState(
                "batch routed put requires routed write mode".to_string(),
            ));
        };
        let resolved_policy = self.resolve_replication_policy(policy)?;

        let object_refs = requests
            .iter()
            .map(|request| {
                let mut object = ObjectRef::new(request.key);
                if let Some(tenant) = request.tenant {
                    object = object.tenant(tenant);
                }
                object
            })
            .collect::<Vec<_>>();
        let rank_tracker = OperationTracker::new("batch_put_stage_rank");
        let rank_result = planner.rank_many(self, &object_refs);
        rank_tracker.finish(&rank_result, 0);
        let plans = rank_result?;

        struct PendingBatchReservation<'a> {
            scoped_key: ObjectKey,
            value: &'a [u8],
            candidates: Vec<ReplicaPlacementCandidate>,
            next_candidate: usize,
            targets: Vec<ReplicaWriteTarget>,
            reservations: Vec<mooncake_store_core::SegmentReservation>,
        }

        let release_pending = |entries: &[PendingBatchReservation<'_>]| {
            for entry in entries {
                let _ = self.release_reserved_allocations(&entry.targets, &entry.reservations);
            }
        };

        let reserve_tracker = OperationTracker::new("batch_put_stage_reserve").input_bytes(
            requests
                .iter()
                .map(|request| request.value.len())
                .sum::<usize>() as u64,
        );
        let reserve_result = (|| {
            let mut shared_candidates = Vec::new();
            let mut shared_seen = BTreeSet::new();
            for segment in &resolved_policy.preferred_segments {
                match self.lookup_preferred_segment(segment) {
                    Ok(preferred) => {
                        if !shared_seen.insert(preferred.owner.clone()) {
                            continue;
                        }
                        shared_candidates.push(ReplicaPlacementCandidate {
                            target: ReplicaPlacementTarget::Segment {
                                storage_runtime: preferred.owner,
                                segment_name: preferred.segment_name,
                            },
                            soft: resolved_policy.with_soft_pin,
                        });
                    }
                    Err(error) if self.should_skip_candidate(&error, resolved_policy.with_soft_pin) => {
                        debug!(
                            segment = %segment.0,
                            error = %error,
                            "batch put is skipping preferred segment after lookup failure"
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
            for storage_runtime in &resolved_policy.preferred_storage_runtimes {
                if !shared_seen.insert(storage_runtime.clone()) {
                    continue;
                }
                shared_candidates.push(ReplicaPlacementCandidate {
                    target: ReplicaPlacementTarget::StorageRuntime(storage_runtime.clone()),
                    soft: true,
                });
            }
            if resolved_policy.prefer_local
                && self.has_active_local_storage()
                && shared_seen.insert(self.lease.runtime.clone())
            {
                shared_candidates.push(ReplicaPlacementCandidate {
                    target: ReplicaPlacementTarget::StorageRuntime(self.lease.runtime.clone()),
                    soft: true,
                });
            }

            let mut pending = Vec::with_capacity(requests.len());
            for (request, plan) in requests.iter().zip(plans.iter()) {
                let tenant = request.tenant.unwrap_or(self.default_tenant());
                let mut candidates = shared_candidates.clone();
                let mut seen = shared_seen.clone();
                for owner in &plan.owners {
                    if seen.insert(owner.clone()) {
                        candidates.push(ReplicaPlacementCandidate {
                            target: ReplicaPlacementTarget::StorageRuntime(owner.clone()),
                            soft: true,
                        });
                    }
                }
                if candidates.is_empty() {
                    return Err(StoreError::InvalidState(format!(
                        "no placement candidates available for tenant={} key={}",
                        tenant, request.key
                    )));
                }
                pending.push(PendingBatchReservation {
                    scoped_key: self.scoped_key(tenant, request.key),
                    value: request.value,
                    candidates,
                    next_candidate: 0,
                    targets: Vec::with_capacity(*replica_count),
                    reservations: Vec::with_capacity(*replica_count),
                });
            }

            while pending
                .iter()
                .any(|entry| entry.targets.len() < resolved_policy.replica_count)
            {
                let mut round_requests = Vec::new();
                let mut round_indices = Vec::new();
                let mut round_candidates = Vec::new();
                let mut exhausted_key = None;
                for (index, entry) in pending.iter_mut().enumerate() {
                    if entry.targets.len() >= resolved_policy.replica_count {
                        continue;
                    }
                    let Some(candidate) = entry.candidates.get(entry.next_candidate).cloned() else {
                        exhausted_key = Some(entry.scoped_key.0.clone());
                        break;
                    };
                    entry.next_candidate += 1;
                    let storage_runtime = candidate.target.storage_runtime().clone();
                    let segment_name = match &candidate.target {
                        ReplicaPlacementTarget::StorageRuntime(_) => None,
                        ReplicaPlacementTarget::Segment { segment_name, .. } => {
                            Some(segment_name.clone())
                        }
                    };
                    round_indices.push(index);
                    round_candidates.push(candidate);
                    round_requests.push(StorageRuntimeReservationRequest {
                        require_local_memory: storage_runtime == self.lease.runtime,
                        storage_runtime,
                        segment_name,
                        length_bytes: entry.value.len() as u64,
                    });
                }
                if let Some(key) = exhausted_key {
                    release_pending(&pending);
                    return Err(StoreError::InvalidState(format!(
                        "not enough writable owners for key {key}"
                    )));
                }

                let round_results = self.reserve_storage_runtime_segments_batch(&round_requests)?;
                let mut exhausted_candidates = 0usize;
                for (((index, request), candidate), result) in round_indices
                    .into_iter()
                    .zip(round_requests.into_iter())
                    .zip(round_candidates.into_iter())
                    .zip(round_results.into_iter())
                {
                    match result {
                        Ok((target, reservation)) => {
                            pending[index].targets.push(target);
                            pending[index].reservations.push(reservation);
                        }
                        Err(error) if self.should_skip_candidate(&error, candidate.soft) => {
                            exhausted_candidates += 1;
                            debug!(
                                key = %pending[index].scoped_key.0,
                                storage_runtime = %request.storage_runtime,
                                segment = request
                                    .segment_name
                                    .as_ref()
                                    .map(|segment| segment.0.as_str())
                                    .unwrap_or("*"),
                                error = %error,
                                "batch put is skipping placement candidate"
                            );
                        }
                        Err(error) => {
                            release_pending(&pending);
                            return Err(error);
                        }
                    }
                }
                if exhausted_candidates == 0 {
                    continue;
                }
                if pending.iter().all(|entry| {
                    entry.targets.len() >= resolved_policy.replica_count
                        || entry.next_candidate >= entry.candidates.len()
                }) {
                    release_pending(&pending);
                    return Err(StoreError::InvalidState(
                        "batch put exhausted all placement candidates".to_string(),
                    ));
                }
            }

            let mut prepared = Vec::with_capacity(pending.len());
            for entry in pending {
                prepared.push(PreparedObjectWrite {
                    scoped_key: entry.scoped_key,
                    value: entry.value,
                    targets: entry.targets,
                    reservations: entry.reservations,
                });
            }
            Ok(prepared)
        })();
        reserve_tracker.finish(&reserve_result, 0);
        let prepared = reserve_result?;
        let release_prepared = |entries: &[PreparedObjectWrite<'_>]| {
            for entry in entries {
                let _ = self.release_reserved_allocations(&entry.targets, &entry.reservations);
            }
        };
        let remote_scratch = match prepared
            .iter()
            .filter(|entry| {
                entry.targets.iter().any(|target| {
                    !(target.storage_runtime == self.lease.runtime
                        && self
                            .state
                            .lock()
                            .memory_ref()
                            .map(|memory| memory.has_storage_segment(&target.segment_name))
                            .unwrap_or(false))
                })
            })
            .map(|entry| entry.value.len())
            .max()
        {
            Some(length) => {
                let state = self.state.lock();
                Some(state.memory_ref()?.plan_scratch(&[length])?[0])
            }
            None => None,
        };

        let route_load_tracker = OperationTracker::new("batch_put_stage_load_routes");
        let current_routes_result = self.route_directory.get_object_routes(
            &self.lease,
            &prepared
                .iter()
                .map(|entry| entry.scoped_key.clone())
                .collect::<Vec<_>>(),
        );
        route_load_tracker.finish(&current_routes_result, 0);
        let current_routes = match current_routes_result {
            Ok(routes) => routes,
            Err(error) => {
                release_prepared(&prepared);
                return Err(error);
            }
        };
        let write_tracker = OperationTracker::new("batch_put_stage_write").input_bytes(
            prepared
                .iter()
                .map(|entry| entry.value.len())
                .sum::<usize>() as u64,
        );
        let write_result = (|| {
            let mut routes = Vec::with_capacity(prepared.len());
            for (entry, current) in prepared.iter().zip(current_routes.into_iter()) {
                let mut replicas = Vec::with_capacity(entry.targets.len());
                let mut remote_requests = Vec::new();
                for (priority, (target, reservation)) in entry
                    .targets
                    .iter()
                    .zip(entry.reservations.iter())
                    .enumerate()
                {
                    let is_local = target.storage_runtime == self.lease.runtime
                        && self
                            .state
                            .lock()
                            .memory_ref()
                            .map(|memory| memory.has_storage_segment(&target.segment_name))
                            .unwrap_or(false);
                    let offset = if is_local {
                        let addr = {
                            let state = self.state.lock();
                            state.memory_ref()?.storage_address(
                                &target.segment_name,
                                reservation.offset_bytes as usize,
                            )?
                        };
                        unsafe {
                            ptr::copy_nonoverlapping(
                                entry.value.as_ptr(),
                                addr.cast::<u8>(),
                                entry.value.len(),
                            );
                        }
                        addr as u64
                    } else {
                        let handle = {
                            let mut state = self.state.lock();
                            state.open_segment(transport, &target.segment_name.0)?
                        };
                        let info = transport.get_segment_info(handle)?;
                        let buffer = info.buffers.first().ok_or_else(|| {
                            StoreError::Transport(format!(
                                "segment {} exposes no buffers",
                                target.segment_name.0
                            ))
                        })?;
                        let target_offset = buffer
                            .base
                            .checked_add(reservation.offset_bytes)
                            .ok_or_else(|| {
                                StoreError::Transport("remote target offset overflow".to_string())
                            })?;
                        remote_requests.push(TransferRequest {
                            opcode: Opcode::Write,
                            source: remote_scratch
                                .ok_or_else(|| {
                                    StoreError::InvalidState(
                                        "remote write is missing a scratch slot".to_string(),
                                    )
                                })?
                                .addr,
                            target_id: handle,
                            target_offset,
                            length: entry.value.len() as u64,
                        });
                        target_offset
                    };
                    replicas.push(ReplicaRoute {
                        owner: target.storage_runtime.clone(),
                        segment_name: target.segment_name.clone(),
                        offset,
                        segment_offset: reservation.offset_bytes,
                        length: entry.value.len() as u64,
                        checksum: None,
                        tier: ReplicaTier::Dram,
                        priority: priority as u16,
                    });
                }
                if !remote_requests.is_empty() {
                    let scratch = remote_scratch.ok_or_else(|| {
                        StoreError::InvalidState(
                            "remote write is missing a scratch slot".to_string(),
                        )
                    })?;
                    copy_into_region(scratch, entry.value);
                    let batch_id = transport.allocate_batch(remote_requests.len())?;
                    let submit_result = transport.submit(batch_id, &remote_requests);
                    if let Err(error) = submit_result {
                        let _ = transport.free_batch(batch_id);
                        return Err(error);
                    }
                    let wait_result =
                        wait_for_batch_completion(transport, batch_id, DEFAULT_TRANSFER_TIMEOUT);
                    let free_result = transport.free_batch(batch_id);
                    wait_result?;
                    free_result?;
                }

                let expected_version = current.as_ref().map(|route| route.version);
                let next_version = current
                    .as_ref()
                    .map(|route| route.version.next())
                    .unwrap_or(RouteVersion(1));
                routes.push(PendingRoutePublish {
                    key: entry.scoped_key.clone(),
                    expected_version,
                    previous: current,
                    route: ObjectRoute {
                        key: entry.scoped_key.clone(),
                        version: next_version,
                        state: RouteState::Active,
                        compatibility: self.lease.compatibility.clone(),
                        replicas,
                    },
                });
            }
            Ok(routes)
        })();
        write_tracker.finish(&write_result, 0);
        let routes = match write_result {
            Ok(routes) => routes,
            Err(error) => {
                release_prepared(&prepared);
                return Err(error);
            }
        };

        let cas_requests = routes
            .iter()
            .map(|pending| RouteCasRequest {
                key: pending.key.clone(),
                expected: pending.expected_version,
                next: Some(pending.route.clone()),
            })
            .collect::<Vec<_>>();
        let cas_tracker = OperationTracker::new("batch_put_stage_route_cas");
        let cas_results_result = self
            .route_directory
            .compare_and_swap_object_routes(&self.lease, &cas_requests);
        cas_tracker.finish(&cas_results_result, 0);
        let cas_results = match cas_results_result {
            Ok(results) => results,
            Err(error) => {
                release_prepared(&prepared);
                return Err(error);
            }
        };

        let mut published = Vec::with_capacity(routes.len());
        let mut first_error = None;
        for ((index, pending), cas_result) in
            routes.into_iter().enumerate().zip(cas_results.into_iter())
        {
            match cas_result {
                Ok(cas) if cas.applied => {
                    self.storage_owner.track_route(&pending.route);
                    if let Some(previous) = pending.previous.as_ref() {
                        self.schedule_route_reclaim(previous)?;
                    }
                    published.push(pending.route);
                }
                Ok(_) => {
                    let _ = self.release_reserved_allocations(
                        &prepared[index].targets,
                        &prepared[index].reservations,
                    );
                    first_error.get_or_insert_with(|| {
                        StoreError::Conflict(format!(
                            "route update lost race for key {}",
                            pending.key.0
                        ))
                    });
                }
                Err(error) => {
                    let _ = self.release_reserved_allocations(
                        &prepared[index].targets,
                        &prepared[index].reservations,
                    );
                    first_error.get_or_insert(error);
                }
            }
        }
        self.track_remote_storage_owners_best_effort(&published);
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(published)
    }

    fn validate_unique_requests(&self, requests: &[PutRequest<'_>]) -> Result<()> {
        let mut seen = std::collections::BTreeSet::new();
        for request in requests {
            let tenant = request.tenant.unwrap_or(self.default_tenant());
            let key = format!("{tenant}::{}", request.key);
            if !seen.insert(key.clone()) {
                return Err(StoreError::Conflict(format!(
                    "batch_put contains duplicate scoped key {key}"
                )));
            }
        }
        Ok(())
    }
}
