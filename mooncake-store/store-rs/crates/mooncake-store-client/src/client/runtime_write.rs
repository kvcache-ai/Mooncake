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

            self.ensure_remote_runtime_reachable(&target.storage_runtime, &target.segment_name.0)?;
            let (handle, info) = {
                let mut state = self.state.lock();
                state.open_segment_with_info(transport, &target.segment_name.0)?
            };
            let target_offset = Self::segment_relative_target_offset(
                &info,
                &target.segment_name,
                reservation.offset_bytes,
                value.len() as u64,
            )?;
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
            let remote_bytes = (remote_requests.len() * value.len()) as u64;
            let request_deadline = self.request_deadline_for_transfer(remote_bytes, 1);
            let tracker = OperationTracker::new("put_remote_batch_write")
                .input_bytes(remote_bytes);
            let result = (|| {
                let scratch = {
                    let state = self.state.lock();
                    state.memory_ref()?.plan_scratch(&[value.len()])?
                };
                copy_into_region(scratch[0], value);
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
                let chunk_size = self.remote_write_chunk_limit(value.len(), requests.len());
                for chunk in requests.chunks(chunk_size) {
                    let batch_id = transport.allocate_batch(chunk.len())?;
                    let hints = self.remote_batch_hints(
                        self.default_tenant(),
                        (chunk.len() * value.len()) as u64,
                        TransferPacingMode::ThroughputOptimized,
                    );
                    let submit_result = transport.submit_with_hints(batch_id, chunk, &hints);
                    if let Err(error) = submit_result {
                        let _ = transport.free_batch(batch_id);
                        return Err(error);
                    }
                    let wait_result = wait_for_batch_completion_detailed(
                        transport,
                        batch_id,
                        self.transfer_stall_timeout,
                        request_deadline.instant(),
                    )
                    .map_err(StoreError::from);
                    let free_result = transport.free_batch(batch_id);
                    wait_result?;
                    free_result?;
                }
                Ok(())
            })();
            tracker.finish(&result, 0);
            result?;
            registry::record_transport_bytes("write", "storage", remote_bytes);
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
                if let Some(domain) = request.domain {
                    object = object.domain(domain);
                }
                if let Some(object_set) = request.object_set {
                    object = object.object_set(object_set);
                }
                if let Some(qos_tier) = request.qos_tier {
                    object = object.qos_tier(qos_tier);
                }
                object
            })
            .collect::<Vec<_>>();
        let rank_tracker = OperationTracker::new("batch_put_stage_rank");
        let rank_result = planner.rank_many(self, &object_refs);
        rank_tracker.finish(&rank_result, 0);
        let plans = rank_result?;

        struct PendingBatchReservation<'a> {
            tenant: &'a str,
            object_id: LogicalObjectId,
            qos_tier: Option<&'a str>,
            scoped_key: ObjectKey,
            current: Option<ObjectRoute>,
            value: &'a [u8],
            quota_reservation: Option<TenantQuotaReservationRequest>,
            candidates: Vec<ReplicaPlacementCandidate>,
            next_candidate: usize,
            targets: Vec<ReplicaWriteTarget>,
            reservations: Vec<mooncake_store_core::SegmentReservation>,
        }

        let release_pending = |entries: &[PendingBatchReservation<'_>], context: &str| {
            for entry in entries {
                let _ = self.release_reserved_allocations(&entry.targets, &entry.reservations);
                let _ = self.abort_tenant_quota_reservation(entry.quota_reservation.as_ref(), context);
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
            let add_preferred_segments = |
                this: &Self,
                segments: &[SegmentName],
                source: PreferredSegmentSource,
                soft: bool,
                shared_seen: &mut BTreeSet<ClientRuntimeId>,
                shared_candidates: &mut Vec<ReplicaPlacementCandidate>,
            | -> Result<()> {
                for segment in segments {
                    match this.lookup_preferred_segment(segment) {
                        Ok(preferred) => {
                            if this.runtime_is_suspect(&preferred.owner) {
                                this.log_skipped_preferred_segment(
                                    this.default_tenant(),
                                    "*",
                                    segment,
                                    source,
                                    "owner_suspect",
                                    None,
                                );
                                continue;
                            }
                            if !shared_seen.insert(preferred.owner.clone()) {
                                continue;
                            }
                            shared_candidates.push(ReplicaPlacementCandidate {
                                target: ReplicaPlacementTarget::Segment {
                                    storage_runtime: preferred.owner,
                                    segment_name: preferred.segment_name,
                                },
                                soft,
                            });
                        }
                        Err(error) if this.should_skip_candidate(&error, soft) => {
                            this.log_skipped_preferred_segment(
                                this.default_tenant(),
                                "*",
                                segment,
                                source,
                                Self::preferred_segment_skip_reason(&error),
                                Some(&error),
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(())
            };
            add_preferred_segments(
                self,
                &resolved_policy.required_preferred_segments,
                PreferredSegmentSource::Request,
                false,
                &mut shared_seen,
                &mut shared_candidates,
            )?;
            add_preferred_segments(
                self,
                &resolved_policy.hint_preferred_segments,
                PreferredSegmentSource::TenantPolicy,
                true,
                &mut shared_seen,
                &mut shared_candidates,
            )?;
            for storage_runtime in &resolved_policy.preferred_storage_runtimes {
                if self.runtime_is_suspect(storage_runtime) {
                    debug!(
                        storage_runtime = %storage_runtime,
                        "batch put is skipping suspect preferred storage owner"
                    );
                    continue;
                }
                if !shared_seen.insert(storage_runtime.clone()) {
                    continue;
                }
                shared_candidates.push(ReplicaPlacementCandidate {
                    target: ReplicaPlacementTarget::StorageRuntime(storage_runtime.clone()),
                    soft: true,
                });
            }
            if resolved_policy.prefer_local
                && self.can_prefer_local_storage_for_write_mode()
                && shared_seen.insert(self.lease.runtime.clone())
            {
                shared_candidates.push(ReplicaPlacementCandidate {
                    target: ReplicaPlacementTarget::StorageRuntime(self.lease.runtime.clone()),
                    soft: true,
                });
            }

            let route_load_tracker = OperationTracker::new("batch_put_stage_load_routes");
            let object_keys = object_refs
                .iter()
                .map(|object_ref| {
                    let object_id = LogicalObjectId::new(
                        NamespaceScope::with_defaults(
                            object_ref.tenant,
                            object_ref.domain,
                            object_ref.object_set,
                        ),
                        object_ref.key,
                    );
                    ObjectKey::from_logical_id(&object_id)
                })
                .collect::<Vec<_>>();
            let current_routes_result =
                self.route_directory.get_object_routes(&self.lease, &object_keys);
            route_load_tracker.finish(&current_routes_result, 0);
            let current_routes = current_routes_result?;

            let mut pending = Vec::with_capacity(requests.len());
            for (((request, plan), object_ref), current) in requests
                .iter()
                .zip(plans.iter())
                .zip(object_refs.iter())
                .zip(current_routes.into_iter())
            {
                let tenant = request.tenant.unwrap_or(self.default_tenant());
                let mut candidates = shared_candidates.clone();
                let mut seen = shared_seen.clone();
                for owner in &plan.owners {
                    if self.runtime_is_suspect(owner) {
                        debug!(
                            key = %self.scoped_key(tenant, request.key).0,
                            storage_runtime = %owner,
                            "batch put is skipping suspect ranked storage owner"
                        );
                        continue;
                    }
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
                let object_id = LogicalObjectId::new(
                    NamespaceScope::with_defaults(
                        Some(tenant),
                        request.domain,
                        request.object_set,
                    ),
                    request.key,
                );
                pending.push(PendingBatchReservation {
                    tenant,
                    object_id,
                    qos_tier: request.qos_tier,
                    scoped_key: ObjectKey::from_logical_id(&LogicalObjectId::new(
                        NamespaceScope::with_defaults(
                            object_ref.tenant,
                            object_ref.domain,
                            object_ref.object_set,
                        ),
                        object_ref.key,
                    )),
                    current,
                    value: request.value,
                    quota_reservation: None,
                    candidates,
                    next_candidate: 0,
                    targets: Vec::with_capacity(*replica_count),
                    reservations: Vec::with_capacity(*replica_count),
                });
            }

            let mut reservation_order = pending
                .iter()
                .enumerate()
                .map(|(index, entry)| (index, entry.scoped_key.0.clone()))
                .collect::<Vec<_>>();
            reservation_order.sort_by(|left, right| left.1.cmp(&right.1));
            let mut reserved_indices = Vec::with_capacity(reservation_order.len());
            for (index, _key) in reservation_order {
                let reserve_result = {
                    let entry = &pending[index];
                    self.reserve_tenant_quota_for_put(
                        &entry.object_id,
                        &entry.scoped_key,
                        entry.current.as_ref(),
                        entry.value.len(),
                    )
                };
                match reserve_result {
                    Ok(reservation) => {
                        pending[index].quota_reservation = reservation;
                        reserved_indices.push(index);
                    }
                    Err(error) => {
                        for reserved_index in reserved_indices {
                            let _ = self.abort_tenant_quota_reservation(
                                pending[reserved_index].quota_reservation.as_ref(),
                                "batch_put_quota_reserve_failed",
                            );
                        }
                        return Err(error);
                    }
                }
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
                    release_pending(&pending, "batch_put_reserve_failed");
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
                            release_pending(&pending, "batch_put_reserve_failed");
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
                    release_pending(&pending, "batch_put_reserve_failed");
                    return Err(StoreError::InvalidState(
                        "batch put exhausted all placement candidates".to_string(),
                    ));
                }
            }

            let mut prepared = Vec::with_capacity(pending.len());
            for entry in pending {
                prepared.push(PreparedObjectWrite {
                    tenant: entry.tenant,
                    object_id: entry.object_id,
                    qos_tier: entry.qos_tier,
                    scoped_key: entry.scoped_key,
                    value: entry.value,
                    quota_reservation: entry.quota_reservation,
                    targets: entry.targets,
                    reservations: entry.reservations,
                });
            }
            Ok(prepared)
        })();
        reserve_tracker.finish(&reserve_result, 0);
        let prepared = reserve_result?;
        let abort_prepared_quota = |entries: &[PreparedObjectWrite<'_>], context: &str| {
            for entry in entries {
                let _ = self.abort_tenant_quota_reservation(entry.quota_reservation.as_ref(), context);
            }
        };
        let release_prepared = |entries: &[PreparedObjectWrite<'_>], context: &str| {
            for entry in entries {
                let _ = self.release_reserved_allocations(&entry.targets, &entry.reservations);
                let _ = self.abort_tenant_quota_reservation(entry.quota_reservation.as_ref(), context);
            }
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
                release_prepared(&prepared, "batch_put_failed_before_publish");
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
            struct RemoteBatchWrite<'a> {
                tenant: &'a str,
                value: &'a [u8],
                requests: Vec<TransferRequest>,
            }

            let mut routes = Vec::with_capacity(prepared.len());
            let mut remote_writes = Vec::new();
            let mut checked_runtimes = BTreeSet::new();
            for (entry, current) in prepared.iter().zip(current_routes.into_iter()) {
                let checksum = payload_checksum(entry.value);
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
                        if target.storage_runtime != self.lease.runtime
                            && checked_runtimes.insert(target.storage_runtime.clone())
                        {
                            self.ensure_remote_runtime_reachable(
                                &target.storage_runtime,
                                &target.segment_name.0,
                            )?;
                        }
                        let (handle, info) = {
                            let mut state = self.state.lock();
                            state.open_segment_with_info(transport, &target.segment_name.0)?
                        };
                        let target_offset = Self::segment_relative_target_offset(
                            &info,
                            &target.segment_name,
                            reservation.offset_bytes,
                            entry.value.len() as u64,
                        )?;
                        remote_requests.push(TransferRequest {
                            opcode: Opcode::Write,
                            source: ptr::null_mut(),
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
                        checksum: Some(checksum),
                        tier: ReplicaTier::Dram,
                        priority: priority as u16,
                    });
                }
                if !remote_requests.is_empty() {
                    remote_writes.push(RemoteBatchWrite {
                        tenant: entry.value_tenant(),
                        value: entry.value,
                        requests: remote_requests,
                    });
                }

                let expected_version = current.as_ref().map(|route| route.version);
                let next_version = current
                    .as_ref()
                    .map(|route| route.version.next())
                    .unwrap_or(RouteVersion(1));
                let mut route = ObjectRoute {
                    key: entry.scoped_key.clone(),
                    namespace: None,
                    logical_key: None,
                    canonical_key: None,
                    sharing_scope: None,
                    qos_tier: None,
                    version: next_version,
                    state: RouteState::Active,
                    compatibility: self.lease.compatibility.clone(),
                    replicas,
                };
                mooncake_store_core::apply_route_identity(&mut route, &entry.object_id);
                route.qos_tier = Some(
                    entry
                        .qos_tier
                        .unwrap_or(mooncake_store_core::DEFAULT_QOS_TIER)
                        .to_string(),
                );
                routes.push(PendingRoutePublish {
                    key: entry.scoped_key.clone(),
                    expected_version,
                    previous: current,
                    quota_reservation: entry.quota_reservation.clone(),
                    route,
                });
            }
            let mut remote_order = (0..remote_writes.len()).collect::<Vec<_>>();
            remote_order.sort_by(|left, right| {
                remote_writes[*left].tenant.cmp(remote_writes[*right].tenant)
            });
            let mut cursor = 0usize;
            while cursor < remote_order.len() {
                let tenant = remote_writes[remote_order[cursor]].tenant;
                let same_tenant_end = remote_order[cursor..]
                    .iter()
                    .position(|index| remote_writes[*index].tenant != tenant)
                    .map(|offset| cursor + offset)
                    .unwrap_or(remote_order.len());
                let remaining_items = same_tenant_end - cursor;
                let item_limit = self
                    .fairness_max_remote_batch_items_per_tenant()
                    .unwrap_or(remaining_items)
                    .min(
                        self.shaping_max_remote_batch_burst_items()
                            .unwrap_or(remaining_items),
                    )
                    .max(1)
                    .min(remaining_items);
                let byte_limit = self.shaping_max_remote_batch_bytes();
                let mut selected = Vec::new();
                let mut scratch_lengths = Vec::new();
                let mut remote_bytes = 0u64;
                for index in &remote_order[cursor..same_tenant_end] {
                    if selected.len() >= item_limit {
                        break;
                    }
                    let write = &remote_writes[*index];
                    let write_remote_bytes =
                        write.requests.iter().map(|request| request.length).sum::<u64>();
                    if let Some(max_bytes) = byte_limit {
                        if !selected.is_empty()
                            && remote_bytes.saturating_add(write_remote_bytes) > max_bytes as u64
                        {
                            break;
                        }
                    }
                    scratch_lengths.push(write.value.len());
                    let planned = {
                        let state = self.state.lock();
                        state.memory_ref()?.plan_scratch(&scratch_lengths)
                    };
                    match planned {
                        Ok(scratch) => drop(scratch),
                        Err(StoreError::Allocator(_)) if !selected.is_empty() => {
                            scratch_lengths.pop();
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                    selected.push(*index);
                    remote_bytes = remote_bytes.saturating_add(write_remote_bytes);
                }
                let scratch = {
                    let state = self.state.lock();
                    state.memory_ref()?.plan_scratch(&scratch_lengths)?
                };
                let mut transfer_requests = Vec::new();
                for (position, index) in selected.iter().enumerate() {
                    let write = &remote_writes[*index];
                    copy_into_region(scratch[position], write.value);
                    transfer_requests.extend(write.requests.iter().map(|request| {
                        let mut request = *request;
                        request.source = scratch[position].addr;
                        request
                    }));
                }
                let batch_id = transport.allocate_batch(transfer_requests.len())?;
                let request_deadline =
                    self.request_deadline_for_transfer(remote_bytes, transfer_requests.len());
                let hints = self.remote_batch_hints(
                    tenant,
                    remote_bytes,
                    TransferPacingMode::ThroughputOptimized,
                );
                let submit_result =
                    transport.submit_with_hints(batch_id, &transfer_requests, &hints);
                if let Err(error) = submit_result {
                    let _ = transport.free_batch(batch_id);
                    return Err(error);
                }
                let wait_result = wait_for_batch_completion_detailed(
                    transport,
                    batch_id,
                    self.transfer_stall_timeout,
                    request_deadline.instant(),
                )
                .map_err(StoreError::from);
                let free_result = transport.free_batch(batch_id);
                wait_result?;
                free_result?;
                registry::record_transport_bytes("write", "storage", remote_bytes);
                cursor += selected.len();
            }
            Ok(routes)
        })();
        write_tracker.finish(&write_result, 0);
        let routes = match write_result {
            Ok(routes) => routes,
            Err(error) => {
                release_prepared(&prepared, "batch_put_failed_before_publish");
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
        let publish_started = Instant::now();
        let cas_results_result = self
            .route_directory
            .compare_and_swap_object_routes(&self.lease, &cas_requests);
        registry::record_replication_publish(
            match &cas_results_result {
                Ok(results) if results.iter().any(|result| result.is_err()) => "error",
                Ok(results) if results.iter().any(|result| {
                    result
                        .as_ref()
                        .ok()
                        .is_some_and(|cas| !cas.applied)
                }) =>
                {
                    "conflict"
                }
                Ok(_) => "ok",
                Err(StoreError::Conflict(_)) => "conflict",
                Err(_) => "error",
            },
            publish_started.elapsed(),
        );
        cas_tracker.finish(&cas_results_result, 0);
        let cas_results = match cas_results_result {
            Ok(results) => results,
            Err(error) => {
                release_prepared(&prepared, "batch_put_failed_before_publish");
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
                    if let Err(error) = self.finalize_tenant_quota_put(
                        pending.quota_reservation.as_ref(),
                        &pending.route,
                        prepared[index].value.len(),
                    ) {
                        first_error.get_or_insert(error);
                        continue;
                    }
                    self.storage_owner.track_route(&pending.route);
                    if let Some(previous) = pending.previous.as_ref() {
                        if let Err(error) = self.schedule_route_reclaim(previous) {
                            warn!(
                                runtime = %self.lease.runtime,
                                key = %pending.key.0,
                                error = %error,
                                "batch route overwrite reclaim scheduling failed after authoritative publish"
                            );
                        }
                    }
                    published.push(pending.route);
                }
                Ok(_) => {
                    let _ = self.release_reserved_allocations(
                        &prepared[index].targets,
                        &prepared[index].reservations,
                    );
                    let _ = self.abort_tenant_quota_reservation(
                        prepared[index].quota_reservation.as_ref(),
                        "batch_put_route_compare_and_swap_conflict",
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
                    let _ = self.abort_tenant_quota_reservation(
                        prepared[index].quota_reservation.as_ref(),
                        "batch_put_route_compare_and_swap_error",
                    );
                    first_error.get_or_insert(error);
                }
            }
        }
        if first_error.is_some() {
            abort_prepared_quota(&prepared, "batch_put_partial_publish_cleanup");
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
