#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchPutReserveRetryReason {
    NoPlacementCandidates,
    NotEnoughWritableOwners,
    ExhaustedCandidates,
}

impl BatchPutReserveRetryReason {
    fn retry_delay(self, attempt: usize) -> Duration {
        match self {
            Self::NoPlacementCandidates
            | Self::NotEnoughWritableOwners
            | Self::ExhaustedCandidates => {
                Duration::from_millis(300_u64.saturating_mul(attempt as u64))
            }
        }
    }
}

#[derive(Clone, Debug)]
struct BatchPutReserveStageError {
    error: StoreError,
    retry_reason: Option<BatchPutReserveRetryReason>,
}

impl BatchPutReserveStageError {
    fn retryable(error: StoreError, retry_reason: BatchPutReserveRetryReason) -> Self {
        Self {
            error,
            retry_reason: Some(retry_reason),
        }
    }

    fn terminal(error: StoreError) -> Self {
        Self {
            error,
            retry_reason: None,
        }
    }
}

#[derive(Clone, Debug)]
struct BatchPutItem<'a> {
    tenant: Option<&'a str>,
    domain: Option<&'a str>,
    object_set: Option<&'a str>,
    qos_tier: Option<&'a str>,
    key: &'a str,
    value: Option<&'a [u8]>,
    value_len: usize,
    registered_source: Option<*mut c_void>,
}

impl<'a> BatchPutItem<'a> {
    fn from_put_request(request: &PutRequest<'a>) -> Self {
        Self {
            tenant: request.tenant,
            domain: request.domain,
            object_set: request.object_set,
            qos_tier: request.qos_tier,
            key: request.key,
            value: Some(request.value),
            value_len: request.value.len(),
            registered_source: None,
        }
    }

    fn checksum(&self) -> Option<u64> {
        self.value.map(payload_checksum)
    }
}

impl StoreClient {
    fn batch_put_reserve_needs_current_routes(&self, requests: &[BatchPutItem<'_>]) -> Result<bool> {
        if self.namespace_quota.is_some() {
            return Ok(true);
        }
        let tenants = requests
            .iter()
            .map(|request| request.tenant.unwrap_or(self.default_tenant()).to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        Ok(self
            .load_tenant_quota_policies(&tenants)?
            .values()
            .any(Option::is_some))
    }

    fn invalidate_remote_write_segments(&self, targets: &[ReplicaWriteTarget]) {
        let mut state = self.state.lock();
        for target in targets {
            if target.storage_runtime != self.lease.runtime {
                state.invalidate_remote_segment(&target.segment_name.0);
            }
        }
    }

    fn describe_write_targets(targets: &[ReplicaWriteTarget]) -> String {
        let mut parts = targets
            .iter()
            .take(8)
            .map(|target| format!("{}:{}", target.storage_runtime, target.segment_name.0))
            .collect::<Vec<_>>();
        if targets.len() > parts.len() {
            parts.push(format!("+{} more", targets.len() - parts.len()));
        }
        parts.join(",")
    }

    fn segment_name(&self) -> Result<SegmentName> {
        self.lease
            .endpoints
            .segment_name
            .clone()
            .ok_or_else(|| StoreError::InvalidState("segment_name is not configured".to_string()))
    }

    fn batch_put_reserve_retry_delay(
        retry_reason: Option<BatchPutReserveRetryReason>,
        attempt: usize,
    ) -> Duration {
        retry_reason
            .map(|reason| reason.retry_delay(attempt))
            .unwrap_or_else(|| Duration::from_millis(0))
    }
    fn write_reserved_replicas(
        &self,
        targets: &[ReplicaWriteTarget],
        reservations: &[mooncake_store_core::SegmentReservation],
        value: &[u8],
        registered_source: Option<*mut c_void>,
    ) -> Result<Vec<u64>> {
        if targets.len() != reservations.len() {
            return Err(StoreError::InvalidState(
                "targets and reservations length mismatch".to_string(),
            ));
        }
        let transport = self.transport()?;
        let mut refreshed = false;
        loop {
            let mut absolute_offsets = vec![0u64; targets.len()];
            let mut remote_requests = Vec::new();
            let mut local_writes = 0usize;

            for (index, (target, reservation)) in
                targets.iter().zip(reservations.iter()).enumerate()
            {
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
                        state.memory_ref()?.storage_address(
                            &target.segment_name,
                            reservation.offset_bytes as usize,
                        )?
                    };
                    unsafe {
                        ptr::copy_nonoverlapping(value.as_ptr(), addr.cast::<u8>(), value.len());
                    }
                    absolute_offsets[index] = addr as u64;
                    local_writes += 1;
                    continue;
                }

                self.ensure_remote_runtime_reachable(
                    &target.storage_runtime,
                    &target.segment_name.0,
                )?;
                let (handle, info) = {
                    let mut state = self.state.lock();
                    state.open_segment_with_info(
                        transport,
                        Self::transport_open_segment_name(
                            &target.segment_name,
                            target.transport_endpoint.as_deref(),
                        ),
                    )?
                };
                let target_offset = Self::storage_target_offset(
                    &target.target_chunks,
                    &target.segment_name,
                    reservation.offset_bytes,
                    value.len() as u64,
                )?;
                remote_requests.push((handle, target_offset, info));
                absolute_offsets[index] = target_offset;
            }

            debug!(
                runtime = %self.lease.runtime,
                replicas = targets.len(),
                local_writes,
                remote_writes = remote_requests.len(),
                value_bytes = value.len(),
                refreshed,
                "writing reserved replicas"
            );

            let result = if remote_requests.is_empty() {
                Ok(())
            } else {
                let remote_bytes = (remote_requests.len() * value.len()) as u64;
                let request_deadline = self.request_deadline_for_transfer(remote_bytes, 1);
                let tracker =
                    OperationTracker::new("put_remote_batch_write").input_bytes(remote_bytes);
                let result = (|| {
                    let requests = if let Some(source) = registered_source {
                        let mut requests = Vec::new();
                        for (handle, target_offset, info) in &remote_requests {
                            requests.extend(Self::target_buffer_transfer_requests(
                                Opcode::Write,
                                *handle,
                                *target_offset,
                                source,
                                value.len() as u64,
                                info,
                                transport.max_registration_bytes(),
                            )?);
                        }
                        requests
                    } else {
                        let scratch = {
                            let state = self.state.lock();
                            state.memory_ref()?.plan_scratch(&[value.len()])?
                        };
                        copy_into_region(scratch[0], value);
                        let mut requests = Vec::new();
                        for (handle, target_offset, info) in &remote_requests {
                            requests.extend(Self::target_buffer_transfer_requests(
                                Opcode::Write,
                                *handle,
                                *target_offset,
                                scratch[0].addr,
                                value.len() as u64,
                                info,
                                transport.max_registration_bytes(),
                            )?);
                        }
                        requests
                    };
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
                            eprintln!(
                                "[mooncake-store] remote put submit failed runtime={} batch_id={} transfers={} bytes={} targets={} error={}",
                                self.lease.runtime,
                                batch_id,
                                chunk.len(),
                                chunk.len() * value.len(),
                                Self::describe_write_targets(targets),
                                error
                            );
                            let _ = transport.free_batch(batch_id);
                            return Err(error);
                        }
                        let request_timeout_ms = request_deadline
                            .instant()
                            .saturating_duration_since(Instant::now())
                            .as_millis();
                        let wait_result = wait_for_batch_completion_detailed(
                            transport,
                            batch_id,
                            self.transfer_stall_timeout,
                            request_deadline.instant(),
                        );
                        let free_result = transport.free_batch(batch_id);
                        if let Err(error) = wait_result {
                            let store_error = StoreError::from(error.clone());
                            eprintln!(
                                "[mooncake-store] remote put wait failed runtime={} batch_id={} transfers={} bytes={} stall_timeout_ms={} request_timeout_ms={} mark_suspect={} targets={} error={}",
                                self.lease.runtime,
                                batch_id,
                                chunk.len(),
                                chunk.len() * value.len(),
                                self.transfer_stall_timeout.as_millis(),
                                request_timeout_ms,
                                error.marks_runtime_suspect(),
                                Self::describe_write_targets(targets),
                                store_error
                            );
                            return Err(store_error);
                        }
                        free_result?;
                    }
                    Ok(())
                })();
                tracker.finish(&result, 0);
                registry::record_transport_operation(
                    "write",
                    "storage",
                    if result.is_ok() { "ok" } else { "error" },
                );
                if result.is_ok() {
                    registry::record_transport_bytes("write", "storage", remote_bytes);
                }
                result
            };

            match result {
                Ok(()) => {
                    if local_writes != 0 {
                        record_success_metric(
                            "put_local_copy",
                            (local_writes * value.len()) as u64,
                            0,
                        );
                    }
                    return Ok(absolute_offsets);
                }
                Err(error) if !refreshed && remote_segment_cache_stale(&error) => {
                    self.invalidate_remote_write_segments(targets);
                    refreshed = true;
                    debug!(
                        runtime = %self.lease.runtime,
                        replicas = targets.len(),
                        error = %error,
                        "retrying remote write after refreshing cached remote segment handle"
                    );
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn batch_put_scoped_routed(
        &self,
        requests: &[PutRequest<'_>],
        policy: Option<&ReplicationPolicy>,
    ) -> Result<Vec<ObjectRoute>> {
        let items = requests
            .iter()
            .map(BatchPutItem::from_put_request)
            .collect::<Vec<_>>();
        self.batch_put_scoped_routed_with_route_conflicts(
            &items,
            policy,
            BatchPutRouteConflictPolicy::Strict,
        )
    }

    fn batch_put_scoped_routed_accept_existing(
        &self,
        requests: &[BatchPutItem<'_>],
        policy: Option<&ReplicationPolicy>,
    ) -> Result<Vec<ObjectRoute>> {
        self.batch_put_scoped_routed_with_route_conflicts(
            requests,
            policy,
            BatchPutRouteConflictPolicy::AcceptExistingActiveRoute,
        )
    }

    fn batch_put_scoped_routed_accept_existing_statuses(
        &self,
        requests: &[BatchPutItem<'_>],
        policy: Option<&ReplicationPolicy>,
    ) -> Vec<Result<ObjectRoute>> {
        if requests.is_empty() {
            return Vec::new();
        }
        self.batch_put_scoped_routed_accept_existing_statuses_inner(
            requests,
            policy,
        )
    }

    fn batch_put_scoped_routed_accept_existing_statuses_inner(
        &self,
        requests: &[BatchPutItem<'_>],
        policy: Option<&ReplicationPolicy>,
    ) -> Vec<Result<ObjectRoute>> {
        match self.batch_put_scoped_routed_accept_existing(
            requests,
            policy,
        ) {
            Ok(routes) if routes.len() == requests.len() => routes.into_iter().map(Ok).collect(),
            Ok(routes) => {
                let error = StoreError::InvalidState(format!(
                    "batch put status route count mismatch: got={} expected={}",
                    routes.len(),
                    requests.len()
                ));
                std::iter::repeat_with(|| Err(error.clone()))
                    .take(requests.len())
                    .collect()
            }
            Err(error) if requests.len() == 1 => vec![Err(error)],
            Err(error) => {
                debug!(
                    runtime = %self.lease.runtime,
                    items = requests.len(),
                    error = %error,
                    "splitting routed batch put failure into per-key status batches"
                );
                let mid = requests.len() / 2;
                let mut statuses = self.batch_put_scoped_routed_accept_existing_statuses_inner(
                    &requests[..mid],
                    policy,
                );
                statuses.extend(self.batch_put_scoped_routed_accept_existing_statuses_inner(
                    &requests[mid..],
                    policy,
                ));
                statuses
            }
        }
    }

    fn batch_put_scoped_routed_with_route_conflicts(
        &self,
        requests: &[BatchPutItem<'_>],
        policy: Option<&ReplicationPolicy>,
        conflict_policy: BatchPutRouteConflictPolicy,
    ) -> Result<Vec<ObjectRoute>> {
        self.ensure_local_memory()?;
        self.flush_due_reclaims()?;
        self.validate_unique_requests(requests)?;
        let transport = self.transport()?;
        let planner = self.request_placement_planner();
        let resolved_policy = self.resolve_replication_policy(policy)?;
        let reserve_needs_current_routes = self.batch_put_reserve_needs_current_routes(requests)?;
        let skip_publish_route_preload = conflict_policy
            == BatchPutRouteConflictPolicy::AcceptExistingActiveRoute
            && !reserve_needs_current_routes;
        let mut write_attempt = 0usize;

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
        let mut max_write_attempts = 1usize;
        let abort_prepared_quota = |entries: &[PreparedObjectWrite<'_>], context: &str| {
            for entry in entries {
                let _ =
                    self.abort_tenant_quota_reservation(entry.quota_reservation.as_ref(), context);
            }
        };
        let release_prepared = |entries: &[PreparedObjectWrite<'_>], context: &str| {
            for entry in entries {
                let _ = self.release_reserved_allocations(&entry.targets, &entry.reservations);
                let _ =
                    self.abort_tenant_quota_reservation(entry.quota_reservation.as_ref(), context);
            }
        };
        let (routes, prepared) = loop {
            write_attempt = write_attempt.saturating_add(1);
            self.flush_due_reclaims()?;

            let rank_tracker = OperationTracker::new("batch_put_stage_rank");
            let rank_result = planner.rank_many(self, &object_refs);
            rank_tracker.finish(&rank_result, 0);
            let plans = rank_result?;
            if write_attempt == 1 {
                let ranked_candidate_count =
                    plans.iter().map(|plan| plan.owners.len()).max().unwrap_or(0);
                max_write_attempts =
                    self.write_retry_limit(&resolved_policy, ranked_candidate_count);
            }

            struct PendingBatchReservation<'a> {
                tenant: &'a str,
                object_id: LogicalObjectId,
                qos_tier: Option<&'a str>,
                scoped_key: ObjectKey,
                current: Option<ObjectRoute>,
                value: Option<&'a [u8]>,
                value_len: usize,
                checksum: Option<u64>,
                registered_source: Option<*mut c_void>,
                quota_reservation: Option<TenantQuotaReservationRequest>,
                candidates: Vec<ReplicaPlacementCandidate>,
                next_candidate: usize,
                targets: Vec<ReplicaWriteTarget>,
                reservations: Vec<mooncake_store_core::SegmentReservation>,
            }

            let release_pending = |entries: &[PendingBatchReservation<'_>], context: &str| {
                for entry in entries {
                    let _ = self.release_reserved_allocations(&entry.targets, &entry.reservations);
                    let _ = self.abort_tenant_quota_reservation(
                        entry.quota_reservation.as_ref(),
                        context,
                    );
                }
            };

            let reserve_tracker = OperationTracker::new("batch_put_stage_reserve").input_bytes(
                requests
                    .iter()
                    .map(|request| request.value_len)
                    .sum::<usize>() as u64,
            );
            let reserve_result = (|| -> std::result::Result<
                Vec<PreparedObjectWrite<'_>>,
                BatchPutReserveStageError,
            > {
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
            )
            .map_err(BatchPutReserveStageError::terminal)?;
            add_preferred_segments(
                self,
                &resolved_policy.hint_preferred_segments,
                PreferredSegmentSource::TenantPolicy,
                true,
                &mut shared_seen,
                &mut shared_candidates,
            )
            .map_err(BatchPutReserveStageError::terminal)?;
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

            let current_routes = if reserve_needs_current_routes {
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
                        mooncake_store_core::ObjectKey::from_logical_id(&object_id)
                    })
                    .collect::<Vec<_>>();
                let current_routes_result = self.route_ops().load_routes(&object_keys);
                route_load_tracker.finish(&current_routes_result, 0);
                current_routes_result.map_err(BatchPutReserveStageError::terminal)?
            } else {
                vec![None; requests.len()]
            };

            let mut pending = Vec::with_capacity(requests.len());
            for (((request, plan), object_ref), current) in requests
                .iter()
                .zip(plans.iter())
                .zip(object_refs.iter())
                .zip(current_routes)
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
                    return Err(BatchPutReserveStageError::retryable(
                        StoreError::InvalidState(format!(
                            "no placement candidates available for tenant={} key={}",
                            tenant, request.key
                        )),
                        BatchPutReserveRetryReason::NoPlacementCandidates,
                    ));
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
                    scoped_key: mooncake_store_core::ObjectKey::from_logical_id(&LogicalObjectId::new(
                        NamespaceScope::with_defaults(
                            object_ref.tenant,
                            object_ref.domain,
                            object_ref.object_set,
                        ),
                        object_ref.key,
                    )),
                    current,
                    value: request.value,
                    value_len: request.value_len,
                    checksum: request.checksum(),
                    registered_source: request.registered_source,
                    quota_reservation: None,
                    candidates,
                    next_candidate: 0,
                    targets: Vec::with_capacity(resolved_policy.replica_count),
                    reservations: Vec::with_capacity(resolved_policy.replica_count),
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
                        entry.value_len,
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
                        return Err(BatchPutReserveStageError::terminal(error));
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
                        length_bytes: entry.value_len as u64,
                    });
                }
                if let Some(key) = exhausted_key {
                    release_pending(&pending, "batch_put_reserve_failed");
                    return Err(BatchPutReserveStageError::retryable(
                        StoreError::InvalidState(format!(
                            "not enough writable owners for key {key}"
                        )),
                        BatchPutReserveRetryReason::NotEnoughWritableOwners,
                    ));
                }

                let round_results = self
                    .reserve_storage_runtime_segments_batch(&round_requests)
                    .map_err(BatchPutReserveStageError::terminal)?;
                let mut exhausted_candidates = 0usize;
                for (((index, request), candidate), result) in round_indices
                    .into_iter()
                    .zip(round_requests)
                    .zip(round_candidates)
                    .zip(round_results)
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
                            return Err(BatchPutReserveStageError::terminal(error));
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
                    return Err(BatchPutReserveStageError::retryable(
                        StoreError::InvalidState(
                            "batch put exhausted all placement candidates".to_string(),
                        ),
                        BatchPutReserveRetryReason::ExhaustedCandidates,
                    ));
                }
            }

            let mut prepared = Vec::with_capacity(pending.len());
            for entry in pending.into_iter() {
                prepared.push(PreparedObjectWrite {
                    tenant: entry.tenant,
                    object_id: entry.object_id,
                    qos_tier: entry.qos_tier,
                    scoped_key: entry.scoped_key,
                    value: entry.value,
                    value_len: entry.value_len,
                    checksum: entry.checksum,
                    registered_source: entry.registered_source,
                    quota_reservation: entry.quota_reservation,
                    targets: entry.targets,
                    reservations: entry.reservations,
                });
            }
            Ok(prepared)
        })();
        let reserve_metrics_result = reserve_result
            .as_ref()
            .map(|prepared| prepared.len())
            .map_err(|error| error.error.clone());
        reserve_tracker.finish(&reserve_metrics_result, 0);
        let prepared = match reserve_result {
            Ok(prepared) => prepared,
            Err(reserve_error) => {
                let error = reserve_error.error;
                if write_attempt < max_write_attempts
                    && resolved_policy.required_preferred_segments.is_empty()
                    && matches!(
                        error,
                        StoreError::Transport(_)
                            | StoreError::NotFound(_)
                            | StoreError::InvalidState(_)
                    )
                {
                    let retry_delay = Self::batch_put_reserve_retry_delay(
                        reserve_error.retry_reason,
                        write_attempt,
                    );
                    if !retry_delay.is_zero() {
                        sleep(retry_delay);
                    }
                    let _ = refresh_live_client_cache(
                        self.metadata.as_ref(),
                        &self.live_client_cache,
                        "live_client_snapshot_batch_put_reserve_failure",
                    );
                    debug!(
                        runtime = %self.lease.runtime,
                        items = requests.len(),
                        attempt = write_attempt,
                        max_write_attempts,
                        error = %error,
                        "retrying batch put after transient placement reservation failure"
                    );
                    continue;
                }
                return Err(error);
            }
        };
        let current_routes = if skip_publish_route_preload {
            vec![None; prepared.len()]
        } else {
            let route_load_tracker = OperationTracker::new("batch_put_stage_load_routes")
                .attribute_u64("mooncake.item_count", prepared.len() as u64);
            let route_keys = prepared
                .iter()
                .map(|entry| entry.scoped_key.clone())
                .collect::<Vec<_>>();
            let current_routes_result = self.route_ops().load_routes(&route_keys);
            route_load_tracker.finish(&current_routes_result, 0);
            match current_routes_result {
                Ok(routes) => routes,
                Err(error) => {
                    release_prepared(&prepared, "batch_put_failed_before_publish");
                    return Err(error);
                }
            }
        };
        let write_bytes = prepared
            .iter()
            .map(|entry| entry.value_len)
            .sum::<usize>() as u64;
        let total_target_count = prepared
            .iter()
            .map(|entry| entry.targets.len())
            .sum::<usize>();
        let remote_target_count = prepared
            .iter()
            .flat_map(|entry| entry.targets.iter())
            .filter(|target| target.storage_runtime != self.lease.runtime)
            .count();
        let local_target_count = total_target_count.saturating_sub(remote_target_count);
        let write_tracker = OperationTracker::new("batch_put_stage_write")
            .attribute_u64("mooncake.item_count", prepared.len() as u64)
            .attribute_u64("mooncake.replica_count", total_target_count as u64)
            .attribute_u64("mooncake.local_target_count", local_target_count as u64)
            .attribute_u64("mooncake.remote_target_count", remote_target_count as u64)
            .input_bytes(write_bytes);
        let has_remote_targets = prepared.iter().any(|entry| {
            entry
                .targets
                .iter()
                .any(|target| target.storage_runtime != self.lease.runtime)
        });
        let write_result = {
            let mut refreshed = false;
            loop {
                let attempt_result = (|| {
                    struct RemoteBatchWrite<'a> {
                        tenant: &'a str,
                        value: Option<&'a [u8]>,
                        value_len: usize,
                        registered_source: Option<*mut c_void>,
                        requests: Vec<RemoteBatchTarget>,
                    }

                    struct RemoteBatchTarget {
                        owner: ClientRuntimeId,
                        segment_name: SegmentName,
                        segment: u64,
                        offset: u64,
                        length: u64,
                        info: SegmentInfo,
                    }

                    let mut routes = Vec::with_capacity(prepared.len());
                    let mut remote_writes = Vec::new();
                    let mut checked_runtimes = BTreeSet::new();
                    for (entry, current) in prepared
                        .iter()
                        .zip(current_routes.iter().cloned())
                    {
                        let mut route_checksum = entry.checksum;
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
                                let value = if let Some(value) = entry.value {
                                    value
                                } else if let Some(source) = entry.registered_source {
                                    unsafe {
                                        slice::from_raw_parts(source.cast::<u8>(), entry.value_len)
                                    }
                                } else {
                                    return Err(StoreError::InvalidState(format!(
                                        "tenant={} key={} batch write has no host value or registered source",
                                        entry.tenant,
                                        entry.object_id.logical_key
                                    )));
                                };
                                if route_checksum.is_none() {
                                    route_checksum = Some(payload_checksum(value));
                                }
                                let addr = {
                                    let state = self.state.lock();
                                    state.memory_ref()?.storage_address(
                                        &target.segment_name,
                                        reservation.offset_bytes as usize,
                                    )?
                                };
                                unsafe {
                                    ptr::copy_nonoverlapping(
                                        value.as_ptr(),
                                        addr.cast::<u8>(),
                                        entry.value_len,
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
                                    state.open_segment_with_info(
                                        transport,
                                        Self::transport_open_segment_name(
                                            &target.segment_name,
                                            target.transport_endpoint.as_deref(),
                                        ),
                                    )?
                                };
                                let target_offset = Self::storage_target_offset(
                                    &target.target_chunks,
                                    &target.segment_name,
                                    reservation.offset_bytes,
                                    entry.value_len as u64,
                                )?;
                                remote_requests.push(RemoteBatchTarget {
                                    owner: target.storage_runtime.clone(),
                                    segment_name: target.segment_name.clone(),
                                    segment: handle,
                                    offset: target_offset,
                                    length: entry.value_len as u64,
                                    info,
                                });
                                target_offset
                            };
                            replicas.push(ReplicaRoute {
                                owner: target.storage_runtime.clone(),
                                segment_name: target.segment_name.clone(),
                                offset: Some(offset),
                                segment_offset: reservation.offset_bytes,
                                length: entry.value_len as u64,
                                checksum: route_checksum,
                                tier: ReplicaTier::Dram,
                                priority: priority as u16,
                            });
                        }
                        if !remote_requests.is_empty() {
                            remote_writes.push(RemoteBatchWrite {
                                tenant: entry.value_tenant(),
                                value: entry.value,
                                value_len: entry.value_len,
                                registered_source: entry.registered_source,
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
                            cold_backing: None,
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
                    if write.registered_source.is_none() {
                        scratch_lengths.push(write.value_len);
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
                    }
                    selected.push(*index);
                    remote_bytes = remote_bytes.saturating_add(write_remote_bytes);
                }
                let scratch = if scratch_lengths.is_empty() {
                    None
                } else {
                    let state = self.state.lock();
                    Some(state.memory_ref()?.plan_scratch(&scratch_lengths)?)
                };
                let mut transfer_requests = Vec::new();
                let mut scratch_position = 0usize;
                for index in &selected {
                    let write = &remote_writes[*index];
                    if let Some(source) = write.registered_source {
                        for request in &write.requests {
                            transfer_requests.extend(Self::target_buffer_transfer_requests(
                                Opcode::Write,
                                request.segment,
                                request.offset,
                                source,
                                request.length,
                                &request.info,
                                transport.max_registration_bytes(),
                            )?);
                        }
                    } else {
                        let allocation = scratch
                            .as_ref()
                            .expect("scratch reservation should exist for staged writes")
                            [scratch_position];
                        let value = write.value.ok_or_else(|| {
                            StoreError::Unsupported(format!(
                                "tenant={tenant} non-host-readable registered source requires direct registered transfer"
                            ))
                        })?;
                        copy_into_region(allocation, value);
                        for request in &write.requests {
                            transfer_requests.extend(Self::target_buffer_transfer_requests(
                                Opcode::Write,
                                request.segment,
                                request.offset,
                                allocation.addr,
                                request.length,
                                &request.info,
                                transport.max_registration_bytes(),
                            )?);
                        }
                        scratch_position += 1;
                    }
                }
                let mut target_parts = selected
                    .iter()
                    .flat_map(|index| {
                        remote_writes[*index].requests.iter().map(|request| {
                            format!("{}:{}", request.owner, request.segment_name.0)
                        })
                    })
                    .take(8)
                    .collect::<Vec<_>>();
                let target_count = selected
                    .iter()
                    .map(|index| remote_writes[*index].requests.len())
                    .sum::<usize>();
                if target_count > target_parts.len() {
                    target_parts.push(format!("+{} more", target_count - target_parts.len()));
                }
                let target_summary = target_parts.join(",");
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
                    eprintln!(
                        "[mooncake-store] remote batch put submit failed runtime={} batch_id={} tenant={} items={} transfers={} bytes={} targets={} error={}",
                        self.lease.runtime,
                        batch_id,
                        tenant,
                        selected.len(),
                        transfer_requests.len(),
                        remote_bytes,
                        target_summary,
                        error
                    );
                    let _ = transport.free_batch(batch_id);
                    return Err(error);
                }
                let request_timeout_ms = request_deadline
                    .instant()
                    .saturating_duration_since(Instant::now())
                    .as_millis();
                let wait_result = wait_for_batch_completion_detailed(
                    transport,
                    batch_id,
                    self.transfer_stall_timeout,
                    request_deadline.instant(),
                );
                let free_result = transport.free_batch(batch_id);
                if let Err(error) = wait_result {
                    let store_error = StoreError::from(error.clone());
                    eprintln!(
                        "[mooncake-store] remote batch put wait failed runtime={} batch_id={} tenant={} items={} transfers={} bytes={} stall_timeout_ms={} request_timeout_ms={} mark_suspect={} targets={} error={}",
                        self.lease.runtime,
                        batch_id,
                        tenant,
                        selected.len(),
                        transfer_requests.len(),
                        remote_bytes,
                        self.transfer_stall_timeout.as_millis(),
                        request_timeout_ms,
                        error.marks_runtime_suspect(),
                        target_summary,
                        store_error
                    );
                    return Err(store_error);
                }
                free_result?;
                registry::record_transport_bytes("write", "storage", remote_bytes);
                cursor += selected.len();
            }
                    Ok(routes)
                })();
                match attempt_result {
                    Ok(routes) => break Ok(routes),
                    Err(error) if !refreshed && remote_segment_cache_stale(&error) => {
                        for entry in &prepared {
                            self.invalidate_remote_write_segments(&entry.targets);
                        }
                        refreshed = true;
                        debug!(
                            runtime = %self.lease.runtime,
                            items = prepared.len(),
                            error = %error,
                            "retrying batch put after refreshing cached remote segment handles"
                        );
                    }
                    Err(error) => break Err(error),
                }
                }
            };
            write_tracker.finish(&write_result, 0);
            if has_remote_targets {
                registry::record_transport_operation(
                    "write",
                    "storage",
                    if write_result.is_ok() { "ok" } else { "error" },
                );
            }
            match write_result {
                Ok(routes) => break (routes, prepared),
                Err(error) => {
                    let failed_targets = prepared
                        .iter()
                        .flat_map(|entry| entry.targets.iter().cloned())
                        .collect::<Vec<_>>();
                    self.note_remote_write_failure(
                        &failed_targets,
                        &error,
                        "remote_batch_write_failed",
                    );
                    if write_attempt < max_write_attempts
                        && resolved_policy.required_preferred_segments.is_empty()
                        && matches!(
                            error,
                            StoreError::Transport(_)
                                | StoreError::NotFound(_)
                                | StoreError::InvalidState(_)
                        )
                    {
                        debug!(
                            runtime = %self.lease.runtime,
                            items = prepared.len(),
                            attempt = write_attempt,
                            max_write_attempts,
                            error = %error,
                            "retrying batch put after transient replica write failure"
                        );
                        continue;
                    }
                    return Err(error);
                }
            };
        };

        let (mut routes, prepared) = (routes, prepared);
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
        let cas_results_result = self.route_ops().publish_routes(&cas_requests);
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
        let mut cas_results = match cas_results_result {
            Ok(results) => results,
            Err(error) => {
                release_prepared(&prepared, "batch_put_failed_before_publish");
                return Err(error);
            }
        };
        let mut floor_retry_requests = Vec::new();
        let mut floor_retry_indices = Vec::new();
        for (index, result) in cas_results.iter().enumerate() {
            let pending = &routes[index];
            if let Ok(cas) = result {
                if !cas.applied && pending.expected_version.is_none() && cas.current.is_none() {
                    let retry_version = cas
                        .version_floor
                        .map(|floor| floor.next())
                        .unwrap_or_else(|| {
                            let keys = &[routes[index].key.clone()];
                            let current = vec![None; 1];
                            next_route_versions(&current, &self.route_ops(), keys)[0]
                        });
                    if retry_version > routes[index].route.version {
                        routes[index].route.version = retry_version;
                        floor_retry_indices.push(index);
                        floor_retry_requests.push(RouteCasRequest {
                            key: routes[index].key.clone(),
                            expected: None,
                            next: Some(routes[index].route.clone()),
                        });
                    }
                }
            }
        }
        if !floor_retry_requests.is_empty() {
            debug!(
                runtime = %self.lease.runtime,
                items = floor_retry_requests.len(),
                "floor retry: using version_floor from CAS response for version correction"
            );
            let retry_tracker =
                OperationTracker::new("batch_put_stage_route_cas_floor_retry");
            let retry_result = self.route_ops().publish_routes(&floor_retry_requests);
            retry_tracker.finish(&retry_result, 0);
            let retry_results = match retry_result {
                Ok(results) => results,
                Err(error) => {
                    release_prepared(&prepared, "batch_put_floor_retry_failed");
                    return Err(error);
                }
            };
            for (index, result) in floor_retry_indices.into_iter().zip(retry_results) {
                cas_results[index] = result;
            }
        }
        let route_namespace = self.metadata.route_namespace();
        let readable_filter_active =
            mooncake_store_route::is_readable_filter_active(&route_namespace);
        let mut stale_conflict_count = 0usize;
        let stale_overwrite_indices = cas_results
            .iter()
            .enumerate()
            .filter_map(|(index, result)| match result {
                Ok(cas) if !cas.applied && routes[index].expected_version.is_none() => {
                    cas.current.as_ref().and_then(|current| {
                        stale_conflict_count += 1;
                        if !mooncake_store_route::route_has_readable_replicas(
                            &route_namespace,
                            current,
                        ) {
                            Some((index, current.version))
                        } else {
                            None
                        }
                    })
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if stale_conflict_count > 0 && stale_overwrite_indices.is_empty() {
            warn!(
                runtime = %self.lease.runtime,
                stale_conflict_count,
                readable_filter_active,
                "stale route conflicts detected but all routes appear readable; stale overwrite skipped"
            );
        }
        if !stale_overwrite_indices.is_empty() {
            let mut retry_requests = Vec::new();
            let mut retry_indices = Vec::new();
            for (index, stale_version) in &stale_overwrite_indices {
                routes[*index].route.version = stale_version.next();
                routes[*index].expected_version = Some(*stale_version);
                retry_indices.push(*index);
                retry_requests.push(RouteCasRequest {
                    key: routes[*index].key.clone(),
                    expected: Some(*stale_version),
                    next: Some(routes[*index].route.clone()),
                });
            }
            let retry_tracker =
                OperationTracker::new("batch_put_stage_route_cas_stale_overwrite");
            let retry_result = self.route_ops().publish_routes(&retry_requests);
            retry_tracker.finish(&retry_result, 0);
            match retry_result {
                Ok(results) => {
                    let mut fallback_indices = Vec::new();
                    let mut fallback_floors = Vec::new();
                    for (index, result) in retry_indices.into_iter().zip(results) {
                        match &result {
                            Ok(cas) if !cas.applied && cas.current.is_none() => {
                                routes[index].expected_version = None;
                                fallback_indices.push(index);
                                fallback_floors.push(cas.version_floor);
                            }
                            _ => {
                                cas_results[index] = result;
                            }
                        }
                    }
                    if !fallback_indices.is_empty() {
                        let fallback_requests = fallback_indices
                            .iter()
                            .zip(fallback_floors)
                            .map(|(index, floor)| {
                                let version = floor
                                    .map(|f| f.next())
                                    .unwrap_or_else(|| {
                                        let keys = &[routes[*index].key.clone()];
                                        let current = vec![None; 1];
                                        next_route_versions(&current, &self.route_ops(), keys)[0]
                                    });
                                routes[*index].route.version = version;
                                RouteCasRequest {
                                    key: routes[*index].key.clone(),
                                    expected: None,
                                    next: Some(routes[*index].route.clone()),
                                }
                            })
                            .collect::<Vec<_>>();
                        let fallback_tracker = OperationTracker::new(
                            "batch_put_stage_route_cas_stale_overwrite_fallback",
                        );
                        let fallback_result =
                            self.route_ops().publish_routes(&fallback_requests);
                        fallback_tracker.finish(&fallback_result, 0);
                        match fallback_result {
                            Ok(fallback_results) => {
                                for (index, result) in
                                    fallback_indices.into_iter().zip(fallback_results)
                                {
                                    cas_results[index] = result;
                                }
                            }
                            Err(error) => {
                                release_prepared(
                                    &prepared,
                                    "batch_put_stale_overwrite_fallback_failed",
                                );
                                return Err(error);
                            }
                        }
                    }
                }
                Err(error) => {
                    release_prepared(&prepared, "batch_put_stale_overwrite_failed");
                    return Err(error);
                }
            }
        }

        let mut published = Vec::with_capacity(routes.len());
        let mut first_error = None;
        for ((index, pending), cas_result) in
            routes.into_iter().enumerate().zip(cas_results)
        {
            match cas_result {
                Ok(cas) if cas.applied => {
                    let route = match self.finalize_published_route(
                        pending.quota_reservation.as_ref(),
                        &pending.route,
                        prepared[index].value_len,
                        "batch_put",
                    ) {
                        Ok(Some(updated)) => updated,
                        Ok(None) => pending.route,
                        Err(error) => {
                            first_error.get_or_insert(error);
                            continue;
                        }
                    };
                    self.storage_owner.track_route(&route);
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
                    published.push(route);
                }
                Ok(cas) => {
                    let _ = self.release_reserved_allocations(
                        &prepared[index].targets,
                        &prepared[index].reservations,
                    );
                    let _ = self.abort_tenant_quota_reservation(
                        prepared[index].quota_reservation.as_ref(),
                        "batch_put_route_compare_and_swap_conflict",
                    );
                    let accepted_current = match conflict_policy {
                        BatchPutRouteConflictPolicy::AcceptExistingActiveRoute => {
                            if let Some(current) = cas.current.filter(|route| {
                                route.state == RouteState::Active
                                    && mooncake_store_route::route_has_readable_replicas(
                                        &route_namespace,
                                        route,
                                    )
                            }) {
                                Some(current)
                            } else {
                                match self
                                    .query_routes_by_object_keys_bounded(std::slice::from_ref(
                                        &pending.key,
                                    ))
                                    .and_then(|routes| {
                                        Self::expect_exactly_one(routes, "route recheck")
                                    }) {
                                    Ok(Some(current))
                                        if current.state == RouteState::Active
                                            && mooncake_store_route::route_has_readable_replicas(
                                                &route_namespace,
                                                &current,
                                            ) => Some(current),
                                    Ok(_) => None,
                                    Err(error) => {
                                        first_error.get_or_insert(error);
                                        None
                                    }
                                }
                            }
                        }
                        _ => None,
                    };
                    match accepted_current {
                        Some(current) => {
                            self.storage_owner.track_route(&current);
                            debug!(
                                runtime = %self.lease.runtime,
                                key = %pending.key.0,
                                route_version = current.version.0,
                                "batch put route conflict accepted existing active route"
                            );
                            published.push(current);
                        }
                        None => {
                            first_error.get_or_insert_with(|| {
                                StoreError::Conflict(format!(
                                    "route update lost race for key {}",
                                    pending.key.0
                                ))
                            });
                        }
                    }
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

    fn validate_unique_requests(&self, requests: &[BatchPutItem<'_>]) -> Result<()> {
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
