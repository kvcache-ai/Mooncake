impl StoreClient {
    pub fn runtime_id(&self) -> &ClientRuntimeId {
        &self.lease.runtime
    }

    pub fn find_hot_upgrade_successor(&self) -> Result<Option<ClientLease>> {
        Ok(self
            .compatible_live_clients(true)?
            .into_iter()
            .filter(|lease| lease.runtime.stable_id == self.lease.runtime.stable_id)
            .filter(|lease| lease.runtime.epoch > self.lease.runtime.epoch)
            .filter(|lease| {
                matches!(
                    lease.state,
                    ClientLifecycleState::Standby | ClientLifecycleState::Active
                )
            })
            .max_by_key(|lease| lease.runtime.epoch))
    }

    pub fn activate_if_targeted_handoff(&mut self) -> Result<Option<HandoffPlan>> {
        if self.lease.state != ClientLifecycleState::Standby {
            return Ok(None);
        }
        let Some(handoff) = self.metadata.get_handoff(&self.lease.runtime.stable_id)? else {
            return Ok(None);
        };
        if handoff.to != self.lease.runtime {
            return Ok(None);
        }
        if !matches!(
            handoff.kind,
            HandoffKind::HotUpgrade | HandoffKind::HotStandbyPromotion
        ) {
            return Ok(None);
        }
        if handoff
            .deadline_ms
            .is_some_and(|deadline_ms| Self::current_time_ms() > deadline_ms)
        {
            return Ok(None);
        }
        self.activate()?;
        Ok(Some(handoff))
    }

    pub fn runtime_state(&self, runtime: &ClientRuntimeId) -> Result<Option<ClientLifecycleState>> {
        Ok(self
            .compatible_live_clients(true)?
            .into_iter()
            .find(|lease| lease.runtime == *runtime)
            .map(|lease| lease.state))
    }

    pub fn evacuate_owned_replicas_to_runtime(
        &mut self,
        successor: &ClientRuntimeId,
    ) -> Result<usize> {
        if !self.has_active_compatible_runtime(successor, false)?
            && !self.has_active_compatible_runtime(successor, true)?
        {
            return Err(StoreError::InvalidState(format!(
                "hot-upgrade successor {} is not active",
                successor
            )));
        }

        let _span = info_span!(
            "store.evacuate_owned_replicas_to_runtime",
            runtime = %self.lease.runtime,
            successor = %successor
        )
        .entered();
        let tracker = OperationTracker::new("evacuate_owned_replicas_to_runtime");
        let result = (|| {
            self.ensure_local_memory()?;
            if self.lease.state != ClientLifecycleState::Draining {
                self.enter_draining()?;
            }
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

            let routes = self.collect_routes_by_replica_owner(&self.lease.runtime)?;
            let helper_writer = self.build_hot_upgrade_helper_writer()?;
            let mut migrated = 0usize;
            for route in routes {
                let migrated_route = match helper_writer.as_ref() {
                    Some(writer) => {
                        self.migrate_owned_route_to_runtime_via_writer(writer, successor, &route)?
                    }
                    None => self.migrate_owned_route_to_runtime(successor, &route)?,
                };
                if migrated_route {
                    migrated = migrated.saturating_add(1);
                }
            }
            drop(helper_writer);

            self.flush_all_reclaims()?;
            let live_allocations = self.current_owned_allocations()?;
            let _ = self.release_stale_local_allocations(&live_allocations)?;
            self.flush_all_reclaims()?;
            self.retire_empty_draining_segments()?;

            let remaining = self
                .list_segments()?
                .into_iter()
                .filter(|segment| segment.used_bytes != 0)
                .collect::<Vec<_>>();
            if !remaining.is_empty() {
                return Err(StoreError::InvalidState(format!(
                    "hot-upgrade evacuation still has live bytes on local segments: {}",
                    remaining
                        .iter()
                        .map(|segment| format!("{}:{}", segment.segment_name.0, segment.used_bytes))
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            Ok(migrated)
        })();
        tracker.finish(&result, 0);
        result
    }

    pub fn evacuate_owned_replicas_via(&mut self, writer: &StoreClient) -> Result<usize> {
        let _span = info_span!(
            "store.evacuate_owned_replicas_via",
            runtime = %self.lease.runtime,
            writer = %writer.lease.runtime
        )
        .entered();
        let tracker = OperationTracker::new("evacuate_owned_replicas_via");
        let result = (|| {
            self.ensure_local_memory()?;
            if self.lease.state != ClientLifecycleState::Draining {
                self.enter_draining()?;
            }
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

            let routes = self.collect_routes_by_replica_owner(&self.lease.runtime)?;
            let mut migrated = 0usize;
            for route in routes {
                if self.migrate_owned_route_via_writer(writer, &route)? {
                    migrated = migrated.saturating_add(1);
                }
            }

            self.flush_all_reclaims()?;
            let live_allocations = self.current_owned_allocations()?;
            let _ = self.release_stale_local_allocations(&live_allocations)?;
            self.flush_all_reclaims()?;
            self.retire_empty_draining_segments()?;

            let remaining = self
                .list_segments()?
                .into_iter()
                .filter(|segment| segment.used_bytes != 0)
                .collect::<Vec<_>>();
            if !remaining.is_empty() {
                return Err(StoreError::InvalidState(format!(
                    "client shrink still has live bytes on local segments: {}",
                    remaining
                        .iter()
                        .map(|segment| format!("{}:{}", segment.segment_name.0, segment.used_bytes))
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            Ok(migrated)
        })();
        tracker.finish(&result, 0);
        result
    }

    pub fn lease(&self) -> &ClientLease {
        &self.lease
    }

    pub fn default_tenant(&self) -> &str {
        &self.default_tenant
    }

    fn build_hot_upgrade_helper_writer(&self) -> Result<Option<StoreClient>> {
        let Some(factory) = self.transport_factory.clone() else {
            return Ok(None);
        };

        let helper_stable_id = format!(
            "{}-hot-upgrade-helper-{}",
            self.lease.runtime.stable_id.0,
            Self::current_time_ms()
        );
        let helper_segment_name = format!(
            "{}-segment-{}",
            helper_stable_id.replace(':', "-"),
            self.lease.runtime.epoch.0
        );
        let helper_transport = factory.create(&helper_segment_name)?;
        let helper_expiry_ms = self
            .lease
            .expires_at_ms
            .max(Self::current_time_ms().saturating_add(30_000));

        let mut builder = StoreClientBuilder::new(self.metadata.clone(), helper_stable_id)
            .epoch(self.lease.runtime.epoch)
            .state(ClientLifecycleState::Active)
            .tenant(self.default_tenant.clone())
            .compatibility(self.lease.compatibility.clone())
            .local_memory(self.local_memory.clone())
            .transport(helper_transport)
            .transport_factory(factory)
            .route_control(self.route_control);

        let mut labels = self.lease.endpoints.labels.clone();
        labels.remove(control_address_label());
        labels.insert("storage".to_string(), "false".to_string());
        labels.insert("route".to_string(), "false".to_string());
        for (key, value) in labels {
            builder = builder.label(key, value);
        }

        if let WriteMode::Routed {
            planner,
            replica_count,
        } = &self.write_mode
        {
            builder = builder.routed_writes(planner.clone(), *replica_count);
        }

        let helper = builder.build(helper_expiry_ms)?;
        helper.register_local_memory()?;
        Ok(Some(helper))
    }

    fn default_replica_count(&self) -> usize {
        match &self.write_mode {
            WriteMode::LocalOnly => 1,
            WriteMode::Routed { replica_count, .. } => *replica_count,
        }
    }

    fn request_placement_planner(&self) -> PlacementPlanner {
        match &self.write_mode {
            WriteMode::LocalOnly => {
                PlacementPlanner::new(self.metadata.clone()).require_label("storage", "true")
            }
            WriteMode::Routed { planner, .. } => planner.clone(),
        }
    }

    fn live_clients_snapshot(&self, force_refresh: bool) -> Result<Vec<ClientLease>> {
        if force_refresh {
            return refresh_live_client_cache(
                self.metadata.as_ref(),
                &self.live_client_cache,
                "live_client_snapshot_force_refresh",
            );
        }
        cached_live_client_snapshot(&self.live_client_cache)
    }

    fn compatible_live_clients(&self, force_refresh: bool) -> Result<Vec<ClientLease>> {
        Ok(self
            .live_clients_snapshot(force_refresh)?
            .into_iter()
            .filter(|lease| compatibility_matches(&self.lease, lease))
            .collect())
    }

    fn available_compatible_live_clients(&self, force_refresh: bool) -> Result<Vec<ClientLease>> {
        let leases = self.compatible_live_clients(force_refresh)?;
        let mut suspect_cache = self.suspect_runtime_cache.lock();
        suspect_cache.reconcile_with_leases(&leases);
        Ok(leases
            .into_iter()
            .filter(|lease| !suspect_cache.contains(&lease.runtime))
            .collect())
    }

    pub(crate) fn placement_candidates_snapshot(
        &self,
        scope_label_key: &str,
        required_labels: &BTreeMap<String, String>,
        force_refresh: bool,
    ) -> Result<Vec<ClientLease>> {
        let scope = self.lease.endpoints.labels.get(scope_label_key).cloned();
        let mut candidates = self
            .available_compatible_live_clients(force_refresh)?
            .into_iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
            .filter(|lease| {
                scope.as_ref().is_none_or(|scope| {
                    lease
                        .endpoints
                        .labels
                        .get(scope_label_key)
                        .is_some_and(|value| value == scope)
                })
            })
            .filter(|lease| {
                required_labels.iter().all(|(key, value)| {
                    lease
                        .endpoints
                        .labels
                        .get(key)
                        .is_some_and(|candidate| candidate == value)
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.runtime.cmp(&right.runtime));
        Ok(candidates)
    }

    fn current_time_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_millis() as u64
    }

    fn mark_runtime_suspect(&self, runtime: &ClientRuntimeId, context: &'static str) {
        if *runtime == self.lease.runtime {
            return;
        }
        let deadline = Instant::now() + DEFAULT_SUSPECT_RUNTIME_TTL;
        self.suspect_runtime_cache
            .lock()
            .mark(runtime.clone(), deadline);
        debug!(
            runtime = %self.lease.runtime,
            suspect_runtime = %runtime,
            context,
            ttl_ms = DEFAULT_SUSPECT_RUNTIME_TTL.as_millis() as u64,
            "marked runtime as temporarily suspect after remote failure"
        );
    }

    fn readable_runtime_set(&self, force_refresh: bool) -> Result<BTreeSet<ClientRuntimeId>> {
        Ok(self
            .available_compatible_live_clients(force_refresh)?
            .into_iter()
            .filter(|lease| {
                matches!(
                    lease.state,
                    ClientLifecycleState::Active | ClientLifecycleState::Draining
                )
            })
            .map(|lease| lease.runtime)
            .collect())
    }

    fn lookup_runtime_lease_once(
        &self,
        runtime: &ClientRuntimeId,
        force_refresh: bool,
    ) -> Result<ClientLease> {
        self.available_compatible_live_clients(force_refresh)?
            .into_iter()
            .find(|lease| lease.runtime == *runtime)
            .ok_or_else(|| StoreError::NotFound(format!("runtime {} is not available", runtime)))
    }

    fn lookup_runtime_lease(&self, runtime: &ClientRuntimeId) -> Result<ClientLease> {
        self.lookup_runtime_lease_once(runtime, false)
    }

    fn lookup_runtime_leases_once(
        &self,
        wanted: &BTreeSet<ClientRuntimeId>,
        force_refresh: bool,
    ) -> Result<BTreeMap<ClientRuntimeId, ClientLease>> {
        let mut leases = BTreeMap::new();
        for lease in self.available_compatible_live_clients(force_refresh)? {
            if !wanted.contains(&lease.runtime) {
                continue;
            }
            leases.insert(lease.runtime.clone(), lease);
        }
        for runtime in wanted {
            if !leases.contains_key(runtime) {
                return Err(StoreError::NotFound(format!(
                    "runtime {} is not available",
                    runtime
                )));
            }
        }
        Ok(leases)
    }

    fn lookup_runtime_leases(
        &self,
        runtimes: impl IntoIterator<Item = ClientRuntimeId>,
    ) -> Result<BTreeMap<ClientRuntimeId, ClientLease>> {
        let wanted = runtimes
            .into_iter()
            .filter(|runtime| *runtime != self.lease.runtime)
            .collect::<BTreeSet<_>>();
        if wanted.is_empty() {
            return Ok(BTreeMap::new());
        }
        self.lookup_runtime_leases_once(&wanted, false)
    }

    fn resolve_preferred_storage_owners_once(
        &self,
        selectors: &[String],
        force_refresh: bool,
    ) -> Result<Vec<ClientRuntimeId>> {
        let compatible = self.available_compatible_live_clients(force_refresh)?;
        let mut resolved = Vec::with_capacity(selectors.len());
        let mut seen = BTreeSet::new();
        for selector in selectors {
            let matched = if selector.contains(':') {
                compatible
                    .iter()
                    .find(|lease| lease.runtime.storage_key() == *selector)
            } else {
                compatible
                    .iter()
                    .filter(|lease| lease.runtime.stable_id.0 == *selector)
                    .max_by_key(|lease| lease.runtime.epoch)
            };
            let lease = matched.ok_or_else(|| {
                StoreError::NotFound(format!(
                    "preferred storage owner {} is not available",
                    selector
                ))
            })?;
            if seen.insert(lease.runtime.clone()) {
                resolved.push(lease.runtime.clone());
            }
        }
        Ok(resolved)
    }

    fn has_active_compatible_runtime(
        &self,
        runtime: &ClientRuntimeId,
        force_refresh: bool,
    ) -> Result<bool> {
        Ok(self
            .live_clients_snapshot(force_refresh)?
            .into_iter()
            .any(|lease| {
                lease.runtime == *runtime
                    && lease.state == ClientLifecycleState::Active
                    && compatibility_matches(&self.lease, &lease)
            }))
    }

    fn reserve_segment_allocation_via_metadata(
        &self,
        owner: &ClientRuntimeId,
        segment_name: Option<&SegmentName>,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        match segment_name {
            Some(segment_name) => self
                .metadata
                .reserve_segment(owner, segment_name, length_bytes),
            None => {
                let mut segments = self.metadata.list_segments(Some(owner))?;
                segments.retain(|segment| segment.state == SegmentLifecycleState::Active);
                segments.sort_by(|left, right| {
                    let left_remaining = left.capacity_bytes.saturating_sub(left.used_bytes);
                    let right_remaining = right.capacity_bytes.saturating_sub(right.used_bytes);
                    right_remaining
                        .cmp(&left_remaining)
                        .then_with(|| left.segment_name.cmp(&right.segment_name))
                });
                let mut last_capacity_error = None;
                for segment in segments {
                    match self
                        .metadata
                        .reserve_segment(owner, &segment.segment_name, length_bytes)
                    {
                        Ok(reservation) => return Ok(reservation),
                        Err(StoreError::Allocator(message)) => {
                            last_capacity_error = Some(StoreError::Allocator(message));
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(last_capacity_error.unwrap_or_else(|| {
                    StoreError::Allocator(format!(
                        "no writable active segment available for {}",
                        owner
                    ))
                }))
            }
        }
    }

    fn reserve_segment_allocation(
        &self,
        owner: &ClientRuntimeId,
        segment_name: Option<&SegmentName>,
        length_bytes: u64,
        require_local_memory: bool,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if *owner == self.lease.runtime {
            if require_local_memory {
                let state = self.state.lock();
                if let Some(segment_name) = segment_name {
                    if !state.memory_ref()?.has_storage_segment(segment_name) {
                        return Err(StoreError::NotFound(format!(
                            "local segment {} not found",
                            segment_name.0
                        )));
                    }
                }
            }
            let allow_local_eviction = self
                .lease
                .endpoints
                .labels
                .get("storage")
                .is_some_and(|value| value == "true");
            if !allow_local_eviction {
                return match segment_name {
                    Some(segment_name) => self
                        .allocator
                        .lock()
                        .reserve_specific(owner, segment_name, length_bytes),
                    None => self.allocator.lock().reserve_any(owner, length_bytes),
                };
            }
            let mut last_error = None;
            for _ in 0..=32usize {
                let reservation = match segment_name {
                    Some(segment_name) => self
                        .allocator
                        .lock()
                        .reserve_specific(owner, segment_name, length_bytes),
                    None => self.allocator.lock().reserve_any(owner, length_bytes),
                };
                match reservation {
                    Ok(reservation) => return Ok(reservation),
                    Err(StoreError::Allocator(message)) => {
                        last_error = Some(message);
                    }
                    Err(error) => return Err(error),
                }
                if !self.storage_owner.evict_one(segment_name)? {
                    break;
                }
            }
            return Err(StoreError::Allocator(last_error.unwrap_or_else(|| {
                format!("no writable active segment available for {}", owner)
            })));
        }

        let owner_lease = self.lookup_runtime_lease(owner)?;
        let rpc_result = match segment_name {
            Some(segment_name) => self.control_client.reserve_specific(
                &owner_lease,
                owner,
                segment_name,
                length_bytes,
            ),
            None => self
                .control_client
                .reserve_any(&owner_lease, owner, length_bytes),
        };
        match rpc_result {
            Ok(reservation) => Ok(reservation),
            Err(error) if should_fallback_to_metadata_allocator(&error) => {
                debug!(
                    owner = %owner,
                    segment = segment_name.map(|segment| segment.0.as_str()).unwrap_or("*"),
                    error = %error,
                    "allocator rpc failed; falling back to metadata allocator"
                );
                self.reserve_segment_allocation_via_metadata(owner, segment_name, length_bytes)
            }
            Err(error) => {
                self.mark_runtime_suspect(owner, "allocator_rpc_failed");
                Err(error)
            }
        }
    }

    fn resolve_replication_policy(
        &self,
        policy: Option<&ReplicationPolicy>,
    ) -> Result<ResolvedReplicationPolicy> {
        let policy = policy.cloned().unwrap_or_default();
        let replica_count = policy
            .replica_count
            .unwrap_or_else(|| self.default_replica_count());
        if replica_count == 0 {
            return Err(StoreError::InvalidState(
                "replica_count must be greater than zero".to_string(),
            ));
        }

        let preferred_segments = policy.preferred_segments.clone();
        let mut seen = BTreeSet::new();
        for segment in &preferred_segments {
            if !seen.insert(segment.clone()) {
                return Err(StoreError::Conflict(format!(
                    "duplicate preferred segment {}",
                    segment.0
                )));
            }
        }

        Ok(ResolvedReplicationPolicy {
            replica_count,
            preferred_segments,
            preferred_storage_runtimes: self
                .resolve_preferred_storage_owners(&policy.preferred_storage_owners)?,
            with_soft_pin: policy.with_soft_pin,
            prefer_local: policy.prefer_local || policy.prefer_alloc_in_same_node,
        })
    }

    fn resolve_preferred_storage_owners(
        &self,
        selectors: &[String],
    ) -> Result<Vec<ClientRuntimeId>> {
        if selectors.is_empty() {
            return Ok(Vec::new());
        }
        self.resolve_preferred_storage_owners_once(selectors, false)
    }

    fn has_active_local_storage(&self) -> bool {
        self.allocator
            .lock()
            .announcements()
            .into_iter()
            .any(|segment| {
                segment.owner == self.lease.runtime
                    && segment.state == SegmentLifecycleState::Active
            })
    }

    fn should_skip_candidate(&self, error: &StoreError, soft: bool) -> bool {
        soft && matches!(
            error,
            StoreError::Allocator(_) | StoreError::NotFound(_) | StoreError::InvalidState(_)
        )
    }

    fn reserve_candidate(
        &self,
        candidate: &ReplicaPlacementTarget,
        length_bytes: usize,
    ) -> Result<(ReplicaWriteTarget, mooncake_store_core::SegmentReservation)> {
        let storage_runtime = candidate.storage_runtime();
        let is_local = storage_runtime == &self.lease.runtime;
        match candidate {
            ReplicaPlacementTarget::StorageRuntime(storage_runtime) => {
                self.reserve_storage_runtime_segment(storage_runtime, length_bytes, is_local)
            }
            ReplicaPlacementTarget::Segment {
                storage_runtime,
                segment_name,
            } => {
                self.reserve_specific_segment(storage_runtime, segment_name, length_bytes, is_local)
            }
        }
    }

    fn reserve_replica_targets(
        &self,
        tenant: &str,
        key: &str,
        length_bytes: usize,
        policy: &ResolvedReplicationPolicy,
    ) -> Result<(
        Vec<ReplicaWriteTarget>,
        Vec<mooncake_store_core::SegmentReservation>,
    )> {
        let mut targets = Vec::with_capacity(policy.replica_count);
        let mut reservations = Vec::with_capacity(policy.replica_count);
        let mut excluded = BTreeSet::new();
        let rollback =
            |this: &Self,
             targets: &[ReplicaWriteTarget],
             reservations: &[mooncake_store_core::SegmentReservation]| {
                let _ = this.release_reserved_allocations(targets, reservations);
            };

        for segment in &policy.preferred_segments {
            match self.lookup_preferred_segment(segment) {
                Ok(preferred) => {
                    let candidate = ReplicaPlacementCandidate {
                        target: ReplicaPlacementTarget::Segment {
                            storage_runtime: preferred.owner,
                            segment_name: preferred.segment_name,
                        },
                        soft: policy.with_soft_pin,
                    };
                    let storage_runtime = candidate.target.storage_runtime().clone();
                    if excluded.contains(&storage_runtime) {
                        continue;
                    }
                    match self.reserve_candidate(&candidate.target, length_bytes) {
                        Ok((target, reservation)) => {
                            excluded.insert(storage_runtime);
                            targets.push(target);
                            reservations.push(reservation);
                            if targets.len() == policy.replica_count {
                                return Ok((targets, reservations));
                            }
                        }
                        Err(error) if self.should_skip_candidate(&error, candidate.soft) => {
                            debug!(
                                tenant,
                                key,
                                segment = %segment.0,
                                error = %error,
                                "skipping preferred segment after reservation failure"
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) if self.should_skip_candidate(&error, policy.with_soft_pin) => {
                    debug!(
                        tenant,
                        key,
                        segment = %segment.0,
                        error = %error,
                        "skipping preferred segment after lookup failure"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        for storage_runtime in &policy.preferred_storage_runtimes {
            let candidate = ReplicaPlacementCandidate {
                target: ReplicaPlacementTarget::StorageRuntime(storage_runtime.clone()),
                soft: true,
            };
            if excluded.contains(storage_runtime) {
                continue;
            }
            match self.reserve_candidate(&candidate.target, length_bytes) {
                Ok((target, reservation)) => {
                    excluded.insert(storage_runtime.clone());
                    targets.push(target);
                    reservations.push(reservation);
                    if targets.len() == policy.replica_count {
                        return Ok((targets, reservations));
                    }
                }
                Err(error) if self.should_skip_candidate(&error, candidate.soft) => {
                    debug!(
                        tenant,
                        key,
                        storage_runtime = %storage_runtime,
                        error = %error,
                        "skipping preferred storage owner after reservation failure"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        if policy.prefer_local
            && !excluded.contains(&self.lease.runtime)
            && self.has_active_local_storage()
        {
            let candidate = ReplicaPlacementCandidate {
                target: ReplicaPlacementTarget::StorageRuntime(self.lease.runtime.clone()),
                soft: true,
            };
            match self.reserve_candidate(&candidate.target, length_bytes) {
                Ok((target, reservation)) => {
                    excluded.insert(self.lease.runtime.clone());
                    targets.push(target);
                    reservations.push(reservation);
                    if targets.len() == policy.replica_count {
                        return Ok((targets, reservations));
                    }
                }
                Err(error) if self.should_skip_candidate(&error, candidate.soft) => {
                    debug!(
                        tenant,
                        key,
                        owner = %self.lease.runtime,
                        error = %error,
                        "skipping local preferred owner after reservation failure"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        let ranked = self
            .request_placement_planner()
            .ranked_candidates(self, &ObjectRef::new(key).tenant(tenant))?;
        for owner in ranked {
            if excluded.contains(&owner) {
                continue;
            }
            let candidate = ReplicaPlacementCandidate {
                target: ReplicaPlacementTarget::StorageRuntime(owner),
                soft: true,
            };
            let storage_runtime = candidate.target.storage_runtime().clone();
            match self.reserve_candidate(&candidate.target, length_bytes) {
                Ok((target, reservation)) => {
                    excluded.insert(storage_runtime);
                    targets.push(target);
                    reservations.push(reservation);
                    if targets.len() == policy.replica_count {
                        return Ok((targets, reservations));
                    }
                }
                Err(error) if self.should_skip_candidate(&error, candidate.soft) => {
                    debug!(
                        tenant,
                        key,
                        storage_runtime = %candidate.target.storage_runtime(),
                        error = %error,
                        "skipping fallback storage owner after reservation failure"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        rollback(self, &targets, &reservations);
        Err(StoreError::InvalidState(format!(
            "not enough writable placement targets: reserved={} need={}",
            targets.len(),
            policy.replica_count
        )))
    }

    fn lookup_preferred_segment(&self, segment_name: &SegmentName) -> Result<SegmentAnnouncement> {
        let segments = self.metadata.list_segments(None)?;
        let segment = segments
            .into_iter()
            .find(|segment| {
                segment.segment_name == *segment_name
                    && segment.state == SegmentLifecycleState::Active
            })
            .ok_or_else(|| {
                StoreError::NotFound(format!("preferred segment {} not found", segment_name.0))
            })?;
        if !self.has_active_compatible_runtime(&segment.owner, false)?
            && !self.has_active_compatible_runtime(&segment.owner, true)?
        {
            return Err(StoreError::InvalidState(format!(
                "preferred segment {} belongs to an unavailable client",
                segment_name.0
            )));
        }
        Ok(segment)
    }
}
