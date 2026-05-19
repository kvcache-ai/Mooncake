struct PreferredSegmentReservationContext<'a> {
    tenant: &'a str,
    key: &'a str,
    length_bytes: usize,
    replica_count: usize,
    excluded: &'a mut BTreeSet<ClientRuntimeId>,
    targets: &'a mut Vec<ReplicaWriteTarget>,
    reservations: &'a mut Vec<mooncake_store_core::SegmentReservation>,
}

impl StoreClient {
    fn drain_lease_guard_timeout(&self) -> Duration {
        self.request_timeout_override
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT_CAP)
            .max(Duration::from_secs(30))
            .saturating_add(self.transfer_stall_timeout)
            .saturating_add(Duration::from_secs(5))
    }

    fn pin_draining_lease_for_evacuation(&self) -> Result<()> {
        if self.lifecycle_state() != ClientLifecycleState::Draining {
            return Ok(());
        }
        let min_expires_at_ms = Self::current_time_ms().saturating_add(
            self.drain_lease_guard_timeout()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        );
        let mut lease = self.lease();
        if lease.expires_at_ms >= min_expires_at_ms {
            return Ok(());
        }
        lease.expires_at_ms = min_expires_at_ms;
        self.metadata.upsert_client_lease(&lease)
    }

    pub fn lifecycle_state(&self) -> ClientLifecycleState {
        decode_lifecycle_state(self.lifecycle_state.load(Ordering::SeqCst))
    }

    fn pending_publish_deadline_ms(&self, length_bytes: u64) -> u64 {
        pending_publish_deadline_ms(
            length_bytes,
            self.transfer_stall_timeout,
            self.request_timeout_override,
        )
    }

    fn mark_local_pending_reservation(
        &self,
        reservation: &mooncake_store_core::SegmentReservation,
    ) {
        let deadline_ms = self.pending_publish_deadline_ms(reservation.length_bytes);
        self.allocator
            .lock()
            .mark_pending_reservation(reservation, deadline_ms);
    }

    fn set_lifecycle_state(&self, next_state: ClientLifecycleState) {
        self.lifecycle_state
            .store(encode_lifecycle_state(next_state), Ordering::SeqCst);
    }

    fn complete_startup_activation_after_local_memory_registration(&self) -> Result<()> {
        if self.lifecycle_state() != ClientLifecycleState::Standby {
            return Ok(());
        }
        if self
            .startup_activation_pending
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        let mut lease = self.lease();
        lease.state = ClientLifecycleState::Active;
        if let Err(error) = self.metadata.upsert_client_lease(&lease) {
            self.startup_activation_pending.store(true, Ordering::SeqCst);
            return Err(error);
        }
        self.set_lifecycle_state(ClientLifecycleState::Active);
        Ok(())
    }

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

    pub fn targeted_handoff_plan_for_state(
        &self,
        current_state: ClientLifecycleState,
    ) -> Result<Option<HandoffPlan>> {
        if current_state != ClientLifecycleState::Standby {
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
        Ok(Some(handoff))
    }

    pub fn activate_if_targeted_handoff(&mut self) -> Result<Option<HandoffPlan>> {
        let Some(handoff) = self.targeted_handoff_plan_for_state(self.lifecycle_state())? else {
            return Ok(None);
        };
        self.activate()?;
        Ok(Some(handoff))
    }

    pub fn runtime_state(&self, runtime: &ClientRuntimeId) -> Result<Option<ClientLifecycleState>> {
        if *runtime == self.lease.runtime {
            return Ok(Some(self.lifecycle_state()));
        }
        let _ = refresh_live_client_cache(
            self.metadata.as_ref(),
            &self.live_client_cache,
            "live_client_snapshot_runtime_state",
        );
        Ok(self
            .metadata
            .get_client_lease(runtime)?
            .filter(|lease| compatibility_matches(&self.lease, lease))
            .map(|lease| lease.state))
    }

    pub fn evacuate_owned_replicas_to_runtime(
        &mut self,
        successor: &ClientRuntimeId,
    ) -> Result<usize> {
        if self.lifecycle_state() != ClientLifecycleState::Draining {
            self.enter_draining()?;
        }
        self.evacuate_owned_replicas_to_runtime_when_draining(successor)
    }

    pub fn evacuate_owned_replicas_to_runtime_when_draining(
        &self,
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
        self.pin_draining_lease_for_evacuation()?;

        let _span = info_span!(
            "store.evacuate_owned_replicas_to_runtime",
            runtime = %self.lease.runtime,
            successor = %successor
        )
        .entered();
        let tracker = OperationTracker::new("evacuate_owned_replicas_to_runtime");
        let helper_writer = self.build_hot_upgrade_helper_writer()?;
        let result = self.evacuate_draining_routes_until_stable(
            "hot-upgrade evacuation still has live bytes on local segments",
            |route| match helper_writer.as_ref() {
                Some(writer) => self.migrate_owned_route_to_runtime_via_writer(
                    writer,
                    successor,
                    route,
                ),
                None => self.migrate_owned_route_to_runtime(successor, route),
            },
        );
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
            if self.lifecycle_state() != ClientLifecycleState::Draining {
                self.enter_draining()?;
            }
            let Some(state) = writer.runtime_state(&self.lease.runtime)? else {
                return Err(StoreError::NotFound(format!(
                    "runtime {} is not available",
                    self.lease.runtime
                )));
            };
            if !state.serves_reads() {
                return Err(StoreError::InvalidState(format!(
                    "runtime {} is not readable while {state:?}",
                    self.lease.runtime
                )));
            }
            self.evacuate_draining_routes_until_stable(
                "client shrink still has live bytes on local segments",
                |route| self.migrate_owned_route_via_writer(writer, route),
            )
        })();
        tracker.finish(&result, 0);
        result
    }

    pub fn lease(&self) -> ClientLease {
        let mut lease = self.lease.clone();
        lease.state = self.lifecycle_state();
        lease.expires_at_ms = lease
            .expires_at_ms
            .max(now_ms().saturating_add(self.lease_ttl_ms));
        lease
    }

    pub fn repair_local_metadata_after_heartbeat_recovery(&self) -> Result<()> {
        let _span = info_span!(
            "store.repair_local_metadata",
            runtime = %self.lease.runtime
        )
        .entered();
        let tracker = OperationTracker::new("repair_local_metadata");
        let result = (|| {
            let refreshed_transports =
                self.repair_local_transport_metadata_after_heartbeat_recovery()?;
            bootstrap_route_policy(
                self.metadata.as_ref(),
                &self.lease(),
                &self.default_tenant,
                self.route_control,
                self.route_topk,
            )?;
            let announcements = self.allocator.lock().announcements();
            for announcement in &announcements {
                self.metadata.publish_segment(announcement)?;
            }
            Ok((announcements.len(), refreshed_transports))
        })();
        match &result {
            Ok((republished, refreshed_transports)) => {
                info!(
                    runtime = %self.lease.runtime,
                    republished_segments = *republished,
                    refreshed_transports = *refreshed_transports,
                    "repaired local metadata after heartbeat recovery"
                );
            }
            Err(error) => {
                warn!(
                    runtime = %self.lease.runtime,
                    error = %error,
                    "failed to repair local metadata after heartbeat recovery"
                );
            }
        }
        let metric_result: Result<()> = result.as_ref().map(|_| ()).map_err(Clone::clone);
        tracker.finish(&metric_result, 0);
        result.map(|_| ())
    }

    fn repair_local_transport_metadata_after_heartbeat_recovery(&self) -> Result<usize> {
        let mut transports = BTreeMap::new();
        if let Some(primary_transport) = self.transport.clone() {
            transports.insert(primary_transport.segment_name()?, primary_transport);
        }
        {
            let state = self.state.lock();
            if state.memory.is_none() {
                return Ok(0);
            }
            for (segment_name, transport) in &state.local_transports {
                transports
                    .entry(segment_name.clone())
                    .or_insert_with(|| transport.clone());
            }
        }
        for transport in transports.values() {
            transport.republish_local_metadata()?;
        }
        Ok(transports.len())
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
            .tenant_scoped_metadata()
            .state(ClientLifecycleState::Active)
            .activate_on_local_memory_registration()
            .tenant(self.default_tenant.clone())
            .compatibility(self.lease.compatibility.clone())
            .local_memory(self.local_memory.clone())
            .transport(helper_transport)
            .transport_factory(factory)
            .route_control(self.route_control)
            .route_topk(self.route_topk)
            .transfer_timeout(self.transfer_stall_timeout)
            .startup_prewarm_max_delay(Duration::ZERO);
        if let Some(timeout) = self.request_timeout_override {
            builder = builder.request_timeout(timeout);
        }

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
        if let Some(replica_count) = self
            .placement_policy
            .as_ref()
            .and_then(|policy| policy.default_replica_count)
        {
            return replica_count.max(1);
        }
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

    fn write_retry_limit(
        &self,
        policy: &ResolvedReplicationPolicy,
        ranked_candidate_count: usize,
    ) -> usize {
        if !policy.required_preferred_segments.is_empty() {
            return 1;
        }

        let soft_candidate_count = policy
            .hint_preferred_segments
            .len()
            .saturating_add(policy.preferred_storage_runtimes.len())
            .saturating_add(ranked_candidate_count)
            .saturating_add(usize::from(
                policy.prefer_local && self.can_prefer_local_storage_for_write_mode(),
            ));

        DEFAULT_PUT_WRITE_RETRY_LIMIT
            .max(policy.replica_count)
            .max(soft_candidate_count)
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
        let observed = self
            .live_client_cache
            .lock()
            .snapshot()
            .and_then(|leases| leases.into_iter().find(|lease| lease.runtime == *runtime));
        self.suspect_runtime_cache
            .lock()
            .mark(runtime.clone(), deadline, observed.as_ref());
        debug!(
            runtime = %self.lease.runtime,
            suspect_runtime = %runtime,
            context,
            quarantine_ms = DEFAULT_SUSPECT_RUNTIME_TTL.as_millis() as u64,
            "marked runtime as suspect after remote failure"
        );
    }

    fn runtime_is_suspect(&self, runtime: &ClientRuntimeId) -> bool {
        self.suspect_runtime_cache.lock().contains(runtime)
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
        match self.lookup_runtime_lease_once(runtime, false) {
            Ok(lease) => Ok(lease),
            Err(StoreError::NotFound(_)) => self.lookup_runtime_lease_once(runtime, true),
            Err(error) => Err(error),
        }
    }

    fn transport_open_segment_name<'a>(
        segment_name: &'a SegmentName,
        transport_endpoint: Option<&'a str>,
    ) -> &'a str {
        transport_endpoint
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .unwrap_or(&segment_name.0)
    }

    fn replica_transport_open_segment_name(&self, replica: &ReplicaRoute) -> Result<String> {
        self.remote_transport_open_segment_name(&replica.owner, &replica.segment_name)
    }

    fn remote_transport_open_segment_name(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
    ) -> Result<String> {
        if let Some(segment) = self.allocator.lock().announcement(segment_name) {
            if segment.owner == *owner {
                self.state.lock().cache_segment_target_metadata(
                    owner,
                    segment_name,
                    &segment.target_chunks,
                    segment.transport_endpoint.clone(),
                    segment.transport_segment_descriptor.clone(),
                );
                return Ok(Self::transport_open_segment_name(
                    segment_name,
                    segment.transport_endpoint.as_deref(),
                )
                .to_string());
            }
        }
        if let Some(metadata) = self
            .state
            .lock()
            .cached_segment_target_metadata(owner, segment_name)
        {
            return Ok(Self::transport_open_segment_name(
                segment_name,
                metadata.transport_endpoint.as_deref(),
            )
            .to_string());
        }
        let segment = self.metadata.get_segment(owner, segment_name)?;
        if let Some(segment) = segment.as_ref() {
            self.state.lock().cache_segment_target_metadata(
                owner,
                segment_name,
                &segment.target_chunks,
                segment.transport_endpoint.clone(),
                segment.transport_segment_descriptor.clone(),
            );
        }
        Ok(Self::transport_open_segment_name(
            segment_name,
            segment
                .as_ref()
                .and_then(|segment| segment.transport_endpoint.as_deref()),
        )
        .to_string())
    }

    fn local_transport_open_segment_name(&self, segment_name: &SegmentName) -> String {
        self.local_transport_endpoint(segment_name)
            .unwrap_or_else(|| segment_name.0.clone())
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
        match self.lookup_runtime_leases_once(&wanted, false) {
            Ok(leases) => Ok(leases),
            Err(StoreError::NotFound(_)) => self.lookup_runtime_leases_once(&wanted, true),
            Err(error) => Err(error),
        }
    }

    fn lookup_cached_runtime_leases_best_effort(
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
        Ok(self
            .available_compatible_live_clients(false)?
            .into_iter()
            .filter(|lease| wanted.contains(&lease.runtime))
            .map(|lease| (lease.runtime.clone(), lease))
            .collect())
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
            let Some(lease) = matched else {
                debug!(
                    runtime = %self.lease.runtime,
                    preferred_storage_owner = selector,
                    "skipping unavailable preferred storage owner"
                );
                continue;
            };
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

    fn reserve_segment_allocation(
        &self,
        owner: &ClientRuntimeId,
        segment_name: Option<&SegmentName>,
        length_bytes: u64,
        require_local_memory: bool,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if *owner == self.lease.runtime {
            lifecycle_accepts_writes(&self.lifecycle_state, "local storage allocator")?;
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
                let reservation = match segment_name {
                    Some(segment_name) => self
                        .allocator
                        .lock()
                        .reserve_specific(owner, segment_name, length_bytes),
                    None => self.allocator.lock().reserve_any(owner, length_bytes),
                }?;
                self.mark_local_pending_reservation(&reservation);
                return Ok(reservation);
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
                    Ok(reservation) => {
                        self.mark_local_pending_reservation(&reservation);
                        return Ok(reservation);
                    }
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
            Err(error) => {
                if should_mark_runtime_suspect_after_allocator_error(&error) {
                    self.mark_runtime_suspect(owner, "allocator_rpc_failed");
                }
                Err(error)
            }
        }
    }

    fn resolve_replication_policy(
        &self,
        policy: Option<&ReplicationPolicy>,
    ) -> Result<ResolvedReplicationPolicy> {
        let mut policy = policy.cloned().unwrap_or_default();
        let request_preferred_segments = policy.preferred_segments.clone();
        let mut hint_preferred_segments = Vec::new();
        if let Some(placement) = self.placement_policy.as_ref() {
            if policy.preferred_storage_owners.is_empty() {
                if let Some(owners) = placement.preferred_storage_owners.as_ref() {
                    policy.preferred_storage_owners = owners.clone();
                }
            }
            if request_preferred_segments.is_empty() {
                if let Some(segments) = placement.preferred_segments.as_ref() {
                    hint_preferred_segments = segments
                        .iter()
                        .cloned()
                        .map(SegmentName::new)
                        .collect();
                }
            }
            if let Some(prefer_local) = placement.prefer_local {
                policy.prefer_local = prefer_local;
            }
            if let Some(prefer_same_node) = placement.prefer_alloc_in_same_node {
                policy.prefer_alloc_in_same_node = prefer_same_node;
            }
        }
        let replica_count = policy
            .replica_count
            .unwrap_or_else(|| self.default_replica_count());
        if replica_count == 0 {
            return Err(StoreError::InvalidState(
                "replica_count must be greater than zero".to_string(),
            ));
        }

        let required_preferred_segments =
            Self::resolve_preferred_segments(&request_preferred_segments, true)?;
        let hint_preferred_segments = Self::resolve_preferred_segments(&hint_preferred_segments, false)?;

        Ok(ResolvedReplicationPolicy {
            replica_count,
            required_preferred_segments: if policy.with_soft_pin {
                Vec::new()
            } else {
                required_preferred_segments.clone()
            },
            hint_preferred_segments: if policy.with_soft_pin {
                let mut segments = required_preferred_segments;
                segments.extend(hint_preferred_segments);
                segments
            } else {
                hint_preferred_segments
            },
            preferred_storage_runtimes: self
                .resolve_preferred_storage_owners(&policy.preferred_storage_owners)?,
            prefer_local: policy.prefer_local || policy.prefer_alloc_in_same_node,
        })
    }

    fn resolve_preferred_segments(
        segments: &[SegmentName],
        reject_duplicates: bool,
    ) -> Result<Vec<SegmentName>> {
        let mut resolved = Vec::with_capacity(segments.len());
        let mut seen = BTreeSet::new();
        for segment in segments {
            if !seen.insert(segment.clone()) {
                if reject_duplicates {
                    return Err(StoreError::Conflict(format!(
                        "duplicate preferred segment {}",
                        segment.0
                    )));
                }
                continue;
            }
            resolved.push(segment.clone());
        }
        Ok(resolved)
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

    fn can_prefer_local_storage_for_write_mode(&self) -> bool {
        if self.lifecycle_state() != ClientLifecycleState::Active {
            return false;
        }
        if !self.has_active_local_storage() {
            return false;
        }
        match self.write_mode {
            WriteMode::Routed { .. } => self
                .lease
                .endpoints
                .labels
                .get("storage")
                .is_some_and(|value| value == "true"),
            WriteMode::LocalOnly => true,
        }
    }

    fn should_skip_candidate(&self, error: &StoreError, soft: bool) -> bool {
        soft && matches!(
            error,
            StoreError::Allocator(_)
                | StoreError::NotFound(_)
                | StoreError::InvalidState(_)
                | StoreError::Transport(_)
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
        object: &ObjectRef<'_>,
        length_bytes: usize,
        policy: &ResolvedReplicationPolicy,
    ) -> Result<(
        Vec<ReplicaWriteTarget>,
        Vec<mooncake_store_core::SegmentReservation>,
    )> {
        let tenant = object.tenant.unwrap_or(self.default_tenant());
        let key = object.key;
        let mut targets = Vec::with_capacity(policy.replica_count);
        let mut reservations = Vec::with_capacity(policy.replica_count);
        let mut excluded = BTreeSet::new();
        let rollback =
            |this: &Self,
             targets: &[ReplicaWriteTarget],
             reservations: &[mooncake_store_core::SegmentReservation]| {
                let _ = this.release_reserved_allocations(targets, reservations);
            };

        {
            let mut preferred = PreferredSegmentReservationContext {
                tenant,
                key,
                length_bytes,
                replica_count: policy.replica_count,
                excluded: &mut excluded,
                targets: &mut targets,
                reservations: &mut reservations,
            };

            self.reserve_preferred_segments(
                &mut preferred,
                &policy.required_preferred_segments,
                PreferredSegmentSource::Request,
                false,
            )?;
            if preferred.targets.len() != policy.replica_count {
                self.reserve_preferred_segments(
                    &mut preferred,
                    &policy.hint_preferred_segments,
                    PreferredSegmentSource::TenantPolicy,
                    true,
                )?;
            }
        }
        if targets.len() == policy.replica_count {
            return Ok((targets, reservations));
        }

        for storage_runtime in &policy.preferred_storage_runtimes {
            if self.runtime_is_suspect(storage_runtime) {
                debug!(
                    tenant,
                    key,
                    storage_runtime = %storage_runtime,
                    "skipping suspect preferred storage owner"
                );
                continue;
            }
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
            && self.can_prefer_local_storage_for_write_mode()
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
            .ranked_candidates(self, object)?;
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

    fn reserve_preferred_segments(
        &self,
        preferred: &mut PreferredSegmentReservationContext<'_>,
        segments: &[SegmentName],
        source: PreferredSegmentSource,
        soft: bool,
    ) -> Result<()> {
        for segment in segments {
            match self.lookup_preferred_segment(segment) {
                Ok(segment_owner) => {
                    if self.runtime_is_suspect(&segment_owner.owner) {
                        self.log_skipped_preferred_segment(
                            preferred.tenant,
                            preferred.key,
                            segment,
                            source,
                            "owner_suspect",
                            None,
                        );
                        continue;
                    }
                    let candidate = ReplicaPlacementCandidate {
                        target: ReplicaPlacementTarget::Segment {
                            storage_runtime: segment_owner.owner,
                            segment_name: segment_owner.segment_name,
                        },
                        soft,
                    };
                    let storage_runtime = candidate.target.storage_runtime().clone();
                    if preferred.excluded.contains(&storage_runtime) {
                        continue;
                    }
                    match self.reserve_candidate(&candidate.target, preferred.length_bytes) {
                        Ok((target, reservation)) => {
                            preferred.excluded.insert(storage_runtime);
                            preferred.targets.push(target);
                            preferred.reservations.push(reservation);
                            if preferred.targets.len() == preferred.replica_count {
                                return Ok(());
                            }
                        }
                        Err(error) if self.should_skip_candidate(&error, candidate.soft) => {
                            self.log_skipped_preferred_segment(
                                preferred.tenant,
                                preferred.key,
                                segment,
                                source,
                                Self::preferred_segment_skip_reason(&error),
                                Some(&error),
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) if self.should_skip_candidate(&error, soft) => {
                    self.log_skipped_preferred_segment(
                        preferred.tenant,
                        preferred.key,
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
    }

    fn log_skipped_preferred_segment(
        &self,
        tenant: &str,
        key: &str,
        segment: &SegmentName,
        source: PreferredSegmentSource,
        reason: &'static str,
        error: Option<&StoreError>,
    ) {
        if source == PreferredSegmentSource::TenantPolicy {
            registry::record_preferred_segment_skip(source.label(), reason);
            warn!(
                tenant,
                key,
                segment = %segment.0,
                source = source.label(),
                reason,
                error = error.map(|error| error.to_string()),
                "skipping tenant-policy preferred segment and falling back"
            );
            return;
        }
        debug!(
            tenant,
            key,
            segment = %segment.0,
            source = source.label(),
            reason,
            error = error.map(|error| error.to_string()),
            "skipping preferred segment"
        );
    }

    fn preferred_segment_skip_reason(error: &StoreError) -> &'static str {
        match error {
            StoreError::NotFound(_) => "not_found",
            StoreError::InvalidState(_) => "owner_unavailable",
            StoreError::Allocator(_) => "allocator",
            StoreError::Transport(_) => "transport",
            _ => "other",
        }
    }

    fn lookup_preferred_segment(&self, segment_name: &SegmentName) -> Result<SegmentAnnouncement> {
        let live_clients = self.available_compatible_live_clients(false)?;
        for lease in live_clients
            .iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
        {
            if let Some(segment) = self.metadata.get_segment(&lease.runtime, segment_name)? {
                if segment.state == SegmentLifecycleState::Active {
                    return Ok(segment);
                }
            }
        }
        let live_clients = self.available_compatible_live_clients(true)?;
        for lease in live_clients
            .iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
        {
            if let Some(segment) = self.metadata.get_segment(&lease.runtime, segment_name)? {
                if segment.state == SegmentLifecycleState::Active {
                    return Ok(segment);
                }
            }
        }
        Err(StoreError::NotFound(format!(
            "preferred segment {} not found on a live storage client",
            segment_name.0
        )))
    }
}
