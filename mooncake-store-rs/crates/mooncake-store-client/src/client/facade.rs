pub trait MooncakeCompatibilityFacade {
    fn heartbeat(&mut self, expires_at_ms: u64) -> Result<()>;
    fn enter_standby(&mut self) -> Result<()>;
    fn activate(&mut self) -> Result<()>;
    fn enter_draining(&mut self) -> Result<()>;
    fn plan_handoff(
        &self,
        successor_epoch: ClientEpoch,
        kind: HandoffKind,
        barrier_version: u64,
        created_at_ms: u64,
        deadline_ms: Option<u64>,
    ) -> Result<HandoffPlan>;
    fn mount_segment(&self, capacity_bytes: u64, used_bytes: u64, tags: Vec<String>) -> Result<()>;
    fn list_segments(&self) -> Result<Vec<SegmentAnnouncement>>;
    fn expand_local_memory(&self, storage_bytes: usize) -> Result<SegmentAnnouncement>;
    fn drain_segment(&self, segment: &SegmentName) -> Result<()>;
    fn retire_segment(&self, segment: &SegmentName) -> Result<bool>;
    fn evacuate_owned_replicas(&mut self) -> Result<usize> {
        Err(StoreError::Unsupported(
            "client evacuation is not supported".to_string(),
        ))
    }
    fn query_route(&self, key: &str) -> Result<Option<ObjectRoute>>;
    fn query_route_in_tenant(&self, tenant: &str, key: &str) -> Result<Option<ObjectRoute>>;
    fn query_route_in_scope(
        &self,
        scope: &NamespaceScope,
        logical_key: &str,
    ) -> Result<Option<ObjectRoute>>;
    fn query_route_by_object_id(&self, object_id: &LogicalObjectId) -> Result<Option<ObjectRoute>>;
    fn list_routes_in_scope(&self, scope: &NamespaceScope) -> Result<Vec<ObjectRoute>>;
    fn list_reuse_candidates(&self, reuse: &mooncake_store_core::ReuseIdentity) -> Result<Vec<ObjectRoute>>;
    fn cas_route(
        &self,
        key: &str,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;
    fn cas_route_in_tenant(
        &self,
        tenant: &str,
        key: &str,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;
    fn register_local_memory(&self) -> Result<()>;
    fn register_buffer(&self, buffer: *mut c_void, size: usize) -> Result<()>;
    fn unregister_buffer(&self, buffer: *mut c_void, size: usize) -> Result<()>;
    fn get_hostname(&self) -> Result<String>;
    fn get_size(&self, key: &str) -> Result<usize>;
    fn get_size_in_tenant(&self, tenant: &str, key: &str) -> Result<usize>;
    fn is_exist(&self, key: &str) -> Result<bool>;
    fn is_exist_in_tenant(&self, tenant: &str, key: &str) -> Result<bool>;
    fn batch_is_exist(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<bool>>;
    fn remove(&self, key: &str, force: bool) -> Result<()>;
    fn remove_in_tenant(&self, tenant: &str, key: &str, force: bool) -> Result<()>;
    fn batch_remove(&self, objects: &[ObjectRef<'_>], force: bool) -> Result<()>;
    fn put(&self, key: &str, value: &[u8]) -> Result<ObjectRoute>;
    fn put_in_tenant(&self, tenant: &str, key: &str, value: &[u8]) -> Result<ObjectRoute>;
    fn put_with_policy(
        &self,
        key: &str,
        value: &[u8],
        policy: &ReplicationPolicy,
    ) -> Result<ObjectRoute>;
    fn put_in_tenant_with_policy(
        &self,
        tenant: &str,
        key: &str,
        value: &[u8],
        policy: &ReplicationPolicy,
    ) -> Result<ObjectRoute>;
    fn put_from(&self, key: &str, buffer: *const c_void, size: usize) -> Result<ObjectRoute>;
    fn put_from_in_tenant(
        &self,
        tenant: &str,
        key: &str,
        buffer: *const c_void,
        size: usize,
    ) -> Result<ObjectRoute>;
    fn put_from_with_policy(
        &self,
        key: &str,
        buffer: *const c_void,
        size: usize,
        policy: &ReplicationPolicy,
    ) -> Result<ObjectRoute>;
    fn put_from_in_tenant_with_policy(
        &self,
        tenant: &str,
        key: &str,
        buffer: *const c_void,
        size: usize,
        policy: &ReplicationPolicy,
    ) -> Result<ObjectRoute>;
    fn batch_put(&self, requests: &[PutRequest<'_>]) -> Result<Vec<ObjectRoute>>;
    fn batch_put_from(&self, requests: &[PutFromRequest<'_>]) -> Result<Vec<ObjectRoute>>;
    fn batch_put_from_multi_buffers(
        &self,
        requests: &[MultiBufferPutRequest<'_>],
    ) -> Result<Vec<ObjectRoute>>;
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    fn get_in_tenant(&self, tenant: &str, key: &str) -> Result<Vec<u8>>;
    fn get_into(&self, key: &str, buffer: &mut [u8]) -> Result<usize>;
    fn get_into_in_tenant(&self, tenant: &str, key: &str, buffer: &mut [u8]) -> Result<usize>;
    fn batch_get(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<Vec<u8>>>;
    fn batch_get_buffer(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<Vec<u8>>>;
    fn batch_get_into(&self, requests: &mut [GetRequest<'_>]) -> Result<Vec<usize>>;
    fn batch_get_into_multi_buffers(
        &self,
        requests: &mut [MultiBufferGetRequest<'_>],
    ) -> Result<Vec<usize>>;
}

fn common_value<'a>(mut values: impl Iterator<Item = &'a str>) -> String {
    let Some(first) = values.next() else {
        return "mixed".to_string();
    };
    if values.all(|value| value == first) {
        first.to_string()
    } else {
        "mixed".to_string()
    }
}

impl StoreClient {
    fn expect_exactly_one<T>(mut items: Vec<T>, operation: &str) -> Result<T> {
        match items.len() {
            1 => Ok(items.pop().expect("single-item vector should contain one element")),
            0 => Err(StoreError::InvalidState(format!(
                "expected exactly one {operation} result, got 0"
            ))),
            len => Err(StoreError::InvalidState(format!(
                "expected exactly one {operation} result, got {len}"
            ))),
        }
    }

    fn query_routes_by_object_keys_bounded(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRoute>>> {
        let tracker = OperationTracker::new("route_lookup_many")
            .scope("route_lookup")
            .attribute_u64("mooncake.item_count", keys.len() as u64);
        let result: Result<Vec<Option<ObjectRoute>>> =
            self.route_ops().load_active_routes_bounded(keys);
        tracker.finish(&result, result.as_ref().map(|routes| routes.len()).unwrap_or(0) as u64);
        result
    }

    fn query_readable_routes_by_object_keys_bounded(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRoute>>> {
        self.filter_routes_to_readable(self.query_routes_by_object_keys_bounded(keys)?)
    }

    fn route_delete_can_follow_rollout_handoff(
        &self,
        previous: &ObjectRoute,
        current: &ObjectRoute,
    ) -> bool {
        if current.state != RouteState::Active
            || previous.compatibility != current.compatibility
            || previous.replicas.len() != current.replicas.len()
        {
            return false;
        }

        let mut changed_replica = false;
        let mut handoffs =
            std::collections::BTreeMap::<String, Option<mooncake_store_core::HandoffPlan>>::new();
        for (before, after) in previous.replicas.iter().zip(current.replicas.iter()) {
            if before.owner.stable_id != after.owner.stable_id
                || before.length != after.length
                || before.checksum.is_none()
                || before.checksum != after.checksum
                || before.tier != after.tier
                || before.priority != after.priority
            {
                return false;
            }
            if before.owner == after.owner {
                continue;
            }
            if after.owner.epoch <= before.owner.epoch {
                return false;
            }

            changed_replica = true;
            let handoff = handoffs
                .entry(before.owner.stable_id.0.clone())
                .or_insert_with(|| match self.metadata.get_handoff(&before.owner.stable_id) {
                    Ok(handoff) => handoff,
                    Err(error) => {
                        warn!(
                            runtime = %self.lease.runtime,
                            stable_id = %before.owner.stable_id,
                            error = %error,
                            "failed to load handoff plan while validating rollout delete follow"
                        );
                        None
                    }
                });
            let Some(handoff) = handoff.as_ref() else {
                return false;
            };
            if !matches!(
                handoff.kind,
                HandoffKind::HotUpgrade | HandoffKind::HotStandbyPromotion
            ) || handoff.from != before.owner
                || handoff.to != after.owner
            {
                return false;
            }
        }

        changed_replica
    }

    fn remove_object_route_with_retry(
        &self,
        object_id: &LogicalObjectId,
        object_key: &ObjectKey,
        force: bool,
    ) -> Result<()> {
        let tenant = object_id.scope.tenant.as_str();
        let key = object_id.logical_key.as_str();
        let mut current = self.route_ops().load_route(object_key)?;
        let max_attempts = DEFAULT_PUT_WRITE_RETRY_LIMIT;
        let mut attempt = 0usize;

        loop {
            let Some(route) = current.take() else {
                return if force {
                    Ok(())
                } else {
                    Err(StoreError::NotFound(format!("tenant={tenant} key={key}")))
                };
            };
            if route.state != RouteState::Active {
                return if force {
                    Ok(())
                } else {
                    Err(StoreError::NotFound(format!("tenant={tenant} key={key}")))
                };
            }

            attempt = attempt.saturating_add(1);
            let quota_reservation =
                self.reserve_tenant_quota_for_delete(object_id, object_key, &route)?;
            let cas = self.route_ops().delete_route(object_key, Some(route.version))?;
            if cas.applied {
                self.finalize_tenant_quota_delete(quota_reservation.as_ref())?;
                if let Err(error) = self.schedule_route_reclaim(&route) {
                    warn!(
                        runtime = %self.lease.runtime,
                        tenant,
                        key,
                        error = %error,
                        "route delete reclaim scheduling failed after authoritative delete"
                    );
                }
                return Ok(());
            }

            let _ = self.abort_tenant_quota_reservation(
                quota_reservation.as_ref(),
                "route_delete_compare_and_swap_conflict",
            );
            match cas.current {
                Some(current_route)
                    if force
                        && self.route_delete_can_follow_rollout_handoff(&route, &current_route)
                        && attempt < max_attempts =>
                {
                    debug!(
                        runtime = %self.lease.runtime,
                        tenant,
                        key,
                        attempt,
                        max_attempts,
                        route_version = current_route.version.0,
                        "retrying remove after route compare-and-swap conflict with rollout successor route"
                    );
                    current = Some(current_route);
                    continue;
                }
                Some(current_route) if current_route.state == RouteState::Active => {
                    return Err(StoreError::Conflict(format!(
                        "route delete lost race for tenant={tenant} key={key}"
                    )));
                }
                _ => {
                    return if force {
                        Ok(())
                    } else {
                        Err(StoreError::NotFound(format!("tenant={tenant} key={key}")))
                    };
                }
            }
        }
    }

    pub fn batch_is_readable(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<bool>> {
        let tracker = OperationTracker::new("batch_is_readable")
            .attribute_u64("mooncake.item_count", objects.len() as u64);
        let result: Result<Vec<bool>> = (|| {
            let keys = objects
                .iter()
                .map(|object| {
                    let tenant = object.tenant.unwrap_or(self.default_tenant());
                    let scope =
                        NamespaceScope::with_defaults(Some(tenant), object.domain, object.object_set);
                    ObjectKey::from_scope(&scope, object.key)
                })
                .collect::<Vec<_>>();
            let result = self
                .query_readable_routes_by_object_keys_bounded(&keys)?
                .into_iter()
                .map(|route| route.is_some())
                .collect();
            Ok(result)
        })();
        tracker.finish(&result, result.as_ref().map(|items| items.len()).unwrap_or(0) as u64);
        result
    }

    fn query_route_by_object_id_inner(
        &self,
        object_id: &LogicalObjectId,
    ) -> Result<Option<ObjectRoute>> {
        self.route_ops().load_active_route_and_report(
            &mooncake_store_core::ObjectKey::from_logical_id(object_id),
            self,
        )
    }

    fn shared_batch_replication_policy(
        requests: &[PutRequest<'_>],
    ) -> Option<Option<ReplicationPolicy>> {
        if requests.is_empty() {
            return Some(None);
        }
        if requests.iter().all(|request| request.policy.is_none()) {
            return Some(None);
        }
        let shared = requests[0].policy.clone().unwrap_or_default();
        requests
            .iter()
            .all(|request| request.policy.clone().unwrap_or_default() == shared)
            .then_some(Some(shared))
    }

    fn shared_batch_put_from_replication_policy(
        requests: &[PutFromRequest<'_>],
    ) -> Option<Option<ReplicationPolicy>> {
        if requests.is_empty() {
            return Some(None);
        }
        if requests.iter().all(|request| request.policy.is_none()) {
            return Some(None);
        }
        let shared = requests[0].policy.clone().unwrap_or_default();
        requests
            .iter()
            .all(|request| request.policy.clone().unwrap_or_default() == shared)
            .then_some(Some(shared))
    }

    fn record_batch_put_from_items(
        &self,
        tracker: &OperationTracker,
        requests: &[PutFromRequest<'_>],
        statuses: &[Result<ObjectRoute>],
        batch_bytes: u64,
    ) {
        let Some(trace_request_id) = tracker.trace_request_id() else {
            return;
        };
        let (tenant, domain, object_set) = self.put_from_batch_namespace(requests);
        let runtime_id = self.lease.runtime.to_string();
        let items = requests
            .iter()
            .enumerate()
            .map(|(index, request)| ApiItemTrace {
                index,
                key: request.key,
                tenant: request.tenant.unwrap_or(self.default_tenant()),
                domain: request.domain.unwrap_or(mooncake_store_core::DEFAULT_DOMAIN),
                object_set: request
                    .object_set
                    .unwrap_or(mooncake_store_core::DEFAULT_OBJECT_SET),
                size: Some(request.size as u64),
                read_len: None,
                status: if statuses.get(index).is_some_and(|status| status.is_ok()) {
                    "ok"
                } else {
                    "error"
                },
            })
            .collect::<Vec<_>>();
        record_api_items(ApiItemsTraceRecord {
            trace_request_id,
            operation: "batch_put_from",
            tenant: &tenant,
            domain: &domain,
            object_set: &object_set,
            runtime_id: &runtime_id,
            batch_bytes,
            items,
        });
    }

    fn record_batch_get_into_items(
        &self,
        tracker: &OperationTracker,
        requests: &[GetRequest<'_>],
        sizes: &[usize],
        batch_bytes: u64,
    ) {
        let Some(trace_request_id) = tracker.trace_request_id() else {
            return;
        };
        let (tenant, domain, object_set) = self.get_batch_namespace(requests);
        let runtime_id = self.lease.runtime.to_string();
        let items = requests
            .iter()
            .enumerate()
            .map(|(index, request)| {
                let read_len = sizes.get(index).copied().unwrap_or_default() as u64;
                ApiItemTrace {
                    index,
                    key: request.key,
                    tenant: request.tenant.unwrap_or(self.default_tenant()),
                    domain: request.domain.unwrap_or(mooncake_store_core::DEFAULT_DOMAIN),
                    object_set: request
                        .object_set
                        .unwrap_or(mooncake_store_core::DEFAULT_OBJECT_SET),
                    size: None,
                    read_len: Some(read_len),
                    status: if read_len == 0 { "soft_miss" } else { "ok" },
                }
            })
            .collect::<Vec<_>>();
        record_api_items(ApiItemsTraceRecord {
            trace_request_id,
            operation: "batch_get_into",
            tenant: &tenant,
            domain: &domain,
            object_set: &object_set,
            runtime_id: &runtime_id,
            batch_bytes,
            items,
        });
    }

    fn put_from_batch_namespace(&self, requests: &[PutFromRequest<'_>]) -> (String, String, String) {
        let tenant = common_value(
            requests
                .iter()
                .map(|request| request.tenant.unwrap_or(self.default_tenant())),
        );
        let domain = common_value(
            requests
                .iter()
                .map(|request| request.domain.unwrap_or(mooncake_store_core::DEFAULT_DOMAIN)),
        );
        let object_set = common_value(requests.iter().map(|request| {
            request
                .object_set
                .unwrap_or(mooncake_store_core::DEFAULT_OBJECT_SET)
        }));
        (tenant, domain, object_set)
    }

    fn get_batch_namespace(&self, requests: &[GetRequest<'_>]) -> (String, String, String) {
        let tenant = common_value(
            requests
                .iter()
                .map(|request| request.tenant.unwrap_or(self.default_tenant())),
        );
        let domain = common_value(
            requests
                .iter()
                .map(|request| request.domain.unwrap_or(mooncake_store_core::DEFAULT_DOMAIN)),
        );
        let object_set = common_value(requests.iter().map(|request| {
            request
                .object_set
                .unwrap_or(mooncake_store_core::DEFAULT_OBJECT_SET)
        }));
        (tenant, domain, object_set)
    }

    pub fn batch_put_from_statuses(
        &self,
        requests: &[PutFromRequest<'_>],
    ) -> Vec<Result<ObjectRoute>> {
        let bytes_in = requests
            .iter()
            .map(|request| request.size as u64)
            .sum::<u64>();
        let _span = info_span!(
            "store.batch_put_from",
            runtime = %self.lease.runtime,
            items = requests.len(),
            bytes_in
        )
        .entered();
        let tracker = OperationTracker::new("batch_put_from").input_bytes(bytes_in);
        let statuses = self.batch_put_from_statuses_inner(requests);
        let aggregate = Self::aggregate_batch_put_statuses(&statuses);
        self.record_batch_put_from_items(&tracker, requests, &statuses, bytes_in);
        tracker.finish(&aggregate, bytes_in);
        statuses
    }

    fn aggregate_batch_put_statuses(statuses: &[Result<ObjectRoute>]) -> Result<Vec<ObjectRoute>> {
        let mut routes = Vec::with_capacity(statuses.len());
        for status in statuses {
            match status {
                Ok(route) => routes.push(route.clone()),
                Err(error) => return Err(error.clone()),
            }
        }
        Ok(routes)
    }

    fn batch_put_from_statuses_inner(
        &self,
        requests: &[PutFromRequest<'_>],
    ) -> Vec<Result<ObjectRoute>> {
        let mut statuses = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<_>>();
        let mut routed_indices = Vec::with_capacity(requests.len());
        let mut routed_requests = Vec::with_capacity(requests.len());
        let mut routed_sources = Vec::with_capacity(requests.len());
        let mut routed_originals = Vec::with_capacity(requests.len());

        for (index, request) in requests.iter().enumerate() {
            if request.buffer.is_null() {
                statuses[index] = Some(Err(StoreError::Allocator(
                    "put_from buffer must not be null".to_string(),
                )));
                continue;
            }
            {
                let state = self.state.lock();
                if !state.buffer_is_registered(request.buffer.cast_mut(), request.size) {
                    let tenant = request.tenant.unwrap_or(self.default_tenant());
                    statuses[index] = Some(Err(StoreError::Allocator(format!(
                        "put_from buffer is not registered for tenant={tenant} key={}",
                        request.key
                    ))));
                    continue;
                }
            }

            if matches!(self.write_mode, WriteMode::Routed { .. }) {
                let value =
                    unsafe { slice::from_raw_parts(request.buffer.cast::<u8>(), request.size) };
                let mut routed = PutRequest::new(request.key, value);
                if let Some(tenant) = request.tenant {
                    routed = routed.tenant(tenant);
                }
                if let Some(domain) = request.domain {
                    routed = routed.domain(domain);
                }
                if let Some(object_set) = request.object_set {
                    routed = routed.object_set(object_set);
                }
                if let Some(qos_tier) = request.qos_tier {
                    routed = routed.qos_tier(qos_tier);
                }
                if let Some(policy) = request.policy.clone() {
                    routed = routed.replication(policy);
                }
                routed_indices.push(index);
                routed_requests.push(routed);
                routed_sources.push(request.buffer.cast_mut());
                routed_originals.push(request.clone());
                continue;
            }

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
            statuses[index] = Some(self.put_object_from_registered(
                &object,
                request.buffer.cast_mut(),
                request.size,
                request.policy.as_ref(),
            ));
        }

        if !routed_requests.is_empty() {
            let routed_statuses =
                if let Some(shared_policy) =
                    Self::shared_batch_put_from_replication_policy(&routed_originals)
                {
                    self.batch_put_scoped_routed_accept_existing_statuses(
                        &routed_requests,
                        &routed_sources,
                        shared_policy.as_ref(),
                    )
                } else {
                    routed_requests
                        .iter()
                        .zip(routed_sources.iter())
                        .map(|(request, source)| {
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
                            self.put_object_from_registered(
                                &object,
                                *source,
                                request.value.len(),
                                request.policy.as_ref(),
                            )
                        })
                        .collect()
                };

            for (index, status) in routed_indices.into_iter().zip(routed_statuses) {
                statuses[index] = Some(status);
            }
        }

        statuses
            .into_iter()
            .map(|status| {
                status.unwrap_or_else(|| {
                    Err(StoreError::InvalidState(
                        "batch_put_from status was not recorded".to_string(),
                    ))
                })
            })
            .collect()
    }
}

impl MooncakeCompatibilityFacade for StoreClient {
    fn heartbeat(&mut self, expires_at_ms: u64) -> Result<()> {
        let result = self.prepare_heartbeat(expires_at_ms).publish();
        match result {
            Ok(()) => {
                let previous_failures =
                    self.heartbeat_repair_pending.swap(0, Ordering::SeqCst);
                if previous_failures == 0 {
                    return Ok(());
                }
                if let Err(error) = self.repair_local_metadata_after_heartbeat_recovery() {
                    self.heartbeat_repair_pending.store(1, Ordering::SeqCst);
                    return Err(error);
                }
                Ok(())
            }
            Err(error) => {
                self.heartbeat_repair_pending.fetch_add(1, Ordering::SeqCst);
                Err(error)
            }
        }
    }

    fn enter_standby(&mut self) -> Result<()> {
        self.prepare_state_update(ClientLifecycleState::Standby, "enter_standby")
            .publish()
    }

    fn activate(&mut self) -> Result<()> {
        self.prepare_state_update(ClientLifecycleState::Active, "activate")
            .publish()
    }

    fn enter_draining(&mut self) -> Result<()> {
        self.prepare_state_update(ClientLifecycleState::Draining, "enter_draining")
            .publish()
    }

    fn plan_handoff(
        &self,
        successor_epoch: ClientEpoch,
        kind: HandoffKind,
        barrier_version: u64,
        created_at_ms: u64,
        deadline_ms: Option<u64>,
    ) -> Result<HandoffPlan> {
        let _span = info_span!(
            "store.plan_handoff",
            runtime = %self.lease.runtime,
            successor_epoch = successor_epoch.0,
            barrier_version,
            kind = ?kind
        )
        .entered();
        let tracker = OperationTracker::new("plan_handoff");
        let plan = HandoffPlan {
            stable_id: self.lease.runtime.stable_id.clone(),
            from: self.lease.runtime.clone(),
            to: ClientRuntimeId {
                stable_id: self.lease.runtime.stable_id.clone(),
                epoch: successor_epoch,
            },
            kind,
            barrier_version,
            created_at_ms,
            deadline_ms,
        };
        let put_result = self.metadata.put_handoff(&plan);
        tracker.finish(&put_result, 0);
        put_result?;
        Ok(plan)
    }

    fn mount_segment(&self, capacity_bytes: u64, used_bytes: u64, tags: Vec<String>) -> Result<()> {
        let _span = info_span!(
            "store.mount_segment",
            runtime = %self.lease.runtime,
            capacity_bytes,
            used_bytes
        )
        .entered();
        let tracker = OperationTracker::new("mount_segment").input_bytes(capacity_bytes);
        let segment_name = self.segment_name()?;
        let segment = SegmentAnnouncement {
            owner: self.lease.runtime.clone(),
            transport_endpoint: self.local_transport_endpoint(&segment_name),
            transport_segment_descriptor: self.local_transport_segment_descriptor(&segment_name),
            segment_name,
            capacity_bytes,
            used_bytes,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: self.local_memory.alignment as u64,
            tags,
        };
        self.allocator.lock().upsert(&segment);
        let result = self.metadata.publish_segment(&segment);
        tracker.finish(&result, used_bytes);
        registry::record_segment_lifecycle(
            "mount_segment",
            if result.is_ok() { "ok" } else { "error" },
        );
        result
    }

    fn list_segments(&self) -> Result<Vec<SegmentAnnouncement>> {
        let local = self.allocator.lock().announcements();
        if !local.is_empty() {
            return Ok(local);
        }
        self.metadata.list_segments(Some(&self.lease.runtime))
    }

    fn expand_local_memory(&self, storage_bytes: usize) -> Result<SegmentAnnouncement> {
        let _span = info_span!(
            "store.expand_local_memory",
            runtime = %self.lease.runtime,
            storage_bytes
        )
        .entered();
        let tracker =
            OperationTracker::new("expand_local_memory").input_bytes(storage_bytes as u64);
        self.ensure_local_memory()?;
        if storage_bytes == 0 {
            let result = Err(StoreError::Allocator(
                "expanded storage_bytes must be greater than zero".to_string(),
            ));
            tracker.finish(&result, 0);
            return result;
        }
        let primary = self.segment_name()?;
        let mut announcements = Vec::new();
        for plan in self.storage_segment_plans(storage_bytes)? {
            let attach_primary = {
                let state = self.state.lock();
                let memory = state.memory_ref()?;
                !memory.has_storage_segment(&primary) && memory.storage_segments().is_empty()
            };
            let segment_name = if attach_primary {
                primary.clone()
            } else {
                let mut state = self.state.lock();
                state.next_segment_name(&primary)
            };
            let transport = if attach_primary {
                self.transport.clone().ok_or_else(|| {
                    StoreError::Unsupported("transport is not configured".to_string())
                })?
            } else {
                self.transport_factory()?.create(&segment_name.0)?
            };
            {
                let mut state = self.state.lock();
                state.memory_mut()?.add_storage_segment(
                    transport.as_ref(),
                    StorageSegmentSpec {
                        segment_name: segment_name.clone(),
                        capacity_bytes: plan.capacity_bytes,
                        state: SegmentLifecycleState::Active,
                        tags: self.local_memory.tags.clone(),
                        location: plan.location,
                        alignment: self.local_memory.alignment,
                        hugepage_enabled: self.local_memory.hugepage_enabled,
                        hugepage_size_bytes: self.local_memory.hugepage_size_bytes,
                    },
                )?;
                if !attach_primary {
                    state
                        .local_transports
                        .insert(segment_name.0.clone(), transport);
                }
            };
            let segment_info = self
                .state
                .lock()
                .memory_ref()?
                .storage_segments()
                .into_iter()
                .find(|entry| entry.segment_name == segment_name)
                .ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "expanded storage segment {} is missing",
                        segment_name.0
                    ))
                })?;
            let mut announcement = segment_info.announcement(self.lease.runtime.clone(), 0);
            announcement.transport_endpoint =
                self.local_transport_endpoint(&announcement.segment_name);
            announcement.transport_segment_descriptor =
                self.local_transport_segment_descriptor(&announcement.segment_name);
            self.allocator.lock().upsert(&announcement);
            info!(
                runtime = %self.lease.runtime,
                segment = %announcement.segment_name.0,
                storage_bytes = announcement.capacity_bytes,
                "expanded local memory with a new active segment"
            );
            self.metadata.publish_segment(&announcement)?;
            announcements.push(announcement);
        }
        let result = announcements.into_iter().next().ok_or_else(|| {
            StoreError::InvalidState("expand_local_memory produced no segment".to_string())
        });
        tracker.finish(&result, 0);
        registry::record_segment_lifecycle(
            "expand_local_memory",
            if result.is_ok() { "ok" } else { "error" },
        );
        result
    }

    fn drain_segment(&self, segment: &SegmentName) -> Result<()> {
        let _span = info_span!(
            "store.drain_segment",
            runtime = %self.lease.runtime,
            segment = %segment.0
        )
        .entered();
        let tracker = OperationTracker::new("drain_segment");
        let result = self.drain_segment_internal(segment, false);
        if result.is_ok() {
            info!(
                runtime = %self.lease.runtime,
                segment = %segment.0,
                "segment entered draining state"
            );
        }
        tracker.finish(&result, 0);
        registry::record_segment_lifecycle(
            "drain_segment",
            if result.is_ok() { "ok" } else { "error" },
        );
        result
    }

    fn retire_segment(&self, segment: &SegmentName) -> Result<bool> {
        let _span = info_span!(
            "store.retire_segment",
            runtime = %self.lease.runtime,
            segment = %segment.0
        )
        .entered();
        let tracker = OperationTracker::new("retire_segment");
        self.ensure_local_memory()?;
        let announcement = self
            .allocator
            .lock()
            .announcement(segment)
            .ok_or_else(|| StoreError::NotFound(format!("segment {} not found", segment.0)))?;
        if announcement.state != SegmentLifecycleState::Draining {
            let result = Err(StoreError::InvalidState(format!(
                "segment {} is not draining",
                segment.0
            )));
            tracker.finish(&result, 0);
            return result;
        }
        if announcement.used_bytes != 0 {
            let result = Ok(false);
            tracker.finish(&result, 0);
            return result;
        }
        {
            let mut state = self.state.lock();
            let transport = if segment == &self.segment_name()? {
                self.transport.clone().ok_or_else(|| {
                    StoreError::Unsupported("transport is not configured".to_string())
                })?
            } else {
                state.local_transports.remove(&segment.0).ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "local transport for segment {} not found",
                        segment.0
                    ))
                })?
            };
            state
                .memory_mut()?
                .remove_storage_segment(transport.as_ref(), segment)?;
            self.allocator.lock().remove(segment);
        }
        info!(
            runtime = %self.lease.runtime,
            segment = %segment.0,
            "retired drained segment"
        );
        let result = self
            .metadata
            .unpublish_segment(&self.lease.runtime, segment)
            .map(|_| true);
        tracker.finish(&result, 0);
        if result.as_ref().copied().unwrap_or(false) {
            registry::record_segment_lifecycle("retire_segment", "ok");
        } else if result.is_err() {
            registry::record_segment_lifecycle("retire_segment", "error");
        }
        result
    }

    fn evacuate_owned_replicas(&mut self) -> Result<usize> {
        let _span = info_span!(
            "store.evacuate_owned_replicas",
            runtime = %self.lease.runtime
        )
        .entered();
        let tracker = OperationTracker::new("evacuate_owned_replicas");
        let result: Result<usize> = (|| {
            self.ensure_local_memory()?;
            if self.lifecycle_state() != ClientLifecycleState::Draining {
                self.enter_draining()?;
            }
            self.evacuate_owned_replicas_when_draining()
        })();
        tracker.finish(&result, 0);
        result
    }

    fn query_route(&self, key: &str) -> Result<Option<ObjectRoute>> {
        self.query_route_in_tenant(self.default_tenant(), key)
    }

    fn query_route_in_tenant(&self, tenant: &str, key: &str) -> Result<Option<ObjectRoute>> {
        self.query_route_in_scope(&NamespaceScope::with_defaults(Some(tenant), None, None), key)
    }

    fn query_route_in_scope(
        &self,
        scope: &NamespaceScope,
        logical_key: &str,
    ) -> Result<Option<ObjectRoute>> {
        self.query_route_by_object_id(&LogicalObjectId::new(scope.clone(), logical_key))
    }

    fn query_route_by_object_id(&self, object_id: &LogicalObjectId) -> Result<Option<ObjectRoute>> {
        let tracker = OperationTracker::new("query_route");
        let result = self.query_route_by_object_id_inner(object_id);
        tracker.finish(&result, result.as_ref().map(|route| route.is_some() as u64).unwrap_or(0));
        result
    }

    fn list_routes_in_scope(&self, scope: &NamespaceScope) -> Result<Vec<ObjectRoute>> {
        self.route_ops().list_routes_in_scope(scope)
    }

    fn list_reuse_candidates(&self, reuse: &mooncake_store_core::ReuseIdentity) -> Result<Vec<ObjectRoute>> {
        self.route_ops().list_reuse_candidates(reuse)
    }

    fn cas_route(
        &self,
        key: &str,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        self.cas_route_in_tenant(self.default_tenant(), key, expected, next)
    }

    fn cas_route_in_tenant(
        &self,
        tenant: &str,
        key: &str,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let object_id = mooncake_store_core::scoped_logical_object_id(tenant, key);
        let object_key = mooncake_store_core::ObjectKey::from_logical_id(&object_id);
        self.route_ops()
            .compare_and_swap_route(&object_key, expected, next)
    }

    fn register_local_memory(&self) -> Result<()> {
        let _span =
            info_span!("store.register_local_memory", runtime = %self.lease.runtime).entered();
        let tracker = OperationTracker::new("register_local_memory");
        let result: Result<()> = (|| {
            self.ensure_local_memory()?;
            self.complete_startup_activation_after_local_memory_registration()
        })();
        tracker.finish(&result, 0);
        result
    }

    fn register_buffer(&self, buffer: *mut c_void, size: usize) -> Result<()> {
        let _span = info_span!(
            "store.register_buffer",
            runtime = %self.lease.runtime,
            size
        )
        .entered();
        let tracker = OperationTracker::new("register_buffer").input_bytes(size as u64);
        if size == 0 {
            let result = Err(StoreError::Allocator(
                "registered buffer size must be greater than zero".to_string(),
            ));
            tracker.finish(&result, 0);
            return result;
        }
        let transport = self.transport()?;
        let mut state = self.state.lock();
        let result = state.register_external_buffer(transport, buffer, size);
        tracker.finish(&result, 0);
        result
    }

    fn unregister_buffer(&self, buffer: *mut c_void, size: usize) -> Result<()> {
        let _span = info_span!(
            "store.unregister_buffer",
            runtime = %self.lease.runtime,
            size
        )
        .entered();
        let tracker = OperationTracker::new("unregister_buffer").input_bytes(size as u64);
        let transport = self.transport()?;
        let mut state = self.state.lock();
        let result = state.unregister_external_buffer(transport, buffer, size);
        tracker.finish(&result, 0);
        result
    }

    fn get_hostname(&self) -> Result<String> {
        Ok(self.lease.endpoints.rpc_address.clone())
    }

    fn get_size(&self, key: &str) -> Result<usize> {
        self.get_size_in_tenant(self.default_tenant(), key)
    }

    fn get_size_in_tenant(&self, tenant: &str, key: &str) -> Result<usize> {
        let tracker = OperationTracker::new("get_size");
        let result = (|| {
            let object_id = mooncake_store_core::scoped_logical_object_id(tenant, key);
            let Some(route) = self.query_route_by_object_id_inner(&object_id)? else {
                return Ok(0);
            };
            Ok(self
                .select_readable_replica_with_refresh(&route)?
                .map(|replica| replica.length as usize)
                .unwrap_or(0))
        })();
        tracker.finish(&result, result.as_ref().copied().unwrap_or_default() as u64);
        result
    }

    fn is_exist(&self, key: &str) -> Result<bool> {
        self.is_exist_in_tenant(self.default_tenant(), key)
    }

    fn is_exist_in_tenant(&self, tenant: &str, key: &str) -> Result<bool> {
        Ok(self.query_route_in_tenant(tenant, key)?.is_some())
    }

    fn batch_is_exist(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<bool>> {
        let tracker = OperationTracker::new("batch_is_exist")
            .attribute_u64("mooncake.item_count", objects.len() as u64);
        let result: Result<Vec<bool>> = (|| {
            let keys = objects
                .iter()
                .map(|object| {
                    let tenant = object.tenant.unwrap_or(self.default_tenant());
                    let scope =
                        NamespaceScope::with_defaults(Some(tenant), object.domain, object.object_set);
                    ObjectKey::from_scope(&scope, object.key)
                })
                .collect::<Vec<_>>();
            let routes = self.query_routes_by_object_keys_bounded(&keys)?;
            self.route_ops().report_route_hits(&routes, self);
            Ok(routes
                .into_iter()
                .map(|route| route.is_some())
                .collect())
        })();
        tracker.finish(&result, result.as_ref().map(|items| items.len()).unwrap_or(0) as u64);
        result
    }

    fn remove(&self, key: &str, force: bool) -> Result<()> {
        self.remove_in_tenant(self.default_tenant(), key, force)
    }

    fn remove_in_tenant(&self, tenant: &str, key: &str, force: bool) -> Result<()> {
        let _span = info_span!(
            "store.remove",
            runtime = %self.lease.runtime,
            tenant,
            key,
            force
        )
        .entered();
        let tracker = OperationTracker::new("remove");
        let object_id = mooncake_store_core::scoped_logical_object_id(tenant, key);
        let object_key = mooncake_store_core::ObjectKey::from_logical_id(&object_id);
        let result = self.remove_object_route_with_retry(&object_id, &object_key, force);
        tracker.finish(&result, 0);
        result
    }

    fn batch_remove(&self, objects: &[ObjectRef<'_>], force: bool) -> Result<()> {
        let _span = info_span!(
            "store.batch_remove",
            runtime = %self.lease.runtime,
            items = objects.len(),
            force
        )
        .entered();
        let tracker = OperationTracker::new("batch_remove");
        for object in objects {
            let tenant = object.tenant.unwrap_or(self.default_tenant());
            let object_id = LogicalObjectId::new(
                NamespaceScope::with_defaults(Some(tenant), object.domain, object.object_set),
                object.key,
            );
            let object_key = mooncake_store_core::ObjectKey::from_logical_id(&object_id);
            if let Err(error) = self.remove_object_route_with_retry(&object_id, &object_key, force) {
                let result: Result<()> = Err(error.clone());
                tracker.finish(&result, 0);
                return Err(error);
            }
        }
        let result = Ok(());
        tracker.finish(&result, 0);
        result
    }

    fn put(&self, key: &str, value: &[u8]) -> Result<ObjectRoute> {
        self.put_in_tenant(self.default_tenant(), key, value)
    }

    fn put_in_tenant(&self, tenant: &str, key: &str, value: &[u8]) -> Result<ObjectRoute> {
        let _span = info_span!(
            "store.put",
            runtime = %self.lease.runtime,
            tenant,
            key,
            bytes = value.len()
        )
        .entered();
        let tracker = OperationTracker::new("put")
            .attribute_str("mooncake.tenant", tenant)
            .attribute_u64("mooncake.item_count", 1)
            .input_bytes(value.len() as u64);
        let result = self.put_object(&ObjectRef::new(key).tenant(tenant), value, None);
        tracker.finish(&result, value.len() as u64);
        result
    }

    fn put_with_policy(
        &self,
        key: &str,
        value: &[u8],
        policy: &ReplicationPolicy,
    ) -> Result<ObjectRoute> {
        self.put_in_tenant_with_policy(self.default_tenant(), key, value, policy)
    }

    fn put_in_tenant_with_policy(
        &self,
        tenant: &str,
        key: &str,
        value: &[u8],
        policy: &ReplicationPolicy,
    ) -> Result<ObjectRoute> {
        let _span = info_span!(
            "store.put",
            runtime = %self.lease.runtime,
            tenant,
            key,
            bytes = value.len()
        )
        .entered();
        let tracker = OperationTracker::new("put")
            .attribute_str("mooncake.tenant", tenant)
            .attribute_u64("mooncake.item_count", 1)
            .input_bytes(value.len() as u64);
        let result = self.put_object(&ObjectRef::new(key).tenant(tenant), value, Some(policy));
        tracker.finish(&result, value.len() as u64);
        result
    }

    fn put_from(&self, key: &str, buffer: *const c_void, size: usize) -> Result<ObjectRoute> {
        self.put_from_in_tenant(self.default_tenant(), key, buffer, size)
    }

    fn put_from_in_tenant(
        &self,
        tenant: &str,
        key: &str,
        buffer: *const c_void,
        size: usize,
    ) -> Result<ObjectRoute> {
        let _span = info_span!(
            "store.put_from",
            runtime = %self.lease.runtime,
            tenant,
            key,
            size
        )
        .entered();
        let tracker = OperationTracker::new("put_from")
            .attribute_str("mooncake.tenant", tenant)
            .attribute_u64("mooncake.item_count", 1)
            .input_bytes(size as u64);
        if buffer.is_null() {
            let result = Err(StoreError::Allocator(
                "put_from buffer must not be null".to_string(),
            ));
            tracker.finish(&result, 0);
            return result;
        }
        {
            let state = self.state.lock();
            if !state.buffer_is_registered(buffer.cast_mut(), size) {
                let result = Err(StoreError::Allocator(format!(
                    "put_from buffer is not registered for tenant={tenant} key={key}"
                )));
                tracker.finish(&result, 0);
                return result;
            }
        }
        let result = self.put_object_from_registered(
            &ObjectRef::new(key).tenant(tenant),
            buffer.cast_mut(),
            size,
            None,
        );
        tracker.finish(&result, size as u64);
        result
    }

    fn put_from_with_policy(
        &self,
        key: &str,
        buffer: *const c_void,
        size: usize,
        policy: &ReplicationPolicy,
    ) -> Result<ObjectRoute> {
        self.put_from_in_tenant_with_policy(self.default_tenant(), key, buffer, size, policy)
    }

    fn put_from_in_tenant_with_policy(
        &self,
        tenant: &str,
        key: &str,
        buffer: *const c_void,
        size: usize,
        policy: &ReplicationPolicy,
    ) -> Result<ObjectRoute> {
        let _span = info_span!(
            "store.put_from",
            runtime = %self.lease.runtime,
            tenant,
            key,
            size
        )
        .entered();
        let tracker = OperationTracker::new("put_from")
            .attribute_str("mooncake.tenant", tenant)
            .attribute_u64("mooncake.item_count", 1)
            .input_bytes(size as u64);
        if buffer.is_null() {
            let result = Err(StoreError::Allocator(
                "put_from buffer must not be null".to_string(),
            ));
            tracker.finish(&result, 0);
            return result;
        }
        {
            let state = self.state.lock();
            if !state.buffer_is_registered(buffer.cast_mut(), size) {
                let result = Err(StoreError::Allocator(format!(
                    "put_from buffer is not registered for tenant={tenant} key={key}"
                )));
                tracker.finish(&result, 0);
                return result;
            }
        }
        let result = self.put_object_from_registered(
            &ObjectRef::new(key).tenant(tenant),
            buffer.cast_mut(),
            size,
            Some(policy),
        );
        tracker.finish(&result, size as u64);
        result
    }

    fn batch_put(&self, requests: &[PutRequest<'_>]) -> Result<Vec<ObjectRoute>> {
        let bytes_in = requests
            .iter()
            .map(|request| request.value.len() as u64)
            .sum::<u64>();
        let _span = info_span!(
            "store.batch_put",
            runtime = %self.lease.runtime,
            items = requests.len(),
            bytes_in
        )
        .entered();
        let tracker = OperationTracker::new("batch_put")
            .attribute_u64("mooncake.item_count", requests.len() as u64)
            .input_bytes(bytes_in);
        if matches!(self.write_mode, WriteMode::Routed { .. }) {
            if let Some(shared_policy) = Self::shared_batch_replication_policy(requests) {
                let result = self.batch_put_scoped_routed(requests, None, shared_policy.as_ref());
                tracker.finish(&result, bytes_in);
                return result;
            }
        }
        let mut routes = Vec::with_capacity(requests.len());
        for request in requests {
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
            routes.push(self.put_object(&object, request.value, request.policy.as_ref())?);
        }
        let result = Ok(routes);
        tracker.finish(&result, bytes_in);
        result
    }

    fn batch_put_from(&self, requests: &[PutFromRequest<'_>]) -> Result<Vec<ObjectRoute>> {
        let statuses = self.batch_put_from_statuses(requests);
        Self::aggregate_batch_put_statuses(&statuses)
    }

    fn batch_put_from_multi_buffers(
        &self,
        requests: &[MultiBufferPutRequest<'_>],
    ) -> Result<Vec<ObjectRoute>> {
        let bytes_in = requests
            .iter()
            .flat_map(|request| request.buffers.iter())
            .map(|buffer| buffer.len() as u64)
            .sum::<u64>();
        let _span = info_span!(
            "store.batch_put_from_multi_buffers",
            runtime = %self.lease.runtime,
            items = requests.len(),
            bytes_in
        )
        .entered();
        let tracker = OperationTracker::new("batch_put_from_multi_buffers")
            .attribute_u64("mooncake.item_count", requests.len() as u64)
            .input_bytes(bytes_in);
        let mut routes = Vec::with_capacity(requests.len());
        for request in requests {
            let payload = flatten_slices(request.buffers);
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
            routes.push(self.put_object(&object, &payload, request.policy.as_ref())?);
        }
        let result = Ok(routes);
        tracker.finish(&result, bytes_in);
        result
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.get_in_tenant(self.default_tenant(), key)
    }

    fn get_in_tenant(&self, tenant: &str, key: &str) -> Result<Vec<u8>> {
        let _span = info_span!(
            "store.get",
            runtime = %self.lease.runtime,
            tenant,
            key
        )
        .entered();
        let tracker = OperationTracker::new("get")
            .attribute_str("mooncake.tenant", tenant)
            .attribute_u64("mooncake.item_count", 1);
        let objects = [ObjectRef::new(key).tenant(tenant)];
        let result = Self::expect_exactly_one(self.batch_get(&objects)?, "batch_get");
        let bytes_out = result.as_ref().map(|value| value.len() as u64).unwrap_or(0);
        tracker.finish(&result, bytes_out);
        result
    }

    fn get_into(&self, key: &str, buffer: &mut [u8]) -> Result<usize> {
        self.get_into_in_tenant(self.default_tenant(), key, buffer)
    }

    fn get_into_in_tenant(&self, tenant: &str, key: &str, buffer: &mut [u8]) -> Result<usize> {
        let _span = info_span!(
            "store.get_into",
            runtime = %self.lease.runtime,
            tenant,
            key,
            buffer_capacity = buffer.len()
        )
        .entered();
        let tracker = OperationTracker::new("get_into")
            .attribute_str("mooncake.tenant", tenant)
            .attribute_u64("mooncake.item_count", 1)
            .attribute_u64("mooncake.buffer_capacity", buffer.len() as u64);
        let mut requests = [GetRequest::new(key, buffer).tenant(tenant)];
        let result = Self::expect_exactly_one(self.batch_get_into(&mut requests)?, "batch_get_into");
        let bytes_out = result.as_ref().copied().unwrap_or(0) as u64;
        tracker.finish(&result, bytes_out);
        result
    }

    fn batch_get(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<Vec<u8>>> {
        let _span = info_span!(
            "store.batch_get",
            runtime = %self.lease.runtime,
            items = objects.len()
        )
        .entered();
        let tracker = OperationTracker::new("batch_get")
            .attribute_u64("mooncake.item_count", objects.len() as u64);
        let mut resolved = self.resolve_objects(objects)?;
        let mut buffers = resolved
            .iter()
            .map(|entry| vec![0u8; entry.replica.length as usize])
            .collect::<Vec<_>>();
        let mut slices = buffers
            .iter_mut()
            .map(Vec::as_mut_slice)
            .collect::<Vec<_>>();
        self.execute_batch_get_into(&mut resolved, &mut slices)?;
        let bytes_out = buffers
            .iter()
            .map(|buffer| buffer.len() as u64)
            .sum::<u64>();
        let result = Ok(buffers);
        tracker.finish(&result, bytes_out);
        result
    }

    fn batch_get_buffer(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<Vec<u8>>> {
        self.batch_get(objects)
    }

    fn batch_get_into(&self, requests: &mut [GetRequest<'_>]) -> Result<Vec<usize>> {
        let _span = info_span!(
            "store.batch_get_into",
            runtime = %self.lease.runtime,
            items = requests.len()
        )
        .entered();
        let tracker = OperationTracker::new("batch_get_into")
            .attribute_u64("mooncake.item_count", requests.len() as u64);
        let objects = requests
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
        let mut resolved = self.resolve_objects(&objects)?;
        let mut buffers = requests
            .iter_mut()
            .map(|request| &mut *request.buffer)
            .collect::<Vec<_>>();
        let sizes = self.execute_batch_get_into(&mut resolved, &mut buffers)?;
        drop(buffers);
        let bytes_out = sizes.iter().copied().sum::<usize>() as u64;
        self.record_batch_get_into_items(&tracker, requests, &sizes, bytes_out);
        let result = Ok(sizes);
        tracker.finish(&result, bytes_out);
        result
    }

    fn batch_get_into_multi_buffers(
        &self,
        requests: &mut [MultiBufferGetRequest<'_>],
    ) -> Result<Vec<usize>> {
        let _span = info_span!(
            "store.batch_get_into_multi_buffers",
            runtime = %self.lease.runtime,
            items = requests.len()
        )
        .entered();
        let tracker = OperationTracker::new("batch_get_into_multi_buffers")
            .attribute_u64("mooncake.item_count", requests.len() as u64);
        let objects = requests
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
        let payloads = self.batch_get(&objects)?;
        let mut sizes = Vec::with_capacity(requests.len());
        for (request, payload) in requests.iter_mut().zip(payloads.iter()) {
            let total = request
                .buffers
                .iter()
                .map(|buffer| buffer.len())
                .sum::<usize>();
            if total < payload.len() {
                return Err(StoreError::Allocator(format!(
                    "multi-buffer capacity too small for key {}: have={} need={}",
                    request.key,
                    total,
                    payload.len()
                )));
            }
            scatter_into_buffers(payload, request.buffers);
            sizes.push(payload.len());
        }
        let bytes_out = sizes.iter().copied().sum::<usize>() as u64;
        let result = Ok(sizes);
        tracker.finish(&result, bytes_out);
        result
    }
}

impl StoreClient {
    pub fn sync_lifecycle_state(&self, next_state: ClientLifecycleState) {
        let _route_write_guard = self.route_write_gate.lock();
        self.set_lifecycle_state(next_state);
    }

    pub fn health_channel(&self) -> HealthChannel {
        HealthChannel::new(self.metadata.clone(), self.lease(), self.lease_ttl_ms)
    }

    pub fn evacuate_owned_replicas_when_draining(&self) -> Result<usize> {
        self.evacuate_draining_routes_until_stable(
            "client shrink still has live bytes on local segments",
            |route| self.migrate_owned_route(route),
        )
    }

    pub fn prepare_heartbeat(&mut self, expires_at_ms: u64) -> HeartbeatLease {
        self.lease.state = self.lifecycle_state();
        self.lease.expires_at_ms = expires_at_ms;
        HealthUpdate::heartbeat(self.metadata.clone(), self.lease.clone())
    }

    pub fn prepare_state_update(
        &mut self,
        next_state: ClientLifecycleState,
        operation: &'static str,
    ) -> HealthUpdate {
        let _route_write_guard = self.route_write_gate.lock();
        self.lease.state = next_state;
        self.lease.expires_at_ms = self
            .lease
            .expires_at_ms
            .max(now_ms().saturating_add(self.lease_ttl_ms));
        self.set_lifecycle_state(next_state);
        HealthUpdate::state_transition(self.metadata.clone(), self.lease.clone(), operation)
    }
}

impl Drop for StoreClient {
    fn drop(&mut self) {
        self.membership_sync.shutdown();
        self.control_client.clear_channels();
        self._control_plane.shutdown();
        let Some(transport) = self.transport.as_deref() else {
            return;
        };
        let mut state = self.state.lock();
        for (_, handle) in std::mem::take(&mut state.remote_segments) {
            let _ = transport.close_segment(handle);
        }
        if let Some(mut memory) = state.memory.take() {
            let segments = memory.storage_segments();
            for segment in segments {
                let local_transport = if segment.segment_name
                    == self
                        .segment_name()
                        .unwrap_or_else(|_| SegmentName::new("__missing_primary_segment__"))
                {
                    self.transport.clone()
                } else {
                    state.local_transports.remove(&segment.segment_name.0)
                };
                if let Some(local_transport) = local_transport {
                    let _ = memory
                        .remove_storage_segment(local_transport.as_ref(), &segment.segment_name);
                }
            }
            let _ = memory.release_scratch(transport);
        }
    }
}
