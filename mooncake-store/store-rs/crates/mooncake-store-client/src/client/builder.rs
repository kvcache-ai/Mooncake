#[cfg(not(test))]
const DEFAULT_STARTUP_PREWARM_MAX_DELAY: Duration = Duration::from_millis(250);
#[cfg(test)]
const DEFAULT_STARTUP_PREWARM_MAX_DELAY: Duration = Duration::ZERO;
const STARTUP_PREWARM_SPREAD_ENV: &str = "MC_STORE_RS_STARTUP_PREWARM_SPREAD_MS";

fn validate_startup_conflicts(
    metadata: &dyn MetadataBackend,
    control_client: &ControlPlaneClient,
    local: &ClientLease,
) -> Result<()> {
    let Some(local_segment) = local.endpoints.segment_name.as_ref() else {
        return validate_runtime_conflicts(metadata, control_client, local, None);
    };
    validate_runtime_conflicts(metadata, control_client, local, Some(local_segment))
}

fn validate_runtime_conflicts(
    metadata: &dyn MetadataBackend,
    control_client: &ControlPlaneClient,
    local: &ClientLease,
    local_segment: Option<&SegmentName>,
) -> Result<()> {
    if let Some(segment) = local_segment {
        validate_segment_owner_conflict(metadata, control_client, local, segment)?;
    }
    Ok(())
}

fn validate_segment_owner_conflict(
    metadata: &dyn MetadataBackend,
    control_client: &ControlPlaneClient,
    local: &ClientLease,
    segment: &SegmentName,
) -> Result<()> {
    for remote in metadata.list_live_clients()? {
        if remote.runtime == local.runtime || remote.state != ClientLifecycleState::Active {
            continue;
        }
        let lease_names_segment = remote
            .endpoints
            .segment_name
            .as_ref()
            .is_some_and(|name| name == segment);
        let metadata_names_segment = metadata
            .get_segment(&remote.runtime, segment)?
            .filter(|announcement| announcement.state == SegmentLifecycleState::Active)
            .is_some();
        if !lease_names_segment && !metadata_names_segment {
            continue;
        }
        if !validate_reachable_runtime(control_client, local, &remote)? {
            continue;
        }
        return Err(StoreError::Conflict(format!(
            "segment {} is already owned by live runtime {}",
            segment.0, remote.runtime
        )));
    }
    Ok(())
}

fn validate_reachable_runtime(
    control_client: &ControlPlaneClient,
    local: &ClientLease,
    remote: &ClientLease,
) -> Result<bool> {
    match control_client.probe_reachability(remote) {
        crate::control_plane::ControlPlaneReachability::Reachable => Ok(true),
        crate::control_plane::ControlPlaneReachability::Unreachable => {
            debug!(
                local_runtime = %local.runtime,
                remote_runtime = %remote.runtime,
                "startup guard ignored unreachable live lease during takeover validation"
            );
            Ok(false)
        }
        crate::control_plane::ControlPlaneReachability::Unknown(error) => {
            Err(StoreError::Conflict(format!(
                "startup guard cannot validate existing runtime {}: {error}",
                remote.runtime
            )))
        }
    }
}

fn normalize_storage_label(
    local_memory: &LocalMemoryConfig,
    labels: &mut BTreeMap<String, String>,
) -> Result<()> {
    match labels.get("storage").map(String::as_str) {
        Some("true") if !local_memory.has_storage() => Err(StoreError::InvalidState(
            "label storage=true requires storage_bytes > 0".to_string(),
        )),
        None if !local_memory.has_storage() => {
            labels.insert("storage".to_string(), "false".to_string());
            Ok(())
        }
        _ => Ok(()),
    }
}

fn normalize_route_label(local_memory: &LocalMemoryConfig, labels: &mut BTreeMap<String, String>) {
    labels.entry("route".to_string()).or_insert_with(|| {
        if local_memory.has_storage() {
            "true".to_string()
        } else {
            "false".to_string()
        }
    });
}

pub struct StoreClientBuilder {
    metadata: Arc<dyn MetadataBackend>,
    metadata_is_tenant_scoped: bool,
    stable_id: ClientStableId,
    compatibility: CompatibilityDescriptor,
    endpoints: ClientEndpointSet,
    initial_state: ClientLifecycleState,
    default_tenant: String,
    local_memory: LocalMemoryConfig,
    transport: Option<Arc<dyn StoreTransport>>,
    transport_factory: Option<Arc<dyn StoreTransportFactory>>,
    write_mode: WriteMode,
    route_control: RouteControlMode,
    route_topk: usize,
    live_client_sync_interval: Duration,
    transfer_stall_timeout: Duration,
    request_timeout_override: Option<Duration>,
    startup_prewarm_max_delay: Duration,
    activate_on_local_memory_registration: bool,
    namespace_quota: Option<NamespaceQuota>,
    execution_fairness: Option<ExecutionFairness>,
    bandwidth_shaping: Option<BandwidthShaping>,
}

impl StoreClientBuilder {
    pub fn new(metadata: Arc<dyn MetadataBackend>, stable_id: impl Into<String>) -> Self {
        Self {
            metadata: crate::observability::observe_metadata_backend(metadata),
            metadata_is_tenant_scoped: false,
            stable_id: ClientStableId::new(stable_id),
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            initial_state: ClientLifecycleState::Standby,
            default_tenant: DEFAULT_TENANT.to_string(),
            local_memory: LocalMemoryConfig::default(),
            transport: None,
            transport_factory: None,
            write_mode: WriteMode::LocalOnly,
            route_control: RouteControlMode::EmbeddedWrh,
            route_topk: DEFAULT_ROUTE_TOPK,
            live_client_sync_interval: DEFAULT_LIVE_CLIENT_SYNC_INTERVAL,
            transfer_stall_timeout: transfer_stall_timeout_from_env(),
            request_timeout_override: request_timeout_override_from_env(),
            startup_prewarm_max_delay: startup_prewarm_max_delay_from_env(),
            activate_on_local_memory_registration: false,
            namespace_quota: None,
            execution_fairness: None,
            bandwidth_shaping: None,
        }
    }

    pub fn compatibility(mut self, compatibility: CompatibilityDescriptor) -> Self {
        self.compatibility = compatibility;
        self
    }

    pub fn rpc_address(mut self, rpc_address: impl Into<String>) -> Self {
        self.endpoints.rpc_address = rpc_address.into();
        self
    }

    pub fn segment_name(mut self, segment_name: impl Into<String>) -> Self {
        self.endpoints.segment_name = Some(SegmentName::new(segment_name));
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.endpoints.labels.insert(key.into(), value.into());
        self
    }

    pub fn state(mut self, state: ClientLifecycleState) -> Self {
        self.initial_state = state;
        self
    }

    /// Sets the default tenant scope for request builders and startup policy resolution.
    ///
    /// This remains the normal way to choose which tenant's policy is resolved at bootstrap.
    /// It is not itself a tenant-policy authoring surface.
    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.default_tenant = tenant.into();
        self
    }

    pub(crate) fn tenant_scoped_metadata(mut self) -> Self {
        self.metadata_is_tenant_scoped = true;
        self
    }

    pub fn local_memory(mut self, local_memory: LocalMemoryConfig) -> Self {
        self.local_memory = local_memory;
        self
    }

    pub fn transport(mut self, transport: Arc<dyn StoreTransport>) -> Self {
        self.transport = Some(transport);
        self
    }

    pub fn transport_factory(mut self, transport_factory: Arc<dyn StoreTransportFactory>) -> Self {
        self.transport_factory = Some(transport_factory);
        self
    }

    pub fn with_tent(mut self, engine: Arc<TentEngine>) -> Self {
        self.transport = Some(Arc::new(TentStoreTransport(engine)));
        self
    }

    pub fn routed_writes(mut self, planner: PlacementPlanner, replica_count: usize) -> Self {
        self.write_mode = WriteMode::Routed {
            planner,
            replica_count: replica_count.max(1),
        };
        self
    }

    /// Sets a compatibility fallback route-control mode for startup.
    ///
    /// Admin-managed tenant policy in metadata is the preferred authoring surface for
    /// tenant-scoped routing. When a tenant policy provides routing settings, those values
    /// take precedence over this builder-local fallback.
    pub fn route_control(mut self, route_control: RouteControlMode) -> Self {
        self.route_control = route_control;
        self
    }

    /// Sets a compatibility fallback WRH route-authority fanout for startup.
    ///
    /// Admin-managed tenant policy in metadata is the preferred authoring surface for
    /// tenant-scoped routing. When a tenant policy provides `route_topk`, that value takes
    /// precedence over this builder-local fallback.
    pub fn route_topk(mut self, route_topk: usize) -> Self {
        self.route_topk = route_topk;
        self
    }

    pub fn live_client_sync_interval(mut self, interval: Duration) -> Self {
        self.live_client_sync_interval = interval;
        self
    }

    pub fn transfer_timeout(mut self, timeout: Duration) -> Self {
        self.transfer_stall_timeout = timeout.max(Duration::from_millis(1));
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout_override = Some(timeout.max(Duration::from_millis(1)));
        self
    }

    pub fn startup_prewarm_max_delay(mut self, delay: Duration) -> Self {
        self.startup_prewarm_max_delay = delay;
        self
    }

    pub fn activate_on_local_memory_registration(mut self) -> Self {
        self.activate_on_local_memory_registration = true;
        self
    }

    /// Sets a compatibility fallback quota used when no admin-managed tenant policy provides one.
    pub fn namespace_quota(mut self, namespace_quota: NamespaceQuota) -> Self {
        self.namespace_quota = Some(namespace_quota);
        self
    }

    /// Sets a compatibility fallback fairness policy used when no admin-managed tenant policy provides one.
    pub fn execution_fairness(mut self, execution_fairness: ExecutionFairness) -> Self {
        self.execution_fairness = Some(execution_fairness);
        self
    }

    /// Sets a compatibility fallback bandwidth-shaping policy used when no admin-managed tenant policy provides one.
    pub fn bandwidth_shaping(mut self, bandwidth_shaping: BandwidthShaping) -> Self {
        self.bandwidth_shaping = Some(bandwidth_shaping);
        self
    }

    pub fn build(self, expires_at_ms: u64) -> Result<StoreClient> {
        if self.default_tenant.is_empty() {
            return Err(StoreError::InvalidState(
                "default tenant must not be empty".to_string(),
            ));
        }
        if self.route_topk < 2 {
            return Err(StoreError::InvalidState(
                "route_topk must be greater than or equal to 2".to_string(),
            ));
        }
        registry::set_process_tenant(&self.default_tenant);
        let lease_ttl_ms = expires_at_ms.saturating_sub(now_ms()).max(1);

        let mut endpoints = self.endpoints;
        normalize_storage_label(&self.local_memory, &mut endpoints.labels)?;
        normalize_route_label(&self.local_memory, &mut endpoints.labels);
        if let Some(transport) = self.transport.as_ref() {
            if endpoints.rpc_address.is_empty() {
                let (host, port) = transport.rpc_server_address()?;
                endpoints.rpc_address = if port == 0 {
                    host
                } else {
                    format!("{host}:{port}")
                };
            }
            if endpoints.segment_name.is_none() {
                endpoints.segment_name = Some(SegmentName::new(transport.segment_name()?));
            }
        }

        let published_initial_state = if self.activate_on_local_memory_registration
            && self.initial_state == ClientLifecycleState::Active
        {
            ClientLifecycleState::Standby
        } else {
            self.initial_state
        };
        let startup_activation_pending = self.activate_on_local_memory_registration
            && self.initial_state == ClientLifecycleState::Active
            && published_initial_state != self.initial_state;

        let default_scope = NamespaceScope::with_defaults(Some(&self.default_tenant), None, None);
        let runtime_metadata = if self.metadata_is_tenant_scoped {
            self.metadata.clone()
        } else {
            self.metadata
                .for_tenant(&self.default_tenant)
                .unwrap_or_else(|| self.metadata.clone())
        };
        let effective_tenant_policy =
            resolve_effective_tenant_policy(runtime_metadata.as_ref(), &default_scope)?;
        let effective_namespace_quota =
            resolved_namespace_quota(&effective_tenant_policy).or(self.namespace_quota.clone());
        let effective_route_policy = route_policy_from_tenant_spec(&effective_tenant_policy);
        let effective_route_control = effective_route_policy
            .as_ref()
            .map(|policy| policy.route_control)
            .unwrap_or(self.route_control);
        let effective_route_topk = effective_route_policy
            .as_ref()
            .map(|policy| policy.route_topk as usize)
            .unwrap_or(self.route_topk);
        let state = Mutex::new(StoreState::default());
        let allocator = Arc::new(Mutex::new(LocalAllocatorState::default()));
        let lifecycle_state = Arc::new(AtomicU8::new(encode_lifecycle_state(
            published_initial_state,
        )));
        let route_write_gate = Arc::new(Mutex::new(()));
        let route_namespace = runtime_metadata.route_namespace();
        let live_client_cache = shared_live_client_cache(&route_namespace);
        let suspect_runtime_cache = shared_suspect_runtime_cache(&route_namespace);
        let control_client = Arc::new(ControlPlaneClient::new()?);

        let template = ClientLease {
            runtime: ClientRuntimeId {
                stable_id: self.stable_id.clone(),
                epoch: ClientEpoch::default(),
            },
            state: published_initial_state,
            compatibility: self.compatibility,
            endpoints: endpoints.clone(),
            expires_at_ms,
        };
        validate_startup_conflicts(runtime_metadata.as_ref(), control_client.as_ref(), &template)?;
        let runtime = runtime_metadata.allocate_client_lease(&template)?;
        let provisional_lease = ClientLease {
            runtime: runtime.clone(),
            state: template.state,
            compatibility: template.compatibility.clone(),
            endpoints: endpoints.clone(),
            expires_at_ms,
        };
        bootstrap_route_policy(
            runtime_metadata.as_ref(),
            &provisional_lease,
            &self.default_tenant,
            effective_route_control,
            effective_route_topk,
        )?;
        let local_authority = Arc::new(LocalAuthorityAdapter {
            lifecycle_state: lifecycle_state.clone(),
            route_write_gate: route_write_gate.clone(),
        });
        let route_directory = build_route_directory(
            effective_route_control,
            effective_route_topk,
            runtime_metadata.clone(),
            &provisional_lease,
            control_client.clone(),
            live_client_cache.clone(),
            suspect_runtime_cache.clone(),
        );
        let storage_owner = Arc::new(StorageOwnerState::new(
            runtime.clone(),
            provisional_lease.clone(),
            route_directory.clone(),
            allocator.clone(),
        ));
        let storage_adapter = Arc::new(LocalAllocatorAdapter {
            runtime: runtime.clone(),
            allocator: allocator.clone(),
            storage_owner: storage_owner.clone(),
            lifecycle_state: lifecycle_state.clone(),
            transfer_stall_timeout: self.transfer_stall_timeout,
            request_timeout_override: self.request_timeout_override,
        });
        let migration_adapter =
            Arc::new(LocalMigrationAdapter::new(LocalMigrationExecutionContext {
                executor_stable_id: runtime.stable_id.clone(),
                base_lease: provisional_lease.clone(),
                metadata: runtime_metadata.clone(),
                transport_factory: self.transport_factory.clone(),
                default_tenant: self.default_tenant.clone(),
                local_memory: self.local_memory.clone(),
                write_mode: self.write_mode.clone(),
                route_control: effective_route_control,
                route_topk: effective_route_topk,
                transfer_stall_timeout: self.transfer_stall_timeout,
                request_timeout_override: self.request_timeout_override,
                executions: Arc::new(Mutex::new(BTreeMap::new())),
            }));
        let control_plane = ControlPlaneHandle::spawn_with_migration(
            &control_bind_host(&endpoints.rpc_address),
            local_authority.clone(),
            storage_adapter.clone(),
            storage_adapter,
            migration_adapter,
        )?;
        crate::route_directory::bind_local_authority_service(
            &route_namespace,
            &runtime.stable_id,
            local_authority,
        );
        endpoints
            .labels
            .entry(control_address_label().to_string())
            .or_insert_with(|| control_plane.address().to_string());
        let lease = ClientLease {
            runtime: runtime.clone(),
            state: published_initial_state,
            compatibility: provisional_lease.compatibility.clone(),
            endpoints,
            expires_at_ms,
        };
        runtime_metadata.upsert_client_lease(&lease)?;
        prewarm_live_client_cache(runtime_metadata.as_ref(), &live_client_cache, &lease)?;
        let prewarm_delay = startup_prewarm_delay(&runtime, self.startup_prewarm_max_delay);
        if !prewarm_delay.is_zero() {
            std::thread::sleep(prewarm_delay);
            refresh_live_client_cache(
                runtime_metadata.as_ref(),
                &live_client_cache,
                "live_client_snapshot_prewarm",
            )?;
        }
        prewarm_segment_target_chunk_cache(
            runtime_metadata.as_ref(),
            &live_client_cache,
            &state,
            &lease,
        )?;
        live_client_cache.lock().store_tenant_quota_policy(
            self.default_tenant.clone(),
            None,
            effective_namespace_quota
                .as_ref()
                .map(|quota| TenantQuotaPolicy {
                    max_bytes: quota.max_bytes,
                    max_objects: quota.max_objects,
                }),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
        let membership_sync = MembershipSyncHandle::spawn(
            &runtime,
            runtime_metadata.clone(),
            live_client_cache.clone(),
            self.live_client_sync_interval,
        )?;
        let async_eviction = AsyncEvictionHandle::spawn(
            &runtime,
            &lease,
            &self.local_memory,
            storage_owner.clone(),
        )?;
        Ok(StoreClient {
            metadata: runtime_metadata,
            route_directory,
            _control_plane: control_plane,
            control_client,
            allocator,
            storage_owner,
            lease,
            lease_ttl_ms,
            live_client_cache,
            suspect_runtime_cache,
            membership_sync,
            _async_eviction: async_eviction,
            default_tenant: self.default_tenant,
            local_memory: self.local_memory,
            transport: self.transport,
            transport_factory: self.transport_factory,
            write_mode: self.write_mode,
            transfer_stall_timeout: self.transfer_stall_timeout,
            request_timeout_override: self.request_timeout_override,
            lifecycle_state,
            route_write_gate,
            startup_activation_pending: AtomicBool::new(startup_activation_pending),
            heartbeat_repair_pending: AtomicUsize::new(0),
            tenant_quota_reservation_counter: AtomicU64::new(1),
            route_control: effective_route_control,
            route_topk: effective_route_topk,
            namespace_quota: effective_namespace_quota,
            execution_fairness: resolved_execution_fairness(&effective_tenant_policy)
                .or(self.execution_fairness),
            bandwidth_shaping: resolved_bandwidth_shaping(&effective_tenant_policy)
                .or(self.bandwidth_shaping),
            placement_policy: resolved_placement_policy(&effective_tenant_policy),
            state,
        })
    }
}

fn bootstrap_route_policy(
    metadata: &dyn MetadataBackend,
    lease: &ClientLease,
    default_tenant: &str,
    route_control: RouteControlMode,
    route_topk: usize,
) -> Result<()> {
    let local = RoutePolicy {
        route_topk: route_topk as u32,
        route_control,
        created_by: lease.runtime.clone(),
        created_at_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    };
    bootstrap_default_route_policy(metadata, &local)?;
    match effective_route_policy(metadata, default_tenant)? {
        Some(effective) => validate_route_policy(&local, &effective),
        None => Err(StoreError::InvalidState(
            "default route policy is missing".to_string(),
        )),
    }
}

fn resolve_effective_tenant_policy(
    metadata: &dyn MetadataBackend,
    scope: &NamespaceScope,
) -> Result<TenantPolicySpec> {
    let mut resolved = TenantPolicySpec::default();
    for policy_scope in TenantPolicyScope::ancestors(scope) {
        if let Some(policy) = metadata.get_tenant_policy(&policy_scope)? {
            resolved = resolved.merged_with(&policy.spec);
        }
    }
    Ok(resolved)
}

fn route_policy_from_tenant_spec(spec: &TenantPolicySpec) -> Option<RoutePolicy> {
    let routing = spec.routing.as_ref()?;
    Some(RoutePolicy {
        route_topk: routing.route_topk?,
        route_control: routing.route_control?,
        created_by: ClientRuntimeId::new("tenant-policy", ClientEpoch(0)),
        created_at_ms: 0,
    })
}

fn effective_route_policy(
    metadata: &dyn MetadataBackend,
    default_tenant: &str,
) -> Result<Option<RoutePolicy>> {
    let scope = NamespaceScope::with_defaults(Some(default_tenant), None, None);
    let tenant_spec = resolve_effective_tenant_policy(metadata, &scope)?;
    if let Some(policy) = route_policy_from_tenant_spec(&tenant_spec) {
        return Ok(Some(policy));
    }
    if let Some(tenant_policy) =
        metadata.get_route_policy(&RoutePolicyDomain::Tenant(default_tenant.to_string()))?
    {
        return Ok(Some(tenant_policy));
    }
    metadata.get_route_policy(&RoutePolicyDomain::Default)
}

fn bootstrap_default_route_policy(
    metadata: &dyn MetadataBackend,
    local: &RoutePolicy,
) -> Result<()> {
    let domain = RoutePolicyDomain::Default;
    match metadata.get_route_policy(&domain)? {
        Some(_) => Ok(()),
        None => {
            if metadata.put_route_policy_if_absent(&domain, local)? {
                return Ok(());
            }
            metadata
                .get_route_policy(&domain)?
                .ok_or_else(|| {
                    StoreError::InvalidState(
                        "route policy bootstrap raced but no policy was readable afterward"
                            .to_string(),
                    )
                })
                .map(|_| ())
        }
    }
}

fn validate_route_policy(local: &RoutePolicy, existing: &RoutePolicy) -> Result<()> {
    if local.semantically_matches(existing) {
        return Ok(());
    }
    Err(StoreError::InvalidState(format!(
        "route policy mismatch: local(route_control={:?}, route_topk={}) metadata(route_control={:?}, route_topk={})",
        local.route_control,
        local.route_topk,
        existing.route_control,
        existing.route_topk,
    )))
}

fn resolved_namespace_quota(spec: &TenantPolicySpec) -> Option<NamespaceQuota> {
    spec.quota.as_ref().map(|quota| NamespaceQuota {
        max_bytes: quota.max_bytes,
        max_objects: quota.max_objects,
    })
}

fn resolved_execution_fairness(spec: &TenantPolicySpec) -> Option<ExecutionFairness> {
    spec.fairness.as_ref().map(|fairness| ExecutionFairness {
        max_remote_batch_items_per_tenant: fairness.max_remote_batch_items_per_tenant,
    })
}

fn resolved_bandwidth_shaping(spec: &TenantPolicySpec) -> Option<BandwidthShaping> {
    spec.shaping.as_ref().map(|shaping| BandwidthShaping {
        max_remote_batch_bytes: shaping.max_remote_batch_bytes,
        max_remote_batch_burst_items: shaping.max_remote_batch_burst_items,
        max_inflight_bytes_per_batch: shaping.max_inflight_bytes_per_batch,
    })
}

fn resolved_placement_policy(spec: &TenantPolicySpec) -> Option<TenantPlacementPolicy> {
    spec.placement.clone()
}

fn prewarm_live_client_cache(
    metadata: &dyn MetadataBackend,
    live_client_cache: &SharedLiveClientCache,
    local_lease: &ClientLease,
) -> Result<()> {
    match metadata.list_live_clients() {
        Ok(leases) => {
            let live_leases = filter_live_client_leases(&leases);
            registry::record_runtime_leases(&live_leases);
            live_client_cache.lock().store(live_leases);
            Ok(())
        }
        Err(StoreError::Unsupported(_)) => {
            registry::record_runtime_leases(std::slice::from_ref(local_lease));
            live_client_cache.lock().store(vec![local_lease.clone()]);
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn prewarm_segment_target_chunk_cache(
    metadata: &dyn MetadataBackend,
    live_client_cache: &SharedLiveClientCache,
    state: &Mutex<StoreState>,
    local_lease: &ClientLease,
) -> Result<()> {
    let leases = cached_live_client_snapshot(live_client_cache)?;
    for lease in leases {
        if lease.runtime == local_lease.runtime
            || !compatibility_matches(local_lease, &lease)
            || !lease.state.allows_new_writes()
            || lease
                .endpoints
                .labels
                .get("storage")
                .is_none_or(|value| value != "true")
        {
            continue;
        }
        let Some(segment_name) = lease.endpoints.segment_name.as_ref() else {
            continue;
        };
        match metadata.get_segment(&lease.runtime, segment_name) {
            Ok(Some(segment))
                if segment.state == SegmentLifecycleState::Active
                    && !segment.target_chunks.is_empty() =>
            {
                state.lock().cache_segment_target_metadata(
                    &lease.runtime,
                    segment_name,
                    &segment.target_chunks,
                    segment.transport_endpoint.clone(),
                    segment.transport_segment_descriptor.clone(),
                );
            }
            Ok(_) => {}
            Err(error) => {
                debug!(
                    runtime = %local_lease.runtime,
                    storage_runtime = %lease.runtime,
                    segment = %segment_name.0,
                    error = %error,
                    "segment target chunk prewarm skipped"
                );
            }
        }
    }
    Ok(())
}

fn startup_prewarm_max_delay_from_env() -> Duration {
    std::env::var(STARTUP_PREWARM_SPREAD_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_STARTUP_PREWARM_MAX_DELAY)
}

fn startup_prewarm_delay(runtime: &ClientRuntimeId, max_delay: Duration) -> Duration {
    let max_delay_ms = max_delay.as_millis().min(u128::from(u64::MAX)) as u64;
    Duration::from_millis(stable_phase_spread_ms(
        &runtime.to_string(),
        max_delay_ms,
        "startup_prewarm",
    ))
}
