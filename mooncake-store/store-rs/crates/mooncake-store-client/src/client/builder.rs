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
    for remote in metadata.list_live_clients()? {
        let same_stable = remote.runtime.stable_id == local.runtime.stable_id;
        let same_segment = local_segment.is_some_and(|segment| {
            remote.endpoints.segment_name.as_ref().is_some_and(|other| other == segment)
        });
        if !same_stable && !same_segment {
            continue;
        }

        match control_client.probe_reachability(&remote) {
            crate::control_plane::ControlPlaneReachability::Reachable => {}
            crate::control_plane::ControlPlaneReachability::Unreachable => {
                debug!(
                    local_runtime = %local.runtime,
                    remote_runtime = %remote.runtime,
                    "startup guard ignored unreachable live lease during takeover validation"
                );
                continue;
            }
            crate::control_plane::ControlPlaneReachability::Unknown(error) => {
                return Err(StoreError::Conflict(format!(
                    "startup guard cannot validate existing runtime {}: {error}",
                    remote.runtime
                )));
            }
        }

        if same_stable && remote.runtime.epoch > local.runtime.epoch {
            return Err(StoreError::StaleEpoch(format!(
                "runtime {} is fenced by newer live runtime {}",
                local.runtime, remote.runtime
            )));
        }

        if same_stable && remote.runtime.epoch == local.runtime.epoch {
            return Err(StoreError::Conflict(format!(
                "runtime {} is already live",
                remote.runtime
            )));
        }

        if same_segment && remote.runtime != local.runtime {
            let segment = local_segment.expect("segment guard only runs with segment");
            return Err(StoreError::Conflict(format!(
                "segment {} is already owned by live runtime {}",
                segment.0, remote.runtime
            )));
        }
    }

    Ok(())
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
    stable_id: ClientStableId,
    epoch: ClientEpoch,
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
            metadata,
            stable_id: ClientStableId::new(stable_id),
            epoch: ClientEpoch(1),
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

    pub fn epoch(mut self, epoch: ClientEpoch) -> Self {
        self.epoch = epoch;
        self
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

    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.default_tenant = tenant.into();
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
        self.transport = Some(engine);
        self
    }

    pub fn routed_writes(mut self, planner: PlacementPlanner, replica_count: usize) -> Self {
        self.write_mode = WriteMode::Routed {
            planner,
            replica_count: replica_count.max(1),
        };
        self
    }

    pub fn route_control(mut self, route_control: RouteControlMode) -> Self {
        self.route_control = route_control;
        self
    }

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

    pub fn namespace_quota(mut self, namespace_quota: NamespaceQuota) -> Self {
        self.namespace_quota = Some(namespace_quota);
        self
    }

    pub fn execution_fairness(mut self, execution_fairness: ExecutionFairness) -> Self {
        self.execution_fairness = Some(execution_fairness);
        self
    }

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
        let startup_activation_pending =
            self.activate_on_local_memory_registration
                && self.initial_state == ClientLifecycleState::Active
                && published_initial_state != self.initial_state;

        let runtime = ClientRuntimeId {
            stable_id: self.stable_id,
            epoch: self.epoch,
        };
        let default_scope = NamespaceScope::with_defaults(Some(&self.default_tenant), None, None);
        let effective_tenant_policy =
            resolve_effective_tenant_policy(self.metadata.as_ref(), &default_scope)?;
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
        let route_namespace = self.metadata.route_namespace();
        let live_client_cache = shared_live_client_cache(&route_namespace);
        let suspect_runtime_cache = shared_suspect_runtime_cache(&route_namespace);
        let control_client = Arc::new(ControlPlaneClient::new()?);
        let provisional_lease = ClientLease {
            runtime: runtime.clone(),
            state: published_initial_state,
            compatibility: self.compatibility,
            endpoints: endpoints.clone(),
            expires_at_ms,
        };
        validate_startup_conflicts(
            self.metadata.as_ref(),
            control_client.as_ref(),
            &provisional_lease,
        )?;
        bootstrap_route_policy(
            self.metadata.as_ref(),
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
            self.metadata.clone(),
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
        let control_plane = ControlPlaneHandle::spawn(
            &control_bind_host(&endpoints.rpc_address),
            local_authority.clone(),
            storage_adapter.clone(),
            storage_adapter,
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
        self.metadata.upsert_client_lease(&lease)?;
        let prewarm_delay = startup_prewarm_delay(&runtime, self.startup_prewarm_max_delay);
        if !prewarm_delay.is_zero() {
            std::thread::sleep(prewarm_delay);
        }
        refresh_live_client_cache(
            self.metadata.as_ref(),
            &live_client_cache,
            "live_client_snapshot_prewarm",
        )?;
        let membership_sync = MembershipSyncHandle::spawn(
            &runtime,
            self.metadata.clone(),
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
            metadata: self.metadata,
            route_directory,
            _control_plane: control_plane,
            control_client,
            allocator,
            storage_owner,
            lease,
            live_client_cache,
            suspect_runtime_cache,
            membership_sync,
            _async_eviction: async_eviction,
            default_tenant: self.default_tenant,
            local_memory: self.local_memory,
            transport: self.transport,
            transport_factory: self.transport_factory,
            write_mode: self.write_mode,
            route_control: self.route_control,
            route_topk: self.route_topk,
            transfer_stall_timeout: self.transfer_stall_timeout,
            request_timeout_override: self.request_timeout_override,
            lifecycle_state,
            route_write_gate,
            startup_activation_pending: AtomicBool::new(startup_activation_pending),
            heartbeat_repair_pending: AtomicUsize::new(0),
            route_control: effective_route_control,
            route_topk: effective_route_topk,
            namespace_quota: resolved_namespace_quota(&effective_tenant_policy)
                .or(self.namespace_quota),
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
    let effective = resolve_effective_route_policy(metadata, default_tenant)?;
    validate_route_policy(&local, &effective)
}

fn resolve_effective_tenant_policy(
    metadata: &dyn MetadataBackend,
    scope: &NamespaceScope,
) -> Result<TenantPolicySpec> {
    let policies = metadata.list_tenant_policies()?;
    Ok(TenantPolicySpec::resolve_for_scope(policies.iter(), scope))
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

fn bootstrap_default_route_policy(metadata: &dyn MetadataBackend, local: &RoutePolicy) -> Result<()> {
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

fn resolve_effective_route_policy(
    metadata: &dyn MetadataBackend,
    default_tenant: &str,
) -> Result<RoutePolicy> {
    let scope = NamespaceScope::with_defaults(Some(default_tenant), None, None);
    let tenant_spec = resolve_effective_tenant_policy(metadata, &scope)?;
    if let Some(policy) = route_policy_from_tenant_spec(&tenant_spec) {
        return Ok(policy);
    }
    if let Some(tenant_policy) = metadata.get_route_policy(&RoutePolicyDomain::Tenant(
        default_tenant.to_string(),
    ))? {
        return Ok(tenant_policy);
    }
    metadata
        .get_route_policy(&RoutePolicyDomain::Default)?
        .ok_or_else(|| StoreError::InvalidState("default route policy is missing".to_string()))
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
    spec.fairness
        .as_ref()
        .map(|fairness| ExecutionFairness {
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
