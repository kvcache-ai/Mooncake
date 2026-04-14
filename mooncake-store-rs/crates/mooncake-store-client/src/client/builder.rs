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
        let state = Mutex::new(StoreState::default());
        let allocator = Arc::new(Mutex::new(LocalAllocatorState::default()));
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
            self.route_control,
            self.route_topk,
        )?;
        let route_directory = build_route_directory(
            self.route_control,
            self.route_topk,
            self.metadata.clone(),
            &provisional_lease,
            control_client.clone(),
            live_client_cache.clone(),
            suspect_runtime_cache.clone(),
        );
        let storage_owner = Arc::new(StorageOwnerState::new(
            runtime.clone(),
            provisional_lease.clone(),
            self.metadata.clone(),
            route_directory.clone(),
            allocator.clone(),
        ));
        let storage_adapter = Arc::new(LocalAllocatorAdapter {
            runtime: runtime.clone(),
            allocator: allocator.clone(),
            storage_owner: storage_owner.clone(),
        });
        let control_plane = ControlPlaneHandle::spawn(
            &control_bind_host(&endpoints.rpc_address),
            Arc::new(LocalAuthorityAdapter),
            storage_adapter.clone(),
            storage_adapter,
        )?;
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
            lifecycle_state: AtomicU8::new(encode_lifecycle_state(published_initial_state)),
            startup_activation_pending: AtomicBool::new(startup_activation_pending),
            state,
        })
    }
}

fn bootstrap_route_policy(
    metadata: &dyn MetadataBackend,
    lease: &ClientLease,
    route_control: RouteControlMode,
    route_topk: usize,
) -> Result<()> {
    let domain = RoutePolicyDomain::Default;
    let local = RoutePolicy {
        route_topk: route_topk as u32,
        route_control,
        created_by: lease.runtime.clone(),
        created_at_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    };
    match metadata.get_route_policy(&domain)? {
        Some(existing) => validate_route_policy(&local, &existing),
        None => {
            if metadata.put_route_policy_if_absent(&domain, &local)? {
                return Ok(());
            }
            let Some(existing) = metadata.get_route_policy(&domain)? else {
                return Err(StoreError::InvalidState(
                    "route policy bootstrap raced but no policy was readable afterward".to_string(),
                ));
            };
            validate_route_policy(&local, &existing)
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
