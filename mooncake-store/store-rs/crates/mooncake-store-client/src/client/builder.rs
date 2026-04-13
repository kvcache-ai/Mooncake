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
        endpoints
            .labels
            .entry("route".to_string())
            .or_insert_with(|| "true".to_string());
        normalize_storage_label(&self.local_memory, &mut endpoints.labels)?;
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
            state: self.initial_state,
            compatibility: self.compatibility,
            endpoints: endpoints.clone(),
            expires_at_ms,
        };
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
            state: self.initial_state,
            compatibility: provisional_lease.compatibility.clone(),
            endpoints,
            expires_at_ms,
        };
        self.metadata.upsert_client_lease(&lease)?;
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
