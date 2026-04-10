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

    pub fn build(self, expires_at_ms: u64) -> Result<StoreClient> {
        if self.default_tenant.is_empty() {
            return Err(StoreError::InvalidState(
                "default tenant must not be empty".to_string(),
            ));
        }

        let mut endpoints = self.endpoints;
        endpoints
            .labels
            .entry("route".to_string())
            .or_insert_with(|| "true".to_string());
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
        let control_client = Arc::new(ControlPlaneClient::new()?);
        let control_plane = ControlPlaneHandle::spawn(
            &control_bind_host(&endpoints.rpc_address),
            Arc::new(LocalAuthorityAdapter),
            Arc::new(LocalAllocatorAdapter {
                runtime: runtime.clone(),
                allocator: allocator.clone(),
            }),
        )?;
        endpoints
            .labels
            .entry(control_address_label().to_string())
            .or_insert_with(|| control_plane.address().to_string());
        let lease = ClientLease {
            runtime: runtime.clone(),
            state: self.initial_state,
            compatibility: self.compatibility,
            endpoints,
            expires_at_ms,
        };
        self.metadata.upsert_client_lease(&lease)?;
        let route_directory = build_route_directory(
            self.route_control,
            self.metadata.clone(),
            &lease,
            control_client.clone(),
        );
        Ok(StoreClient {
            metadata: self.metadata,
            route_directory,
            _control_plane: control_plane,
            control_client,
            allocator,
            lease,
            live_client_cache: Mutex::new(LiveClientCache::default()),
            default_tenant: self.default_tenant,
            local_memory: self.local_memory,
            transport: self.transport,
            transport_factory: self.transport_factory,
            write_mode: self.write_mode,
            route_control: self.route_control,
            state,
        })
    }
}
