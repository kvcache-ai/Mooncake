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
    cold_tier_targets: Vec<ColdTierTargetConfig>,
    cold_tier_watermarks: ColdTierWatermarkConfig,
    cold_tier_rate_limits: ColdTierRateLimitConfig,
    cold_tier_offload_mode: ColdTierOffloadMode,
    cold_tier_offload_priority: ColdTierOffloadPriorityConfig,
    cold_tier_shutdown_mode: ColdTierShutdownMode,
    nof_low_level_targets:
        BTreeMap<String, crate::client::cold_tier::nof::NofLowLevelTarget>,
    #[cfg(test)]
    cold_tier_backend_overrides: BTreeMap<String, Arc<dyn PersistentStorageBackend>>,
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
            cold_tier_targets: Vec::new(),
            cold_tier_watermarks: ColdTierWatermarkConfig::default(),
            cold_tier_rate_limits: ColdTierRateLimitConfig::default(),
            cold_tier_offload_mode: ColdTierOffloadMode::default(),
            cold_tier_offload_priority: ColdTierOffloadPriorityConfig::default(),
            cold_tier_shutdown_mode: ColdTierShutdownMode::default(),
            nof_low_level_targets: BTreeMap::new(),
            #[cfg(test)]
            cold_tier_backend_overrides: BTreeMap::new(),
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

    /// Sets the cluster-level route-control mode (EmbeddedWrh or MetadataOnly).
    ///
    /// This is a deployment-level decision that determines the route storage architecture
    /// and is fixed for the lifetime of the client. It is not overridable per-tenant.
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

    /// Configures a single startup cold tier device (replaces any previously configured targets).
    ///
    /// This resolves and enables the runtime's cold tier device during client build, matching the
    /// Admin HTTP create/register/enable lifecycle for initial cluster bring-up. Use Admin HTTP
    /// cold-tier device endpoints for runtime lifecycle changes after startup.
    ///
    /// For multiple devices, use [`cold_tier_targets`](Self::cold_tier_targets) instead.
    pub fn cold_tier_target(mut self, cold_tier_target: ColdTierTargetConfig) -> Self {
        self.cold_tier_targets = vec![cold_tier_target];
        self
    }

    /// Configures startup cold tier devices (replaces any previously configured targets).
    pub fn cold_tier_targets<I>(mut self, cold_tier_targets: I) -> Self
    where
        I: IntoIterator<Item = ColdTierTargetConfig>,
    {
        self.cold_tier_targets = cold_tier_targets.into_iter().collect();
        self
    }

    pub fn cold_tier_watermarks(mut self, watermarks: ColdTierWatermarkConfig) -> Self {
        self.cold_tier_watermarks = watermarks;
        self
    }

    pub fn cold_tier_rate_limits(mut self, rate_limits: ColdTierRateLimitConfig) -> Self {
        self.cold_tier_rate_limits = rate_limits;
        self
    }

    pub fn cold_tier_offload_mode(mut self, mode: ColdTierOffloadMode) -> Self {
        self.cold_tier_offload_mode = mode;
        self
    }

    pub fn cold_tier_offload_priority(mut self, priority: ColdTierOffloadPriorityConfig) -> Self {
        self.cold_tier_offload_priority = priority;
        self
    }

    pub fn cold_tier_shutdown_mode(mut self, mode: ColdTierShutdownMode) -> Self {
        self.cold_tier_shutdown_mode = mode;
        self
    }

    /// Uses a physical NoF executor for an existing Cold Tier target.
    ///
    /// The matching [`ColdTierTargetConfig`] still supplies device lifecycle, capacity,
    /// admission, route, replica, and maintenance ownership. Only its physical backend I/O is
    /// replaced by this target.
    pub fn nof_low_level_target(
        mut self,
        cold_tier_id: impl Into<String>,
        target: crate::client::cold_tier::nof::NofLowLevelTarget,
    ) -> Self {
        self.nof_low_level_targets
            .insert(cold_tier_id.into(), target);
        self
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn cold_tier_backend_override(
        mut self,
        cold_tier_id: impl Into<String>,
        backend: Arc<dyn PersistentStorageBackend>,
    ) -> Self {
        self.cold_tier_backend_overrides
            .insert(cold_tier_id.into(), backend);
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
        for cold_tier_id in self.nof_low_level_targets.keys() {
            if !self
                .cold_tier_targets
                .iter()
                .any(|target| &target.cold_tier_id == cold_tier_id)
            {
                return Err(StoreError::InvalidState(format!(
                    "NoF low-level target {cold_tier_id} has no matching Cold Tier target"
                )));
            }
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
        let effective_route_control = self.route_control;
        let effective_route_topk = route_topk_from_tenant_spec(&effective_tenant_policy)
            .unwrap_or(self.route_topk);
        let state = Arc::new(Mutex::new(StoreState::default()));
        let allocator = Arc::new(Mutex::new(LocalAllocatorState::default()));
        let lifecycle_state = Arc::new(AtomicU8::new(encode_lifecycle_state(
            published_initial_state,
        )));
        let route_write_gate = Arc::new(RouteWriteGate::default());
        let route_namespace = runtime_metadata.route_namespace();
        let live_client_cache = shared_live_client_cache(&route_namespace);
        let suspect_runtime_cache = shared_suspect_runtime_cache(&route_namespace);
        let cold_tier_device_cache = shared_cold_tier_device_cache(&route_namespace);
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
        let cold_tier_enabled = cold_tier::cold_tier_enabled();
        let resolved_cold_tier = if cold_tier_enabled {
            self.cold_tier_targets
                .iter()
                .map(resolve_cold_tier_target)
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        for cold_tier_id in self.nof_low_level_targets.keys() {
            if !resolved_cold_tier
                .iter()
                .any(|resolved| &resolved.cold_tier_id == cold_tier_id)
            {
                return Err(StoreError::InvalidState(format!(
                    "NoF low-level target {cold_tier_id} has no matching Cold Tier target"
                )));
            }
        }
        let route_directory = build_route_directory(
            effective_route_control,
            effective_route_topk,
            runtime_metadata.clone(),
            &provisional_lease,
            control_client.clone(),
            live_client_cache.clone(),
            suspect_runtime_cache.clone(),
        );
        // Pre-populate the live client cache before cold tier bootstrap so that
        // route_directory queries (which call cached_live_client_snapshot) do not
        // fail with "snapshot not initialized".
        if !resolved_cold_tier.is_empty() {
            prewarm_live_client_cache(
                runtime_metadata.as_ref(),
                &live_client_cache,
                &provisional_lease,
            )?;
        }
        // Detect if any configured device was previously registered under a
        // different cold_tier_id (same physical target). Track old IDs so we
        // can register aliases in the backend resolver for route compatibility.
        let mut cold_tier_aliases: Vec<(String, String)> = Vec::new(); // (old_id, new_id)
        for resolved in &resolved_cold_tier {
            if let Some(existing) =
                find_existing_device_by_target(runtime_metadata.as_ref(), resolved)?
            {
                tracing::info!(
                    old_device_id = %existing.device_id,
                    new_cold_tier_id = %resolved.cold_tier_id,
                    target = ?resolved.target,
                    "cold tier device previously registered under different ID, will register alias"
                );
                cold_tier_aliases.push((existing.device_id.clone(), resolved.cold_tier_id.clone()));
            }
            bootstrap_cold_tier_device(runtime_metadata.as_ref(), &runtime, resolved)?;
        }
        let default_cold_tier_id = resolved_cold_tier
            .first()
            .map(|r| r.cold_tier_id.clone())
            .unwrap_or_else(|| runtime.stable_id.to_string());
        let mut cold_tier_handles: BTreeMap<String, Arc<dyn PersistentStorageBackend>> =
            BTreeMap::new();
        let mut deferred_reconciles: Vec<DeferredColdTierReconcile> = Vec::new();
        for resolved in &resolved_cold_tier {
            #[cfg(test)]
            if let Some(backend) = self
                .cold_tier_backend_overrides
                .get(&resolved.cold_tier_id)
                .cloned()
            {
                cold_tier_handles.insert(resolved.cold_tier_id.clone(), backend);
                continue;
            }
            let mut reconcile_devices = vec![resolved.clone()];
            for (old_id, new_id) in &cold_tier_aliases {
                if new_id == &resolved.cold_tier_id {
                    let mut alias_device = resolved.clone();
                    alias_device.cold_tier_id = old_id.clone();
                    reconcile_devices.push(alias_device);
                }
            }
            let backend: Arc<dyn PersistentStorageBackend> = if let Some(target) =
                self.nof_low_level_targets.get(&resolved.cold_tier_id).cloned()
            {
                Arc::new(NofLowLevelBackend::new(
                    resolved.cold_tier_id.clone(),
                    target,
                )?)
            } else if resolved.kind == ColdTierKind::Ssd
                && resolved.ssd_engine == ColdTierSsdEngine::ExtentStore
            {
                let backend = Arc::new(ExtentStoreStorageBackend::new(
                    resolved
                        .root_dir
                        .join(encode_backend_component(&resolved.cold_tier_id)),
                )?);
                deferred_reconciles.push(DeferredColdTierReconcile::ExtentStore(
                    backend.clone(),
                    reconcile_devices,
                ));
                backend
            } else {
                let backend = Arc::new(LocalDirPersistentStorageBackend::new_with_root(
                    resolved.root_dir.clone(),
                ));
                deferred_reconciles.push(DeferredColdTierReconcile::LocalDir(
                    backend.clone(),
                    reconcile_devices,
                ));
                backend
            };
            cold_tier_handles.insert(resolved.cold_tier_id.clone(), backend);
        }
        // Register aliases so routes referencing old cold_tier_ids still resolve.
        for (old_id, new_id) in &cold_tier_aliases {
            if let Some(backend) = cold_tier_handles.get(new_id).cloned() {
                cold_tier_handles.entry(old_id.clone()).or_insert(backend);
            }
        }
        let cold_tier_resolver =
            ColdTierBackendResolver::from_handles(default_cold_tier_id, cold_tier_handles);
        let storage_owner = Arc::new(StorageOwnerState::new(
            runtime.clone(),
            mooncake_store_route::RouteOperations::new(
                route_directory.clone(),
                provisional_lease.clone(),
            ),
            runtime_metadata.clone(),
            allocator.clone(),
            state.clone(),
            StorageOwnerColdTierConfig {
                resolver: cold_tier_resolver,
                devices: cold_tier_device_cache.clone(),
                watermarks: self.cold_tier_watermarks,
                rate_limits: self.cold_tier_rate_limits,
                offload_mode: self.cold_tier_offload_mode,
                offload_priority: self.cold_tier_offload_priority,
                runtime: runtime.clone(),
            },
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
        let cold_tier_control: Arc<dyn crate::control_plane::ColdTierControlService> =
            if cold_tier_enabled {
                storage_adapter.clone()
            } else {
                Arc::new(UnsupportedColdTierControlService)
            };
        let control_plane = ControlPlaneHandle::spawn_with_migration(
            &control_bind_host(&endpoints.rpc_address),
            local_authority.clone(),
            storage_adapter.clone(),
            storage_adapter.clone(),
            migration_adapter,
            cold_tier_control,
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
        if !startup_activation_pending {
            run_deferred_cold_tier_reconciles(
                std::mem::take(&mut deferred_reconciles),
                runtime_metadata.as_ref(),
                route_directory.as_ref(),
                &lease,
            );
        }
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
        let refresh_cold_tier_devices = storage_owner.has_local_cold_tier_work();
        if refresh_cold_tier_devices {
            refresh_cold_tier_device_cache(
                runtime_metadata.as_ref(),
                &cold_tier_device_cache,
                "cold_tier_device_snapshot_prewarm",
            )?;
        }
        let membership_sync = MembershipSyncHandle::spawn(
            &runtime,
            runtime_metadata.clone(),
            live_client_cache.clone(),
            if refresh_cold_tier_devices { Some(cold_tier_device_cache.clone()) } else { None },
            self.live_client_sync_interval,
            route_namespace.clone(),
            suspect_runtime_cache.clone(),
        )?;
        let async_eviction = AsyncEvictionHandle::spawn(
            &runtime,
            &lease,
            &self.local_memory,
            storage_owner.clone(),
        )?;
        let async_replica_tracking = AsyncReplicaTrackHandle::spawn(
            &runtime,
            runtime_metadata.clone(),
            control_client.clone(),
            live_client_cache.clone(),
        )?;
        let async_route_hit_reporting = AsyncRouteHitReportHandle::spawn(
            &runtime,
            runtime_metadata.clone(),
            control_client.clone(),
            live_client_cache.clone(),
        )?;
        let cold_tier_configured = cold_tier_enabled && !resolved_cold_tier.is_empty();
        let restore_promotions = Arc::new(RestorePromotionQueue::new(1024, 32, 32));
        let cold_restore_flights = Arc::new(ColdRestoreSingleflight::default());
        let cold_tier = if cold_tier_configured {
            cold_tier::ColdTierHandle::spawn(
                &runtime,
                &lease,
                &self.local_memory,
                storage_owner.clone(),
                restore_promotions.clone(),
            )?
        } else {
            cold_tier::ColdTierHandle::disabled(restore_promotions.clone())
        };
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
            async_replica_tracking,
            async_route_hit_reporting,
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
            tenant_quota_reservation_counter: Arc::new(AtomicU64::new(1)),
            route_control: effective_route_control,
            route_topk: effective_route_topk,
            namespace_quota: effective_namespace_quota,
            execution_fairness: resolved_execution_fairness(&effective_tenant_policy)
                .or(self.execution_fairness),
            bandwidth_shaping: resolved_bandwidth_shaping(&effective_tenant_policy)
                .or(self.bandwidth_shaping),
            placement_policy: resolved_placement_policy(&effective_tenant_policy),
            state,
            owns_local_state_lifecycle: true,
            owns_cold_tier_lifecycle: true,
            cold_tier_shutdown_mode: self.cold_tier_shutdown_mode,
            restore_promotions,
            cold_tier,
            cold_restore_flights,
            deferred_cold_tier_reconciles: Mutex::new(deferred_reconciles),
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

fn route_topk_from_tenant_spec(spec: &TenantPolicySpec) -> Option<usize> {
    spec.routing.as_ref()?.route_topk.map(|v| v as usize)
}

fn effective_route_policy(
    metadata: &dyn MetadataBackend,
    default_tenant: &str,
) -> Result<Option<RoutePolicy>> {
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
