use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mooncake_store_core::{
    CasResult, ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend, ObjectKey,
    ObjectRoute, ReplicaRoute, ReplicaTier, Result, RouteCasRequest, RouteDirectory, RouteState,
    RouteVersion, SegmentAnnouncement, SegmentLifecycleState, SegmentName, StoreError,
};
use mooncake_transport::{Opcode, TentEngine, TransferRequest};
use parking_lot::Mutex;
use tracing::{debug, info, info_span};

use crate::control_plane::{
    control_address_label, AllocatorService, AuthorityService, ControlPlaneClient,
    ControlPlaneHandle, ReleaseOp, ReserveSpecificOp,
};
use crate::memory::{
    LocalMemoryConfig, LocalMemoryState, RegionAllocation, StorageExtentInfo, StorageSegmentSpec,
};
use crate::observability::OperationTracker;
use crate::placement::PlacementPlanner;
use crate::route_directory::{
    authority_compare_and_swap, authority_compare_and_swap_many, authority_get, authority_get_many,
    authority_replace, authority_replace_many, build_route_directory, RouteControlMode,
};
use crate::transport::{wait_for_batch_completion, StoreTransport, StoreTransportFactory};

const DEFAULT_TENANT: &str = "default";
const DEFAULT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
pub struct ObjectRef<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
}

impl<'a> ObjectRef<'a> {
    pub fn new(key: &'a str) -> Self {
        Self { tenant: None, key }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicationPolicy {
    pub replica_count: Option<usize>,
    pub with_soft_pin: bool,
    pub preferred_segments: Vec<SegmentName>,
    pub prefer_alloc_in_same_node: bool,
}

impl ReplicationPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn replica_count(mut self, replica_count: usize) -> Self {
        self.replica_count = Some(replica_count);
        self
    }

    pub fn with_soft_pin(mut self, with_soft_pin: bool) -> Self {
        self.with_soft_pin = with_soft_pin;
        self
    }

    pub fn prefer_alloc_in_same_node(mut self, prefer_alloc_in_same_node: bool) -> Self {
        self.prefer_alloc_in_same_node = prefer_alloc_in_same_node;
        self
    }

    pub fn preferred_segment(mut self, segment: impl Into<String>) -> Self {
        self.preferred_segments.push(SegmentName::new(segment));
        self
    }

    pub fn preferred_segments<I, S>(mut self, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.preferred_segments = segments
            .into_iter()
            .map(SegmentName::new)
            .collect::<Vec<_>>();
        self
    }
}

#[derive(Clone, Debug)]
pub struct PutRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub value: &'a [u8],
    pub policy: Option<ReplicationPolicy>,
}

impl<'a> PutRequest<'a> {
    pub fn new(key: &'a str, value: &'a [u8]) -> Self {
        Self {
            tenant: None,
            key,
            value,
            policy: None,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn replication(mut self, policy: ReplicationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

#[derive(Clone, Debug)]
pub struct PutFromRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub buffer: *const c_void,
    pub size: usize,
    pub policy: Option<ReplicationPolicy>,
}

impl<'a> PutFromRequest<'a> {
    pub fn new(key: &'a str, buffer: *const c_void, size: usize) -> Self {
        Self {
            tenant: None,
            key,
            buffer,
            size,
            policy: None,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn replication(mut self, policy: ReplicationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

#[derive(Debug)]
pub struct GetRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub buffer: &'a mut [u8],
}

pub struct MultiBufferPutRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub buffers: &'a [&'a [u8]],
    pub policy: Option<ReplicationPolicy>,
}

impl<'a> MultiBufferPutRequest<'a> {
    pub fn new(key: &'a str, buffers: &'a [&'a [u8]]) -> Self {
        Self {
            tenant: None,
            key,
            buffers,
            policy: None,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn replication(mut self, policy: ReplicationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

pub struct MultiBufferGetRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub buffers: &'a mut [&'a mut [u8]],
}

impl<'a> MultiBufferGetRequest<'a> {
    pub fn new(key: &'a str, buffers: &'a mut [&'a mut [u8]]) -> Self {
        Self {
            tenant: None,
            key,
            buffers,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }
}

impl<'a> GetRequest<'a> {
    pub fn new(key: &'a str, buffer: &'a mut [u8]) -> Self {
        Self {
            tenant: None,
            key,
            buffer,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
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
            default_tenant: self.default_tenant,
            local_memory: self.local_memory,
            transport: self.transport,
            transport_factory: self.transport_factory,
            write_mode: self.write_mode,
            state,
        })
    }
}

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
    fn query_route(&self, key: &str) -> Result<Option<ObjectRoute>>;
    fn query_route_in_tenant(&self, tenant: &str, key: &str) -> Result<Option<ObjectRoute>>;
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

pub struct StoreClient {
    metadata: Arc<dyn MetadataBackend>,
    route_directory: Arc<dyn RouteDirectory>,
    _control_plane: ControlPlaneHandle,
    control_client: Arc<ControlPlaneClient>,
    allocator: Arc<Mutex<LocalAllocatorState>>,
    lease: ClientLease,
    default_tenant: String,
    local_memory: LocalMemoryConfig,
    transport: Option<Arc<dyn StoreTransport>>,
    transport_factory: Option<Arc<dyn StoreTransportFactory>>,
    write_mode: WriteMode,
    state: Mutex<StoreState>,
}

impl StoreClient {
    pub fn runtime_id(&self) -> &ClientRuntimeId {
        &self.lease.runtime
    }

    pub fn lease(&self) -> &ClientLease {
        &self.lease
    }

    pub fn default_tenant(&self) -> &str {
        &self.default_tenant
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

    fn lookup_runtime_lease(&self, runtime: &ClientRuntimeId) -> Result<ClientLease> {
        self.metadata
            .list_live_clients()?
            .into_iter()
            .find(|lease| lease.runtime == *runtime && compatibility_matches(&self.lease, lease))
            .ok_or_else(|| StoreError::NotFound(format!("runtime {} is not available", runtime)))
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
        let mut leases = BTreeMap::new();
        for lease in self.metadata.list_live_clients()? {
            if !wanted.contains(&lease.runtime) {
                continue;
            }
            if !compatibility_matches(&self.lease, &lease) {
                continue;
            }
            leases.insert(lease.runtime.clone(), lease);
        }
        for runtime in &wanted {
            if !leases.contains_key(runtime) {
                return Err(StoreError::NotFound(format!(
                    "runtime {} is not available",
                    runtime
                )));
            }
        }
        Ok(leases)
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
            return match segment_name {
                Some(segment_name) => {
                    self.allocator
                        .lock()
                        .reserve_specific(owner, segment_name, length_bytes)
                }
                None => self.allocator.lock().reserve_any(owner, length_bytes),
            };
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
                debug!(
                    owner = %owner,
                    segment = segment_name.map(|segment| segment.0.as_str()).unwrap_or("*"),
                    error = %error,
                    "allocator rpc failed; falling back to metadata allocator"
                );
                self.reserve_segment_allocation_via_metadata(owner, segment_name, length_bytes)
            }
        }
    }

    fn resolve_replication_policy(
        &self,
        policy: Option<&ReplicationPolicy>,
    ) -> Result<ResolvedReplicationPolicy> {
        let Some(policy) = policy else {
            return Ok(ResolvedReplicationPolicy {
                replica_count: self.default_replica_count(),
                preferred_segments: Vec::new(),
                with_soft_pin: false,
                prefer_alloc_in_same_node: false,
            });
        };

        let mut preferred_segments = policy.preferred_segments.clone();
        if policy.prefer_alloc_in_same_node
            && preferred_segments.is_empty()
            && matches!(self.write_mode, WriteMode::LocalOnly)
        {
            preferred_segments.push(self.segment_name()?);
        }
        let replica_count = policy
            .replica_count
            .unwrap_or_else(|| self.default_replica_count());
        if replica_count == 0 {
            return Err(StoreError::InvalidState(
                "replica_count must be greater than zero".to_string(),
            ));
        }

        let mut seen = BTreeSet::new();
        for segment in &preferred_segments {
            if !seen.insert(segment.clone()) {
                return Err(StoreError::Conflict(format!(
                    "duplicate preferred segment {}",
                    segment.0
                )));
            }
        }
        if preferred_segments.len() > replica_count {
            return Err(StoreError::InvalidState(format!(
                "preferred segments exceed replica_count: have={} limit={replica_count}",
                preferred_segments.len()
            )));
        }

        Ok(ResolvedReplicationPolicy {
            replica_count,
            preferred_segments,
            with_soft_pin: policy.with_soft_pin,
            prefer_alloc_in_same_node: policy.prefer_alloc_in_same_node,
        })
    }

    fn resolve_replica_targets(
        &self,
        tenant: &str,
        key: &str,
        policy: &ResolvedReplicationPolicy,
    ) -> Result<Vec<ReplicaPlacementTarget>> {
        let _ = policy.with_soft_pin;
        let _ = policy.prefer_alloc_in_same_node;
        let mut targets = Vec::with_capacity(policy.replica_count);
        let mut excluded = BTreeSet::new();
        for segment in &policy.preferred_segments {
            let preferred = self.lookup_preferred_segment(segment)?;
            excluded.insert(preferred.owner.clone());
            targets.push(ReplicaPlacementTarget::Segment {
                owner: preferred.owner,
                segment_name: preferred.segment_name,
            });
        }
        if targets.len() == policy.replica_count {
            return Ok(targets);
        }

        let needs_routed = !targets.is_empty()
            || policy.replica_count > 1
            || matches!(self.write_mode, WriteMode::Routed { .. });
        if !needs_routed {
            return Ok(vec![ReplicaPlacementTarget::Owner(
                self.lease.runtime.clone(),
            )]);
        }

        let ranked = self
            .request_placement_planner()
            .ranked_candidates(self, &ObjectRef::new(key).tenant(tenant))?;
        for owner in ranked {
            if excluded.contains(&owner) {
                continue;
            }
            excluded.insert(owner.clone());
            targets.push(ReplicaPlacementTarget::Owner(owner));
            if targets.len() == policy.replica_count {
                break;
            }
        }

        if targets.len() != policy.replica_count {
            return Err(StoreError::InvalidState(format!(
                "not enough placement targets: resolved={} need={}",
                targets.len(),
                policy.replica_count
            )));
        }
        Ok(targets)
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
        let owner = self
            .metadata
            .list_live_clients()?
            .into_iter()
            .find(|lease| {
                lease.runtime == segment.owner
                    && lease.state == ClientLifecycleState::Active
                    && compatibility_matches(&self.lease, lease)
            })
            .ok_or_else(|| {
                StoreError::InvalidState(format!(
                    "preferred segment {} belongs to an unavailable client",
                    segment_name.0
                ))
            })?;
        let _ = owner;
        Ok(segment)
    }

    fn reserve_specific_segment(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: usize,
        require_local_memory: bool,
    ) -> Result<(ReplicaWriteTarget, mooncake_store_core::SegmentReservation)> {
        let reservation = self.reserve_segment_allocation(
            owner,
            Some(segment_name),
            length_bytes as u64,
            require_local_memory,
        )?;
        Ok((
            ReplicaWriteTarget {
                runtime: owner.clone(),
                segment_name: reservation.segment_name.clone(),
            },
            reservation,
        ))
    }

    fn reserve_owner_segments_batch(
        &self,
        requests: &[OwnerReservationRequest],
    ) -> Result<Vec<(ReplicaWriteTarget, mooncake_store_core::SegmentReservation)>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let remote_leases = self.lookup_runtime_leases(
            requests
                .iter()
                .filter(|request| request.owner != self.lease.runtime)
                .map(|request| request.owner.clone()),
        )?;
        let mut resolved = vec![None; requests.len()];
        let rollback = |resolved: &[Option<(
            ReplicaWriteTarget,
            mooncake_store_core::SegmentReservation,
        )>]| {
            let releases = resolved
                .iter()
                .filter_map(|entry| entry.as_ref())
                .map(|(target, reservation)| AllocationReleaseRequest {
                    owner: target.runtime.clone(),
                    segment_name: target.segment_name.clone(),
                    offset_bytes: reservation.offset_bytes,
                    length_bytes: reservation.length_bytes,
                })
                .collect::<Vec<_>>();
            let _ = self.release_segment_allocations_batch(&releases);
        };
        let mut remote_groups = BTreeMap::<ClientRuntimeId, (ClientLease, Vec<usize>)>::new();
        for (index, request) in requests.iter().enumerate() {
            if request.owner == self.lease.runtime {
                let reservation = match self.reserve_segment_allocation(
                    &request.owner,
                    None,
                    request.length_bytes,
                    request.require_local_memory,
                ) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        rollback(&resolved);
                        return Err(error);
                    }
                };
                resolved[index] = Some((
                    ReplicaWriteTarget {
                        runtime: request.owner.clone(),
                        segment_name: reservation.segment_name.clone(),
                    },
                    reservation,
                ));
                continue;
            }
            let lease = remote_leases.get(&request.owner).cloned().ok_or_else(|| {
                StoreError::NotFound(format!("runtime {} is not available", request.owner))
            })?;
            remote_groups
                .entry(request.owner.clone())
                .or_insert_with(|| (lease, Vec::new()))
                .1
                .push(index);
        }

        for (owner, (lease, indices)) in remote_groups {
            let lengths = indices
                .iter()
                .map(|index| requests[*index].length_bytes)
                .collect::<Vec<_>>();
            match self
                .control_client
                .batch_reserve_any(&lease, &owner, &lengths)
            {
                Ok(results) => {
                    for ((index, length_bytes), result) in indices
                        .iter()
                        .copied()
                        .zip(lengths.into_iter())
                        .zip(results.into_iter())
                    {
                        let reservation = match result {
                            Ok(reservation) => reservation,
                            Err(error) => {
                                debug!(
                                    owner = %owner,
                                    error = %error,
                                    length_bytes,
                                    "allocator batch reserve rpc failed; falling back to metadata allocator"
                                );
                                match self.reserve_segment_allocation_via_metadata(
                                    &owner,
                                    None,
                                    length_bytes,
                                ) {
                                    Ok(reservation) => reservation,
                                    Err(error) => {
                                        rollback(&resolved);
                                        return Err(error);
                                    }
                                }
                            }
                        };
                        resolved[index] = Some((
                            ReplicaWriteTarget {
                                runtime: owner.clone(),
                                segment_name: reservation.segment_name.clone(),
                            },
                            reservation,
                        ));
                    }
                }
                Err(error) => {
                    debug!(
                        owner = %owner,
                        error = %error,
                        items = indices.len(),
                        "allocator batch reserve rpc failed; falling back to metadata allocator"
                    );
                    for index in indices {
                        let reservation = match self.reserve_segment_allocation_via_metadata(
                            &owner,
                            None,
                            requests[index].length_bytes,
                        ) {
                            Ok(reservation) => reservation,
                            Err(error) => {
                                rollback(&resolved);
                                return Err(error);
                            }
                        };
                        resolved[index] = Some((
                            ReplicaWriteTarget {
                                runtime: owner.clone(),
                                segment_name: reservation.segment_name.clone(),
                            },
                            reservation,
                        ));
                    }
                }
            }
        }

        resolved
            .into_iter()
            .map(|entry| {
                entry.ok_or_else(|| {
                    StoreError::InvalidState(
                        "missing owner reservation result from batch allocator".to_string(),
                    )
                })
            })
            .collect()
    }

    fn release_segment_allocations_batch(
        &self,
        requests: &[AllocationReleaseRequest],
    ) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        let remote_leases = self.lookup_runtime_leases(
            requests
                .iter()
                .filter(|request| request.owner != self.lease.runtime)
                .map(|request| request.owner.clone()),
        )?;
        let mut remote_groups = BTreeMap::<ClientRuntimeId, (ClientLease, Vec<usize>)>::new();
        for (index, request) in requests.iter().enumerate() {
            if request.owner == self.lease.runtime {
                self.allocator.lock().release(
                    &request.owner,
                    &request.segment_name,
                    request.offset_bytes,
                    request.length_bytes,
                )?;
                continue;
            }
            let lease = remote_leases.get(&request.owner).cloned().ok_or_else(|| {
                StoreError::NotFound(format!("runtime {} is not available", request.owner))
            })?;
            remote_groups
                .entry(request.owner.clone())
                .or_insert_with(|| (lease, Vec::new()))
                .1
                .push(index);
        }

        for (owner, (lease, indices)) in remote_groups {
            let ops = indices
                .iter()
                .map(|index| ReleaseOp {
                    segment_name: requests[*index].segment_name.clone(),
                    offset_bytes: requests[*index].offset_bytes,
                    length_bytes: requests[*index].length_bytes,
                })
                .collect::<Vec<_>>();
            match self.control_client.batch_release(&lease, &owner, &ops) {
                Ok(results) => {
                    for (index, result) in indices.iter().copied().zip(results.into_iter()) {
                        if let Err(error) = result {
                            debug!(
                                owner = %owner,
                                segment = %requests[index].segment_name.0,
                                offset_bytes = requests[index].offset_bytes,
                                length_bytes = requests[index].length_bytes,
                                error = %error,
                                "allocator batch release rpc failed; falling back to metadata allocator"
                            );
                            self.metadata.release_segment(
                                &owner,
                                &requests[index].segment_name,
                                requests[index].offset_bytes,
                                requests[index].length_bytes,
                            )?;
                        }
                    }
                }
                Err(error) => {
                    debug!(
                        owner = %owner,
                        error = %error,
                        items = indices.len(),
                        "allocator batch release rpc failed; falling back to metadata allocator"
                    );
                    for index in indices {
                        self.metadata.release_segment(
                            &owner,
                            &requests[index].segment_name,
                            requests[index].offset_bytes,
                            requests[index].length_bytes,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn release_reserved_allocations(
        &self,
        targets: &[ReplicaWriteTarget],
        reservations: &[mooncake_store_core::SegmentReservation],
    ) -> Result<()> {
        if targets.len() != reservations.len() {
            return Err(StoreError::InvalidState(
                "targets and reservations length mismatch".to_string(),
            ));
        }
        let releases = targets
            .iter()
            .zip(reservations.iter())
            .map(|(target, reservation)| AllocationReleaseRequest {
                owner: target.runtime.clone(),
                segment_name: target.segment_name.clone(),
                offset_bytes: reservation.offset_bytes,
                length_bytes: reservation.length_bytes,
            })
            .collect::<Vec<_>>();
        self.release_segment_allocations_batch(&releases)
    }

    fn flush_due_reclaims(&self) -> Result<()> {
        let due = {
            let mut state = self.state.lock();
            state.take_due_reclaims(now_ms())
        };
        let releases = due
            .into_iter()
            .map(|reclaim| AllocationReleaseRequest {
                owner: reclaim.owner,
                segment_name: reclaim.segment_name,
                offset_bytes: reclaim.offset_bytes,
                length_bytes: reclaim.length_bytes,
            })
            .collect::<Vec<_>>();
        self.release_segment_allocations_batch(&releases)
    }

    fn schedule_route_reclaim(&self, route: &ObjectRoute) -> Result<()> {
        let grace_ms = self.local_memory.reclaim_grace_ms;
        if grace_ms == 0 {
            return self.release_route_allocations(route);
        }
        let mut state = self.state.lock();
        let due_at_ms = now_ms().saturating_add(grace_ms);
        for replica in &route.replicas {
            state.pending_reclaims.push_back(PendingReclaim {
                due_at_ms,
                owner: replica.owner.clone(),
                segment_name: replica.segment_name.clone(),
                offset_bytes: replica.segment_offset,
                length_bytes: replica.length,
            });
        }
        Ok(())
    }

    fn transport(&self) -> Result<&dyn StoreTransport> {
        self.transport
            .as_deref()
            .ok_or_else(|| StoreError::Unsupported("transport is not configured".to_string()))
    }

    fn transport_factory(&self) -> Result<&dyn StoreTransportFactory> {
        self.transport_factory.as_deref().ok_or_else(|| {
            StoreError::Unsupported("transport factory is not configured".to_string())
        })
    }

    fn ensure_local_memory(&self) -> Result<()> {
        if self.state.lock().memory.is_some() {
            return Ok(());
        }
        let transport = self.transport()?;
        let primary_segment = self.segment_name()?;
        let memory = LocalMemoryState::register(transport, &primary_segment, &self.local_memory)?;
        let primary = memory
            .storage_segments()
            .into_iter()
            .find(|segment| segment.segment_name == primary_segment)
            .ok_or_else(|| {
                StoreError::InvalidState("primary segment registration is missing".to_string())
            })?;
        {
            let mut state = self.state.lock();
            state.next_local_segment_id = 1;
            if let Some(transport) = self.transport.clone() {
                state
                    .local_transports
                    .insert(primary.segment_name.0.clone(), transport);
            }
            state.memory = Some(memory);
        }
        self.publish_local_segment(&primary, 0)
    }

    fn scoped_key(&self, tenant: &str, key: &str) -> ObjectKey {
        ObjectKey::new(format!("{tenant}::{key}"))
    }

    fn publish_local_segment(&self, segment: &StorageExtentInfo, used_bytes: u64) -> Result<()> {
        debug!(
            runtime = %self.lease.runtime,
            segment = %segment.segment_name.0,
            capacity_bytes = segment.capacity_bytes,
            used_bytes,
            state = ?segment.state,
            "publishing local segment"
        );
        let announcement = SegmentAnnouncement {
            owner: self.lease.runtime.clone(),
            segment_name: segment.segment_name.clone(),
            capacity_bytes: segment.capacity_bytes,
            used_bytes,
            state: segment.state,
            alignment_bytes: segment.alignment_bytes,
            tags: segment.tags.clone(),
        };
        self.allocator.lock().upsert(&announcement);
        self.metadata.publish_segment(&announcement)
    }

    fn reserve_owner_segment(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: usize,
        require_local_memory: bool,
    ) -> Result<(ReplicaWriteTarget, mooncake_store_core::SegmentReservation)> {
        self.flush_due_reclaims()?;
        let reservation = self.reserve_segment_allocation(
            owner,
            None,
            length_bytes as u64,
            require_local_memory,
        )?;
        debug!(
            owner = %owner,
            segment = %reservation.segment_name.0,
            offset_bytes = reservation.offset_bytes,
            length_bytes = reservation.length_bytes,
            "reserved segment space"
        );
        Ok((
            ReplicaWriteTarget {
                runtime: owner.clone(),
                segment_name: reservation.segment_name.clone(),
            },
            reservation,
        ))
    }

    fn release_route_allocations(&self, route: &ObjectRoute) -> Result<()> {
        let releases = route
            .replicas
            .iter()
            .map(|replica| {
                debug!(
                    owner = %replica.owner,
                    segment = %replica.segment_name.0,
                    offset_bytes = replica.segment_offset,
                    length_bytes = replica.length,
                    "releasing route allocation"
                );
                AllocationReleaseRequest {
                    owner: replica.owner.clone(),
                    segment_name: replica.segment_name.clone(),
                    offset_bytes: replica.segment_offset,
                    length_bytes: replica.length,
                }
            })
            .collect::<Vec<_>>();
        self.release_segment_allocations_batch(&releases)
    }

    fn put_scoped_with_policy(
        &self,
        tenant: &str,
        key: &str,
        value: &[u8],
        policy: Option<&ReplicationPolicy>,
    ) -> Result<ObjectRoute> {
        self.ensure_local_memory()?;
        self.flush_due_reclaims()?;
        let scoped_key = self.scoped_key(tenant, key);
        let policy = self.resolve_replication_policy(policy)?;
        let placements = self.resolve_replica_targets(tenant, key, &policy)?;
        let mut targets = Vec::with_capacity(placements.len());
        let mut reservations = Vec::with_capacity(placements.len());
        for placement in placements {
            let is_local = placement.owner() == &self.lease.runtime;
            let (target, reservation) = match placement {
                ReplicaPlacementTarget::Owner(owner) => {
                    self.reserve_owner_segment(&owner, value.len(), is_local)?
                }
                ReplicaPlacementTarget::Segment {
                    owner,
                    segment_name,
                } => self.reserve_specific_segment(&owner, &segment_name, value.len(), is_local)?,
            };
            targets.push(target);
            reservations.push(reservation);
        }
        let offsets = match self.write_reserved_replicas(&targets, &reservations, value) {
            Ok(offsets) => offsets,
            Err(error) => {
                let _ = self.release_reserved_allocations(&targets, &reservations);
                return Err(error);
            }
        };

        let current = self
            .route_directory
            .get_object_route(&self.lease, &scoped_key)?;
        let expected_version = current.as_ref().map(|route| route.version);
        let next_version = current
            .as_ref()
            .map(|route| route.version.next())
            .unwrap_or(RouteVersion(1));
        let route = ObjectRoute {
            key: scoped_key,
            version: next_version,
            state: RouteState::Active,
            compatibility: self.lease.compatibility.clone(),
            replicas: targets
                .iter()
                .zip(offsets.iter())
                .enumerate()
                .map(|(priority, (target, offset))| ReplicaRoute {
                    owner: target.runtime.clone(),
                    segment_name: target.segment_name.clone(),
                    offset: *offset,
                    segment_offset: reservations[priority].offset_bytes,
                    length: value.len() as u64,
                    checksum: None,
                    tier: ReplicaTier::Dram,
                    priority: priority as u16,
                })
                .collect(),
        };
        let cas = self.route_directory.compare_and_swap_object_route(
            &self.lease,
            &route.key,
            expected_version,
            Some(&route),
        )?;
        if !cas.applied {
            let _ = self.release_reserved_allocations(&targets, &reservations);
            return Err(StoreError::Conflict(format!(
                "route update lost race for tenant={tenant} key={key}"
            )));
        }
        if let Some(previous) = current.as_ref() {
            self.schedule_route_reclaim(previous)?;
        }
        Ok(route)
    }

    fn resolve_objects(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<ResolvedObject>> {
        let scoped = objects
            .iter()
            .map(|object| {
                let tenant = object.tenant.unwrap_or(self.default_tenant());
                (tenant.to_string(), self.scoped_key(tenant, object.key))
            })
            .collect::<Vec<_>>();
        let routes = self.route_directory.get_object_routes(
            &self.lease,
            &scoped
                .iter()
                .map(|(_, scoped)| scoped.clone())
                .collect::<Vec<_>>(),
        )?;
        let mut resolved = Vec::with_capacity(objects.len());
        for (((tenant, _scoped), object), route) in scoped
            .into_iter()
            .zip(objects.iter())
            .zip(routes.into_iter())
        {
            let route = route.ok_or_else(|| {
                StoreError::NotFound(format!("tenant={tenant} key={}", object.key))
            })?;
            if route.state != RouteState::Active {
                return Err(StoreError::InvalidState(format!(
                    "tenant={tenant} key={} is not active",
                    object.key
                )));
            }
            let replica = route
                .replicas
                .iter()
                .min_by_key(|replica| replica.priority)
                .cloned()
                .ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "tenant={tenant} key={} has no replica",
                        object.key
                    ))
                })?;
            resolved.push(ResolvedObject {
                tenant: tenant.to_string(),
                key: object.key.to_string(),
                replica,
            });
        }
        Ok(resolved)
    }

    fn execute_batch_get_into(
        &self,
        resolved: &[ResolvedObject],
        buffers: &mut [&mut [u8]],
    ) -> Result<Vec<usize>> {
        self.ensure_local_memory()?;
        let transport = self.transport()?;
        if resolved.len() != buffers.len() {
            return Err(StoreError::InvalidState(
                "resolved routes and output buffers length mismatch".to_string(),
            ));
        }

        let lengths = resolved
            .iter()
            .map(|entry| entry.replica.length as usize)
            .collect::<Vec<_>>();
        for (index, buffer) in buffers.iter().enumerate() {
            let needed = lengths[index];
            if buffer.len() < needed {
                return Err(StoreError::Allocator(format!(
                    "buffer too small for tenant={} key={}: need {needed}, have {}",
                    resolved[index].tenant,
                    resolved[index].key,
                    buffer.len()
                )));
            }
        }

        let local_paths = {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            resolved
                .iter()
                .map(|entry| {
                    entry.replica.owner == self.lease.runtime
                        && memory.has_storage_segment(&entry.replica.segment_name)
                })
                .collect::<Vec<_>>()
        };

        {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            for ((entry, buffer), local) in resolved
                .iter()
                .zip(buffers.iter_mut())
                .zip(local_paths.iter())
            {
                if !*local {
                    continue;
                }
                let source = memory.storage_address(
                    &entry.replica.segment_name,
                    entry.replica.segment_offset as usize,
                )?;
                unsafe {
                    ptr::copy_nonoverlapping(
                        source.cast::<u8>(),
                        buffer.as_mut_ptr(),
                        entry.replica.length as usize,
                    );
                }
            }
        }

        let remote_indices = local_paths
            .iter()
            .enumerate()
            .filter_map(|(index, local)| (!*local).then_some(index))
            .collect::<Vec<_>>();
        if remote_indices.is_empty() {
            return Ok(lengths);
        }

        let direct_paths = {
            let state = self.state.lock();
            remote_indices
                .iter()
                .map(|index| {
                    state.buffer_is_registered(
                        buffers[*index].as_ptr().cast_mut().cast::<c_void>(),
                        lengths[*index],
                    )
                })
                .collect::<Vec<_>>()
        };
        let scratch_lengths = direct_paths
            .iter()
            .zip(remote_indices.iter())
            .filter_map(|(direct, index)| (!*direct).then_some(lengths[*index]))
            .collect::<Vec<_>>();
        let scratch = if scratch_lengths.is_empty() {
            Vec::new()
        } else {
            let state = self.state.lock();
            state.memory_ref()?.plan_scratch(&scratch_lengths)?
        };

        let requests = {
            let mut state = self.state.lock();
            let mut batch = Vec::with_capacity(remote_indices.len());
            let mut scratch_index = 0usize;
            for (position, index) in remote_indices.iter().enumerate() {
                let entry = &resolved[*index];
                let source = if direct_paths[position] {
                    buffers[*index].as_mut_ptr().cast::<c_void>()
                } else {
                    let addr = scratch[scratch_index].addr;
                    scratch_index += 1;
                    addr
                };
                let segment = state.open_segment(transport, &entry.replica.segment_name.0)?;
                batch.push(TransferRequest {
                    opcode: Opcode::Read,
                    source,
                    target_id: segment,
                    target_offset: entry.replica.offset,
                    length: entry.replica.length,
                });
            }
            batch
        };

        let batch_id = transport.allocate_batch(requests.len())?;
        let submit_result = transport.submit(batch_id, &requests);
        if let Err(error) = submit_result {
            let _ = transport.free_batch(batch_id);
            return Err(error);
        }
        let wait_result = wait_for_batch_completion(transport, batch_id, DEFAULT_TRANSFER_TIMEOUT);
        let free_result = transport.free_batch(batch_id);
        wait_result?;
        free_result?;

        let mut scratch_index = 0usize;
        for (position, index) in remote_indices.iter().enumerate() {
            if !direct_paths[position] {
                unsafe {
                    ptr::copy_nonoverlapping(
                        scratch[scratch_index].addr.cast::<u8>(),
                        buffers[*index].as_mut_ptr(),
                        lengths[*index],
                    );
                }
                scratch_index += 1;
            }
        }

        Ok(lengths)
    }

    fn segment_name(&self) -> Result<SegmentName> {
        self.lease
            .endpoints
            .segment_name
            .clone()
            .ok_or_else(|| StoreError::InvalidState("segment_name is not configured".to_string()))
    }

    fn write_reserved_replicas(
        &self,
        targets: &[ReplicaWriteTarget],
        reservations: &[mooncake_store_core::SegmentReservation],
        value: &[u8],
    ) -> Result<Vec<u64>> {
        if targets.len() != reservations.len() {
            return Err(StoreError::InvalidState(
                "targets and reservations length mismatch".to_string(),
            ));
        }
        let transport = self.transport()?;
        let mut absolute_offsets = vec![0u64; targets.len()];
        let mut remote_requests = Vec::new();

        for (index, (target, reservation)) in targets.iter().zip(reservations.iter()).enumerate() {
            if target.runtime == self.lease.runtime
                && self
                    .state
                    .lock()
                    .memory_ref()
                    .map(|memory| memory.has_storage_segment(&target.segment_name))
                    .unwrap_or(false)
            {
                let addr = {
                    let state = self.state.lock();
                    state
                        .memory_ref()?
                        .storage_address(&target.segment_name, reservation.offset_bytes as usize)?
                };
                unsafe {
                    ptr::copy_nonoverlapping(value.as_ptr(), addr.cast::<u8>(), value.len());
                }
                absolute_offsets[index] = addr as u64;
                continue;
            }

            let handle = {
                let mut state = self.state.lock();
                state.open_segment(transport, &target.segment_name.0)?
            };
            let info = transport.get_segment_info(handle)?;
            let buffer = info.buffers.first().ok_or_else(|| {
                StoreError::Transport(format!(
                    "segment {} exposes no buffers",
                    target.segment_name.0
                ))
            })?;
            let target_offset = buffer
                .base
                .checked_add(reservation.offset_bytes)
                .ok_or_else(|| {
                    StoreError::Transport("remote target offset overflow".to_string())
                })?;
            remote_requests.push((index, handle, target_offset));
            absolute_offsets[index] = target_offset;
        }

        if !remote_requests.is_empty() {
            let scratch = {
                let state = self.state.lock();
                state.memory_ref()?.plan_scratch(&[value.len()])?
            };
            copy_into_region(scratch[0], value);
            let batch_id = transport.allocate_batch(remote_requests.len())?;
            let requests = remote_requests
                .iter()
                .map(|(_, handle, target_offset)| TransferRequest {
                    opcode: Opcode::Write,
                    source: scratch[0].addr,
                    target_id: *handle,
                    target_offset: *target_offset,
                    length: value.len() as u64,
                })
                .collect::<Vec<_>>();
            let submit_result = transport.submit(batch_id, &requests);
            if let Err(error) = submit_result {
                let _ = transport.free_batch(batch_id);
                return Err(error);
            }
            let wait_result =
                wait_for_batch_completion(transport, batch_id, DEFAULT_TRANSFER_TIMEOUT);
            let free_result = transport.free_batch(batch_id);
            wait_result?;
            free_result?;
        }

        Ok(absolute_offsets)
    }

    fn batch_put_scoped_routed(&self, requests: &[PutRequest<'_>]) -> Result<Vec<ObjectRoute>> {
        self.ensure_local_memory()?;
        self.flush_due_reclaims()?;
        self.validate_unique_requests(requests)?;
        let transport = self.transport()?;
        let WriteMode::Routed {
            planner,
            replica_count,
        } = &self.write_mode
        else {
            return Err(StoreError::InvalidState(
                "batch routed put requires routed write mode".to_string(),
            ));
        };

        let object_refs = requests
            .iter()
            .map(|request| {
                let mut object = ObjectRef::new(request.key);
                if let Some(tenant) = request.tenant {
                    object = object.tenant(tenant);
                }
                object
            })
            .collect::<Vec<_>>();
        let plans = planner.plan(self, &object_refs, *replica_count)?;

        let reservation_requests = requests
            .iter()
            .zip(plans.iter())
            .flat_map(|(request, plan)| {
                plan.owners
                    .iter()
                    .map(move |owner| OwnerReservationRequest {
                        owner: owner.clone(),
                        length_bytes: request.value.len() as u64,
                        require_local_memory: false,
                    })
            })
            .collect::<Vec<_>>();
        let mut reserved_allocations = self
            .reserve_owner_segments_batch(&reservation_requests)?
            .into_iter();
        let release_prepared = |entries: &[PreparedObjectWrite<'_>]| {
            for entry in entries {
                let _ = self.release_reserved_allocations(&entry.targets, &entry.reservations);
            }
        };

        let mut prepared = Vec::with_capacity(requests.len());
        let mut scratch_lengths = Vec::new();
        for (request, plan) in requests.iter().zip(plans.iter()) {
            let tenant = request.tenant.unwrap_or(self.default_tenant());
            let scoped_key = self.scoped_key(tenant, request.key);
            let mut targets = Vec::with_capacity(plan.owners.len());
            let mut reservations = Vec::with_capacity(plan.owners.len());
            for _ in &plan.owners {
                let Some((target, reservation)) = reserved_allocations.next() else {
                    release_prepared(&prepared);
                    return Err(StoreError::InvalidState(
                        "missing owner reservation from batch allocator".to_string(),
                    ));
                };
                targets.push(target);
                reservations.push(reservation);
            }
            let has_remote = targets.iter().any(|target| {
                !(target.runtime == self.lease.runtime
                    && self
                        .state
                        .lock()
                        .memory_ref()
                        .map(|memory| memory.has_storage_segment(&target.segment_name))
                        .unwrap_or(false))
            });
            let scratch_index = has_remote.then(|| {
                let index = scratch_lengths.len();
                scratch_lengths.push(request.value.len());
                index
            });
            prepared.push(PreparedObjectWrite {
                scoped_key,
                value: request.value,
                targets,
                reservations,
                scratch_index,
            });
        }
        if reserved_allocations.next().is_some() {
            release_prepared(&prepared);
            return Err(StoreError::InvalidState(
                "extra owner reservations remain after batch prepare".to_string(),
            ));
        }

        let scratch_slots = if scratch_lengths.is_empty() {
            Vec::new()
        } else {
            let state = self.state.lock();
            state.memory_ref()?.plan_scratch(&scratch_lengths)?
        };
        for entry in &prepared {
            if let Some(index) = entry.scratch_index {
                copy_into_region(scratch_slots[index], entry.value);
            }
        }

        let mut remote_requests = Vec::new();
        let current_routes = match self.route_directory.get_object_routes(
            &self.lease,
            &prepared
                .iter()
                .map(|entry| entry.scoped_key.clone())
                .collect::<Vec<_>>(),
        ) {
            Ok(routes) => routes,
            Err(error) => {
                release_prepared(&prepared);
                return Err(error);
            }
        };
        let mut routes = Vec::with_capacity(prepared.len());

        for (entry, current) in prepared.iter().zip(current_routes.into_iter()) {
            let mut replicas = Vec::with_capacity(entry.targets.len());
            for (priority, (target, reservation)) in entry
                .targets
                .iter()
                .zip(entry.reservations.iter())
                .enumerate()
            {
                let is_local = target.runtime == self.lease.runtime
                    && self
                        .state
                        .lock()
                        .memory_ref()
                        .map(|memory| memory.has_storage_segment(&target.segment_name))
                        .unwrap_or(false);
                let offset = if is_local {
                    let addr = {
                        let state = self.state.lock();
                        state.memory_ref()?.storage_address(
                            &target.segment_name,
                            reservation.offset_bytes as usize,
                        )?
                    };
                    unsafe {
                        ptr::copy_nonoverlapping(
                            entry.value.as_ptr(),
                            addr.cast::<u8>(),
                            entry.value.len(),
                        );
                    }
                    addr as u64
                } else {
                    let handle = {
                        let mut state = self.state.lock();
                        state.open_segment(transport, &target.segment_name.0)?
                    };
                    let info = transport.get_segment_info(handle)?;
                    let buffer = info.buffers.first().ok_or_else(|| {
                        StoreError::Transport(format!(
                            "segment {} exposes no buffers",
                            target.segment_name.0
                        ))
                    })?;
                    let target_offset = buffer
                        .base
                        .checked_add(reservation.offset_bytes)
                        .ok_or_else(|| {
                            StoreError::Transport("remote target offset overflow".to_string())
                        })?;
                    let scratch = scratch_slots[entry.scratch_index.ok_or_else(|| {
                        StoreError::InvalidState(
                            "remote write is missing a scratch slot".to_string(),
                        )
                    })?];
                    remote_requests.push(TransferRequest {
                        opcode: Opcode::Write,
                        source: scratch.addr,
                        target_id: handle,
                        target_offset,
                        length: entry.value.len() as u64,
                    });
                    target_offset
                };
                replicas.push(ReplicaRoute {
                    owner: target.runtime.clone(),
                    segment_name: target.segment_name.clone(),
                    offset,
                    segment_offset: reservation.offset_bytes,
                    length: entry.value.len() as u64,
                    checksum: None,
                    tier: ReplicaTier::Dram,
                    priority: priority as u16,
                });
            }

            let expected_version = current.as_ref().map(|route| route.version);
            let next_version = current
                .as_ref()
                .map(|route| route.version.next())
                .unwrap_or(RouteVersion(1));
            routes.push(PendingRoutePublish {
                key: entry.scoped_key.clone(),
                expected_version,
                previous: current,
                route: ObjectRoute {
                    key: entry.scoped_key.clone(),
                    version: next_version,
                    state: RouteState::Active,
                    compatibility: self.lease.compatibility.clone(),
                    replicas,
                },
            });
        }

        if !remote_requests.is_empty() {
            let batch_id = transport.allocate_batch(remote_requests.len())?;
            let submit_result = transport.submit(batch_id, &remote_requests);
            if let Err(error) = submit_result {
                let _ = transport.free_batch(batch_id);
                release_prepared(&prepared);
                return Err(error);
            }
            let wait_result =
                wait_for_batch_completion(transport, batch_id, DEFAULT_TRANSFER_TIMEOUT);
            let free_result = transport.free_batch(batch_id);
            if let Err(error) = wait_result {
                release_prepared(&prepared);
                return Err(error);
            }
            if let Err(error) = free_result {
                release_prepared(&prepared);
                return Err(error);
            }
        }

        let cas_requests = routes
            .iter()
            .map(|pending| RouteCasRequest {
                key: pending.key.clone(),
                expected: pending.expected_version,
                next: Some(pending.route.clone()),
            })
            .collect::<Vec<_>>();
        let cas_results = match self
            .route_directory
            .compare_and_swap_object_routes(&self.lease, &cas_requests)
        {
            Ok(results) => results,
            Err(error) => {
                release_prepared(&prepared);
                return Err(error);
            }
        };

        let mut published = Vec::with_capacity(routes.len());
        let mut first_error = None;
        for ((index, pending), cas_result) in
            routes.into_iter().enumerate().zip(cas_results.into_iter())
        {
            match cas_result {
                Ok(cas) if cas.applied => {
                    if let Some(previous) = pending.previous.as_ref() {
                        self.schedule_route_reclaim(previous)?;
                    }
                    published.push(pending.route);
                }
                Ok(_) => {
                    let _ = self.release_reserved_allocations(
                        &prepared[index].targets,
                        &prepared[index].reservations,
                    );
                    first_error.get_or_insert_with(|| {
                        StoreError::Conflict(format!(
                            "route update lost race for key {}",
                            pending.key.0
                        ))
                    });
                }
                Err(error) => {
                    let _ = self.release_reserved_allocations(
                        &prepared[index].targets,
                        &prepared[index].reservations,
                    );
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(published)
    }

    fn validate_unique_requests(&self, requests: &[PutRequest<'_>]) -> Result<()> {
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

impl MooncakeCompatibilityFacade for StoreClient {
    fn heartbeat(&mut self, expires_at_ms: u64) -> Result<()> {
        let _span = info_span!(
            "store.heartbeat",
            runtime = %self.lease.runtime,
            expires_at_ms
        )
        .entered();
        let tracker = OperationTracker::new("heartbeat");
        self.lease.expires_at_ms = expires_at_ms;
        let result = self.metadata.upsert_client_lease(&self.lease);
        tracker.finish(&result, 0);
        result
    }

    fn enter_standby(&mut self) -> Result<()> {
        let _span = info_span!("store.enter_standby", runtime = %self.lease.runtime).entered();
        let tracker = OperationTracker::new("enter_standby");
        self.lease.state = ClientLifecycleState::Standby;
        let result = self
            .metadata
            .update_client_state(&self.lease.runtime, self.lease.state);
        tracker.finish(&result, 0);
        result
    }

    fn activate(&mut self) -> Result<()> {
        let _span = info_span!("store.activate", runtime = %self.lease.runtime).entered();
        let tracker = OperationTracker::new("activate");
        self.lease.state = ClientLifecycleState::Active;
        let result = self
            .metadata
            .update_client_state(&self.lease.runtime, self.lease.state);
        tracker.finish(&result, 0);
        result
    }

    fn enter_draining(&mut self) -> Result<()> {
        let _span = info_span!("store.enter_draining", runtime = %self.lease.runtime).entered();
        let tracker = OperationTracker::new("enter_draining");
        self.lease.state = ClientLifecycleState::Draining;
        let result = self
            .metadata
            .update_client_state(&self.lease.runtime, self.lease.state);
        tracker.finish(&result, 0);
        result
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
        let segment = SegmentAnnouncement {
            owner: self.lease.runtime.clone(),
            segment_name: self.segment_name()?,
            capacity_bytes,
            used_bytes,
            state: SegmentLifecycleState::Active,
            alignment_bytes: self.local_memory.alignment as u64,
            tags,
        };
        self.allocator.lock().upsert(&segment);
        let result = self.metadata.publish_segment(&segment);
        tracker.finish(&result, used_bytes);
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
        let segment_name = {
            let mut state = self.state.lock();
            state.next_segment_name(&primary)
        };
        let transport = self.transport_factory()?.create(&segment_name.0)?;
        {
            let mut state = self.state.lock();
            state.memory_mut()?.add_storage_segment(
                transport.as_ref(),
                StorageSegmentSpec {
                    segment_name: segment_name.clone(),
                    capacity_bytes: storage_bytes,
                    state: SegmentLifecycleState::Active,
                    tags: self.local_memory.tags.clone(),
                    location: self.local_memory.location.clone(),
                    alignment: self.local_memory.alignment,
                },
            )?;
            state
                .local_transports
                .insert(segment_name.0.clone(), transport);
        };
        let announcement = SegmentAnnouncement {
            owner: self.lease.runtime.clone(),
            segment_name,
            capacity_bytes: storage_bytes as u64,
            used_bytes: 0,
            state: SegmentLifecycleState::Active,
            alignment_bytes: self.local_memory.alignment as u64,
            tags: self.local_memory.tags.clone(),
        };
        self.allocator.lock().upsert(&announcement);
        info!(
            runtime = %self.lease.runtime,
            segment = %announcement.segment_name.0,
            storage_bytes,
            "expanded local memory with a new active segment"
        );
        let publish_result = self.metadata.publish_segment(&announcement);
        tracker.finish(&publish_result, 0);
        publish_result?;
        Ok(announcement)
    }

    fn drain_segment(&self, segment: &SegmentName) -> Result<()> {
        let _span = info_span!(
            "store.drain_segment",
            runtime = %self.lease.runtime,
            segment = %segment.0
        )
        .entered();
        let tracker = OperationTracker::new("drain_segment");
        self.ensure_local_memory()?;
        let primary = self.segment_name()?;
        let active_count = {
            let state = self.state.lock();
            state
                .memory_ref()?
                .storage_segments()
                .into_iter()
                .filter(|entry| entry.state == SegmentLifecycleState::Active)
                .count()
        };
        if active_count <= 1 && *segment == primary {
            let result = Err(StoreError::InvalidState(
                "cannot drain the last active local segment".to_string(),
            ));
            tracker.finish(&result, 0);
            return result;
        }
        {
            let mut state = self.state.lock();
            state
                .memory_mut()?
                .update_storage_state(segment, SegmentLifecycleState::Draining)?;
        }
        self.allocator
            .lock()
            .update_state(segment, SegmentLifecycleState::Draining)?;
        info!(
            runtime = %self.lease.runtime,
            segment = %segment.0,
            "segment entered draining state"
        );
        let result = self.metadata.update_segment_state(
            &self.lease.runtime,
            segment,
            SegmentLifecycleState::Draining,
        );
        tracker.finish(&result, 0);
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
        result
    }

    fn query_route(&self, key: &str) -> Result<Option<ObjectRoute>> {
        self.query_route_in_tenant(self.default_tenant(), key)
    }

    fn query_route_in_tenant(&self, tenant: &str, key: &str) -> Result<Option<ObjectRoute>> {
        self.route_directory
            .get_object_route(&self.lease, &self.scoped_key(tenant, key))
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
        self.route_directory.compare_and_swap_object_route(
            &self.lease,
            &self.scoped_key(tenant, key),
            expected,
            next,
        )
    }

    fn register_local_memory(&self) -> Result<()> {
        let _span =
            info_span!("store.register_local_memory", runtime = %self.lease.runtime).entered();
        let tracker = OperationTracker::new("register_local_memory");
        let result = self.ensure_local_memory();
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
        Ok(self
            .query_route_in_tenant(tenant, key)?
            .and_then(|route| {
                route
                    .replicas
                    .iter()
                    .min_by_key(|replica| replica.priority)
                    .map(|replica| replica.length as usize)
            })
            .unwrap_or(0))
    }

    fn is_exist(&self, key: &str) -> Result<bool> {
        self.is_exist_in_tenant(self.default_tenant(), key)
    }

    fn is_exist_in_tenant(&self, tenant: &str, key: &str) -> Result<bool> {
        Ok(self.query_route_in_tenant(tenant, key)?.is_some())
    }

    fn batch_is_exist(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<bool>> {
        let mut results = Vec::with_capacity(objects.len());
        for object in objects {
            let tenant = object.tenant.unwrap_or(self.default_tenant());
            results.push(self.is_exist_in_tenant(tenant, object.key)?);
        }
        Ok(results)
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
        let _ = force;
        let scoped_key = self.scoped_key(tenant, key);
        let Some(route) = self
            .route_directory
            .get_object_route(&self.lease, &scoped_key)?
        else {
            let result = Err(StoreError::NotFound(format!("tenant={tenant} key={key}")));
            tracker.finish(&result, 0);
            return result;
        };
        let cas = self.route_directory.compare_and_swap_object_route(
            &self.lease,
            &scoped_key,
            Some(route.version),
            None,
        )?;
        let result = if cas.applied {
            self.schedule_route_reclaim(&route)
        } else {
            Err(StoreError::Conflict(format!(
                "route delete lost race for tenant={tenant} key={key}"
            )))
        };
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
            self.remove_in_tenant(tenant, object.key, force)?;
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
        let tracker = OperationTracker::new("put").input_bytes(value.len() as u64);
        let result = self.put_scoped_with_policy(tenant, key, value, None);
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
        let tracker = OperationTracker::new("put").input_bytes(value.len() as u64);
        let result = self.put_scoped_with_policy(tenant, key, value, Some(policy));
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
        let tracker = OperationTracker::new("put_from").input_bytes(size as u64);
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
        let value = unsafe { slice::from_raw_parts(buffer.cast::<u8>(), size) };
        let result = self.put_scoped_with_policy(tenant, key, value, None);
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
        let tracker = OperationTracker::new("put_from").input_bytes(size as u64);
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
        let value = unsafe { slice::from_raw_parts(buffer.cast::<u8>(), size) };
        let result = self.put_scoped_with_policy(tenant, key, value, Some(policy));
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
        let tracker = OperationTracker::new("batch_put").input_bytes(bytes_in);
        let can_use_fast_routed_batch = matches!(self.write_mode, WriteMode::Routed { .. })
            && requests.iter().all(|request| request.policy.is_none());
        if can_use_fast_routed_batch {
            let result = self.batch_put_scoped_routed(requests);
            tracker.finish(&result, bytes_in);
            return result;
        }
        let mut routes = Vec::with_capacity(requests.len());
        for request in requests {
            let tenant = request.tenant.unwrap_or(self.default_tenant());
            routes.push(self.put_scoped_with_policy(
                tenant,
                request.key,
                request.value,
                request.policy.as_ref(),
            )?);
        }
        let result = Ok(routes);
        tracker.finish(&result, bytes_in);
        result
    }

    fn batch_put_from(&self, requests: &[PutFromRequest<'_>]) -> Result<Vec<ObjectRoute>> {
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
        let mut routes = Vec::with_capacity(requests.len());
        for request in requests {
            let tenant = request.tenant.unwrap_or(self.default_tenant());
            routes.push(
                self.put_from_in_tenant_with_policy(
                    tenant,
                    request.key,
                    request.buffer,
                    request.size,
                    request
                        .policy
                        .as_ref()
                        .unwrap_or(&ReplicationPolicy::default()),
                )?,
            );
        }
        let result = Ok(routes);
        tracker.finish(&result, bytes_in);
        result
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
        let tracker = OperationTracker::new("batch_put_from_multi_buffers").input_bytes(bytes_in);
        let mut routes = Vec::with_capacity(requests.len());
        for request in requests {
            let tenant = request.tenant.unwrap_or(self.default_tenant());
            let payload = flatten_slices(request.buffers);
            routes.push(self.put_scoped_with_policy(
                tenant,
                request.key,
                &payload,
                request.policy.as_ref(),
            )?);
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
        let tracker = OperationTracker::new("get");
        let objects = [ObjectRef::new(key).tenant(tenant)];
        let mut results = self.batch_get(&objects)?;
        let result = results
            .pop()
            .ok_or_else(|| StoreError::InvalidState("missing batch_get result".to_string()));
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
        let tracker = OperationTracker::new("get_into");
        let mut requests = [GetRequest::new(key, buffer).tenant(tenant)];
        let mut sizes = self.batch_get_into(&mut requests)?;
        let result = sizes
            .pop()
            .ok_or_else(|| StoreError::InvalidState("missing batch_get_into result".to_string()));
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
        let tracker = OperationTracker::new("batch_get");
        let resolved = self.resolve_objects(objects)?;
        let mut buffers = resolved
            .iter()
            .map(|entry| vec![0u8; entry.replica.length as usize])
            .collect::<Vec<_>>();
        let mut slices = buffers
            .iter_mut()
            .map(|buffer| buffer.as_mut_slice())
            .collect::<Vec<_>>();
        self.execute_batch_get_into(&resolved, &mut slices)?;
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
        let tracker = OperationTracker::new("batch_get_into");
        let objects = requests
            .iter()
            .map(|request| {
                let mut object = ObjectRef::new(request.key);
                if let Some(tenant) = request.tenant {
                    object = object.tenant(tenant);
                }
                object
            })
            .collect::<Vec<_>>();
        let resolved = self.resolve_objects(&objects)?;
        let mut buffers = requests
            .iter_mut()
            .map(|request| &mut *request.buffer)
            .collect::<Vec<_>>();
        let result = self.execute_batch_get_into(&resolved, &mut buffers);
        let bytes_out = result
            .as_ref()
            .map(|sizes| sizes.iter().copied().sum::<usize>() as u64)
            .unwrap_or(0);
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
        let tracker = OperationTracker::new("batch_get_into_multi_buffers");
        let objects = requests
            .iter()
            .map(|request| {
                let mut object = ObjectRef::new(request.key);
                if let Some(tenant) = request.tenant {
                    object = object.tenant(tenant);
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

impl Drop for StoreClient {
    fn drop(&mut self) {
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

#[derive(Default)]
struct StoreState {
    memory: Option<LocalMemoryState>,
    registered_buffers: BTreeMap<usize, usize>,
    local_transports: BTreeMap<String, Arc<dyn StoreTransport>>,
    remote_segments: BTreeMap<String, u64>,
    pending_reclaims: VecDeque<PendingReclaim>,
    next_local_segment_id: u64,
}

#[derive(Default)]
struct LocalAllocatorState {
    segments: BTreeMap<SegmentName, SegmentAllocator>,
}

impl LocalAllocatorState {
    fn upsert(&mut self, announcement: &SegmentAnnouncement) {
        match self.segments.get_mut(&announcement.segment_name) {
            Some(segment) => segment.merge_announcement(announcement),
            None => {
                self.segments.insert(
                    announcement.segment_name.clone(),
                    SegmentAllocator::new(announcement.clone()),
                );
            }
        }
    }

    fn announcement(&self, segment_name: &SegmentName) -> Option<SegmentAnnouncement> {
        self.segments
            .get(segment_name)
            .map(|segment| segment.announcement.clone())
    }

    fn announcements(&self) -> Vec<SegmentAnnouncement> {
        self.segments
            .values()
            .map(|segment| segment.announcement.clone())
            .collect()
    }

    fn update_state(
        &mut self,
        segment_name: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        let segment = self
            .segments
            .get_mut(segment_name)
            .ok_or_else(|| StoreError::NotFound(format!("segment {} not found", segment_name.0)))?;
        segment.announcement.state = next;
        Ok(())
    }

    fn remove(&mut self, segment_name: &SegmentName) {
        self.segments.remove(segment_name);
    }

    fn reserve_any(
        &mut self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let mut candidates = self
            .segments
            .values()
            .filter(|segment| segment.announcement.state == SegmentLifecycleState::Active)
            .map(|segment| {
                (
                    segment.remaining_capacity(),
                    segment.announcement.segment_name.clone(),
                )
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
        let mut last_capacity_error = None;
        for (_, segment_name) in candidates {
            match self.reserve_specific(owner, &segment_name, length_bytes) {
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

    fn reserve_specific(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let segment = self
            .segments
            .get_mut(segment_name)
            .ok_or_else(|| StoreError::NotFound(format!("segment {} not found", segment_name.0)))?;
        segment.reserve(owner, segment_name, length_bytes)
    }

    fn release(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let segment = self
            .segments
            .get_mut(segment_name)
            .ok_or_else(|| StoreError::NotFound(format!("segment {} not found", segment_name.0)))?;
        segment.release(owner, segment_name, offset_bytes, length_bytes)
    }
}

struct SegmentAllocator {
    announcement: SegmentAnnouncement,
    cursor_bytes: u64,
    free_spans: Vec<FreeSpan>,
}

impl SegmentAllocator {
    fn new(announcement: SegmentAnnouncement) -> Self {
        Self {
            cursor_bytes: announcement.used_bytes,
            announcement,
            free_spans: Vec::new(),
        }
    }

    fn merge_announcement(&mut self, next: &SegmentAnnouncement) {
        self.announcement.owner = next.owner.clone();
        self.announcement.segment_name = next.segment_name.clone();
        self.announcement.capacity_bytes = next.capacity_bytes;
        self.announcement.tags = next.tags.clone();
        self.announcement.state = next.state;
        self.announcement.alignment_bytes = next.alignment_bytes.max(1);
        self.announcement.used_bytes = self.announcement.used_bytes.max(next.used_bytes);
        self.cursor_bytes = self.cursor_bytes.max(next.used_bytes);
    }

    fn remaining_capacity(&self) -> u64 {
        self.announcement
            .capacity_bytes
            .saturating_sub(self.announcement.used_bytes)
    }

    fn reserve(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if self.announcement.state != SegmentLifecycleState::Active {
            return Err(StoreError::InvalidState(format!(
                "segment {}:{} is not active",
                owner, segment_name.0
            )));
        }
        if length_bytes == 0 {
            return Err(StoreError::Allocator(
                "zero-length segment reservation is not supported".to_string(),
            ));
        }
        let alignment = self.announcement.alignment_bytes.max(1);
        let reserved_len = align_up_u64(length_bytes, alignment);

        if let Some(index) = self
            .free_spans
            .iter()
            .position(|span| span.length_bytes >= reserved_len)
        {
            let span = self.free_spans.remove(index);
            if span.length_bytes > reserved_len {
                self.free_spans.push(FreeSpan {
                    offset_bytes: span.offset_bytes + reserved_len,
                    length_bytes: span.length_bytes - reserved_len,
                });
                self.free_spans.sort_by_key(|entry| entry.offset_bytes);
            }
            self.announcement.used_bytes = self
                .announcement
                .used_bytes
                .checked_add(reserved_len)
                .ok_or_else(|| {
                StoreError::Allocator("segment reservation overflow".to_string())
            })?;
            return Ok(mooncake_store_core::SegmentReservation {
                owner: owner.clone(),
                segment_name: segment_name.clone(),
                offset_bytes: span.offset_bytes,
                length_bytes,
            });
        }

        let offset = align_up_u64(self.cursor_bytes, alignment);
        let next_cursor = offset
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("segment reservation overflow".to_string()))?;
        if next_cursor > self.announcement.capacity_bytes {
            return Err(StoreError::Allocator(format!(
                "segment capacity exhausted for {}:{} requested={} remaining={}",
                owner,
                segment_name.0,
                length_bytes,
                self.announcement.capacity_bytes.saturating_sub(offset)
            )));
        }
        self.cursor_bytes = next_cursor;
        self.announcement.used_bytes = self
            .announcement
            .used_bytes
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("segment reservation overflow".to_string()))?;
        Ok(mooncake_store_core::SegmentReservation {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            offset_bytes: offset,
            length_bytes,
        })
    }

    fn release(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let alignment = self.announcement.alignment_bytes.max(1);
        let reserved_len = align_up_u64(length_bytes, alignment);
        let end = offset_bytes
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("segment release overflow".to_string()))?;
        if end > self.announcement.capacity_bytes {
            return Err(StoreError::Allocator(format!(
                "segment release exceeds capacity for {}:{} offset={} len={}",
                owner, segment_name.0, offset_bytes, length_bytes
            )));
        }
        self.insert_free_span(offset_bytes, reserved_len);
        self.announcement.used_bytes = self.announcement.used_bytes.saturating_sub(reserved_len);
        self.trim_tail();
        if self.announcement.used_bytes == 0 {
            self.cursor_bytes = 0;
            self.free_spans.clear();
        }
        Ok(())
    }

    fn trim_tail(&mut self) {
        loop {
            let Some(last) = self.free_spans.last().cloned() else {
                return;
            };
            if last.offset_bytes + last.length_bytes != self.cursor_bytes {
                return;
            }
            self.cursor_bytes = last.offset_bytes;
            self.free_spans.pop();
        }
    }

    fn insert_free_span(&mut self, offset_bytes: u64, length_bytes: u64) {
        self.free_spans.push(FreeSpan {
            offset_bytes,
            length_bytes,
        });
        self.free_spans.sort_by_key(|entry| entry.offset_bytes);
        let mut merged: Vec<FreeSpan> = Vec::with_capacity(self.free_spans.len());
        for span in self.free_spans.drain(..) {
            if let Some(previous) = merged.last_mut() {
                let prev_end = previous.offset_bytes + previous.length_bytes;
                if prev_end >= span.offset_bytes {
                    let merged_end = prev_end.max(span.offset_bytes + span.length_bytes);
                    previous.length_bytes = merged_end - previous.offset_bytes;
                    continue;
                }
            }
            merged.push(span);
        }
        self.free_spans = merged;
    }
}

#[derive(Clone)]
struct FreeSpan {
    offset_bytes: u64,
    length_bytes: u64,
}

#[derive(Clone)]
enum WriteMode {
    LocalOnly,
    Routed {
        planner: PlacementPlanner,
        replica_count: usize,
    },
}

#[derive(Clone, Debug)]
struct ReplicaWriteTarget {
    runtime: ClientRuntimeId,
    segment_name: SegmentName,
}

struct PreparedObjectWrite<'a> {
    scoped_key: ObjectKey,
    value: &'a [u8],
    targets: Vec<ReplicaWriteTarget>,
    reservations: Vec<mooncake_store_core::SegmentReservation>,
    scratch_index: Option<usize>,
}

#[derive(Clone, Debug)]
struct OwnerReservationRequest {
    owner: ClientRuntimeId,
    length_bytes: u64,
    require_local_memory: bool,
}

#[derive(Clone, Debug)]
struct AllocationReleaseRequest {
    owner: ClientRuntimeId,
    segment_name: SegmentName,
    offset_bytes: u64,
    length_bytes: u64,
}

struct PendingRoutePublish {
    key: ObjectKey,
    expected_version: Option<RouteVersion>,
    previous: Option<ObjectRoute>,
    route: ObjectRoute,
}

#[derive(Clone, Debug)]
struct ResolvedReplicationPolicy {
    replica_count: usize,
    preferred_segments: Vec<SegmentName>,
    with_soft_pin: bool,
    prefer_alloc_in_same_node: bool,
}

#[derive(Clone, Debug)]
enum ReplicaPlacementTarget {
    Owner(ClientRuntimeId),
    Segment {
        owner: ClientRuntimeId,
        segment_name: SegmentName,
    },
}

impl ReplicaPlacementTarget {
    fn owner(&self) -> &ClientRuntimeId {
        match self {
            Self::Owner(owner) => owner,
            Self::Segment { owner, .. } => owner,
        }
    }
}

#[derive(Clone, Debug)]
struct PendingReclaim {
    due_at_ms: u64,
    owner: ClientRuntimeId,
    segment_name: SegmentName,
    offset_bytes: u64,
    length_bytes: u64,
}

struct LocalAuthorityAdapter;

impl AuthorityService for LocalAuthorityAdapter {
    fn get_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        authority_get(namespace, authority, key)
    }

    fn compare_and_swap_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        authority_compare_and_swap(namespace, authority, key, expected, next)
    }

    fn replace_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> Result<()> {
        authority_replace(namespace, authority, key, next)
    }

    fn batch_get_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Vec<Result<Option<ObjectRoute>>> {
        match authority_get_many(namespace, authority, keys) {
            Ok(routes) => routes.into_iter().map(Ok).collect(),
            Err(error) => keys
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }

    fn batch_compare_and_swap_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<CasResult>> {
        match authority_compare_and_swap_many(namespace, authority, requests) {
            Ok(results) => results.into_iter().map(Ok).collect(),
            Err(error) => requests
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }

    fn batch_replace_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<()>> {
        match authority_replace_many(namespace, authority, requests) {
            Ok(()) => requests.iter().map(|_| Ok(())).collect(),
            Err(error) => requests
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }
}

struct LocalAllocatorAdapter {
    runtime: ClientRuntimeId,
    allocator: Arc<Mutex<LocalAllocatorState>>,
}

impl AllocatorService for LocalAllocatorAdapter {
    fn reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if *owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "allocator rpc targeted runtime {} on {}",
                owner, self.runtime
            )));
        }
        self.allocator.lock().reserve_any(owner, length_bytes)
    }

    fn reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if *owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "allocator rpc targeted runtime {} on {}",
                owner, self.runtime
            )));
        }
        self.allocator
            .lock()
            .reserve_specific(owner, segment_name, length_bytes)
    }

    fn release(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        if *owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "allocator rpc targeted runtime {} on {}",
                owner, self.runtime
            )));
        }
        self.allocator
            .lock()
            .release(owner, segment_name, offset_bytes, length_bytes)
    }

    fn batch_reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: &[u64],
    ) -> Vec<Result<mooncake_store_core::SegmentReservation>> {
        if *owner != self.runtime {
            return length_bytes
                .iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "allocator rpc targeted runtime {} on {}",
                        owner, self.runtime
                    )))
                })
                .collect();
        }
        let mut allocator = self.allocator.lock();
        length_bytes
            .iter()
            .map(|length_bytes| allocator.reserve_any(owner, *length_bytes))
            .collect()
    }

    fn batch_reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        requests: &[ReserveSpecificOp],
    ) -> Vec<Result<mooncake_store_core::SegmentReservation>> {
        if *owner != self.runtime {
            return requests
                .iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "allocator rpc targeted runtime {} on {}",
                        owner, self.runtime
                    )))
                })
                .collect();
        }
        let mut allocator = self.allocator.lock();
        requests
            .iter()
            .map(|request| {
                allocator.reserve_specific(owner, &request.segment_name, request.length_bytes)
            })
            .collect()
    }

    fn batch_release(&self, owner: &ClientRuntimeId, requests: &[ReleaseOp]) -> Vec<Result<()>> {
        if *owner != self.runtime {
            return requests
                .iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "allocator rpc targeted runtime {} on {}",
                        owner, self.runtime
                    )))
                })
                .collect();
        }
        let mut allocator = self.allocator.lock();
        requests
            .iter()
            .map(|request| {
                allocator.release(
                    owner,
                    &request.segment_name,
                    request.offset_bytes,
                    request.length_bytes,
                )
            })
            .collect()
    }
}

impl StoreState {
    fn memory_ref(&self) -> Result<&LocalMemoryState> {
        self.memory
            .as_ref()
            .ok_or_else(|| StoreError::InvalidState("local memory is not registered".to_string()))
    }

    fn memory_mut(&mut self) -> Result<&mut LocalMemoryState> {
        self.memory
            .as_mut()
            .ok_or_else(|| StoreError::InvalidState("local memory is not registered".to_string()))
    }

    fn next_segment_name(&mut self, primary: &SegmentName) -> SegmentName {
        loop {
            let segment_name =
                SegmentName::new(format!("{}-ext-{}", primary.0, self.next_local_segment_id));
            self.next_local_segment_id = self.next_local_segment_id.saturating_add(1);
            if self
                .memory
                .as_ref()
                .is_none_or(|memory| !memory.has_storage_segment(&segment_name))
            {
                return segment_name;
            }
        }
    }

    fn open_segment(&mut self, transport: &dyn StoreTransport, segment_name: &str) -> Result<u64> {
        if let Some(handle) = self.remote_segments.get(segment_name) {
            return Ok(*handle);
        }
        let handle = transport.open_segment(segment_name)?;
        self.remote_segments
            .insert(segment_name.to_string(), handle);
        Ok(handle)
    }

    fn register_external_buffer(
        &mut self,
        transport: &dyn StoreTransport,
        buffer: *mut c_void,
        size: usize,
    ) -> Result<()> {
        self.ensure_non_overlapping(buffer, size)?;
        transport.register_memory(buffer, size)?;
        self.registered_buffers.insert(buffer as usize, size);
        Ok(())
    }

    fn unregister_external_buffer(
        &mut self,
        transport: &dyn StoreTransport,
        buffer: *mut c_void,
        size: usize,
    ) -> Result<()> {
        match self.registered_buffers.remove(&(buffer as usize)) {
            Some(registered) if registered == size => {
                transport.unregister_memory(buffer, size)?;
                Ok(())
            }
            Some(registered) => {
                self.registered_buffers.insert(buffer as usize, registered);
                Err(StoreError::Allocator(format!(
                    "registered buffer size mismatch: requested={size} registered={registered}"
                )))
            }
            None => Err(StoreError::NotFound(format!(
                "registered buffer {:p} not found",
                buffer
            ))),
        }
    }

    fn buffer_is_registered(&self, buffer: *mut c_void, size: usize) -> bool {
        self.registered_buffers
            .get(&(buffer as usize))
            .is_some_and(|registered| *registered >= size)
    }

    fn ensure_non_overlapping(&self, buffer: *mut c_void, size: usize) -> Result<()> {
        let start = buffer as usize;
        let end = start
            .checked_add(size)
            .ok_or_else(|| StoreError::Allocator("registered buffer range overflow".to_string()))?;
        if let Some((other_start, other_size)) = self
            .registered_buffers
            .range(..=start)
            .next_back()
            .map(|(key, value)| (*key, *value))
        {
            let other_end = other_start.saturating_add(other_size);
            if start < other_end {
                return Err(StoreError::Allocator(format!(
                    "registered buffer overlaps existing range {:x}..{:x}",
                    other_start, other_end
                )));
            }
        }
        if let Some((other_start, _)) = self
            .registered_buffers
            .range(start..)
            .next()
            .map(|(key, value)| (*key, *value))
        {
            if end > other_start {
                return Err(StoreError::Allocator(format!(
                    "registered buffer overlaps existing range starting at {:x}",
                    other_start
                )));
            }
        }
        Ok(())
    }

    fn take_due_reclaims(&mut self, now_ms: u64) -> Vec<PendingReclaim> {
        let mut ready = Vec::new();
        while self
            .pending_reclaims
            .front()
            .is_some_and(|entry| entry.due_at_ms <= now_ms)
        {
            if let Some(entry) = self.pending_reclaims.pop_front() {
                ready.push(entry);
            }
        }
        ready
    }
}

#[derive(Clone, Debug)]
struct ResolvedObject {
    tenant: String,
    key: String,
    replica: ReplicaRoute,
}

fn copy_into_region(allocation: RegionAllocation, value: &[u8]) {
    unsafe {
        ptr::copy_nonoverlapping(value.as_ptr(), allocation.addr.cast::<u8>(), value.len());
    }
}

fn flatten_slices(buffers: &[&[u8]]) -> Vec<u8> {
    let total = buffers.iter().map(|buffer| buffer.len()).sum();
    let mut payload = Vec::with_capacity(total);
    for buffer in buffers {
        payload.extend_from_slice(buffer);
    }
    payload
}

fn scatter_into_buffers(payload: &[u8], buffers: &mut [&mut [u8]]) {
    let mut cursor = 0usize;
    for buffer in buffers {
        if cursor >= payload.len() {
            buffer.fill(0);
            continue;
        }
        let remaining = payload.len() - cursor;
        let to_copy = remaining.min(buffer.len());
        buffer[..to_copy].copy_from_slice(&payload[cursor..cursor + to_copy]);
        if to_copy < buffer.len() {
            buffer[to_copy..].fill(0);
        }
        cursor += to_copy;
    }
}

fn compatibility_matches(left: &ClientLease, right: &ClientLease) -> bool {
    left.compatibility.store_api_version == right.compatibility.store_api_version
        && left.compatibility.metadata_schema_version == right.compatibility.metadata_schema_version
        && left.compatibility.transport_api_version == right.compatibility.transport_api_version
}

fn control_bind_host(rpc_address: &str) -> String {
    if rpc_address.is_empty() {
        return "127.0.0.1".to_string();
    }
    rpc_address
        .rsplit_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty() && *host != "0.0.0.0" && *host != "::")
        .unwrap_or("127.0.0.1")
        .to_string()
}

fn align_up_u64(value: u64, alignment: u64) -> u64 {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should advance")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::ffi::c_void;
    use std::ptr;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        CompatibilityDescriptor, HandoffKind, MetadataBackend, ObjectKey, SegmentAnnouncement,
        SegmentLifecycleState, SegmentName, StoreError,
    };
    use mooncake_transport::{
        Opcode, SegmentBuffer, SegmentInfo, SegmentKind, TransferProgress, TransferRequest,
        TransferStatus,
    };
    use parking_lot::Mutex;

    use crate::{
        render_prometheus_metrics, reset_metrics, transport::StoreTransport, GetRequest,
        LocalMemoryConfig, MooncakeCompatibilityFacade, MultiBufferGetRequest, ObjectRef,
        PlacementPlanner, PutRequest, ReplicationPolicy, StoreClientBuilder,
    };

    struct TestTransport {
        local_segment: String,
        state: Arc<Mutex<TestTransportState>>,
    }

    struct TestTransportState {
        next_handle: u64,
        next_batch: u64,
        allocations: BTreeMap<usize, Box<[u8]>>,
        segments_by_name: BTreeMap<String, u64>,
        segments_by_handle: BTreeMap<u64, TestSegment>,
        live_batches: BTreeSet<u64>,
        registered_memory: BTreeMap<usize, usize>,
    }

    #[derive(Clone, Copy)]
    struct TestSegment {
        base: usize,
        len: usize,
    }

    struct NoHotPathMetadataBackend {
        inner: Arc<InMemoryMetadataBackend>,
    }

    impl NoHotPathMetadataBackend {
        fn new(inner: Arc<InMemoryMetadataBackend>) -> Self {
            Self { inner }
        }
    }

    impl MetadataBackend for NoHotPathMetadataBackend {
        fn route_namespace(&self) -> String {
            self.inner.route_namespace()
        }

        fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
            self.inner.upsert_client_lease(lease)
        }

        fn update_client_state(
            &self,
            runtime: &ClientRuntimeId,
            next: ClientLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.inner.update_client_state(runtime, next)
        }

        fn list_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
            self.inner.list_live_clients()
        }

        fn publish_segment(
            &self,
            segment: &SegmentAnnouncement,
        ) -> mooncake_store_core::Result<()> {
            self.inner.publish_segment(segment)
        }

        fn unpublish_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
        ) -> mooncake_store_core::Result<()> {
            self.inner.unpublish_segment(owner, segment)
        }

        fn list_segments(
            &self,
            owner: Option<&ClientRuntimeId>,
        ) -> mooncake_store_core::Result<Vec<SegmentAnnouncement>> {
            self.inner.list_segments(owner)
        }

        fn update_segment_state(
            &self,
            owner: &ClientRuntimeId,
            segment: &SegmentName,
            next: SegmentLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.inner.update_segment_state(owner, segment, next)
        }

        fn reserve_segment(
            &self,
            _owner: &ClientRuntimeId,
            _segment: &SegmentName,
            _length_bytes: u64,
        ) -> mooncake_store_core::Result<mooncake_store_core::SegmentReservation> {
            Err(StoreError::Unsupported(
                "metadata allocator hot path is disabled in this test".to_string(),
            ))
        }

        fn release_segment(
            &self,
            _owner: &ClientRuntimeId,
            _segment: &SegmentName,
            _offset_bytes: u64,
            _length_bytes: u64,
        ) -> mooncake_store_core::Result<()> {
            Err(StoreError::Unsupported(
                "metadata allocator hot path is disabled in this test".to_string(),
            ))
        }

        fn get_object_route(
            &self,
            _key: &ObjectKey,
        ) -> mooncake_store_core::Result<Option<mooncake_store_core::ObjectRoute>> {
            Err(StoreError::Unsupported(
                "metadata route hot path is disabled in this test".to_string(),
            ))
        }

        fn compare_and_swap_object_route(
            &self,
            _key: &ObjectKey,
            _expected: Option<mooncake_store_core::RouteVersion>,
            _next: Option<&mooncake_store_core::ObjectRoute>,
        ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
            Err(StoreError::Unsupported(
                "metadata route hot path is disabled in this test".to_string(),
            ))
        }

        fn put_handoff(
            &self,
            handoff: &mooncake_store_core::HandoffPlan,
        ) -> mooncake_store_core::Result<()> {
            self.inner.put_handoff(handoff)
        }

        fn get_handoff(
            &self,
            stable_id: &mooncake_store_core::ClientStableId,
        ) -> mooncake_store_core::Result<Option<mooncake_store_core::HandoffPlan>> {
            self.inner.get_handoff(stable_id)
        }
    }

    impl TestTransport {
        fn new(local_segment: &str) -> Self {
            Self {
                local_segment: local_segment.to_string(),
                state: Arc::new(Mutex::new(TestTransportState {
                    next_handle: 1,
                    next_batch: 1,
                    allocations: BTreeMap::new(),
                    segments_by_name: BTreeMap::new(),
                    segments_by_handle: BTreeMap::new(),
                    live_batches: BTreeSet::new(),
                    registered_memory: BTreeMap::new(),
                })),
            }
        }

        fn peer(&self, local_segment: &str) -> Self {
            Self {
                local_segment: local_segment.to_string(),
                state: self.state.clone(),
            }
        }

        fn add_external_segment(&self, segment_name: &str, size: usize) -> u64 {
            let mut state = self.state.lock();
            let base = allocate_boxed_region(&mut state, size);
            register_segment(&mut state, segment_name.to_string(), base, size)
        }

        fn segment_bounds(&self, segment_name: &str) -> Option<(u64, u64)> {
            let state = self.state.lock();
            let handle = state.segments_by_name.get(segment_name)?;
            let segment = state.segments_by_handle.get(handle)?;
            Some((segment.base as u64, segment.len as u64))
        }
    }

    impl StoreTransport for TestTransport {
        fn segment_name(&self) -> mooncake_store_core::Result<String> {
            Ok(self.local_segment.clone())
        }

        fn rpc_server_address(&self) -> mooncake_store_core::Result<(String, u16)> {
            Ok(("127.0.0.1".to_string(), 0))
        }

        fn open_segment(&self, segment_name: &str) -> mooncake_store_core::Result<u64> {
            let state = self.state.lock();
            state
                .segments_by_name
                .get(segment_name)
                .copied()
                .ok_or_else(|| StoreError::NotFound(format!("segment {segment_name} not found")))
        }

        fn close_segment(&self, handle: u64) -> mooncake_store_core::Result<()> {
            let state = self.state.lock();
            if state.segments_by_handle.contains_key(&handle) {
                return Ok(());
            }
            Err(StoreError::NotFound(format!(
                "segment handle {handle} not found"
            )))
        }

        fn get_segment_info(&self, handle: u64) -> mooncake_store_core::Result<SegmentInfo> {
            let state = self.state.lock();
            let segment = state
                .segments_by_handle
                .get(&handle)
                .copied()
                .ok_or_else(|| {
                    StoreError::NotFound(format!("segment handle {handle} not found"))
                })?;
            Ok(SegmentInfo {
                kind: SegmentKind::Memory,
                buffers: vec![SegmentBuffer {
                    base: segment.base as u64,
                    length: segment.len as u64,
                    location: "cpu:0".to_string(),
                }],
            })
        }

        fn allocate_memory(
            &self,
            size: usize,
            _location: &str,
        ) -> mooncake_store_core::Result<*mut c_void> {
            let mut state = self.state.lock();
            let base = allocate_boxed_region(&mut state, size);
            if !state.segments_by_name.contains_key(&self.local_segment) {
                register_segment(&mut state, self.local_segment.clone(), base, size);
            }
            Ok(base as *mut c_void)
        }

        fn free_memory(&self, addr: *mut c_void) -> mooncake_store_core::Result<()> {
            let mut state = self.state.lock();
            let key = addr as usize;
            if state.allocations.remove(&key).is_none() {
                return Err(StoreError::NotFound(format!(
                    "allocation {:p} not found",
                    addr
                )));
            }
            let orphaned = state
                .segments_by_handle
                .iter()
                .filter_map(|(handle, segment)| (segment.base == key).then_some(*handle))
                .collect::<Vec<_>>();
            for handle in orphaned {
                state.segments_by_handle.remove(&handle);
                state
                    .segments_by_name
                    .retain(|_, current_handle| *current_handle != handle);
            }
            state.registered_memory.remove(&key);
            Ok(())
        }

        fn register_memory(
            &self,
            addr: *mut c_void,
            size: usize,
        ) -> mooncake_store_core::Result<()> {
            self.state
                .lock()
                .registered_memory
                .insert(addr as usize, size);
            Ok(())
        }

        fn unregister_memory(
            &self,
            addr: *mut c_void,
            size: usize,
        ) -> mooncake_store_core::Result<()> {
            let mut state = self.state.lock();
            match state.registered_memory.remove(&(addr as usize)) {
                Some(registered) if registered == size => Ok(()),
                Some(registered) => Err(StoreError::Allocator(format!(
                    "registered size mismatch: expected={registered} got={size}"
                ))),
                None => Err(StoreError::NotFound(format!(
                    "registered allocation {:p} not found",
                    addr
                ))),
            }
        }

        fn allocate_batch(&self, batch_size: usize) -> mooncake_store_core::Result<u64> {
            if batch_size == 0 {
                return Err(StoreError::Transport(
                    "batch_size must be greater than zero".to_string(),
                ));
            }
            let mut state = self.state.lock();
            let batch_id = state.next_batch;
            state.next_batch += 1;
            state.live_batches.insert(batch_id);
            Ok(batch_id)
        }

        fn free_batch(&self, batch_id: u64) -> mooncake_store_core::Result<()> {
            if self.state.lock().live_batches.remove(&batch_id) {
                return Ok(());
            }
            Err(StoreError::NotFound(format!("batch {batch_id} not found")))
        }

        fn submit(
            &self,
            batch_id: u64,
            requests: &[TransferRequest],
        ) -> mooncake_store_core::Result<()> {
            let state = self.state.lock();
            if !state.live_batches.contains(&batch_id) {
                return Err(StoreError::NotFound(format!("batch {batch_id} not found")));
            }
            for request in requests {
                let segment = state
                    .segments_by_handle
                    .get(&request.target_id)
                    .ok_or_else(|| {
                        StoreError::NotFound(format!(
                            "segment handle {} not found",
                            request.target_id
                        ))
                    })?;
                validate_request_bounds(*segment, request)?;
                unsafe {
                    match request.opcode {
                        Opcode::Write => ptr::copy_nonoverlapping(
                            request.source.cast::<u8>(),
                            request.target_offset as *mut u8,
                            request.length as usize,
                        ),
                        Opcode::Read => ptr::copy_nonoverlapping(
                            request.target_offset as *const u8,
                            request.source.cast::<u8>(),
                            request.length as usize,
                        ),
                    }
                }
            }
            Ok(())
        }

        fn task_status(
            &self,
            batch_id: u64,
            _task_id: usize,
        ) -> mooncake_store_core::Result<TransferProgress> {
            self.overall_status(batch_id)
        }

        fn overall_status(&self, batch_id: u64) -> mooncake_store_core::Result<TransferProgress> {
            if self.state.lock().live_batches.contains(&batch_id) {
                return Ok(TransferProgress {
                    status: TransferStatus::Completed,
                    transferred_bytes: 0,
                });
            }
            Err(StoreError::NotFound(format!("batch {batch_id} not found")))
        }
    }

    fn allocate_boxed_region(state: &mut TestTransportState, size: usize) -> usize {
        let mut memory = vec![0u8; size].into_boxed_slice();
        let base = memory.as_mut_ptr() as usize;
        state.allocations.insert(base, memory);
        base
    }

    fn register_segment(
        state: &mut TestTransportState,
        segment_name: String,
        base: usize,
        size: usize,
    ) -> u64 {
        let handle = state.next_handle;
        state.next_handle += 1;
        state.segments_by_name.insert(segment_name, handle);
        state
            .segments_by_handle
            .insert(handle, TestSegment { base, len: size });
        handle
    }

    fn validate_request_bounds(
        segment: TestSegment,
        request: &TransferRequest,
    ) -> mooncake_store_core::Result<()> {
        let start = usize::try_from(request.target_offset)
            .map_err(|_| StoreError::Transport("target offset does not fit usize".to_string()))?;
        let end = start
            .checked_add(request.length as usize)
            .ok_or_else(|| StoreError::Transport("request length overflow".to_string()))?;
        let segment_end = segment
            .base
            .checked_add(segment.len)
            .ok_or_else(|| StoreError::Transport("segment length overflow".to_string()))?;
        if start < segment.base || end > segment_end {
            return Err(StoreError::Transport(format!(
                "request out of segment bounds: start={start} end={end} segment={}..{}",
                segment.base, segment_end
            )));
        }
        Ok(())
    }

    fn storage_config() -> LocalMemoryConfig {
        LocalMemoryConfig::new()
            .storage_bytes(4096)
            .scratch_bytes(4096)
            .reclaim_grace_ms(0)
    }

    fn publish_storage_node(
        metadata: &InMemoryMetadataBackend,
        transport: &TestTransport,
        stable_id: &str,
        segment_name: &str,
        pool: &str,
    ) -> ClientRuntimeId {
        transport.add_external_segment(segment_name, 4096);
        let runtime = ClientRuntimeId::new(stable_id, ClientEpoch(1));
        let mut endpoints = ClientEndpointSet {
            rpc_address: "127.0.0.1:0".to_string(),
            segment_name: Some(SegmentName::new(segment_name)),
            labels: Default::default(),
        };
        endpoints
            .labels
            .insert("pool".to_string(), pool.to_string());
        endpoints
            .labels
            .insert("storage".to_string(), "true".to_string());
        metadata
            .upsert_client_lease(&ClientLease {
                runtime: runtime.clone(),
                state: ClientLifecycleState::Active,
                compatibility: CompatibilityDescriptor::default(),
                endpoints,
                expires_at_ms: 10_000,
            })
            .expect("storage lease should upsert");
        metadata
            .publish_segment(&SegmentAnnouncement {
                owner: runtime.clone(),
                segment_name: SegmentName::new(segment_name),
                capacity_bytes: 4096,
                used_bytes: 0,
                state: SegmentLifecycleState::Active,
                alignment_bytes: 64,
                tags: vec!["dram".to_string()],
            })
            .expect("storage segment should publish");
        runtime
    }

    #[test]
    fn hot_upgrade_handoff_is_published_after_draining() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let mut client = StoreClientBuilder::new(metadata.clone(), "client-a")
            .epoch(ClientEpoch(1))
            .state(ClientLifecycleState::Active)
            .rpc_address("127.0.0.1:7001")
            .segment_name("client-a-segment")
            .build(10_000)
            .expect("client build should succeed");

        client.enter_draining().expect("draining should succeed");
        let handoff = client
            .plan_handoff(ClientEpoch(2), HandoffKind::HotUpgrade, 7, 100, Some(1_000))
            .expect("handoff planning should succeed");

        let leases = metadata
            .list_live_clients()
            .expect("list clients should succeed");
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].state, ClientLifecycleState::Draining);

        let stored = metadata
            .get_handoff(&handoff.stable_id)
            .expect("get handoff should succeed")
            .expect("handoff should exist");
        assert_eq!(stored.from.epoch, ClientEpoch(1));
        assert_eq!(stored.to.epoch, ClientEpoch(2));
        assert_eq!(stored.kind, HandoffKind::HotUpgrade);
    }

    #[test]
    fn query_route_uses_default_tenant_scope() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let client = StoreClientBuilder::new(metadata.clone(), "client-a")
            .tenant("tenant-a")
            .rpc_address("127.0.0.1:7001")
            .segment_name("client-a-segment")
            .build(10_000)
            .expect("client build should succeed");

        assert!(client
            .query_route("same-key")
            .expect("query should succeed")
            .is_none());
        assert!(client
            .query_route_in_tenant("tenant-a", "same-key")
            .expect("tenant query should succeed")
            .is_none());
    }

    #[test]
    fn routed_batch_put_rejects_duplicate_scoped_keys() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("router-segment"));
        let planner = PlacementPlanner::new(metadata.clone());
        let client = StoreClientBuilder::new(metadata, "router")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "false")
            .transport(transport)
            .local_memory(storage_config())
            .routed_writes(planner, 1)
            .build(10_000)
            .expect("client build should succeed");

        let error = client
            .batch_put(&[
                PutRequest::new("dup-key", b"left").tenant("tenant-a"),
                PutRequest::new("dup-key", b"right").tenant("tenant-a"),
            ])
            .expect_err("duplicate scoped key should fail");

        assert!(matches!(error, StoreError::Conflict(_)));
        assert!(error
            .to_string()
            .contains("duplicate scoped key tenant-a::dup-key"));
    }

    #[test]
    fn batch_get_into_multi_buffers_rejects_insufficient_capacity() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("client-segment"));
        let client = StoreClientBuilder::new(metadata, "client-a")
            .state(ClientLifecycleState::Active)
            .transport(transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("client build should succeed");

        client.put("blob", b"abcdefgh").expect("put should succeed");
        let mut part_a = [0u8; 3];
        let mut part_b = [0u8; 3];
        let mut buffers: [&mut [u8]; 2] = [&mut part_a, &mut part_b];
        let mut requests = [MultiBufferGetRequest::new("blob", &mut buffers)];

        let error = client
            .batch_get_into_multi_buffers(&mut requests)
            .expect_err("insufficient multi-buffer capacity should fail");

        assert!(matches!(error, StoreError::Allocator(_)));
        assert!(error
            .to_string()
            .contains("multi-buffer capacity too small"));
    }

    #[test]
    fn observability_metrics_render_after_put_and_get() {
        reset_metrics();
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("metrics-segment"));
        let client = StoreClientBuilder::new(metadata, "client-metrics")
            .state(ClientLifecycleState::Active)
            .transport(transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("client build should succeed");

        client
            .put("metrics-key", b"abcdefgh")
            .expect("put should succeed");
        let value = client.get("metrics-key").expect("get should succeed");
        assert_eq!(value, b"abcdefgh");

        let metrics = render_prometheus_metrics();
        assert!(metrics.contains("mooncake_store_client_operation_total"));
        assert!(metrics.contains("operation=\"put\",status=\"ok\""));
        assert!(metrics.contains("operation=\"get\",status=\"ok\""));
        assert!(metrics.contains("mooncake_store_client_operation_bytes_out_total"));
    }

    #[test]
    fn routed_batch_put_publishes_replicated_route_with_absolute_offsets() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("router-segment"));
        let owner_a = publish_storage_node(
            metadata.as_ref(),
            transport.as_ref(),
            "storage-a",
            "seg-a",
            "pool-a",
        );
        let owner_b = publish_storage_node(
            metadata.as_ref(),
            transport.as_ref(),
            "storage-b",
            "seg-b",
            "pool-a",
        );
        let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
        let client = StoreClientBuilder::new(metadata.clone(), "router")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "false")
            .transport(transport.clone())
            .local_memory(storage_config())
            .routed_writes(planner, 2)
            .build(10_000)
            .expect("client build should succeed");

        let payload = b"routed-payload";
        let routes = client
            .batch_put(&[PutRequest::new("key-a", payload).tenant("tenant-a")])
            .expect("batch routed put should succeed");

        assert_eq!(routes.len(), 1);
        let route = &routes[0];
        assert_eq!(route.key, ObjectKey::new("tenant-a::key-a"));
        assert_eq!(route.version.0, 1);
        assert_eq!(route.replicas.len(), 2);
        assert_eq!(
            route
                .replicas
                .iter()
                .map(|replica| replica.priority)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        let owners = route
            .replicas
            .iter()
            .map(|replica| replica.owner.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(owners, BTreeSet::from([owner_a.clone(), owner_b.clone()]));

        for replica in &route.replicas {
            let (base, len) = transport
                .segment_bounds(&replica.segment_name.0)
                .expect("segment bounds should exist");
            assert!(replica.offset >= base);
            assert!(replica.offset + replica.length <= base + len);
            assert_eq!(replica.length as usize, payload.len());
        }

        assert!(
            metadata
                .get_object_route(&ObjectKey::new("tenant-a::key-a"))
                .expect("metadata query should succeed")
                .is_none(),
            "embedded WRH authority should keep routes off metadata backend"
        );
        assert_eq!(
            client
                .query_route_in_tenant("tenant-a", "key-a")
                .expect("route query should succeed")
                .expect("route should exist"),
            *route
        );
        assert_eq!(
            client
                .get_in_tenant("tenant-a", "key-a")
                .expect("get should succeed"),
            payload
        );
    }

    #[test]
    fn embedded_wrh_route_directory_serves_peer_clients_without_metadata_routes() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let writer_transport = Arc::new(TestTransport::new("writer-segment"));
        let reader_transport = writer_transport.clone();
        let writer = StoreClientBuilder::new(metadata.clone(), "writer")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .transport(writer_transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("writer build should succeed");
        let reader = StoreClientBuilder::new(metadata.clone(), "reader")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .transport(reader_transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("reader build should succeed");

        writer
            .put_in_tenant("tenant-a", "peer-key", b"peer-payload")
            .expect("writer put should succeed");

        assert!(
            metadata
                .get_object_route(&ObjectKey::new("tenant-a::peer-key"))
                .expect("metadata query should succeed")
                .is_none(),
            "WRH route directory should keep peer route state off metadata backend"
        );
        assert_eq!(
            reader
                .query_route_in_tenant("tenant-a", "peer-key")
                .expect("reader route query should succeed")
                .expect("route should exist")
                .key,
            ObjectKey::new("tenant-a::peer-key")
        );
        assert_eq!(
            reader
                .get_in_tenant("tenant-a", "peer-key")
                .expect("reader get should succeed"),
            b"peer-payload"
        );
    }

    #[test]
    fn embedded_wrh_route_directory_ignores_pool_boundaries_by_default() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("cross-pool-segment"));
        let writer = StoreClientBuilder::new(metadata.clone(), "writer-cross-pool")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-reclaim")
            .transport(transport.clone())
            .local_memory(storage_config())
            .build(10_000)
            .expect("writer build should succeed");
        let reader = StoreClientBuilder::new(metadata.clone(), "reader-cross-pool")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .transport(transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("reader build should succeed");

        writer
            .put_in_tenant("tenant-a", "cross-pool-key", b"cross-pool-payload")
            .expect("writer put should succeed");

        assert!(
            metadata
                .get_object_route(&ObjectKey::new("tenant-a::cross-pool-key"))
                .expect("metadata query should succeed")
                .is_none(),
            "WRH route directory should stay off metadata across pools"
        );
        assert_eq!(
            reader
                .get_in_tenant("tenant-a", "cross-pool-key")
                .expect("reader get should succeed"),
            b"cross-pool-payload"
        );
    }

    #[test]
    fn routed_io_works_when_metadata_hot_paths_are_disabled() {
        let metadata = Arc::new(NoHotPathMetadataBackend::new(Arc::new(
            InMemoryMetadataBackend::new(),
        )));
        let storage_transport = Arc::new(TestTransport::new("storage-hot-path-segment"));
        let router_transport = Arc::new(storage_transport.peer("router-hot-path-segment"));
        let reader_transport = Arc::new(storage_transport.peer("reader-hot-path-segment"));

        let storage = StoreClientBuilder::new(metadata.clone(), "storage-hot-path")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "true")
            .transport(storage_transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("storage build should succeed");
        storage
            .register_local_memory()
            .expect("storage memory should register");

        let router = StoreClientBuilder::new(metadata.clone(), "router-hot-path")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "false")
            .transport(router_transport)
            .local_memory(storage_config())
            .routed_writes(
                PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
                1,
            )
            .build(10_000)
            .expect("router build should succeed");
        router
            .register_local_memory()
            .expect("router memory should register");

        let reader = StoreClientBuilder::new(metadata.clone(), "reader-hot-path")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "false")
            .transport(reader_transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("reader build should succeed");
        reader
            .register_local_memory()
            .expect("reader memory should register");

        router
            .put_in_tenant("tenant-a", "hot-path-key", b"hot-path-payload")
            .expect("routed put should succeed without metadata hot paths");
        assert_eq!(
            storage
                .get_in_tenant("tenant-a", "hot-path-key")
                .expect("storage get should succeed"),
            b"hot-path-payload"
        );
        assert_eq!(
            reader
                .get_in_tenant("tenant-a", "hot-path-key")
                .expect("reader get should succeed"),
            b"hot-path-payload"
        );
    }

    #[test]
    fn routed_batch_io_works_when_metadata_hot_paths_are_disabled() {
        let metadata = Arc::new(NoHotPathMetadataBackend::new(Arc::new(
            InMemoryMetadataBackend::new(),
        )));
        let storage_transport = Arc::new(TestTransport::new("storage-hot-batch-segment"));
        let router_transport = Arc::new(storage_transport.peer("router-hot-batch-segment"));
        let reader_transport = Arc::new(storage_transport.peer("reader-hot-batch-segment"));

        let storage = StoreClientBuilder::new(metadata.clone(), "storage-hot-batch")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "true")
            .transport(storage_transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("storage build should succeed");
        storage
            .register_local_memory()
            .expect("storage memory should register");

        let router = StoreClientBuilder::new(metadata.clone(), "router-hot-batch")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "false")
            .transport(router_transport)
            .local_memory(storage_config())
            .routed_writes(
                PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
                1,
            )
            .build(10_000)
            .expect("router build should succeed");
        router
            .register_local_memory()
            .expect("router memory should register");

        let reader = StoreClientBuilder::new(metadata.clone(), "reader-hot-batch")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "false")
            .transport(reader_transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("reader build should succeed");
        reader
            .register_local_memory()
            .expect("reader memory should register");

        let first = router
            .batch_put(&[
                PutRequest::new("hot-batch-a", b"alpha-0").tenant("tenant-a"),
                PutRequest::new("hot-batch-b", b"beta-00").tenant("tenant-a"),
                PutRequest::new("hot-batch-c", b"gamma-0").tenant("tenant-a"),
            ])
            .expect("first batch routed put should succeed");
        assert_eq!(first.len(), 3);
        for key in ["hot-batch-a", "hot-batch-b", "hot-batch-c"] {
            assert!(
                metadata
                    .inner
                    .get_object_route(&ObjectKey::new(format!("tenant-a::{key}")))
                    .expect("metadata query should succeed")
                    .is_none(),
                "embedded WRH route directory should keep batch routes off metadata backend"
            );
        }

        let first_values = reader
            .batch_get(&[
                ObjectRef::new("hot-batch-a").tenant("tenant-a"),
                ObjectRef::new("hot-batch-b").tenant("tenant-a"),
                ObjectRef::new("hot-batch-c").tenant("tenant-a"),
            ])
            .expect("reader batch_get should succeed");
        assert_eq!(
            first_values,
            vec![
                b"alpha-0".to_vec(),
                b"beta-00".to_vec(),
                b"gamma-0".to_vec()
            ]
        );

        let second = router
            .batch_put(&[
                PutRequest::new("hot-batch-a", b"alpha-1").tenant("tenant-a"),
                PutRequest::new("hot-batch-b", b"beta-11").tenant("tenant-a"),
                PutRequest::new("hot-batch-c", b"gamma-1").tenant("tenant-a"),
            ])
            .expect("overwrite batch routed put should succeed");
        assert_eq!(second.len(), 3);

        let mut buf_a = [0u8; 7];
        let mut buf_b = [0u8; 7];
        let mut buf_c = [0u8; 7];
        let sizes = reader
            .batch_get_into(&mut [
                GetRequest::new("hot-batch-a", &mut buf_a).tenant("tenant-a"),
                GetRequest::new("hot-batch-b", &mut buf_b).tenant("tenant-a"),
                GetRequest::new("hot-batch-c", &mut buf_c).tenant("tenant-a"),
            ])
            .expect("reader batch_get_into should succeed");
        assert_eq!(sizes, vec![7, 7, 7]);
        assert_eq!(&buf_a, b"alpha-1");
        assert_eq!(&buf_b, b"beta-11");
        assert_eq!(&buf_c, b"gamma-1");
    }

    #[test]
    fn remove_reclaims_segment_space_immediately_when_grace_zero() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("reclaim-segment"));
        let client = StoreClientBuilder::new(metadata, "reclaim-client")
            .state(ClientLifecycleState::Active)
            .transport(transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("client build should succeed");

        let first = client
            .put("key-a", b"abcdefgh")
            .expect("put should succeed");
        let first_offset = first.replicas[0].segment_offset;
        client
            .remove("key-a", true)
            .expect("remove should reclaim the route");
        let second = client
            .put("key-b", b"abcdefgh")
            .expect("put should succeed");

        assert_eq!(second.replicas[0].segment_offset, first_offset);
        assert!(!client.is_exist("key-a").expect("is_exist should succeed"));
    }

    #[test]
    fn remove_defers_reclaim_until_grace_deadline() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("grace-segment"));
        let client = StoreClientBuilder::new(metadata, "grace-client")
            .state(ClientLifecycleState::Active)
            .transport(transport)
            .local_memory(
                LocalMemoryConfig::new()
                    .storage_bytes(4096)
                    .scratch_bytes(4096)
                    .reclaim_grace_ms(30),
            )
            .build(10_000)
            .expect("client build should succeed");

        let first = client
            .put("key-a", b"abcdefgh")
            .expect("put should succeed");
        let first_offset = first.replicas[0].segment_offset;
        client
            .remove("key-a", true)
            .expect("remove should schedule delayed reclaim");
        let second = client
            .put("key-b", b"abcdefgh")
            .expect("put should succeed");
        assert_ne!(second.replicas[0].segment_offset, first_offset);

        sleep(Duration::from_millis(50));
        let third = client
            .put("key-c", b"abcdefgh")
            .expect("put should succeed");
        assert_eq!(third.replicas[0].segment_offset, first_offset);
    }

    #[test]
    fn request_replication_policy_overrides_default_local_write() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("writer-segment"));
        let owner_a = publish_storage_node(
            metadata.as_ref(),
            transport.as_ref(),
            "storage-a",
            "seg-a",
            "pool-a",
        );
        let owner_b = publish_storage_node(
            metadata.as_ref(),
            transport.as_ref(),
            "storage-b",
            "seg-b",
            "pool-a",
        );
        let client = StoreClientBuilder::new(metadata.clone(), "writer")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "false")
            .transport(transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("client build should succeed");

        let route = client
            .put_with_policy(
                "replicated-key",
                b"replicated-payload",
                &ReplicationPolicy::new().replica_count(2),
            )
            .expect("replicated put should succeed");

        assert_eq!(route.replicas.len(), 2);
        let owners = route
            .replicas
            .iter()
            .map(|replica| replica.owner.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(owners, BTreeSet::from([owner_a, owner_b]));
        assert_eq!(
            client.get("replicated-key").expect("get should succeed"),
            b"replicated-payload"
        );
    }

    #[test]
    fn request_replication_policy_honors_preferred_segment() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("writer-segment"));
        let owner_b = publish_storage_node(
            metadata.as_ref(),
            transport.as_ref(),
            "storage-b",
            "seg-b",
            "pool-a",
        );
        let client = StoreClientBuilder::new(metadata, "writer")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "false")
            .transport(transport)
            .local_memory(storage_config())
            .build(10_000)
            .expect("client build should succeed");

        let route = client
            .put_with_policy(
                "preferred-key",
                b"preferred-payload",
                &ReplicationPolicy::new()
                    .replica_count(1)
                    .preferred_segment("seg-b"),
            )
            .expect("preferred-segment put should succeed");

        assert_eq!(route.replicas.len(), 1);
        assert_eq!(route.replicas[0].owner, owner_b);
        assert_eq!(route.replicas[0].segment_name, SegmentName::new("seg-b"));
    }
}
