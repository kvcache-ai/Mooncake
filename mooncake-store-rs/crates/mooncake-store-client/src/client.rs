use std::collections::BTreeMap;
use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::Arc;
use std::time::Duration;

use mooncake_store_core::{
    CasResult, ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend, ObjectKey,
    ObjectRoute, ReplicaRoute, ReplicaTier, Result, RouteState, RouteVersion, SegmentAnnouncement,
    SegmentLifecycleState, SegmentName, StoreError,
};
use mooncake_transport::{Opcode, TentEngine, TransferRequest};
use parking_lot::Mutex;
use tracing::{debug, info, info_span};

use crate::memory::{
    LocalMemoryConfig, LocalMemoryState, RegionAllocation, StorageExtentInfo, StorageSegmentSpec,
};
use crate::observability::OperationTracker;
use crate::placement::PlacementPlanner;
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

#[derive(Clone, Copy, Debug)]
pub struct PutRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub value: &'a [u8],
}

impl<'a> PutRequest<'a> {
    pub fn new(key: &'a str, value: &'a [u8]) -> Self {
        Self {
            tenant: None,
            key,
            value,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PutFromRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub buffer: *const c_void,
    pub size: usize,
}

impl<'a> PutFromRequest<'a> {
    pub fn new(key: &'a str, buffer: *const c_void, size: usize) -> Self {
        Self {
            tenant: None,
            key,
            buffer,
            size,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
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
}

impl<'a> MultiBufferPutRequest<'a> {
    pub fn new(key: &'a str, buffers: &'a [&'a [u8]]) -> Self {
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

    pub fn build(self, expires_at_ms: u64) -> Result<StoreClient> {
        if self.default_tenant.is_empty() {
            return Err(StoreError::InvalidState(
                "default tenant must not be empty".to_string(),
            ));
        }

        let mut endpoints = self.endpoints;
        if let Some(transport) = self.transport.as_ref() {
            if endpoints.rpc_address.is_empty() {
                endpoints.rpc_address = transport.rpc_server_address()?.0;
            }
            if endpoints.segment_name.is_none() {
                endpoints.segment_name = Some(SegmentName::new(transport.segment_name()?));
            }
        }

        let runtime = ClientRuntimeId {
            stable_id: self.stable_id,
            epoch: self.epoch,
        };
        let lease = ClientLease {
            runtime: runtime.clone(),
            state: self.initial_state,
            compatibility: self.compatibility,
            endpoints,
            expires_at_ms,
        };
        self.metadata.upsert_client_lease(&lease)?;
        Ok(StoreClient {
            metadata: self.metadata,
            lease,
            default_tenant: self.default_tenant,
            local_memory: self.local_memory,
            transport: self.transport,
            transport_factory: self.transport_factory,
            write_mode: self.write_mode,
            state: Mutex::new(StoreState::default()),
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
    fn put(&self, key: &str, value: &[u8]) -> Result<ObjectRoute>;
    fn put_in_tenant(&self, tenant: &str, key: &str, value: &[u8]) -> Result<ObjectRoute>;
    fn put_from(&self, key: &str, buffer: *const c_void, size: usize) -> Result<ObjectRoute>;
    fn put_from_in_tenant(
        &self,
        tenant: &str,
        key: &str,
        buffer: *const c_void,
        size: usize,
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
        self.metadata.publish_segment(&SegmentAnnouncement {
            owner: self.lease.runtime.clone(),
            segment_name: segment.segment_name.clone(),
            capacity_bytes: segment.capacity_bytes,
            used_bytes,
            state: segment.state,
            alignment_bytes: segment.alignment_bytes,
            tags: segment.tags.clone(),
        })
    }

    fn sorted_segments(
        &self,
        owner: &ClientRuntimeId,
        require_local_memory: bool,
    ) -> Result<Vec<SegmentAnnouncement>> {
        let mut segments = self.metadata.list_segments(Some(owner))?;
        if require_local_memory {
            let state = self.state.lock();
            let memory = state.memory_ref()?;
            segments.retain(|segment| memory.has_storage_segment(&segment.segment_name));
        }
        segments.retain(|segment| segment.state == SegmentLifecycleState::Active);
        segments.sort_by(|left, right| {
            let left_remaining = left.capacity_bytes.saturating_sub(left.used_bytes);
            let right_remaining = right.capacity_bytes.saturating_sub(right.used_bytes);
            right_remaining
                .cmp(&left_remaining)
                .then_with(|| left.segment_name.cmp(&right.segment_name))
        });
        Ok(segments)
    }

    fn reserve_owner_segment(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: usize,
        require_local_memory: bool,
    ) -> Result<(ReplicaWriteTarget, mooncake_store_core::SegmentReservation)> {
        let mut last_capacity_error = None;
        for segment in self.sorted_segments(owner, require_local_memory)? {
            match self
                .metadata
                .reserve_segment(owner, &segment.segment_name, length_bytes as u64)
            {
                Ok(reservation) => {
                    debug!(
                        owner = %owner,
                        segment = %segment.segment_name.0,
                        offset_bytes = reservation.offset_bytes,
                        length_bytes = reservation.length_bytes,
                        "reserved segment space"
                    );
                    return Ok((
                        ReplicaWriteTarget {
                            runtime: owner.clone(),
                            segment_name: segment.segment_name,
                        },
                        reservation,
                    ));
                }
                Err(StoreError::Allocator(error)) => {
                    last_capacity_error = Some(StoreError::Allocator(error));
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_capacity_error.unwrap_or_else(|| {
            StoreError::Allocator(format!("no writable active segment available for {}", owner))
        }))
    }

    fn release_route_allocations(&self, route: &ObjectRoute) -> Result<()> {
        for replica in &route.replicas {
            debug!(
                owner = %replica.owner,
                segment = %replica.segment_name.0,
                offset_bytes = replica.segment_offset,
                length_bytes = replica.length,
                "releasing route allocation"
            );
            self.metadata.release_segment(
                &replica.owner,
                &replica.segment_name,
                replica.segment_offset,
                replica.length,
            )?;
        }
        Ok(())
    }

    fn put_scoped(&self, tenant: &str, key: &str, value: &[u8]) -> Result<ObjectRoute> {
        self.ensure_local_memory()?;
        let scoped_key = self.scoped_key(tenant, key);
        let (target, reservation) =
            self.reserve_owner_segment(&self.lease.runtime, value.len(), true)?;
        let allocation = {
            let state = self.state.lock();
            state.memory_ref()?.storage_address(
                &target.segment_name,
                reservation.offset_bytes as usize,
            )?
        };
        unsafe {
            ptr::copy_nonoverlapping(value.as_ptr(), allocation.cast::<u8>(), value.len());
        }

        let current = self.metadata.get_object_route(&scoped_key)?;
        let expected_version = current.as_ref().map(|route| route.version);
        let next_version = current
            .as_ref()
            .map(|route| route.version.next())
            .unwrap_or(RouteVersion(1));
        let route = ObjectRoute {
            key: scoped_key.clone(),
            version: next_version,
            state: RouteState::Active,
            compatibility: self.lease.compatibility.clone(),
            replicas: vec![ReplicaRoute {
                owner: self.lease.runtime.clone(),
                segment_name: target.segment_name,
                offset: allocation as u64,
                segment_offset: reservation.offset_bytes,
                length: value.len() as u64,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
        };
        let cas = self.metadata.compare_and_swap_object_route(
            &scoped_key,
            expected_version,
            Some(&route),
        )?;
        if !cas.applied {
            return Err(StoreError::Conflict(format!(
                "route update lost race for tenant={tenant} key={key}"
            )));
        }
        if let Some(previous) = current.as_ref() {
            self.release_route_allocations(previous)?;
        }
        Ok(route)
    }

    fn put_scoped_routed(&self, tenant: &str, key: &str, value: &[u8]) -> Result<ObjectRoute> {
        self.ensure_local_memory()?;
        let scoped_key = self.scoped_key(tenant, key);
        let owners = self.resolve_routed_owners(tenant, key)?;
        let mut targets = Vec::with_capacity(owners.len());
        let mut reservations = Vec::with_capacity(owners.len());
        for owner in owners {
            let (target, reservation) = self.reserve_owner_segment(&owner, value.len(), false)?;
            targets.push(target);
            reservations.push(reservation);
        }
        let offsets = self.write_reserved_replicas(&targets, &reservations, value)?;

        let current = self.metadata.get_object_route(&scoped_key)?;
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
        let cas = self.metadata.compare_and_swap_object_route(
            &route.key,
            expected_version,
            Some(&route),
        )?;
        if !cas.applied {
            return Err(StoreError::Conflict(format!(
                "route update lost race for tenant={tenant} key={key}"
            )));
        }
        if let Some(previous) = current.as_ref() {
            self.release_route_allocations(previous)?;
        }
        Ok(route)
    }

    fn resolve_objects(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<ResolvedObject>> {
        let mut resolved = Vec::with_capacity(objects.len());
        for object in objects {
            let tenant = object.tenant.unwrap_or(self.default_tenant());
            let scoped = self.scoped_key(tenant, object.key);
            let route = self.metadata.get_object_route(&scoped)?.ok_or_else(|| {
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

    fn resolve_routed_owners(&self, tenant: &str, key: &str) -> Result<Vec<ClientRuntimeId>> {
        let WriteMode::Routed {
            planner,
            replica_count,
        } = &self.write_mode
        else {
            return Err(StoreError::InvalidState(
                "routed target resolution requires routed write mode".to_string(),
            ));
        };
        let plan = planner.plan(self, &[ObjectRef::new(key).tenant(tenant)], *replica_count)?;
        let owners = plan
            .into_iter()
            .next()
            .ok_or_else(|| StoreError::InvalidState("placement returned no plan".to_string()))?
            .owners;
        Ok(owners)
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
                    state.memory_ref()?.storage_address(
                        &target.segment_name,
                        reservation.offset_bytes as usize,
                    )?
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

        let mut prepared = Vec::with_capacity(requests.len());
        let mut scratch_lengths = Vec::new();
        for (request, plan) in requests.iter().zip(plans.iter()) {
            let tenant = request.tenant.unwrap_or(self.default_tenant());
            let scoped_key = self.scoped_key(tenant, request.key);
            let mut targets = Vec::with_capacity(plan.owners.len());
            let mut reservations = Vec::with_capacity(plan.owners.len());
            for runtime in &plan.owners {
                let (target, reservation) =
                    self.reserve_owner_segment(runtime, request.value.len(), false)?;
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
        let mut routes = Vec::with_capacity(prepared.len());

        for entry in &prepared {
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
                        state
                            .memory_ref()?
                            .storage_address(
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

            let current = self.metadata.get_object_route(&entry.scoped_key)?;
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
                return Err(error);
            }
            let wait_result =
                wait_for_batch_completion(transport, batch_id, DEFAULT_TRANSFER_TIMEOUT);
            let free_result = transport.free_batch(batch_id);
            wait_result?;
            free_result?;
        }

        let mut published = Vec::with_capacity(routes.len());
        for pending in routes {
            let cas = self.metadata.compare_and_swap_object_route(
                &pending.key,
                pending.expected_version,
                Some(&pending.route),
            )?;
            if !cas.applied {
                return Err(StoreError::Conflict(format!(
                    "route update lost race for key {}",
                    pending.key.0
                )));
            }
            if let Some(previous) = pending.previous.as_ref() {
                self.release_route_allocations(previous)?;
            }
            published.push(pending.route);
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
        let result = self.metadata.publish_segment(&segment);
        tracker.finish(&result, used_bytes);
        result
    }

    fn list_segments(&self) -> Result<Vec<SegmentAnnouncement>> {
        self.metadata.list_segments(Some(&self.lease.runtime))
    }

    fn expand_local_memory(&self, storage_bytes: usize) -> Result<SegmentAnnouncement> {
        let _span = info_span!(
            "store.expand_local_memory",
            runtime = %self.lease.runtime,
            storage_bytes
        )
        .entered();
        let tracker = OperationTracker::new("expand_local_memory").input_bytes(storage_bytes as u64);
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
        let segments = self.metadata.list_segments(Some(&self.lease.runtime))?;
        let announcement = segments
            .into_iter()
            .find(|entry| entry.segment_name == *segment)
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
                state
                    .local_transports
                    .remove(&segment.0)
                    .ok_or_else(|| {
                        StoreError::NotFound(format!(
                            "local transport for segment {} not found",
                            segment.0
                        ))
                    })?
            };
            state
                .memory_mut()?
                .remove_storage_segment(transport.as_ref(), segment)?;
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
        self.metadata
            .get_object_route(&self.scoped_key(tenant, key))
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
        self.metadata
            .compare_and_swap_object_route(&self.scoped_key(tenant, key), expected, next)
    }

    fn register_local_memory(&self) -> Result<()> {
        let _span = info_span!("store.register_local_memory", runtime = %self.lease.runtime).entered();
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
        let result = match &self.write_mode {
            WriteMode::LocalOnly => self.put_scoped(tenant, key, value),
            WriteMode::Routed { .. } => self.put_scoped_routed(tenant, key, value),
        };
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
        let result = self.put_in_tenant(tenant, key, value);
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
        if matches!(self.write_mode, WriteMode::Routed { .. }) {
            let result = self.batch_put_scoped_routed(requests);
            tracker.finish(&result, bytes_in);
            return result;
        }
        let mut routes = Vec::with_capacity(requests.len());
        for request in requests {
            let tenant = request.tenant.unwrap_or(self.default_tenant());
            routes.push(self.put_in_tenant(tenant, request.key, request.value)?);
        }
        let result = Ok(routes);
        tracker.finish(&result, bytes_in);
        result
    }

    fn batch_put_from(&self, requests: &[PutFromRequest<'_>]) -> Result<Vec<ObjectRoute>> {
        let bytes_in = requests.iter().map(|request| request.size as u64).sum::<u64>();
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
            routes.push(self.put_from_in_tenant(
                tenant,
                request.key,
                request.buffer,
                request.size,
            )?);
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
            routes.push(self.put_in_tenant(tenant, request.key, &payload)?);
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
        let bytes_out = buffers.iter().map(|buffer| buffer.len() as u64).sum::<u64>();
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
                let local_transport = if segment.segment_name == self.segment_name().unwrap_or_else(|_| {
                    SegmentName::new("__missing_primary_segment__")
                }) {
                    self.transport.clone()
                } else {
                    state.local_transports.remove(&segment.segment_name.0)
                };
                if let Some(local_transport) = local_transport {
                    let _ =
                        memory.remove_storage_segment(local_transport.as_ref(), &segment.segment_name);
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
    next_local_segment_id: u64,
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

struct PendingRoutePublish {
    key: ObjectKey,
    expected_version: Option<RouteVersion>,
    previous: Option<ObjectRoute>,
    route: ObjectRoute,
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
            let segment_name = SegmentName::new(format!(
                "{}-ext-{}",
                primary.0, self.next_local_segment_id
            ));
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::ffi::c_void;
    use std::ptr;
    use std::sync::Arc;

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
        render_prometheus_metrics, reset_metrics, transport::StoreTransport, LocalMemoryConfig,
        MooncakeCompatibilityFacade, MultiBufferGetRequest, PlacementPlanner, PutRequest,
        StoreClientBuilder,
    };

    struct TestTransport {
        local_segment: String,
        state: Mutex<TestTransportState>,
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

    impl TestTransport {
        fn new(local_segment: &str) -> Self {
            Self {
                local_segment: local_segment.to_string(),
                state: Mutex::new(TestTransportState {
                    next_handle: 1,
                    next_batch: 1,
                    allocations: BTreeMap::new(),
                    segments_by_name: BTreeMap::new(),
                    segments_by_handle: BTreeMap::new(),
                    live_batches: BTreeSet::new(),
                    registered_memory: BTreeMap::new(),
                }),
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

        client.put("metrics-key", b"abcdefgh").expect("put should succeed");
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

        let stored = metadata
            .get_object_route(&ObjectKey::new("tenant-a::key-a"))
            .expect("route query should succeed")
            .expect("route should exist");
        assert_eq!(stored, *route);
        assert_eq!(
            client
                .get_in_tenant("tenant-a", "key-a")
                .expect("get should succeed"),
            payload
        );
    }
}
