use std::collections::BTreeMap;
use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::Arc;
use std::time::Duration;

use mooncake_store_core::{
    CasResult, ClientEpoch, ClientEndpointSet, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend, ObjectKey,
    ObjectRoute, ReplicaRoute, ReplicaTier, Result, RouteState, RouteVersion, SegmentAnnouncement,
    SegmentName, StoreError,
};
use mooncake_transport::{Opcode, TentEngine, TransferRequest};
use parking_lot::Mutex;

use crate::memory::{LocalMemoryConfig, LocalMemoryState, RegionAllocation};
use crate::transport::{wait_for_batch_completion, StoreTransport};

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

    pub fn with_tent(mut self, engine: Arc<TentEngine>) -> Self {
        self.transport = Some(engine);
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
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    fn get_in_tenant(&self, tenant: &str, key: &str) -> Result<Vec<u8>>;
    fn get_into(&self, key: &str, buffer: &mut [u8]) -> Result<usize>;
    fn get_into_in_tenant(&self, tenant: &str, key: &str, buffer: &mut [u8]) -> Result<usize>;
    fn batch_get(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<Vec<u8>>>;
    fn batch_get_buffer(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<Vec<u8>>>;
    fn batch_get_into(&self, requests: &mut [GetRequest<'_>]) -> Result<Vec<usize>>;
}

pub struct StoreClient {
    metadata: Arc<dyn MetadataBackend>,
    lease: ClientLease,
    default_tenant: String,
    local_memory: LocalMemoryConfig,
    transport: Option<Arc<dyn StoreTransport>>,
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

    fn ensure_local_memory(&self) -> Result<()> {
        if self.state.lock().memory.is_some() {
            return Ok(());
        }
        let transport = self.transport()?;
        let memory = LocalMemoryState::register(transport, &self.local_memory)?;
        let capacity_bytes = memory.storage_capacity_bytes() as u64;
        let used_bytes = memory.storage_used_bytes() as u64;
        {
            let mut state = self.state.lock();
            state.memory = Some(memory);
        }
        self.mount_segment(capacity_bytes, used_bytes, self.local_memory.tags.clone())
    }

    fn scoped_key(&self, tenant: &str, key: &str) -> ObjectKey {
        ObjectKey::new(format!("{tenant}::{key}"))
    }

    fn put_scoped(&self, tenant: &str, key: &str, value: &[u8]) -> Result<ObjectRoute> {
        self.ensure_local_memory()?;
        let scoped_key = self.scoped_key(tenant, key);
        let segment_name = self.segment_name()?;
        let allocation = {
            let mut state = self.state.lock();
            let memory = state.memory_mut()?;
            memory.allocate_storage(value.len())?
        };
        copy_into_region(allocation, value);

        let current = self.metadata.get_object_route(&scoped_key)?;
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
                segment_name,
                offset: allocation.absolute_offset,
                length: value.len() as u64,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
        };
        let cas = self
            .metadata
            .compare_and_swap_object_route(&scoped_key, current.map(|route| route.version), Some(&route))?;
        if !cas.applied {
            return Err(StoreError::Conflict(format!(
                "route update lost race for tenant={tenant} key={key}"
            )));
        }
        let used_bytes = {
            let mut state = self.state.lock();
            state.memory_mut()?.storage_used_bytes() as u64
        };
        self.mount_segment(self.storage_capacity_bytes()? as u64, used_bytes, self.local_memory.tags.clone())?;
        Ok(route)
    }

    fn resolve_objects(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<ResolvedObject>> {
        let mut resolved = Vec::with_capacity(objects.len());
        for object in objects {
            let tenant = object.tenant.unwrap_or(self.default_tenant());
            let scoped = self.scoped_key(tenant, object.key);
            let route = self
                .metadata
                .get_object_route(&scoped)?
                .ok_or_else(|| StoreError::NotFound(format!("tenant={tenant} key={}", object.key)))?;
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

        let direct_paths = {
            let state = self.state.lock();
            buffers
                .iter()
                .zip(lengths.iter())
                .map(|(buffer, length)| {
                    state.buffer_is_registered(buffer.as_ptr().cast_mut().cast::<c_void>(), *length)
                })
                .collect::<Vec<_>>()
        };
        let scratch_lengths = direct_paths
            .iter()
            .zip(lengths.iter())
            .filter_map(|(direct, length)| (!*direct).then_some(*length))
            .collect::<Vec<_>>();
        let scratch = if scratch_lengths.is_empty() {
            Vec::new()
        } else {
            let state = self.state.lock();
            state.memory_ref()?.plan_scratch(&scratch_lengths)?
        };

        let requests = {
            let mut state = self.state.lock();
            let mut batch = Vec::with_capacity(resolved.len());
            let mut scratch_index = 0usize;
            for ((entry, buffer), direct) in resolved
                .iter()
                .zip(buffers.iter_mut())
                .zip(direct_paths.iter())
            {
                let source = if *direct {
                    buffer.as_mut_ptr().cast::<c_void>()
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

        let mut sizes = Vec::with_capacity(buffers.len());
        let mut scratch_index = 0usize;
        for ((buffer, direct), length) in buffers
            .iter_mut()
            .zip(direct_paths.iter())
            .zip(lengths)
        {
            if !*direct {
                unsafe {
                    ptr::copy_nonoverlapping(
                        scratch[scratch_index].addr.cast::<u8>(),
                        buffer.as_mut_ptr(),
                        length,
                    );
                }
                scratch_index += 1;
            }
            sizes.push(length);
        }
        Ok(sizes)
    }

    fn segment_name(&self) -> Result<SegmentName> {
        self.lease
            .endpoints
            .segment_name
            .clone()
            .ok_or_else(|| StoreError::InvalidState("segment_name is not configured".to_string()))
    }

    fn storage_capacity_bytes(&self) -> Result<usize> {
        let state = self.state.lock();
        Ok(state.memory_ref()?.storage_capacity_bytes())
    }
}

impl MooncakeCompatibilityFacade for StoreClient {
    fn heartbeat(&mut self, expires_at_ms: u64) -> Result<()> {
        self.lease.expires_at_ms = expires_at_ms;
        self.metadata.upsert_client_lease(&self.lease)
    }

    fn enter_standby(&mut self) -> Result<()> {
        self.lease.state = ClientLifecycleState::Standby;
        self.metadata
            .update_client_state(&self.lease.runtime, self.lease.state)
    }

    fn activate(&mut self) -> Result<()> {
        self.lease.state = ClientLifecycleState::Active;
        self.metadata
            .update_client_state(&self.lease.runtime, self.lease.state)
    }

    fn enter_draining(&mut self) -> Result<()> {
        self.lease.state = ClientLifecycleState::Draining;
        self.metadata
            .update_client_state(&self.lease.runtime, self.lease.state)
    }

    fn plan_handoff(
        &self,
        successor_epoch: ClientEpoch,
        kind: HandoffKind,
        barrier_version: u64,
        created_at_ms: u64,
        deadline_ms: Option<u64>,
    ) -> Result<HandoffPlan> {
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
        self.metadata.put_handoff(&plan)?;
        Ok(plan)
    }

    fn mount_segment(&self, capacity_bytes: u64, used_bytes: u64, tags: Vec<String>) -> Result<()> {
        let segment = SegmentAnnouncement {
            owner: self.lease.runtime.clone(),
            segment_name: self.segment_name()?,
            capacity_bytes,
            used_bytes,
            tags,
        };
        self.metadata.publish_segment(&segment)
    }

    fn query_route(&self, key: &str) -> Result<Option<ObjectRoute>> {
        self.query_route_in_tenant(self.default_tenant(), key)
    }

    fn query_route_in_tenant(&self, tenant: &str, key: &str) -> Result<Option<ObjectRoute>> {
        self.metadata.get_object_route(&self.scoped_key(tenant, key))
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
        self.ensure_local_memory()
    }

    fn register_buffer(&self, buffer: *mut c_void, size: usize) -> Result<()> {
        if size == 0 {
            return Err(StoreError::Allocator(
                "registered buffer size must be greater than zero".to_string(),
            ));
        }
        let transport = self.transport()?;
        let mut state = self.state.lock();
        state.register_external_buffer(transport, buffer, size)
    }

    fn unregister_buffer(&self, buffer: *mut c_void, size: usize) -> Result<()> {
        let transport = self.transport()?;
        let mut state = self.state.lock();
        state.unregister_external_buffer(transport, buffer, size)
    }

    fn put(&self, key: &str, value: &[u8]) -> Result<ObjectRoute> {
        self.put_in_tenant(self.default_tenant(), key, value)
    }

    fn put_in_tenant(&self, tenant: &str, key: &str, value: &[u8]) -> Result<ObjectRoute> {
        self.put_scoped(tenant, key, value)
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
        if buffer.is_null() {
            return Err(StoreError::Allocator(
                "put_from buffer must not be null".to_string(),
            ));
        }
        {
            let state = self.state.lock();
            if !state.buffer_is_registered(buffer.cast_mut(), size) {
                return Err(StoreError::Allocator(format!(
                    "put_from buffer is not registered for tenant={tenant} key={key}"
                )));
            }
        }
        let value = unsafe { slice::from_raw_parts(buffer.cast::<u8>(), size) };
        self.put_in_tenant(tenant, key, value)
    }

    fn batch_put(&self, requests: &[PutRequest<'_>]) -> Result<Vec<ObjectRoute>> {
        let mut routes = Vec::with_capacity(requests.len());
        for request in requests {
            let tenant = request.tenant.unwrap_or(self.default_tenant());
            routes.push(self.put_in_tenant(tenant, request.key, request.value)?);
        }
        Ok(routes)
    }

    fn batch_put_from(&self, requests: &[PutFromRequest<'_>]) -> Result<Vec<ObjectRoute>> {
        let mut routes = Vec::with_capacity(requests.len());
        for request in requests {
            let tenant = request.tenant.unwrap_or(self.default_tenant());
            routes.push(self.put_from_in_tenant(tenant, request.key, request.buffer, request.size)?);
        }
        Ok(routes)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.get_in_tenant(self.default_tenant(), key)
    }

    fn get_in_tenant(&self, tenant: &str, key: &str) -> Result<Vec<u8>> {
        let objects = [ObjectRef::new(key).tenant(tenant)];
        let mut results = self.batch_get(&objects)?;
        results
            .pop()
            .ok_or_else(|| StoreError::InvalidState("missing batch_get result".to_string()))
    }

    fn get_into(&self, key: &str, buffer: &mut [u8]) -> Result<usize> {
        self.get_into_in_tenant(self.default_tenant(), key, buffer)
    }

    fn get_into_in_tenant(&self, tenant: &str, key: &str, buffer: &mut [u8]) -> Result<usize> {
        let mut requests = [GetRequest::new(key, buffer).tenant(tenant)];
        let mut sizes = self.batch_get_into(&mut requests)?;
        sizes
            .pop()
            .ok_or_else(|| StoreError::InvalidState("missing batch_get_into result".to_string()))
    }

    fn batch_get(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<Vec<u8>>> {
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
        Ok(buffers)
    }

    fn batch_get_buffer(&self, objects: &[ObjectRef<'_>]) -> Result<Vec<Vec<u8>>> {
        self.batch_get(objects)
    }

    fn batch_get_into(&self, requests: &mut [GetRequest<'_>]) -> Result<Vec<usize>> {
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
        self.execute_batch_get_into(&resolved, &mut buffers)
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
        if let Some(memory) = state.memory.take() {
            let _ = memory.release(transport);
        }
    }
}

#[derive(Default)]
struct StoreState {
    memory: Option<LocalMemoryState>,
    registered_buffers: BTreeMap<usize, usize>,
    remote_segments: BTreeMap<String, u64>,
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

    fn open_segment(
        &mut self,
        transport: &dyn StoreTransport,
        segment_name: &str,
    ) -> Result<u64> {
        if let Some(handle) = self.remote_segments.get(segment_name) {
            return Ok(*handle);
        }
        let handle = transport.open_segment(segment_name)?;
        self.remote_segments.insert(segment_name.to_string(), handle);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{ClientEpoch, ClientLifecycleState, HandoffKind, MetadataBackend};

    use crate::{MooncakeCompatibilityFacade, StoreClientBuilder};

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

        let leases = metadata.list_live_clients().expect("list clients should succeed");
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
}
