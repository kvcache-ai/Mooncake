use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
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

use super::{
    LocalAllocatorAdapter, LocalAllocatorState, PendingReclaim, SegmentAllocator, StoreState,
};
use crate::{
    control_plane::AllocatorService, render_prometheus_metrics, reset_metrics,
    transport::StoreTransport, GetRequest, LocalMemoryConfig, MooncakeCompatibilityFacade,
    MultiBufferGetRequest, ObjectRef, PlacementPlanner, PutFromRequest, PutRequest,
    ReplicationPolicy, StoreClientBuilder,
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

struct CountingMetadataBackend {
    inner: Arc<InMemoryMetadataBackend>,
    list_live_clients_calls: AtomicUsize,
}

impl CountingMetadataBackend {
    fn new(inner: Arc<InMemoryMetadataBackend>) -> Self {
        Self {
            inner,
            list_live_clients_calls: AtomicUsize::new(0),
        }
    }

    fn list_live_clients_calls(&self) -> usize {
        self.list_live_clients_calls.load(Ordering::Relaxed)
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

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> mooncake_store_core::Result<()> {
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

    fn list_object_routes(
        &self,
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::ObjectRoute>> {
        self.inner.list_object_routes()
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

impl MetadataBackend for CountingMetadataBackend {
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
        self.list_live_clients_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.list_live_clients()
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> mooncake_store_core::Result<()> {
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
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> mooncake_store_core::Result<mooncake_store_core::SegmentReservation> {
        self.inner.reserve_segment(owner, segment, length_bytes)
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> mooncake_store_core::Result<()> {
        self.inner
            .release_segment(owner, segment, offset_bytes, length_bytes)
    }

    fn get_object_route(
        &self,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<mooncake_store_core::ObjectRoute>> {
        self.inner.get_object_route(key)
    }

    fn list_object_routes(
        &self,
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::ObjectRoute>> {
        self.inner.list_object_routes()
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<mooncake_store_core::RouteVersion>,
        next: Option<&mooncake_store_core::ObjectRoute>,
    ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
        self.inner
            .compare_and_swap_object_route(key, expected, next)
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
            .ok_or_else(|| StoreError::NotFound(format!("segment handle {handle} not found")))?;
        Ok(SegmentInfo {
            kind: SegmentKind::Memory,
            buffers: vec![SegmentBuffer {
                base: segment.base as u64,
                length: segment.len as u64,
                location: "cpu:0".to_string(),
            }],
        })
    }

    fn adopt_local_memory(
        &self,
        addr: *mut c_void,
        size: usize,
        _location: &str,
    ) -> mooncake_store_core::Result<()> {
        let mut state = self.state.lock();
        let segment_name = self.local_segment.clone();
        if state.segments_by_name.contains_key(&segment_name) {
            return Ok(());
        }
        register_segment(&mut state, segment_name, addr as usize, size);
        Ok(())
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

    fn register_memory(&self, addr: *mut c_void, size: usize) -> mooncake_store_core::Result<()> {
        self.state
            .lock()
            .registered_memory
            .insert(addr as usize, size);
        Ok(())
    }

    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> mooncake_store_core::Result<()> {
        let mut state = self.state.lock();
        match state.registered_memory.remove(&(addr as usize)) {
            Some(registered) if registered == size => {
                let orphaned = state
                    .segments_by_handle
                    .iter()
                    .filter_map(|(handle, segment)| {
                        (segment.base == addr as usize).then_some(*handle)
                    })
                    .collect::<Vec<_>>();
                for handle in orphaned {
                    state.segments_by_handle.remove(&handle);
                    state
                        .segments_by_name
                        .retain(|_, current_handle| *current_handle != handle);
                }
                Ok(())
            }
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
                    StoreError::NotFound(format!("segment handle {} not found", request.target_id))
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

fn storage_config_with_bytes(storage_bytes: usize) -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .storage_bytes(storage_bytes)
        .scratch_bytes(4096)
        .alignment(1)
        .reclaim_grace_ms(0)
}

fn storage_config_with_layout(
    storage_bytes: usize,
    scratch_bytes: usize,
    alignment: usize,
) -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .storage_bytes(storage_bytes)
        .scratch_bytes(scratch_bytes)
        .alignment(alignment)
        .reclaim_grace_ms(0)
}

fn publish_storage_node(
    metadata: &InMemoryMetadataBackend,
    transport: &TestTransport,
    stable_id: &str,
    segment_name: &str,
    pool: &str,
) -> ClientRuntimeId {
    publish_storage_node_with_capacity(metadata, transport, stable_id, segment_name, pool, 4096, 64)
}

fn publish_storage_node_with_capacity(
    metadata: &InMemoryMetadataBackend,
    transport: &TestTransport,
    stable_id: &str,
    segment_name: &str,
    pool: &str,
    capacity_bytes: u64,
    alignment_bytes: u64,
) -> ClientRuntimeId {
    transport.add_external_segment(segment_name, capacity_bytes as usize);
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
            capacity_bytes,
            used_bytes: 0,
            state: SegmentLifecycleState::Active,
            alignment_bytes,
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
    assert!(metrics.contains("operation=\"put_local_copy\",status=\"ok\""));
    assert!(metrics.contains("operation=\"get_local_copy\",status=\"ok\""));
    assert!(metrics.contains("mooncake_store_client_operation_bytes_out_total"));
}

#[test]
fn observability_metrics_render_remote_datapaths() {
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("metrics-remote-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        metadata.as_ref(),
        transport.as_ref(),
        "metrics-storage-remote",
        "metrics-seg-remote",
        "pool-a",
        1024,
        1,
    );
    let writer = StoreClientBuilder::new(metadata.clone(), "client-metrics-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(10_000)
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "client-metrics-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config_with_layout(4096, 4, 1))
        .build(10_000)
        .expect("reader build should succeed");

    writer
        .batch_put(&[
            PutRequest::new("metrics-remote-a", b"aaaa").replication(
                ReplicationPolicy::new()
                    .prefer_local(false)
                    .preferred_storage_owner(remote_owner.storage_key()),
            ),
            PutRequest::new("metrics-remote-b", b"bbbb").replication(
                ReplicationPolicy::new()
                    .prefer_local(false)
                    .preferred_storage_owner(remote_owner.storage_key()),
            ),
        ])
        .expect("remote batch put should succeed");
    let mut direct = [0u8; 8];
    writer
        .put_with_policy(
            "metrics-remote-c",
            b"abcdefgh",
            &ReplicationPolicy::new()
                .prefer_local(false)
                .preferred_storage_owner(remote_owner.storage_key()),
        )
        .expect("remote put should succeed");
    let batch = reader
        .batch_get(&[
            ObjectRef::new("metrics-remote-a"),
            ObjectRef::new("metrics-remote-b"),
        ])
        .expect("remote batch get should succeed");
    assert_eq!(batch, vec![b"aaaa".to_vec(), b"bbbb".to_vec()]);
    let direct_sizes = reader
        .batch_get_into(&mut [GetRequest::new("metrics-remote-c", &mut direct)])
        .expect("remote direct get should succeed");
    assert_eq!(direct_sizes, vec![8]);
    assert_eq!(&direct, b"abcdefgh");

    let metrics = render_prometheus_metrics();
    assert!(metrics.contains("operation=\"route_lookup_many\",status=\"ok\""));
    assert!(metrics.contains("operation=\"put_remote_batch_write\",status=\"ok\""));
    assert!(metrics.contains("operation=\"get_remote_batch_chunk\",status=\"ok\""));
    assert!(metrics.contains("operation=\"get_remote_direct\",status=\"ok\""));
}

#[test]
fn lookup_runtime_lease_reuses_live_client_snapshot() {
    let metadata = Arc::new(CountingMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let storage_transport = Arc::new(TestTransport::new("storage-cache-segment"));
    let client_transport = Arc::new(storage_transport.peer("client-cache-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "storage-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("storage build should succeed");
    let client = StoreClientBuilder::new(metadata.clone(), "client-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .transport(client_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("client build should succeed");

    let runtime = storage.runtime_id().clone();
    let before = metadata.list_live_clients_calls();
    let lease = client
        .lookup_runtime_lease(&runtime)
        .expect("first runtime lookup should succeed");
    assert_eq!(lease.runtime, runtime);
    let after_first = metadata.list_live_clients_calls();

    let lease = client
        .lookup_runtime_lease(&runtime)
        .expect("second runtime lookup should succeed");
    assert_eq!(lease.runtime, runtime);
    let after_second = metadata.list_live_clients_calls();

    assert!(after_first > before);
    assert_eq!(after_second, after_first);
}

#[test]
fn embedded_wrh_route_directory_reuses_authority_snapshot() {
    let metadata = Arc::new(CountingMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let storage_transport = Arc::new(TestTransport::new("storage-route-cache-segment"));
    let writer_transport = Arc::new(storage_transport.peer("writer-route-cache-segment"));
    let reader_transport = Arc::new(storage_transport.peer("reader-route-cache-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "storage-route-cache")
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

    let writer = StoreClientBuilder::new(metadata.clone(), "writer-route-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(10_000)
        .expect("writer build should succeed");
    writer
        .register_local_memory()
        .expect("writer memory should register");

    let reader = StoreClientBuilder::new(metadata.clone(), "reader-route-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("reader build should succeed");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    writer
        .put_in_tenant("tenant-a", "route-cache-key", b"route-cache-payload")
        .expect("writer put should succeed");
    assert!(
        metadata
            .inner
            .get_object_route(&ObjectKey::new("tenant-a::route-cache-key"))
            .expect("metadata query should succeed")
            .is_none(),
        "embedded WRH should keep route state off metadata"
    );

    let before = metadata.list_live_clients_calls();
    let route = reader
        .query_route_in_tenant("tenant-a", "route-cache-key")
        .expect("first route query should succeed")
        .expect("route should exist");
    assert_eq!(route.key, ObjectKey::new("tenant-a::route-cache-key"));
    let after_first = metadata.list_live_clients_calls();

    let route = reader
        .query_route_in_tenant("tenant-a", "route-cache-key")
        .expect("second route query should succeed")
        .expect("route should exist");
    assert_eq!(route.key, ObjectKey::new("tenant-a::route-cache-key"));
    let after_second = metadata.list_live_clients_calls();

    assert!(after_first > before);
    assert_eq!(after_second, after_first);
}

#[test]
fn singleton_control_plane_paths_use_stream_sessions() {
    let metadata = Arc::new(NoHotPathMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let storage_transport = Arc::new(TestTransport::new("storage-single-stream-segment"));
    let router_transport = Arc::new(storage_transport.peer("router-single-stream-segment"));
    let reader_transport = Arc::new(storage_transport.peer("reader-single-stream-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "storage-single-stream")
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

    let router = StoreClientBuilder::new(metadata.clone(), "router-single-stream")
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

    let reader = StoreClientBuilder::new(metadata.clone(), "reader-single-stream")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("reader build should succeed");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    assert_eq!(router.control_client.active_stream_sessions(), 0);
    router
        .put_in_tenant("tenant-a", "single-stream-key", b"single-stream-payload")
        .expect("routed put should succeed");
    assert!(
        router.control_client.active_stream_sessions() >= 1,
        "single-item allocator path should open a reusable control stream"
    );

    assert_eq!(reader.control_client.active_stream_sessions(), 0);
    assert_eq!(
        reader
            .get_in_tenant("tenant-a", "single-stream-key")
            .expect("reader get should succeed"),
        b"single-stream-payload"
    );
    assert!(
        reader.control_client.active_stream_sessions() >= 1,
        "single-item route lookup should open a reusable control stream"
    );
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
    assert!(owners.contains(client.runtime_id()));
    assert!(owners.contains(&owner_a) || owners.contains(&owner_b));

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
            PutRequest::new("hot-batch-a", b"alpha-0")
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().prefer_local(false)),
            PutRequest::new("hot-batch-b", b"beta-00")
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().prefer_local(false)),
            PutRequest::new("hot-batch-c", b"gamma-0")
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().prefer_local(false)),
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
            PutRequest::new("hot-batch-a", b"alpha-1")
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().prefer_local(false)),
            PutRequest::new("hot-batch-b", b"beta-11")
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().prefer_local(false)),
            PutRequest::new("hot-batch-c", b"gamma-1")
                .tenant("tenant-a")
                .replication(ReplicationPolicy::new().prefer_local(false)),
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
fn batch_get_chunks_remote_reads_when_scratch_window_is_small() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-batch-chunk-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        metadata.as_ref(),
        transport.as_ref(),
        "storage-batch-chunk",
        "seg-batch-chunk",
        "pool-a",
        1024,
        1,
    );
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-batch-chunk")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(10_000)
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-chunk")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config_with_layout(4096, 8, 1))
        .build(10_000)
        .expect("reader build should succeed");

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(remote_owner.storage_key());
    writer
        .batch_put(&[
            PutRequest::new("chunk-a", b"aaaa").replication(policy.clone()),
            PutRequest::new("chunk-b", b"bbbb").replication(policy.clone()),
            PutRequest::new("chunk-c", b"cccc").replication(policy),
        ])
        .expect("remote batch put should succeed");

    let values = reader
        .batch_get(&[
            ObjectRef::new("chunk-a"),
            ObjectRef::new("chunk-b"),
            ObjectRef::new("chunk-c"),
        ])
        .expect("batch_get should chunk remote reads");
    assert_eq!(
        values,
        vec![b"aaaa".to_vec(), b"bbbb".to_vec(), b"cccc".to_vec()]
    );
}

#[test]
fn batch_get_into_falls_back_to_direct_when_value_exceeds_scratch() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-batch-direct-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        metadata.as_ref(),
        transport.as_ref(),
        "storage-batch-direct",
        "seg-batch-direct",
        "pool-a",
        1024,
        1,
    );
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-batch-direct")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(10_000)
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-direct")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config_with_layout(4096, 4, 1))
        .build(10_000)
        .expect("reader build should succeed");

    writer
        .put_with_policy(
            "direct-fallback",
            b"abcdefgh",
            &ReplicationPolicy::new()
                .prefer_local(false)
                .preferred_storage_owner(remote_owner.storage_key()),
        )
        .expect("remote put should succeed");

    let mut buffer = [0u8; 8];
    let sizes = reader
        .batch_get_into(&mut [GetRequest::new("direct-fallback", &mut buffer)])
        .expect("batch_get_into should fall back to direct transfer");
    assert_eq!(sizes, vec![8]);
    assert_eq!(&buffer, b"abcdefgh");
}

#[test]
fn registered_buffer_subranges_support_put_from_and_batch_get_into() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-subrange-segment"));
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-subrange")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(10_000)
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-subrange")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .transport(transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("reader build should succeed");

    let mut source = [0u8; 128];
    let payload_a = b"page-one";
    let payload_b = b"page-two!";
    source[..payload_a.len()].copy_from_slice(payload_a);
    source[64..64 + payload_b.len()].copy_from_slice(payload_b);

    writer
        .register_buffer(source.as_mut_ptr().cast(), source.len())
        .expect("writer register buffer should succeed");
    let written = writer
        .batch_put_from(&[
            PutFromRequest::new("subrange-a", source.as_ptr().cast(), payload_a.len()),
            PutFromRequest::new(
                "subrange-b",
                unsafe { source.as_ptr().add(64).cast() },
                payload_b.len(),
            ),
        ])
        .expect("batch_put_from should accept registered subranges");
    assert_eq!(written.len(), 2);

    let mut target = [0u8; 128];
    reader
        .register_buffer(target.as_mut_ptr().cast(), target.len())
        .expect("reader register buffer should succeed");
    let sizes = reader
        .batch_get_into(&mut [
            GetRequest::new("subrange-a", unsafe {
                std::slice::from_raw_parts_mut(target.as_mut_ptr(), payload_a.len())
            }),
            GetRequest::new("subrange-b", unsafe {
                std::slice::from_raw_parts_mut(target.as_mut_ptr().add(64), payload_b.len())
            }),
        ])
        .expect("batch_get_into should accept registered subranges");
    assert_eq!(sizes, vec![payload_a.len(), payload_b.len()]);
    assert_eq!(&target[..payload_a.len()], payload_a);
    assert_eq!(&target[64..64 + payload_b.len()], payload_b);
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
fn replication_policy_prefers_local_before_remote_replica() {
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
    assert_eq!(route.replicas[0].owner, client.runtime_id().clone());
    let owners = route
        .replicas
        .iter()
        .map(|replica| replica.owner.clone())
        .collect::<BTreeSet<_>>();
    assert!(owners.contains(client.runtime_id()));
    assert!(owners.contains(&owner_a) || owners.contains(&owner_b));
    assert_eq!(
        client.get("replicated-key").expect("get should succeed"),
        b"replicated-payload"
    );
}

#[test]
fn default_put_spills_to_remote_after_local_capacity_is_exhausted() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-spill-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        metadata.as_ref(),
        transport.as_ref(),
        "storage-spill",
        "seg-spill",
        "pool-a",
        1024,
        1,
    );
    let client = StoreClientBuilder::new(metadata, "writer-spill")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config_with_bytes(8))
        .build(10_000)
        .expect("client build should succeed");

    let first = client
        .put("spill-a", b"abcdefgh")
        .expect("first put should succeed");
    let second = client
        .put("spill-b", b"ijklmnop")
        .expect("second put should succeed");

    assert_eq!(first.replicas[0].owner, client.runtime_id().clone());
    assert_eq!(second.replicas[0].owner, remote_owner);
    assert_eq!(
        client.get("spill-a").expect("first get should succeed"),
        b"abcdefgh"
    );
    assert_eq!(
        client.get("spill-b").expect("second get should succeed"),
        b"ijklmnop"
    );
}

#[test]
fn preferred_storage_owner_overrides_local_default_and_falls_back_when_full() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-prefer-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        metadata.as_ref(),
        transport.as_ref(),
        "storage-prefer",
        "seg-prefer",
        "pool-a",
        8,
        1,
    );
    let client = StoreClientBuilder::new(metadata, "writer-prefer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config_with_bytes(16))
        .build(10_000)
        .expect("client build should succeed");

    let policy = ReplicationPolicy::new()
        .preferred_storage_owner(remote_owner.storage_key())
        .replica_count(1);
    let first = client
        .put_with_policy("prefer-remote-a", b"abcdefgh", &policy)
        .expect("preferred remote put should succeed");
    let second = client
        .put_with_policy("prefer-remote-b", b"ijklmnop", &policy)
        .expect("fallback put should succeed");

    assert_eq!(first.replicas[0].owner, remote_owner);
    assert_eq!(second.replicas[0].owner, client.runtime_id().clone());
    assert_eq!(
        client
            .get("prefer-remote-a")
            .expect("first preferred get should succeed"),
        b"abcdefgh"
    );
    assert_eq!(
        client
            .get("prefer-remote-b")
            .expect("fallback get should succeed"),
        b"ijklmnop"
    );
}

#[test]
fn routed_batch_put_prefers_local_before_spilling_remote() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("router-local-first-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        metadata.as_ref(),
        transport.as_ref(),
        "storage-batch",
        "seg-batch",
        "pool-a",
        1024,
        1,
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let client = StoreClientBuilder::new(metadata, "router-local-first")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config_with_bytes(8))
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("client build should succeed");

    let routes = client
        .batch_put(&[
            PutRequest::new("batch-spill-a", b"abcdefgh"),
            PutRequest::new("batch-spill-b", b"ijklmnop"),
        ])
        .expect("batch put should succeed");

    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0].replicas[0].owner, client.runtime_id().clone());
    assert_eq!(routes[1].replicas[0].owner, remote_owner);
    assert_eq!(
        client
            .get("batch-spill-a")
            .expect("first batch get should succeed"),
        b"abcdefgh"
    );
    assert_eq!(
        client
            .get("batch-spill-b")
            .expect("second batch get should succeed"),
        b"ijklmnop"
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

#[test]
fn true_client_shrink_evacuates_live_routes_and_retires_segments() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("shrink-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("shrink-b-segment"));
    let router_transport = Arc::new(store_a_transport.peer("shrink-router-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("shrink-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut store_a = StoreClientBuilder::new(metadata.clone(), "shrink-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "shrink-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("store-b build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "shrink-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(router_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("router build should succeed");
    let reader = StoreClientBuilder::new(metadata, "shrink-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    router
        .register_local_memory()
        .expect("router memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    let mut expected = BTreeMap::new();
    let mut owned_keys = Vec::new();
    for index in 0..32 {
        let key = format!("shrink-key-{index}");
        let value = format!("value-{index:02}").into_bytes();
        router
            .put_with_policy(&key, &value, &ReplicationPolicy::new().prefer_local(false))
            .expect("seed routed put should succeed");
        let route = router
            .query_route(&key)
            .expect("route query should succeed")
            .expect("seed route should exist");
        if route.replicas[0].owner == *store_a.runtime_id() {
            expected.insert(key.clone(), value);
            owned_keys.push(key);
        }
    }
    assert!(
        !owned_keys.is_empty(),
        "expected at least one object to land on store-a"
    );

    let migrated = store_a
        .evacuate_owned_replicas()
        .expect("true client shrink should succeed");
    assert_eq!(migrated, owned_keys.len());
    assert_eq!(store_a.lease().state, ClientLifecycleState::Draining);
    assert!(
        store_a
            .list_segments()
            .expect("segment listing should succeed")
            .is_empty(),
        "all drained local segments should retire after shrink"
    );

    for key in owned_keys {
        let route = router
            .query_route(&key)
            .expect("post-shrink route query should succeed")
            .expect("post-shrink route should exist");
        assert!(
            route
                .replicas
                .iter()
                .all(|replica| replica.owner != *store_a.runtime_id()),
            "post-shrink route still references evacuated runtime"
        );
        assert_eq!(
            reader.get(&key).expect("reader get should succeed"),
            expected[&key]
        );
    }
}

#[test]
fn evacuate_owned_replicas_via_explicit_writer_preserves_readability() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("writer-via-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("writer-via-b-segment"));
    let router_transport = Arc::new(store_a_transport.peer("writer-via-router-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("writer-via-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut store_a = StoreClientBuilder::new(metadata.clone(), "writer-via-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "writer-via-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("store-b build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "writer-via-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(router_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(10_000)
        .expect("router build should succeed");
    let reader = StoreClientBuilder::new(metadata, "writer-via-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(10_000)
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    router
        .register_local_memory()
        .expect("router memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    let mut expected = BTreeMap::new();
    let mut owned_keys = Vec::new();
    for index in 0..24 {
        let key = format!("writer-via-key-{index}");
        let value = format!("value-{index:02}").into_bytes();
        router
            .put_with_policy(&key, &value, &ReplicationPolicy::new().prefer_local(false))
            .expect("seed routed put should succeed");
        let route = router
            .query_route(&key)
            .expect("route query should succeed")
            .expect("route should exist");
        if route.replicas[0].owner == *store_a.runtime_id() {
            expected.insert(key.clone(), value);
            owned_keys.push(key);
        }
    }
    assert!(
        !owned_keys.is_empty(),
        "expected at least one object to land on store-a"
    );

    let migrated = store_a
        .evacuate_owned_replicas_via(&router)
        .expect("explicit writer evacuation should succeed");
    assert_eq!(migrated, owned_keys.len());
    assert_eq!(store_a.lease().state, ClientLifecycleState::Draining);
    assert!(
        store_a
            .list_segments()
            .expect("segment listing should succeed")
            .is_empty(),
        "all local segments should retire after explicit writer evacuation"
    );

    for key in owned_keys {
        let route = router
            .query_route(&key)
            .expect("post-evacuation route query should succeed")
            .expect("post-evacuation route should exist");
        assert!(
            route
                .replicas
                .iter()
                .all(|replica| replica.owner != *store_a.runtime_id()),
            "evacuated route still references store-a"
        );
        assert_eq!(
            reader.get(&key).expect("reader get should succeed"),
            expected[&key]
        );
    }
}

#[test]
fn internal_allocator_and_store_state_cover_edge_cases() {
    let owner = ClientRuntimeId::new("writer", ClientEpoch(7));
    let primary = SegmentAnnouncement {
        owner: owner.clone(),
        segment_name: SegmentName::new("alloc-primary"),
        capacity_bytes: 128,
        used_bytes: 0,
        state: SegmentLifecycleState::Active,
        alignment_bytes: 16,
        tags: vec!["dram".to_string()],
    };
    let secondary = SegmentAnnouncement {
        segment_name: SegmentName::new("alloc-secondary"),
        capacity_bytes: 64,
        ..primary.clone()
    };
    let draining = SegmentAnnouncement {
        segment_name: SegmentName::new("alloc-draining"),
        state: SegmentLifecycleState::Draining,
        ..primary.clone()
    };

    let mut allocator = LocalAllocatorState::default();
    allocator.upsert(&primary);
    allocator.upsert(&secondary);
    allocator.upsert(&draining);
    assert_eq!(allocator.announcements().len(), 3);
    assert!(allocator.announcement(&primary.segment_name).is_some());
    assert!(matches!(
        allocator.update_state(&SegmentName::new("missing"), SegmentLifecycleState::Active),
        Err(StoreError::NotFound(_))
    ));

    let first = allocator
        .reserve_any(&owner, 17)
        .expect("allocator should reserve from the largest segment");
    assert_eq!(first.segment_name, primary.segment_name);
    allocator
        .release(
            &owner,
            &first.segment_name,
            first.offset_bytes,
            first.length_bytes,
        )
        .expect("release should succeed");
    let reused = allocator
        .reserve_specific(&owner, &first.segment_name, 17)
        .expect("free span should be reusable");
    assert_eq!(reused.offset_bytes, first.offset_bytes);
    allocator.remove(&draining.segment_name);
    assert!(allocator.announcement(&draining.segment_name).is_none());

    let mut mismatch = SegmentAllocator::new(primary.clone());
    let reserved = mismatch
        .reserve(&owner, &primary.segment_name, 15)
        .expect("reservation should succeed");
    assert!(matches!(
        mismatch.release(&owner, &primary.segment_name, reserved.offset_bytes, 14),
        Err(StoreError::Allocator(_))
    ));

    let mut missing = SegmentAllocator::new(primary.clone());
    assert!(matches!(
        missing.release(&owner, &primary.segment_name, 0, 1),
        Err(StoreError::Allocator(_))
    ));

    let mut merged = SegmentAllocator::new(primary.clone());
    let recycled = merged
        .reserve(&owner, &primary.segment_name, 15)
        .expect("reservation should succeed");
    merged
        .release(
            &owner,
            &primary.segment_name,
            recycled.offset_bytes,
            recycled.length_bytes,
        )
        .expect("release should succeed");
    let next_announcement = SegmentAnnouncement {
        used_bytes: 64,
        alignment_bytes: 1,
        state: SegmentLifecycleState::Active,
        tags: vec!["nvme".to_string()],
        ..primary.clone()
    };
    merged.merge_announcement(&next_announcement);
    assert_eq!(merged.announcement.state, SegmentLifecycleState::Active);
    assert_eq!(merged.cursor_bytes, 64);
    assert!(matches!(
        merged.reserve(&owner, &primary.segment_name, 0),
        Err(StoreError::Allocator(_))
    ));

    let shared_allocator = Arc::new(Mutex::new(LocalAllocatorState::default()));
    shared_allocator.lock().upsert(&primary);
    let adapter = LocalAllocatorAdapter {
        runtime: owner.clone(),
        allocator: shared_allocator.clone(),
    };
    let wrong_owner = ClientRuntimeId::new("other", ClientEpoch(9));
    assert!(matches!(
        adapter.reserve_any(&wrong_owner, 8),
        Err(StoreError::InvalidState(_))
    ));
    assert!(adapter
        .batch_reserve_any(&wrong_owner, &[8, 8])
        .into_iter()
        .all(|result| matches!(result, Err(StoreError::InvalidState(_)))));

    let batch = adapter.batch_reserve_any(&owner, &[8, 8]);
    assert_eq!(batch.len(), 2);
    let specific = adapter.batch_reserve_specific(
        &owner,
        &[crate::control_plane::ReserveSpecificOp {
            segment_name: primary.segment_name.clone(),
            length_bytes: 8,
        }],
    );
    assert!(specific[0].is_ok());

    let transport = TestTransport::new("state-primary");
    let remote_handle = transport.add_external_segment("remote-segment", 128);
    let mut state = StoreState::default();
    assert!(matches!(
        state.memory_ref(),
        Err(StoreError::InvalidState(_))
    ));
    state.memory = Some(
        crate::memory::LocalMemoryState::register(
            &transport,
            &SegmentName::new("state-primary"),
            &storage_config(),
        )
        .expect("local memory should register"),
    );
    assert!(state.memory_mut().is_ok());
    let next_a = state.next_segment_name(&SegmentName::new("state-primary"));
    let next_b = state.next_segment_name(&SegmentName::new("state-primary"));
    assert_ne!(next_a, next_b);
    assert_eq!(
        state
            .open_segment(&transport, "remote-segment")
            .expect("remote segment should open"),
        remote_handle
    );
    assert_eq!(
        state
            .open_segment(&transport, "remote-segment")
            .expect("remote segment should be cached"),
        remote_handle
    );

    let mut buffer = [0u8; 64];
    let buffer_ptr = buffer.as_mut_ptr().cast::<c_void>();
    state
        .register_external_buffer(&transport, buffer_ptr, buffer.len())
        .expect("buffer registration should succeed");
    assert!(state.buffer_is_registered(buffer_ptr, 16));
    assert!(matches!(
        state.register_external_buffer(&transport, unsafe { buffer.as_mut_ptr().add(8).cast() }, 8),
        Err(StoreError::Allocator(_))
    ));
    assert!(matches!(
        state.unregister_external_buffer(&transport, buffer_ptr, 8),
        Err(StoreError::Allocator(_))
    ));
    assert!(state.buffer_is_registered(buffer_ptr, 16));
    state
        .unregister_external_buffer(&transport, buffer_ptr, buffer.len())
        .expect("buffer unregister should succeed");
    assert!(!state.buffer_is_registered(buffer_ptr, 1));

    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 5,
        storage_runtime: owner.clone(),
        segment_name: SegmentName::new("seg-a"),
        offset_bytes: 0,
        length_bytes: 8,
    });
    state.pending_reclaims.push_back(PendingReclaim {
        due_at_ms: 50,
        storage_runtime: owner,
        segment_name: SegmentName::new("seg-b"),
        offset_bytes: 16,
        length_bytes: 8,
    });
    assert_eq!(state.take_due_reclaims(10).len(), 1);
    assert_eq!(state.pending_reclaims.len(), 1);
}
