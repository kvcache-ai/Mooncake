use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, CompatibilityDescriptor, HandoffKind, MetadataBackend, ObjectKey,
    RouteCasRequest, RoutePolicy, RoutePolicyDomain, RouteVersion, SegmentAnnouncement,
    SegmentLifecycleState, SegmentName, StoreError,
};
use mooncake_transport::{
    Opcode, SegmentBuffer, SegmentInfo, SegmentKind, TransferProgress, TransferRequest,
    TransferStatus,
};
use parking_lot::Mutex;

use super::{
    align_up_u64, bootstrap_route_policy, cached_live_client_snapshot, compatibility_matches,
    control_bind_host, copy_into_region, flatten_slices, now_ms, record_success_metric,
    scatter_into_buffers, startup_prewarm_delay, LiveClientCache, LocalAllocatorAdapter,
    LocalAllocatorState, LocalAuthorityAdapter, PendingReclaim, ReplicaWriteTarget,
    SegmentAllocator, StorageOwnerState, StoreState, SuspectRuntimeCache,
};
use crate::{
    control_plane::{
        control_address_label, AllocatorService, AuthorityService, ControlPlaneClient, ReleaseOp,
    },
    memory::RegionAllocation,
    metrics_test_lock, render_prometheus_metrics, reset_metrics,
    route_directory::build_route_directory,
    snapshot_metrics,
    transport::{StoreTransport, StoreTransportFactory},
    GetRequest, LocalMemoryConfig, MooncakeCompatibilityFacade, MultiBufferGetRequest,
    MultiBufferPutRequest, ObjectRef, PlacementPlanner, PutFromRequest, PutRequest,
    ReplicationPolicy, RouteControlMode, StoreClient, StoreClientBuilder,
};

struct TestTransport {
    local_segment: String,
    state: Arc<Mutex<TestTransportState>>,
}

struct TestTransportFactory {
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

    fn get_route_policy(
        &self,
        domain: &RoutePolicyDomain,
    ) -> mooncake_store_core::Result<Option<RoutePolicy>> {
        self.inner.get_route_policy(domain)
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> mooncake_store_core::Result<bool> {
        self.inner.put_route_policy_if_absent(domain, policy)
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

    fn get_route_policy(
        &self,
        domain: &RoutePolicyDomain,
    ) -> mooncake_store_core::Result<Option<RoutePolicy>> {
        self.inner.get_route_policy(domain)
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> mooncake_store_core::Result<bool> {
        self.inner.put_route_policy_if_absent(domain, policy)
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

    fn factory(&self) -> Arc<dyn StoreTransportFactory> {
        Arc::new(TestTransportFactory {
            state: self.state.clone(),
        })
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

impl StoreTransportFactory for TestTransportFactory {
    fn create(&self, segment_name: &str) -> mooncake_store_core::Result<Arc<dyn StoreTransport>> {
        Ok(Arc::new(TestTransport {
            local_segment: segment_name.to_string(),
            state: self.state.clone(),
        }))
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

fn storage_config_with_background_eviction(
    storage_bytes: usize,
    high_percent: u8,
    low_percent: u8,
) -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .storage_bytes(storage_bytes)
        .scratch_bytes(4096)
        .alignment(1)
        .reclaim_grace_ms(0)
        .eviction_watermarks(high_percent, low_percent)
        .eviction_poll_interval(Duration::from_millis(10))
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

fn rw_only_config() -> LocalMemoryConfig {
    storage_config_with_layout(0, 4096, 1)
}

fn fast_live_client_sync_interval() -> Duration {
    Duration::from_millis(25)
}

fn test_future_expiry_ms() -> u64 {
    now_ms().saturating_add(30_000)
}

fn test_storage_owner_state(
    runtime: &ClientRuntimeId,
    allocator: Arc<Mutex<LocalAllocatorState>>,
) -> Arc<StorageOwnerState> {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let live_client_cache = Arc::new(Mutex::new(LiveClientCache::default()));
    let control_client = Arc::new(ControlPlaneClient::new().expect("control client should build"));
    let lease = ClientLease {
        runtime: runtime.clone(),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: test_future_expiry_ms(),
    };
    let route_directory = build_route_directory(
        RouteControlMode::MetadataOnly,
        2,
        metadata.clone(),
        &lease,
        control_client.clone(),
        live_client_cache.clone(),
        Arc::new(Mutex::new(SuspectRuntimeCache::default())),
    );
    Arc::new(StorageOwnerState::new(
        runtime.clone(),
        lease,
        metadata,
        route_directory,
        allocator,
    ))
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
            expires_at_ms: test_future_expiry_ms(),
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

fn wait_for_runtime_visibility(client: &StoreClient, runtime: &ClientRuntimeId) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if client.lookup_runtime_lease(runtime).is_ok() {
            return;
        }
        sleep(Duration::from_millis(10));
    }
    panic!(
        "runtime {} did not become visible in the client snapshot in time",
        runtime
    );
}

fn wait_for_membership_convergence(clients: &[&StoreClient]) {
    let runtimes = clients
        .iter()
        .map(|client| client.runtime_id().clone())
        .collect::<Vec<_>>();
    for client in clients {
        for runtime in &runtimes {
            if runtime == client.runtime_id() {
                continue;
            }
            wait_for_runtime_visibility(client, runtime);
        }
    }
}

#[test]
fn hot_upgrade_handoff_is_published_after_draining() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata.clone(), "client-a")
        .epoch(ClientEpoch(1))
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7001")
        .segment_name("client-a-segment")
        .build(test_future_expiry_ms())
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
fn hot_upgrade_successor_discovery_uses_same_stable_higher_epoch() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let predecessor = StoreClientBuilder::new(metadata.clone(), "upgrade-find")
        .epoch(ClientEpoch(1))
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7101")
        .segment_name("upgrade-find-old")
        .build(test_future_expiry_ms())
        .expect("predecessor build should succeed");
    let successor = StoreClientBuilder::new(metadata.clone(), "upgrade-find")
        .epoch(ClientEpoch(2))
        .state(ClientLifecycleState::Standby)
        .rpc_address("127.0.0.1:7102")
        .segment_name("upgrade-find-new")
        .build(test_future_expiry_ms())
        .expect("successor build should succeed");
    let _other_stable = StoreClientBuilder::new(metadata, "upgrade-other")
        .epoch(ClientEpoch(9))
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7103")
        .segment_name("upgrade-other")
        .build(test_future_expiry_ms())
        .expect("other stable build should succeed");

    let found = predecessor
        .find_hot_upgrade_successor()
        .expect("successor lookup should succeed")
        .expect("successor should exist");
    assert_eq!(found.runtime, successor.runtime_id().clone());
}

#[test]
fn targeted_hot_upgrade_handoff_promotes_standby_successor() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut predecessor = StoreClientBuilder::new(metadata.clone(), "upgrade-promote")
        .epoch(ClientEpoch(1))
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7111")
        .segment_name("upgrade-promote-old")
        .build(test_future_expiry_ms())
        .expect("predecessor build should succeed");
    let mut successor = StoreClientBuilder::new(metadata, "upgrade-promote")
        .epoch(ClientEpoch(2))
        .state(ClientLifecycleState::Standby)
        .rpc_address("127.0.0.1:7112")
        .segment_name("upgrade-promote-new")
        .build(test_future_expiry_ms())
        .expect("successor build should succeed");

    predecessor
        .enter_draining()
        .expect("predecessor should drain");
    predecessor
        .plan_handoff(
            ClientEpoch(2),
            HandoffKind::HotUpgrade,
            8,
            100,
            Some(u64::MAX),
        )
        .expect("handoff planning should succeed");

    let plan = successor
        .activate_if_targeted_handoff()
        .expect("successor activation should succeed")
        .expect("targeted handoff should be consumed");
    assert_eq!(plan.from, predecessor.runtime_id().clone());
    assert_eq!(plan.to, successor.runtime_id().clone());
    assert_eq!(successor.lease().state, ClientLifecycleState::Active);
    assert_eq!(
        predecessor
            .runtime_state(successor.runtime_id())
            .expect("runtime state lookup should succeed"),
        Some(ClientLifecycleState::Active)
    );
}

#[test]
fn query_route_uses_default_tenant_scope() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let client = StoreClientBuilder::new(metadata.clone(), "client-a")
        .tenant("tenant-a")
        .rpc_address("127.0.0.1:7001")
        .segment_name("client-a-segment")
        .build(test_future_expiry_ms())
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
        .build(test_future_expiry_ms())
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
        .build(test_future_expiry_ms())
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
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("metrics-segment"));
    let client = StoreClientBuilder::new(metadata, "client-metrics")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
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
    let _guard = metrics_test_lock().lock();
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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "client-metrics-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport)
        .local_memory(storage_config_with_layout(4096, 4, 1))
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    wait_for_membership_convergence(&[&writer, &reader]);

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
    assert!(metrics.contains("operation=\"put_stage_load_route\",status=\"ok\""));
    assert!(metrics.contains("operation=\"put_stage_reserve\",status=\"ok\""));
    assert!(metrics.contains("operation=\"put_stage_write\",status=\"ok\""));
    assert!(metrics.contains("operation=\"put_stage_route_cas\",status=\"ok\""));
    assert!(metrics.contains("operation=\"put_remote_batch_write\",status=\"ok\""));
    assert!(metrics.contains("operation=\"get_remote_batch_chunk\",status=\"ok\""));
    assert!(metrics.contains("operation=\"get_remote_direct\",status=\"ok\""));
    assert!(metrics
        .contains("mooncake_store_replication_publish_duration_seconds_count{result=\"ok\"}"));
    assert!(metrics.contains(
        "mooncake_store_transport_bytes_total{direction=\"write\",peer_kind=\"storage\"}"
    ));
    assert!(metrics.contains(
        "mooncake_store_transport_bytes_total{direction=\"read\",peer_kind=\"storage\"}"
    ));
    assert!(metrics.contains("mooncake_store_checksum_validation_total{result=\"ok\"}"));
}

#[test]
fn observability_metrics_render_fast_batch_put_stages() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("metrics-fast-batch-segment"));
    publish_storage_node(
        metadata.as_ref(),
        transport.as_ref(),
        "metrics-fast-storage",
        "metrics-fast-storage-seg",
        "pool-a",
    );
    let writer = StoreClientBuilder::new(metadata.clone(), "metrics-fast-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    writer
        .batch_put(&[
            PutRequest::new("fast-batch-a", b"left"),
            PutRequest::new("fast-batch-b", b"right"),
        ])
        .expect("fast routed batch put should succeed");

    let metrics = render_prometheus_metrics();
    assert!(metrics.contains("operation=\"batch_put_stage_rank\",status=\"ok\""));
    assert!(metrics.contains("operation=\"batch_put_stage_reserve\",status=\"ok\""));
    assert!(metrics.contains("operation=\"batch_put_stage_load_routes\",status=\"ok\""));
    assert!(metrics.contains("operation=\"batch_put_stage_write\",status=\"ok\""));
    assert!(metrics.contains("operation=\"batch_put_stage_route_cas\",status=\"ok\""));
}

#[test]
fn observability_metrics_render_fast_batch_put_stages_for_shared_policy() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("metrics-fast-shared-policy-segment"));
    publish_storage_node(
        metadata.as_ref(),
        transport.as_ref(),
        "metrics-fast-shared-policy-storage",
        "metrics-fast-shared-policy-storage-seg",
        "pool-a",
    );
    let writer = StoreClientBuilder::new(metadata.clone(), "metrics-fast-shared-policy-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_segment("metrics-fast-shared-policy-storage-seg");
    let routes = writer
        .batch_put(&[
            PutRequest::new("fast-policy-a", b"left").replication(policy.clone()),
            PutRequest::new("fast-policy-b", b"right").replication(policy),
        ])
        .expect("shared-policy fast routed batch put should succeed");
    assert_eq!(routes.len(), 2);
    assert!(routes.iter().all(|route| {
        route.replicas.len() == 1
            && route.replicas[0].segment_name
                == SegmentName::new("metrics-fast-shared-policy-storage-seg")
    }));

    let metrics = render_prometheus_metrics();
    assert!(metrics.contains("operation=\"batch_put_stage_rank\",status=\"ok\""));
    assert!(metrics.contains("operation=\"batch_put_stage_reserve\",status=\"ok\""));
    assert!(metrics.contains("operation=\"batch_put_stage_load_routes\",status=\"ok\""));
    assert!(metrics.contains("operation=\"batch_put_stage_write\",status=\"ok\""));
    assert!(metrics.contains("operation=\"batch_put_stage_route_cas\",status=\"ok\""));
}

#[test]
fn rw_only_client_registers_without_segments_and_routes_to_remote_storage() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let storage_transport = Arc::new(TestTransport::new("rw-only-storage-segment"));
    let writer_transport = Arc::new(storage_transport.peer("rw-only-writer-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "rw-only-storage")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage local memory should register");

    let writer = StoreClientBuilder::new(metadata.clone(), "rw-only-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("rw-only writer should build");
    writer
        .register_local_memory()
        .expect("rw-only writer scratch should register");

    assert!(writer
        .list_segments()
        .expect("rw-only segment listing should succeed")
        .is_empty());

    writer
        .put("remote-key", b"payload")
        .expect("remote put should succeed");
    writer
        .batch_put(&[
            PutRequest::new("remote-batch-a", b"left"),
            PutRequest::new("remote-batch-b", b"right"),
        ])
        .expect("remote batch put should succeed");

    let route = writer
        .query_route("remote-key")
        .expect("route lookup should succeed")
        .expect("route should exist");
    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner.stable_id.0, "rw-only-storage");
    assert_eq!(
        writer.get("remote-key").expect("remote get should succeed"),
        b"payload"
    );

    let values = writer
        .batch_get(&[
            ObjectRef::new("remote-batch-a"),
            ObjectRef::new("remote-batch-b"),
        ])
        .expect("remote batch get should succeed");
    assert_eq!(values, vec![b"left".to_vec(), b"right".to_vec()]);
    assert!(writer
        .list_segments()
        .expect("rw-only segment listing should still succeed")
        .is_empty());
}

#[test]
fn builder_rejects_storage_true_without_storage_bytes() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("invalid-storage-role-segment"));
    let result = StoreClientBuilder::new(metadata, "invalid-storage-role")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms());
    let error = match result {
        Ok(_) => panic!("builder should reject storage=true without storage bytes"),
        Err(error) => error,
    };
    assert!(matches!(error, StoreError::InvalidState(_)));
}

#[test]
fn builder_defaults_storage_label_to_false_when_storage_bytes_is_zero() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("rw-default-storage-false-segment"));
    let client = StoreClientBuilder::new(metadata, "rw-default-storage-false")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("builder should succeed");
    assert_eq!(
        client
            .lease()
            .endpoints
            .labels
            .get("storage")
            .map(String::as_str),
        Some("false")
    );
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
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    let client = StoreClientBuilder::new(metadata.clone(), "client-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(client_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("client build should succeed");

    let runtime = storage.runtime_id().clone();
    let after_build = metadata.list_live_clients_calls();
    let lease = client
        .lookup_runtime_lease(&runtime)
        .expect("runtime lookup should succeed");
    assert_eq!(lease.runtime, runtime);
    let lease = client
        .lookup_runtime_lease(&runtime)
        .expect("second runtime lookup should succeed");
    assert_eq!(lease.runtime, runtime);
    let after_second = metadata.list_live_clients_calls();

    assert_eq!(after_second, after_build);
}

#[test]
fn rw_only_client_can_expand_into_primary_segment() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("rw-expand-segment"));
    let client = StoreClientBuilder::new(metadata, "rw-expand")
        .state(ClientLifecycleState::Active)
        .transport(transport.clone())
        .transport_factory(transport.factory())
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("rw-only client should build");

    client
        .register_local_memory()
        .expect("scratch-only registration should succeed");
    assert!(client
        .list_segments()
        .expect("scratch-only segment listing should succeed")
        .is_empty());

    let primary = client.segment_name().expect("primary segment should exist");
    let expanded = client
        .expand_local_memory(256)
        .expect("rw-only expansion should succeed");
    assert_eq!(expanded.segment_name, primary);

    let listed = client.list_segments().expect("segment list should succeed");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].segment_name, primary);
    assert_eq!(listed[0].capacity_bytes, 256);

    client
        .put("local-after-expand", b"hello")
        .expect("local put after expansion should succeed");
    assert_eq!(
        client
            .get("local-after-expand")
            .expect("local get after expansion should succeed"),
        b"hello"
    );
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
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let writer = StoreClientBuilder::new(metadata.clone(), "writer-route-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    writer
        .register_local_memory()
        .expect("writer memory should register");

    let reader = StoreClientBuilder::new(metadata.clone(), "reader-route-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
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

    let after_build = metadata.list_live_clients_calls();
    let route = reader
        .query_route_in_tenant("tenant-a", "route-cache-key")
        .expect("route query should succeed")
        .expect("route should exist");
    assert_eq!(route.key, ObjectKey::new("tenant-a::route-cache-key"));

    let route = reader
        .query_route_in_tenant("tenant-a", "route-cache-key")
        .expect("second route query should succeed")
        .expect("route should exist");
    assert_eq!(route.key, ObjectKey::new("tenant-a::route-cache-key"));
    let after_second = metadata.list_live_clients_calls();

    assert_eq!(after_second, after_build);
}

#[test]
fn routed_put_reuses_live_client_snapshot_for_placement() {
    let metadata = Arc::new(CountingMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let storage_transport = Arc::new(TestTransport::new("storage-placement-cache-segment"));
    let writer_transport = Arc::new(storage_transport.peer("writer-placement-cache-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "storage-placement-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let writer = StoreClientBuilder::new(metadata.clone(), "writer-placement-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    writer
        .register_local_memory()
        .expect("writer memory should register");

    let after_build = metadata.list_live_clients_calls();
    writer
        .put_in_tenant("tenant-a", "placement-cache-a", b"alpha")
        .expect("first put should succeed");

    writer
        .put_in_tenant("tenant-a", "placement-cache-b", b"beta")
        .expect("second put should succeed");
    let after_second = metadata.list_live_clients_calls();

    assert_eq!(
        after_second, after_build,
        "steady-state routed put should reuse the client live snapshot"
    );
}

#[test]
fn build_prewarms_live_client_snapshot_for_first_routed_put() {
    let metadata = Arc::new(CountingMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let storage_transport = Arc::new(TestTransport::new("storage-first-put-shared-cache-segment"));
    let writer_transport =
        Arc::new(storage_transport.peer("writer-first-put-shared-cache-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "storage-first-put-shared-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let writer = StoreClientBuilder::new(metadata.clone(), "writer-first-put-shared-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    writer
        .register_local_memory()
        .expect("writer memory should register");

    let after_build = metadata.list_live_clients_calls();
    writer
        .put_in_tenant_with_policy(
            "tenant-a",
            "cold-start-key",
            b"payload",
            &ReplicationPolicy::new().prefer_local(false),
        )
        .expect("cold-start routed put should succeed");
    let after_first = metadata.list_live_clients_calls();

    assert_eq!(
        after_first, after_build,
        "first routed put should use the prewarmed live-client snapshot without an on-request refresh"
    );
}

#[test]
fn startup_prewarm_delay_is_stably_spread_and_bounded() {
    let runtime = ClientRuntimeId::new("startup-prewarm", ClientEpoch(4));
    let delay = startup_prewarm_delay(&runtime, Duration::from_millis(250));
    assert!((1..=250).contains(&delay.as_millis()));
    assert_eq!(
        delay,
        startup_prewarm_delay(&runtime, Duration::from_millis(250))
    );
    assert_eq!(
        startup_prewarm_delay(&runtime, Duration::ZERO),
        Duration::ZERO
    );
}

#[test]
fn routed_batch_put_reuses_live_client_snapshot_for_placement() {
    let metadata = Arc::new(CountingMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let storage_transport = Arc::new(TestTransport::new("storage-batch-placement-cache-segment"));
    let writer_transport = Arc::new(storage_transport.peer("writer-batch-placement-cache-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "storage-batch-placement-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let writer = StoreClientBuilder::new(metadata.clone(), "writer-batch-placement-cache")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    writer
        .register_local_memory()
        .expect("writer memory should register");

    let after_build = metadata.list_live_clients_calls();
    writer
        .batch_put(&[
            PutRequest::new("placement-batch-a", b"alpha").tenant("tenant-a"),
            PutRequest::new("placement-batch-b", b"bravo").tenant("tenant-a"),
        ])
        .expect("first batch put should succeed");

    writer
        .batch_put(&[
            PutRequest::new("placement-batch-c", b"charlie").tenant("tenant-a"),
            PutRequest::new("placement-batch-d", b"delta").tenant("tenant-a"),
        ])
        .expect("second batch put should succeed");
    let after_second = metadata.list_live_clients_calls();

    assert_eq!(
        after_second, after_build,
        "steady-state routed batch put should reuse the client live snapshot"
    );
}

#[test]
fn background_live_client_sync_refreshes_snapshot_without_request_refresh() {
    let metadata = Arc::new(CountingMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let writer_transport = Arc::new(TestTransport::new("writer-background-sync-segment"));

    let writer = StoreClientBuilder::new(metadata.clone(), "writer-background-sync")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(Duration::from_millis(25))
        .transport(writer_transport.clone())
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    writer
        .register_local_memory()
        .expect("writer memory should register");

    let after_build = metadata.list_live_clients_calls();
    let storage_runtime = publish_storage_node(
        metadata.inner.as_ref(),
        writer_transport.as_ref(),
        "storage-background-sync",
        "storage-background-sync-seg",
        "pool-a",
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if writer.lookup_runtime_lease(&storage_runtime).is_ok()
            && metadata.list_live_clients_calls() > after_build
        {
            return;
        }
        sleep(Duration::from_millis(10));
    }

    panic!("background live-client sync did not observe the new storage runtime in time");
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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let router = StoreClientBuilder::new(metadata.clone(), "router-single-stream")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    router
        .register_local_memory()
        .expect("router memory should register");

    let reader = StoreClientBuilder::new(metadata.clone(), "reader-single-stream")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    wait_for_membership_convergence(&[&storage, &router, &reader]);

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
        .build(test_future_expiry_ms())
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
    assert!(!owners.contains(client.runtime_id()));
    assert!(owners.contains(&owner_a));
    assert!(owners.contains(&owner_b));

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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    wait_for_membership_convergence(&[&writer, &reader]);

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
fn routed_read_skips_inactive_primary_replica_and_prunes_stale_route() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("inactive-primary-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("inactive-primary-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("inactive-primary-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("inactive-primary-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "inactive-primary-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "inactive-primary-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "inactive-primary-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "inactive-primary-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "inactive-primary-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "inactive-primary-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 2)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "inactive-primary-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "inactive-primary-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &writer, &reader]);

    writer
        .put_with_policy(
            "inactive-primary-key",
            b"inactive-primary-payload",
            &ReplicationPolicy::new()
                .replica_count(2)
                .prefer_local(false)
                .preferred_storage_owners([
                    store_a.runtime_id().storage_key(),
                    store_b.runtime_id().storage_key(),
                ]),
        )
        .expect("replicated put should succeed");

    let seeded = reader
        .query_route("inactive-primary-key")
        .expect("seeded route query should succeed")
        .expect("seeded route should exist");
    assert_eq!(seeded.replicas[0].owner, *store_a.runtime_id());
    assert_eq!(seeded.replicas[1].owner, *store_b.runtime_id());

    metadata
        .update_client_state(store_a.runtime_id(), ClientLifecycleState::Offline)
        .expect("store-a state should update");
    assert_eq!(
        reader
            .runtime_state(store_a.runtime_id())
            .expect("runtime state query should succeed"),
        Some(ClientLifecycleState::Offline)
    );

    assert_eq!(
        reader
            .get("inactive-primary-key")
            .expect("reader get should succeed via live secondary"),
        b"inactive-primary-payload"
    );

    let repaired = reader
        .query_route("inactive-primary-key")
        .expect("repaired route query should succeed")
        .expect("repaired route should exist");
    assert_eq!(repaired.replicas.len(), 1);
    assert_eq!(repaired.replicas[0].owner, *store_b.runtime_id());
    assert_eq!(repaired.replicas[0].priority, 0);
}

#[test]
fn routed_read_fails_over_within_same_request_after_primary_transport_failure() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("transport-failed-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("transport-failed-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("transport-failed-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("transport-failed-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "transport-failed-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "transport-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "transport-failed-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "transport-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "transport-failed-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "transport-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 2)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "transport-failed-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "transport-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &writer, &reader]);

    writer
        .put_with_policy(
            "transport-failed-key",
            b"transport-failed-payload",
            &ReplicationPolicy::new()
                .replica_count(2)
                .prefer_local(false)
                .preferred_storage_owners([
                    store_a.runtime_id().storage_key(),
                    store_b.runtime_id().storage_key(),
                ]),
        )
        .expect("replicated put should succeed");

    let seeded = reader
        .query_route("transport-failed-key")
        .expect("seeded route query should succeed")
        .expect("seeded route should exist");
    assert_eq!(seeded.replicas[0].owner, *store_a.runtime_id());
    assert_eq!(seeded.replicas[1].owner, *store_b.runtime_id());
    assert_eq!(
        reader
            .runtime_state(store_a.runtime_id())
            .expect("runtime state query should succeed"),
        Some(ClientLifecycleState::Active)
    );

    {
        let mut state = store_a_transport.state.lock();
        let handle = state
            .segments_by_name
            .remove("transport-failed-a-segment")
            .expect("primary segment handle should exist");
        state.segments_by_handle.remove(&handle);
    }

    assert_eq!(
        reader
            .get("transport-failed-key")
            .expect("reader get should fail over to live secondary in the same request"),
        b"transport-failed-payload"
    );

    let repaired = reader
        .query_route("transport-failed-key")
        .expect("repaired route query should succeed")
        .expect("repaired route should exist");
    assert_eq!(repaired.replicas.len(), 1);
    assert_eq!(repaired.replicas[0].owner, *store_b.runtime_id());
    assert_eq!(repaired.replicas[0].priority, 0);
}

#[test]
fn batch_get_fails_over_within_same_request_after_primary_transport_failure() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("batch-transport-failed-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("batch-transport-failed-b-segment"));
    let writer_transport =
        Arc::new(store_a_transport.peer("batch-transport-failed-writer-segment"));
    let reader_transport =
        Arc::new(store_a_transport.peer("batch-transport-failed-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "batch-transport-failed-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "batch-transport-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "batch-transport-failed-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "batch-transport-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "batch-transport-failed-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "batch-transport-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 2)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "batch-transport-failed-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "batch-transport-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &writer, &reader]);

    for (key, payload) in [
        ("batch-transport-failed-key-a", b"batch-transport-payload-a".as_slice()),
        ("batch-transport-failed-key-b", b"batch-transport-payload-b".as_slice()),
    ] {
        writer
            .put_with_policy(
                key,
                payload,
                &ReplicationPolicy::new()
                    .replica_count(2)
                    .prefer_local(false)
                    .preferred_storage_owners([
                        store_a.runtime_id().storage_key(),
                        store_b.runtime_id().storage_key(),
                    ]),
            )
            .expect("replicated put should succeed");
    }

    {
        let mut state = store_a_transport.state.lock();
        let handle = state
            .segments_by_name
            .remove("batch-transport-failed-a-segment")
            .expect("primary segment handle should exist");
        state.segments_by_handle.remove(&handle);
    }

    let values = reader
        .batch_get(&[
            ObjectRef::new("batch-transport-failed-key-a"),
            ObjectRef::new("batch-transport-failed-key-b"),
        ])
        .expect("batch_get should fail over to surviving replicas");
    assert_eq!(
        values,
        vec![
            b"batch-transport-payload-a".to_vec(),
            b"batch-transport-payload-b".to_vec()
        ]
    );
}

#[test]
fn route_lookup_many_stays_ok_when_live_replica_survives_and_snapshot_converges() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("route-lookup-live-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("route-lookup-live-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("route-lookup-live-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("route-lookup-live-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let short_expiry_ms = now_ms().saturating_add(1_000);

    let store_a = StoreClientBuilder::new(metadata.clone(), "route-lookup-live-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "route-lookup-live-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(short_expiry_ms)
        .expect("store-a build should succeed");
    let mut store_b = StoreClientBuilder::new(metadata.clone(), "route-lookup-live-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "route-lookup-live-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(short_expiry_ms)
        .expect("store-b build should succeed");
    let mut writer = StoreClientBuilder::new(metadata.clone(), "route-lookup-live-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "route-lookup-live-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 2)
        .build(short_expiry_ms)
        .expect("writer build should succeed");
    let mut reader = StoreClientBuilder::new(metadata.clone(), "route-lookup-live-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "route-lookup-live-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(short_expiry_ms)
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &writer, &reader]);

    writer
        .put_with_policy(
            "route-lookup-live-key",
            b"route-lookup-live-payload",
            &ReplicationPolicy::new()
                .replica_count(2)
                .prefer_local(false)
                .preferred_storage_owners([
                    store_a.runtime_id().storage_key(),
                    store_b.runtime_id().storage_key(),
                ]),
        )
        .expect("replicated put should succeed");

    {
        let mut state = store_a_transport.state.lock();
        let handle = state
            .segments_by_name
            .remove("route-lookup-live-a-segment")
            .expect("primary segment handle should exist");
        state.segments_by_handle.remove(&handle);
    }

    assert_eq!(
        reader
            .get("route-lookup-live-key")
            .expect("first read should fail over to the surviving replica"),
        b"route-lookup-live-payload"
    );

    for _ in 0..3 {
        assert_eq!(
            reader
                .get("route-lookup-live-key")
                .expect("subsequent reads should keep using the surviving replica"),
            b"route-lookup-live-payload"
        );
    }

    sleep(Duration::from_millis(1_100));
    let refreshed_expiry = now_ms().saturating_add(30_000);
    store_b
        .heartbeat(refreshed_expiry)
        .expect("store-b heartbeat should extend its lease");
    writer
        .heartbeat(refreshed_expiry)
        .expect("writer heartbeat should extend its lease");
    reader
        .heartbeat(refreshed_expiry)
        .expect("reader heartbeat should extend its lease");
    sleep(fast_live_client_sync_interval() * 3);

    assert_eq!(
        reader
            .get("route-lookup-live-key")
            .expect("read should still succeed after live snapshot drops the dead lease"),
        b"route-lookup-live-payload"
    );

    let metrics = snapshot_metrics();
    let ok = metrics
        .iter()
        .find(|snapshot| snapshot.operation == "route_lookup_many" && snapshot.status == "ok")
        .expect("route lookup should stay healthy with a surviving replica");
    assert!(ok.calls_total >= 4);
    assert!(
        metrics
            .iter()
            .all(|snapshot| !(snapshot.operation == "route_lookup_many"
                && snapshot.status == "error")),
        "route lookup should not report persistent errors when a live replica still exists"
    );
}

#[test]
fn route_lookup_many_records_miss_after_killed_single_replica_is_quarantined() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_transport = Arc::new(TestTransport::new("route-lookup-miss-store-segment"));
    let writer_transport = Arc::new(store_transport.peer("route-lookup-miss-writer-segment"));
    let reader_transport = Arc::new(store_transport.peer("route-lookup-miss-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store = StoreClientBuilder::new(metadata.clone(), "route-lookup-miss-store")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "route-lookup-miss-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "route-lookup-miss-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "route-lookup-miss-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "route-lookup-miss-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "route-lookup-miss-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store
        .register_local_memory()
        .expect("store memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store, &writer, &reader]);

    writer
        .put_with_policy(
            "route-lookup-miss-key",
            b"route-lookup-miss-payload",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owner(store.runtime_id().storage_key()),
        )
        .expect("single-replica put should succeed");

    {
        let mut state = store_transport.state.lock();
        let handle = state
            .segments_by_name
            .remove("route-lookup-miss-store-segment")
            .expect("primary segment handle should exist");
        state.segments_by_handle.remove(&handle);
    }

    let first = reader.get("route-lookup-miss-key");
    assert!(matches!(
        first,
        Err(StoreError::NotFound(_))
            | Err(StoreError::Transport(_))
            | Err(StoreError::InvalidState(_))
    ));

    let second = reader.get("route-lookup-miss-key");
    assert!(matches!(second, Err(StoreError::NotFound(_))));

    let metrics = snapshot_metrics();
    let ok = metrics
        .iter()
        .find(|snapshot| snapshot.operation == "route_lookup_many" && snapshot.status == "ok")
        .expect("route lookup should record the initial successful resolve");
    assert!(ok.calls_total >= 1);
    let miss = metrics
        .iter()
        .find(|snapshot| snapshot.operation == "route_lookup_many" && snapshot.status == "miss")
        .expect("route lookup should record miss after the dead replica is quarantined");
    assert!(miss.calls_total >= 1);
    assert!(
        metrics
            .iter()
            .all(|snapshot| !(snapshot.operation == "route_lookup_many"
                && snapshot.status == "error")),
        "route lookup miss should not be counted as an error"
    );
}

#[test]
fn suspect_authority_quarantine_is_shared_across_clients() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("shared-suspect-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("shared-suspect-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("shared-suspect-writer-segment"));
    let reader_a_transport = Arc::new(store_a_transport.peer("shared-suspect-reader-a-segment"));
    let reader_b_transport = Arc::new(store_a_transport.peer("shared-suspect-reader-b-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "shared-suspect-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "shared-suspect-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "shared-suspect-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "shared-suspect-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "shared-suspect-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "shared-suspect-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 2)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader_a = StoreClientBuilder::new(metadata.clone(), "shared-suspect-reader-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "shared-suspect-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader-a build should succeed");
    let reader_b = StoreClientBuilder::new(metadata.clone(), "shared-suspect-reader-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "shared-suspect-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader-b build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader_a
        .register_local_memory()
        .expect("reader-a memory should register");
    reader_b
        .register_local_memory()
        .expect("reader-b memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &writer, &reader_a, &reader_b]);

    writer
        .put_with_policy(
            "shared-suspect-key",
            b"shared-suspect-payload",
            &ReplicationPolicy::new()
                .replica_count(2)
                .prefer_local(false)
                .preferred_storage_owners([
                    store_a.runtime_id().storage_key(),
                    store_b.runtime_id().storage_key(),
                ]),
        )
        .expect("replicated put should succeed");

    {
        let mut state = store_a_transport.state.lock();
        let handle = state
            .segments_by_name
            .remove("shared-suspect-a-segment")
            .expect("primary segment handle should exist");
        state.segments_by_handle.remove(&handle);
    }

    let first = reader_a.get("shared-suspect-key");
    assert!(
        matches!(
            first,
            Err(StoreError::NotFound(_))
                | Err(StoreError::Transport(_))
                | Err(StoreError::InvalidState(_))
        ),
        "first reader should discover the dead authority and quarantine it"
    );

    assert_eq!(
        reader_b
            .get("shared-suspect-key")
            .expect("second reader should reuse the shared quarantine and avoid a fresh dead-authority failure"),
        b"shared-suspect-payload"
    );
}

#[test]
fn routed_put_skips_transport_failed_storage_owner_and_uses_live_peer() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("put-failed-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("put-failed-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("put-failed-writer-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "put-failed-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "put-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "put-failed-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "put-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "put-failed-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "put-failed-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &writer]);

    let mut stale_lease = store_a.lease().clone();
    stale_lease.endpoints.labels.insert(
        control_address_label().to_string(),
        "127.0.0.1:1".to_string(),
    );
    metadata
        .upsert_client_lease(&stale_lease)
        .expect("stale lease update should succeed");
    sleep(fast_live_client_sync_interval() * 3);

    let dead_runtime = store_a.runtime_id().clone();
    let dead_storage_key = dead_runtime.storage_key();
    let live_runtime = store_b.runtime_id().clone();
    let live_storage_key = live_runtime.storage_key();

    let route = writer
        .put_with_policy(
            "put-failed-key",
            b"put-failed-payload",
            &ReplicationPolicy::new()
                .replica_count(1)
                .prefer_local(false)
                .preferred_storage_owners([dead_storage_key, live_storage_key]),
        )
        .expect("put should skip dead preferred owner");
    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner, live_runtime);
    assert_ne!(route.replicas[0].owner, dead_runtime);
    assert_eq!(
        writer
            .get("put-failed-key")
            .expect("writer get should succeed"),
        b"put-failed-payload"
    );
}

#[test]
fn embedded_wrh_query_route_repairs_from_old_authority_after_churn() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("repair-old-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("repair-old-b-segment"));
    let store_c_transport = Arc::new(store_a_transport.peer("repair-old-c-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("repair-old-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("repair-old-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "repair-old-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-scope")
        .label("route_weight", "0.01")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "repair-old-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "repair-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner.clone(), 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "repair-old-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "repair-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    writer
        .put_with_policy(
            "route-repair-old-authority",
            b"route-repair-payload",
            &ReplicationPolicy::new().prefer_local(false),
        )
        .expect("seed routed put should succeed");

    let namespace = metadata.route_namespace();
    let scoped_key = ObjectKey::new("default::route-repair-old-authority");
    let seed_route = reader
        .query_route("route-repair-old-authority")
        .expect("seed route query should succeed")
        .expect("seed route should exist");
    assert_eq!(seed_route.replicas[0].owner, *store_a.runtime_id());
    assert_eq!(
        crate::route_directory::authority_get(
            &namespace,
            &store_a.runtime_id().stable_id,
            &scoped_key,
        )
        .expect("old authority route should be readable"),
        Some(seed_route.clone())
    );
    assert!(
        metadata
            .get_object_route(&scoped_key)
            .expect("metadata query should succeed")
            .is_none(),
        "seed route should stay off metadata in embedded WRH mode"
    );

    let store_b = StoreClientBuilder::new(metadata.clone(), "repair-old-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-scope")
        .label("route_weight", "1000")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "repair-old-store-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-scope")
        .label("route_weight", "1000")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");

    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &writer, &reader]);

    assert!(crate::route_directory::authority_get(
        &namespace,
        &store_b.runtime_id().stable_id,
        &scoped_key,
    )
    .expect("store-b route query should succeed")
    .is_none());
    assert!(crate::route_directory::authority_get(
        &namespace,
        &store_c.runtime_id().stable_id,
        &scoped_key,
    )
    .expect("store-c route query should succeed")
    .is_none());

    let repaired_route = reader
        .query_route("route-repair-old-authority")
        .expect("repair route query should succeed")
        .expect("repaired route should exist");
    assert_eq!(repaired_route, seed_route);
    assert_eq!(
        crate::route_directory::authority_get(
            &namespace,
            &store_b.runtime_id().stable_id,
            &scoped_key,
        )
        .expect("store-b repaired route should be readable"),
        Some(seed_route.clone())
    );
    assert_eq!(
        crate::route_directory::authority_get(
            &namespace,
            &store_c.runtime_id().stable_id,
            &scoped_key,
        )
        .expect("store-c repaired route should be readable"),
        Some(seed_route.clone())
    );
    assert_eq!(
        reader
            .get("route-repair-old-authority")
            .expect("reader get should succeed after repair"),
        b"route-repair-payload"
    );
}

#[test]
fn embedded_wrh_query_route_repairs_from_metadata_after_authority_miss() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("repair-meta-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("repair-meta-b-segment"));
    let store_c_transport = Arc::new(store_a_transport.peer("repair-meta-c-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("repair-meta-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("repair-meta-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "repair-meta-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-scope")
        .label("route_weight", "0.01")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "repair-meta-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "repair-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "repair-meta-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "repair-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    writer
        .put_with_policy(
            "route-repair-metadata",
            b"route-repair-metadata-payload",
            &ReplicationPolicy::new().prefer_local(false),
        )
        .expect("seed routed put should succeed");

    let namespace = metadata.route_namespace();
    let scoped_key = ObjectKey::new("default::route-repair-metadata");
    let seed_route = reader
        .query_route("route-repair-metadata")
        .expect("seed route query should succeed")
        .expect("seed route should exist");
    assert_eq!(seed_route.replicas[0].owner, *store_a.runtime_id());
    metadata
        .compare_and_swap_object_route(&scoped_key, None, Some(&seed_route))
        .expect("metadata route insert should succeed");
    crate::route_directory::authority_replace(
        &namespace,
        &store_a.runtime_id().stable_id,
        &scoped_key,
        None,
    )
    .expect("old authority route removal should succeed");
    assert!(crate::route_directory::authority_get(
        &namespace,
        &store_a.runtime_id().stable_id,
        &scoped_key,
    )
    .expect("old authority route query should succeed")
    .is_none());

    let store_b = StoreClientBuilder::new(metadata.clone(), "repair-meta-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-scope")
        .label("route_weight", "1000")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "repair-meta-store-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-scope")
        .label("route_weight", "1000")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");

    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &writer, &reader]);

    let repaired_route = reader
        .query_route("route-repair-metadata")
        .expect("metadata repair route query should succeed")
        .expect("metadata repair route should exist");
    assert_eq!(repaired_route, seed_route);
    assert_eq!(
        crate::route_directory::authority_get(
            &namespace,
            &store_b.runtime_id().stable_id,
            &scoped_key,
        )
        .expect("store-b repaired route should be readable"),
        Some(seed_route.clone())
    );
    assert_eq!(
        crate::route_directory::authority_get(
            &namespace,
            &store_c.runtime_id().stable_id,
            &scoped_key,
        )
        .expect("store-c repaired route should be readable"),
        Some(seed_route.clone())
    );
    assert_eq!(
        reader
            .get("route-repair-metadata")
            .expect("reader get should succeed after metadata repair"),
        b"route-repair-metadata-payload"
    );
}

#[test]
fn embedded_wrh_query_route_repairs_divergent_authorities_from_old_authority() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("repair-stale-old-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("repair-stale-old-b-segment"));
    let store_c_transport = Arc::new(store_a_transport.peer("repair-stale-old-c-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("repair-stale-old-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("repair-stale-old-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "repair-stale-old-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-stale-scope")
        .label("route_weight", "0.000001")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "repair-stale-old-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "repair-stale-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner.clone(), 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "repair-stale-old-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "repair-stale-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    writer
        .put_with_policy(
            "route-repair-stale-old",
            b"route-repair-stale-old-v1",
            &ReplicationPolicy::new().prefer_local(false),
        )
        .expect("first routed put should succeed");
    let stale_route = reader
        .query_route("route-repair-stale-old")
        .expect("stale route query should succeed")
        .expect("stale route should exist");
    assert_eq!(stale_route.version, RouteVersion(1));

    writer
        .put_with_policy(
            "route-repair-stale-old",
            b"route-repair-stale-old-v2",
            &ReplicationPolicy::new().prefer_local(false),
        )
        .expect("second routed put should succeed");
    let fresh_route = reader
        .query_route("route-repair-stale-old")
        .expect("fresh route query should succeed")
        .expect("fresh route should exist");
    assert!(fresh_route.version > stale_route.version);
    assert_eq!(
        reader
            .get("route-repair-stale-old")
            .expect("reader should see fresh payload"),
        b"route-repair-stale-old-v2"
    );

    let namespace = metadata.route_namespace();
    let scoped_key = ObjectKey::new("default::route-repair-stale-old");

    let store_b = StoreClientBuilder::new(metadata.clone(), "repair-stale-old-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-stale-scope")
        .label("route_weight", "1000000")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "repair-stale-old-store-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-stale-scope")
        .label("route_weight", "1000000")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");

    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &writer, &reader]);

    let mut divergent_route = fresh_route.clone();
    divergent_route.replicas[0].owner =
        ClientRuntimeId::new("zzzz-divergent-old-owner", ClientEpoch(99));
    divergent_route.replicas[0].segment_name = SegmentName::new("zzzz-divergent-old-segment");
    divergent_route.replicas[0].offset = divergent_route.replicas[0].offset.saturating_add(1);
    divergent_route.replicas[0].segment_offset =
        divergent_route.replicas[0].segment_offset.saturating_add(1);

    crate::route_directory::authority_replace(
        &namespace,
        &store_b.runtime_id().stable_id,
        &scoped_key,
        Some(&divergent_route),
    )
    .expect("store-b divergent route insert should succeed");
    crate::route_directory::authority_replace(
        &namespace,
        &store_c.runtime_id().stable_id,
        &scoped_key,
        Some(&divergent_route),
    )
    .expect("store-c divergent route insert should succeed");

    let repaired_route = reader
        .query_route("route-repair-stale-old")
        .expect("repaired route query should succeed")
        .expect("repaired route should exist");
    assert_eq!(repaired_route, fresh_route);
    let metrics = render_prometheus_metrics();
    assert!(metrics.contains("operation=\"route_repair_divergent_authority\",status=\"ok\""));
    assert_eq!(
        crate::route_directory::authority_get(
            &namespace,
            &store_b.runtime_id().stable_id,
            &scoped_key,
        )
        .expect("store-b repaired route should be readable"),
        Some(fresh_route.clone())
    );
    assert_eq!(
        crate::route_directory::authority_get(
            &namespace,
            &store_c.runtime_id().stable_id,
            &scoped_key,
        )
        .expect("store-c repaired route should be readable"),
        Some(fresh_route.clone())
    );
    assert_eq!(
        reader
            .get("route-repair-stale-old")
            .expect("reader get should succeed after divergent-authority repair"),
        b"route-repair-stale-old-v2"
    );
}

#[test]
fn embedded_wrh_query_route_repairs_divergent_authorities_from_metadata() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("repair-stale-meta-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("repair-stale-meta-b-segment"));
    let store_c_transport = Arc::new(store_a_transport.peer("repair-stale-meta-c-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("repair-stale-meta-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("repair-stale-meta-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "repair-stale-meta-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-stale-scope")
        .label("route_weight", "0.000001")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "repair-stale-meta-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "repair-stale-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "repair-stale-meta-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .label("route_scope", "repair-stale-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    writer
        .put_with_policy(
            "route-repair-stale-metadata",
            b"route-repair-stale-metadata-v1",
            &ReplicationPolicy::new().prefer_local(false),
        )
        .expect("first routed put should succeed");
    let stale_route = reader
        .query_route("route-repair-stale-metadata")
        .expect("stale route query should succeed")
        .expect("stale route should exist");
    assert_eq!(stale_route.version, RouteVersion(1));

    writer
        .put_with_policy(
            "route-repair-stale-metadata",
            b"route-repair-stale-metadata-v2",
            &ReplicationPolicy::new().prefer_local(false),
        )
        .expect("second routed put should succeed");
    let fresh_route = reader
        .query_route("route-repair-stale-metadata")
        .expect("fresh route query should succeed")
        .expect("fresh route should exist");
    assert!(fresh_route.version > stale_route.version);

    let namespace = metadata.route_namespace();
    let scoped_key = ObjectKey::new("default::route-repair-stale-metadata");
    metadata
        .compare_and_swap_object_route(&scoped_key, None, Some(&fresh_route))
        .expect("metadata fresh route insert should succeed");
    crate::route_directory::authority_replace(
        &namespace,
        &store_a.runtime_id().stable_id,
        &scoped_key,
        None,
    )
    .expect("old authority route removal should succeed");

    let store_b = StoreClientBuilder::new(metadata.clone(), "repair-stale-meta-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-stale-scope")
        .label("route_weight", "1000000")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "repair-stale-meta-store-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "repair-stale-scope")
        .label("route_weight", "1000000")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");

    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &writer, &reader]);

    let mut divergent_route = fresh_route.clone();
    divergent_route.replicas[0].owner =
        ClientRuntimeId::new("zzzz-divergent-metadata-owner", ClientEpoch(99));
    divergent_route.replicas[0].segment_name = SegmentName::new("zzzz-divergent-metadata-segment");
    divergent_route.replicas[0].offset = divergent_route.replicas[0].offset.saturating_add(1);
    divergent_route.replicas[0].segment_offset =
        divergent_route.replicas[0].segment_offset.saturating_add(1);

    crate::route_directory::authority_replace(
        &namespace,
        &store_b.runtime_id().stable_id,
        &scoped_key,
        Some(&divergent_route),
    )
    .expect("store-b divergent route insert should succeed");
    crate::route_directory::authority_replace(
        &namespace,
        &store_c.runtime_id().stable_id,
        &scoped_key,
        Some(&divergent_route),
    )
    .expect("store-c divergent route insert should succeed");

    let repaired_route = reader
        .query_route("route-repair-stale-metadata")
        .expect("repaired route query should succeed")
        .expect("repaired route should exist");
    assert_eq!(repaired_route, fresh_route);
    let metrics = render_prometheus_metrics();
    assert!(metrics.contains("operation=\"route_repair_divergent_authority\",status=\"ok\""));
    assert_eq!(
        crate::route_directory::authority_get(
            &namespace,
            &store_b.runtime_id().stable_id,
            &scoped_key,
        )
        .expect("store-b repaired route should be readable"),
        Some(fresh_route.clone())
    );
    assert_eq!(
        crate::route_directory::authority_get(
            &namespace,
            &store_c.runtime_id().stable_id,
            &scoped_key,
        )
        .expect("store-c repaired route should be readable"),
        Some(fresh_route.clone())
    );
    assert_eq!(
        reader
            .get("route-repair-stale-metadata")
            .expect("reader get should succeed after metadata stale repair"),
        b"route-repair-stale-metadata-v2"
    );
}

#[test]
fn embedded_wrh_route_directory_ignores_pool_boundaries_by_default() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("cross-pool-segment"));
    let writer = StoreClientBuilder::new(metadata.clone(), "writer-cross-pool")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-reclaim")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "reader-cross-pool")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    wait_for_membership_convergence(&[&writer, &reader]);

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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let router = StoreClientBuilder::new(metadata.clone(), "router-hot-path")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    router
        .register_local_memory()
        .expect("router memory should register");

    let reader = StoreClientBuilder::new(metadata.clone(), "reader-hot-path")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    wait_for_membership_convergence(&[&storage, &router, &reader]);

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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let router = StoreClientBuilder::new(metadata.clone(), "router-hot-batch")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    router
        .register_local_memory()
        .expect("router memory should register");

    let reader = StoreClientBuilder::new(metadata.clone(), "reader-hot-batch")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    wait_for_membership_convergence(&[&storage, &router, &reader]);

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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-chunk")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport)
        .local_memory(storage_config_with_layout(4096, 8, 1))
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    wait_for_membership_convergence(&[&writer, &reader]);

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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-batch-direct")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport)
        .local_memory(storage_config_with_layout(4096, 4, 1))
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    wait_for_membership_convergence(&[&writer, &reader]);

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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "reader-subrange")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    wait_for_membership_convergence(&[&writer, &reader]);

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
        .build(test_future_expiry_ms())
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
        .build(test_future_expiry_ms())
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
        .build(test_future_expiry_ms())
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
        .build(test_future_expiry_ms())
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
fn storage_owner_clock_evicts_cold_local_replicas_before_hot_ones() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("clock-local-segment"));
    let client = StoreClientBuilder::new(metadata, "clock-local")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(32))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let hot = b"0123456789abcdef";
    let cold = b"fedcba9876543210";
    let fresh = b"abcdefghijklmnop";

    client
        .put("clock-hot", hot)
        .expect("hot put should succeed");
    client
        .put("clock-cold", cold)
        .expect("cold put should succeed");
    assert_eq!(
        client.get("clock-hot").expect("hot get should succeed"),
        hot
    );

    let route = client
        .put("clock-fresh", fresh)
        .expect("fresh put should trigger eviction and succeed");
    assert_eq!(route.replicas[0].owner, client.runtime_id().clone());
    assert!(client
        .query_route("clock-hot")
        .expect("hot route query should succeed")
        .is_some());
    assert!(client
        .query_route("clock-fresh")
        .expect("fresh route query should succeed")
        .is_some());
    assert!(client
        .query_route("clock-cold")
        .expect("cold route query should succeed")
        .is_none());
    assert_eq!(
        client
            .get("clock-hot")
            .expect("hot get should still succeed"),
        hot
    );
    assert_eq!(
        client
            .get("clock-fresh")
            .expect("fresh get should still succeed"),
        fresh
    );
    assert!(matches!(
        client.get("clock-cold"),
        Err(StoreError::NotFound(_))
    ));
}

#[test]
fn remote_hit_reports_drive_storage_owner_clock_eviction() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let storage_transport = Arc::new(TestTransport::new("clock-remote-storage-segment"));
    let router_transport = Arc::new(storage_transport.peer("clock-remote-router-segment"));

    let storage = StoreClientBuilder::new(metadata.clone(), "clock-remote-storage")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config_with_bytes(32))
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let router = StoreClientBuilder::new(metadata.clone(), "clock-remote-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(rw_only_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    router
        .register_local_memory()
        .expect("router memory should register");

    wait_for_membership_convergence(&[&storage, &router]);

    let hot = b"0123456789abcdef";
    let cold = b"fedcba9876543210";
    let fresh = b"abcdefghijklmnop";

    let hot_route = router
        .put("remote-clock-hot", hot)
        .expect("hot put should succeed");
    let cold_route = router
        .put("remote-clock-cold", cold)
        .expect("cold put should succeed");
    assert_eq!(hot_route.replicas[0].owner, storage.runtime_id().clone());
    assert_eq!(cold_route.replicas[0].owner, storage.runtime_id().clone());
    assert_eq!(
        router
            .get("remote-clock-hot")
            .expect("remote hot get should succeed"),
        hot
    );

    let fresh_route = router
        .put("remote-clock-fresh", fresh)
        .expect("fresh put should trigger remote eviction and succeed");
    assert_eq!(fresh_route.replicas[0].owner, storage.runtime_id().clone());
    assert!(router
        .query_route("remote-clock-hot")
        .expect("hot route query should succeed")
        .is_some());
    assert!(router
        .query_route("remote-clock-fresh")
        .expect("fresh route query should succeed")
        .is_some());
    assert!(router
        .query_route("remote-clock-cold")
        .expect("cold route query should succeed")
        .is_none());
    assert_eq!(
        router
            .get("remote-clock-hot")
            .expect("hot object should survive eviction"),
        hot
    );
    assert_eq!(
        router
            .get("remote-clock-fresh")
            .expect("fresh object should be readable"),
        fresh
    );
    assert!(matches!(
        router.get("remote-clock-cold"),
        Err(StoreError::NotFound(_))
    ));
}

#[test]
fn background_watermark_eviction_reclaims_without_front_path_pressure() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("background-evict-segment"));
    let client = StoreClientBuilder::new(metadata, "background-evict")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_background_eviction(100, 60, 20))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    client
        .put("background-a", &[1u8; 20])
        .expect("put a should succeed");
    client
        .put("background-b", &[2u8; 20])
        .expect("put b should succeed");
    client
        .put("background-c", &[3u8; 20])
        .expect("put c should succeed");

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let used = client
            .list_segments()
            .expect("segment list should succeed")
            .into_iter()
            .map(|segment| segment.used_bytes)
            .sum::<u64>();
        if used <= 20 {
            break;
        }
        sleep(Duration::from_millis(20));
    }

    let used = client
        .list_segments()
        .expect("segment list should succeed")
        .into_iter()
        .map(|segment| segment.used_bytes)
        .sum::<u64>();
    assert!(
        used <= 20,
        "background watermark eviction should reclaim down to the low watermark; used={used}"
    );

    let metrics = snapshot_metrics();
    let background = metrics
        .iter()
        .find(|snapshot| {
            snapshot.operation == "storage_owner_background_eviction" && snapshot.status == "ok"
        })
        .expect("background eviction metric should be recorded");
    assert!(background.calls_total >= 1);
    assert!(background.bytes_out_total >= 1);

    let evict_one = metrics
        .iter()
        .find(|snapshot| snapshot.operation == "storage_owner_evict_one" && snapshot.status == "ok")
        .expect("per-eviction metric should be recorded");
    assert!(evict_one.calls_total >= 1);
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
        .build(test_future_expiry_ms())
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
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(8))
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
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
fn routed_batch_put_ignores_local_segments_when_storage_role_is_disabled() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("router-local-disabled-segment"));
    let remote_owner = publish_storage_node_with_capacity(
        metadata.as_ref(),
        transport.as_ref(),
        "storage-batch-disabled",
        "seg-batch-disabled",
        "pool-a",
        1024,
        1,
    );
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let client = StoreClientBuilder::new(metadata, "router-local-disabled")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(transport)
        .local_memory(storage_config_with_bytes(8))
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("client build should succeed");

    let routes = client
        .batch_put(&[
            PutRequest::new("batch-remote-a", b"abcdefgh"),
            PutRequest::new("batch-remote-b", b"ijklmnop"),
        ])
        .expect("batch put should succeed");

    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0].replicas[0].owner, remote_owner);
    assert_eq!(routes[1].replicas[0].owner, remote_owner);
    assert_eq!(
        client
            .get("batch-remote-a")
            .expect("first batch get should succeed"),
        b"abcdefgh"
    );
    assert_eq!(
        client
            .get("batch-remote-b")
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
        .build(test_future_expiry_ms())
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
fn helper_primitives_and_request_builders_cover_contracts() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();

    let object = ObjectRef::new("alpha").tenant("tenant-a");
    assert_eq!(object.tenant, Some("tenant-a"));
    assert_eq!(object.key, "alpha");

    let policy = ReplicationPolicy::new()
        .replica_count(2)
        .with_soft_pin(true)
        .prefer_alloc_in_same_node(true)
        .prefer_local(false)
        .preferred_segment("seg-a")
        .preferred_segments(["seg-b", "seg-c"])
        .preferred_storage_owner("owner-a")
        .preferred_storage_owners(["owner-b", "owner-c"]);
    assert_eq!(policy.replica_count, Some(2));
    assert!(policy.with_soft_pin);
    assert!(policy.prefer_alloc_in_same_node);
    assert!(!policy.prefer_local);
    assert_eq!(
        policy.preferred_segments,
        vec![SegmentName::new("seg-b"), SegmentName::new("seg-c")]
    );
    assert_eq!(
        policy.preferred_storage_owners,
        vec!["owner-b".to_string(), "owner-c".to_string()]
    );

    let put = PutRequest::new("put-key", b"put-value")
        .tenant("tenant-a")
        .replication(policy.clone());
    assert_eq!(put.tenant, Some("tenant-a"));
    assert_eq!(put.key, "put-key");
    assert_eq!(put.value, b"put-value");
    assert_eq!(put.policy, Some(policy.clone()));

    let source = [1u8, 2, 3, 4];
    let put_from = PutFromRequest::new("from-key", source.as_ptr().cast(), source.len())
        .tenant("tenant-a")
        .replication(policy.clone());
    assert_eq!(put_from.tenant, Some("tenant-a"));
    assert_eq!(put_from.key, "from-key");
    assert_eq!(put_from.size, source.len());
    assert_eq!(put_from.policy, Some(policy.clone()));

    let slices = [b"left".as_slice(), b"-right".as_slice()];
    let multi_put = MultiBufferPutRequest::new("multi-put", &slices)
        .tenant("tenant-a")
        .replication(policy);
    assert_eq!(multi_put.tenant, Some("tenant-a"));
    assert_eq!(multi_put.key, "multi-put");
    assert_eq!(multi_put.buffers.len(), 2);
    assert!(multi_put.policy.is_some());

    let mut get_buffer = [0u8; 8];
    let get = GetRequest::new("get-key", &mut get_buffer).tenant("tenant-a");
    assert_eq!(get.tenant, Some("tenant-a"));
    assert_eq!(get.key, "get-key");
    assert_eq!(get.buffer.len(), 8);

    let mut shard_a = [0u8; 2];
    let mut shard_b = [0u8; 4];
    let mut shard_refs: [&mut [u8]; 2] = [&mut shard_a, &mut shard_b];
    let multi_get = MultiBufferGetRequest::new("multi-get", &mut shard_refs).tenant("tenant-a");
    assert_eq!(multi_get.tenant, Some("tenant-a"));
    assert_eq!(multi_get.key, "multi-get");
    assert_eq!(multi_get.buffers.len(), 2);

    let payload = flatten_slices(&[b"hello".as_slice(), b"-world".as_slice()]);
    assert_eq!(payload, b"hello-world");

    let mut scatter_a = [0u8; 3];
    let mut scatter_b = [0u8; 4];
    let mut scatter_c = [0u8; 2];
    let mut scatter_refs: [&mut [u8]; 3] = [&mut scatter_a, &mut scatter_b, &mut scatter_c];
    scatter_into_buffers(b"hello", &mut scatter_refs);
    assert_eq!(&scatter_a, b"hel");
    assert_eq!(&scatter_b, b"lo\0\0");
    assert_eq!(&scatter_c, b"\0\0");

    let mut region = [0u8; 8];
    copy_into_region(
        RegionAllocation {
            addr: region.as_mut_ptr().cast(),
        },
        b"rust",
    );
    assert_eq!(&region[..4], b"rust");

    let lease_a = ClientLease {
        runtime: ClientRuntimeId::new("compat-a", ClientEpoch(1)),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: 1,
    };
    let mut lease_b = lease_a.clone();
    assert!(compatibility_matches(&lease_a, &lease_b));
    lease_b.compatibility.transport_api_version += 1;
    assert!(!compatibility_matches(&lease_a, &lease_b));

    assert_eq!(control_bind_host(""), "127.0.0.1");
    assert_eq!(control_bind_host("0.0.0.0:7001"), "127.0.0.1");
    assert_eq!(control_bind_host("10.0.0.9:7001"), "10.0.0.9");
    assert_eq!(align_up_u64(17, 8), 24);
    assert_eq!(align_up_u64(64, 64), 64);
    assert!(now_ms() > 0);

    record_success_metric("helper_metric", 12, 34);
    let metrics = render_prometheus_metrics();
    assert!(metrics.contains("operation=\"helper_metric\",status=\"ok\""));
    assert!(metrics.contains("mooncake_store_client_operation_bytes_in_total"));

    let metadata = Arc::new(InMemoryMetadataBackend::new());
    assert!(matches!(
        StoreClientBuilder::new(metadata, "builder-tenant-empty")
            .tenant("")
            .build(test_future_expiry_ms()),
        Err(StoreError::InvalidState(_))
    ));

    let custom_compat = CompatibilityDescriptor {
        store_api_version: 7,
        ..CompatibilityDescriptor::default()
    };
    let builder_transport = Arc::new(TestTransport::new("builder-compat-segment"));
    let built = StoreClientBuilder::new(Arc::new(InMemoryMetadataBackend::new()), "builder-compat")
        .compatibility(custom_compat.clone())
        .route_control(RouteControlMode::MetadataOnly)
        .state(ClientLifecycleState::Active)
        .transport(builder_transport)
        .build(test_future_expiry_ms())
        .expect("builder with compatibility and route_control should succeed");
    assert_eq!(built.lease().compatibility, custom_compat);
}

#[test]
fn suspect_runtime_cache_requires_fresh_lease_before_recovery() {
    let runtime = ClientRuntimeId::new("suspect-runtime", ClientEpoch(1));
    let mut endpoints = ClientEndpointSet::default();
    endpoints.labels.insert(
        control_address_label().to_string(),
        "127.0.0.1:17001".to_string(),
    );
    let lease = ClientLease {
        runtime: runtime.clone(),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints,
        expires_at_ms: test_future_expiry_ms(),
    };
    let elapsed = Instant::now() - Duration::from_millis(1);
    let mut cache = SuspectRuntimeCache::default();
    cache.mark(runtime.clone(), elapsed, Some(&lease));
    cache.reconcile_with_leases(std::slice::from_ref(&lease));
    assert!(cache.contains(&runtime));

    let mut renewed = lease.clone();
    renewed.expires_at_ms += 1;
    cache.reconcile_with_leases(&[renewed]);
    assert!(!cache.contains(&runtime));

    let mut cache = SuspectRuntimeCache::default();
    cache.mark(runtime.clone(), elapsed, Some(&lease));
    let mut moved = lease.clone();
    moved.endpoints.labels.insert(
        control_address_label().to_string(),
        "127.0.0.1:17002".to_string(),
    );
    cache.reconcile_with_leases(&[moved]);
    assert!(!cache.contains(&runtime));
}

#[test]
fn cached_live_client_snapshot_filters_expired_runtimes_without_refresh() {
    let runtime_live = ClientRuntimeId::new("live-runtime", ClientEpoch(1));
    let runtime_dead = ClientRuntimeId::new("dead-runtime", ClientEpoch(1));
    let cache = Arc::new(Mutex::new(LiveClientCache::default()));
    cache.lock().store(vec![
        ClientLease {
            runtime: runtime_live.clone(),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: test_future_expiry_ms(),
        },
        ClientLease {
            runtime: runtime_dead,
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: now_ms().saturating_sub(1),
        },
    ]);

    let snapshot =
        cached_live_client_snapshot(&cache).expect("cached live-client snapshot should exist");

    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].runtime, runtime_live);
}

#[test]
fn builder_rejects_route_topk_below_two_before_runtime_start() {
    let result = StoreClientBuilder::new(Arc::new(InMemoryMetadataBackend::new()), "builder-topk")
        .route_topk(1)
        .build(test_future_expiry_ms());
    assert!(matches!(result, Err(StoreError::InvalidState(_))));
}

#[test]
fn bootstrap_route_policy_bootstraps_once_and_rejects_mismatch() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let writer = ClientLease {
        runtime: ClientRuntimeId::new("writer", ClientEpoch(1)),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: test_future_expiry_ms(),
    };
    bootstrap_route_policy(metadata.as_ref(), &writer, RouteControlMode::EmbeddedWrh, 3)
        .expect("initial route policy bootstrap should succeed");
    let stored = metadata
        .get_route_policy(&RoutePolicyDomain::Default)
        .expect("route policy get should succeed")
        .expect("route policy should exist");
    assert_eq!(stored.route_topk, 3);
    assert_eq!(stored.route_control, RouteControlMode::EmbeddedWrh);

    let follower = ClientLease {
        runtime: ClientRuntimeId::new("reader", ClientEpoch(1)),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: test_future_expiry_ms(),
    };
    bootstrap_route_policy(
        metadata.as_ref(),
        &follower,
        RouteControlMode::EmbeddedWrh,
        3,
    )
    .expect("matching route policy should pass");
    let error = bootstrap_route_policy(
        metadata.as_ref(),
        &follower,
        RouteControlMode::EmbeddedWrh,
        4,
    )
    .expect_err("mismatched route_topk should fail");
    assert!(matches!(error, StoreError::InvalidState(_)));
}

#[test]
fn compatibility_facade_surface_covers_aliases_and_buffers() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("facade-segment"));
    let mut client = StoreClientBuilder::new(metadata.clone(), "facade-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:7011")
        .transport(transport.clone())
        .transport_factory(transport.factory())
        .local_memory(storage_config_with_bytes(512))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");

    client
        .register_local_memory()
        .expect("local memory should register");
    client
        .mount_segment(512, 0, vec!["dram".to_string()])
        .expect("mount_segment should refresh the primary announcement");
    assert_eq!(
        client.get_hostname().expect("hostname should exist"),
        "127.0.0.1:7011"
    );
    assert!(matches!(
        client.expand_local_memory(0),
        Err(StoreError::Allocator(_))
    ));

    client.heartbeat(20_000).expect("heartbeat should succeed");
    client.enter_standby().expect("standby should succeed");
    client.activate().expect("activate should succeed");
    let handoff = client
        .plan_handoff(ClientEpoch(2), HandoffKind::HotUpgrade, 9, 100, Some(200))
        .expect("handoff should be planned");
    assert_eq!(handoff.to.epoch, ClientEpoch(2));

    let primary = client.segment_name().expect("primary segment should exist");
    assert!(matches!(
        client.retire_segment(&primary),
        Err(StoreError::InvalidState(_))
    ));

    let expanded = client
        .expand_local_memory(128)
        .expect("local memory expansion should succeed");
    let listed = client.list_segments().expect("segment list should succeed");
    assert!(listed.iter().any(|segment| segment.segment_name == primary));
    assert!(listed
        .iter()
        .any(|segment| segment.segment_name == expanded.segment_name));
    client
        .drain_segment(&expanded.segment_name)
        .expect("drain should succeed");
    assert!(client
        .retire_segment(&expanded.segment_name)
        .expect("retire should succeed"));

    let plain = client.put("plain", b"hello").expect("put should succeed");
    assert_eq!(client.get("plain").expect("get should succeed"), b"hello");
    let tenant_plain = client
        .put_in_tenant("tenant-b", "plain", b"world")
        .expect("tenant put should succeed");
    assert_eq!(
        client
            .get_in_tenant("tenant-b", "plain")
            .expect("tenant get should succeed"),
        b"world"
    );

    let policy = ReplicationPolicy::new()
        .replica_count(1)
        .with_soft_pin(true)
        .prefer_local(true);
    client
        .put_with_policy("policy", b"policy!", &policy)
        .expect("policy put should succeed");
    client
        .put_in_tenant_with_policy("tenant-b", "policy", b"tenant-policy", &policy)
        .expect("tenant policy put should succeed");

    let mut source = [0u8; 128];
    source[..5].copy_from_slice(b"from!");
    source[16..22].copy_from_slice(b"tenant");
    source[32..39].copy_from_slice(b"policy!");
    source[48..56].copy_from_slice(b"tpolicy!");
    source[64..69].copy_from_slice(b"batch");
    source[80..86].copy_from_slice(b"buffer");
    assert!(matches!(
        client.put_from("null", ptr::null(), 4),
        Err(StoreError::Allocator(_))
    ));
    let outside = [7u8; 4];
    assert!(matches!(
        client.put_from("outside", outside.as_ptr().cast(), outside.len()),
        Err(StoreError::Allocator(_))
    ));
    assert!(matches!(
        client.register_buffer(source.as_mut_ptr().cast(), 0),
        Err(StoreError::Allocator(_))
    ));
    client
        .register_buffer(source.as_mut_ptr().cast(), source.len())
        .expect("buffer register should succeed");
    client
        .put_from("from", source.as_ptr().cast(), 5)
        .expect("put_from should succeed");
    client
        .put_from_in_tenant(
            "tenant-b",
            "from",
            unsafe { source.as_ptr().add(16).cast() },
            6,
        )
        .expect("tenant put_from should succeed");
    client
        .put_from_with_policy(
            "from-policy",
            unsafe { source.as_ptr().add(32).cast() },
            7,
            &policy,
        )
        .expect("policy put_from should succeed");
    client
        .put_from_in_tenant_with_policy(
            "tenant-b",
            "from-policy",
            unsafe { source.as_ptr().add(48).cast() },
            8,
            &policy,
        )
        .expect("tenant policy put_from should succeed");

    let batch_routes = client
        .batch_put(&[
            PutRequest::new("batch-a", b"alpha"),
            PutRequest::new("batch-b", b"bravo")
                .tenant("tenant-b")
                .replication(ReplicationPolicy::new().replica_count(1)),
        ])
        .expect("batch_put should succeed");
    assert_eq!(batch_routes.len(), 2);

    let batch_from_routes = client
        .batch_put_from(&[
            PutFromRequest::new("batch-from-a", unsafe { source.as_ptr().add(64).cast() }, 5),
            PutFromRequest::new("batch-from-b", unsafe { source.as_ptr().add(80).cast() }, 6)
                .tenant("tenant-b"),
        ])
        .expect("batch_put_from should succeed");
    assert_eq!(batch_from_routes.len(), 2);

    let slices_a = [b"multi".as_slice(), b"-a".as_slice()];
    let slices_b = [b"multi".as_slice(), b"-tenant".as_slice()];
    let multi_routes = client
        .batch_put_from_multi_buffers(&[
            MultiBufferPutRequest::new("multi-a", &slices_a),
            MultiBufferPutRequest::new("multi-b", &slices_b)
                .tenant("tenant-b")
                .replication(ReplicationPolicy::new().replica_count(1)),
        ])
        .expect("multi-buffer put should succeed");
    assert_eq!(multi_routes.len(), 2);

    assert_eq!(
        client.get_size("plain").expect("plain size should exist"),
        plain.replicas[0].length as usize
    );
    assert_eq!(
        client
            .get_size_in_tenant("tenant-b", "plain")
            .expect("tenant size should exist"),
        tenant_plain.replicas[0].length as usize
    );
    assert!(client.is_exist("plain").expect("plain should exist"));
    assert!(client
        .is_exist_in_tenant("tenant-b", "plain")
        .expect("tenant plain should exist"));
    assert_eq!(
        client
            .batch_is_exist(&[
                ObjectRef::new("plain"),
                ObjectRef::new("plain").tenant("tenant-b"),
                ObjectRef::new("missing"),
            ])
            .expect("batch exist should succeed"),
        vec![true, true, false]
    );

    let queried = client
        .query_route("plain")
        .expect("query should succeed")
        .expect("plain route should exist");
    assert_eq!(queried.key, client.scoped_key("tenant-a", "plain"));
    let queried_tenant = client
        .query_route_in_tenant("tenant-b", "plain")
        .expect("tenant query should succeed")
        .expect("tenant route should exist");
    assert_eq!(queried_tenant.key, client.scoped_key("tenant-b", "plain"));

    let mut direct_insert = queried.clone();
    direct_insert.key = client.scoped_key("tenant-a", "cas-direct");
    direct_insert.version = queried.version.next();
    assert!(
        client
            .cas_route("cas-direct", None, Some(&direct_insert))
            .expect("cas insert should succeed")
            .applied
    );
    assert!(
        client
            .cas_route("cas-direct", Some(direct_insert.version), None)
            .expect("cas delete should succeed")
            .applied
    );

    let mut tenant_insert = queried_tenant.clone();
    tenant_insert.key = client.scoped_key("tenant-b", "cas-tenant");
    tenant_insert.version = queried_tenant.version.next();
    assert!(
        client
            .cas_route_in_tenant("tenant-b", "cas-tenant", None, Some(&tenant_insert))
            .expect("tenant cas insert should succeed")
            .applied
    );

    let batch_get = client
        .batch_get(&[
            ObjectRef::new("plain"),
            ObjectRef::new("from"),
            ObjectRef::new("plain").tenant("tenant-b"),
        ])
        .expect("batch_get should succeed");
    assert_eq!(batch_get[0], b"hello");
    assert_eq!(batch_get[1], b"from!");
    assert_eq!(batch_get[2], b"world");

    let batch_get_buffer = client
        .batch_get_buffer(&[ObjectRef::new("policy"), ObjectRef::new("from-policy")])
        .expect("batch_get_buffer should succeed");
    assert_eq!(batch_get_buffer[0], b"policy!");
    assert_eq!(batch_get_buffer[1], b"policy!");

    let mut get_plain = [0u8; 8];
    let plain_size = client
        .get_into("plain", &mut get_plain)
        .expect("get_into should succeed");
    assert_eq!(plain_size, 5);
    assert_eq!(&get_plain[..plain_size], b"hello");

    let mut get_tenant = [0u8; 8];
    let tenant_size = client
        .get_into_in_tenant("tenant-b", "plain", &mut get_tenant)
        .expect("tenant get_into should succeed");
    assert_eq!(tenant_size, 5);
    assert_eq!(&get_tenant[..tenant_size], b"world");

    let mut batch_into_a = [0u8; 8];
    let mut batch_into_b = [0u8; 8];
    let batch_sizes = client
        .batch_get_into(&mut [
            GetRequest::new("from", &mut batch_into_a),
            GetRequest::new("from", &mut batch_into_b).tenant("tenant-b"),
        ])
        .expect("batch_get_into should succeed");
    assert_eq!(batch_sizes, vec![5, 6]);
    assert_eq!(&batch_into_a[..5], b"from!");
    assert_eq!(&batch_into_b[..6], b"tenant");

    let mut shard_left = [0u8; 3];
    let mut shard_right = [0u8; 4];
    let mut shard_refs: [&mut [u8]; 2] = [&mut shard_left, &mut shard_right];
    let multi_sizes = client
        .batch_get_into_multi_buffers(&mut [MultiBufferGetRequest::new("policy", &mut shard_refs)])
        .expect("multi-buffer get should succeed");
    assert_eq!(multi_sizes, vec![7]);
    assert_eq!(&shard_left, b"pol");
    assert_eq!(&shard_right, b"icy!");

    client
        .remove("plain", false)
        .expect("remove should succeed");
    client
        .remove_in_tenant("tenant-b", "plain", false)
        .expect("tenant remove should succeed");
    client
        .batch_remove(
            &[
                ObjectRef::new("batch-a"),
                ObjectRef::new("batch-b").tenant("tenant-b"),
                ObjectRef::new("batch-from-b").tenant("tenant-b"),
            ],
            false,
        )
        .expect("batch_remove should succeed");
    assert!(!client.is_exist("plain").expect("plain should be gone"));
    assert!(!client
        .is_exist_in_tenant("tenant-b", "plain")
        .expect("tenant plain should be gone"));

    client
        .unregister_buffer(source.as_mut_ptr().cast(), source.len())
        .expect("buffer unregister should succeed");
}

#[test]
fn local_control_plane_and_state_adapters_cover_single_and_batch_paths() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("adapter-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "adapter-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .transport(transport)
        .local_memory(storage_config_with_bytes(256))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");

    client
        .register_local_memory()
        .expect("local memory should register");
    let route = client
        .put_in_tenant("tenant-z", "alpha", b"payload")
        .expect("seed put should succeed");

    let namespace = metadata.route_namespace();
    let authority = client.runtime_id().stable_id.clone();
    let scoped_key = client.scoped_key("tenant-z", "alpha");

    let local_authority = LocalAuthorityAdapter;
    assert_eq!(
        local_authority
            .get_route(&namespace, &authority, &scoped_key)
            .expect("single authority get should succeed"),
        Some(route.clone())
    );
    assert_eq!(
        local_authority
            .batch_get_routes(
                &namespace,
                &authority,
                &[scoped_key.clone(), ObjectKey::new("tenant-z::missing")]
            )
            .len(),
        2
    );
    assert_eq!(
        local_authority
            .list_routes_by_replica_owner(&namespace, &authority, client.runtime_id())
            .expect("list by owner should succeed")
            .len(),
        1
    );

    let mut updated = route.clone();
    updated.version = route.version.next();
    assert!(
        local_authority
            .compare_and_swap_route(
                &namespace,
                &authority,
                &scoped_key,
                Some(route.version),
                Some(&updated),
            )
            .expect("single authority cas should succeed")
            .applied
    );
    let cas_many = local_authority.batch_compare_and_swap_routes(
        &namespace,
        &authority,
        &[
            RouteCasRequest {
                key: scoped_key.clone(),
                expected: Some(updated.version),
                next: Some(route.clone()),
            },
            RouteCasRequest {
                key: ObjectKey::new("tenant-z::ghost"),
                expected: Some(RouteVersion(999)),
                next: None,
            },
        ],
    );
    assert!(
        cas_many[0]
            .as_ref()
            .expect("first batch cas should decode")
            .applied
    );
    assert!(
        !cas_many[1]
            .as_ref()
            .expect("second batch cas should decode")
            .applied
    );

    let replace_key = ObjectKey::new("tenant-z::replaced");
    let mut replaced_route = route.clone();
    replaced_route.key = replace_key.clone();
    replaced_route.version = route.version.next();
    local_authority
        .replace_route(&namespace, &authority, &replace_key, Some(&replaced_route))
        .expect("single replace should succeed");
    let replaced = local_authority.batch_get_routes(
        &namespace,
        &authority,
        std::slice::from_ref(&replace_key),
    );
    assert!(replaced[0]
        .as_ref()
        .expect("replaced route should decode")
        .is_some());
    let replace_many = local_authority.batch_replace_routes(
        &namespace,
        &authority,
        &[RouteCasRequest {
            key: replace_key.clone(),
            expected: None,
            next: None,
        }],
    );
    assert!(replace_many[0].is_ok());

    let missing_authority = ClientStableId::new("missing-authority");
    let missing_get = local_authority.batch_get_routes(
        &namespace,
        &missing_authority,
        std::slice::from_ref(&scoped_key),
    );
    assert!(matches!(missing_get[0], Err(StoreError::NotFound(_))));
    let missing_cas = local_authority.batch_compare_and_swap_routes(
        &namespace,
        &missing_authority,
        &[RouteCasRequest {
            key: scoped_key.clone(),
            expected: None,
            next: Some(route.clone()),
        }],
    );
    assert!(matches!(missing_cas[0], Err(StoreError::NotFound(_))));
    let missing_replace = local_authority.batch_replace_routes(
        &namespace,
        &missing_authority,
        &[RouteCasRequest {
            key: scoped_key.clone(),
            expected: None,
            next: None,
        }],
    );
    assert!(matches!(missing_replace[0], Err(StoreError::NotFound(_))));

    let local_allocator = LocalAllocatorAdapter {
        runtime: client.runtime_id().clone(),
        allocator: client.allocator.clone(),
        storage_owner: client.storage_owner.clone(),
    };
    let primary = client.segment_name().expect("primary segment should exist");
    let single_reservation = local_allocator
        .reserve_specific(client.runtime_id(), &primary, 8)
        .expect("single reserve_specific should succeed");
    local_allocator
        .release(
            client.runtime_id(),
            &primary,
            single_reservation.offset_bytes,
            single_reservation.length_bytes,
        )
        .expect("single allocator release should succeed");
    let wrong_owner = ClientRuntimeId::new("wrong-owner", ClientEpoch(9));
    assert!(matches!(
        local_allocator.reserve_any(&wrong_owner, 8),
        Err(StoreError::InvalidState(_))
    ));
    assert!(matches!(
        local_allocator.reserve_specific(&wrong_owner, &primary, 8),
        Err(StoreError::InvalidState(_))
    ));
    assert!(matches!(
        local_allocator.release(&wrong_owner, &primary, 0, 8),
        Err(StoreError::InvalidState(_))
    ));

    let lease = client.lease().clone();
    let control = client.control_client.clone();
    let rpc_get = control
        .batch_get_routes(
            &lease,
            &namespace,
            &authority,
            std::slice::from_ref(&scoped_key),
        )
        .expect("rpc batch get should succeed");
    assert!(rpc_get[0]
        .as_ref()
        .expect("rpc route should decode")
        .is_some());

    let mut rpc_updated = route.clone();
    rpc_updated.version = route.version.next();
    let rpc_cas = control
        .batch_compare_and_swap_routes(
            &lease,
            &namespace,
            &authority,
            &[RouteCasRequest {
                key: scoped_key.clone(),
                expected: Some(route.version),
                next: Some(rpc_updated.clone()),
            }],
        )
        .expect("rpc batch cas should succeed");
    assert!(rpc_cas[0].as_ref().expect("rpc cas should decode").applied);
    let rpc_list = control
        .list_routes_by_replica_owner(&lease, &namespace, &authority, client.runtime_id())
        .expect("rpc list should succeed");
    assert_eq!(rpc_list.len(), 1);
    let rpc_replace = control
        .batch_replace_routes(
            &lease,
            &namespace,
            &authority,
            &[RouteCasRequest {
                key: scoped_key.clone(),
                expected: None,
                next: Some(route.clone()),
            }],
        )
        .expect("rpc batch replace should succeed");
    assert!(rpc_replace[0].is_ok());

    let rpc_reservations = control
        .batch_reserve_any(&lease, client.runtime_id(), &[8, 16])
        .expect("rpc reserve_any should succeed");
    let rpc_reservations = rpc_reservations
        .into_iter()
        .map(|result| result.expect("reserve_any item should succeed"))
        .collect::<Vec<_>>();
    let rpc_specific = control
        .batch_reserve_specific(
            &lease,
            client.runtime_id(),
            &[crate::control_plane::ReserveSpecificOp {
                segment_name: primary.clone(),
                length_bytes: 8,
            }],
        )
        .expect("rpc reserve_specific should succeed");
    let rpc_specific = rpc_specific[0]
        .as_ref()
        .expect("reserve_specific item should succeed")
        .clone();
    let release_results = control
        .batch_release(
            &lease,
            client.runtime_id(),
            &[
                ReleaseOp {
                    segment_name: rpc_reservations[0].segment_name.clone(),
                    offset_bytes: rpc_reservations[0].offset_bytes,
                    length_bytes: rpc_reservations[0].length_bytes,
                },
                ReleaseOp {
                    segment_name: rpc_reservations[1].segment_name.clone(),
                    offset_bytes: rpc_reservations[1].offset_bytes,
                    length_bytes: rpc_reservations[1].length_bytes,
                },
                ReleaseOp {
                    segment_name: rpc_specific.segment_name.clone(),
                    offset_bytes: rpc_specific.offset_bytes,
                    length_bytes: rpc_specific.length_bytes,
                },
            ],
        )
        .expect("rpc release should succeed");
    assert!(release_results.iter().all(|result| result.is_ok()));

    let wrong_reserve = control
        .batch_reserve_any(&lease, &wrong_owner, &[8])
        .expect("wrong-owner reserve should still reply");
    assert!(matches!(wrong_reserve[0], Err(StoreError::InvalidState(_))));
    let wrong_release = control
        .batch_release(
            &lease,
            &wrong_owner,
            &[ReleaseOp {
                segment_name: primary.clone(),
                offset_bytes: 0,
                length_bytes: 8,
            }],
        )
        .expect("wrong-owner release should still reply");
    assert!(matches!(wrong_release[0], Err(StoreError::InvalidState(_))));
    let missing_rpc_get = control
        .batch_get_routes(
            &lease,
            &namespace,
            &missing_authority,
            std::slice::from_ref(&scoped_key),
        )
        .expect("missing authority get should still reply");
    assert!(matches!(missing_rpc_get[0], Err(StoreError::NotFound(_))));
}

#[test]
fn runtime_alloc_helper_methods_cover_empty_batches_and_mismatch_paths() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("alloc-helper-segment"));
    let client = StoreClientBuilder::new(metadata, "alloc-helper-client")
        .tenant("tenant-a")
        .state(ClientLifecycleState::Active)
        .transport(transport.clone())
        .transport_factory(transport.factory())
        .local_memory(storage_config_with_bytes(256))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");

    client
        .register_local_memory()
        .expect("local memory should register");

    assert_eq!(client.default_replica_count(), 1);
    let _planner = client.request_placement_planner();
    assert_eq!(
        client.scoped_key("tenant-a", "alpha"),
        ObjectKey::new("tenant-a::alpha")
    );
    assert_eq!(
        client
            .transport()
            .expect("transport should exist")
            .segment_name()
            .expect("transport segment should exist"),
        "alloc-helper-segment"
    );
    assert_eq!(
        client
            .transport_factory()
            .expect("transport factory should exist")
            .create("alloc-helper-factory")
            .expect("factory transport should build")
            .segment_name()
            .expect("factory segment should exist"),
        "alloc-helper-factory"
    );

    let empty_reservations = client
        .reserve_storage_runtime_segments_batch(&[])
        .expect("empty reserve batch should succeed");
    assert!(empty_reservations.is_empty());
    client
        .release_segment_allocations_batch(&[])
        .expect("empty release batch should succeed");
    client
        .flush_due_reclaims()
        .expect("flush_due_reclaims should accept empty queue");
    client
        .flush_all_reclaims()
        .expect("flush_all_reclaims should accept empty queue");

    let primary = client.segment_name().expect("primary segment should exist");
    let (_target, reservation) = client
        .reserve_specific_segment(client.runtime_id(), &primary, 8, true)
        .expect("reserve_specific_segment should succeed");
    client
        .release_reserved_allocations(
            &[ReplicaWriteTarget {
                storage_runtime: client.runtime_id().clone(),
                segment_name: primary.clone(),
            }],
            std::slice::from_ref(&reservation),
        )
        .expect("release_reserved_allocations should succeed");
    assert!(matches!(
        client.release_reserved_allocations(
            &[ReplicaWriteTarget {
                storage_runtime: client.runtime_id().clone(),
                segment_name: primary,
            }],
            &[],
        ),
        Err(StoreError::InvalidState(_))
    ));
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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "shrink-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "shrink-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    let reader = StoreClientBuilder::new(metadata, "shrink-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
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

    wait_for_membership_convergence(&[&store_a, &store_b, &router, &reader]);

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
fn true_client_shrink_ignores_unreachable_route_authority() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("shrink-dead-auth-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("shrink-dead-auth-b-segment"));
    let router_transport = Arc::new(store_a_transport.peer("shrink-dead-auth-router-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("shrink-dead-auth-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut store_a = StoreClientBuilder::new(metadata.clone(), "shrink-dead-auth-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "shrink-dead-auth-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "shrink-dead-auth-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route_scope", "shrink-dead-auth-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "shrink-dead-auth-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route_scope", "shrink-dead-auth-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "shrink-dead-auth-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route_scope", "shrink-dead-auth-scope")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
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

    wait_for_membership_convergence(&[&store_a, &store_b, &router, &reader]);

    let mut expected = BTreeMap::new();
    let mut owned_keys = Vec::new();
    for index in 0..32 {
        let key = format!("shrink-dead-auth-key-{index}");
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

    let mut endpoints = ClientEndpointSet {
        rpc_address: "127.0.0.1:1".to_string(),
        segment_name: Some(SegmentName::new("shrink-dead-auth-dead-segment")),
        labels: BTreeMap::new(),
    };
    endpoints
        .labels
        .insert("pool".to_string(), "pool-a".to_string());
    endpoints
        .labels
        .insert("storage".to_string(), "false".to_string());
    endpoints
        .labels
        .insert("route".to_string(), "true".to_string());
    endpoints.labels.insert(
        "route_scope".to_string(),
        "shrink-dead-auth-scope".to_string(),
    );
    endpoints.labels.insert(
        control_address_label().to_string(),
        "127.0.0.1:1".to_string(),
    );
    metadata
        .upsert_client_lease(&ClientLease {
            runtime: ClientRuntimeId::new("shrink-dead-auth-dead", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: router.lease().compatibility.clone(),
            endpoints,
            expires_at_ms: test_future_expiry_ms(),
        })
        .expect("dead authority lease should publish");

    let migrated = store_a
        .evacuate_owned_replicas()
        .expect("true client shrink should ignore unreachable mirrored authorities");
    assert_eq!(migrated, owned_keys.len());

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
    let _guard = metrics_test_lock().lock();
    reset_metrics();
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
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "writer-via-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let router = StoreClientBuilder::new(metadata.clone(), "writer-via-router")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(router_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("router build should succeed");
    let reader = StoreClientBuilder::new(metadata, "writer-via-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
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
    store_a
        .mount_segment(4096, 0, vec!["dram".to_string()])
        .expect("admin mount should succeed");

    wait_for_membership_convergence(&[&store_a, &store_b, &router, &reader]);

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

    let metrics = render_prometheus_metrics();
    assert!(metrics.contains(
        "mooncake_store_segment_lifecycle_total{action=\"mount_segment\",result=\"ok\"}"
    ));
    assert!(metrics.contains(
        "mooncake_store_segment_lifecycle_total{action=\"retire_segment\",result=\"ok\"}"
    ));
    assert!(
        metrics.contains("mooncake_store_rebalance_routes_total{phase=\"migrate\",result=\"ok\"}")
    );
    assert!(metrics.contains("mooncake_store_rebalance_bytes_total{phase=\"migrate\"}"));
}

#[test]
fn hot_upgrade_evacuation_pins_owned_routes_to_successor() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let predecessor_transport = Arc::new(TestTransport::new("pin-upgrade-old-segment"));
    let predecessor_factory = predecessor_transport.factory();
    let successor_transport = Arc::new(predecessor_transport.peer("pin-upgrade-new-segment"));
    let spare_transport = Arc::new(predecessor_transport.peer("pin-upgrade-spare-segment"));
    let reader_transport = Arc::new(predecessor_transport.peer("pin-upgrade-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let mut predecessor = StoreClientBuilder::new(metadata.clone(), "pin-upgrade-store")
        .epoch(ClientEpoch(1))
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(predecessor_transport)
        .transport_factory(predecessor_factory)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("predecessor build should succeed");
    let mut successor = StoreClientBuilder::new(metadata.clone(), "pin-upgrade-store")
        .epoch(ClientEpoch(2))
        .state(ClientLifecycleState::Standby)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(successor_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("successor build should succeed");
    let spare = StoreClientBuilder::new(metadata.clone(), "pin-upgrade-spare")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .transport(spare_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("spare build should succeed");
    let reader = StoreClientBuilder::new(metadata, "pin-upgrade-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .transport(reader_transport)
        .local_memory(storage_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    predecessor
        .register_local_memory()
        .expect("predecessor memory should register");
    successor
        .register_local_memory()
        .expect("successor memory should register");
    spare
        .register_local_memory()
        .expect("spare memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");

    predecessor
        .put("pin-upgrade-key", b"pin-upgrade-payload")
        .expect("seed put should succeed");
    let initial = predecessor
        .query_route("pin-upgrade-key")
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    assert!(initial
        .replicas
        .iter()
        .any(|replica| replica.owner == *predecessor.runtime_id()));

    predecessor
        .enter_draining()
        .expect("predecessor should drain");
    predecessor
        .plan_handoff(
            ClientEpoch(2),
            HandoffKind::HotUpgrade,
            1,
            100,
            Some(u64::MAX),
        )
        .expect("handoff planning should succeed");
    successor
        .activate_if_targeted_handoff()
        .expect("successor activation should succeed")
        .expect("targeted handoff should be visible");

    let successor_runtime = successor.runtime_id().clone();
    let migrated = predecessor
        .evacuate_owned_replicas_to_runtime(&successor_runtime)
        .expect("targeted hot-upgrade evacuation should succeed");
    assert_eq!(migrated, 1);
    assert!(
        predecessor
            .list_segments()
            .expect("segment listing should succeed")
            .is_empty(),
        "predecessor segments should retire after migration"
    );

    let route = reader
        .query_route("pin-upgrade-key")
        .expect("post-upgrade route query should succeed")
        .expect("post-upgrade route should exist");
    assert!(route
        .replicas
        .iter()
        .all(|replica| replica.owner != *predecessor.runtime_id()));
    assert!(route
        .replicas
        .iter()
        .any(|replica| replica.owner == successor_runtime));
    assert_eq!(
        reader
            .get("pin-upgrade-key")
            .expect("reader get should succeed"),
        b"pin-upgrade-payload"
    );
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
        storage_owner: test_storage_owner_state(&owner, shared_allocator.clone()),
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
