#[cfg(any(test, feature = "test-support"))]
use std::sync::OnceLock;

#[cfg(any(test, feature = "test-support"))]
pub fn env_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::net::TcpListener;
use std::ptr;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use mooncake_store_client::{
    LocalMemoryConfig, PlacementPlanner, RouteControlMode, StoreClient, StoreClientBuilder,
    StoreTransport,
};
use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
    CompatibilityDescriptor, HandoffPlan, MetadataBackend, ObjectKey, ObjectRoute, RoutePolicy,
    RoutePolicyDomain, RouteVersion, SegmentAnnouncement, SegmentLifecycleState, SegmentName,
    SegmentReservation, StoreError, TenantObjectAccounting, TenantPolicy, TenantPolicyScope,
    TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome, TenantQuotaFinalizeRequest,
    TenantQuotaReservation, TenantQuotaReservationOutcome, TenantQuotaReservationRequest,
    TenantQuotaState,
};
use mooncake_store_rs_metadata::InMemoryMetadataBackend;
use mooncake_store_rs_transport::{
    Opcode, SegmentBuffer, SegmentInfo, SegmentKind, TransferProgress, TransferRequest,
    TransferStatus,
};
use parking_lot::Mutex;

pub fn bind_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener addr should resolve")
        .to_string();
    drop(listener);
    address
}

pub struct EnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvVarGuard {
    pub fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }

    pub fn unset(key: &'static str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.as_deref() {
            std::env::set_var(self.key, previous);
        } else {
            std::env::remove_var(self.key);
        }
    }
}
pub struct TestTransport {
    local_segment: String,
    rpc_host: String,
    rpc_port: u16,
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

impl TestTransport {
    pub fn new(local_segment: &str) -> Self {
        Self::new_with_rpc_address(local_segment, "127.0.0.1", 0)
    }

    pub fn new_with_rpc_address(local_segment: &str, rpc_host: &str, rpc_port: u16) -> Self {
        Self {
            local_segment: local_segment.to_string(),
            rpc_host: rpc_host.to_string(),
            rpc_port,
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
}

impl StoreTransport for TestTransport {
    fn segment_name(&self) -> mooncake_store_core::Result<String> {
        Ok(self.local_segment.clone())
    }

    fn rpc_server_address(&self) -> mooncake_store_core::Result<(String, u16)> {
        Ok((self.rpc_host.clone(), self.rpc_port))
    }

    fn open_segment(&self, segment_name: &str) -> mooncake_store_core::Result<u64> {
        self.state
            .lock()
            .segments_by_name
            .get(segment_name)
            .copied()
            .ok_or_else(|| StoreError::NotFound(format!("segment {segment_name} not found")))
    }

    fn close_segment(&self, handle: u64) -> mooncake_store_core::Result<()> {
        if self.state.lock().segments_by_handle.contains_key(&handle) {
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
        if state.segments_by_name.contains_key(&self.local_segment) {
            return Ok(());
        }
        register_segment(&mut state, self.local_segment.clone(), addr as usize, size);
        Ok(())
    }

    fn allocate_memory(
        &self,
        size: usize,
        _location: &str,
    ) -> mooncake_store_core::Result<*mut c_void> {
        let mut state = self.state.lock();
        let base = allocate_boxed_region(&mut state, size);
        Ok(base as *mut c_void)
    }

    fn free_memory(&self, addr: *mut c_void) -> mooncake_store_core::Result<()> {
        let base = addr as usize;
        let mut state = self.state.lock();
        state
            .allocations
            .remove(&base)
            .ok_or_else(|| StoreError::NotFound(format!("allocation {base:#x} not found")))?;
        let orphaned = state
            .segments_by_handle
            .iter()
            .filter_map(|(handle, segment)| (segment.base == base).then_some(*handle))
            .collect::<Vec<_>>();
        for handle in orphaned {
            state.segments_by_handle.remove(&handle);
            state
                .segments_by_name
                .retain(|_, current_handle| *current_handle != handle);
        }
        state.registered_memory.remove(&base);
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
            Some(recorded) if recorded == size => Ok(()),
            Some(recorded) => Err(StoreError::Allocator(format!(
                "registered size mismatch: expected={recorded} actual={size}"
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
                .copied()
                .ok_or_else(|| {
                    StoreError::NotFound(format!("segment handle {} not found", request.target_id))
                })?;
            validate_request_bounds(segment, request)?;
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

pub fn build_client(name: &str) -> StoreClient {
    build_client_with_metadata(name, Arc::new(InMemoryMetadataBackend::new()))
}

pub fn build_client_with_metadata(name: &str, metadata: Arc<dyn MetadataBackend>) -> StoreClient {
    build_client_with_metadata_and_state(name, metadata, ClientLifecycleState::Active)
}

#[cfg(test)]
pub(crate) fn build_client_with_metadata_and_expiry(
    name: &str,
    metadata: Arc<dyn MetadataBackend>,
    state: ClientLifecycleState,
    expires_at_ms: u64,
) -> StoreClient {
    let transport = Arc::new(TestTransport::new(&format!("{name}-segment")));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    StoreClientBuilder::new(metadata, name)
        .state(state)
        .label("storage", "true")
        .live_client_sync_interval(Duration::from_millis(25))
        .compatibility(CompatibilityDescriptor::default())
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(4 * 1024)
                .scratch_bytes(4 * 1024)
                .alignment(1)
                .reclaim_grace_ms(0),
        )
        .routed_writes(planner, 1)
        .build(expires_at_ms)
        .expect("test store client should build")
}

pub(crate) fn build_client_with_metadata_and_state(
    name: &str,
    metadata: Arc<dyn MetadataBackend>,
    state: ClientLifecycleState,
) -> StoreClient {
    let transport = Arc::new(TestTransport::new(&format!("{name}-segment")));
    build_client_with_metadata_and_transport(name, metadata, transport, state)
}

pub fn build_client_with_metadata_and_transport(
    name: &str,
    metadata: Arc<dyn MetadataBackend>,
    transport: Arc<dyn StoreTransport>,
    state: ClientLifecycleState,
) -> StoreClient {
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    StoreClientBuilder::new(metadata, name)
        .state(state)
        .label("storage", "true")
        .live_client_sync_interval(Duration::from_millis(25))
        .compatibility(CompatibilityDescriptor::default())
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(4 * 1024)
                .scratch_bytes(4 * 1024)
                .alignment(1)
                .reclaim_grace_ms(0),
        )
        .routed_writes(planner, 1)
        .build(60_000)
        .expect("test store client should build")
}

pub struct BlockingHealthMetadata {
    inner: InMemoryMetadataBackend,
    block_lease: Arc<AtomicBool>,
    block_state_update: Arc<AtomicBool>,
    block_route_cas: Arc<AtomicBool>,
    block_route_lookup: Arc<AtomicBool>,
    block_publish_segment: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
    entered: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

#[cfg(test)]
pub(crate) struct RecoveryCountingMetadata {
    inner: InMemoryMetadataBackend,
    lease_failures_remaining: Mutex<usize>,
    publish_segment_calls: AtomicUsize,
}

impl BlockingHealthMetadata {
    pub fn new(
        block_lease: Arc<AtomicBool>,
        block_state_update: Arc<AtomicBool>,
        block_route_cas: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
        entered: std::sync::mpsc::Sender<()>,
    ) -> Self {
        Self::new_with_route_lookup(
            block_lease,
            block_state_update,
            block_route_cas,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            release,
            entered,
        )
    }

    pub fn new_with_route_lookup(
        block_lease: Arc<AtomicBool>,
        block_state_update: Arc<AtomicBool>,
        block_route_cas: Arc<AtomicBool>,
        block_route_lookup: Arc<AtomicBool>,
        block_publish_segment: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
        entered: std::sync::mpsc::Sender<()>,
    ) -> Self {
        Self {
            inner: InMemoryMetadataBackend::new(),
            block_lease,
            block_state_update,
            block_route_cas,
            block_route_lookup,
            block_publish_segment,
            release,
            entered: std::sync::Mutex::new(Some(entered)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_publish_segment(
        block_publish_segment: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
        entered: std::sync::mpsc::Sender<()>,
    ) -> Self {
        Self::new_with_route_lookup(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            block_publish_segment,
            release,
            entered,
        )
    }

    fn maybe_block(&self, enabled: &AtomicBool) {
        if enabled.load(Ordering::SeqCst) {
            if let Some(sender) = self.entered.lock().expect("mutex poisoned").take() {
                let _ = sender.send(());
            }
            while !self.release.load(Ordering::SeqCst) {
                sleep(Duration::from_millis(10));
            }
        }
    }
}

#[cfg(test)]
impl RecoveryCountingMetadata {
    pub(crate) fn new() -> Self {
        Self {
            inner: InMemoryMetadataBackend::new(),
            lease_failures_remaining: Mutex::new(0),
            publish_segment_calls: AtomicUsize::new(0),
        }
    }

    pub(crate) fn fail_next_lease_upserts(&self, count: usize) {
        *self.lease_failures_remaining.lock() = count;
    }

    pub(crate) fn publish_segment_calls(&self) -> usize {
        self.publish_segment_calls.load(Ordering::SeqCst)
    }
}

impl MetadataBackend for BlockingHealthMetadata {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
        self.maybe_block(&self.block_lease);
        self.inner.upsert_client_lease(lease)
    }

    fn allocate_client_lease(
        &self,
        template: &ClientLease,
    ) -> mooncake_store_core::Result<ClientRuntimeId> {
        self.inner.allocate_client_lease(template)
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> mooncake_store_core::Result<()> {
        self.maybe_block(&self.block_state_update);
        self.inner.update_client_state(runtime, next)
    }

    fn get_client_lease(
        &self,
        runtime: &ClientRuntimeId,
    ) -> mooncake_store_core::Result<Option<ClientLease>> {
        self.inner.get_client_lease(runtime)
    }

    fn get_live_runtime_by_stable_id(
        &self,
        stable_id: &ClientStableId,
    ) -> mooncake_store_core::Result<Option<ClientLease>> {
        self.inner.get_live_runtime_by_stable_id(stable_id)
    }

    fn list_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
        self.inner.list_live_clients()
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> mooncake_store_core::Result<()> {
        self.maybe_block(&self.block_publish_segment);
        self.inner.publish_segment(segment)
    }

    fn unpublish_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> mooncake_store_core::Result<()> {
        self.inner.unpublish_segment(owner, segment)
    }

    fn get_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> mooncake_store_core::Result<Option<SegmentAnnouncement>> {
        self.inner.get_segment(owner, segment)
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
    ) -> mooncake_store_core::Result<SegmentReservation> {
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
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        self.maybe_block(&self.block_route_lookup);
        self.inner.get_object_route(key)
    }

    fn list_object_routes(&self) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
        self.inner.list_object_routes()
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<CasResult> {
        self.maybe_block(&self.block_route_cas);
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

    fn put_route_policy(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> mooncake_store_core::Result<()> {
        self.inner.put_route_policy(domain, policy)
    }

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> mooncake_store_core::Result<bool> {
        self.inner.delete_route_policy(domain)
    }

    fn list_route_policies(
        &self,
    ) -> mooncake_store_core::Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
        self.inner.list_route_policies()
    }

    fn get_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
    ) -> mooncake_store_core::Result<Option<TenantPolicy>> {
        self.inner.get_tenant_policy(scope)
    }

    fn list_tenant_policies(
        &self,
        tenant: Option<&str>,
    ) -> mooncake_store_core::Result<Vec<TenantPolicy>> {
        self.inner.list_tenant_policies(tenant)
    }

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> mooncake_store_core::Result<TenantPolicy> {
        self.inner.put_tenant_policy(policy, expected_version)
    }

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> mooncake_store_core::Result<bool> {
        self.inner.delete_tenant_policy(scope, expected_version)
    }

    fn get_tenant_quota_state(
        &self,
        scope: &TenantPolicyScope,
    ) -> mooncake_store_core::Result<Option<TenantQuotaState>> {
        self.inner.get_tenant_quota_state(scope)
    }

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<TenantObjectAccounting>> {
        self.inner.get_tenant_object_accounting(key)
    }

    fn get_tenant_quota_reservation(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<Option<TenantQuotaReservation>> {
        self.inner.get_tenant_quota_reservation(reservation_id)
    }

    fn list_tenant_eviction_candidates(
        &self,
        scope: &TenantPolicyScope,
        limit: usize,
    ) -> mooncake_store_core::Result<Vec<TenantObjectAccounting>> {
        self.inner.list_tenant_eviction_candidates(scope, limit)
    }

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> mooncake_store_core::Result<Vec<TenantQuotaReservation>> {
        self.inner.list_tenant_quota_reservations(scope)
    }

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
        self.inner.abort_tenant_quota(reservation_id)
    }

    fn put_handoff(&self, handoff: &HandoffPlan) -> mooncake_store_core::Result<()> {
        self.inner.put_handoff(handoff)
    }

    fn get_handoff(
        &self,
        stable_id: &ClientStableId,
    ) -> mooncake_store_core::Result<Option<HandoffPlan>> {
        self.inner.get_handoff(stable_id)
    }

    fn list_cold_tier_devices(
        &self,
        filter: &mooncake_store_core::ColdTierDeviceFilter,
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::ColdTierDeviceRecord>> {
        self.inner.list_cold_tier_devices(filter)
    }
}

#[cfg(test)]
impl MetadataBackend for RecoveryCountingMetadata {
    fn route_namespace(&self) -> String {
        self.inner.route_namespace()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> mooncake_store_core::Result<()> {
        let mut remaining = self.lease_failures_remaining.lock();
        if *remaining > 0 {
            *remaining -= 1;
            return Err(StoreError::Metadata("simulated redis outage".to_string()));
        }
        drop(remaining);
        self.inner.upsert_client_lease(lease)
    }

    fn allocate_client_lease(
        &self,
        template: &ClientLease,
    ) -> mooncake_store_core::Result<ClientRuntimeId> {
        self.inner.allocate_client_lease(template)
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> mooncake_store_core::Result<()> {
        self.inner.update_client_state(runtime, next)
    }

    fn get_client_lease(
        &self,
        runtime: &ClientRuntimeId,
    ) -> mooncake_store_core::Result<Option<ClientLease>> {
        self.inner.get_client_lease(runtime)
    }

    fn get_live_runtime_by_stable_id(
        &self,
        stable_id: &ClientStableId,
    ) -> mooncake_store_core::Result<Option<ClientLease>> {
        self.inner.get_live_runtime_by_stable_id(stable_id)
    }

    fn list_live_clients(&self) -> mooncake_store_core::Result<Vec<ClientLease>> {
        self.inner.list_live_clients()
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> mooncake_store_core::Result<()> {
        self.publish_segment_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.publish_segment(segment)
    }

    fn unpublish_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> mooncake_store_core::Result<()> {
        self.inner.unpublish_segment(owner, segment)
    }

    fn get_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> mooncake_store_core::Result<Option<SegmentAnnouncement>> {
        self.inner.get_segment(owner, segment)
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
    ) -> mooncake_store_core::Result<SegmentReservation> {
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
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        self.inner.get_object_route(key)
    }

    fn list_object_routes(&self) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
        self.inner.list_object_routes()
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<CasResult> {
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

    fn put_route_policy(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> mooncake_store_core::Result<()> {
        self.inner.put_route_policy(domain, policy)
    }

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> mooncake_store_core::Result<bool> {
        self.inner.delete_route_policy(domain)
    }

    fn list_route_policies(
        &self,
    ) -> mooncake_store_core::Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
        self.inner.list_route_policies()
    }

    fn get_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
    ) -> mooncake_store_core::Result<Option<TenantPolicy>> {
        self.inner.get_tenant_policy(scope)
    }

    fn list_tenant_policies(
        &self,
        tenant: Option<&str>,
    ) -> mooncake_store_core::Result<Vec<TenantPolicy>> {
        self.inner.list_tenant_policies(tenant)
    }

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> mooncake_store_core::Result<TenantPolicy> {
        self.inner.put_tenant_policy(policy, expected_version)
    }

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> mooncake_store_core::Result<bool> {
        self.inner.delete_tenant_policy(scope, expected_version)
    }

    fn get_tenant_quota_state(
        &self,
        scope: &TenantPolicyScope,
    ) -> mooncake_store_core::Result<Option<TenantQuotaState>> {
        self.inner.get_tenant_quota_state(scope)
    }

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<TenantObjectAccounting>> {
        self.inner.get_tenant_object_accounting(key)
    }

    fn get_tenant_quota_reservation(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<Option<TenantQuotaReservation>> {
        self.inner.get_tenant_quota_reservation(reservation_id)
    }

    fn list_tenant_eviction_candidates(
        &self,
        scope: &TenantPolicyScope,
        limit: usize,
    ) -> mooncake_store_core::Result<Vec<TenantObjectAccounting>> {
        self.inner.list_tenant_eviction_candidates(scope, limit)
    }

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> mooncake_store_core::Result<Vec<TenantQuotaReservation>> {
        self.inner.list_tenant_quota_reservations(scope)
    }

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
        self.inner.reserve_tenant_quota(request)
    }

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
        self.inner.finalize_tenant_quota(request)
    }

    fn abort_tenant_quota(
        &self,
        reservation_id: &str,
    ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
        self.inner.abort_tenant_quota(reservation_id)
    }

    fn put_handoff(&self, handoff: &HandoffPlan) -> mooncake_store_core::Result<()> {
        self.inner.put_handoff(handoff)
    }

    fn get_handoff(
        &self,
        stable_id: &ClientStableId,
    ) -> mooncake_store_core::Result<Option<HandoffPlan>> {
        self.inner.get_handoff(stable_id)
    }

    fn list_cold_tier_devices(
        &self,
        filter: &mooncake_store_core::ColdTierDeviceFilter,
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::ColdTierDeviceRecord>> {
        self.inner.list_cold_tier_devices(filter)
    }
}
